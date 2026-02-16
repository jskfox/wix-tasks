import cron from 'node-cron';
import { logger } from './utils/logger';
import { BaseTask } from './tasks/base-task';

const CTX = 'Scheduler';

// ── Execution history entry ──────────────────────────────────────────────────

export interface TaskRunEntry {
  startedAt: string;   // ISO timestamp
  finishedAt: string;  // ISO timestamp
  durationMs: number;
  status: 'success' | 'error';
  error?: string;
}

// ── Task state exposed to admin API ──────────────────────────────────────────

export interface TaskState {
  name: string;
  description: string;
  cronExpression: string;
  timezone: string;
  enabled: boolean;
  running: boolean;
  lastRun: TaskRunEntry | null;
  history: TaskRunEntry[];  // most recent first, capped
}

const MAX_HISTORY = 50;

// ── Internal registered task ─────────────────────────────────────────────────

interface RegisteredTask {
  task: BaseTask;
  job: cron.ScheduledTask;
  state: TaskState;
}

const registeredTasks: Map<string, RegisteredTask> = new Map();

// ── Core execution wrapper (shared by cron + manual trigger) ─────────────────

async function executeTask(entry: RegisteredTask): Promise<TaskRunEntry> {
  const { task, state } = entry;
  if (state.running) {
    throw new Error(`Task "${task.name}" is already running`);
  }

  state.running = true;
  const start = Date.now();
  const startedAt = new Date().toISOString();

  try {
    logger.info(CTX, `▶ Starting task: ${task.name}`);
    await task.execute();
    const durationMs = Date.now() - start;
    const duration = (durationMs / 1000).toFixed(2);
    logger.info(CTX, `✔ Task completed: ${task.name} (${duration}s)`);

    const run: TaskRunEntry = {
      startedAt,
      finishedAt: new Date().toISOString(),
      durationMs,
      status: 'success',
    };
    pushHistory(state, run);
    return run;
  } catch (err) {
    const durationMs = Date.now() - start;
    const duration = (durationMs / 1000).toFixed(2);
    const message = err instanceof Error ? err.message : String(err);
    logger.error(CTX, `✖ Task failed: ${task.name} (${duration}s) — ${message}`);

    const run: TaskRunEntry = {
      startedAt,
      finishedAt: new Date().toISOString(),
      durationMs,
      status: 'error',
      error: message,
    };
    pushHistory(state, run);
    return run;
  } finally {
    state.running = false;
  }
}

function pushHistory(state: TaskState, run: TaskRunEntry): void {
  state.lastRun = run;
  state.history.unshift(run);
  if (state.history.length > MAX_HISTORY) state.history.length = MAX_HISTORY;
}

// ── Public API ───────────────────────────────────────────────────────────────

export function registerTask(task: BaseTask): void {
  if (registeredTasks.has(task.name)) {
    logger.warn(CTX, `Task "${task.name}" is already registered — skipping`);
    return;
  }

  const state: TaskState = {
    name: task.name,
    description: task.description,
    cronExpression: task.cronExpression,
    timezone: task.timezone,
    enabled: true,
    running: false,
    lastRun: null,
    history: [],
  };

  const entry: RegisteredTask = { task, job: null as unknown as cron.ScheduledTask, state };

  entry.job = cron.schedule(
    task.cronExpression,
    async () => { await executeTask(entry); },
    { timezone: task.timezone, scheduled: false },
  );

  registeredTasks.set(task.name, entry);
  logger.info(CTX, `Registered task: "${task.name}" [${task.cronExpression}] tz=${task.timezone}`);
}

export function startAll(): void {
  for (const [name, { job }] of registeredTasks) {
    job.start();
    logger.info(CTX, `Started task: "${name}"`);
  }
  logger.info(CTX, `All ${registeredTasks.size} task(s) running`);
}

export function stopAll(): void {
  for (const [name, { job }] of registeredTasks) {
    job.stop();
    logger.info(CTX, `Stopped task: "${name}"`);
  }
}

export function listTasks(): string[] {
  return Array.from(registeredTasks.keys());
}

// ── Admin helpers ────────────────────────────────────────────────────────────

/** Get state snapshots for all registered tasks */
export function getTaskStates(): TaskState[] {
  return Array.from(registeredTasks.values()).map(e => e.state);
}

/** Get state for a single task */
export function getTaskState(name: string): TaskState | undefined {
  return registeredTasks.get(name)?.state;
}

/** Trigger a manual execution (async — returns immediately with a promise) */
export async function triggerTask(name: string): Promise<TaskRunEntry> {
  const entry = registeredTasks.get(name);
  if (!entry) throw new Error(`Unknown task: "${name}"`);
  return executeTask(entry);
}

/** Update the cron expression at runtime (reschedules the job) */
export function updateCron(name: string, newCron: string): void {
  if (!cron.validate(newCron)) {
    throw new Error(`Invalid cron expression: "${newCron}"`);
  }
  const entry = registeredTasks.get(name);
  if (!entry) throw new Error(`Unknown task: "${name}"`);

  entry.job.stop();
  entry.state.cronExpression = newCron;

  entry.job = cron.schedule(
    newCron,
    async () => { await executeTask(entry); },
    { timezone: entry.task.timezone, scheduled: true },
  );

  logger.info(CTX, `Updated cron for "${name}": ${newCron}`);
}

/** Enable or disable a task's cron schedule */
export function setTaskEnabled(name: string, enabled: boolean): void {
  const entry = registeredTasks.get(name);
  if (!entry) throw new Error(`Unknown task: "${name}"`);

  if (enabled) {
    entry.job.start();
  } else {
    entry.job.stop();
  }
  entry.state.enabled = enabled;
  logger.info(CTX, `Task "${name}" ${enabled ? 'enabled' : 'disabled'}`);
}

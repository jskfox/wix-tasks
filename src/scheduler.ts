import cron from 'node-cron';
import { Cron } from 'croner';
import { logger } from './utils/logger';
import { BaseTask } from './tasks/base-task';
import { getSetting, updateSetting } from './services/settings-db';

const CTX = 'Scheduler';

// ── Task state persistence helpers ───────────────────────────────────────────

/** Load task enabled state from SQLite (default: true if not found) */
function loadTaskEnabled(taskName: string): boolean {
  const key = `task.${taskName}.enabled`;
  const value = getSetting(key, 'true');
  return value === 'true';
}

/** Save task enabled state to SQLite */
function saveTaskEnabled(taskName: string, enabled: boolean): void {
  const key = `task.${taskName}.enabled`;
  updateSetting(key, enabled ? 'true' : 'false', 'tasks', `Enable/disable ${taskName} task`);
}

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
  nextRun: string | null;   // ISO timestamp of next scheduled execution
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
    // Recompute next run after execution
    state.nextRun = state.enabled ? computeNextRun(state.cronExpression, state.timezone) : null;
  }
}

function pushHistory(state: TaskState, run: TaskRunEntry): void {
  state.lastRun = run;
  state.history.unshift(run);
  if (state.history.length > MAX_HISTORY) state.history.length = MAX_HISTORY;
}

/** Compute next scheduled execution time for a cron expression in a given timezone */
function computeNextRun(cronExpr: string, tz: string): string | null {
  try {
    const job = new Cron(cronExpr, { timezone: tz });
    const next = job.nextRun();
    return next ? next.toISOString() : null;
  } catch (err) {
    logger.error(CTX, `Failed to compute next run for cron "${cronExpr}": ${(err as Error).message}`);
    return null;
  }
}

// ── Public API ───────────────────────────────────────────────────────────────

export function registerTask(task: BaseTask): void {
  if (registeredTasks.has(task.name)) {
    logger.warn(CTX, `Task "${task.name}" is already registered — skipping`);
    return;
  }

  // Load persisted enabled state from SQLite (default: true)
  const persistedEnabled = loadTaskEnabled(task.name);

  const state: TaskState = {
    name: task.name,
    description: task.description,
    cronExpression: task.cronExpression,
    timezone: task.timezone,
    enabled: persistedEnabled,
    running: false,
    lastRun: null,
    history: [],
    nextRun: computeNextRun(task.cronExpression, task.timezone),
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
  for (const [name, { job, state }] of registeredTasks) {
    if (state.enabled) {
      job.start();
      logger.info(CTX, `Started task: "${name}"`);
    } else {
      logger.info(CTX, `Task "${name}" disabled — not starting`);
    }
  }
  const enabledCount = Array.from(registeredTasks.values()).filter(e => e.state.enabled).length;
  logger.info(CTX, `${enabledCount}/${registeredTasks.size} task(s) running`);
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
  entry.state.nextRun = entry.state.enabled ? computeNextRun(newCron, entry.state.timezone) : null;

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
  entry.state.nextRun = enabled ? computeNextRun(entry.state.cronExpression, entry.state.timezone) : null;
  
  // Persist to SQLite
  saveTaskEnabled(name, enabled);
  
  logger.info(CTX, `Task "${name}" ${enabled ? 'enabled' : 'disabled'}`);
}

import cron from 'node-cron';
import { logger } from './utils/logger';
import { BaseTask } from './tasks/base-task';

const CTX = 'Scheduler';

interface RegisteredTask {
  task: BaseTask;
  job: cron.ScheduledTask;
}

const registeredTasks: Map<string, RegisteredTask> = new Map();

export function registerTask(task: BaseTask): void {
  if (registeredTasks.has(task.name)) {
    logger.warn(CTX, `Task "${task.name}" is already registered — skipping`);
    return;
  }

  const job = cron.schedule(
    task.cronExpression,
    async () => {
      const start = Date.now();
      logger.info(CTX, `▶ Starting task: ${task.name}`);
      try {
        await task.execute();
        const duration = ((Date.now() - start) / 1000).toFixed(2);
        logger.info(CTX, `✔ Task completed: ${task.name} (${duration}s)`);
      } catch (err) {
        const duration = ((Date.now() - start) / 1000).toFixed(2);
        const message = err instanceof Error ? err.message : String(err);
        logger.error(CTX, `✖ Task failed: ${task.name} (${duration}s) — ${message}`);
      }
    },
    {
      timezone: task.timezone,
      scheduled: false,
    },
  );

  registeredTasks.set(task.name, { task, job });
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

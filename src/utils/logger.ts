type LogLevel = 'debug' | 'info' | 'warn' | 'error';

const LEVELS: Record<LogLevel, number> = {
  debug: 0,
  info: 1,
  warn: 2,
  error: 3,
};

// ── In-memory log buffer for admin dashboard ─────────────────────────────────

export interface LogEntry {
  timestamp: string;
  level: LogLevel;
  context: string;
  message: string;
  data?: unknown;
}

const MAX_LOG_BUFFER = 2000;
const logBuffer: LogEntry[] = [];

/** Get recent log entries, optionally filtered by level and/or context */
export function getLogBuffer(opts?: {
  level?: LogLevel;
  context?: string;
  limit?: number;
}): LogEntry[] {
  let entries = logBuffer;
  if (opts?.level) {
    const minLevel = LEVELS[opts.level];
    entries = entries.filter(e => LEVELS[e.level] >= minLevel);
  }
  if (opts?.context) {
    const ctx = opts.context.toLowerCase();
    entries = entries.filter(e => e.context.toLowerCase().includes(ctx));
  }
  const limit = opts?.limit ?? 200;
  return entries.slice(0, limit);
}

// ── Logger class ─────────────────────────────────────────────────────────────

class Logger {
  private level: number;

  constructor(level: LogLevel = 'info') {
    this.level = LEVELS[level] ?? LEVELS.info;
  }

  private timestamp(): string {
    return new Date().toISOString();
  }

  private log(level: LogLevel, context: string, message: string, data?: unknown): void {
    if (LEVELS[level] < this.level) return;

    const ts = this.timestamp();
    const prefix = `[${ts}] [${level.toUpperCase()}] [${context}]`;
    const line = data !== undefined
      ? `${prefix} ${message} ${JSON.stringify(data)}`
      : `${prefix} ${message}`;

    // Buffer for admin dashboard
    logBuffer.unshift({ timestamp: ts, level, context, message, data });
    if (logBuffer.length > MAX_LOG_BUFFER) logBuffer.length = MAX_LOG_BUFFER;

    if (level === 'error') {
      console.error(line);
    } else if (level === 'warn') {
      console.warn(line);
    } else {
      console.log(line);
    }
  }

  debug(context: string, message: string, data?: unknown): void {
    this.log('debug', context, message, data);
  }

  info(context: string, message: string, data?: unknown): void {
    this.log('info', context, message, data);
  }

  warn(context: string, message: string, data?: unknown): void {
    this.log('warn', context, message, data);
  }

  error(context: string, message: string, data?: unknown): void {
    this.log('error', context, message, data);
  }
}

import { config } from '../config';
export const logger = new Logger(config.logLevel as LogLevel);

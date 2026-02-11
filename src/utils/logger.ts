type LogLevel = 'debug' | 'info' | 'warn' | 'error';

const LEVELS: Record<LogLevel, number> = {
  debug: 0,
  info: 1,
  warn: 2,
  error: 3,
};

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

    const prefix = `[${this.timestamp()}] [${level.toUpperCase()}] [${context}]`;
    const line = data !== undefined
      ? `${prefix} ${message} ${JSON.stringify(data)}`
      : `${prefix} ${message}`;

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

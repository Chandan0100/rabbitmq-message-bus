import { inspect } from 'node:util';
import { Logger as NestLogger } from '@nestjs/common';
import { Logger } from '../core/logger';

function format(message: string, meta: unknown[]): string {
  if (!meta.length) return message;

  const details = meta.map((value) =>
    value instanceof Error ? (value.stack ?? value.message) : inspect(value),
  );
  return `${message} ${details.join(' ')}`;
}

/** Sends the message bus logs through the NestJS logger instead of the console. */
export class NestLoggerAdapter implements Logger {
  private readonly logger: NestLogger;

  constructor(context = 'MessageBus') {
    this.logger = new NestLogger(context);
  }

  debug(message: string, ...meta: unknown[]): void {
    this.logger.debug(format(message, meta));
  }

  info(message: string, ...meta: unknown[]): void {
    this.logger.log(format(message, meta));
  }

  warn(message: string, ...meta: unknown[]): void {
    this.logger.warn(format(message, meta));
  }

  error(message: string, ...meta: unknown[]): void {
    this.logger.error(format(message, meta));
  }
}

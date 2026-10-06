export interface Logger {
  debug(message: string, ...meta: unknown[]): void;
  info(message: string, ...meta: unknown[]): void;
  warn(message: string, ...meta: unknown[]): void;
  error(message: string, ...meta: unknown[]): void;
}

export class ConsoleLogger implements Logger {
  constructor(private readonly debugEnabled = false) {}

  debug(message: string, ...meta: unknown[]): void {
    if (this.debugEnabled) console.debug(`DEBUG ${message}`, ...meta);
  }

  info(message: string, ...meta: unknown[]): void {
    console.log(`INFO ${message}`, ...meta);
  }

  warn(message: string, ...meta: unknown[]): void {
    console.warn(`WARN ${message}`, ...meta);
  }

  error(message: string, ...meta: unknown[]): void {
    console.error(`ERROR ${message}`, ...meta);
  }
}

export const silentLogger: Logger = {
  debug: () => undefined,
  info: () => undefined,
  warn: () => undefined,
  error: () => undefined,
};

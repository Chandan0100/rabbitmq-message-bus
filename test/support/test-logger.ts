import { Logger } from '../../src';

export interface TestLogger extends Logger {
  lines: { level: string; message: string; meta: unknown[] }[];
  messages(level?: string): string[];
}

/** Records every log line so tests can assert on what was reported. */
export function createTestLogger(): TestLogger {
  const lines: TestLogger['lines'] = [];
  const record =
    (level: string) =>
    (message: string, ...meta: unknown[]) => {
      lines.push({ level, message, meta });
    };

  return {
    lines,
    messages: (level) =>
      lines
        .filter((line) => !level || line.level === level)
        .map((line) => line.message),
    debug: record('debug'),
    info: record('info'),
    warn: record('warn'),
    error: record('error'),
  };
}

import { Injectable } from '@nestjs/common';
import { Logger } from 'rabbitmq-message-bus';
import { DemoDatabase } from './demo-database';

export interface LoggedEvent {
  id: number;
  at: string;
  source: string;
  level: string;
  message_id: string | null;
  text: string;
}

const UUID = /[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}/i;

function describe(meta: unknown[]): string {
  return meta
    .flat()
    .map((value) => (value instanceof Error ? value.message : String(value)))
    .join('; ');
}

/**
 * Collects what happens in every process into one table, which the dashboard
 * streams to the browser. A real service would log to stdout instead.
 */
@Injectable()
export class EventLog {
  constructor(private readonly db: DemoDatabase) {}

  write(source: string, level: string, text: string, messageId?: string): void {
    this.db.run(
      'insert into event_log (at, source, level, message_id, text) values (?, ?, ?, ?, ?)',
      new Date().toISOString(),
      source,
      level,
      messageId ?? UUID.exec(text)?.[0] ?? null,
      text,
    );
  }

  /**
   * A logger to hand to the library (`logger` option), so its own log lines
   * show up in the dashboard next to the application's.
   */
  loggerFor(source: string): Logger {
    const log =
      (level: string) =>
      (message: string, ...meta: unknown[]) =>
        this.write(source, level, meta.length ? `${message} ${describe(meta)}` : message);

    return {
      debug: () => undefined,
      info: log('info'),
      warn: log('warn'),
      error: log('error'),
    };
  }

  after(id: number, limit = 200): LoggedEvent[] {
    return this.db.all<LoggedEvent>(
      'select * from event_log where id > ? order by id limit ?',
      id,
      limit,
    );
  }

  latestId(): number {
    return this.db.get<{ id: number | null }>('select max(id) as id from event_log')?.id ?? 0;
  }
}

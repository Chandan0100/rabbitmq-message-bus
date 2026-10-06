import { DatabaseSync, SQLInputValue } from 'node:sqlite';
import { Injectable, OnApplicationShutdown } from '@nestjs/common';

const SCHEMA = `
  create table if not exists task (
    id text primary key,
    title text not null,
    assignee text,
    status text not null,
    created_at text not null
  );

  -- Messages waiting to be published. Written in the same transaction as the business change.
  create table if not exists outbox_message (
    id integer primary key autoincrement,
    message_id text not null unique,
    type text not null,
    exchange text not null,
    routing_key text not null,
    properties text not null,
    body text not null,
    status text not null default 'PENDING',
    created_at text not null,
    sent_at text
  );

  -- Which handler already processed which message.
  create table if not exists inbox_message (
    id integer primary key autoincrement,
    message_id text not null,
    handler_name text not null,
    handled_at text not null,
    unique (message_id, handler_name)
  );

  -- What the consumers produce.
  create table if not exists sent_email (
    id integer primary key autoincrement,
    message_id text not null,
    recipient text not null,
    subject text not null,
    sent_at text not null
  );
  create table if not exists audit_entry (
    id integer primary key autoincrement,
    message_id text not null,
    type text not null,
    summary text not null,
    recorded_at text not null
  );

  -- Demo bookkeeping: what the dashboard shows.
  create table if not exists handler_run (
    id integer primary key autoincrement,
    message_id text not null,
    handler_name text not null,
    redelivery_count integer not null,
    outcome text not null,
    error text,
    at text not null
  );
  create table if not exists event_log (
    id integer primary key autoincrement,
    at text not null,
    source text not null,
    level text not null,
    message_id text,
    text text not null
  );
  create table if not exists setting (
    key text primary key,
    value text not null
  );
`;

/**
 * The service's database. A SQLite file, so the API, the dispatcher and the
 * consumers can run as separate processes and still share it.
 */
@Injectable()
export class DemoDatabase implements OnApplicationShutdown {
  private readonly db: DatabaseSync;

  constructor() {
    this.db = new DatabaseSync(process.env.DEMO_DB ?? 'demo.sqlite');
    this.db.exec('pragma busy_timeout = 5000');
    this.initialise();
  }

  /** Several processes may open a fresh file at the same moment; the losers wait and try again. */
  private initialise(): void {
    for (let attempt = 1; ; attempt++) {
      try {
        this.db.exec('pragma journal_mode = wal');
        this.db.exec(SCHEMA);
        return;
      } catch (error) {
        if (attempt === 20) throw error;
        Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, 250);
      }
    }
  }

  run(sql: string, ...params: SQLInputValue[]): void {
    this.db.prepare(sql).run(...params);
  }

  all<T = Record<string, unknown>>(sql: string, ...params: SQLInputValue[]): T[] {
    return this.db.prepare(sql).all(...params) as T[];
  }

  get<T = Record<string, unknown>>(sql: string, ...params: SQLInputValue[]): T | undefined {
    return this.db.prepare(sql).get(...params) as T | undefined;
  }

  /** Everything `work` writes is committed together, or not at all. */
  transaction<T>(work: () => T): T {
    this.db.exec('begin immediate');
    try {
      const result = work();
      this.db.exec('commit');
      return result;
    } catch (error) {
      this.db.exec('rollback');
      throw error;
    }
  }

  getSetting(key: string, fallback: string): string {
    return (
      this.get<{ value: string }>('select value from setting where key = ?', key)
        ?.value ?? fallback
    );
  }

  setSetting(key: string, value: string): void {
    this.run(
      'insert into setting (key, value) values (?, ?) on conflict (key) do update set value = excluded.value',
      key,
      value,
    );
  }

  onApplicationShutdown(): void {
    this.db.close();
  }
}

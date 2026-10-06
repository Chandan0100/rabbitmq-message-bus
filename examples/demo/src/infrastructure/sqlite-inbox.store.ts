import { Injectable } from '@nestjs/common';
import { InboxStore } from 'rabbitmq-message-bus';
import { DemoDatabase } from './demo-database';

/**
 * The library's InboxStore port over the `inbox_message` table.
 *
 * The consumer calls `isHandled` before running a handler. The handlers of
 * this demo call `record` themselves, inside their own transaction (the
 * consumer modules use `inbox: { store, record: 'handler' }`).
 */
@Injectable()
export class SqliteInboxStore implements InboxStore {
  constructor(private readonly db: DemoDatabase) {}

  async isHandled(messageId: string, handlerName: string): Promise<boolean> {
    return (
      this.db.get(
        'select 1 from inbox_message where message_id = ? and handler_name = ?',
        messageId,
        handlerName,
      ) !== undefined
    );
  }

  async markHandled(messageId: string, handlerName: string): Promise<void> {
    this.record(messageId, handlerName);
  }

  /** Synchronous, so a handler can call it inside `db.transaction(...)`. */
  record(messageId: string, handlerName: string): void {
    this.db.run(
      'insert into inbox_message (message_id, handler_name, handled_at) values (?, ?, ?)',
      messageId,
      handlerName,
      new Date().toISOString(),
    );
  }
}

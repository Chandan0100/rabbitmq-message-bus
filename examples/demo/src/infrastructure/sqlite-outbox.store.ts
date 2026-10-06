import { Injectable } from '@nestjs/common';
import { OutboxRecord, OutboxStore } from 'rabbitmq-message-bus';
import { DemoDatabase } from './demo-database';

interface OutboxRow {
  id: number;
  message_id: string;
  type: string;
  exchange: string;
  routing_key: string;
  properties: string;
  body: string;
}

/** The library's OutboxStore port over the `outbox_message` table. */
@Injectable()
export class SqliteOutboxStore implements OutboxStore {
  constructor(private readonly db: DemoDatabase) {}

  async fetchPending(limit: number): Promise<OutboxRecord[]> {
    return this.db
      .all<OutboxRow>(
        "select * from outbox_message where status = 'PENDING' order by id limit ?",
        limit,
      )
      .map((row) => ({
        messageId: row.message_id,
        type: row.type,
        exchange: row.exchange,
        routingKey: row.routing_key,
        body: JSON.parse(row.body),
        properties: JSON.parse(row.properties),
      }));
  }

  async markAsSent(record: OutboxRecord): Promise<void> {
    this.db.run(
      "update outbox_message set status = 'SENT', sent_at = ? where message_id = ?",
      new Date().toISOString(),
      record.messageId,
    );
  }
}

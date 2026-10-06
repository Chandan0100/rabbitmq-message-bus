import { PublishProperties } from './confirm';

/** A message waiting in the outbox to be published. */
export interface OutboxRecord {
  messageId: string;
  type: string;
  /** Falls back to the relay's `defaultExchange` when empty. */
  exchange?: string;
  routingKey: string;
  body: unknown;
  /** AMQP properties, including headers, stored with the message. */
  properties?: PublishProperties;
}

/**
 * Where pending messages are stored, usually a table written in the same
 * transaction as the business change.
 *
 * `scope` is an opaque partition (database schema, tenant...) forwarded from
 * the worker; stores that are bound to a single partition can ignore it.
 */
export interface OutboxStore<TRecord extends OutboxRecord = OutboxRecord> {
  /** Oldest pending records first. */
  fetchPending(limit: number, scope?: string): Promise<TRecord[]>;

  /** Receives the very object returned by `fetchPending`. */
  markAsSent(record: TRecord, scope?: string): Promise<void>;
}

/** Non-durable store for tests and local experiments. */
export class InMemoryOutboxStore implements OutboxStore {
  readonly sent: OutboxRecord[] = [];
  private readonly pending: OutboxRecord[] = [];

  add(...records: OutboxRecord[]): void {
    this.pending.push(...records);
  }

  getPending(): OutboxRecord[] {
    return [...this.pending];
  }

  async fetchPending(limit: number): Promise<OutboxRecord[]> {
    return this.pending.slice(0, limit);
  }

  async markAsSent(record: OutboxRecord): Promise<void> {
    const index = this.pending.indexOf(record);
    if (index !== -1) this.pending.splice(index, 1);
    this.sent.push(record);
  }
}

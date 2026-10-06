import {
  ConfigurationError,
  ConnectionFailedError,
  toError,
} from '../errors';
import { ContextRunner } from '../consumer/message-handler';
import { ConsoleLogger, Logger } from '../logger';
import { OutboxRecord, OutboxStore } from './outbox-store';
import { Publisher } from './publisher';

export interface OutboxRelayOptions {
  /** Exchange used for records that do not name one. */
  defaultExchange?: string;
  /** Records published per run when the caller gives no limit. Default: 10. */
  batchSize?: number;
  /**
   * Wraps a whole run, from reading the pending records to marking them as
   * sent. Use it to open what the store expects, such as an ORM request context.
   */
  runInContext?: ContextRunner;
}

export interface DispatchOptions {
  limit?: number;
  /** Opaque partition (database schema, tenant...) forwarded to the outbox store. */
  scope?: string;
}

export interface DispatchResult {
  fetched: number;
  published: number;
  failed: number;
}

/**
 * Publishes the pending records of an outbox, in order, and marks each one as
 * sent once the broker has confirmed it. A record that fails stays pending and
 * is picked up by the next run, so consumers must tolerate duplicates.
 */
export class OutboxRelay<TRecord extends OutboxRecord = OutboxRecord> {
  private readonly batchSize: number;

  constructor(
    private readonly store: OutboxStore<TRecord>,
    private readonly publisher: Publisher,
    private readonly options: OutboxRelayOptions = {},
    private readonly logger: Logger = new ConsoleLogger(),
  ) {
    this.batchSize = options.batchSize ?? 10;
  }

  /** Rejects with `ConnectionFailedError` when the broker cannot be reached; other failures are counted. */
  async dispatch(options: DispatchOptions = {}): Promise<DispatchResult> {
    const runInContext = this.options.runInContext ?? ((work) => work());
    return runInContext(() => this.run(options));
  }

  private async run(options: DispatchOptions): Promise<DispatchResult> {
    const { scope } = options;
    const records = await this.store.fetchPending(
      options.limit ?? this.batchSize,
      scope,
    );
    const result: DispatchResult = {
      fetched: records.length,
      published: 0,
      failed: 0,
    };

    if (!records.length) {
      this.logger.info('No messages pending to dispatch.');
      return result;
    }

    for (const record of records) {
      try {
        await this.publish(record);
      } catch (error) {
        if (error instanceof ConnectionFailedError) throw error;
        result.failed += 1;
        this.logger.error(
          `Error while publishing message ${record.type} with id ${record.messageId}: ${
            toError(error).message
          }`,
        );
        continue;
      }

      try {
        await this.store.markAsSent(record, scope);
        result.published += 1;
      } catch (error) {
        result.failed += 1;
        this.logger.error(
          `Message ${record.type} with id ${record.messageId} was published but could not be marked as sent; it will be published again: ${
            toError(error).message
          }`,
        );
      }
    }

    this.logger.info(
      `Published ${result.published} of ${result.fetched} messages.`,
    );
    return result;
  }

  private async publish(record: TRecord): Promise<void> {
    const exchange = record.exchange || this.options.defaultExchange;
    if (exchange === undefined) {
      throw new ConfigurationError(
        'The record names no exchange and the relay has no defaultExchange.',
      );
    }

    await this.publisher.publish({
      exchange,
      routingKey: record.routingKey,
      body: record.body,
      properties: {
        messageId: record.messageId,
        type: record.type,
        ...record.properties,
      },
    });
  }
}

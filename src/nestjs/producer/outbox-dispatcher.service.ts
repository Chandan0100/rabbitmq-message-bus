import { Inject, Injectable, OnApplicationShutdown } from '@nestjs/common';
import { ConnectionManager } from '../../core/connection/connection-manager';
import { ConnectionOptions } from '../../core/connection/connection-options';
import { ContextRunner } from '../../core/consumer/message-handler';
import { ConfigurationError } from '../../core/errors';
import { ConsoleLogger, Logger } from '../../core/logger';
import {
  DispatchOptions,
  DispatchResult,
  OutboxRelay,
} from '../../core/publisher/outbox-relay';
import { OutboxStore } from '../../core/publisher/outbox-store';
import { Publisher } from '../../core/publisher/publisher';
import { MessageSerializer } from '../../core/serialization/message-serializer';
import { ExchangeDefinition } from '../../core/topology/topology';
import {
  RABBITMQ_PRODUCER_CONNECTION,
  RABBITMQ_PRODUCER_OPTIONS,
} from '../tokens';

export interface RabbitMqProducerModuleOptions {
  connection: ConnectionOptions;
  /** Exchanges messages are published to. They are declared before the first publish. */
  exchanges: ExchangeDefinition[];
  /** Exchange of outbox records that do not name one. Default: the first of `exchanges`. */
  defaultExchange?: string;
  /** Required to dispatch an outbox; not needed to use `Publisher` alone. */
  outboxStore?: OutboxStore<any>;
  /** Records published per run when the caller gives no limit. Default: 10. */
  batchSize?: number;
  /** Wraps a whole dispatch run, for example in an ORM request context. */
  runInContext?: ContextRunner;
  confirmTimeoutMs?: number;
  serializer?: MessageSerializer;
  /** Default: logs to the console. */
  logger?: Logger;
}

/** Publishes the pending messages of the outbox of a `RabbitMqProducerModule`. */
@Injectable()
export class OutboxDispatcher implements OnApplicationShutdown {
  constructor(
    @Inject(RABBITMQ_PRODUCER_OPTIONS)
    private readonly options: RabbitMqProducerModuleOptions,
    @Inject(RABBITMQ_PRODUCER_CONNECTION)
    private readonly connection: ConnectionManager,
    private readonly publisher: Publisher,
  ) {}

  /** One run: publishes up to `limit` pending records, then releases the channel. */
  async dispatch(options: DispatchOptions = {}): Promise<DispatchResult> {
    const {
      outboxStore,
      exchanges,
      defaultExchange,
      batchSize,
      runInContext,
      logger,
    } = this.options;

    if (!outboxStore) {
      throw new ConfigurationError(
        'RabbitMqProducerModule needs an outboxStore to dispatch messages.',
      );
    }

    const relay = new OutboxRelay(
      outboxStore,
      this.publisher,
      {
        defaultExchange: defaultExchange ?? exchanges[0]?.name,
        batchSize,
        runInContext,
      },
      logger ?? new ConsoleLogger(),
    );

    try {
      return await relay.dispatch(options);
    } finally {
      await this.publisher.close();
    }
  }

  async onApplicationShutdown(): Promise<void> {
    await this.publisher.close();
    await this.connection.close();
  }
}

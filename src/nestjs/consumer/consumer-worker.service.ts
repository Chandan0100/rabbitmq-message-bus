import { EventEmitter } from 'node:events';
import {
  Inject,
  Injectable,
  OnApplicationBootstrap,
  OnApplicationShutdown,
} from '@nestjs/common';
import { ConnectionManager } from '../../core/connection/connection-manager';
import { ConnectionOptions } from '../../core/connection/connection-options';
import { Consumer, ConsumerOptions } from '../../core/consumer/consumer';
import { ResolvedConsumerEndpoint } from '../../core/consumer/consumer-endpoint';
import {
  applyEndpointOverrides,
  EndpointOverrides,
} from '../../core/consumer/endpoint-overrides';
import { ConsoleLogger, Logger } from '../../core/logger';
import { NestHandlerMap, NestHandlerResolver } from '../handler-resolver';
import { RABBITMQ_CONSUMER_OPTIONS } from '../tokens';

export interface RabbitMqConsumerModuleOptions extends ConsumerOptions {
  connection: ConnectionOptions;
  /** Message type to the processors handling it. */
  handlers: NestHandlerMap;
  /** Default: logs to the console. */
  logger?: Logger;
  /**
   * Start consuming when the application boots. Leave it off for a worker
   * started from the command line, which calls `start()` itself. Default: false.
   */
  autoStart?: boolean;
}

export interface ConsumerStartOptions {
  /** Opaque partition (database schema, tenant...) forwarded to the inbox store. */
  scope?: string;
  endpoint?: EndpointOverrides;
}

/**
 * Runs the consumer of a `RabbitMqConsumerModule`.
 *
 * Events:
 * - `failed` (Error): the broker is unreachable or the subscription cannot be
 *   restored. The process should exit so that its supervisor restarts it.
 */
@Injectable()
export class ConsumerWorker
  extends EventEmitter
  implements OnApplicationBootstrap, OnApplicationShutdown
{
  private connection?: ConnectionManager;
  private consumer?: Consumer;

  constructor(
    @Inject(RABBITMQ_CONSUMER_OPTIONS)
    private readonly options: RabbitMqConsumerModuleOptions,
    private readonly handlerResolver: NestHandlerResolver,
  ) {
    super();
  }

  isConsuming(): boolean {
    return this.consumer?.isConsuming() ?? false;
  }

  getEndpoint(): ResolvedConsumerEndpoint | undefined {
    return this.consumer?.getEndpoint();
  }

  async start(startOptions: ConsumerStartOptions = {}): Promise<void> {
    if (this.consumer) return;

    const {
      connection: connectionOptions,
      handlers,
      logger = new ConsoleLogger(),
      autoStart: _autoStart,
      ...consumerOptions
    } = this.options;

    const connection = new ConnectionManager(connectionOptions, logger);
    const consumer = new Consumer(
      connection,
      this.handlerResolver.createRegistry(handlers),
      {
        ...consumerOptions,
        scope: startOptions.scope ?? consumerOptions.scope,
        endpoint: applyEndpointOverrides(
          consumerOptions.endpoint,
          startOptions.endpoint,
        ),
      },
      logger,
    );

    connection.on('failed', (error: Error) => this.emit('failed', error));
    consumer.on('failed', (error: Error) => this.emit('failed', error));
    this.connection = connection;
    this.consumer = consumer;

    try {
      await consumer.start();
    } catch (error) {
      await this.stop();
      throw error;
    }
  }

  /** Finishes the in-flight messages, then closes the connection. */
  async stop(): Promise<void> {
    const { consumer, connection } = this;
    this.consumer = undefined;
    this.connection = undefined;

    await consumer?.stop();
    await connection?.close();
  }

  async onApplicationBootstrap(): Promise<void> {
    if (this.options.autoStart) await this.start();
  }

  async onApplicationShutdown(): Promise<void> {
    await this.stop();
  }
}

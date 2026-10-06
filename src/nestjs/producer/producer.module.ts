import {
  DynamicModule,
  FactoryProvider,
  Module,
  ModuleMetadata,
  Provider,
} from '@nestjs/common';
import { ConnectionManager } from '../../core/connection/connection-manager';
import { ConsoleLogger } from '../../core/logger';
import { Publisher } from '../../core/publisher/publisher';
import {
  moduleInstanceMarker,
  RABBITMQ_PRODUCER_CONNECTION,
  RABBITMQ_PRODUCER_OPTIONS,
} from '../tokens';
import {
  OutboxDispatcher,
  RabbitMqProducerModuleOptions,
} from './outbox-dispatcher.service';

export interface RabbitMqProducerModuleAsyncOptions {
  imports?: ModuleMetadata['imports'];
  inject?: FactoryProvider['inject'];
  useFactory: (
    ...args: any[]
  ) => RabbitMqProducerModuleOptions | Promise<RabbitMqProducerModuleOptions>;
  /** Providers the factory depends on, such as the outbox store. */
  extraProviders?: Provider[];
}

// The connection is opened by the first publish, not when the module is created.
const sharedProviders: Provider[] = [
  {
    provide: RABBITMQ_PRODUCER_CONNECTION,
    useFactory: (options: RabbitMqProducerModuleOptions) =>
      new ConnectionManager(
        options.connection,
        options.logger ?? new ConsoleLogger(),
      ),
    inject: [RABBITMQ_PRODUCER_OPTIONS],
  },
  {
    provide: Publisher,
    useFactory: (
      connection: ConnectionManager,
      options: RabbitMqProducerModuleOptions,
    ) =>
      new Publisher(
        connection,
        {
          exchanges: options.exchanges,
          confirmTimeoutMs: options.confirmTimeoutMs,
          serializer: options.serializer,
        },
        options.logger ?? new ConsoleLogger(),
      ),
    inject: [RABBITMQ_PRODUCER_CONNECTION, RABBITMQ_PRODUCER_OPTIONS],
  },
  OutboxDispatcher,
];

/** Gives a NestJS module a confirming `Publisher` and an `OutboxDispatcher`. */
@Module({})
export class RabbitMqProducerModule {
  static register(options: RabbitMqProducerModuleOptions): DynamicModule {
    return {
      module: RabbitMqProducerModule,
      providers: [
        moduleInstanceMarker(),
        { provide: RABBITMQ_PRODUCER_OPTIONS, useValue: options },
        ...sharedProviders,
      ],
      exports: [Publisher, OutboxDispatcher],
    };
  }

  static registerAsync(
    options: RabbitMqProducerModuleAsyncOptions,
  ): DynamicModule {
    return {
      module: RabbitMqProducerModule,
      imports: options.imports ?? [],
      providers: [
        moduleInstanceMarker(),
        {
          provide: RABBITMQ_PRODUCER_OPTIONS,
          useFactory: options.useFactory,
          inject: options.inject ?? [],
        },
        ...sharedProviders,
        ...(options.extraProviders ?? []),
      ],
      exports: [Publisher, OutboxDispatcher],
    };
  }
}

import {
  DynamicModule,
  FactoryProvider,
  Module,
  ModuleMetadata,
  Provider,
} from '@nestjs/common';
import { NestHandlerResolver } from '../handler-resolver';
import { moduleInstanceMarker, RABBITMQ_CONSUMER_OPTIONS } from '../tokens';
import {
  ConsumerWorker,
  RabbitMqConsumerModuleOptions,
} from './consumer-worker.service';

export interface RabbitMqConsumerModuleAsyncOptions {
  imports?: ModuleMetadata['imports'];
  inject?: FactoryProvider['inject'];
  useFactory: (
    ...args: any[]
  ) => RabbitMqConsumerModuleOptions | Promise<RabbitMqConsumerModuleOptions>;
  /** Providers the factory depends on, such as the inbox store. */
  extraProviders?: Provider[];
}

/** Gives a NestJS module a `ConsumerWorker` for one consumer endpoint. */
@Module({})
export class RabbitMqConsumerModule {
  static register(options: RabbitMqConsumerModuleOptions): DynamicModule {
    return {
      module: RabbitMqConsumerModule,
      providers: [
        moduleInstanceMarker(),
        { provide: RABBITMQ_CONSUMER_OPTIONS, useValue: options },
        NestHandlerResolver,
        ConsumerWorker,
      ],
      exports: [ConsumerWorker],
    };
  }

  static registerAsync(
    options: RabbitMqConsumerModuleAsyncOptions,
  ): DynamicModule {
    return {
      module: RabbitMqConsumerModule,
      imports: options.imports ?? [],
      providers: [
        moduleInstanceMarker(),
        {
          provide: RABBITMQ_CONSUMER_OPTIONS,
          useFactory: options.useFactory,
          inject: options.inject ?? [],
        },
        NestHandlerResolver,
        ConsumerWorker,
        ...(options.extraProviders ?? []),
      ],
      exports: [ConsumerWorker],
    };
  }
}

import { DynamicModule, Global, Module } from '@nestjs/common';
import {
  DestinationRegistry,
  DestinationResolver,
  MessageDestination,
} from '../core/publisher/destination-registry';

export interface MessageDestinationOptions {
  /** Message type to the exchange and routing key it is published with. */
  destinations?: Record<string, MessageDestination>;
  /** Used for message types that are not listed, for example `routeByMessageType(exchange)`. */
  fallback?: DestinationResolver;
}

const MESSAGE_DESTINATION_OPTIONS = Symbol('MESSAGE_DESTINATION_OPTIONS');
const MESSAGE_DESTINATION_REGISTRATION = Symbol(
  'MESSAGE_DESTINATION_REGISTRATION',
);

@Global()
@Module({
  providers: [
    { provide: DestinationRegistry, useFactory: () => new DestinationRegistry() },
  ],
  exports: [DestinationRegistry],
})
class DestinationRegistryModule {}

/**
 * Makes one `DestinationRegistry` injectable across the application. Each
 * bounded context contributes its own message types with `register()`.
 */
@Module({})
export class MessageDestinationModule {
  static register(options: MessageDestinationOptions = {}): DynamicModule {
    return {
      module: MessageDestinationModule,
      imports: [DestinationRegistryModule],
      providers: [
        // Kept as a value so that two registrations never look like the same module to NestJS.
        { provide: MESSAGE_DESTINATION_OPTIONS, useValue: options },
        {
          provide: MESSAGE_DESTINATION_REGISTRATION,
          useFactory: (
            registry: DestinationRegistry,
            registered: MessageDestinationOptions,
          ) => {
            registry.registerAll(registered.destinations ?? {});
            if (registered.fallback) registry.setFallback(registered.fallback);
            return true;
          },
          inject: [DestinationRegistry, MESSAGE_DESTINATION_OPTIONS],
        },
      ],
      exports: [DestinationRegistryModule],
    };
  }
}

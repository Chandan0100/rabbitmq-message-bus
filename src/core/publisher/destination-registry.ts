import { ConfigurationError } from '../errors';

export interface MessageDestination {
  exchange: string;
  routingKey: string;
}

/** Works out a destination for a message type that was not registered explicitly. */
export type DestinationResolver = (
  messageType: string,
) => MessageDestination | undefined;

/**
 * Routes every message to one exchange with its type as routing key. Fits a
 * `direct` exchange whose queues bind one key per handled type, and a `topic`
 * exchange whose queues bind a pattern covering the type names.
 */
export const routeByMessageType =
  (exchange: string): DestinationResolver =>
  (messageType) => ({ exchange, routingKey: messageType });

export interface DestinationRegistryOptions {
  fallback?: DestinationResolver;
}

/** Knows which exchange and routing key each message type is published with. */
export class DestinationRegistry {
  private readonly destinations = new Map<string, MessageDestination>();
  private fallback?: DestinationResolver;

  constructor(options: DestinationRegistryOptions = {}) {
    this.fallback = options.fallback;
  }

  setFallback(fallback: DestinationResolver | undefined): this {
    this.fallback = fallback;
    return this;
  }

  register(messageType: string, destination: MessageDestination): this {
    this.destinations.set(messageType, destination);
    return this;
  }

  registerAll(destinations: Record<string, MessageDestination>): this {
    for (const [messageType, destination] of Object.entries(destinations)) {
      this.register(messageType, destination);
    }
    return this;
  }

  get(messageType: string): MessageDestination | undefined {
    return this.destinations.get(messageType) ?? this.fallback?.(messageType);
  }

  /** Like `get`, but an unknown message type is an error instead of `undefined`. */
  resolve(messageType: string): MessageDestination {
    const destination = this.get(messageType);
    if (!destination) {
      throw new ConfigurationError(
        `No destination registered for message type "${messageType}".`,
      );
    }
    return destination;
  }
}

import { ConfigurationError } from '../errors';
import { BackoffStrategy, linearBackoff } from '../recoverability/backoff';
import {
  assertExchangeDefinition,
  ExchangeDefinition,
  QueueDefinition,
  QueueType,
  Topology,
  uniqueExchanges,
} from '../topology/topology';

export interface RetryOptions {
  /** Direct exchange that routes failed messages into the retry queue and back to the primary queue. */
  exchange: ExchangeDefinition;
  /** Queue where failed messages wait before the next attempt. Default: `<queue>.events-retry`. */
  queue?: string;
  /** Routing key that leads into the retry queue. Default: the retry queue name. */
  queueBindingKey?: string;
  /** Routing key that brings an expired message back to the primary queue only. Default: `<queue>.retry`. */
  returnBindingKey?: string;
  /** In-process attempts made straight after a failure, before a delayed retry. Default: 5. */
  immediateRetries?: number;
  /** Trips through the retry queue before the message goes to the error queue. Default: 3. */
  delayedRetries?: number;
  /** `x-message-ttl` of the retry queue in milliseconds; the longest possible delay. Default: 10000. */
  queueMessageTtl?: number;
  /** Delay before each delayed retry. Default: `linearBackoff(2000)`. */
  backoff?: BackoffStrategy;
}

export interface ErrorQueueOptions {
  exchange: ExchangeDefinition;
  routingKey: string;
  /**
   * When set, the queue is declared and bound so failed messages are never
   * dropped. Leave it out if another team owns the error queue.
   */
  queue?: string;
}

export interface ConsumerEndpointOptions {
  /** Identity of this endpoint. Stamped on retried messages so that only this endpoint picks them up again. */
  appName: string;
  /** Primary queue this endpoint consumes from. */
  queue: string;
  /** Exchange the primary queue is bound to. */
  exchange: ExchangeDefinition;
  /**
   * Keys binding the primary queue to its exchange. Patterns for a `topic`
   * exchange, exact routing keys for a `direct` one, ignored for `fanout`.
   */
  bindingKeys?: string | string[];
  /** Also bind one key per registered message type. Handy with a `direct` exchange. Default: false. */
  bindHandledTypes?: boolean;
  /** Type of the declared queues. Left to the broker default when omitted. */
  queueType?: QueueType;
  /** Maximum number of unacknowledged messages processed at once. Default: 10. */
  prefetch?: number;
  retry: RetryOptions;
  error: ErrorQueueOptions;
}

export interface ResolvedConsumerEndpoint {
  appName: string;
  queue: string;
  exchange: ExchangeDefinition;
  bindingKeys: string[];
  bindHandledTypes: boolean;
  queueType?: QueueType;
  prefetch: number;
  retry: Required<RetryOptions>;
  error: ErrorQueueOptions;
}

function requireText(value: unknown, name: string): string {
  if (typeof value !== 'string' || value.trim() === '') {
    throw new ConfigurationError(`Missing required consumer option: ${name}`);
  }
  return value;
}

function requireInteger(value: unknown, name: string, minimum: number): number {
  if (typeof value !== 'number' || !Number.isInteger(value) || value < minimum) {
    throw new ConfigurationError(
      `Consumer option ${name} must be an integer >= ${minimum}, received ${String(value)}.`,
    );
  }
  return value;
}

function toArray(value: string | string[] | undefined): string[] {
  if (value === undefined) return [];
  return Array.isArray(value) ? value : [value];
}

export function resolveConsumerEndpoint(
  options: ConsumerEndpointOptions,
): ResolvedConsumerEndpoint {
  if (!options) throw new ConfigurationError('Consumer endpoint is required.');

  const appName = requireText(options.appName, 'appName');
  const queue = requireText(options.queue, 'queue');
  assertExchangeDefinition(options.exchange, 'Primary');

  if (!options.retry) {
    throw new ConfigurationError('Missing required consumer option: retry');
  }
  assertExchangeDefinition(options.retry.exchange, 'Retry');
  if (options.retry.exchange.type !== 'direct') {
    throw new ConfigurationError(
      `Retry exchange "${options.retry.exchange.name}" must be of type "direct": retried messages are routed by exact key back to a single queue.`,
    );
  }

  if (!options.error) {
    throw new ConfigurationError('Missing required consumer option: error');
  }
  assertExchangeDefinition(options.error.exchange, 'Error');

  const retryQueue = options.retry.queue ?? `${queue}.events-retry`;

  const endpoint: ResolvedConsumerEndpoint = {
    appName,
    queue,
    exchange: options.exchange,
    bindingKeys: [...new Set(toArray(options.bindingKeys))],
    bindHandledTypes: options.bindHandledTypes ?? false,
    queueType: options.queueType,
    prefetch: requireInteger(options.prefetch ?? 10, 'prefetch', 1),
    retry: {
      exchange: options.retry.exchange,
      queue: requireText(retryQueue, 'retry.queue'),
      queueBindingKey: requireText(
        options.retry.queueBindingKey ?? retryQueue,
        'retry.queueBindingKey',
      ),
      returnBindingKey: requireText(
        options.retry.returnBindingKey ?? `${queue}.retry`,
        'retry.returnBindingKey',
      ),
      immediateRetries: requireInteger(
        options.retry.immediateRetries ?? 5,
        'retry.immediateRetries',
        0,
      ),
      delayedRetries: requireInteger(
        options.retry.delayedRetries ?? 3,
        'retry.delayedRetries',
        0,
      ),
      queueMessageTtl: requireInteger(
        options.retry.queueMessageTtl ?? 10000,
        'retry.queueMessageTtl',
        1,
      ),
      backoff: options.retry.backoff ?? linearBackoff(),
    },
    error: {
      exchange: options.error.exchange,
      routingKey: requireText(options.error.routingKey, 'error.routingKey'),
      queue: options.error.queue,
    },
  };

  assertNoRoutingCollision(endpoint);

  return endpoint;
}

/**
 * The retry flow relies on three keys reaching exactly one queue each. Keys
 * that overlap on a shared exchange would duplicate or short-circuit messages.
 */
function assertNoRoutingCollision(endpoint: ResolvedConsumerEndpoint): void {
  const { retry, error, queue } = endpoint;

  uniqueExchanges([retry.exchange, endpoint.exchange, error.exchange]);

  if (retry.queue === queue) {
    throw new ConfigurationError(
      `Retry queue and primary queue must differ, both are "${queue}".`,
    );
  }
  if (retry.queueBindingKey === retry.returnBindingKey) {
    throw new ConfigurationError(
      `retry.queueBindingKey and retry.returnBindingKey must differ, both are "${retry.queueBindingKey}": a failed message would reach the primary queue without waiting.`,
    );
  }
  if (
    retry.exchange.name === endpoint.exchange.name &&
    endpoint.bindingKeys.includes(retry.queueBindingKey)
  ) {
    throw new ConfigurationError(
      `Binding key "${retry.queueBindingKey}" is used both by the primary queue and the retry queue on exchange "${retry.exchange.name}".`,
    );
  }
  if (
    error.exchange.name === retry.exchange.name &&
    [retry.queueBindingKey, retry.returnBindingKey].includes(error.routingKey)
  ) {
    throw new ConfigurationError(
      `error.routingKey "${error.routingKey}" collides with a retry key on exchange "${error.exchange.name}".`,
    );
  }
}

/** Keys the primary queue is bound with, once the handled message types are known. */
export function resolveBindingKeys(
  endpoint: ResolvedConsumerEndpoint,
  handledTypes: string[],
): string[] {
  if (endpoint.exchange.type === 'fanout') return [''];

  const keys = [
    ...new Set([
      ...endpoint.bindingKeys,
      ...(endpoint.bindHandledTypes ? handledTypes : []),
    ]),
  ];

  if (keys.length === 0) {
    throw new ConfigurationError(
      `Queue "${endpoint.queue}" has no binding key for ${endpoint.exchange.type} exchange "${endpoint.exchange.name}". Set bindingKeys or enable bindHandledTypes.`,
    );
  }
  if (
    endpoint.retry.exchange.name === endpoint.exchange.name &&
    keys.includes(endpoint.retry.queueBindingKey)
  ) {
    throw new ConfigurationError(
      `Binding key "${endpoint.retry.queueBindingKey}" is used both by the primary queue and the retry queue on exchange "${endpoint.exchange.name}".`,
    );
  }

  return keys;
}

/** Non-fatal configuration smells worth a log line at startup. */
export function describeEndpointWarnings(
  endpoint: ResolvedConsumerEndpoint,
  bindingKeys: string[],
): string[] {
  const warnings: string[] = [];

  if (endpoint.exchange.type === 'direct') {
    const wildcards = bindingKeys.filter((key) => /[*#]/.test(key));
    if (wildcards.length) {
      warnings.push(
        `Exchange "${endpoint.exchange.name}" is direct, so "${wildcards.join(
          '", "',
        )}" is matched literally and not as a pattern.`,
      );
    }
  }

  const { delayedRetries, backoff, queueMessageTtl } = endpoint.retry;
  if (delayedRetries > 0 && backoff(delayedRetries) > queueMessageTtl) {
    warnings.push(
      `The last delayed retry asks for ${backoff(
        delayedRetries,
      )}ms but retry queue "${endpoint.retry.queue}" expires messages after ${queueMessageTtl}ms; delays are capped at the queue TTL.`,
    );
  }

  return warnings;
}

/**
 * Primary queue, retry queue and error routing of a consumer endpoint:
 *
 *   primary exchange --bindingKeys------> primary queue
 *   retry exchange   --returnBindingKey-> primary queue
 *   retry exchange   --queueBindingKey--> retry queue (TTL, dead-letters with returnBindingKey)
 *   error exchange   --routingKey-------> error queue (only when error.queue is set)
 */
export function buildConsumerTopology(
  endpoint: ResolvedConsumerEndpoint,
  bindingKeys: string[],
): Topology {
  const { retry, error } = endpoint;

  const queues: QueueDefinition[] = [
    { name: endpoint.queue, type: endpoint.queueType },
    {
      name: retry.queue,
      type: endpoint.queueType,
      deadLetterExchange: retry.exchange.name,
      deadLetterRoutingKey: retry.returnBindingKey,
      messageTtl: retry.queueMessageTtl,
    },
  ];

  const bindings = [
    ...bindingKeys.map((routingKey) => ({
      queue: endpoint.queue,
      exchange: endpoint.exchange.name,
      routingKey,
    })),
    {
      queue: endpoint.queue,
      exchange: retry.exchange.name,
      routingKey: retry.returnBindingKey,
    },
    {
      queue: retry.queue,
      exchange: retry.exchange.name,
      routingKey: retry.queueBindingKey,
    },
  ];

  if (error.queue) {
    queues.push({ name: error.queue, type: endpoint.queueType });
    bindings.push({
      queue: error.queue,
      exchange: error.exchange.name,
      routingKey: error.routingKey,
    });
  }

  return {
    exchanges: uniqueExchanges([retry.exchange, endpoint.exchange, error.exchange]),
    queues,
    bindings,
  };
}

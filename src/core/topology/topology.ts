import { ConfigurationError } from '../errors';

export type ExchangeType = 'topic' | 'direct' | 'fanout';

export const EXCHANGE_TYPES: readonly ExchangeType[] = [
  'topic',
  'direct',
  'fanout',
];

export interface ExchangeDefinition {
  name: string;
  type: ExchangeType;
  /** Default: true. */
  durable?: boolean;
}

export type QueueType = 'classic' | 'quorum';

export interface QueueDefinition {
  name: string;
  /** Default: true. */
  durable?: boolean;
  /** Replicated `quorum` queue or `classic`. Left to the broker default when omitted. */
  type?: QueueType;
  deadLetterExchange?: string;
  deadLetterRoutingKey?: string;
  messageTtl?: number;
  arguments?: Record<string, unknown>;
}

export interface BindingDefinition {
  queue: string;
  exchange: string;
  routingKey: string;
}

export interface Topology {
  exchanges: ExchangeDefinition[];
  queues: QueueDefinition[];
  bindings: BindingDefinition[];
}

export function assertExchangeDefinition(
  exchange: ExchangeDefinition | undefined,
  label: string,
): asserts exchange is ExchangeDefinition {
  if (!exchange?.name) {
    throw new ConfigurationError(`${label} exchange name is required.`);
  }
  if (!EXCHANGE_TYPES.includes(exchange.type)) {
    throw new ConfigurationError(
      `${label} exchange "${exchange.name}" has unsupported type "${String(
        exchange.type,
      )}". Supported types: ${EXCHANGE_TYPES.join(', ')}.`,
    );
  }
}

/** Merges exchange definitions by name; the same name with a different type is a configuration error. */
export function uniqueExchanges(
  exchanges: ExchangeDefinition[],
): ExchangeDefinition[] {
  const byName = new Map<string, ExchangeDefinition>();

  for (const exchange of exchanges) {
    const known = byName.get(exchange.name);
    if (!known) {
      byName.set(exchange.name, exchange);
      continue;
    }
    if (known.type !== exchange.type) {
      throw new ConfigurationError(
        `Exchange "${exchange.name}" is declared both as "${known.type}" and "${exchange.type}".`,
      );
    }
  }

  return [...byName.values()];
}

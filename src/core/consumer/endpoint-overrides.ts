import { ExchangeDefinition } from '../topology/topology';
import {
  ConsumerEndpointOptions,
  ErrorQueueOptions,
  RetryOptions,
} from './consumer-endpoint';

/**
 * Values that replace parts of a configured endpoint at start time, typically
 * from command line flags, so one build can serve several queues.
 */
export interface EndpointOverrides {
  appName?: string;
  queue?: string;
  exchange?: Partial<ExchangeDefinition>;
  bindingKeys?: string[];
  bindHandledTypes?: boolean;
  prefetch?: number;
  retry?: Partial<Omit<RetryOptions, 'exchange' | 'backoff'>> & {
    exchange?: Partial<ExchangeDefinition>;
  };
  error?: Partial<Omit<ErrorQueueOptions, 'exchange'>> & {
    exchange?: Partial<ExchangeDefinition>;
  };
}

function defined<T extends object>(value: T | undefined): Partial<T> {
  return Object.fromEntries(
    Object.entries(value ?? {}).filter(([, entry]) => entry !== undefined),
  ) as Partial<T>;
}

/** Returns the endpoint with every override that is set applied on top of it. */
export function applyEndpointOverrides(
  endpoint: ConsumerEndpointOptions,
  overrides: EndpointOverrides = {},
): ConsumerEndpointOptions {
  const { exchange, retry = {}, error = {}, ...rest } = overrides;
  const { exchange: retryExchange, ...retryRest } = retry;
  const { exchange: errorExchange, ...errorRest } = error;

  return {
    ...endpoint,
    ...defined(rest),
    exchange: { ...endpoint.exchange, ...defined(exchange) },
    retry: {
      ...endpoint.retry,
      ...defined(retryRest),
      exchange: { ...endpoint.retry?.exchange, ...defined(retryExchange) },
    },
    error: {
      ...endpoint.error,
      ...defined(errorRest),
      exchange: { ...endpoint.error?.exchange, ...defined(errorExchange) },
    },
  };
}

import { ConfigurationError } from '../errors';

export interface ReconnectOptions {
  /** Reconnect when the connection is lost or cannot be established. Default: true. */
  enabled?: boolean;
  /** Consecutive failed attempts tolerated before giving up. Use `Infinity` to never give up. Default: 3. */
  maxAttempts?: number;
  /** Delay between attempts, in milliseconds. Default: 5000. */
  delayMs?: number;
}

export interface ConnectionOptions {
  /** Broker URL, for example `amqp://rabbitmq:5672/` or `amqps://host/vhost`. */
  url: string;
  /** When set together with `password`, overrides any credentials in the URL. */
  username?: string;
  password?: string;
  /** Heartbeat interval in seconds. Default: 30. */
  heartbeat?: number;
  /** Shown in the RabbitMQ management UI next to the connection. */
  connectionName?: string;
  /** Extra socket options passed to amqplib (TLS certificates, timeout, noDelay...). */
  socketOptions?: Record<string, unknown>;
  reconnect?: ReconnectOptions;
}

export interface ResolvedConnectionOptions extends ConnectionOptions {
  heartbeat: number;
  reconnect: Required<ReconnectOptions>;
}

export function resolveConnectionOptions(
  options: ConnectionOptions,
): ResolvedConnectionOptions {
  if (!options?.url) {
    throw new ConfigurationError('RabbitMQ connection url is required.');
  }
  if (!/^amqps?:\/\//.test(options.url) || !URL.canParse(options.url)) {
    throw new ConfigurationError(
      'RabbitMQ connection url must be a valid amqp:// or amqps:// URL.',
    );
  }

  return {
    ...options,
    heartbeat: options.heartbeat ?? 30,
    reconnect: {
      enabled: options.reconnect?.enabled ?? true,
      maxAttempts: options.reconnect?.maxAttempts ?? 3,
      delayMs: options.reconnect?.delayMs ?? 5000,
    },
  };
}

/**
 * Reads the connection settings from the conventional environment variables:
 * `RABBITMQ_DSN`, `RABBITMQ_USERNAME`, `RABBITMQ_PASSWORD`, `RABBITMQ_HEARTBEAT_INTERVAL`.
 */
export function connectionOptionsFromEnv(
  env: Record<string, string | undefined> = process.env,
): ConnectionOptions {
  const heartbeat = Number.parseInt(env.RABBITMQ_HEARTBEAT_INTERVAL ?? '', 10);

  return {
    url: env.RABBITMQ_DSN ?? '',
    username: env.RABBITMQ_USERNAME,
    password: env.RABBITMQ_PASSWORD,
    heartbeat: Number.isNaN(heartbeat) ? undefined : heartbeat,
  };
}

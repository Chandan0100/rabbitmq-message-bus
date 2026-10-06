import { randomUUID } from 'node:crypto';
import { connect } from 'amqplib';
import type { ChannelModel, ConfirmChannel, GetMessage, Options } from 'amqplib';
import {
  ConnectionManager,
  ConsumerEndpointOptions,
  fixedBackoff,
  Logger,
} from '../../src';

export const BROKER_URL =
  process.env.TEST_RABBITMQ_URL ?? 'amqp://guest:guest@localhost:5679';
export const MANAGEMENT_URL =
  process.env.TEST_RABBITMQ_MANAGEMENT_URL ?? 'http://localhost:15679';
const MANAGEMENT_AUTH = `Basic ${Buffer.from('guest:guest').toString('base64')}`;

export const sleep = (ms: number) =>
  new Promise((resolve) => setTimeout(resolve, ms));

export async function waitFor(
  predicate: () => boolean | Promise<boolean>,
  timeoutMs = 8000,
  intervalMs = 25,
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (!(await predicate())) {
    if (Date.now() > deadline) {
      throw new Error(`Condition not met within ${timeoutMs}ms`);
    }
    await sleep(intervalMs);
  }
}

/** Names unique to one test, so tests can share a broker and run in parallel. */
export interface TestNames {
  exchange: string;
  retryExchange: string;
  queue: string;
  retryQueue: string;
  errorQueue: string;
  errorRoutingKey: string;
}

export function createNames(): TestNames {
  const id = randomUUID().slice(0, 8);
  return {
    exchange: `bus-test.${id}.events`,
    retryExchange: `bus-test.${id}.direct`,
    queue: `bus-test.${id}.queue`,
    retryQueue: `bus-test.${id}.queue.events-retry`,
    errorQueue: `bus-test.${id}.error`,
    errorRoutingKey: `bus-test.${id}.dead-letter`,
  };
}

/** A topic endpoint with short delays; tests override what they exercise. */
export function endpointFor(
  names: TestNames,
  overrides: Partial<ConsumerEndpointOptions> = {},
): ConsumerEndpointOptions {
  return {
    appName: 'bus-test',
    queue: names.queue,
    exchange: { name: names.exchange, type: 'topic' },
    bindingKeys: 'bus-test.#',
    ...overrides,
    retry: {
      exchange: { name: names.retryExchange, type: 'direct' },
      immediateRetries: 0,
      delayedRetries: 2,
      queueMessageTtl: 2000,
      backoff: fixedBackoff(150),
      ...overrides.retry,
    },
    error: overrides.error ?? {
      exchange: { name: names.retryExchange, type: 'direct' },
      routingKey: names.errorRoutingKey,
      queue: names.errorQueue,
    },
  };
}

export function createConnection(
  logger: Logger,
  connectionName?: string,
): ConnectionManager {
  return new ConnectionManager(
    {
      url: BROKER_URL,
      connectionName,
      reconnect: { maxAttempts: 20, delayMs: 200 },
    },
    logger,
  );
}

/** A plain amqplib connection used to set the stage and inspect the outcome. */
export class BrokerAdmin {
  private constructor(
    private readonly model: ChannelModel,
    private readonly channel: ConfirmChannel,
  ) {}

  static async connect(): Promise<BrokerAdmin> {
    const model = await connect(BROKER_URL);
    return new BrokerAdmin(model, await model.createConfirmChannel());
  }

  async publish(
    exchange: string,
    routingKey: string,
    body: unknown,
    properties: Options.Publish = {},
  ): Promise<void> {
    const content = Buffer.isBuffer(body)
      ? body
      : Buffer.from(typeof body === 'string' ? body : JSON.stringify(body));
    this.channel.publish(exchange, routingKey, content, properties);
    await this.channel.waitForConfirms();
  }

  async waitForMessage(queue: string, timeoutMs = 8000): Promise<GetMessage> {
    let message: GetMessage | false = false;
    await waitFor(async () => {
      message = await this.channel.get(queue, { noAck: true });
      return message !== false;
    }, timeoutMs);
    return message as unknown as GetMessage;
  }

  async messageCount(queue: string): Promise<number> {
    return (await this.channel.checkQueue(queue)).messageCount;
  }

  async deleteQueue(queue: string): Promise<void> {
    await this.channel.deleteQueue(queue);
  }

  /**
   * Declares the topology exactly as the original padmasana-service does
   * (RabbitmqConfigurerService.consumerTopologyConfigurer), call for call.
   */
  async declareLikeOriginalService(config: {
    primaryQueue: string;
    primaryQueueBindingKey: string;
    primaryQueueExchange: string;
    primaryQueueRetryBindingKey: string;
    retryQueue: string;
    retryQueueBindingKey: string;
    retryQueueExchange: string;
    retryQueueMessageTtl: number;
  }): Promise<void> {
    await this.channel.assertExchange(config.retryQueueExchange, 'direct', { durable: true });
    await this.channel.assertExchange(config.primaryQueueExchange, 'topic', { durable: true });

    await this.channel.assertQueue(config.primaryQueue, { durable: true });
    await this.channel.bindQueue(config.primaryQueue, config.primaryQueueExchange, config.primaryQueueBindingKey);
    await this.channel.bindQueue(config.primaryQueue, config.retryQueueExchange, config.primaryQueueRetryBindingKey);

    await this.channel.assertQueue(config.retryQueue, {
      durable: true,
      deadLetterExchange: config.retryQueueExchange,
      deadLetterRoutingKey: config.primaryQueueRetryBindingKey,
      messageTtl: config.retryQueueMessageTtl,
    });
    await this.channel.bindQueue(config.retryQueue, config.retryQueueExchange, config.retryQueueBindingKey);
  }

  /** Declares a queue bound to an exchange that already exists. */
  async bindQueue(
    queue: string,
    exchange: string,
    routingKey: string,
  ): Promise<void> {
    await this.channel.assertQueue(queue, { durable: true });
    await this.channel.bindQueue(queue, exchange, routingKey);
  }

  async cleanup(names: TestNames): Promise<void> {
    for (const queue of [names.queue, names.retryQueue, names.errorQueue]) {
      await this.channel.deleteQueue(queue).catch(() => undefined);
    }
    for (const exchange of [names.exchange, names.retryExchange]) {
      await this.channel.deleteExchange(exchange).catch(() => undefined);
    }
  }

  async close(): Promise<void> {
    await this.model.close().catch(() => undefined);
  }
}

/** Kills a client connection from the broker side, as a network failure would. */
export async function forceCloseConnection(connectionName: string): Promise<void> {
  let brokerName: string | undefined;

  // The management API lists a connection a few seconds after it opens.
  await waitFor(
    async () => {
      const response = await fetch(`${MANAGEMENT_URL}/api/connections`, {
        headers: { authorization: MANAGEMENT_AUTH },
      });
      const connections = (await response.json()) as {
        name: string;
        client_properties?: { connection_name?: string };
      }[];
      brokerName = connections.find(
        (connection) =>
          connection.client_properties?.connection_name === connectionName,
      )?.name;
      return brokerName !== undefined;
    },
    20000,
    250,
  );

  await fetch(
    `${MANAGEMENT_URL}/api/connections/${encodeURIComponent(brokerName!)}`,
    { method: 'DELETE', headers: { authorization: MANAGEMENT_AUTH } },
  );
}

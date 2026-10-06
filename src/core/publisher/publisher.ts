import type { ConfirmChannel } from 'amqplib';
import { ConnectionManager } from '../connection/connection-manager';
import { ConsoleLogger, Logger } from '../logger';
import {
  JsonMessageSerializer,
  MessageSerializer,
} from '../serialization/message-serializer';
import {
  assertExchangeDefinition,
  ExchangeDefinition,
  uniqueExchanges,
} from '../topology/topology';
import { TopologyDeclarator } from '../topology/topology-declarator';
import {
  DEFAULT_CONFIRM_TIMEOUT_MS,
  PublishProperties,
  publishWithConfirm,
} from './confirm';

export interface PublisherOptions {
  /** Exchanges this publisher sends to. They are declared before the first publish. */
  exchanges?: ExchangeDefinition[];
  /** How long to wait for the broker to confirm a message, in milliseconds. Default: 10000. */
  confirmTimeoutMs?: number;
  serializer?: MessageSerializer;
}

export interface OutgoingMessage {
  exchange: string;
  routingKey: string;
  /** Serialized with the configured serializer, unless it already is a Buffer. */
  body: unknown;
  properties?: PublishProperties;
}

/** Publishes persistent messages and waits for the broker to confirm each one. */
export class Publisher {
  private readonly exchanges: ExchangeDefinition[];
  private readonly confirmTimeoutMs: number;
  private readonly serializer: MessageSerializer;
  private readonly declarator = new TopologyDeclarator();
  private channel?: ConfirmChannel;
  private opening?: Promise<ConfirmChannel>;

  constructor(
    private readonly connection: ConnectionManager,
    options: PublisherOptions = {},
    private readonly logger: Logger = new ConsoleLogger(),
  ) {
    for (const exchange of options.exchanges ?? []) {
      assertExchangeDefinition(exchange, 'Publisher');
    }
    this.exchanges = uniqueExchanges(options.exchanges ?? []);
    this.confirmTimeoutMs =
      options.confirmTimeoutMs ?? DEFAULT_CONFIRM_TIMEOUT_MS;
    this.serializer = options.serializer ?? new JsonMessageSerializer();
  }

  /** Resolves once the broker has taken responsibility for the message. */
  async publish(message: OutgoingMessage): Promise<void> {
    const channel = await this.getChannel();
    const content = Buffer.isBuffer(message.body)
      ? message.body
      : this.serializer.serialize(message.body);

    await publishWithConfirm(
      channel,
      message.exchange,
      message.routingKey,
      content,
      { persistent: true, ...message.properties },
      this.confirmTimeoutMs,
    );
  }

  /** Closes the channel. The connection stays open. */
  async close(): Promise<void> {
    await this.opening?.catch(() => undefined);

    const channel = this.channel;
    this.channel = undefined;
    if (!channel) return;

    try {
      await channel.close();
      this.logger.info('Channel closed explicitly.');
    } catch {
      // Already closed.
    }
  }

  private getChannel(): Promise<ConfirmChannel> {
    if (this.channel) return Promise.resolve(this.channel);

    this.opening ??= this.openChannel().finally(() => {
      this.opening = undefined;
    });
    return this.opening;
  }

  private async openChannel(): Promise<ConfirmChannel> {
    await this.connection.connect();
    const channel = await this.connection.createConfirmChannel();

    channel.on('error', (error: Error) => {
      this.logger.error(`RabbitMQ publisher channel error: ${error.message}`);
    });
    // A closed channel is replaced on the next publish.
    channel.on('close', () => {
      if (this.channel === channel) this.channel = undefined;
    });

    await this.declarator.declare(channel, {
      exchanges: this.exchanges,
      queues: [],
      bindings: [],
    });

    this.channel = channel;
    return channel;
  }
}

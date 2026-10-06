import { Injectable, OnApplicationShutdown } from '@nestjs/common';
import {
  ConnectionManager,
  MessageHeaders,
  PublishProperties,
  publishWithConfirm,
  silentLogger,
} from 'rabbitmq-message-bus';
import { MessagingConfig } from '../infrastructure/messaging.config';

type Channel = Awaited<ReturnType<ConnectionManager['createChannel']>>;

export interface QueueStats {
  messages: number;
  consumers: number;
}

export interface PeekedMessage {
  messageId: string | null;
  type: string | null;
  exchange: string;
  routingKey: string;
  redeliveryCount: number;
  expiresInMs: number | null;
  body: string;
  /** Present on messages in the error queue. */
  failure: {
    endpoint: string;
    errors: { message: string; failedAt: string }[];
    replayExchange: string;
    replayRoutingKey: string;
  } | null;
}

const MANAGEMENT_URL =
  process.env.RABBITMQ_MANAGEMENT_URL ?? 'http://localhost:15681';
const MANAGEMENT_AUTH = `Basic ${Buffer.from(
  process.env.RABBITMQ_MANAGEMENT_AUTH ?? 'guest:guest',
).toString('base64')}`;

// Headers the retry flow and the broker added; a replayed message starts clean.
const RECOVERY_HEADERS = [
  MessageHeaders.REDELIVERY_COUNT,
  MessageHeaders.RETRY_ENDPOINT,
  MessageHeaders.EXCEPTION_DETAILS,
  MessageHeaders.ENDPOINT,
];

/**
 * DEMO ONLY. Looks into the broker the way the RabbitMQ management console
 * does: queue depths, the messages waiting in a queue, replaying a failed
 * message. It uses the library's ConnectionManager for its own connection.
 */
@Injectable()
export class BrokerInspector implements OnApplicationShutdown {
  readonly managementUrl = MANAGEMENT_URL;
  private readonly connection: ConnectionManager;
  private statsChannel?: Channel;

  constructor(config: MessagingConfig) {
    this.connection = new ConnectionManager(
      {
        ...config.connection('demo-dashboard'),
        reconnect: { maxAttempts: Infinity, delayMs: 2000 },
      },
      silentLogger,
    );
    void this.connection.connect().catch(() => undefined);
  }

  isReachable(): boolean {
    return this.connection.isConnected();
  }

  /** Depth and consumer count per queue; `null` for a queue that does not exist yet. */
  async queueStats(queues: string[]): Promise<Record<string, QueueStats | null>> {
    const stats: Record<string, QueueStats | null> = {};

    for (const queue of queues) {
      stats[queue] = null;
      if (!this.isReachable()) continue;

      try {
        this.statsChannel ??= await this.openChannel();
        const { messageCount, consumerCount } =
          await this.statsChannel.checkQueue(queue);
        stats[queue] = { messages: messageCount, consumers: consumerCount };
      } catch {
        // Checking a missing queue closes the channel; the next check opens a new one.
        this.statsChannel = undefined;
      }
    }

    return stats;
  }

  /**
   * Reads up to `limit` messages without consuming them: they are fetched
   * unacknowledged and the broker puts them back when the channel closes.
   */
  async peek(queue: string, limit = 20): Promise<PeekedMessage[]> {
    if (!this.isReachable()) return [];

    const messages: PeekedMessage[] = [];
    const channel = await this.openChannel();

    try {
      for (let i = 0; i < limit; i++) {
        const message = await channel.get(queue, { noAck: false });
        if (!message) break;

        const headers = (message.properties.headers ?? {}) as Record<string, any>;
        const endpoint = headers[MessageHeaders.ENDPOINT];
        messages.push({
          messageId: message.properties.messageId ?? null,
          type: message.properties.type ?? headers.type ?? null,
          exchange: message.fields.exchange,
          routingKey: message.fields.routingKey,
          redeliveryCount: Number(headers[MessageHeaders.REDELIVERY_COUNT] ?? 0),
          expiresInMs: message.properties.expiration
            ? Number(message.properties.expiration)
            : null,
          body: message.content.toString().slice(0, 2000),
          failure: endpoint
            ? {
                endpoint: endpoint.name,
                errors: (headers[MessageHeaders.EXCEPTION_DETAILS] ?? []).map(
                  (detail: Record<string, string>) => ({
                    message: detail.exception_type,
                    failedAt: detail.failed_at,
                  }),
                ),
                replayExchange: endpoint.delivery_metadata.exchange,
                replayRoutingKey: endpoint.delivery_metadata.routing_key,
              }
            : null,
        });
      }
    } catch {
      // The queue does not exist yet.
    } finally {
      await channel.close().catch(() => undefined);
    }

    return messages;
  }

  /**
   * Sends failed messages back to the queue they came from, using the
   * `endpoint` header the library attached when it moved them to the error
   * queue. Replays one message, or all of them when no id is given.
   */
  async replay(errorQueue: string, messageId?: string): Promise<number> {
    const channel = await this.connection.createConfirmChannel();
    channel.on('error', () => undefined);
    let replayed = 0;

    try {
      for (let i = 0; i < 500; i++) {
        const message = await channel.get(errorQueue, { noAck: false });
        if (!message) break;

        const headers = { ...(message.properties.headers ?? {}) } as Record<string, any>;
        const target = headers[MessageHeaders.ENDPOINT]?.delivery_metadata;
        const wanted = !messageId || message.properties.messageId === messageId;
        // Left unacknowledged, so it returns to the error queue when the channel closes.
        if (!wanted || !target) continue;

        for (const header of Object.keys(headers)) {
          if (RECOVERY_HEADERS.includes(header as never) || header.startsWith('x-')) {
            delete headers[header];
          }
        }
        const { userId, clusterId, expiration, ...properties } = message.properties;

        await publishWithConfirm(
          channel,
          target.exchange,
          target.routing_key,
          message.content,
          { ...properties, headers, persistent: true } as PublishProperties,
        );
        channel.ack(message);
        replayed += 1;
      }
    } finally {
      await channel.close().catch(() => undefined);
    }

    return replayed;
  }

  /** Publishes straight to an exchange, bypassing the outbox. Used to send odd messages on purpose. */
  async publish(
    exchange: string,
    routingKey: string,
    content: string,
    properties: PublishProperties,
  ): Promise<void> {
    const channel = await this.connection.createConfirmChannel();
    channel.on('error', () => undefined);

    try {
      await publishWithConfirm(channel, exchange, routingKey, Buffer.from(content), {
        persistent: true,
        ...properties,
      });
    } finally {
      await channel.close().catch(() => undefined);
    }
  }

  async purge(queues: string[]): Promise<void> {
    for (const queue of queues) {
      const channel = await this.openChannel();
      await channel.purgeQueue(queue).catch(() => undefined);
      await channel.close().catch(() => undefined);
    }
  }

  async deleteQueue(queue: string): Promise<void> {
    const channel = await this.openChannel();
    await channel.deleteQueue(queue).catch(() => undefined);
    await channel.close().catch(() => undefined);
  }

  /**
   * Closes the consumers' connections from the broker side, like a network
   * failure would. Needs the management plugin, which the demo broker has.
   */
  async killConsumerConnections(): Promise<number> {
    const response = await fetch(`${MANAGEMENT_URL}/api/connections`, {
      headers: { authorization: MANAGEMENT_AUTH },
    });
    if (!response.ok) {
      throw new Error(`Management API answered ${response.status}`);
    }

    const connections = (await response.json()) as {
      name: string;
      client_properties?: { connection_name?: string };
    }[];
    const consumers = connections.filter((connection) =>
      connection.client_properties?.connection_name?.startsWith('demo-consumer'),
    );

    for (const connection of consumers) {
      await fetch(
        `${MANAGEMENT_URL}/api/connections/${encodeURIComponent(connection.name)}`,
        { method: 'DELETE', headers: { authorization: MANAGEMENT_AUTH } },
      );
    }
    return consumers.length;
  }

  async onApplicationShutdown(): Promise<void> {
    await this.connection.close();
  }

  private async openChannel(): Promise<Channel> {
    const channel = await this.connection.createChannel();
    // A failed check or purge closes the channel with an error event; that is expected here.
    channel.on('error', () => undefined);
    return channel;
  }
}

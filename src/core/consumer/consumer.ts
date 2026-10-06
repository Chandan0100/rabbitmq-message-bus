import { EventEmitter } from 'node:events';
import type { ConfirmChannel, ConsumeMessage, Message } from 'amqplib';
import { ConnectionManager } from '../connection/connection-manager';
import { HandlerExecutionError, toError } from '../errors';
import { MessageHeaders } from '../headers';
import { ConsoleLogger, Logger } from '../logger';
import { DEFAULT_CONFIRM_TIMEOUT_MS } from '../publisher/confirm';
import { RecoverabilityExecutor } from '../recoverability/recoverability-executor';
import { RetryPolicy } from '../recoverability/retry-policy';
import {
  JsonMessageSerializer,
  MessageSerializer,
} from '../serialization/message-serializer';
import { TopologyDeclarator } from '../topology/topology-declarator';
import {
  buildConsumerTopology,
  ConsumerEndpointOptions,
  describeEndpointWarnings,
  resolveBindingKeys,
  resolveConsumerEndpoint,
  ResolvedConsumerEndpoint,
} from './consumer-endpoint';
import { HandlerExecutor } from './handler-executor';
import { HandlerRegistry } from './handler-registry';
import { InboxOptions } from './inbox-store';
import { ContextRunner, IncomingMessage } from './message-handler';

export interface ConsumerOptions {
  endpoint: ConsumerEndpointOptions;
  /** Makes handling idempotent. Without it a redelivered message is handled again. */
  inbox?: InboxOptions;
  /** Opaque partition (database schema, tenant...) forwarded to the inbox store. */
  scope?: string;
  serializer?: MessageSerializer;
  /**
   * Wraps the handling of each message, inbox check included. Use it to open
   * what the handlers expect around them, such as an ORM request context.
   */
  runInContext?: ContextRunner;
  /** Resolve every handler before consuming, so a broken one fails at startup. Default: true. */
  preloadHandlers?: boolean;
  /** How long `stop()` waits for in-flight messages, in milliseconds. Default: 30000. */
  shutdownTimeoutMs?: number;
  /** How long to wait for the broker to confirm a retry or error-queue publish. Default: 10000. */
  confirmTimeoutMs?: number;
}

interface Subscription {
  channel: ConfirmChannel;
  consumerTag: string;
}

// Pause before requeueing a message that could be neither retried nor moved
// to the error queue, so a broken exchange does not become a hot loop.
const REQUEUE_DELAY_MS = 1000;

async function closeQuietly(channel: ConfirmChannel): Promise<void> {
  try {
    await channel.close();
  } catch {
    // Already closed.
  }
}

/**
 * Consumes the primary queue of an endpoint and acknowledges a message only
 * once it is handled, parked for a delayed retry, or moved to the error queue.
 *
 * Events:
 * - `failed` (Error): the subscription could not be restored; the process should exit.
 */
export class Consumer extends EventEmitter {
  private readonly endpoint: ResolvedConsumerEndpoint;
  private readonly executor: HandlerExecutor;
  private readonly retryPolicy: RetryPolicy;
  private readonly recoverability: RecoverabilityExecutor;
  private readonly declarator = new TopologyDeclarator();
  private readonly inFlight = new Set<Promise<void>>();
  private readonly preloadHandlers: boolean;
  private readonly shutdownTimeoutMs: number;
  private readonly runInContext: ContextRunner;

  private running = false;
  private subscription?: Subscription;
  private subscribing?: Promise<void>;
  private resubscribeTimer?: NodeJS.Timeout;
  private setupFailures = 0;

  constructor(
    private readonly connection: ConnectionManager,
    private readonly registry: HandlerRegistry,
    options: ConsumerOptions,
    private readonly logger: Logger = new ConsoleLogger(),
  ) {
    super();
    this.endpoint = resolveConsumerEndpoint(options.endpoint);
    this.preloadHandlers = options.preloadHandlers ?? true;
    this.shutdownTimeoutMs = options.shutdownTimeoutMs ?? 30000;
    this.runInContext = options.runInContext ?? ((work) => work());

    this.executor = new HandlerExecutor(
      registry,
      options.serializer ?? new JsonMessageSerializer(),
      {
        immediateRetries: this.endpoint.retry.immediateRetries,
        inbox: options.inbox,
        scope: options.scope,
      },
      logger,
    );
    this.retryPolicy = new RetryPolicy({
      delayedRetries: this.endpoint.retry.delayedRetries,
      backoff: this.endpoint.retry.backoff,
      maxDelayMs: this.endpoint.retry.queueMessageTtl,
    });
    this.recoverability = new RecoverabilityExecutor(
      this.endpoint,
      logger,
      options.confirmTimeoutMs ?? DEFAULT_CONFIRM_TIMEOUT_MS,
    );
  }

  getEndpoint(): ResolvedConsumerEndpoint {
    return this.endpoint;
  }

  isConsuming(): boolean {
    return this.subscription !== undefined;
  }

  /** Connects if needed and starts consuming. Rejects when the first subscription cannot be set up. */
  async start(): Promise<void> {
    if (this.running) return;
    this.running = true;

    try {
      if (this.preloadHandlers) await this.registry.resolveAll();
      await this.connection.connect();
      await this.subscribe();
    } catch (error) {
      this.running = false;
      throw error;
    }

    this.connection.on('connected', this.onConnected);
  }

  /** Stops receiving, waits for in-flight messages, then closes the channel. The connection stays open. */
  async stop(): Promise<void> {
    if (!this.running) return;
    this.running = false;

    this.connection.off('connected', this.onConnected);
    if (this.resubscribeTimer) clearTimeout(this.resubscribeTimer);
    this.resubscribeTimer = undefined;
    await this.subscribing?.catch(() => undefined);

    const subscription = this.subscription;
    this.subscription = undefined;
    if (!subscription) return;

    try {
      await subscription.channel.cancel(subscription.consumerTag);
    } catch {
      // The channel is already gone; nothing more will be delivered.
    }

    await this.drain();
    await closeQuietly(subscription.channel);
    this.logger.info(`Stopped consuming ${this.endpoint.queue}.`);
  }

  private readonly onConnected = (): void => {
    void this.resubscribe();
  };

  private subscribe(): Promise<void> {
    this.subscribing ??= this.openSubscription().finally(() => {
      this.subscribing = undefined;
    });
    return this.subscribing;
  }

  private async resubscribe(): Promise<void> {
    if (!this.running) return;

    try {
      await this.subscribe();
      this.setupFailures = 0;
    } catch (error) {
      this.handleSetupFailure(toError(error));
    }
  }

  private handleSetupFailure(error: Error): void {
    if (!this.running) return;
    // A lost connection is the connection manager's business; `connected` brings us back here.
    if (!this.connection.isConnected()) return;

    const { maxAttempts, delayMs } = this.connection.getReconnectOptions();
    this.setupFailures += 1;

    if (this.setupFailures > maxAttempts) {
      this.logger.error(
        `Giving up subscribing to ${this.endpoint.queue}: ${error.message}`,
      );
      this.emit('failed', error);
      return;
    }

    this.logger.warn(
      `Could not subscribe to ${this.endpoint.queue}: ${error.message}. Retrying in ${delayMs}ms (attempt ${this.setupFailures}/${maxAttempts}).`,
    );
    this.scheduleResubscribe(delayMs);
  }

  private scheduleResubscribe(delayMs: number): void {
    if (this.resubscribeTimer) return;

    this.resubscribeTimer = setTimeout(() => {
      this.resubscribeTimer = undefined;
      void this.resubscribe();
    }, delayMs);
  }

  private async openSubscription(): Promise<void> {
    if (!this.running || this.subscription) return;

    const { endpoint } = this;
    const bindingKeys = resolveBindingKeys(endpoint, this.registry.getTypes());
    const channel = await this.connection.createConfirmChannel();

    channel.on('error', (error: Error) => {
      this.logger.error(
        `RabbitMQ channel error on ${endpoint.queue}: ${error.message}`,
      );
    });
    channel.on('close', () => this.handleChannelClosed(channel));
    channel.on('return', (returned: Message) => {
      this.logger.error(
        `Message ${returned.properties.messageId} was not routed to any queue by exchange "${returned.fields.exchange}" with routing key "${returned.fields.routingKey}" and has been dropped. Bind a queue to it, or set error.queue.`,
      );
    });

    let consumerTag: string;
    try {
      await channel.prefetch(endpoint.prefetch);
      await this.declarator.declare(
        channel,
        buildConsumerTopology(endpoint, bindingKeys),
      );
      ({ consumerTag } = await channel.consume(endpoint.queue, (message) =>
        this.onMessage(channel, message),
      ));
    } catch (error) {
      await closeQuietly(channel);
      throw error;
    }

    if (!this.running) {
      await closeQuietly(channel);
      return;
    }

    this.subscription = { channel, consumerTag };

    for (const warning of describeEndpointWarnings(endpoint, bindingKeys)) {
      this.logger.warn(warning);
    }
    this.logger.info(
      `Waiting for messages in ${endpoint.queue}... Registered signatures: [${this.registry
        .getTypes()
        .join(', ')}]`,
    );
  }

  private handleChannelClosed(channel: ConfirmChannel): void {
    if (this.subscription?.channel !== channel) return;

    this.logger.warn(`Channel consuming ${this.endpoint.queue} was closed.`);
    this.releaseSubscription(channel);
  }

  /** Forgets a subscription that is no longer delivering and arranges for a new one. */
  private releaseSubscription(channel: ConfirmChannel): void {
    if (this.subscription?.channel !== channel) return;
    this.subscription = undefined;

    if (this.running && this.connection.isConnected()) {
      this.scheduleResubscribe(this.connection.getReconnectOptions().delayMs);
    }
  }

  private onMessage(
    channel: ConfirmChannel,
    message: ConsumeMessage | null,
  ): void {
    // The broker cancelled the consumer, typically because the queue was deleted.
    if (message === null) {
      this.logger.warn(
        `Consumer of ${this.endpoint.queue} was cancelled by the broker; subscribing again.`,
      );
      this.releaseSubscription(channel);
      void closeQuietly(channel);
      return;
    }

    const task: Promise<void> = this.process(channel, message)
      .catch((error) => {
        this.logger.error(
          `Unexpected error while processing message ${message.properties.messageId}:`,
          error,
        );
      })
      .finally(() => {
        this.inFlight.delete(task);
      });
    this.inFlight.add(task);
  }

  private async process(
    channel: ConfirmChannel,
    message: ConsumeMessage,
  ): Promise<void> {
    const headers = (message.properties.headers ?? {}) as Record<string, unknown>;
    const redeliveryCount =
      Number(headers[MessageHeaders.REDELIVERY_COUNT]) || 0;
    const type = message.properties.type || headers[MessageHeaders.TYPE];
    const messageId = message.properties.messageId;

    this.logger.info(
      `Received message: ${String(type)} (${String(messageId)}) | Message redelivery count: ${redeliveryCount}`,
    );

    if (!messageId) {
      this.logger.info('Message ignored: Message does not have a messageId.');
      this.ack(channel, message);
      return;
    }

    if (typeof type !== 'string' || !this.registry.has(type)) {
      this.logger.info(
        'Message ignored: No available handler found or missing message type property.',
      );
      this.ack(channel, message);
      return;
    }

    if (
      redeliveryCount > 0 &&
      headers[MessageHeaders.RETRY_ENDPOINT] !== this.endpoint.appName
    ) {
      this.logger.info(
        'Message ignored: Message is not intended for this service.',
      );
      this.ack(channel, message);
      return;
    }

    const incoming: IncomingMessage = {
      messageId: String(messageId),
      type,
      content: message.content,
      headers,
    };

    try {
      await this.runInContext(() => this.executor.execute(incoming));
    } catch (error) {
      await this.recover(channel, message, error, redeliveryCount);
      return;
    }

    this.ack(channel, message);
  }

  private async recover(
    channel: ConfirmChannel,
    message: ConsumeMessage,
    error: unknown,
    redeliveryCount: number,
  ): Promise<void> {
    const failures =
      error instanceof HandlerExecutionError ? error.failures : [toError(error)];
    const decision = this.retryPolicy.decide(redeliveryCount, error);

    try {
      if (decision.action === 'retry') {
        await this.recoverability.scheduleRetry(
          channel,
          message,
          decision.attempt,
          decision.delayMs,
          failures,
        );
      } else {
        await this.recoverability.moveToErrorQueue(channel, message, failures);
      }
    } catch (publishError) {
      this.logger.error(
        `Message ${message.properties.messageId} could not be ${
          decision.action === 'retry'
            ? 'scheduled for a retry'
            : 'moved to the error queue'
        } and will be redelivered: ${toError(publishError).message}`,
      );
      await new Promise((resolve) => setTimeout(resolve, REQUEUE_DELAY_MS));
      this.requeue(channel, message);
      return;
    }

    this.ack(channel, message);
  }

  // Always on the channel that delivered the message: a delivery tag means nothing on another one.
  private ack(channel: ConfirmChannel, message: ConsumeMessage): void {
    try {
      channel.ack(message);
    } catch (error) {
      this.logger.warn(
        `Could not acknowledge message ${message.properties.messageId}; the broker will redeliver it: ${
          toError(error).message
        }`,
      );
    }
  }

  private requeue(channel: ConfirmChannel, message: ConsumeMessage): void {
    try {
      channel.nack(message, false, true);
    } catch {
      // The channel is closed, so the broker has requeued the message already.
    }
  }

  private async drain(): Promise<void> {
    if (this.inFlight.size === 0) return;

    let timer: NodeJS.Timeout | undefined;
    const timedOut = new Promise<boolean>((resolve) => {
      timer = setTimeout(() => resolve(true), this.shutdownTimeoutMs);
    });
    const drained = Promise.allSettled([...this.inFlight]).then(() => false);

    if (await Promise.race([drained, timedOut])) {
      this.logger.warn(
        `${this.inFlight.size} message(s) still in flight after ${this.shutdownTimeoutMs}ms; the broker will redeliver them.`,
      );
    }
    clearTimeout(timer);
  }
}

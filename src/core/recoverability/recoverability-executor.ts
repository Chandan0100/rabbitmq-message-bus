import type { ConfirmChannel, ConsumeMessage } from 'amqplib';
import { ResolvedConsumerEndpoint } from '../consumer/consumer-endpoint';
import { MessageHeaders } from '../headers';
import { Logger } from '../logger';
import {
  DEFAULT_CONFIRM_TIMEOUT_MS,
  PublishProperties,
  publishWithConfirm,
} from '../publisher/confirm';

/**
 * Properties of a consumed message that are safe to publish again.
 * `userId` is dropped because the broker rejects one that differs from the
 * connected user, and `CC`/`BCC` because they would route extra copies.
 */
function republishableProperties(message: ConsumeMessage): {
  properties: PublishProperties;
  headers: Record<string, unknown>;
} {
  const { headers, userId, clusterId, expiration, ...rest } = message.properties;
  const { CC, BCC, ...safeHeaders } = (headers ?? {}) as Record<string, unknown>;

  const properties = Object.fromEntries(
    Object.entries(rest).filter(([, value]) => value !== undefined),
  ) as PublishProperties;

  return { properties, headers: safeHeaders };
}

/** Moves a failed message to the retry queue or to the error queue. */
export class RecoverabilityExecutor {
  constructor(
    private readonly endpoint: ResolvedConsumerEndpoint,
    private readonly logger: Logger,
    private readonly confirmTimeoutMs = DEFAULT_CONFIRM_TIMEOUT_MS,
  ) {}

  /** Parks a copy in the retry queue; the broker returns it to the primary queue after `delayMs`. */
  async scheduleRetry(
    channel: ConfirmChannel,
    message: ConsumeMessage,
    attempt: number,
    delayMs: number,
    errors: Error[],
  ): Promise<void> {
    const { retry, appName } = this.endpoint;
    const { properties, headers } = republishableProperties(message);

    this.logger.warn(
      `RecoverabilityExecutor Delayed Retries will reschedule message ${
        message.properties.messageId
      } after a delay of ${delayMs / 1000} seconds because of exception:`,
      errors,
    );

    await publishWithConfirm(
      channel,
      retry.exchange.name,
      retry.queueBindingKey,
      message.content,
      {
        ...properties,
        headers: {
          ...headers,
          [MessageHeaders.REDELIVERY_COUNT]: attempt,
          [MessageHeaders.RETRY_ENDPOINT]: appName,
        },
        expiration: String(delayMs),
        persistent: true,
      },
      this.confirmTimeoutMs,
    );
  }

  /** Publishes the message to the error exchange with the failure details and where to replay it. */
  async moveToErrorQueue(
    channel: ConfirmChannel,
    message: ConsumeMessage,
    errors: Error[],
  ): Promise<void> {
    const { retry, error, queue } = this.endpoint;
    const { properties, headers } = republishableProperties(message);

    this.logger.error(
      `RecoverabilityExecutor Moving message ${message.properties.messageId} to error queue because processing failed due to an exception:`,
      errors,
    );

    await publishWithConfirm(
      channel,
      error.exchange.name,
      error.routingKey,
      message.content,
      {
        ...properties,
        headers: {
          ...headers,
          [MessageHeaders.EXCEPTION_DETAILS]: errors.map((failure) => ({
            exception_type: failure.message,
            stack_trace: failure.stack,
            failed_at: new Date().toISOString(),
          })),
          [MessageHeaders.ENDPOINT]: {
            name: queue,
            delivery_metadata: {
              message_type:
                message.properties.type ?? headers[MessageHeaders.TYPE],
              exchange: retry.exchange.name,
              routing_key: retry.returnBindingKey,
            },
          },
        },
        persistent: true,
        // Lets the consumer report an error exchange that has no queue bound.
        mandatory: true,
      },
      this.confirmTimeoutMs,
    );
  }
}

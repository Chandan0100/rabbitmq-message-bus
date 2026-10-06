import type { ConfirmChannel, Options } from 'amqplib';
import { PublishError, toError } from '../errors';

export type PublishProperties = Options.Publish;

export const DEFAULT_CONFIRM_TIMEOUT_MS = 10000;

/** Publishes and resolves only once the broker has confirmed the message. */
export function publishWithConfirm(
  channel: ConfirmChannel,
  exchange: string,
  routingKey: string,
  content: Buffer,
  properties: PublishProperties,
  timeoutMs = DEFAULT_CONFIRM_TIMEOUT_MS,
): Promise<void> {
  return new Promise<void>((resolve, reject) => {
    const target = `exchange "${exchange}" with routing key "${routingKey}"`;
    let settled = false;

    const settle = (error?: Error) => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      if (error) reject(error);
      else resolve();
    };

    const timer = setTimeout(
      () =>
        settle(
          new PublishError(
            `Broker did not confirm the publish to ${target} within ${timeoutMs}ms.`,
          ),
        ),
      timeoutMs,
    );

    try {
      channel.publish(exchange, routingKey, content, properties, (error) => {
        settle(
          error
            ? new PublishError(
                `Broker rejected the publish to ${target}: ${toError(error).message}`,
                { cause: error },
              )
            : undefined,
        );
      });
    } catch (error) {
      settle(
        new PublishError(
          `Could not publish to ${target}: ${toError(error).message}`,
          { cause: error },
        ),
      );
    }
  });
}

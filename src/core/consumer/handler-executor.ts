import {
  HandlerExecutionError,
  toError,
  UnrecoverableMessageError,
} from '../errors';
import { Logger } from '../logger';
import { MessageSerializer } from '../serialization/message-serializer';
import { HandlerRegistry } from './handler-registry';
import { InboxOptions } from './inbox-store';
import {
  EventPayload,
  IncomingMessage,
  MessageHandler,
} from './message-handler';

export interface HandlerExecutorOptions {
  /** Extra in-process attempts after the first failure. */
  immediateRetries: number;
  inbox?: InboxOptions;
  scope?: string;
}

/**
 * Runs every handler registered for a message. Each handler is skipped when
 * the inbox says it already processed the message, and retried in-process
 * before the message is reported as failed.
 */
export class HandlerExecutor {
  constructor(
    private readonly registry: HandlerRegistry,
    private readonly serializer: MessageSerializer,
    private readonly options: HandlerExecutorOptions,
    private readonly logger: Logger,
  ) {}

  /** Throws `HandlerExecutionError` when at least one handler failed. */
  async execute(message: IncomingMessage): Promise<void> {
    const handlers = await this.registry.resolve(message.type);

    const results = await Promise.allSettled(
      handlers.map((handler) => this.run(handler, message)),
    );

    const failures = results
      .filter((result) => result.status === 'rejected')
      .map((result) => toError((result as PromiseRejectedResult).reason));

    if (failures.length) {
      throw new HandlerExecutionError(message.messageId, failures);
    }
  }

  private async run(
    handler: MessageHandler,
    message: IncomingMessage,
  ): Promise<void> {
    const { inbox, scope } = this.options;
    const { messageId } = message;
    const handlerName = handler.getHandlerName();

    if (await inbox?.store.isHandled(messageId, handlerName, scope)) {
      this.logger.info(
        `Message with id ${messageId} already handled with ${handlerName}. Duplicate message ignored.`,
      );
      return;
    }

    const payload: EventPayload = {
      messageId,
      body: this.serializer.deserialize(message.content),
      type: message.type,
      headers: message.headers,
    };

    await this.handleWithImmediateRetries(handler, handlerName, payload);

    if (inbox && (inbox.record ?? 'pipeline') === 'pipeline') {
      await inbox.store.markHandled(messageId, handlerName, scope);
    }
  }

  private async handleWithImmediateRetries(
    handler: MessageHandler,
    handlerName: string,
    payload: EventPayload,
  ): Promise<void> {
    const attempts = this.options.immediateRetries + 1;
    let lastError: Error = new Error('Processing failed');

    for (let attempt = 1; attempt <= attempts; attempt++) {
      try {
        this.logger.info(
          `Handling message with messageId: ${payload.messageId} and handler ${handlerName}`,
        );
        await handler.handle(payload);
        this.logger.info(`Message ${payload.messageId} handled successfully.`);
        return;
      } catch (error) {
        lastError = toError(error);
        if (lastError instanceof UnrecoverableMessageError) break;
        this.logger.debug(
          `Handler ${handlerName} failed on attempt ${attempt}/${attempts} for message ${payload.messageId}: ${lastError.message}`,
        );
      }
    }

    throw lastError;
  }
}

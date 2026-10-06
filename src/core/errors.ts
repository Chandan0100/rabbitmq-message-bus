export class MessageBusError extends Error {
  constructor(message: string, options?: { cause?: unknown }) {
    super(message, options);
    this.name = new.target.name;
  }
}

/** The configuration is invalid; retrying will not help. */
export class ConfigurationError extends MessageBusError {}

/** The broker could not be reached within the reconnect policy. */
export class ConnectionFailedError extends MessageBusError {}

/** The broker did not confirm a published message. */
export class PublishError extends MessageBusError {}

/**
 * Thrown by a handler to signal that retrying is pointless. The message skips
 * the remaining immediate and delayed retries and goes to the error queue.
 */
export class UnrecoverableMessageError extends MessageBusError {}

/** The message content could not be deserialized. Never retried. */
export class MessageParseError extends UnrecoverableMessageError {}

/** One or more handlers of a message failed after their immediate retries. */
export class HandlerExecutionError extends MessageBusError {
  constructor(
    readonly messageId: string,
    readonly failures: Error[],
  ) {
    super(
      `${failures.length} handler(s) failed for message ${messageId}: ${failures
        .map((failure) => failure.message)
        .join('; ')}`,
    );
  }

  get unrecoverable(): boolean {
    return this.failures.every(
      (failure) => failure instanceof UnrecoverableMessageError,
    );
  }
}

export function toError(value: unknown): Error {
  if (value instanceof Error) return value;
  return new Error(typeof value === 'string' ? value : JSON.stringify(value));
}

export interface EventPayload<T = any> {
  messageId: string;
  body: T;
  /** Message type the handler was registered for. */
  type?: string;
  headers?: Record<string, unknown>;
}

/**
 * Processes one message type. `getHandlerName()` identifies the handler in the
 * inbox, so a message is handled at most once per handler.
 */
export interface MessageHandler<T = any> {
  getHandlerName(): string;
  handle(message: EventPayload<T>): Promise<unknown> | unknown;
}

/** What the consumer hands to the handlers, independent of the transport library. */
export interface IncomingMessage {
  messageId: string;
  type: string;
  content: Buffer;
  headers: Record<string, unknown>;
}

/**
 * Runs a unit of work inside an ambient context, for example an ORM request
 * context or a tracing span. Must return what `work` returns.
 */
export type ContextRunner = <T>(work: () => Promise<T>) => Promise<T>;

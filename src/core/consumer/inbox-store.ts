/**
 * Remembers which handler already processed which message, so redelivered
 * messages are not handled twice.
 *
 * `scope` is an opaque partition (database schema, tenant...) forwarded from
 * the worker; stores that are bound to a single partition can ignore it.
 */
export interface InboxStore {
  isHandled(
    messageId: string,
    handlerName: string,
    scope?: string,
  ): Promise<boolean>;

  markHandled(
    messageId: string,
    handlerName: string,
    scope?: string,
  ): Promise<void>;
}

export interface InboxOptions {
  store: InboxStore;
  /**
   * Who writes the inbox entry once a handler succeeded:
   * - `pipeline` (default): the consumer does, right after the handler returns.
   * - `handler`: the handler does, usually in the same database transaction as
   *   its own changes, which makes handling and recording atomic.
   */
  record?: 'pipeline' | 'handler';
}

/** Non-durable store for tests and local experiments. */
export class InMemoryInboxStore implements InboxStore {
  private readonly handled = new Set<string>();

  async isHandled(
    messageId: string,
    handlerName: string,
    scope?: string,
  ): Promise<boolean> {
    return this.handled.has(this.key(messageId, handlerName, scope));
  }

  async markHandled(
    messageId: string,
    handlerName: string,
    scope?: string,
  ): Promise<void> {
    this.handled.add(this.key(messageId, handlerName, scope));
  }

  private key(messageId: string, handlerName: string, scope = ''): string {
    return `${scope}|${handlerName}|${messageId}`;
  }
}

import { ConfigurationError } from '../errors';
import { MessageHandler } from './message-handler';

export type HandlerFactory = () => MessageHandler | PromiseLike<MessageHandler>;

/** A handler, a promise of one, or a factory called the first time the message type arrives. */
export type HandlerSource =
  | MessageHandler
  | PromiseLike<MessageHandler>
  | HandlerFactory;

export type HandlerMap = Record<string, HandlerSource | HandlerSource[]>;

function isPromiseLike(value: unknown): value is PromiseLike<unknown> {
  return (
    typeof value === 'object' &&
    value !== null &&
    typeof (value as { then?: unknown }).then === 'function'
  );
}

/** Maps a message type to the handlers that process it. */
export class HandlerRegistry {
  private readonly sources = new Map<string, HandlerSource[]>();
  private readonly resolved = new Map<string, Promise<MessageHandler[]>>();

  constructor(handlers: HandlerMap = {}) {
    for (const [type, sources] of Object.entries(handlers)) {
      this.register(type, ...(Array.isArray(sources) ? sources : [sources]));
    }
  }

  register(type: string, ...sources: HandlerSource[]): this {
    if (!type) {
      throw new ConfigurationError('A message type is required to register a handler.');
    }

    for (const source of sources) {
      // A promise created up front may reject before the first message needs it.
      if (isPromiseLike(source)) {
        Promise.resolve(source).catch(() => undefined);
      }
    }

    this.sources.set(type, [...(this.sources.get(type) ?? []), ...sources]);
    this.resolved.delete(type);
    return this;
  }

  has(type: string): boolean {
    return this.sources.has(type);
  }

  getTypes(): string[] {
    return [...this.sources.keys()];
  }

  /** Handlers of a message type. Resolved once and cached; a failed resolution is retried on the next call. */
  resolve(type: string): Promise<MessageHandler[]> {
    let handlers = this.resolved.get(type);

    if (!handlers) {
      handlers = Promise.all(
        (this.sources.get(type) ?? []).map((source) =>
          this.resolveSource(type, source),
        ),
      );
      handlers.catch(() => {
        if (this.resolved.get(type) === handlers) this.resolved.delete(type);
      });
      this.resolved.set(type, handlers);
    }

    return handlers;
  }

  /** Resolves every handler now, so a broken registration fails at startup instead of on the first message. */
  async resolveAll(): Promise<void> {
    await Promise.all(this.getTypes().map((type) => this.resolve(type)));
  }

  private async resolveSource(
    type: string,
    source: HandlerSource,
  ): Promise<MessageHandler> {
    const handler = await (typeof source === 'function' ? source() : source);

    if (
      typeof handler?.handle !== 'function' ||
      typeof handler?.getHandlerName !== 'function'
    ) {
      throw new ConfigurationError(
        `Handler registered for "${type}" must implement handle() and getHandlerName().`,
      );
    }

    return handler;
  }
}

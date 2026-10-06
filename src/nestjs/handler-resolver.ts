import { Injectable, Type } from '@nestjs/common';
import { LazyModuleLoader, ModuleRef } from '@nestjs/core';
import { HandlerRegistry, HandlerSource } from '../core/consumer/handler-registry';
import { MessageHandler } from '../core/consumer/message-handler';

/** A handler living in a module that is loaded on demand. */
export interface LazyHandlerRef {
  module: Type<unknown>;
  handler: Type<MessageHandler>;
}

/**
 * How a handler is found:
 * - `lazyHandler(Module, Handler)`: the module is loaded on demand and the handler taken from it;
 * - a handler class: looked up in the modules already part of the application;
 * - a handler instance: used as is.
 */
export type NestHandlerSource =
  | LazyHandlerRef
  | Type<MessageHandler>
  | MessageHandler;

export type NestHandlerMap = Record<
  string,
  NestHandlerSource | NestHandlerSource[]
>;

export function lazyHandler(
  module: Type<unknown>,
  handler: Type<MessageHandler>,
): LazyHandlerRef {
  return { module, handler };
}

function isLazyHandlerRef(source: NestHandlerSource): source is LazyHandlerRef {
  return (
    typeof source === 'object' &&
    typeof (source as LazyHandlerRef).module === 'function' &&
    typeof (source as LazyHandlerRef).handler === 'function'
  );
}

/** Turns a map of NestJS handler references into a registry the consumer can resolve. */
@Injectable()
export class NestHandlerResolver {
  constructor(
    private readonly lazyModuleLoader: LazyModuleLoader,
    private readonly moduleRef: ModuleRef,
  ) {}

  createRegistry(handlers: NestHandlerMap): HandlerRegistry {
    const registry = new HandlerRegistry();

    for (const [type, sources] of Object.entries(handlers)) {
      registry.register(
        type,
        ...(Array.isArray(sources) ? sources : [sources]).map((source) =>
          this.toHandlerSource(source),
        ),
      );
    }

    return registry;
  }

  private toHandlerSource(source: NestHandlerSource): HandlerSource {
    if (isLazyHandlerRef(source)) {
      return async () => {
        const moduleRef = await this.lazyModuleLoader.load(
          () => source.module,
          { logger: false },
        );
        return moduleRef.get(source.handler);
      };
    }

    if (typeof source === 'function') {
      return () => this.moduleRef.get(source, { strict: false });
    }

    return source;
  }
}

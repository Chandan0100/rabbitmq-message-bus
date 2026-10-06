import {
  DynamicModule,
  INestApplicationContext,
  LogLevel,
  Type,
} from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import { EndpointOverrides } from '../../core/consumer/endpoint-overrides';
import { toError } from '../../core/errors';
import { DispatchResult } from '../../core/publisher/outbox-relay';
import { ConsumerWorker } from '../consumer/consumer-worker.service';
import { OutboxDispatcher } from '../producer/outbox-dispatcher.service';
import { CLI_USAGE, CliUsageError, parseCliArgs } from './parse-args';

export type WorkerModule = Type<unknown> | DynamicModule;

export interface WorkerRunOptions {
  /** Prefetch for a consumer, batch size for a dispatcher. */
  limit?: number;
  /** Opaque partition (database schema, tenant...) forwarded to the inbox/outbox store. */
  scope?: string;
  /** NestJS log levels while the module boots. Default: warnings and errors. */
  nestLogger?: LogLevel[] | false;
}

export interface ConsumerWorkerRunOptions extends WorkerRunOptions {
  endpoint?: EndpointOverrides;
  /** Called when the worker cannot go on, so the caller can end the process. */
  onFailed?: (error: Error) => void;
}

export interface MessageBusCliOptions {
  /** Modules importing `RabbitMqConsumerModule`, by the name given to `--module`. */
  consumers?: Record<string, WorkerModule>;
  /** Modules importing `RabbitMqProducerModule`, by the name given to `--module`. */
  producers?: Record<string, WorkerModule>;
  /**
   * Translates the value of `--schema`/`--scope` into what the stores receive,
   * for example a bounded context name into its database schema. When given,
   * a value that is not listed is rejected.
   */
  scopes?: Record<string, string | undefined>;
  /** Default: `process.argv.slice(2)`. */
  argv?: string[];
  nestLogger?: LogLevel[] | false;
}

const DEFAULT_NEST_LOG_LEVELS: LogLevel[] = ['warn', 'error'];

function createContext(
  module: WorkerModule,
  nestLogger: LogLevel[] | false | undefined,
): Promise<INestApplicationContext> {
  return NestFactory.createApplicationContext(module, {
    logger: nestLogger ?? DEFAULT_NEST_LOG_LEVELS,
    abortOnError: false,
  });
}

/**
 * Boots the module and starts its consumer. Resolves once it is consuming;
 * closing the returned application context stops it gracefully, which also
 * happens on SIGTERM and SIGINT.
 */
export async function startConsumerWorker(
  module: WorkerModule,
  options: ConsumerWorkerRunOptions = {},
): Promise<INestApplicationContext> {
  const app = await createContext(module, options.nestLogger);
  app.enableShutdownHooks();

  try {
    const worker = app.get(ConsumerWorker, { strict: false });
    if (options.onFailed) worker.on('failed', options.onFailed);

    await worker.start({
      scope: options.scope,
      endpoint: {
        ...options.endpoint,
        prefetch: options.limit ?? options.endpoint?.prefetch,
      },
    });
  } catch (error) {
    await app.close();
    throw error;
  }

  return app;
}

/** Boots the module, publishes one batch of its outbox and shuts the module down. */
export async function dispatchOutbox(
  module: WorkerModule,
  options: WorkerRunOptions = {},
): Promise<DispatchResult> {
  const app = await createContext(module, options.nestLogger);

  try {
    return await app
      .get(OutboxDispatcher, { strict: false })
      .dispatch({ limit: options.limit, scope: options.scope });
  } finally {
    await app.close();
  }
}

function resolveModule(
  modules: Record<string, WorkerModule> | undefined,
  name: string | undefined,
): WorkerModule {
  const module = name ? modules?.[name] : undefined;
  if (!module) {
    throw new CliUsageError(
      `Invalid or missing --module. Available modules: ${Object.keys(
        modules ?? {},
      ).join(', ')}`,
    );
  }
  return module;
}

function resolveScope(
  scopes: Record<string, string | undefined> | undefined,
  scope: string | undefined,
): string | undefined {
  if (!scopes || scope === undefined) return scope;

  if (!scopes[scope]) {
    throw new CliUsageError(
      `Invalid schema name "${scope}". Available: ${Object.keys(scopes).join(', ')}`,
    );
  }
  return scopes[scope];
}

/**
 * Entry point of the worker processes:
 *
 *   node dist/message-bus-cli dispatch-messages --module <name> --limit 10
 *   node dist/message-bus-cli handle-messages --module <name> --limit 10
 *
 * Exits with a non-zero code when the broker cannot be reached or the worker
 * cannot go on, so that its supervisor (Kubernetes, PM2...) restarts it.
 */
export async function runMessageBusCli(
  options: MessageBusCliOptions,
): Promise<void> {
  const fail = (error: unknown): never => {
    const { message } = toError(error);
    console.error(`ERROR: ${message}`);
    if (error instanceof CliUsageError) console.error(`\n${CLI_USAGE}`);
    process.exit(1);
  };

  let dispatched = false;

  try {
    const args = parseCliArgs(options.argv ?? process.argv.slice(2));
    const run: WorkerRunOptions = {
      limit: args.limit,
      scope: resolveScope(options.scopes, args.scope),
      nestLogger: options.nestLogger,
    };

    if (args.command === 'dispatch-messages') {
      await dispatchOutbox(resolveModule(options.producers, args.module), run);
      dispatched = true;
    } else {
      await startConsumerWorker(resolveModule(options.consumers, args.module), {
        ...run,
        endpoint: args.endpoint,
        onFailed: fail,
      });
    }
  } catch (error) {
    fail(error);
  }

  // A dispatcher is a one-shot job; other providers may still hold the event loop open.
  if (dispatched) process.exit(0);
}

import { EventEmitter } from 'node:events';
import { connect, credentials } from 'amqplib';
import type { Channel, ChannelModel, ConfirmChannel } from 'amqplib';
import { ConnectionFailedError, MessageBusError, toError } from '../errors';
import { ConsoleLogger, Logger } from '../logger';
import {
  ConnectionOptions,
  ResolvedConnectionOptions,
  resolveConnectionOptions,
} from './connection-options';

export type ConnectionState =
  | 'idle'
  | 'connecting'
  | 'connected'
  | 'reconnecting'
  | 'failed'
  | 'closed';

export interface ReconnectingInfo {
  attempt: number;
  maxAttempts: number;
  delayMs: number;
  error: Error;
}

interface Deferred {
  promise: Promise<void>;
  resolve: () => void;
  reject: (error: Error) => void;
}

function createDeferred(): Deferred {
  let resolve!: () => void;
  let reject!: (error: Error) => void;
  const promise = new Promise<void>((res, rej) => {
    resolve = res;
    reject = rej;
  });
  // Nobody may be awaiting when the connection fails; the 'failed' event reports it.
  promise.catch(() => undefined);
  return { promise, resolve, reject };
}

/**
 * Owns the broker connection and keeps it alive.
 *
 * Events:
 * - `connected`: a connection is ready (emitted again after every reconnect)
 * - `disconnected` (error?): the connection was lost
 * - `reconnecting` (ReconnectingInfo): a new attempt has been scheduled
 * - `failed` (ConnectionFailedError): the reconnect policy is exhausted
 */
export class ConnectionManager extends EventEmitter {
  private readonly options: ResolvedConnectionOptions;
  private model?: ChannelModel;
  private state: ConnectionState = 'idle';
  private attempts = 0;
  private generation = 0;
  private timer?: NodeJS.Timeout;
  private ready?: Deferred;

  constructor(
    options: ConnectionOptions,
    private readonly logger: Logger = new ConsoleLogger(),
  ) {
    super();
    this.options = resolveConnectionOptions(options);
  }

  getState(): ConnectionState {
    return this.state;
  }

  isConnected(): boolean {
    return this.state === 'connected';
  }

  getReconnectOptions() {
    return this.options.reconnect;
  }

  /** Resolves once connected; rejects when the reconnect policy is exhausted. */
  connect(): Promise<void> {
    if (this.state === 'connected') return Promise.resolve();

    this.ready ??= createDeferred();
    const ready = this.ready.promise;

    if (
      this.state === 'idle' ||
      this.state === 'failed' ||
      this.state === 'closed'
    ) {
      this.state = 'connecting';
      this.attempts = 0;
      this.generation += 1;
      void this.open(this.generation);
    }

    return ready;
  }

  async createChannel(): Promise<Channel> {
    return this.requireModel().createChannel();
  }

  async createConfirmChannel(): Promise<ConfirmChannel> {
    return this.requireModel().createConfirmChannel();
  }

  async close(): Promise<void> {
    this.generation += 1;
    this.state = 'closed';
    if (this.timer) clearTimeout(this.timer);
    this.timer = undefined;

    this.ready?.reject(new MessageBusError('RabbitMQ connection was closed.'));
    this.ready = undefined;

    const model = this.model;
    this.model = undefined;
    if (!model) return;

    try {
      await model.close();
      this.logger.info('RabbitMQ connection closed.');
    } catch (error) {
      this.logger.warn(
        `RabbitMQ connection did not close cleanly: ${toError(error).message}`,
      );
    }
  }

  private requireModel(): ChannelModel {
    if (!this.model) {
      throw new MessageBusError(
        'RabbitMQ connection has not been established yet.',
      );
    }
    return this.model;
  }

  private async open(generation: number): Promise<void> {
    let model: ChannelModel;

    try {
      model = await connect(this.buildUrl(), this.buildSocketOptions());
    } catch (error) {
      if (generation !== this.generation) return;
      const cause = toError(error);
      this.logger.error(
        `Failed to establish connection to RabbitMQ: ${cause.message}`,
      );
      this.scheduleReconnect(cause);
      return;
    }

    if (generation !== this.generation) {
      await model.close().catch(() => undefined);
      return;
    }

    model.on('error', (error: Error) => {
      this.logger.error(`Error in RabbitMQ connection: ${error.message}`);
    });
    model.on('close', (error?: Error) => this.handleClose(model, error));
    model.on('blocked', (reason: string) => {
      this.logger.warn(`RabbitMQ connection blocked by the broker: ${reason}`);
    });
    model.on('unblocked', () => {
      this.logger.info('RabbitMQ connection unblocked.');
    });

    this.model = model;
    this.state = 'connected';
    this.attempts = 0;
    this.logger.info('RabbitMQ is connected.');

    const ready = this.ready;
    this.ready = undefined;
    ready?.resolve();
    this.emit('connected');
  }

  private handleClose(model: ChannelModel, error?: Error): void {
    if (this.model !== model) return;
    this.model = undefined;
    if (this.state === 'closed') return;

    this.logger.warn(
      `Disconnected from RabbitMQ${error ? `: ${error.message}` : '.'}`,
    );
    this.emit('disconnected', error);
    this.scheduleReconnect(error ?? new Error('Connection closed.'));
  }

  private scheduleReconnect(error: Error): void {
    const { enabled, maxAttempts, delayMs } = this.options.reconnect;
    this.attempts += 1;

    if (!enabled || this.attempts > maxAttempts) {
      this.fail(error);
      return;
    }

    this.state = 'reconnecting';
    const info: ReconnectingInfo = {
      attempt: this.attempts,
      maxAttempts,
      delayMs,
      error,
    };
    this.logger.info(
      `Reconnecting to RabbitMQ in ${delayMs}ms (attempt ${this.attempts}/${maxAttempts}).`,
    );
    this.emit('reconnecting', info);

    const generation = this.generation;
    this.timer = setTimeout(() => {
      this.timer = undefined;
      void this.open(generation);
    }, delayMs);
  }

  private fail(cause: Error): void {
    this.state = 'failed';
    const error = new ConnectionFailedError(
      `Could not connect to RabbitMQ: ${cause.message}`,
      { cause },
    );
    this.logger.error(
      this.options.reconnect.enabled
        ? 'Maximum reconnect tries reached.'
        : 'RabbitMQ connection lost and reconnect is disabled.',
    );

    const ready = this.ready;
    this.ready = undefined;
    ready?.reject(error);
    this.emit('failed', error);
  }

  // amqplib only honours the heartbeat when it is part of the URL.
  private buildUrl(): string {
    const url = new URL(this.options.url);
    if (!url.searchParams.has('heartbeat')) {
      url.searchParams.set('heartbeat', String(this.options.heartbeat));
    }
    return url.toString();
  }

  private buildSocketOptions(): Record<string, unknown> {
    const { username, password, connectionName, socketOptions } = this.options;
    const options: Record<string, unknown> = { ...socketOptions };

    if (username !== undefined && password !== undefined) {
      options.credentials = credentials.plain(username, password);
    }
    if (connectionName) {
      options.clientProperties = {
        ...(options.clientProperties as Record<string, unknown> | undefined),
        connection_name: connectionName,
      };
    }

    return options;
  }
}

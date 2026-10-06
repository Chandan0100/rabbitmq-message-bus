import { Injectable } from '@nestjs/common';
import {
  ConnectionOptions,
  ConsumerEndpointOptions,
  ExchangeDefinition,
  linearBackoff,
  MessageDestination,
} from 'rabbitmq-message-bus';
import { DemoDatabase } from './demo-database';

/** The three kinds of exchange the demo can route its events through. */
export type ExchangeMode = 'topic' | 'direct' | 'fanout';
export const EXCHANGE_MODES: ExchangeMode[] = ['topic', 'direct', 'fanout'];

export type ConsumerContext = 'notifications' | 'audit';
export const CONSUMER_CONTEXTS: ConsumerContext[] = ['notifications', 'audit'];

export const TASK_CREATED = 'demo.task_created';
export const TASK_ASSIGNED = 'demo.task_assigned';
export const TASK_COMPLETED = 'demo.task_completed';

/** Routing keys on the topic exchange; the direct exchange routes by message type instead. */
const TOPIC_ROUTING_KEYS: Record<string, string> = {
  [TASK_CREATED]: 'demo.tasks.created',
  [TASK_ASSIGNED]: 'demo.tasks.assigned',
  [TASK_COMPLETED]: 'demo.tasks.completed',
};

/** What each consumer asks the topic exchange for. */
const TOPIC_BINDING_KEYS: Record<ConsumerContext, string[]> = {
  notifications: ['demo.tasks.assigned', 'demo.tasks.completed'],
  audit: ['demo.tasks.#'],
};

/** One direct exchange carries the retries and the failed messages of every consumer. */
export const RECOVERY_EXCHANGE: ExchangeDefinition = {
  name: 'demo.recovery',
  type: 'direct',
};
export const ERROR_QUEUE = 'demo.error';
export const ERROR_ROUTING_KEY = 'demo.dead-letter';

/**
 * Everything the service needs to know about RabbitMQ, in one place. A real
 * service reads these from its environment; here the exchange type can also be
 * switched from the dashboard, which is why it is looked up on every call.
 */
@Injectable()
export class MessagingConfig {
  constructor(private readonly db: DemoDatabase) {}

  get mode(): ExchangeMode {
    return this.db.getSetting(
      'exchange_mode',
      process.env.DEMO_EXCHANGE_TYPE ?? 'topic',
    ) as ExchangeMode;
  }

  setMode(mode: ExchangeMode): void {
    this.db.setSetting('exchange_mode', mode);
  }

  connection(connectionName: string): ConnectionOptions {
    return {
      url: process.env.RABBITMQ_DSN ?? 'amqp://guest:guest@localhost:5681',
      connectionName,
      // Patient on purpose: the demo should survive a broker restart.
      reconnect: { maxAttempts: 30, delayMs: 2000 },
    };
  }

  eventsExchange(mode: ExchangeMode = this.mode): ExchangeDefinition {
    return { name: `demo.${mode}.events`, type: mode };
  }

  /** Where a message type is published. Stored on the outbox row when the event is recorded. */
  destination(type: string, mode: ExchangeMode = this.mode): MessageDestination {
    const exchange = this.eventsExchange(mode).name;

    if (mode === 'topic') {
      return { exchange, routingKey: TOPIC_ROUTING_KEYS[type] ?? type };
    }
    // Direct: the routing key is the message type. Fanout ignores the key.
    return { exchange, routingKey: mode === 'direct' ? type : '' };
  }

  /** How a consumer's queue is bound, which is the only thing the exchange type changes. */
  bindings(
    context: ConsumerContext,
    mode: ExchangeMode = this.mode,
  ): Pick<ConsumerEndpointOptions, 'bindingKeys' | 'bindHandledTypes'> {
    if (mode === 'topic') {
      return { bindingKeys: TOPIC_BINDING_KEYS[context], bindHandledTypes: false };
    }
    // Direct: one exact key per message type the consumer has a handler for.
    return { bindingKeys: [], bindHandledTypes: mode === 'direct' };
  }

  endpoint(
    context: ConsumerContext,
    mode: ExchangeMode = this.mode,
  ): ConsumerEndpointOptions {
    return {
      appName: `demo.${context}`,
      queue: `demo.${mode}.${context}`,
      exchange: this.eventsExchange(mode),
      ...this.bindings(context, mode),
      prefetch: 5,
      retry: {
        exchange: RECOVERY_EXCHANGE,
        // The retry queue and its keys are derived from the queue name.
        immediateRetries: 2,
        delayedRetries: 3,
        backoff: linearBackoff(2000), // 2s, 4s, 6s
        queueMessageTtl: 10000,
      },
      error: {
        exchange: RECOVERY_EXCHANGE,
        routingKey: ERROR_ROUTING_KEY,
        queue: ERROR_QUEUE,
      },
    };
  }
}

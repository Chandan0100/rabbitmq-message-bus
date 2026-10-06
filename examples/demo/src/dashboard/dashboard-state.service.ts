import { Injectable } from '@nestjs/common';
import {
  buildConsumerTopology,
  resolveBindingKeys,
  resolveConsumerEndpoint,
} from 'rabbitmq-message-bus';
import { LazyHandlerRef, NestHandlerMap } from 'rabbitmq-message-bus/nestjs';
import { auditHandlers } from '../audit/audit.handlers';
import { DemoDatabase } from '../infrastructure/demo-database';
import { BEHAVIOURS, FaultInjector } from '../infrastructure/fault-injector';
import {
  CONSUMER_CONTEXTS,
  ConsumerContext,
  ERROR_QUEUE,
  ERROR_ROUTING_KEY,
  EXCHANGE_MODES,
  MessagingConfig,
  RECOVERY_EXCHANGE,
} from '../infrastructure/messaging.config';
import { notificationHandlers } from '../notifications/notifications.handlers';
import { BrokerInspector, PeekedMessage } from './broker-inspector.service';
import { DemoRuntime } from './demo-runtime.service';

const HANDLER_MAPS: Record<ConsumerContext, NestHandlerMap> = {
  notifications: notificationHandlers,
  audit: auditHandlers,
};

interface HandlerInfo {
  name: string;
  context: ConsumerContext;
  types: string[];
}

/** Every processor of the demo, read from the handler maps the consumers use. */
function handlerCatalog(): HandlerInfo[] {
  const catalog = new Map<string, HandlerInfo>();

  for (const context of CONSUMER_CONTEXTS) {
    for (const [type, sources] of Object.entries(HANDLER_MAPS[context])) {
      for (const source of [sources].flat() as LazyHandlerRef[]) {
        const name = source.handler.name;
        const info = catalog.get(name) ?? { name, context, types: [] };
        info.types.push(type);
        catalog.set(name, info);
      }
    }
  }

  return [...catalog.values()];
}

/** DEMO ONLY. Assembles everything the dashboard draws. */
@Injectable()
export class DashboardState {
  private readonly handlers = handlerCatalog();
  private failed: { count: number; at: number; messages: PeekedMessage[] } = {
    count: -1,
    at: 0,
    messages: [],
  };

  constructor(
    private readonly db: DemoDatabase,
    private readonly config: MessagingConfig,
    private readonly faults: FaultInjector,
    private readonly inspector: BrokerInspector,
    private readonly runtime: DemoRuntime,
  ) {}

  handlerNames(): string[] {
    return this.handlers.map((handler) => handler.name);
  }

  /** The queues of every exchange type, for the reset button. */
  allQueues(): string[] {
    return [
      ERROR_QUEUE,
      ...EXCHANGE_MODES.flatMap((mode) =>
        CONSUMER_CONTEXTS.flatMap((context) => {
          const endpoint = resolveConsumerEndpoint(this.config.endpoint(context, mode));
          return [endpoint.queue, endpoint.retry.queue];
        }),
      ),
    ];
  }

  async read() {
    const mode = this.config.mode;

    // Ask the library what it declares for each consumer, then ask the broker how full it is.
    const consumers = CONSUMER_CONTEXTS.map((context) => {
      const endpoint = resolveConsumerEndpoint(this.config.endpoint(context));
      const handlers = this.handlers.filter((handler) => handler.context === context);
      const handledTypes = [...new Set(handlers.flatMap((handler) => handler.types))];
      const bindingKeys = resolveBindingKeys(endpoint, handledTypes);
      return {
        context,
        endpoint,
        handlers,
        bindingKeys,
        topology: buildConsumerTopology(endpoint, bindingKeys),
      };
    });

    const stats = await this.inspector.queueStats([
      ERROR_QUEUE,
      ...consumers.flatMap(({ endpoint }) => [endpoint.queue, endpoint.retry.queue]),
    ]);
    const failed = await this.failedMessages(stats[ERROR_QUEUE]?.messages ?? 0);

    return {
      mode,
      modes: EXCHANGE_MODES,
      broker: {
        reachable: this.inspector.isReachable(),
        managementUrl: this.inspector.managementUrl,
      },
      inlineWorkers: this.runtime.inlineWorkers,
      autoDispatch: this.runtime.autoDispatch,
      behaviours: BEHAVIOURS,
      eventsExchange: this.config.eventsExchange(),
      recoveryExchange: RECOVERY_EXCHANGE,
      errorQueue: {
        name: ERROR_QUEUE,
        routingKey: ERROR_ROUTING_KEY,
        messages: stats[ERROR_QUEUE]?.messages ?? null,
      },
      consumers: consumers.map(({ context, endpoint, handlers, bindingKeys }) => ({
        context,
        state: this.runtime.workerState(context),
        queue: { name: endpoint.queue, ...stats[endpoint.queue] },
        retryQueue: { name: endpoint.retry.queue, ...stats[endpoint.retry.queue] },
        bindingKeys,
        retry: {
          immediateRetries: endpoint.retry.immediateRetries,
          delayedRetries: endpoint.retry.delayedRetries,
          returnBindingKey: endpoint.retry.returnBindingKey,
          queueBindingKey: endpoint.retry.queueBindingKey,
        },
        handlers: handlers.map((handler) => ({
          name: handler.name,
          types: handler.types,
          behaviour: this.faults.getBehaviour(handler.name),
          handled:
            this.db.get<{ count: number }>(
              'select count(*) as count from inbox_message where handler_name = ?',
              handler.name,
            )?.count ?? 0,
        })),
      })),
      outbox: this.outbox(),
      messages: this.journeys(failed),
      failed,
      results: {
        emails: this.db.get<{ count: number }>('select count(*) as count from sent_email')?.count ?? 0,
        auditEntries: this.db.get<{ count: number }>('select count(*) as count from audit_entry')?.count ?? 0,
      },
    };
  }

  private outbox() {
    const counts = this.db.all<{ status: string; count: number }>(
      'select status, count(*) as count from outbox_message group by status',
    );
    const count = (status: string) =>
      counts.find((row) => row.status === status)?.count ?? 0;

    return { pending: count('PENDING'), sent: count('SENT') };
  }

  /** Reading the error queue disturbs it slightly, so only do it when its depth changed. */
  private async failedMessages(count: number): Promise<PeekedMessage[]> {
    if (count !== this.failed.count || Date.now() - this.failed.at > 10000) {
      this.failed = {
        count,
        at: Date.now(),
        messages: count ? await this.inspector.peek(ERROR_QUEUE, 50) : [],
      };
    }
    return this.failed.messages;
  }

  /** For each recent message, how far each of its handlers got. */
  private journeys(failed: PeekedMessage[]) {
    const messages = this.db.all<{
      message_id: string;
      type: string;
      status: string;
      routing_key: string;
      created_at: string;
    }>('select * from outbox_message order by id desc limit 30');
    if (!messages.length) return [];

    const ids = messages.map((message) => message.message_id);
    const marks = ids.map(() => '?').join(', ');
    const handled = new Set(
      this.db
        .all<{ message_id: string; handler_name: string }>(
          `select message_id, handler_name from inbox_message where message_id in (${marks})`,
          ...ids,
        )
        .map((row) => `${row.message_id}|${row.handler_name}`),
    );
    const runs = new Map(
      this.db
        .all<{
          message_id: string;
          handler_name: string;
          attempts: number;
          failures: number;
          last_error: string | null;
        }>(
          `select message_id, handler_name, count(*) as attempts,
                  sum(outcome <> 'passed') as failures,
                  (select error from handler_run latest
                    where latest.message_id = run.message_id
                      and latest.handler_name = run.handler_name
                      and latest.error is not null
                    order by latest.id desc limit 1) as last_error
             from handler_run run
            where message_id in (${marks})
            group by message_id, handler_name`,
          ...ids,
        )
        .map((row) => [`${row.message_id}|${row.handler_name}`, row]),
    );

    return messages.map((message) => ({
      messageId: message.message_id,
      type: message.type,
      routingKey: message.routing_key,
      createdAt: message.created_at,
      outbox: message.status,
      handlers: this.handlers
        .filter((handler) => handler.types.includes(message.type))
        .map((handler) => {
          const key = `${message.message_id}|${handler.name}`;
          const run = runs.get(key);
          const inErrorQueue = failed.some(
            (entry) =>
              entry.messageId === message.message_id &&
              entry.failure?.endpoint.endsWith(`.${handler.context}`),
          );

          let status = message.status === 'PENDING' ? 'pending' : 'waiting';
          if (handled.has(key)) status = 'handled';
          else if (inErrorQueue && run?.failures) status = 'failed';
          else if (run?.failures) status = 'retrying';

          return {
            name: handler.name,
            status,
            attempts: run?.attempts ?? 0,
            lastError: status === 'handled' ? null : (run?.last_error ?? null),
          };
        }),
    }));
  }
}

import {
  Inject,
  Injectable,
  OnApplicationBootstrap,
  OnApplicationShutdown,
} from '@nestjs/common';
import { ConsumerWorker, OutboxDispatcher } from 'rabbitmq-message-bus/nestjs';
import { AUDIT_WORKER } from '../audit/audit.consumer.module';
import { EventLog } from '../infrastructure/event-log';
import {
  CONSUMER_CONTEXTS,
  ConsumerContext,
  ExchangeMode,
  MessagingConfig,
} from '../infrastructure/messaging.config';
import { DemoDatabase } from '../infrastructure/demo-database';
import { NOTIFICATIONS_WORKER } from '../notifications/notifications.consumer.module';

export type WorkerState = 'stopped' | 'starting' | 'consuming' | 'reconnecting';

const AUTO_DISPATCH_INTERVAL_MS = 3000;

/**
 * DEMO ONLY. Runs inside the API process what production runs as separate
 * workers: the two consumers, and the outbox dispatcher on a timer in place of
 * the CronJob. Set DEMO_INLINE_WORKERS=false to run them yourself with the CLI.
 */
@Injectable()
export class DemoRuntime implements OnApplicationBootstrap, OnApplicationShutdown {
  readonly inlineWorkers = process.env.DEMO_INLINE_WORKERS !== 'false';

  private readonly workers: Record<ConsumerContext, ConsumerWorker>;
  private readonly wanted = new Set<ConsumerContext>();
  private readonly starting = new Set<ConsumerContext>();
  private dispatchTimer?: NodeJS.Timeout;
  private dispatching = false;

  constructor(
    @Inject(NOTIFICATIONS_WORKER) notifications: ConsumerWorker,
    @Inject(AUDIT_WORKER) audit: ConsumerWorker,
    private readonly dispatcher: OutboxDispatcher,
    private readonly config: MessagingConfig,
    private readonly db: DemoDatabase,
    private readonly events: EventLog,
  ) {
    this.workers = { notifications, audit };
  }

  onApplicationBootstrap(): void {
    if (this.inlineWorkers) {
      for (const context of CONSUMER_CONTEXTS) void this.startWorker(context);
    }
    if (this.autoDispatch) this.scheduleDispatch();
  }

  async onApplicationShutdown(): Promise<void> {
    if (this.dispatchTimer) clearInterval(this.dispatchTimer);
    this.wanted.clear();
  }

  // --- consumers ---------------------------------------------------------

  workerState(context: ConsumerContext): WorkerState {
    if (this.workers[context].isConsuming()) return 'consuming';
    if (this.starting.has(context)) return 'starting';
    return this.wanted.has(context) ? 'reconnecting' : 'stopped';
  }

  async startWorker(context: ConsumerContext): Promise<void> {
    this.wanted.add(context);
    if (this.starting.has(context) || this.workers[context].isConsuming()) return;

    this.starting.add(context);
    try {
      // The module was configured for the exchange type at boot; these
      // overrides point the worker at the one selected now.
      const { queue, exchange, bindingKeys, bindHandledTypes } =
        this.config.endpoint(context);
      await this.workers[context].start({
        endpoint: {
          queue,
          exchange,
          bindingKeys: bindingKeys as string[],
          bindHandledTypes,
        },
      });
      this.events.write('demo', 'info', `The ${context} consumer is running on ${queue}.`);
    } catch (error) {
      this.events.write(
        'demo',
        'error',
        `The ${context} consumer could not start: ${(error as Error).message}. Trying again in 5s.`,
      );
      setTimeout(() => {
        if (this.wanted.has(context)) void this.startWorker(context);
      }, 5000).unref();
    } finally {
      this.starting.delete(context);
    }
  }

  /** A graceful stop: in-flight messages finish first. */
  async stopWorker(context: ConsumerContext): Promise<void> {
    this.wanted.delete(context);
    await this.workers[context].stop();
    this.events.write('demo', 'info', `The ${context} consumer was stopped.`);
  }

  async switchMode(mode: ExchangeMode): Promise<void> {
    if (mode === this.config.mode) return;

    const running = [...this.wanted];
    await Promise.all(running.map((context) => this.stopWorker(context)));
    this.config.setMode(mode);
    this.events.write('demo', 'info', `Now routing events through a ${mode} exchange.`);
    await Promise.all(running.map((context) => this.startWorker(context)));
  }

  // --- dispatcher --------------------------------------------------------

  get autoDispatch(): boolean {
    return this.db.getSetting('auto_dispatch', 'true') === 'true';
  }

  setAutoDispatch(enabled: boolean): void {
    this.db.setSetting('auto_dispatch', String(enabled));
    if (this.dispatchTimer) clearInterval(this.dispatchTimer);
    this.dispatchTimer = undefined;
    if (enabled) this.scheduleDispatch();
  }

  /** One run of the outbox relay, what `dispatch-messages --module tasks` does. */
  async dispatch(): Promise<{ fetched: number; published: number; failed: number }> {
    // Like the CronJob's concurrencyPolicy: Forbid, never two runs at once.
    if (this.dispatching) return { fetched: 0, published: 0, failed: 0 };

    this.dispatching = true;
    try {
      return await this.dispatcher.dispatch();
    } finally {
      this.dispatching = false;
    }
  }

  private scheduleDispatch(): void {
    this.dispatchTimer = setInterval(() => {
      const pending = this.db.get<{ count: number }>(
        "select count(*) as count from outbox_message where status = 'PENDING'",
      );
      if (!pending?.count) return;

      void this.dispatch().catch((error) =>
        this.events.write('dispatcher', 'error', `Dispatch failed: ${error.message}`),
      );
    }, AUTO_DISPATCH_INTERVAL_MS);
  }
}

import {
  BadRequestException,
  Body,
  Controller,
  Get,
  Headers,
  HttpCode,
  MessageEvent,
  Param,
  Post,
  Sse,
} from '@nestjs/common';
import { concatMap, from, interval, map, Observable, startWith } from 'rxjs';
import { DemoDatabase } from '../infrastructure/demo-database';
import { EventLog } from '../infrastructure/event-log';
import { Behaviour, BEHAVIOURS, FaultInjector } from '../infrastructure/fault-injector';
import {
  CONSUMER_CONTEXTS,
  ConsumerContext,
  ERROR_QUEUE,
  EXCHANGE_MODES,
  ExchangeMode,
  MessagingConfig,
} from '../infrastructure/messaging.config';
import { TasksService } from '../tasks/tasks.service';
import { BrokerInspector } from './broker-inspector.service';
import { DashboardState } from './dashboard-state.service';
import { DemoRuntime } from './demo-runtime.service';

const UNHANDLED_TYPE = 'demo.task_archived';

/** DEMO ONLY. The HTTP API behind the dashboard in ../../public. */
@Controller('api')
export class DashboardController {
  constructor(
    private readonly state: DashboardState,
    private readonly runtime: DemoRuntime,
    private readonly tasks: TasksService,
    private readonly faults: FaultInjector,
    private readonly inspector: BrokerInspector,
    private readonly config: MessagingConfig,
    private readonly events: EventLog,
    private readonly db: DemoDatabase,
  ) {}

  @Get('state')
  read() {
    return this.state.read();
  }

  /** Live log: every process writes to the event_log table, this streams new rows. */
  @Sse('events')
  stream(@Headers('last-event-id') lastEventId?: string): Observable<MessageEvent> {
    let cursor = lastEventId
      ? Number(lastEventId)
      : Math.max(0, this.events.latestId() - 150);

    return interval(300).pipe(
      startWith(0),
      map(() => this.events.after(cursor)),
      concatMap((rows) => {
        if (rows.length) cursor = rows[rows.length - 1].id;
        return from(rows.map((row) => ({ id: String(row.id), data: row })));
      }),
    );
  }

  // --- business actions: these only touch the database -------------------

  @Post('tasks')
  @HttpCode(200)
  act(@Body() body: { action: 'create' | 'assign' | 'complete'; count?: number }) {
    const count = Math.min(Math.max(Number(body.count) || 1, 1), 50);

    for (let i = 0; i < count; i++) {
      if (body.action === 'create') {
        this.tasks.createTask();
      } else if (body.action === 'assign') {
        this.tasks.assignTask(this.tasks.findOpenTask(false) ?? this.tasks.createTask());
      } else if (body.action === 'complete') {
        const task =
          this.tasks.findOpenTask(true) ??
          this.tasks.assignTask(this.tasks.createTask());
        this.tasks.completeTask(task);
      } else {
        throw new BadRequestException('Unknown action');
      }
    }

    this.events.write('demo', 'info', `${count} x "${body.action} task": events written to the outbox.`);
    return { ok: true };
  }

  // --- what the workers do ------------------------------------------------

  @Post('dispatch')
  @HttpCode(200)
  dispatch() {
    return this.runtime.dispatch();
  }

  @Post('settings')
  @HttpCode(200)
  async configure(@Body() body: { autoDispatch?: boolean; mode?: ExchangeMode }) {
    if (body.autoDispatch !== undefined) this.runtime.setAutoDispatch(body.autoDispatch);
    if (body.mode !== undefined) {
      if (!EXCHANGE_MODES.includes(body.mode)) {
        throw new BadRequestException('Unknown exchange type');
      }
      await this.runtime.switchMode(body.mode);
    }
    return { ok: true };
  }

  @Post('workers/:context/:action')
  @HttpCode(200)
  async controlWorker(
    @Param('context') context: ConsumerContext,
    @Param('action') action: 'start' | 'stop',
  ) {
    if (!CONSUMER_CONTEXTS.includes(context)) {
      throw new BadRequestException('Unknown consumer');
    }
    if (action === 'stop') await this.runtime.stopWorker(context);
    else void this.runtime.startWorker(context);
    return { ok: true };
  }

  @Post('behaviours')
  @HttpCode(200)
  setBehaviour(@Body() body: { handler: string; behaviour: Behaviour }) {
    if (!this.state.handlerNames().includes(body.handler) || !(body.behaviour in BEHAVIOURS)) {
      throw new BadRequestException('Unknown handler or behaviour');
    }
    this.faults.setBehaviour(body.handler, body.behaviour);
    this.events.write('demo', 'info', `${body.handler} now: ${BEHAVIOURS[body.behaviour].toLowerCase()}.`);
    return { ok: true };
  }

  // --- things that go wrong in production --------------------------------

  @Post('chaos/kill-connections')
  @HttpCode(200)
  async killConnections() {
    const closed = await this.inspector.killConsumerConnections();
    this.events.write(
      'demo',
      'warn',
      closed
        ? `Closed ${closed} consumer connection(s) from the broker side.`
        : 'No consumer connection is listed by the broker yet; try again in a few seconds.',
    );
    return { closed };
  }

  /** Publishes the latest sent message a second time, as an outbox relay may after a crash. */
  @Post('chaos/duplicate')
  @HttpCode(200)
  async duplicate() {
    const row = this.db.get<{ message_id: string; type: string; properties: string; body: string }>(
      "select * from outbox_message where status = 'SENT' order by id desc limit 1",
    );
    if (!row) throw new BadRequestException('Nothing has been published yet.');

    const { exchange, routingKey } = this.config.destination(row.type);
    await this.inspector.publish(exchange, routingKey, row.body, JSON.parse(row.properties));
    this.events.write('demo', 'warn', `Published ${row.type} ${row.message_id} a second time.`, row.message_id);
    return { messageId: row.message_id };
  }

  /** Publishes an event type that no consumer has a handler for. */
  @Post('chaos/unhandled')
  @HttpCode(200)
  async unhandled() {
    const messageId = crypto.randomUUID();
    const mode = this.config.mode;
    const { exchange } = this.config.destination(UNHANDLED_TYPE);
    const routingKey = mode === 'topic' ? 'demo.tasks.archived' : mode === 'direct' ? UNHANDLED_TYPE : '';

    await this.inspector.publish(exchange, routingKey, JSON.stringify({ uuid: messageId }), {
      messageId,
      type: UNHANDLED_TYPE,
      headers: { type: UNHANDLED_TYPE },
    });
    this.events.write(
      'demo',
      'warn',
      mode === 'direct'
        ? `Published ${UNHANDLED_TYPE}. No queue binds that key on a direct exchange, so the broker drops it.`
        : `Published ${UNHANDLED_TYPE}. Queues that receive it acknowledge and ignore it.`,
      messageId,
    );
    return { messageId };
  }

  /** Deletes a consumer's queue under its feet; the broker cancels the consumer, which declares it again. */
  @Post('chaos/delete-queue/:context')
  @HttpCode(200)
  async deleteQueue(@Param('context') context: ConsumerContext) {
    if (!CONSUMER_CONTEXTS.includes(context)) {
      throw new BadRequestException('Unknown consumer');
    }
    const { queue } = this.config.endpoint(context);
    await this.inspector.deleteQueue(queue);
    this.events.write('demo', 'warn', `Deleted queue ${queue}.`);
    return { queue };
  }

  // --- looking into queues ------------------------------------------------

  @Get('queues/:queue/messages')
  peek(@Param('queue') queue: string) {
    if (!this.state.allQueues().includes(queue)) {
      throw new BadRequestException('Not a queue of this demo');
    }
    return this.inspector.peek(queue, 20);
  }

  @Post('error-queue/replay')
  @HttpCode(200)
  async replay(@Body() body: { messageId?: string }) {
    const replayed = await this.inspector.replay(ERROR_QUEUE, body.messageId);
    this.events.write(
      'demo',
      'info',
      `Replayed ${replayed} message(s) from ${ERROR_QUEUE} to the queue they failed in.`,
      body.messageId,
    );
    return { replayed };
  }

  @Post('reset')
  @HttpCode(200)
  async reset() {
    await this.inspector.purge(this.state.allQueues());
    for (const table of ['task', 'outbox_message', 'inbox_message', 'sent_email', 'audit_entry', 'handler_run', 'event_log']) {
      this.db.run(`delete from ${table}`);
    }
    for (const handler of this.state.handlerNames()) {
      this.faults.setBehaviour(handler, 'succeed');
    }
    this.events.write('demo', 'info', 'Queues purged and tables cleared.');
    return { ok: true };
  }
}

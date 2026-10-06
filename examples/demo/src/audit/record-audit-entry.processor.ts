import { Injectable, Module } from '@nestjs/common';
import { EventPayload, MessageHandler } from 'rabbitmq-message-bus';
import { DemoDatabase } from '../infrastructure/demo-database';
import { EventLog } from '../infrastructure/event-log';
import { FaultInjector } from '../infrastructure/fault-injector';
import { SqliteInboxStore } from '../infrastructure/sqlite-inbox.store';

interface TaskEventBody {
  task_uuid: string;
  title: string;
}

/** Keeps a trail of everything that happens to a task. One processor, registered for every task event. */
@Injectable()
export class RecordAuditEntryProcessor implements MessageHandler<TaskEventBody> {
  constructor(
    private readonly db: DemoDatabase,
    private readonly inbox: SqliteInboxStore,
    private readonly faults: FaultInjector,
    private readonly events: EventLog,
  ) {}

  getHandlerName(): string {
    return this.constructor.name;
  }

  async handle(message: EventPayload<TaskEventBody>): Promise<void> {
    await this.faults.before(this.getHandlerName(), message); // demo only

    const { body, messageId, type = 'unknown' } = message;
    const summary = `${type.replace('demo.task_', '')}: ${body.title}`;

    this.db.transaction(() => {
      this.db.run(
        'insert into audit_entry (message_id, type, summary, recorded_at) values (?, ?, ?, ?)',
        messageId,
        type,
        summary,
        new Date().toISOString(),
      );
      this.inbox.record(messageId, this.getHandlerName());
    });

    this.events.write('handler', 'info', `Audit trail: ${summary}`, messageId);
  }
}

@Module({ providers: [RecordAuditEntryProcessor] })
export class RecordAuditEntryProcessorModule {}

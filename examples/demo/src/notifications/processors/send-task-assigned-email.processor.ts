import { Injectable, Module } from '@nestjs/common';
import { EventPayload, MessageHandler } from 'rabbitmq-message-bus';
import { DemoDatabase } from '../../infrastructure/demo-database';
import { EventLog } from '../../infrastructure/event-log';
import { FaultInjector } from '../../infrastructure/fault-injector';
import { SqliteInboxStore } from '../../infrastructure/sqlite-inbox.store';

interface TaskAssignedBody {
  task_uuid: string;
  title: string;
  assignee: string;
}

/** Emails the person a task was assigned to. */
@Injectable()
export class SendTaskAssignedEmailProcessor
  implements MessageHandler<TaskAssignedBody>
{
  constructor(
    private readonly db: DemoDatabase,
    private readonly inbox: SqliteInboxStore,
    private readonly faults: FaultInjector,
    private readonly events: EventLog,
  ) {}

  /** Identifies this handler in the inbox. */
  getHandlerName(): string {
    return this.constructor.name;
  }

  async handle(message: EventPayload<TaskAssignedBody>): Promise<void> {
    await this.faults.before(this.getHandlerName(), message); // demo only

    const { body, messageId } = message;
    const subject = `You were assigned "${body.title}"`;

    // The email and the inbox entry are committed together. If this message is
    // delivered again, the consumer finds the inbox entry and skips this handler.
    this.db.transaction(() => {
      this.db.run(
        'insert into sent_email (message_id, recipient, subject, sent_at) values (?, ?, ?, ?)',
        messageId,
        body.assignee,
        subject,
        new Date().toISOString(),
      );
      this.inbox.record(messageId, this.getHandlerName());
    });

    this.events.write('handler', 'info', `Emailed ${body.assignee}: ${subject}`, messageId);
  }
}

/** Loaded on demand by the consumer, see ../notifications.handlers.ts. */
@Module({ providers: [SendTaskAssignedEmailProcessor] })
export class SendTaskAssignedEmailProcessorModule {}

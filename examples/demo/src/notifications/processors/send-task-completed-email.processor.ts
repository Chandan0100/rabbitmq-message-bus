import { Injectable, Module } from '@nestjs/common';
import { EventPayload, MessageHandler } from 'rabbitmq-message-bus';
import { DemoDatabase } from '../../infrastructure/demo-database';
import { EventLog } from '../../infrastructure/event-log';
import { FaultInjector } from '../../infrastructure/fault-injector';
import { SqliteInboxStore } from '../../infrastructure/sqlite-inbox.store';

interface TaskCompletedBody {
  task_uuid: string;
  title: string;
  assignee: string | null;
}

const TEAM_LEAD = 'lead@example.com';

/** Emails the team lead when a task is done. */
@Injectable()
export class SendTaskCompletedEmailProcessor
  implements MessageHandler<TaskCompletedBody>
{
  constructor(
    private readonly db: DemoDatabase,
    private readonly inbox: SqliteInboxStore,
    private readonly faults: FaultInjector,
    private readonly events: EventLog,
  ) {}

  getHandlerName(): string {
    return this.constructor.name;
  }

  async handle(message: EventPayload<TaskCompletedBody>): Promise<void> {
    await this.faults.before(this.getHandlerName(), message); // demo only

    const { body, messageId } = message;
    const subject = `"${body.title}" is done`;

    this.db.transaction(() => {
      this.db.run(
        'insert into sent_email (message_id, recipient, subject, sent_at) values (?, ?, ?, ?)',
        messageId,
        TEAM_LEAD,
        subject,
        new Date().toISOString(),
      );
      this.inbox.record(messageId, this.getHandlerName());
    });

    this.events.write('handler', 'info', `Emailed ${TEAM_LEAD}: ${subject}`, messageId);
  }
}

@Module({ providers: [SendTaskCompletedEmailProcessor] })
export class SendTaskCompletedEmailProcessorModule {}

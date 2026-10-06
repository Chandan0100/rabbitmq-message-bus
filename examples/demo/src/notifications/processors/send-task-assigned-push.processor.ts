import { Injectable, Module } from '@nestjs/common';
import { EventPayload, MessageHandler } from 'rabbitmq-message-bus';
import { EventLog } from '../../infrastructure/event-log';
import { FaultInjector } from '../../infrastructure/fault-injector';
import { SqliteInboxStore } from '../../infrastructure/sqlite-inbox.store';

interface TaskAssignedBody {
  title: string;
  assignee: string;
}

/**
 * A second handler for the same message type as the email. The two run
 * side by side; when only one fails, only that one runs again on the retry.
 */
@Injectable()
export class SendTaskAssignedPushProcessor
  implements MessageHandler<TaskAssignedBody>
{
  constructor(
    private readonly inbox: SqliteInboxStore,
    private readonly faults: FaultInjector,
    private readonly events: EventLog,
  ) {}

  getHandlerName(): string {
    return this.constructor.name;
  }

  async handle(message: EventPayload<TaskAssignedBody>): Promise<void> {
    await this.faults.before(this.getHandlerName(), message); // demo only

    // Nothing to store here, so only the inbox entry is written.
    this.inbox.record(message.messageId, this.getHandlerName());

    this.events.write(
      'handler',
      'info',
      `Pushed to ${message.body.assignee}'s phone: ${message.body.title}`,
      message.messageId,
    );
  }
}

@Module({ providers: [SendTaskAssignedPushProcessor] })
export class SendTaskAssignedPushProcessorModule {}

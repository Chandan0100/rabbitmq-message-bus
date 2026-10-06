import 'reflect-metadata';
import { runMessageBusCli } from 'rabbitmq-message-bus/nestjs';
import { AuditConsumerModule } from './audit/audit.consumer.module';
import { NotificationsConsumerModule } from './notifications/notifications.consumer.module';
import { TasksProducerModule } from './tasks/tasks.producer.module';

/**
 * The worker processes of the service, exactly as they run in production:
 *
 *   node dist/message-bus-cli.js dispatch-messages --module tasks
 *   node dist/message-bus-cli.js handle-messages --module notifications
 *   node dist/message-bus-cli.js handle-messages --module audit
 *
 * `npm start` runs the same modules inside the API process instead, so the
 * demo is one command.
 */
void runMessageBusCli({
  consumers: {
    notifications: NotificationsConsumerModule,
    audit: AuditConsumerModule,
  },
  producers: {
    tasks: TasksProducerModule,
  },
});

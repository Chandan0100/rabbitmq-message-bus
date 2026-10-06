import { Module } from '@nestjs/common';
import { ConsumerWorker, RabbitMqConsumerModule } from 'rabbitmq-message-bus/nestjs';
import { EventLog } from '../infrastructure/event-log';
import { InfrastructureModule } from '../infrastructure/infrastructure.module';
import { MessagingConfig } from '../infrastructure/messaging.config';
import { SqliteInboxStore } from '../infrastructure/sqlite-inbox.store';
import { notificationHandlers } from './notifications.handlers';

/** Lets an application that hosts several consumers tell their workers apart. */
export const NOTIFICATIONS_WORKER = Symbol('NOTIFICATIONS_WORKER');

/**
 * The notifications consumer: one queue, its retry queue, and the processors
 * that send emails and push notifications.
 *
 * Run on its own with `handle-messages --module notifications`.
 */
@Module({
  imports: [
    InfrastructureModule,
    RabbitMqConsumerModule.registerAsync({
      inject: [MessagingConfig, SqliteInboxStore, EventLog],
      useFactory: (
        config: MessagingConfig,
        store: SqliteInboxStore,
        events: EventLog,
      ) => ({
        connection: config.connection('demo-consumer-notifications'),
        endpoint: config.endpoint('notifications'),
        handlers: notificationHandlers,
        // The processors write the inbox entry in their own transaction.
        inbox: { store, record: 'handler' },
        logger: events.loggerFor('notifications'),
      }),
    }),
  ],
  providers: [{ provide: NOTIFICATIONS_WORKER, useExisting: ConsumerWorker }],
  exports: [NOTIFICATIONS_WORKER],
})
export class NotificationsConsumerModule {}

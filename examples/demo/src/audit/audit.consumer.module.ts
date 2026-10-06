import { Module } from '@nestjs/common';
import { ConsumerWorker, RabbitMqConsumerModule } from 'rabbitmq-message-bus/nestjs';
import { EventLog } from '../infrastructure/event-log';
import { InfrastructureModule } from '../infrastructure/infrastructure.module';
import { MessagingConfig } from '../infrastructure/messaging.config';
import { SqliteInboxStore } from '../infrastructure/sqlite-inbox.store';
import { auditHandlers } from './audit.handlers';

export const AUDIT_WORKER = Symbol('AUDIT_WORKER');

/**
 * The audit consumer. It has its own queue and retry queue, so a failure here
 * never delays the notifications consumer, and the other way round.
 *
 * Run on its own with `handle-messages --module audit`.
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
        connection: config.connection('demo-consumer-audit'),
        endpoint: config.endpoint('audit'),
        handlers: auditHandlers,
        inbox: { store, record: 'handler' },
        logger: events.loggerFor('audit'),
      }),
    }),
  ],
  providers: [{ provide: AUDIT_WORKER, useExisting: ConsumerWorker }],
  exports: [AUDIT_WORKER],
})
export class AuditConsumerModule {}

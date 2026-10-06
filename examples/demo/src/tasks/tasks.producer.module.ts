import { Module } from '@nestjs/common';
import { RabbitMqProducerModule } from 'rabbitmq-message-bus/nestjs';
import { EventLog } from '../infrastructure/event-log';
import { InfrastructureModule } from '../infrastructure/infrastructure.module';
import {
  EXCHANGE_MODES,
  MessagingConfig,
} from '../infrastructure/messaging.config';
import { SqliteOutboxStore } from '../infrastructure/sqlite-outbox.store';

const producer = RabbitMqProducerModule.registerAsync({
  inject: [MessagingConfig, SqliteOutboxStore, EventLog],
  useFactory: (
    config: MessagingConfig,
    outboxStore: SqliteOutboxStore,
    events: EventLog,
  ) => ({
    connection: config.connection('demo-dispatcher'),
    // A real service lists its one exchange. The demo declares one per exchange type.
    exchanges: EXCHANGE_MODES.map((mode) => config.eventsExchange(mode)),
    outboxStore,
    batchSize: 20,
    logger: events.loggerFor('dispatcher'),
  }),
});

/**
 * The publishing side of the tasks context: gives `dispatch-messages --module
 * tasks` an OutboxDispatcher that relays `outbox_message` to RabbitMQ.
 */
@Module({
  imports: [InfrastructureModule, producer],
  exports: [producer],
})
export class TasksProducerModule {}

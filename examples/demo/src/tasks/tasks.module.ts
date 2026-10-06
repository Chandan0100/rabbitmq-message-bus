import { Module, OnModuleInit } from '@nestjs/common';
import {
  DestinationRegistry,
  MessageDestinationModule,
} from 'rabbitmq-message-bus/nestjs';
import { MessagingConfig } from '../infrastructure/messaging.config';
import { TasksService } from './tasks.service';

/**
 * The task features of the API.
 *
 * A service with a fixed exchange would list its message types here:
 *
 *   MessageDestinationModule.register({
 *     destinations: {
 *       'demo.task_created': { exchange: 'events', routingKey: 'demo.tasks.created' },
 *     },
 *   })
 *
 * The demo lets you switch the exchange type while it runs, so it resolves
 * destinations through a fallback that asks the current configuration.
 */
@Module({
  imports: [MessageDestinationModule.register()],
  providers: [TasksService],
  exports: [TasksService],
})
export class TasksModule implements OnModuleInit {
  constructor(
    private readonly destinations: DestinationRegistry,
    private readonly config: MessagingConfig,
  ) {}

  onModuleInit(): void {
    this.destinations.setFallback((type) => this.config.destination(type));
  }
}

import { Global, Module } from '@nestjs/common';
import { DemoDatabase } from './demo-database';
import { EventLog } from './event-log';
import { FaultInjector } from './fault-injector';
import { MessagingConfig } from './messaging.config';
import { SqliteInboxStore } from './sqlite-inbox.store';
import { SqliteOutboxStore } from './sqlite-outbox.store';

const providers = [
  DemoDatabase,
  EventLog,
  FaultInjector,
  MessagingConfig,
  SqliteInboxStore,
  SqliteOutboxStore,
];

/** Database, configuration and stores shared by every part of the service. */
@Global()
@Module({ providers, exports: providers })
export class InfrastructureModule {}

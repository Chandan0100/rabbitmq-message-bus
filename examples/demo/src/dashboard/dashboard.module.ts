import { Module } from '@nestjs/common';
import { AuditConsumerModule } from '../audit/audit.consumer.module';
import { NotificationsConsumerModule } from '../notifications/notifications.consumer.module';
import { TasksModule } from '../tasks/tasks.module';
import { TasksProducerModule } from '../tasks/tasks.producer.module';
import { BrokerInspector } from './broker-inspector.service';
import { DashboardController } from './dashboard.controller';
import { DashboardState } from './dashboard-state.service';
import { DemoRuntime } from './demo-runtime.service';

/** DEMO ONLY. The dashboard and the in-process stand-ins for the workers. */
@Module({
  imports: [
    TasksModule,
    TasksProducerModule,
    NotificationsConsumerModule,
    AuditConsumerModule,
  ],
  controllers: [DashboardController],
  providers: [BrokerInspector, DashboardState, DemoRuntime],
})
export class DashboardModule {}

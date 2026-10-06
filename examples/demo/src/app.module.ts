import { Module } from '@nestjs/common';
import { DashboardModule } from './dashboard/dashboard.module';
import { InfrastructureModule } from './infrastructure/infrastructure.module';

/**
 * The API process. In production it would hold the HTTP features only; the
 * dashboard module also hosts the consumers and the dispatcher so that
 * `npm start` shows the whole flow.
 */
@Module({
  imports: [InfrastructureModule, DashboardModule],
})
export class AppModule {}

import { lazyHandler, NestHandlerMap } from 'rabbitmq-message-bus/nestjs';
import {
  TASK_ASSIGNED,
  TASK_COMPLETED,
  TASK_CREATED,
} from '../infrastructure/messaging.config';
import {
  RecordAuditEntryProcessor,
  RecordAuditEntryProcessorModule,
} from './record-audit-entry.processor';

const recordAuditEntry = lazyHandler(
  RecordAuditEntryProcessorModule,
  RecordAuditEntryProcessor,
);

/** The audit consumer handles every task event with the same processor. */
export const auditHandlers: NestHandlerMap = {
  [TASK_CREATED]: recordAuditEntry,
  [TASK_ASSIGNED]: recordAuditEntry,
  [TASK_COMPLETED]: recordAuditEntry,
};

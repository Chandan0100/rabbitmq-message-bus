import { lazyHandler, NestHandlerMap } from 'rabbitmq-message-bus/nestjs';
import {
  TASK_ASSIGNED,
  TASK_COMPLETED,
} from '../infrastructure/messaging.config';
import {
  SendTaskAssignedEmailProcessor,
  SendTaskAssignedEmailProcessorModule,
} from './processors/send-task-assigned-email.processor';
import {
  SendTaskAssignedPushProcessor,
  SendTaskAssignedPushProcessorModule,
} from './processors/send-task-assigned-push.processor';
import {
  SendTaskCompletedEmailProcessor,
  SendTaskCompletedEmailProcessorModule,
} from './processors/send-task-completed-email.processor';

/** Which processors handle which message type in the notifications consumer. */
export const notificationHandlers: NestHandlerMap = {
  [TASK_ASSIGNED]: [
    lazyHandler(SendTaskAssignedEmailProcessorModule, SendTaskAssignedEmailProcessor),
    lazyHandler(SendTaskAssignedPushProcessorModule, SendTaskAssignedPushProcessor),
  ],
  [TASK_COMPLETED]: [
    lazyHandler(SendTaskCompletedEmailProcessorModule, SendTaskCompletedEmailProcessor),
  ],
};

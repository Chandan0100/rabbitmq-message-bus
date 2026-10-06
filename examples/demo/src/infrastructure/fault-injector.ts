import { Injectable } from '@nestjs/common';
import { EventPayload, UnrecoverableMessageError } from 'rabbitmq-message-bus';
import { DemoDatabase } from './demo-database';
import { EventLog } from './event-log';

export const BEHAVIOURS = {
  succeed: 'Succeeds',
  'fail-once': 'Fails the first attempt',
  flaky: 'Fails until the 2nd delayed retry',
  'always-fail': 'Always fails',
  unrecoverable: 'Throws an unrecoverable error',
  slow: 'Takes 3 seconds',
} as const;

export type Behaviour = keyof typeof BEHAVIOURS;

/**
 * DEMO ONLY. Lets the dashboard make a handler misbehave, and records every
 * attempt so the dashboard can show it. A real handler has none of this.
 */
@Injectable()
export class FaultInjector {
  constructor(
    private readonly db: DemoDatabase,
    private readonly events: EventLog,
  ) {}

  getBehaviour(handlerName: string): Behaviour {
    return this.db.getSetting(`behaviour:${handlerName}`, 'succeed') as Behaviour;
  }

  setBehaviour(handlerName: string, behaviour: Behaviour): void {
    this.db.setSetting(`behaviour:${handlerName}`, behaviour);
  }

  /** Call first in `handle()`. Throws when the dashboard asked this handler to fail. */
  async before(handlerName: string, message: EventPayload): Promise<void> {
    const behaviour = this.getBehaviour(handlerName);
    const redeliveryCount = Number(message.headers?.redelivery_count ?? 0);
    const failuresSoFar =
      this.db.get<{ count: number }>(
        "select count(*) as count from handler_run where message_id = ? and handler_name = ? and outcome <> 'passed'",
        message.messageId,
        handlerName,
      )?.count ?? 0;

    let failure: Error | undefined;
    if (behaviour === 'fail-once' && failuresSoFar === 0) {
      failure = new Error('Simulated glitch on the first attempt');
    } else if (behaviour === 'flaky' && redeliveryCount < 2) {
      failure = new Error('Simulated outage, back on the 2nd delayed retry');
    } else if (behaviour === 'always-fail') {
      failure = new Error('Simulated bug, this will never work');
    } else if (behaviour === 'unrecoverable') {
      failure = new UnrecoverableMessageError('Simulated invalid message');
    } else if (behaviour === 'slow') {
      await new Promise((resolve) => setTimeout(resolve, 3000));
    }

    this.db.run(
      'insert into handler_run (message_id, handler_name, redelivery_count, outcome, error, at) values (?, ?, ?, ?, ?, ?)',
      message.messageId,
      handlerName,
      redeliveryCount,
      failure ? 'failed' : 'passed',
      failure?.message ?? null,
      new Date().toISOString(),
    );

    if (failure) {
      this.events.write(
        'handler',
        'warn',
        `${handlerName} failed on message ${message.messageId} (delivery ${redeliveryCount + 1}): ${failure.message}`,
        message.messageId,
      );
      throw failure;
    }
  }
}

import { HandlerExecutionError, UnrecoverableMessageError } from '../errors';
import { BackoffStrategy } from './backoff';

export type RecoveryDecision =
  | { action: 'retry'; attempt: number; delayMs: number }
  | { action: 'error-queue'; reason: 'unrecoverable' | 'retries-exhausted' };

export interface RetryPolicyOptions {
  delayedRetries: number;
  backoff: BackoffStrategy;
  /** Upper bound for a delay; the retry queue TTL caps it on the broker anyway. */
  maxDelayMs: number;
}

/** Decides what happens to a message once its handlers have failed. */
export class RetryPolicy {
  constructor(private readonly options: RetryPolicyOptions) {}

  decide(redeliveryCount: number, error: unknown): RecoveryDecision {
    if (this.isUnrecoverable(error)) {
      return { action: 'error-queue', reason: 'unrecoverable' };
    }
    if (redeliveryCount >= this.options.delayedRetries) {
      return { action: 'error-queue', reason: 'retries-exhausted' };
    }

    const attempt = redeliveryCount + 1;
    const delayMs = Math.max(
      0,
      Math.min(Math.round(this.options.backoff(attempt)), this.options.maxDelayMs),
    );

    return { action: 'retry', attempt, delayMs };
  }

  private isUnrecoverable(error: unknown): boolean {
    if (error instanceof HandlerExecutionError) return error.unrecoverable;
    return error instanceof UnrecoverableMessageError;
  }
}

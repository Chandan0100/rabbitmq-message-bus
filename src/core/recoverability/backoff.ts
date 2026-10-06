/**
 * Returns the delay in milliseconds before a delayed retry.
 * `attempt` starts at 1 for the first delayed retry.
 */
export type BackoffStrategy = (attempt: number) => number;

/** 2s, 4s, 6s... with the default step. */
export const linearBackoff =
  (stepMs = 2000): BackoffStrategy =>
  (attempt) =>
    attempt * stepMs;

export const fixedBackoff =
  (delayMs: number): BackoffStrategy =>
  () =>
    delayMs;

export const exponentialBackoff =
  (initialMs = 1000, factor = 2): BackoffStrategy =>
  (attempt) =>
    initialMs * factor ** (attempt - 1);

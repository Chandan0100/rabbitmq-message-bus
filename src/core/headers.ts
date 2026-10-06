/** Message headers written and read by the retry and error-queue flow. */
export const MessageHeaders = {
  /** Number of delayed retries the message has already been through. */
  REDELIVERY_COUNT: 'redelivery_count',
  /** Name of the endpoint that scheduled the retry; only that endpoint handles it. */
  RETRY_ENDPOINT: 'retry_endpoint',
  /** Fallback location of the message type when `properties.type` is not set. */
  TYPE: 'type',
  /** Failure details attached when a message is moved to the error queue. */
  EXCEPTION_DETAILS: 'exception_details',
  /** Where the failed message came from, so it can be replayed. */
  ENDPOINT: 'endpoint',
} as const;

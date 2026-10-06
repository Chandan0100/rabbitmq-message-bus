# 9. Troubleshooting

Find the symptom or the log line; each entry says what it means and what to do.

- [A message is published but never handled](#a-message-is-published-but-never-handled)
- [A message is handled more than once](#a-message-is-handled-more-than-once)
- [A message disappeared](#a-message-disappeared)
- [The worker does not start](#the-worker-does-not-start)
- [The worker keeps restarting](#the-worker-keeps-restarting)
- [Retries behave unexpectedly](#retries-behave-unexpectedly)
- [The outbox does not empty](#the-outbox-does-not-empty)
- [NestJS errors](#nestjs-errors)
- [Looking at the broker](#looking-at-the-broker)

## A message is published but never handled

Work from the queue backwards.

**The message never reaches the queue.** In the management console, the queue's
"Ready" count does not move when you publish.

- The routing key matches none of the queue's bindings. Compare the key you
  publish with the bindings listed on the queue. On a direct exchange the match
  must be exact; `billing.#` there is a literal key.
- The publisher uses another exchange than the queue is bound to.
- The consumer has never run, so the queue and its bindings do not exist yet.
  Exchanges drop messages that no queue is bound for.

**The message reaches the queue and is ignored.** The consumer logs one of:

| Log line | Fix |
| --- | --- |
| `Message ignored: Message does not have a messageId.` | Set `messageId` in the message properties |
| `Message ignored: No available handler found or missing message type property.` | Set `type` in the properties (or a `type` header), and check it is spelled exactly like the key in your handler map |
| `Message ignored: Message is not intended for this service.` | The message is a retry scheduled under another `appName`. Usually `appName` was changed while messages were waiting in the retry queue; they are dropped by design |

**The message sits in the queue.** "Ready" stays above zero: no consumer is
connected (the queue's "Consumers" is 0), or all its `prefetch` slots are busy
with slow handlers.

## A message is handled more than once

Expected without an inbox. With one:

- **`inbox.record` is `'handler'` but the handler never records.** The consumer
  checks the inbox and finds nothing every time. Write the entry in the
  handler, or switch to `'pipeline'`.
- **`getHandlerName()` changed.** The inbox is keyed by message id and handler
  name; a renamed handler looks like one that never ran.
- **The store looks in the wrong place.** With several schemas or tenants, the
  check and the write must use the same `scope`.
- **The handler's effect is outside the database.** An email sent just before a
  crash is sent again on redelivery, because the inbox entry was never written.
  That window cannot be closed for external effects; make them repeatable, or
  let the other side de-duplicate on `messageId`.

## A message disappeared

- **`... was not routed to any queue by exchange "..." with routing key "..." and has been dropped.`**
  A message that used up its retries was published to the error exchange and
  nothing is bound there. Set `error.queue` so the consumer declares it, or
  bind the queue that is supposed to exist.
- **It was ignored.** See the table above; ignored messages are acknowledged.
- **It was published to an exchange with no matching binding.** RabbitMQ drops
  such messages silently. This is the usual cause on a direct exchange.

## The worker does not start

| Message | Meaning |
| --- | --- |
| `Missing required consumer option: <name>` | That option is empty, often an environment variable that is not set |
| `Consumer option <name> must be an integer >= <n>, received NaN` | A numeric option was parsed from a missing or non-numeric environment variable |
| `Retry exchange "..." must be of type "direct"` | Use a direct exchange for retries; see [Exchange types](05-exchange-types.md#the-retry-and-error-exchanges) |
| `Exchange "..." is declared both as "topic" and "direct"` | The same exchange name appears with two types in one endpoint |
| `Queue "..." has no binding key for topic exchange "..."` | Set `bindingKeys` or `bindHandledTypes` |
| `Binding key "..." is used both by the primary queue and the retry queue` | A key would route new messages into the retry queue. Rename one |
| `retry.queueBindingKey and retry.returnBindingKey must differ` | With equal keys a failed message would skip the wait |
| `error.routingKey "..." collides with a retry key` | Failed messages would land in a retry or primary queue |
| `Handler registered for "..." must implement handle() and getHandlerName()` | The handler map points at something that is not a handler |
| `RabbitMQ connection url must be a valid amqp:// or amqps:// URL` | Check `RABBITMQ_DSN` |
| `Invalid or missing --module. Available modules: ...` | The name is not a key of `consumers` / `producers` in your command file |
| `PRECONDITION_FAILED - inequivalent arg 'x-message-ttl' for queue '...'` | The queue exists with different arguments. See below |

**`PRECONDITION_FAILED - inequivalent arg ...`** RabbitMQ does not let a queue
or exchange be re-declared with different settings. It happens after changing
`retry.queueMessageTtl`, `queueType`, the retry exchange or
`retry.returnBindingKey`, or an exchange's type. Either restore the old value,
or delete the queue (when empty) or exchange and let the consumer declare it
again.

## The worker keeps restarting

- **`Failed to establish connection to RabbitMQ: ...` then
  `Maximum reconnect tries reached.`** The broker is unreachable: wrong host or
  port, wrong credentials (`ACCESS_REFUSED`), or a virtual host that does not
  exist. The worker exits so its supervisor restarts it; this is by design.
- **It exits right after `Disconnected from RabbitMQ`.** The broker was down
  longer than the reconnect policy tolerates. Raise `reconnect.maxAttempts` or
  `reconnect.delayMs` if short outages are normal for you.
- **`Giving up subscribing to <queue>: ...`** The connection came back but the
  topology could not be declared; the message says why, usually a
  `PRECONDITION_FAILED` or missing permissions.

## Retries behave unexpectedly

- **A delay is shorter than configured.** It is capped at
  `retry.queueMessageTtl`. The consumer warns at startup:
  `... delays are capped at the queue TTL`.
- **A delay is longer than configured.** The retry queue expires messages from
  its front, so a message waits behind one with a longer delay.
- **A failing message goes to the error queue at once.** The handler threw
  `UnrecoverableMessageError`, the content could not be parsed, or
  `delayedRetries` is `0`.
- **A handler that already succeeded runs again on a retry.** There is no inbox,
  or it is not being written; see [handled more than once](#a-message-is-handled-more-than-once).
- **`Message <id> could not be scheduled for a retry and will be redelivered`.**
  The publish to the retry exchange failed, so the original was put back in the
  queue instead of being lost. It resolves itself when the channel recovers;
  if it repeats, the retry exchange is probably gone or misnamed.

## The outbox does not empty

- **The dispatcher is not running**, or two schedules were expected and one is
  missing. Check the CronJob's last run.
- **`Error while publishing message <type> with id <id>: ...`** One record
  fails each run. When the reason is `channel closed`, look at the line just
  before it: `RabbitMQ publisher channel error: ... NOT_FOUND - no exchange '...'`
  means the row names an exchange that does not exist. List it in the
  producer's `exchanges`, or fix the destination.
- **`was published but could not be marked as sent`.** The store's `markAsSent`
  throws. The message does go out, again on every run; fix the store.
- **`The record names no exchange and the relay has no defaultExchange.`** Set
  `defaultExchange`, or store an exchange on the row.

## NestJS errors

- **`Nest can't resolve dependencies of the NestHandlerResolver`** or of
  `ConsumerWorker`, naming `LazyModuleLoader` or `ModuleRef`. Two copies of `@nestjs/core` are loaded, one by your
  application and one by the library. It happens when the library is installed
  as a symlink (`npm link`, or a `file:` dependency). Install it as a real
  package; for a `file:` dependency add `install-links=true` to `.npmrc`, as
  the [playground](../examples/demo/.npmrc) does.
- **`Nest can't resolve dependencies of the <Processor>`** at startup. The
  processor's module does not import what the processor needs. A lazily loaded
  module sees global modules only.
- **`Nest could not find ConsumerWorker element`** from `handle-messages`. The
  module given under `consumers` does not import `RabbitMqConsumerModule`. The
  same goes for `OutboxDispatcher` and `producers`.
- **`RabbitMqProducerModule needs an outboxStore to dispatch messages.`** Pass
  `outboxStore` in the producer options.

## Looking at the broker

The management console (port 15672 by default) answers most questions:

- **Queues**: "Ready" is what waits to be consumed, "Unacked" what is being
  handled right now, "Consumers" how many workers are attached.
- **A queue's Bindings**: what it asked its exchanges for.
- **A queue's Get messages**: read what is waiting. Choose "Nack message
  requeue true" so the message stays. Use it on the error queue to read
  `exception_details`.
- **Connections**: one per worker, named after `connectionName` if you set it.

The [playground](../examples/demo/README.md) reproduces most of the situations on
this page with one click each, which is the quickest way to learn what they
look like.

Next: [Migrating padmasana-service](10-migrating-padmasana-service.md).

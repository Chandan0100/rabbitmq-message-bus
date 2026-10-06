# 3. Consuming

- [Handlers](#handlers)
- [Idempotency](#idempotency)
- [Handlers and database transactions](#handlers-and-database-transactions)
- [Retries](#retries)
- [The error queue](#the-error-queue)
- [Messages the consumer ignores](#messages-the-consumer-ignores)
- [Throughput: prefetch and instances](#throughput-prefetch-and-instances)
- [Stopping](#stopping)
- [Message format](#message-format)

## Handlers

A handler is an object with two methods.

```ts
import { EventPayload, MessageHandler } from 'rabbitmq-message-bus';

interface InvoicePaid {
  invoiceId: string;
  amount: number;
}

class SendReceipt implements MessageHandler<InvoicePaid> {
  getHandlerName() {
    return 'SendReceipt';
  }

  async handle(message: EventPayload<InvoicePaid>) {
    // message.messageId, message.body, message.type, message.headers
  }
}
```

`getHandlerName()` identifies the handler in the inbox. It must be stable:
renaming a handler makes it look new, and messages it already handled would be
handled again if they were redelivered. Returning `this.constructor.name` is a
common choice; just remember the class name is then part of your data.

`handle()` succeeds by returning and fails by throwing.

Handlers are registered by message type. A type can have several handlers; they
run concurrently and independently.

```ts
const handlers = new HandlerRegistry({
  'billing.invoice_paid': [new SendReceipt(), new UpdateLedger()],
  'billing.invoice_voided': new ReverseLedgerEntry(),
});
```

A registration can also be a function or a promise that produces the handler,
which lets you build it on first use:

```ts
handlers.register('billing.invoice_paid', async () => new SendReceipt(await openMailer()));
```

By default the consumer resolves every handler when it starts, so one that
cannot be built stops the worker at boot instead of failing on its first
message. Set `preloadHandlers: false` to resolve on first use.

In NestJS, handlers are processor classes and are registered with
`lazyHandler(Module, Processor)`; see [NestJS](06-nestjs.md#handlers).

## Idempotency

RabbitMQ delivers a message at least once. It will deliver one twice when a
worker dies before acknowledging, when a connection drops mid-message, or when
the dispatcher published it twice. Without protection, the customer gets two
receipts.

Give the consumer an inbox:

```ts
interface InboxStore {
  isHandled(messageId: string, handlerName: string, scope?: string): Promise<boolean>;
  markHandled(messageId: string, handlerName: string, scope?: string): Promise<void>;
}
```

Back it with a table that has a unique constraint on the pair:

```sql
create table inbox_message (
  id          bigserial primary key,
  message_id  uuid not null,
  handler_name text not null,
  handled_at  timestamptz not null default now(),
  unique (message_id, handler_name)
);
```

```ts
class SqlInboxStore implements InboxStore {
  constructor(private readonly db: Database) {}

  async isHandled(messageId: string, handlerName: string) {
    const row = await this.db.one(
      'select 1 from inbox_message where message_id = $1 and handler_name = $2',
      [messageId, handlerName],
    );
    return row !== undefined;
  }

  async markHandled(messageId: string, handlerName: string) {
    await this.db.run(
      'insert into inbox_message (message_id, handler_name) values ($1, $2)',
      [messageId, handlerName],
    );
  }
}

new Consumer(connection, handlers, { endpoint, inbox: { store: new SqlInboxStore(db) } });
```

The inbox is per handler, not per message. When a message has two handlers and
one fails, the retry runs only the one that failed.

A working store over SQLite is in the playground:
[sqlite-inbox.store.ts](../examples/demo/src/infrastructure/sqlite-inbox.store.ts).
`InMemoryInboxStore` ships with the library for tests; it forgets everything on
restart.

### Who writes the inbox entry

| `inbox.record` | Who calls `markHandled` | Consequence |
| --- | --- | --- |
| `'pipeline'` (default) | The consumer, right after the handler returns | Simple. If the process dies between the handler and the write, the handler runs once more on redelivery |
| `'handler'` | Your handler | You write the entry in the same database transaction as the handler's own changes. Either both are saved or neither is, so the handler's effect happens exactly once |

Use `'handler'` whenever the handler writes to the same database as the inbox.
Use `'pipeline'` for handlers whose effect is elsewhere (an HTTP call, an
email) and make that effect safe to repeat if you can.

With `'handler'`, the consumer still checks `isHandled` before calling you; it
only leaves the writing to you. Do not use `'handler'` and then forget to
record: nothing would ever be de-duplicated.

## Handlers and database transactions

A handler that records its own inbox entry looks like this:

```ts
async handle(message: EventPayload<InvoicePaid>) {
  await this.db.transaction(async (tx) => {
    await tx.run('update invoice set status = $1 where id = $2', ['paid', message.body.invoiceId]);
    await tx.run(
      'insert into inbox_message (message_id, handler_name) values ($1, $2)',
      [message.messageId, this.getHandlerName()],
    );
  });
}
```

Some ORMs need something opened around the handler before it can start a
transaction: a request context, a unit of work, a session. Pass `runInContext`
and the consumer wraps the inbox check and all the handlers of each message in
it:

```ts
new Consumer(connection, handlers, {
  endpoint,
  inbox: { store, record: 'handler' },
  // MikroORM: gives each message its own EntityManager fork.
  runInContext: (work) => RequestContext.create(orm.em, work),
});
```

The function receives the work to do and must return its result. It is also the
place for a tracing span or a logging context.

## Retries

Two layers, both configured on the endpoint.

```ts
retry: {
  exchange: { name: 'events.recovery', type: 'direct' },
  immediateRetries: 5,
  delayedRetries: 3,
  backoff: linearBackoff(2000),
  queueMessageTtl: 10000,
}
```

**Immediate retries** call the failing handler again at once, in the same
delivery. They fix a deadlock, a dropped connection, a race.

**Delayed retries** park the message in the retry queue and bring it back
later. They fix a dependency that is down for a few seconds. Each trip has a
longer delay, set by `backoff`:

| Strategy | Delays for retries 1, 2, 3 |
| --- | --- |
| `linearBackoff(2000)` (default) | 2s, 4s, 6s |
| `fixedBackoff(5000)` | 5s, 5s, 5s |
| `exponentialBackoff(1000, 2)` | 1s, 2s, 4s |
| `(attempt) => attempt * attempt * 1000` | 1s, 4s, 9s |

Two limits apply to delays:

- `queueMessageTtl` is the longest a message can wait. A longer delay is cut to
  it, and the consumer warns about that at startup. This value becomes an
  argument of the retry queue, and RabbitMQ refuses to change the arguments of
  an existing queue: to change it later, delete the retry queue first (when it
  is empty) and let the consumer declare it again.
- RabbitMQ expires messages from the front of a queue. A message with a short
  delay waits behind one with a longer delay. With delays of a few seconds this
  costs little; do not build minute-long retries on this mechanism.

While a message waits in the retry queue, the consumer is free: it keeps
handling other messages.

### Failing without retries

When retrying cannot help, throw `UnrecoverableMessageError`. The message goes
to the error queue at once.

```ts
import { UnrecoverableMessageError } from 'rabbitmq-message-bus';

async handle(message: EventPayload<InvoicePaid>) {
  if (!message.body.invoiceId) {
    throw new UnrecoverableMessageError('invoice_paid without an invoiceId');
  }
  // ...
}
```

A message whose content cannot be read is treated the same way. If a message
has several handlers, it skips the retries only when all the failing ones were
unrecoverable.

## The error queue

A message that used up its retries is published to `error.exchange` with
`error.routingKey`, and only then acknowledged.

```ts
error: {
  exchange: { name: 'events.recovery', type: 'direct' },
  routingKey: 'billing.dead-letter',
  queue: 'billing.error',
}
```

Set `error.queue` and the consumer declares and binds the queue, so failed
messages always have somewhere to go. Leave it out only if another team owns
that queue. If nothing is bound, the broker has nowhere to put the message: the
consumer logs `was not routed to any queue ... and has been dropped`, which is
your cue to fix the binding.

The failed message keeps its original content and properties, plus two headers:

```jsonc
{
  "exception_details": [
    {
      "exception_type": "Mail server refused the connection", // the error message
      "stack_trace": "Error: Mail server refused the connection\n    at ...",
      "failed_at": "2026-10-06T11:03:37.000Z",
    },
  ],
  "endpoint": {
    "name": "billing.receipts", // the queue it failed in
    "delivery_metadata": {
      "message_type": "billing.invoice_paid",
      "exchange": "events.recovery", // publish here...
      "routing_key": "billing.receipts.retry", // ...with this key to replay
    },
  },
}
```

There is one `exception_details` entry per handler that failed on the last
attempt.

### Replaying

After fixing the cause, send the message back to the queue it failed in by
publishing it to `endpoint.delivery_metadata.exchange` with
`endpoint.delivery_metadata.routing_key`. Remove the retry headers so that it
starts fresh:

```ts
import { MessageHeaders, publishWithConfirm } from 'rabbitmq-message-bus';

const channel = await connection.createConfirmChannel();
const message = await channel.get('billing.error', { noAck: false });

if (message) {
  const headers = { ...message.properties.headers };
  const target = headers[MessageHeaders.ENDPOINT].delivery_metadata;
  delete headers[MessageHeaders.ENDPOINT];
  delete headers[MessageHeaders.EXCEPTION_DETAILS];
  delete headers[MessageHeaders.REDELIVERY_COUNT];
  delete headers[MessageHeaders.RETRY_ENDPOINT];

  await publishWithConfirm(channel, target.exchange, target.routing_key, message.content, {
    messageId: message.properties.messageId,
    type: message.properties.type,
    headers,
    persistent: true,
  });
  channel.ack(message); // only once the replay is confirmed
}
```

Handlers that had already succeeded are skipped on the replay, thanks to the
inbox. A complete version, which replays one message or all of them, is in the
playground: [broker-inspector.service.ts](../examples/demo/src/dashboard/broker-inspector.service.ts).

Watch the depth of the error queue. A message there is work that did not
happen.

## Messages the consumer ignores

These are acknowledged without running any handler, and logged at `info`:

| Log line | Why |
| --- | --- |
| `Message ignored: Message does not have a messageId.` | It could not be de-duplicated. Set `messageId` when publishing |
| `Message ignored: No available handler found or missing message type property.` | No `type`, or no handler for it. Normal on a topic or fanout exchange, where a queue receives more types than it handles |
| `Message ignored: Message is not intended for this service.` | It is a retry scheduled by an endpoint with a different `appName` |

## Throughput: prefetch and instances

`prefetch` is how many messages one consumer processes at the same time
(default 10). Their handlers run concurrently in one Node.js process, so raise
it for I/O-bound handlers and lower it if handlers compete for something
scarce, such as database connections.

To go further, start more instances of the same consumer. They share the queue
and the broker spreads messages between them. Nothing needs configuring: the
declarations are idempotent and the inbox keeps handling single.

## Stopping

`consumer.stop()` stops taking new messages, waits for those in flight (up to
`shutdownTimeoutMs`, 30 seconds by default), then closes its channel. Anything
still running after the timeout is redelivered by the broker later.

Call it on SIGTERM. The worker command does this for you; see
[Workers and deployment](07-workers-and-deployment.md#graceful-shutdown) for
making sure the signal reaches the process.

## Message format

The consumer reads:

- `messageId` (property): required;
- `type` (property), falling back to a `type` header: required;
- the content as JSON.

Malformed JSON is repaired when possible (trailing commas, single quotes,
truncated documents). Pass `serializer: new JsonMessageSerializer({ repair: false })`
to reject it instead, or your own `MessageSerializer` for another format.

The retry flow uses two headers of its own, `redelivery_count` and
`retry_endpoint`. Do not set them when publishing.

Next: [Publishing](04-publishing.md).

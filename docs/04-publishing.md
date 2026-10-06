# 4. Publishing

- [Why an outbox](#why-an-outbox)
- [Step 1: the outbox table](#step-1-the-outbox-table)
- [Step 2: record messages with the business change](#step-2-record-messages-with-the-business-change)
- [Step 3: the outbox store](#step-3-the-outbox-store)
- [Step 4: dispatch](#step-4-dispatch)
- [Where a message type goes](#where-a-message-type-goes)
- [Publishing without an outbox](#publishing-without-an-outbox)

## Why an outbox

Suppose a request saves an invoice and then publishes `invoice_paid`. If the
process dies between the two, the invoice is saved and nobody is told. Publish
first instead, and a rollback leaves a message about something that never
happened.

The outbox removes the gap. The message is written to a table in the same
transaction as the invoice. A dispatcher, running separately, publishes what is
in the table. The cost is a short delay, however often the dispatcher runs.

## Step 1: the outbox table

Any shape works as long as you can list pending rows in order and mark one as
sent. For example:

```sql
create table outbox_message (
  id          bigserial primary key,
  message_id  uuid not null unique,
  type        text not null,
  exchange    text not null,
  routing_key text not null,
  properties  jsonb not null,   -- AMQP properties and headers
  body        jsonb not null,
  status      text not null default 'PENDING',
  created_at  timestamptz not null default now(),
  sent_at     timestamptz
);
```

## Step 2: record messages with the business change

This is your code; the library is not involved yet. The one rule: the insert
into the outbox happens in the same transaction as the change it describes.

```ts
await db.transaction(async (tx) => {
  await tx.run('update invoice set status = $1 where id = $2', ['paid', invoiceId]);

  const messageId = randomUUID();
  const type = 'billing.invoice_paid';
  const { exchange, routingKey } = destinations.resolve(type); // see below

  await tx.run(
    `insert into outbox_message (message_id, type, exchange, routing_key, properties, body)
     values ($1, $2, $3, $4, $5, $6)`,
    [
      messageId,
      type,
      exchange,
      routingKey,
      { messageId, type, appId: 'billing', contentType: 'application/json', headers: { type } },
      { invoiceId, paidAt: new Date().toISOString() },
    ],
  );
});
```

Give every message a `messageId` and a `type` in its properties. Consumers need
both.

## Step 3: the outbox store

Tell the library how to read and update your table:

```ts
import { OutboxRecord, OutboxStore } from 'rabbitmq-message-bus';

class SqlOutboxStore implements OutboxStore {
  constructor(private readonly db: Database) {}

  async fetchPending(limit: number): Promise<OutboxRecord[]> {
    const rows = await this.db.all(
      "select * from outbox_message where status = 'PENDING' order by id limit $1",
      [limit],
    );
    return rows.map((row) => ({
      messageId: row.message_id,
      type: row.type,
      exchange: row.exchange,
      routingKey: row.routing_key,
      body: row.body,
      properties: row.properties,
    }));
  }

  async markAsSent(record: OutboxRecord): Promise<void> {
    await this.db.run(
      "update outbox_message set status = 'SENT', sent_at = now() where message_id = $1",
      [record.messageId],
    );
  }
}
```

`markAsSent` receives the very object `fetchPending` returned. If your store
works with entities, return objects that carry the entity and update it there:

```ts
type Pending = OutboxRecord & { entity: OutboxMessage };

class OrmOutboxStore implements OutboxStore<Pending> {
  async fetchPending(limit: number): Promise<Pending[]> { /* ... */ }

  async markAsSent({ entity }: Pending) {
    entity.markAsSent();
    await this.repository.save(entity);
  }
}
```

Both methods receive an optional `scope`, the value of `--schema` on the worker
command line, for stores that serve several schemas or tenants.

A working store is in the playground:
[sqlite-outbox.store.ts](../examples/demo/src/infrastructure/sqlite-outbox.store.ts).

## Step 4: dispatch

```ts
import { ConnectionManager, OutboxRelay, Publisher } from 'rabbitmq-message-bus';

const connection = new ConnectionManager({ url: process.env.RABBITMQ_DSN! });
const publisher = new Publisher(connection, {
  exchanges: [{ name: 'events', type: 'topic' }], // declared before the first publish
});
const relay = new OutboxRelay(new SqlOutboxStore(db), publisher, {
  defaultExchange: 'events', // for records that name no exchange
  batchSize: 10,
});

const result = await relay.dispatch(); // { fetched: 3, published: 3, failed: 0 }

await publisher.close();
await connection.close();
```

Run this on a schedule. In NestJS the `dispatch-messages` command does exactly
this; see [Workers and deployment](07-workers-and-deployment.md).

What one run does:

1. Reads up to `batchSize` pending records (or `dispatch({ limit })`).
2. Publishes them one by one, in order, each as a persistent message, and waits
   for the broker to confirm each one.
3. Marks a record as sent once it is confirmed.

What happens when something fails:

| Failure | Result |
| --- | --- |
| The broker rejects or does not confirm one message | That record stays pending and is counted in `failed`; the run continues with the next |
| The message is confirmed but `markAsSent` fails | Counted in `failed`. The next run publishes it again, and consumers de-duplicate it by `messageId` |
| The broker cannot be reached | `dispatch` rejects with `ConnectionFailedError`. Nothing is marked; the next run retries |

**Never run two dispatchers on the same outbox at the same time.** Both would
read the same pending rows and publish them twice. A Kubernetes CronJob with
`concurrencyPolicy: Forbid` guarantees this; the manifest generator sets it.

If the store needs something opened around it, such as an ORM request context,
pass `runInContext`; it wraps the whole run.

## Where a message type goes

`DestinationRegistry` answers "which exchange and routing key for this type".
You consult it when writing the outbox row, as in step 2.

```ts
import { DestinationRegistry, routeByMessageType } from 'rabbitmq-message-bus';

const destinations = new DestinationRegistry()
  .register('billing.invoice_paid', { exchange: 'events', routingKey: 'billing.invoices.paid' })
  .register('billing.invoice_voided', { exchange: 'events', routingKey: 'billing.invoices.voided' });

destinations.resolve('billing.invoice_paid');
// { exchange: 'events', routingKey: 'billing.invoices.paid' }
```

`resolve` throws for a type it does not know, which catches a forgotten
registration when the row is written rather than when nobody receives the
message. `get` returns `undefined` instead.

If every message goes to one exchange with its type as routing key, skip the
list:

```ts
const destinations = new DestinationRegistry({ fallback: routeByMessageType('events') });

destinations.resolve('billing.invoice_paid');
// { exchange: 'events', routingKey: 'billing.invoice_paid' }
```

That convention suits a direct exchange, and works with a topic exchange too;
see [Exchange types](05-exchange-types.md).

## Publishing without an outbox

For messages that are not tied to a database change, use `Publisher` directly.

```ts
await publisher.publish({
  exchange: 'events',
  routingKey: 'billing.report_requested',
  body: { month: '2026-10' },
  properties: { messageId: randomUUID(), type: 'billing.report_requested' },
});
```

`publish` resolves when the broker has confirmed the message and rejects with
`PublishError` otherwise, including when the exchange does not exist. Messages
are persistent by default. The connection opens on the first publish, and a
channel the broker closed is replaced on the next one.

`body` is serialized as JSON; pass a `Buffer` to send bytes as they are.

Next: [Exchange types](05-exchange-types.md).

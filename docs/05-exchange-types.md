# 5. Exchange types

The exchange type decides one thing: how a published message finds its queues.
Handlers, retries, the error queue and the outbox work the same under all
three. Switching type means changing the bindings on the consuming side and the
routing keys on the publishing side, nothing else.

| | Topic | Direct | Fanout |
| --- | --- | --- | --- |
| A queue asks for | Patterns | Exact keys | Everything |
| Typical routing key | `billing.invoices.paid` | The message type | Unused |
| A queue receives types it has no handler for | Often | No | Always |
| Adding a message type | Usually no binding change | One new binding per interested queue, added automatically with `bindHandledTypes` | No change |
| Choose it when | Consumers subscribe to families of events | You want each queue to receive exactly what it handles | Every consumer wants every message |

The [playground](../examples/demo/README.md) switches between the three while it
runs; watch the binding keys change in its diagram.

## Topic

```ts
// consumer
endpoint: {
  queue: 'billing.receipts',
  exchange: { name: 'events', type: 'topic' },
  bindingKeys: ['billing.invoices.*', 'orders.#'],
  // ...
}

// publisher
destinations.register('billing.invoice_paid', {
  exchange: 'events',
  routingKey: 'billing.invoices.paid',
});
```

In a pattern, `*` stands for exactly one word and `#` for zero or more. Words
are separated by dots.

A queue bound with a wide pattern receives message types it has no handler for.
That is fine: the consumer acknowledges and ignores them. It does mean those
messages travel to the queue for nothing, so prefer the narrowest pattern that
covers what you handle.

## Direct

A direct exchange delivers a message to the queues bound with exactly its
routing key. The convention that needs the least configuration is to use the
message type as the routing key on both sides.

```ts
// consumer: one binding per message type that has a handler
endpoint: {
  queue: 'billing.receipts',
  exchange: { name: 'events.direct', type: 'direct' },
  bindHandledTypes: true,
  // ...
}

// publisher: every type is published with its own name as the key
const destinations = new DestinationRegistry({
  fallback: routeByMessageType('events.direct'),
});
```

With `bindHandledTypes`, registering a handler for a new message type is all it
takes for the queue to start receiving it; the binding is added at the next
start.

You can also list keys yourself, or do both:

```ts
bindingKeys: ['billing.high-priority'],
bindHandledTypes: true,
```

Things to know:

- **No patterns.** `billing.#` on a direct exchange matches only a message
  published with the literal key `billing.#`. The consumer logs a warning when
  it sees `*` or `#` in a key of a direct exchange.
- **A message nobody is bound for disappears.** That is how exchanges work, but
  it is more visible here: publish a type before any consumer has a handler for
  it and it is gone.
- **Bindings are never removed.** If you delete a handler, the queue keeps the
  binding for its type and the consumer ignores those messages. Remove the
  binding in the management console when it matters.

## Fanout

```ts
endpoint: {
  queue: 'billing.audit',
  exchange: { name: 'events.fanout', type: 'fanout' },
  // no binding keys
  // ...
}
```

Every queue bound to the exchange gets every message. Binding keys and routing
keys are ignored. Each consumer handles the types it knows and ignores the
rest.

## The retry and error exchanges

These are separate from the primary exchange and have their own rules.

**The retry exchange must be `direct`.** A failed message is published to it
with a key that leads to the retry queue, and comes back with a key that leads
to its own primary queue. Both are exact matches on purpose: a retried message
must not reach any other consumer. The library refuses any other type at
startup.

One direct exchange can serve the retries of every consumer in a service, or in
a whole company; each consumer uses keys derived from its own queue name:

| Key | Default | Leads to |
| --- | --- | --- |
| `retry.queueBindingKey` | `<queue>.events-retry` | The retry queue |
| `retry.returnBindingKey` | `<queue>.retry` | Back to the primary queue |

**The error exchange can be any type**, and is usually the same direct exchange
as the retries.

**With a direct primary exchange, one exchange can do everything:**

```ts
const exchange = { name: 'events.direct', type: 'direct' } as const;

endpoint: {
  queue: 'billing.receipts',
  exchange,
  bindHandledTypes: true,
  retry: { exchange },
  error: { exchange, routingKey: 'billing.dead-letter', queue: 'billing.error' },
}
```

The library checks at startup that the keys cannot collide: that no primary
binding key equals the retry queue's key, that the two retry keys differ, and
that the error routing key is neither of them.

## Changing the type of a running system

An exchange cannot change type; RabbitMQ rejects the declaration. Changing type
therefore means a new exchange with a new name.

1. Start consumers bound to the new exchange. Give them **the same queue**: a
   queue can be bound to several exchanges, so it now receives from both.
2. Switch the publishers to the new exchange and routing keys.
3. When the old exchange has gone quiet, remove the queue's bindings to it, then
   delete it.

Messages published during the switch may arrive through either exchange.
Nothing is handled twice, because the inbox goes by `messageId`.

The worker command can do step 1 without a new build:

```bash
handle-messages --module billing \
  --primary-queue-exchange events.direct \
  --primary-queue-exchange-type direct \
  --primary-queue-binding-key billing.invoice_paid \
  --primary-queue-binding-key billing.invoice_voided
```

Next: [NestJS](06-nestjs.md).

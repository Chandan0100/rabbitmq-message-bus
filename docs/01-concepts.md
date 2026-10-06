# 1. Concepts

## What the library is for

Two services, or two parts of one service, exchange messages through RabbitMQ.
Things go wrong along the way: the broker restarts, a handler throws, a pod is
killed mid-message, the same message arrives twice. This library handles those
cases so that your code only has to say what to publish and what to do with a
message.

It rests on two patterns.

**Outbox.** The publishing side does not talk to RabbitMQ when the business
change happens. It stores the message in an outbox table, in the same database
transaction as the change. A separate dispatcher publishes the outbox later.
The change and its message are therefore saved together or not at all.

**Inbox.** The consuming side records which handler has processed which message.
A message that arrives twice is recognised and skipped.

## The moving parts

```
 your code ──► outbox table ──► dispatcher ──► exchange ──► queue ──► consumer ──► handlers
 (one transaction)             (OutboxRelay)                         (Consumer)   (your code + inbox)
```

| Part | What it is | Runs as |
| --- | --- | --- |
| Outbox store | Your table of pending messages, behind the `OutboxStore` interface | Part of your service |
| Dispatcher | `OutboxRelay`: publishes pending messages and marks them as sent | A scheduled job (`dispatch-messages`) |
| Consumer | `Consumer`: receives messages and runs their handlers | A long-running worker (`handle-messages`) |
| Handler | Your code for one message type | Inside the consumer |
| Inbox store | Your table of handled messages, behind the `InboxStore` interface | Part of your service |

## What a consumer declares on the broker

A consumer is described by an **endpoint**: one primary queue plus what it needs
to retry and to fail safely.

```
primary exchange --bindingKeys------------> primary queue --> handlers
                                              ^        |
                                              |        | handlers failed
retry exchange   --returnBindingKey-----------+        v
retry exchange   --queueBindingKey--------> retry queue (waits, then the broker
                                              sends it back with returnBindingKey)

error exchange   --routingKey-------------> error queue
```

- The **primary exchange** is where publishers send. It can be `topic`,
  `direct` or `fanout`.
- The **retry queue** is a waiting room. A failed message is parked there with
  an expiry; when it expires, the broker routes it back to the primary queue.
  Nothing consumes the retry queue.
- The **retry exchange** is always `direct`: a retried message must come back to
  exactly one queue, the one it failed in.
- The **error queue** collects messages that could not be processed after every
  retry. Several consumers usually share one.

The consumer declares all of this when it starts. Declaring is idempotent, so
starting a second instance, or restarting, changes nothing.

## The life of one message

1. **Received.** The consumer takes the message from the primary queue.
2. **Filtered.** It is acknowledged and ignored if it has no `messageId`, if no
   handler is registered for its type, or if it is a retry scheduled by a
   different endpoint.
3. **Handled.** Every handler registered for the type runs, unless the inbox
   says that handler already processed this message.
4. **Immediate retries.** A handler that throws is called again straight away,
   up to `immediateRetries` more times. This absorbs glitches.
5. **Delayed retries.** If a handler still fails, a copy of the message goes to
   the retry queue with a delay, and the original is acknowledged. The copy
   returns later and steps 2 to 4 repeat. This happens up to `delayedRetries`
   times, each with a longer delay. This absorbs outages.
6. **Error queue.** After the last delayed retry the message is published to the
   error exchange, with headers saying what failed and where to send it back.

With the defaults (5 immediate retries, 3 delayed retries), a handler that
always fails is called 24 times over about 12 seconds before the message is
given up on.

## What the library guarantees

**A message is not lost between the queue and the handler.** The consumer
acknowledges a message only after its handlers succeeded, or after the broker
confirmed the copy in the retry or error queue. If neither happens, for example
because the process dies, the broker delivers the message again.

**A message may be delivered more than once.** That follows from the point
above, and from the dispatcher, which can publish a message twice if it stops
between publishing and marking as sent. This is why handlers need the inbox:
see [Consuming](03-consuming.md#idempotency).

**A published message reached the broker.** The dispatcher marks an outbox row
as sent only after the broker confirmed it.

**Order is not guaranteed.** The dispatcher publishes in outbox order, but
retries, several consumer instances and `prefetch` above 1 all reorder
messages. Do not write handlers that depend on order.

## What it does when the broker goes away

- The connection is retried at a fixed interval, a limited number of times.
- After a reconnect, the consumer creates a new channel and subscribes again.
- Messages that were in flight are delivered again by the broker.
- If the broker stays unreachable, the library reports it (`failed` event) and
  the worker command exits with a non-zero code, so that its supervisor
  restarts it. The library never ends the process itself.

## Vocabulary

| Term | Meaning |
| --- | --- |
| Message type | A string naming what happened, such as `billing.invoice_paid`. Carried in the `type` property |
| Handler, processor | The code that handles one message type. "Processor" is the usual name for a handler class in a NestJS service |
| Endpoint | One consumer: its queue, bindings, retry settings and error routing |
| Binding key | What a queue asks its exchange for |
| Routing key | What a message is published with |
| Scope | An opaque value passed from the worker command line to your stores, such as a database schema or tenant |

Next: [Getting started](02-getting-started.md).

---
layout: home
title: Reliable RabbitMQ messaging for Node.js
titleTemplate: rabbitmq-message-bus

hero:
  name: rabbitmq-message-bus
  text: Messages that survive failure
  tagline: Consumers with retries and an error queue, an outbox publisher, and worker commands for Node.js and NestJS. Works with topic, direct and fanout exchanges.
  image:
    src: /logo.svg
    alt: rabbitmq-message-bus
  actions:
    - theme: brand
      text: Get started
      link: /guide/getting-started
    - theme: alt
      text: How it works
      link: /guide/concepts
    - theme: alt
      text: Try the playground
      link: /playground

features:
  - icon: 📥
    title: Consumers that do not lose messages
    details: A failing handler is retried at once, then again after a delay, and ends in an error queue with the reason attached. A message is acknowledged only when it is safe to.
    link: /guide/consuming
    linkText: Consuming
  - icon: 📤
    title: Publishing through an outbox
    details: Save the change and its event in one transaction. A relay publishes pending events, waits for the broker to confirm, and marks them sent.
    link: /guide/publishing
    linkText: Publishing
  - icon: 🔁
    title: Handled once
    details: An inbox keyed by message and handler skips what has already been done, so a redelivery or a retry does not send the email twice.
    link: /guide/consuming#idempotency
    linkText: The inbox
  - icon: 🔀
    title: Topic, direct or fanout
    details: The exchange type is configuration. Handlers, retries and the error queue behave the same on all three.
    link: /guide/exchange-types
    linkText: Exchange types
  - icon: 🧩
    title: NestJS modules
    details: Register a consumer or a producer as a module, load processors lazily, and keep each bounded context in its own worker.
    link: /guide/nestjs
    linkText: NestJS
  - icon: 🚀
    title: Workers and Kubernetes
    details: Two commands, handle-messages and dispatch-messages, and a generator for their Deployment and CronJob manifests.
    link: /guide/workers-and-deployment
    linkText: Workers and deployment
---

<div class="home-section vp-doc">

## A consumer in thirty lines

```ts
import { ConnectionManager, Consumer, HandlerRegistry, InMemoryInboxStore } from 'rabbitmq-message-bus';

const connection = new ConnectionManager({ url: 'amqp://localhost:5672/' });

const consumer = new Consumer(
  connection,
  new HandlerRegistry({
    'billing.invoice_paid': {
      getHandlerName: () => 'SendReceipt',
      handle: async ({ messageId, body }) => {
        console.log(`Receipt for invoice ${body.invoiceId} (message ${messageId})`);
      },
    },
  }),
  {
    endpoint: {
      appName: 'billing',
      queue: 'billing.receipts',
      exchange: { name: 'events', type: 'topic' },
      bindingKeys: 'billing.#',
      retry: { exchange: { name: 'events.recovery', type: 'direct' } },
      error: {
        exchange: { name: 'events.recovery', type: 'direct' },
        routingKey: 'billing.dead-letter',
        queue: 'billing.error',
      },
    },
    inbox: { store: new InMemoryInboxStore() }, // a database table in production
  },
);

connection.on('failed', () => process.exit(1));
consumer.start().catch(() => process.exit(1));
```

That declares the queue, its retry queue and the error queue. A failing handler
is retried five times at once and three more times after 2, 4 and 6 seconds. A
message that still fails moves to the error queue with the reason attached. A
message already handled is skipped, and the consumer reconnects if the broker
goes away.

## Three entry points

| Import | Contents | Needs |
| --- | --- | --- |
| `rabbitmq-message-bus` | Connection, consumer, publisher, outbox relay, store interfaces | Node.js 20+ |
| `rabbitmq-message-bus/nestjs` | Modules, lazy processor loading, worker commands (re-exports the core) | `@nestjs/common`, `@nestjs/core` 10, 11 or 12 |
| `rabbitmq-message-bus/k8s` | Worker manifest generator, `rabbitmq-bus-k8s` binary | |

## See it before you write any code

The [playground](/playground) is a small task service with a live dashboard.
Publish events, make processors fail, cut the connection, and watch the queues,
the retries and the error queue react.

<a href="./playground"><img class="screenshot light" src="/playground-light.png" alt="The playground dashboard"><img class="screenshot dark" src="/playground-dark.png" alt="The playground dashboard"></a>

</div>

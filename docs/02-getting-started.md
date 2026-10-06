# 2. Getting started

## Install

```bash
npm install rabbitmq-message-bus
```

Node.js 20 or later. For the NestJS integration, `@nestjs/common` and
`@nestjs/core` version 10, 11 or 12 must already be in your project.

You need a RabbitMQ broker. For local work:

```bash
docker run -d --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:4.1-management-alpine
```

The management console is then at <http://localhost:15672> (guest / guest).

## One message, without a framework

Two files: a consumer that stays running, and a script that publishes.

```ts
// consumer.ts
import { ConnectionManager, Consumer, HandlerRegistry } from 'rabbitmq-message-bus';

const connection = new ConnectionManager({ url: 'amqp://localhost:5672/' });

const handlers = new HandlerRegistry({
  'billing.invoice_paid': {
    getHandlerName: () => 'SendReceipt',
    handle: async ({ messageId, body }) => {
      console.log(`Receipt for invoice ${body.invoiceId} (message ${messageId})`);
    },
  },
});

const consumer = new Consumer(connection, handlers, {
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
});

// The library reports a broker it cannot reach; ending the process is your call.
connection.on('failed', () => process.exit(1));
consumer.on('failed', () => process.exit(1));

process.on('SIGTERM', async () => {
  await consumer.stop(); // finishes the messages in flight
  await connection.close();
});

consumer.start().catch((error) => {
  console.error(error);
  process.exit(1);
});
```

```ts
// publish.ts
import { randomUUID } from 'node:crypto';
import { ConnectionManager, Publisher } from 'rabbitmq-message-bus';

const connection = new ConnectionManager({ url: 'amqp://localhost:5672/' });
const publisher = new Publisher(connection, {
  exchanges: [{ name: 'events', type: 'topic' }],
});

async function main() {
  await publisher.publish({
    exchange: 'events',
    routingKey: 'billing.invoice_paid',
    body: { invoiceId: 'inv-1' },
    properties: { messageId: randomUUID(), type: 'billing.invoice_paid' },
  });

  await publisher.close();
  await connection.close();
}

void main();
```

Run the consumer, then the publisher. The consumer logs the receipt.

Three things in this example are required for a message to be handled:

- a **`messageId`**: without one the consumer acknowledges and ignores the
  message, because it could not be de-duplicated;
- a **`type`** (the `type` property, or a `type` header) that has a handler;
- a **routing key** that matches one of the queue's binding keys.

Open the management console and look at what the consumer declared: the
`events` and `events.recovery` exchanges, and the `billing.receipts`,
`billing.receipts.events-retry` and `billing.error` queues.

Now make the handler throw and publish again. You will see it retried in the
log, the message wait in `billing.receipts.events-retry` three times, and then
appear in `billing.error`.

This example has no inbox, so a redelivered message would be handled twice. Add
one before going further: [Consuming, idempotency](03-consuming.md#idempotency).

## One message, with NestJS

A processor, a consumer module that registers it, and one file for the worker
commands.

```ts
// send-receipt.processor.ts
import { Injectable, Module } from '@nestjs/common';
import { EventPayload, MessageHandler } from 'rabbitmq-message-bus/nestjs';

@Injectable()
export class SendReceiptProcessor implements MessageHandler<{ invoiceId: string }> {
  getHandlerName() {
    return this.constructor.name;
  }

  async handle(message: EventPayload<{ invoiceId: string }>) {
    console.log(`Receipt for invoice ${message.body.invoiceId}`);
  }
}

@Module({ providers: [SendReceiptProcessor] })
export class SendReceiptProcessorModule {}
```

```ts
// billing.consumer.module.ts
import { Module } from '@nestjs/common';
import {
  connectionOptionsFromEnv,
  InMemoryInboxStore,
  lazyHandler,
  RabbitMqConsumerModule,
} from 'rabbitmq-message-bus/nestjs';
import { SendReceiptProcessor, SendReceiptProcessorModule } from './send-receipt.processor';

@Module({
  imports: [
    RabbitMqConsumerModule.register({
      connection: connectionOptionsFromEnv(), // RABBITMQ_DSN, RABBITMQ_USERNAME, RABBITMQ_PASSWORD
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
      handlers: {
        'billing.invoice_paid': lazyHandler(SendReceiptProcessorModule, SendReceiptProcessor),
      },
      // Replace with a database-backed store before production.
      inbox: { store: new InMemoryInboxStore() },
    }),
  ],
})
export class BillingConsumerModule {}
```

```ts
// message-bus-cli.ts
import 'reflect-metadata';
import { runMessageBusCli } from 'rabbitmq-message-bus/nestjs';
import { BillingConsumerModule } from './billing.consumer.module';

void runMessageBusCli({
  consumers: { billing: BillingConsumerModule },
});
```

```bash
RABBITMQ_DSN=amqp://localhost:5672/ node dist/message-bus-cli.js handle-messages --module billing
```

Publish from the plain `publish.ts` above, or add a producer module as shown in
[NestJS](06-nestjs.md).

## Where to go next

- [Consuming](03-consuming.md): a real inbox store, retries, the error queue.
- [Publishing](04-publishing.md): the outbox.
- [NestJS](06-nestjs.md): configuration from `ConfigService`, several consumers.
- The [playground](../examples/demo/README.md) is this guide grown into a service
  with three bounded contexts.

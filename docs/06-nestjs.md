# 6. NestJS

Everything here is imported from `rabbitmq-message-bus/nestjs`, which also
re-exports the core.

- [The shape of a service](#the-shape-of-a-service)
- [A consumer module](#a-consumer-module)
- [Handlers](#handlers)
- [A producer module](#a-producer-module)
- [Destinations](#destinations)
- [Several consumers in one application](#several-consumers-in-one-application)
- [Running a consumer inside the API process](#running-a-consumer-inside-the-api-process)
- [Logging](#logging)
- [Testing](#testing)

## The shape of a service

A service that both publishes and consumes ends up with these pieces:

```
src/
  billing/
    billing.service.ts              business code: writes the change and the outbox row
    billing.producer.module.ts      RabbitMqProducerModule      -> dispatch-messages --module billing
  notifications/
    processors/
      send-receipt.processor.ts     a processor and its module
    notifications.handlers.ts       message type -> processors
    notifications.consumer.module.ts  RabbitMqConsumerModule    -> handle-messages --module notifications
  message-bus-cli.ts                the worker commands
```

The API process, the dispatcher and each consumer are separate processes built
from the same code. Each worker boots only its own module.

The [playground](../examples/demo/README.md) is laid out exactly like this.

## A consumer module

```ts
import { Module } from '@nestjs/common';
import { ConfigModule, ConfigService } from '@nestjs/config';
import { RabbitMqConsumerModule } from 'rabbitmq-message-bus/nestjs';
import { NotificationsInboxStore } from './notifications.inbox-store';
import { notificationHandlers } from './notifications.handlers';

@Module({
  imports: [
    RabbitMqConsumerModule.registerAsync({
      imports: [ConfigModule],
      inject: [ConfigService, NotificationsInboxStore],
      extraProviders: [NotificationsInboxStore],
      useFactory: (config: ConfigService, store: NotificationsInboxStore) => ({
        connection: {
          url: config.getOrThrow('RABBITMQ_DSN'),
          username: config.get('RABBITMQ_USERNAME'),
          password: config.get('RABBITMQ_PASSWORD'),
          connectionName: 'notifications-consumer',
        },
        endpoint: {
          appName: config.getOrThrow('APP_NAME'),
          queue: config.getOrThrow('NOTIFICATIONS_QUEUE'),
          exchange: { name: config.getOrThrow('EVENTS_EXCHANGE'), type: 'topic' },
          bindingKeys: config.getOrThrow('NOTIFICATIONS_BINDING_KEY'),
          retry: { exchange: { name: config.getOrThrow('RECOVERY_EXCHANGE'), type: 'direct' } },
          error: {
            exchange: { name: config.getOrThrow('RECOVERY_EXCHANGE'), type: 'direct' },
            routingKey: config.getOrThrow('ERROR_ROUTING_KEY'),
          },
        },
        handlers: notificationHandlers,
        inbox: { store, record: 'handler' },
      }),
    }),
  ],
})
export class NotificationsConsumerModule {}
```

`registerAsync` takes:

| Option | |
| --- | --- |
| `useFactory` | Returns the options: `connection`, `endpoint`, `handlers`, and anything from the [consumer options](08-configuration-reference.md#consumer) (`inbox`, `runInContext`, `logger`...) |
| `inject` | What the factory receives |
| `imports` | Modules that provide what is injected |
| `extraProviders` | Providers created inside this module, typically the inbox store |

`register(options)` is the same without a factory, for when the options are
known up front.

The module exports a `ConsumerWorker`. The `handle-messages` command starts it;
you rarely touch it directly. It stops the consumer when the application shuts
down.

Every call to `register` or `registerAsync` creates one consumer of its own.
Create the dynamic module once and import that, rather than calling `register`
in several places.

## Handlers

A handler is an injectable class, usually called a processor, with its own
module so that it brings its dependencies with it.

```ts
@Injectable()
export class SendReceiptProcessor implements MessageHandler<InvoicePaid> {
  constructor(
    private readonly mailer: MailerService,
    private readonly inbox: NotificationsInboxStore,
  ) {}

  getHandlerName() {
    return this.constructor.name;
  }

  async handle(message: EventPayload<InvoicePaid>) {
    await this.mailer.sendReceipt(message.body.invoiceId);
    // With inbox.record 'handler', the processor writes its own inbox entry,
    // ideally in the same transaction as its database changes.
    await this.inbox.markHandled(message.messageId, this.getHandlerName());
  }
}

@Module({
  imports: [MailerModule],
  providers: [SendReceiptProcessor, NotificationsInboxStore],
})
export class SendReceiptProcessorModule {}
```

The handler map says which processors handle which message type:

```ts
import { lazyHandler, NestHandlerMap } from 'rabbitmq-message-bus/nestjs';

export const notificationHandlers: NestHandlerMap = {
  'billing.invoice_paid': [
    lazyHandler(SendReceiptProcessorModule, SendReceiptProcessor),
    lazyHandler(UpdateDashboardProcessorModule, UpdateDashboardProcessor),
  ],
  'billing.invoice_voided': lazyHandler(SendVoidNoticeProcessorModule, SendVoidNoticeProcessor),
};
```

Three ways to name a handler:

| Form | How it is found |
| --- | --- |
| `lazyHandler(Module, Processor)` | The module is loaded on demand with NestJS's lazy loader, and the processor taken from it. Nothing else needs to import the module |
| `Processor` (the class) | Looked up among the modules the application already loaded. Import its module yourself |
| a handler instance | Used as it is |

Handlers are resolved when the consumer starts. If a processor's dependencies
cannot be satisfied, the worker fails at boot with the NestJS error, instead of
on the first message. Set `preloadHandlers: false` to resolve on first use.

A lazily loaded module can use providers of the application's global modules.
Anything else it needs, it must import itself.

## A producer module

```ts
@Module({
  imports: [
    RabbitMqProducerModule.registerAsync({
      imports: [ConfigModule],
      inject: [ConfigService, BillingOutboxStore],
      extraProviders: [BillingOutboxStore],
      useFactory: (config: ConfigService, outboxStore: BillingOutboxStore) => ({
        connection: { url: config.getOrThrow('RABBITMQ_DSN'), connectionName: 'billing-dispatcher' },
        exchanges: [{ name: config.getOrThrow('EVENTS_EXCHANGE'), type: 'topic' }],
        outboxStore,
        batchSize: 10,
      }),
    }),
  ],
})
export class BillingProducerModule {}
```

It exports:

- `OutboxDispatcher`: `dispatch({ limit, scope })` runs one batch of the outbox.
  The `dispatch-messages` command calls it.
- `Publisher`: for publishing without an outbox. `outboxStore` is not needed if
  this is all you use.

The connection is opened by the first publish, so importing this module into
the API process costs nothing until it is used.

## Destinations

One registry for the whole application, filled by each bounded context:

```ts
@Module({
  imports: [
    MessageDestinationModule.register({
      destinations: {
        'billing.invoice_paid': { exchange: 'events', routingKey: 'billing.invoices.paid' },
        'billing.invoice_voided': { exchange: 'events', routingKey: 'billing.invoices.voided' },
      },
    }),
  ],
  providers: [BillingService],
})
export class BillingModule {}
```

```ts
@Injectable()
export class BillingService {
  constructor(private readonly destinations: DestinationRegistry) {}

  private outboxRow(type: string) {
    const { exchange, routingKey } = this.destinations.resolve(type);
    // ...
  }
}
```

`DestinationRegistry` is injectable everywhere once any module has called
`MessageDestinationModule.register`. Pass `fallback: routeByMessageType('events')`
instead of, or as well as, the list.

## Several consumers in one application

Each consumer module has its own `ConsumerWorker`. When one application hosts
several, give each a token so they can be told apart:

```ts
export const NOTIFICATIONS_WORKER = Symbol('NOTIFICATIONS_WORKER');

@Module({
  imports: [RabbitMqConsumerModule.registerAsync({ /* ... */ })],
  providers: [{ provide: NOTIFICATIONS_WORKER, useExisting: ConsumerWorker }],
  exports: [NOTIFICATIONS_WORKER],
})
export class NotificationsConsumerModule {}
```

```ts
constructor(
  @Inject(NOTIFICATIONS_WORKER) private readonly notifications: ConsumerWorker,
  @Inject(AUDIT_WORKER) private readonly audit: ConsumerWorker,
) {}
```

The worker commands do not need this: each boots a single consumer module.

## Running a consumer inside the API process

For a small service, or in development, a consumer can run inside the
application instead of as a separate worker:

```ts
RabbitMqConsumerModule.register({
  // ...
  autoStart: true,
});
```

It starts when the application has booted and stops with it. Call
`app.enableShutdownHooks()` so that SIGTERM lets it finish its messages.

Or control it yourself:

```ts
await worker.start();
worker.isConsuming(); // true
await worker.stop(); // finishes in-flight messages, closes the connection
worker.on('failed', (error) => { /* the broker stayed unreachable */ });
```

`start()` accepts overrides for the configured endpoint, which is how one
module can serve several queues:

```ts
await worker.start({ endpoint: { queue: 'billing.receipts.priority', bindingKeys: ['billing.priority.#'] } });
```

## Logging

The library logs to the console by default, in the same style whatever the
application's logger. To route its logs elsewhere:

```ts
import { NestLoggerAdapter } from 'rabbitmq-message-bus/nestjs';

RabbitMqConsumerModule.register({
  // ...
  logger: new NestLoggerAdapter('NotificationsConsumer'),
});
```

Any object with `debug`, `info`, `warn` and `error` methods works, so you can
plug in pino, winston or a metrics counter. `silentLogger` turns logging off.

## Testing

Processors are plain classes: test `handle()` directly with a hand-made
payload.

```ts
await processor.handle({ messageId: randomUUID(), body: { invoiceId: 'inv-1' } });
```

To test the wiring without a database, use the in-memory stores:

```ts
RabbitMqConsumerModule.register({
  // ...
  inbox: { store: new InMemoryInboxStore() },
  logger: silentLogger,
});
```

`startConsumerWorker(Module)` and `dispatchOutbox(Module)` are the worker
commands as functions; they return instead of exiting the process, which makes
them usable in an end-to-end test against a real broker. The library's own
[integration tests](../test/integration/nestjs.spec.ts) show the pattern.

Next: [Workers and deployment](07-workers-and-deployment.md).

# 10. Migrating padmasana-service

This package was extracted from the RabbitMQ layer of `padmasana-service`
(`modules/shared/infrastructure/message-bus`). This guide replaces that layer
with the package, one consumer at a time.

**Status: not yet applied.** The steps follow the service's current code, and
the package's behaviour they rely on is tested, but the snippets below have not
been compiled or run inside the service.

- [What stays the same](#what-stays-the-same)
- [What changes in behaviour](#what-changes-in-behaviour)
- [Order of work](#order-of-work)
- [Step by step](#step-by-step)
- [What to delete](#what-to-delete)
- [Checking the result](#checking-the-result)

## What stays the same

- **The topology.** Same exchanges, queues, bindings and queue arguments, from
  the same environment variables. The package can take over queues the current
  code declared; an integration test declares a topology with the service's
  original calls, leaves a retry message in it, and has the package consume it.
- **The wire format.** Header names (`redelivery_count`, `retry_endpoint`), the
  error-queue headers (`exception_details`, `endpoint`) and the message
  properties are unchanged, so messages in flight during the deployment are
  handled by whichever version receives them.
- **The tables.** `outbox_message` and `inbox_message` are used as they are.
- **The processors.** Same `getHandlerName()` and `handle()`, same
  `@Transactional()` with their own inbox entry.
- **The commands and manifests.** `npm run dispatch-messages` and
  `npm run handle-messages` keep their names and flags; the files in
  `etc/kubernetes/ci-cd/workers` keep working unchanged.

## What changes in behaviour

| Before | With the package |
| --- | --- |
| A failed message was acknowledged even if the retry or error-queue publish had failed, which could lose it | It is acknowledged only after the broker confirms that publish; otherwise it is requeued |
| After a reconnect, a message still in flight was acknowledged on the new channel, which makes the broker close that channel | Acknowledgements go to the channel that delivered the message |
| Unreadable content went through every retry before reaching the error queue | It goes straight to the error queue |
| The error-queue copy was the re-serialized body | It is the original bytes |
| `RABBITMQ_HEARTBEAT_INTERVAL` had no effect, because amqplib only reads the heartbeat from the URL | It is applied |
| An outbox row whose publish returned `false` was logged as failed although it had been sent, and was sent again a minute later | Rows are marked as sent on the broker's confirmation |
| `dispatch-messages` always exited `0` | It exits `1` when the broker is unreachable, so the CronJob pod is retried |
| `--limit` defaulted to 10 and overrode `RABBITMQ_CONSUME_MESSAGE_LIMIT` | Without `--limit`, the configured value applies |
| A worker was killed mid-message on a rolling update | It finishes in-flight messages on SIGTERM, if the signal reaches it; see step 8 |
| A channel closed by the broker, or a cancelled consumer, was not handled | The consumer subscribes again |

One thing to decide: **the error queue.** Nothing in the service declares a
queue bound to `sagittarius-a-direct` with
`strategic-direction.padmasana.dead-letter`. If another system owns it, nothing
changes. If nobody does, failed messages are being dropped today; add
`error.queue` in step 2 and the consumers will declare it.

## Order of work

1. Add the package and the two store adapters. Nothing uses them yet.
2. Migrate **one consumer**, `task-management`: it has two processors and is
   the smallest. Deploy it and watch it next to the two unmigrated consumers.
3. Migrate the other consumers.
4. Migrate the producers and the command file.
5. Delete the old layer.

Each step is deployable on its own, because both implementations speak the same
wire format.

## Step by step

### 1. Stores

Two adapters over the existing repositories.

```ts
// modules/shared/infrastructure/message-bus/mikro-orm-inbox.store.ts
@Injectable()
export class MikroOrmInboxStore implements InboxStore {
  constructor(private readonly repository: InboxMessageRepository) {}

  async isHandled(messageId: string, handlerName: string, scope?: string) {
    return (await this.repository.getInboxMessageById(messageId, handlerName, scope)) !== null;
  }

  // Not called while inbox.record is 'handler': the processors record themselves.
  async markHandled(messageId: string, handlerName: string, scope?: string) {
    await this.repository.storeInboxMessage(
      { message_id: messageId, handler_name: handlerName },
      scope,
    );
  }
}
```

```ts
// modules/shared/infrastructure/message-bus/mikro-orm-outbox.store.ts
type PendingOutboxMessage = OutboxRecord & { entity: OutboxMessage };

@Injectable()
export class MikroOrmOutboxStore implements OutboxStore<PendingOutboxMessage> {
  constructor(private readonly repository: OutboxMessageRepository) {}

  async fetchPending(limit: number, scope?: string): Promise<PendingOutboxMessage[]> {
    const [messages] = await this.repository.findAndCount(
      { status: OutBoxStatus.PENDING },
      { limit, schema: scope },
    );
    return messages.map((entity) => ({
      entity,
      messageId: entity.message_id,
      type: entity.type,
      ...entity.getDestination(),
      body: entity.getBody(),
      properties: entity.getProperties(),
    }));
  }

  async markAsSent({ entity }: PendingOutboxMessage) {
    entity.markAsSent();
    await this.repository.save(entity);
  }
}
```

`scope` is the schema that `--schema` selects, as the `schema` argument is
today.

### 2. Endpoint configuration

The three 98-line `*-rabbitmq.config.ts` classes become one function. Every
environment variable keeps its name.

```ts
// modules/shared/infrastructure/message-bus/consumer-endpoint.ts
type ConsumerName = 'NOTIFICATION' | 'TASK_MANAGEMENT' | 'IDENTITY_AND_ACCESS_MANAGEMENT';

export function consumerEndpoint(config: ConfigService, consumer: ConsumerName): ConsumerEndpointOptions {
  const env = (suffix: string) => config.getOrThrow<string>(`RABBITMQ_${consumer}_EVENTS_${suffix}`);
  const direct = { name: config.getOrThrow<string>('RABBITMQ_DIRECT_EXCHANGE'), type: 'direct' as const };

  return {
    appName: config.getOrThrow('APP_NAME'),
    queue: env('QUEUE'),
    exchange: { name: config.getOrThrow('RABBITMQ_TOPIC_EXCHANGE'), type: 'topic' },
    bindingKeys: env('QUEUE_BINDING_KEY'),
    prefetch: Number(config.get('RABBITMQ_CONSUME_MESSAGE_LIMIT')),
    retry: {
      exchange: direct,
      queue: env('RETRY_QUEUE'),
      queueBindingKey: env('RETRY_QUEUE_BINDING_KEY'),
      returnBindingKey: env('QUEUE_RETRY_BINDING_KEY'),
      immediateRetries: Number(config.get('RABBITMQ_IMMEDIATE_RETRIES')),
      delayedRetries: Number(config.get('RABBITMQ_DELAYED_RETRIES')),
      queueMessageTtl: Number(config.get('RABBITMQ_RETRY_QUEUE_MESSAGE_TTL')),
    },
    error: {
      exchange: direct,
      routingKey: config.getOrThrow('RABBITMQ_ERROR_QUEUE_ROUTING_KEY'),
      // queue: 'strategic-direction.padmasana.error',  // see "the error queue" above
    },
  };
}
```

A missing variable now fails with the name of the option, for example
`Consumer option retry.delayedRetries must be an integer >= 0, received NaN`.

### 3. Handler maps

Each `signature.types.service.ts` class becomes a constant. Replace
`this.lazyLoader.handle(Module, Processor)` with `lazyHandler(Module, Processor)`.

```ts
// modules/task-management/src/infrastructure/processors/task-management.handlers.ts
export const taskManagementHandlers: NestHandlerMap = {
  'strategic-direction.padmasana.user_account_activated': [
    lazyHandler(MarkAssigneeAsActiveProcessorModule, MarkAssigneeAsActiveProcessor),
  ],
  'strategic-direction.padmasana.user_account_deactivated': [
    lazyHandler(MarkAssigneeAsInactiveProcessorModule, MarkAssigneeAsInactiveProcessor),
  ],
};
```

Check that each processor module **exports or at least provides** its processor.
`MarkAssigneeAsActiveProcessorModule` provides it without exporting it, which is
enough: the lazy loader reads it from the module itself.

### 4. Processors

No change in behaviour. Only the import of the payload type moves:

```ts
import type { EventPayload } from 'rabbitmq-message-bus/nestjs';
```

### 5. Consumer modules

```ts
// modules/task-management/src/infrastructure/message-bus/rabbitmq/consumer/consumer.module.ts
@Module({
  imports: [
    ConfigModule.forRoot({ isGlobal: true }),
    MikroOrmModule.forRoot({}),
    RabbitMqConsumerModule.registerAsync({
      inject: [ConfigService, MikroOrmInboxStore, EntityManager],
      extraProviders: [InboxMessageRepository, MikroOrmInboxStore],
      useFactory: (config: ConfigService, store: MikroOrmInboxStore, em: EntityManager) => ({
        connection: connectionOptionsFromEnv(),
        endpoint: consumerEndpoint(config, 'TASK_MANAGEMENT'),
        handlers: taskManagementHandlers,
        // The processors write their inbox row inside @Transactional().
        inbox: { store, record: 'handler' },
        // What @CreateRequestContext on InboxMessageHandler.handleMessage did.
        runInContext: (work) => RequestContext.create(em, work),
      }),
    }),
  ],
})
export class TaskManagementConsumerModule {}
```

Two points that are easy to miss:

- **`record: 'handler'`.** With the default, the consumer would insert a second
  inbox row after each processor and hit the unique constraint.
- **`runInContext`.** The processors' `@Transactional()` finds its
  `EntityManager` through MikroORM's request context. The old
  `InboxMessageHandler` opened one per message; this line does the same.

### 6. Producer modules

```ts
// modules/task-management/src/infrastructure/message-bus/rabbitmq/producer/producer.module.ts
@Module({
  imports: [
    ConfigModule.forRoot({ isGlobal: true }),
    MikroOrmModule.forRoot({}),
    TaskManagementMessageDestinationModule,
    RabbitMqProducerModule.registerAsync({
      inject: [ConfigService, MikroOrmOutboxStore, EntityManager],
      extraProviders: [OutboxMessageRepository, MikroOrmOutboxStore],
      useFactory: (config: ConfigService, outboxStore: MikroOrmOutboxStore, em: EntityManager) => ({
        connection: connectionOptionsFromEnv(),
        exchanges: [{ name: config.getOrThrow('RABBITMQ_TOPIC_EXCHANGE'), type: 'topic' }],
        outboxStore,
        batchSize: Number(config.get('RABBITMQ_DISPATCH_MESSAGE_LIMIT')),
        // What @CreateRequestContext on OutboxMessageRelay.dispatchMessages did.
        runInContext: (work) => RequestContext.create(em, work),
      }),
    }),
  ],
})
export class TaskManagementProducerModule {}
```

### 7. Destinations

`MessageDestinationRegistry` becomes the package's `DestinationRegistry`; it has
the same `get(type)`, so `OutboxMessageRepository` only changes its import. Each
`register...MessageDestination` function and its module become:

```ts
@Module({
  imports: [
    MessageDestinationModule.register({
      destinations: {
        'strategic-direction.padmasana.task_assigned': {
          exchange: 'sagittarius-a-topic',
          routingKey: 'strategic-direction.padmasana.task-management.#',
        },
        // ...
      },
    }),
  ],
})
export class TaskManagementMessageDestinationModule {}
```

The routing keys above are copied as they are. They contain a literal `#`,
which works because the consumers bind a topic exchange with
`strategic-direction.padmasana.#`. They would not work on a direct exchange; if
you move to one, use `routeByMessageType` and `bindHandledTypes`, as described
in [Exchange types](05-exchange-types.md#direct).

### 8. Commands

`cli-commands/command-handler.ts` becomes:

```ts
import { runMessageBusCli } from 'rabbitmq-message-bus/nestjs';
import { CONSUMER_MODULE_MAP, PRODUCER_MODULE_MAP } from './module.map';

void runMessageBusCli({
  consumers: CONSUMER_MODULE_MAP,
  producers: PRODUCER_MODULE_MAP,
  scopes: {
    'team-management': process.env.DB_TEAM_MANAGEMENT_SCHEMA,
    'task-management': process.env.DB_TASK_MANAGEMENT_SCHEMA,
    'board-management': process.env.DB_BOARD_MANAGEMENT_SCHEMA,
    'task-collaboration': process.env.DB_TASK_COLLABORATION_SCHEMA,
    'notification-module': process.env.DB_NOTIFICATION_SCHEMA,
    'identity-and-access-management': process.env.DB_IDENTITY_AND_ACCESS_MANAGEMENT_SCHEMA,
  },
});
```

The `scopes` map is the `schemaMapper` that both old commands contained. The
npm scripts do not change. The environment must be loaded before this file
runs: in the cluster it always is; locally, the module files imported by
`module.map` call `ConfigModule.forRoot()`, which loads `.env`.

**Graceful shutdown.** The worker manifests start the process with
`/bin/sh -c "npm run handle-messages -- ..."`. Whether SIGTERM reaches Node.js
through the shell and npm in the service's image has not been checked. To be
sure, regenerate the manifests with an `entrypoint`, as described in
[Workers and deployment](07-workers-and-deployment.md#graceful-shutdown):

```yaml
# etc/kubernetes/workers.yml
placeholders:
  KUB_SERVICE_NAME: padmasana-service
entrypoint: ['dumb-init', 'node', 'dist/shared/infrastructure/message-bus/cli-commands/command-handler']
dispatchers:
  - module: board-management
  - module: task-management
  - module: team-management
  - module: task-collaboration
    alias: collaboration
  - module: identity-and-access-management
    alias: iam
consumers:
  - module: task-management
  - module: identity-and-access-management
    alias: iam
  - module: notification-module
    volumeMounts:
      - name: google-mailing-service-account
        mountPath: /app/etc/secrets/mailer-service-account.json
        subPath: mailer-service-account.json
        readOnly: true
    volumes:
      - name: google-mailing-service-account
        secret:
          secretName: mailer-service-account
```

Without the `entrypoint` line, this configuration generates manifests
equivalent to the eight files that exist today: compared after parsing, they
differ only in how the command line is wrapped.

## What to delete

Once every module is migrated:

- `modules/shared/infrastructure/message-bus/rabbitmq/`
- `modules/shared/infrastructure/message-bus/cli-commands/dispatch-messages.ts`
  and `handle-messages.ts`
- `inbox-message-handler.service.ts`, `lazy-load-handler.service.ts`,
  `outbox-message-relay.service.ts`, `message-destination-registry.ts`,
  `message-destination.module.ts`
- every `*-rabbitmq.config.ts` and `signature.types.service.ts`
- the dependencies `nest-commander`, `amqplib`, `@types/amqplib` and
  `jsonrepair`

## Checking the result

After migrating a consumer, in a development environment:

1. Start it and confirm in the management console that no queue or binding
   changed, and that the queue shows one consumer.
2. Publish an event it handles; confirm the processor ran and an
   `inbox_message` row exists.
3. Make a processor throw temporarily; confirm the message visits the retry
   queue three times and then reaches the error exchange.
4. Publish the same message again; confirm
   `Duplicate message ignored` in the log.
5. Restart RabbitMQ; confirm the worker reconnects, or exits and is restarted.

The [playground](../examples/demo/README.md) shows what each of these looks like when
it works.

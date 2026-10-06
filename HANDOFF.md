# rabbitmq-message-bus: design and status

A reusable npm package that extracts the RabbitMQ messaging layer of
`padmasana-service` (`~/Desktop/FUNIBER/padmasana/padmasana-service`) so that
each responsibility lives in its own piece and any service can reuse it. It must
work with the topic exchange used today and with a direct exchange.

This document records what the source service does, what is wrong with it, how
the package is designed, and how far the build has got. The status section at
the end is the place to look when resuming work.

## 1. How padmasana-service does it today

Everything lives under `modules/shared/infrastructure/message-bus/`.

**Publishing (outbox pattern).** A feature handler stores domain events in the
`outbox_message` table of its bounded context, in the same transaction as the
business change. `OutboxMessageRepository` looks up the exchange and routing key
for each message type in `MessageDestinationRegistry`. A Kubernetes CronJob per
module runs `npm run dispatch-messages -- --limit 10 --module=X --schema=X`
every minute; `OutboxMessageRelay` reads pending rows and `ProducerService`
publishes them and marks them as sent.

**Consuming (inbox pattern).** A Kubernetes Deployment per consuming module runs
`npm run handle-messages -- --limit 10 --module X --schema X`. `ConsumerService`
declares the topology, consumes the primary queue and hands each message to
`InboxMessageHandler`, which looks up the processors registered for the message
type in a `SignatureTypes` map, skips the ones already recorded in
`inbox_message`, and calls `processor.handle()`. Each processor records its own
inbox row inside its `@Transactional()` method.

**Resilience.**

| Mechanism | Where | Behaviour |
| --- | --- | --- |
| Reconnect | `rabbitmq-connection.service.ts` | 3 attempts, 5 s apart, then `process.exit(1)` so Kubernetes restarts the pod |
| Immediate retries | `inbox-message-handler.service.ts` | `RABBITMQ_IMMEDIATE_RETRIES` (5) extra in-process attempts per handler |
| Delayed retries | `rabbitmq-connection.service.ts` `retry()` | Message is republished to the retry queue with `redelivery_count + 1` and a per-message expiration of `count * 2000` ms; when it expires the broker dead-letters it back to the primary queue. `RABBITMQ_DELAYED_RETRIES` (3) trips |
| Error queue | `deadLetter()` | After the delayed retries the message is published to the error exchange with `exception_details` and `endpoint` headers |
| Idempotency | `inbox_message` unique on `(message_id, handler_name)` | A redelivered message is not handled twice by the same processor |

**Topology of one consumer.**

```
topic exchange  --"strategic-direction.padmasana.#"--> primary queue
direct exchange --"<queue>.retry"---------------------> primary queue
direct exchange --"<queue>.events-retry"--------------> retry queue
                                                        (x-message-ttl, dead-letters
                                                         to direct exchange with "<queue>.retry")
direct exchange --"...dead-letter"--------------------> (no queue declared)
```

**Deployment.** `etc/kubernetes/ci-cd/workers/` holds five near-identical
CronJob manifests (dispatchers) and three near-identical Deployment manifests
(consumers). They differ only by module name, a shortened name where the
52-character CronJob limit would be exceeded, and a volume mount for the
notification consumer. `etc/ci-cd/deploy.kubectl.sh` substitutes
`{KUB_SERVICE_NAME}`, `{KUB_NAMESPACE_PREFIX}`, `{BUILD_ENV}` and the image with
`sed`, then applies each file by name.

I did not find the term "RMHA" anywhere in the repository. I have read it as
"the resilience mechanisms of the RabbitMQ layer" listed above. If it names
something specific (for example a high-availability setup with quorum queues),
that needs confirming.

## 2. Problems in the current implementation

These are the reasons the package is not a straight copy. Paths are relative to
`modules/shared/infrastructure/message-bus/` in the service.

1. **Messages can be lost on failure.** `rabbitmq/consumer/consumer.service.ts:146`
   acknowledges in a `finally`, even when the retry or error-queue publish
   failed. Publishing uses a plain channel with no publisher confirms
   (`rabbitmq-connection.service.ts:103`), so the acknowledgement is sent before
   the broker has accepted the copy.
2. **Messages sent to the error queue are dropped unless someone else declared
   it.** The consumer topology (`rabbitmq-configurer.service.ts:24`) never
   declares or binds an error queue, and the publish is not `mandatory`.
3. **Acknowledging on the wrong channel after a reconnect.** The handler uses
   `this.channel.ack(message)`; after a reconnect `this.channel` is the new
   channel, and acknowledging a delivery tag from the old one makes the broker
   close the new channel.
4. **The heartbeat setting has no effect.** `rabbitmq-config.service.ts:116`
   passes `heartbeat` in the socket options; amqplib only reads it from the URL.
5. **Outbox rows can be published twice or reported as failed wrongly.**
   `producer.service.ts:55` treats `publish() === false` as a failure, but that
   return value only means the write buffer is full; the message is still sent.
   Without confirms there is also no proof that a row marked as sent arrived.
6. **Unparseable messages are retried for nothing.** A body that cannot be
   repaired becomes `{}`, the handler fails on it, and it goes through every
   immediate and delayed retry before reaching the error queue.
7. **Channel errors are not handled.** No `error` or `close` listener on the
   channel; a broker-side channel close crashes the process by accident, and a
   consumer cancellation (`message === null`) is silently ignored.
8. **No graceful shutdown.** On a rolling update in-flight handlers are killed.
9. **Responsibilities are tangled.** `RabbitmqConnectionService` owns the
   connection, reconnect, channel, topology primitives, publishing, retry
   policy, error-queue format and JSON repair. `RabbitmqModule` imports
   `MikroOrmModule.forRoot({})` and `ConfigModule`, so messaging cannot be used
   without that ORM. The CLI commands hard-code the list of padmasana modules
   and schemas (`cli-commands/handle-messages.ts:37`,
   `cli-commands/dispatch-messages.ts:22`, `cli-commands/module.map.ts`).
10. **Direct exchanges are not really supported.** One binding key per queue
    only, and the destination registry publishes with routing keys that contain
    a literal `#` (`task-management.message-destination.ts`), which only works
    because the consumers bind a topic exchange with `#`. A direct exchange
    needs exact keys and usually several bindings per queue.

## 3. Package design

One package, three entry points, so a service only pulls in what it uses.

| Entry point | Contents | Depends on |
| --- | --- | --- |
| `rabbitmq-message-bus` | Framework-agnostic core | `amqplib`, `jsonrepair` |
| `rabbitmq-message-bus/nestjs` | Dynamic modules, lazy handler resolution, worker CLI | `@nestjs/common`, `@nestjs/core` (optional peers) |
| `rabbitmq-message-bus/k8s` | Worker manifest generator and `rabbitmq-bus-k8s` binary | `yaml` |

### Core: one responsibility per class

| Class | Responsibility | Replaces |
| --- | --- | --- |
| `ConnectionManager` | Connection lifecycle and reconnect policy; emits `connected`, `disconnected`, `reconnecting`, `failed`. Never calls `process.exit` | half of `RabbitmqConnectionService` |
| `TopologyDeclarator` | Declares exchanges, queues, bindings from a plain `Topology` object | `RabbitmqConfigurerService` |
| `resolveConsumerEndpoint`, `buildConsumerTopology` | Defaults, validation and the primary/retry/error layout of a consumer | `RabbitmqConfigService` validation |
| `HandlerRegistry` | Message type to handlers, resolved lazily and cached | `SignatureTypes` |
| `HandlerExecutor` | Inbox check, immediate retries, inbox recording | `InboxMessageHandler` |
| `RetryPolicy` + backoff strategies | Decides between delayed retry and error queue | logic inside `ConsumerService.handleError` |
| `RecoverabilityExecutor` | Publishes to the retry queue or the error queue, with confirms | `retry()` and `deadLetter()` |
| `Consumer` | Subscription, prefetch, message filtering, acknowledgement discipline, graceful stop | `ConsumerService` |
| `Publisher` | Publish with broker confirmation | `publish()` |
| `OutboxRelay` | Reads pending outbox records and publishes them | `OutboxMessageRelay` + `ProducerService` |
| `DestinationRegistry` | Message type to exchange and routing key, with a fallback resolver | `MessageDestinationRegistry` |
| `JsonMessageSerializer` | Parse and repair | `robustParseMessageContent` |

Persistence is behind two ports, so the package has no ORM dependency:

- `InboxStore`: `isHandled(messageId, handlerName, scope?)`, `markHandled(...)`.
- `OutboxStore`: `fetchPending(limit, scope?)`, `markAsSent(record, scope?)`.

`scope` is an opaque partition forwarded from the worker command line. In
padmasana it carries the database schema, which is what `--schema` does today.
The service implements both ports with its existing MikroORM repositories; an
in-memory implementation of each ships for tests.

`inbox.record` chooses who writes the inbox row: `pipeline` (the consumer,
after the handler returns) or `handler` (the processor, inside its own
transaction, which is what padmasana does today and should keep).

`runInContext` wraps the handling of each message (and each outbox run). It
exists because padmasana's processors rely on the MikroORM request context that
`InboxMessageHandler` and `OutboxMessageRelay` open with `@CreateRequestContext`;
the service passes `(work) => RequestContext.create(em, work)`.

### Behaviour changes compared with the service

- Publisher confirms everywhere. A message is acknowledged only after the retry
  or error-queue copy is confirmed; otherwise it is requeued.
- Acknowledgements go to the channel that delivered the message.
- `error.queue` is optional; when set, the queue is declared and bound. The
  error publish is `mandatory` and an unroutable message is logged.
- `UnrecoverableMessageError` (and unparseable content) skips all retries.
- Channel close and consumer cancellation trigger a re-subscription.
- `Consumer.stop()` cancels the subscription and waits for in-flight handlers.
- The heartbeat is applied through the URL.
- Header names and the error-queue format are unchanged
  (`redelivery_count`, `retry_endpoint`, `exception_details`, `endpoint`), so
  messages already in flight keep working across the migration.

### Exchange types

- `topic`: `bindingKeys` are patterns. Current behaviour.
- `direct`: `bindingKeys` are exact keys and there can be several.
  `bindHandledTypes: true` binds one key per registered message type, which
  pairs with `routeByMessageType(exchange)` on the publishing side (routing key
  equals message type). A wildcard in a key on a direct exchange logs a warning.
- `fanout`: binding keys are ignored.
- The retry exchange must be `direct`, whatever the primary exchange is,
  because retried messages have to return to exactly one queue. Key collisions
  between primary, retry and error routing are rejected at startup.

### Option names, old to new

| padmasana `IRabbitMqConfig` | Package `ConsumerEndpointOptions` |
| --- | --- |
| `appName` | `appName` |
| `primaryQueue` | `queue` |
| `primaryQueueExchange`, `primaryQueueExchangeType` | `exchange: { name, type }` |
| `primaryQueueBindingKey` | `bindingKeys` (string or array) |
| `primaryQueueRetryBindingKey` | `retry.returnBindingKey` (default `<queue>.retry`) |
| `retryQueue` | `retry.queue` (default `<queue>.events-retry`) |
| `retryQueueBindingKey` | `retry.queueBindingKey` (default: retry queue name) |
| `retryQueueExchange`, `retryQueueExchangeType` | `retry.exchange: { name, type: 'direct' }` |
| `retryQueueMessageTtl` | `retry.queueMessageTtl` (default 10000) |
| `immediateRetriesNumber` | `retry.immediateRetries` (default 5) |
| `delayedRetriesNumber` | `retry.delayedRetries` (default 3) |
| `errorQueueExchange`, `errorQueueExchangeType` | `error.exchange: { name, type }` |
| `errorQueueRoutingKey` | `error.routingKey` |
| (none) | `error.queue`, `queueType`, `bindHandledTypes`, `retry.backoff` |
| `consumeMessageLimit` | `prefetch` (default 10) |
| `dispatchMessageLimit` | producer `batchSize` (default 10) |

### NestJS layer

- `RabbitMqConsumerModule.register / registerAsync`: options, handler map,
  inbox store. Provides `ConsumerWorker` (`start(overrides)`, `stop()`).
- `RabbitMqProducerModule.register / registerAsync`: options, outbox store.
  Provides `OutboxDispatcher` and `Publisher`.
- `MessageDestinationModule.register({ destinations, fallback })`: adds to one
  application-wide `DestinationRegistry`.
- `lazyHandler(Module, Processor)`: replaces `LazyLoadHandler`; modules are
  loaded with `LazyModuleLoader`, by default at startup so a broken processor
  fails the pod immediately.
- `runMessageBusCli({ consumers, producers, scopes })`: the service's whole
  `command-handler.ts` becomes one call. Commands and flags stay compatible:
  `handle-messages` and `dispatch-messages` with `--module`, `--limit`,
  `--schema` (alias of `--scope`) and the topology override flags. Arguments are
  parsed with `node:util.parseArgs`, so `nest-commander` is no longer needed.
  The CLI exits with code 1 when the connection cannot be established, which
  keeps the "let Kubernetes restart it" behaviour outside the library.

### Kubernetes manifests

`generateWorkerManifests(config)` returns one CronJob per dispatcher and one
Deployment per consumer, in the same shape and with the same `{PLACEHOLDER}`
convention as `etc/kubernetes/ci-cd/workers/`, so `deploy.kubectl.sh` keeps
working. It checks the 52-character CronJob name limit and accepts an `alias`
per worker for the shortened names. `rabbitmq-bus-k8s generate --config <file>
--out <dir>` writes the files.

## 4. Open decisions

1. **Package name.** `rabbitmq-message-bus` is a placeholder. It needs your
   registry scope before publishing. Nothing has been published.
2. **MikroORM adapters.** They live in the service, not in the package (about
   ten lines per port, shown in the README). Say so if you would rather ship
   them as a fourth entry point.
3. **Kubernetes generator.** Included because the worker manifests are part of
   what was asked to be read. It is isolated in its own entry point and can be
   dropped.
4. **What "RMHA" refers to**, see section 1.

## 5. Status

Last updated: 2026-10-06. The package, its documentation and a demo application
are complete; padmasana-service has not been touched, and nothing is published.

- [x] Core, `src/core/`, exported from `src/index.ts`
- [x] NestJS layer and worker CLI, `src/nestjs/`
- [x] Kubernetes manifest generator and `rabbitmq-bus-k8s` binary, `src/k8s/`
- [x] Usage guides, `docs/` (ten guides, indexed in `docs/README.md`); the root
      `README.md` is an overview that links to them
- [x] Demo application with a dashboard, `examples/demo/`
- [x] `npm run build` and `npm pack`

### What has been verified

Library:

- `npm run typecheck` passes.
- `npm test`: 103 tests in 9 suites pass, on repeated runs. 69 are unit tests;
  34 run against a real RabbitMQ 4.1 broker and cover topic, direct and fanout
  routing, immediate and delayed retries, the error queue, idempotency,
  reconnect, losing the connection mid-message, consumer cancellation, graceful
  stop, the outbox relay, the NestJS modules and the CLI.
- One integration test declares the topology with the original service's exact
  calls, leaves a retry message in the original format, and checks the package
  takes both over without a declaration conflict.
- The packed tarball was installed into a separate project with a
  padmasana-like `tsconfig` (`module: nodenext`); all three entry points
  resolve and typecheck.
- The CLI was run as real processes from that project against NestJS 10.4, 11.2
  and 12.1: `dispatch-messages` publishes and exits 0, and a `handle-messages`
  worker sent SIGTERM in the middle of a message finishes it, stops consuming
  and closes its connection. The full test suite runs on NestJS 11 only.
- The manifest generator, given padmasana's eight workers, produces manifests
  that are equivalent to the eight files in `etc/kubernetes/ci-cd/workers/`
  once parsed (the consumer commands differ only in line wrapping and
  `--module X` versus `--module=X`).

Documentation:

- The complete examples in `docs/02-getting-started.md` (plain Node.js consumer
  and publisher, NestJS processor, consumer module and command file) were
  extracted from the guide, compiled against the packed tarball, and run as
  processes against a broker; each handled its message.
- All 127 relative links and anchors in the guides and READMEs resolve.
- Error messages quoted in `docs/09-troubleshooting.md` were checked against
  the source.

Demo:

- Every dashboard action was exercised through its HTTP API against a real
  broker: the happy path, immediate and delayed retries, the error queue and
  replay, duplicates, unhandled events, switching between topic, direct and
  fanout, cutting the consumers' connections, deleting a queue, stopping and
  starting a consumer.
- The page was loaded in headless Chrome at 1500px and 900px wide, in light and
  dark mode, and driven by real clicks (experiments, a processor menu, replay,
  selecting a message, switching exchange type) with no console errors.
- Split mode was run: the API, two `handle-messages` workers and
  `dispatch-messages` as four processes sharing the SQLite file.
- `npm run manifests` generates the demo's CronJob and Deployments.

### What has not been verified

- The migration steps in `docs/10-migrating-padmasana-service.md` (MikroORM
  stores, `runInContext` with `RequestContext.create`, the module rewrites)
  follow the service's current code but were not compiled or run against it.
  Migrating the service is the next piece of work.
- The code fragments in guides 3 to 7 are excerpts, not whole files. They were
  written against the real API and reviewed, but only the getting-started
  examples were compiled and run.
- Nothing was run in a Kubernetes cluster. In particular, whether SIGTERM
  reaches Node.js through `/bin/sh -c "npm run ..."` in the service image is
  unknown; the guides explain the `entrypoint` option that avoids the question.
- Quorum queues (`queueType: 'quorum'`) are only covered by a unit test of the
  generated topology, not by a run against the broker.
- The dashboard was checked in Chrome only.

### Suggested next steps

1. Decide the package name and publish it to your registry.
2. Migrate one consumer of padmasana-service first (task-management has two
   processors) following guide 10, run it next to the others, then do the rest
   and the producers.
3. Add `error.queue` or confirm who owns the error queue bound to
   `sagittarius-a-direct` with `strategic-direction.padmasana.dead-letter`.
   Today nothing in the service declares it.

### Running the integration tests

They expect a broker on port 5679, separate from the padmasana one on 5672:

```bash
docker run -d --rm --name rmq-bus-pkg-test -p 5679:5672 -p 15679:15672 rabbitmq:4.1-management-alpine
npm run test:integration
docker stop rmq-bus-pkg-test
```

## Documentation website and playground (added 2026-10-06)

- `website/` is a VitePress site built from `docs/*.md` (copied by
  `website/scripts/sync-docs.mjs`; `docs/` stays the single source). It builds
  to static files in `website/.vitepress/dist`. Deployment options, including a
  Dockerfile, are in `website/README.md`.
- The demo in `examples/demo` is now called the **playground** in the docs, the
  website and its own page title. The directory and its `DEMO_*` variables keep
  their names.
- The playground gained a theme switch, icons, animated message movement in the
  flow diagram and a terminal-style live log.
- Verified: the site builds (the build fails on dead links), also under a
  sub-path with `DOCS_BASE`; the Docker image builds and serves every page.
  Not verified: any real hosting provider.

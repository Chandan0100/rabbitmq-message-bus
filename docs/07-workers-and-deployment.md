# 7. Workers and deployment

A service that uses the message bus runs three kinds of process:

| Process | Command | Lifetime | Kubernetes object |
| --- | --- | --- | --- |
| The API | your `main.ts` | Long-running | Deployment |
| A dispatcher per publishing module | `dispatch-messages --module <name>` | A few seconds, on a schedule | CronJob |
| A worker per consuming module | `handle-messages --module <name>` | Long-running | Deployment |

- [The command file](#the-command-file)
- [dispatch-messages](#dispatch-messages)
- [handle-messages](#handle-messages)
- [Exit codes](#exit-codes)
- [Graceful shutdown](#graceful-shutdown)
- [Kubernetes manifests](#kubernetes-manifests)
- [Sizing and scaling](#sizing-and-scaling)
- [What to monitor](#what-to-monitor)

## The command file

One file maps module names to NestJS modules:

```ts
// src/message-bus-cli.ts
import 'reflect-metadata';
import { runMessageBusCli } from 'rabbitmq-message-bus/nestjs';

void runMessageBusCli({
  consumers: {
    notifications: NotificationsConsumerModule,
    audit: AuditConsumerModule,
  },
  producers: {
    billing: BillingProducerModule,
  },
});
```

```json
{
  "scripts": {
    "dispatch-messages": "node dist/message-bus-cli.js dispatch-messages",
    "handle-messages": "node dist/message-bus-cli.js handle-messages"
  }
}
```

A consumer entry is a module that imports `RabbitMqConsumerModule`; a producer
entry is one that imports `RabbitMqProducerModule` with an `outboxStore`. Each
command boots only the module it was asked for.

### Options

| Option | |
| --- | --- |
| `consumers`, `producers` | Name to module, as above |
| `scopes` | Translates the value of `--schema`; see below |
| `nestLogger` | NestJS log levels during boot. Default `['warn', 'error']`; `false` for none |
| `argv` | Default `process.argv.slice(2)` |

### `--schema` and scopes

`--schema <value>` (or `--scope`) is passed to your stores as the `scope`
argument. Use it when one store class serves several database schemas or
tenants and the worker has to say which.

With `scopes`, the command line carries a friendly name and the stores receive
what it stands for. Values that are not listed are rejected:

```ts
runMessageBusCli({
  // ...
  scopes: {
    billing: process.env.DB_BILLING_SCHEMA,
    notifications: process.env.DB_NOTIFICATIONS_SCHEMA,
  },
});
```

```bash
dispatch-messages --module billing --schema billing   # the store receives DB_BILLING_SCHEMA's value
```

If your stores are bound to their schema already, leave `--schema` out.

## dispatch-messages

```bash
node dist/message-bus-cli.js dispatch-messages --module billing --limit 10
```

Publishes one batch of the module's outbox and exits. `--limit` is the batch
size; without it the module's `batchSize` applies (default 10).

Schedule it as often as you want messages to leave. Never let two runs for the
same outbox overlap: they would publish the same rows twice.

## handle-messages

```bash
node dist/message-bus-cli.js handle-messages --module notifications --limit 10
```

Declares the topology, then consumes until stopped. `--limit` is the prefetch;
without it the endpoint's `prefetch` applies (default 10).

Flags can override the configured endpoint, so the same build can serve another
queue or another exchange:

| Flag | Overrides |
| --- | --- |
| `--app-name` | `appName` |
| `--primary-queue` | `queue` |
| `--primary-queue-binding-key` (repeatable) | `bindingKeys` |
| `--primary-queue-exchange`, `--primary-queue-exchange-type` | `exchange` |
| `--primary-queue-retry-binding-key` | `retry.returnBindingKey` |
| `--retry-queue` | `retry.queue` |
| `--retry-queue-binding-key` | `retry.queueBindingKey` |
| `--retry-queue-exchange`, `--retry-queue-exchange-type` | `retry.exchange` |
| `--retry-queue-message-ttl` | `retry.queueMessageTtl` |
| `--immediate-retries-number` | `retry.immediateRetries` |
| `--delayed-retries-number` | `retry.delayedRetries` |
| `--error-queue` | `error.queue` |
| `--error-queue-exchange`, `--error-queue-exchange-type` | `error.exchange` |
| `--error-queue-routing-key` | `error.routingKey` |

An unknown flag is an error, not a silent no-op.

## Exit codes

| Situation | `dispatch-messages` | `handle-messages` |
| --- | --- | --- |
| Normal | `0` after the batch, even if some records failed and stay pending | Runs until signalled |
| Bad arguments, unknown module | `1`, with the usage text | `1`, with the usage text |
| The module cannot boot (a processor cannot be built, bad configuration) | `1` | `1` |
| The broker cannot be reached within the reconnect policy | `1` | `1` |
| The subscription cannot be restored after a reconnect | not applicable | `1` |
| SIGTERM or SIGINT | not applicable | Finishes in-flight messages, closes the connection, then ends with the signal |

Exiting on an unreachable broker is deliberate: the supervisor restarts the
process, which is simpler and more visible than a worker that retries forever
in silence. Tune how long it tries with `connection.reconnect` (default: 3
attempts, 5 seconds apart).

The exiting is done by `runMessageBusCli`, never by the library's classes. If
you need different behaviour, `startConsumerWorker(module, options)` and
`dispatchOutbox(module, options)` do the same work and return.

## Graceful shutdown

On SIGTERM a consumer stops taking messages, lets the ones in flight finish (up
to `shutdownTimeoutMs`, 30 seconds by default), acknowledges them and closes its
connection. Without that, a rolling update kills handlers mid-flight; the
messages are redelivered and the inbox keeps them from being applied twice, but
work is repeated and side effects outside the database may happen twice.

For this to work, **the signal has to reach the Node.js process**. It often does
not when the container starts the worker through a shell or npm:

```yaml
command: ['/bin/sh', '-c', 'npm run handle-messages -- --module notifications']
```

Here the shell is process 1 and receives the SIGTERM; whether it passes it on
depends on the shell and the npm version. Start Node.js directly instead:

```yaml
command: ['node', 'dist/message-bus-cli.js', 'handle-messages', '--module=notifications']
```

Keep `terminationGracePeriodSeconds` above `shutdownTimeoutMs`.

## Kubernetes manifests

Describe the workers once:

```yaml
# etc/kubernetes/workers.yml
placeholders:
  KUB_SERVICE_NAME: billing-service # sample value, only used to check name lengths
entrypoint: ['node', 'dist/message-bus-cli.js']
dispatchers:
  - module: billing
  - module: identity-and-access-management
    alias: iam
consumers:
  - module: notifications
    replicas: 2
    terminationGracePeriodSeconds: 45
  - module: audit
```

and generate the manifests:

```bash
npx rabbitmq-bus-k8s generate --config etc/kubernetes/workers.yml --out etc/kubernetes/ci-cd/workers
```

This writes one `dispatch-messages.<module>.cronjob.yml` per dispatcher and one
`handle-messages.<module>.yml` per consumer. Without `--out` the manifests are
printed.

The service name, namespace and image are left as placeholders for your deploy
script to substitute:

| Setting | Default |
| --- | --- |
| `serviceName` | `{KUB_SERVICE_NAME}` |
| `namespace` | `{KUB_NAMESPACE_PREFIX}-{BUILD_ENV}` |
| `image` | `localhost:32000/image:latest` |
| `imagePullPolicy` | `Always` |
| `imagePullSecrets` | `['gcr-secret']` |
| `envFrom` | ConfigMap `<serviceName>-config`, Secret `<serviceName>-secrets`, Secret `rabbitmq-credentials` |
| `resources` | 100m / 256Mi requested, 250m / 512Mi limit |
| `entrypoint` | none: `/bin/sh -c "npm run <command> -- <args>"` |

Per worker:

| Setting | Default | |
| --- | --- | --- |
| `module` | required | Value of `--module` |
| `schema` | the module name | Value of `--schema`; `false` to leave the flag out |
| `alias` | the module name | Used in resource names, to keep them short |
| `limit` | `10` | Value of `--limit` |
| `args` | none | Extra arguments, such as topology overrides |
| `resources`, `env`, `volumes`, `volumeMounts` | none | Passed through to the pod |
| `schedule` (dispatchers) | `*/1 * * * *` | |
| `replicas` (consumers) | `1` | |
| `terminationGracePeriodSeconds` (consumers) | the cluster default | |

What the generator takes care of:

- Dispatcher CronJobs get `concurrencyPolicy: Forbid`, so two runs never
  overlap.
- CronJob names longer than Kubernetes' 52 characters are refused, with the
  name and its length; give the worker an `alias`. Set `placeholders` to
  realistic values so the check measures the real names.
- The configuration can be `.yml`, `.json` or a `.js` module.

The same thing from code:

```ts
import { generateWorkerManifests } from 'rabbitmq-message-bus/k8s';

for (const { fileName, yaml } of generateWorkerManifests(config)) {
  writeFileSync(join(outDir, fileName), yaml);
}
```

## Sizing and scaling

- **A consumer falls behind**: raise `replicas`. Instances share the queue.
  Raising `--limit` helps when handlers mostly wait on I/O.
- **Messages leave the outbox too slowly**: raise the dispatcher's `--limit`,
  or run it more often. Do not add a second dispatcher for the same outbox.
- **Many small consumers**: each worker holds one connection and one channel.

## What to monitor

| Signal | Meaning |
| --- | --- |
| Depth of the error queue above zero | Messages were given up on. Read their `exception_details`, fix, replay |
| Depth of a primary queue growing | The consumer is down or too slow |
| Pending rows in an outbox growing | The dispatcher is failing or not running |
| Worker restarts | The broker was unreachable, or the module does not boot |
| Log line `was not routed to any queue` | Nothing is bound to the error exchange; a failed message was dropped |

Next: [Configuration reference](08-configuration-reference.md).

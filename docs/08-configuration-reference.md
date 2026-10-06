# 8. Configuration reference

- [Connection](#connection)
- [Consumer endpoint](#consumer-endpoint)
- [Consumer](#consumer)
- [Publisher](#publisher)
- [Outbox relay](#outbox-relay)
- [NestJS module options](#nestjs-module-options)
- [Environment variables](#environment-variables)
- [Message headers](#message-headers)
- [Errors](#errors)
- [Events](#events)

## Connection

`ConnectionOptions`, passed to `new ConnectionManager(options)` or as
`connection` in the NestJS modules.

| Option | Default | |
| --- | --- | --- |
| `url` | required | `amqp://host:port/vhost` or `amqps://...`. Anything else is rejected at startup |
| `username`, `password` | from the URL | When both are set, they replace the credentials in the URL |
| `heartbeat` | `30` | Seconds between heartbeats |
| `connectionName` | none | Shown next to the connection in the management console |
| `socketOptions` | none | Passed to amqplib: TLS certificates, `timeout`, `noDelay` |
| `reconnect.enabled` | `true` | |
| `reconnect.maxAttempts` | `3` | Consecutive failed attempts before giving up. `Infinity` never gives up |
| `reconnect.delayMs` | `5000` | Wait between attempts |

The counter resets every time a connection succeeds. With the defaults, a
broker that is down for more than about 15 seconds makes the worker give up and
exit.

## Consumer endpoint

`ConsumerEndpointOptions`, the `endpoint` of a consumer.

| Option | Default | |
| --- | --- | --- |
| `appName` | required | Identity of this endpoint. Stamped on the messages it schedules for a retry; only an endpoint with the same name handles them when they return |
| `queue` | required | The primary queue |
| `exchange` | required | `{ name, type }`. Type `topic`, `direct` or `fanout`. `durable` defaults to `true` |
| `bindingKeys` | none | A string or an array. Patterns for a topic exchange, exact keys for a direct one, ignored for fanout |
| `bindHandledTypes` | `false` | Also bind one key per message type that has a handler |
| `queueType` | broker default | `'quorum'` or `'classic'`, applied to every queue the endpoint declares |
| `prefetch` | `10` | Messages processed at the same time |
| `retry.exchange` | required | `{ name, type: 'direct' }`. Must be direct |
| `retry.queue` | `<queue>.events-retry` | |
| `retry.queueBindingKey` | the retry queue name | Routing key into the retry queue |
| `retry.returnBindingKey` | `<queue>.retry` | Routing key back to the primary queue |
| `retry.immediateRetries` | `5` | Extra in-process attempts per delivery. `0` disables |
| `retry.delayedRetries` | `3` | Trips through the retry queue. `0` sends a failed message straight to the error queue |
| `retry.queueMessageTtl` | `10000` | Milliseconds. The longest possible delay; an argument of the retry queue |
| `retry.backoff` | `linearBackoff(2000)` | `(attempt) => milliseconds`. Also `fixedBackoff(ms)` and `exponentialBackoff(initialMs, factor)` |
| `error.exchange` | required | `{ name, type }` |
| `error.routingKey` | required | |
| `error.queue` | none | When set, the queue is declared and bound to the error exchange |

A topic or direct endpoint needs at least one binding key, from `bindingKeys`
or `bindHandledTypes`.

**Options that become queue arguments cannot be changed on an existing queue**:
`queueType`, `retry.queueMessageTtl`, `retry.exchange.name` and
`retry.returnBindingKey` (the last three are arguments of the retry queue).
RabbitMQ rejects the new declaration with `PRECONDITION_FAILED`. Delete the
queue while it is empty and let the consumer declare it again, or use a new
queue name.

## Consumer

`ConsumerOptions`, the third argument of `new Consumer(...)`.

| Option | Default | |
| --- | --- | --- |
| `endpoint` | required | See above |
| `inbox.store` | none | An `InboxStore`. Without one, redelivered messages are handled again |
| `inbox.record` | `'pipeline'` | `'pipeline'`: the consumer writes the inbox entry. `'handler'`: your handler does |
| `scope` | none | Passed to the inbox store on every call |
| `runInContext` | none | `(work) => Promise`. Wraps the inbox check and the handlers of each message |
| `serializer` | `new JsonMessageSerializer()` | `{ serialize, deserialize }`. `new JsonMessageSerializer({ repair: false })` rejects malformed JSON |
| `preloadHandlers` | `true` | Resolve every handler at start |
| `shutdownTimeoutMs` | `30000` | How long `stop()` waits for in-flight messages |
| `confirmTimeoutMs` | `10000` | How long to wait for the broker to confirm a retry or error-queue publish |

## Publisher

`PublisherOptions`, the second argument of `new Publisher(...)`.

| Option | Default | |
| --- | --- | --- |
| `exchanges` | none | Declared before the first publish |
| `confirmTimeoutMs` | `10000` | How long `publish` waits for the broker's confirmation |
| `serializer` | `new JsonMessageSerializer()` | |

`publish({ exchange, routingKey, body, properties })`: `properties` are AMQP
properties (`messageId`, `type`, `appId`, `headers`, `contentType`...).
`persistent` defaults to `true`.

## Outbox relay

`OutboxRelayOptions`, the third argument of `new OutboxRelay(...)`.

| Option | Default | |
| --- | --- | --- |
| `defaultExchange` | none | For records without an `exchange` |
| `batchSize` | `10` | Records per `dispatch()` when no `limit` is given |
| `runInContext` | none | Wraps a whole run |

`dispatch({ limit, scope })` returns `{ fetched, published, failed }`.

## NestJS module options

`RabbitMqConsumerModule.register(options)`: every [consumer](#consumer) option,
plus:

| Option | Default | |
| --- | --- | --- |
| `connection` | required | [Connection](#connection) options |
| `handlers` | required | Message type to `lazyHandler(Module, Processor)`, a processor class, an instance, or an array of them |
| `logger` | console | `{ debug, info, warn, error }` |
| `autoStart` | `false` | Start consuming when the application boots |

`RabbitMqProducerModule.register(options)`:

| Option | Default | |
| --- | --- | --- |
| `connection` | required | |
| `exchanges` | required | Declared before the first publish |
| `defaultExchange` | the first of `exchanges` | |
| `outboxStore` | none | Required by `OutboxDispatcher`, not by `Publisher` |
| `batchSize` | `10` | |
| `runInContext` | none | |
| `confirmTimeoutMs` | `10000` | |
| `serializer`, `logger` | | As above |

Both modules have `registerAsync({ imports, inject, useFactory, extraProviders })`.

`MessageDestinationModule.register({ destinations, fallback })`.

## Environment variables

The library reads none on its own. `connectionOptionsFromEnv()` is a helper for
the usual names:

| Variable | Becomes |
| --- | --- |
| `RABBITMQ_DSN` | `url` |
| `RABBITMQ_USERNAME` | `username` |
| `RABBITMQ_PASSWORD` | `password` |
| `RABBITMQ_HEARTBEAT_INTERVAL` | `heartbeat` |

## Message headers

Exported as `MessageHeaders`.

| Header | Written by | Meaning |
| --- | --- | --- |
| `type` | you | Message type, read when the `type` property is absent |
| `redelivery_count` | the retry flow | Delayed retries so far |
| `retry_endpoint` | the retry flow | `appName` of the endpoint that scheduled the retry |
| `exception_details` | the error-queue move | Why it failed: `exception_type` (the error message), `stack_trace`, `failed_at`, one entry per failed handler |
| `endpoint` | the error-queue move | Where it failed and how to replay it: `name`, `delivery_metadata.exchange`, `delivery_metadata.routing_key`, `delivery_metadata.message_type` |

## Errors

All extend `MessageBusError`.

| Error | Thrown when |
| --- | --- |
| `ConfigurationError` | Options are invalid. Fix the configuration; retrying will not help |
| `ConnectionFailedError` | The broker was not reached within the reconnect policy |
| `PublishError` | The broker rejected or did not confirm a message |
| `UnrecoverableMessageError` | Throw it from a handler to skip the retries |
| `MessageParseError` | The content could not be deserialized. Treated as unrecoverable |
| `HandlerExecutionError` | One or more handlers failed; `failures` holds each error |

## Events

`ConnectionManager`:

| Event | Argument | |
| --- | --- | --- |
| `connected` | none | Also after every reconnect |
| `disconnected` | the error, if any | |
| `reconnecting` | `{ attempt, maxAttempts, delayMs, error }` | |
| `failed` | `ConnectionFailedError` | The reconnect policy is exhausted |

`Consumer`:

| Event | Argument | |
| --- | --- | --- |
| `failed` | the error | The subscription could not be restored after a reconnect. End the process |

`ConsumerWorker` (NestJS) emits one `failed` event for both cases: the broker
stayed unreachable, or the subscription could not be restored.

Next: [Troubleshooting](09-troubleshooting.md).

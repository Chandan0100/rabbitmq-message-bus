# rabbitmq-message-bus

Resilient RabbitMQ messaging for Node.js services, with a NestJS integration.

It gives a service the outbox and inbox patterns over RabbitMQ without tying it
to an ORM, a framework or an exchange type:

- **Publishing**: an outbox relay that publishes pending messages with broker
  confirmation and marks them as sent.
- **Consuming**: idempotent handlers, in-process retries, delayed retries
  through a retry queue, and an error queue for messages that cannot be
  processed.
- **Connection**: automatic reconnect, re-subscription, graceful shutdown.
- **Operations**: `dispatch-messages` and `handle-messages` worker commands,
  and a generator for their Kubernetes CronJob and Deployment manifests.

Works with `topic`, `direct` and `fanout` exchanges.

## See it work

The [playground](examples/demo/README.md) is a small task service with a dashboard:
publish events, make processors fail, cut the connection, switch exchange type,
and watch the queues, retries and error queue react.

```bash
npm install && npm run build
cd examples/demo && npm install
npm run broker && npm start      # http://localhost:4100
```

## Install

```bash
npm install rabbitmq-message-bus
```

| Import | Contents | Needs |
| --- | --- | --- |
| `rabbitmq-message-bus` | Connection, consumer, publisher, outbox relay, store interfaces | Node.js 20+ |
| `rabbitmq-message-bus/nestjs` | Modules, lazy processor loading, worker commands (re-exports the core) | `@nestjs/common`, `@nestjs/core` 10, 11 or 12 |
| `rabbitmq-message-bus/k8s` | Worker manifest generator, `rabbitmq-bus-k8s` binary | |

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

That declares the queue, its retry queue and the error queue; retries a failing
handler five times at once and three more times after 2, 4 and 6 seconds; moves
a message that still fails to the error queue with the reason attached; skips a
message it has already handled; and reconnects if the broker goes away.

## Documentation

The guides are also published as a website, built from the same files. See
[website/](website/README.md) for how to run and deploy it:

```bash
cd website && npm install && npm run dev      # http://localhost:5173
```

| Guide | |
| --- | --- |
| [1. Concepts](docs/01-concepts.md) | What happens to a message, and what the library promises |
| [2. Getting started](docs/02-getting-started.md) | One message through, with and without NestJS |
| [3. Consuming](docs/03-consuming.md) | Handlers, idempotency, retries, the error queue, replaying |
| [4. Publishing](docs/04-publishing.md) | The outbox, the dispatcher, destinations |
| [5. Exchange types](docs/05-exchange-types.md) | Topic, direct and fanout, and moving between them |
| [6. NestJS](docs/06-nestjs.md) | Consumer and producer modules, processors, several consumers |
| [7. Workers and deployment](docs/07-workers-and-deployment.md) | The worker commands, exit codes, graceful shutdown, Kubernetes manifests |
| [8. Configuration reference](docs/08-configuration-reference.md) | Every option, header, error and event |
| [9. Troubleshooting](docs/09-troubleshooting.md) | Log lines and errors explained |
| [10. Migrating padmasana-service](docs/10-migrating-padmasana-service.md) | Replacing that service's RabbitMQ layer with this package |

## Development

```bash
npm install
npm run typecheck
npm run test:unit

# Integration tests need a broker on port 5679 with the management plugin.
docker run -d --rm --name rmq-bus-pkg-test -p 5679:5672 -p 15679:15672 rabbitmq:4.1-management-alpine
npm run test:integration
docker stop rmq-bus-pkg-test

npm run build
```

`TEST_RABBITMQ_URL` and `TEST_RABBITMQ_MANAGEMENT_URL` point the integration
tests at another broker.

[HANDOFF.md](HANDOFF.md) records how the package was designed, what has been
verified and what has not.

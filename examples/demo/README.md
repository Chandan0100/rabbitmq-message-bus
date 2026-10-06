# Playground: a task service with a dashboard

A small NestJS service that uses `rabbitmq-message-bus` the way a real one
would, plus a browser dashboard that shows what the library is doing: the
outbox, the exchange, each queue and its retry queue, the processors, the error
queue and a live log.

Use it to see the behaviour before you write any code, then read the source to
see how little wiring it takes.

## Run it

Needs Node.js 22.13 or later (the demo stores its data with `node:sqlite`) and
Docker for the broker.

```bash
# from the repository root, once: build the library the demo installs
npm install && npm run build

cd examples/demo
npm install
npm run broker     # RabbitMQ on 5681, its console on http://localhost:15681 (guest / guest)
npm start          # the service and the dashboard on http://localhost:4100
```

The broker is a separate container on its own ports, so it does not touch a
RabbitMQ you already run. `npm run broker:down` removes it.

After changing the library, run `npm run library` here to rebuild and reinstall
it.

| Variable | Default | |
| --- | --- | --- |
| `PORT` | `4100` | Dashboard port |
| `RABBITMQ_DSN` | `amqp://guest:guest@localhost:5681` | Broker |
| `RABBITMQ_MANAGEMENT_URL` | `http://localhost:15681` | Used by "Cut the consumers' connections" |
| `DEMO_EXCHANGE_TYPE` | `topic` | Exchange type on first start |
| `DEMO_INLINE_WORKERS` | `true` | `false` to run the workers yourself, see below |
| `DEMO_DB` | `demo.sqlite` | The service's database file |

## What the service does

Three bounded contexts, as in a real service:

- **tasks** publishes. Creating, assigning or completing a task writes the
  change and its event to the database in one transaction. A dispatcher relays
  the outbox to RabbitMQ.
- **notifications** consumes `task_assigned` (an email and a push
  notification, two processors for one event) and `task_completed`.
- **audit** consumes every task event with one processor.

Each consumer has its own queue and retry queue. They share one error queue.

## Using the dashboard

Start with **Guided experiments**. Each sets the processors up, publishes, and
tells you what to watch:

| Experiment | What it shows |
| --- | --- |
| The happy path | Outbox, dispatch, one handling per processor |
| A glitch, retried at once | Immediate, in-process retries |
| An outage, retried later | Delayed retries through the retry queue, with a growing delay |
| One of two processors fails | Only the failed processor runs again; the inbox protects the other |
| A message that can never work | The error queue, the failure details it carries, and replaying |
| The same message twice | Idempotency: "Duplicate message ignored" |
| A lost connection | Reconnect and re-subscription |
| Another exchange type | Topic, direct and fanout: only the bindings change |

Then try things by hand. In the diagram, each processor has a menu that makes it
succeed, fail once, fail until the second delayed retry, always fail, throw an
unrecoverable error, or take three seconds. Select a row under **Messages** to
follow one message through the log.

Some things worth trying:

- Turn the dispatch timer off, create tasks, and watch them wait in the outbox.
- Stop a consumer, publish, and watch its queue fill; start it and watch it
  drain.
- Set a processor to "Takes 3 seconds", publish, and stop its consumer right
  away: it finishes the message first.
- Publish an event nobody handles under each exchange type. A topic or fanout
  queue receives and ignores it; a direct exchange has no binding for it and
  drops it.
- Delete the audit queue: the broker cancels the consumer, which declares the
  queue again and carries on.

## Where to look in the code

The dashboard has the same table at the bottom of the page.

| What | File |
| --- | --- |
| A change and its event in one transaction | [src/tasks/tasks.service.ts](src/tasks/tasks.service.ts) |
| The outbox store | [src/infrastructure/sqlite-outbox.store.ts](src/infrastructure/sqlite-outbox.store.ts) |
| The producer module | [src/tasks/tasks.producer.module.ts](src/tasks/tasks.producer.module.ts) |
| Exchanges, queues and keys per exchange type | [src/infrastructure/messaging.config.ts](src/infrastructure/messaging.config.ts) |
| A consumer module | [src/notifications/notifications.consumer.module.ts](src/notifications/notifications.consumer.module.ts) |
| Which processor handles which event | [src/notifications/notifications.handlers.ts](src/notifications/notifications.handlers.ts) |
| A processor | [src/notifications/processors/send-task-assigned-email.processor.ts](src/notifications/processors/send-task-assigned-email.processor.ts) |
| The inbox store | [src/infrastructure/sqlite-inbox.store.ts](src/infrastructure/sqlite-inbox.store.ts) |
| The worker commands | [src/message-bus-cli.ts](src/message-bus-cli.ts) |
| The worker manifests | [workers.yml](workers.yml) |

Everything marked `DEMO ONLY` in the source exists for the dashboard: the fault
injector, the broker inspector, the in-process runtime and the event log table.
A real service has none of it.

## Running the workers as separate processes

`npm start` runs the consumers and the dispatcher inside the API process so the
demo is one command. In production they are separate processes, and the demo can
run that way too:

```bash
npm run start:api                                    # terminal 1: API and dashboard only
npm run handle-messages -- --module notifications    # terminal 2
npm run handle-messages -- --module audit            # terminal 3
npm run dispatch-messages -- --module tasks          # terminal 4, once per batch
```

The dashboard shows the workers as "external worker" on each queue, and their
log lines appear in the live log because every process writes to the same
database. Turn the dispatch timer off in the dashboard if you want
`dispatch-messages` to be the only dispatcher. Press Ctrl+C in a worker to see
it finish its in-flight messages and close its connection.

## The Kubernetes manifests

```bash
npm run manifests
```

writes a CronJob for the dispatcher and a Deployment per consumer to `k8s/`,
from [workers.yml](workers.yml).

---
title: Playground
---

<script setup>
import { useData } from 'vitepress';
const { theme } = useData();
</script>

# Playground

The playground is a small task service that uses the library the way a real one
would, with a dashboard that shows what the library is doing: the outbox, the
exchange, each queue and its retry queue, the processors, the error queue and a
live log.

Use it to see the behaviour before you write any code, then read its source to
see how little wiring it takes.

<img class="screenshot light" src="/playground-light.png" alt="The playground dashboard, light theme">
<img class="screenshot dark" src="/playground-dark.png" alt="The playground dashboard, dark theme">

## Run it

It runs on your machine, from a checkout of the repository. It needs Node.js
22.13 or later and Docker.

```bash
# from the repository root, once: build the library the playground installs
npm install && npm run build

cd examples/demo
npm install
npm run broker     # RabbitMQ on 5681, its console on http://localhost:15681 (guest / guest)
npm start          # the playground on http://localhost:4100
```

<p><a class="VPButton medium brand" :href="theme.playgroundUrl" target="_blank" rel="noopener" style="text-decoration: none; display: inline-block; padding: 0 20px; line-height: 38px; border-radius: 20px; color: var(--vp-button-brand-text); background: var(--vp-button-brand-bg); font-weight: 600;">Open the playground</a></p>

The broker is a separate container on its own ports, so it does not touch a
RabbitMQ you already run. `npm run broker:down` removes it.

## What it contains

Three bounded contexts, as in a real service:

- **tasks** publishes. Creating, assigning or completing a task writes the
  change and its event to the database in one transaction. A dispatcher relays
  the outbox to RabbitMQ.
- **notifications** consumes `task_assigned` (an email and a push notification,
  two processors for one event) and `task_completed`.
- **audit** consumes every task event with one processor.

Each consumer has its own queue and retry queue. They share one error queue.

## What to try

Start with **Guided experiments**. Each one sets the processors up, publishes,
and tells you what to watch.

| Experiment | What it shows | Read more |
| --- | --- | --- |
| The happy path | Events cross the outbox and each processor handles them once | [Concepts](/guide/concepts) |
| A glitch, retried at once | A processor fails, then succeeds in the same delivery | [Retries](/guide/consuming#retries) |
| An outage, retried later | The retry queue holds the message for 2 seconds, then 4 | [Retries](/guide/consuming#retries) |
| One of two processors fails | Only the failed one runs again; the other is not repeated | [Idempotency](/guide/consuming#idempotency) |
| A message that can never work | It lands in the error queue, from where you replay it | [The error queue](/guide/consuming#the-error-queue) |
| The same message twice | The inbox ignores the duplicate | [Idempotency](/guide/consuming#idempotency) |
| A lost connection | Both consumers reconnect and resume | [Troubleshooting](/guide/troubleshooting) |
| Another exchange type | The binding keys change; processors and retries do not | [Exchange types](/guide/exchange-types) |

On the dashboard:

- **Dots** travel along the connectors when messages move, and a step is ringed
  when its numbers change.
- **Each processor has a menu** that makes it succeed, fail once, fail for a
  while, always fail, or run slowly.
- **The live log** is a terminal that follows every process. Select a message in
  the table to see only its lines.
- **The theme switch** in the top bar chooses light, dark, or the system's.

## Run the workers as separate processes

By default the consumers and the dispatcher run inside the playground. To run
them the way production does, with the real worker commands:

```bash
DEMO_INLINE_WORKERS=false npm start

# each in its own terminal
npm run handle-messages -- --module notifications
npm run handle-messages -- --module audit
npm run dispatch-messages -- --module tasks
```

See [Workers and deployment](/guide/workers-and-deployment) for what these
commands do.

## Settings

| Variable | Default | |
| --- | --- | --- |
| `PORT` | `4100` | Playground port |
| `RABBITMQ_DSN` | `amqp://guest:guest@localhost:5681` | Broker |
| `RABBITMQ_MANAGEMENT_URL` | `http://localhost:15681` | Used by "Cut the consumers' connections" |
| `DEMO_EXCHANGE_TYPE` | `topic` | Exchange type on first start |
| `DEMO_INLINE_WORKERS` | `true` | `false` to run the workers yourself |
| `DEMO_DB` | `demo.sqlite` | The service's database file |

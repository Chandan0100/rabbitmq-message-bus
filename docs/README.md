# Using rabbitmq-message-bus

These guides take you from nothing to a service that publishes and consumes
messages reliably, and then to running it in production.

If you would rather see it work first, start the [playground](../examples/demo/README.md):
a small service with a dashboard that shows every queue, retry and failure as it
happens.

## Read in this order the first time

| Guide | Read it to |
| --- | --- |
| [1. Concepts](01-concepts.md) | Understand what happens to a message, and what the library promises |
| [2. Getting started](02-getting-started.md) | Install it and get one message through, with and without NestJS |
| [3. Consuming](03-consuming.md) | Write handlers, make them idempotent, tune retries, deal with failed messages |
| [4. Publishing](04-publishing.md) | Publish through an outbox, or directly |
| [5. Exchange types](05-exchange-types.md) | Choose and configure topic, direct or fanout routing |
| [6. NestJS](06-nestjs.md) | Wire consumers, producers and processors into NestJS modules |
| [7. Workers and deployment](07-workers-and-deployment.md) | Run the worker commands and generate their Kubernetes manifests |

## Come back to these

| Guide | Use it when |
| --- | --- |
| [8. Configuration reference](08-configuration-reference.md) | You need the exact name, default and meaning of an option |
| [9. Troubleshooting](09-troubleshooting.md) | A log line or an error needs explaining |
| [10. Migrating padmasana-service](10-migrating-padmasana-service.md) | You are replacing that service's own RabbitMQ layer with this package |

## The three entry points

```ts
import { Consumer, Publisher } from 'rabbitmq-message-bus'; // framework-free core
import { RabbitMqConsumerModule } from 'rabbitmq-message-bus/nestjs'; // NestJS modules and worker CLI
import { generateWorkerManifests } from 'rabbitmq-message-bus/k8s'; // Kubernetes manifests
```

`rabbitmq-message-bus/nestjs` re-exports the core, so a NestJS service imports
everything from there.

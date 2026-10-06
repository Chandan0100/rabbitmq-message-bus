import 'reflect-metadata';
import { Injectable, Module } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import {
  DestinationRegistry,
  EventPayload,
  fixedBackoff,
  InMemoryInboxStore,
  InMemoryOutboxStore,
  MessageHandler,
  OutboxRecord,
  routeByMessageType,
  silentLogger,
} from '../../src';
import {
  ConsumerWorker,
  dispatchOutbox,
  lazyHandler,
  MessageDestinationModule,
  Publisher,
  RabbitMqConsumerModule,
  RabbitMqProducerModule,
  runMessageBusCli,
  startConsumerWorker,
} from '../../src/nestjs';
import {
  BROKER_URL,
  BrokerAdmin,
  createNames,
  TestNames,
  waitFor,
} from '../support/broker';

const TASK_CREATED = 'bus-test.task_created';
const TASK_REMOVED = 'bus-test.task_removed';

// What a real service keeps in its own modules: a feature, its processor and stores.
const handled: { handler: string; message: EventPayload }[] = [];

@Injectable()
class ProjectionService {
  project(handler: string, message: EventPayload) {
    handled.push({ handler, message });
  }
}

@Injectable()
class TaskCreatedProcessor implements MessageHandler {
  constructor(private readonly projection: ProjectionService) {}

  getHandlerName() {
    return this.constructor.name;
  }

  async handle(message: EventPayload) {
    this.projection.project(this.getHandlerName(), message);
  }
}

@Module({
  providers: [ProjectionService, TaskCreatedProcessor],
  exports: [TaskCreatedProcessor],
})
class TaskCreatedProcessorModule {}

@Injectable()
class TaskRemovedProcessor implements MessageHandler {
  constructor(private readonly destinations: DestinationRegistry) {}

  getHandlerName() {
    return this.constructor.name;
  }

  async handle(message: EventPayload) {
    // Proves a lazily loaded module sees the application-wide registry.
    handled.push({
      handler: this.destinations.resolve(TASK_REMOVED).routingKey,
      message,
    });
  }
}

@Module({ providers: [TaskRemovedProcessor] })
class TaskRemovedProcessorModule {}

@Injectable()
class TestInboxStore extends InMemoryInboxStore {}

describe('NestJS modules and worker CLI against a real broker', () => {
  let admin: BrokerAdmin;
  let names: TestNames;
  let outbox: InMemoryOutboxStore;
  let ConsumerAppModule: any;
  let ProducerAppModule: any;

  const record = (messageId: string, type: string): OutboxRecord => ({
    messageId,
    type,
    exchange: names.exchange,
    routingKey: type,
    body: { uuid: messageId },
  });

  beforeAll(async () => {
    admin = await BrokerAdmin.connect();
  });

  afterAll(async () => {
    await admin.close();
  });

  beforeEach(() => {
    names = createNames();
    handled.length = 0;
    outbox = new InMemoryOutboxStore();
    const connection = {
      url: BROKER_URL,
      reconnect: { maxAttempts: 1, delayMs: 100 },
    };
    const exchange = { name: names.exchange, type: 'direct' as const };

    @Module({
      imports: [
        MessageDestinationModule.register({
          fallback: routeByMessageType(names.exchange),
        }),
        RabbitMqConsumerModule.registerAsync({
          inject: [TestInboxStore],
          extraProviders: [TestInboxStore],
          useFactory: (store: TestInboxStore) => ({
            connection,
            logger: silentLogger,
            inbox: { store },
            handlers: {
              [TASK_CREATED]: lazyHandler(
                TaskCreatedProcessorModule,
                TaskCreatedProcessor,
              ),
              [TASK_REMOVED]: [
                lazyHandler(TaskRemovedProcessorModule, TaskRemovedProcessor),
              ],
            },
            endpoint: {
              appName: 'bus-test',
              queue: names.queue,
              exchange,
              bindHandledTypes: true,
              retry: {
                exchange: { name: names.retryExchange, type: 'direct' },
                backoff: fixedBackoff(100),
              },
              error: {
                exchange: { name: names.retryExchange, type: 'direct' },
                routingKey: names.errorRoutingKey,
                queue: names.errorQueue,
              },
            },
          }),
        }),
      ],
    })
    class TestConsumerModule {}

    @Module({
      imports: [
        RabbitMqProducerModule.register({
          connection,
          logger: silentLogger,
          exchanges: [exchange],
          outboxStore: outbox,
        }),
      ],
    })
    class TestProducerModule {}

    ConsumerAppModule = TestConsumerModule;
    ProducerAppModule = TestProducerModule;
  });

  afterEach(async () => {
    await admin.cleanup(names);
    await admin.cleanup({ ...names, queue: `${names.queue}.audit`, retryQueue: `${names.queue}.audit.events-retry` });
  });

  it('dispatches an outbox and handles it with lazily loaded processors', async () => {
    const app = await startConsumerWorker(ConsumerAppModule, { nestLogger: false });
    const worker = app.get(ConsumerWorker);
    expect(worker.isConsuming()).toBe(true);

    outbox.add(
      record('m-1', TASK_CREATED),
      record('m-2', TASK_REMOVED),
      record('m-3', TASK_CREATED),
    );
    expect(
      await dispatchOutbox(ProducerAppModule, { limit: 2, nestLogger: false }),
    ).toEqual({ fetched: 2, published: 2, failed: 0 });
    expect(
      await dispatchOutbox(ProducerAppModule, { nestLogger: false }),
    ).toEqual({ fetched: 1, published: 1, failed: 0 });

    await waitFor(() => handled.length === 3);
    expect(handled.map((entry) => [entry.handler, entry.message.messageId])).toEqual(
      expect.arrayContaining([
        ['TaskCreatedProcessor', 'm-1'],
        [TASK_REMOVED, 'm-2'],
        ['TaskCreatedProcessor', 'm-3'],
      ]),
    );

    await app.close();
    expect(worker.isConsuming()).toBe(false);
  });

  it('applies the limit, scope and topology overrides given at start', async () => {
    const app = await startConsumerWorker(ConsumerAppModule, {
      nestLogger: false,
      limit: 3,
      scope: 'tenant_a',
      endpoint: { queue: `${names.queue}.audit` },
    });
    const worker = app.get(ConsumerWorker);

    expect(worker.getEndpoint()).toMatchObject({
      queue: `${names.queue}.audit`,
      prefetch: 3,
      retry: { queue: `${names.queue}.audit.events-retry` },
    });

    await admin.publish(names.exchange, TASK_CREATED, {}, {
      messageId: 'm-1',
      type: TASK_CREATED,
    });
    await waitFor(() => handled.length === 1);
    await waitFor(() =>
      app.get(TestInboxStore).isHandled('m-1', 'TaskCreatedProcessor', 'tenant_a'),
    );

    await app.close();
  });

  it('starts consuming with the application when autoStart is set', async () => {
    const handler: MessageHandler = {
      getHandlerName: () => 'Inline',
      handle: async (message) => {
        handled.push({ handler: 'Inline', message });
      },
    };

    @Module({
      imports: [
        RabbitMqConsumerModule.register({
          connection: { url: BROKER_URL },
          logger: silentLogger,
          autoStart: true,
          handlers: { [TASK_CREATED]: handler },
          endpoint: {
            appName: 'bus-test',
            queue: names.queue,
            exchange: { name: names.exchange, type: 'fanout' },
            retry: { exchange: { name: names.retryExchange, type: 'direct' } },
            error: {
              exchange: { name: names.retryExchange, type: 'direct' },
              routingKey: names.errorRoutingKey,
            },
          },
        }),
        RabbitMqProducerModule.register({
          connection: { url: BROKER_URL },
          logger: silentLogger,
          exchanges: [{ name: names.exchange, type: 'fanout' }],
        }),
      ],
    })
    class InProcessModule {}

    const app = await NestFactory.createApplicationContext(InProcessModule, {
      logger: false,
    });
    expect(app.get(ConsumerWorker).isConsuming()).toBe(true);

    await app.get(Publisher).publish({
      exchange: names.exchange,
      routingKey: '',
      body: { direct: true },
      properties: { messageId: 'm-1', type: TASK_CREATED },
    });

    await waitFor(() => handled.length === 1);
    expect(handled[0].message.body).toEqual({ direct: true });
    await app.close();
  });

  describe('runMessageBusCli', () => {
    let exit: jest.SpyInstance;
    let errors: jest.SpyInstance;

    beforeEach(() => {
      exit = jest.spyOn(process, 'exit').mockImplementation(((code?: number) => {
        throw new Error(`exit ${code}`);
      }) as never);
      errors = jest.spyOn(console, 'error').mockImplementation(() => undefined);
    });

    afterEach(() => {
      exit.mockRestore();
      errors.mockRestore();
    });

    const run = (argv: string[], scopes?: Record<string, string | undefined>) =>
      runMessageBusCli({
        argv,
        scopes,
        nestLogger: false,
        consumers: { tasks: ConsumerAppModule },
        producers: { tasks: ProducerAppModule },
      }).catch((error) => error.message);

    it('dispatches one batch and exits successfully', async () => {
      const fetchPending = jest.spyOn(outbox, 'fetchPending');
      outbox.add(record('m-1', TASK_CREATED));

      expect(
        await run(
          ['dispatch-messages', '--module=tasks', '--limit', '7', '--schema=tasks'],
          { tasks: 'task_management' },
        ),
      ).toBe('exit 0');

      expect(fetchPending).toHaveBeenCalledWith(7, 'task_management');
      expect(outbox.sent).toHaveLength(1);
    });

    it('exits with an error for an unknown module, naming the available ones', async () => {
      expect(await run(['handle-messages', '--module', 'billing'])).toBe('exit 1');
      expect(errors.mock.calls[0][0]).toBe(
        'ERROR: Invalid or missing --module. Available modules: tasks',
      );
    });

    it('exits with an error for a schema that is not mapped', async () => {
      expect(
        await run(['dispatch-messages', '--module', 'tasks', '--schema', 'other'], {
          tasks: 'task_management',
        }),
      ).toBe('exit 1');
      expect(errors.mock.calls[0][0]).toMatch(/Invalid schema name "other"/);
    });

    it('exits with an error when the broker cannot be reached', async () => {
      @Module({
        imports: [
          RabbitMqProducerModule.register({
            connection: {
              url: 'amqp://localhost:5999',
              reconnect: { maxAttempts: 1, delayMs: 50 },
            },
            logger: silentLogger,
            exchanges: [{ name: names.exchange, type: 'direct' }],
            outboxStore: outbox,
          }),
        ],
      })
      class UnreachableModule {}
      outbox.add(record('m-1', TASK_CREATED));

      const outcome = await runMessageBusCli({
        argv: ['dispatch-messages', '--module', 'tasks'],
        nestLogger: false,
        producers: { tasks: UnreachableModule },
      }).catch((error) => error.message);

      expect(outcome).toBe('exit 1');
      expect(errors.mock.calls[0][0]).toMatch(/Could not connect to RabbitMQ/);
      expect(outbox.getPending()).toHaveLength(1);
    });
  });
});

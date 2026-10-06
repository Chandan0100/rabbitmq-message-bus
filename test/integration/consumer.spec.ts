import { AsyncLocalStorage } from 'node:async_hooks';
import {
  ConnectionManager,
  Consumer,
  ConsumerOptions,
  EventPayload,
  HandlerMap,
  HandlerRegistry,
  InMemoryInboxStore,
  JsonMessageSerializer,
  MessageHandler,
  MessageHeaders,
  UnrecoverableMessageError,
} from '../../src';
import {
  BrokerAdmin,
  createConnection,
  createNames,
  endpointFor,
  sleep,
  TestNames,
  waitFor,
} from '../support/broker';
import { createTestLogger, TestLogger } from '../support/test-logger';

const TYPE = 'bus-test.task_created';

function recordingHandler(
  name: string,
  behaviour: (message: EventPayload, call: number) => void | Promise<void> = () =>
    undefined,
): MessageHandler & { calls: EventPayload[] } {
  const calls: EventPayload[] = [];
  return {
    calls,
    getHandlerName: () => name,
    handle: async (message) => {
      calls.push(message);
      await behaviour(message, calls.length);
    },
  };
}

describe('Consumer against a real broker', () => {
  let admin: BrokerAdmin;
  let names: TestNames;
  let logger: TestLogger;
  let connection: ConnectionManager;
  let consumer: Consumer | undefined;

  async function startConsumer(
    handlers: HandlerMap,
    options: Partial<ConsumerOptions> = {},
  ): Promise<Consumer> {
    consumer = new Consumer(
      connection,
      new HandlerRegistry(handlers),
      { endpoint: endpointFor(names), ...options },
      logger,
    );
    await consumer.start();
    return consumer;
  }

  const properties = (messageId: string, type = TYPE) => ({
    messageId,
    type,
    headers: { type },
  });

  beforeAll(async () => {
    admin = await BrokerAdmin.connect();
  });

  afterAll(async () => {
    await admin.close();
  });

  beforeEach(() => {
    names = createNames();
    logger = createTestLogger();
    connection = createConnection(logger);
  });

  afterEach(async () => {
    await consumer?.stop();
    consumer = undefined;
    await connection.close();
    await admin.cleanup(names);
  });

  describe('routing', () => {
    it('receives messages matching the pattern of a topic exchange', async () => {
      const handler = recordingHandler('Handler');
      await startConsumer({ [TYPE]: handler });

      await admin.publish(
        names.exchange,
        'bus-test.tasks.created',
        { task_uuid: 't-1' },
        properties('m-1'),
      );

      await waitFor(() => handler.calls.length === 1);
      expect(handler.calls[0]).toMatchObject({
        messageId: 'm-1',
        type: TYPE,
        body: { task_uuid: 't-1' },
      });
      await waitFor(async () => (await admin.messageCount(names.queue)) === 0);
    });

    it('binds one exact key per handled type on a direct exchange', async () => {
      const created = recordingHandler('Created');
      const removed = recordingHandler('Removed');
      await startConsumer(
        { 'bus-test.task_created': created, 'bus-test.task_removed': removed },
        {
          endpoint: endpointFor(names, {
            exchange: { name: names.exchange, type: 'direct' },
            bindingKeys: undefined,
            bindHandledTypes: true,
          }),
        },
      );

      await admin.publish(names.exchange, 'bus-test.task_created', {}, properties('m-1', 'bus-test.task_created'));
      await admin.publish(names.exchange, 'bus-test.task_removed', {}, properties('m-2', 'bus-test.task_removed'));
      // No queue is bound with this key, so the direct exchange drops it.
      await admin.publish(names.exchange, 'bus-test.task_archived', {}, properties('m-3', 'bus-test.task_created'));

      await waitFor(() => created.calls.length === 1 && removed.calls.length === 1);
      await sleep(200);
      expect(created.calls.map((call) => call.messageId)).toEqual(['m-1']);
      expect(removed.calls.map((call) => call.messageId)).toEqual(['m-2']);
    });

    it('receives everything published to a fanout exchange', async () => {
      const handler = recordingHandler('Handler');
      await startConsumer(
        { [TYPE]: handler },
        {
          endpoint: endpointFor(names, {
            exchange: { name: names.exchange, type: 'fanout' },
            bindingKeys: undefined,
          }),
        },
      );

      await admin.publish(names.exchange, 'whatever', {}, properties('m-1'));

      await waitFor(() => handler.calls.length === 1);
    });
  });

  describe('messages it should not handle', () => {
    it('acknowledges and skips a message without id, type or handler', async () => {
      const handler = recordingHandler('Handler');
      await startConsumer({ [TYPE]: handler });

      await admin.publish(names.exchange, 'bus-test.a', {}, { type: TYPE });
      await admin.publish(names.exchange, 'bus-test.b', {}, { messageId: 'm-2' });
      await admin.publish(names.exchange, 'bus-test.c', {}, properties('m-3', 'bus-test.unknown'));
      await admin.publish(names.exchange, 'bus-test.d', {}, properties('m-4'));

      await waitFor(() => handler.calls.length === 1);
      expect(handler.calls[0].messageId).toBe('m-4');
      await waitFor(async () => (await admin.messageCount(names.queue)) === 0);
      expect(logger.messages('info')).toEqual(
        expect.arrayContaining([
          'Message ignored: Message does not have a messageId.',
          'Message ignored: No available handler found or missing message type property.',
        ]),
      );
    });

    it('skips a retried message that belongs to another endpoint', async () => {
      const handler = recordingHandler('Handler');
      await startConsumer({ [TYPE]: handler });

      await admin.publish(names.exchange, 'bus-test.a', {}, {
        messageId: 'm-1',
        type: TYPE,
        headers: {
          [MessageHeaders.REDELIVERY_COUNT]: 1,
          [MessageHeaders.RETRY_ENDPOINT]: 'another-service',
        },
      });
      await admin.publish(names.exchange, 'bus-test.a', {}, properties('m-2'));

      await waitFor(() => handler.calls.length === 1);
      expect(handler.calls[0].messageId).toBe('m-2');
    });
  });

  describe('recoverability', () => {
    it('retries in-process before involving the broker', async () => {
      const handler = recordingHandler('Flaky', (_message, call) => {
        if (call <= 2) throw new Error('transient');
      });
      await startConsumer(
        { [TYPE]: handler },
        { endpoint: endpointFor(names, { retry: { immediateRetries: 2 } as never }) },
      );

      await admin.publish(names.exchange, 'bus-test.a', {}, properties('m-1'));

      await waitFor(() => handler.calls.length === 3);
      await waitFor(async () => (await admin.messageCount(names.queue)) === 0);
      expect(handler.calls[2].headers?.[MessageHeaders.REDELIVERY_COUNT]).toBeUndefined();
      expect(await admin.messageCount(names.retryQueue)).toBe(0);
    });

    it('parks a failed message in the retry queue and handles it when it comes back', async () => {
      const receivedAt: number[] = [];
      const handler = recordingHandler('Flaky', (_message, call) => {
        receivedAt.push(Date.now());
        if (call === 1) throw new Error('transient');
      });
      await startConsumer({ [TYPE]: handler });

      await admin.publish(names.exchange, 'bus-test.a', { n: 1 }, properties('m-1'));

      await waitFor(() => handler.calls.length === 2);
      expect(handler.calls[1].body).toEqual({ n: 1 });
      expect(handler.calls[1].headers).toMatchObject({
        [MessageHeaders.REDELIVERY_COUNT]: 1,
        [MessageHeaders.RETRY_ENDPOINT]: 'bus-test',
      });
      expect(receivedAt[1] - receivedAt[0]).toBeGreaterThanOrEqual(140);
      await waitFor(async () => (await admin.messageCount(names.queue)) === 0);
    });

    it('moves the message to the error queue once the delayed retries are exhausted', async () => {
      const handler = recordingHandler('Broken', () => {
        throw new Error('always fails');
      });
      await startConsumer(
        { [TYPE]: handler },
        { endpoint: endpointFor(names, { retry: { immediateRetries: 1 } as never }) },
      );

      await admin.publish(names.exchange, 'bus-test.a', { n: 1 }, {
        ...properties('m-1'),
        appId: 'publisher',
      });

      const failed = await admin.waitForMessage(names.errorQueue);
      // (1 immediate retry + 1) attempts on each of (2 delayed retries + 1) deliveries.
      expect(handler.calls).toHaveLength(6);
      expect(JSON.parse(failed.content.toString())).toEqual({ n: 1 });
      expect(failed.properties.messageId).toBe('m-1');
      expect(failed.properties.appId).toBe('publisher');
      expect(failed.properties.expiration).toBeUndefined();

      const headers = failed.properties.headers!;
      expect(headers[MessageHeaders.REDELIVERY_COUNT]).toBe(2);
      expect(headers[MessageHeaders.EXCEPTION_DETAILS]).toHaveLength(1);
      expect(headers[MessageHeaders.EXCEPTION_DETAILS][0]).toMatchObject({
        exception_type: 'always fails',
        stack_trace: expect.stringContaining('always fails'),
        failed_at: expect.any(String),
      });
      expect(headers[MessageHeaders.ENDPOINT]).toEqual({
        name: names.queue,
        delivery_metadata: {
          message_type: TYPE,
          exchange: names.retryExchange,
          routing_key: `${names.queue}.retry`,
        },
      });
      await waitFor(async () => (await admin.messageCount(names.queue)) === 0);
    });

    it('sends an unrecoverable failure to the error queue without retrying', async () => {
      const handler = recordingHandler('Poison', () => {
        throw new UnrecoverableMessageError('schema mismatch');
      });
      await startConsumer({ [TYPE]: handler });

      await admin.publish(names.exchange, 'bus-test.a', {}, properties('m-1'));

      const failed = await admin.waitForMessage(names.errorQueue);
      expect(handler.calls).toHaveLength(1);
      expect(failed.properties.headers![MessageHeaders.REDELIVERY_COUNT]).toBeUndefined();
    });

    it('sends unreadable content to the error queue untouched', async () => {
      const handler = recordingHandler('Handler');
      await startConsumer(
        { [TYPE]: handler },
        { serializer: new JsonMessageSerializer({ repair: false }) },
      );

      await admin.publish(names.exchange, 'bus-test.a', '{not json', properties('m-1'));

      const failed = await admin.waitForMessage(names.errorQueue);
      expect(failed.content.toString()).toBe('{not json');
      expect(handler.calls).toHaveLength(0);
    });

    it('reports a failed message that no queue is bound to receive', async () => {
      const handler = recordingHandler('Poison', () => {
        throw new UnrecoverableMessageError('schema mismatch');
      });
      await startConsumer(
        { [TYPE]: handler },
        {
          endpoint: endpointFor(names, {
            error: {
              exchange: { name: names.retryExchange, type: 'direct' },
              routingKey: names.errorRoutingKey,
            },
          }),
        },
      );

      await admin.publish(names.exchange, 'bus-test.a', {}, properties('m-1'));

      await waitFor(() =>
        logger.messages('error').some((line) => /was not routed to any queue/.test(line)),
      );
    });
  });

  describe('idempotency', () => {
    it('handles a message delivered twice only once', async () => {
      const handler = recordingHandler('Handler');
      await startConsumer(
        { [TYPE]: handler },
        { inbox: { store: new InMemoryInboxStore() } },
      );

      await admin.publish(names.exchange, 'bus-test.a', {}, properties('m-1'));
      await waitFor(() => handler.calls.length === 1);
      await admin.publish(names.exchange, 'bus-test.a', {}, properties('m-1'));
      await admin.publish(names.exchange, 'bus-test.a', {}, properties('m-2'));

      await waitFor(() => handler.calls.length === 2);
      expect(handler.calls.map((call) => call.messageId)).toEqual(['m-1', 'm-2']);
    });

    it('does not run a handler again when only its sibling failed', async () => {
      const good = recordingHandler('Good');
      const flaky = recordingHandler('Flaky', (_message, call) => {
        if (call === 1) throw new Error('transient');
      });
      await startConsumer(
        { [TYPE]: [good, flaky] },
        { inbox: { store: new InMemoryInboxStore() } },
      );

      await admin.publish(names.exchange, 'bus-test.a', {}, properties('m-1'));

      await waitFor(() => flaky.calls.length === 2);
      await waitFor(async () => (await admin.messageCount(names.queue)) === 0);
      expect(good.calls).toHaveLength(1);
    });

    it('trusts the handler to record the message when configured that way', async () => {
      const store = new InMemoryInboxStore();
      const handler: MessageHandler & { calls: number } = {
        calls: 0,
        getHandlerName: () => 'Transactional',
        async handle(message) {
          this.calls += 1;
          await store.markHandled(message.messageId, this.getHandlerName());
        },
      };
      await startConsumer({ [TYPE]: handler }, { inbox: { store, record: 'handler' } });

      await admin.publish(names.exchange, 'bus-test.a', {}, properties('m-1'));
      await waitFor(() => handler.calls === 1);
      await admin.publish(names.exchange, 'bus-test.a', {}, properties('m-1'));

      await waitFor(() =>
        logger.messages('info').some((line) => /Duplicate message ignored/.test(line)),
      );
      expect(handler.calls).toBe(1);
    });
  });

  it('takes over a topology declared by the original service and a message it left in the retry queue', async () => {
    const original = {
      primaryQueue: names.queue,
      primaryQueueBindingKey: 'bus-test.#',
      primaryQueueExchange: names.exchange,
      primaryQueueRetryBindingKey: `${names.queue}.retry`,
      retryQueue: names.retryQueue,
      retryQueueBindingKey: names.retryQueue,
      retryQueueExchange: names.retryExchange,
      retryQueueMessageTtl: 2000,
    };
    await admin.declareLikeOriginalService(original);
    // What the original service publishes when it schedules a delayed retry.
    await admin.publish(original.retryQueueExchange, original.retryQueueBindingKey, { n: 1 }, {
      messageId: 'm-1',
      type: TYPE,
      persistent: true,
      expiration: '200',
      headers: {
        type: TYPE,
        [MessageHeaders.REDELIVERY_COUNT]: 1,
        [MessageHeaders.RETRY_ENDPOINT]: 'bus-test',
      },
    });

    const handler = recordingHandler('Handler');
    await startConsumer(
      { [TYPE]: handler },
      {
        endpoint: endpointFor(names, {
          retry: {
            queue: original.retryQueue,
            queueBindingKey: original.retryQueueBindingKey,
            returnBindingKey: original.primaryQueueRetryBindingKey,
            queueMessageTtl: original.retryQueueMessageTtl,
          } as never,
          error: {
            exchange: { name: names.retryExchange, type: 'direct' },
            routingKey: names.errorRoutingKey,
          },
        }),
      },
    );

    await waitFor(() => handler.calls.length === 1);
    expect(handler.calls[0]).toMatchObject({ messageId: 'm-1', body: { n: 1 } });
    expect(logger.messages('error')).toEqual([]);
  });

  it('handles each message inside the context it is given, inbox check included', async () => {
    const context = new AsyncLocalStorage<{ unitOfWork: number }>();
    let unitsOfWork = 0;
    const seen: (number | undefined)[] = [];
    const store = new InMemoryInboxStore();
    const isHandled = store.isHandled.bind(store);
    store.isHandled = async (...args) => {
      seen.push(context.getStore()?.unitOfWork);
      return isHandled(...args);
    };
    const handler = recordingHandler('Handler', () => {
      seen.push(context.getStore()?.unitOfWork);
    });
    await startConsumer(
      { [TYPE]: handler },
      {
        inbox: { store },
        runInContext: (work) => context.run({ unitOfWork: ++unitsOfWork }, work),
      },
    );

    await admin.publish(names.exchange, 'bus-test.a', {}, properties('m-1'));
    await waitFor(() => handler.calls.length === 1);
    await admin.publish(names.exchange, 'bus-test.a', {}, properties('m-2'));
    await waitFor(() => handler.calls.length === 2);

    expect(seen).toEqual([1, 1, 2, 2]);
  });

  it('fails at startup when a handler cannot be resolved', async () => {
    const registry = new HandlerRegistry({
      [TYPE]: async () => {
        throw new Error('processor module is broken');
      },
    });
    const broken = new Consumer(connection, registry, { endpoint: endpointFor(names) }, logger);

    await expect(broken.start()).rejects.toThrow('processor module is broken');
    expect(broken.isConsuming()).toBe(false);
  });
});

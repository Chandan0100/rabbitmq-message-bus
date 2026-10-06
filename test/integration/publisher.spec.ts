import {
  ConnectionManager,
  Consumer,
  DestinationRegistry,
  EventPayload,
  HandlerRegistry,
  InMemoryInboxStore,
  InMemoryOutboxStore,
  OutboxRecord,
  OutboxRelay,
  Publisher,
  PublishError,
  routeByMessageType,
} from '../../src';
import {
  BrokerAdmin,
  createConnection,
  createNames,
  endpointFor,
  TestNames,
  waitFor,
} from '../support/broker';
import { createTestLogger, TestLogger } from '../support/test-logger';

describe('Publisher and OutboxRelay against a real broker', () => {
  let admin: BrokerAdmin;
  let names: TestNames;
  let logger: TestLogger;
  let connection: ConnectionManager;
  let publisher: Publisher;
  let consumer: Consumer | undefined;

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
    publisher = new Publisher(
      connection,
      { exchanges: [{ name: names.exchange, type: 'topic' }] },
      logger,
    );
  });

  afterEach(async () => {
    await consumer?.stop();
    consumer = undefined;
    await publisher.close();
    await connection.close();
    await admin.cleanup(names);
  });

  it('declares its exchange and publishes a persistent, confirmed message', async () => {
    await publisher.publish({
      exchange: names.exchange,
      routingKey: 'bus-test.created',
      body: { n: 0 },
    });
    await admin.bindQueue(names.queue, names.exchange, 'bus-test.#');

    await publisher.publish({
      exchange: names.exchange,
      routingKey: 'bus-test.created',
      body: { n: 1 },
      properties: { messageId: 'm-1', type: 'bus-test.created' },
    });

    const message = await admin.waitForMessage(names.queue);
    expect(JSON.parse(message.content.toString())).toEqual({ n: 1 });
    expect(message.properties).toMatchObject({
      messageId: 'm-1',
      type: 'bus-test.created',
      deliveryMode: 2,
    });
  });

  it('rejects a publish to a missing exchange and recovers for the next one', async () => {
    await admin.bindQueue(names.queue, 'amq.topic', names.queue);

    await expect(
      publisher.publish({
        exchange: `${names.exchange}.missing`,
        routingKey: 'k',
        body: {},
      }),
    ).rejects.toThrow(PublishError);

    await publisher.publish({
      exchange: 'amq.topic',
      routingKey: names.queue,
      body: { ok: true },
    });
    expect(
      JSON.parse((await admin.waitForMessage(names.queue)).content.toString()),
    ).toEqual({ ok: true });
  });

  it('relays an outbox through a direct exchange to a consumer, in order and once', async () => {
    const DIRECT = { name: names.exchange + '.by-type', type: 'direct' as const };
    const destinations = new DestinationRegistry({
      fallback: routeByMessageType(DIRECT.name),
    });
    const received: EventPayload[] = [];
    const handler = {
      getHandlerName: () => 'Projector',
      handle: async (message: EventPayload) => {
        received.push(message);
      },
    };

    consumer = new Consumer(
      connection,
      new HandlerRegistry({
        'bus-test.task_created': handler,
        'bus-test.task_removed': handler,
      }),
      {
        endpoint: endpointFor(names, {
          exchange: DIRECT,
          bindingKeys: undefined,
          bindHandledTypes: true,
        }),
        inbox: { store: new InMemoryInboxStore() },
      },
      logger,
    );
    await consumer.start();

    const record = (messageId: string, type: string): OutboxRecord => ({
      messageId,
      type,
      ...destinations.resolve(type),
      body: { uuid: messageId },
      properties: { messageId, type, appId: 'bus-test', headers: { type } },
    });
    const store = new InMemoryOutboxStore();
    store.add(
      record('m-1', 'bus-test.task_created'),
      record('m-2', 'bus-test.task_removed'),
      record('m-3', 'bus-test.task_created'),
    );
    const relay = new OutboxRelay(
      store,
      new Publisher(connection, { exchanges: [DIRECT] }, logger),
      {},
      logger,
    );

    expect(await relay.dispatch({ limit: 2 })).toEqual({
      fetched: 2,
      published: 2,
      failed: 0,
    });
    expect(await relay.dispatch({ limit: 2 })).toEqual({
      fetched: 1,
      published: 1,
      failed: 0,
    });
    expect(await relay.dispatch()).toEqual({ fetched: 0, published: 0, failed: 0 });

    await waitFor(() => received.length === 3);
    expect(received.map((message) => [message.messageId, message.type])).toEqual([
      ['m-1', 'bus-test.task_created'],
      ['m-2', 'bus-test.task_removed'],
      ['m-3', 'bus-test.task_created'],
    ]);
    expect(store.sent).toHaveLength(3);

    await admin.cleanup({ ...names, exchange: DIRECT.name });
  });

  it('keeps a record pending when its exchange does not exist and still sends the others', async () => {
    await admin.bindQueue(names.queue, 'amq.topic', names.queue);
    const store = new InMemoryOutboxStore();
    store.add(
      { messageId: 'm-1', type: 't', exchange: `${names.exchange}.missing`, routingKey: 'k', body: {} },
      { messageId: 'm-2', type: 't', routingKey: names.queue, body: { n: 2 } },
    );
    const relay = new OutboxRelay(store, publisher, { defaultExchange: 'amq.topic' }, logger);

    expect(await relay.dispatch()).toEqual({ fetched: 2, published: 1, failed: 1 });

    expect(store.getPending().map((pending) => pending.messageId)).toEqual(['m-1']);
    expect((await admin.waitForMessage(names.queue)).properties.messageId).toBe('m-2');
  });
});

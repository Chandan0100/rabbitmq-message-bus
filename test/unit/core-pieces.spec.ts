import {
  ConfigurationError,
  ConnectionFailedError,
  connectionOptionsFromEnv,
  DestinationRegistry,
  exponentialBackoff,
  fixedBackoff,
  HandlerExecutionError,
  InMemoryOutboxStore,
  JsonMessageSerializer,
  linearBackoff,
  MessageParseError,
  OutboxRelay,
  OutgoingMessage,
  Publisher,
  resolveConnectionOptions,
  RetryPolicy,
  routeByMessageType,
  UnrecoverableMessageError,
} from '../../src';
import { createTestLogger } from '../support/test-logger';

describe('backoff strategies', () => {
  it('compute the delay of each delayed retry', () => {
    expect([1, 2, 3].map(linearBackoff(2000))).toEqual([2000, 4000, 6000]);
    expect([1, 2, 3].map(fixedBackoff(500))).toEqual([500, 500, 500]);
    expect([1, 2, 3].map(exponentialBackoff(1000, 2))).toEqual([1000, 2000, 4000]);
  });
});

describe('RetryPolicy', () => {
  const policy = new RetryPolicy({
    delayedRetries: 3,
    backoff: linearBackoff(2000),
    maxDelayMs: 5000,
  });

  it('schedules a delayed retry while attempts remain', () => {
    expect(policy.decide(0, new Error('x'))).toEqual({
      action: 'retry',
      attempt: 1,
      delayMs: 2000,
    });
  });

  it('caps the delay at the retry queue TTL', () => {
    expect(policy.decide(2, new Error('x'))).toEqual({
      action: 'retry',
      attempt: 3,
      delayMs: 5000,
    });
  });

  it('sends the message to the error queue once the retries are exhausted', () => {
    expect(policy.decide(3, new Error('x'))).toEqual({
      action: 'error-queue',
      reason: 'retries-exhausted',
    });
  });

  it('sends an unrecoverable failure straight to the error queue', () => {
    const unrecoverable = new HandlerExecutionError('m-1', [
      new UnrecoverableMessageError('bad'),
    ]);

    expect(policy.decide(0, unrecoverable)).toEqual({
      action: 'error-queue',
      reason: 'unrecoverable',
    });
  });

  it('keeps retrying when only some handlers failed unrecoverably', () => {
    const mixed = new HandlerExecutionError('m-1', [
      new UnrecoverableMessageError('bad'),
      new Error('transient'),
    ]);

    expect(policy.decide(0, mixed).action).toBe('retry');
  });
});

describe('JsonMessageSerializer', () => {
  it('round-trips a body', () => {
    const serializer = new JsonMessageSerializer();

    expect(serializer.deserialize(serializer.serialize({ a: 1 }))).toEqual({ a: 1 });
  });

  it('repairs malformed JSON by default', () => {
    expect(
      new JsonMessageSerializer().deserialize(Buffer.from("{'a': 1,}")),
    ).toEqual({ a: 1 });
  });

  it('reads empty content as an empty object', () => {
    expect(new JsonMessageSerializer().deserialize(Buffer.alloc(0))).toEqual({});
  });

  it('throws a parse error when repair is off', () => {
    expect(() =>
      new JsonMessageSerializer({ repair: false }).deserialize(Buffer.from('{oops')),
    ).toThrow(MessageParseError);
  });
});

describe('DestinationRegistry', () => {
  it('returns the registered destination of a message type', () => {
    const registry = new DestinationRegistry().registerAll({
      task_created: { exchange: 'events', routingKey: 'tasks.created' },
    });

    expect(registry.resolve('task_created')).toEqual({
      exchange: 'events',
      routingKey: 'tasks.created',
    });
    expect(registry.get('unknown')).toBeUndefined();
    expect(() => registry.resolve('unknown')).toThrow(ConfigurationError);
  });

  it('falls back to routing by message type, which suits a direct exchange', () => {
    const registry = new DestinationRegistry({
      fallback: routeByMessageType('events-direct'),
    }).register('special', { exchange: 'other', routingKey: 'k' });

    expect(registry.resolve('task_created')).toEqual({
      exchange: 'events-direct',
      routingKey: 'task_created',
    });
    expect(registry.resolve('special').exchange).toBe('other');
  });
});

describe('connection options', () => {
  it('apply the reconnect defaults of the original service', () => {
    const options = resolveConnectionOptions({ url: 'amqp://localhost:5672/' });

    expect(options.heartbeat).toBe(30);
    expect(options.reconnect).toEqual({
      enabled: true,
      maxAttempts: 3,
      delayMs: 5000,
    });
  });

  it('reject a missing or malformed url', () => {
    expect(() => resolveConnectionOptions({ url: '' })).toThrow(ConfigurationError);
    expect(() => resolveConnectionOptions({ url: 'http://localhost' })).toThrow(
      /amqp:\/\/ or amqps:\/\//,
    );
  });

  it('are read from the conventional environment variables', () => {
    expect(
      connectionOptionsFromEnv({
        RABBITMQ_DSN: 'amqp://rabbit:5672/',
        RABBITMQ_USERNAME: 'user',
        RABBITMQ_PASSWORD: 'secret',
        RABBITMQ_HEARTBEAT_INTERVAL: '45',
      }),
    ).toEqual({
      url: 'amqp://rabbit:5672/',
      username: 'user',
      password: 'secret',
      heartbeat: 45,
    });
  });
});

describe('OutboxRelay', () => {
  const record = (messageId: string, exchange?: string) => ({
    messageId,
    type: 'task_created',
    exchange,
    routingKey: 'tasks.created',
    body: { id: messageId },
    properties: { appId: 'padmasana', headers: { type: 'task_created' } },
  });

  function relayWith(publish: (message: OutgoingMessage) => Promise<void>) {
    const store = new InMemoryOutboxStore();
    const published: OutgoingMessage[] = [];
    const publisher = {
      publish: async (message: OutgoingMessage) => {
        await publish(message);
        published.push(message);
      },
    } as unknown as Publisher;
    const relay = new OutboxRelay(
      store,
      publisher,
      { defaultExchange: 'events' },
      createTestLogger(),
    );
    return { store, published, relay };
  }

  it('publishes pending records in order and marks them as sent', async () => {
    const { store, published, relay } = relayWith(async () => undefined);
    store.add(record('m-1'), record('m-2', 'other'), record('m-3'));

    const result = await relay.dispatch({ limit: 2 });

    expect(result).toEqual({ fetched: 2, published: 2, failed: 0 });
    expect(published.map((message) => message.exchange)).toEqual(['events', 'other']);
    expect(published[0].properties).toEqual({
      messageId: 'm-1',
      type: 'task_created',
      appId: 'padmasana',
      headers: { type: 'task_created' },
    });
    expect(store.sent.map((sent) => sent.messageId)).toEqual(['m-1', 'm-2']);
    expect(store.getPending().map((pending) => pending.messageId)).toEqual(['m-3']);
  });

  it('keeps a failed record pending and carries on with the rest', async () => {
    const { store, relay } = relayWith(async (message) => {
      if (message.properties?.messageId === 'm-1') throw new Error('rejected');
    });
    store.add(record('m-1'), record('m-2'));

    const result = await relay.dispatch();

    expect(result).toEqual({ fetched: 2, published: 1, failed: 1 });
    expect(store.getPending().map((pending) => pending.messageId)).toEqual(['m-1']);
  });

  it('stops the run when the broker cannot be reached', async () => {
    const { store, relay } = relayWith(async () => {
      throw new ConnectionFailedError('down');
    });
    store.add(record('m-1'), record('m-2'));

    await expect(relay.dispatch()).rejects.toThrow(ConnectionFailedError);
    expect(store.sent).toHaveLength(0);
  });

  it('runs the whole batch inside the context it is given', async () => {
    const steps: string[] = [];
    const store = new InMemoryOutboxStore();
    store.add(record('m-1'));
    const fetchPending = store.fetchPending.bind(store);
    store.fetchPending = async (limit) => {
      steps.push('fetch');
      return fetchPending(limit);
    };
    const publisher = {
      publish: async () => void steps.push('publish'),
    } as unknown as Publisher;
    const relay = new OutboxRelay(
      store,
      publisher,
      {
        defaultExchange: 'events',
        runInContext: async (work) => {
          steps.push('open');
          const result = await work();
          steps.push('close');
          return result;
        },
      },
      createTestLogger(),
    );

    await relay.dispatch();

    expect(steps).toEqual(['open', 'fetch', 'publish', 'close']);
  });

  it('does nothing when the outbox is empty', async () => {
    const { relay } = relayWith(async () => undefined);

    expect(await relay.dispatch()).toEqual({ fetched: 0, published: 0, failed: 0 });
  });
});

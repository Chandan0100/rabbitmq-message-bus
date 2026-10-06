import { randomUUID } from 'node:crypto';
import {
  ConnectionFailedError,
  ConnectionManager,
  Consumer,
  EventPayload,
  HandlerRegistry,
  MessageHandler,
} from '../../src';
import {
  BrokerAdmin,
  createConnection,
  createNames,
  endpointFor,
  forceCloseConnection,
  sleep,
  TestNames,
  waitFor,
} from '../support/broker';
import { createTestLogger, TestLogger } from '../support/test-logger';

const TYPE = 'bus-test.task_created';

describe('Resilience against a real broker', () => {
  let admin: BrokerAdmin;
  let names: TestNames;
  let logger: TestLogger;
  let connection: ConnectionManager;
  let connectionName: string;
  let consumer: Consumer | undefined;

  const publish = (messageId: string) =>
    admin.publish(names.exchange, 'bus-test.a', { messageId }, {
      messageId,
      type: TYPE,
    });

  async function startConsumer(
    handle: (message: EventPayload) => Promise<void> | void,
  ): Promise<{ calls: string[] }> {
    const calls: string[] = [];
    const handler: MessageHandler = {
      getHandlerName: () => 'Handler',
      handle: async (message) => {
        calls.push(message.messageId);
        await handle(message);
      },
    };
    consumer = new Consumer(
      connection,
      new HandlerRegistry({ [TYPE]: handler }),
      { endpoint: endpointFor(names) },
      logger,
    );
    await consumer.start();
    return { calls };
  }

  beforeAll(async () => {
    admin = await BrokerAdmin.connect();
  });

  afterAll(async () => {
    await admin.close();
  });

  beforeEach(() => {
    names = createNames();
    logger = createTestLogger();
    connectionName = `bus-test-${randomUUID().slice(0, 8)}`;
    connection = createConnection(logger, connectionName);
  });

  afterEach(async () => {
    await consumer?.stop();
    consumer = undefined;
    await connection.close();
    await admin.cleanup(names);
  });

  it('reconnects and resumes consuming after the connection is lost', async () => {
    const { calls } = await startConsumer(() => undefined);
    const events: string[] = [];
    connection.on('disconnected', () => events.push('disconnected'));
    connection.on('connected', () => events.push('connected'));

    await publish('m-1');
    await waitFor(() => calls.length === 1);

    await forceCloseConnection(connectionName);
    await waitFor(() => events.includes('connected'), 10000);
    await waitFor(() => consumer!.isConsuming());

    await publish('m-2');
    await waitFor(() => calls.length === 2);
    expect(calls).toEqual(['m-1', 'm-2']);
    expect(events).toEqual(['disconnected', 'connected']);
  }, 40000);

  it('survives losing the connection while a message is being handled', async () => {
    let release!: () => void;
    const gate = new Promise<void>((resolve) => (release = resolve));
    const { calls } = await startConsumer(async (message) => {
      if (message.messageId === 'm-1' && calls.length === 1) await gate;
    });

    await publish('m-1');
    await waitFor(() => calls.length === 1);

    await forceCloseConnection(connectionName);
    await waitFor(() => logger.messages('warn').some((line) => /Disconnected/.test(line)), 10000);
    await waitFor(() => consumer!.isConsuming(), 10000);
    // The first attempt finishes on a channel that no longer exists.
    release();

    // The broker redelivers the unacknowledged message on the new channel.
    await waitFor(() => calls.filter((id) => id === 'm-1').length === 2);
    await waitFor(() =>
      logger.messages('warn').some((line) => /Could not acknowledge message m-1/.test(line)),
    );

    // The new channel is healthy: the stale acknowledgement did not reach it.
    await publish('m-2');
    await waitFor(() => calls.includes('m-2'));
    await waitFor(async () => (await admin.messageCount(names.queue)) === 0);
    expect(consumer!.isConsuming()).toBe(true);
  }, 40000);

  it('subscribes again when the broker cancels the consumer', async () => {
    const { calls } = await startConsumer(() => undefined);

    await admin.deleteQueue(names.queue);
    await waitFor(() =>
      logger.messages('warn').some((line) => /cancelled by the broker/.test(line)),
    );
    // Reported as not consuming from the moment the cancellation arrives.
    expect(consumer!.isConsuming()).toBe(false);
    await waitFor(() => consumer!.isConsuming(), 10000);

    await publish('m-1');
    await waitFor(() => calls.length === 1);
  }, 20000);

  it('lets an in-flight message finish before stopping', async () => {
    let finished = false;
    const { calls } = await startConsumer(async () => {
      await sleep(400);
      finished = true;
    });

    await publish('m-1');
    await waitFor(() => calls.length === 1);
    await consumer!.stop();

    expect(finished).toBe(true);
    expect(await admin.messageCount(names.queue)).toBe(0);

    // Nothing is consumed once stopped.
    await publish('m-2');
    await sleep(300);
    expect(calls).toEqual(['m-1']);
    expect(await admin.messageCount(names.queue)).toBe(1);
  });

  it('gives up after the configured number of attempts when the broker is unreachable', async () => {
    const unreachable = new ConnectionManager(
      { url: 'amqp://localhost:5999', reconnect: { maxAttempts: 2, delayMs: 50 } },
      logger,
    );
    const attempts: number[] = [];
    const failed = jest.fn();
    unreachable.on('reconnecting', (info) => attempts.push(info.attempt));
    unreachable.on('failed', failed);

    await expect(unreachable.connect()).rejects.toThrow(ConnectionFailedError);

    expect(attempts).toEqual([1, 2]);
    expect(failed).toHaveBeenCalledTimes(1);
    expect(unreachable.getState()).toBe('failed');
  });

  it('does not reconnect after being closed on purpose', async () => {
    await connection.connect();
    const reconnecting = jest.fn();
    connection.on('reconnecting', reconnecting);

    await connection.close();
    await sleep(400);

    expect(connection.getState()).toBe('closed');
    expect(reconnecting).not.toHaveBeenCalled();
  });
});

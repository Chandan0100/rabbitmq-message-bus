import {
  ConfigurationError,
  EventPayload,
  HandlerExecutionError,
  HandlerExecutor,
  HandlerRegistry,
  IncomingMessage,
  InMemoryInboxStore,
  JsonMessageSerializer,
  MessageHandler,
  MessageParseError,
  UnrecoverableMessageError,
} from '../../src';
import { createTestLogger } from '../support/test-logger';

const TYPE = 'padmasana.task_created';

const incoming = (content = '{"task_uuid":"t-1"}'): IncomingMessage => ({
  messageId: 'm-1',
  type: TYPE,
  content: Buffer.from(content),
  headers: { type: TYPE },
});

function handler(
  name: string,
  handle: (message: EventPayload) => Promise<void> | void,
): MessageHandler & { calls: EventPayload[] } {
  const calls: EventPayload[] = [];
  return {
    calls,
    getHandlerName: () => name,
    handle: async (message) => {
      calls.push(message);
      await handle(message);
    },
  };
}

function executor(
  handlers: MessageHandler[],
  options: Partial<ConstructorParameters<typeof HandlerExecutor>[2]> = {},
) {
  return new HandlerExecutor(
    new HandlerRegistry({ [TYPE]: handlers }),
    new JsonMessageSerializer(),
    { immediateRetries: 2, ...options },
    createTestLogger(),
  );
}

describe('HandlerExecutor', () => {
  it('hands the parsed body, id, type and headers to the handler', async () => {
    const first = handler('First', () => undefined);

    await executor([first]).execute(incoming());

    expect(first.calls).toEqual([
      {
        messageId: 'm-1',
        body: { task_uuid: 't-1' },
        type: TYPE,
        headers: { type: TYPE },
      },
    ]);
  });

  it('retries a failing handler in-process until it succeeds', async () => {
    let failuresLeft = 2;
    const flaky = handler('Flaky', () => {
      if (failuresLeft-- > 0) throw new Error('boom');
    });

    await executor([flaky]).execute(incoming());

    expect(flaky.calls).toHaveLength(3);
  });

  it('reports the failure once the immediate retries are used up', async () => {
    const broken = handler('Broken', () => {
      throw new Error('boom');
    });

    const failure = await executor([broken])
      .execute(incoming())
      .catch((error) => error);

    expect(failure).toBeInstanceOf(HandlerExecutionError);
    expect(failure.failures.map((error: Error) => error.message)).toEqual(['boom']);
    expect(failure.unrecoverable).toBe(false);
    expect(broken.calls).toHaveLength(3);
  });

  it('does not retry an unrecoverable error', async () => {
    const poison = handler('Poison', () => {
      throw new UnrecoverableMessageError('never going to work');
    });

    const failure = await executor([poison])
      .execute(incoming())
      .catch((error) => error);

    expect(poison.calls).toHaveLength(1);
    expect(failure.unrecoverable).toBe(true);
  });

  it('runs every handler and reports only the ones that failed', async () => {
    const good = handler('Good', () => undefined);
    const bad = handler('Bad', () => {
      throw new Error('bad failed');
    });

    const failure = await executor([good, bad], { immediateRetries: 0 })
      .execute(incoming())
      .catch((error) => error);

    expect(good.calls).toHaveLength(1);
    expect(failure.failures).toHaveLength(1);
    expect(failure.failures[0].message).toBe('bad failed');
  });

  it('skips a handler that already processed the message and records the others', async () => {
    const store = new InMemoryInboxStore();
    await store.markHandled('m-1', 'Done', 'tenant_a');
    const done = handler('Done', () => undefined);
    const pending = handler('Pending', () => undefined);

    await executor([done, pending], {
      inbox: { store },
      scope: 'tenant_a',
    }).execute(incoming());

    expect(done.calls).toHaveLength(0);
    expect(pending.calls).toHaveLength(1);
    expect(await store.isHandled('m-1', 'Pending', 'tenant_a')).toBe(true);
    expect(await store.isHandled('m-1', 'Pending', 'tenant_b')).toBe(false);
  });

  it('leaves recording to the handler when asked to', async () => {
    const store = new InMemoryInboxStore();
    const own = handler('Own', () => undefined);

    await executor([own], { inbox: { store, record: 'handler' } }).execute(
      incoming(),
    );

    expect(await store.isHandled('m-1', 'Own')).toBe(false);
  });

  it('does not run the handler again when only the inbox write failed', async () => {
    const store = new InMemoryInboxStore();
    store.markHandled = async () => {
      throw new Error('database is down');
    };
    const once = handler('Once', () => undefined);

    const failure = await executor([once], { inbox: { store } })
      .execute(incoming())
      .catch((error) => error);

    expect(once.calls).toHaveLength(1);
    expect(failure.failures[0].message).toBe('database is down');
  });

  it('treats unreadable content as unrecoverable without calling the handler', async () => {
    const never = handler('Never', () => undefined);
    const strict = new HandlerExecutor(
      new HandlerRegistry({ [TYPE]: [never] }),
      new JsonMessageSerializer({ repair: false }),
      { immediateRetries: 2 },
      createTestLogger(),
    );

    const failure = await strict.execute(incoming('{oops')).catch((error) => error);

    expect(never.calls).toHaveLength(0);
    expect(failure.failures[0]).toBeInstanceOf(MessageParseError);
    expect(failure.unrecoverable).toBe(true);
  });
});

describe('HandlerRegistry', () => {
  const noop: MessageHandler = {
    getHandlerName: () => 'Noop',
    handle: async () => undefined,
  };

  it('resolves instances, promises and factories, and calls a factory once', async () => {
    const factory = jest.fn(async () => noop);
    const registry = new HandlerRegistry({
      a: noop,
      b: [Promise.resolve(noop), factory],
    });

    expect(registry.getTypes()).toEqual(['a', 'b']);
    expect(await registry.resolve('a')).toEqual([noop]);
    expect(await registry.resolve('b')).toEqual([noop, noop]);
    await registry.resolve('b');
    expect(factory).toHaveBeenCalledTimes(1);
  });

  it('tries again after a failed resolution', async () => {
    const factory = jest
      .fn<Promise<MessageHandler>, []>()
      .mockRejectedValueOnce(new Error('module not ready'))
      .mockResolvedValue(noop);
    const registry = new HandlerRegistry({ a: factory });

    await expect(registry.resolve('a')).rejects.toThrow('module not ready');
    expect(await registry.resolve('a')).toEqual([noop]);
  });

  it('rejects something that is not a handler', async () => {
    const registry = new HandlerRegistry({ a: { nope: true } as never });

    await expect(registry.resolveAll()).rejects.toThrow(ConfigurationError);
  });

  it('returns no handlers for an unknown type', async () => {
    const registry = new HandlerRegistry();

    expect(registry.has('missing')).toBe(false);
    expect(await registry.resolve('missing')).toEqual([]);
  });
});

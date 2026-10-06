import {
  buildConsumerTopology,
  ConfigurationError,
  ConsumerEndpointOptions,
  describeEndpointWarnings,
  fixedBackoff,
  resolveBindingKeys,
  resolveConsumerEndpoint,
} from '../../src';

const baseOptions = (
  overrides: Partial<ConsumerEndpointOptions> = {},
): ConsumerEndpointOptions => ({
  appName: 'padmasana',
  queue: 'padmasana.notification',
  exchange: { name: 'events-topic', type: 'topic' },
  bindingKeys: 'padmasana.#',
  retry: { exchange: { name: 'events-direct', type: 'direct' } },
  error: {
    exchange: { name: 'events-direct', type: 'direct' },
    routingKey: 'padmasana.dead-letter',
  },
  ...overrides,
});

describe('resolveConsumerEndpoint', () => {
  it('derives the retry names from the queue and applies the defaults', () => {
    const endpoint = resolveConsumerEndpoint(baseOptions());

    expect(endpoint.bindingKeys).toEqual(['padmasana.#']);
    expect(endpoint.prefetch).toBe(10);
    expect(endpoint.retry).toMatchObject({
      queue: 'padmasana.notification.events-retry',
      queueBindingKey: 'padmasana.notification.events-retry',
      returnBindingKey: 'padmasana.notification.retry',
      immediateRetries: 5,
      delayedRetries: 3,
      queueMessageTtl: 10000,
    });
    expect(endpoint.retry.backoff(2)).toBe(4000);
  });

  it('rejects a retry exchange that is not direct', () => {
    expect(() =>
      resolveConsumerEndpoint(
        baseOptions({ retry: { exchange: { name: 'retry-topic', type: 'topic' } } }),
      ),
    ).toThrow(/must be of type "direct"/);
  });

  it('rejects the same exchange declared with two types', () => {
    expect(() =>
      resolveConsumerEndpoint(
        baseOptions({ exchange: { name: 'events-direct', type: 'topic' } }),
      ),
    ).toThrow(/declared both as/);
  });

  it('rejects an unsupported exchange type', () => {
    expect(() =>
      resolveConsumerEndpoint(
        baseOptions({ exchange: { name: 'events', type: 'headers' as never } }),
      ),
    ).toThrow(ConfigurationError);
  });

  it('rejects numbers that did not parse, such as a missing environment variable', () => {
    expect(() =>
      resolveConsumerEndpoint(
        baseOptions({
          retry: {
            exchange: { name: 'events-direct', type: 'direct' },
            delayedRetries: Number.parseInt('', 10),
          },
        }),
      ),
    ).toThrow(/retry.delayedRetries must be an integer/);
  });

  it('rejects retry keys that would bypass the retry queue', () => {
    expect(() =>
      resolveConsumerEndpoint(
        baseOptions({
          retry: {
            exchange: { name: 'events-direct', type: 'direct' },
            queueBindingKey: 'same',
            returnBindingKey: 'same',
          },
        }),
      ),
    ).toThrow(/must differ/);
  });

  it('rejects an error routing key that collides with a retry key', () => {
    expect(() =>
      resolveConsumerEndpoint(
        baseOptions({
          error: {
            exchange: { name: 'events-direct', type: 'direct' },
            routingKey: 'padmasana.notification.retry',
          },
        }),
      ),
    ).toThrow(/collides with a retry key/);
  });

  it('reports a missing required option by name', () => {
    expect(() =>
      resolveConsumerEndpoint(baseOptions({ queue: '' })),
    ).toThrow('Missing required consumer option: queue');
  });
});

describe('resolveBindingKeys', () => {
  it('uses the configured patterns for a topic exchange', () => {
    const endpoint = resolveConsumerEndpoint(
      baseOptions({ bindingKeys: ['a.#', 'b.*'] }),
    );

    expect(resolveBindingKeys(endpoint, ['ignored'])).toEqual(['a.#', 'b.*']);
  });

  it('binds one key per handled type on a direct exchange when asked to', () => {
    const endpoint = resolveConsumerEndpoint(
      baseOptions({
        exchange: { name: 'events-direct', type: 'direct' },
        bindingKeys: undefined,
        bindHandledTypes: true,
      }),
    );

    expect(resolveBindingKeys(endpoint, ['task_created', 'task_removed'])).toEqual([
      'task_created',
      'task_removed',
    ]);
  });

  it('ignores binding keys for a fanout exchange', () => {
    const endpoint = resolveConsumerEndpoint(
      baseOptions({ exchange: { name: 'events-fanout', type: 'fanout' } }),
    );

    expect(resolveBindingKeys(endpoint, [])).toEqual(['']);
  });

  it('fails when a topic or direct queue would end up unbound', () => {
    const endpoint = resolveConsumerEndpoint(
      baseOptions({ bindingKeys: undefined }),
    );

    expect(() => resolveBindingKeys(endpoint, [])).toThrow(/has no binding key/);
  });

  it('fails when a handled type would also feed the retry queue', () => {
    const endpoint = resolveConsumerEndpoint(
      baseOptions({
        exchange: { name: 'events-direct', type: 'direct' },
        bindingKeys: undefined,
        bindHandledTypes: true,
      }),
    );

    expect(() =>
      resolveBindingKeys(endpoint, ['padmasana.notification.events-retry']),
    ).toThrow(/used both by the primary queue and the retry queue/);
  });
});

describe('describeEndpointWarnings', () => {
  it('warns about wildcards on a direct exchange', () => {
    const endpoint = resolveConsumerEndpoint(
      baseOptions({ exchange: { name: 'events-direct', type: 'direct' } }),
    );

    expect(describeEndpointWarnings(endpoint, ['padmasana.#'])[0]).toMatch(
      /matched literally/,
    );
  });

  it('warns when a delay exceeds the retry queue TTL', () => {
    const endpoint = resolveConsumerEndpoint(
      baseOptions({
        retry: {
          exchange: { name: 'events-direct', type: 'direct' },
          backoff: fixedBackoff(60000),
        },
      }),
    );

    expect(describeEndpointWarnings(endpoint, ['padmasana.#'])[0]).toMatch(
      /capped at the queue TTL/,
    );
  });

  it('stays quiet for a sound configuration', () => {
    const endpoint = resolveConsumerEndpoint(baseOptions());

    expect(describeEndpointWarnings(endpoint, ['padmasana.#'])).toEqual([]);
  });
});

describe('buildConsumerTopology', () => {
  it('lays out the primary queue, the retry loop and the error queue', () => {
    const endpoint = resolveConsumerEndpoint(
      baseOptions({
        queueType: 'quorum',
        error: {
          exchange: { name: 'events-direct', type: 'direct' },
          routingKey: 'padmasana.dead-letter',
          queue: 'padmasana.error',
        },
      }),
    );

    const topology = buildConsumerTopology(endpoint, ['padmasana.#']);

    expect(topology.exchanges).toEqual([
      { name: 'events-direct', type: 'direct' },
      { name: 'events-topic', type: 'topic' },
    ]);
    expect(topology.queues).toEqual([
      { name: 'padmasana.notification', type: 'quorum' },
      {
        name: 'padmasana.notification.events-retry',
        type: 'quorum',
        deadLetterExchange: 'events-direct',
        deadLetterRoutingKey: 'padmasana.notification.retry',
        messageTtl: 10000,
      },
      { name: 'padmasana.error', type: 'quorum' },
    ]);
    expect(topology.bindings).toEqual([
      {
        queue: 'padmasana.notification',
        exchange: 'events-topic',
        routingKey: 'padmasana.#',
      },
      {
        queue: 'padmasana.notification',
        exchange: 'events-direct',
        routingKey: 'padmasana.notification.retry',
      },
      {
        queue: 'padmasana.notification.events-retry',
        exchange: 'events-direct',
        routingKey: 'padmasana.notification.events-retry',
      },
      {
        queue: 'padmasana.error',
        exchange: 'events-direct',
        routingKey: 'padmasana.dead-letter',
      },
    ]);
  });

  it('declares no error queue unless one is named', () => {
    const topology = buildConsumerTopology(
      resolveConsumerEndpoint(baseOptions()),
      ['padmasana.#'],
    );

    expect(topology.queues.map((queue) => queue.name)).toEqual([
      'padmasana.notification',
      'padmasana.notification.events-retry',
    ]);
  });
});

import { applyEndpointOverrides, ConsumerEndpointOptions } from '../../src';
import { CliUsageError, parseCliArgs } from '../../src/nestjs';

const endpoint: ConsumerEndpointOptions = {
  appName: 'padmasana',
  queue: 'padmasana.notification',
  exchange: { name: 'events-topic', type: 'topic' },
  bindingKeys: 'padmasana.#',
  prefetch: 10,
  retry: {
    exchange: { name: 'events-direct', type: 'direct' },
    delayedRetries: 3,
  },
  error: {
    exchange: { name: 'events-direct', type: 'direct' },
    routingKey: 'padmasana.dead-letter',
  },
};

describe('parseCliArgs', () => {
  it('reads the flags the existing worker manifests pass', () => {
    expect(
      parseCliArgs([
        'dispatch-messages',
        '--limit',
        '10',
        '--module=board-management',
        '--schema=board-management',
      ]),
    ).toMatchObject({
      command: 'dispatch-messages',
      module: 'board-management',
      limit: 10,
      scope: 'board-management',
    });

    expect(
      parseCliArgs(['handle-messages', '-l', '5', '--module', 'iam', '-s', 'iam']),
    ).toMatchObject({
      command: 'handle-messages',
      module: 'iam',
      limit: 5,
      scope: 'iam',
    });
  });

  it('turns the topology flags into endpoint overrides', () => {
    const { endpoint: overrides } = parseCliArgs([
      'handle-messages',
      '--module',
      'notification-module',
      '--primary-queue',
      'padmasana.audit',
      '--primary-queue-exchange',
      'events-direct',
      '--primary-queue-exchange-type',
      'direct',
      '--primary-queue-binding-key',
      'task_created',
      '--primary-queue-binding-key',
      'task_removed',
      '--delayed-retries-number',
      '0',
      '--error-queue',
      'padmasana.error',
    ]);

    expect(applyEndpointOverrides(endpoint, overrides)).toEqual({
      appName: 'padmasana',
      queue: 'padmasana.audit',
      exchange: { name: 'events-direct', type: 'direct' },
      bindingKeys: ['task_created', 'task_removed'],
      prefetch: 10,
      retry: {
        exchange: { name: 'events-direct', type: 'direct' },
        delayedRetries: 0,
      },
      error: {
        exchange: { name: 'events-direct', type: 'direct' },
        routingKey: 'padmasana.dead-letter',
        queue: 'padmasana.error',
      },
    });
  });

  it('can switch an endpoint to another exchange type at start time', () => {
    expect(
      applyEndpointOverrides(endpoint, {
        exchange: { name: 'events-direct', type: 'direct' },
        bindingKeys: [],
        bindHandledTypes: true,
      }),
    ).toMatchObject({
      exchange: { name: 'events-direct', type: 'direct' },
      bindingKeys: [],
      bindHandledTypes: true,
    });
  });

  it('leaves the configured endpoint alone when no override is given', () => {
    const { endpoint: overrides } = parseCliArgs(['handle-messages', '--module', 'x']);

    expect(applyEndpointOverrides(endpoint, overrides)).toEqual(endpoint);
  });

  it.each([
    [['publish'], /Unknown command "publish"/],
    [[], /Unknown command ""/],
    [['handle-messages', '--limit', 'ten'], /--limit must be an integer/],
    [['handle-messages', '--limit', '0'], /--limit must be an integer >= 1/],
    [['handle-messages', '--primary-queue-exchange-type', 'headers'], /must be one of topic, direct, fanout/],
    [['handle-messages', '--no-such-flag'], /Unknown option/],
    [['dispatch-messages', '--primary-queue', 'q'], /only applies to handle-messages/],
    [['dispatch-messages', 'extra'], /Unexpected argument "extra"/],
  ])('rejects %j', (argv, message) => {
    expect(() => parseCliArgs(argv)).toThrow(CliUsageError);
    expect(() => parseCliArgs(argv)).toThrow(message);
  });
});

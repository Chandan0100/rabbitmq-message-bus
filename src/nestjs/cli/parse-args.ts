import { parseArgs, ParseArgsConfig } from 'node:util';
import { EndpointOverrides } from '../../core/consumer/endpoint-overrides';
import { EXCHANGE_TYPES, ExchangeType } from '../../core/topology/topology';

export const MESSAGE_BUS_COMMANDS = [
  'handle-messages',
  'dispatch-messages',
] as const;

export type MessageBusCommand = (typeof MESSAGE_BUS_COMMANDS)[number];

export interface ParsedCliArgs {
  command: MessageBusCommand;
  module?: string;
  /** Prefetch for `handle-messages`, batch size for `dispatch-messages`. */
  limit?: number;
  scope?: string;
  /** Topology overrides, only for `handle-messages`. */
  endpoint: EndpointOverrides;
}

export class CliUsageError extends Error {}

export const CLI_USAGE = `Usage:
  dispatch-messages --module <name> [--limit <n>] [--schema <scope>]
  handle-messages   --module <name> [--limit <n>] [--schema <scope>] [topology overrides]

Common options:
  --module <name>                          Which registered module to run
  -l, --limit <n>                          Messages per batch (dispatch) or prefetch (handle)
  -s, --schema <scope>, --scope <scope>    Partition forwarded to the inbox/outbox store

Topology overrides for handle-messages:
  --app-name <name>
  --primary-queue <queue>
  --primary-queue-binding-key <key>        Repeat the flag to bind several keys
  --primary-queue-exchange <exchange>
  --primary-queue-exchange-type <topic|direct|fanout>
  --primary-queue-retry-binding-key <key>
  --retry-queue <queue>
  --retry-queue-binding-key <key>
  --retry-queue-exchange <exchange>
  --retry-queue-exchange-type <direct>
  --retry-queue-message-ttl <ms>
  --immediate-retries-number <n>
  --delayed-retries-number <n>
  --error-queue <queue>
  --error-queue-exchange <exchange>
  --error-queue-exchange-type <topic|direct|fanout>
  --error-queue-routing-key <key>`;

const string = { type: 'string' } as const;

const COMMON_OPTIONS = {
  module: string,
  limit: { type: 'string', short: 'l' },
  scope: string,
  schema: { type: 'string', short: 's' },
} as const;

const ENDPOINT_OPTIONS = {
  'app-name': string,
  'primary-queue': string,
  'primary-queue-binding-key': { type: 'string', multiple: true },
  'primary-queue-exchange': string,
  'primary-queue-exchange-type': string,
  'primary-queue-retry-binding-key': string,
  'retry-queue': string,
  'retry-queue-binding-key': string,
  'retry-queue-exchange': string,
  'retry-queue-exchange-type': string,
  'retry-queue-message-ttl': string,
  'immediate-retries-number': string,
  'delayed-retries-number': string,
  'error-queue': string,
  'error-queue-exchange': string,
  'error-queue-exchange-type': string,
  'error-queue-routing-key': string,
} as const;

function toInteger(value: string | undefined, flag: string, minimum: number) {
  if (value === undefined) return undefined;

  const parsed = Number(value);
  if (!Number.isInteger(parsed) || parsed < minimum) {
    throw new CliUsageError(
      `--${flag} must be an integer >= ${minimum}, received "${value}".`,
    );
  }
  return parsed;
}

function toExchangeType(value: string | undefined, flag: string) {
  if (value === undefined) return undefined;

  if (!EXCHANGE_TYPES.includes(value as ExchangeType)) {
    throw new CliUsageError(
      `--${flag} must be one of ${EXCHANGE_TYPES.join(', ')}, received "${value}".`,
    );
  }
  return value as ExchangeType;
}

export function parseCliArgs(argv: string[]): ParsedCliArgs {
  let parsed: ReturnType<typeof parseArgs<ParseArgsConfig>>;

  try {
    parsed = parseArgs({
      args: argv,
      options: { ...COMMON_OPTIONS, ...ENDPOINT_OPTIONS },
      allowPositionals: true,
      strict: true,
    });
  } catch (error) {
    throw new CliUsageError((error as Error).message);
  }

  const [command, ...extra] = parsed.positionals;
  if (!MESSAGE_BUS_COMMANDS.includes(command as MessageBusCommand)) {
    throw new CliUsageError(
      `Unknown command "${command ?? ''}". Available commands: ${MESSAGE_BUS_COMMANDS.join(', ')}.`,
    );
  }
  if (extra.length) {
    throw new CliUsageError(`Unexpected argument "${extra[0]}".`);
  }

  const values = parsed.values as Record<string, string | undefined> & {
    'primary-queue-binding-key'?: string[];
  };

  if (command === 'dispatch-messages') {
    const misplaced = Object.keys(ENDPOINT_OPTIONS).find(
      (flag) => values[flag] !== undefined,
    );
    if (misplaced) {
      throw new CliUsageError(
        `--${misplaced} only applies to handle-messages.`,
      );
    }
  }

  return {
    command: command as MessageBusCommand,
    module: values.module?.trim(),
    limit: toInteger(values.limit, 'limit', 1),
    scope: (values.scope ?? values.schema)?.trim(),
    endpoint: {
      appName: values['app-name'],
      queue: values['primary-queue'],
      bindingKeys: values['primary-queue-binding-key'],
      exchange: {
        name: values['primary-queue-exchange'],
        type: toExchangeType(
          values['primary-queue-exchange-type'],
          'primary-queue-exchange-type',
        ),
      },
      retry: {
        queue: values['retry-queue'],
        queueBindingKey: values['retry-queue-binding-key'],
        returnBindingKey: values['primary-queue-retry-binding-key'],
        queueMessageTtl: toInteger(
          values['retry-queue-message-ttl'],
          'retry-queue-message-ttl',
          1,
        ),
        immediateRetries: toInteger(
          values['immediate-retries-number'],
          'immediate-retries-number',
          0,
        ),
        delayedRetries: toInteger(
          values['delayed-retries-number'],
          'delayed-retries-number',
          0,
        ),
        exchange: {
          name: values['retry-queue-exchange'],
          type: toExchangeType(
            values['retry-queue-exchange-type'],
            'retry-queue-exchange-type',
          ),
        },
      },
      error: {
        queue: values['error-queue'],
        routingKey: values['error-queue-routing-key'],
        exchange: {
          name: values['error-queue-exchange'],
          type: toExchangeType(
            values['error-queue-exchange-type'],
            'error-queue-exchange-type',
          ),
        },
      },
    },
  };
}

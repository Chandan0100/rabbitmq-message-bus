import { jsonrepair } from 'jsonrepair';
import { MessageParseError } from '../errors';

export interface MessageSerializer {
  serialize(body: unknown): Buffer;
  /** Throws `MessageParseError` when the content cannot be read. */
  deserialize(content: Buffer): unknown;
}

export interface JsonMessageSerializerOptions {
  /** Try to repair malformed JSON (trailing commas, missing quotes...) before giving up. Default: true. */
  repair?: boolean;
}

export class JsonMessageSerializer implements MessageSerializer {
  private readonly repair: boolean;

  constructor(options: JsonMessageSerializerOptions = {}) {
    this.repair = options.repair ?? true;
  }

  serialize(body: unknown): Buffer {
    return Buffer.from(JSON.stringify(body));
  }

  deserialize(content: Buffer): unknown {
    const text = content?.toString() || '{}';

    try {
      return JSON.parse(text);
    } catch (parseError) {
      if (!this.repair) {
        throw new MessageParseError('Message content is not valid JSON.', {
          cause: parseError,
        });
      }
    }

    try {
      return JSON.parse(jsonrepair(text));
    } catch (repairError) {
      throw new MessageParseError(
        'Message content is not valid JSON and could not be repaired.',
        { cause: repairError },
      );
    }
  }
}

import type { Channel } from 'amqplib';
import { Topology } from './topology';

/** Declares exchanges, queues and bindings. Every operation is idempotent on the broker. */
export class TopologyDeclarator {
  async declare(channel: Channel, topology: Topology): Promise<void> {
    for (const exchange of topology.exchanges) {
      // The default exchange always exists and cannot be declared.
      if (exchange.name === '') continue;
      await channel.assertExchange(exchange.name, exchange.type, {
        durable: exchange.durable ?? true,
      });
    }

    for (const queue of topology.queues) {
      const queueArguments = {
        ...queue.arguments,
        ...(queue.type ? { 'x-queue-type': queue.type } : {}),
      };

      await channel.assertQueue(queue.name, {
        durable: queue.durable ?? true,
        deadLetterExchange: queue.deadLetterExchange,
        deadLetterRoutingKey: queue.deadLetterRoutingKey,
        messageTtl: queue.messageTtl,
        ...(Object.keys(queueArguments).length
          ? { arguments: queueArguments }
          : {}),
      });
    }

    for (const binding of topology.bindings) {
      await channel.bindQueue(
        binding.queue,
        binding.exchange,
        binding.routingKey,
      );
    }
  }
}

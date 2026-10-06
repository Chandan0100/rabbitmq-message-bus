export const RABBITMQ_CONSUMER_OPTIONS = Symbol('RABBITMQ_CONSUMER_OPTIONS');
export const RABBITMQ_PRODUCER_OPTIONS = Symbol('RABBITMQ_PRODUCER_OPTIONS');
export const RABBITMQ_PRODUCER_CONNECTION = Symbol(
  'RABBITMQ_PRODUCER_CONNECTION',
);

const RABBITMQ_MODULE_INSTANCE = Symbol('RABBITMQ_MODULE_INSTANCE');
let moduleInstances = 0;

/**
 * Every `register()` call is one endpoint of its own. The marker keeps NestJS
 * versions that compare dynamic modules by content from merging two of them.
 */
export function moduleInstanceMarker() {
  return { provide: RABBITMQ_MODULE_INSTANCE, useValue: ++moduleInstances };
}

/**
 * coney — an opinionated RabbitMQ toolkit: work queues with broker-held
 * retry ladders and dead-letter queues, publisher confirms, automatic
 * reconnection, graceful drain, and topic pub/sub.
 *
 * The public surface is specified in docs/api/coney.d.ts (CI enforces an exact
 * match) and its semantics in docs/design.md.
 */

export { createConey } from './client.js'
export type {
  Coney,
  ConeyOptions,
  ConeyState,
  OpenOptions,
  CloseOptions,
  CloseResult,
  RetryBucketStats,
  QueueHandle,
  TopicHandle,
  QueueStats,
} from './client.js'
export type { ReconnectPolicy } from './connection.js'
export type { RetryPolicy } from './retry.js'
export { defineQueue, defineTopic } from './define.js'
export type { AnyDef, QueueDef, QueueDefInput, Schema, TopicBinding, TopicDef, TopicDefInput } from './define.js'
export type { HeaderValue, PublishOptions } from './codec.js'
export type { ConsumeOptions, Handler, Message } from './consumer.js'
export type { CancelOptions, SubscribeOptions, Subscription } from './subscription.js'
export {
  ConeyError,
  ConeyUnavailableError,
  ConeyClosedError,
  ConeyPublishError,
  ConeySchemaError,
  ConeyTopologyError,
  ConeyConfigError,
  UnrecoverableError,
} from './errors.js'
export type { ConeyErrorCode } from './errors.js'
export type { ConeyLogEvent, LogLevel } from './log.js'
export { createConeyMock } from './mock.js'
export type { ConeyMock, DeliverOptions, DeliveryOutcome, PublishedRecord } from './mock.js'

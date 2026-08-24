/**
 * coney — an opinionated RabbitMQ toolkit: work queues with broker-held
 * retry ladders and dead-letter queues, publisher confirms, automatic
 * reconnection, graceful drain, and topic pub/sub.
 *
 * The public surface is specified in docs/api/coney.d.ts (CI enforces an exact
 * match) and its semantics in docs/design.md. This is the only barrel file —
 * internal code imports concrete modules directly.
 */

export { createConey } from './client/coney.js'
export type {
  Coney,
  ConeyOptions,
  ConeyState,
  OpenOptions,
  CloseOptions,
  RetryBucketStats,
  QueueHandle,
  TopicHandle,
  QueueStats,
} from './client/types.js'
export type { ReconnectPolicy } from './connection/policy.js'
export type { RetryPolicy } from './defs/retry.js'
export { defineQueue, defineTopic } from './defs/define.js'
export type {
  AnyDef,
  QueueDef,
  QueueDefInput,
  Schema,
  TopicBinding,
  TopicDef,
  TopicDefInput,
} from './defs/define.js'
export type { HeaderValue, PublishOptions } from './core/codec.js'
export type {
  CancelOptions,
  CloseResult,
  ConsumeOptions,
  Handler,
  Message,
  SubscribeOptions,
  Subscription,
} from './consume/types.js'
export {
  ConeyError,
  ConeyUnavailableError,
  ConeyClosedError,
  ConeyPublishError,
  ConeySchemaError,
  ConeyTopologyError,
  ConeyConfigError,
  UnrecoverableError,
} from './core/errors.js'
export type { ConeyErrorCode } from './core/errors.js'
export type { ConeyLogEvent, LogLevel } from './core/log.js'
export { createConeyMock } from './mock/client.js'
export type { ConeyMock, DeliverOptions, DeliveryOutcome, PublishedRecord } from './mock/types.js'

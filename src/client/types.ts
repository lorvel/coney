/**
 * The public client contract: `Coney`, its options and the queue/topic handles.
 * Semantics live in docs/design.md; docs/api/coney.d.ts is the enforced shape.
 */

import type { AnyDef, QueueDef, TopicDef } from '../defs/define.js'
import type { RetryPolicy } from '../defs/retry.js'
import type { ReconnectPolicy } from '../connection/policy.js'
import type { PublishOptions } from '../core/codec.js'
import type { ConeyLogEvent } from '../core/log.js'
import type { CloseResult, ConsumeOptions, Handler, SubscribeOptions, Subscription } from '../consume/types.js'

/** Lifecycle of a {@link Coney} client. */
export type ConeyState = 'idle' | 'connecting' | 'open' | 'reconnecting' | 'closing' | 'closed'

export interface ConeyOptions {
  /** `amqp://` or `amqps://` URL. Credentials are redacted in every log event. */
  url: string
  /** Prefix for every queue/exchange of this client (`<prefix>.<name>`). NO default; same rules as queue names. */
  prefix?: string
  /** Client name → AMQP `appId` of published messages + first segment of consumer tags (`<name>.<queue>.<n>`). Default: none. */
  name?: string
  /**
   * Log sink for every {@link ConeyLogEvent}, e.g. `log: (e) => logger[e.level](e.event, e)`.
   * Default: `warn`/`error` to stderr as JSON lines, rest dropped. `false` = silent.
   */
  log?: ((event: ConeyLogEvent) => void) | false
  /** Reconnect policy — partial override of the defaults documented on {@link ReconnectPolicy}. */
  reconnect?: Partial<ReconnectPolicy>
  /** ONE budget for the whole publish pipeline (connection wait, lazy topology assert, confirm) before {@link ConeyUnavailableError}. Default `5000`; `0` = fail at once; finite, <= 2^31-1. Per-call: {@link PublishOptions.timeoutMs}. */
  publishTimeoutMs?: number
  /** Ladder for queue defs without `retry`. Default `{ delays: [10_000, 60_000, 300_000] }`. */
  retry?: RetryPolicy
  /** Passed through untouched as `amqplib.connect(url, socketOptions)` (TLS, `clientProperties`, …). */
  socketOptions?: Record<string, unknown>
}

export interface OpenOptions {
  /** Reject with {@link ConeyUnavailableError} after this many ms (finite, <= 2^31-1); the reconnect loop keeps running (only `close()` stops it). Default: wait per {@link ReconnectPolicy}. */
  timeoutMs?: number
}

export interface CloseOptions {
  /** Max wait for in-flight handlers after consumers are cancelled (finite, <= 2^31-1). Default `10_000`. On expiry their `signal` is aborted, messages stay unacked (redelivered) and `drained` is `false`. */
  drainMs?: number
}

/** Depth of every retry bucket declared through this client, keyed by duration label (`"10s"`, `"1m"`, …). */
export type RetryBucketStats = Readonly<Record<string, number>>

export interface Coney {
  readonly state: ConeyState
  /** The configured prefix, or `undefined`. */
  readonly prefix: string | undefined
  /** Wire name of a logical name under this client's prefix (pure function, no I/O). */
  resolveName(name: string): string

  /**
   * Connect now (otherwise the first publish/consume connects lazily). Idempotent.
   * @throws {ConeyUnavailableError} `timeoutMs` elapsed or `reconnect.maxAttempts` exhausted.
   * @throws {ConeyClosedError} the client was closed.
   */
  open(options?: OpenOptions): Promise<void>

  /**
   * Graceful shutdown: reject new publishes ({@link ConeyClosedError}) → cancel consumers →
   * wait ≤ `drainMs` for in-flight handlers → wait for publisher confirms → close the
   * connection. Never throws for a drain timeout. Idempotent.
   */
  close(options?: CloseOptions): Promise<CloseResult>

  /**
   * Handle for a queue def, cached per wire name. Topology is asserted lazily on first
   * publish/consume and re-asserted after every reconnect.
   * @throws {ConeyConfigError} two defs with the same name but different options.
   */
  queue<T>(def: QueueDef<T>): QueueHandle<T>

  /** Handle for a topic def. Same caching and lazy assertion rules as {@link Coney.queue}. */
  topic<T>(def: TopicDef<T>): TopicHandle<T>

  /** Eagerly assert the topology of `defs` (queues, buckets, dead queues, exchanges, bindings) to fail fast at boot ({@link ConeyTopologyError}) instead of at the first publish. */
  declare(defs: readonly AnyDef[]): Promise<void>

  /** Depth of every retry bucket known to this client. Buckets are shared by all queues. */
  inspectRetry(): Promise<RetryBucketStats>
}

export interface QueueStats {
  /** Messages ready in the main queue. */
  readonly ready: number
  /** Consumers attached to the main queue (all clients). */
  readonly consumers: number
  /** Messages in `<name>.dead`. */
  readonly dead: number
}

export interface QueueHandle<T> {
  readonly def: QueueDef<T>
  /** Wire name of the main queue. */
  readonly name: string

  /**
   * Publish through the default exchange; resolves when the broker CONFIRMS.
   * @throws {ConeySchemaError} body fails the schema (before any I/O).
   * @throws {ConeyUnavailableError} no connection within `timeoutMs`.
   * @throws {ConeyPublishError} broker nack. @throws {ConeyClosedError} closing/closed.
   */
  publish(body: T, options?: PublishOptions): Promise<void>

  /** Attach a consumer on its own channel; resolves once registered with the broker; re-attached after every reconnect. */
  consume(handler: Handler<T>, options?: ConsumeOptions): Promise<Subscription>

  /** Passive inspection (no topology changes). */
  inspect(): Promise<QueueStats>

  /** Move messages from `<name>.dead` back to the main queue (attempt reset, `messageId` kept), up to `limit` (default: all present at call time). */
  replayDead(options?: { readonly limit?: number }): Promise<{ readonly replayed: number }>

  /** Delete every message in `<name>.dead`. */
  purgeDead(): Promise<{ readonly purged: number }>
}

export interface TopicHandle<T> {
  readonly def: TopicDef<T>
  /** Wire name of the topic exchange. */
  readonly name: string

  /** Publish with a routing key; no matching binding → dropped silently. Same errors as {@link QueueHandle.publish}. */
  publish(routingKey: string, body: T, options?: PublishOptions): Promise<void>

  /**
   * Per-instance subscription on a server-named exclusive queue bound with `patterns`.
   * Acked after the handler settles; a throw is logged (`coney.subscription_error`) and
   * dropped — no retry. Dies with the connection: after a reconnect a new queue is bound and
   * messages in between are lost (`coney.subscription_reset`). Durable delivery → {@link QueueDefInput.bindings}.
   */
  subscribe(patterns: readonly string[], handler: Handler<T>, options?: SubscribeOptions): Promise<Subscription>
}

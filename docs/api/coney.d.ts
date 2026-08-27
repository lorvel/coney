/**
 * @lorvel/coney — public API surface (v0.1). This file IS the contract; nothing
 * here references amqplib types. "wire name" = `<prefix>.<name>` (or `<name>`);
 * "attempt" = `x-retries` + 1; delivery is at-least-once — handlers must be idempotent.
 */

// ── Client ──

/** Lifecycle of a {@link Coney} client. */
export type ConeyState = 'idle' | 'connecting' | 'open' | 'reconnecting' | 'closing' | 'closed'

/** Backoff policy used for the initial connect AND every reconnect. */
export interface ReconnectPolicy {
  /** Delay before the first retry. Default `1000` ms. */
  initialDelayMs: number
  /** Upper bound for the growing delay. Default `30_000` ms. */
  maxDelayMs: number
  /** Multiplier applied after each failed attempt. Default `2`. */
  factor: number
  /** Random jitter as a fraction of the delay (`0.2` = ±20 %). Default `0.2`. */
  jitter: number
  /** Give up after this many consecutive failures (`coney.reconnect_exhausted`). Default `Infinity`. */
  maxAttempts: number
}

/** Retry ladder of a queue: one delay per retry, in milliseconds. */
export interface RetryPolicy {
  /**
   * `delays[k - 1]` is the wait before attempt `k + 1`. The number of retries is
   * `delays.length`; a message that fails attempt `delays.length + 1` is dead-lettered.
   * Each distinct value creates one shared retry bucket on the broker
   * (`<prefix>.retry.<duration>`), so keep the set of distinct values small.
   * Values must be positive integers; `[]` means "no retry, straight to dead".
   */
  readonly delays: readonly number[]
}

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

/** The bounded-wait escape hatch of the connection-dependent admin/ops calls. */
export interface OpsOptions {
  /** ONE budget for the whole call — connection wait AND the broker round-trips — before {@link ConeyUnavailableError} (finite, <= 2^31-1). Default: wait per {@link ReconnectPolicy}, exactly like {@link Coney.open} without one. */
  timeoutMs?: number
}

export interface CloseResult {
  /** `true` when every in-flight handler settled before the deadline. */
  readonly drained: boolean
  /** Handlers still running when the connection was closed (0 when drained). */
  readonly inFlight: number
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
  declare(defs: readonly AnyDef[], options?: OpsOptions): Promise<void>

  /** Depth of every retry bucket known to this client. Buckets are shared by all queues. */
  inspectRetry(options?: OpsOptions): Promise<RetryBucketStats>
}

/**
 * Create a client — no I/O until {@link Coney.open} or the first publish/consume.
 * One per process: one connection, one confirm channel for publishes, one channel per consumer.
 * @throws {ConeyConfigError} invalid `url`, `prefix`, `reconnect` or `retry`.
 */
export function createConey(options: ConeyOptions): Coney

// ── Definitions (pure values, no I/O) ──

/** Structural schema: anything with a throwing `parse` (zod, valibot `v.parser`, arktype, a hand-written guard). */
export interface Schema<T> {
  parse(input: unknown): T
}

/** A topic → queue binding. The topic's payload type must match the queue's. */
export interface TopicBinding<T> {
  readonly topic: TopicDef<T>
  /** AMQP topic patterns (`*` = one word, `#` = zero or more words). At least one. */
  readonly patterns: readonly string[]
}

export interface QueueDefInput<T> {
  /** Logical name: non-empty, ≤ 200 bytes UTF-8, no whitespace/control chars, not starting with `amq.`/`retry.`, not ending with `.dead`. Case preserved. */
  readonly name: string
  /** Validated on publish (→ {@link ConeySchemaError}) and on consume (→ dead-lettered with reason `schema`). */
  readonly schema?: Schema<T>
  /** Retry ladder. Default: the client's `retry` option. `false` = dead-letter on first failure. */
  readonly retry?: RetryPolicy | false
  /** Make this queue a durable subscriber of topics (competing consumers, full retry/dead). Asserted with the queue. */
  readonly bindings?: readonly TopicBinding<T>[]
}

export interface QueueDef<T> extends QueueDefInput<T> {
  readonly kind: 'queue'
}

export interface TopicDefInput<T> {
  /** Logical exchange name; same rules as queue names. */
  readonly name: string
  /** Validated on publish and on delivery to subscribers/bound queues. */
  readonly schema?: Schema<T>
}

export interface TopicDef<T> extends TopicDefInput<T> {
  readonly kind: 'topic'
}

export type AnyDef = QueueDef<unknown> | TopicDef<unknown>

/**
 * Describe a work queue: a frozen value usable by any client, no I/O. `T` is inferred from `schema` (else `unknown`).
 * @throws {ConeyConfigError} invalid name, non-positive/non-integer delay, empty pattern list.
 */
export function defineQueue<T = unknown>(def: QueueDefInput<T>): QueueDef<T>

/** Describe a topic exchange. Same guarantees as {@link defineQueue}. */
export function defineTopic<T = unknown>(def: TopicDefInput<T>): TopicDef<T>

// ── Publishing ──

/** AMQP header values the library accepts (a flat subset of the AMQP field table). */
export type HeaderValue = string | number | boolean | null

export interface PublishOptions {
  /** AMQP `messageId`. Default: a random UUID v4. Use a deterministic id to let consumers deduplicate. */
  readonly messageId?: string
  /** Application headers. Keys starting with `x-` are reserved for the library and rejected. */
  readonly headers?: Readonly<Record<string, HeaderValue>>
  /** `deliveryMode: 2`. Default `true`; `false` for transient fan-out where loss on broker restart is acceptable. */
  readonly persistent?: boolean
  /** Overrides {@link ConeyOptions.publishTimeoutMs} for this call (finite, <= 2^31-1). */
  readonly timeoutMs?: number
}

// ── Consuming ──

export interface Message<T> {
  /** Decoded body, already validated by the def's schema (when one is set). */
  readonly body: T
  /** AMQP `messageId` (see {@link PublishOptions.messageId}). Stable across retries and redeliveries. */
  readonly id: string
  /** 1 on the first delivery; incremented by each retry (`x-retries` + 1). Broker redeliveries do NOT increment it. */
  readonly attempt: number
  /** AMQP redelivered flag: the broker already delivered this message once and it was not acknowledged. */
  readonly redelivered: boolean
  /** Routing key the message was published with (topic key for bound queues, queue name otherwise). */
  readonly routingKey: string
  /** All AMQP headers, including the library's `x-*` ones. */
  readonly headers: Readonly<Record<string, unknown>>
  /** From the `x-published-at` header (ms precision); absent for messages from foreign producers. */
  readonly publishedAt?: Date
  /** AMQP `appId` — the `name` of the publishing client, if any. */
  readonly appId?: string
  /** Aborted when `timeoutMs` elapses or the close drain deadline passes — pass it to fetch/db calls. */
  readonly signal: AbortSignal
}

/**
 * Outcome is implicit:
 *   - resolve                       → ack
 *   - throw                          → retry (next bucket of the ladder) or dead when the ladder is exhausted
 *   - throw {@link UnrecoverableError} → dead immediately, no retry
 * The library never requeues to the head of the queue.
 */
export type Handler<T> = (message: Message<T>) => void | Promise<void>

export interface ConsumeOptions {
  /** Max messages in flight (= max concurrent handler calls) for this consumer. Default `1`. */
  readonly prefetch?: number
  /** Handler deadline (finite, <= 2^31-1; omit for none — `Infinity` is rejected): on expiry the message fails right away (retry/dead + ack), `signal` is aborted, the late result is ignored (`coney.handler_late`). Default: none. */
  readonly timeoutMs?: number
}

export interface SubscribeOptions {
  /** Max messages in flight for this subscription. Default `1` (in-order processing). */
  readonly prefetch?: number
  /** Same semantics as {@link ConsumeOptions.timeoutMs}, except the outcome is "drop" (subscriptions have no retry). */
  readonly timeoutMs?: number
}

export interface CancelOptions {
  /** Max wait for in-flight handlers (finite, <= 2^31-1). Default `10_000` ms. */
  readonly drainMs?: number
}

export interface Subscription {
  /** `false` once cancelled or after the client closed. */
  readonly active: boolean
  /** Stop receiving (survives reconnects until called). Resolves after in-flight handlers settle or `drainMs` passes. Idempotent. */
  cancel(options?: CancelOptions): Promise<CloseResult>
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

  /** Attach a consumer on its own channel; resolves once registered with the broker. Re-attached after every reconnect AND, on its own backoff loop, whenever it is detached while the connection stays up (broker cancel, channel-level error) — a detach never leaves it silently dead. */
  consume(handler: Handler<T>, options?: ConsumeOptions): Promise<Subscription>

  /** Passive inspection (no topology changes). */
  inspect(options?: OpsOptions): Promise<QueueStats>

  /** Move messages from `<name>.dead` back to the main queue (attempt reset, `messageId` kept), up to `limit` (default: all present at call time). */
  replayDead(
    options?: OpsOptions & { readonly limit?: number },
  ): Promise<{ readonly replayed: number }>

  /** Delete every message in `<name>.dead`. */
  purgeDead(options?: OpsOptions): Promise<{ readonly purged: number }>
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
   * dropped — no retry. Dies with its queue: after a reconnect — or any other detach, which
   * is re-attached the same way as {@link QueueHandle.consume} — a NEW queue is bound and
   * messages in between are lost (`coney.subscription_reset`). Durable delivery → {@link QueueDefInput.bindings}.
   */
  subscribe(patterns: readonly string[], handler: Handler<T>, options?: SubscribeOptions): Promise<Subscription>
}

// ── Errors ──

export type ConeyErrorCode = 'UNAVAILABLE' | 'CLOSED' | 'PUBLISH' | 'SCHEMA' | 'TOPOLOGY' | 'CONFIG'

/** Base class of every error thrown by the library (user handler errors are never wrapped). */
export declare class ConeyError extends Error {
  readonly code: ConeyErrorCode
  constructor(code: ConeyErrorCode, message: string, options?: { cause?: unknown })
}

/** No broker connection within the allowed time, or the reconnect policy gave up. */
export declare class ConeyUnavailableError extends ConeyError {
  readonly code: 'UNAVAILABLE'
  constructor(message: string, options?: { cause?: unknown })
}

/** Operation attempted on a closing/closed client. */
export declare class ConeyClosedError extends ConeyError {
  readonly code: 'CLOSED'
  constructor(message: string, options?: { cause?: unknown })
}

/** The broker refused (nacked) a publish. */
export declare class ConeyPublishError extends ConeyError {
  readonly code: 'PUBLISH'
  constructor(message: string, options?: { cause?: unknown })
}

/** A body failed the def's schema on the publishing side. `cause` is the schema's own error. */
export declare class ConeySchemaError extends ConeyError {
  readonly code: 'SCHEMA'
  readonly cause: unknown
  constructor(message: string, options?: { cause?: unknown })
}

/** Queue/exchange assertion failed — typically AMQP 406 PRECONDITION_FAILED when an existing object has different arguments. */
export declare class ConeyTopologyError extends ConeyError {
  readonly code: 'TOPOLOGY'
  /** Wire name of the offending queue or exchange. */
  readonly target: string
  /** AMQP reply code when available (e.g. 406). */
  readonly amqpCode?: number
  constructor(message: string, info: { target: string; amqpCode?: number; cause?: unknown })
}

/** Invalid options or definitions; always thrown synchronously, never on the network path. */
export declare class ConeyConfigError extends ConeyError {
  readonly code: 'CONFIG'
  constructor(message: string, options?: { cause?: unknown })
}

/** Throw from a handler to dead-letter immediately, skipping the ladder. `x-reason` = `unrecoverable: <message>`. */
export declare class UnrecoverableError extends Error {
  constructor(message: string, options?: { cause?: unknown })
}

// ── Log events (exhaustive catalog) ──

export type LogLevel = 'debug' | 'info' | 'warn' | 'error'

export type ConeyLogEvent =
  | { event: 'coney.connecting'; level: 'info'; attempt: number; url: string }
  | { event: 'coney.connected'; level: 'info'; url: string }
  | { event: 'coney.disconnected'; level: 'warn'; reason: string }
  | { event: 'coney.reconnecting'; level: 'warn'; attempt: number; delayMs: number }
  | { event: 'coney.reconnected'; level: 'info'; downtimeMs: number }
  | { event: 'coney.reconnect_exhausted'; level: 'error'; attempts: number }
  | { event: 'coney.topology_asserted'; level: 'debug'; queues: string[]; exchanges: string[] }
  | { event: 'coney.topology_failed'; level: 'error'; target: string; reason: string; amqpCode?: number }
  | { event: 'coney.consumer_started'; level: 'info'; queue: string; consumerTag: string; prefetch: number }
  | { event: 'coney.consumer_stopped'; level: 'info'; queue: string; consumerTag: string }
  | {
      event: 'coney.consumer_reattach_failed'
      level: 'warn'
      queue: string
      reason: string
      retryInMs: number
    }
  | { event: 'coney.subscription_reset'; level: 'warn'; topic: string; patterns: string[] }
  | { event: 'coney.subscription_error'; level: 'error'; topic: string; messageId: string; reason: string }
  | { event: 'coney.message_retried'; level: 'warn'; queue: string; messageId: string; attempt: number; delayMs: number; reason: string }
  | { event: 'coney.message_dead'; level: 'error'; queue: string; messageId: string; attempt: number; reason: string }
  | { event: 'coney.handler_timeout'; level: 'warn'; queue: string; messageId: string; timeoutMs: number }
  | { event: 'coney.handler_late'; level: 'warn'; queue: string; messageId: string; outcome: 'resolved' | 'rejected' }
  | { event: 'coney.ack_failed'; level: 'warn'; queue: string; messageId: string; reason: string }
  | { event: 'coney.publish_failed'; level: 'warn'; target: string; routingKey: string; reason: string }
  | { event: 'coney.dead_replayed'; level: 'info'; queue: string; replayed: number }
  | { event: 'coney.closing'; level: 'info'; drainMs: number; inFlight: number }
  | { event: 'coney.closed'; level: 'info'; drained: boolean; inFlight: number }

// ── Test double ──

export interface PublishedRecord {
  readonly target: { readonly kind: 'queue' | 'topic'; readonly name: string }
  readonly routingKey: string
  readonly body: unknown
  readonly options: PublishOptions
}

export type DeliveryOutcome =
  | { readonly outcome: 'ack' }
  | { readonly outcome: 'retry'; readonly attempt: number; readonly delayMs: number; readonly error: unknown }
  | { readonly outcome: 'dead'; readonly reason: string; readonly error?: unknown }
  /** Subscriptions only: the handler threw and the message was dropped. */
  | { readonly outcome: 'dropped'; readonly error: unknown }

export interface DeliverOptions {
  /** Simulated attempt number. Default `1`. */
  readonly attempt?: number
  readonly messageId?: string
  readonly headers?: Readonly<Record<string, HeaderValue>>
  readonly redelivered?: boolean
}

/** In-memory {@link Coney}: `publish` records, `consume`/`subscribe` register, {@link ConeyMock.deliver} runs the real consumer pipeline (schema gate, ladder, `timeoutMs`). `close()` mirrors the real lifecycle: state `'closed'`, registrations deactivated, gated operations reject with {@link ConeyClosedError}. */
export interface ConeyMock extends Coney {
  /** Every publish since creation or the last {@link ConeyMock.reset}, in order. */
  readonly published: readonly PublishedRecord[]
  /**
   * Deliver one message to the consumer registered for `def`.
   * @throws {ConeyConfigError} no consumer is registered for the queue.
   */
  deliver<T>(def: QueueDef<T>, body: T, options?: DeliverOptions): Promise<DeliveryOutcome>
  /** Deliver to every subscription and bound queue whose patterns match `routingKey`, in registration order. */
  deliverTopic<T>(def: TopicDef<T>, routingKey: string, body: T, options?: DeliverOptions): Promise<readonly DeliveryOutcome[]>
  /** Clear `published`; registered consumers stay. */
  reset(): void
}

export function createConeyMock(options?: { readonly prefix?: string; readonly name?: string }): ConeyMock

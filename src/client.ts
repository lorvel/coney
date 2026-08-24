/**
 * The client: `createConey`, the state machine, handle caching, and the
 * orchestration of connection, topology and publisher (spec §1, §4).
 */

import { ConeyClosedError, ConeyConfigError, ConeySchemaError } from './errors.js'
import type { AnyDef, QueueDef, TopicDef } from './define.js'
import { validateName, validateRetryPolicy, wireName } from './define.js'
import type { RetryPolicy } from './retry.js'
import { ConnectionManager, DEFAULT_RECONNECT_POLICY, type ReconnectPolicy } from './connection.js'
import { TopologyManager } from './topology.js'
import { Publisher } from './publisher.js'
import { encodeMessage, type PublishOptions } from './codec.js'
import { makeEmitter, redactText, type ConeyLogEvent, type LogEmitter } from './log.js'
import { notImplemented, type ConsumeOptions, type Handler } from './consumer.js'
import type { SubscribeOptions, Subscription } from './subscription.js'

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
  /** Max wait for a (re)connection per publish before {@link ConeyUnavailableError}. Default `5000`; `0` = fail at once. Per-call: {@link PublishOptions.timeoutMs}. */
  publishTimeoutMs?: number
  /** Ladder for queue defs without `retry`. Default `{ delays: [10_000, 60_000, 300_000] }`. */
  retry?: RetryPolicy
  /** Passed through untouched as `amqplib.connect(url, socketOptions)` (TLS, `clientProperties`, …). */
  socketOptions?: Record<string, unknown>
}

export interface OpenOptions {
  /** Reject with {@link ConeyUnavailableError} after this many ms; the reconnect loop keeps running (only `close()` stops it). Default: wait per {@link ReconnectPolicy}. */
  timeoutMs?: number
}

export interface CloseOptions {
  /** Max wait for in-flight handlers after consumers are cancelled. Default `10_000`. On expiry their `signal` is aborted, messages stay unacked (redelivered) and `drained` is `false`. */
  drainMs?: number
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

interface PublishArgs<T> {
  def: QueueDef<T> | TopicDef<T>
  target: string
  exchange: string
  routingKey: string
  typeProp: string
  body: T
  options: PublishOptions | undefined
}

const DEFAULT_RETRY: RetryPolicy = Object.freeze({ delays: Object.freeze([10_000, 60_000, 300_000]) as readonly number[] })
const DEFAULT_PUBLISH_TIMEOUT_MS = 5000
const DEFAULT_DRAIN_MS = 10_000

function validateReconnectPolicy(policy: ReconnectPolicy): void {
  const isNonNegative = (value: number): boolean => typeof value === 'number' && !Number.isNaN(value) && value >= 0
  if (!isNonNegative(policy.initialDelayMs) || !Number.isFinite(policy.initialDelayMs)) {
    throw new ConeyConfigError('reconnect.initialDelayMs must be a finite number >= 0')
  }
  if (!isNonNegative(policy.maxDelayMs) || !Number.isFinite(policy.maxDelayMs) || policy.maxDelayMs < policy.initialDelayMs) {
    throw new ConeyConfigError('reconnect.maxDelayMs must be a finite number >= initialDelayMs')
  }
  if (typeof policy.factor !== 'number' || Number.isNaN(policy.factor) || policy.factor < 1) {
    throw new ConeyConfigError('reconnect.factor must be a number >= 1')
  }
  if (typeof policy.jitter !== 'number' || Number.isNaN(policy.jitter) || policy.jitter < 0 || policy.jitter > 1) {
    throw new ConeyConfigError('reconnect.jitter must be a number between 0 and 1')
  }
  if (!isNonNegative(policy.maxAttempts)) {
    throw new ConeyConfigError('reconnect.maxAttempts must be >= 0 (Infinity allowed)')
  }
}

/**
 * Create a client — no I/O until {@link Coney.open} or the first publish/consume.
 * One per process: one connection, one confirm channel for publishes, one channel per consumer.
 * @throws {ConeyConfigError} invalid `url`, `prefix`, `reconnect` or `retry`.
 */
export function createConey(options: ConeyOptions): Coney {
  if (options === null || typeof options !== 'object') {
    throw new ConeyConfigError('createConey needs an options object with a url')
  }
  if (typeof options.url !== 'string' || options.url.length === 0) {
    throw new ConeyConfigError('url must be a non-empty string')
  }
  let parsed: URL
  try {
    parsed = new URL(options.url)
  } catch (err) {
    throw new ConeyConfigError('url is not a valid URL', { cause: err })
  }
  if (parsed.protocol !== 'amqp:' && parsed.protocol !== 'amqps:') {
    throw new ConeyConfigError(`url must use the amqp:// or amqps:// scheme, got "${parsed.protocol}"`)
  }
  if (options.prefix !== undefined) validateName(options.prefix, 'prefix')
  if (options.name !== undefined && (typeof options.name !== 'string' || options.name.length === 0)) {
    throw new ConeyConfigError('name must be a non-empty string')
  }
  const reconnect: ReconnectPolicy = { ...DEFAULT_RECONNECT_POLICY, ...options.reconnect }
  validateReconnectPolicy(reconnect)
  if (options.publishTimeoutMs !== undefined) {
    const value = options.publishTimeoutMs
    if (typeof value !== 'number' || Number.isNaN(value) || value < 0) {
      throw new ConeyConfigError('publishTimeoutMs must be a number >= 0')
    }
  }
  const retry = options.retry ?? DEFAULT_RETRY
  validateRetryPolicy(retry, 'retry')
  return new ConeyClient(options, reconnect, retry)
}

class ConeyClient implements Coney {
  readonly prefix: string | undefined

  private readonly emit: LogEmitter
  private readonly manager: ConnectionManager
  private readonly topology: TopologyManager
  private readonly publisher: Publisher
  private readonly appId: string | undefined
  private readonly publishTimeoutMs: number
  private readonly queueHandles = new Map<string, QueueHandle<never>>()
  private readonly topicHandles = new Map<string, TopicHandle<never>>()
  private phaseOverride: 'closing' | 'closed' | null = null
  private closePromise: Promise<CloseResult> | null = null
  /** Every accepted publish, from pipeline entry to confirm — close() waits for these. */
  private readonly activePublishes = new Set<Promise<void>>()

  constructor(options: ConeyOptions, reconnect: ReconnectPolicy, defaultRetry: RetryPolicy) {
    this.prefix = options.prefix
    this.appId = options.name
    this.publishTimeoutMs = options.publishTimeoutMs ?? DEFAULT_PUBLISH_TIMEOUT_MS
    this.emit = makeEmitter(options.log)
    this.manager = new ConnectionManager({
      url: options.url,
      socketOptions: options.socketOptions,
      connectionName: options.name !== undefined ? `coney:${options.name}` : 'coney',
      policy: reconnect,
      emit: this.emit,
    })
    this.topology = new TopologyManager({
      prefix: options.prefix,
      defaultRetry,
      manager: this.manager,
      emit: this.emit,
    })
    this.publisher = new Publisher(this.manager)
    this.manager.onSetup = async (model) => {
      await this.topology.replayAll(model)
      await this.publisher.onSetup(model)
    }
    this.manager.onDisconnect = (err) => {
      this.publisher.rejectPending(err)
    }
  }

  get state(): ConeyState {
    if (this.phaseOverride !== null) return this.phaseOverride
    return this.manager.phase
  }

  resolveName(name: string): string {
    validateName(name, 'name')
    return wireName(this.prefix, name)
  }

  async open(options?: OpenOptions): Promise<void> {
    if (this.phaseOverride !== null) throw new ConeyClosedError('the client is closing or closed')
    await this.manager.waitForOpen(options?.timeoutMs)
  }

  async close(options?: CloseOptions): Promise<CloseResult> {
    this.closePromise ??= this.doClose(options?.drainMs ?? DEFAULT_DRAIN_MS)
    return this.closePromise
  }

  queue<T>(def: QueueDef<T>): QueueHandle<T> {
    if (def === null || typeof def !== 'object' || def.kind !== 'queue') {
      throw new ConeyConfigError('queue() needs a def created by defineQueue()')
    }
    const wire = this.topology.register(def) // also rejects a different def with the same name
    const cached = this.queueHandles.get(wire)
    if (cached !== undefined) return cached as QueueHandle<T>
    const handle = this.buildQueueHandle(def, wire)
    this.queueHandles.set(wire, handle as QueueHandle<never>)
    return handle
  }

  topic<T>(def: TopicDef<T>): TopicHandle<T> {
    if (def === null || typeof def !== 'object' || def.kind !== 'topic') {
      throw new ConeyConfigError('topic() needs a def created by defineTopic()')
    }
    const wire = this.topology.register(def)
    const cached = this.topicHandles.get(wire)
    if (cached !== undefined) return cached as TopicHandle<T>
    const handle = this.buildTopicHandle(def, wire)
    this.topicHandles.set(wire, handle as TopicHandle<never>)
    return handle
  }

  async declare(defs: readonly AnyDef[]): Promise<void> {
    if (this.phaseOverride !== null) throw new ConeyClosedError('the client is closing or closed')
    for (const def of defs) this.topology.register(def)
    await this.manager.waitForOpen()
    for (const def of defs) await this.topology.ensure(def)
  }

  async inspectRetry(): Promise<RetryBucketStats> {
    if (this.phaseOverride !== null) throw new ConeyClosedError('the client is closing or closed')
    if (this.topology.knownBuckets().size === 0) return Object.freeze({})
    await this.manager.waitForOpen()
    return Object.freeze(await this.topology.inspectRetry())
  }

  private buildQueueHandle<T>(def: QueueDef<T>, wire: string): QueueHandle<T> {
    return {
      def,
      name: wire,
      publish: (body, options) =>
        this.publishTo({ def, target: wire, exchange: '', routingKey: wire, typeProp: def.name, body, options }),
      consume: () => notImplemented('QueueHandle.consume'),
      inspect: () => notImplemented('QueueHandle.inspect'),
      replayDead: () => notImplemented('QueueHandle.replayDead'),
      purgeDead: () => notImplemented('QueueHandle.purgeDead'),
    }
  }

  private buildTopicHandle<T>(def: TopicDef<T>, wire: string): TopicHandle<T> {
    return {
      def,
      name: wire,
      publish: (routingKey, body, options) => {
        if (typeof routingKey !== 'string' || routingKey.length === 0) {
          return Promise.reject(new ConeyConfigError('routingKey must be a non-empty string'))
        }
        return this.publishTo({ def, target: wire, exchange: wire, routingKey, typeProp: routingKey, body, options })
      },
      subscribe: () => notImplemented('TopicHandle.subscribe'),
    }
  }

  private publishTo<T>(args: PublishArgs<T>): Promise<void> {
    const task = this.runPublish(args)
    this.activePublishes.add(task)
    return task.finally(() => {
      this.activePublishes.delete(task)
    })
  }

  /** The publish pipeline (spec §4): schema → encode → bounded wait → topology → confirm. */
  private async runPublish<T>(args: PublishArgs<T>): Promise<void> {
    if (this.phaseOverride !== null) throw new ConeyClosedError('the client is closing or closed')
    let body: unknown = args.body
    if (args.def.schema !== undefined) {
      try {
        body = args.def.schema.parse(args.body)
      } catch (err) {
        throw new ConeySchemaError(`the body published to "${args.def.name}" failed its schema`, { cause: err })
      }
    }
    // Encoding is pure — do it before any waiting so config errors (reserved
    // headers, non-JSON bodies) fail fast. `timestamp`/`x-published-at` therefore
    // record when publish() was CALLED, which only differs from wire time while a
    // disconnect wait is in progress.
    const encoded = encodeMessage({
      body,
      type: args.typeProp,
      appId: this.appId,
      options: args.options,
      now: new Date(),
    })
    const timeoutMs = args.options?.timeoutMs ?? this.publishTimeoutMs
    try {
      await this.manager.waitForOpen(timeoutMs)
      await this.topology.ensure(args.def)
      await this.publisher.send({ exchange: args.exchange, routingKey: args.routingKey, encoded, timeoutMs })
    } catch (err) {
      this.emit({
        event: 'coney.publish_failed',
        level: 'warn',
        target: args.target,
        routingKey: args.routingKey,
        reason: redactText(err instanceof Error ? err.message : String(err)),
      })
      throw err
    }
  }

  private async doClose(drainMs: number): Promise<CloseResult> {
    this.phaseOverride = 'closing'
    // Slice A has no consumers: nothing to cancel, no in-flight handlers to drain.
    // The consuming slice extends this sequence between `closing` and the confirm wait.
    this.emit({ event: 'coney.closing', level: 'info', drainMs, inFlight: 0 })
    // Accepted publishes finish (or fail on their own bounded timeouts) before the
    // connection goes away — the "fire N publishes, then close()" pattern loses no tail.
    while (this.activePublishes.size > 0) {
      await Promise.allSettled([...this.activePublishes])
    }
    await this.publisher.settleAll()
    await this.manager.closeConnection()
    this.phaseOverride = 'closed'
    this.emit({ event: 'coney.closed', level: 'info', drained: true, inFlight: 0 })
    return Object.freeze({ drained: true, inFlight: 0 })
  }
}

/**
 * The client: `createConey`, the state machine, handle caching, and the
 * orchestration of connection, topology, publisher and consumers (spec §1, §4).
 */

import { ConeyClosedError, ConeyConfigError, ConeyUnavailableError } from '../core/errors.js'
import type { AnyDef, QueueDef, TopicDef } from '../defs/define.js'
import {
  assertQueueDef,
  assertTopicDef,
  parseBody,
  validateInstanceName,
  validateName,
  validateRetryPolicy,
  wireName,
} from '../defs/define.js'
import {
  DEFAULT_RETRY,
  bucketLogicalName,
  effectiveDelays,
  type RetryPolicy,
} from '../defs/retry.js'
import {
  backoffDelayMs,
  resolveReconnectPolicy,
  type ReconnectPolicy,
} from '../connection/policy.js'
import { ConnectionManager } from '../connection/manager.js'
import { TopologyManager } from '../topology/manager.js'
import { Publisher } from '../publish/publisher.js'
import { encodeMessage, validateRoutingKey, type PublishOptions } from '../core/codec.js'
import { makeEmitter, reasonOf, type LogEmitter } from '../core/log.js'
import { validateDurationMs, withinDeadline } from '../core/time.js'
import {
  validateConsumeOptions,
  validateHandler,
  validateSubscribePatterns,
  type CloseResult,
  type ConsumeOptions,
  type Handler,
  type SubscribeOptions,
  type Subscription,
} from '../consume/types.js'
import { BaseConsumer, type ConsumerHooks } from '../consume/runtime.js'
import { QueueConsumer } from '../consume/queue-consumer.js'
import { TopicSubscription } from '../consume/topic-subscription.js'
import { inspectQueue, purgeDeadQueue, replayDeadQueue, type QueueOpsContext } from './queue-ops.js'
import type {
  CloseOptions,
  Coney,
  ConeyOptions,
  ConeyState,
  OpenOptions,
  OpsOptions,
  QueueHandle,
  QueueStats,
  RetryBucketStats,
  TopicHandle,
} from './types.js'

interface PublishArgs<T> {
  def: QueueDef<T> | TopicDef<T>
  target: string
  exchange: string
  routingKey: string
  typeProp: string
  body: T
  options: PublishOptions | undefined
}

const DEFAULT_PUBLISH_TIMEOUT_MS = 5000
const DEFAULT_DRAIN_MS = 10_000
/**
 * Floor for the consumer re-attach backoff. The reconnect policy legally allows
 * `initialDelayMs: 0` ("retry at once"), which is survivable for the connection loop —
 * bounded by `maxAttempts`, and each round costs a real TCP connect against an
 * unreachable endpoint. This loop has no ceiling and each round opens a channel and
 * issues declares against a HEALTHY broker, so a zero delay is a self-inflicted flood.
 */
const MIN_REATTACH_DELAY_MS = 100

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
    throw new ConeyConfigError(
      `url must use the amqp:// or amqps:// scheme, got "${parsed.protocol}"`,
    )
  }
  if (options.prefix !== undefined) validateName(options.prefix, 'prefix')
  if (options.name !== undefined) validateInstanceName(options.name)
  const reconnect = resolveReconnectPolicy(options.reconnect)
  if (options.publishTimeoutMs !== undefined)
    validateDurationMs(options.publishTimeoutMs, 'publishTimeoutMs')
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
  private readonly defaultRetry: RetryPolicy
  /** Kept for the consumer re-attach loop — the connection's own copy lives in the manager. */
  private readonly reconnectPolicy: ReconnectPolicy
  private phaseOverride: 'closing' | 'closed' | null = null
  private closePromise: Promise<CloseResult> | null = null
  /** Every accepted publish, from pipeline entry to confirm — close() waits for these. */
  private readonly activePublishes = new Set<Promise<void>>()
  /** Every live consumer/subscription runtime; re-attached on each `setup`. */
  private readonly consumers = new Set<BaseConsumer>()
  /** Runtimes with a re-attach loop running, so a second detach cannot start a twin. */
  private readonly reattaching = new Set<BaseConsumer>()
  /** Releases for parked re-attach sleeps, so close()/cancel() can drop the runtime now. */
  private readonly reattachWaits = new Map<BaseConsumer, () => void>()
  private tagCounter = 0

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
    this.defaultRetry = defaultRetry
    this.reconnectPolicy = reconnect
    this.manager.onSetup = async (_model, channels) => {
      await this.topology.replayAll(channels)
      await this.publisher.onSetup(channels)
      // No consumer re-attach while close() runs: a reconnect completing inside
      // the drain window must not resurrect cancelled consumers (topology replay
      // and the publisher channel above still run, so accepted tail publishes
      // complete across a mid-close reconnect).
      if (this.phaseOverride !== null) return
      for (const runtime of [...this.consumers]) {
        // A runtime whose attach is already in flight is parked on amqplib's
        // connection waiters — those resolve only AFTER this setup returns, so
        // awaiting it here would deadlock the reconnect forever. Skip it; the
        // parked attach completes on the new connection by itself.
        if (runtime.attachPending) continue
        // One consumer failing to attach (its topology failed this round — already
        // logged by the replay) must not block the others or reject `setup`.
        await runtime.attach(channels).catch(() => {})
      }
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
    if (options?.timeoutMs !== undefined) validateDurationMs(options.timeoutMs, 'timeoutMs')
    await this.manager.waitForOpen(options?.timeoutMs)
  }

  async close(options?: CloseOptions): Promise<CloseResult> {
    if (options?.drainMs !== undefined) validateDurationMs(options.drainMs, 'drainMs')
    this.closePromise ??= this.doClose(options?.drainMs ?? DEFAULT_DRAIN_MS)
    return this.closePromise
  }

  queue<T>(def: QueueDef<T>): QueueHandle<T> {
    assertQueueDef(def)
    const wire = this.topology.register(def) // also rejects a different def with the same name
    const cached = this.queueHandles.get(wire)
    if (cached !== undefined) return cached as QueueHandle<T>
    const handle = this.buildQueueHandle(def, wire)
    this.queueHandles.set(wire, handle as QueueHandle<never>)
    return handle
  }

  topic<T>(def: TopicDef<T>): TopicHandle<T> {
    assertTopicDef(def)
    const wire = this.topology.register(def)
    const cached = this.topicHandles.get(wire)
    if (cached !== undefined) return cached as TopicHandle<T>
    const handle = this.buildTopicHandle(def, wire)
    this.topicHandles.set(wire, handle as TopicHandle<never>)
    return handle
  }

  async declare(defs: readonly AnyDef[], options?: OpsOptions): Promise<void> {
    if (this.phaseOverride !== null) throw new ConeyClosedError('the client is closing or closed')
    const deadline = this.opsDeadline(options)
    for (const def of defs) this.topology.register(def)
    await this.manager.waitForOpen(options?.timeoutMs)
    for (const def of defs) await this.ensureWithinBudget(def, deadline)
  }

  async inspectRetry(options?: OpsOptions): Promise<RetryBucketStats> {
    if (this.phaseOverride !== null) throw new ConeyClosedError('the client is closing or closed')
    const deadline = this.opsDeadline(options)
    if (this.topology.knownBuckets().size === 0) return Object.freeze({})
    await this.manager.waitForOpen(options?.timeoutMs)
    const inspecting = this.topology.inspectRetry()
    const stats = await withinDeadline(
      inspecting,
      deadline,
      () =>
        new ConeyUnavailableError(
          `inspectRetry did not complete within ${String(options?.timeoutMs)} ms`,
        ),
    )
    return Object.freeze(stats)
  }

  /** Validated budget of an admin/ops call as an absolute deadline (`undefined` = none). */
  private opsDeadline(options: OpsOptions | undefined): number | undefined {
    if (options?.timeoutMs === undefined) return undefined
    validateDurationMs(options.timeoutMs, 'timeoutMs')
    return Date.now() + options.timeoutMs
  }

  private buildQueueHandle<T>(def: QueueDef<T>, wire: string): QueueHandle<T> {
    // The free functions in queue-ops.ts cannot take the client itself (its
    // members are private) — hand them exactly what they need.
    const ops: QueueOpsContext = {
      manager: this.manager,
      topology: this.topology,
      emit: this.emit,
      ensureNotClosed: () => this.ensureNotClosed(),
    }
    return {
      def,
      name: wire,
      publish: (body, options) =>
        this.publishTo({
          def,
          target: wire,
          exchange: '',
          routingKey: wire,
          typeProp: def.name,
          body,
          options,
        }),
      consume: (handler, options) => this.startQueueConsumer(def, wire, handler, options),
      inspect: (options): Promise<QueueStats> => inspectQueue(ops, wire, options),
      replayDead: (options) => replayDeadQueue(ops, def, wire, options),
      purgeDead: (options) => purgeDeadQueue(ops, wire, options),
    }
  }

  private buildTopicHandle<T>(def: TopicDef<T>, wire: string): TopicHandle<T> {
    return {
      def,
      name: wire,
      publish: (routingKey, body, options) => {
        try {
          validateRoutingKey(routingKey)
        } catch (err) {
          return Promise.reject(err)
        }
        return this.publishTo({
          def,
          target: wire,
          exchange: wire,
          routingKey,
          typeProp: routingKey,
          body,
          options,
        })
      },
      subscribe: (patterns, handler, options) =>
        this.startSubscription(def, wire, patterns, handler, options),
    }
  }

  /**
   * Track the promise the CALLER gets, not the inner task: `.finally` runs before
   * the derived promise settles, so close()'s drain loop finds the set already
   * empty instead of spinning one redundant extra round on settled entries.
   */
  private publishTo<T>(args: PublishArgs<T>): Promise<void> {
    const tracked: Promise<void> = this.runPublish(args).finally(() => {
      this.activePublishes.delete(tracked)
    })
    this.activePublishes.add(tracked)
    return tracked
  }

  /**
   * The publish pipeline (spec §4): schema → encode → bounded wait → topology →
   * confirm. ONE deadline covers connection wait, lazy topology assert and the
   * confirm-channel wait — a first-use assert racing a disconnect must reject
   * within the budget, never park for the whole outage.
   */
  private async runPublish<T>(args: PublishArgs<T>): Promise<void> {
    if (this.phaseOverride !== null) throw new ConeyClosedError('the client is closing or closed')
    if (args.options?.timeoutMs !== undefined)
      validateDurationMs(args.options.timeoutMs, 'timeoutMs')
    const body = parseBody(args.def, args.body)
    // Encoding is pure — do it before any waiting so config errors (reserved
    // headers, non-JSON bodies, an invalid ttlMs) fail fast. `timestamp`/`x-published-at` therefore
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
    const deadline = Date.now() + timeoutMs
    try {
      await this.manager.waitForOpen(timeoutMs)
      await this.ensureWithinBudget(args.def, deadline)
      const remaining = Math.max(0, deadline - Date.now())
      await this.publisher.send({
        exchange: args.exchange,
        routingKey: args.routingKey,
        encoded,
        timeoutMs: remaining,
      })
    } catch (err) {
      this.emit({
        event: 'coney.publish_failed',
        level: 'warn',
        target: args.target,
        routingKey: args.routingKey,
        reason: reasonOf(err),
      })
      throw err
    }
  }

  /**
   * `ensure` raced against the caller's remaining budget (`undefined` = none), per
   * caller and from the OUTSIDE: the shared in-flight assert must not be rejected
   * for everyone by one caller's timeout, and a timeout must never be cached as a
   * topology failure. An abandoned assert is safe — its channel closes in
   * `assertNow`'s finally whenever the parked create eventually settles, and a late
   * success just marks the def asserted for future callers.
   */
  private async ensureWithinBudget(def: AnyDef, deadline: number | undefined): Promise<void> {
    const ensure = this.topology.ensure(def)
    await withinDeadline(
      ensure,
      deadline,
      () =>
        new ConeyUnavailableError(
          `the topology assert of "${def.name}" did not complete within the allowed time`,
        ),
    )
  }

  /**
   * Spec §4 order: mark runtimes inactive FIRST (a reconnect completing inside
   * the drain window must not resurrect them — mirrors the ordering doCancel
   * uses) → reject new publishes → cancel consumers → drain → confirms → close.
   */
  private async doClose(drainMs: number): Promise<CloseResult> {
    this.phaseOverride = 'closing'
    const runtimes = [...this.consumers]
    for (const runtime of runtimes) runtime.markClosed()
    const inFlightAtStart = runtimes.reduce((sum, runtime) => sum + runtime.inFlightCount(), 0)
    this.emit({ event: 'coney.closing', level: 'info', drainMs, inFlight: inFlightAtStart })
    await Promise.all(runtimes.map((runtime) => runtime.stopDelivery()))
    const deadline = Date.now() + drainMs
    const drainResults = await Promise.all(
      runtimes.map((runtime) => runtime.waitInFlight(deadline)),
    )
    const drained = drainResults.every(Boolean)
    let inFlight = 0
    if (!drained) {
      // Abort the stragglers' signals; their messages stay unacked and the broker
      // redelivers them (never a loss).
      for (const runtime of runtimes) runtime.abortInFlight()
      inFlight = runtimes.reduce((sum, runtime) => sum + runtime.inFlightCount(), 0)
    }
    // Accepted publishes finish (or fail on their own bounded timeouts) before the
    // connection goes away — the "fire N publishes, then close()" pattern loses no tail.
    while (this.activePublishes.size > 0) {
      await Promise.allSettled([...this.activePublishes])
    }
    await this.publisher.settleAll()
    // The channel-close RPC flushes the final acks — a bare connection close
    // would drop the fire-and-forget frames still in amqplib's write buffer.
    // (A runtime whose attach is still in flight has no channel here; its
    // post-attach check tears the late channel down as soon as the attach settles.)
    await Promise.all(runtimes.map((runtime) => runtime.releaseChannel()))
    // Drop the runtimes: each one holds the user's handler and everything its
    // closure captured, and a closed client can never re-attach them. A parked
    // re-attach sleep would keep exactly those alive behind its timer closure for
    // the rest of its delay, so release them here rather than let them expire.
    for (const release of [...this.reattachWaits.values()]) release()
    this.consumers.clear()
    await this.manager.closeConnection()
    this.phaseOverride = 'closed'
    this.emit({ event: 'coney.closed', level: 'info', drained, inFlight })
    return Object.freeze({ drained, inFlight })
  }

  private ensureNotClosed(): void {
    if (this.phaseOverride !== null) throw new ConeyClosedError('the client is closing or closed')
  }

  private nextConsumerTag(logical: string): string {
    this.tagCounter += 1
    return `${this.appId ?? 'coney'}.${logical}.${this.tagCounter}`
  }

  private async startQueueConsumer<T>(
    def: QueueDef<T>,
    wire: string,
    handler: Handler<T>,
    options: ConsumeOptions | undefined,
  ): Promise<Subscription> {
    this.ensureNotClosed()
    validateHandler(handler, 'consume')
    validateConsumeOptions(options)
    const runtime = new QueueConsumer<T>({
      def,
      wire,
      handler,
      options,
      delays: effectiveDelays(def.retry, this.defaultRetry),
      bucketWire: (delayMs) => wireName(this.prefix, bucketLogicalName(delayMs)),
      consumerTag: this.nextConsumerTag(def.name),
      emit: this.emit,
      hooks: this.hooksFor(def),
    })
    return this.startRuntime(runtime, def)
  }

  private async startSubscription<T>(
    def: TopicDef<T>,
    wire: string,
    patterns: readonly string[],
    handler: Handler<T>,
    options: SubscribeOptions | undefined,
  ): Promise<Subscription> {
    this.ensureNotClosed()
    validateHandler(handler, 'subscribe')
    validateSubscribePatterns(patterns)
    validateConsumeOptions(options)
    const runtime = new TopicSubscription<T>({
      def,
      topicWire: wire,
      patterns: [...patterns],
      handler,
      options,
      consumerTag: this.nextConsumerTag(def.name),
      emit: this.emit,
      hooks: this.hooksFor(def),
    })
    return this.startRuntime(runtime, def)
  }

  /** Register, wait for a connection, assert the def, attach — or roll back cleanly. */
  private async startRuntime(runtime: BaseConsumer, def: AnyDef): Promise<Subscription> {
    this.consumers.add(runtime)
    try {
      await this.manager.waitForOpen()
      await this.topology.ensure(def)
      // `setup` may have attached it already (a reconnect finished in between) —
      // attach() is a no-op while the runtime holds a live channel.
      await runtime.attach(this.manager)
      return runtime.subscriptionFacade()
    } catch (err) {
      this.consumers.delete(runtime)
      // Full teardown, not just a flag: setup may have attached this runtime
      // before `ensure` threw (its topology failure was cached by the replay
      // while the queue itself still exists) — leaving the channel open would
      // park a phantom consumer that swallows deliveries without acking.
      await runtime.dispose()
      throw err
    }
  }

  /** The lifecycle hooks every runtime gets; named fields, so the pair cannot transpose. */
  private hooksFor(def: AnyDef): ConsumerHooks {
    return {
      onDispose: (disposed) => {
        this.consumers.delete(disposed)
        // Release a parked re-attach sleep NOW: its timer closure is what would keep
        // the runtime, the user's handler and everything it captured reachable.
        this.reattachWaits.get(disposed)?.()
      },
      onDetached: (detached) => void this.reattachDetached(detached, def),
    }
  }

  /** Every reason the re-attach loop must give up, in one place (checked at each await). */
  private stopReattach(runtime: BaseConsumer): boolean {
    return !runtime.active || this.phaseOverride !== null || this.manager.phase === 'closed'
  }

  /**
   * Bring back a runtime that lost its channel without asking to (spec §4).
   *
   * The assumption this exists to kill is that a reconnect is always coming. A broker
   * cancel with the connection still open — deleted queue, failed queue node, an
   * operator deleting a queue in the management UI — leaves nothing to trigger `setup`,
   * so waiting for `setup` means waiting forever: the process keeps its connection,
   * keeps publishing, keeps passing every health check, and silently never consumes
   * again. A channel-level error (404/406/403) reaches this the same way and was even
   * quieter: amqplib delivers `null` only for a real `basic.cancel`, so that runtime
   * detached with no event at all.
   *
   * The topology is re-asserted before attaching because the usual cause is that the
   * queue is GONE; a bare `consume` would 404 and kill its channel every round.
   *
   * Racing a real reconnect is safe: `attach` parks on the manager's open wait, the
   * setup hook skips a runtime whose attach is pending (see the deadlock note there),
   * and a round that wakes to find the runtime already attached returns without
   * touching the broker at all — which is what makes it cheap to hand EVERY detach here
   * rather than only the ones no reconnect will repair.
   */
  private async reattachDetached(runtime: BaseConsumer, def: AnyDef): Promise<void> {
    if (this.reattaching.has(runtime)) return // one loop per runtime; rounds are idempotent
    this.reattaching.add(runtime)
    try {
      let attempt = 0
      // No attempt ceiling, deliberately. `maxAttempts` bounds the CONNECTION loop,
      // where giving up is loud and observable: `coney.reconnect_exhausted`, state
      // `closed`, pending work rejected. Giving up HERE would leave a silently dead
      // consumer on a healthy connection — precisely the failure this loop exists to
      // end. It stops only when there is nothing left to re-attach to: the runtime was
      // cancelled, the client is closing, or the connection itself is terminally gone
      // (`stopReattach`) — the last of which is exactly the exhausted-policy state, and
      // is why an unbounded loop here does not mean an immortal one.
      for (;;) {
        const delay = Math.max(
          MIN_REATTACH_DELAY_MS,
          backoffDelayMs(this.reconnectPolicy, ++attempt),
        )
        const stop = await this.sleepUnlessStopped(runtime, delay)
        if (stop) return
        try {
          await this.manager.waitForOpen()
          // The open wait is unbounded and can park for a whole outage: re-check
          // before mutating shared topology on behalf of a runtime that is now gone.
          // `attached` covers the reconnect that repaired it while we slept.
          if (this.stopReattach(runtime) || runtime.attached) return
          // `reassert`, not `ensure`: the queue is very likely GONE (that is what
          // detached the consumer), and `ensure` would trust its `asserted` cache —
          // which only clears on a reconnect — and skip the redeclare, leaving
          // `attach` to 404 for good.
          await this.topology.reassert(def)
          await runtime.attach(this.manager)
          return
        } catch (err) {
          if (this.stopReattach(runtime)) return
          this.emit({
            event: 'coney.consumer_reattach_failed',
            level: 'warn',
            queue: runtime.queueLabel,
            reason: reasonOf(err),
            retryInMs: Math.max(
              MIN_REATTACH_DELAY_MS,
              backoffDelayMs(this.reconnectPolicy, attempt + 1),
            ),
          })
        }
      }
    } finally {
      this.reattaching.delete(runtime)
    }
  }

  /**
   * Sleep, then say whether the re-attach loop should stop. Unref'd on purpose: a
   * pending re-attach must never be the reason a process refuses to exit. Cancellable
   * on purpose too — `unref` drops the event-loop reference but NOT the timer entry, so
   * an uncancelled sleep holds its closure (and the runtime behind it) until it fires,
   * which is up to `maxDelayMs` after `close()`/`cancel()` already resolved.
   */
  private sleepUnlessStopped(runtime: BaseConsumer, ms: number): Promise<boolean> {
    return new Promise((resolve) => {
      const settle = (): void => {
        clearTimeout(timer)
        this.reattachWaits.delete(runtime)
        resolve(this.stopReattach(runtime))
      }
      const timer = setTimeout(settle, ms)
      timer.unref()
      this.reattachWaits.set(runtime, settle)
    })
  }
}

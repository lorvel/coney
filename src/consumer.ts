/**
 * Consuming: the per-delivery pipeline (spec §4 — shared verbatim with the test
 * double so the mock cannot drift from the runtime) and the channel-per-consumer
 * runtime for work queues. Iron rules: nothing thrown by user code may reach
 * amqplib's emitter, and a retry/dead publish is CONFIRMED before the original
 * delivery is acked — a crash in between yields a duplicate, never a loss.
 */

import type { Channel, ConfirmChannel, ConsumeMessage } from 'amqplib'
import { ConeyConfigError, UnrecoverableError } from './errors.js'
import type { QueueDef, Schema } from './define.js'
import { redactText, type LogEmitter } from './log.js'
import type { CancelOptions, SubscribeOptions, Subscription } from './subscription.js'
// Type-only: no runtime cycle (client.ts imports the runtimes as values).
import type { CloseResult } from './client.js'

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
  /** Handler deadline: on expiry the message fails right away (retry/dead + ack), `signal` is aborted, the late result is ignored (`coney.handler_late`). Default: none. */
  readonly timeoutMs?: number
}

/** Shared by `consume()`, `subscribe()` and the mock (the option shapes are identical). */
export function validateConsumeOptions(options: ConsumeOptions | SubscribeOptions | undefined): void {
  if (options === undefined) return
  if (options.prefetch !== undefined && (!Number.isInteger(options.prefetch) || options.prefetch < 1)) {
    throw new ConeyConfigError('prefetch must be an integer >= 1')
  }
  if (
    options.timeoutMs !== undefined &&
    (typeof options.timeoutMs !== 'number' || Number.isNaN(options.timeoutMs) || options.timeoutMs <= 0)
  ) {
    throw new ConeyConfigError('timeoutMs must be a positive number')
  }
}

// ── The shared pipeline ──

/** `x-reason` values are capped (spec §3); events reuse the same capped string. */
export function truncateReason(reason: string): string {
  return reason.length > 500 ? reason.slice(0, 500) : reason
}

/** Human text of an unknown thrown value, credentials redacted. */
export function reasonOf(error: unknown): string {
  return redactText(error instanceof Error ? error.message : String(error))
}

/** Everything about one delivery except the body. */
export interface DeliveryContext {
  id: string
  attempt: number
  redelivered: boolean
  routingKey: string
  headers: Readonly<Record<string, unknown>>
  publishedAt?: Date
  appId?: string
}

export type PipelineOutcome =
  | { kind: 'ack' }
  | { kind: 'retry'; attempt: number; delayMs: number; error: unknown; reason: string }
  | { kind: 'dead'; attempt: number; reason: string; error?: unknown }
  /** Subscription mode only (`delays: null`): any failure drops the message. */
  | { kind: 'drop'; error: unknown; reason: string }

export interface PipelineArgs<T> {
  schema: Schema<T> | undefined
  /** Decoded body, BEFORE the schema gate. */
  body: unknown
  context: DeliveryContext
  handler: Handler<T>
  /** Created by the caller so it can also abort on the close drain deadline. */
  controller: AbortController
  timeoutMs: number | undefined
  /** Queue mode: the retry ladder. Subscription mode: `null` (failure → drop). */
  delays: readonly number[] | null
  onTimeout?: (timeoutMs: number) => void
  onLate?: (outcome: 'resolved' | 'rejected') => void
}

/**
 * One delivery through the spec §4 table: schema gate → handler (with deadline)
 * → outcome. Never throws, never lets a user throw escape. The caller maps the
 * outcome onto broker operations (ack / retry publish / dead publish / drop) —
 * or, in the mock, returns it directly.
 */
export async function runConsumePipeline<T>(args: PipelineArgs<T>): Promise<PipelineOutcome> {
  const { context, delays } = args
  let body: T
  try {
    body = args.schema !== undefined ? args.schema.parse(args.body) : (args.body as T)
  } catch (err) {
    const reason = truncateReason(`schema: ${reasonOf(err)}`)
    if (delays === null) return { kind: 'drop', error: err, reason }
    return { kind: 'dead', attempt: context.attempt, reason, error: err }
  }
  const message: Message<T> = {
    body,
    id: context.id,
    attempt: context.attempt,
    redelivered: context.redelivered,
    routingKey: context.routingKey,
    headers: context.headers,
    ...(context.publishedAt !== undefined ? { publishedAt: context.publishedAt } : {}),
    ...(context.appId !== undefined ? { appId: context.appId } : {}),
    signal: args.controller.signal,
  }
  const result = await invokeHandler(message, args)
  if (result.ok) return { kind: 'ack' }
  const { error } = result
  const reason = truncateReason(reasonOf(error))
  if (delays === null) return { kind: 'drop', error, reason }
  if (error instanceof UnrecoverableError) {
    return { kind: 'dead', attempt: context.attempt, reason: truncateReason(`unrecoverable: ${reasonOf(error)}`), error }
  }
  const delayMs = delays[context.attempt - 1]
  if (delayMs !== undefined) {
    return { kind: 'retry', attempt: context.attempt, delayMs, error, reason }
  }
  return {
    kind: 'dead',
    attempt: context.attempt,
    reason: truncateReason(`exhausted after ${context.attempt} attempts: ${reasonOf(error)}`),
    error,
  }
}

/** Run the handler under the optional deadline; on expiry fail NOW and only log the late settlement. */
function invokeHandler<T>(
  message: Message<T>,
  args: PipelineArgs<T>,
): Promise<{ ok: true } | { ok: false; error: unknown }> {
  // An async wrapper turns a synchronous handler throw into a rejection.
  const invocation = (async () => {
    await args.handler(message)
  })()
  const { timeoutMs } = args
  return new Promise((resolve) => {
    let done = false
    let timer: NodeJS.Timeout | undefined
    if (timeoutMs !== undefined) {
      timer = setTimeout(() => {
        if (done) return
        done = true
        args.onTimeout?.(timeoutMs)
        const timeoutError = new Error(`handler timeout after ${timeoutMs} ms`)
        args.controller.abort(timeoutError)
        // The abandoned invocation settles later; observe it so the rejection is
        // handled and the outcome is visible in the log (`coney.handler_late`).
        invocation.then(
          () => args.onLate?.('resolved'),
          () => args.onLate?.('rejected'),
        )
        resolve({ ok: false, error: timeoutError })
      }, timeoutMs)
    }
    invocation.then(
      () => {
        if (done) return
        done = true
        if (timer !== undefined) clearTimeout(timer)
        resolve({ ok: true })
      },
      (error: unknown) => {
        if (done) return
        done = true
        if (timer !== undefined) clearTimeout(timer)
        resolve({ ok: false, error })
      },
    )
  })
}

/** Read the delivery context out of a raw amqplib message (foreign producers included). */
export function contextOf(raw: ConsumeMessage): DeliveryContext {
  const properties = raw.properties as {
    messageId?: unknown
    appId?: unknown
    headers?: Record<string, unknown>
  }
  const headers: Readonly<Record<string, unknown>> = properties.headers ?? {}
  const retries = headers['x-retries']
  const publishedAtRaw = headers['x-published-at']
  let publishedAt: Date | undefined
  if (typeof publishedAtRaw === 'string') {
    const parsed = new Date(publishedAtRaw)
    if (!Number.isNaN(parsed.getTime())) publishedAt = parsed
  }
  return {
    // The contract requires a string id; a foreign message without one gets ''
    // (a generated id would not be stable across redeliveries).
    id: typeof properties.messageId === 'string' ? properties.messageId : '',
    attempt: typeof retries === 'number' && Number.isInteger(retries) && retries >= 0 ? retries + 1 : 1,
    redelivered: raw.fields.redelivered,
    routingKey: raw.fields.routingKey,
    headers,
    ...(publishedAt !== undefined ? { publishedAt } : {}),
    ...(typeof properties.appId === 'string' && properties.appId !== '' ? { appId: properties.appId } : {}),
  }
}

// ── Runtime plumbing shared by queue consumers and topic subscriptions ──

/** How a runtime gets channels: the manager (lazy paths) or the raw model (inside `setup`). */
export interface ChannelFactory {
  createChannel(): Promise<Channel>
  createConfirmChannel(): Promise<ConfirmChannel>
}

/** What the client needs from every consumer/subscription it tracks. */
export interface ConsumerRuntime {
  readonly active: boolean
  /** (Re)attach when not attached; throws (initial `consume()` propagates, `setup` catches). */
  attach(factory: ChannelFactory): Promise<void>
  /** `basic.cancel` + stop taking new deliveries; never throws. */
  stopDelivery(): Promise<void>
  inFlightCount(): number
  /** Wait until in-flight deliveries settle or the absolute deadline passes; true when drained. */
  waitInFlight(deadlineMs: number): Promise<boolean>
  /** Abort the `signal` of every in-flight handler (close drain expired). */
  abortInFlight(): void
  /** Mark inactive without draining (client close finished). */
  markClosed(): void
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms))
}

const DEFAULT_CANCEL_DRAIN_MS = 10_000

/** State + behavior shared by {@link QueueConsumer} and the topic subscription runtime. */
export abstract class BaseConsumer implements ConsumerRuntime {
  protected channel: Channel | ConfirmChannel | null = null
  protected consumerTag: string | null = null
  protected activeFlag = true
  protected readonly inFlight = new Set<Promise<void>>()
  protected readonly controllers = new Set<AbortController>()
  private attaching: Promise<void> | null = null
  private cancelPromise: Promise<CloseResult> | null = null

  constructor(
    protected readonly tag: string,
    protected readonly prefetchCount: number,
    protected readonly emit: LogEmitter,
    private readonly onDispose: (runtime: BaseConsumer) => void,
  ) {}

  get active(): boolean {
    return this.activeFlag
  }

  async attach(factory: ChannelFactory): Promise<void> {
    if (!this.activeFlag || this.channel !== null) return
    this.attaching ??= this.doAttach(factory).finally(() => {
      this.attaching = null
    })
    await this.attaching
  }

  /** Create the channel, declare what the consumer needs, start consuming. */
  protected abstract doAttach(factory: ChannelFactory): Promise<void>

  /** Track a channel; detaches this runtime when the channel dies (reconnects re-attach via `setup`). */
  protected adoptChannel(channel: Channel | ConfirmChannel): void {
    channel.on('close', () => {
      if (this.channel === channel) {
        this.channel = null
        this.consumerTag = null
      }
    })
    this.channel = channel
  }

  /**
   * The sync amqplib consume callback: detach into a tracked async task; NEVER
   * throw. `channel` comes through the closure, NOT `this.channel` — a delivery
   * can arrive between the consume-ok and the continuation that records the
   * channel on the runtime, and reading the field there silently dropped the
   * message while it stayed unacked (caught by the forced-disconnect test).
   */
  protected onDelivery(channel: Channel | ConfirmChannel, raw: ConsumeMessage | null): void {
    try {
      if (raw === null) {
        this.handleBrokerCancel()
        return
      }
      if (!this.activeFlag) return
      const task = this.processDelivery(channel, raw).catch((err: unknown) => {
        // processDelivery handles its own failures; anything here is a coney bug.
        process.emitWarning(err instanceof Error ? err : new Error(String(err)))
      })
      this.inFlight.add(task)
      void task.finally(() => {
        this.inFlight.delete(task)
      })
    } catch (err) {
      process.emitWarning(err instanceof Error ? err : new Error(String(err)))
    }
  }

  protected abstract processDelivery(channel: Channel | ConfirmChannel, raw: ConsumeMessage): Promise<void>

  /** Broker-initiated cancel (queue deleted): drop the channel; the next `setup` re-attaches. */
  protected abstract handleBrokerCancel(): void

  async stopDelivery(): Promise<void> {
    const { channel, consumerTag } = this
    this.consumerTag = null
    if (channel !== null && consumerTag !== null) {
      await channel.cancel(consumerTag).catch(() => {})
    }
  }

  inFlightCount(): number {
    return this.inFlight.size
  }

  async waitInFlight(deadlineMs: number): Promise<boolean> {
    while (this.inFlight.size > 0) {
      const remaining = deadlineMs - Date.now()
      if (remaining <= 0) return false
      await Promise.race([Promise.allSettled([...this.inFlight]), sleep(remaining)])
    }
    return true
  }

  abortInFlight(): void {
    for (const controller of this.controllers) {
      controller.abort(new Error('the close drain deadline passed'))
    }
  }

  /**
   * Close the channel with its RPC handshake. Load-bearing during shutdown: an
   * `ack` is a fire-and-forget frame and amqplib's connection close does NOT
   * flush pending writes (verified on 2.0.1 — an ack followed at once by a
   * connection close is lost and the message redelivered). The CloseOk
   * round-trip forces every prior frame on the channel through the broker.
   */
  async releaseChannel(): Promise<void> {
    const { channel } = this
    this.channel = null
    this.consumerTag = null
    if (channel !== null) await channel.close().catch(() => {})
  }

  markClosed(): void {
    this.activeFlag = false
  }

  /** The `Subscription.cancel` path: stop, drain (bounded), abort the rest, release the channel. */
  cancel(options?: CancelOptions): Promise<CloseResult> {
    this.cancelPromise ??= this.doCancel(options?.drainMs ?? DEFAULT_CANCEL_DRAIN_MS)
    return this.cancelPromise
  }

  private async doCancel(drainMs: number): Promise<CloseResult> {
    this.activeFlag = false
    this.onDispose(this)
    await this.stopDelivery()
    const drained = await this.waitInFlight(Date.now() + drainMs)
    if (!drained) this.abortInFlight()
    const inFlight = this.inFlight.size
    await this.releaseChannel()
    return Object.freeze({ drained, inFlight })
  }

  /** The user-facing handle (a stable facade; `active` reflects the runtime). */
  subscriptionFacade(): Subscription {
    const runtime = this
    return {
      get active(): boolean {
        return runtime.activeFlag
      },
      cancel: (options?: CancelOptions) => runtime.cancel(options),
    }
  }
}

// ── Work-queue consumer ──

interface QueueConsumerArgs<T> {
  def: QueueDef<T>
  /** Wire name of the consumed queue — also the routing key of every retry publish. */
  wire: string
  handler: Handler<T>
  options: ConsumeOptions | undefined
  /** Effective ladder (def override or client default). */
  delays: readonly number[]
  /** Wire name of the retry bucket for a delay. */
  bucketWire: (delayMs: number) => string
  consumerTag: string
  emit: LogEmitter
  onDispose: (runtime: BaseConsumer) => void
}

/** amqplib publish options rebuilt from a delivery's properties: what coney understands, headers replaced. */
export function republishOptions(rawProperties: unknown, headers: Record<string, unknown>): Record<string, unknown> {
  const properties = (rawProperties ?? {}) as Record<string, unknown>
  const options: Record<string, unknown> = { headers }
  // Whitelisted pass-through: userId is dropped (RabbitMQ rejects a mismatch with
  // the republishing connection's user), expiration too (a per-message TTL must not
  // silently expire messages parked in retry/dead queues).
  for (const key of [
    'messageId',
    'timestamp',
    'type',
    'appId',
    'deliveryMode',
    'contentType',
    'contentEncoding',
    'correlationId',
    'replyTo',
    'priority',
  ]) {
    const value = properties[key]
    if (value !== undefined) options[key] = value
  }
  return options
}

export class QueueConsumer<T> extends BaseConsumer {
  constructor(private readonly args: QueueConsumerArgs<T>) {
    super(args.consumerTag, args.options?.prefetch ?? 1, args.emit, args.onDispose)
  }

  protected async doAttach(factory: ChannelFactory): Promise<void> {
    const channel = await factory.createConfirmChannel()
    try {
      await channel.prefetch(this.prefetchCount)
      // Adopt BEFORE consume: deliveries may arrive the moment the broker sees
      // the consumer (a failed consume un-adopts through the close listener).
      this.adoptChannel(channel)
      const ok = await channel.consume(this.args.wire, (raw) => this.onDelivery(channel, raw), { consumerTag: this.tag })
      this.consumerTag = ok.consumerTag
      this.emit({
        event: 'coney.consumer_started',
        level: 'info',
        queue: this.args.wire,
        consumerTag: ok.consumerTag,
        prefetch: this.prefetchCount,
      })
    } catch (err) {
      await channel.close().catch(() => {}) // a failed consume (404) already killed it
      throw err
    }
  }

  protected handleBrokerCancel(): void {
    const { channel } = this
    this.channel = null
    this.consumerTag = null
    this.emit({ event: 'coney.consumer_stopped', level: 'info', queue: this.args.wire, consumerTag: this.tag })
    if (channel !== null) void channel.close().catch(() => {})
  }

  protected async processDelivery(channel: Channel | ConfirmChannel, raw: ConsumeMessage): Promise<void> {
    const context = contextOf(raw)
    const outcome = await this.runDelivery(raw, context)
    await this.settleOutcome(channel as ConfirmChannel, raw, context, outcome)
  }

  private async runDelivery(raw: ConsumeMessage, context: DeliveryContext): Promise<PipelineOutcome> {
    let body: unknown
    try {
      body = JSON.parse(raw.content.toString('utf8'))
    } catch (err) {
      return { kind: 'dead', attempt: context.attempt, reason: truncateReason(`decode: ${reasonOf(err)}`), error: err }
    }
    const controller = new AbortController()
    this.controllers.add(controller)
    try {
      return await runConsumePipeline({
        schema: this.args.def.schema,
        body,
        context,
        handler: this.args.handler,
        controller,
        timeoutMs: this.args.options?.timeoutMs,
        delays: this.args.delays,
        onTimeout: (timeoutMs) =>
          this.emit({ event: 'coney.handler_timeout', level: 'warn', queue: this.args.wire, messageId: context.id, timeoutMs }),
        onLate: (late) =>
          this.emit({ event: 'coney.handler_late', level: 'warn', queue: this.args.wire, messageId: context.id, outcome: late }),
      })
    } finally {
      this.controllers.delete(controller)
    }
  }

  private async settleOutcome(
    channel: ConfirmChannel,
    raw: ConsumeMessage,
    context: DeliveryContext,
    outcome: PipelineOutcome,
  ): Promise<void> {
    if (outcome.kind === 'ack') {
      this.ack(channel, raw, context)
      return
    }
    if (outcome.kind === 'retry') {
      const confirmed = await this.publishInternal(channel, {
        exchange: this.args.bucketWire(outcome.delayMs),
        // The queue's own wire name — NEVER fields.routingKey (a bound queue's key
        // is the topic key; the bucket's DLX delivers by the key it was published with).
        routingKey: this.args.wire,
        content: raw.content,
        options: republishOptions(raw.properties, {
          ...context.headers,
          'x-retries': outcome.attempt,
          'x-retry-at': new Date(Date.now() + outcome.delayMs).toISOString(),
        }),
      })
      if (confirmed) {
        this.ack(channel, raw, context)
        this.emit({
          event: 'coney.message_retried',
          level: 'warn',
          queue: this.args.wire,
          messageId: context.id,
          attempt: outcome.attempt,
          delayMs: outcome.delayMs,
          reason: outcome.reason,
        })
      }
      return
    }
    if (outcome.kind === 'dead') {
      const confirmed = await this.publishInternal(channel, {
        exchange: '',
        routingKey: `${this.args.wire}.dead`,
        content: raw.content,
        options: republishOptions(raw.properties, {
          ...context.headers,
          'x-reason': outcome.reason,
          'x-failed-at': new Date().toISOString(),
        }),
      })
      if (confirmed) {
        this.ack(channel, raw, context)
        this.emit({
          event: 'coney.message_dead',
          level: 'error',
          queue: this.args.wire,
          messageId: context.id,
          attempt: outcome.attempt,
          reason: outcome.reason,
        })
      }
      return
    }
    // 'drop' cannot happen in queue mode (delays is never null here).
  }

  /**
   * Publish retry/dead on the consuming channel and wait for the broker confirm.
   * `false` = the channel died mid-flight: the original stays unacked and the
   * broker redelivers it with the SAME attempt — the "free retry" (spec §4).
   */
  private async publishInternal(
    channel: ConfirmChannel,
    args: { exchange: string; routingKey: string; content: Buffer; options: Record<string, unknown> },
  ): Promise<boolean> {
    try {
      await new Promise<void>((resolve, reject) => {
        channel.publish(args.exchange, args.routingKey, args.content, args.options, (err) => {
          if (err == null) resolve()
          else reject(err instanceof Error ? err : new Error(String(err)))
        })
      })
      return true
    } catch (err) {
      this.emit({
        event: 'coney.publish_failed',
        level: 'warn',
        target: args.exchange === '' ? args.routingKey : args.exchange,
        routingKey: args.routingKey,
        reason: reasonOf(err),
      })
      return false
    }
  }

  private ack(channel: ConfirmChannel, raw: ConsumeMessage, context: DeliveryContext): void {
    try {
      channel.ack(raw)
    } catch (err) {
      // Channel died between confirm and ack: the broker redelivers (duplicate,
      // never a loss — the at-least-once contract).
      this.emit({ event: 'coney.ack_failed', level: 'warn', queue: this.args.wire, messageId: context.id, reason: reasonOf(err) })
    }
  }
}

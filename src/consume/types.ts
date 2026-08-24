/**
 * The consuming contract: what a handler receives, the consume/subscribe options
 * and the `Subscription` returned by both. `CloseResult` lives here too — it is
 * the drain result, produced wherever in-flight handlers are waited on
 * (`Subscription.cancel` and `Coney.close`).
 */

import { ConeyConfigError } from '../core/errors.js'
import { validateDurationMs } from '../core/time.js'

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

export interface CloseResult {
  /** `true` when every in-flight handler settled before the deadline. */
  readonly drained: boolean
  /** Handlers still running when the connection was closed (0 when drained). */
  readonly inFlight: number
}

export interface Subscription {
  /** `false` once cancelled or after the client closed. */
  readonly active: boolean
  /** Stop receiving (survives reconnects until called). Resolves after in-flight handlers settle or `drainMs` passes. Idempotent. */
  cancel(options?: CancelOptions): Promise<CloseResult>
}

/** Shared by `consume()`, `subscribe()` and the mock (the option shapes are identical). */
export function validateConsumeOptions(options: ConsumeOptions | SubscribeOptions | undefined): void {
  if (options === undefined) return
  if (options.prefetch !== undefined && (!Number.isInteger(options.prefetch) || options.prefetch < 1)) {
    throw new ConeyConfigError('prefetch must be an integer >= 1')
  }
  // "No deadline" is spelled by OMITTING timeoutMs — Infinity would be clamped
  // to 1 ms by setTimeout and abort every handler immediately.
  if (options.timeoutMs !== undefined) validateDurationMs(options.timeoutMs, 'timeoutMs', { positive: true })
}

/** Shared by the real client and the mock. */
export function validateHandler(handler: unknown, verb: 'consume' | 'subscribe'): void {
  if (typeof handler !== 'function') {
    throw new ConeyConfigError(`${verb}() needs a handler function`)
  }
}

/** Shared by the real client and the mock. */
export function validateSubscribePatterns(patterns: unknown): asserts patterns is readonly string[] {
  if (!Array.isArray(patterns) || patterns.length === 0) {
    throw new ConeyConfigError('subscribe() needs at least one binding pattern')
  }
  for (const pattern of patterns) {
    if (typeof pattern !== 'string' || pattern.length === 0) {
      throw new ConeyConfigError('binding patterns must be non-empty strings')
    }
  }
}

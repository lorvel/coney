/**
 * The per-delivery pipeline (spec §4), shared VERBATIM between the broker
 * runtimes and the test double so the mock cannot drift from production: expiry
 * gate → schema gate → handler under the optional deadline → outcome. Pure with
 * respect to the broker — the caller maps the outcome onto ack/retry/dead/drop
 * operations.
 */

import { UnrecoverableError } from '../core/errors.js'
import { reasonOf } from '../core/log.js'
import { nextDelayMs } from '../defs/retry.js'
import type { Schema } from '../defs/define.js'
import type { Handler, Message } from './types.js'

/** `x-reason` values are capped (spec §3); events reuse the same capped string. */
export function truncateReason(reason: string): string {
  return reason.length > 500 ? reason.slice(0, 500) : reason
}

/** Everything about one delivery except the body. */
export interface DeliveryContext {
  id: string
  attempt: number
  redelivered: boolean
  routingKey: string
  headers: Readonly<Record<string, unknown>>
  publishedAt?: Date
  /** From `x-expires-at` (`PublishOptions.ttlMs`); absent = no deadline. */
  expiresAt?: Date
  appId?: string
}

export type PipelineOutcome =
  | { kind: 'ack' }
  | { kind: 'retry'; attempt: number; delayMs: number; error: unknown; reason: string }
  | { kind: 'dead'; attempt: number; reason: string; error?: unknown }
  /** Subscription mode only (`delays: null`): any failure drops the message. */
  | { kind: 'drop'; error: unknown; reason: string }
  /** The deadline had passed: acked without calling the handler, in both modes. */
  | { kind: 'expired'; expiresAt: Date }

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
 * One delivery through the spec §4 table: expiry gate → schema gate → handler
 * (with deadline) → outcome. Never throws, never lets a user throw escape. The caller maps the
 * outcome onto broker operations (ack / retry publish / dead publish / drop) —
 * or, in the mock, returns it directly.
 */
export async function runConsumePipeline<T>(args: PipelineArgs<T>): Promise<PipelineOutcome> {
  const { context, delays } = args
  // ONE gate, before the schema gate and the handler: a delivery past its deadline
  // is dropped whatever its body says. It decides whether a delivery STARTS — a
  // handler that started in time and fails late follows the table below (a retry
  // then meets this gate on its next delivery; a dead letter keeps its reason).
  if (context.expiresAt !== undefined && Date.now() >= context.expiresAt.getTime()) {
    return { kind: 'expired', expiresAt: context.expiresAt }
  }
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
    ...(context.expiresAt !== undefined ? { expiresAt: context.expiresAt } : {}),
    ...(context.appId !== undefined ? { appId: context.appId } : {}),
    signal: args.controller.signal,
  }
  const result = await invokeHandler(message, args)
  if (result.ok) return { kind: 'ack' }
  const { error } = result
  const reason = truncateReason(reasonOf(error))
  if (delays === null) return { kind: 'drop', error, reason }
  if (error instanceof UnrecoverableError) {
    return {
      kind: 'dead',
      attempt: context.attempt,
      reason: truncateReason(`unrecoverable: ${reasonOf(error)}`),
      error,
    }
  }
  const delayMs = nextDelayMs(delays, context.attempt)
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

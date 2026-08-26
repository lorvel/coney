/**
 * The per-queue operations behind `QueueHandle`: passive inspection and the
 * dead-letter tools (spec §5). Replay is at-least-once by construction — each
 * message is re-published (confirmed) before its dead copy is acked, so an
 * interruption can duplicate, never lose.
 *
 * Every one of them waits for the connection. Without `timeoutMs` that wait lasts
 * as long as the reconnect policy allows — the documented default `open()` has —
 * and WITH one it is a single budget for the whole call, exactly like the publish
 * pipeline: the wait, the channel and the broker round-trips share it (spec §4).
 */

import type { Channel } from 'amqplib'
import {
  ConeyConfigError,
  ConeyError,
  ConeyUnavailableError,
  topologyError,
} from '../core/errors.js'
import type { LogEmitter } from '../core/log.js'
import type { QueueDef } from '../defs/define.js'
import { validateDurationMs, withinDeadline } from '../core/time.js'
import type { ConnectionManager } from '../connection/manager.js'
import type { TopologyManager } from '../topology/manager.js'
import { confirmPublish, republishOptions } from '../consume/delivery.js'
import type { OpsOptions, QueueStats } from './types.js'

/** What the ops need from the client. */
export interface QueueOpsContext {
  readonly manager: ConnectionManager
  readonly topology: TopologyManager
  readonly emit: LogEmitter
  /** Throws {@link ConeyClosedError} once the client is closing or closed. */
  ensureNotClosed(): void
}

/** Validated budget of an ops call as an absolute deadline (`undefined` = none). */
function opsDeadline(options: OpsOptions | undefined): number | undefined {
  if (options?.timeoutMs === undefined) return undefined
  validateDurationMs(options.timeoutMs, 'timeoutMs')
  return Date.now() + options.timeoutMs
}

function opsTimedOut(what: string, options: OpsOptions | undefined): ConeyUnavailableError {
  return new ConeyUnavailableError(
    `${what} did not complete within ${String(options?.timeoutMs)} ms`,
  )
}

/**
 * A channel within the deadline. An abandoned create still resolves later — close
 * that late channel, or a timed-out call leaks one on the broker. (The ops that
 * create their channel inside a helper clean up in its `finally` instead.)
 */
async function channelWithin<T extends { close(): Promise<void> }>(
  creating: Promise<T>,
  deadline: number | undefined,
  makeError: () => Error,
): Promise<T> {
  try {
    return await withinDeadline(creating, deadline, makeError)
  } catch (err) {
    void creating.then(
      (late) => late.close().catch(() => {}),
      () => {},
    )
    throw err
  }
}

export async function inspectQueue(
  ctx: QueueOpsContext,
  wire: string,
  options?: OpsOptions,
): Promise<QueueStats> {
  ctx.ensureNotClosed()
  const deadline = opsDeadline(options)
  await ctx.manager.waitForOpen(options?.timeoutMs)
  const work = inspectNow(ctx, wire)
  return withinDeadline(work, deadline, () => opsTimedOut(`inspect("${wire}")`, options))
}

async function inspectNow(ctx: QueueOpsContext, wire: string): Promise<QueueStats> {
  const channel = await ctx.manager.createChannel()
  try {
    const main = await checkQueue(channel, wire)
    const dead = await checkQueue(channel, `${wire}.dead`)
    return Object.freeze({
      ready: main.messageCount,
      consumers: main.consumerCount,
      dead: dead.messageCount,
    })
  } finally {
    await channel.close().catch(() => {})
  }
}

export async function replayDeadQueue(
  ctx: QueueOpsContext,
  def: QueueDef<unknown>,
  wire: string,
  options?: OpsOptions & { readonly limit?: number },
): Promise<{ readonly replayed: number }> {
  ctx.ensureNotClosed()
  if (options?.limit !== undefined && (!Number.isInteger(options.limit) || options.limit < 0)) {
    throw new ConeyConfigError('limit must be an integer >= 0')
  }
  const deadline = opsDeadline(options)
  await ctx.manager.waitForOpen(options?.timeoutMs)
  // The main queue must exist — a publish to a missing queue is dropped silently,
  // which would LOSE dead messages instead of replaying them.
  const ensure = ctx.topology.ensure(def)
  await withinDeadline(ensure, deadline, () => opsTimedOut(`replayDead("${def.name}")`, options))
  const deadWire = `${wire}.dead`
  const creating = ctx.manager.createConfirmChannel()
  const channel = await channelWithin(creating, deadline, () =>
    opsTimedOut(`replayDead("${def.name}")`, options),
  )
  let replayed = 0
  try {
    // Snapshot: replay what is present at call time, not what keeps arriving.
    const snapshot = (await checkQueue(channel, deadWire)).messageCount
    const target = Math.min(options?.limit ?? snapshot, snapshot)
    while (replayed < target) {
      // The budget is checked BETWEEN messages rather than raced around the loop:
      // a caller that gave up must actually stop the replay, not leave it moving
      // messages in the background. What was already moved stays moved.
      if (deadline !== undefined && Date.now() >= deadline) {
        throw new ConeyUnavailableError(
          `replayDead("${def.name}") stopped after ${replayed} messages: the ${String(options?.timeoutMs)} ms budget was spent`,
        )
      }
      const raw = await channel.get(deadWire, { noAck: false })
      if (raw === false) break
      const headers: Record<string, unknown> = { ...(raw.properties.headers ?? {}) }
      for (const key of Object.keys(headers)) {
        if (
          key === 'x-retries' ||
          key === 'x-retry-at' ||
          key === 'x-reason' ||
          key === 'x-failed-at' ||
          key.startsWith('x-death') ||
          key.startsWith('x-first-death') ||
          key.startsWith('x-last-death')
        ) {
          delete headers[key]
        }
      }
      const previous = headers['x-replayed']
      headers['x-replayed'] =
        (typeof previous === 'number' && Number.isInteger(previous) && previous >= 0
          ? previous
          : 0) + 1
      await confirmPublish(channel, {
        exchange: '',
        routingKey: wire,
        content: raw.content,
        options: republishOptions(raw.properties, headers),
      })
      channel.ack(raw)
      replayed += 1
    }
    return Object.freeze({ replayed })
  } catch (err) {
    // Interrupted mid-loop: unacked messages return to the dead queue on channel
    // close — the completed part stays replayed (at-least-once, never a loss).
    if (err instanceof ConeyError) throw err
    throw new ConeyUnavailableError(
      `replayDead("${def.name}") was interrupted after ${replayed} messages`,
      { cause: err },
    )
  } finally {
    ctx.emit({ event: 'coney.dead_replayed', level: 'info', queue: wire, replayed })
    await channel.close().catch(() => {})
  }
}

export async function purgeDeadQueue(
  ctx: QueueOpsContext,
  wire: string,
  options?: OpsOptions,
): Promise<{ readonly purged: number }> {
  ctx.ensureNotClosed()
  const deadline = opsDeadline(options)
  await ctx.manager.waitForOpen(options?.timeoutMs)
  const work = purgeNow(ctx, wire)
  return withinDeadline(work, deadline, () => opsTimedOut(`purgeDead("${wire}")`, options))
}

async function purgeNow(ctx: QueueOpsContext, wire: string): Promise<{ readonly purged: number }> {
  const deadWire = `${wire}.dead`
  const channel = await ctx.manager.createChannel()
  try {
    const reply = await channel.purgeQueue(deadWire)
    return Object.freeze({ purged: reply.messageCount })
  } catch (err) {
    if (err instanceof ConeyError) throw err
    throw topologyError('could not purge', deadWire, err)
  } finally {
    await channel.close().catch(() => {})
  }
}

/** `checkQueue` with the 404 wrapped as {@link ConeyTopologyError} (target + AMQP code). */
async function checkQueue(
  channel: Channel,
  name: string,
): Promise<{ messageCount: number; consumerCount: number }> {
  try {
    return await channel.checkQueue(name)
  } catch (err) {
    throw topologyError('could not inspect', name, err)
  }
}

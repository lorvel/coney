/**
 * The per-queue operations behind `QueueHandle`: passive inspection and the
 * dead-letter tools (spec §5). Replay is at-least-once by construction — each
 * message is re-published (confirmed) before its dead copy is acked, so an
 * interruption can duplicate, never lose.
 */

import type { Channel } from 'amqplib'
import { ConeyConfigError, ConeyError, ConeyUnavailableError, topologyError } from '../core/errors.js'
import type { LogEmitter } from '../core/log.js'
import type { QueueDef } from '../defs/define.js'
import type { ConnectionManager } from '../connection/manager.js'
import type { TopologyManager } from '../topology/manager.js'
import { confirmPublish, republishOptions } from '../consume/delivery.js'
import type { QueueStats } from './types.js'

/** What the ops need from the client. */
export interface QueueOpsContext {
  readonly manager: ConnectionManager
  readonly topology: TopologyManager
  readonly emit: LogEmitter
  /** Throws {@link ConeyClosedError} once the client is closing or closed. */
  ensureNotClosed(): void
}

export async function inspectQueue(ctx: QueueOpsContext, wire: string): Promise<QueueStats> {
  ctx.ensureNotClosed()
  await ctx.manager.waitForOpen()
  const channel = await ctx.manager.createChannel()
  try {
    const main = await checkQueue(channel, wire)
    const dead = await checkQueue(channel, `${wire}.dead`)
    return Object.freeze({ ready: main.messageCount, consumers: main.consumerCount, dead: dead.messageCount })
  } finally {
    await channel.close().catch(() => {})
  }
}

export async function replayDeadQueue(
  ctx: QueueOpsContext,
  def: QueueDef<unknown>,
  wire: string,
  options?: { readonly limit?: number },
): Promise<{ readonly replayed: number }> {
  ctx.ensureNotClosed()
  if (options?.limit !== undefined && (!Number.isInteger(options.limit) || options.limit < 0)) {
    throw new ConeyConfigError('limit must be an integer >= 0')
  }
  await ctx.manager.waitForOpen()
  // The main queue must exist — a publish to a missing queue is dropped silently,
  // which would LOSE dead messages instead of replaying them.
  await ctx.topology.ensure(def)
  const deadWire = `${wire}.dead`
  const channel = await ctx.manager.createConfirmChannel()
  let replayed = 0
  try {
    // Snapshot: replay what is present at call time, not what keeps arriving.
    const snapshot = (await checkQueue(channel, deadWire)).messageCount
    const target = Math.min(options?.limit ?? snapshot, snapshot)
    while (replayed < target) {
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
      headers['x-replayed'] = (typeof previous === 'number' && Number.isInteger(previous) && previous >= 0 ? previous : 0) + 1
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

export async function purgeDeadQueue(ctx: QueueOpsContext, wire: string): Promise<{ readonly purged: number }> {
  ctx.ensureNotClosed()
  await ctx.manager.waitForOpen()
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
async function checkQueue(channel: Channel, name: string): Promise<{ messageCount: number; consumerCount: number }> {
  try {
    return await channel.checkQueue(name)
  } catch (err) {
    throw topologyError('could not inspect', name, err)
  }
}

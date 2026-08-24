/**
 * Topology registry: every def used through the client, asserted lazily on first
 * use and replayed on every (re)connect from the connection's `setup` hook.
 * Spec §3 describes the objects; §4 the iron law that a replay must never reject.
 */

import type { Channel, ChannelModel } from 'amqplib'
import { ConeyConfigError, ConeyTopologyError } from './errors.js'
import type { AnyDef, QueueDef, TopicBinding } from './define.js'
import { wireName } from './define.js'
import { bucketLogicalName, durationLabel, effectiveDelays, type RetryPolicy } from './retry.js'
import { redactText, type LogEmitter } from './log.js'
import type { ConnectionManager } from './connection.js'

interface RegisteredDef {
  def: AnyDef
  wire: string
  asserted: boolean
  error: ConeyTopologyError | null
  /** In-flight lazy assertion, so N concurrent first publishes assert once. */
  pending: Promise<void> | null
}

function retryEqual(a: RetryPolicy | false | undefined, b: RetryPolicy | false | undefined): boolean {
  if (a === b) return true
  if (a === false || b === false || a === undefined || b === undefined) return false
  return a.delays.length === b.delays.length && a.delays.every((delay, i) => delay === b.delays[i])
}

function bindingsEqual(
  a: readonly TopicBinding<unknown>[] | undefined,
  b: readonly TopicBinding<unknown>[] | undefined,
): boolean {
  const left = a ?? []
  const right = b ?? []
  if (left.length !== right.length) return false
  return left.every((bindingA, i) => {
    const bindingB = right[i]
    if (bindingB === undefined) return false
    return (
      bindingA.topic.name === bindingB.topic.name &&
      bindingA.patterns.length === bindingB.patterns.length &&
      bindingA.patterns.every((pattern, j) => pattern === bindingB.patterns[j])
    )
  })
}

/** Two defs may share a name only when they are interchangeable. */
export function defsEqual(a: AnyDef, b: AnyDef): boolean {
  if (a === b) return true
  if (a.kind !== b.kind || a.name !== b.name || a.schema !== b.schema) return false
  if (a.kind === 'queue' && b.kind === 'queue') {
    return retryEqual(a.retry, b.retry) && bindingsEqual(a.bindings, b.bindings)
  }
  return true
}

export class TopologyManager {
  private readonly defs = new Map<string, RegisteredDef>()
  /** duration label → delay ms, for every bucket any registered def can use. */
  private readonly buckets = new Map<string, number>()
  /** Buckets asserted on the CURRENT connection (cleared on replay). */
  private readonly bucketsAsserted = new Set<string>()

  constructor(
    private readonly args: {
      prefix: string | undefined
      defaultRetry: RetryPolicy
      manager: ConnectionManager
      emit: LogEmitter
    },
  ) {}

  /**
   * Register a def (idempotent). Throws {@link ConeyConfigError} when a different
   * def already uses the same name. Returns the wire name.
   */
  register(def: AnyDef): string {
    const wire = wireName(this.args.prefix, def.name)
    const existing = this.defs.get(wire)
    if (existing !== undefined) {
      if (!defsEqual(existing.def, def)) {
        throw new ConeyConfigError(`two different defs share the name "${def.name}" — reuse one def per name`)
      }
      return wire
    }
    if (def.kind === 'queue') {
      for (const delayMs of effectiveDelays(def.retry, this.args.defaultRetry)) {
        this.buckets.set(durationLabel(delayMs), delayMs)
      }
      for (const binding of def.bindings ?? []) this.register(binding.topic)
    }
    this.defs.set(wire, { def, wire, asserted: false, error: null, pending: null })
    return wire
  }

  /** The delay of every known bucket, keyed by duration label. */
  knownBuckets(): ReadonlyMap<string, number> {
    return this.buckets
  }

  /**
   * Assert the topology of a def now (no-op when already asserted on this
   * connection). Requires an open connection. Throws {@link ConeyTopologyError} —
   * cached, so later callers fail fast until the next successful replay.
   */
  async ensure(def: AnyDef): Promise<void> {
    const wire = this.register(def)
    const entry = this.defs.get(wire)
    if (entry === undefined) throw new ConeyConfigError(`def "${def.name}" is not registered`) // unreachable
    if (entry.asserted) return
    if (entry.error !== null) throw entry.error
    entry.pending ??= this.assertNow(entry).finally(() => {
      entry.pending = null
    })
    await entry.pending
  }

  private async assertNow(entry: RegisteredDef): Promise<void> {
    const channel = await this.args.manager.createChannel()
    try {
      await this.assertEntry(channel, entry)
    } finally {
      await channel.close().catch(() => {}) // a failed assert already killed it
    }
  }

  /**
   * Re-assert everything registered; called from the connection's `setup` on every
   * (re)connect. NEVER rejects: per-def failures are logged (`coney.topology_failed`),
   * cached on the entry (its callers get {@link ConeyTopologyError}) and skipped —
   * one bad def must not block the connection or the other defs (spec §4).
   */
  async replayAll(model: ChannelModel): Promise<void> {
    this.bucketsAsserted.clear()
    for (const entry of this.defs.values()) {
      entry.asserted = false
      entry.error = null
    }
    if (this.defs.size === 0) return
    const queues: string[] = []
    const exchanges: string[] = []
    let channel: Channel | null = null
    for (const entry of this.defs.values()) {
      if (entry.asserted) continue // topics can be asserted through a queue's bindings
      try {
        if (channel === null) {
          channel = await model.createChannel()
          this.args.manager.registerChannel(channel)
        }
        await this.assertEntry(channel, entry, queues, exchanges)
      } catch {
        // assertEntry logged and cached the failure; the channel died with it.
        channel = null
      }
    }
    if (channel !== null) await channel.close().catch(() => {})
    if (queues.length > 0 || exchanges.length > 0) {
      this.args.emit({ event: 'coney.topology_asserted', level: 'debug', queues, exchanges })
    }
  }

  /** Depth of every known bucket (asserts them first so `checkQueue` cannot 404). */
  async inspectRetry(): Promise<Record<string, number>> {
    const stats: Record<string, number> = {}
    if (this.buckets.size === 0) return stats
    const channel = await this.args.manager.createChannel()
    try {
      for (const [label, delayMs] of this.buckets) {
        await this.assertBucket(channel, delayMs)
        const reply = await channel.checkQueue(this.bucketWire(delayMs))
        stats[label] = reply.messageCount
      }
    } finally {
      await channel.close().catch(() => {})
    }
    return stats
  }

  private bucketWire(delayMs: number): string {
    return wireName(this.args.prefix, bucketLogicalName(delayMs))
  }

  private async assertEntry(
    channel: Channel,
    entry: RegisteredDef,
    queues?: string[],
    exchanges?: string[],
  ): Promise<void> {
    const { def, wire } = entry
    try {
      if (def.kind === 'topic') {
        await channel.assertExchange(wire, 'topic', { durable: true })
        exchanges?.push(wire)
      } else {
        await this.assertQueueObjects(channel, def, wire, queues, exchanges)
      }
      entry.asserted = true
      entry.error = null
    } catch (err) {
      const amqpCode = (err as { code?: unknown }).code
      const topologyError = new ConeyTopologyError(
        `failed to assert the topology of "${wire}": ${redactText(err instanceof Error ? err.message : String(err))}`,
        {
          target: wire,
          ...(typeof amqpCode === 'number' ? { amqpCode } : {}),
          cause: err,
        },
      )
      entry.error = topologyError
      this.args.emit({
        event: 'coney.topology_failed',
        level: 'error',
        target: wire,
        reason: redactText(err instanceof Error ? err.message : String(err)),
        ...(typeof amqpCode === 'number' ? { amqpCode } : {}),
      })
      throw topologyError
    }
  }

  private async assertQueueObjects(
    channel: Channel,
    def: QueueDef<unknown>,
    wire: string,
    queues?: string[],
    exchanges?: string[],
  ): Promise<void> {
    await channel.assertQueue(wire, { durable: true })
    queues?.push(wire)
    await channel.assertQueue(`${wire}.dead`, { durable: true })
    queues?.push(`${wire}.dead`)
    for (const delayMs of effectiveDelays(def.retry, this.args.defaultRetry)) {
      if (await this.assertBucket(channel, delayMs)) {
        queues?.push(this.bucketWire(delayMs))
        exchanges?.push(this.bucketWire(delayMs))
      }
    }
    for (const binding of def.bindings ?? []) {
      const topicWire = wireName(this.args.prefix, binding.topic.name)
      const topicEntry = this.defs.get(topicWire)
      if (topicEntry !== undefined && !topicEntry.asserted) {
        await channel.assertExchange(topicWire, 'topic', { durable: true })
        topicEntry.asserted = true
        topicEntry.error = null
        exchanges?.push(topicWire)
      }
      for (const pattern of binding.patterns) {
        await channel.bindQueue(wire, topicWire, pattern)
      }
    }
  }

  /**
   * One shared bucket per distinct delay: a fanout exchange and a TTL queue of the
   * same name, dead-lettering to the default exchange with NO routing-key override —
   * the expired message keeps its original routing key (= origin queue wire name)
   * and lands back in its queue (decision #2). Returns whether it asserted now.
   */
  private async assertBucket(channel: Channel, delayMs: number): Promise<boolean> {
    const label = durationLabel(delayMs)
    if (this.bucketsAsserted.has(label)) return false
    const wire = this.bucketWire(delayMs)
    await channel.assertExchange(wire, 'fanout', { durable: true })
    await channel.assertQueue(wire, {
      durable: true,
      messageTtl: delayMs,
      deadLetterExchange: '',
    })
    await channel.bindQueue(wire, wire, '')
    this.bucketsAsserted.add(label)
    return true
  }
}

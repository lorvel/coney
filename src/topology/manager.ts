/**
 * Topology registry: every def used through the client, asserted lazily on first
 * use and replayed on every (re)connect from the connection's `setup` hook.
 * Spec §3 describes the objects; §4 the iron law that a replay must never reject.
 */

import type { Channel } from 'amqplib'
import { ConeyConfigError, ConeyTopologyError, topologyError } from '../core/errors.js'
import { reasonOf, type LogEmitter } from '../core/log.js'
import type { AnyDef, QueueDef } from '../defs/define.js'
import { defsEqual, wireName } from '../defs/define.js'
import {
  bucketLogicalName,
  durationLabel,
  effectiveDelays,
  type RetryPolicy,
} from '../defs/retry.js'
import type { ChannelFactory, ConnectionManager } from '../connection/manager.js'

interface RegisteredDef {
  def: AnyDef
  wire: string
  asserted: boolean
  error: ConeyTopologyError | null
  /** In-flight lazy assertion, so N concurrent first publishes assert once. */
  pending: Promise<void> | null
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
        throw new ConeyConfigError(
          `two different defs share the name "${def.name}" — reuse one def per name`,
        )
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
    const entry = this.entryFor(def)
    if (entry.asserted) return
    if (entry.error !== null) throw entry.error
    entry.pending ??= this.assertNow(entry).finally(() => {
      entry.pending = null
    })
    await entry.pending
  }

  /**
   * Assert a def again even if it is already marked asserted — for when its broker-side
   * objects may have vanished while the connection stayed UP.
   *
   * `ensure` cannot serve this case: it trusts `asserted`, and that flag is only cleared
   * by `replayAll` on a reconnect. A queue deleted under a live connection would
   * therefore never be redeclared, and the consumer trying to re-attach to it would 404
   * against a queue nothing intends to recreate, forever.
   *
   * A FAILED re-assert leaves the shared cache exactly as it found it. `asserted` and
   * `error` drive the LAZY path, where a caller asked for this def and wants the
   * answer; a background re-attach must never be the reason an unrelated `publish()`
   * on the same def starts failing — and the thing that failed is usually not the main
   * queue but `<q>.dead`, a bucket or a binding, so the publish would have routed fine.
   * The re-attach loop reports its own failures (`coney.consumer_reattach_failed`).
   *
   * The bucket and topic flags are deliberately NOT restored: re-asserting those is
   * idempotent, and they may genuinely be gone. Only the fail-fast state is preserved.
   */
  async reassert(def: AnyDef): Promise<void> {
    const entry = this.entryFor(def)
    const wasAsserted = entry.asserted
    this.invalidate(entry)
    try {
      await this.ensure(def)
    } catch (err) {
      entry.asserted = wasAsserted
      entry.error = null
      throw err
    }
  }

  /** Register and look up in one step — after `register` the entry always exists. */
  private entryFor(def: AnyDef): RegisteredDef {
    const wire = this.register(def)
    const entry = this.defs.get(wire)
    if (entry === undefined) throw new ConeyConfigError(`def "${def.name}" is not registered`) // unreachable
    return entry
  }

  /**
   * Forget everything cached about a def's broker-side objects. Invalidation is
   * TRANSITIVE on purpose: a def's retry buckets and its bound topic exchanges sit
   * behind their own caches (`bucketsAsserted`, the topic entries' own `asserted`),
   * each cleared only by `replayAll` — and whatever removed the queue under a live
   * connection usually removed those too. Clearing the entry alone redeclares the
   * queue and silently leaves the ladder missing (the next handler failure publishes
   * into nothing) or `bindQueue` 404-ing against an exchange nobody will recreate.
   */
  private invalidate(entry: RegisteredDef): void {
    entry.asserted = false
    entry.error = null
    const { def } = entry
    if (def.kind !== 'queue') return
    for (const delayMs of effectiveDelays(def.retry, this.args.defaultRetry)) {
      this.bucketsAsserted.delete(durationLabel(delayMs))
    }
    for (const binding of def.bindings ?? []) {
      const topicEntry = this.defs.get(wireName(this.args.prefix, binding.topic.name))
      if (topicEntry === undefined) continue
      topicEntry.asserted = false
      topicEntry.error = null
    }
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
  async replayAll(channels: ChannelFactory): Promise<void> {
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
        channel ??= await channels.createChannel()
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

  /**
   * Depth of every known bucket (asserts them first so `checkQueue` cannot 404).
   * Wrapped like every other broker path: a bucket someone else declared with a
   * different `x-message-ttl` must surface as {@link ConeyTopologyError} (target +
   * `amqpCode`), never as a raw amqplib error.
   */
  async inspectRetry(): Promise<Record<string, number>> {
    const stats: Record<string, number> = {}
    if (this.buckets.size === 0) return stats
    const channel = await this.args.manager.createChannel()
    try {
      for (const [label, delayMs] of this.buckets) {
        const wire = this.bucketWire(delayMs)
        try {
          await this.assertBucket(channel, delayMs)
          const reply = await channel.checkQueue(wire)
          stats[label] = reply.messageCount
        } catch (err) {
          throw topologyError('could not inspect the retry bucket', wire, err)
        }
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
      const failure = topologyError('failed to assert the topology of', wire, err)
      entry.error = failure
      // Not while closing: an assert can be in flight in the background (the consumer
      // re-attach loop) when close() lands, and the close itself is what rejects its
      // RPC. Paging on an orderly shutdown is worse than losing that line — the caller
      // still gets `failure` thrown, and a lazy caller reports it for itself.
      if (!this.args.manager.closing) {
        this.args.emit({
          event: 'coney.topology_failed',
          level: 'error',
          target: wire,
          reason: reasonOf(err),
          ...(failure.amqpCode !== undefined ? { amqpCode: failure.amqpCode } : {}),
        })
      }
      throw failure
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

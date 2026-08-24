/**
 * The in-memory test double (decision #15): `publish` validates + records,
 * `consume`/`subscribe` register, `deliver`/`deliverTopic` run the REAL consumer
 * pipeline (imported from the runtime), so the schema gate, the ladder
 * arithmetic and `timeoutMs` cannot drift from production behavior. Argument
 * validation and the close lifecycle are shared/mirrored for the same reason —
 * a shutdown bug must not pass against the mock and throw in production.
 */

import { randomUUID } from 'node:crypto'
import { ConeyClosedError, ConeyConfigError } from '../core/errors.js'
import type { AnyDef, QueueDef, Schema, TopicDef } from '../defs/define.js'
import {
  assertAnyDef,
  assertQueueDef,
  assertTopicDef,
  defsEqual,
  parseBody,
  validateInstanceName,
  validateName,
  wireName,
} from '../defs/define.js'
import { DEFAULT_RETRY, effectiveDelays } from '../defs/retry.js'
import { encodeMessage, stringifyBody, validateRoutingKey, type PublishOptions } from '../core/codec.js'
import { runConsumePipeline, type DeliveryContext, type PipelineOutcome } from '../consume/pipeline.js'
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
import type {
  CloseOptions,
  ConeyState,
  OpenOptions,
  QueueHandle,
  QueueStats,
  RetryBucketStats,
  TopicHandle,
} from '../client/types.js'
import { matchTopicPattern } from './pattern.js'
import type { ConeyMock, DeliverOptions, DeliveryOutcome, PublishedRecord } from './types.js'

interface QueueRegistration {
  kind: 'queue'
  def: QueueDef<unknown>
  wire: string
  handler: Handler<unknown>
  options: ConsumeOptions | undefined
  active: boolean
}

interface TopicRegistration {
  kind: 'topic'
  def: TopicDef<unknown>
  wire: string
  patterns: readonly string[]
  handler: Handler<unknown>
  options: SubscribeOptions | undefined
  active: boolean
}

type Registration = QueueRegistration | TopicRegistration

const EMPTY_CLOSE: CloseResult = Object.freeze({ drained: true, inFlight: 0 })
const EMPTY_STATS: QueueStats = Object.freeze({ ready: 0, consumers: 0, dead: 0 })

class ConeyMockClient implements ConeyMock {
  readonly prefix: string | undefined
  private readonly name: string | undefined
  private readonly records: PublishedRecord[] = []
  private readonly registrations: Registration[] = []
  private readonly roundRobin = new Map<string, number>()
  private readonly defs = new Map<string, AnyDef>()
  private readonly queueHandles = new Map<string, QueueHandle<never>>()
  private readonly topicHandles = new Map<string, TopicHandle<never>>()
  private closed = false

  constructor(options: { readonly prefix?: string; readonly name?: string } | undefined) {
    if (options?.prefix !== undefined) validateName(options.prefix, 'prefix')
    if (options?.name !== undefined) validateInstanceName(options.name)
    this.prefix = options?.prefix
    this.name = options?.name
  }

  get state(): ConeyState {
    return this.closed ? 'closed' : 'open'
  }

  get published(): readonly PublishedRecord[] {
    return this.records
  }

  resolveName(name: string): string {
    validateName(name, 'name')
    return wireName(this.prefix, name)
  }

  async open(_options?: OpenOptions): Promise<void> {
    this.ensureNotClosed()
  }

  /**
   * Mirrors the real close contract the mock inherits from {@link Coney}: the
   * state flips to `'closed'`, every registration is deactivated
   * (`Subscription.active` = false) and the gated operations reject with
   * {@link ConeyClosedError} — so shutdown bugs surface in tests too. Idempotent.
   */
  async close(_options?: CloseOptions): Promise<CloseResult> {
    this.closed = true
    for (const registration of this.registrations) registration.active = false
    this.registrations.length = 0
    return EMPTY_CLOSE
  }

  queue<T>(def: QueueDef<T>): QueueHandle<T> {
    assertQueueDef(def)
    const wire = this.registerDef(def)
    const cached = this.queueHandles.get(wire)
    if (cached !== undefined) return cached as QueueHandle<T>
    const handle = this.buildQueueHandle(def, wire)
    this.queueHandles.set(wire, handle as QueueHandle<never>)
    return handle
  }

  topic<T>(def: TopicDef<T>): TopicHandle<T> {
    assertTopicDef(def)
    const wire = this.registerDef(def)
    const cached = this.topicHandles.get(wire)
    if (cached !== undefined) return cached as TopicHandle<T>
    const handle = this.buildTopicHandle(def, wire)
    this.topicHandles.set(wire, handle as TopicHandle<never>)
    return handle
  }

  async declare(defs: readonly AnyDef[]): Promise<void> {
    this.ensureNotClosed()
    for (const def of defs) this.registerDef(def)
  }

  async inspectRetry(): Promise<RetryBucketStats> {
    this.ensureNotClosed()
    return Object.freeze({})
  }

  async deliver<T>(def: QueueDef<T>, body: T, options?: DeliverOptions): Promise<DeliveryOutcome> {
    const wire = wireName(this.prefix, def.name)
    const candidates = this.registrations.filter(
      (registration): registration is QueueRegistration => registration.kind === 'queue' && registration.wire === wire && registration.active,
    )
    if (candidates.length === 0) {
      throw new ConeyConfigError(`no consumer is registered for the queue "${def.name}"`)
    }
    // Competing consumers on one queue: the broker round-robins, so does the mock.
    const index = this.roundRobin.get(wire) ?? 0
    this.roundRobin.set(wire, index + 1)
    const registration = candidates[index % candidates.length] as QueueRegistration
    return this.runMockPipeline({
      schema: registration.def.schema,
      handler: registration.handler,
      timeoutMs: registration.options?.timeoutMs,
      delays: effectiveDelays(registration.def.retry, DEFAULT_RETRY),
      routingKey: wire,
      body,
      options,
    })
  }

  async deliverTopic<T>(def: TopicDef<T>, routingKey: string, body: T, options?: DeliverOptions): Promise<readonly DeliveryOutcome[]> {
    validateRoutingKey(routingKey)
    const topicWire = wireName(this.prefix, def.name)
    const outcomes: DeliveryOutcome[] = []
    // Snapshot: a handler cancelling a subscription mid-delivery splices the live
    // array, which would shift the next registration into the consumed slot and
    // skip it — the broker fans out BEFORE any cancel can land. The `active`
    // check below still honors cancels of registrations not yet reached.
    for (const registration of [...this.registrations]) {
      if (!registration.active) continue
      if (registration.kind === 'topic') {
        if (registration.wire !== topicWire) continue
        if (!registration.patterns.some((pattern) => matchTopicPattern(pattern, routingKey))) continue
        // Subscriptions carry the topic's payload type: gate with the topic's schema.
        const outcome = await this.runMockPipeline({
          schema: registration.def.schema,
          handler: registration.handler,
          timeoutMs: registration.options?.timeoutMs,
          delays: null,
          routingKey,
          body,
          options,
        })
        outcomes.push(outcome)
        continue
      }
      const bindings = registration.def.bindings ?? []
      const bound = bindings.some(
        (binding) =>
          wireName(this.prefix, binding.topic.name) === topicWire &&
          binding.patterns.some((pattern) => matchTopicPattern(pattern, routingKey)),
      )
      if (!bound) continue
      // Bound queues validate on consume with the QUEUE's schema (spec §4).
      const outcome = await this.runMockPipeline({
        schema: registration.def.schema,
        handler: registration.handler,
        timeoutMs: registration.options?.timeoutMs,
        delays: effectiveDelays(registration.def.retry, DEFAULT_RETRY),
        routingKey,
        body,
        options,
      })
      outcomes.push(outcome)
    }
    return outcomes
  }

  reset(): void {
    this.records.length = 0
  }

  private ensureNotClosed(): void {
    if (this.closed) throw new ConeyClosedError('the client is closing or closed')
  }

  private registerDef(def: AnyDef): string {
    assertAnyDef(def)
    const wire = wireName(this.prefix, def.name)
    const existing = this.defs.get(wire)
    if (existing !== undefined) {
      if (!defsEqual(existing, def)) {
        throw new ConeyConfigError(`two different defs share the name "${def.name}" — reuse one def per name`)
      }
      return wire
    }
    if (def.kind === 'queue') {
      for (const binding of def.bindings ?? []) this.registerDef(binding.topic)
    }
    this.defs.set(wire, def)
    return wire
  }

  private buildQueueHandle<T>(def: QueueDef<T>, wire: string): QueueHandle<T> {
    return {
      def,
      name: wire,
      publish: async (body, options) => {
        this.ensureNotClosed()
        this.recordPublish({ kind: 'queue', def, wire, routingKey: wire, typeProp: def.name, body, options })
      },
      consume: async (handler, options) => {
        this.ensureNotClosed()
        validateHandler(handler, 'consume')
        validateConsumeOptions(options)
        return this.register({
          kind: 'queue',
          def: def as QueueDef<unknown>,
          wire,
          handler: handler as Handler<unknown>,
          options,
          active: true,
        })
      },
      inspect: async () => {
        this.ensureNotClosed()
        return EMPTY_STATS
      },
      replayDead: async () => {
        this.ensureNotClosed()
        return Object.freeze({ replayed: 0 })
      },
      purgeDead: async () => {
        this.ensureNotClosed()
        return Object.freeze({ purged: 0 })
      },
    }
  }

  private buildTopicHandle<T>(def: TopicDef<T>, wire: string): TopicHandle<T> {
    return {
      def,
      name: wire,
      publish: async (routingKey, body, options) => {
        this.ensureNotClosed()
        validateRoutingKey(routingKey)
        this.recordPublish({ kind: 'topic', def, wire, routingKey, typeProp: routingKey, body, options })
      },
      subscribe: async (patterns, handler, options) => {
        this.ensureNotClosed()
        validateHandler(handler, 'subscribe')
        validateSubscribePatterns(patterns)
        validateConsumeOptions(options)
        return this.register({
          kind: 'topic',
          def: def as TopicDef<unknown>,
          wire,
          patterns: [...patterns],
          handler: handler as Handler<unknown>,
          options,
          active: true,
        })
      },
    }
  }

  /** Validate exactly like a real publish (schema, `x-` headers, JSON body), then record. */
  private recordPublish<T>(args: {
    kind: 'queue' | 'topic'
    def: QueueDef<T> | TopicDef<T>
    wire: string
    routingKey: string
    typeProp: string
    body: T
    options: PublishOptions | undefined
  }): void {
    const body = parseBody(args.def, args.body)
    encodeMessage({ body, type: args.typeProp, appId: this.name, options: args.options, now: new Date() })
    this.records.push(
      Object.freeze({
        target: Object.freeze({ kind: args.kind, name: args.wire }),
        routingKey: args.routingKey,
        // The schema-parsed body — exactly what a real publish puts on the wire.
        body,
        options: args.options ?? {},
      }),
    )
  }

  private register(registration: Registration): Subscription {
    this.registrations.push(registration)
    const registrations = this.registrations
    return {
      get active(): boolean {
        return registration.active
      },
      cancel: async () => {
        registration.active = false
        const index = registrations.indexOf(registration)
        if (index !== -1) registrations.splice(index, 1)
        return EMPTY_CLOSE
      },
    }
  }

  private async runMockPipeline(args: {
    schema: Schema<unknown> | undefined
    handler: Handler<unknown>
    timeoutMs: number | undefined
    delays: readonly number[] | null
    routingKey: string
    body: unknown
    options: DeliverOptions | undefined
  }): Promise<DeliveryOutcome> {
    const attempt = args.options?.attempt ?? 1
    if (!Number.isInteger(attempt) || attempt < 1) {
      throw new ConeyConfigError('attempt must be an integer >= 1')
    }
    // Round-trip through JSON so the handler sees WIRE data (a Date arrives as a
    // string, undefined fields disappear) — the class of bug the mock must catch.
    const wireBody: unknown = JSON.parse(stringifyBody(args.body))
    const now = new Date()
    const context: DeliveryContext = {
      id: args.options?.messageId ?? randomUUID(),
      attempt,
      redelivered: args.options?.redelivered ?? false,
      routingKey: args.routingKey,
      headers: {
        ...args.options?.headers,
        ...(attempt > 1 ? { 'x-retries': attempt - 1 } : {}),
        'x-published-at': now.toISOString(),
      },
      publishedAt: now,
      ...(this.name !== undefined ? { appId: this.name } : {}),
    }
    const outcome = await runConsumePipeline({
      schema: args.schema,
      body: wireBody,
      context,
      handler: args.handler,
      controller: new AbortController(),
      timeoutMs: args.timeoutMs,
      delays: args.delays,
    })
    return toDeliveryOutcome(outcome)
  }
}

function toDeliveryOutcome(outcome: PipelineOutcome): DeliveryOutcome {
  switch (outcome.kind) {
    case 'ack':
      return Object.freeze({ outcome: 'ack' })
    case 'retry':
      return Object.freeze({ outcome: 'retry', attempt: outcome.attempt, delayMs: outcome.delayMs, error: outcome.error })
    case 'dead':
      return Object.freeze({
        outcome: 'dead',
        reason: outcome.reason,
        ...(outcome.error !== undefined ? { error: outcome.error } : {}),
      })
    case 'drop':
      return Object.freeze({ outcome: 'dropped', error: outcome.error })
  }
}

export function createConeyMock(options?: { readonly prefix?: string; readonly name?: string }): ConeyMock {
  return new ConeyMockClient(options)
}

/**
 * The in-memory test double (decision #15): `publish` validates + records,
 * `consume`/`subscribe` register, `deliver`/`deliverTopic` run the REAL consumer
 * pipeline (imported from the runtime), so the schema gate, the ladder
 * arithmetic and `timeoutMs` cannot drift from production behavior.
 */

import { randomUUID } from 'node:crypto'
import { ConeyConfigError, ConeySchemaError } from './errors.js'
import type { AnyDef, QueueDef, Schema, TopicDef } from './define.js'
import { validateName, wireName } from './define.js'
import { defsEqual } from './topology.js'
import { DEFAULT_RETRY, effectiveDelays } from './retry.js'
import { encodeMessage, type HeaderValue, type PublishOptions } from './codec.js'
import {
  runConsumePipeline,
  validateConsumeOptions,
  type ConsumeOptions,
  type DeliveryContext,
  type Handler,
  type PipelineOutcome,
} from './consumer.js'
import type { SubscribeOptions, Subscription } from './subscription.js'
import type {
  CloseOptions,
  CloseResult,
  Coney,
  ConeyState,
  OpenOptions,
  QueueHandle,
  QueueStats,
  RetryBucketStats,
  TopicHandle,
} from './client.js'

export interface PublishedRecord {
  readonly target: { readonly kind: 'queue' | 'topic'; readonly name: string }
  readonly routingKey: string
  readonly body: unknown
  readonly options: PublishOptions
}

export type DeliveryOutcome =
  | { readonly outcome: 'ack' }
  | { readonly outcome: 'retry'; readonly attempt: number; readonly delayMs: number; readonly error: unknown }
  | { readonly outcome: 'dead'; readonly reason: string; readonly error?: unknown }
  /** Subscriptions only: the handler threw and the message was dropped. */
  | { readonly outcome: 'dropped'; readonly error: unknown }

export interface DeliverOptions {
  /** Simulated attempt number. Default `1`. */
  readonly attempt?: number
  readonly messageId?: string
  readonly headers?: Readonly<Record<string, HeaderValue>>
  readonly redelivered?: boolean
}

/** In-memory {@link Coney}: `publish` records, `consume`/`subscribe` register, {@link ConeyMock.deliver} runs the real consumer pipeline (schema gate, ladder, `timeoutMs`). */
export interface ConeyMock extends Coney {
  /** Every publish since creation or the last {@link ConeyMock.reset}, in order. */
  readonly published: readonly PublishedRecord[]
  /**
   * Deliver one message to the consumer registered for `def`.
   * @throws {ConeyConfigError} no consumer is registered for the queue.
   */
  deliver<T>(def: QueueDef<T>, body: T, options?: DeliverOptions): Promise<DeliveryOutcome>
  /** Deliver to every subscription and bound queue whose patterns match `routingKey`, in registration order. */
  deliverTopic<T>(def: TopicDef<T>, routingKey: string, body: T, options?: DeliverOptions): Promise<readonly DeliveryOutcome[]>
  /** Clear `published`; registered consumers stay. */
  reset(): void
}

/**
 * AMQP topic-pattern match (`.`-separated words): `*` matches exactly one word,
 * `#` matches zero or more — so `a.#` also matches `a`. Internal (the mock's
 * `deliverTopic`), exported for its own tests.
 */
export function matchTopicPattern(pattern: string, routingKey: string): boolean {
  const patternWords = pattern === '' ? [] : pattern.split('.')
  const keyWords = routingKey === '' ? [] : routingKey.split('.')
  const walk = (pi: number, ki: number): boolean => {
    if (pi === patternWords.length) return ki === keyWords.length
    const word = patternWords[pi]
    if (word === '#') {
      for (let skip = ki; skip <= keyWords.length; skip++) {
        if (walk(pi + 1, skip)) return true
      }
      return false
    }
    if (ki >= keyWords.length) return false
    if (word === '*' || word === keyWords[ki]) return walk(pi + 1, ki + 1)
    return false
  }
  return walk(0, 0)
}

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

  constructor(options: { readonly prefix?: string; readonly name?: string } | undefined) {
    if (options?.prefix !== undefined) validateName(options.prefix, 'prefix')
    if (options?.name !== undefined && (typeof options.name !== 'string' || options.name.length === 0)) {
      throw new ConeyConfigError('name must be a non-empty string')
    }
    this.prefix = options?.prefix
    this.name = options?.name
  }

  get state(): ConeyState {
    return 'open'
  }

  get published(): readonly PublishedRecord[] {
    return this.records
  }

  resolveName(name: string): string {
    validateName(name, 'name')
    return wireName(this.prefix, name)
  }

  async open(_options?: OpenOptions): Promise<void> {}

  async close(_options?: CloseOptions): Promise<CloseResult> {
    return EMPTY_CLOSE
  }

  queue<T>(def: QueueDef<T>): QueueHandle<T> {
    if (def === null || typeof def !== 'object' || def.kind !== 'queue') {
      throw new ConeyConfigError('queue() needs a def created by defineQueue()')
    }
    const wire = this.registerDef(def)
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
    const wire = this.registerDef(def)
    const cached = this.topicHandles.get(wire)
    if (cached !== undefined) return cached as TopicHandle<T>
    const handle = this.buildTopicHandle(def, wire)
    this.topicHandles.set(wire, handle as TopicHandle<never>)
    return handle
  }

  async declare(defs: readonly AnyDef[]): Promise<void> {
    for (const def of defs) this.registerDef(def)
  }

  async inspectRetry(): Promise<RetryBucketStats> {
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
    if (typeof routingKey !== 'string' || routingKey.length === 0) {
      throw new ConeyConfigError('routingKey must be a non-empty string')
    }
    const topicWire = wireName(this.prefix, def.name)
    const outcomes: DeliveryOutcome[] = []
    for (const registration of this.registrations) {
      if (!registration.active) continue
      if (registration.kind === 'topic') {
        if (registration.wire !== topicWire) continue
        if (!registration.patterns.some((pattern) => matchTopicPattern(pattern, routingKey))) continue
        outcomes.push(
          await this.runMockPipeline({
            // Subscriptions carry the topic's payload type: gate with the topic's schema.
            schema: registration.def.schema,
            handler: registration.handler,
            timeoutMs: registration.options?.timeoutMs,
            delays: null,
            routingKey,
            body,
            options,
          }),
        )
        continue
      }
      const bindings = registration.def.bindings ?? []
      const bound = bindings.some(
        (binding) =>
          wireName(this.prefix, binding.topic.name) === topicWire &&
          binding.patterns.some((pattern) => matchTopicPattern(pattern, routingKey)),
      )
      if (!bound) continue
      outcomes.push(
        await this.runMockPipeline({
          // Bound queues validate on consume with the QUEUE's schema (spec §4).
          schema: registration.def.schema,
          handler: registration.handler,
          timeoutMs: registration.options?.timeoutMs,
          delays: effectiveDelays(registration.def.retry, DEFAULT_RETRY),
          routingKey,
          body,
          options,
        }),
      )
    }
    return outcomes
  }

  reset(): void {
    this.records.length = 0
  }

  private registerDef(def: AnyDef): string {
    if (def === null || typeof def !== 'object' || (def.kind !== 'queue' && def.kind !== 'topic')) {
      throw new ConeyConfigError('defs must be created by defineQueue()/defineTopic()')
    }
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
        this.recordPublish({ kind: 'queue', def, wire, routingKey: wire, typeProp: def.name, body, options })
      },
      consume: async (handler, options) => {
        if (typeof handler !== 'function') throw new ConeyConfigError('consume() needs a handler function')
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
      inspect: async () => EMPTY_STATS,
      replayDead: async () => Object.freeze({ replayed: 0 }),
      purgeDead: async () => Object.freeze({ purged: 0 }),
    }
  }

  private buildTopicHandle<T>(def: TopicDef<T>, wire: string): TopicHandle<T> {
    return {
      def,
      name: wire,
      publish: async (routingKey, body, options) => {
        if (typeof routingKey !== 'string' || routingKey.length === 0) {
          throw new ConeyConfigError('routingKey must be a non-empty string')
        }
        this.recordPublish({ kind: 'topic', def, wire, routingKey, typeProp: routingKey, body, options })
      },
      subscribe: async (patterns, handler, options) => {
        if (typeof handler !== 'function') throw new ConeyConfigError('subscribe() needs a handler function')
        if (!Array.isArray(patterns) || patterns.length === 0) {
          throw new ConeyConfigError('subscribe() needs at least one binding pattern')
        }
        for (const pattern of patterns) {
          if (typeof pattern !== 'string' || pattern.length === 0) {
            throw new ConeyConfigError('binding patterns must be non-empty strings')
          }
        }
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
    let body: unknown = args.body
    if (args.def.schema !== undefined) {
      try {
        body = args.def.schema.parse(args.body)
      } catch (err) {
        throw new ConeySchemaError(`the body published to "${args.def.name}" failed its schema`, { cause: err })
      }
    }
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
    const json = JSON.stringify(args.body)
    if (json === undefined) {
      throw new ConeyConfigError('body is not JSON-serializable (undefined, a function, or a symbol)')
    }
    const wireBody: unknown = JSON.parse(json)
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

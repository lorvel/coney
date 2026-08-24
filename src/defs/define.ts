/** Definitions: pure, frozen values describing queues and topics — no I/O, usable by any client. */

import { ConeyConfigError, ConeySchemaError } from '../core/errors.js'
import type { RetryPolicy } from './retry.js'

/** Structural schema: anything with a throwing `parse` (zod, valibot `v.parser`, arktype, a hand-written guard). */
export interface Schema<T> {
  parse(input: unknown): T
}

/** A topic → queue binding. The topic's payload type must match the queue's. */
export interface TopicBinding<T> {
  readonly topic: TopicDef<T>
  /** AMQP topic patterns (`*` = one word, `#` = zero or more words). At least one. */
  readonly patterns: readonly string[]
}

export interface QueueDefInput<T> {
  /** Logical name: non-empty, ≤ 200 bytes UTF-8, no whitespace/control chars, not starting with `amq.`/`retry.`, not ending with `.dead`. Case preserved. */
  readonly name: string
  /** Validated on publish (→ {@link ConeySchemaError}) and on consume (→ dead-lettered with reason `schema`). */
  readonly schema?: Schema<T>
  /** Retry ladder. Default: the client's `retry` option. `false` = dead-letter on first failure. */
  readonly retry?: RetryPolicy | false
  /** Make this queue a durable subscriber of topics (competing consumers, full retry/dead). Asserted with the queue. */
  readonly bindings?: readonly TopicBinding<T>[]
}

export interface QueueDef<T> extends QueueDefInput<T> {
  readonly kind: 'queue'
}

export interface TopicDefInput<T> {
  /** Logical exchange name; same rules as queue names. */
  readonly name: string
  /** Validated on publish and on delivery to subscribers/bound queues. */
  readonly schema?: Schema<T>
}

export interface TopicDef<T> extends TopicDefInput<T> {
  readonly kind: 'topic'
}

export type AnyDef = QueueDef<unknown> | TopicDef<unknown>

/** Whitespace (incl. unicode) and ASCII control characters (0x00-0x1f, 0x7f). */
const FORBIDDEN_CHARS = new RegExp('[\\s\\x00-\\x1f\\x7f]', 'u')

/**
 * Validate a logical name (queue, topic exchange or prefix). Throws {@link ConeyConfigError}
 * when it is empty, exceeds 200 bytes of UTF-8, contains whitespace/control characters,
 * starts with a reserved prefix (`amq.`, `retry.`) or ends with the reserved `.dead` suffix.
 */
export function validateName(name: unknown, what: string): string {
  if (typeof name !== 'string' || name.length === 0) {
    throw new ConeyConfigError(`${what} must be a non-empty string`)
  }
  if (Buffer.byteLength(name, 'utf8') > 200) {
    throw new ConeyConfigError(`${what} "${name.slice(0, 40)}..." exceeds 200 bytes (UTF-8)`)
  }
  if (FORBIDDEN_CHARS.test(name)) {
    throw new ConeyConfigError(`${what} "${name}" must not contain whitespace or control characters`)
  }
  if (name.startsWith('amq.') || name.startsWith('retry.')) {
    throw new ConeyConfigError(`${what} "${name}" must not start with the reserved prefix "amq." or "retry."`)
  }
  if (name.endsWith('.dead')) {
    throw new ConeyConfigError(`${what} "${name}" must not end with the reserved suffix ".dead"`)
  }
  return name
}

/** Wire name of a logical name under a prefix (`<prefix>.<name>`, or `<name>` without one). */
export function wireName(prefix: string | undefined, name: string): string {
  return prefix === undefined ? name : `${prefix}.${name}`
}

/** Validate a {@link RetryPolicy}: `delays` must be an array of positive integers (empty allowed). */
export function validateRetryPolicy(retry: RetryPolicy, what: string): void {
  if (!Array.isArray(retry.delays)) {
    throw new ConeyConfigError(`${what}.delays must be an array of positive integers`)
  }
  for (const delay of retry.delays) {
    if (typeof delay !== 'number' || !Number.isInteger(delay) || delay <= 0) {
      throw new ConeyConfigError(`${what}.delays must contain positive integers, got ${String(delay)}`)
    }
  }
}

function frozenRetry(retry: RetryPolicy | false | undefined): RetryPolicy | false | undefined {
  if (retry === undefined || retry === false) return retry
  return Object.freeze({ delays: Object.freeze([...retry.delays]) as readonly number[] })
}

/**
 * Describe a work queue: a frozen value usable by any client, no I/O. `T` is inferred from `schema` (else `unknown`).
 * @throws {ConeyConfigError} invalid name, non-positive/non-integer delay, empty pattern list.
 */
export function defineQueue<T = unknown>(def: QueueDefInput<T>): QueueDef<T> {
  validateName(def.name, 'queue name')
  if (def.retry !== undefined && def.retry !== false) validateRetryPolicy(def.retry, 'retry')
  let bindings: readonly TopicBinding<T>[] | undefined
  if (def.bindings !== undefined) {
    bindings = Object.freeze(
      def.bindings.map((binding) => {
        if (binding.topic?.kind !== 'topic') {
          throw new ConeyConfigError(`binding of queue "${def.name}" must reference a def created by defineTopic()`)
        }
        if (!Array.isArray(binding.patterns) || binding.patterns.length === 0) {
          throw new ConeyConfigError(`binding of queue "${def.name}" to topic "${binding.topic.name}" needs at least one pattern`)
        }
        for (const pattern of binding.patterns) {
          if (typeof pattern !== 'string' || pattern.length === 0) {
            throw new ConeyConfigError(`binding of queue "${def.name}" to topic "${binding.topic.name}" has an empty pattern`)
          }
        }
        return Object.freeze({ topic: binding.topic, patterns: Object.freeze([...binding.patterns]) as readonly string[] })
      }),
    )
  }
  const retry = frozenRetry(def.retry)
  return Object.freeze({
    kind: 'queue' as const,
    name: def.name,
    ...(def.schema !== undefined ? { schema: def.schema } : {}),
    ...(retry !== undefined ? { retry } : {}),
    ...(bindings !== undefined ? { bindings } : {}),
  })
}

/** Describe a topic exchange. Same guarantees as {@link defineQueue}. */
export function defineTopic<T = unknown>(def: TopicDefInput<T>): TopicDef<T> {
  validateName(def.name, 'topic name')
  return Object.freeze({
    kind: 'topic' as const,
    name: def.name,
    ...(def.schema !== undefined ? { schema: def.schema } : {}),
  })
}

// ── Def interchangeability ──

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

/**
 * Two defs may share a name only when they are interchangeable. Lives beside the
 * def shapes on purpose: a new def field (e.g. a future queue `type`) must be
 * reflected here in the same file, or two different defs would silently pass.
 */
export function defsEqual(a: AnyDef, b: AnyDef): boolean {
  if (a === b) return true
  if (a.kind !== b.kind || a.name !== b.name || a.schema !== b.schema) return false
  if (a.kind === 'queue' && b.kind === 'queue') {
    return retryEqual(a.retry, b.retry) && bindingsEqual(a.bindings, b.bindings)
  }
  return true
}

// ── Shared argument validation — the real client and the mock run the SAME checks, so the test double cannot drift ──

export function assertQueueDef(def: unknown): void {
  if (def === null || typeof def !== 'object' || (def as AnyDef).kind !== 'queue') {
    throw new ConeyConfigError('queue() needs a def created by defineQueue()')
  }
}

export function assertTopicDef(def: unknown): void {
  if (def === null || typeof def !== 'object' || (def as AnyDef).kind !== 'topic') {
    throw new ConeyConfigError('topic() needs a def created by defineTopic()')
  }
}

export function assertAnyDef(def: unknown): void {
  const kind = def === null || typeof def !== 'object' ? undefined : (def as AnyDef).kind
  if (kind !== 'queue' && kind !== 'topic') {
    throw new ConeyConfigError('defs must be created by defineQueue()/defineTopic()')
  }
}

/** The publish-side schema gate: parsed body out, {@link ConeySchemaError} (cause = the schema's own error) on failure. */
export function parseBody<T>(def: QueueDef<T> | TopicDef<T>, body: T): unknown {
  if (def.schema === undefined) return body
  try {
    return def.schema.parse(body)
  } catch (err) {
    throw new ConeySchemaError(`the body published to "${def.name}" failed its schema`, { cause: err })
  }
}

/** The `name` option of a client/mock (AMQP `appId` + first consumer-tag segment). */
export function validateInstanceName(name: unknown): void {
  if (typeof name !== 'string' || name.length === 0) {
    throw new ConeyConfigError('name must be a non-empty string')
  }
}

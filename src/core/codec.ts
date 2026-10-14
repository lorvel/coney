/** JSON body codec and the mapping to AMQP properties/headers (spec §3 "Message on the wire"). */

import { randomUUID } from 'node:crypto'
import { ConeyConfigError } from './errors.js'
import { validateDurationMs } from './time.js'

/** AMQP header values the library accepts (a flat subset of the AMQP field table). */
export type HeaderValue = string | number | boolean | null

export interface PublishOptions {
  /** AMQP `messageId`. Default: a random UUID v4. Use a deterministic id to let consumers deduplicate. */
  readonly messageId?: string
  /** Application headers. Keys starting with `x-` are reserved for the library and rejected. */
  readonly headers?: Readonly<Record<string, HeaderValue>>
  /** `deliveryMode: 2`. Default `true`; `false` for transient fan-out where loss on broker restart is acceptable. */
  readonly persistent?: boolean
  /** Overrides {@link ConeyOptions.publishTimeoutMs} for this call (finite, <= 2^31-1). */
  readonly timeoutMs?: number
  /** Time to live in ms (positive, finite, <= 2^31-1): an absolute deadline stamped as the `x-expires-at` header at publish; retries never extend it. Checked once per delivery, before the handler, against the CONSUMER's clock: a delivery past it is acked without calling the handler and logged as `coney.message_expired` (no retry, no dead letter). A handler that starts in time and fails late follows the normal retry/dead outcome. Default: no deadline. */
  readonly ttlMs?: number
}

/**
 * The `x-expires-at` deadline of a delivery, or `undefined` for none. Only a value
 * that `Date#toISOString()` reproduces character for character is a deadline.
 * Strict on purpose: `new Date()` alone reads `'0'` or `'Oct 14'` as dates in
 * 2000/2001, zone-less strings in local time, and rolls `2026-02-31` over into
 * March — each would drop a foreign producer's message on a value it never meant
 * as a deadline.
 */
export function deadlineOf(headers: Readonly<Record<string, unknown>>): Date | undefined {
  const raw = headers['x-expires-at']
  if (typeof raw !== 'string') return undefined
  const parsed = new Date(raw)
  if (Number.isNaN(parsed.getTime())) return undefined
  return parsed.toISOString() === raw ? parsed : undefined
}

/** The amqplib publish options the codec produces (a plain object — no amqplib types). */
export interface WireProperties {
  messageId: string
  /** Unix seconds. */
  timestamp: number
  /** Queue publish: the logical queue name. Topic publish: the routing key. */
  type: string
  appId?: string
  deliveryMode: 1 | 2
  contentType: 'application/json'
  headers: Record<string, unknown>
}

export interface EncodedMessage {
  content: Buffer
  properties: WireProperties
}

/**
 * Encode a body and publish options into wire content + AMQP properties.
 * Throws {@link ConeyConfigError} for non-JSON-serializable bodies, for user
 * headers with reserved `x-` keys and for an invalid `ttlMs`. No I/O.
 */
export function encodeMessage(args: {
  body: unknown
  /** Value of the AMQP `type` property (logical queue name or topic routing key). */
  type: string
  appId: string | undefined
  options: PublishOptions | undefined
  now: Date
}): EncodedMessage {
  const { body, type, appId, options, now } = args
  if (options?.ttlMs !== undefined) validateDurationMs(options.ttlMs, 'ttlMs', { positive: true })
  const json = stringifyBody(body)
  const headers: Record<string, unknown> = {}
  if (options?.headers !== undefined) {
    for (const [key, value] of Object.entries(options.headers)) {
      if (key.startsWith('x-')) {
        throw new ConeyConfigError(`header "${key}" uses the reserved "x-" prefix`)
      }
      headers[key] = value
    }
  }
  headers['x-published-at'] = now.toISOString()
  // Same clock reading as x-published-at: the deadline is exactly publish time + ttlMs.
  if (options?.ttlMs !== undefined) {
    headers['x-expires-at'] = new Date(now.getTime() + options.ttlMs).toISOString()
  }
  return {
    content: Buffer.from(json, 'utf8'),
    properties: {
      messageId: options?.messageId ?? randomUUID(),
      timestamp: Math.floor(now.getTime() / 1000),
      type,
      ...(appId !== undefined ? { appId } : {}),
      deliveryMode: options?.persistent === false ? 1 : 2,
      contentType: 'application/json',
      headers,
    },
  }
}

/**
 * JSON-encode a body, mapping EVERY non-serializable shape to {@link ConeyConfigError}:
 * `undefined`/function/symbol (stringify returns undefined) as well as circular
 * structures, `BigInt` values and throwing `toJSON()` (stringify throws) — a raw
 * `TypeError` must never escape the library's error taxonomy.
 */
export function stringifyBody(body: unknown): string {
  let json: string | undefined
  try {
    json = JSON.stringify(body)
  } catch (err) {
    throw new ConeyConfigError(
      'body is not JSON-serializable (circular structure, BigInt, or a throwing toJSON)',
      {
        cause: err,
      },
    )
  }
  if (json === undefined) {
    throw new ConeyConfigError('body is not JSON-serializable (undefined, a function, or a symbol)')
  }
  return json
}

/** Decode a JSON body. Throws `SyntaxError` on invalid JSON (the consumer maps it to a `decode` dead-letter). */
export function decodeJson(content: Buffer): unknown {
  return JSON.parse(content.toString('utf8'))
}

/** Validate a topic routing key (publish and mock delivery). */
export function validateRoutingKey(routingKey: unknown): asserts routingKey is string {
  if (typeof routingKey !== 'string' || routingKey.length === 0) {
    throw new ConeyConfigError('routingKey must be a non-empty string')
  }
}

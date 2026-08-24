/** JSON body codec and the mapping to AMQP properties/headers (spec §3 "Message on the wire"). */

import { randomUUID } from 'node:crypto'
import { ConeyConfigError } from './errors.js'

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
 * Throws {@link ConeyConfigError} for non-JSON-serializable bodies and for user
 * headers with reserved `x-` keys. No I/O.
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

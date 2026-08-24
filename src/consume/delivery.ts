/**
 * Raw amqplib deliveries ↔ coney: reading the delivery context out of a
 * message (foreign producers included) and rebuilding publish options when a
 * message is republished (retry, dead, replay).
 */

import type { ConfirmChannel, ConsumeMessage } from 'amqplib'
import { toError } from '../core/errors.js'
import type { DeliveryContext } from './pipeline.js'

/** Read the delivery context out of a raw amqplib message (foreign producers included). */
export function contextOf(raw: ConsumeMessage): DeliveryContext {
  const properties = raw.properties as {
    messageId?: unknown
    appId?: unknown
    headers?: Record<string, unknown>
  }
  const headers: Readonly<Record<string, unknown>> = properties.headers ?? {}
  const retries = headers['x-retries']
  const publishedAtRaw = headers['x-published-at']
  let publishedAt: Date | undefined
  if (typeof publishedAtRaw === 'string') {
    const parsed = new Date(publishedAtRaw)
    if (!Number.isNaN(parsed.getTime())) publishedAt = parsed
  }
  return {
    // The contract requires a string id; a foreign message without one gets ''
    // (a generated id would not be stable across redeliveries).
    id: typeof properties.messageId === 'string' ? properties.messageId : '',
    attempt:
      typeof retries === 'number' && Number.isInteger(retries) && retries >= 0 ? retries + 1 : 1,
    redelivered: raw.fields.redelivered,
    routingKey: raw.fields.routingKey,
    headers,
    ...(publishedAt !== undefined ? { publishedAt } : {}),
    ...(typeof properties.appId === 'string' && properties.appId !== ''
      ? { appId: properties.appId }
      : {}),
  }
}

/**
 * Publish on a confirm channel and resolve only on the broker's confirm — the
 * shape shared by every internal republish (retry, dead, replay). NOT for the
 * user-facing publish path: the Publisher adds pending-confirm bookkeeping and
 * back-pressure on top.
 */
export function confirmPublish(
  channel: ConfirmChannel,
  args: { exchange: string; routingKey: string; content: Buffer; options: Record<string, unknown> },
): Promise<void> {
  return new Promise<void>((resolve, reject) => {
    channel.publish(args.exchange, args.routingKey, args.content, args.options, (err) => {
      if (err == null) resolve()
      else reject(toError(err))
    })
  })
}

/** amqplib publish options rebuilt from a delivery's properties: what coney understands, headers replaced. */
export function republishOptions(
  rawProperties: unknown,
  headers: Record<string, unknown>,
): Record<string, unknown> {
  const properties = (rawProperties ?? {}) as Record<string, unknown>
  const options: Record<string, unknown> = { headers }
  // Whitelisted pass-through: userId is dropped (RabbitMQ rejects a mismatch with
  // the republishing connection's user), expiration too (a per-message TTL must not
  // silently expire messages parked in retry/dead queues).
  for (const key of [
    'messageId',
    'timestamp',
    'type',
    'appId',
    'deliveryMode',
    'contentType',
    'contentEncoding',
    'correlationId',
    'replyTo',
    'priority',
  ]) {
    const value = properties[key]
    if (value !== undefined) options[key] = value
  }
  return options
}

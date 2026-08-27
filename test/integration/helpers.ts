/**
 * Shared plumbing for the broker-backed suite. Every name a test creates goes
 * through an `it.<runId>` prefix and is deleted afterwards — the local broker
 * may be shared with real development traffic, so `cleanup` refuses to touch
 * anything outside that namespace.
 */

import assert from 'node:assert/strict'
import amqplib from 'amqplib'
import type { ConeyLogEvent } from '../../src/index.js'
import { durationLabel } from '../../src/defs/retry.js'

export const BROKER_URL = process.env['RABBITMQ_URL']
export const MGMT_URL = process.env['RABBITMQ_MGMT_URL']

/** node:test `skip` option for suites needing a broker (and optionally the management API). */
export const skipWithoutBroker = BROKER_URL === undefined && 'RABBITMQ_URL not set'
export const skipWithoutMgmt =
  (BROKER_URL === undefined || MGMT_URL === undefined) && 'RABBITMQ_URL / RABBITMQ_MGMT_URL not set'

/** A unique prefix per test FILE (files run concurrently — never share state). */
export function newRunPrefix(tag: string): string {
  return `it.${tag}${process.pid.toString(36)}${Date.now().toString(36)}`
}

export async function waitFor(
  predicate: () => boolean,
  what: string,
  timeoutMs = 10_000,
): Promise<void> {
  const deadline = Date.now() + timeoutMs
  while (!predicate()) {
    if (Date.now() > deadline) throw new Error(`timed out waiting for ${what}`)
    await new Promise((resolve) => setTimeout(resolve, 50))
  }
}

export async function waitForAsync(
  predicate: () => Promise<boolean>,
  what: string,
  timeoutMs = 10_000,
): Promise<void> {
  const deadline = Date.now() + timeoutMs
  while (!(await predicate())) {
    if (Date.now() > deadline) throw new Error(`timed out waiting for ${what}`)
    await new Promise((resolve) => setTimeout(resolve, 100))
  }
}

export function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms))
}

/** Raw amqplib access for seeding/verification (no coney involved). */
export async function withRawChannel<T>(fn: (ch: amqplib.Channel) => Promise<T>): Promise<T> {
  const conn = await amqplib.connect(BROKER_URL as string)
  try {
    const ch = await conn.createChannel()
    ch.on('error', () => {})
    return await fn(ch)
  } finally {
    await conn.close()
  }
}

/** Queue/exchange names created under a prefix for a set of defs. */
export function namesFor(
  prefix: string,
  queueNames: readonly string[],
  delays: readonly number[],
  topicNames: readonly string[] = [],
) {
  const queues = queueNames.flatMap((name) => [`${prefix}.${name}`, `${prefix}.${name}.dead`])
  const buckets = [...new Set(delays)].map((delayMs) => `${prefix}.retry.${durationLabel(delayMs)}`)
  return {
    queues: [...queues, ...buckets],
    exchanges: [...buckets, ...topicNames.map((name) => `${prefix}.${name}`)],
  }
}

/** Delete everything a test created. Refuses names outside the `it.` namespace. */
export async function cleanup(names: {
  queues: readonly string[]
  exchanges: readonly string[]
}): Promise<void> {
  for (const name of [...names.queues, ...names.exchanges]) {
    assert.ok(name.startsWith('it.'), `cleanup refuses to touch "${name}" (shared broker)`)
  }
  const conn = await amqplib.connect(BROKER_URL as string)
  try {
    for (const queue of names.queues) {
      // One channel per delete: a 404 kills the channel it happens on.
      const ch = await conn.createChannel().catch(() => null)
      if (ch === null) continue
      ch.on('error', () => {})
      await ch.deleteQueue(queue).catch(() => {})
      await ch.close().catch(() => {})
    }
    for (const exchange of names.exchanges) {
      const ch = await conn.createChannel().catch(() => null)
      if (ch === null) continue
      ch.on('error', () => {})
      await ch.deleteExchange(exchange).catch(() => {})
      await ch.close().catch(() => {})
    }
  } finally {
    await conn.close()
  }
}

export function eventCollector(): {
  events: ConeyLogEvent[]
  log: (event: ConeyLogEvent) => void
  names: () => string[]
} {
  const events: ConeyLogEvent[] = []
  return { events, log: (event) => events.push(event), names: () => events.map((e) => e.event) }
}

/**
 * Publish raw payloads with confirms — a plain channel's fire-and-forget frames
 * are LOST when the connection closes right after (amqplib does not flush).
 */
export async function rawPublish(queue: string, payloads: readonly Buffer[]): Promise<void> {
  const conn = await amqplib.connect(BROKER_URL as string)
  try {
    const ch = await conn.createConfirmChannel()
    ch.on('error', () => {})
    for (const payload of payloads) {
      ch.sendToQueue(queue, payload, { contentType: 'application/json' })
    }
    await ch.waitForConfirms()
  } finally {
    await conn.close()
  }
}

/** Read one message off a queue without acking side effects (removes it). */
export async function takeMessage(queue: string): Promise<{
  content: unknown
  headers: Record<string, unknown>
  properties: Record<string, unknown>
} | null> {
  return withRawChannel(async (ch) => {
    const raw = await ch.get(queue, { noAck: true })
    if (raw === false) return null
    const text = raw.content.toString('utf8')
    let content: unknown
    try {
      content = JSON.parse(text)
    } catch {
      content = text // e.g. a decode-dead message deliberately holds invalid JSON
    }
    return {
      content,
      headers: (raw.properties.headers ?? {}) as Record<string, unknown>,
      properties: raw.properties as unknown as Record<string, unknown>,
    }
  })
}

export async function queueDepth(queue: string): Promise<number> {
  return withRawChannel(async (ch) => (await ch.checkQueue(queue)).messageCount)
}

/**
 * Delete a queue over AMQP from a SECOND connection: a broker-initiated consumer
 * cancel (`basic.cancel`) with the consumer's own connection left untouched — the
 * case no reconnect ever repairs.
 *
 * A second connection suffices for the durable queues a queue def declares; `cleanup`
 * below deletes exactly these the same way on every run. It would NOT suffice for a
 * subscription's exclusive queue, which refuses every connection but its owner (the
 * management API refuses it too, with 400) — so a broker cancel cannot be provoked on
 * one at all, and only the `consume()` path is coverable here.
 */
export async function deleteQueue(queue: string): Promise<void> {
  assert.ok(queue.startsWith('it.'), `refuses to delete "${queue}" (shared broker)`)
  await withRawChannel(async (ch) => {
    await ch.deleteQueue(queue)
  })
}

/**
 * Force-close the connection named `connectionName` via the management API.
 * The listing lags ~5 s behind reality — poll until it shows up, then DELETE.
 */
export async function forceCloseConnection(connectionName: string): Promise<void> {
  const mgmt = new URL(MGMT_URL as string)
  const auth = `Basic ${Buffer.from(`${decodeURIComponent(mgmt.username)}:${decodeURIComponent(mgmt.password)}`).toString('base64')}`
  const base = `${mgmt.protocol}//${mgmt.host}`
  let target: string | undefined
  const deadline = Date.now() + 30_000
  while (target === undefined) {
    if (Date.now() > deadline)
      throw new Error(`connection "${connectionName}" not visible in the management API`)
    const res = await fetch(`${base}/api/connections`, { headers: { authorization: auth } })
    assert.ok(res.ok, `management API reachable (${res.status})`)
    const connections = (await res.json()) as {
      name: string
      client_properties?: { connection_name?: string }
    }[]
    target = connections.find((c) => c.client_properties?.connection_name === connectionName)?.name
    if (target === undefined) await sleep(1000)
  }
  const del = await fetch(`${base}/api/connections/${encodeURIComponent(target)}`, {
    method: 'DELETE',
    headers: { authorization: auth },
  })
  assert.ok(del.ok, `force-close accepted (${del.status})`)
}

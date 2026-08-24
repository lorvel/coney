/**
 * Broker smoke tests — SKIPPED unless RABBITMQ_URL is set (CI unit runs stay green).
 *
 *   RABBITMQ_URL=amqp://user:pass@host:5672/vhost \
 *   RABBITMQ_MGMT_URL=http://user:pass@host:15672 \   # optional: enables the reconnect leg
 *   yarn test
 *
 * Everything created here is prefixed `test.` and deleted afterwards.
 */

import { test } from 'node:test'
import assert from 'node:assert/strict'
import amqplib from 'amqplib'
import { createConey, defineQueue, type ConeyLogEvent } from '../src/index.js'

const url = process.env['RABBITMQ_URL']
const mgmtUrl = process.env['RABBITMQ_MGMT_URL']
const runId = `${process.pid.toString(36)}${Date.now().toString(36)}`

const waitFor = async (
  predicate: () => boolean,
  what: string,
  timeoutMs: number,
): Promise<void> => {
  const deadline = Date.now() + timeoutMs
  while (!predicate()) {
    if (Date.now() > deadline) throw new Error(`timed out waiting for ${what}`)
    await new Promise((resolve) => setTimeout(resolve, 100))
  }
}

/** Raw amqplib helper for verification/cleanup (no coney involved). */
const withRawChannel = async <T>(fn: (ch: import('amqplib').Channel) => Promise<T>): Promise<T> => {
  const conn = await amqplib.connect(url as string)
  try {
    const ch = await conn.createChannel()
    ch.on('error', () => {})
    return await fn(ch)
  } finally {
    await conn.close()
  }
}

test(
  'smoke: open → connected; 200 unawaited publishes then close() → depth 200',
  { skip: url === undefined && 'RABBITMQ_URL not set' },
  async () => {
    const events: ConeyLogEvent[] = []
    const def = defineQueue({ name: `smoke.${runId}`, retry: false }) // no buckets: nothing shared left behind
    const mq = createConey({
      url: url as string,
      prefix: 'test',
      name: `smoke-${runId}`,
      log: (e) => events.push(e),
    })
    const wire = mq.resolveName(def.name)
    try {
      await mq.open({ timeoutMs: 15_000 })
      assert.equal(mq.state, 'open')
      assert.ok(
        events.some((e) => e.event === 'coney.connected'),
        'coney.connected was logged',
      )

      const jobs = mq.queue(def)
      const publishes: Promise<void>[] = []
      for (let i = 0; i < 200; i += 1) publishes.push(jobs.publish({ i }))
      // The point of this leg (KU-156 class of bug): close() is called while confirms
      // are still outstanding — the tail must be flushed, not dropped.
      const closeResult = await mq.close()
      assert.deepEqual(closeResult, { drained: true, inFlight: 0 })
      const settled = await Promise.allSettled(publishes)
      assert.equal(
        settled.filter((s) => s.status === 'fulfilled').length,
        200,
        'all 200 publishes confirmed',
      )

      await withRawChannel(async (ch) => {
        const reply = await ch.checkQueue(wire)
        assert.equal(reply.messageCount, 200, `queue depth of ${wire}`)
      })
    } finally {
      await mq.close()
      await withRawChannel(async (ch) => {
        await ch.deleteQueue(wire)
        await ch.deleteQueue(`${wire}.dead`)
      }).catch(() => {})
    }
  },
)

test(
  'smoke: forced disconnect → coney.reconnecting → coney.reconnected → publish works again',
  {
    skip:
      (url === undefined || mgmtUrl === undefined) && 'RABBITMQ_URL / RABBITMQ_MGMT_URL not set',
  },
  async () => {
    const events: ConeyLogEvent[] = []
    const connectionName = `coney:smoke-rc-${runId}`
    const def = defineQueue({ name: `smoke.rc.${runId}`, retry: false })
    const mq = createConey({
      url: url as string,
      prefix: 'test',
      name: `smoke-rc-${runId}`,
      reconnect: { initialDelayMs: 200, maxDelayMs: 1000 },
      log: (e) => events.push(e),
    })
    const wire = mq.resolveName(def.name)

    const mgmt = new URL(mgmtUrl as string)
    const auth = `Basic ${Buffer.from(`${decodeURIComponent(mgmt.username)}:${decodeURIComponent(mgmt.password)}`).toString('base64')}`
    const mgmtBase = `${mgmt.protocol}//${mgmt.host}`
    const listConnections = async (): Promise<
      { name: string; client_properties?: { connection_name?: string } }[]
    > => {
      const res = await fetch(`${mgmtBase}/api/connections`, { headers: { authorization: auth } })
      assert.ok(res.ok, `management API reachable (${res.status})`)
      return (await res.json()) as {
        name: string
        client_properties?: { connection_name?: string }
      }[]
    }

    try {
      await mq.open({ timeoutMs: 15_000 })
      await mq.queue(def).publish({ probe: 1 })

      // The management API's connection listing lags ~5 s behind reality — poll.
      let target: string | undefined
      const deadline = Date.now() + 30_000
      while (target === undefined) {
        if (Date.now() > deadline) throw new Error('connection not visible in the management API')
        const connections = await listConnections()
        target = connections.find(
          (c) => c.client_properties?.connection_name === connectionName,
        )?.name
        if (target === undefined) await new Promise((resolve) => setTimeout(resolve, 1000))
      }

      const del = await fetch(`${mgmtBase}/api/connections/${encodeURIComponent(target)}`, {
        method: 'DELETE',
        headers: { authorization: auth },
      })
      assert.ok(del.ok, `force-close accepted (${del.status})`)

      await waitFor(
        () => events.some((e) => e.event === 'coney.disconnected'),
        'coney.disconnected',
        15_000,
      )
      await waitFor(
        () => events.some((e) => e.event === 'coney.reconnecting'),
        'coney.reconnecting',
        15_000,
      )
      await waitFor(
        () => events.some((e) => e.event === 'coney.reconnected'),
        'coney.reconnected',
        15_000,
      )
      assert.equal(mq.state, 'open')

      await mq.queue(def).publish({ probe: 2 }) // the replayed topology + fresh channel carry a publish
      await withRawChannel(async (ch) => {
        const reply = await ch.checkQueue(wire)
        assert.equal(reply.messageCount, 2)
      })
    } finally {
      await mq.close()
      await withRawChannel(async (ch) => {
        await ch.deleteQueue(wire)
        await ch.deleteQueue(`${wire}.dead`)
      }).catch(() => {})
    }
  },
)

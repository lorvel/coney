/**
 * Failure modes: 406 isolation (one bad def cannot hurt the rest), forced
 * disconnects (consumer re-attach, redelivery with the SAME attempt, exclusive
 * subscription reset) and close() while disconnected. Force-close legs need the
 * management API (RABBITMQ_MGMT_URL) and are skipped without it.
 */

import assert from 'node:assert/strict'
import { test } from 'node:test'
import { ConeyTopologyError, createConey, defineQueue, defineTopic } from '../../src/index.js'
import {
  BROKER_URL,
  cleanup,
  eventCollector,
  forceCloseConnection,
  namesFor,
  newRunPrefix,
  skipWithoutBroker,
  skipWithoutMgmt,
  waitFor,
  waitForAsync,
  withRawChannel,
} from './helpers.js'

test(
  'a 406 def fails its callers only: other consumers keep working, no reconnect storm',
  { skip: skipWithoutBroker },
  async () => {
    const prefix = newRunPrefix('fl1')
    const collector = eventCollector()
    const mq = createConey({
      url: BROKER_URL as string,
      prefix,
      name: 'it-fl1',
      log: collector.log,
    })
    const clashWire = `${prefix}.clash`
    try {
      // Pre-existing queue with DIFFERENT arguments → coney's plain assert gets 406.
      await withRawChannel(async (ch) => {
        await ch.assertQueue(clashWire, { durable: true, messageTtl: 5000 })
      })
      const good = defineQueue<{ n: number }>({ name: 'good', retry: false })
      const clash = defineQueue({ name: 'clash', retry: false })
      let handled = 0
      await mq.queue(good).consume(() => {
        handled += 1
      })
      await assert.rejects(mq.queue(clash).publish({ any: 1 }), (err: unknown) => {
        assert.ok(err instanceof ConeyTopologyError)
        assert.equal(err.target, clashWire)
        assert.equal(err.amqpCode, 406)
        return true
      })
      await assert.rejects(
        mq.queue(clash).consume(() => {}),
        ConeyTopologyError,
      )
      await mq.queue(good).publish({ n: 1 }) // the healthy def is untouched
      await waitFor(() => handled === 1, 'good queue still consuming')
      assert.equal(mq.state, 'open')
      assert.ok(!collector.names().includes('coney.reconnecting'), 'no reconnect storm')
      assert.ok(
        collector.events.some((e) => e.event === 'coney.topology_failed' && e.target === clashWire),
      )
    } finally {
      await mq.close()
      await cleanup({ ...namesFor(prefix, ['good'], []), exchanges: [] })
      await cleanup({ queues: [clashWire, `${clashWire}.dead`], exchanges: [] })
    }
  },
)

test(
  'forced disconnect: consumer re-attaches, in-flight message redelivers with the SAME attempt',
  { skip: skipWithoutMgmt },
  async () => {
    const prefix = newRunPrefix('fl2')
    const collector = eventCollector()
    const connectionName = `coney:${prefix}`
    const mq = createConey({
      url: BROKER_URL as string,
      prefix,
      name: prefix, // unique connection_name for the management API kill
      reconnect: { initialDelayMs: 200, maxDelayMs: 1000 },
      log: collector.log,
    })
    try {
      const def = defineQueue<{ n: number }>({ name: 'work', retry: { delays: [60_000] } }) // a retry would be visible as a long stall
      const deliveries: { id: string; attempt: number; redelivered: boolean }[] = []
      // The management listing lags several seconds behind reality, so the first
      // delivery must stay in flight until the kill actually lands — a gate, not a
      // fixed sleep (a finished handler would ack and leave nothing to redeliver).
      let releaseFirst: () => void = () => {}
      const firstHold = new Promise<void>((resolve) => {
        releaseFirst = resolve
      })
      await mq.queue(def).consume(async (message) => {
        const first = deliveries.length === 0
        deliveries.push({
          id: message.id,
          attempt: message.attempt,
          redelivered: message.redelivered,
        })
        if (first) await firstHold
      })
      await mq.queue(def).publish({ n: 1 }, { messageId: 'survivor' })
      await waitFor(() => deliveries.length === 1, 'first delivery in flight')
      await forceCloseConnection(connectionName)
      await waitFor(() => collector.names().includes('coney.reconnected'), 'reconnected', 20_000)
      await waitFor(() => deliveries.length === 2, 'redelivery after the reconnect', 20_000)
      releaseFirst() // the zombie invocation settles; its ack fails on the dead channel (harmless)
      const second = deliveries[1] as { id: string; attempt: number; redelivered: boolean }
      assert.equal(second.id, 'survivor')
      assert.equal(second.redelivered, true)
      assert.equal(second.attempt, 1) // the interrupted attempt was free — never counted
      assert.ok(
        collector.events.filter((e) => e.event === 'coney.consumer_started').length >= 2,
        're-attach logged',
      )
      await waitForAsync(
        async () =>
          (await withRawChannel((ch) => ch.checkQueue(`${prefix}.work`))).messageCount === 0,
        'redelivery acked',
        15_000,
      )
    } finally {
      await mq.close()
      await cleanup(namesFor(prefix, ['work'], [60_000]))
    }
  },
)

test(
  'forced disconnect resets an exclusive subscription (new queue, stream resumes)',
  { skip: skipWithoutMgmt },
  async () => {
    const prefix = newRunPrefix('fl3')
    const collector = eventCollector()
    const connectionName = `coney:${prefix}`
    const mq = createConey({
      url: BROKER_URL as string,
      prefix,
      name: prefix,
      reconnect: { initialDelayMs: 200, maxDelayMs: 1000 },
      log: collector.log,
    })
    try {
      const events = defineTopic<{ seq: number }>({ name: 'feed' })
      const seen: number[] = []
      await mq.topic(events).subscribe(['ev.*'], (message) => {
        seen.push(message.body.seq)
      })
      await mq.topic(events).publish('ev.tick', { seq: 1 })
      await waitFor(() => seen.length === 1, 'first event received')
      await forceCloseConnection(connectionName)
      await waitFor(
        () => collector.names().includes('coney.subscription_reset'),
        'subscription reset',
        20_000,
      )
      const reset = collector.events.find((e) => e.event === 'coney.subscription_reset')
      assert.deepEqual((reset as { patterns: string[] }).patterns, ['ev.*'])
      await mq.topic(events).publish('ev.tick', { seq: 2 })
      await waitFor(() => seen.includes(2), 'stream resumed on the fresh queue', 15_000)
    } finally {
      await mq.close()
      await cleanup(namesFor(prefix, [], [], ['feed']))
    }
  },
)

test(
  'close() while disconnected resolves immediately and stops the loop',
  { skip: skipWithoutMgmt },
  async () => {
    const prefix = newRunPrefix('fl4')
    const collector = eventCollector()
    const connectionName = `coney:${prefix}`
    const mq = createConey({
      url: BROKER_URL as string,
      prefix,
      name: prefix,
      reconnect: { initialDelayMs: 8000, maxDelayMs: 8000 }, // hold the reconnecting window open
      log: collector.log,
    })
    try {
      await mq.open({ timeoutMs: 15_000 })
      await forceCloseConnection(connectionName)
      await waitFor(() => mq.state === 'reconnecting', 'client noticed the disconnect', 15_000)
      const before = Date.now()
      const result = await mq.close({ drainMs: 5000 })
      assert.ok(Date.now() - before < 3000, 'close() did not wait for the backoff')
      assert.deepEqual(result, { drained: true, inFlight: 0 })
      assert.equal(mq.state, 'closed')
    } finally {
      await mq.close()
    }
  },
)

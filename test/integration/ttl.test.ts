/**
 * Per-message TTL (`PublishOptions.ttlMs`, spec §4 and decision 19) on a real
 * broker: a message past its `x-expires-at` deadline is acked WITHOUT calling the
 * handler and logged as `coney.message_expired` — on a work queue, after a stay
 * in a retry bucket (retries never extend the deadline), on a subscription, on a
 * bound queue, and as a dead letter replayed after its deadline — and expiring
 * never puts it in `.dead`. Margins are wide (deadline vs. wait >= 2x, and seconds
 * where a slow runner could otherwise start a handler late) on purpose.
 */

import assert from 'node:assert/strict'
import { test } from 'node:test'
import {
  createConey,
  defineQueue,
  defineTopic,
  UnrecoverableError,
  type ConeyLogEvent,
} from '../../src/index.js'
import {
  BROKER_URL,
  cleanup,
  eventCollector,
  namesFor,
  newRunPrefix,
  queueDepth,
  skipWithoutBroker,
  sleep,
  waitFor,
} from './helpers.js'

type Expired = Extract<ConeyLogEvent, { event: 'coney.message_expired' }>

function expiredEvents(events: readonly ConeyLogEvent[]): Expired[] {
  return events.filter((e): e is Expired => e.event === 'coney.message_expired')
}

test(
  'a queue message past its ttlMs is acked without calling the handler, logged, never dead',
  { skip: skipWithoutBroker },
  async () => {
    const prefix = newRunPrefix('ttl1')
    const { events, log } = eventCollector()
    const mq = createConey({ url: BROKER_URL as string, prefix, name: 'it-ttl1', log })
    try {
      const def = defineQueue<{ n: number }>({ name: 'jobs', retry: false })
      // Published with no consumer attached, so it waits in the queue past its deadline.
      await mq.queue(def).publish({ n: 1 }, { ttlMs: 200, messageId: 'stale' })
      await sleep(450)
      let calls = 0
      await mq.queue(def).consume(() => {
        calls += 1
      })
      await waitFor(() => expiredEvents(events).length === 1, 'coney.message_expired')
      const [expired] = expiredEvents(events)
      assert.equal(expired?.queue, `${prefix}.jobs`)
      assert.equal(expired?.messageId, 'stale')
      assert.equal(expired?.attempt, 1)
      assert.ok(Date.parse(expired?.expiresAt ?? '') < Date.now())
      await mq.close()
      // After close an unacked message would be back to ready: depth 0 means it was acked.
      assert.equal(calls, 0)
      assert.equal(await queueDepth(`${prefix}.jobs`), 0)
      assert.equal(await queueDepth(`${prefix}.jobs.dead`), 0)
      assert.ok(!events.some((e) => e.event === 'coney.message_dead'))
    } finally {
      await mq.close()
      await cleanup(namesFor(prefix, ['jobs'], []))
    }
  },
)

test(
  'a message still within its ttlMs is handled normally and carries expiresAt',
  { skip: skipWithoutBroker },
  async () => {
    const prefix = newRunPrefix('ttl2')
    const { events, log } = eventCollector()
    const mq = createConey({ url: BROKER_URL as string, prefix, name: 'it-ttl2', log })
    try {
      const def = defineQueue<{ n: number }>({ name: 'jobs', retry: false })
      const seen: { expiresAt: number | undefined; publishedAt: number | undefined }[] = []
      await mq.queue(def).consume((m) => {
        seen.push({ expiresAt: m.expiresAt?.getTime(), publishedAt: m.publishedAt?.getTime() })
      })
      await mq.queue(def).publish({ n: 1 }, { ttlMs: 60_000 })
      await waitFor(() => seen.length === 1, 'the handler ran')
      const [message] = seen
      assert.ok(message?.expiresAt !== undefined && message.publishedAt !== undefined)
      assert.equal(message.expiresAt - message.publishedAt, 60_000)
      await mq.close()
      assert.equal(await queueDepth(`${prefix}.jobs`), 0)
      assert.equal(expiredEvents(events).length, 0)
    } finally {
      await mq.close()
      await cleanup(namesFor(prefix, ['jobs'], []))
    }
  },
)

test(
  'retries never extend the deadline: a message that expires in the retry bucket is not run again',
  { skip: skipWithoutBroker },
  async () => {
    const prefix = newRunPrefix('ttl3')
    const { events, log } = eventCollector()
    const mq = createConey({ url: BROKER_URL as string, prefix, name: 'it-ttl3', log })
    try {
      const def = defineQueue<{ n: number }>({ name: 'jobs', retry: { delays: [2_000] } })
      const calls: string[] = []
      await mq.queue(def).consume((m) => {
        calls.push(`${m.id}#${m.attempt}`)
        if (m.attempt === 1) throw new Error('first attempt fails')
      })
      // 'short' expires while parked in the 2 s bucket; 'long' survives it and succeeds.
      await mq.queue(def).publish({ n: 1 }, { ttlMs: 1_000, messageId: 'short' })
      await mq.queue(def).publish({ n: 2 }, { ttlMs: 20_000, messageId: 'long' })
      await waitFor(
        () => expiredEvents(events).length === 1 && calls.includes('long#2'),
        'short expired and long retried',
      )
      await sleep(200)
      assert.deepEqual([...calls].sort(), ['long#1', 'long#2', 'short#1'])
      const [expired] = expiredEvents(events)
      assert.equal(expired?.messageId, 'short')
      assert.equal(expired?.attempt, 2) // the delivery back from the bucket
      const retried = events.filter((e) => e.event === 'coney.message_retried').length
      assert.equal(retried, 2)
      await mq.close()
      assert.equal(await queueDepth(`${prefix}.jobs`), 0)
      assert.equal(await queueDepth(`${prefix}.jobs.dead`), 0)
    } finally {
      await mq.close()
      await cleanup(namesFor(prefix, ['jobs'], [2_000]))
    }
  },
)

test(
  'topic publish: a subscription and a bound queue both drop a message that expires while waiting',
  { skip: skipWithoutBroker },
  async () => {
    const prefix = newRunPrefix('ttl4')
    const { events, log } = eventCollector()
    const mq = createConey({ url: BROKER_URL as string, prefix, name: 'it-ttl4', log })
    try {
      const topic = defineTopic<{ n: number }>({ name: 'events' })
      const audit = defineQueue<{ n: number }>({
        name: 'audit',
        retry: false,
        bindings: [{ topic, patterns: ['task.*'] }],
      })
      const subCalls: number[] = []
      const queueCalls: number[] = []
      // prefetch 1 on both: the first message holds the slot while the second waits behind it.
      await mq.topic(topic).subscribe(['task.*'], async (m) => {
        subCalls.push(m.body.n)
        if (m.body.n === 1) await sleep(2_500)
      })
      await mq.queue(audit).consume(async (m) => {
        queueCalls.push(m.body.n)
        if (m.body.n === 1) await sleep(2_500)
      })
      await mq.topic(topic).publish('task.updated', { n: 1 })
      await mq.topic(topic).publish('task.updated', { n: 2 }, { ttlMs: 1_000 })
      await waitFor(() => expiredEvents(events).length === 2, 'both copies expired')
      await sleep(200)
      assert.deepEqual(subCalls, [1])
      assert.deepEqual(queueCalls, [1])
      const queues = expiredEvents(events).map((e) => e.queue)
      assert.ok(queues.includes(`${prefix}.audit`), 'the bound queue logged it')
      assert.ok(
        queues.some((q) => q.startsWith('amq.gen-')),
        'the subscription logged it on its exclusive queue',
      )
      await mq.close()
      assert.equal(await queueDepth(`${prefix}.audit`), 0)
      assert.equal(await queueDepth(`${prefix}.audit.dead`), 0)
    } finally {
      await mq.close()
      await cleanup(namesFor(prefix, ['audit'], [], ['events']))
    }
  },
)

test(
  'replayDead keeps the deadline: a dead letter replayed after it is dropped on arrival',
  { skip: skipWithoutBroker },
  async () => {
    const prefix = newRunPrefix('ttl5')
    const { events, log } = eventCollector()
    const mq = createConey({ url: BROKER_URL as string, prefix, name: 'it-ttl5', log })
    try {
      const def = defineQueue<{ n: number }>({ name: 'jobs' })
      let calls = 0
      await mq.queue(def).consume(() => {
        calls += 1
        throw new UnrecoverableError('dead before its deadline')
      })
      await mq.queue(def).publish({ n: 1 }, { ttlMs: 1_000, messageId: 'replayed' })
      await waitFor(() => events.some((e) => e.event === 'coney.message_dead'), 'dead-lettered')
      await sleep(1_500) // past the deadline while sitting in .dead
      assert.deepEqual(await mq.queue(def).replayDead(), { replayed: 1 })
      await waitFor(() => expiredEvents(events).length === 1, 'the replay expired')
      const [expired] = expiredEvents(events)
      assert.equal(expired?.messageId, 'replayed')
      assert.equal(expired?.attempt, 1) // replay resets the attempt, not the deadline
      await mq.close()
      assert.equal(calls, 1)
      assert.equal(await queueDepth(`${prefix}.jobs`), 0)
      assert.equal(await queueDepth(`${prefix}.jobs.dead`), 0)
    } finally {
      await mq.close()
      await cleanup(namesFor(prefix, ['jobs'], [10_000, 60_000, 300_000]))
    }
  },
)

/**
 * Work-queue semantics against a real broker: publish→consume→ack, the schema
 * and decode dead-letter gates, the shared retry buckets with real TTL timing,
 * and the dead-queue ops (replayDead / purgeDead / inspect).
 */

import assert from 'node:assert/strict'
import { after, test } from 'node:test'
import { createConey, defineQueue, type Message } from '../../src/index.js'
import {
  BROKER_URL,
  cleanup,
  eventCollector,
  namesFor,
  newRunPrefix,
  queueDepth,
  rawPublish,
  skipWithoutBroker,
  takeMessage,
  waitFor,
  waitForAsync,
  withRawChannel,
} from './helpers.js'

const prefix = newRunPrefix('rt')
const collector = eventCollector()
const mq =
  BROKER_URL === undefined
    ? null
    : createConey({ url: BROKER_URL, prefix, name: 'it-rt', log: collector.log })

after(async () => {
  if (mq === null) return
  await mq.close()
  await cleanup(
    namesFor(
      prefix,
      ['ack', 'gates', 'unrec', 'ladder', 'shared.a', 'shared.b', 'replay', 'purge'],
      [200, 400, 300],
    ),
  )
})

test(
  'publish → consume → ack, with a faithful Message and inspect()',
  { skip: skipWithoutBroker },
  async () => {
    const client = mq as NonNullable<typeof mq>
    const def = defineQueue<{ n: number }>({ name: 'ack', retry: false })
    const seen: Message<{ n: number }>[] = []
    const queue = client.queue(def)
    await queue.consume((message) => {
      seen.push(message)
    })
    for (let n = 0; n < 3; n += 1) await queue.publish({ n }, { messageId: `ack-${n}` })
    await waitFor(() => seen.length === 3, '3 deliveries')
    const first = seen.find((m) => m.id === 'ack-0')
    assert.ok(first !== undefined)
    assert.deepEqual(first.body, { n: 0 })
    assert.equal(first.attempt, 1)
    assert.equal(first.redelivered, false)
    assert.equal(first.routingKey, `${prefix}.ack`)
    assert.equal(first.appId, 'it-rt')
    assert.ok(first.publishedAt instanceof Date)
    const stats = await queue.inspect()
    assert.deepEqual(stats, { ready: 0, consumers: 1, dead: 0 })
  },
)

test(
  'invalid JSON and schema failures dead-letter without running the handler',
  { skip: skipWithoutBroker },
  async () => {
    const client = mq as NonNullable<typeof mq>
    const def = defineQueue<{ id: string }>({
      name: 'gates',
      retry: { delays: [200] }, // must NOT be consumed by either gate
      schema: {
        parse: (input) => {
          if (typeof (input as { id?: unknown }).id !== 'string')
            throw new Error('id must be a string')
          return input as { id: string }
        },
      },
    })
    let handled = 0
    const queue = client.queue(def)
    await queue.consume(() => {
      handled += 1
    })
    const wire = `${prefix}.gates`
    await rawPublish(wire, [Buffer.from('not json at all'), Buffer.from('{"id":42}')])
    await waitFor(
      () =>
        collector.events.filter((e) => e.event === 'coney.message_dead' && e.queue === wire)
          .length === 2,
      '2 dead events',
    )
    assert.equal(handled, 0)
    const reasons: string[] = []
    for (let i = 0; i < 2; i += 1) {
      const dead = await takeMessage(`${wire}.dead`)
      assert.ok(dead !== null)
      reasons.push(String(dead.headers['x-reason']))
      assert.equal(typeof dead.headers['x-failed-at'], 'string')
    }
    reasons.sort()
    assert.match(reasons[0] as string, /^decode: /)
    assert.match(reasons[1] as string, /^schema: id must be a string/)
  },
)

test(
  'UnrecoverableError skips the ladder and dead-letters at once',
  { skip: skipWithoutBroker },
  async () => {
    const client = mq as NonNullable<typeof mq>
    const { UnrecoverableError } = await import('../../src/index.js')
    const def = defineQueue({ name: 'unrec', retry: { delays: [200, 400] } })
    let calls = 0
    const queue = client.queue(def)
    await queue.consume(() => {
      calls += 1
      throw new UnrecoverableError('poison message')
    })
    await queue.publish({ poison: true })
    await waitForAsync(async () => (await queueDepth(`${prefix}.unrec.dead`)) === 1, 'dead depth 1')
    assert.equal(calls, 1)
    const dead = await takeMessage(`${prefix}.unrec.dead`)
    assert.equal(dead?.headers['x-reason'], 'unrecoverable: poison message')
    assert.equal(dead?.headers['x-retries'], undefined) // the ladder was never consumed
  },
)

test(
  'the ladder [200, 400] retries with real bucket timing, then dead-letters',
  { skip: skipWithoutBroker },
  async () => {
    const client = mq as NonNullable<typeof mq>
    const def = defineQueue({ name: 'ladder', retry: { delays: [200, 400] } })
    const calls: { attempt: number; at: number }[] = []
    const queue = client.queue(def)
    await queue.consume((message) => {
      calls.push({ attempt: message.attempt, at: Date.now() })
      throw new Error('always failing')
    })
    await queue.publish({ job: 1 }, { messageId: 'ladder-1' })
    await waitFor(() => calls.length === 3, '3 attempts', 15_000)
    await waitForAsync(
      async () => (await queueDepth(`${prefix}.ladder.dead`)) === 1,
      'dead depth 1',
      15_000,
    )
    assert.deepEqual(
      calls.map((c) => c.attempt),
      [1, 2, 3],
    )
    const gap1 = (calls[1] as { at: number }).at - (calls[0] as { at: number }).at
    const gap2 = (calls[2] as { at: number }).at - (calls[1] as { at: number }).at
    assert.ok(gap1 >= 150, `first retry waited ~200ms (got ${gap1}ms)`)
    assert.ok(gap2 >= 350, `second retry waited ~400ms (got ${gap2}ms)`)
    const dead = await takeMessage(`${prefix}.ladder.dead`)
    assert.equal(dead?.headers['x-retries'], 2)
    assert.match(String(dead?.headers['x-reason']), /^exhausted after 3 attempts: always failing/)
    assert.equal(dead?.properties['messageId'], 'ladder-1') // identity survives the whole ladder
    const retried = collector.events.filter(
      (e) => e.event === 'coney.message_retried' && e.queue === `${prefix}.ladder`,
    )
    assert.deepEqual(
      retried.map((e) => (e as { delayMs: number }).delayMs),
      [200, 400],
    )
  },
)

test(
  'two queues share one bucket and each message returns to its own queue',
  { skip: skipWithoutBroker },
  async () => {
    const client = mq as NonNullable<typeof mq>
    const defA = defineQueue<{ from: string }>({ name: 'shared.a', retry: { delays: [300] } })
    const defB = defineQueue<{ from: string }>({ name: 'shared.b', retry: { delays: [300] } })
    const got = { a: [] as string[], b: [] as string[] }
    const failedOnce = { a: false, b: false }
    await client.queue(defA).consume((message) => {
      if (!failedOnce.a) {
        failedOnce.a = true
        throw new Error('a fails once')
      }
      got.a.push(message.body.from)
    })
    await client.queue(defB).consume((message) => {
      if (!failedOnce.b) {
        failedOnce.b = true
        throw new Error('b fails once')
      }
      got.b.push(message.body.from)
    })
    await client.queue(defA).publish({ from: 'a' })
    await client.queue(defB).publish({ from: 'b' })
    await waitFor(
      () => got.a.length === 1 && got.b.length === 1,
      'both messages back on their own queues',
      15_000,
    )
    assert.deepEqual(got, { a: ['a'], b: ['b'] }) // the fanout bucket kept each routing key
    await withRawChannel(async (ch) => {
      await ch.checkQueue(`${prefix}.retry.300ms`) // the single shared bucket exists
    })
    const buckets = await client.inspectRetry()
    assert.ok('300ms' in buckets)
  },
)

test(
  'replayDead moves messages back with attempt reset and identity kept',
  { skip: skipWithoutBroker },
  async () => {
    const client = mq as NonNullable<typeof mq>
    const def = defineQueue<{ id: string }>({ name: 'replay', retry: false })
    let failing = true
    const deliveries: Message<{ id: string }>[] = []
    const queue = client.queue(def)
    await queue.consume((message) => {
      deliveries.push(message)
      if (failing) throw new Error('replay me')
    })
    await queue.publish({ id: 'r1' }, { messageId: 'replay-1' })
    await waitForAsync(
      async () => (await queueDepth(`${prefix}.replay.dead`)) === 1,
      'message dead',
      15_000,
    )
    failing = false
    const result = await queue.replayDead()
    assert.deepEqual(result, { replayed: 1 })
    await waitFor(() => deliveries.length === 2, 'replayed delivery')
    const second = deliveries[1] as Message<{ id: string }>
    assert.equal(second.attempt, 1) // reset
    assert.equal(second.id, 'replay-1') // kept
    assert.equal(second.headers['x-replayed'], 1)
    assert.equal(second.headers['x-reason'], undefined)
    assert.equal(second.headers['x-death'], undefined) // broker forensics stripped on replay
    assert.ok(collector.events.some((e) => e.event === 'coney.dead_replayed' && e.replayed === 1))
  },
)

test('purgeDead deletes everything in the dead queue', { skip: skipWithoutBroker }, async () => {
  const client = mq as NonNullable<typeof mq>
  const def = defineQueue({ name: 'purge', retry: false })
  const queue = client.queue(def)
  await queue.consume(() => {
    throw new Error('never works')
  })
  await queue.publish({ n: 1 })
  await queue.publish({ n: 2 })
  await waitForAsync(async () => (await queueDepth(`${prefix}.purge.dead`)) === 2, '2 dead', 15_000)
  assert.deepEqual(await queue.purgeDead(), { purged: 2 })
  assert.deepEqual((await queue.inspect()).dead, 0)
})

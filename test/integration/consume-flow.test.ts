/**
 * Consumer-lifecycle semantics: prefetch as real concurrency, handler timeouts,
 * the close() drain (both outcomes), and cancel(). One client per test — close()
 * is part of what several of them assert.
 */

import assert from 'node:assert/strict'
import { test } from 'node:test'
import { createConey, defineQueue, type ConeyLogEvent } from '../../src/index.js'
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

const makeClient = (prefix: string, log?: (e: ConeyLogEvent) => void) =>
  createConey({
    url: BROKER_URL as string,
    prefix,
    name: `it-cf`,
    ...(log !== undefined ? { log } : { log: false }),
  })

test('prefetch 4 means exactly 4 handlers in flight', { skip: skipWithoutBroker }, async () => {
  const prefix = newRunPrefix('cf1')
  const mq = makeClient(prefix)
  try {
    const def = defineQueue({ name: 'work', retry: false })
    let current = 0
    let peak = 0
    let handled = 0
    let release: () => void = () => {}
    const gate = new Promise<void>((resolve) => {
      release = resolve
    })
    const queue = mq.queue(def)
    await queue.consume(
      async () => {
        current += 1
        peak = Math.max(peak, current)
        await gate
        current -= 1
        handled += 1
      },
      { prefetch: 4 },
    )
    for (let i = 0; i < 8; i += 1) await queue.publish({ i })
    await waitFor(() => current === 4, '4 handlers in flight')
    await sleep(300) // give a 5th delivery every chance to appear …
    assert.equal(peak, 4) // … it must not (per-consumer basic.qos)
    release()
    await waitFor(() => handled === 8, 'all 8 handled')
    assert.equal(peak, 4)
  } finally {
    await mq.close()
    await cleanup(namesFor(prefix, ['work'], []))
  }
})

test(
  'timeoutMs fails the delivery now, aborts the signal, logs the late result',
  { skip: skipWithoutBroker },
  async () => {
    const prefix = newRunPrefix('cf2')
    const collector = eventCollector()
    const mq = makeClient(prefix, collector.log)
    try {
      const def = defineQueue({ name: 'slow', retry: false })
      let sawAbort = false
      const queue = mq.queue(def)
      await queue.consume(
        (message) =>
          new Promise<void>((resolve) => {
            message.signal.addEventListener('abort', () => {
              sawAbort = true
              resolve() // settle AFTER the timeout: this is the "late" result
            })
          }),
        { timeoutMs: 150 },
      )
      await queue.publish({ n: 1 })
      await waitFor(
        () => collector.names().includes('coney.message_dead'),
        'dead-lettered on timeout',
      )
      assert.equal(sawAbort, true)
      assert.ok(
        collector.events.some((e) => e.event === 'coney.handler_timeout' && e.timeoutMs === 150),
      )
      const dead = collector.events.find((e) => e.event === 'coney.message_dead')
      assert.match((dead as { reason: string }).reason, /handler timeout after 150 ms/)
      await waitFor(
        () => collector.names().includes('coney.handler_late'),
        'late settlement logged',
      )
      assert.equal(
        (collector.events.find((e) => e.event === 'coney.handler_late') as { outcome: string })
          .outcome,
        'resolved',
      )
    } finally {
      await mq.close()
      await cleanup(namesFor(prefix, ['slow'], []))
    }
  },
)

test(
  'close({drainMs}) waits for a running handler, then reports drained',
  { skip: skipWithoutBroker },
  async () => {
    const prefix = newRunPrefix('cf3')
    const mq = makeClient(prefix)
    const def = defineQueue({ name: 'drain', retry: false })
    try {
      let started = false
      let finished = false
      const queue = mq.queue(def)
      await queue.consume(async () => {
        started = true
        await sleep(300)
        finished = true
      })
      await queue.publish({ n: 1 })
      await waitFor(() => started, 'handler started')
      const result = await mq.close({ drainMs: 3000 })
      assert.deepEqual(result, { drained: true, inFlight: 0 })
      assert.equal(finished, true)
      assert.equal(await queueDepth(`${prefix}.drain`), 0) // acked before the connection went away
    } finally {
      await mq.close()
      await cleanup(namesFor(prefix, ['drain'], []))
    }
  },
)

test(
  'a hanging handler: close() aborts its signal and leaves the message for redelivery',
  { skip: skipWithoutBroker },
  async () => {
    const prefix = newRunPrefix('cf4')
    const mq = makeClient(prefix)
    const def = defineQueue({ name: 'hang', retry: false })
    try {
      let started = false
      let aborted = false
      const queue = mq.queue(def)
      await queue.consume((message) => {
        started = true
        message.signal.addEventListener('abort', () => {
          aborted = true
        })
        return new Promise(() => {}) // never settles
      })
      await queue.publish({ n: 1 })
      await waitFor(() => started, 'handler started')
      const result = await mq.close({ drainMs: 300 })
      assert.deepEqual(result, { drained: false, inFlight: 1 })
      assert.equal(aborted, true)
      assert.equal(await queueDepth(`${prefix}.hang`), 1) // unacked → requeued, never lost
    } finally {
      await cleanup(namesFor(prefix, ['hang'], []))
    }
  },
)

test(
  'cancel() stops delivery; later publishes stay ready; active flips to false',
  { skip: skipWithoutBroker },
  async () => {
    const prefix = newRunPrefix('cf5')
    const mq = makeClient(prefix)
    const def = defineQueue({ name: 'stop', retry: false })
    try {
      let handled = 0
      const queue = mq.queue(def)
      const subscription = await queue.consume(() => {
        handled += 1
      })
      await queue.publish({ n: 1 })
      await waitFor(() => handled === 1, 'first message handled')
      assert.equal(subscription.active, true)
      const result = await subscription.cancel()
      assert.deepEqual(result, { drained: true, inFlight: 0 })
      assert.equal(subscription.active, false)
      await queue.publish({ n: 2 })
      await sleep(400)
      assert.equal(handled, 1) // nothing consumed it …
      assert.equal((await queue.inspect()).ready, 1) // … the message waits in the queue
    } finally {
      await mq.close()
      await cleanup(namesFor(prefix, ['stop'], []))
    }
  },
)

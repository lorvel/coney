/**
 * Topic semantics: exclusive fan-out to several connections, unroutable
 * publishes, and durable bound queues with the full retry machinery — including
 * the load-bearing detail that a retried message returns to the BOUND QUEUE
 * (the retry publish uses the queue's wire name, never the topic key).
 */

import assert from 'node:assert/strict'
import { test } from 'node:test'
import { createConey, defineQueue, defineTopic, type Message } from '../../src/index.js'
import { BROKER_URL, cleanup, namesFor, newRunPrefix, skipWithoutBroker, sleep, waitFor } from './helpers.js'

test('two connections, two exclusive subscriptions: both receive a matching event', { skip: skipWithoutBroker }, async () => {
  const prefix = newRunPrefix('tp1')
  const a = createConey({ url: BROKER_URL as string, prefix, name: 'it-tp-a', log: false })
  const b = createConey({ url: BROKER_URL as string, prefix, name: 'it-tp-b', log: false })
  try {
    const events = defineTopic<{ id: number }>({ name: 'events' })
    const gotA: string[] = []
    const gotB: string[] = []
    await a.topic(events).subscribe(['task.*'], (m) => {
      gotA.push(m.routingKey)
    })
    await b.topic(events).subscribe(['task.*'], (m) => {
      gotB.push(m.routingKey)
    })
    await a.topic(events).publish('task.updated', { id: 1 })
    await waitFor(() => gotA.length === 1 && gotB.length === 1, 'both subscribers got the event')
    assert.deepEqual(gotA, ['task.updated'])
    assert.deepEqual(gotB, ['task.updated'])
    await a.topic(events).publish('ku.updated', { id: 2 }) // matches neither pattern
    await sleep(300)
    assert.equal(gotA.length, 1)
    assert.equal(gotB.length, 1)
  } finally {
    await a.close()
    await b.close()
    await cleanup(namesFor(prefix, [], [], ['events']))
  }
})

test('a topic publish with no matching binding resolves (confirmed and dropped)', { skip: skipWithoutBroker }, async () => {
  const prefix = newRunPrefix('tp2')
  const mq = createConey({ url: BROKER_URL as string, prefix, name: 'it-tp2', log: false })
  try {
    const events = defineTopic({ name: 'void' })
    await mq.topic(events).publish('nobody.listens', { ok: true }) // must not hang or reject
  } finally {
    await mq.close()
    await cleanup(namesFor(prefix, [], [], ['void']))
  }
})

test('a bound queue gets topic deliveries AND the full retry/dead machinery', { skip: skipWithoutBroker }, async () => {
  const prefix = newRunPrefix('tp3')
  const mq = createConey({ url: BROKER_URL as string, prefix, name: 'it-tp3', log: false })
  try {
    const events = defineTopic<{ id: number }>({ name: 'stream' })
    const def = defineQueue<{ id: number }>({
      name: 'audit',
      retry: { delays: [250] },
      bindings: [{ topic: events, patterns: ['ku.*'] }],
    })
    const deliveries: Message<{ id: number }>[] = []
    await mq.queue(def).consume((message) => {
      deliveries.push(message)
      if (message.attempt === 1) throw new Error('first attempt fails')
    })
    await mq.topic(events).publish('ku.embed', { id: 7 }, { messageId: 'bound-1' })
    await waitFor(() => deliveries.length === 2, 'retry came back to the bound queue', 15_000)
    const [first, second] = deliveries as [Message<{ id: number }>, Message<{ id: number }>]
    assert.equal(first.attempt, 1)
    assert.equal(first.routingKey, 'ku.embed') // original topic key
    assert.equal(second.attempt, 2)
    assert.equal(second.id, 'bound-1')
    assert.deepEqual(second.body, { id: 7 })
    // The retry publish routes by the QUEUE's wire name (spec §3): the redelivery
    // carries that key — proof the message came back to this queue, not the topic.
    assert.equal(second.routingKey, `${prefix}.audit`)
  } finally {
    await mq.close()
    await cleanup(namesFor(prefix, ['audit'], [250], ['stream']))
  }
})

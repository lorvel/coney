import assert from 'node:assert/strict'
import { describe, it } from 'node:test'
import { createConeyMock } from '../src/mock/client.js'
import { defineQueue, defineTopic, type Schema } from '../src/defs/define.js'
import {
  ConeyClosedError,
  ConeyConfigError,
  ConeySchemaError,
  UnrecoverableError,
} from '../src/core/errors.js'

interface Job {
  id: string
}

const jobSchema: Schema<Job> = {
  parse: (input) => {
    if (typeof (input as { id?: unknown }).id !== 'string') throw new Error('id must be a string')
    return input as Job
  },
}

describe('createConeyMock: the Coney surface', () => {
  it('is open, resolves names, and every op resolves with empty values', async () => {
    const mock = createConeyMock({ prefix: 'test', name: 'svc' })
    assert.equal(mock.state, 'open')
    assert.equal(mock.prefix, 'test')
    assert.equal(mock.resolveName('jobs'), 'test.jobs')
    await mock.open()
    const jobs = mock.queue(defineQueue({ name: 'jobs' }))
    assert.deepEqual(await jobs.inspect(), { ready: 0, consumers: 0, dead: 0 })
    assert.deepEqual(await jobs.replayDead(), { replayed: 0 })
    assert.deepEqual(await jobs.purgeDead(), { purged: 0 })
    assert.deepEqual(await mock.inspectRetry(), {})
    await mock.declare([defineQueue({ name: 'other' })])
  })

  it('close() mirrors the real lifecycle: state, gates and subscriptions', async () => {
    const mock = createConeyMock({ prefix: 'test' })
    const def = defineQueue({ name: 'jobs' })
    const jobs = mock.queue(def)
    const subscription = await jobs.consume(() => {})
    assert.deepEqual(await mock.close(), { drained: true, inFlight: 0 })
    assert.deepEqual(await mock.close(), { drained: true, inFlight: 0 }) // idempotent
    assert.equal(mock.state, 'closed')
    assert.equal(subscription.active, false)
    await assert.rejects(jobs.publish({ x: 1 }), ConeyClosedError)
    await assert.rejects(
      jobs.consume(() => {}),
      ConeyClosedError,
    )
    await assert.rejects(jobs.inspect(), ConeyClosedError)
    await assert.rejects(jobs.replayDead(), ConeyClosedError)
    await assert.rejects(jobs.purgeDead(), ConeyClosedError)
    await assert.rejects(mock.open(), ConeyClosedError)
    await assert.rejects(mock.declare([def]), ConeyClosedError)
    await assert.rejects(mock.inspectRetry(), ConeyClosedError)
    // Delivery finds no consumers left.
    await assert.rejects(mock.deliver(def, { x: 1 }), ConeyConfigError)
    const events = defineTopic({ name: 'events' })
    mock.queue(defineQueue({ name: 'listener', bindings: [{ topic: events, patterns: ['#'] }] }))
    assert.deepEqual(await mock.deliverTopic(events, 'any.key', { x: 1 }), [])
  })

  it('caches handles per wire name and rejects conflicting defs', () => {
    const mock = createConeyMock()
    const def = defineQueue({ name: 'jobs' })
    assert.equal(mock.queue(def), mock.queue(defineQueue({ name: 'jobs' })))
    assert.throws(
      () => mock.queue(defineQueue({ name: 'jobs', retry: { delays: [5] } })),
      ConeyConfigError,
    )
  })
})

describe('mock publish: validate exactly like the real client, then record', () => {
  it('records the schema-parsed body with the wire name', async () => {
    const mock = createConeyMock({ prefix: 'test' })
    const jobs = mock.queue(defineQueue({ name: 'jobs', schema: jobSchema }))
    await jobs.publish({ id: 'a' })
    const events = mock.topic(defineTopic({ name: 'events' }))
    await events.publish('task.updated', { any: 1 }, { messageId: 'm-1' })
    assert.equal(mock.published.length, 2)
    assert.deepEqual(mock.published[0], {
      target: { kind: 'queue', name: 'test.jobs' },
      routingKey: 'test.jobs',
      body: { id: 'a' },
      options: {},
    })
    assert.deepEqual(mock.published[1]?.target, { kind: 'topic', name: 'test.events' })
    assert.equal(mock.published[1]?.routingKey, 'task.updated')
    assert.deepEqual(mock.published[1]?.options, { messageId: 'm-1' })
  })

  it('rejects schema failures, x- headers, empty routing keys and non-JSON bodies', async () => {
    const mock = createConeyMock()
    const jobs = mock.queue(defineQueue({ name: 'jobs', schema: jobSchema }))
    await assert.rejects(jobs.publish({ id: 1 as never }), ConeySchemaError)
    await assert.rejects(jobs.publish({ id: 'a' }, { headers: { 'x-mine': 1 } }), ConeyConfigError)
    const events = mock.topic(defineTopic({ name: 'events' }))
    await assert.rejects(events.publish('', { ok: 1 }), ConeyConfigError)
    await assert.rejects(
      mock.queue(defineQueue({ name: 'raw' })).publish(undefined),
      ConeyConfigError,
    )
    assert.equal(mock.published.length, 0)
  })

  it('reset clears published but keeps consumers registered', async () => {
    const mock = createConeyMock()
    const def = defineQueue({ name: 'jobs' })
    const jobs = mock.queue(def)
    await jobs.consume(() => {})
    await jobs.publish({ n: 1 })
    mock.reset()
    assert.equal(mock.published.length, 0)
    assert.deepEqual(await mock.deliver(def, { n: 2 }), { outcome: 'ack' })
  })
})

describe('mock deliver: the real pipeline in-process', () => {
  it('throws ConeyConfigError when no consumer is registered', async () => {
    const mock = createConeyMock()
    await assert.rejects(mock.deliver(defineQueue({ name: 'jobs' }), { n: 1 }), ConeyConfigError)
  })

  it('maps outcomes: ack, retry with ladder arithmetic, dead when exhausted', async () => {
    const mock = createConeyMock()
    const def = defineQueue({ name: 'jobs', retry: { delays: [100, 200] } })
    let fail = false
    await mock.queue(def).consume(() => {
      if (fail) throw new Error('boom')
    })
    assert.deepEqual(await mock.deliver(def, { n: 1 }), { outcome: 'ack' })
    fail = true
    const first = await mock.deliver(def, { n: 1 })
    assert.equal(first.outcome, 'retry')
    assert.deepEqual(
      {
        attempt: (first as { attempt: number }).attempt,
        delayMs: (first as { delayMs: number }).delayMs,
      },
      {
        attempt: 1,
        delayMs: 100,
      },
    )
    const second = await mock.deliver(def, { n: 1 }, { attempt: 2 })
    assert.equal((second as { delayMs: number }).delayMs, 200)
    const third = await mock.deliver(def, { n: 1 }, { attempt: 3 })
    assert.equal(third.outcome, 'dead')
    assert.match((third as { reason: string }).reason, /^exhausted after 3 attempts: boom/)
  })

  it('uses the client default ladder when the def has none, and retry: false goes straight to dead', async () => {
    const mock = createConeyMock()
    const plain = defineQueue({ name: 'plain' })
    await mock.queue(plain).consume(() => {
      throw new Error('x')
    })
    const outcome = await mock.deliver(plain, {})
    assert.equal((outcome as { delayMs: number }).delayMs, 10_000)
    const strict = defineQueue({ name: 'strict', retry: false })
    await mock.queue(strict).consume(() => {
      throw new Error('x')
    })
    assert.equal((await mock.deliver(strict, {})).outcome, 'dead')
  })

  it('UnrecoverableError and schema failures dead-letter with the spec reasons', async () => {
    const mock = createConeyMock()
    const def = defineQueue({ name: 'jobs', schema: jobSchema })
    let handled = 0
    await mock.queue(def).consume(() => {
      handled += 1
      throw new UnrecoverableError('cannot process')
    })
    const bad = await mock.deliver(def, { id: 42 } as never)
    assert.equal(bad.outcome, 'dead')
    assert.match((bad as { reason: string }).reason, /^schema: /)
    assert.equal(handled, 0) // the schema gate ran before the handler
    const unrecoverable = await mock.deliver(def, { id: 'a' })
    assert.equal((unrecoverable as { reason: string }).reason, 'unrecoverable: cannot process')
  })

  it('round-trips the body through JSON so wire fidelity bugs surface', async () => {
    const mock = createConeyMock()
    const def = defineQueue<{ at: Date }>({ name: 'jobs' })
    let seen: unknown
    await mock.queue(def).consume((message) => {
      seen = message.body
    })
    await mock.deliver(def, { at: new Date('2026-08-24T00:00:00.000Z') })
    assert.deepEqual(seen, { at: '2026-08-24T00:00:00.000Z' }) // a Date arrives as a string
  })

  it('builds a faithful Message: id, attempt, headers, redelivered, signal + timeoutMs', async () => {
    const mock = createConeyMock({ name: 'svc' })
    const def = defineQueue({ name: 'jobs' })
    const messages: Array<{
      id: string
      attempt: number
      redelivered: boolean
      headers: Record<string, unknown>
      appId?: string
    }> = []
    await mock.queue(def).consume((incoming) => {
      messages.push(incoming as (typeof messages)[number])
    })
    await mock.deliver(
      def,
      { n: 1 },
      { messageId: 'm-7', attempt: 3, redelivered: true, headers: { tenant: 't1' } },
    )
    const seen = messages[0]
    assert.ok(seen !== undefined)
    assert.equal(seen.id, 'm-7')
    assert.equal(seen.attempt, 3)
    assert.equal(seen.redelivered, true)
    assert.equal(seen.headers['x-retries'], 2)
    assert.equal(seen.headers['tenant'], 't1')
    assert.equal(seen.appId, 'svc')
    const slow = defineQueue({ name: 'slow' })
    await mock
      .queue(slow)
      .consume(() => new Promise((resolve) => setTimeout(resolve, 1000)), { timeoutMs: 15 })
    const timedOut = await mock.deliver(slow, {})
    assert.equal(timedOut.outcome, 'retry')
    assert.match(
      ((timedOut as { error: unknown }).error as Error).message,
      /handler timeout after 15 ms/,
    )
  })

  it('round-robins between competing consumers of one queue', async () => {
    const mock = createConeyMock()
    const def = defineQueue({ name: 'jobs' })
    const seen: string[] = []
    await mock.queue(def).consume(() => {
      seen.push('a')
    })
    await mock.queue(def).consume(() => {
      seen.push('b')
    })
    await mock.deliver(def, {})
    await mock.deliver(def, {})
    await mock.deliver(def, {})
    assert.deepEqual(seen, ['a', 'b', 'a'])
  })

  it('a cancelled consumer no longer receives, and its subscription reports inactive', async () => {
    const mock = createConeyMock()
    const def = defineQueue({ name: 'jobs' })
    const subscription = await mock.queue(def).consume(() => {})
    assert.equal(subscription.active, true)
    assert.deepEqual(await subscription.cancel(), { drained: true, inFlight: 0 })
    assert.equal(subscription.active, false)
    await assert.rejects(mock.deliver(def, {}), ConeyConfigError)
  })
})

describe('mock deliverTopic: subscriptions and bound queues, in registration order', () => {
  it('routes by AMQP patterns and reports one outcome per match', async () => {
    const mock = createConeyMock()
    const events = defineTopic({ name: 'events' })
    const audit = defineQueue({
      name: 'audit',
      retry: { delays: [50] },
      bindings: [{ topic: events, patterns: ['task.#'] }],
    })
    const calls: string[] = []
    await mock.queue(audit).consume(() => {
      calls.push('audit')
      throw new Error('audit down')
    })
    await mock.topic(events).subscribe(['task.*'], () => {
      calls.push('sub')
    })
    const outcomes = await mock.deliverTopic(events, 'task.updated', { any: 1 })
    assert.deepEqual(calls, ['audit', 'sub']) // registration order
    assert.equal(outcomes.length, 2)
    assert.equal(outcomes[0]?.outcome, 'retry') // the bound queue keeps its full ladder
    assert.equal(outcomes[1]?.outcome, 'ack')
    assert.deepEqual(await mock.deliverTopic(events, 'ku.updated', { any: 1 }), [])
  })

  it('a handler cancelling its own (or an earlier) subscription cannot skip later recipients', async () => {
    const mock = createConeyMock()
    const events = defineTopic({ name: 'events' })
    const topic = mock.topic(events)
    const calls: string[] = []
    const subA = await topic.subscribe(['orders.#'], async () => {
      calls.push('a')
      await subA.cancel() // splices the live registrations array mid-delivery
    })
    await topic.subscribe(['orders.#'], () => {
      calls.push('b')
    })
    const outcomes = await mock.deliverTopic(events, 'orders.created', {})
    // The broker fans out BEFORE any cancel can land: both receive this delivery.
    assert.deepEqual(calls, ['a', 'b'])
    assert.equal(outcomes.length, 2)
    assert.equal(subA.active, false)
    // The next delivery reaches only B.
    assert.equal((await mock.deliverTopic(events, 'orders.created', {})).length, 1)
    assert.deepEqual(calls, ['a', 'b', 'b'])
  })

  it('a throwing subscription handler is dropped, not retried', async () => {
    const mock = createConeyMock()
    const events = defineTopic({ name: 'events' })
    const thrown = new Error('listener boom')
    await mock.topic(events).subscribe(['#'], () => {
      throw thrown
    })
    assert.deepEqual(await mock.deliverTopic(events, 'anything', {}), [
      { outcome: 'dropped', error: thrown },
    ])
  })

  it('gates subscriptions with the topic schema and bound queues with the queue schema', async () => {
    const mock = createConeyMock()
    const events = defineTopic({ name: 'events', schema: jobSchema })
    const strictQueue = defineQueue({
      name: 'strict',
      schema: jobSchema,
      bindings: [{ topic: events, patterns: ['#'] }],
    })
    await mock.topic(events).subscribe(['#'], () => {})
    await mock.queue(strictQueue).consume(() => {})
    const outcomes = await mock.deliverTopic(events, 'task.x', { id: 99 } as never)
    assert.equal(outcomes.length, 2)
    assert.equal(outcomes[0]?.outcome, 'dropped') // subscription: schema failure drops
    assert.equal(outcomes[1]?.outcome, 'dead') // bound queue: schema failure dead-letters
    assert.match((outcomes[1] as { reason: string }).reason, /^schema: /)
  })
})

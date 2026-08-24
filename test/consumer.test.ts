import assert from 'node:assert/strict'
import { describe, it } from 'node:test'
import {
  runConsumePipeline,
  truncateReason,
  type DeliveryContext,
  type PipelineOutcome,
} from '../src/consume/pipeline.js'
import { contextOf, republishOptions } from '../src/consume/delivery.js'
import { UnrecoverableError } from '../src/core/errors.js'

const context = (attempt = 1): DeliveryContext => ({
  id: 'm-1',
  attempt,
  redelivered: false,
  routingKey: 'jobs',
  headers: attempt > 1 ? { 'x-retries': attempt - 1 } : {},
})

const run = (args: {
  attempt?: number
  handler: (message: { body: unknown; signal: AbortSignal }) => void | Promise<void>
  delays?: readonly number[] | null
  schema?: { parse(input: unknown): unknown }
  timeoutMs?: number
  onTimeout?: (ms: number) => void
  onLate?: (outcome: 'resolved' | 'rejected') => void
}): Promise<PipelineOutcome> =>
  runConsumePipeline({
    schema: args.schema,
    body: { ok: true },
    context: context(args.attempt ?? 1),
    handler: args.handler,
    controller: new AbortController(),
    timeoutMs: args.timeoutMs,
    delays: args.delays === undefined ? [100, 200] : args.delays,
    ...(args.onTimeout !== undefined ? { onTimeout: args.onTimeout } : {}),
    ...(args.onLate !== undefined ? { onLate: args.onLate } : {}),
  })

describe('the consume pipeline outcome table (spec §4)', () => {
  it('a resolving handler acks', async () => {
    assert.deepEqual(await run({ handler: () => {} }), { kind: 'ack' })
  })

  it('a throw within the ladder retries with delays[attempt - 1]', async () => {
    const error = new Error('boom')
    const outcome = await run({ handler: () => Promise.reject(error) })
    assert.deepEqual(outcome, { kind: 'retry', attempt: 1, delayMs: 100, error, reason: 'boom' })
    const second = await run({ attempt: 2, handler: () => Promise.reject(error) })
    assert.deepEqual(second, { kind: 'retry', attempt: 2, delayMs: 200, error, reason: 'boom' })
  })

  it('a throw beyond the ladder dead-letters with the exhausted reason', async () => {
    const outcome = await run({
      attempt: 3,
      handler: () => {
        throw new Error('still broken')
      },
    })
    assert.equal(outcome.kind, 'dead')
    assert.equal((outcome as { reason: string }).reason, 'exhausted after 3 attempts: still broken')
  })

  it('an empty ladder dead-letters on the first failure', async () => {
    const outcome = await run({ delays: [], handler: () => Promise.reject(new Error('nope')) })
    assert.equal(outcome.kind, 'dead')
    assert.equal((outcome as { reason: string }).reason, 'exhausted after 1 attempts: nope')
  })

  it('UnrecoverableError dead-letters immediately, ladder untouched', async () => {
    const outcome = await run({ handler: () => Promise.reject(new UnrecoverableError('bad payload')) })
    assert.equal(outcome.kind, 'dead')
    assert.equal((outcome as { reason: string }).reason, 'unrecoverable: bad payload')
  })

  it('a schema failure dead-letters without calling the handler', async () => {
    let called = false
    const outcome = await run({
      schema: {
        parse: () => {
          throw new Error('id missing')
        },
      },
      handler: () => {
        called = true
      },
    })
    assert.equal(called, false)
    assert.equal(outcome.kind, 'dead')
    assert.equal((outcome as { reason: string }).reason, 'schema: id missing')
  })

  it('subscription mode (delays: null) drops on any failure', async () => {
    const thrown = new Error('sub boom')
    assert.deepEqual(await run({ delays: null, handler: () => Promise.reject(thrown) }), {
      kind: 'drop',
      error: thrown,
      reason: 'sub boom',
    })
    const schemaDrop = await run({
      delays: null,
      schema: {
        parse: () => {
          throw new Error('shape')
        },
      },
      handler: () => {},
    })
    assert.equal(schemaDrop.kind, 'drop')
    assert.equal((schemaDrop as { reason: string }).reason, 'schema: shape')
  })

  it('the schema-parsed value (not the raw body) reaches the handler', async () => {
    let seen: unknown
    await run({
      schema: { parse: () => ({ normalized: true }) },
      handler: (message) => {
        seen = message.body
      },
    })
    assert.deepEqual(seen, { normalized: true })
  })

  it('a synchronous handler throw is a normal failure, never an exception', async () => {
    const outcome = await run({
      handler: () => {
        throw new Error('sync')
      },
    })
    assert.equal(outcome.kind, 'retry')
  })
})

describe('handler timeout (fail now, log the late result)', () => {
  it('fails the delivery at the deadline, aborts the signal, reports the late settlement', async () => {
    const timeouts: number[] = []
    const late: string[] = []
    let abortedInsideHandler = false
    let release: () => void = () => {}
    const outcome = await run({
      timeoutMs: 20,
      handler: (message) =>
        new Promise<void>((resolve) => {
          release = resolve
          message.signal.addEventListener('abort', () => {
            abortedInsideHandler = true
          })
        }),
      onTimeout: (ms) => timeouts.push(ms),
      onLate: (kind) => late.push(kind),
    })
    assert.equal(outcome.kind, 'retry')
    assert.equal((outcome as { reason: string }).reason, 'handler timeout after 20 ms')
    assert.deepEqual(timeouts, [20])
    assert.equal(abortedInsideHandler, true)
    assert.deepEqual(late, [])
    release() // the abandoned invocation settles now …
    await new Promise((resolve) => setImmediate(resolve))
    assert.deepEqual(late, ['resolved']) // … and is only logged
  })

  it('a late rejection is logged as rejected and never unhandled', async () => {
    const late: string[] = []
    let reject: (err: Error) => void = () => {}
    const outcome = await run({
      timeoutMs: 10,
      delays: [],
      handler: () =>
        new Promise<void>((_resolve, rej) => {
          reject = rej
        }),
      onLate: (kind) => late.push(kind),
    })
    assert.equal(outcome.kind, 'dead')
    reject(new Error('too late'))
    await new Promise((resolve) => setImmediate(resolve))
    assert.deepEqual(late, ['rejected'])
  })

  it('a handler finishing under the deadline wins the race', async () => {
    const late: string[] = []
    const outcome = await run({
      timeoutMs: 1000,
      handler: () => new Promise((resolve) => setTimeout(resolve, 5)),
      onLate: (kind) => late.push(kind),
    })
    assert.deepEqual(outcome, { kind: 'ack' })
    assert.deepEqual(late, [])
  })
})

describe('reason truncation and delivery context', () => {
  it('x-reason style strings are capped at 500 chars', () => {
    assert.equal(truncateReason('a'.repeat(600)).length, 500)
    assert.equal(truncateReason('short'), 'short')
  })

  it('reasons longer than 500 chars are capped in dead outcomes', async () => {
    const outcome = await run({ attempt: 3, handler: () => Promise.reject(new Error('e'.repeat(600))) })
    assert.equal((outcome as { reason: string }).reason.length, 500)
  })

  it('contextOf reads attempt from x-retries and tolerates foreign messages', () => {
    const raw = (properties: Record<string, unknown>, redelivered = false) =>
      ({ fields: { routingKey: 'k', redelivered }, properties, content: Buffer.from('{}') }) as never
    const ours = contextOf(
      raw({ messageId: 'id-1', appId: 'svc', headers: { 'x-retries': 2, 'x-published-at': '2026-08-24T00:00:00.000Z' } }),
    )
    assert.equal(ours.attempt, 3)
    assert.equal(ours.id, 'id-1')
    assert.equal(ours.appId, 'svc')
    assert.equal(ours.publishedAt?.toISOString(), '2026-08-24T00:00:00.000Z')
    const foreign = contextOf(raw({}, true))
    assert.equal(foreign.attempt, 1)
    assert.equal(foreign.id, '') // no generated id: it would not survive a redelivery
    assert.equal(foreign.redelivered, true)
    assert.equal(foreign.publishedAt, undefined)
    const garbage = contextOf(raw({ headers: { 'x-retries': 'two', 'x-published-at': 'not-a-date' } }))
    assert.equal(garbage.attempt, 1)
    assert.equal(garbage.publishedAt, undefined)
  })

  it('republishOptions whitelists properties and drops userId/expiration', () => {
    const options = republishOptions(
      {
        messageId: 'm',
        type: 'jobs',
        appId: 'svc',
        deliveryMode: 2,
        contentType: 'application/json',
        userId: 'someone-else',
        expiration: '5000',
        clusterId: 'x',
        headers: { old: true },
      },
      { fresh: true },
    )
    assert.deepEqual(options, {
      headers: { fresh: true },
      messageId: 'm',
      type: 'jobs',
      appId: 'svc',
      deliveryMode: 2,
      contentType: 'application/json',
    })
  })
})

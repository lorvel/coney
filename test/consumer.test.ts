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
import { BaseConsumer, type ConsumerHooks } from '../src/consume/runtime.js'
import type { ChannelFactory } from '../src/connection/manager.js'
import type { ConeyLogEvent, LogEmitter } from '../src/core/log.js'
import type { ConsumeMessage } from 'amqplib'
import { FakeConfirmChannel, fakeFactory } from './fakes.js'

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
    const outcome = await run({
      handler: () => Promise.reject(new UnrecoverableError('bad payload')),
    })
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

describe('the expiry gate (PublishOptions.ttlMs, spec §4)', () => {
  const runAt = (
    expiresAt: Date | undefined,
    args: {
      delays?: readonly number[] | null
      body?: unknown
      schema?: { parse(input: unknown): unknown }
      attempt?: number
    } = {},
  ) => {
    let calls = 0
    let seen: Date | undefined
    const outcome = runConsumePipeline({
      schema: args.schema,
      body: args.body ?? { ok: true },
      context: { ...context(args.attempt ?? 1), ...(expiresAt !== undefined ? { expiresAt } : {}) },
      handler: (message) => {
        calls += 1
        seen = message.expiresAt
      },
      controller: new AbortController(),
      timeoutMs: undefined,
      delays: args.delays === undefined ? [100, 200] : args.delays,
    })
    return outcome.then((result) => ({ result, calls, seen }))
  }

  it('a passed deadline expires the message without calling the handler', async () => {
    const past = new Date(Date.now() - 1)
    const { result, calls } = await runAt(past)
    assert.deepEqual(result, { kind: 'expired', expiresAt: past })
    assert.equal(calls, 0)
  })

  it('expires in subscription mode too, and on a later attempt (retries never extend it)', async () => {
    const past = new Date(Date.now() - 1)
    assert.equal((await runAt(past, { delays: null })).result.kind, 'expired')
    assert.equal((await runAt(past, { attempt: 3 })).result.kind, 'expired')
  })

  it('runs before the schema gate: an expired message never reaches .dead', async () => {
    const schema = {
      parse: () => {
        throw new Error('bad body')
      },
    }
    const { result, calls } = await runAt(new Date(Date.now() - 1), { schema })
    assert.equal(result.kind, 'expired')
    assert.equal(calls, 0)
  })

  it('a future deadline runs the handler, which sees it as message.expiresAt', async () => {
    const future = new Date(Date.now() + 60_000)
    const { result, calls, seen } = await runAt(future)
    assert.deepEqual(result, { kind: 'ack' })
    assert.equal(calls, 1)
    assert.equal(seen?.getTime(), future.getTime())
  })

  it('the deadline itself is already expired (now >= expiresAt), one ms before is not', async (t) => {
    const now = 1_760_000_000_000
    t.mock.method(Date, 'now', () => now)
    assert.equal((await runAt(new Date(now))).result.kind, 'expired')
    const justBefore = await runAt(new Date(now + 1))
    assert.equal(justBefore.result.kind, 'ack')
    assert.equal(justBefore.calls, 1)
  })

  it('the gate decides whether a delivery STARTS: a late failure follows the outcome table', async () => {
    const lateFailure = (delays: readonly number[]) =>
      runConsumePipeline({
        schema: undefined,
        body: {},
        context: { ...context(1), expiresAt: new Date(Date.now() + 20) },
        handler: async () => {
          await new Promise((resolve) => setTimeout(resolve, 60))
          throw new Error('failed after the deadline')
        },
        controller: new AbortController(),
        timeoutMs: undefined,
        delays,
      })
    assert.equal((await lateFailure([100])).kind, 'retry') // the next delivery meets the gate
    assert.equal((await lateFailure([])).kind, 'dead') // keeps its failure reason
  })

  it('no deadline means no gate', async () => {
    const { result, calls, seen } = await runAt(undefined)
    assert.deepEqual(result, { kind: 'ack' })
    assert.equal(calls, 1)
    assert.equal(seen, undefined)
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
    const outcome = await run({
      attempt: 3,
      handler: () => Promise.reject(new Error('e'.repeat(600))),
    })
    assert.equal((outcome as { reason: string }).reason.length, 500)
  })

  it('contextOf reads attempt from x-retries and tolerates foreign messages', () => {
    const raw = (properties: Record<string, unknown>, redelivered = false) =>
      ({
        fields: { routingKey: 'k', redelivered },
        properties,
        content: Buffer.from('{}'),
      }) as never
    const ours = contextOf(
      raw({
        messageId: 'id-1',
        appId: 'svc',
        headers: { 'x-retries': 2, 'x-published-at': '2026-08-24T00:00:00.000Z' },
      }),
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
    const garbage = contextOf(
      raw({ headers: { 'x-retries': 'two', 'x-published-at': 'not-a-date' } }),
    )
    assert.equal(garbage.attempt, 1)
    assert.equal(garbage.publishedAt, undefined)
  })

  it('contextOf honours x-expires-at only in the exact toISOString form', () => {
    const raw = (headers: Record<string, unknown>) =>
      ({
        fields: { routingKey: 'k', redelivered: false },
        properties: { headers },
        content: Buffer.from('{}'),
      }) as never
    const ours = contextOf(raw({ 'x-expires-at': '2026-10-14T00:00:01.500Z' }))
    assert.equal(ours.expiresAt?.toISOString(), '2026-10-14T00:00:01.500Z')
    assert.equal(contextOf(raw({})).expiresAt, undefined)
    // new Date() alone would read '0' and 'Oct 14' as dates in 2000/2001 and zone-less
    // or offset forms in other zones — a foreign value must never become a deadline.
    for (const value of [
      'soon',
      '0',
      '1',
      'Oct 14',
      '2026-10-14T00:00:01Z',
      '2026-10-14T00:00:01.500',
      '2026-10-14T07:00:01.500+07:00',
      '2026-13-01T00:00:00.000Z',
      '2026-02-31T00:00:00.000Z', // V8 would roll it over into March
      '2026-10-14T24:00:00.000Z', // ...and this into the next day
      1_700_000_000_000,
    ]) {
      assert.equal(contextOf(raw({ 'x-expires-at': value })).expiresAt, undefined, String(value))
    }
    // x-published-at keeps its 3.0.0 reading (lenient): only the new header is strict.
    assert.ok(contextOf(raw({ 'x-published-at': 'Oct 14' })).publishedAt instanceof Date)
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

/**
 * A minimal runtime: `doAttach` adopts a fake channel and nothing else, so the only
 * behaviour under test is the base class's detach dispatch.
 */
class ProbeConsumer extends BaseConsumer {
  constructor(
    hooks: ConsumerHooks,
    private readonly fake: FakeConfirmChannel,
    emit: LogEmitter = () => {},
  ) {
    super('probe-tag', 1, emit, hooks)
  }

  get queueLabel(): string {
    return 'probe.queue'
  }

  protected async doAttach(factory: ChannelFactory): Promise<void> {
    const channel = await factory.createConfirmChannel()
    this.adoptChannel(channel)
  }

  protected async processDelivery(): Promise<void> {}

  /** amqplib's sync consume callback, reachable from a test. */
  deliver(raw: ConsumeMessage | null): void {
    this.onDelivery(this.fake.asChannel(), raw)
  }
}

describe('BaseConsumer detach dispatch (spec §4)', () => {
  const attached = async (
    emit?: LogEmitter,
  ): Promise<{
    fake: FakeConfirmChannel
    runtime: ProbeConsumer
    seen: { disposed: number; detached: number }
  }> => {
    const fake = new FakeConfirmChannel()
    const seen = { disposed: 0, detached: 0 }
    const runtime = new ProbeConsumer(
      {
        onDispose: () => {
          seen.disposed += 1
        },
        onDetached: () => {
          seen.detached += 1
        },
      },
      fake,
      emit,
    )
    await runtime.attach(fakeFactory({ createConfirmChannel: async () => fake.asChannel() }))
    return { fake, runtime, seen }
  }

  it('a channel that dies on its own hands the runtime to recovery', async () => {
    // A channel-level error (404 on a vanished bucket, 406, 403) closes just this
    // channel with the CONNECTION still up. amqplib delivers `null` only for a real
    // `basic.cancel`, so before the hand-off this path detached in total silence and
    // nothing ever re-attached: `active` stayed true and the consumer was dead.
    const { fake, runtime, seen } = await attached()
    fake.kill()
    assert.deepEqual(seen, { disposed: 0, detached: 1 })
    assert.equal(runtime.active, true)
    assert.equal(runtime.attached, false)
  })

  it('a deliberate release is not a detach', async () => {
    const { runtime, seen } = await attached()
    await runtime.releaseChannel()
    assert.equal(seen.detached, 0)
    assert.equal(runtime.attached, false)
  })

  it('cancel() disposes and never resurrects', async () => {
    const { fake, runtime, seen } = await attached()
    await runtime.cancel({ drainMs: 0 })
    fake.kill()
    assert.deepEqual(seen, { disposed: 1, detached: 0 })
    assert.equal(runtime.active, false)
  })

  it('a broker cancel logs consumer_stopped and hands off too', async () => {
    const logged: ConeyLogEvent[] = []
    const { runtime, seen } = await attached((event) => logged.push(event))
    runtime.deliver(null) // amqplib's `null` delivery IS the basic.cancel
    assert.equal(seen.detached, 1)
    const stopped = logged.filter((event) => event.event === 'coney.consumer_stopped')
    assert.equal(stopped.length, 1)
    assert.equal(stopped[0]?.queue, 'probe.queue')
  })
})

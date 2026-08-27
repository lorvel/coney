import { describe, it } from 'node:test'
import assert from 'node:assert/strict'
import { defsEqual } from '../src/defs/define.js'
import { defineQueue, defineTopic } from '../src/defs/define.js'
import { TopologyManager } from '../src/topology/manager.js'
import { ConeyConfigError, ConeyTopologyError } from '../src/core/errors.js'
import type { ConeyLogEvent } from '../src/core/log.js'
import type { RetryPolicy } from '../src/defs/retry.js'
import { FakeChannel, amqpError, fakeFactory, fakeManager } from './fakes.js'

const schema = { parse: (input: unknown) => input as { id: string } }

const NO_RETRY: RetryPolicy = { delays: [] }

function makeTopology(args: {
  channel: FakeChannel
  prefix?: string
  defaultRetry?: RetryPolicy
  emit?: (event: ConeyLogEvent) => void
  onCreate?: () => void
}): TopologyManager {
  return new TopologyManager({
    prefix: args.prefix,
    defaultRetry: args.defaultRetry ?? NO_RETRY,
    manager: fakeManager({
      createChannel: async () => {
        args.onCreate?.()
        return args.channel.asChannel()
      },
    }),
    emit: args.emit ?? ((): void => {}),
  })
}

describe('defsEqual', () => {
  it('equal by value: same name, schema reference, ladder and bindings', () => {
    const topic = defineTopic({ name: 'events' })
    const a = defineQueue({
      name: 'jobs',
      schema,
      retry: { delays: [10, 20] },
      bindings: [{ topic, patterns: ['a.*'] }],
    })
    const b = defineQueue({
      name: 'jobs',
      schema,
      retry: { delays: [10, 20] },
      bindings: [{ topic, patterns: ['a.*'] }],
    })
    assert.ok(defsEqual(a, b))
    assert.ok(defsEqual(a, a))
  })

  it('different ladders, schemas, bindings or kinds are not interchangeable', () => {
    const base = defineQueue({ name: 'jobs', retry: { delays: [10] } })
    assert.ok(!defsEqual(base, defineQueue({ name: 'jobs', retry: { delays: [10, 20] } })))
    assert.ok(!defsEqual(base, defineQueue({ name: 'jobs', retry: false })))
    assert.ok(!defsEqual(base, defineQueue({ name: 'jobs' })))
    assert.ok(!defsEqual(base, defineQueue({ name: 'jobs', retry: { delays: [10] }, schema })))
    assert.ok(!defsEqual(base, defineTopic({ name: 'jobs' })))
    const topic = defineTopic({ name: 'events' })
    const bound = defineQueue({
      name: 'jobs',
      retry: { delays: [10] },
      bindings: [{ topic, patterns: ['a.*'] }],
    })
    assert.ok(!defsEqual(base, bound))
    assert.ok(
      !defsEqual(
        bound,
        defineQueue({
          name: 'jobs',
          retry: { delays: [10] },
          bindings: [{ topic, patterns: ['b.*'] }],
        }),
      ),
    )
  })

  it('undefined retry (client default) differs from an explicit ladder', () => {
    assert.ok(
      !defsEqual(
        defineQueue({ name: 'jobs' }),
        defineQueue({ name: 'jobs', retry: { delays: [10_000] } }),
      ),
    )
  })
})

describe('TopologyManager.ensure', () => {
  it('asserts a queue, its dead queue and its buckets exactly once for N concurrent callers', async () => {
    const channel = new FakeChannel()
    let created = 0
    const topology = makeTopology({
      channel,
      defaultRetry: { delays: [10_000] },
      onCreate: () => {
        created += 1
      },
    })
    const jobs = defineQueue({ name: 'jobs' })
    await Promise.all([topology.ensure(jobs), topology.ensure(jobs), topology.ensure(jobs)])
    assert.equal(created, 1, 'the in-flight assert is shared')
    assert.deepEqual(channel.calls, [
      'assertQueue:jobs',
      'assertQueue:jobs.dead',
      'assertExchange:retry.10s',
      'assertQueue:retry.10s',
      'bindQueue:retry.10s|retry.10s|',
    ])
    await topology.ensure(jobs)
    assert.equal(created, 1, 'an asserted def never touches the broker again')
  })

  it('asserts the bound topic and one binding per pattern', async () => {
    const channel = new FakeChannel()
    const topology = makeTopology({ channel, prefix: 'test' })
    const events = defineTopic({ name: 'events' })
    const notify = defineQueue({
      name: 'notify',
      retry: false,
      bindings: [{ topic: events, patterns: ['task.*', 'ku.#'] }],
    })
    await topology.ensure(notify)
    assert.deepEqual(channel.calls, [
      'assertQueue:test.notify',
      'assertQueue:test.notify.dead',
      'assertExchange:test.events',
      'bindQueue:test.notify|test.events|task.*',
      'bindQueue:test.notify|test.events|ku.#',
    ])
  })

  it('rejects two different defs sharing a name', () => {
    const topology = makeTopology({ channel: new FakeChannel() })
    topology.register(defineQueue({ name: 'jobs', retry: { delays: [10] } }))
    assert.throws(
      () => topology.register(defineQueue({ name: 'jobs', retry: { delays: [20] } })),
      ConeyConfigError,
    )
  })
})

describe('TopologyManager.replayAll: one bad def must not take the connection down', () => {
  it('never rejects, logs the failure and caches it for that def only', async () => {
    const channel = new FakeChannel()
    channel.failures.set('assertQueue:test.bad', amqpError('PRECONDITION_FAILED - x-args', 406))
    const logged: ConeyLogEvent[] = []
    const topology = makeTopology({ channel, prefix: 'test', emit: (event) => logged.push(event) })
    const bad = defineQueue({ name: 'bad', retry: false })
    const good = defineQueue({ name: 'good', retry: false })
    topology.register(bad)
    topology.register(good)

    // The iron law (spec §4): amqplib treats a rejected `setup` as a failed
    // connection and would retry forever.
    await topology.replayAll(fakeFactory({ createChannel: async () => channel.asChannel() }))

    const failures = logged.filter((event) => event.event === 'coney.topology_failed')
    assert.equal(failures.length, 1)
    assert.deepEqual(
      { target: failures[0]?.target, amqpCode: failures[0]?.amqpCode },
      { target: 'test.bad', amqpCode: 406 },
    )
    // The good def came up on the replacement channel and needs no further assert.
    await topology.ensure(good)
    const err = await topology.ensure(bad).then(
      () => null,
      (reason: unknown) => reason,
    )
    assert.ok(err instanceof ConeyTopologyError)
    assert.equal(err.target, 'test.bad')
    assert.equal(err.amqpCode, 406)
  })
})

describe('TopologyManager.reassert', () => {
  const topic = defineTopic({ name: 'events' })
  const bound = defineQueue({
    name: 'jobs',
    retry: { delays: [10_000] },
    bindings: [{ topic, patterns: ['a.*'] }],
  })

  it('redeclares the buckets and bound exchanges too, not just the queue', async () => {
    const channel = new FakeChannel()
    const topology = makeTopology({ channel, prefix: 'test' })
    await topology.ensure(bound)
    const first = [...channel.calls]
    channel.calls.length = 0

    await topology.reassert(bound)

    // The whole point: `ensure` would have returned on the `asserted` cache, and a
    // partial invalidation would redeclare the queue while the ladder and the bound
    // exchange — which the same operator action usually removed — stay missing.
    assert.deepEqual(channel.calls, first)
    assert.ok(channel.calls.includes('assertQueue:test.retry.10s'))
    assert.ok(channel.calls.includes('assertExchange:test.retry.10s'))
    assert.ok(channel.calls.includes('assertExchange:test.events'))
    assert.ok(channel.calls.includes('bindQueue:test.jobs|test.events|a.*'))
  })

  it('never caches its failure: a publisher on the same def is unaffected', async () => {
    const channel = new FakeChannel()
    const topology = makeTopology({ channel, prefix: 'test', defaultRetry: NO_RETRY })
    const def = defineQueue({ name: 'jobs', retry: false })
    await topology.ensure(def)

    // The main queue still declares; it is `<q>.dead` that an operator recreated
    // with different arguments — so a publish through the default exchange is fine.
    channel.failures.set(
      'assertQueue:test.jobs.dead',
      amqpError('PRECONDITION_FAILED - inequivalent arg x-message-ttl', 406),
    )
    const err = await topology.reassert(def).then(
      () => null,
      (reason: unknown) => reason,
    )
    assert.ok(err instanceof ConeyTopologyError)

    // `ensure`'s fail-fast cache is for callers who asked for this def. A background
    // re-attach must not make an unrelated publish start rejecting.
    channel.failures.clear()
    await topology.ensure(def)
    assert.ok(channel.calls.includes('assertQueue:test.jobs'))
  })

  it('reports its failure once per call, so a retry loop is not silent', async () => {
    const channel = new FakeChannel()
    const logged: ConeyLogEvent[] = []
    const topology = makeTopology({
      channel,
      prefix: 'test',
      emit: (event) => logged.push(event),
    })
    const def = defineQueue({ name: 'jobs', retry: false })
    channel.failures.set('assertQueue:test.jobs', amqpError('NOT_FOUND - no queue', 404))
    await topology.reassert(def).catch(() => {})
    await topology.reassert(def).catch(() => {})
    assert.equal(logged.filter((event) => event.event === 'coney.topology_failed').length, 2)
  })
})

describe('TopologyManager.inspectRetry', () => {
  it('reports the depth of every known bucket', async () => {
    const channel = new FakeChannel()
    channel.depths.set('test.retry.10s', 4)
    const topology = makeTopology({ channel, prefix: 'test', defaultRetry: { delays: [10_000] } })
    topology.register(defineQueue({ name: 'jobs' }))
    assert.deepEqual(await topology.inspectRetry(), { '10s': 4 })
    assert.ok(channel.closed)
  })

  it('maps a bucket someone else declared differently onto ConeyTopologyError', async () => {
    const channel = new FakeChannel()
    channel.failures.set(
      'assertQueue:test.retry.10s',
      amqpError('PRECONDITION_FAILED - inequivalent arg x-message-ttl', 406),
    )
    const topology = makeTopology({ channel, prefix: 'test', defaultRetry: { delays: [10_000] } })
    topology.register(defineQueue({ name: 'jobs' }))
    const err = await topology.inspectRetry().then(
      () => null,
      (reason: unknown) => reason,
    )
    assert.ok(err instanceof ConeyTopologyError, `expected TOPOLOGY, got ${String(err)}`)
    assert.equal(err.code, 'TOPOLOGY')
    assert.equal(err.target, 'test.retry.10s')
    assert.equal(err.amqpCode, 406)
  })

  it('wraps a failing checkQueue too', async () => {
    const channel = new FakeChannel()
    channel.failures.set('checkQueue:retry.500ms', amqpError('NOT_FOUND', 404))
    const topology = makeTopology({ channel, defaultRetry: { delays: [500] } })
    topology.register(defineQueue({ name: 'jobs' }))
    await assert.rejects(topology.inspectRetry(), ConeyTopologyError)
  })

  it('is a no-op without buckets', async () => {
    const channel = new FakeChannel()
    const topology = makeTopology({ channel })
    topology.register(defineQueue({ name: 'jobs', retry: false }))
    assert.deepEqual(await topology.inspectRetry(), {})
    assert.deepEqual(channel.calls, [])
  })
})

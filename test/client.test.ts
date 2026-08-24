import { describe, it } from 'node:test'
import assert from 'node:assert/strict'
import { createConey } from '../src/client/coney.js'
import { defineQueue, defineTopic } from '../src/defs/define.js'
import { ConeyClosedError, ConeyConfigError, ConeySchemaError } from '../src/core/errors.js'

const URL_OK = 'amqp://user:pw@localhost:5672/vhost'
/** No I/O in this file: every client stays `idle` (or is closed while idle). */
const makeClient = () => createConey({ url: URL_OK, prefix: 'lorvel', name: 'tester', log: false })

describe('createConey validation', () => {
  const throwsConfig = (fn: () => unknown, match: RegExp) => {
    assert.throws(fn, (err: unknown) => err instanceof ConeyConfigError && match.test((err as Error).message))
  }

  it('rejects bad urls synchronously', () => {
    throwsConfig(() => createConey({ url: '' }), /non-empty/)
    throwsConfig(() => createConey({ url: 'not a url' }), /not a valid URL/)
    throwsConfig(() => createConey({ url: 'http://host' }), /amqp:\/\/ or amqps:\/\//)
  })

  it('validates prefix, name, reconnect, publishTimeoutMs and retry', () => {
    throwsConfig(() => createConey({ url: URL_OK, prefix: 'has space' }), /whitespace/)
    throwsConfig(() => createConey({ url: URL_OK, prefix: 'retry.x' }), /reserved prefix/)
    throwsConfig(() => createConey({ url: URL_OK, name: '' }), /name/)
    throwsConfig(() => createConey({ url: URL_OK, reconnect: { factor: 0.5 } }), /factor/)
    throwsConfig(() => createConey({ url: URL_OK, reconnect: { jitter: 2 } }), /jitter/)
    throwsConfig(() => createConey({ url: URL_OK, reconnect: { initialDelayMs: -1 } }), /initialDelayMs/)
    throwsConfig(() => createConey({ url: URL_OK, reconnect: { maxDelayMs: 1 } }), /maxDelayMs/) // < initialDelayMs default
    throwsConfig(() => createConey({ url: URL_OK, publishTimeoutMs: -1 }), /publishTimeoutMs/)
    throwsConfig(() => createConey({ url: URL_OK, retry: { delays: [0] } }), /positive integers/)
  })

  it('rejects Infinity and beyond-setTimeout timeouts everywhere (Node clamps them to 1 ms)', async () => {
    throwsConfig(() => createConey({ url: URL_OK, publishTimeoutMs: Infinity }), /publishTimeoutMs/)
    throwsConfig(() => createConey({ url: URL_OK, publishTimeoutMs: 2 ** 31 }), /publishTimeoutMs/)
    const mq = createConey({ url: URL_OK, log: false })
    await assert.rejects(mq.open({ timeoutMs: Infinity }), ConeyConfigError)
    await assert.rejects(mq.open({ timeoutMs: -5 }), ConeyConfigError)
    const jobs = mq.queue(defineQueue({ name: 'jobs' }))
    await assert.rejects(jobs.consume(() => {}, { timeoutMs: Infinity }), ConeyConfigError)
    await assert.rejects(jobs.publish({ x: 1 }, { timeoutMs: Infinity }), ConeyConfigError)
    await assert.rejects(mq.close({ drainMs: Infinity }), ConeyConfigError)
    assert.equal(mq.state, 'idle') // every rejection happened before any I/O
  })

  it('accepts the kitchen sink without any I/O', () => {
    const mq = createConey({
      url: URL_OK,
      prefix: 'lorvel',
      name: 'worker',
      log: false,
      reconnect: { initialDelayMs: 100, maxAttempts: 3 },
      publishTimeoutMs: 0,
      retry: { delays: [1000] },
      socketOptions: { timeout: 5000 },
    })
    assert.equal(mq.state, 'idle')
    assert.equal(mq.prefix, 'lorvel')
  })
})

describe('resolveName', () => {
  it('applies the prefix (or none) and validates the name', () => {
    assert.equal(makeClient().resolveName('ku.embed'), 'lorvel.ku.embed')
    assert.equal(createConey({ url: URL_OK, log: false }).resolveName('ku.embed'), 'ku.embed')
    assert.throws(() => makeClient().resolveName('amq.x'), ConeyConfigError)
  })
})

describe('handles', () => {
  it('caches per wire name: same or value-equal defs share one handle', () => {
    const mq = makeClient()
    const def = defineQueue({ name: 'jobs', retry: { delays: [10] } })
    const twin = defineQueue({ name: 'jobs', retry: { delays: [10] } })
    const handle = mq.queue(def)
    assert.equal(handle.name, 'lorvel.jobs')
    assert.equal(handle.def, def)
    assert.equal(mq.queue(def), handle)
    assert.equal(mq.queue(twin), handle)
  })

  it('rejects a different def under the same name, and defs of the wrong kind', () => {
    const mq = makeClient()
    mq.queue(defineQueue({ name: 'jobs', retry: { delays: [10] } }))
    assert.throws(
      () => mq.queue(defineQueue({ name: 'jobs', retry: { delays: [10, 20] } })),
      (err: unknown) => err instanceof ConeyConfigError && /share the name/.test((err as Error).message),
    )
    assert.throws(() => mq.queue(defineTopic({ name: 'events' }) as never), /defineQueue/)
    assert.throws(() => mq.topic(defineQueue({ name: 'jobs2' }) as never), /defineTopic/)
  })

  it('a queue and a topic cannot share a wire name either', () => {
    const mq = makeClient()
    mq.queue(defineQueue({ name: 'clash' }))
    assert.throws(() => mq.topic(defineTopic({ name: 'clash' })), ConeyConfigError)
  })

  it('consume/subscribe/ops validate their arguments without any I/O', async () => {
    const mq = makeClient()
    const queue = mq.queue(defineQueue({ name: 'jobs' }))
    const topic = mq.topic(defineTopic({ name: 'events' }))
    await assert.rejects(queue.consume('nope' as never), ConeyConfigError)
    await assert.rejects(queue.consume(() => {}, { prefetch: 0 }), ConeyConfigError)
    await assert.rejects(queue.consume(() => {}, { timeoutMs: -1 }), ConeyConfigError)
    await assert.rejects(topic.subscribe([], () => {}), ConeyConfigError)
    await assert.rejects(topic.subscribe([''], () => {}), ConeyConfigError)
    await assert.rejects(queue.replayDead({ limit: -1 }), ConeyConfigError)
    assert.equal(mq.state, 'idle') // nothing above may start the connection
  })

  it('consume/subscribe/ops after close reject with ConeyClosedError', async () => {
    const mq = makeClient()
    const queue = mq.queue(defineQueue({ name: 'jobs' }))
    const topic = mq.topic(defineTopic({ name: 'events' }))
    await mq.close()
    await assert.rejects(queue.consume(() => {}), ConeyClosedError)
    await assert.rejects(topic.subscribe(['a.*'], () => {}), ConeyClosedError)
    await assert.rejects(queue.inspect(), ConeyClosedError)
    await assert.rejects(queue.replayDead(), ConeyClosedError)
    await assert.rejects(queue.purgeDead(), ConeyClosedError)
  })
})

describe('publish-side gates that never touch the network', () => {
  it('schema failures throw ConeySchemaError with the schema error as cause, before any I/O', async () => {
    const mq = makeClient()
    const schemaError = new Error('id must be a string')
    const def = defineQueue<{ id: string }>({
      name: 'jobs',
      schema: {
        parse: (input) => {
          if (typeof (input as { id?: unknown }).id !== 'string') throw schemaError
          return input as { id: string }
        },
      },
    })
    await assert.rejects(
      mq.queue(def).publish({ id: 42 as never }),
      (err: unknown) => err instanceof ConeySchemaError && err.cause === schemaError,
    )
    assert.equal(mq.state, 'idle') // the failed publish never started the connection
  })

  it('reserved x- headers and non-JSON bodies fail before any I/O', async () => {
    const mq = makeClient()
    const jobs = mq.queue(defineQueue({ name: 'jobs' }))
    await assert.rejects(jobs.publish({ ok: 1 }, { headers: { 'x-nope': 'v' } }), ConeyConfigError)
    await assert.rejects(jobs.publish(undefined), ConeyConfigError)
    const events = mq.topic(defineTopic({ name: 'events' }))
    await assert.rejects(events.publish('', { ok: 1 }), ConeyConfigError)
    assert.equal(mq.state, 'idle')
  })
})

describe('close() on a client that never connected', () => {
  it('resolves fully drained without any I/O and is idempotent', async () => {
    const mq = makeClient()
    const result = await mq.close()
    assert.deepEqual(result, { drained: true, inFlight: 0 })
    assert.equal(mq.state, 'closed')
    assert.equal(await mq.close(), result)
  })

  it('open/publish after close reject with ConeyClosedError', async () => {
    const mq = makeClient()
    const jobs = mq.queue(defineQueue({ name: 'jobs' }))
    await mq.close()
    await assert.rejects(mq.open(), ConeyClosedError)
    await assert.rejects(jobs.publish({ any: 1 }), ConeyClosedError)
    await assert.rejects(mq.declare([]), ConeyClosedError)
    await assert.rejects(mq.inspectRetry(), ConeyClosedError)
  })
})

describe('inspectRetry without buckets', () => {
  it('resolves an empty record without connecting', async () => {
    const mq = makeClient()
    assert.deepEqual(await mq.inspectRetry(), {})
    assert.equal(mq.state, 'idle')
  })
})

import { describe, it } from 'node:test'
import assert from 'node:assert/strict'
import { decodeJson, encodeMessage } from '../src/core/codec.js'
import { ConeyConfigError } from '../src/core/errors.js'

const NOW = new Date('2026-08-24T10:00:00.123Z')

describe('encodeMessage', () => {
  it('encodes the body as plain UTF-8 JSON (no envelope) and round-trips', () => {
    const body = { kuId: 'abc', note: 'tiếng Việt ✓' }
    const { content } = encodeMessage({
      body,
      type: 'ku.embed',
      appId: undefined,
      options: undefined,
      now: NOW,
    })
    assert.deepEqual(decodeJson(content), body)
    assert.deepEqual(JSON.parse(content.toString('utf8')), body)
  })

  it('sets the AMQP properties of the spec §3 table', () => {
    const { properties } = encodeMessage({
      body: 1,
      type: 'ku.embed',
      appId: 'worker',
      options: undefined,
      now: NOW,
    })
    assert.match(
      properties.messageId,
      /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/,
    )
    assert.equal(properties.timestamp, Math.floor(NOW.getTime() / 1000))
    assert.equal(properties.type, 'ku.embed')
    assert.equal(properties.appId, 'worker')
    assert.equal(properties.deliveryMode, 2)
    assert.equal(properties.contentType, 'application/json')
    assert.equal(properties.headers['x-published-at'], NOW.toISOString())
  })

  it('omits appId when the client has no name', () => {
    const { properties } = encodeMessage({
      body: 1,
      type: 't',
      appId: undefined,
      options: undefined,
      now: NOW,
    })
    assert.ok(!('appId' in properties))
  })

  it('respects an explicit messageId and persistent: false', () => {
    const { properties } = encodeMessage({
      body: 1,
      type: 't',
      appId: undefined,
      options: { messageId: 'fixed-id', persistent: false },
      now: NOW,
    })
    assert.equal(properties.messageId, 'fixed-id')
    assert.equal(properties.deliveryMode, 1)
  })

  it('keeps user headers and rejects reserved x- keys', () => {
    const { properties } = encodeMessage({
      body: 1,
      type: 't',
      appId: undefined,
      options: { headers: { tenant: 'a', count: 2, flag: true, nothing: null } },
      now: NOW,
    })
    assert.equal(properties.headers['tenant'], 'a')
    assert.equal(properties.headers['count'], 2)
    assert.equal(properties.headers['flag'], true)
    assert.equal(properties.headers['nothing'], null)
    assert.throws(
      () =>
        encodeMessage({
          body: 1,
          type: 't',
          appId: undefined,
          options: { headers: { 'x-mine': 'v' } },
          now: NOW,
        }),
      (err: unknown) =>
        err instanceof ConeyConfigError && /reserved "x-" prefix/.test((err as Error).message),
    )
  })

  it('rejects bodies JSON cannot represent', () => {
    assert.throws(
      () =>
        encodeMessage({
          body: undefined,
          type: 't',
          appId: undefined,
          options: undefined,
          now: NOW,
        }),
      ConeyConfigError,
    )
    assert.throws(
      () =>
        encodeMessage({ body: () => 1, type: 't', appId: undefined, options: undefined, now: NOW }),
      ConeyConfigError,
    )
  })

  it('maps stringify THROWS (circular, BigInt, throwing toJSON) to ConeyConfigError too', () => {
    const circular: Record<string, unknown> = {}
    circular['self'] = circular
    for (const body of [
      circular,
      { n: 1n },
      {
        toJSON: () => {
          throw new Error('boom from toJSON')
        },
      },
    ]) {
      try {
        encodeMessage({ body, type: 't', appId: undefined, options: undefined, now: NOW })
        assert.fail('expected a throw')
      } catch (err) {
        // A raw TypeError must never escape the library's error taxonomy.
        assert.ok(err instanceof ConeyConfigError, `got ${String(err)}`)
        assert.equal((err as ConeyConfigError).code, 'CONFIG')
        assert.notEqual((err as ConeyConfigError).cause, undefined)
      }
    }
  })
})

describe('decodeJson', () => {
  it('throws SyntaxError on invalid JSON', () => {
    assert.throws(() => decodeJson(Buffer.from('{oops', 'utf8')), SyntaxError)
  })
})

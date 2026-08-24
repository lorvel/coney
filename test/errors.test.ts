import { describe, it } from 'node:test'
import assert from 'node:assert/strict'
import {
  ConeyError,
  ConeyUnavailableError,
  ConeyClosedError,
  ConeyPublishError,
  ConeySchemaError,
  ConeyTopologyError,
  ConeyConfigError,
  UnrecoverableError,
} from '../src/core/errors.js'

describe('errors', () => {
  it('every subclass carries its code, name and the base classes', () => {
    const cases = [
      [new ConeyUnavailableError('u'), 'UNAVAILABLE', 'ConeyUnavailableError'],
      [new ConeyClosedError('c'), 'CLOSED', 'ConeyClosedError'],
      [new ConeyPublishError('p'), 'PUBLISH', 'ConeyPublishError'],
      [new ConeySchemaError('s'), 'SCHEMA', 'ConeySchemaError'],
      [new ConeyTopologyError('t', { target: 'q' }), 'TOPOLOGY', 'ConeyTopologyError'],
      [new ConeyConfigError('k'), 'CONFIG', 'ConeyConfigError'],
    ] as const
    for (const [err, code, name] of cases) {
      assert.ok(err instanceof ConeyError)
      assert.ok(err instanceof Error)
      assert.equal(err.code, code)
      assert.equal(err.name, name)
    }
  })

  it('codes are distinct so switch(err.code) can replace instanceof', () => {
    const codes = [
      new ConeyUnavailableError('u').code,
      new ConeyClosedError('c').code,
      new ConeyPublishError('p').code,
      new ConeySchemaError('s').code,
      new ConeyTopologyError('t', { target: 'q' }).code,
      new ConeyConfigError('k').code,
    ]
    assert.equal(new Set(codes).size, codes.length)
  })

  it('ConeySchemaError exposes the schema error as cause', () => {
    const cause = new Error('bad field')
    const err = new ConeySchemaError('schema failed', { cause })
    assert.equal(err.cause, cause)
  })

  it('ConeyTopologyError carries target and optional amqpCode', () => {
    const bare = new ConeyTopologyError('no code', { target: 'lorvel.q' })
    assert.equal(bare.target, 'lorvel.q')
    assert.equal(bare.amqpCode, undefined)
    const cause = new Error('PRECONDITION_FAILED')
    const full = new ConeyTopologyError('conflict', { target: 'lorvel.q', amqpCode: 406, cause })
    assert.equal(full.amqpCode, 406)
    assert.equal(full.cause, cause)
  })

  it('UnrecoverableError is a plain Error, not a ConeyError', () => {
    const err = new UnrecoverableError('give up', { cause: 'row gone' })
    assert.ok(err instanceof Error)
    assert.ok(!(err instanceof ConeyError))
    assert.equal(err.name, 'UnrecoverableError')
    assert.equal(err.cause, 'row gone')
  })

  it('base ConeyError keeps an arbitrary code and message', () => {
    const err = new ConeyError('PUBLISH', 'boom', { cause: 1 })
    assert.equal(err.code, 'PUBLISH')
    assert.equal(err.message, 'boom')
    assert.equal(err.name, 'ConeyError')
  })
})

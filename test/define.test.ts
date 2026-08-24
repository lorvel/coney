import { describe, it } from 'node:test'
import assert from 'node:assert/strict'
import { defineQueue, defineTopic, validateName, wireName } from '../src/define.js'
import { ConeyConfigError } from '../src/errors.js'

const throwsConfig = (fn: () => unknown, match?: RegExp) => {
  assert.throws(
    fn,
    (err: unknown) => err instanceof ConeyConfigError && (match === undefined || match.test((err as Error).message)),
  )
}

describe('validateName / wireName', () => {
  it('accepts dotted lowercase names and preserves case', () => {
    assert.equal(validateName('ku.embed', 'queue name'), 'ku.embed')
    assert.equal(validateName('MiXeD.Case', 'queue name'), 'MiXeD.Case')
  })

  it('rejects empty, whitespace, control characters and oversized names', () => {
    throwsConfig(() => validateName('', 'queue name'), /non-empty/)
    throwsConfig(() => validateName('has space', 'queue name'), /whitespace/)
    throwsConfig(() => validateName('tab' + String.fromCharCode(9) + 'x', 'queue name'), /whitespace or control/)
    throwsConfig(() => validateName('nul' + String.fromCharCode(0) + 'x', 'queue name'), /whitespace or control/)
    throwsConfig(() => validateName('del' + String.fromCharCode(127) + 'x', 'queue name'), /whitespace or control/)
    throwsConfig(() => validateName('x'.repeat(201), 'queue name'), /200 bytes/)
    // 67 three-byte chars = 201 bytes but only 67 JS chars: the BYTE length counts
    throwsConfig(() => validateName('ế'.repeat(67), 'queue name'), /200 bytes/)
    assert.equal(validateName('x'.repeat(200), 'queue name'), 'x'.repeat(200))
  })

  it('rejects the reserved prefixes and suffix', () => {
    throwsConfig(() => validateName('amq.direct', 'queue name'), /reserved prefix/)
    throwsConfig(() => validateName('retry.10s', 'queue name'), /reserved prefix/)
    throwsConfig(() => validateName('jobs.dead', 'queue name'), /reserved suffix/)
    assert.equal(validateName('retrying.jobs', 'queue name'), 'retrying.jobs')
    assert.equal(validateName('deadline.jobs', 'queue name'), 'deadline.jobs')
  })

  it('wireName joins the prefix with a dot, or passes through without one', () => {
    assert.equal(wireName('lorvel', 'ku.embed'), 'lorvel.ku.embed')
    assert.equal(wireName(undefined, 'ku.embed'), 'ku.embed')
  })
})

describe('defineQueue', () => {
  it('freezes the def and its nested retry policy', () => {
    const def = defineQueue({ name: 'jobs', retry: { delays: [10, 20] } })
    assert.equal(def.kind, 'queue')
    assert.ok(Object.isFrozen(def))
    assert.ok(Object.isFrozen(def.retry))
    assert.ok(Object.isFrozen((def.retry as { delays: readonly number[] }).delays))
    assert.throws(() => {
      ;(def as { name: string }).name = 'other'
    }, TypeError)
  })

  it('copies the delays so later mutation of the input cannot change the def', () => {
    const delays = [10, 20]
    const def = defineQueue({ name: 'jobs', retry: { delays } })
    delays.push(30)
    assert.deepEqual((def.retry as { delays: readonly number[] }).delays, [10, 20])
  })

  it('rejects invalid delays', () => {
    throwsConfig(() => defineQueue({ name: 'jobs', retry: { delays: [0] } }), /positive integers/)
    throwsConfig(() => defineQueue({ name: 'jobs', retry: { delays: [-5] } }), /positive integers/)
    throwsConfig(() => defineQueue({ name: 'jobs', retry: { delays: [1.5] } }), /positive integers/)
    throwsConfig(() => defineQueue({ name: 'jobs', retry: { delays: [Number.NaN] } }), /positive integers/)
  })

  it('accepts retry: false and an empty ladder', () => {
    assert.equal(defineQueue({ name: 'jobs', retry: false }).retry, false)
    assert.deepEqual(
      (defineQueue({ name: 'jobs', retry: { delays: [] } }).retry as { delays: readonly number[] }).delays,
      [],
    )
  })

  it('validates bindings: topic def required, at least one non-empty pattern', () => {
    const topic = defineTopic({ name: 'events' })
    const bound = defineQueue({ name: 'notify', bindings: [{ topic, patterns: ['task.*'] }] })
    assert.equal(bound.bindings?.[0]?.topic, topic)
    throwsConfig(
      () => defineQueue({ name: 'notify', bindings: [{ topic: { name: 'x' } as never, patterns: ['a'] }] }),
      /defineTopic/,
    )
    throwsConfig(() => defineQueue({ name: 'notify', bindings: [{ topic, patterns: [] }] }), /at least one pattern/)
    throwsConfig(() => defineQueue({ name: 'notify', bindings: [{ topic, patterns: [''] }] }), /empty pattern/)
  })
})

describe('defineTopic', () => {
  it('freezes and tags the def', () => {
    const def = defineTopic({ name: 'events' })
    assert.equal(def.kind, 'topic')
    assert.ok(Object.isFrozen(def))
  })

  it('applies the same name rules', () => {
    throwsConfig(() => defineTopic({ name: 'amq.topic' }), /reserved prefix/)
  })
})

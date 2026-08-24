import { describe, it } from 'node:test'
import assert from 'node:assert/strict'
import { defsEqual } from '../src/topology.js'
import { defineQueue, defineTopic } from '../src/define.js'

const schema = { parse: (input: unknown) => input as { id: string } }

describe('defsEqual', () => {
  it('equal by value: same name, schema reference, ladder and bindings', () => {
    const topic = defineTopic({ name: 'events' })
    const a = defineQueue({ name: 'jobs', schema, retry: { delays: [10, 20] }, bindings: [{ topic, patterns: ['a.*'] }] })
    const b = defineQueue({ name: 'jobs', schema, retry: { delays: [10, 20] }, bindings: [{ topic, patterns: ['a.*'] }] })
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
    const bound = defineQueue({ name: 'jobs', retry: { delays: [10] }, bindings: [{ topic, patterns: ['a.*'] }] })
    assert.ok(!defsEqual(base, bound))
    assert.ok(
      !defsEqual(bound, defineQueue({ name: 'jobs', retry: { delays: [10] }, bindings: [{ topic, patterns: ['b.*'] }] })),
    )
  })

  it('undefined retry (client default) differs from an explicit ladder', () => {
    assert.ok(!defsEqual(defineQueue({ name: 'jobs' }), defineQueue({ name: 'jobs', retry: { delays: [10_000] } })))
  })
})

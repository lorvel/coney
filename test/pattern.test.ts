import assert from 'node:assert/strict'
import { describe, it } from 'node:test'
import { matchTopicPattern } from '../src/mock/pattern.js'

describe('AMQP topic pattern matching (* = one word, # = zero or more)', () => {
  const cases: Array<[pattern: string, key: string, expected: boolean]> = [
    ['task.updated', 'task.updated', true],
    ['task.updated', 'task.created', false],
    ['task.updated', 'task.updated.now', false],
    ['task.*', 'task.updated', true],
    ['task.*', 'task', false],
    ['task.*', 'task.a.b', false],
    ['*.orange.*', 'quick.orange.rabbit', true],
    ['*.orange.*', 'lazy.orange.elephant', true],
    ['*.orange.*', 'quick.brown.fox', false],
    ['*', 'word', true],
    ['*', 'two.words', false],
    ['#', 'anything.at.all', true],
    ['#', 'one', true],
    ['#', '', true],
    ['lazy.#', 'lazy', true],
    ['lazy.#', 'lazy.brown.fox', true],
    ['lazy.#', 'quick.brown.fox', false],
    ['#.fox', 'quick.brown.fox', true],
    ['#.fox', 'fox', true],
    ['#.fox', 'fox.tail', false],
    ['a.#.z', 'a.z', true],
    ['a.#.z', 'a.b.c.z', true],
    ['a.#.z', 'a.b.c', false],
    ['#.#', 'a.b', true],
    ['a.*.#', 'a.b', true],
    ['a.*.#', 'a', false],
  ]
  for (const [pattern, key, expected] of cases) {
    it(`"${pattern}" vs "${key}" -> ${expected}`, () => {
      assert.equal(matchTopicPattern(pattern, key), expected)
    })
  }

  it('stays fast on pathological many-# patterns (memoized, not exponential)', () => {
    const key = Array.from({ length: 40 }, (_, i) => `w${i}`).join('.')
    const start = Date.now()
    // Un-memoized backtracking took ~1.5s on this exact input (and ~45s with 10 #s).
    assert.equal(matchTopicPattern('#.#.#.#.#.#.#.#.x', key), false)
    assert.equal(matchTopicPattern('#.#.#.#.#.#.#.#.#.#.w39', key), true)
    assert.ok(Date.now() - start < 500, `took ${Date.now() - start} ms`)
  })
})

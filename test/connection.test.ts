import { describe, it } from 'node:test'
import assert from 'node:assert/strict'
import { ConeyConfigError } from '../src/core/errors.js'
import {
  DEFAULT_RECONNECT_POLICY,
  backoffDelayMs,
  resolveReconnectPolicy,
  toRecoveryOptions,
} from '../src/connection/policy.js'
import { TIMEOUT_MAX } from '../src/core/time.js'

describe('backoffDelayMs', () => {
  it('follows initialDelayMs * factor^(attempt-1), clamped by maxDelayMs', () => {
    const policy = resolveReconnectPolicy({ initialDelayMs: 100, maxDelayMs: 800, jitter: 0 })
    assert.deepEqual(
      [1, 2, 3, 4, 5].map((attempt) => backoffDelayMs(policy, attempt)),
      [100, 200, 400, 800, 800],
    )
  })

  it('stays a number once the growth term overflows to Infinity', () => {
    // `initialDelayMs: 0` is a legal policy and `factor ** (attempt - 1)` reaches
    // Infinity around attempt 1025, so the product is `0 * Infinity` = NaN — which
    // survives Math.min, the jitter guard and Math.max(0, Math.round(NaN)) alike.
    // `setTimeout(NaN)` then fires after 1 ms with no warning: "back off" inverted
    // into "hammer the broker", and `retryInMs` is typed `number` in the contract.
    const zero = resolveReconnectPolicy({ initialDelayMs: 0, maxDelayMs: 30_000 })
    for (const attempt of [1, 1024, 1025, 2000]) {
      assert.ok(
        Number.isFinite(backoffDelayMs(zero, attempt)),
        `attempt ${attempt} did not produce a finite delay`,
      )
    }
    const grown = resolveReconnectPolicy({ initialDelayMs: 1000, maxDelayMs: 30_000, jitter: 0 })
    assert.equal(backoffDelayMs(grown, 2000), 30_000)
  })

  it('never exceeds TIMEOUT_MAX, which setTimeout would silently clamp to 1 ms', () => {
    // The reconnect policy is validated as "finite and >= 0", never against
    // TIMEOUT_MAX like every other duration — so a legal policy can ask for weeks.
    const policy = resolveReconnectPolicy({
      initialDelayMs: 2_500_000_000,
      maxDelayMs: 3_000_000_000,
    })
    for (let attempt = 1; attempt <= 30; attempt++) {
      assert.ok(backoffDelayMs(policy, attempt) <= TIMEOUT_MAX, `attempt ${attempt} overflowed`)
    }
  })
})

describe('reconnect policy', () => {
  it('spec defaults: 1s → 30s, factor 2, jitter 0.2, no give-up', () => {
    assert.deepEqual(DEFAULT_RECONNECT_POLICY, {
      initialDelayMs: 1000,
      maxDelayMs: 30_000,
      factor: 2,
      jitter: 0.2,
      maxAttempts: Infinity,
    })
  })

  it('maps the public *Ms/maxAttempts names onto amqplib recovery options', () => {
    assert.deepEqual(
      toRecoveryOptions({
        initialDelayMs: 250,
        maxDelayMs: 8_000,
        factor: 3,
        jitter: 0.5,
        maxAttempts: 7,
      }),
      // maxRetries counts the retries AFTER the first attempt (amqplib gives up at
      // `_attempt >= maxRetries` starting from 0), maxAttempts counts attempts —
      // so 7 attempts is 6 retries. Passing it through unchanged let the probe loop
      // and the reconnect loop honor the same policy differently.
      { initialDelay: 250, maxDelay: 8_000, factor: 3, jitter: 0.5, maxRetries: 6 },
    )
    assert.equal(toRecoveryOptions(DEFAULT_RECONNECT_POLICY).maxRetries, Infinity)
  })

  it('never maps below zero: one attempt is one attempt on both loops', () => {
    assert.equal(toRecoveryOptions({ ...DEFAULT_RECONNECT_POLICY, maxAttempts: 1 }).maxRetries, 0)
    assert.equal(toRecoveryOptions({ ...DEFAULT_RECONNECT_POLICY, maxAttempts: 0 }).maxRetries, 0)
  })

  it('rejects a non-finite factor (amqplib substitutes its own default for it)', () => {
    assert.throws(() => resolveReconnectPolicy({ factor: Infinity }), ConeyConfigError)
    assert.throws(() => resolveReconnectPolicy({ factor: NaN }), ConeyConfigError)
    assert.equal(resolveReconnectPolicy({ factor: 1.5 }).factor, 1.5)
  })
})

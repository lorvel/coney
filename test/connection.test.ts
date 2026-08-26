import { describe, it } from 'node:test'
import assert from 'node:assert/strict'
import { ConeyConfigError } from '../src/core/errors.js'
import {
  DEFAULT_RECONNECT_POLICY,
  resolveReconnectPolicy,
  toRecoveryOptions,
} from '../src/connection/policy.js'

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

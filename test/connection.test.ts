import { describe, it } from 'node:test'
import assert from 'node:assert/strict'
import { DEFAULT_RECONNECT_POLICY, toRecoveryOptions } from '../src/connection.js'

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
      toRecoveryOptions({ initialDelayMs: 250, maxDelayMs: 8_000, factor: 3, jitter: 0.5, maxAttempts: 7 }),
      { initialDelay: 250, maxDelay: 8_000, factor: 3, jitter: 0.5, maxRetries: 7 },
    )
    assert.equal(toRecoveryOptions(DEFAULT_RECONNECT_POLICY).maxRetries, Infinity)
  })
})

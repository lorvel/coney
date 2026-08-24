import { describe, it } from 'node:test'
import assert from 'node:assert/strict'
import { bucketLogicalName, durationLabel, effectiveDelays, nextDelayMs } from '../src/retry.js'

describe('durationLabel', () => {
  it('formats largest units first, omitting zero units', () => {
    assert.equal(durationLabel(500), '500ms')
    assert.equal(durationLabel(1_000), '1s')
    assert.equal(durationLabel(10_000), '10s')
    assert.equal(durationLabel(60_000), '1m')
    assert.equal(durationLabel(90_000), '1m30s')
    assert.equal(durationLabel(300_000), '5m')
    assert.equal(durationLabel(3_600_000), '1h')
    assert.equal(durationLabel(9_000_000), '2h30m')
    assert.equal(durationLabel(3_661_001), '1h1m1s1ms')
    assert.equal(durationLabel(1), '1ms')
  })

  it('is total (0 still yields a label, even though delays must be positive)', () => {
    assert.equal(durationLabel(0), '0ms')
  })
})

describe('effectiveDelays', () => {
  const fallback = { delays: [10_000, 60_000] }
  it('false disables the ladder', () => {
    assert.deepEqual(effectiveDelays(false, fallback), [])
  })
  it('undefined uses the client default', () => {
    assert.deepEqual(effectiveDelays(undefined, fallback), [10_000, 60_000])
  })
  it('an explicit policy wins', () => {
    assert.deepEqual(effectiveDelays({ delays: [5] }, fallback), [5])
  })
})

describe('nextDelayMs', () => {
  it('walks the ladder by attempt and returns undefined past the end', () => {
    const delays = [10, 20, 30]
    assert.equal(nextDelayMs(delays, 1), 10)
    assert.equal(nextDelayMs(delays, 2), 20)
    assert.equal(nextDelayMs(delays, 3), 30)
    assert.equal(nextDelayMs(delays, 4), undefined)
    assert.equal(nextDelayMs([], 1), undefined)
  })
})

describe('bucketLogicalName', () => {
  it('uses the reserved retry. prefix plus the duration label', () => {
    assert.equal(bucketLogicalName(10_000), 'retry.10s')
    assert.equal(bucketLogicalName(90_000), 'retry.1m30s')
  })
})

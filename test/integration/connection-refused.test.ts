/**
 * Connect-failure semantics against a CLOSED PORT (127.0.0.1:1) — deterministic,
 * no broker or env needed, so these run everywhere the integration job runs.
 */

import assert from 'node:assert/strict'
import { test } from 'node:test'
import { ConeyUnavailableError, createConey, defineQueue } from '../../src/index.js'
import { eventCollector, waitFor } from './helpers.js'

const DEAD_URL = 'amqp://127.0.0.1:1'

test('open({timeoutMs}) rejects while the reconnect loop keeps running; close() stops it', async () => {
  const mq = createConey({
    url: DEAD_URL,
    reconnect: { initialDelayMs: 50, maxDelayMs: 100, jitter: 0 },
    log: false,
  })
  await assert.rejects(mq.open({ timeoutMs: 300 }), ConeyUnavailableError)
  assert.equal(mq.state, 'connecting') // the loop is still trying
  const result = await mq.close()
  assert.deepEqual(result, { drained: true, inFlight: 0 })
  assert.equal(mq.state, 'closed')
})

test('maxAttempts exhaustion: open() rejects, the client is closed, publishes fail fast', async () => {
  const collector = eventCollector()
  const mq = createConey({
    url: DEAD_URL,
    reconnect: { initialDelayMs: 50, maxDelayMs: 100, jitter: 0, maxAttempts: 2 },
    log: collector.log,
  })
  await assert.rejects(mq.open(), ConeyUnavailableError)
  await waitFor(() => collector.names().includes('coney.reconnect_exhausted'), 'exhausted event', 5000)
  assert.equal(mq.state, 'closed')
  await assert.rejects(mq.queue(defineQueue({ name: 'q' })).publish({ n: 1 }), ConeyUnavailableError)
  await mq.close() // still idempotent and safe
})

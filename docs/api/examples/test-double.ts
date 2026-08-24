// Example 4 — service test suite: one in-memory client replaces the 8 divergent `vi.mock(...)` stubs.
import assert from 'node:assert/strict'
import { createConeyMock, UnrecoverableError } from '@lorvel/coney'
import { EmbedKu, Events } from './defs.js'

const mq = createConeyMock({ prefix: 'lorvel' })

// --- publisher side: assert what the action enqueued -------------------------
async function publishEmbedJob(kuId: string): Promise<void> {
  await mq.queue(EmbedKu).publish({ kuId })
}

await publishEmbedJob('5a7c9b1e-2f3d-4e8a-9b0c-1d2e3f4a5b6c')
assert.equal(mq.published.length, 1)
assert.deepEqual(mq.published[0]?.body, { kuId: '5a7c9b1e-2f3d-4e8a-9b0c-1d2e3f4a5b6c' })
assert.equal(mq.published[0]?.target.name, 'lorvel.ku.embed')
mq.reset()

// --- consumer side: run the real handler without a broker ----------------------
const seen: string[] = []
await mq.queue(EmbedKu).consume(async (msg) => {
  if (msg.body.kuId.startsWith('0000')) throw new UnrecoverableError('gone')
  if (msg.attempt < 2) throw new Error('transient')
  seen.push(msg.body.kuId)
})

const first = await mq.deliver(EmbedKu, { kuId: '5a7c9b1e-2f3d-4e8a-9b0c-1d2e3f4a5b6c' })
assert.equal(first.outcome, 'retry')
if (first.outcome === 'retry') assert.equal(first.delayMs, 10_000)

const second = await mq.deliver(EmbedKu, { kuId: '5a7c9b1e-2f3d-4e8a-9b0c-1d2e3f4a5b6c' }, { attempt: 2 })
assert.equal(second.outcome, 'ack')
assert.deepEqual(seen, ['5a7c9b1e-2f3d-4e8a-9b0c-1d2e3f4a5b6c'])

const poison = await mq.deliver(EmbedKu, { kuId: '00000000-0000-4000-8000-000000000000' })
assert.equal(poison.outcome, 'dead')

// Schema gate runs in the mock too: an invalid body never reaches the handler.
const invalid = await mq.deliver(EmbedKu, { kuId: 'not-a-uuid' })
assert.equal(invalid.outcome, 'dead')

// --- topic side ---------------------------------------------------------------
const received: string[] = []
await mq.topic(Events).subscribe(['task.*'], async (msg) => {
  received.push(`${msg.routingKey}:${msg.body.id}`)
})
const outcomes = await mq.deliverTopic(Events, 'task.updated', {
  entity: 'task', action: 'updated',
  project_id: '5a7c9b1e-2f3d-4e8a-9b0c-1d2e3f4a5b6c', id: '5a7c9b1e-2f3d-4e8a-9b0c-1d2e3f4a5b6d',
})
assert.equal(outcomes.length, 1)
assert.deepEqual(received, ['task.updated:5a7c9b1e-2f3d-4e8a-9b0c-1d2e3f4a5b6d'])

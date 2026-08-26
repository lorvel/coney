import { describe, it } from 'node:test'
import assert from 'node:assert/strict'
import { Publisher } from '../src/publish/publisher.js'
import { encodeMessage, type EncodedMessage } from '../src/core/codec.js'
import { ConeyPublishError, ConeyUnavailableError } from '../src/core/errors.js'
import { FakeConfirmChannel, fakeFactory, fakeManager, tick } from './fakes.js'

const encoded = (): EncodedMessage =>
  encodeMessage({
    body: { n: 1 },
    type: 'jobs',
    appId: undefined,
    options: undefined,
    now: new Date(),
  })

const send = (publisher: Publisher, timeoutMs: number): Promise<void> =>
  publisher.send({ exchange: '', routingKey: 'jobs', encoded: encoded(), timeoutMs })

/** A publisher already holding `channel`, exactly as the connection `setup` leaves it. */
async function attached(
  channel: FakeConfirmChannel,
  spare?: FakeConfirmChannel,
): Promise<Publisher> {
  const lazy = spare ?? channel
  const publisher = new Publisher(
    fakeManager({ createConfirmChannel: async () => lazy.asChannel() }),
  )
  await publisher.onSetup(fakeFactory({ createConfirmChannel: async () => channel.asChannel() }))
  return publisher
}

describe('publisher: the deadline covers the confirm', () => {
  it('a broker that accepts the frame and never confirms fails inside the budget', async () => {
    const channel = new FakeConfirmChannel()
    const publisher = await attached(channel)
    const sent = send(publisher, 30)
    await assert.rejects(
      sent,
      (err: unknown) =>
        err instanceof ConeyUnavailableError && /did not confirm the publish/.test(err.message),
    )
    // The frame WAS written — this is the blocked-connection case (a resource alarm
    // stops confirms without dropping the connection), not a refusal.
    assert.equal(channel.published.length, 1)
  })

  it('close() can still settle: an unconfirmed publish leaves nothing in flight', async () => {
    const channel = new FakeConfirmChannel()
    const publisher = await attached(channel)
    const sent = send(publisher, 20)
    await assert.rejects(sent, ConeyUnavailableError)
    // settleAll() is what close() awaits; before the confirm had a deadline this
    // loop never terminated against a broker that stopped confirming.
    await publisher.settleAll()
  })

  it('a spent budget writes nothing at all — no "did it go out?" ambiguity', async () => {
    const channel = new FakeConfirmChannel()
    const publisher = await attached(channel)
    await assert.rejects(send(publisher, 0), ConeyUnavailableError)
    assert.equal(channel.published.length, 0)
  })

  it('resolves on the broker ack', async () => {
    const channel = new FakeConfirmChannel()
    const publisher = await attached(channel)
    const sent = send(publisher, 1000)
    await tick()
    channel.ack()
    await sent
    assert.equal(channel.published[0]?.routingKey, 'jobs')
  })
})

describe('publisher: a lost channel is not a nack', () => {
  it('a channel death rejects in-flight publishes as UNAVAILABLE, not PUBLISH', async () => {
    const channel = new FakeConfirmChannel()
    const publisher = await attached(channel)
    const sent = send(publisher, 5000)
    await tick()
    assert.equal(channel.inFlight, 1)
    // amqplib's own close listener flushes the unconfirmed callback with
    // Error('channel closed') BEFORE coney's listener runs — the publisher must
    // not read that as a broker refusal (spec §4: the caller may retry).
    channel.kill()
    const err = await sent.then(
      () => null,
      (reason: unknown) => reason,
    )
    assert.ok(err instanceof ConeyUnavailableError, `expected UNAVAILABLE, got ${String(err)}`)
    assert.equal((err as ConeyUnavailableError).code, 'UNAVAILABLE')
  })

  it('a real nack is still a permanent PUBLISH error', async () => {
    const channel = new FakeConfirmChannel()
    const publisher = await attached(channel)
    const sent = send(publisher, 5000)
    await tick()
    channel.nack()
    const err = await sent.then(
      () => null,
      (reason: unknown) => reason,
    )
    assert.ok(err instanceof ConeyPublishError, `expected PUBLISH, got ${String(err)}`)
    assert.equal((err as ConeyPublishError).code, 'PUBLISH')
  })

  it('a synchronous write failure on a dead channel is UNAVAILABLE', async () => {
    const channel = new FakeConfirmChannel()
    const publisher = await attached(channel)
    channel.writeError = new Error('IllegalOperationError: Channel closed')
    await assert.rejects(send(publisher, 1000), ConeyUnavailableError)
  })
})

describe('publisher: back-pressure', () => {
  it('gates the next publish until drain, then confirms it', async () => {
    const channel = new FakeConfirmChannel()
    const publisher = await attached(channel)
    channel.blockWrites = true
    const first = send(publisher, 5000)
    await tick()
    assert.equal(channel.published.length, 1)
    const second = send(publisher, 5000)
    await tick()
    assert.equal(channel.published.length, 1) // parked on the gate
    channel.releaseDrain()
    await tick()
    assert.equal(channel.published.length, 2)
    channel.ack()
    channel.ack()
    await Promise.all([first, second])
  })

  it('the gate is bounded by the caller budget', async () => {
    const channel = new FakeConfirmChannel()
    const publisher = await attached(channel)
    channel.blockWrites = true
    const first = send(publisher, 5000)
    await tick()
    await assert.rejects(
      send(publisher, 25),
      (err: unknown) => err instanceof ConeyUnavailableError && /back-pressured/.test(err.message),
    )
    channel.releaseDrain()
    channel.ack()
    await first
  })

  it('a publish released by the DEATH of the gating channel resumes on the live one', async () => {
    // `close` releases the gate too, so the channel captured before the wait can be
    // the dead one — publishing on it failed a caller whose budget was untouched.
    const dying = new FakeConfirmChannel()
    const fresh = new FakeConfirmChannel()
    const publisher = await attached(dying, fresh)
    dying.blockWrites = true
    const first = send(publisher, 5000)
    await tick()
    const second = send(publisher, 5000)
    await tick()
    assert.equal(dying.published.length, 1)
    dying.kill()
    await assert.rejects(first, ConeyUnavailableError)
    await tick()
    assert.equal(fresh.published.length, 1, 'the parked publish went out on the fresh channel')
    fresh.ack()
    await second
  })
})

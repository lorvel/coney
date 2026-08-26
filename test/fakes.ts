/**
 * Fakes of the amqplib surface the publisher and the topology manager are built
 * on (spec §7 "Unit — no broker; fakes of the amqplib surface").
 *
 * These reproduce amqplib's OBSERVABLE behaviour, including the parts coney has to
 * work around — most importantly the `close` listener the real `ConfirmChannel`
 * registers in its own constructor, which flushes every unconfirmed callback
 * before any listener coney attaches afterwards.
 */

import { EventEmitter } from 'node:events'
import type { Channel, ConfirmChannel } from 'amqplib'
import type { ChannelFactory, ConnectionManager } from '../src/connection/manager.js'

type Confirm = (err: unknown) => void

/** An AMQP channel-level error as amqplib reports it (`err.code` is the reply code). */
export function amqpError(message: string, code: number): Error {
  return Object.assign(new Error(message), { code })
}

export class FakeConfirmChannel extends EventEmitter {
  readonly published: { exchange: string; routingKey: string; content: Buffer }[] = []
  /** While true `publish` reports back-pressure (amqplib buffered the frame). */
  blockWrites = false
  /** When set, `publish` throws it — amqplib's invalidated sender after a channel death. */
  writeError: Error | null = null
  private unconfirmed: Confirm[] = []
  private killed = false

  constructor() {
    super()
    // amqplib/lib/channel.js registers exactly this in the ConfirmChannel
    // constructor, so it ALWAYS runs before a listener coney adds later.
    this.on('close', () => {
      const err = new Error('channel closed')
      const pending = this.unconfirmed
      this.unconfirmed = []
      for (const confirm of pending) confirm(err)
    })
  }

  publish(
    exchange: string,
    routingKey: string,
    content: Buffer,
    _properties: unknown,
    confirm: Confirm,
  ): boolean {
    if (this.writeError !== null) throw this.writeError
    this.published.push({ exchange, routingKey, content })
    this.unconfirmed.push(confirm)
    return !this.blockWrites
  }

  /** Publishes written but not yet confirmed by this fake broker. */
  get inFlight(): number {
    return this.unconfirmed.length
  }

  /** Broker ack for the oldest unconfirmed publish. */
  ack(): void {
    this.unconfirmed.shift()?.(null)
  }

  /** Broker nack — amqplib hands the callback `new Error('message nacked')`. */
  nack(): void {
    this.unconfirmed.shift()?.(new Error('message nacked'))
  }

  /** Back-pressure relieved. */
  releaseDrain(): void {
    this.blockWrites = false
    this.emit('drain')
  }

  /** Channel death: a connection loss, a broker restart or a channel-level error. */
  kill(): void {
    if (this.killed) return
    this.killed = true
    this.emit('close')
  }

  async close(): Promise<void> {
    this.kill()
  }

  asChannel(): ConfirmChannel {
    return this as unknown as ConfirmChannel
  }
}

export class FakeChannel extends EventEmitter {
  /** Every broker call in order, as `"<op>:<name>"`. */
  readonly calls: string[] = []
  /** `"<op>:<name>"` → the error that call throws instead of succeeding. */
  readonly failures = new Map<string, Error>()
  /** Queue wire name → depth reported by `checkQueue`. */
  readonly depths = new Map<string, number>()
  closed = false

  async assertQueue(name: string): Promise<{ queue: string }> {
    this.record(`assertQueue:${name}`)
    return { queue: name }
  }

  async assertExchange(name: string): Promise<{ exchange: string }> {
    this.record(`assertExchange:${name}`)
    return { exchange: name }
  }

  async bindQueue(queue: string, exchange: string, pattern: string): Promise<object> {
    this.record(`bindQueue:${queue}|${exchange}|${pattern}`)
    return {}
  }

  async checkQueue(name: string): Promise<{ messageCount: number; consumerCount: number }> {
    this.record(`checkQueue:${name}`)
    return { messageCount: this.depths.get(name) ?? 0, consumerCount: 0 }
  }

  async purgeQueue(name: string): Promise<{ messageCount: number }> {
    this.record(`purgeQueue:${name}`)
    return { messageCount: this.depths.get(name) ?? 0 }
  }

  async close(): Promise<void> {
    this.closed = true
  }

  private record(call: string): void {
    this.calls.push(call)
    const failure = this.failures.get(call)
    // A failed assert kills the channel on a real broker; callers must not reuse it.
    if (failure !== undefined) throw failure
  }

  asChannel(): Channel {
    return this as unknown as Channel
  }
}

/** A {@link ConnectionManager} that only knows how to hand out the given channels. */
export function fakeManager(channels: {
  createChannel?: () => Promise<Channel>
  createConfirmChannel?: () => Promise<ConfirmChannel>
}): ConnectionManager {
  const reject = async (): Promise<never> => {
    throw new Error('the fake manager was not given this channel kind')
  }
  return {
    createChannel: channels.createChannel ?? reject,
    createConfirmChannel: channels.createConfirmChannel ?? reject,
  } as unknown as ConnectionManager
}

/** The setup-scoped factory `ConnectionManager.onSetup` hands its hook. */
export function fakeFactory(channels: {
  createChannel?: () => Promise<Channel>
  createConfirmChannel?: () => Promise<ConfirmChannel>
}): ChannelFactory {
  return fakeManager(channels) as unknown as ChannelFactory
}

/** Let every already-queued microtask AND timer callback run. */
export function tick(): Promise<void> {
  return new Promise((resolve) => setImmediate(resolve))
}

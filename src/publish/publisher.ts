/**
 * The one shared confirm channel for every publish: broker confirms (resolve on
 * ack, {@link ConeyPublishError} on nack), a bounded wait while disconnected
 * (decision #3 — nothing is ever buffered), and `drain` back-pressure.
 */

import type { ConfirmChannel } from 'amqplib'
import { ConeyPublishError, ConeyUnavailableError } from '../core/errors.js'
import type { EncodedMessage } from '../core/codec.js'
import { raceTimeout } from '../core/time.js'
import type { ChannelFactory, ConnectionManager } from '../connection/manager.js'

interface PendingConfirm {
  reject: (err: Error) => void
}

export class Publisher {
  private channel: ConfirmChannel | null = null
  private channelPromise: Promise<ConfirmChannel> | null = null
  private drainGate: Promise<void> | null = null
  private readonly pending = new Set<PendingConfirm>()
  private readonly inFlight = new Set<Promise<void>>()

  constructor(private readonly manager: ConnectionManager) {}

  /** From the connection's `setup`: (re)create the shared confirm channel eagerly. */
  async onSetup(channels: ChannelFactory): Promise<void> {
    this.channel = null
    this.channelPromise = null
    this.drainGate = null
    try {
      const channel = await channels.createConfirmChannel()
      this.adoptChannel(channel)
    } catch {
      // Practically unreachable on a fresh connection; publishes recover lazily
      // through ensureChannel(). `setup` must never reject (spec §4).
    }
  }

  /** Reject every in-flight confirm — the connection (or the current channel) is gone. */
  rejectPending(err: ConeyUnavailableError): void {
    const entries = [...this.pending]
    this.pending.clear()
    for (const entry of entries) entry.reject(err)
  }

  /** Publish and resolve only on the broker's confirm. Assumes the caller already waited for `open`. */
  async send(args: {
    exchange: string
    routingKey: string
    encoded: EncodedMessage
    timeoutMs: number
  }): Promise<void> {
    const promise = this.doSend(args)
    this.inFlight.add(promise)
    try {
      await promise
    } finally {
      this.inFlight.delete(promise)
    }
  }

  /** Wait until every accepted publish settled (used by `close()`); never throws. */
  async settleAll(): Promise<void> {
    while (this.inFlight.size > 0) {
      await Promise.allSettled([...this.inFlight])
    }
  }

  private async doSend(args: {
    exchange: string
    routingKey: string
    encoded: EncodedMessage
    timeoutMs: number
  }): Promise<void> {
    let channel = this.channel
    if (channel === null) {
      // The setup-created channel is gone (solo channel death, or a disconnect right
      // after the open wait) — recreate it within the same bounded-wait budget.
      if (args.timeoutMs === 0) {
        throw new ConeyUnavailableError(
          'the publish channel is not available, and the allowed wait is 0 ms',
        )
      }
      channel = await raceTimeout(
        this.ensureChannel(),
        args.timeoutMs,
        () => new ConeyUnavailableError(`no publish channel within ${args.timeoutMs} ms`),
      )
    }
    while (this.drainGate !== null) {
      await this.drainGate
    }
    const target = channel
    await new Promise<void>((resolve, reject) => {
      const entry: PendingConfirm = {
        reject: (err) => {
          this.pending.delete(entry)
          reject(err)
        },
      }
      this.pending.add(entry)
      let keepWriting: boolean
      try {
        keepWriting = target.publish(
          args.exchange,
          args.routingKey,
          args.encoded.content,
          args.encoded.properties,
          (err) => {
            // Broker confirm: ack (err == null) or nack. A settled promise ignores
            // the late callback amqplib may fire after a disconnect rejection.
            this.pending.delete(entry)
            if (err == null) resolve()
            else
              reject(
                new ConeyPublishError(`the broker rejected the publish to "${args.routingKey}"`, {
                  cause: err,
                }),
              )
          },
        )
      } catch (err) {
        this.pending.delete(entry)
        reject(
          new ConeyUnavailableError('the publish channel failed while writing', { cause: err }),
        )
        return
      }
      if (!keepWriting && this.drainGate === null) {
        // amqplib buffered the frame but asks us to pause: gate SUBSEQUENT publishes
        // until `drain` (spec §4). `close` releases the gate too — a dead channel
        // sends no `drain`, and the waiters will fail fast on the dead channel.
        // Whichever event fires must remove BOTH listeners: leaving the loser
        // attached leaked one 'close' listener per back-pressure episode.
        const gate = new Promise<void>((resolve) => {
          const release = (): void => {
            target.removeListener('drain', release)
            target.removeListener('close', release)
            resolve()
          }
          target.once('drain', release)
          target.once('close', release)
        })
        this.drainGate = gate
        void gate.then(() => {
          if (this.drainGate === gate) this.drainGate = null
        })
      }
    })
  }

  private ensureChannel(): Promise<ConfirmChannel> {
    if (this.channel !== null) return Promise.resolve(this.channel)
    this.channelPromise ??= this.manager.createConfirmChannel().then(
      (channel) => {
        this.channelPromise = null
        // A create parked during an outage resolves only AFTER the reconnect's
        // setup ran (amqplib resolves its waiters post-setup) — by then setup has
        // already adopted a fresh channel. Adopting the late one would orphan the
        // current channel; close it and keep the current one instead.
        if (this.channel !== null) {
          void channel.close().catch(() => {})
          return this.channel
        }
        this.adoptChannel(channel)
        return channel
      },
      (err: unknown) => {
        this.channelPromise = null
        throw err
      },
    )
    return this.channelPromise
  }

  private adoptChannel(channel: ConfirmChannel): void {
    // No registerChannel here: both creation paths (manager lazy method, setup
    // factory) already registered the safety listeners exactly once.
    channel.on('close', () => {
      if (this.channel !== channel) return // a superseded channel must not touch current state
      this.channel = null
      this.drainGate = null
      // A solo channel death (no disconnect) must also fail outstanding confirms;
      // after a connection loss this is a harmless double reject. Scoped to the
      // CURRENT channel: every pending confirm was written on it.
      this.rejectPending(
        new ConeyUnavailableError('the publish channel closed before the broker confirmed'),
      )
    })
    this.channel = channel
  }
}

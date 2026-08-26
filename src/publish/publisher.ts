/**
 * The one shared confirm channel for every publish: broker confirms (resolve on
 * ack, {@link ConeyPublishError} on nack), a bounded wait while disconnected
 * (decision #3 — nothing is ever buffered), and `drain` back-pressure.
 *
 * Every wait in here is on ONE deadline handed down by the client (spec §4):
 * channel (re)creation, the back-pressure gate AND the broker confirm share it.
 * A broker that accepts the frame and never confirms — a disk/memory alarm blocks
 * the connection without dropping it — must fail the publish, not park it forever.
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
    const deadline = Date.now() + args.timeoutMs
    // Back-pressure gate FIRST, then read the channel: `close` releases the gate
    // too, so a channel captured before the wait can be the dead one that armed it
    // while a healthy replacement is already in place.
    await this.awaitDrain(args, deadline)
    const target = await this.acquireChannel(args, deadline)
    await this.awaitConfirm(target, args, deadline)
  }

  /** Park while another publish is draining, never past the caller's deadline. */
  private async awaitDrain(args: { timeoutMs: number }, deadline: number): Promise<void> {
    while (this.drainGate !== null) {
      const gate = this.drainGate
      const remaining = Math.max(0, deadline - Date.now())
      await raceTimeout(
        gate,
        remaining,
        () =>
          new ConeyUnavailableError(
            `the publish channel was still back-pressured after ${args.timeoutMs} ms`,
          ),
      )
    }
  }

  /** The current confirm channel, recreated within the remaining budget when it is gone. */
  private async acquireChannel(
    args: { timeoutMs: number },
    deadline: number,
  ): Promise<ConfirmChannel> {
    const current = this.channel
    if (current !== null) return current
    // The setup-created channel is gone (solo channel death, or a disconnect right
    // after the open wait) — recreate it within the same bounded-wait budget.
    const remaining = Math.max(0, deadline - Date.now())
    if (remaining === 0) {
      throw new ConeyUnavailableError(
        args.timeoutMs === 0
          ? 'the publish channel is not available, and the allowed wait is 0 ms'
          : `no publish channel within ${args.timeoutMs} ms`,
      )
    }
    return raceTimeout(
      this.ensureChannel(),
      remaining,
      () => new ConeyUnavailableError(`no publish channel within ${args.timeoutMs} ms`),
    )
  }

  /** Write the frame and resolve on the broker's confirm — within what is left of the budget. */
  private awaitConfirm(
    target: ConfirmChannel,
    args: { exchange: string; routingKey: string; encoded: EncodedMessage; timeoutMs: number },
    deadline: number,
  ): Promise<void> {
    const remaining = deadline - Date.now()
    if (remaining <= 0) {
      // Nothing is written: a caller whose budget is already spent must not be left
      // wondering whether the message went out (spec §4 — `0` = fail at once).
      return Promise.reject(
        new ConeyUnavailableError(
          `the publish budget of ${args.timeoutMs} ms was spent before "${args.routingKey}" could be confirmed`,
        ),
      )
    }
    return new Promise<void>((resolve, reject) => {
      let settled = false
      const finish = (err?: Error): void => {
        if (settled) return
        settled = true
        clearTimeout(timer)
        this.pending.delete(entry)
        if (err === undefined) resolve()
        else reject(err)
      }
      const entry: PendingConfirm = { reject: finish }
      const timer = setTimeout(() => {
        finish(
          new ConeyUnavailableError(
            `the broker did not confirm the publish to "${args.routingKey}" within ${args.timeoutMs} ms`,
          ),
        )
      }, remaining)
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
            if (err == null) {
              finish()
              return
            }
            // amqplib's ConfirmChannel registers its OWN `close` listener in the
            // channel constructor, so a connection/channel loss flushes every
            // unconfirmed callback through here BEFORE coney's listener runs —
            // at this instant a nack and a dead channel look identical. Decide one
            // microtask later, once that synchronous dispatch is done: a nack
            // leaves the channel current, a close does not. Spec §4 — publishes in
            // flight when the connection drops reject ConeyUnavailableError (state
            // unknown, the caller may retry), never the permanent ConeyPublishError.
            queueMicrotask(() => {
              finish(
                this.channel === target
                  ? new ConeyPublishError(
                      `the broker rejected the publish to "${args.routingKey}"`,
                      { cause: err },
                    )
                  : new ConeyUnavailableError(
                      'the publish channel closed before the broker confirmed',
                      { cause: err },
                    ),
              )
            })
          },
        )
      } catch (err) {
        finish(
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
        const gate = new Promise<void>((release) => {
          const releaseGate = (): void => {
            target.removeListener('drain', releaseGate)
            target.removeListener('close', releaseGate)
            release()
          }
          target.once('drain', releaseGate)
          target.once('close', releaseGate)
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

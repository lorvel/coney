/**
 * Runtime plumbing shared by queue consumers and topic subscriptions: the
 * attach/deliver/cancel/drain/flush lifecycle the client tracks. Iron rule:
 * nothing thrown by user code may reach amqplib's emitter.
 */

import type { Channel, ConfirmChannel, ConsumeMessage } from 'amqplib'
import { toError } from '../core/errors.js'
import { reasonOf, type LogEmitter } from '../core/log.js'
import { validateDurationMs } from '../core/time.js'
import type { ChannelFactory } from '../connection/manager.js'
import type { CancelOptions, CloseResult, Subscription } from './types.js'

const DEFAULT_CANCEL_DRAIN_MS = 10_000

/** State + behavior shared by the queue-consumer and topic-subscription runtimes. */
export abstract class BaseConsumer {
  protected channel: Channel | ConfirmChannel | null = null
  protected consumerTag: string | null = null
  protected activeFlag = true
  protected readonly inFlight = new Set<Promise<void>>()
  protected readonly controllers = new Set<AbortController>()
  private attaching: Promise<void> | null = null
  private cancelPromise: Promise<CloseResult> | null = null

  constructor(
    protected readonly tag: string,
    protected readonly prefetchCount: number,
    protected readonly emit: LogEmitter,
    private readonly onDispose: (runtime: BaseConsumer) => void,
  ) {}

  get active(): boolean {
    return this.activeFlag
  }

  /**
   * True while a `doAttach` is in flight. The setup re-attach loop MUST skip such
   * runtimes: a lazy-path attach parks on amqplib's connection waiters, which
   * resolve only AFTER setup returns — awaiting it from setup deadlocks the
   * reconnect permanently (verified on 2.0.1). The parked attach completes on the
   * new connection by itself once setup is done.
   */
  get attachPending(): boolean {
    return this.attaching !== null
  }

  /** (Re)attach when not attached; throws (initial `consume()` propagates, `setup` catches). */
  async attach(factory: ChannelFactory): Promise<void> {
    if (!this.activeFlag || this.channel !== null) return
    this.attaching ??= this.runAttach(factory)
    await this.attaching
  }

  private runAttach(factory: ChannelFactory): Promise<void> {
    return this.doAttach(factory)
      .then(() => this.postAttachCheck())
      .finally(() => {
        this.attaching = null
      })
  }

  /**
   * An attach can outlive the runtime: `cancel()`/`close()` may flip the flag
   * while `doAttach` is suspended (they cannot wait for it — it may be parked on
   * a broker outage), and the consumer that then comes up would silently swallow
   * deliveries without acking. Tear it down the moment the attach settles.
   */
  private async postAttachCheck(): Promise<void> {
    if (!this.activeFlag && this.channel !== null) {
      await this.stopDelivery()
      await this.releaseChannel()
    }
  }

  /** Create the channel, declare what the consumer needs, start consuming. */
  protected abstract doAttach(factory: ChannelFactory): Promise<void>

  /** Track a channel; detaches this runtime when the channel dies (reconnects re-attach via `setup`). */
  protected adoptChannel(channel: Channel | ConfirmChannel): void {
    channel.on('close', () => {
      if (this.channel === channel) {
        this.channel = null
        this.consumerTag = null
      }
    })
    this.channel = channel
  }

  /**
   * The sync amqplib consume callback: detach into a tracked async task; NEVER
   * throw. `channel` comes through the closure, NOT `this.channel` — a delivery
   * can arrive between the consume-ok and the continuation that records the
   * channel on the runtime, and reading the field there silently dropped the
   * message while it stayed unacked (caught by the forced-disconnect test).
   */
  protected onDelivery(channel: Channel | ConfirmChannel, raw: ConsumeMessage | null): void {
    try {
      if (raw === null) {
        this.handleBrokerCancel()
        return
      }
      if (!this.activeFlag) return
      const task = this.processDelivery(channel, raw).catch((err: unknown) => {
        // processDelivery handles its own failures; anything here is a coney bug.
        process.emitWarning(toError(err))
      })
      this.inFlight.add(task)
      void task.finally(() => {
        this.inFlight.delete(task)
      })
    } catch (err) {
      process.emitWarning(toError(err))
    }
  }

  protected abstract processDelivery(
    channel: Channel | ConfirmChannel,
    raw: ConsumeMessage,
  ): Promise<void>

  /** Broker-initiated cancel (queue deleted): drop the channel; the next `setup` re-attaches. */
  protected abstract handleBrokerCancel(): void

  /** `basic.cancel` + stop taking new deliveries; never throws. */
  async stopDelivery(): Promise<void> {
    const { channel, consumerTag } = this
    this.consumerTag = null
    if (channel === null || consumerTag === null) return
    try {
      // try/catch, not `.catch()`: a synchronous throw from a dying channel
      // would escape before a rejection handler could attach.
      await channel.cancel(consumerTag)
    } catch {
      // The channel is dead or mid-teardown — delivery stops with it.
    }
  }

  inFlightCount(): number {
    return this.inFlight.size
  }

  /** Wait until in-flight deliveries settle or the absolute deadline passes; true when drained. */
  async waitInFlight(deadlineMs: number): Promise<boolean> {
    while (this.inFlight.size > 0) {
      const remaining = deadlineMs - Date.now()
      if (remaining <= 0) return false
      // The losing sleep timer MUST be cleared: un-unref'd, it would hold the
      // event loop open for the rest of the drain budget after close() resolved.
      let timer: NodeJS.Timeout | undefined
      const timedOut = new Promise<void>((resolve) => {
        timer = setTimeout(resolve, remaining)
      })
      try {
        await Promise.race([Promise.allSettled([...this.inFlight]), timedOut])
      } finally {
        if (timer !== undefined) clearTimeout(timer)
      }
    }
    return true
  }

  /** Abort the `signal` of every in-flight handler (close drain expired). */
  abortInFlight(): void {
    for (const controller of this.controllers) {
      controller.abort(new Error('the close drain deadline passed'))
    }
  }

  /**
   * Close the channel with its RPC handshake. Load-bearing during shutdown: an
   * `ack` is a fire-and-forget frame and amqplib's connection close does NOT
   * flush pending writes (verified on 2.0.1 — an ack followed at once by a
   * connection close is lost and the message redelivered). The CloseOk
   * round-trip forces every prior frame on the channel through the broker.
   */
  async releaseChannel(): Promise<void> {
    const { channel } = this
    this.channel = null
    this.consumerTag = null
    if (channel === null) return
    try {
      await channel.close()
    } catch {
      // Already closing/closed — the RPC flush is best-effort on a dead channel.
    }
  }

  /** Mark inactive without draining (client close, or before a full dispose). */
  markClosed(): void {
    this.activeFlag = false
  }

  /**
   * Full teardown without draining: mark inactive, cancel the broker-side
   * consumer, release the channel. Used when a runtime must not survive an error
   * path (e.g. `consume()` rolling back after its topology assert failed while
   * setup had already attached it — leaving the channel open would park a
   * phantom consumer on the queue).
   */
  async dispose(): Promise<void> {
    this.activeFlag = false
    await this.stopDelivery()
    await this.releaseChannel()
  }

  /** The `Subscription.cancel` path: stop, drain (bounded), abort the rest, release the channel. */
  cancel(options?: CancelOptions): Promise<CloseResult> {
    if (options?.drainMs !== undefined) validateDurationMs(options.drainMs, 'drainMs')
    this.cancelPromise ??= this.doCancel(options?.drainMs ?? DEFAULT_CANCEL_DRAIN_MS)
    return this.cancelPromise
  }

  private async doCancel(drainMs: number): Promise<CloseResult> {
    this.activeFlag = false
    this.onDispose(this)
    await this.stopDelivery()
    const drained = await this.waitInFlight(Date.now() + drainMs)
    if (!drained) this.abortInFlight()
    const inFlight = this.inFlight.size
    await this.releaseChannel()
    return Object.freeze({ drained, inFlight })
  }

  /** ack with the failure contained: a dead channel means the broker redelivers (duplicate, never a loss). */
  protected safeAck(
    channel: Channel | ConfirmChannel,
    raw: ConsumeMessage,
    queue: string,
    messageId: string,
  ): void {
    try {
      channel.ack(raw)
    } catch (err) {
      this.emit({
        event: 'coney.ack_failed',
        level: 'warn',
        queue,
        messageId,
        reason: reasonOf(err),
      })
    }
  }

  /** The user-facing handle (a stable facade; `active` reflects the runtime). */
  subscriptionFacade(): Subscription {
    const isActive = (): boolean => this.activeFlag
    return {
      get active(): boolean {
        return isActive()
      },
      cancel: (options?: CancelOptions) => this.cancel(options),
    }
  }
}

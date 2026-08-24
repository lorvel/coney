/**
 * Connection lifecycle: a thin adapter over amqplib >= 1.1 `recovery` (decision #17).
 * amqplib owns the retry loop; coney maps the policy, provides the `setup(model)`
 * hook (topology replay + channel recreation) and translates events into the
 * typed log catalog and the client state machine.
 */

import amqplib from 'amqplib'
import type { Channel, ChannelModel, ConfirmChannel, RecoveringChannelModel } from 'amqplib'
import { ConeyClosedError, ConeyUnavailableError, toError } from '../core/errors.js'
import { redactText, redactUrl, type LogEmitter } from '../core/log.js'
import { toRecoveryOptions, type ReconnectPolicy } from './policy.js'

/** Manager phases; the client overlays `closing`/`closed` while `close()` runs. */
export type ConnectionPhase = 'idle' | 'connecting' | 'open' | 'reconnecting' | 'closed'

/**
 * How channels are created: the manager itself (lazy paths — waits for `open`)
 * or the setup-scoped factory passed to {@link ConnectionManager.onSetup}. Inside
 * setup the factory is the ONLY correct source: the lazy manager paths would
 * deadlock there (their `open` wait resolves only AFTER setup returns), and the
 * factory also attaches the mandatory safety listeners so no setup-time caller
 * can forget them.
 */
export interface ChannelFactory {
  createChannel(): Promise<Channel>
  createConfirmChannel(): Promise<ConfirmChannel>
}

interface OpenWaiter {
  resolve: () => void
  reject: (err: Error) => void
}

export class ConnectionManager {
  /**
   * Runs on EVERY successful (re)connection, before pending channel waiters resolve.
   * Set by the client: topology replay + publisher + consumer channel recreation,
   * all through `channels` (see {@link ChannelFactory} for why the raw model must
   * not be used directly). Iron law: it must never reject — amqplib treats a
   * rejected `setup` as a failed connection and retries forever. Guarded here anyway.
   */
  onSetup: (model: ChannelModel, channels: ChannelFactory) => Promise<void> = async () => {}
  /** Called once per connection loss, before any reconnect: reject in-flight confirms. */
  onDisconnect: (err: ConeyUnavailableError) => void = () => {}

  readonly redactedUrl: string

  private phaseValue: ConnectionPhase = 'idle'
  private readonly url: string
  private readonly socketOptions: Record<string, unknown> | undefined
  private readonly connectionName: string
  private readonly policy: ReconnectPolicy
  private readonly emit: LogEmitter

  private started = false
  private recovering: RecoveringChannelModel | null = null
  private closeRequested = false
  private disconnectedAt = 0
  private openWaiters: OpenWaiter[] = []
  private cancelBackoff: (() => void) | null = null

  constructor(args: {
    url: string
    socketOptions: Record<string, unknown> | undefined
    connectionName: string
    policy: ReconnectPolicy
    emit: LogEmitter
  }) {
    this.url = args.url
    this.redactedUrl = redactUrl(args.url)
    this.socketOptions = args.socketOptions
    this.connectionName = args.connectionName
    this.policy = args.policy
    this.emit = args.emit
  }

  get phase(): ConnectionPhase {
    return this.phaseValue
  }

  /**
   * Kick off the connect loop (idempotent, non-blocking).
   *
   * Two phases (docs/design.md §4): amqplib's promise API only hands the
   * recovering model back after the FIRST successful connection, so its own
   * initial retry loop cannot be stopped from outside — a `close()` during a
   * boot-time outage would leak it forever. Phase 1 is therefore coney's own
   * cancellable probe loop (plain connects under the same policy); phase 2 hands
   * over to the recovery connect the moment a probe succeeded.
   */
  start(): void {
    if (this.started || this.closeRequested) return
    this.started = true
    this.phaseValue = 'connecting'
    void this.runInitialConnect()
  }

  private async runInitialConnect(): Promise<void> {
    const userClientProperties = (this.socketOptions?.['clientProperties'] ?? {}) as Record<string, unknown>
    let attempt = 0
    for (;;) {
      if (this.closeRequested) return
      attempt += 1
      this.emit({ event: 'coney.connecting', level: 'info', attempt, url: this.redactedUrl })
      try {
        // The `:probe` suffix keeps tooling keyed on connection_name (e.g. the
        // management-API force-close used in tests) off this throwaway connection.
        const probe = await amqplib.connect(this.url, {
          ...this.socketOptions,
          clientProperties: { connection_name: `${this.connectionName}:probe`, ...userClientProperties },
        })
        await probe.close().catch(() => {})
        if (this.closeRequested) return
        break
      } catch (err) {
        if (this.closeRequested) return
        if (attempt >= this.policy.maxAttempts) {
          this.handleInitialFailure(err)
          return
        }
        await this.backoffSleep(attempt)
      }
    }
    const promise = amqplib.connect(this.url, {
      ...this.socketOptions,
      clientProperties: { connection_name: this.connectionName, ...userClientProperties },
      recovery: {
        ...toRecoveryOptions(this.policy),
        setup: (model: ChannelModel) => this.runSetup(model),
      },
    })
    promise.then(
      (recovering) => this.handleFirstConnect(recovering),
      (err: unknown) => this.handleInitialFailure(err),
    )
  }

  /** The recovery backoff formula (delay, factor, jitter) — resolves early on close. */
  private backoffSleep(attempt: number): Promise<void> {
    const base = Math.min(this.policy.maxDelayMs, this.policy.initialDelayMs * this.policy.factor ** (attempt - 1))
    const jitterPart = base * this.policy.jitter
    const offset = jitterPart > 0 ? Math.random() * jitterPart * 2 - jitterPart : 0
    const delay = Math.max(0, Math.round(base + offset))
    return new Promise((resolve) => {
      const timer = setTimeout(() => {
        this.cancelBackoff = null
        resolve()
      }, delay)
      this.cancelBackoff = () => {
        clearTimeout(timer)
        this.cancelBackoff = null
        resolve()
      }
    })
  }

  /** Resolves when connected; rejects when closed or the reconnect policy gave up. */
  waitForOpen(timeoutMs?: number): Promise<void> {
    if (this.closeRequested) {
      return Promise.reject(new ConeyClosedError('the client is closing or closed'))
    }
    if (this.phaseValue === 'closed') {
      return Promise.reject(new ConeyUnavailableError('the reconnect policy gave up; the client is closed'))
    }
    if (this.phaseValue === 'open') return Promise.resolve()
    this.start()
    if (timeoutMs === 0) {
      return Promise.reject(new ConeyUnavailableError('not connected, and the allowed wait is 0 ms'))
    }
    return new Promise<void>((resolve, reject) => {
      let timer: NodeJS.Timeout | undefined
      const waiter: OpenWaiter = {
        resolve: () => {
          if (timer !== undefined) clearTimeout(timer)
          resolve()
        },
        reject: (err) => {
          if (timer !== undefined) clearTimeout(timer)
          reject(err)
        },
      }
      if (timeoutMs !== undefined) {
        timer = setTimeout(() => {
          const index = this.openWaiters.indexOf(waiter)
          if (index !== -1) this.openWaiters.splice(index, 1)
          reject(new ConeyUnavailableError(`no broker connection within ${timeoutMs} ms (the reconnect loop keeps running)`))
        }, timeoutMs)
      }
      this.openWaiters.push(waiter)
    })
  }

  /** A plain channel on the current connection (waits while disconnected). */
  async createChannel(): Promise<Channel> {
    const recovering = await this.requireRecovering()
    const channel = await this.wrapChannelErrors(recovering.createChannel())
    this.registerChannel(channel)
    return channel
  }

  /** A confirm channel on the current connection (waits while disconnected). */
  async createConfirmChannel(): Promise<ConfirmChannel> {
    const recovering = await this.requireRecovering()
    const channel = await this.wrapChannelErrors(recovering.createConfirmChannel())
    this.registerChannel(channel)
    return channel
  }

  /**
   * Attach the safety listeners every amqplib emitter needs: without a
   * `handler-error` listener a throwing listener propagates into amqplib's socket
   * handling and kills the connection, and an unhandled `error` event would crash
   * the process. Coney wraps all of its own callbacks, so both are last-resort nets.
   * Private on purpose: both creation paths (the lazy methods above and the setup
   * factory) register automatically, exactly once.
   */
  private registerChannel(channel: Channel | ConfirmChannel): void {
    channel.on('error', () => {
      // The channel is dead; per-operation promises reject on their own.
    })
    channel.on('handler-error', (err: Error) => {
      process.emitWarning(err)
    })
  }

  /** Stop everything. Safe in every phase, idempotent, never throws. */
  async closeConnection(): Promise<void> {
    if (this.closeRequested) {
      this.phaseValue = 'closed'
      return
    }
    this.closeRequested = true
    this.rejectOpenWaiters(new ConeyClosedError('the client was closed'))
    this.cancelBackoff?.()
    if (this.recovering !== null) {
      // Stops the reconnect loop immediately, even mid-backoff (verified on 1.2.0/2.0.1).
      await this.recovering.close().catch(() => {})
    }
    // Phase 1 (the probe loop) exits at its next closeRequested check; a recovery
    // connect already in flight is closed by handleFirstConnect the moment amqplib
    // hands it over (bounded zombie window).
    this.phaseValue = 'closed'
  }

  private async runSetup(model: ChannelModel): Promise<void> {
    try {
      model.on('handler-error', (err: Error) => {
        process.emitWarning(err)
      })
      const channels: ChannelFactory = {
        createChannel: async () => {
          const channel = await model.createChannel()
          this.registerChannel(channel)
          return channel
        },
        createConfirmChannel: async () => {
          const channel = await model.createConfirmChannel()
          this.registerChannel(channel)
          return channel
        },
      }
      await this.onSetup(model, channels)
    } catch (err) {
      // Iron law (docs/design.md §4): never reject `setup` — amqplib would treat the
      // connection as failed and retry forever. Anything reaching this catch is a
      // coney bug; surface it without taking the connection down.
      process.emitWarning(toError(err))
    }
  }

  private handleFirstConnect(recovering: RecoveringChannelModel): void {
    this.recovering = recovering
    this.attachRecoveringListeners(recovering)
    if (this.closeRequested) {
      void recovering.close().catch(() => {})
      return
    }
    this.phaseValue = 'open'
    this.emit({ event: 'coney.connected', level: 'info', url: this.redactedUrl })
    this.resolveOpenWaiters()
  }

  private handleInitialFailure(err: unknown): void {
    if (this.closeRequested) return // close() during the initial connect rejects the promise; expected
    this.phaseValue = 'closed'
    this.emit({ event: 'coney.reconnect_exhausted', level: 'error', attempts: this.policy.maxAttempts })
    this.rejectOpenWaiters(
      new ConeyUnavailableError(`could not connect to ${this.redactedUrl} (reconnect policy exhausted)`, { cause: err }),
    )
  }

  private attachRecoveringListeners(recovering: RecoveringChannelModel): void {
    recovering.on('disconnect', this.safe((err: Error) => {
      if (this.closeRequested) return
      this.phaseValue = 'reconnecting'
      this.disconnectedAt = Date.now()
      this.emit({ event: 'coney.disconnected', level: 'warn', reason: redactText(err.message) })
      this.onDisconnect(new ConeyUnavailableError('the broker connection was lost', { cause: err }))
    }))
    recovering.on('connect', this.safe(() => {
      // The very first `connect` fired before coney had the emitter; every one seen
      // here is a RE-connect.
      if (this.closeRequested) return
      this.phaseValue = 'open'
      this.emit({ event: 'coney.reconnected', level: 'info', downtimeMs: Date.now() - this.disconnectedAt })
      this.resolveOpenWaiters()
    }))
    recovering.on('reconnect-scheduled', this.safe((info: { attempt: number; delay: number }) => {
      if (this.closeRequested) return
      this.emit({ event: 'coney.reconnecting', level: 'warn', attempt: info.attempt, delayMs: info.delay })
    }))
    recovering.on('reconnect-failed', this.safe((err: Error) => {
      // maxAttempts exhausted: terminal.
      if (this.closeRequested) return
      this.phaseValue = 'closed'
      this.emit({ event: 'coney.reconnect_exhausted', level: 'error', attempts: this.policy.maxAttempts })
      this.rejectOpenWaiters(new ConeyUnavailableError('the reconnect policy gave up', { cause: err }))
    }))
    recovering.on('error', this.safe(() => {
      // Followed by `disconnect`; an unhandled `error` event would crash the process.
    }))
    recovering.on('handler-error', this.safe((err: Error) => {
      process.emitWarning(err)
    }))
  }

  private async requireRecovering(): Promise<RecoveringChannelModel> {
    if (this.closeRequested) throw new ConeyClosedError('the client is closing or closed')
    if (this.recovering !== null) return this.recovering
    this.start()
    await this.waitForOpen()
    if (this.recovering === null) throw new ConeyUnavailableError('not connected')
    return this.recovering
  }

  /** Map amqplib's plain `Error('Connection closed')` rejections onto coney errors. */
  private async wrapChannelErrors<T>(promise: Promise<T>): Promise<T> {
    try {
      return await promise
    } catch (err) {
      if (this.closeRequested) throw new ConeyClosedError('the client is closing or closed', { cause: err })
      throw new ConeyUnavailableError('the broker connection is not available', { cause: err })
    }
  }

  private resolveOpenWaiters(): void {
    const waiters = this.openWaiters.splice(0)
    for (const waiter of waiters) waiter.resolve()
  }

  private rejectOpenWaiters(err: Error): void {
    const waiters = this.openWaiters.splice(0)
    for (const waiter of waiters) waiter.reject(err)
  }

  /** Wrap a listener so a coney bug can never throw into amqplib's emitter. */
  private safe<A extends unknown[]>(listener: (...args: A) => void): (...args: A) => void {
    return (...args: A) => {
      try {
        listener(...args)
      } catch (err) {
        process.emitWarning(toError(err))
      }
    }
  }
}

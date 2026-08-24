/**
 * Consuming: the message pipeline types. The runtime (channel-per-consumer,
 * schema gate, retry/dead outcomes, timeouts, re-attach) lands in the next slice;
 * the surface is declared now so the public API is complete from the start.
 */

export interface Message<T> {
  /** Decoded body, already validated by the def's schema (when one is set). */
  readonly body: T
  /** AMQP `messageId` (see {@link PublishOptions.messageId}). Stable across retries and redeliveries. */
  readonly id: string
  /** 1 on the first delivery; incremented by each retry (`x-retries` + 1). Broker redeliveries do NOT increment it. */
  readonly attempt: number
  /** AMQP redelivered flag: the broker already delivered this message once and it was not acknowledged. */
  readonly redelivered: boolean
  /** Routing key the message was published with (topic key for bound queues, queue name otherwise). */
  readonly routingKey: string
  /** All AMQP headers, including the library's `x-*` ones. */
  readonly headers: Readonly<Record<string, unknown>>
  /** From the `x-published-at` header (ms precision); absent for messages from foreign producers. */
  readonly publishedAt?: Date
  /** AMQP `appId` — the `name` of the publishing client, if any. */
  readonly appId?: string
  /** Aborted when `timeoutMs` elapses or the close drain deadline passes — pass it to fetch/db calls. */
  readonly signal: AbortSignal
}

/**
 * Outcome is implicit:
 *   - resolve                       → ack
 *   - throw                          → retry (next bucket of the ladder) or dead when the ladder is exhausted
 *   - throw {@link UnrecoverableError} → dead immediately, no retry
 * The library never requeues to the head of the queue.
 */
export type Handler<T> = (message: Message<T>) => void | Promise<void>

export interface ConsumeOptions {
  /** Max messages in flight (= max concurrent handler calls) for this consumer. Default `1`. */
  readonly prefetch?: number
  /** Handler deadline: on expiry the message fails right away (retry/dead + ack), `signal` is aborted, the late result is ignored (`coney.handler_late`). Default: none. */
  readonly timeoutMs?: number
}

/** Internal: the not-yet-implemented slice of the surface (consuming, ops, mock). */
export function notImplemented(what: string): never {
  throw new Error(`${what} is not implemented yet — it arrives with the consuming slice (this package is unpublished)`)
}

/** Internal: marks parameters of stubbed methods as intentionally unused. */
export function unused(..._args: readonly unknown[]): void {}
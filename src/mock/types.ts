/**
 * The test-double contract: what `createConeyMock` records and how a simulated
 * delivery reports its outcome.
 */

import type { HeaderValue, PublishOptions } from '../core/codec.js'
import type { QueueDef, TopicDef } from '../defs/define.js'
import type { Coney } from '../client/types.js'

export interface PublishedRecord {
  readonly target: { readonly kind: 'queue' | 'topic'; readonly name: string }
  readonly routingKey: string
  readonly body: unknown
  readonly options: PublishOptions
}

export type DeliveryOutcome =
  | { readonly outcome: 'ack' }
  | {
      readonly outcome: 'retry'
      readonly attempt: number
      readonly delayMs: number
      readonly error: unknown
    }
  | { readonly outcome: 'dead'; readonly reason: string; readonly error?: unknown }
  /** Subscriptions only: the handler threw and the message was dropped. */
  | { readonly outcome: 'dropped'; readonly error: unknown }

export interface DeliverOptions {
  /** Simulated attempt number. Default `1`. */
  readonly attempt?: number
  readonly messageId?: string
  readonly headers?: Readonly<Record<string, HeaderValue>>
  readonly redelivered?: boolean
}

/** In-memory {@link Coney}: `publish` records, `consume`/`subscribe` register, {@link ConeyMock.deliver} runs the real consumer pipeline (schema gate, ladder, `timeoutMs`). `close()` mirrors the real lifecycle: state `'closed'`, registrations deactivated, gated operations reject with {@link ConeyClosedError}. */
export interface ConeyMock extends Coney {
  /** Every publish since creation or the last {@link ConeyMock.reset}, in order. */
  readonly published: readonly PublishedRecord[]
  /**
   * Deliver one message to the consumer registered for `def`.
   * @throws {ConeyConfigError} no consumer is registered for the queue.
   */
  deliver<T>(def: QueueDef<T>, body: T, options?: DeliverOptions): Promise<DeliveryOutcome>
  /** Deliver to every subscription and bound queue whose patterns match `routingKey`, in registration order. */
  deliverTopic<T>(
    def: TopicDef<T>,
    routingKey: string,
    body: T,
    options?: DeliverOptions,
  ): Promise<readonly DeliveryOutcome[]>
  /** Clear `published`; registered consumers stay. */
  reset(): void
}

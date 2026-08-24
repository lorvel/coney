/**
 * Subscriptions: types for the per-instance exclusive listeners of a topic.
 * The runtime lands with the consuming slice.
 */

import type { CloseResult } from './client.js'

export interface SubscribeOptions {
  /** Max messages in flight for this subscription. Default `1` (in-order processing). */
  readonly prefetch?: number
  /** Same semantics as {@link ConsumeOptions.timeoutMs}, except the outcome is "drop" (subscriptions have no retry). */
  readonly timeoutMs?: number
}

export interface CancelOptions {
  /** Max wait for in-flight handlers. Default `10_000` ms. */
  readonly drainMs?: number
}

export interface Subscription {
  /** `false` once cancelled or after the client closed. */
  readonly active: boolean
  /** Stop receiving (survives reconnects until called). Resolves after in-flight handlers settle or `drainMs` passes. Idempotent. */
  cancel(options?: CancelOptions): Promise<CloseResult>
}

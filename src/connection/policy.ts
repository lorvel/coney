/**
 * The reconnect policy: the public shape, its defaults and validation, and the
 * mapping onto amqplib's `recovery` option names. One policy drives both the
 * initial connect (coney's probe loop) and every reconnect (amqplib's loop).
 */

import { ConeyConfigError } from '../core/errors.js'
import { TIMEOUT_MAX } from '../core/time.js'

/** Backoff policy used for the initial connect AND every reconnect. */
export interface ReconnectPolicy {
  /** Delay before the first retry. Default `1000` ms. */
  initialDelayMs: number
  /** Upper bound for the growing delay. Default `30_000` ms. */
  maxDelayMs: number
  /** Multiplier applied after each failed attempt. Finite, `>= 1`. Default `2`. */
  factor: number
  /** Random jitter as a fraction of the delay (`0.2` = ±20 %). Default `0.2`. */
  jitter: number
  /** Give up after this many consecutive failed ATTEMPTS (`coney.reconnect_exhausted`). Default `Infinity`. */
  maxAttempts: number
}

export const DEFAULT_RECONNECT_POLICY: Readonly<ReconnectPolicy> = Object.freeze({
  initialDelayMs: 1000,
  maxDelayMs: 30_000,
  factor: 2,
  jitter: 0.2,
  maxAttempts: Infinity,
})

/** amqplib's `recovery` option shape (subset coney sets; no amqplib types leak from here). */
export interface RecoveryOptionsMapped {
  initialDelay: number
  maxDelay: number
  factor: number
  jitter: number
  maxRetries: number
}

/**
 * Map the public policy (`*Ms` fields, `maxAttempts`) onto amqplib's `recovery` names.
 *
 * `maxAttempts` counts ATTEMPTS, `maxRetries` counts RETRIES AFTER the first one
 * (amqplib gives up at `_attempt >= maxRetries` having started at 0), so the two
 * differ by exactly one — passing the number through unchanged made a
 * `maxAttempts: 3` boot try three times and a reconnect four.
 */
export function toRecoveryOptions(policy: ReconnectPolicy): RecoveryOptionsMapped {
  return {
    initialDelay: policy.initialDelayMs,
    maxDelay: policy.maxDelayMs,
    factor: policy.factor,
    jitter: policy.jitter,
    maxRetries: policy.maxAttempts === Infinity ? Infinity : Math.max(0, policy.maxAttempts - 1),
  }
}

/**
 * One step of the recovery backoff curve: `initialDelayMs * factor^(attempt-1)`, clamped
 * by `maxDelayMs` and spread by `jitter`. Shared rather than inlined at each caller so
 * the connection loop and the consumer re-attach loop cannot drift apart — a broker
 * refusing one is very likely refusing the other, and two curves against the same broker
 * would make the logs impossible to read together.
 *
 * Total by construction, because a caller with no attempt ceiling reaches the ugly
 * inputs within seconds: `factor ** (attempt - 1)` overflows to `Infinity` after ~1000
 * rounds, which is `NaN` when `initialDelayMs` is 0 (a legal policy), and NaN survives
 * `Math.min`, `Math.round` and `Math.max(0, …)` alike. `setTimeout` turns BOTH NaN and
 * anything above {@link TIMEOUT_MAX} into 1 ms, so an unclamped result inverts "back
 * off" into "hammer the broker as fast as you can".
 */
export function backoffDelayMs(policy: ReconnectPolicy, attempt: number): number {
  const grown = policy.initialDelayMs * policy.factor ** (attempt - 1)
  const base = Number.isNaN(grown) ? policy.maxDelayMs : Math.min(policy.maxDelayMs, grown)
  const jitterPart = base * policy.jitter
  const offset = jitterPart > 0 ? Math.random() * jitterPart * 2 - jitterPart : 0
  return Math.min(TIMEOUT_MAX, Math.max(0, Math.round(base + offset)))
}

/**
 * Resolve a user's partial override into a full, validated policy — the ONLY
 * way a policy should be constructed, so merge-then-validate cannot be skipped.
 * Note the interplay: overriding `initialDelayMs` above the default `maxDelayMs`
 * (30 000) without also raising `maxDelayMs` is rejected by validation.
 */
export function resolveReconnectPolicy(
  partial: Partial<ReconnectPolicy> | undefined,
): ReconnectPolicy {
  const policy: ReconnectPolicy = { ...DEFAULT_RECONNECT_POLICY, ...partial }
  validateReconnectPolicy(policy)
  return policy
}

export function validateReconnectPolicy(policy: ReconnectPolicy): void {
  const isNonNegative = (value: number): boolean =>
    typeof value === 'number' && !Number.isNaN(value) && value >= 0
  if (!isNonNegative(policy.initialDelayMs) || !Number.isFinite(policy.initialDelayMs)) {
    throw new ConeyConfigError('reconnect.initialDelayMs must be a finite number >= 0')
  }
  if (
    !isNonNegative(policy.maxDelayMs) ||
    !Number.isFinite(policy.maxDelayMs) ||
    policy.maxDelayMs < policy.initialDelayMs
  ) {
    throw new ConeyConfigError('reconnect.maxDelayMs must be a finite number >= initialDelayMs')
  }
  if (
    typeof policy.factor !== 'number' ||
    Number.isNaN(policy.factor) ||
    !Number.isFinite(policy.factor) ||
    policy.factor < 1
  ) {
    // Finiteness matters: coney's own backoff clamps an infinite factor through
    // maxDelayMs while amqplib silently substitutes its default of 2, so the boot
    // and reconnect curves would diverge for the very same policy object.
    throw new ConeyConfigError('reconnect.factor must be a finite number >= 1')
  }
  if (
    typeof policy.jitter !== 'number' ||
    Number.isNaN(policy.jitter) ||
    policy.jitter < 0 ||
    policy.jitter > 1
  ) {
    throw new ConeyConfigError('reconnect.jitter must be a number between 0 and 1')
  }
  if (!isNonNegative(policy.maxAttempts)) {
    throw new ConeyConfigError('reconnect.maxAttempts must be >= 0 (Infinity allowed)')
  }
}

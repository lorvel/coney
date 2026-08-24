/** Retry-ladder arithmetic and the naming of the shared, delay-keyed retry buckets. */

/** Retry ladder of a queue: one delay per retry, in milliseconds. */
export interface RetryPolicy {
  /**
   * `delays[k - 1]` is the wait before attempt `k + 1`. The number of retries is
   * `delays.length`; a message that fails attempt `delays.length + 1` is dead-lettered.
   * Each distinct value creates one shared retry bucket on the broker
   * (`<prefix>.retry.<duration>`), so keep the set of distinct values small.
   * Values must be positive integers; `[]` means "no retry, straight to dead".
   */
  readonly delays: readonly number[]
}

/** Ladder for queue defs without `retry` when the client sets none (spec §6 defaults). */
export const DEFAULT_RETRY: RetryPolicy = Object.freeze({
  delays: Object.freeze([10_000, 60_000, 300_000]) as readonly number[],
})

/** The ladder a queue def actually uses: its own `retry`, the client default, or none. */
export function effectiveDelays(
  defRetry: RetryPolicy | false | undefined,
  clientDefault: RetryPolicy,
): readonly number[] {
  if (defRetry === false) return []
  return (defRetry ?? clientDefault).delays
}

/**
 * Delay before the next attempt after `attempt` just failed (`attempt` = `x-retries` + 1),
 * or `undefined` when the ladder is exhausted (dead-letter).
 */
export function nextDelayMs(delays: readonly number[], attempt: number): number | undefined {
  return delays[attempt - 1]
}

/**
 * Human duration label for a bucket: largest units first, zero units omitted —
 * `500ms`, `10s`, `1m`, `1m30s`, `2h30m`. Deterministic, so every client with the
 * same delay computes the same bucket name.
 */
export function durationLabel(ms: number): string {
  const units = [
    ['h', 3_600_000],
    ['m', 60_000],
    ['s', 1_000],
  ] as const
  const parts: string[] = []
  let rest = ms
  for (const [unit, size] of units) {
    const n = Math.floor(rest / size)
    if (n > 0) {
      parts.push(`${n}${unit}`)
      rest -= n * size
    }
  }
  if (rest > 0 || parts.length === 0) parts.push(`${rest}ms`)
  return parts.join('')
}

/** Logical name of the shared retry bucket for a delay (`retry.<duration>`; reserved — user names cannot start with `retry.`). */
export function bucketLogicalName(delayMs: number): string {
  return `retry.${durationLabel(delayMs)}`
}

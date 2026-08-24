/**
 * Timeout bounds and deadline racing. Node's `setTimeout` silently clamps any
 * delay above 2^31 - 1 (including `Infinity`) to 1 ms — which would invert
 * "wait forever" into "fail immediately" — so every user-supplied timeout and
 * drain duration is validated against {@link TIMEOUT_MAX} up front ("no
 * deadline" is spelled by omitting the option).
 */

import { ConeyConfigError, toError } from './errors.js'

/** Largest delay `setTimeout` honors (2^31 - 1 ms ≈ 24.8 days). */
export const TIMEOUT_MAX = 2_147_483_647

/**
 * Validate a user-supplied duration in milliseconds: a finite number within
 * `[0, TIMEOUT_MAX]` (or `(0, TIMEOUT_MAX]` with `positive`). Throws
 * {@link ConeyConfigError} naming `what`.
 */
export function validateDurationMs(
  value: unknown,
  what: string,
  options?: { positive?: boolean },
): asserts value is number {
  const floor = options?.positive === true ? 'a positive' : 'a'
  const bound =
    options?.positive === true ? `number <= ${TIMEOUT_MAX}` : `number >= 0 and <= ${TIMEOUT_MAX}`
  if (
    typeof value !== 'number' ||
    Number.isNaN(value) ||
    !Number.isFinite(value) ||
    value > TIMEOUT_MAX ||
    (options?.positive === true ? value <= 0 : value < 0)
  ) {
    throw new ConeyConfigError(`${what} must be ${floor} finite ${bound}`)
  }
}

/** Race a promise against a deadline; the loser's late rejection is swallowed. */
export function raceTimeout<T>(
  promise: Promise<T>,
  timeoutMs: number,
  makeError: () => Error,
): Promise<T> {
  return new Promise<T>((resolve, reject) => {
    const timer = setTimeout(() => {
      promise.catch(() => {})
      reject(makeError())
    }, timeoutMs)
    promise.then(
      (value) => {
        clearTimeout(timer)
        resolve(value)
      },
      (err: unknown) => {
        clearTimeout(timer)
        reject(toError(err))
      },
    )
  })
}

/**
 * AMQP topic-pattern matching, used by the mock's `deliverTopic` (the broker
 * does this server-side in production).
 */

/**
 * AMQP topic-pattern match (`.`-separated words): `*` matches exactly one word,
 * `#` matches zero or more — so `a.#` also matches `a`. Internal (the mock's
 * `deliverTopic`), exported for its own tests. Memoized on the (pattern index,
 * key index) state: naive backtracking is exponential in the number of `#`
 * segments (a pathological pattern against a long non-matching key would block
 * the event loop for seconds); the memo bounds it at O(p·k) states.
 */
export function matchTopicPattern(pattern: string, routingKey: string): boolean {
  const patternWords = pattern === '' ? [] : pattern.split('.')
  const keyWords = routingKey === '' ? [] : routingKey.split('.')
  const memo = new Map<number, boolean>()
  const walk = (pi: number, ki: number): boolean => {
    if (pi === patternWords.length) return ki === keyWords.length
    const memoKey = pi * (keyWords.length + 1) + ki
    const seen = memo.get(memoKey)
    if (seen !== undefined) return seen
    const word = patternWords[pi]
    let matched = false
    if (word === '#') {
      for (let skip = ki; skip <= keyWords.length; skip++) {
        if (walk(pi + 1, skip)) {
          matched = true
          break
        }
      }
    } else if (ki < keyWords.length && (word === '*' || word === keyWords[ki])) {
      matched = walk(pi + 1, ki + 1)
    }
    memo.set(memoKey, matched)
    return matched
  }
  return walk(0, 0)
}

/** The typed log-event catalog and the default sink (decision #14). */

export type LogLevel = 'debug' | 'info' | 'warn' | 'error'

export type ConeyLogEvent =
  | { event: 'coney.connecting'; level: 'info'; attempt: number; url: string }
  | { event: 'coney.connected'; level: 'info'; url: string }
  | { event: 'coney.disconnected'; level: 'warn'; reason: string }
  | { event: 'coney.reconnecting'; level: 'warn'; attempt: number; delayMs: number }
  | { event: 'coney.reconnected'; level: 'info'; downtimeMs: number }
  | { event: 'coney.reconnect_exhausted'; level: 'error'; attempts: number }
  | { event: 'coney.topology_asserted'; level: 'debug'; queues: string[]; exchanges: string[] }
  | {
      event: 'coney.topology_failed'
      level: 'error'
      target: string
      reason: string
      amqpCode?: number
    }
  | {
      event: 'coney.consumer_started'
      level: 'info'
      queue: string
      consumerTag: string
      prefetch: number
    }
  | { event: 'coney.consumer_stopped'; level: 'info'; queue: string; consumerTag: string }
  | { event: 'coney.subscription_reset'; level: 'warn'; topic: string; patterns: string[] }
  | {
      event: 'coney.subscription_error'
      level: 'error'
      topic: string
      messageId: string
      reason: string
    }
  | {
      event: 'coney.message_retried'
      level: 'warn'
      queue: string
      messageId: string
      attempt: number
      delayMs: number
      reason: string
    }
  | {
      event: 'coney.message_dead'
      level: 'error'
      queue: string
      messageId: string
      attempt: number
      reason: string
    }
  | {
      event: 'coney.handler_timeout'
      level: 'warn'
      queue: string
      messageId: string
      timeoutMs: number
    }
  | {
      event: 'coney.handler_late'
      level: 'warn'
      queue: string
      messageId: string
      outcome: 'resolved' | 'rejected'
    }
  | { event: 'coney.ack_failed'; level: 'warn'; queue: string; messageId: string; reason: string }
  | {
      event: 'coney.publish_failed'
      level: 'warn'
      target: string
      routingKey: string
      reason: string
    }
  | { event: 'coney.dead_replayed'; level: 'info'; queue: string; replayed: number }
  | { event: 'coney.closing'; level: 'info'; drainMs: number; inFlight: number }
  | { event: 'coney.closed'; level: 'info'; drained: boolean; inFlight: number }

/** Internal: the emit function every module receives. Never throws. */
export type LogEmitter = (event: ConeyLogEvent) => void

/**
 * Redact the credentials of an `amqp(s)://` URL (`amqp://user:***@host/vhost`).
 * Applied when events are CREATED, so every sink — including custom ones — sees
 * redacted URLs. Falls back to a regex when the URL does not parse.
 *
 * A userinfo with no password is redacted WHOLE (`amqp://***@host`): brokers that
 * authenticate by token carry the secret in the username, so keeping it would leak
 * the credential that {@link redactText} already masks for the same shape.
 */
export function redactUrl(url: string): string {
  try {
    const parsed = new URL(url)
    if (parsed.password !== '') {
      parsed.password = '***'
      return parsed.toString()
    }
    if (parsed.username !== '') {
      parsed.username = '***'
      return parsed.toString()
    }
    return url
  } catch {
    return redactText(url)
  }
}

/**
 * Redact the credentials of any `amqp(s)://…@` occurrence embedded in free text
 * (error messages). The whole userinfo is replaced up to the LAST `@` of the
 * token, so empty usernames (`amqp://:secret@host`) and unencoded `@` inside a
 * password (`amqp://u:p@ss@host`) cannot leak — over-redacting the username is
 * the acceptable cost of a redactor.
 */
export function redactText(text: string): string {
  return text.replace(/\b(amqps?:\/\/)\S*@/gi, '$1***@')
}

/** Human text of an unknown thrown value, credentials redacted. */
export function reasonOf(error: unknown): string {
  return redactText(error instanceof Error ? error.message : String(error))
}

/**
 * Build the internal emitter from the `log` option:
 * a function → called with every event (its own throws are swallowed);
 * `false` → silent; `undefined` → default sink: `warn`/`error` as JSON lines to
 * stderr, `info`/`debug` dropped.
 */
export function makeEmitter(
  log: ((event: ConeyLogEvent) => void) | false | undefined,
  stderr: { write(chunk: string): unknown } = process.stderr,
): LogEmitter {
  if (log === false) return () => {}
  if (log !== undefined) {
    return (event) => {
      try {
        log(event)
      } catch {
        // A throwing sink must never take down the connection (or the process).
      }
    }
  }
  return (event) => {
    if (event.level === 'warn' || event.level === 'error') {
      try {
        stderr.write(`${JSON.stringify(event)}\n`)
      } catch {
        // stderr gone — nothing sensible left to do.
      }
    }
  }
}

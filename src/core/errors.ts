/**
 * Error classes thrown by the library. `code` mirrors the class so callers may
 * `switch (err.code)` without `instanceof`.
 */

import { reasonOf } from './log.js'

export type ConeyErrorCode = 'UNAVAILABLE' | 'CLOSED' | 'PUBLISH' | 'SCHEMA' | 'TOPOLOGY' | 'CONFIG'

/** Base class of every error thrown by the library (user handler errors are never wrapped). */
export class ConeyError extends Error {
  readonly code: ConeyErrorCode
  constructor(code: ConeyErrorCode, message: string, options?: { cause?: unknown }) {
    super(message, options)
    this.name = new.target.name
    this.code = code
  }
}

/** No broker connection within the allowed time, or the reconnect policy gave up. */
export class ConeyUnavailableError extends ConeyError {
  declare readonly code: 'UNAVAILABLE'
  constructor(message: string, options?: { cause?: unknown }) {
    super('UNAVAILABLE', message, options)
  }
}

/** Operation attempted on a closing/closed client. */
export class ConeyClosedError extends ConeyError {
  declare readonly code: 'CLOSED'
  constructor(message: string, options?: { cause?: unknown }) {
    super('CLOSED', message, options)
  }
}

/** The broker refused (nacked) a publish. */
export class ConeyPublishError extends ConeyError {
  declare readonly code: 'PUBLISH'
  constructor(message: string, options?: { cause?: unknown }) {
    super('PUBLISH', message, options)
  }
}

/** A body failed the def's schema on the publishing side. `cause` is the schema's own error. */
export class ConeySchemaError extends ConeyError {
  declare readonly code: 'SCHEMA'
  declare readonly cause: unknown
  constructor(message: string, options?: { cause?: unknown }) {
    super('SCHEMA', message, options)
  }
}

/** Queue/exchange assertion failed — typically AMQP 406 PRECONDITION_FAILED when an existing object has different arguments. */
export class ConeyTopologyError extends ConeyError {
  declare readonly code: 'TOPOLOGY'
  /** Wire name of the offending queue or exchange. */
  readonly target: string
  /** AMQP reply code when available (e.g. 406). */
  readonly amqpCode?: number
  constructor(message: string, info: { target: string; amqpCode?: number; cause?: unknown }) {
    super('TOPOLOGY', message, info.cause !== undefined ? { cause: info.cause } : undefined)
    this.target = info.target
    if (info.amqpCode !== undefined) this.amqpCode = info.amqpCode
  }
}

/** Invalid options or definitions; always thrown synchronously, never on the network path. */
export class ConeyConfigError extends ConeyError {
  declare readonly code: 'CONFIG'
  constructor(message: string, options?: { cause?: unknown }) {
    super('CONFIG', message, options)
  }
}

/** Throw from a handler to dead-letter immediately, skipping the ladder. `x-reason` = `unrecoverable: <message>`. */
export class UnrecoverableError extends Error {
  constructor(message: string, options?: { cause?: unknown }) {
    super(message, options)
    this.name = 'UnrecoverableError'
  }
}

/** Normalize an unknown thrown value into an Error (amqplib callbacks may reject with anything). */
export function toError(err: unknown): Error {
  return err instanceof Error ? err : new Error(String(err))
}

/**
 * Build a {@link ConeyTopologyError} from a raw broker error:
 * `<action> "<target>": <redacted reason>`, AMQP reply code extracted when present.
 */
export function topologyError(action: string, target: string, err: unknown): ConeyTopologyError {
  const amqpCode = (err as { code?: unknown }).code
  return new ConeyTopologyError(`${action} "${target}": ${reasonOf(err)}`, {
    target,
    ...(typeof amqpCode === 'number' ? { amqpCode } : {}),
    cause: err,
  })
}

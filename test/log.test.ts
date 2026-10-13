import { describe, it } from 'node:test'
import assert from 'node:assert/strict'
import { makeEmitter, redactText, redactUrl, type ConeyLogEvent } from '../src/core/log.js'

const warnEvent: ConeyLogEvent = { event: 'coney.disconnected', level: 'warn', reason: 'boom' }
const infoEvent: ConeyLogEvent = { event: 'coney.connected', level: 'info', url: 'amqp://h' }

describe('redactUrl', () => {
  it('masks the password of a parseable URL', () => {
    assert.equal(
      redactUrl('amqp://lorvel:s3cr3t@rabbit-1:5672/lorvel'),
      'amqp://lorvel:***@rabbit-1:5672/lorvel',
    )
    assert.equal(redactUrl('amqps://u:p@h/%2f'), 'amqps://u:***@h/%2f')
  })

  it('leaves URLs without credentials untouched', () => {
    assert.equal(redactUrl('amqp://rabbit-1:5672/lorvel'), 'amqp://rabbit-1:5672/lorvel')
  })

  it('masks a whole password-less userinfo — token brokers put the secret there', () => {
    assert.equal(redactUrl('amqp://user@rabbit-1'), 'amqp://***@rabbit-1')
    assert.equal(
      redactUrl('amqps://eyJhbGciOiJIUzI1NiJ9@rabbit.example.com/vhost'),
      'amqps://***@rabbit.example.com/vhost',
    )
    // The two redactors must not disagree about the same string.
    assert.equal(redactUrl('amqp://user@rabbit-1'), redactText('amqp://user@rabbit-1'))
  })

  it('falls back to the text redactor for unparseable input', () => {
    assert.equal(
      redactUrl('not a url with amqp://u:pw@h inside'),
      'not a url with amqp://***@h inside',
    )
  })
})

describe('redactText', () => {
  it('masks the whole userinfo embedded in error messages', () => {
    assert.equal(
      redactText('connect ECONNREFUSED amqp://lorvel:secret@host:5672/vhost failed'),
      'connect ECONNREFUSED amqp://***@host:5672/vhost failed',
    )
    assert.equal(redactText('no url here'), 'no url here')
  })

  it('cannot be defeated by an empty username or an unencoded @ in the password', () => {
    assert.equal(
      redactText('cannot reach amqp://:secret@host:5672'),
      'cannot reach amqp://***@host:5672',
    )
    assert.equal(
      redactText('cannot reach amqp://user:p@ss@host:5672'),
      'cannot reach amqp://***@host:5672',
    )
    // No credentials, no '@' — untouched.
    assert.equal(
      redactText('cannot reach amqp://host:5672/vhost'),
      'cannot reach amqp://host:5672/vhost',
    )
  })
})

describe('makeEmitter', () => {
  it('default sink writes warn/error as JSON lines and drops info/debug', () => {
    const lines: string[] = []
    const emit = makeEmitter(undefined, { write: (chunk: string) => lines.push(chunk) })
    emit(infoEvent)
    emit(warnEvent)
    assert.equal(lines.length, 1)
    assert.deepEqual(JSON.parse(lines[0] as string), warnEvent)
    assert.ok((lines[0] as string).endsWith('\n'))
  })

  it('false silences everything', () => {
    const emit = makeEmitter(false, {
      write: () => {
        throw new Error('must not be called')
      },
    })
    emit(warnEvent)
  })

  it('a custom sink gets every event and its throws are swallowed', () => {
    const seen: ConeyLogEvent[] = []
    const emit = makeEmitter((event) => {
      seen.push(event)
      throw new Error('sink bug')
    })
    emit(infoEvent)
    emit(warnEvent)
    assert.deepEqual(
      seen.map((event) => event.event),
      ['coney.connected', 'coney.disconnected'],
    )
  })

  it('a broken default stream is swallowed too', () => {
    const emit = makeEmitter(undefined, {
      write: () => {
        throw new Error('EPIPE')
      },
    })
    emit(warnEvent)
  })
})

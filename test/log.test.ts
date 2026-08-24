import { describe, it } from 'node:test'
import assert from 'node:assert/strict'
import { makeEmitter, redactText, redactUrl, type ConeyLogEvent } from '../src/log.js'

const warnEvent: ConeyLogEvent = { event: 'coney.disconnected', level: 'warn', reason: 'boom' }
const infoEvent: ConeyLogEvent = { event: 'coney.connected', level: 'info', url: 'amqp://h' }

describe('redactUrl', () => {
  it('masks the password of a parseable URL', () => {
    assert.equal(redactUrl('amqp://lorvel:s3cr3t@home-dev:5672/lorvel'), 'amqp://lorvel:***@home-dev:5672/lorvel')
    assert.equal(redactUrl('amqps://u:p@h/%2f'), 'amqps://u:***@h/%2f')
  })

  it('leaves URLs without credentials untouched', () => {
    assert.equal(redactUrl('amqp://home-dev:5672/lorvel'), 'amqp://home-dev:5672/lorvel')
    assert.equal(redactUrl('amqp://user@home-dev'), 'amqp://user@home-dev')
  })

  it('falls back to the text redactor for unparseable input', () => {
    assert.equal(redactUrl('not a url with amqp://u:pw@h inside'), 'not a url with amqp://u:***@h inside')
  })
})

describe('redactText', () => {
  it('masks credentials embedded in error messages', () => {
    assert.equal(
      redactText('connect ECONNREFUSED amqp://lorvel:secret@host:5672/vhost failed'),
      'connect ECONNREFUSED amqp://lorvel:***@host:5672/vhost failed',
    )
    assert.equal(redactText('no url here'), 'no url here')
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

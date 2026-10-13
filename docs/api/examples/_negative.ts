// Negative litmus: every line below MUST be a type error (tsc flags unused @ts-expect-error).
import { z } from 'zod'
import { createConey, defineQueue, defineTopic, type ConeyLogEvent } from 'coney'
import { EmbedKu, Events } from './defs.js'

const mq = createConey({ url: 'amqp://localhost' })

// wrong body shape
// @ts-expect-error kuId must be a string
await mq.queue(EmbedKu).publish({ kuId: 42 })
// @ts-expect-error missing routing key
await mq.topic(Events).publish({ entity: 'task', action: 'updated', project_id: 'x', id: 'y' })

// binding a topic whose payload type differs from the queue's
const Other = defineTopic({ name: 'other', schema: z.object({ foo: z.number() }) })
// @ts-expect-error payload types differ
defineQueue({ name: 'q', schema: z.object({ bar: z.string() }), bindings: [{ topic: Other, patterns: ['#'] }] })

// handler body is typed — no implicit any
await mq.queue(EmbedKu).consume(async (msg) => {
  // @ts-expect-error no such field
  msg.body.taskId
  // @ts-expect-error messages are read-only
  msg.attempt = 2
})

// log events are a closed union
const sink = (e: ConeyLogEvent): void => {
  if (e.event === 'coney.message_retried') e.delayMs.toFixed()
  // @ts-expect-error unknown event name
  if (e.event === 'coney.nope') return
}
void sink

// consume options are typed
// @ts-expect-error no noAck option (removed on purpose)
await mq.queue(EmbedKu).consume(async () => {}, { noAck: true })

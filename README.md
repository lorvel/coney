# coney

> **Status: in development (0.x, unpublished).** The v0.1 design is locked — see [docs/design.md](docs/design.md) — and the whole surface is implemented: connection core, consuming with the broker-held retry ladder, topic pub/sub, dead-letter ops and the in-memory test double, verified by a unit suite and a broker-backed integration suite. The contract is [docs/api/coney.d.ts](docs/api/coney.d.ts) — CI keeps the built declarations identical to it. Publishing to npm arrives with the release slice.

An opinionated RabbitMQ toolkit for Node.js.

- **Work queues** with a retry ladder held on the broker — shared TTL buckets, no head-of-line blocking, no plugins — and a per-queue dead-letter queue with failure reasons and `replayDead()`.
- **Publisher confirms** end to end: `publish()` resolves only when the broker confirmed; `close()` waits for the tail, so batch scripts need no flush barrier.
- **Automatic reconnection**, initial connect included: topology replay and consumer re-attach, one backoff policy.
- **Graceful drain**: `close({ drainMs })` cancels consumers, waits for in-flight handlers, returns `{ drained, inFlight }`.
- **Topic pub/sub**: per-instance exclusive subscriptions, or durable bound queues with the full retry machinery.
- **Typed definitions** (`defineQueue` / `defineTopic` with any structural `schema` — zod fits as-is), a typed log-event stream, and an in-memory test double (`createConeyMock`).

## Install

```sh
yarn add @lorvel/coney amqplib
```

`amqplib >= 1.1 < 3` is a peer dependency. Requires Node ≥ 20.19 (ESM-only package; `require()` works from 20.19 up). Until the public release the package lives in a private GitLab registry.

## Quickstart

```ts
import { createConey, defineQueue } from '@lorvel/coney'
import { z } from 'zod'

const EmbedKu = defineQueue({
  name: 'ku.embed',
  schema: z.object({ kuId: z.string().uuid() }),
  retry: { delays: [10_000, 60_000, 300_000] }, // 3 retries, then ku.embed.dead
})

const mq = createConey({ url: env.RABBITMQ_URL, prefix: 'lorvel', name: 'worker' })

await mq.queue(EmbedKu).consume(
  async (msg) => embed(msg.body.kuId, { signal: msg.signal }), // resolve → ack · throw → retry · UnrecoverableError → dead
  { prefetch: 4 },
)
await mq.queue(EmbedKu).publish({ kuId }) // resolves on broker confirm
```

## Topics

```ts
import { defineTopic } from '@lorvel/coney'

const Events = defineTopic({ name: 'events', schema: DomainEvent })

// Per-instance listener: exclusive server-named queue, acked after the handler,
// a throw is dropped (logged) — resets on reconnect, missed messages are lost.
const sub = await mq.topic(Events).subscribe(['task.*', 'ku.*'], async (msg) => refresh(msg.body))
await sub.cancel() // survives reconnects until cancelled

// Durable subscriber: a queue def WITH bindings — competing consumers and the
// full retry/dead machinery for topic traffic.
const Notify = defineQueue({
  name: 'events.notify',
  schema: DomainEvent,
  retry: { delays: [10_000, 60_000] },
  bindings: [{ topic: Events, patterns: ['task.*'] }],
})

await mq.topic(Events).publish('task.updated', event) // no matching binding → confirmed and dropped
```

## Dead-letter ops

```ts
const jobs = mq.queue(EmbedKu)
await jobs.inspect() // { ready, consumers, dead }
await jobs.replayDead({ limit: 100 }) // dead → main queue; attempt reset, messageId kept
await jobs.purgeDead()
await mq.inspectRetry() // depth of every shared retry bucket, keyed by duration label
```

## Testing your services

`createConeyMock()` implements the same `Coney` interface in memory: `publish`
records into `mock.published`, and `deliver` / `deliverTopic` run the REAL
consumer pipeline (schema gate, ladder arithmetic, `timeoutMs`), returning the
outcome (`ack` / `retry` / `dead` / `dropped`):

```ts
import { createConeyMock } from '@lorvel/coney'

const mock = createConeyMock({ prefix: 'lorvel' })
await mock.queue(EmbedKu).consume(handler)
const outcome = await mock.deliver(EmbedKu, { kuId }, { attempt: 2 })
// e.g. { outcome: 'retry', attempt: 2, delayMs: 60_000, error } — straight from the ladder
```

## Migrating from 2.x

coney 3.x is a ground-up rewrite and is **not** compatible with coney 2.x, neither on the API nor on the wire (2.x per-message-TTL retry queues become shared, delay-keyed buckets). A full migration guide ships with the public release; until then, treat 3.x as a new library.

## License

MIT © Tu Tran

# coney

> **Status: design phase (0.x, unpublished).** The v0.1 design is locked — see [docs/design.md](docs/design.md). The API surface is already real: [docs/api/coney.d.ts](docs/api/coney.d.ts) and the [examples](docs/api/examples) type-check today (`yarn typecheck:design`).

An opinionated RabbitMQ toolkit for Node.js.

- **Work queues** with a retry ladder held on the broker — shared TTL buckets, no head-of-line blocking, no plugins — and a per-queue dead-letter queue with failure reasons and `replayDead()`.
- **Publisher confirms** end to end: `publish()` resolves only when the broker confirmed; `close()` waits for the tail, so batch scripts need no flush barrier.
- **Automatic reconnection**, initial connect included: topology replay and consumer re-attach, one backoff policy.
- **Graceful drain**: `close({ drainMs })` cancels consumers, waits for in-flight handlers, returns `{ drained, inFlight }`.
- **Topic pub/sub**: per-instance exclusive subscriptions, or durable bound queues with the full retry machinery.
- **Typed definitions** (`defineQueue` / `defineTopic` with any structural `schema` — zod fits as-is), a typed log-event stream, and an in-memory test double (`createConeyMock`).

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

Requires Node ≥ 20.19 (ESM-only package; `require()` works from 20.19 up). `amqplib >= 1.1 < 3` is a peer dependency.

coney 3.x is a ground-up rewrite and is **not** compatible with coney 2.x, neither on the API nor on the wire. A migration guide ships with the public release.

MIT © Tu Tran

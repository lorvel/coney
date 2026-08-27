# coney

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

No broker at hand? `docker compose -f docker-compose.test.yml up -d --wait` starts a throwaway RabbitMQ on `amqp://guest:guest@localhost:5673`.

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

## How it works

One `Coney` per process: one AMQP connection, one confirm channel shared by all publishes, one confirm channel per consumer (so `prefetch` is truly per consumer, and a channel-level failure cannot take down the others).

Every name on the broker is the logical name under the client's optional `prefix`: queue `lorvel.ku.embed`, its dead queue `lorvel.ku.embed.dead`, retry buckets `lorvel.retry.<duration>`, topic exchange `lorvel.events`. Without a prefix, names are used as-is.

**Work queue** (names shown with `prefix: 'lorvel'`):

```mermaid
flowchart LR
  P[Publisher] -->|default exchange, confirm| Q[lorvel.ku.embed]
  Q --> C[Consumer]
  C -->|resolve| A[ack]
  C -->|throw, attempt k ≤ n| B[lorvel.retry.1m<br/>shared bucket: fanout + TTL queue]
  B -->|TTL expires| Q
  C -->|UnrecoverableError · schema · exhausted| D[lorvel.ku.embed.dead]
  D -->|replayDead| Q
```

Retries live in **shared buckets keyed by the delay value**: one fanout exchange + one TTL queue per _distinct_ delay across all your queues (`retry: { delays: [10_000, 60_000] }` → buckets `retry.10s` and `retry.1m`). An expired message is dead-lettered to the default exchange with the routing key it was published with — the consuming queue's own name — so it returns exactly where it came from. Constant broker footprint, exact timing, no head-of-line blocking (a bucket holds messages of a single TTL), and no plugins. Dead letters stay **per queue** (`<name>.dead`) with an `x-reason` header (`schema: …`, `unrecoverable: …`, `exhausted after N attempts: …`, `decode: …`).

**Topics** are plain topic exchanges. `subscribe()` binds an exclusive server-named queue (per instance, reset on reconnect); a queue def with `bindings` is a durable subscriber — competing consumers, retry ladder and dead queue included:

```mermaid
flowchart LR
  W[Writers] -->|task.updated| X[lorvel.events<br/>topic exchange]
  X -->|task.* ku.*| E1[exclusive queue<br/>instance A]
  X -->|task.* ku.*| E2[exclusive queue<br/>instance B]
  X -->|task.*| N[lorvel.events.notify<br/>durable queue def with bindings]
```

On the wire the body is plain JSON — no envelope — so foreign producers and consumers interoperate; coney sets standard AMQP properties (`messageId`, `timestamp`, `appId`, `type`) plus `x-*` headers for retry bookkeeping. The full wire contract is in [docs/design.md §3](docs/design.md).

## Handler outcomes

The outcome of a delivery is implicit in how the handler settles — there is no manual `ack`/`nack`:

| What happens                         | Outcome                                                                              |
| ------------------------------------ | ------------------------------------------------------------------------------------ |
| body is not valid JSON               | dead, `x-reason: decode: …`                                                          |
| `schema.parse` throws                | dead, `x-reason: schema: …` — the retry ladder is **not** consumed                   |
| handler resolves                     | ack                                                                                  |
| handler throws `UnrecoverableError`  | dead immediately, `x-reason: unrecoverable: …`                                       |
| handler throws / `timeoutMs` expires | retry: published to the bucket for `delays[attempt - 1]`, then the original is acked |
| …and the ladder is exhausted         | dead, `x-reason: exhausted after N attempts: …`                                      |

Invariants worth knowing:

- The retry/dead publish is **confirmed before the original message is acked**. A crash in between yields a duplicate, never a loss — delivery is at-least-once, so handlers must be idempotent.
- `timeoutMs` fails the message immediately (retry/dead as above) and aborts `msg.signal`; the handler's eventual settlement is only logged. Pass `msg.signal` to your fetch/db calls to actually stop the work.
- Attempt counting is broker-held (`x-retries` header): `msg.attempt` is 1 on first delivery and survives reconnects and process restarts. Broker redeliveries (unacked messages after a connection loss) do not increment it.

## Reconnection

Reconnection is built on amqplib's `recovery` mode with one backoff policy (defaults: 1 s initial, ×2, ±20 % jitter, 30 s cap, infinite attempts) that also covers the **initial** connect — a service booting while the broker restarts converges on its own; `open({ timeoutMs })` rejects when you want a boot deadline, but the loop keeps going until `close()`. After every (re)connect the library replays the registered topology and re-attaches every consumer. Exclusive subscription queues cannot survive the connection; they are re-bound as new queues (messages published in the gap are lost — use a durable bound queue when that matters).

While disconnected, `publish()` waits for the connection **up to a bounded budget** (`publishTimeoutMs`, default 5 s, per-call override) and then rejects with `ConeyUnavailableError`. That budget is ONE deadline for the whole pipeline — the connection wait, a first-use topology assert, back-pressure **and the broker confirm** — so a broker that blocks the connection under a resource alarm (accepting frames, confirming nothing) fails the publish instead of parking it forever. `0` therefore means _fail at once_ literally: a confirm always needs some budget, so use a small positive value if you want fail-fast-while-disconnected with a real confirm wait. Nothing is ever buffered in memory beyond that wait: a resolved `publish()` means the broker confirmed, a rejected one means it may not have — there is no third state where the library holds your message and a crash silently drops it.

Every connection-dependent admin call (`declare`, `inspectRetry`, `inspect`, `replayDead`, `purgeDead`) takes the same escape hatch through `{ timeoutMs }`. Without it they wait for the connection as long as the reconnect policy allows, exactly like `open()` with no deadline.

`close({ drainMs })` shuts down in order: stop accepting publishes → cancel consumers → wait up to `drainMs` for in-flight handlers (then abort their `signal`) → wait for outstanding publisher confirms → close the connection. It returns `{ drained, inFlight }` and never throws for a drain timeout.

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

## Running the test suites

Unit tests need nothing: `yarn test`. The broker-backed suite runs against any RabbitMQ you point it at and skips when `RABBITMQ_URL` is unset:

```sh
docker compose -f docker-compose.test.yml up -d --wait
RABBITMQ_URL=amqp://guest:guest@localhost:5673 \
RABBITMQ_MGMT_URL=http://guest:guest@localhost:15673 \
yarn test:integration
docker compose -f docker-compose.test.yml down -v
```

The suite creates everything under a disposable `it.<run-id>` prefix and cleans up after itself, so a shared broker stays untouched. `RABBITMQ_MGMT_URL` (the management API) is only needed by the forced-disconnect tests.

## API reference

The authoritative reference is [docs/api/coney.d.ts](docs/api/coney.d.ts) — every export with normative TSDoc (semantics, defaults, thrown errors). CI keeps the built declarations identical to it. The map:

### Client

| Export                                           | What                                                                                                                    |
| ------------------------------------------------ | ----------------------------------------------------------------------------------------------------------------------- |
| `createConey(options)`                           | Create a client — no I/O until `open()` or the first publish/consume. One per process.                                  |
| `coney.queue(def)` / `.topic(def)`               | Typed handles (cached per name); topology is asserted lazily on first use and after every reconnect.                    |
| `coney.open(options?)`                           | Connect now; `{ timeoutMs }` bounds the wait. Idempotent.                                                               |
| `coney.close(options?)`                          | Graceful shutdown; `{ drainMs }` bounds the handler drain. Returns `CloseResult`.                                       |
| `coney.declare(defs, options?)`                  | Eagerly assert topology to fail fast at boot instead of at the first publish. `{ timeoutMs }` bounds the whole call.    |
| `coney.inspectRetry(options?)`                   | Depth of every shared retry bucket, keyed by duration label. `{ timeoutMs }` bounds the whole call.                     |
| `coney.state` / `.prefix` / `.resolveName(name)` | Lifecycle state (`idle → connecting → open ⇄ reconnecting → closing → closed`), configured prefix, logical → wire name. |

`ConeyOptions`: `url` (required) · `prefix` · `name` (AMQP `appId` + consumer-tag segment) · `log` (one typed callback, see below) · `reconnect` (partial `ReconnectPolicy`) · `publishTimeoutMs` · `retry` (default ladder) · `socketOptions` (passed to amqplib untouched — TLS etc.).

### Definitions

| Export                                              | What                                                                                                                                                        |
| --------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `defineQueue({ name, schema?, retry?, bindings? })` | A work queue as a frozen pure value; `T` is inferred from `schema`. `retry: false` = dead on first failure; `bindings` makes it a durable topic subscriber. |
| `defineTopic({ name, schema? })`                    | A topic exchange as a pure value.                                                                                                                           |
| `Schema<T>`                                         | Structural: anything with a throwing `parse(input: unknown): T` — zod, valibot parsers, arktype, or a hand-written guard.                                   |

Defs are client-free: keep them in a shared catalog module and pass them to any client or the mock.

### Queue and topic handles

| Method                                               | What                                                                                                                 |
| ---------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------- |
| `queue.publish(body, options?)`                      | Schema-validated, confirm-backed publish. `PublishOptions`: `messageId`, `headers`, `persistent`, `timeoutMs`.       |
| `queue.consume(handler, options?)`                   | Attach a consumer on its own channel; `ConsumeOptions`: `prefetch` (default 1), `timeoutMs`. Returns `Subscription`. |
| `queue.inspect(options?)`                            | `{ ready, consumers, dead }` — passive, no topology changes.                                                         |
| `queue.replayDead({ limit? })` / `queue.purgeDead()` | Move dead letters back to the main queue (attempt reset, `messageId` kept) / delete them.                            |
| … all four take `{ timeoutMs }`                      | ONE budget for the call (connection wait + round-trips); `replayDead` stops between messages, keeping what it moved. |
| `topic.publish(routingKey, body, options?)`          | Publish to the exchange; no matching binding → confirmed and dropped.                                                |
| `topic.subscribe(patterns, handler, options?)`       | Exclusive per-instance subscription (`*` = one word, `#` = zero or more). Returns `Subscription`.                    |

The handler receives `Message<T>`: `body` (validated), `id`, `attempt`, `redelivered`, `routingKey`, `headers`, `publishedAt?`, `appId?`, `signal`. A `Subscription` has `active` and `cancel({ drainMs? })`.

### Errors

All library errors extend `ConeyError` and carry a stable `code`, so `switch (err.code)` works without `instanceof`:

| Class                   | `code`        | When                                                                        |
| ----------------------- | ------------- | --------------------------------------------------------------------------- |
| `ConeyConfigError`      | `CONFIG`      | Invalid options/defs — always synchronous, never on the network path.       |
| `ConeySchemaError`      | `SCHEMA`      | Body failed the def's schema at publish time (`cause` = schema's error).    |
| `ConeyUnavailableError` | `UNAVAILABLE` | No connection within the allowed time, or the reconnect policy gave up.     |
| `ConeyClosedError`      | `CLOSED`      | Operation on a closing/closed client.                                       |
| `ConeyPublishError`     | `PUBLISH`     | The broker nacked a publish.                                                |
| `ConeyTopologyError`    | `TOPOLOGY`    | Queue/exchange assertion failed (carries `target` and `amqpCode`).          |
| `UnrecoverableError`    | —             | Thrown by **your handler** to dead-letter immediately, skipping the ladder. |

### Log events

`log` is a single typed callback receiving the exhaustive `ConeyLogEvent` union (22 events, fixed level each — connection lifecycle, topology, consumer lifecycle, retry/dead per message, drain). Credentials in URLs are always redacted. One-line adapter for any structured logger:

```ts
createConey({ url, log: (e) => logger[e.level](e.event, e) })
```

Default sink: `warn`/`error` as JSON lines on stderr, the rest dropped; `log: false` silences everything.

### Test double

`createConeyMock({ prefix?, name? })` returns a `ConeyMock` (a full `Coney`): `published` (every recorded publish), `deliver(def, body, options?)` and `deliverTopic(def, routingKey, body, options?)` (run the real pipeline, return `DeliveryOutcome`), `reset()`. `close()` mirrors the real lifecycle — state `'closed'`, registrations deactivated, gated calls reject with `ConeyClosedError`.

## FAQ

**What are these `retry.10s` / `retry.1m` queues and exchanges?**
Shared retry buckets — one fanout exchange + TTL queue per distinct delay used by any queue of this client (with the client's prefix). All queues with a 1-minute step share `retry.1m`. They are cheap, constant in number, and empty when nothing is retrying.

**I changed a ladder's `delays` — do I need to migrate anything?**
No. New delay values simply declare new buckets on first use; messages already sleeping in old buckets drain back to their queues on their original schedule, and the old bucket is left empty (delete it manually if you care). Because bucket names encode the TTL, coney never re-declares an existing queue with different arguments — the classic `406 PRECONDITION_FAILED` of DLX-per-queue setups cannot happen here. A 406 can still occur when a name collides with an object created _outside_ coney with different arguments; it surfaces as `ConeyTopologyError` on the callers of that def and leaves every other consumer untouched.

**What is `prefix` for?**
Namespacing everything a client touches (`lorvel.ku.embed`, `lorvel.retry.1m`, …) so multiple environments or tenants share a broker without a shared naming convention. There is no default — omit it and names are used as-is.

**Can `subscribe()` lose messages?**
Yes, by design, in one window: an exclusive subscription queue dies with the connection, and messages published between the drop and the re-bind are gone (`coney.subscription_reset` tells you when). Use it for signals where the consumer can catch up from state. When every message matters, declare a queue def with `bindings` — durable, competing consumers, retry ladder, dead queue.

**How does `timeoutMs` relate to RabbitMQ's `consumer_timeout`?**
The broker's `consumer_timeout` (default 30 min) force-closes a channel whose delivery stays unacked too long. coney acks only after your handler settles, so keep handler `timeoutMs` well below the broker's limit — with the 30-minute default there is plenty of room, but don't run hour-long handlers on one message.

## Migrating from 2.x

coney 3.x is a ground-up rewrite: **neither the API nor the wire format is compatible** with coney 2.x. The API moves are mechanical:

| coney 2.x                                                                 | coney 3.x                                                                                |
| ------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------- |
| `const conn = await createRabbitMQConnection(url)` then `new Coney(conn)` | `const mq = createConey({ url })` — lazy; `await mq.open()` to connect eagerly           |
| `coney.sendToQueue(name, body)`                                           | `mq.queue(def).publish(body)` — resolves on broker confirm                               |
| `coney.consume(name, { maxRetries, noAck: true }, handler)`               | `mq.queue(def).consume(handler, { prefetch })` — the ladder comes from the def           |
| `noAck: true` (meaning "the library acks for you")                        | gone — implicit ack is the only mode; outcomes come from how the handler settles         |
| retry via one `.requeue` queue with per-message TTL                       | shared delay-keyed buckets — exact timing, no head-of-line blocking                      |
| `maxRetries: 3`                                                           | `retry: { delays: [60_000, 240_000, 540_000] }` — count _and_ spacing, explicit          |
| queue names `coney:que.<name>`, `coney:que.<name>.requeue`                | `[<prefix>.]<name>`, shared `[<prefix>.]retry.<dur>`, per-queue `[<prefix>.]<name>.dead` |

Because the wire names and retry topology differ, 2.x and 3.x must **never consume the same queue**. Cut over per queue, the safe order being: deploy 3.x consumers (new queues exist and are consumed from the start) → switch producers to 3.x → drain what remains in the old `coney:que.*` queues (re-publish or process with the old consumer) → delete the 2.x objects. This exact sequence has been run against a live three-service production deployment (one consumer, two producers) with zero stranded messages, ending in a broker-restart drill where every service reconnected on its own within seconds.

## License

MIT © Tu Tran

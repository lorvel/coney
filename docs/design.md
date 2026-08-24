# @lorvel/coney — v0.1 design specification

## 0. Status

Design spec for `@lorvel/coney` v0.1 (coney 3.x). Review rounds 1–4 (17 decisions) closed with the owner on 2026-08-24. This file is the canonical copy and the contract for the v0.1 implementation; when code and spec disagree, fix one place. §2 is a real declaration file ([api/coney.d.ts](api/coney.d.ts)): it and the examples in §5 type-check under `tsc --strict` (+ `exactOptionalPropertyTypes`, `noUncheckedIndexedAccess`) — run `yarn typecheck:design`; [api/examples/_negative.ts](api/examples/_negative.ts) uses `@ts-expect-error` to prove the types reject wrong bodies, mismatched bindings and removed options (`noAck`). Broker behaviour in §3/§4 was verified on RabbitMQ 4.3.0 with amqplib 1.2.0.

## 1. Architecture

**What the library is**: an opinionated, thin layer over `amqplib` that owns the connection, the topology and the message lifecycle. Applications describe *queues/topics + payload + retry ladder* as pure values and write handlers. Generic by construction: no env access, no validation-library dependency, no tenant names in `src/`.

**Process model** — one `Coney` per process: **one AMQP connection**, **one ConfirmChannel shared by all publishes** (confirms arrive in order → a single pending map), **one ConfirmChannel per consumer/subscription** (prefetch is per channel; a channel-level failure such as 406 cannot take down other consumers; retry/dead publishes go through the consuming channel so they are confirmed before the original is acked).

**Modules (`src/`)** — `client.ts` (createConey, state, open/close, handle cache) · `connection.ts` (adapter over amqplib `recovery`: policy mapping, `setup(model)`, state + log events, `handler-error`) · `topology.ts` (registry of used defs; assert queue/exchange/binding/bucket/dead; replay on every `setup`) · `publisher.ts` (confirm channel, bounded wait, `drain` back-pressure, pending confirms) · `consumer.ts` (channel per consumer; decode → schema → handler with `AbortSignal`; outcome → ack/retry/dead; timeout + late-result guard; re-attach) · `subscription.ts` (exclusive queue, bind, ack-after-handler, reset) · `retry.ts` (ladder arithmetic, bucket naming, duration labels) · `codec.ts` (JSON + property/header mapping) · `define.ts` (defs, name rules, freezing) · `errors.ts` · `log.ts` (default sink, URL redaction) · `mock.ts` · `index.ts`.

**Client state machine**

```mermaid
stateDiagram-v2
  [*] --> idle: createConey()
  idle --> connecting: open() | first publish/consume
  connecting --> open: connect + setup ok
  connecting --> connecting: attempt fails (backoff)
  open --> reconnecting: connection lost
  reconnecting --> open: reconnect + setup ok
  reconnecting --> reconnecting: attempt fails (backoff)
  connecting --> closed: maxAttempts exhausted
  open --> closing: close()
  reconnecting --> closing: close()
  closing --> closed: drained or drainMs elapsed
```

**Work-queue message flow** (names shown with `prefix: 'lorvel'`)

```mermaid
flowchart LR
  P[Publisher] -->|default exchange, confirm| Q[lorvel.ku.embed]
  Q --> C[Consumer runtime]
  C -->|resolve| A[ack]
  C -->|throw, attempt k ≤ n| B[lorvel.retry.&lt;delays k&gt;<br/>shared bucket: fanout + queue, TTL]
  B -->|TTL expires → DLX '' + original routing key| Q
  C -->|UnrecoverableError · schema · exhausted| D[lorvel.ku.embed.dead]
  D -->|replayDead| Q
```

**Topic flow**

```mermaid
flowchart LR
  W[Writers] -->|routing key task.updated| X[lorvel.events<br/>topic exchange]
  X -->|bind task.* ku.*| E1[amq.gen-… exclusive<br/>instance A, ack after handler, no retry]
  X -->|bind task.* ku.*| E2[amq.gen-… exclusive<br/>instance B]
  X -->|bind task.*| N[lorvel.events.notify<br/>durable queue def with bindings → full retry/dead]
```

## 2. API surface

The complete public API lives in [api/coney.d.ts](api/coney.d.ts) — every export, with normative TSDoc (semantics, defaults, thrown errors). Nothing in it references amqplib types, so consumers never need `@types/amqplib`. The implementation must match these signatures exactly; the README API reference derives from this file.
## 3. Topology & wire

**Objects** (wire name = `<prefix>.<name>` when a prefix is set; `durable: true` everywhere; no `autoDelete` except exclusive subscription queues)

| Object | Count | Declared with | Args |
|---|---|---|---|
| `<q>` main queue | 1 per def | `assertQueue` | none (classic) |
| `<q>.dead` | 1 per def | `assertQueue` | none |
| `<prefix>.retry.<dur>` exchange | 1 per distinct delay in the client | `assertExchange` type `fanout` | — |
| `<prefix>.retry.<dur>` queue, bound to the exchange of the same name (key `''`) | same | `assertQueue` | `x-message-ttl: <ms>`, `x-dead-letter-exchange: ''` — **no** `x-dead-letter-routing-key` |
| `<t>` topic exchange | 1 per def | `assertExchange` type `topic` | — |
| bound queue bindings | per `bindings[]` entry × pattern | `bindQueue(<q>, <t>, pattern)` | — |
| subscription queue | 1 per `subscribe()` per connection | `assertQueue('', { exclusive: true, autoDelete: true })` + `bindQueue` per pattern | none |

Why the bucket works: without `x-dead-letter-routing-key`, an expired message is dead-lettered with the routing key it was *published* with; a fanout exchange keeps that key, so `DLX ''` (default exchange) delivers it to the queue named by the key. The retry publish therefore uses **the consuming queue's wire name as routing key** — never `fields.routingKey` (for bound queues that is the topic key). Verified on 4.3.0: two messages in one bucket with different keys returned to their own queues after exactly the TTL, headers intact, `x-death[0] = { reason: 'expired', 'routing-keys': [<q>] }` added by the broker.

**Duration label** `<dur>`: from ms, largest units first, zero units omitted: `500ms`, `10s`, `1m`, `1m30s`, `2h30m`. Deterministic, so clients with the same delay share the bucket.

**Reserved names**: logical names must not start with `retry.` or `amq.` and must not end with `.dead` (checked by `defineQueue`/`defineTopic`/`createConey`).

**Message on the wire** — body is the JSON encoding of exactly what was published (`contentType: application/json`, UTF-8, no envelope). Properties:

| Property / header | Set by | Value |
|---|---|---|
| `messageId` | publish | `PublishOptions.messageId` or UUID v4; preserved through retry/dead/replay |
| `timestamp` | publish | unix seconds |
| `type` | publish | queue: logical queue name · topic: routing key |
| `appId` | publish | `ConeyOptions.name` when set |
| `deliveryMode` | publish | `2` unless `persistent: false` |
| `x-published-at` | publish | ISO-8601 ms |
| user headers | publish | `PublishOptions.headers` (keys starting `x-` rejected with `ConeyConfigError`) |
| `x-retries` | retry | number of failed attempts so far (`attempt = x-retries + 1`) |
| `x-retry-at` | retry | ISO-8601 — when the bucket will return the message |
| `x-reason` | dead | `schema: <msg>` · `unrecoverable: <msg>` · `exhausted after <n> attempts: <msg>` · `decode: <msg>` — truncated to 500 chars |
| `x-failed-at` | dead | ISO-8601 |
| `x-replayed` | replayDead | count of replays (informational) |
| `x-death`, `x-first-death-*`, `x-last-death-*` | broker | never read by the library |

## 4. Semantics

**Publish** (`QueueHandle.publish` / `TopicHandle.publish`): (1) `schema.parse(body)` → `ConeySchemaError` (before any I/O); (2) if not connected, wait up to `timeoutMs` (call) / `publishTimeoutMs` (client) for the connection → `ConeyUnavailableError`; while `closing`/`closed` → `ConeyClosedError`; (3) encode, `publish` on the shared ConfirmChannel — if amqplib returns `false`, wait for `drain` before resolving the next publish (back-pressure, no unbounded buffer); (4) resolve on broker ack, reject `ConeyPublishError` on nack. Publishes in flight when the connection drops are rejected with `ConeyUnavailableError` (state unknown — the caller may retry; at-least-once). Nothing is ever buffered beyond the wait window. Topic publishes with no matching binding are confirmed and dropped (no `mandatory`).

**Consume** — per delivery on the consumer's channel, `prefetch` bounds concurrent handler calls:

| Step | Failure | Outcome |
|---|---|---|
| decode JSON | invalid | dead, `x-reason: decode` |
| `schema.parse` | throws | dead, `x-reason: schema: …` (retry ladder NOT consumed) |
| handler resolves | — | `ack` |
| handler throws `UnrecoverableError` | — | dead, `x-reason: unrecoverable: <message>` |
| handler throws other / times out | `attempt ≤ delays.length` | publish to bucket `delays[attempt-1]` with `x-retries = attempt`, `x-retry-at`; wait for confirm; then `ack` original → `coney.message_retried` |
| handler throws other / times out | `attempt > delays.length` or `retry: false` | publish to `<q>.dead`; confirm; `ack` → `coney.message_dead` |

Invariants: retry/dead publishes are **confirmed before the original is acked** (no loss inside the library; a crash in between yields a duplicate, never a loss). If that publish fails because the channel is gone, the original cannot be acked either → the broker redelivers with `redelivered: true` and the **same** `attempt` ("free retry"). `timeoutMs`: on expiry the message is failed immediately (retry/dead as above), `signal` is aborted, and the handler's eventual settlement is only logged (`coney.handler_late`) — duplicates are possible and acceptable under the at-least-once contract. Broker-initiated cancel (queue deleted) arrives as a `null` delivery: log `coney.consumer_stopped` and re-attach on the next `setup`. Handler exceptions are always caught; nothing thrown by user code ever reaches amqplib's emitter (a synchronous throw inside an amqplib callback kills the connection — verified).

**Subscriptions** (`TopicHandle.subscribe`): exclusive server-named queue bound to each pattern; consumer tag `<name|coney>.<topic>.<n>`; lib acks after the handler settles; a throw → `coney.subscription_error` + ack (drop); `prefetch` default 1 gives in-order processing. After a reconnect a *new* queue is declared and bound; messages published in between are lost (`coney.subscription_reset`). `cancel()` deletes nothing (exclusive queues vanish with the channel).

**Reconnect** — delegated to amqplib ≥ 1.1 `recovery` (`connect(url, { ...socketOptions, recovery })`, mapping `initialDelayMs→initialDelay`, `maxDelayMs→maxDelay`, `factor`, `jitter`, `maxAttempts→maxRetries`). Verified on 1.2.0: the initial connect retries with the same policy and `connect()` rejects when `maxRetries` is exhausted (`ECONNREFUSED` after 304 ms with 2 retries); on a forced close the model emits `disconnect` → `reconnect-scheduled {attempt, delay}` → runs `setup(model)` again → `connect`; **channels created outside `setup` are dead after a reconnect** → the publisher channel and every consumer channel are (re)created inside `setup`; `createChannel()` called while disconnected waits for the connection (used for the bounded publish wait); `close()` during a reconnect resolves immediately and stops the loop. **`setup` must never throw for configuration problems** — amqplib treats a rejected `setup` as a failed connection and retries forever; coney's `setup` catches per-def assertion errors, logs `coney.topology_failed`, rejects the pending callers of that def with `ConeyTopologyError` and keeps the connection. A 406 closes only the channel that issued the assert (verified; `err.code === 406`). `coney.reconnect_exhausted` → state `closed`, pending publishes/open() reject `ConeyUnavailableError`.

**Close** — order: reject new publishes (`ConeyClosedError`) → `basic.cancel` every consumer/subscription → wait ≤ `drainMs` for in-flight handlers (then abort their `signal`) → `waitForConfirms` on the publisher → `connection.close()`. Returns `{ drained, inFlight }`, never throws for a drain timeout. Idempotent; safe while disconnected.

**Schema** — `Schema<T>` is structural (`parse(unknown): T`, throwing). Validated at publish (`ConeySchemaError`, `cause` = the schema's error) and on delivery (dead). Consumers should prefer non-strict object schemas so producers can add fields first.

**Mock** (`createConeyMock`) — same interface; `publish` validates + records (`target.name` is the resolved wire name); `consume`/`subscribe` register handlers; `deliver` runs the consumer pipeline in-process and returns the outcome including ladder arithmetic (`delayMs = delays[attempt-1]`), `timeoutMs` and the schema gate; `deliverTopic` matches AMQP patterns (`*`, `#`) against subscriptions and bound queue defs. `state` is `'open'`; `open`/`close`/`inspect*`/`replayDead`/`purgeDead` resolve with empty values.

**Log events** — the exhaustive catalog is the `ConeyLogEvent` union in §2 (21 events, fixed level each). `url` fields are redacted (`amqp://user:***@host/vhost`). **Errors** — `ConeyError` subclasses in §2; `code` mirrors the class so callers may `switch (err.code)` without `instanceof`.

**Gotchas** — (1) per-message TTL only expires at the head of a queue → never mix TTLs in one queue; (2) `x-message-ttl` is immutable → bucket names encode the TTL; (3) no `x-expires` on buckets ("unused" = no consumers, and buckets have none); (4) `consumer_timeout` (30 min default; 4.3: quorum only) caps unacked time — keep `timeoutMs` well below; (5) management-API connection listing lags ~5 s — force-close tests must poll.

## 5. Examples

Five real consumer flows written against §2, type-checked with it — see [api/examples/](api/examples/):

- [`defs.ts`](api/examples/defs.ts) — the shared catalog: queue/topic defs as pure values (zod schemas, retry ladders, a durable subscriber via `bindings`).
- [`worker.ts`](api/examples/worker.ts) — a job worker: two consumers, `prefetch`, handler `timeoutMs`, `UnrecoverableError`, graceful drain on SIGTERM.
- [`publisher.ts`](api/examples/publisher.ts) — best-effort enqueue from a request handler: confirm-backed `publish` in try/catch, background `open()`.
- [`backfill.ts`](api/examples/backfill.ts) — a one-off producer: publish N then `close()` without losing the tail (no manual flush barrier).
- [`test-double.ts`](api/examples/test-double.ts) — `createConeyMock()` in a service test suite: recorded publishes, in-process delivery outcomes, topic pattern matching.
- [`events.ts`](api/examples/events.ts) — topic pub/sub: signal publishing (`persistent: false`), per-instance exclusive subscription (SSE fan-out), durable bound notifier queue.

[`logger.ts`](api/examples/logger.ts) is a stand-in for a service logger; [`_negative.ts`](api/examples/_negative.ts) holds the `@ts-expect-error` cases.

## 6. Decision log

Each entry: **decision — why — rejected**. Owner-approved 2026-08-24 unless marked *default*.

1. **API shape: handle style** (`defineQueue`/`defineTopic` pure defs; `mq.queue(def)`/`mq.topic(def)` cached handles) — types flow from the def, ops methods hang off the handle, defs live in a catalog without a client — rejected flat `mq.publish(def, body)` (client grows per op) and merged `mq.queue({...})` (def tied to a client).
2. **Retry: shared buckets keyed by delay value** — only design that is broker-held, exact, ladder-capable and constant-size (K = distinct delays); matches MassTransit 9.1 `UseQueueBasedDelayedRedelivery`, NServiceBus (28 binary levels), EasyNetQ, Symfony — rejected per-level-per-queue (M×n queues), one `.retry` + per-message TTL (coney 2: head-of-line blocking), bounce via main queue (each hop queues behind the backlog), park in worker RAM (Celery/Dramatiq/Rascal: memory cliff + `consumer_timeout`), constant per-queue delay (Sneakers/Spring: no ladder), delayed-message-exchange plugin (archived 2026-04-16, Mnesia removed in RabbitMQ 4.3). Dead stays per-queue (alerting + replay ergonomics). DLX targets the default exchange (0 bindings) instead of MassTransit's `mt-delay-return`.
3. **Publish while disconnected: bounded wait** (`publishTimeoutMs` 5000, per-call override, `0` = fail fast); promise resolves only on broker confirm — rejected unbounded in-memory buffering (amqp-connection-manager: a crash loses what the caller believed sent).
4. **Lifecycle: lazy + explicit `open()`**, one reconnect policy for first and later connects, `open({timeoutMs})` rejects but the loop continues, `consume()` resolves when attached — rejected fail-on-first-connect (coney 2: a service booting during a broker restart never reconnects).
5. **Wire: plain JSON body + AMQP properties/headers** — interop with foreign producers; versioning is the application's concern — rejected body envelope `{v,type,data}` (framework-style; MassTransit needed a raw-JSON mode for interop).
6. **Handler `timeoutMs` = abort + fail now**, late result ignored — frees the prefetch slot; at-least-once already requires idempotency — rejected abort-only (ignored signal holds the slot; quorum `consumer_timeout`) and no timeout.
7. **`close()` returns `{drained, inFlight}`**, never throws on drain timeout — rejected `ConeyDrainTimeoutError` (Rascal `closeTimeout`).
8. **Implicit ack only** (`return`/`throw`/`UnrecoverableError`) — rejected `msg.ack()/retry()/dead()` (double-ack hazards; Rascal `ackOrNack`).
9. **Schema validated on both sides** — bugs surface at the producer; rejected consume-only.
10. **Durable subscribers are queue defs with `bindings`**; `subscribe()` is exclusive-only — queue = durable unit of work, subscription = ephemeral listener (rabbitmq-client, Rascal, MassTransit) — rejected `subscribe({ name })` mode switch.
11. **Exclusive subscriptions: library acks after the handler, `prefetch` 1** (ordered, bounded); `noAck` is not exposed — rejected AMQP `noAck` fire-and-forget.
12. **Exclusive queues are server-named** with descriptive consumer tags — rejected client-named (`<prefix>.<topic>.sub.<app>.<rand>`: collision handling on reconnect).
13. **`persistent` defaults to `true` for every publish**, opt-out per call — rejected non-persistent default for topics (silent loss for durable bound queues).
14. **Logger = one typed callback** `log(e: ConeyLogEvent)` — one-line adapters for any logger, exhaustive union for metrics (Temporal/OTel diag precedent) — rejected 4-method object (pino needs wrappers) and EventEmitter.
15. **`createConeyMock()` in the main entry** — rejected subpath `@lorvel/coney/testing` and no double.
16. **ESM-only, Node ≥ 20.19** (`require(esm)` unflagged), one `exports` entry — rejected dual CJS/ESM.
17. **Reconnect built on amqplib ≥ 1.1 `recovery`**, coney owns `setup(model)`; spike verified (§4); peer `amqplib >=1.1.0 <3` — fallback to a hand-written loop with the same public API if a future amqplib change breaks the contract; rejected own loop as the default (~150 lines to maintain, and amqplib now also routes listener exceptions via `handler-error`).

*Defaults (not separately approved)*: `prefetch` 1; default ladder `[10s, 1m, 5m]` (total ≈ 6 min; coney 2's `[1m, 4m, 9m]` kept only as a migration example — Lorvel defs set their delays explicitly); names permissive (≤ 200 bytes, case preserved; lowercase dotted names are a Lorvel convention enforced in its catalog, not by the library); `ConeyError.code` strings; `UnrecoverableError` unprefixed (BullMQ precedent); no amqplib types in the public surface.

**Parent OQ-1..11** — OQ-1 catalog per service (`src/mq/defs.ts`), shared package when > 3 queues; the DomainEvent guard in `@lorvel/core` is wrapped as `Schema<T>`. OQ-2 drop `QUEUE_NAME`/`TASK_QUEUE_NAME`/`MAX_RETRIES` env (code contracts), keep `RABBITMQ_URL`/`CONSUMER_PREFETCH`. OQ-3 classic in v0.1; quorum = v0.2 `type` option (`x-queue-type` immutable → new queues + cutover; single-node prod gains nothing today; buckets must share the type). OQ-4 → default ladder above. OQ-5 → 3. OQ-6 `npm deprecate coney@"<3"` at the 3.0.0 publish. OQ-7 → 15. OQ-8 integration disconnect = management API `DELETE /api/connections/<name>` (library sets `clientProperties.connection_name = coney:<name>`), polled until visible. OQ-9 rename `name` + consumer specifiers at OSS time, no dual publish. OQ-10 → 16. OQ-11 GitHub `tutv/coney` public home, GitLab source of truth until then.

**Deferred to v0.2 (API leaves room)**: `RetryLater({ afterMs })` from a handler (bucket on demand, attempt not consumed); `type: 'quorum'`; typed routing keys (second generic on `defineTopic`); delayed publish (same buckets); priority; `mandatory`; OTel headers; pluggable codec; dead-queue CLI; RPC.

## 7. Test outline

**Unit (no broker; fakes of the amqplib surface)** — name rules; duration labels; ladder arithmetic (`retry: false`, `[]`); codec + header mapping + `x-` rejection; schema gate both sides; handler outcomes (resolve/throw/Unrecoverable/timeout/late); `close()` ordering + `{drained,inFlight}`; bounded publish wait and `0`; log sink default + redaction; mock `published`/`deliver`/`deliverTopic` (`*`, `#`).

**Integration (CI broker via `docker-compose.test.yml`, prefix `it.<run-id>`, cleanup after)** — topology idempotent + replay after reconnect; bucket round-trip (two queues, one bucket, key preserved, TTL timing); per-queue dead with each `x-reason`; `replayDead` resets attempt, keeps `messageId`; prefetch = real concurrency; publish N then `close()` → depth N; unroutable topic publish resolves; exclusive fan-out to 2 connections + reset after forced disconnect; bound queue gets retry/dead; 406 → `ConeyTopologyError` on callers, other consumers unaffected, no reconnect storm; forced disconnect → consumers re-attached, in-flight message redelivered with `redelivered: true` and unchanged `attempt`; `open({timeoutMs})` on a closed port; `maxAttempts` exhaustion; `close()` while disconnected; drain timeout aborts `signal`.

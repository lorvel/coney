# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Core client: `createConey` (lazy connect, explicit `open()`), automatic
  reconnection built on amqplib `recovery` with topology replay, graceful
  `close()` that waits for outstanding publisher confirms.
- Definitions: `defineQueue` / `defineTopic` — pure, frozen, schema-typed values.
- Publishing with broker confirms, a bounded wait while disconnected
  (`publishTimeoutMs`), and `drain` back-pressure.
- Topology: shared delay-keyed retry buckets (`<prefix>.retry.<duration>`),
  per-queue dead-letter queues, topic exchanges and bindings, eager `declare()`,
  `inspectRetry()`.
- Typed error classes (`ConeyError` + `code`) and the typed log-event catalog
  (`ConeyLogEvent`) with credential redaction in URLs.
- Consuming: `consume()` on a channel per consumer with per-consumer `prefetch`,
  the implicit-outcome pipeline (resolve → ack, throw → broker-held retry ladder,
  `UnrecoverableError` / schema / decode failures → dead with `x-reason`),
  handler `timeoutMs` (abort + fail now, late results only logged), and automatic
  re-attach after every reconnect. Retry/dead publishes are confirmed before the
  original delivery is acked.
- Topic subscriptions: `subscribe()` on server-named exclusive queues (ack after
  handler, drop on failure, reset on reconnect); durable subscribers via queue
  defs with `bindings`, including the full retry machinery.
- Dead-letter ops: `inspect()`, `replayDead()` (attempt reset, `messageId` kept,
  `x-replayed` incremented), `purgeDead()`.
- `createConeyMock()`: an in-memory `Coney` that records publishes and runs the
  real consumer pipeline in-process (`deliver` / `deliverTopic` with AMQP `*`/`#`
  pattern matching).
- `close({ drainMs })` now cancels consumers, waits for in-flight handlers and
  reports a real `{ drained, inFlight }`.
- A broker-backed integration suite (`yarn test:integration`) covering retry
  timing, shared buckets, dead-letter flows, prefetch, drains, forced
  disconnects and 406 isolation; CI runs it against a RabbitMQ service container.

### Fixed (pre-release hardening review)

- Reconnect could deadlock permanently when a `consume()` was mid-attach at the
  moment the connection dropped (the setup hook awaited a channel promise that
  only resolves after setup returns); setup now skips in-flight attaches, which
  complete on the new connection by themselves.
- `close()` ordering: consumer runtimes are marked inactive first and the setup
  hook skips consumer re-attach while closing, so a reconnect landing inside the
  drain window can no longer resurrect cancelled consumers or run handlers after
  `close()` resolved; a `cancel()` or close racing an in-flight attach now tears
  the late channel down instead of leaving a phantom consumer, and `consume()`'s
  rollback fully disposes a runtime the setup hook had already attached.
- The whole publish pipeline shares one `timeoutMs` budget: a first-use topology
  assert racing a disconnect now rejects within the budget instead of parking
  for the outage (which could also stall `close()`).
- Publisher: back-pressure episodes no longer leak a `close` listener each; a
  channel creation resolving after a reconnect already provided a fresh channel
  is discarded instead of superseding it, and a stale channel's close no longer
  rejects confirms pending on the healthy one; the lazy path no longer registers
  safety listeners twice.
- Every timeout/drain option now rejects non-finite values and values above
  Node's `setTimeout` bound (2^31 − 1 ms) — `Infinity` used to be silently
  clamped to 1 ms, inverting "wait forever" into "fail immediately".
- `close()`/`cancel()` no longer hold the process open for the remainder of the
  drain budget after the handlers already settled (leaked race timer).
- Credential redaction also covers `amqp://` URLs with an empty username or an
  unencoded `@` in the password; non-JSON-serializable bodies (circular, BigInt,
  throwing `toJSON`) throw `ConeyConfigError` instead of a raw `TypeError`; the
  mock's topic matcher is memoized (no exponential backtracking on `#`-heavy
  patterns).
- `createConeyMock().close()` now mirrors the real lifecycle (state `closed`,
  registrations deactivated, gated operations reject with `ConeyClosedError`),
  and a handler cancelling a subscription during `deliverTopic` can no longer
  make later recipients miss the delivery.

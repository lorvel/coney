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

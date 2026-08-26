# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
During 0.x, a MINOR bump may contain breaking changes (called out here); PATCH
releases are fixes only.

## [0.2.0] - 2026-08-26

### Added

- `OpsOptions` — every connection-dependent admin call (`declare`, `inspectRetry`,
  `QueueHandle.inspect` / `replayDead` / `purgeDead`) now accepts `{ timeoutMs }`,
  ONE budget covering the connection wait and the broker round-trips. Omitting it
  keeps the previous behaviour: wait as long as the reconnect policy allows, the
  same default `open()` has. `replayDead` enforces its budget BETWEEN messages, so
  a caller that gives up actually stops the replay.

### Changed

Behaviour changes a caller can observe (0.x MINOR, called out per the policy above):

- `publishTimeoutMs: 0` now means _fail at once_ literally. The budget covers the
  broker confirm, and a confirm always needs some budget, so a publish with `0`
  rejects without writing the frame instead of publishing and then waiting for the
  confirm indefinitely. Use a small positive budget for fail-fast-while-
  disconnected with a real confirm wait.
- `reconnect.maxAttempts` now maps onto amqplib's `maxRetries - 1`, so a reconnect
  makes the same number of attempts as the boot loop (previously one more).
- `reconnect.factor` must be finite; `Infinity` used to pass validation and was
  then silently replaced by amqplib's default of 2.
- `redactUrl` masks a password-less userinfo whole (`amqp://***@host`).

### Fixed

- **A publish could hang forever.** The `timeoutMs` budget was spent acquiring a
  channel and never covered the back-pressure gate or the broker confirm, so a
  broker that blocks the connection under a resource alarm — accepting frames and
  confirming nothing — parked `publish()` indefinitely and with it `close()`. The
  deadline now covers every wait in the pipeline. When the budget is already spent
  the frame is not written at all, so a caller is never left wondering whether the
  message went out.
- **A lost connection is no longer reported as a broker refusal.** amqplib's
  `ConfirmChannel` flushes unconfirmed callbacks from a listener registered in its
  own constructor, ahead of coney's, so a channel death reached callers as
  `ConeyPublishError` ("the broker rejected the publish") — a permanent failure
  that told callers to drop a message they should have retried. In-flight
  publishes now reject with `ConeyUnavailableError` as the spec requires.
- A publish parked on the back-pressure gate resumed on the channel that armed it
  even when that channel had died in the meantime (`close` releases the gate too),
  failing a caller whose budget was untouched; the current channel is now re-read
  after the wait.
- `reconnect.maxAttempts` meant a different number of attempts at boot than on a
  reconnect: it counts ATTEMPTS but was passed straight through to amqplib's
  `maxRetries`, which counts the retries AFTER the first one. `reconnect.factor`
  must now be finite — amqplib silently replaced `Infinity` with its own default,
  so the two loops used different curves for the same policy.
- `close()` during the hand-over to amqplib's recovery connect now waits a short
  grace and closes the model as soon as amqplib returns it. The residual case (a
  broker that never comes back) is documented in `docs/design.md` §4: amqplib 2.x
  exposes no handle for it.
- `redactUrl` left a password-less userinfo untouched, so brokers that carry the
  secret in the username (token auth) leaked it into log events and error
  messages; the whole userinfo is now masked, matching `redactText`.
- `TopologyManager.inspectRetry` let raw amqplib errors escape — a retry bucket
  declared elsewhere with a different `x-message-ttl` now surfaces as
  `ConeyTopologyError` with `target` and `amqpCode`, like every other broker path.
- `defineQueue({ retry: null })` threw a raw `TypeError` instead of
  `ConeyConfigError`.
- `close()` no longer keeps every consumer runtime (and the handler closure it
  captured) referenced after the client is closed, and its publish-drain loop no
  longer spins one redundant round on already-settled promises.
- Mock fidelity: `deliverTopic` delivered a bound queue's message to EVERY
  competing consumer while the broker (and `deliver`) round-robins one — a
  double-processing bug could pass a green suite; `published` recorded the
  pre-JSON body rather than the wire body handlers actually receive; and
  `deliver` was the only entry point that did not validate its def.
- The GitLab `publish` job now verifies that `package.json` matches the pushed
  tag instead of only documenting the requirement, and `yarn release` refuses to
  tag a dirty tree, an existing tag or unpushed commits.

### Tests

- Unit coverage for `Publisher` and `TopologyManager` against a fake amqplib
  channel that reproduces the three behaviours coney depends on: a confirm
  callback that never fires, a `close` that flushes unconfirmed callbacks before
  coney's own listener, and `publish` returning `false` until `drain`.

## [0.1.0] - 2026-08-24

The first release: the complete v0.1 surface described in `docs/design.md`.

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

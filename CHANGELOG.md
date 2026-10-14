# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

The 0.x entries below are the private `@lorvel/coney` releases the rewrite went
through before going public; in that line a MINOR bump could contain breaking
changes (called out in its entry).

## [Unreleased]

### Added

- **Per-message TTL: `PublishOptions.ttlMs`.** Publishing with `ttlMs` stamps an
  absolute deadline as the `x-expires-at` header (`x-published-at` + `ttlMs`).
  Retries never extend it. Each delivery is checked once, before the handler,
  against the consumer's clock: a delivery past its deadline is acked without
  calling the handler and logged as the new `coney.message_expired` event (`warn`).
  It is not retried and not dead-lettered. A handler that starts in time and fails
  late follows the usual retry/dead outcome; a retry then meets the check on its next
  delivery. `replayDead()` keeps the deadline, so a dead letter replayed after it is
  dropped on arrival. The deadline keeps a stale message away from the handler; it
  does not remove it from the queue sooner. Publishes without `ttlMs` are unchanged.
- `Message.expiresAt`, read from `x-expires-at`.
- Test double: `DeliverOptions.expiresAt` simulates a deadline, and
  `{ outcome: 'expired' }` is the new `DeliveryOutcome`.

### Notes for upgrading

- `ConeyLogEvent` and `DeliveryOutcome` each gained a member. Code that switches
  over either one exhaustively (with a `never` check) needs a case for it. These
  unions can grow in minor releases.
- A 3.1.0 consumer honours `x-expires-at` from any producer, but only in the exact
  form `Date#toISOString()` writes; any other value means no deadline. A 3.0.0
  consumer ignores the header, so in a mixed fleet only upgraded consumers drop
  expired messages.

## [3.0.0] - 2026-10-13

First public release of the ground-up rewrite, published to npm as `coney`. It
replaces the unmaintained coney 2.x line.

### Changed

- **Breaking for coney 2.x users: neither the API nor the wire format is
  compatible with 2.x.** Queue names and the retry topology differ, so 2.x and 3.x
  must never consume the same queue. The API mapping and the safe per-queue cutover
  order are in [Migrating from 2.x](https://github.com/lorvel/coney#migrating-from-2x).
- The package is named `coney` (it was `@lorvel/coney` during the private 0.x line)
  and is published from [github.com/lorvel/coney](https://github.com/lorvel/coney)
  by GitHub Actions when a version tag is pushed.
- The library code is identical to `@lorvel/coney@0.3.0`: nothing in `src/` changed
  for this release. The one behavioural difference is the packaging fix below.

### Fixed

- **`require('coney')` works.** The `exports` map declared no condition that
  `require()` matches (only `types` and `import`), so it threw
  `ERR_PACKAGE_PATH_NOT_EXPORTED` although the README promised it from Node 20.19 up.
  The entry now uses the `default` condition, which `import` and `require()` both
  match.

## [0.3.0] - 2026-08-27

### Fixed

- A **broker-initiated consumer cancel no longer kills a consumer for good**. When the
  broker cancels a consumer — its queue was deleted, or the queue's node failed — coney
  used to detach the runtime and wait for the next connection `setup` to rebuild it. That
  is only correct when the connection is going down too; when it stays UP nothing ever
  calls `setup` again, so the consumer was dead for the rest of the process's life while
  `Subscription.active` still read `true`, the connection kept publishing, and the only
  trace was one info-level `coney.consumer_stopped` that looks exactly like an orderly
  shutdown. The runtime now re-attaches on its own backoff loop (the reconnect policy's
  curve), re-asserting the def's topology first because the queue is usually gone.
- **A channel-level error no longer kills a consumer for good either.** amqplib delivers
  a `null` message ONLY for a real `basic.cancel`, so a 404/406/403 that killed just the
  consumer's channel — a retry publish to a bucket that is gone, a policy change — took a
  disjoint path that detached the runtime with `active` still `true` and **no log line at
  all**: the same failure as above, with even less trace. Every unexpected detach now
  reaches the same re-attach loop.
- **The re-attach re-declares the whole def, not just its queue.** A def's retry buckets
  and its bound topic exchanges sit behind their own caches that only a reconnect clears,
  so the first fix redeclared the queue and left them missing — the consumer came back
  looking healthy and then lost or stranded its first retried message. Invalidation is
  now transitive.
- **A re-attach failure no longer breaks publishing.** The re-assert shares the topology
  cache with the publish path, so a consumer-side failure (usually on `<q>.dead`, a bucket
  or a binding — not the main queue) made every unrelated `publish()` for that def reject
  until the next round. A failed background re-assert now leaves that cache exactly as it
  found it. (One narrow window remains by design: a publish that arrives while the
  re-assert is still in flight joins it and shares its outcome, as any two concurrent
  `ensure` callers always have.)
- The backoff curve is now total: `reconnect: { initialDelayMs: 0 }` used to yield a `NaN`
  delay after ~1000 rounds (`0 * Infinity`), and any delay above `TIMEOUT_MAX` was silently
  clamped by `setTimeout` to 1 ms — both inverting "back off" into "retry immediately".
- `close()` and `Subscription.cancel()` release a parked re-attach immediately instead of
  letting its timer hold the runtime (and the user's handler closure) for up to
  `maxDelayMs` after they resolved.
- An orderly `close()` landing on a background re-attach no longer emits an error-level
  `coney.topology_failed` for the RPC the close itself killed.

### Added

- `coney.consumer_reattach_failed` (warn; `queue`, `reason`, `retryInMs`) — one per failed
  re-attach round, so a consumer that cannot come back is loud instead of silent. The log
  union is now 22 events.

### Changed

Behaviour changes a caller can observe (0.x MINOR, called out per the policy above):

- A detached consumer **comes back on its own**. Anything built on top of the old
  behaviour has to go: a caller who watched for `coney.consumer_stopped` and re-subscribed
  by hand now ends up with two consumers on the queue. Drop the workaround —
  `coney.consumer_reattach_failed` is the signal to watch instead, and it only fires when
  coney could not do it.
- The consumer re-attach backoff now has a **100 ms floor**. `reconnect.initialDelayMs: 0`
  is still honoured by the connection loop, which is bounded by `maxAttempts` and whose
  rounds cost a TCP connect against an unreachable endpoint; the re-attach loop has no
  ceiling and each round opens a channel and declares against a HEALTHY broker, so a zero
  delay there was measured at ~370 rounds/second.
- The re-attach loop is unbounded but no longer immortal: it stops once the connection is
  terminally `closed` (an exhausted `reconnect.maxAttempts`), where previously it warned
  once per `maxDelayMs` for the life of the process on a client already reporting
  `state === 'closed'`.
- `ConeyLogEvent` gained a member. Adapters that widen (`logger[e.level](e.event, e)`) are
  unaffected, but an exhaustive `switch` on `e.event` with a `never` fallback stops
  compiling until the new event is handled.

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

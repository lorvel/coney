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
- The full public type surface, including consuming and the test double; the
  implementations of `consume` / `subscribe` / dead-letter ops / `createConeyMock`
  are stubs that throw until the next slice lands.

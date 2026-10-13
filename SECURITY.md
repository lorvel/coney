# Security

## Supported versions

Only the latest 3.x release receives fixes. coney 2.x is unmaintained.

## Reporting a vulnerability

Email security@lorvel.ai — please do not open a public issue for security reports.

## Posture

- The library talks only to the AMQP broker you configure — no telemetry, no other
  network calls, no install scripts.
- Broker credentials in URLs are redacted in every log event; message bodies are
  never logged.

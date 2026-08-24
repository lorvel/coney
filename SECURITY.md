# Security

## Supported versions

Only the latest 0.x release receives fixes.

## Reporting a vulnerability

Email tutv95@gmail.com — please do not open a public issue for security reports.

## Posture

- The library talks only to the AMQP broker you configure — no telemetry, no other
  network calls, no install scripts.
- Broker credentials in URLs are redacted in every log event; message bodies are
  never logged.

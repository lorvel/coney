# Contributing

## Setup

Node ≥ 20.19 and Yarn 1 (`yarn install`). Scripts:

| Script                              | What                                                                             |
| ----------------------------------- | -------------------------------------------------------------------------------- |
| `yarn typecheck`                    | Type-check `src/` + `test/` (no emit)                                            |
| `yarn typecheck:design`             | Type-check the API contract and its examples (`docs/api/`)                       |
| `yarn build`                        | Compile to `dist/` with declarations                                             |
| `yarn test`                         | Unit tests (`node:test` via tsx); broker tests skip unless `RABBITMQ_URL` is set |
| `yarn lint` / `yarn lint:fix`       | ESLint (flat config; Prettier-conflicting rules disabled)                        |
| `yarn format` / `yarn format:check` | Prettier (the formatting authority — do not hand-format against it)              |
| `node scripts/diff-api.mjs`         | Verify the built `dist/index.d.ts` matches `docs/api/coney.d.ts` exactly         |

For the broker-backed tests, start a throwaway RabbitMQ with
`docker compose -f docker-compose.test.yml up -d --wait` and pass
`RABBITMQ_URL` / `RABBITMQ_MGMT_URL` as documented in that file.

## Ground rules

- `docs/design.md` is the canonical spec. Code follows it; when a change of
  behaviour is needed, change the spec in the same commit — never silently.
- `docs/api/coney.d.ts` is the public API contract. Any public API change edits
  that file (and `docs/design.md` where semantics move) in the same commit;
  CI enforces a zero diff against the built declarations.
- The library stays generic: no `process.env` reads, no default `prefix`, no
  application names in `src/`, credentials redacted in every logged URL.
- Commits: English, conventional (`feat:`, `fix:`, `docs:`, `chore:`, …).

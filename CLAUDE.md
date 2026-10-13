# Project: coney

An opinionated RabbitMQ toolkit for Node.js. `docs/design.md` is the canonical spec and
`docs/api/coney.d.ts` the enforced public API contract — read `CONTRIBUTING.md` for the
ground rules (spec-first changes, contract zero-diff, generic-library constraints).

## Tech Stack

- TypeScript (strict, `exactOptionalPropertyTypes`), ESM only (NodeNext), Node >= 20.19
- `amqplib` as a peer dependency; zero runtime dependencies
- Tests: `node:test` via tsx; package manager: Yarn 1 (never npm/pnpm)

## Commands

| Command                                    | What                                                   |
| ------------------------------------------ | ------------------------------------------------------ |
| `yarn typecheck` / `yarn typecheck:design` | Type-check src+test / the API contract                 |
| `yarn build`                               | Clean compile to `dist/`                               |
| `yarn test` / `yarn test:integration`      | Unit / broker-backed suite (`RABBITMQ_URL`)            |
| `yarn lint` / `yarn lint:fix`              | ESLint (flat config)                                   |
| `yarn format` / `yarn format:check`        | Prettier                                               |
| `node scripts/diff-api.mjs`                | Built `.d.ts` must match `docs/api/coney.d.ts` exactly |

## Code Style

Prettier owns formatting (`.prettierrc.json`): 2-space indent, single quotes, no semicolons,
trailing commas, print width 100, LF. Never hand-format against it — run `yarn format`.

- Never pass an `await` expression directly as a function argument — assign it to a named
  `const` first, then pass the variable (enforced by ESLint in `src/`).
- `src/` is folder-per-domain with strictly downward dependencies
  (`core ← defs ← connection ← topology/publish ← consume ← client`; `mock` beside `client`);
  `src/index.ts` is the ONLY barrel — internal code imports concrete files directly.
- No `console` in `src/` — all output goes through the typed log emitter (`core/log.ts`).
- No `any`; prefer `unknown` + narrowing. Comments/TSDoc/commits in English.
- Every user-supplied timeout/duration is validated via `core/time.ts` (`setTimeout` clamps
  values above 2^31-1, including `Infinity`).

## Do NOT

- Change public behavior without updating `docs/design.md` first, and the contract
  `docs/api/coney.d.ts` in the same change (CI enforces a zero structural diff).
- Put application names in `src/` (`grep -ri lorvel src/` must stay 0), read `process.env`
  in the library, or log unredacted credentials.
- Touch broker objects without a disposable prefix in tests (`it.<run-id>` for integration,
  `test.*` for smoke) — shared brokers are used for local runs; always clean up.
- Create channels inside the connection `setup` hook via the manager's lazy methods — use the
  `channels` factory the hook receives (the lazy path deadlocks there).

## Git

- Conventional Commits (`feat:`, `fix:`, `refactor:`, `ci:`, `docs:`, `test:`, `chore:`).
- Do not commit or push unless explicitly asked.

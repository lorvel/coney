import { describe, it } from 'node:test'
import assert from 'node:assert/strict'
import { existsSync } from 'node:fs'
import { createRequire } from 'node:module'

// A package resolves its own name through its "exports" map, so loading `coney` from
// here takes the path a consumer takes. That needs the build output: a local run
// without it skips, while in CI a missing build fails rather than passing untested.
const SELF = 'coney'
const built = existsSync(new URL('../dist/index.js', import.meta.url))
const skip = !built && process.env['CI'] === undefined && 'dist/ is not built — run `yarn build`'

describe('package entry', () => {
  it('loads through import', { skip }, async () => {
    const esm = (await import(SELF)) as Record<string, unknown>
    assert.equal(typeof esm['createConey'], 'function')
  })

  it('loads through require() — an import-only "exports" map rejects it', { skip }, () => {
    const cjs = createRequire(import.meta.url)(SELF) as Record<string, unknown>
    assert.equal(typeof cjs['createConey'], 'function')
  })
})

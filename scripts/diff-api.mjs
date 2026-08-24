#!/usr/bin/env node
/**
 * Verify that the BUILT public API (dist/index.d.ts) matches the CONTRACT
 * (docs/api/coney.d.ts) exactly: same export names, same declarations.
 *
 * Comparison is formatting-insensitive: each exported symbol's declarations are
 * re-printed with the TypeScript printer (comments removed) and normalized, so
 * only structural drift fails. TSDoc drift is review territory, not CI's.
 *
 * Exit 0: surfaces match. Exit 1: any mismatch (missing, extra, or different).
 */

import { createRequire } from 'node:module'
import { resolve } from 'node:path'
import { existsSync } from 'node:fs'

const require = createRequire(import.meta.url)
const ts = require('typescript')

const CONTRACT = resolve('docs/api/coney.d.ts')
const BUILT = resolve('dist/index.d.ts')

if (!existsSync(BUILT)) {
  console.error(`diff-api: ${BUILT} does not exist — run \`yarn build\` first`)
  process.exit(1)
}

const printer = ts.createPrinter({ removeComments: true, newLine: ts.NewLineKind.LineFeed })

/** name → normalized declaration text, for every export of the module at entryPath. */
function exportedDeclarations(entryPath) {
  const program = ts.createProgram([entryPath], {
    target: ts.ScriptTarget.ES2022,
    module: ts.ModuleKind.NodeNext,
    moduleResolution: ts.ModuleResolutionKind.NodeNext,
    strict: true,
    skipLibCheck: true,
    noEmit: true,
  })
  const checker = program.getTypeChecker()
  const source = program.getSourceFile(entryPath)
  if (source === undefined) throw new Error(`cannot load ${entryPath}`)
  const moduleSymbol = checker.getSymbolAtLocation(source)
  if (moduleSymbol === undefined) throw new Error(`${entryPath} has no exports`)
  const out = new Map()
  for (const symbol of checker.getExportsOfModule(moduleSymbol)) {
    const resolved = symbol.flags & ts.SymbolFlags.Alias ? checker.getAliasedSymbol(symbol) : symbol
    const declarations = (resolved.declarations ?? [])
      .filter((decl) => !ts.isSourceFile(decl))
      .map((decl) =>
        normalize(printer.printNode(ts.EmitHint.Unspecified, decl, decl.getSourceFile())),
      )
      .sort()
    out.set(symbol.name, declarations.join('\n---\n'))
  }
  return out
}

/** Collapse whitespace and cosmetic emit differences. */
function normalize(text) {
  return text
    .replace(/\bexport\s+declare\s+/g, 'export ') // built files add `declare`
    .replace(/\s+/g, ' ')
    .replace(/;\s*}/g, ' }') // `{ cause?: unknown; }` vs `{ cause?: unknown }`
    .replace(/;/g, ' ')
    .replace(/\s+/g, ' ')
    .trim()
}

const contract = exportedDeclarations(CONTRACT)
const built = exportedDeclarations(BUILT)

let failures = 0
const fail = (message) => {
  failures += 1
  console.error(`diff-api: ${message}`)
}

for (const name of contract.keys()) {
  if (!built.has(name)) fail(`missing from the built API: ${name}`)
}
for (const name of built.keys()) {
  if (!contract.has(name)) fail(`not in the contract: ${name}`)
}
for (const [name, contractText] of contract) {
  const builtText = built.get(name)
  if (builtText === undefined || builtText === contractText) continue
  fail(`declaration of ${name} differs\n  contract: ${contractText}\n  built:    ${builtText}`)
}

if (failures > 0) {
  console.error(
    `diff-api: ${failures} mismatch(es) between dist/index.d.ts and docs/api/coney.d.ts`,
  )
  process.exit(1)
}
console.log(`diff-api: ${contract.size} exports match the contract exactly`)

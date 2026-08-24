import js from '@eslint/js'
import tseslint from 'typescript-eslint'
import prettier from 'eslint-config-prettier'

export default tseslint.config(
  { ignores: ['dist/', 'node_modules/', 'docs/api/'] },
  js.configs.recommended,
  ...tseslint.configs.recommended,
  {
    rules: {
      // TypeScript already checks for undefined identifiers.
      'no-undef': 'off',
      // `_`-prefixed parameters mark deliberately unused arguments (interface conformance).
      '@typescript-eslint/no-unused-vars': [
        'error',
        { argsIgnorePattern: '^_', varsIgnorePattern: '^_' },
      ],
    },
  },
  {
    // The library itself must stay silent: all output goes through the typed log
    // emitter (spec: default sink writes JSON lines to stderr, never console).
    files: ['src/**/*.ts'],
    rules: {
      'no-console': 'error',
      // Workspace rule: never pass an `await` expression directly as a function
      // argument — assign it to a named const first, then pass the variable.
      'no-restricted-syntax': [
        'error',
        {
          selector: 'CallExpression > AwaitExpression.arguments',
          message: 'Do not inline `await` as a call argument — assign it to a named const first.',
        },
        {
          selector: 'NewExpression > AwaitExpression.arguments',
          message:
            'Do not inline `await` as a constructor argument — assign it to a named const first.',
        },
      ],
    },
  },
  {
    // CLI helpers: console IS the interface, and diff-api needs createRequire.
    files: ['scripts/**'],
    rules: {
      'no-console': 'off',
      '@typescript-eslint/no-require-imports': 'off',
    },
  },
  // Disables ESLint rules that conflict with Prettier — keep last.
  prettier,
)

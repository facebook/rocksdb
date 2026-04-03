You are reviewing a pull request to RocksDB to determine if the AI-context documentation in docs/components/ needs updating.

## Your Task

1. Read ARCHITECTURE.md and docs/components/README.md to understand what documentation exists
2. Read the PR diff provided below to understand what source code changed
3. For each changed source file, check if any doc in docs/components/ covers that component
4. If a doc covers a changed component, read BOTH the doc AND the changed source carefully to determine:
   - Does the diff change any behavior, API, invariant, data format, or algorithm described in the doc?
   - Does the diff add a new feature, option, or code path the doc should mention?
   - Does the diff rename, remove, or restructure something the doc references?
5. Output your verdict:

If documentation updates ARE needed:
- List exactly which docs need updating and what specifically needs to change
- End with exactly: RESULT: FAIL

If documentation updates are NOT needed:
- Explain briefly why (e.g., "internal implementation detail", "test-only change", "docs already accurate")
- End with exactly: RESULT: PASS

## Guidelines

- Trivial changes (formatting, comments, variable renames not affecting public API) do NOT require doc updates
- Test-only changes do NOT require doc updates
- Changes to files not referenced by any doc do NOT require doc updates
- New features or options significant enough for component docs DO require updates
- Bug fixes changing documented behavior DO require updates
- If the PR itself modifies docs/components/ files, those docs are considered updated

## PR Diff

The diff follows below:

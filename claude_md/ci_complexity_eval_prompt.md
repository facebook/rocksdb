# PR Complexity Classifier — CI Triage Prompt

## Purpose
Quickly classify a RocksDB pull request as **simple** or **complex** so the
downstream reviewer can pick an appropriate thinking budget.

## Output Contract (STRICT)
Your final response MUST be exactly one of these two lowercase tokens, on a
line by itself, with no other text:

```
simple
```

or

```
complex
```

If you are unsure, output `complex`. The downstream review will use this token
to set its thinking budget — when in doubt, prefer the more thorough budget.

## Classification Heuristics

Treat a PR as **simple** when ALL of the following hold:
- Diff touches < ~200 lines of non-test code
- No changes to public headers under `include/rocksdb/`
- No changes to on-disk format (SST block layout, WAL record layout,
  manifest, options file format, version edit encoding)
- No changes to compaction picker, write-path locking, recovery, or
  WAL/memtable lifecycle
- No new public option, no new statistics counter, no new RPC/serialized
  message
- Tests-only or docs-only changes
- Trivial refactors (rename, comment, dead-code removal, lint fixes)
- One-line bug fix with obvious root cause and a focused regression test

Treat a PR as **complex** when ANY of the following hold:
- Touches files in `db/`, `db/compaction/`, `db/blob/`, `cache/`,
  `table/block_based/`, `file/`, `env/io_*` with > ~50 lines of logic change
- Modifies thread-safety/synchronization (mutexes, atomics, memory ordering,
  refcounting, condvars)
- Changes serialization or on-disk format
- Changes public API surface in `include/rocksdb/`
- Adds a new feature flag, option, or compaction style
- Touches transactions (`utilities/transactions/`), backup/checkpoint,
  user-defined timestamps, or remote compaction
- Cross-cutting refactor spanning > 5 directories

## How To Decide
1. Read the diff summary and changed-file list provided below.
2. Skim the diff hunks — focus on which subsystems are touched and whether
   the change is mechanical vs. semantic.
3. Apply the heuristics above. Default to `complex` if the rules conflict.
4. Output the single token. No preamble. No explanation.

## Tool Use
You may use `View`, `GlobTool`, and `GrepTool` to peek at one or two files
if the diff snippet alone is ambiguous, but keep this fast — this is a
triage step, not a review. Do NOT spawn sub-agents.

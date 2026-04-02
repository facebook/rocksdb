---
description: 'Run RocksDB crash/stress tests using the Sandcastle-aligned stress test matrix and fix loop scripts. Triggers: stress test, crash test, run stress, stress matrix, stress_fix_loop, validate under stress.'
---

# RocksDB Stress Test Runner

Scripts: `run_stress_matrix.sh` and `stress_fix_loop.sh` in `~/bin/` (on PATH).
Mirrors Sandcastle: 3 variants (debug, asan, tsan) x 22 modes = 66 tests.
Concurrent-safe via SLUG isolation (git HEAD hash prefix).

## Pre-Flight Checklist

1. **Commit changes** — worktrees only see committed state. Uncommitted changes are invisible.
2. **Check memory**: `free -g | awk '/Mem/{print $7}'` — need 50GB+ free. Kill stale `buck2d` and EdenFS if needed.
3. **Check disk**: `df -h /tmp` — need 100GB+.
4. **Clean stale worktrees**: `rm -rf /tmp/stress-wt-*` if seeing "getcwd" errors.
5. **Never use tmux** on shared devvms (other processes kill tmux server).

## run_stress_matrix.sh

| Flag | Default | Description |
|------|---------|-------------|
| --repo DIR | cwd | RocksDB repo path |
| --parallel N | 8 | Concurrent test slots |
| --batches LIST | 300,600,1800,3600 | Durations (seconds) |
| --variants LIST | debug,asan,tsan | Build variants |
| --modes LIST | all | Mode groups (see below) |
| --extra-flags F | | Extra db_crashtest.py flags |
| --skip-build | | Reuse existing binaries |
| --stop | | Stop a running matrix for this repo |

### 22 Test Modes (8 groups)

core(4), atomic_flush(2), txn(6), optimistic_txn(2),
best_efforts(1), ts(2), tiered_storage(2), multiops(3)

### Scheduling

3-pass variant interleave over priority-ordered modes.
Each parallel batch covers all variants x distinct features.
Early exit after any parallel batch with failures.

### Isolation (SLUG)

SLUG = git rev-parse HEAD | head -c 8
- Worktrees: /tmp/stress-wt-{SLUG}-{variant}
- Test DBs: /tmp/stress-db-{SLUG}/{label}/ (via TEST_TMPDIR)
- Results: /tmp/stress-results-{SLUG}-YYYYMMDD-HHMMSS/
- PID file: /tmp/stress-matrix-{SLUG}.pid

### Monitoring

The script prints `=== FINAL SUMMARY ===` on both success and failure.
Background monitors should watch for this marker to know when the matrix is done.

### Cleanup

- On pass: cleans /tmp/stress-db-{SLUG}/* between batches
- On failure: preserves ALL DB artifacts + LOG files + failures.txt summary
- Never touches other runs' dirs

### Examples

```
run_stress_matrix.sh --repo ~/workspace/ws21/rocksdb --modes core --batches 300
run_stress_matrix.sh --repo ~/workspace/ws21/rocksdb
run_stress_matrix.sh --variants asan --modes core,txn --batches 1800
run_stress_matrix.sh --repo ~/workspace/ws21/rocksdb --stop
```

## stress_fix_loop.sh

Automated: stress matrix → analyze failures → report and exit.
SLUG = md5sum(repo_path) for stability across commits.

| Flag | Default | Description |
|------|---------|-------------|
| --repo DIR | cwd | RocksDB repo path |
| --target-duration N | 3600 | Seconds to pass clean |
| --parallel N | 4 | Parallel runs per variant |
| --variants LIST | debug,asan,tsan | Build variants |
| --modes LIST | all | Mode groups to test |
| --extra-flags F | | Extra db_crashtest.py flags |
| --max-iterations N | 10 | Max fix rounds |
| --push | | Push to GitHub on success |
| --stop | | Stop a running loop for this repo |

On failure, prints:
- Categorized failures (sanitizer/assertion/verification/crash)
- Exact re-run command with --skip-first-build
- Exact --stop command
- Artifact locations

## Critical Warnings

### Extra-flags bypass finalize_and_sanitize()

`--extra-flags` are appended AFTER `finalize_and_sanitize()` runs. This means
incompatible feature combinations that the sanitizer would normally prevent
can slip through. Always explicitly disable incompatible features:
```
--extra-flags "--enable_my_feature=1 --incompatible_feature_a=0 --incompatible_feature_b=0"
```

### Whitebox timing: 900s grace period

Whitebox has a hardcoded 900s (15min) grace period per iteration. A 300s batch
with whitebox can run 20+ minutes. For short batches (<=600s), the script
automatically reduces ops_per_thread for ASAN/TSAN whitebox variants.

### MightHaveUnsyncedDataLoss()

If your feature has lower durability (data can be lost on crash, like
blob direct write with sync=false or disable_wal), add it to
`MightHaveUnsyncedDataLoss()` in `db_stress_test_base.h`. Without this,
`db_stress` will report false verification failures — it expects all data
to survive crashes unless this function says otherwise.

### Expected state trace replay

When a feature transforms WriteBatch entries (e.g., Put → PutBlobIndex via
TransformBatch), the trace records the *transformed* batch. The
`ExpectedStateTraceRecordHandler` in `expected_state.cc` must handle the
transformed record type, or `Restore()` will fail with `assert(IsDone())`.
Add a handler override (e.g., `PutBlobIndexCF`) that resolves the transformed
value back to the original value_base.

## Troubleshooting

| Problem | Fix |
|---------|-----|
| getcwd errors | rm -rf /tmp/stress-wt-* |
| OOM kills | pkill -9 buck2d; reduce --parallel; check cgroup memory.events |
| Process dies on disconnect | Use setsid or systemd-run --user --scope |
| False verification failures | Check MightHaveUnsyncedDataLoss() for your feature |
| assert(IsDone()) in Restore | Check ExpectedStateTraceRecordHandler for missing record type |
| Incompatible feature combo | Pass explicit disables in --extra-flags |
| Build fails (assembler errors) | Don't parallel-build variants; sequential only |

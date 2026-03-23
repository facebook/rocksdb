# RocksDB Stress Test Guide

A practical guide to running and debugging RocksDB's crash/stress tests (`db_stress` + `db_crashtest.py`). Distilled from real-world experience developing and testing new features.

## Overview

RocksDB's stress test (`db_stress`) is the primary tool for catching concurrency bugs, crash recovery issues, and data corruption. It runs random operations against the database while periodically killing and restarting it, verifying data integrity after each recovery.

The Python wrapper `db_crashtest.py` orchestrates the stress test — randomizing parameters, managing crash/recovery cycles, and running multiple test modes.

## Architecture

```
db_crashtest.py (orchestrator)
  ├── Randomizes ~200 db_stress parameters
  ├── Runs finalize_and_sanitize() to resolve conflicts
  ├── Launches db_stress with generated flags
  │
  ├── Blackbox mode: kills db_stress externally every N seconds
  └── Whitebox mode: db_stress self-crashes via kill_random_test
```

### Key Components

- **`db_stress`** — C++ binary that runs random read/write/delete/merge operations against RocksDB. Verifies expected state after crash recovery.
- **`db_crashtest.py`** — Python orchestrator that randomizes parameters, manages the crash-recovery loop, and handles different test modes.
- **`crash_test.mk`** — Makefile targets that define the 22 standard test modes run in CI (Sandcastle).
- **Build variants** — `db_stress` is built in 3 variants: `debug` (assertions enabled), `asan` (AddressSanitizer — catches memory errors), and `tsan` (ThreadSanitizer — catches data races). Each variant is a separate binary built with different compiler flags.

## Test Modes (22 total)

| Category | Modes | What They Test |
|----------|-------|----------------|
| Core (4) | blackbox, whitebox, simple-blackbox, simple-whitebox | Basic crash recovery |
| Atomic flush (2) | cf_consistency blackbox/whitebox | Multi-CF atomic flush |
| Transactions (6) | wc/wp/wup_txn blackbox/whitebox | Write-committed/prepared/unprepared txn |
| Optimistic txn (2) | optimistic_txn blackbox/whitebox | Optimistic transaction |
| Best efforts recovery (1) | blackbox only | Recovery with missing/corrupt files |
| Timestamps (2) | enable_ts blackbox/whitebox | User-defined timestamps |
| Tiered storage (2) | tiered_storage blackbox/whitebox | Hot/cold data tiering |
| Multi-ops txn (3) | multiops wc/wp/wup blackbox only | Multi-operation transactions |

### Blackbox vs Whitebox

**Blackbox:** `db_crashtest.py` kills `db_stress` externally every `interval` seconds (default 120s). Simulates sudden process death. Each iteration is tightly bounded.

**Whitebox:** `db_stress` self-crashes via `kill_random_test` at random code points. More realistic — crashes happen mid-operation. Has 4 check_modes that cycle through different compaction styles:
- `check_mode=0`: Kill test (3 sub-modes). `ops_per_thread` multiplied by 100x.
- `check_mode=1`: Universal compaction
- `check_mode=2`: FIFO compaction. `ops_per_thread` divided by 5.
- `check_mode=3`: Normal (default compaction)

Check modes only advance after `duration/2` (first half is all check_mode=0).

## Parameter Randomization: How `db_crashtest.py` Works

### The Flag Pipeline

Understanding the order of operations is critical:

1. **Base params** — hardcoded defaults in `db_crashtest.py` (e.g., `default_params`, `blackbox_default_params`)
2. **Test mode params** — mode-specific overrides (e.g., `cf_consistency_params` for atomic flush)
3. **`finalize_and_sanitize()`** — resolves conflicts between randomly chosen params
4. **CLI extra flags** — appended last to `db_stress` command line

### The `finalize_and_sanitize()` Trap

This is the single most important thing to understand. `finalize_and_sanitize()` runs *before* CLI flags are applied, but it makes decisions based on the *randomized* params:

```
Randomized params → finalize_and_sanitize() → db_stress CLI → extra-flags (last-one-wins)
```

**The problem:** If `finalize_and_sanitize()` sees `feature_X=0` and decides not to disable an incompatible `feature_Y`, then your extra-flag `--feature_X=1` re-enables X at the CLI level — but Y is still enabled. Now you have an incompatible combination.

**Example (blob direct write + transactions):**
1. Randomizer picks `use_txn=1`, `enable_blob_direct_write=0`
2. `finalize_and_sanitize()` sees bdw=0, doesn't disable txn
3. Extra-flags add `--enable_blob_direct_write=1` (last-one-wins)
4. Now txn + bdw are both on — an incompatible combination

**Solution:** Always pass ALL incompatible feature disables alongside your feature enable:
```bash
--extra-flags "--enable_blob_direct_write=1 --enable_blob_files=1 \
  --use_txn=0 --use_optimistic_txn=0"
```

### Test Mode vs gflags Distinction

Some parameters exist at two levels:
- **`db_crashtest.py` mode flags** (e.g., `--txn whitebox`) — these configure the *test orchestrator*
- **`db_stress` gflags** (e.g., `--use_txn=1`) — these configure the *binary*

They can conflict. The `--txn` mode flag makes `db_crashtest.py` set up TransactionDB *before* generating flags. Your extra-flag `--use_txn=0` wins at the gflags level, but the Python script already configured the test for txn mode. **Don't use extra-flags to contradict the test mode — exclude incompatible modes instead.**

### Flags NOT to Pass

- **`--db`** and **`--expected_values_dir`** — managed internally by `db_crashtest.py`. Passing them causes argparse misrouting.
- **`--ops_per_thread`** in whitebox — gets multiplied by 100x for check_mode=0, divided by 5 for check_mode=2.

## Whitebox Timing: The 900-Second Grace Period

Whitebox has a hardcoded 900-second (15-minute) grace period per iteration:

```python
timeout = exit_time - time.time() + 900  # remaining duration + 15 min
```

For a `--duration=300` batch:
- Iteration 1 timeout: 300 + 900 = 1200s (20 min)
- Last iteration timeout: ~0 + 900 = 900s (15 min)

This means a 5-minute whitebox test can run up to 20 minutes. This is by design for Sandcastle (where duration is 1-3 hours and 15 min is a small fraction), but it makes short batches unreliable for timing.

### ASAN/TSAN with Short Batches

Instrumented variants (ASAN 2-3x slower, TSAN 5-10x slower) compound the timing problem:
- ASAN whitebox with `ops_per_thread=200000`: each iteration takes minutes under instrumentation
- With the 900s grace, a 300s ASAN whitebox can run 20+ minutes

**Mitigation:** Reduce `ops_per_thread` for short batches (≤600s). But there's a minimum:
- Whitebox has `reopen=20` (hardcoded)
- `check_mode=2` divides `ops_per_thread` by 5
- `db_stress` requires `reopen < ops_per_thread`
- **Minimum ops_per_thread = 105** (so check_mode=2 gets 21 > 20 reopens)

Alternatively, wrap whitebox tests with a process-level `timeout`:
```bash
timeout $((duration + 120)) python3 tools/db_crashtest.py whitebox ...
```

## Testing a New Feature

### Step 1: Understand Feature Interactions

Before running stress tests, map which existing features are incompatible with yours. Check `finalize_and_sanitize()` in `db_crashtest.py` to see what conditions disable your feature.

### Step 2: Configure Extra Flags

Pass your feature's enable flags AND all incompatible feature disables:
```bash
python3 tools/db_crashtest.py blackbox \
  --enable_my_feature=1 \
  --incompatible_feature_a=0 \
  --incompatible_feature_b=0
```

### Step 3: Select Appropriate Test Modes

Don't run test modes that are fundamentally incompatible with your feature. For example, if your feature doesn't work with transactions, exclude `txn`, `optimistic_txn`, and `multiops` mode groups.

### Step 4: Update `MightHaveUnsyncedDataLoss()`

If your feature has lower durability guarantees (data can be lost on crash), add it to `MightHaveUnsyncedDataLoss()` in `db_stress_test_base.h`. Without this, `db_stress` will flag false verification failures — it expects all data to survive crashes unless this function says otherwise.

### Step 5: Update `finalize_and_sanitize()`

Add your feature to `finalize_and_sanitize()` in `db_crashtest.py` so the randomizer knows which features conflict with yours. This prevents impossible parameter combinations.

## Debugging Failures

### Failure Categories

1. **Verification failure** — "expected state has key, Get() returns NotFound" or vice versa. Data corruption or incorrect recovery.
2. **Assertion failure** — `db_stress` or RocksDB asserts. Logic bug.
3. **Sanitizer error** — ASAN (memory), TSAN (data race), UBSAN (undefined behavior).
4. **Crash/SIGKILL during verification** — usually OOM.
5. **Exit code 2** — parameter validation failure (e.g., `reopen >= ops_per_thread`).

### Triage Methodology: Evidence First, Then Code

The most important principle for debugging stress test failures: **understand the feature first, then let the artifacts guide you.** Read the relevant code to build a mental model of the feature's design — its data flow, concurrency model, and durability guarantees. Then turn to the LOG files and crash artifacts. The code tells you what *should* happen; the artifacts tell you what *actually* happened. The gap between the two is where the bug lives.

#### Step 1: Preserve the Crash Artifacts

When a stress test fails, these artifacts are your evidence:
- **RocksDB LOG file** — in the crash DB directory (e.g., `/tmp/rocksdb_crashtest_*/LOG`)
- **LOG.old.*** files — previous LOG files before crash
- **db_stress stderr** — the test harness output showing the failure message
- **db_crashtest.py output** — shows the full command line with all flags
- **DB files** — the SST files, MANIFEST, and WAL in the crash DB directory. Use `ldb` and `sst_dump` tools to dump records from SST files, inspect the MANIFEST, and examine WAL entries. These help trace what data was actually persisted vs what was expected.

**Critical:** Always redirect command output to a file first, then grep. Piping directly through grep loses the output forever if your filter is wrong.

#### Step 2: Build a Timeline from the LOG

The LOG file is the single most valuable artifact. Key things to extract:

1. **File lifecycle** — when was each file opened, sealed, committed to MANIFEST, compacted, deleted? Search for file numbers mentioned in the error.
2. **Recovery events** — what files were recovered after crash? Any "missing file" during recovery?
3. **Purge events** — `PurgeObsoleteFiles` logs include `blob_live_set[size=N]` and `active_blob[size=N]`. These tell you exactly which files were protected and which were deleted — and why.
4. **Timestamps** — correlate events across concurrent threads. Races show up as interleaved events within milliseconds.

**Example timeline (from a real blob file deletion race):**
```
11:04:02.710130 — AddFilePartitionMapping: file 5498, map size=4
11:04:02.714148 — Opened blob file 5498
11:04:02.714430 — PurgeObsoleteFiles: DELETING blob file 5498
                   (not in active_blob[size=3])
11:04:02.733618 — blob_file_creation for 5498: "IO error: No such file"
```
This 4ms window between file creation and deletion revealed the exact race condition — `FindObsoleteFiles` had snapshotted the active file set *before* file 5498 was added, but the directory scan found it on disk *after* creation.

#### Step 3: Classify the Failure

Before diving into code, classify what you're seeing:

- **Is the feature flag on or off?** Check the db_stress command line for your feature's flag. If bdw=0 and you're debugging a blob issue, the failure is pre-existing — not your bug.
- **Is it a test bug or an engine bug?** Check if `MightHaveUnsyncedDataLoss()` accounts for your feature's durability semantics. False verification failures mean the *test* has wrong expectations, not that the engine is broken.
- **Is it a known incompatibility?** Some features are fundamentally incompatible (e.g., remote compaction + blob files). The stress test may randomly combine them.

#### Step 4: Run Parallel Investigations

Different approaches find different bugs. When triaging a hard failure:

- **AI code analysis** catches logic bugs visible from reading code paths — missing null checks, wrong conditions, unhandled edge cases.
- **LOG-based timeline analysis** catches races and ordering bugs that look correct in code but fail at runtime. The code *looks* right in isolation; the bug is in the interleaving.
- **Running both in parallel** is valuable. In practice, we found cases where CC (Claude Code) and Codex each identified a different real bug from the same failure — they have different blind spots.

#### Step 5: Write a Regression Test

Before fixing, write a test that reproduces the failure deterministically:
- **For races:** Use SyncPoints to force the exact interleaving. If the race involves two mutexes (e.g., `db_mutex` vs `file_partition_mutex_`), you can pause one thread between operations to create the race window.
- **For hard-to-reproduce races:** Simulate the end-state directly. Create the "phantom" artifact on disk and verify the protection mechanism handles it. Deterministic, no flakiness.
- **Always verify the test fails without the fix.** A test that passes both with and without the fix proves nothing.

## Build Variants

| Variant | What It Catches | Build Overhead | Runtime Overhead |
|---------|----------------|----------------|------------------|
| debug | Assertions, logic bugs | 1x | 1x |
| asan | Memory errors (use-after-free, buffer overflow, leaks) | 1.5x | 2-3x |
| tsan | Data races, lock-order violations | 1.5x | 5-10x |

## Running at Scale

### Concurrent Feature Testing

When running stress tests for multiple features simultaneously (different repos/branches), isolate resources:

- **Worktree paths**: Use a unique SLUG per feature (e.g., first 8 chars of commit hash) → `/tmp/stress-wt-{SLUG}-{variant}`
- **DB paths**: `/tmp/stress-db-{SLUG}/`
- **Result paths**: `/tmp/stress-results-{SLUG}-YYYYMMDD-HHMMSS/`
- Use `--destroy_db_initially=1` to avoid stale DB state between runs

### Memory Management

- Each `db_stress` process uses ~1-3GB RAM (varies with cache_size, column_families)
- 8 parallel tests × 3 variants = 24 processes = ~24-72GB
- Sequential builds, not parallel — 3 concurrent `make -j128` will OOM
- Before starting: check available memory, kill unused build daemons (buck2, EdenFS)
- Check for OOM kills: `cat /sys/fs/cgroup/user.slice/user-$(id -u).slice/memory.events | grep oom_kill`

### Process Management

- **Use PID files** for process lifecycle management — never `pkill -f` with broad patterns
- **Kill process groups, not individual PIDs**: `kill -TERM -$(cat pidfile)` (negative PID = process group)
- **Orphaned `db_stress` processes eat memory** — always clean up the full process tree
- **Use `setsid`** for background processes to create isolated process groups

## Running Stress Tests with Scripts

Two scripts automate stress testing at scale. They live in `~/bin/` on the devvm (on PATH) and work with any RocksDB checkout.

### `run_stress_matrix.sh` — One-Shot Stress Test Run

Builds 3 binary variants and runs all 22 Sandcastle-equivalent test modes with escalating durations. Stops at first failure.

```bash
# Quick smoke test — core modes only, 5 min
run_stress_matrix.sh --repo ~/workspace/rocksdb --modes core --batches 300

# Full Sandcastle coverage — 3 variants × 22 modes = 66 tests per batch
run_stress_matrix.sh --repo ~/workspace/rocksdb

# Test a specific feature with extra flags
run_stress_matrix.sh --repo ~/workspace/rocksdb \
  --modes core,atomic_flush,best_efforts,ts,tiered_storage \
  --extra-flags "--enable_blob_direct_write=1 --enable_blob_files=1 \
    --use_txn=0 --use_optimistic_txn=0"

# Only ASAN, just transaction modes, 30-min batches
run_stress_matrix.sh --variants asan --modes txn --batches 1800
```

**Key flags:**
| Flag | Default | Description |
|------|---------|-------------|
| `--repo DIR` | current dir | Path to RocksDB checkout |
| `--parallel N` | 8 | Concurrent test slots |
| `--batches LIST` | 300,600,1800,3600 | Comma-separated durations in seconds |
| `--variants LIST` | debug,asan,tsan | Build variants |
| `--modes LIST` | all | Mode groups: core, atomic_flush, txn, optimistic_txn, best_efforts, ts, tiered_storage, multiops |
| `--extra-flags F` | | Extra flags for db_crashtest.py |
| `--skip-build` | | Reuse existing worktree binaries |
| `--jobs N` | 128 | Build parallelism |

**How it works:**
1. Builds `db_stress` in worktrees under `/tmp/stress-wt-{SLUG}-{variant}` (SLUG = first 8 chars of HEAD commit hash)
2. Runs tests in priority order: Tier 1 (blackbox per feature) → Tier 2 (whitebox counterparts) → Tier 3 (policy variants)
3. Escalates through batch durations (300s → 600s → 1800s → 3600s). All tests must pass at one duration before advancing.
4. On failure: preserves crash DB LOG files in the results directory. On success: cleans up test DBs between batches.

**Output:** `/tmp/stress-results-{SLUG}-YYYYMMDD-HHMMSS/`

### `stress_fix_loop.sh` — Automated Fix Loop

Runs stress matrix → on failure, launches Claude Code (CC) to analyze and fix → commits locally → rebuilds → re-runs. Repeats until the target duration passes clean.

```bash
# Fix loop until 1hr passes clean, then push
stress_fix_loop.sh --repo ~/workspace/rocksdb --target-duration 3600 --push

# Feature-specific: blob direct write with incompatible features disabled
stress_fix_loop.sh --repo ~/workspace/rocksdb --parallel 4 \
  --modes core,atomic_flush,best_efforts,ts,tiered_storage \
  --extra-flags "--enable_blob_direct_write=1 --enable_blob_files=1 \
    --blob_direct_write_partitions=4 --blob_direct_write_buffer_size=1048576 \
    --use_txn=0 --use_optimistic_txn=0"

# Quick: 5-min target, debug only
stress_fix_loop.sh --repo ~/workspace/rocksdb \
  --target-duration 300 --variants debug --parallel 2

# Stop a running loop (kills entire process group cleanly)
stress_fix_loop.sh --repo ~/workspace/rocksdb --stop
```

**Key flags:**
| Flag | Default | Description |
|------|---------|-------------|
| `--repo DIR` | current dir | Path to RocksDB checkout |
| `--target-duration N` | 3600 | Seconds that must pass clean to declare success |
| `--parallel N` | 4 | Parallel runs per variant |
| `--variants LIST` | debug,asan,tsan | Build variants |
| `--modes LIST` | all | Mode groups to test |
| `--extra-flags F` | | Extra flags for db_crashtest.py |
| `--max-iterations N` | 10 | Give up after N fix rounds |
| `--push` | | Push to GitHub after passing |
| `--stop` | | Stop the running loop for this repo |

**How it works:**
1. Runs `run_stress_matrix.sh` with escalating batch durations up to `--target-duration`
2. On failure: collects failure logs, writes a prompt with full context, launches CC to analyze and fix
3. CC reads failures, fixes code, builds `db_stress`, runs unit tests — but does NOT commit
4. Loop commits CC's changes locally, rebuilds worktrees, and re-runs the stress matrix
5. Repeats until all batches pass clean or `--max-iterations` is reached
6. On success with `--push`: pushes to GitHub

**Concurrency safety:**
- PID file at `/tmp/stress-fix-loop-{SLUG}.pid` (SLUG = first 8 chars of md5 of repo path)
- Auto-detects if another loop for the same repo is already running
- `--stop` kills the entire process group (loop + matrix + crashtest + db_stress)
- **Never use `pkill -f stress`** — it kills loops for ALL repos. Always use `--stop`.

## Common Pitfalls

1. **Worktrees only see committed state.** Uncommitted changes are invisible to worktrees. Always `git commit` before running stress tests.
2. **Stale worktrees cause build failures.** If you see "getcwd: cannot access parent directories", force-remove and re-add the worktree.
3. **`set -e` and stress test scripts don't mix.** The stress matrix returns non-zero on test failures — `set -e` treats this as fatal and kills your script.
4. **Blanket `rm -rf /tmp/rocksdb_crashtest_*` during runs** will delete databases from concurrent tests. Use `--destroy_db_initially=1` instead.
5. **Extra-flags can bypass `finalize_and_sanitize()`** — the most common source of "impossible" parameter combinations.


## Execution Tracing for Debugging

When a stress test failure is hard to reproduce or the root cause is unclear from
logs alone, you can build `db_stress` with execution tracing enabled. This
instruments every function entry with a lightweight ring-buffer trace that is
dumped on crash, showing the exact sequence of function calls leading up to the
failure.

### Building with Trace Support

```bash
# Makefile
STRESS_TRACE=1 DEBUG_LEVEL=0 make -j$(nproc) db_stress

# CMake
cmake -DWITH_STRESS_TRACE=ON -DCMAKE_BUILD_TYPE=Release ..
make -j$(nproc) db_stress
```

The `STRESS_TRACE=1` flag adds `-DROCKSDB_STRESS_TRACE -finstrument-functions`
to all compilation units. This inserts a call to a recording function at every
function entry, which writes to a per-thread lock-free ring buffer.

### What Gets Captured

Two types of trace entries are interleaved by timestamp:

1. **Function entries** (automatic): Every function call across all RocksDB code
   is recorded with its address and an `rdtsc` timestamp. The ring buffer keeps
   the last 8192 entries per thread (~128 KB per thread).

2. **Semantic events** (manual): Code can call `STRESS_TRACE_EVENT("msg", ...)`
   at key decision points to record human-readable context. These use a separate
   ring buffer (last 256 entries per thread).

### Crash Dump Output

On crash (SIGABRT, SIGSEGV, SIGQUIT, etc.), per-thread trace files are written to:
```
<db_path>/../stress-trace-<pid>-thread-<tid>.txt
```

Each file contains an interleaved, time-ordered trace of function entries and
semantic events for that thread. Example:

```
=== Stress Trace: thread 2533811810808473599 ===
    func_entries=8192  event_entries=3
  [4434887560583342] ENTER rocksdb::DBImpl::Write (0x936d15)
  [4434887560583357] ENTER rocksdb::WriteBatchInternal::Count (0x92f368)
  [4434887560583388] EVENT flush_reason=2 imm_count=3
  [4434887560583404] ENTER rocksdb::WriteToWAL (0x936df0)
  ...
```

### Resolving Addresses

Function addresses in the trace can be resolved to symbols using `addr2line`:

```bash
# Single address
addr2line -f -e db_stress 0x936d15

# Batch resolve all addresses from a trace file
grep "ENTER 0x" trace-file.txt |   sed 's/.*ENTER \(0x[0-9a-f]*\).*//' |   sort -u |   xargs -I{} sh -c 'echo -n "{} -> "; addr2line -f -e db_stress {}'
```

For binaries with debug info (`DEBUG_LEVEL=2`), `dladdr()` resolves symbols
directly in the crash dump (no post-processing needed).

### Adding Semantic Events

To add context at key decision points, use the `STRESS_TRACE_EVENT` macro:

```cpp
#include "monitoring/stress_trace.h"

// At a key decision point:
STRESS_TRACE_EVENT("flush: reason=%d imm_count=%zu cf=%s",
                   flush_reason, imm_count, cf_name.c_str());

// In error handling:
STRESS_TRACE_EVENT("write failed: %s key=%s", s.ToString().c_str(), key.c_str());
```

The macro is a no-op when `ROCKSDB_STRESS_TRACE` is not defined, so it has zero
cost in normal builds.

### Performance Overhead

| Function type | Overhead |
|---|---|
| Real work (>= 1μs) | < 0.1% — indistinguishable from noise |
| Trivial leaf functions | ~2x (dominated by ring buffer write) |

For stress tests, the overhead is negligible because RocksDB functions do real
work (I/O, memcpy, comparisons). To exclude specific hot leaf functions:

```cpp
ROCKSDB_NO_INSTRUMENT int MyHotFunction() { ... }
```

### Workflow: Debugging a Stress Test Failure

1. Reproduce the failure with `STRESS_TRACE=1`:
   ```bash
   STRESS_TRACE=1 DEBUG_LEVEL=0 make -j$(nproc) db_stress
   python3 tools/db_crashtest.py blackbox --simple ...
   ```

2. On failure, find the trace files:
   ```bash
   ls /tmp/rocksdb_crashtest_*/stress-trace-*.txt
   ```

3. Resolve addresses and examine the trace:
   ```bash
   # Find the thread that crashed (largest/most recent trace file)
   ls -lS /tmp/rocksdb_crashtest_*/stress-trace-*.txt | head -1
   
   # Resolve and view
   cat trace.txt | head -100
   ```

4. The trace shows the last ~8K function calls before the crash — usually enough
   to reconstruct the exact code path and identify the race condition or logic error.

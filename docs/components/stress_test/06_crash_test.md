# Crash Test Orchestration

**Files:** `tools/db_crashtest.py`

## Overview

`db_crashtest.py` is the Python orchestration script that wraps `db_stress` to perform crash-recovery testing. It generates random configurations, repeatedly starts and kills `db_stress`, and validates that the database recovers correctly after each crash.

## Blackbox Mode

Blackbox mode simulates power failures by externally killing the `db_stress` process at timed intervals.

### Execution Flow

Step 1: Generate parameters via `gen_cmd_params()`, which merges default params with test-type-specific params and applies `finalize_and_sanitize()`. Step 2: Launch `db_stress` as a subprocess. Step 3: Wait for `--interval` seconds (default: 120). Step 4: Kill `db_stress` with SIGTERM. If unresponsive, escalate to SIGKILL. Step 5: Wait 1-2 seconds for stabilization. Step 6: Restart `db_stress` with the same `--db` directory. The process recovers from WAL and runs `VerifyDb()`. Step 7: Repeat until `--duration` seconds have elapsed (default: 6000). Step 8: Run a final verification-only pass with `--verification_only=1` and a longer timeout (`--verify_timeout`, default: 1200 seconds).

### Key Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `--duration` | 6000 | Total test duration in seconds |
| `--interval` | 120 | Seconds between crashes |
| `--verify_timeout` | 1200 | Timeout for final verification |
| `--ops_per_thread` | 100000000 | Large value since process is killed externally |
| `--reopen` | 0 | Disabled in blackbox mode |
| `--set_options_one_in` | 1000 | Dynamic option changes during run |
| `--disable_wal` | random (0 with 75% probability, 1 with 25% probability) | WAL can be disabled to test non-WAL recovery |

## Whitebox Mode

Whitebox mode injects crashes from within `db_stress` itself using kill points, allowing precise control over crash timing (e.g., mid-flush, mid-compaction).

### Kill Points

In debug builds, `KillPoint::GetInstance()` maintains a counter (`rocksdb_kill_odds`) that controls crash probability. At various code locations (write paths, flush, compaction), the kill point check calls `exit()` with probability `1/rocksdb_kill_odds`.

### Check Modes

Whitebox mode cycles through four check modes:

| Mode | Description |
|------|-------------|
| 0 | Crash mode with `--kill_random_test` enabled. Cycles through three kill sub-modes varying kill odds and excluded prefixes. |
| 1 | Normal run with universal compaction (`--compaction_style=1`). Sometimes tests single-level universal. |
| 2 | Normal run with FIFO compaction (`--compaction_style=2`). Reduced ops (1/5) since FIFO is slower on reads. |
| 3 | Normal run with base-parameter compaction style (no override). |

### Kill Sub-Modes (within check mode 0)

| Sub-Mode | Kill Odds | Excluded Prefixes |
|----------|-----------|-------------------|
| 0 | `random_kill_odd` (default: 888887) | None |
| 1 | `kill_odds // 10 + 1` (or `// 50 + 1` with WAL disabled) | `WritableFileWriter::Append`, `WritableFileWriter::WriteBuffered` |
| 2 | `kill_odds // 5000 + 1` | Above plus `PosixMmapFile::Allocate`, `WritableFileWriter::Flush` |

Higher sub-modes use lower odds (more frequent kills) but exclude common write paths, focusing crashes on less-frequent code paths.

### Key Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `--duration` | 10000 | Total test duration |
| `--ops_per_thread` | 200000 | Operations per thread per run |
| `--random_kill_odd` | 888887 | Base kill probability (1/N) |
| `--reopen` | 20 | Periodic DB reopens between kills |
| `--log2_keys_per_lock` | 10 | One lock per 1024 keys |
| `--disable_wal` | 0 | WAL always enabled in whitebox mode |

## Recovery Verification

After each crash (or kill), `db_stress` restarts and verifies the database:

Step 1: Open the database, which triggers WAL replay and recovery. Step 2: If `--expected_values_dir` is set, `FileExpectedStateManager::Restore()` replays the write trace to reconstruct the expected state at the recovery point. Step 3: All worker threads run `VerifyDb()` during initialization. Step 4: If verification passes, the test prints "Crash-recovery verification passed" and cleans up unverified state. Step 5: If verification fails, the test prints "Crash-recovery verification failed" and the run is considered failed.

### Unverified State Preservation

When `--preserve_unverified_changes=1` is set, the test creates shadow copies of the DB and expected state directories before starting operations. If a crash occurs and recovery verification fails, these copies can be used for post-mortem analysis.

## Running Crash Tests

### Make Targets

| Target | Description |
|--------|-------------|
| `make crash_test` | Runs blackbox crash test with default params |
| `make crash_test_with_atomic_flush` | Runs with `--atomic_flush=1` |
| `make crash_test_with_wc_txn` | Runs with write-committed transactions |
| `make crash_test_with_wp_txn` | Runs with write-prepared transactions |
| `make crash_test_with_wup_txn` | Runs with write-unprepared transactions |
| `make crash_test_with_best_efforts_recovery` | Runs with best-efforts recovery |
| `make crash_test_with_ts` | Runs with user-defined timestamps |
| `make crash_test_with_tiered_storage` | Runs with tiered storage |
| `make crash_test_with_multiops_txn` | Runs with multi-operation transactions |
| `make crash_test_with_multiops_wc_txn` | Runs with multi-ops write-committed transactions |
| `make crash_test_with_multiops_wp_txn` | Runs with multi-ops write-prepared transactions |

Note: `make crash_test_with_txn` is a deprecated alias for `crash_test_with_wc_txn`.

### Manual Execution

```bash
# Blackbox mode
python3 tools/db_crashtest.py blackbox --duration=3600 --interval=60

# Whitebox mode
python3 tools/db_crashtest.py whitebox --duration=3600

# With specific test type
python3 tools/db_crashtest.py blackbox --simple --duration=3600
python3 tools/db_crashtest.py blackbox --cf_consistency --duration=3600
python3 tools/db_crashtest.py blackbox --txn --duration=3600
```

### Sanitizer Builds

To detect memory and concurrency bugs, crash tests should be run under various sanitizers. The `CRASH_TEST_EXT_ARGS` environment variable can pass additional arguments to `db_crashtest.py`.

### Seed Reproducibility

Both `db_crashtest.py` and `db_stress` use seeds for reproducibility. The `--initial_random_seed_override` and `--per_iteration_random_seed_override` flags control the Python-side randomization. The `--seed` flag controls C++-side key generation and operation sequencing. Seeds are printed at the start of each run.

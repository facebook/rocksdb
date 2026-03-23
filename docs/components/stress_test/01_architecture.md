# Architecture and Execution Flow

**Files:** `db_stress_tool/db_stress_tool.cc`, `db_stress_tool/db_stress_driver.cc`, `db_stress_tool/db_stress_test_base.{h,cc}`, `db_stress_tool/db_stress_listener.h`, `db_stress_tool/db_stress_env_wrapper.h`, `tools/db_crashtest.py`

## Design Philosophy

The stress test framework complements unit tests by covering scenarios that are impractical to test deterministically:

| Unit Tests | Stress Tests |
|-----------|-------------|
| Deterministic | Randomized |
| Single feature focus | Feature combinations |
| Short execution | Continuous execution |
| Controlled failures | Simulated crashes and faults |

The framework has two main components: `db_stress` (C++ binary) performs the actual database operations and verification, while `db_crashtest.py` (Python script) orchestrates crash-recovery testing by repeatedly starting, killing, and restarting `db_stress`.

## Two-Layer Architecture

### Layer 1: db_crashtest.py (Orchestrator)

`db_crashtest.py` generates random parameter configurations, launches `db_stress`, and manages the crash-restart loop. It operates in two modes (blackbox and whitebox, described in Chapter 6) and supports multiple test types (simple, cf_consistency, txn, ts, multiops_txn, tiered, blob, best_efforts_recovery).

Step 1: Generate random parameters from layered parameter dicts. Step 2: Resolve lambdas and apply sanitization rules via `finalize_and_sanitize()`. Step 3: Launch `db_stress` with the resolved parameters. Step 4: After the configured interval, kill `db_stress` (blackbox) or wait for internal crash (whitebox). Step 5: Restart `db_stress` which recovers the DB and verifies expected state. Step 6: Repeat until total duration expires. Step 7: Run a final verification-only pass.

### Layer 2: db_stress (Worker)

`db_stress` is the C++ binary that performs database operations. Its entry point is `db_stress_tool()` in `db_stress_tool/db_stress_tool.cc`.

Step 1: Parse command-line flags and validate compatibility (operation percentages must sum to 100, incompatible options are rejected). Step 2: Set up the environment, including `FaultInjectionTestFS` if fault injection is enabled. Step 3: Select test mode based on flags and instantiate the appropriate `StressTest` subclass. Step 4: Call `RunStressTest()` which delegates to `RunStressTestImpl()` in `db_stress_driver.cc`. Step 5: Initialize the DB via `StressTest::InitDb()` and `FinishInitDb()`. Step 6: Spawn worker threads that execute `ThreadBody()`. Step 7: Wait for all threads to complete, then aggregate and report statistics.

## Test Mode Selection

The test mode is selected in `db_stress_tool()` based on mutually exclusive flags:

| Flag | Test Mode | Factory Function |
|------|-----------|-----------------|
| `--test_cf_consistency=1` | `CfConsistencyStressTest` | `CreateCfConsistencyStressTest()` |
| `--test_batches_snapshots=1` | `BatchedOpsStressTest` | `CreateBatchedOpsStressTest()` |
| `--test_multi_ops_txns=1` | `MultiOpsTxnsStressTest` | `CreateMultiOpsTxnsStressTest()` |
| (default) | `NonBatchedOpsStressTest` | `CreateNonBatchedOpsStressTest()` |

All test modes inherit from `StressTest` (see `db_stress_tool/db_stress_test_base.h`), which provides shared infrastructure: DB lifecycle management, thread orchestration, transaction support, and expected state tracking.

## RunStressTestImpl Workflow

The core execution flow in `RunStressTestImpl()` (see `db_stress_tool/db_stress_driver.cc`) orchestrates the thread lifecycle:

Step 1: If crash-recovery verification is enabled (`--expected_values_dir` is set), initialize unverified state directories. Step 2: Call `stress->InitDb()` and `stress->FinishInitDb()` to open the database. Step 3: Create `ThreadState` objects and spawn worker threads via `db_stress_env->StartThread(ThreadBody, ...)`. Step 4: Optionally spawn background threads for compaction thread pool adjustment, continuous verification, compressed cache capacity changes, and remote compaction workers. Step 5: Wait for all worker threads to signal `AllInitialized()` (crash-recovery verification happens during initialization). Step 6: Enable fault injection and auto-compaction, then signal threads to start operations. Step 7: Wait for `AllOperated()`, then signal threads to start final verification. Step 8: Wait for `AllDone()`, merge per-thread statistics, and report results.

## ThreadBody Lifecycle

Each worker thread executes `ThreadBody()` in `db_stress_driver.cc`:

Step 1: Register thread status. Step 2: If crash-recovery verification is enabled, run `VerifyDb()` to validate the DB against persisted expected state. Step 3: Call `IncInitialized()` and wait for all threads to initialize. Step 4: Wait for the `start_` signal from the main thread. Step 5: Execute `OperateDb()` which runs the main operation loop for `--ops_per_thread` iterations. Step 6: Call `IncOperated()` and wait for all threads to finish operating. Step 7: Run final `VerifyDb()` unless `--skip_verifydb` is set. Step 8: Call `IncDone()` and signal completion.

## Environment Setup

`db_stress` wraps the base filesystem with several layers:

| Layer | Class | Purpose |
|-------|-------|---------|
| Base | `Env` / `FileSystem` | Platform filesystem (or custom via `--env_uri` / `--fs_uri`) |
| Fault injection | `FaultInjectionTestFS` | Injects I/O errors and simulates power failures (when any fault injection flag is enabled) |
| Stress wrapper | `DbStressFSWrapper` | Verifies IOActivity correctness and file checksum propagation for SST opens |

The fault injection layer is set to "direct writable" (bypass faults) during DB open, then switched to active fault injection after initialization completes.

## Event Listener

`DbStressListener` (see `db_stress_tool/db_stress_listener.h`) implements `EventListener` to coordinate with the stress test framework:

- **Fault injection on background threads**: Enables thread-local error injection context during flush and compaction callbacks (`OnFlushBegin`, `OnCompactionBegin`), ensuring background operations are also subject to fault injection.
- **Sequence number tracking**: Records the latest sequence number after flush and compaction events for expected state synchronization.
- **Unique ID validation**: Verifies that SST files report consistent unique IDs across the `GetUniqueId()` and `GetUniqueIdFromTableProperties()` APIs.

This listener is critical because without it, fault injection would only affect foreground (worker thread) operations.

## Build Modes

The stress test framework supports multiple build configurations for different types of bug detection:

| Build | Purpose | Command |
|-------|---------|---------|
| Debug | Assertions enabled, kill points active | `make db_stress` |
| Release | Performance testing, no assertions | `DEBUG_LEVEL=0 make db_stress` |
| ASAN | Address sanitizer for memory bugs | `COMPILE_WITH_ASAN=1 make db_stress` |
| TSAN | Thread sanitizer for data races | `COMPILE_WITH_TSAN=1 make db_stress` |
| UBSAN | Undefined behavior sanitizer | `COMPILE_WITH_UBSAN=1 make db_stress` |

Important: Read fault injection (`--read_fault_one_in`) is only supported in debug builds because it relies on sync points (`IGNORE_STATUS_IF_ERROR`) that are compiled out in release mode.

Note: The whitebox crash mode requires debug builds because kill points use `KillPoint::GetInstance()` which is compiled out when `NDEBUG` is defined.

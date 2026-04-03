# Thread Model and Synchronization

**Files:** `db_stress_tool/db_stress_shared_state.h`, `db_stress_tool/db_stress_stat.h`, `db_stress_tool/db_stress_driver.cc`

## SharedState: Thread Coordination

`SharedState` (see `db_stress_tool/db_stress_shared_state.h`) coordinates all stress test threads. It owns the expected state manager, key locks, and synchronization primitives.

### Core Fields

| Field | Type | Purpose |
|-------|------|---------|
| `mu_` | `port::Mutex` | Protects shared state fields |
| `cv_` | `port::CondVar` | Thread synchronization for lifecycle transitions |
| `seed_` | `uint32_t` | Global seed from `FLAGS_seed` |
| `max_key_` | `int64_t` | Total key space size from `FLAGS_max_key` |
| `num_threads_` | `int` | Total number of worker threads |
| `num_initialized_` | `long` | Threads that completed initialization |
| `num_populated_` | `long` | Threads that finished stress operations |
| `num_done_` | `long` | Threads that completed final verification |
| `start_` | `bool` | Signal for threads to begin operations |
| `start_verify_` | `bool` | Signal for threads to begin final verification |
| `verification_failure_` | `std::atomic<bool>` | Set by any thread that detects a verification error |
| `should_stop_test_` | `std::atomic<bool>` | Set to request graceful test termination |
| `expected_state_manager_` | `unique_ptr<ExpectedStateManager>` | Manages expected values |
| `key_locks_` | `vector<unique_ptr<port::Mutex[]>>` | Per-key lock arrays, one per column family |
| `no_overwrite_ids_` | `unordered_set<int64_t>` | Keys that should never be overwritten |

### Thread Lifecycle Phases

All worker threads progress through these phases, synchronized via `mu_` and `cv_`:

Phase 1 -- Initialization: Each thread runs `VerifyDb()` if crash-recovery verification is enabled, then calls `IncInitialized()`. The main thread waits for `AllInitialized()`.

Phase 2 -- Wait: Threads wait on `cv_` until the main thread calls `SetStart()`.

Phase 3 -- Operation: Threads execute `OperateDb()` for `--ops_per_thread` iterations. When done, each thread calls `IncOperated()`. The main thread waits for `AllOperated()`.

Phase 4 -- Verification: After all threads finish operating, the main thread calls `SetStartVerify()`. Threads run final `VerifyDb()`, then call `IncDone()`.

Phase 5 -- Completion: The main thread waits for `AllDone()`, then merges statistics and reports results.

Important: All threads must call `IncInitialized()` before the stress test starts. If any thread fails to initialize, `AllInitialized()` will never be true and all threads will deadlock waiting on `cv_`.

### No-Overwrite Keys

A subset of keys is designated as "no overwrite" based on `--nooverwritepercent`. These keys are selected using a Knuth shuffle seeded with `FLAGS_seed`, ensuring the same keys are chosen across crash-restart invocations. The set is stored in `no_overwrite_ids_` and checked via `AllowsOverwrite()`.

## Key Locks

### Locking Granularity

Key locks prevent races between threads operating on the same key. The granularity is controlled by `--log2_keys_per_lock`:

- One lock covers `2^log2_keys_per_lock` consecutive keys
- Total locks per CF = `max_key >> log2_keys_per_lock` (rounded up)
- Default gflag value is `log2_keys_per_lock = 2` (one lock per 4 keys). The whitebox crash test overrides this to 10 (one lock per 1024 keys)

### Lock Acquisition

`GetMutexForKey(cf, key)` returns the lock for a given key:

```
lock_index = key >> log2_keys_per_lock
lock = key_locks_[cf][lock_index]
```

For delete range operations, `GetLocksForKeyRange(cf, start, end)` acquires all locks covering the range `[start, end)`, returning a vector of `MutexLock` RAII guards.

For column family clear operations, `LockColumnFamily(cf)` and `UnlockColumnFamily(cf)` acquire all locks in the column family.

Important: In NonBatchedOps mode, the key lock must be held while both updating expected state and performing the DB operation. Releasing the lock between these steps allows another thread to interleave, causing expected state desynchronization.

Note: In `BatchedOpsStressTest` mode (`--test_batches_snapshots=1`), key locks are not created because expected state is not tracked externally.

## ThreadState: Per-Thread State

Each worker thread has a `ThreadState` struct (see `db_stress_tool/db_stress_shared_state.h`):

| Field | Type | Purpose |
|-------|------|---------|
| `tid` | `uint32_t` | Thread ID (0 to n-1) |
| `rand` | `Random` | Thread-local PRNG, seeded with `1000 + tid + FLAGS_seed` |
| `shared` | `SharedState*` | Pointer to shared coordination state |
| `stats` | `Stats` | Per-thread operation statistics |
| `snapshot_queue` | queue of `(sequence_number, SnapshotState)` pairs | Snapshots acquired during operation for later release and verification |

### Deterministic Seeding

Each thread's PRNG is seeded with `1000 + tid + FLAGS_seed`. This ensures that for the same seed and thread ID, the operation sequence is reproducible, enabling failure reproduction by replaying with the same `--seed` value.

### Snapshot State

When a snapshot is acquired (controlled by `--acquire_snapshot_one_in`), the thread records the snapshot along with metadata about what was read at that snapshot. This `SnapshotState` includes the column family, key, value, and status at read time. When the snapshot is later released, the recorded state is verified against the current expected state to detect snapshot isolation violations.

## Background Threads

In addition to worker threads, `RunStressTestImpl()` spawns several optional background threads:

| Thread | Flag | Purpose |
|--------|------|---------|
| Pool size change | `--compaction_thread_pool_adjust_interval > 0` | Periodically adjusts the compaction thread pool size |
| Continuous verification | `--continuous_verification_interval > 0` | Opens a secondary instance and continuously verifies DB state |
| Compressed cache capacity | `--compressed_secondary_cache_size > 0` | Randomly adjusts compressed cache capacity |
| Remote compaction workers | `--remote_compaction_worker_threads > 0` | Process remote compaction jobs from a shared queue |

All background threads check `SharedState::ShouldStopBgThread()` to know when to exit, and call `IncBgThreadsFinished()` upon completion.

## Statistics

Per-thread statistics are tracked in the `Stats` class (see `db_stress_tool/db_stress_stat.h`). Key counters include `gets_`, `writes_`, `deletes_`, `single_deletes_`, `iterations_`, `range_deletions_`, `founds_`, `errors_`, and `verified_errors_`. At the end of the test, all per-thread stats are merged into thread 0's stats via `Stats::Merge()` and reported via `Stats::Report()`.

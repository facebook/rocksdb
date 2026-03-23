# RocksDB Stress Testing Framework

**Purpose**: Validate RocksDB correctness and stability under extreme concurrent load, random configuration changes, simulated failures, and crash-recovery scenarios.

**Key Files**:
- `db_stress_tool/db_stress_test_base.{h,cc}` — Base `StressTest` class
- `db_stress_tool/db_stress_tool.cc` — Test mode selection and entry point
- `db_stress_tool/db_stress_driver.cc` — Thread orchestration and run loop
- `db_stress_tool/no_batched_ops_stress.cc` — Non-batched operations mode
- `db_stress_tool/batched_ops_stress.cc` — Batched operations mode
- `db_stress_tool/expected_state.{h,cc}` — Expected value tracking
- `db_stress_tool/expected_value.{h,cc}` — Expected value encoding
- `db_stress_tool/db_stress_shared_state.h` — Thread coordination (`SharedState`, `ThreadState`)
- `db_stress_tool/db_stress_stat.h` — Per-thread statistics (`Stats`)
- `db_stress_tool/db_stress_gflags.cc` — Configuration flags
- `tools/db_crashtest.py` — Crash test orchestration script

---

## 1. db_stress Overview

`db_stress` is RocksDB's primary correctness testing tool. It:

1. **Generates random workloads**: Concurrent mix of Put/Get/Delete/Merge/Iterator operations with randomized keys and values
2. **Randomizes DB configuration**: Block size, cache size, compression, compaction style, memtable type, etc.
3. **Injects faults**: Simulates I/O errors, crashes, and data corruption
4. **Validates correctness**: Maintains expected state and compares against actual DB state
5. **Tests recovery**: Randomly kills and restarts the DB to validate crash recovery

**How it finds bugs**:
- **Data loss**: Expected value exists but DB returns NotFound
- **Corruption**: Value from DB doesn't match expected value
- **Assertion failures**: Internal invariant violations (e.g., snapshot isolation)
- **Deadlocks**: Threads hang waiting for locks
- **Crashes**: Segfaults, aborts, uncaught exceptions

**Example command**:
```bash
./db_stress --max_key=100000 --ops_per_thread=10000 \
  --threads=32 --column_families=10 \
  --clear_column_family_one_in=1000
```

---

## 2. StressTest Base Class and Test Modes

All stress tests inherit from `StressTest` (defined in `db_stress_tool/db_stress_test_base.h`). The base class provides:
- DB lifecycle management (`InitDb`, `FinishInitDb`, `CleanUp`)
- Thread orchestration (`OperateDb`, thread pool management)
- Expected state tracking (`TrackExpectedState`)
- Transaction support (`NewTxn`, `CommitTxn`, `ExecuteTransaction`)

**⚠️ INVARIANT**: All subclasses must implement these pure virtual methods: `VerifyDb()`, `ContinuouslyVerifyDb()`, `IsStateTracked()`, `TestGet()`, `TestMultiGet()`, `TestGetEntity()`, `TestMultiGetEntity()`, `TestPrefixScan()`, `TestPut()`, `TestDelete()`, `TestDeleteRange()`, and `TestIngestExternalFile()`.

### Test Modes

#### 2.1 NonBatchedOpsStressTest

**Source**: `db_stress_tool/no_batched_ops_stress.cc`

Standard stress test mode where each operation (Put/Get/Delete) is performed individually. This mode:
- Tracks expected state with lock-per-key granularity
- Verifies each operation against expected state using multiple methods (iterator, Get, MultiGet, GetEntity)
- Supports secondary instance validation (if `FLAGS_test_secondary` is enabled)

**State tracking**: `IsStateTracked()` returns `true`. Expected state is maintained in `SharedState::expected_state_manager_`.

**Verification methods** (randomly selected):
1. **Iterator**: Seeks to start key and iterates forward, comparing with expected values
2. **Get**: Issues individual Get() calls for each key
3. **GetEntity**: Uses wide-column Get API
4. **MultiGet**: Batches Get() calls
5. **MultiGetEntity**: Batches wide-column Get calls
6. **GetMergeOperands**: Validates merge operand history

**⚠️ INVARIANT**: For iterator verification, if `iter->key() < expected_key`, the test aborts (indicates out-of-order keys).

#### 2.2 BatchedOpsStressTest

**Enabled with**: `--test_batches_snapshots=1`

**Source**: `db_stress_tool/batched_ops_stress.cc`

Tests atomicity of WriteBatch operations. For a base key `K` and value `V`:
- **Put**: Writes 10 key-value pairs: `("0"+K, V+"0")`, `("1"+K, V+"1")`, ..., `("9"+K, V+"9")` in a single batch
- **Get**: Reads all 10 keys in the same snapshot and verifies consistency (all values must share the same base)
- **Delete**: Deletes all 10 keys in a single batch

**State tracking**: `IsStateTracked()` returns `false`. Verification is done inline during Get operations rather than against external expected state.

**Why this mode matters**: Validates that WriteBatch operations are truly atomic — either all 10 writes succeed or all fail. Partial application would be detected as inconsistent values.

**Limitations**:
- Does not support `TestDeleteRange()` or `TestIngestExternalFile()`
- Cannot use expected state verification

#### 2.3 CfConsistency Test

**Enabled with**: `--test_cf_consistency=1`

**Source**: `db_stress_tool/cf_consistency_stress.cc`

Tests atomicity of writes across multiple column families when `atomic_flush` is enabled. Uses snapshots to verify that all column families see consistent state at the same point in time.

**⚠️ INVARIANT**: If `atomic_flush=true` and writes span multiple CFs, all CFs must either see the write or not see it — no partial visibility.

**Config restrictions** (from `db_crashtest.py` `cf_consistency_params`):
- `enable_compaction_filter=0` (incompatible with snapshots)
- `inplace_update_support=0` (incompatible with snapshots)
- `ingest_external_file_one_in=0` (not implemented for CF consistency)

#### 2.4 MultiOpsTxnsStressTest

**Enabled with**: `--test_multi_ops_txns=1`

**Source**: `db_stress_tool/multi_ops_txns_stress.h`

Simulates a relational table with primary and secondary indexes, testing multi-operation transactions. Operations include:
- Read-Modify-Write sequences
- Cross-key consistency (e.g., updating primary key and secondary index atomically)
- Rollback and commit paths

**Why this mode matters**: Exercises TransactionDB and OptimisticTransactionDB with realistic access patterns (unlike single-key operations in other modes).

---

## 3. Crash Test (db_crashtest.py)

`db_crashtest.py` is a Python wrapper around `db_stress` that orchestrates crash-recovery testing.

### 3.1 Blackbox vs. Whitebox

#### Blackbox Mode (default)

**How it works**:
1. Start `db_stress` in background
2. Let it run for `--interval` seconds (default: 120)
3. Kill `db_stress` with SIGTERM (falls back to SIGKILL if unresponsive)
4. Restart `db_stress` with same config
5. DB recovers from WAL and validates expected state
6. Repeat until total `--duration` seconds elapsed (default: 6000)

**Verification**: After recovery, `db_stress` runs `VerifyDb()` to compare DB state against expected state persisted in `FLAGS_expected_values_dir`.

**⚠️ INVARIANT**: After crash recovery, all writes up to the last persisted WAL record must be recoverable. Expected state tracks pending writes to handle crashes mid-operation.

**Example**:
```bash
python3 tools/db_crashtest.py blackbox --duration=6000 --interval=120
```

#### Whitebox Mode

**How it works**:
1. Start `db_stress` with `--kill_random_test=N` flag
2. db_stress inserts "kill points" at various code locations (write paths, flush, compaction)
3. At each kill point, db_stress calls `exit()` with probability `1/N`, simulating a crash
4. `db_crashtest.py` detects the exit, then restarts `db_stress` which recovers the DB
5. Validates recovery at each crash point

Additionally, whitebox mode cycles through multiple `check_mode` values:
- **Mode 0**: Runs with `kill_random_test` enabled (crash mode)
- **Mode 1**: Normal run with universal compaction
- **Mode 2**: Normal run with FIFO compaction
- **Mode 3**: Normal run with default compaction

**Key difference from blackbox**: Crashes are triggered from within `db_stress` itself via kill points, allowing precise control over crash timing (e.g., mid-flush, mid-compaction).

**Verification**: Continuous verification thread (`ContinuouslyVerifyDb()`) runs in parallel, periodically checking DB state.

**Example**:
```bash
python3 tools/db_crashtest.py whitebox --duration=10000
```

### 3.2 Parameter Randomization

`db_crashtest.py` generates random DB configurations for each run. See `default_params` dict:

```python
default_params = {
    "block_size": 16384,
    "bloom_bits": lambda: random.choice([random.randint(0, 19), random.lognormvariate(2.3, 1.3)]),
    "cache_size": lambda: random.choice([8388608, 33554432]),
    "compression_type": lambda: random.choice(["none", "snappy", "zlib", "lz4", "lz4hc", "xpress", "zstd"]),
    "max_key": random.choice([100000, 25000000]),
    # ... hundreds more parameters
}
```

**⚠️ INVARIANT**: `max_key` and `seed` must be **fixed across invocations** when using `--expected_values_dir` for verification. Otherwise, key generation is non-deterministic and verification will fail.

**Parameter overwrite priority**:
1. `default_params` (base configuration)
2. `{blackbox,whitebox}_default_params` (mode-specific overrides)
3. `simple_default_params` / `cf_consistency_params` / `txn_params` / etc. (feature-specific configs)
4. Command-line arguments (highest priority)

---

## 4. Key-Value Verification: Expected State Tracking

### 4.1 ExpectedState Architecture

**Source**: `db_stress_tool/expected_state.h`

`ExpectedState` maintains the expected value for every key in the database. Two implementations:

#### FileExpectedState
- Backs expected state with memory-mapped file (`expected_state_file_path`)
- Persists across crashes for crash-recovery verification
- Used when `FLAGS_expected_values_dir` is set

#### AnonExpectedState
- Backs expected state with in-memory allocation
- Does not persist across crashes
- Used when `FLAGS_expected_values_dir` is empty (verification-only mode)

**Storage layout**:
```
values_[cf * max_key + key] = std::atomic<uint32_t>
```

Each `uint32_t` encodes:
- Value base (bits for generating expected value)
- Deletion marker (indicates if key is deleted)
- Pending bit (indicates in-flight operation)

### 4.2 ExpectedValue Encoding

**Source**: `db_stress_tool/expected_value.h`

The expected value for a key is encoded in a single `uint32_t`:

```
Bit layout:
  [0-14]  - Value base (15 bits)
  [15]    - Pending write flag
  [16-29] - Deletion counter (14 bits, tracks number of times this value base has been deleted)
  [30]    - Pending delete flag
  [31]    - Deleted flag
```

**Operations**:
- `Put(bool pending)`: When `pending=true`, only sets the pending write flag. When `pending=false`, advances value base to next value, clears deleted flag, clears pending write flag.
- `Delete(bool pending)`: When `pending=true`, sets pending delete flag (returns `false` if key doesn't exist). When `pending=false`, advances deletion counter, sets deleted flag, clears pending delete flag. Returns `bool` indicating whether the key existed.
- `Exists()`: Returns true if key is not deleted (asserts no pending operations)
- `Read()`: Returns raw uint32_t encoding

**⚠️ INVARIANT**: All updates to `ExpectedState` must use atomic operations to prevent data races in multi-threaded scenarios.

### 4.3 PendingExpectedValue: Two-Phase Commit

**Source**: `db_stress_tool/expected_value.h`

When an operation (Put/Delete) is started, the expected state is updated in two phases:

1. **Precommit**: Mark operation as pending in expected state
2. **Commit/Rollback**: Finalize expected state after DB operation completes

**Why this matters**: If a crash occurs between Precommit and Commit, the expected state correctly reflects the pending operation. During recovery verification, pending operations are treated as "may or may not exist" rather than strict "must exist".

**Example (Put operation)**:
```cpp
// Phase 1: Precommit (mark as pending)
PendingExpectedValue pending = expected_state->PreparePut(cf, key);
// pending.expected_ is now in "pending Put" state

// Phase 2: Perform DB operation
Status s = db_->Put(write_opts, key, value);

// Phase 3: Commit or Rollback (one MUST be called before destruction)
if (s.ok()) {
  pending.Commit();  // Clears pending bit, stores final value
} else {
  pending.Rollback();  // Reverts to original state
}
```

**⚠️ INVARIANT**: `PendingExpectedValue` requires explicit closure. Either `Commit()` or `Rollback()` must be called before destruction — the destructor asserts `pending_state_closed_` and aborts if neither was called. For cases where no pending state was introduced, `PermitUnclosedPendingState()` must be called explicitly.

### 4.4 Verification Logic

**Source**: `db_stress_tool/no_batched_ops_stress.cc:VerifyOrSyncValue()`

For each key, compare DB value against expected value:

```
Expected state: DELETED
  DB returns NotFound → ✓ Correct
  DB returns value → ✗ Resurrection bug

Expected state: EXISTS with value_base V
  DB returns value with value_base V → ✓ Correct
  DB returns value with value_base ≠ V → ✗ Corruption
  DB returns NotFound → ✗ Data loss

Expected state: PENDING
  DB returns value or NotFound → ✓ Either is acceptable (crash mid-operation)
```

**⚠️ INVARIANT**: If verification fails, `VerificationAbort()` prints diagnostics and sets `SharedState::verification_failure_`, causing all threads to stop.

---

## 5. Operation Mix: Configurable Weights

Operations are selected randomly based on configurable percentages (must sum to 100):

```cpp
// From db_stress_gflags.cc (default values)
DEFINE_int32(readpercent, 10, "");
DEFINE_int32(prefixpercent, 20, "");
DEFINE_int32(writepercent, 45, "");  // Put or PutEntity
DEFINE_int32(delpercent, 15, "");    // Delete or SingleDelete
DEFINE_int32(delrangepercent, 0, "");
DEFINE_int32(iterpercent, 10, "");
DEFINE_int32(customopspercent, 0, "");  // Custom operations
```

**Operation selection** (from `db_stress_tool/db_stress_test_base.cc:OperateDb()`):
```cpp
int prob_op = thread->rand.Uniform(100);
if (prob_op >= 0 && prob_op < static_cast<int>(FLAGS_readpercent)) {
  TestGet(thread);
} else if (prob_op < prefix_bound) {
  TestPrefixScan(thread);
} else if (prob_op < write_bound) {
  TestPut(thread);
} // ... etc
```

**Probabilistic operations** (controlled by `_one_in` flags):
- `--flush_one_in`: Probability of calling `Flush()` (e.g., 1000 → 1/1000 chance per operation)
- `--compact_range_one_in`: Probability of calling `CompactRange()`
- `--checkpoint_one_in`: Probability of creating a checkpoint
- `--backup_one_in`: Probability of creating a backup
- `--acquire_snapshot_one_in`: Probability of acquiring a snapshot

**Example**: `--flush_one_in=1000` means on average, flush is called once every 1000 operations.

---

## 6. Random Option Generation

### 6.1 Static vs. Dynamic Options

**Static options** (fixed at DB open):
- `max_key`: Total key space size
- `column_families`: Number of column families
- `compaction_style`: Level, Universal, FIFO
- `use_txn`: Whether to use TransactionDB

**Dynamic options** (changeable via `SetOptions()`):
- `write_buffer_size`, `max_write_buffer_number`
- `level0_slowdown_writes_trigger`, `level0_stop_writes_trigger`
- `compression_type` (per-level), `target_file_size_base`

**⚠️ INVARIANT**: When `--set_options_one_in > 0`, db_stress randomly calls `SetOptions()` to change dynamic options. All options must remain compatible (e.g., can't set `write_buffer_size` larger than `db_write_buffer_size`).

### 6.2 BuildOptionsTable

**Source**: `db_stress_tool/db_stress_test_base.cc:BuildOptionsTable()`

Builds a table of valid option values for dynamic SetOptions. Returns `bool` indicating success.

```cpp
std::unordered_map<std::string, std::vector<std::string>> options_tbl = {
  {"write_buffer_size", {
    std::to_string(options_.write_buffer_size),
    std::to_string(options_.write_buffer_size * 2),
    std::to_string(options_.write_buffer_size * 4)
  }},
  {"max_write_buffer_number", {
    std::to_string(options_.max_write_buffer_number),
    std::to_string(options_.max_write_buffer_number * 2)
  }},
  // ...
};
```

When `SetOptions()` is called, a random option is selected from the table and applied.

**Sanitization** (from `db_crashtest.py:finalize_and_sanitize()` and feature param dicts):
- If `inplace_update_support=1`, disable delete range (`delrangepercent=0`)
- If `test_batches_snapshots=1`, disable compaction filter, inplace update, fault injection
- If `disable_wal=1`, enable `atomic_flush`, disable `sync`
- In `ts_params`: if `user_timestamp_size > 0`, disable merge operations (`use_merge=0`)

---

## 7. Fault Injection: FaultInjectionTestFS

**Source**: `utilities/fault_injection_fs.h`

`FaultInjectionTestFS` wraps the base `FileSystem` and injects errors at various points:

### 7.1 Error Types

- **Read errors**: `--read_fault_one_in` — Fails `Read()` calls
- **Write errors**: `--write_fault_one_in` — Fails `Write()` calls
- **Metadata read errors**: `--metadata_read_fault_one_in` — Fails reading file metadata (size, modification time)
- **Metadata write errors**: `--metadata_write_fault_one_in` — Fails `Sync()`, `Fsync()`, directory operations

**Example config**:
```bash
--write_fault_one_in=1000 --exclude_wal_from_write_fault_injection=true
```
→ 1/1000 writes to SST files fail, but WAL writes are not affected

### 7.2 Unsynced Data Loss Simulation

**Flag**: `--sync_fault_injection=1`

Simulates power failure by:
1. Tracking all written data that has not been synced
2. On simulated crash, deleting unsynced data
3. Reopening DB and validating recovery

**⚠️ INVARIANT**: After power failure, only data that was synced (via `Sync()` or `Fsync()`) must survive. Unsynced data may be lost.

**Implementation**:
- `FaultInjectionTestFS::DeleteFilesCreatedAfterLastDirSync()`: Removes files created after last directory sync
- `FaultInjectionTestFS::DropUnsyncedFileData()`: Truncates unsynced data

### 7.3 Error Severity Levels

**Flag**: `--inject_error_severity` (default: 1)

Controls whether injected errors are **retryable** or **permanent**:
- **1** (default): Soft errors (retryable, e.g., `IOStatus::IOError()` with retryable bit set)
- **2**: Fatal errors (permanent failures, DB may need to stop)

**Recovery behavior**:
- Retryable errors → DB retries operation
- Fatal errors → DB may enter read-only mode or crash

**⚠️ INVARIANT**: DB must not lose data on retryable errors. Only data-loss-flagged errors are allowed to cause data loss.

---

## 8. Multi-Threaded Stress: Shared vs. Per-Thread State

### 8.1 SharedState: Thread Coordination

**Source**: `db_stress_tool/db_stress_shared_state.h`

`SharedState` coordinates all stress test threads:

```cpp
class SharedState {
  port::Mutex mu_;                     // Protects shared state
  port::CondVar cv_;                   // Thread synchronization
  int num_threads_;                    // Total threads
  long num_initialized_;               // Threads that finished initialization
  long num_populated_;                 // Threads that finished preload
  long num_done_;                      // Threads that finished stress test
  bool start_;                         // Signal to start stress test
  std::atomic<bool> verification_failure_;  // Any thread failed verification

  std::unique_ptr<ExpectedStateManager> expected_state_manager_;  // Expected value tracking
  std::vector<std::unique_ptr<port::Mutex[]>> key_locks_;  // Per-key locks
};
```

**Thread lifecycle** (see `db_stress_driver.cc:ThreadBody()`):
1. Thread runs initial `VerifyDb()` (if crash-recovery verification enabled) → `IncInitialized()`
2. Wait on `cv_` until `start_ == true`
3. Run operations via `OperateDb()` → `IncOperated()`
4. Wait until `AllOperated()` and `VerifyStarted()`, then run `VerifyDb()`
5. After verification → `IncDone()`
6. All threads done → `AllDone() == true` → Main thread proceeds

**⚠️ INVARIANT**: All threads must call `IncInitialized()` before stress test starts. Otherwise, `AllInitialized()` will never be true and threads will deadlock waiting on `cv_`.

### 8.2 Key Locks: Preventing Conflicting Updates

**Locking granularity**: One lock per `2^log2_keys_per_lock` keys.

**Example**: `--log2_keys_per_lock=10`
- Lock 0 covers keys [0, 1023]
- Lock 1 covers keys [1024, 2047]
- ...

**Lock acquisition** (source: `db_stress_tool/db_stress_shared_state.h:SharedState::GetMutexForKey()`):
```cpp
port::Mutex* GetMutexForKey(int cf, int64_t key) {
  return &key_locks_[cf][key >> log2_keys_per_lock_];
}
```

**Why locking is needed**:
- Without locks, two threads could race: Thread A reads expected value → Thread B updates expected value → Thread A updates expected value → Mismatch
- Locks ensure expected state updates are serialized

**⚠️ INVARIANT**: For non-batched mode, lock must be held while updating expected state AND performing DB operation. Releasing lock between these two steps allows races.

### 8.3 ThreadState: Per-Thread State

**Source**: `db_stress_tool/db_stress_shared_state.h`

Each thread has its own `ThreadState`:

```cpp
struct ThreadState {
  uint32_t tid;                      // Thread ID
  Random rand;                       // Thread-local PRNG
  SharedState* shared;               // Pointer to shared state
  Stats stats;                       // Per-thread operation stats
  std::queue<std::pair<uint64_t, SnapshotState>> snapshot_queue;  // Queue of snapshots to be released
  struct SnapshotState {
    const Snapshot* snapshot;
    int cf_at;                       // CF from which we read at this snapshot
    std::string cf_at_name;          // CF name at read time
    std::string key;                 // Key read at this snapshot
    Status status;                   // Status of the read
    std::string value;               // Value read
    std::vector<bool>* key_vec;      // Optional key vector for full DB state comparison
    std::string timestamp;           // Timestamp (if user_timestamp_size > 0)
  };
};
```

**Thread-local random number generator**:
- Seeded with `1000 + tid + FLAGS_seed`
- Ensures reproducible operation sequences per thread (for debugging)

**Per-thread stats**:
```cpp
class Stats {
  long done_;
  long gets_;
  long prefixes_;
  long writes_;
  long deletes_;
  size_t single_deletes_;
  long iterator_size_sums_;
  long founds_;
  long iterations_;
  long range_deletions_;
  long covered_by_range_deletions_;
  long errors_;
  long verified_errors_;
  long num_compact_files_succeed_;
  long num_compact_files_failed_;
  size_t bytes_;
  // ... plus timing and histogram fields
};
```

Aggregated at the end and printed by `StressTest::PrintStatistics()`.

---

## 9. How to Add New Features to Stress Test

### 9.1 Adding a New Option

**Steps**:

1. **Add FLAG to `db_stress_gflags.cc`**:
   ```cpp
   DEFINE_int32(my_new_option, 0, "Description of my new option");
   ```

2. **Add to `db_crashtest.py` parameter dicts**:
   ```python
   default_params = {
       # ...
       "my_new_option": lambda: random.choice([0, 1, 10, 100]),
   }
   ```

3. **Use the flag in `StressTest`**:
   ```cpp
   void StressTest::InitDb(SharedState* shared) {
     options_.my_new_option = FLAGS_my_new_option;
     // ...
   }
   ```

4. **Test sanitization** (if needed):
   Add to `finalize_and_sanitize()` in `db_crashtest.py` if your option conflicts with existing options.

### 9.2 Adding a New Operation

**Steps**:

1. **Add operation percentage FLAG**:
   ```cpp
   DEFINE_int32(my_op_percent, 0, "Percentage of my_op operations");
   ```

2. **Implement operation in `StressTest` subclass**:
   ```cpp
   Status NonBatchedOpsStressTest::TestMyOp(ThreadState* thread) {
     int64_t rand_key = GenerateOneKey(thread, iteration);
     // Acquire lock
     std::unique_ptr<MutexLock> lock(
         new MutexLock(thread->shared->GetMutexForKey(cf, rand_key)));

     // Update expected state
     PendingExpectedValue pending = expected_state_->PrepareMyOp(cf, rand_key);

     // Perform DB operation
     Status s = db_->MyOp(...);

     // Commit or rollback expected state
     if (s.ok()) {
       pending.Commit();
     } else {
       pending.Rollback();
     }
     return s;
   }
   ```

3. **Add to operation selection in `OperateDb()` (in `db_stress_test_base.cc`)**:
   ```cpp
   int sum = FLAGS_readpercent + FLAGS_writepercent + ... + FLAGS_my_op_percent;
   assert(sum == 100);  // Must sum to 100

   if (rand < FLAGS_readpercent) {
     TestGet(thread);
   } else if (...) {
     // ...
   } else if (rand < sum - FLAGS_my_op_percent) {
     TestMyOp(thread);
   }
   ```

4. **Add expected state support** (if needed):
   Extend `ExpectedState` and `ExpectedValue` to handle the new operation.

### 9.3 Adding a New Test Mode

**Steps**:

1. **Create new subclass of `StressTest`**:
   ```cpp
   class MyNewStressTest : public StressTest {
    public:
     bool IsStateTracked() const override { return true; }

     void VerifyDb(ThreadState* thread) const override {
       // Custom verification logic
     }

     void ContinuouslyVerifyDb(ThreadState* thread) const override {
       // Continuous verification logic
     }

     Status TestGet(...) override {
       // Custom Get implementation
     }
     // ... other operations
   };
   ```

2. **Add factory function**:
   ```cpp
   StressTest* CreateMyNewStressTest() {
     return new MyNewStressTest();
   }
   ```

3. **Add to `db_stress_tool/db_stress_tool.cc`** (test selection logic):
   ```cpp
   if (FLAGS_test_my_new_mode) {
     stress.reset(CreateMyNewStressTest());
   } else if (...) {
     // ...
   }
   ```

4. **Add to `db_crashtest.py`**:
   ```python
   my_new_mode_params = {
       "test_my_new_mode": 1,
       "reopen": 0,  # Disable reopens if incompatible
       # ... other restrictions
   }
   ```

---

## 10. Common Crash Test Failure Patterns and Debugging

### 10.1 Failure Pattern: Data Loss

**Symptom**:
```
Verification failed for key 12345 in cf 0:
  Expected: value_base=678901
  Actual: NotFound
```

**Possible causes**:
1. **Bug in WAL recovery**: Write was committed but not recovered after crash
2. **Incorrect expected state**: Expected state incorrectly marked key as existing
3. **Compaction bug**: Key was dropped during compaction despite being live

**Debugging**:
1. Check WAL logs: Was the write persisted to WAL before crash?
2. Check expected state file: Was the expected state correctly updated?
3. Add logging to compaction: Was the key incorrectly considered obsolete?

**How stress test detects this**:
- `VerifyDb()` compares expected state (key exists) against DB state (NotFound)
- If mismatch, calls `VerificationAbort()` which prints key, expected value, actual value

### 10.2 Failure Pattern: Corruption

**Symptom**:
```
Verification failed for key 67890 in cf 1:
  Expected: value_base=111213
  Actual: value_base=141516
```

**Possible causes**:
1. **Checksum mismatch**: Data corruption in SST file or WAL
2. **Merge operator bug**: Incorrect merge result
3. **Race condition**: Two threads wrote to same key without proper locking

**Debugging**:
1. Enable `--paranoid_file_checks=1`: Validates checksums on every read
2. Add logging to merge operator: Print all merge operands and result
3. Check thread sanitizer output: Race detected?

**How stress test detects this**:
- Generates value from `value_base` using `GenerateValue(value_base, ...)`
- Compares generated value against DB value
- If mismatch, corruption detected

### 10.3 Failure Pattern: Assertion Failure

**Symptom**:
```
db_stress: db/version_set.cc:123: Assertion `level < num_levels_` failed
```

**Possible causes**:
1. **Invariant violation**: Internal RocksDB invariant broken (e.g., level number out of range)
2. **Configuration bug**: Incompatible options (e.g., `num_levels=3` but compaction tried to use level 5)
3. **Concurrency bug**: Race condition in multi-threaded code

**Debugging**:
1. Reproduce with `--seed=X` and `--max_key=Y` from failed run (printed in crash output)
2. Run under debugger (gdb): `gdb --args ./db_stress --seed=X ...`
3. Enable thread sanitizer: `COMPILE_WITH_TSAN=1 make db_stress`

**How stress test surfaces this**:
- Asserts fire immediately when invariant is violated
- Stack trace printed to stderr
- Core dump generated (if enabled)

### 10.4 Failure Pattern: Deadlock

**Symptom**:
```
[After 10 minutes]
No progress: All threads blocked
```

**Possible causes**:
1. **Lock ordering bug**: Thread A acquires lock1 then lock2; Thread B acquires lock2 then lock1
2. **Condition variable bug**: Thread waiting on CV but no thread signals it
3. **Leaked lock**: Lock acquired but never released

**Debugging**:
1. Attach debugger to running process: `gdb -p <pid>`
2. Print all thread stacks: `thread apply all bt`
3. Look for threads waiting on mutexes or condition variables

**How stress test detects this**:
- Timeout in `db_crashtest.py` (blackbox default: 120 seconds per interval)
- If timeout exceeded, kills db_stress with SIGTERM (then SIGKILL if unresponsive)

### 10.5 Debugging Tips

**Reproduce failures**:
```bash
# Seed is printed at start of run
./db_stress --seed=1234567890 --max_key=100000 ...
```

**Run under valgrind** (for memory errors):
```bash
valgrind --leak-check=full ./db_stress --ops_per_thread=1000
```

**Enable verbose logging**:
```bash
./db_stress --verbose=1 --ops_per_thread=1000 2>&1 | tee stress.log
```

**Bisect to find culprit commit** (if failure is recent):
```bash
git bisect start
git bisect bad HEAD
git bisect good <last-known-good-commit>
make clean && make db_stress
python3 tools/db_crashtest.py blackbox --interval=120
# ... mark good/bad until culprit found
```

**Check continuous integration**:
- RocksDB CI runs stress tests on every PR
- Check CircleCI or GitHub Actions for stress test results
- Reproduce CI failures locally with same config

---

## Stress Test Execution Flow Diagram

```
db_crashtest.py
  |
  +-- Blackbox Mode / Whitebox Mode
  |     Randomize params
  |     |
  |     v
  |   Generate db_stress command line
  |     --max_key --threads --ops_per_thread
  |     --cache_size --compression_type ...
  |     |
  |     v
  |   Execute db_stress  <-- Restart after crash (loop)
  |     |
  |     v
  |   Wait --interval seconds
  |     (blackbox: external kill)
  |     (whitebox: internal crash)

db_stress
  |
  +-- Main Thread
  |     1. Parse FLAGS
  |     2. InitDb() - Open DB with randomized options
  |     3. Create SharedState and ThreadStates
  |     4. Spawn worker threads
  |
  +-- Worker Threads (--threads=N)
  |     Loop for --ops_per_thread iterations:
  |       1. Select random operation (Get/Put/Delete/...)
  |       2. Select random key (GenerateOneKey)
  |       3. Acquire key lock (if state tracked)
  |       4. Update expected state (Precommit)
  |       5. Perform DB operation
  |       6. Commit/Rollback expected state
  |       7. Release lock
  |
  |     Periodically:
  |       - Flush (--flush_one_in)
  |       - Compact (--compact_range_one_in)
  |       - Checkpoint (--checkpoint_one_in)
  |       - SetOptions (--set_options_one_in)
  |
  +-- Verification (after all ops complete)
        VerifyDb():
          For each key in key space:
            expected = ExpectedState.Get(cf, key)
            actual = db_->Get(key)
            if (expected != actual):
              VerificationAbort()  // Print mismatch & exit

FaultInjectionTestFS
  Intercepts FileSystem calls:
    - Read()  -> Inject error with probability 1/FLAGS_read_fault
    - Write() -> Inject error 1/FLAGS_write_fault
    - Sync()  -> Track synced data, inject metadata errors

  On crash:
    - DropUnsyncedFileData()                  -> Simulate power failure
    - DeleteFilesCreatedAfterLastDirSync()    -> Orphan files
```

---

## Related Documentation

- `ARCHITECTURE.md` — Overall RocksDB architecture and invariants
- `docs/components/write_flow.md` — WAL and memtable write mechanics
- `docs/components/flush.md` — Flush mechanics
- `docs/components/compaction.md` — Compaction mechanics
- `utilities/fault_injection_fs.h` — Fault injection details

# Expected State Tracking

**Files:** `db_stress_tool/expected_value.{h,cc}`, `db_stress_tool/expected_state.{h,cc}`, `db_stress_tool/no_batched_ops_stress.cc`

## Overview

The expected state system is the core correctness mechanism of the stress test. It maintains an independent in-memory (or file-backed) record of what every key in the database should contain, enabling detection of data loss, corruption, and resurrection bugs after crash recovery.

## ExpectedValue Encoding

Each key's expected state is encoded in a single `uint32_t` (see `ExpectedValue` in `db_stress_tool/expected_value.h`):

| Bits | Field | Width | Purpose |
|------|-------|-------|---------|
| 0-14 | Value base | 15 bits | Identifies the expected value content |
| 15 | Pending write | 1 bit | Set when a Put is in progress but not yet committed |
| 16-29 | Deletion counter | 14 bits | Number of times this value base has been deleted |
| 30 | Pending delete | 1 bit | Set when a Delete is in progress but not yet committed |
| 31 | Deleted | 1 bit | Whether this key is currently deleted |

### Key Operations

- **`Put(pending)`**: When `pending=true`, sets the pending write flag only. When `pending=false`, advances value base to the next value, clears the deleted flag, and clears the pending write flag.
- **`Delete(pending)`**: When `pending=true`, sets the pending delete flag (returns false if the key is already marked as deleted in the expected state). When `pending=false`, advances the deletion counter, sets the deleted flag, and clears the pending delete flag.
- **`Exists()`**: Returns true if the key is not deleted. Asserts that no pending operations are active.
- **`GetValueBase()`**: Returns the current value base (bits 0-14).

## PendingExpectedValue: Two-Phase Commit

The `PendingExpectedValue` class (see `db_stress_tool/expected_value.h`) ensures crash-safe expected state updates by splitting each operation into two phases:

Step 1 -- Precommit: Mark the operation as pending in the expected state. This records that a write or delete is in flight. Step 2 -- DB operation: Perform the actual database operation. Step 3 -- Commit or Rollback: If the DB operation succeeded, call `Commit()` to finalize the expected state. If it failed, call `Rollback()` to revert to the original state.

### Why Two-Phase Commit Matters

If a crash occurs between precommit and commit, the pending flag remains set. During recovery verification, the system treats keys with pending operations as "may or may not exist" rather than strictly requiring existence, avoiding false verification failures.

### Closure Requirement

Key Invariant: `PendingExpectedValue`'s destructor asserts that `pending_state_closed_` is true. Either `Commit()`, `Rollback()`, or `PermitUnclosedPendingState()` must be called before the object is destroyed. Failure to do so triggers an assertion failure.

The `Commit()` and `Rollback()` methods both use `std::atomic_thread_fence(std::memory_order_release)` before storing the final value, ensuring the expected state update is visible to other threads only after the fence.

## ExpectedState Storage

`ExpectedState` (see `db_stress_tool/expected_state.h`) provides read/write access to the array of `std::atomic<uint32_t>` values. The storage layout is:

```
values_[cf * max_key + key] = std::atomic<uint32_t>
```

Two implementations exist:

| Implementation | Backing | Persistence | Use Case |
|----------------|---------|-------------|----------|
| `FileExpectedState` | Memory-mapped file | Survives crashes | Crash-recovery verification (`--expected_values_dir` is set) |
| `AnonExpectedState` | Heap allocation | Lost on crash | In-process verification only |

Note: `FileExpectedState` also persists a `SequenceNumber` (`persisted_seqno`) in a separate memory-mapped file, used by the trace-and-replay mechanism to determine the recovery point.

## ExpectedStateManager

`ExpectedStateManager` (see `db_stress_tool/expected_state.h`) sits above `ExpectedState` and adds history tracking for crash recovery:

### FileExpectedStateManager

Provides `SaveAtAndAfter()` and `Restore()` for crash-recovery verification:

Step 1 -- `SaveAtAndAfter(db)`: Copies the current `LATEST.state` file to `<seqno>.state` and starts a write trace in `<seqno>.trace`. Step 2 -- During operation, all writes are recorded in the trace file. Step 3 -- After crash recovery, `Restore(db)` reads the current sequence number `b`, replays `b - a` write operations from `<a>.trace` onto `<a>.state`, and copies the result into `LATEST.state`.

This mechanism allows the expected state to be reconstructed to the exact point where the database recovered, even if some in-flight operations were lost.

### AnonExpectedStateManager

Returns `Status::NotSupported()` for `SaveAtAndAfter()` and `Restore()` since in-memory state cannot survive crashes.

## Verification Logic

Verification is performed by `VerifyOrSyncValue()` in `NonBatchedOpsStressTest` (see `db_stress_tool/no_batched_ops_stress.cc`). For each key, the DB value is compared against the expected value:

| Expected State | DB Returns | Result |
|---------------|-----------|--------|
| DELETED | NotFound | Correct |
| DELETED | Value | Resurrection bug |
| EXISTS (value_base=V) | Value with value_base=V | Correct |
| EXISTS (value_base=V) | Value with value_base!=V | Corruption |
| EXISTS (value_base=V) | NotFound | Data loss |
| PENDING | Value or NotFound | Acceptable (crash mid-operation) |

### Pre-Read / Post-Read Validation

For concurrent reads (where the expected state may change during the read), the framework reads the expected value twice -- once before and once after the DB read. `ExpectedValueHelper::MustHaveExisted()` and `MustHaveNotExisted()` use both readings to determine whether the key was definitely present or absent throughout the read window. `InExpectedValueBaseRange()` validates that a returned value base falls within the range of values that could have been active during the read.

### Verification Failure

When verification fails, `VerificationAbort()` (see `StressTest` in `db_stress_tool/db_stress_test_base.h`) prints diagnostic information including the key, column family, expected value, and actual value, then sets `SharedState::verification_failure_` to stop all threads.

# Fix Summary: transaction

## Issues Fixed

| Category | Count |
|----------|-------|
| Correctness | 18 |
| Completeness | 8 |
| Structure/style | 3 |
| Total | 29 |

## Disagreements Found

6 disagreements documented in `transaction_debates.md`:
1. DFS vs BFS for cycle detection (Codex correct)
2. OptimisticTransaction::Prepare() return status (Codex correct)
3. PessimisticTransaction::Prepare() unnamed return status (Codex correct)
4. waiter_queue ownership: PointLockManager vs PerKeyPointLockManager (Codex correct)
5. deadlock_timeout_us scope (Codex correct)
6. IncrementWaiters lock ordering relative to stripe mutex (Codex correct)

## Changes Made

### index.md
- Changed "DFS-based" to "BFS-based" in chapter 4 description and key characteristics
- Moved "Transactions are NOT thread-safe" from "Key Invariants" to new "Key Constraints" section

### 01_types_and_models.md
- Fixed optimistic conflict status: added `Status::TryAgain()` alongside `Status::Busy()`
- Fixed 2PC support column: "NotSupported" -> "InvalidArgument"

### 02_write_policies.md
- Fixed `two_write_queues` description: not the default for WritePrepared, lives in `DBOptions`, defaults to `false`
- Changed commit cost from "Light (WAL marker only)" to "Light (WAL marker in common case)" for WritePrepared/WriteUnprepared

### 03_lock_management.md
- Fixed lock manager selection precedence: `lock_mgr_handle` takes highest precedence unconditionally
- Fixed `waiter_queue` in `LockInfo`: clarified it exists in the struct but is only used by `PerKeyPointLockManager`, not the default
- Rewrote lock acquisition flow to mention stripe condition variables and deadlock detection step
- Added note distinguishing PointLockManager (stripe CVs) from PerKeyPointLockManager (per-key queues)
- Fixed lock upgrade: "in-place only when sole shared holder"
- Fixed `num_stripes`: noted 0 is treated as 1
- Fixed `max_num_locks` status: `Status::Busy()` -> `Status::Aborted()` / `IsLockLimit()`
- Expanded `PerKeyPointLockManager` section with: per-key waiter queues, upgrade priority, delayed deadlock detection, wake-up efficiency, thread-local KeyLockWaiter optimization
- Scoped lock expiration/stealing to point lock manager; clarified `Status::Expired()` surfaces on `Prepare()`/`Commit()`

### 04_deadlock_detection.md
- Changed DFS to BFS throughout
- Added range-lock manager deadlock reporting to overview
- Fixed deadlock detection result: `Status::Busy()` -> `Status::Busy(SubCode::kDeadlock)`
- Scoped `deadlock_timeout_us` to `PerKeyPointLockManager` only (default ignores it)
- Fixed lock ordering note: `IncrementWaiters()` called with stripe mutex held, not after releasing

### 05_two_phase_commit.md
- Fixed Prepare unnamed status: `TxnNotPrepared` -> `InvalidArgument`
- Expanded `SetName()` constraints: empty name, >512 bytes, already named, wrong state
- Fixed WritePrepared rollback: "Both prepare_seq entries" -> "prepare_seq and rollback batch's sequence number"
- Added `rollback_merge_operands` option to configuration table

### 06_write_prepared_internals.md
- Added "(not user-configurable; internal testing parameter)" to CommitCache size description
- Added `prep_seq == 0` special case (Step 0) to IsInSnapshot algorithm

### 08_snapshot_and_conflicts.md
- Fixed `SetSnapshot()`: clarified it affects conflict checking only, NOT read visibility
- Scoped `OptimisticTransactionCallback` to serial validation only
- Expanded error codes table: added `Busy(SubCode::kDeadlock)`, `Aborted(IsLockLimit)`, separated deadlock from general Busy

### 09_optimistic_internals.md
- Fixed `Prepare()` return: `NotSupported` -> `InvalidArgument` (twice)
- Added bucket ordering detail to parallel validation: hash to buckets, deduplicate, lock in ascending order
- Added `occ_lock_buckets` minimum clamp to 16

### 10_api_and_lifecycle.md
- Added WritePrepared/WriteUnprepared NotSupported note for multi-CF iterators
- Changed DisableIndexing behavior from "NOT visible" to "undefined" (matching API comment)

### 11_advanced_features.md
- Kept `Merge()` in commit bypass unsupported list (CC review incorrectly claimed PR #13410 enabled it; WBWIMemTable handles merges but the transaction-level bypass path still rejects them per the TransactionOptions comment)
- Fixed `GetTimestampedSnapshots` range: documented half-open `[ts_lb, ts_ub)` semantics
- Scoped secondary indices to WriteCommitted only (via `SecondaryIndexMixin`)
- Fixed DB-level `skip_concurrency_control`: affects `TransactionDB::Write()` calls, not `BeginTransaction()` defaults

### 12_performance_and_best_practices.md
- Fixed WritePrepared write policy description to note commit-time batch exception
- Fixed non-transactional write description: clarified internal transaction creation via `CommitBatch()`
- Replaced DeleteRange prose with support matrix by DB type and write policy
- Removed non-existent `--transaction_write_policy` db_bench flag
- Fixed `two_write_queues` tuning: moved to `DBOptions`, default `false`, noted as recommended not default

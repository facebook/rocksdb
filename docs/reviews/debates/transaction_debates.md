# Transaction Documentation: Reviewer Disagreements

## Debate: Deadlock detection algorithm -- DFS vs BFS

- **CC position**: CC initially documents it as DFS in the positive notes ("correctly describes the DFS-based cycle detection") but then in the correctness issues section says the algorithm is actually BFS ("uses a queue with head/tail indices, processing nodes in FIFO order"). CC contradicts itself.
- **Codex position**: Codex clearly states BFS: "The point-lock search in IncrementWaiters() is breadth-first over an explicit queue, not DFS."
- **Code evidence**: `IncrementWaiters()` in `point_lock_manager.cc` (lines 870-933) uses `queue_values` and `queue_parents` arrays with `head` and `tail` indices. The loop `for (int tail = 0, head = 0; head < depth; head++)` processes elements in FIFO order -- `head` advances linearly while new elements are appended at `tail`. This is textbook BFS.
- **Resolution**: Codex is correct. CC caught the issue in the review but contradicted itself in the positive notes. The algorithm is BFS, not DFS. Fixed throughout.
- **Risk level**: medium -- calling it DFS vs BFS changes understanding of traversal order and worst-case behavior.

## Debate: Optimistic transaction Prepare() return status

- **CC position**: Did not flag this issue. The original doc says `Prepare()` returns `NotSupported`.
- **Codex position**: Says it returns `Status::InvalidArgument("Two phase commit not supported for optimistic transactions.")`, not `NotSupported`.
- **Code evidence**: `optimistic_transaction.cc` line 55-58: `Status OptimisticTransaction::Prepare() { return Status::InvalidArgument("Two phase commit not supported for optimistic transactions."); }`
- **Resolution**: Codex is correct. The return is `InvalidArgument`, not `NotSupported`. Fixed in chapters 01, 09.
- **Risk level**: medium -- callers checking `IsNotSupported()` vs `IsInvalidArgument()` would get wrong results.

## Debate: Pessimistic Prepare() return for unnamed transactions

- **CC position**: Did not flag the status code, only noted minor issues.
- **Codex position**: Says `Prepare()` on unnamed transaction returns `Status::InvalidArgument("Cannot prepare a transaction that has not been named.")`, not `Status::TxnNotPrepared()`.
- **Code evidence**: `pessimistic_transaction.cc` lines 590-594: the code checks `name_.empty()` and returns `Status::InvalidArgument(...)`.
- **Resolution**: Codex is correct. Fixed in chapter 05.
- **Risk level**: medium -- the doc used a non-existent status code.

## Debate: waiter_queue ownership -- PointLockManager vs PerKeyPointLockManager

- **CC position**: Correctly notes that `LockInfo` doesn't have a simple "list of transactions" waiter_queue and that the actual type is `KeyLockWaiter`. But implies waiter_queue is part of `LockInfo`.
- **Codex position**: Says the default `PointLockManager` waits on stripe condition variables; the per-key waiter queue and `KeyLockWaiter` machinery belong to `PerKeyPointLockManager`.
- **Code evidence**: Default `PointLockManager::AcquireWithTimeout` (line 582) ignores `deadlock_timeout_us` and waits on `stripe->stripe_cond_var_`. The `KeyLockWaiterContext` with per-key `waiter_queue` usage is only in `PerKeyPointLockManager::AcquireWithTimeout` (line 1138+). `LockInfo` does have a `waiter_queue` field (line 123), but it is only populated by `PerKeyPointLockManager`, not the default.
- **Resolution**: Both reviewers are partially right. CC correctly identified `KeyLockWaiter` as the waiter type, but implied the waiter_queue is always used. Codex correctly scoped it to `PerKeyPointLockManager`. Updated doc to note `waiter_queue` exists in `LockInfo` but is only used by `PerKeyPointLockManager`.
- **Risk level**: high -- conflating the two lock managers leads to incorrect mental model of the locking system.

## Debate: deadlock_timeout_us scope

- **CC position**: Documents `deadlock_timeout_us` as a general feature without scoping it to a specific lock manager.
- **Codex position**: Says "the base PointLockManager::AcquireWithTimeout() ignores deadlock_timeout_us; the delayed wait-then-detect behavior is implemented only by PerKeyPointLockManager."
- **Code evidence**: In `PointLockManager::AcquireWithTimeout` (line 582), the parameter is `int64_t /*deadlock_timeout_us*/` -- the commented-out parameter name means it's unused. In `PerKeyPointLockManager::AcquireWithTimeout` (line 1141), it's `int64_t deadlock_timeout_us` and is actively used (line 1172: `auto wait_before_deadlock_detection = txn->IsDeadlockDetect() && (deadlock_timeout_us > 0)`).
- **Resolution**: Codex is correct. `deadlock_timeout_us` is ignored by the default `PointLockManager`. Fixed in chapter 04.
- **Risk level**: medium -- users of the default lock manager would think delayed detection works when it doesn't.

## Debate: IncrementWaiters lock ordering relative to stripe mutex

- **CC position**: Did not flag this specific detail.
- **Codex position**: Says "the point-lock path calls IncrementWaiters() while the stripe mutex is still held."
- **Code evidence**: In `PointLockManager::AcquireWithTimeout` (line 632), `IncrementWaiters()` is called while inside the `stripe->stripe_mutex->Lock()` scope. The doc said "Deadlock detection acquires wait_txn_map_mutex_ after releasing stripe mutexes" which is wrong.
- **Resolution**: Codex is correct. Fixed the lock ordering note in chapter 04.
- **Risk level**: low -- internal implementation detail, but important for developers modifying the lock manager.

## Debate: Merge support in commit bypass memtable

- **CC position**: Claims PR #13410 added Merge support in WBWIMemTable, enabling Merge operations for commit bypass. Says to remove Merge from the unsupported operations list.
- **Codex position**: Did not specifically address this claim.
- **Code evidence**: `WBWIMemTable` does handle merge records (`wbwi_memtable.cc` line 14 maps `kMergeRecord` to `kTypeMerge`). However, `TransactionOptions::commit_bypass_memtable` comment in `include/rocksdb/utilities/transaction_db.h` line 412 explicitly says: "Transactions with Merge() or PutEntity() is not supported yet." The restriction is at the transaction-level commit bypass path, not in `WBWIMemTable`.
- **Resolution**: CC is wrong. While `WBWIMemTable` can handle merge records, the transaction-level commit bypass still rejects them. Kept `Merge()` in the unsupported list with a clarifying note.
- **Risk level**: medium -- incorrectly removing the restriction would cause users to hit runtime errors.

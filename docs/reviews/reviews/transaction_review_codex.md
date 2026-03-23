# Review: transaction — Codex

## Summary

Overall quality rating: **significant issues**

The documentation is well partitioned and easy to navigate, and the chapter split is close to what a maintainer would want for this subsystem. The index is the right size, each chapter has a `Files:` line, and the WritePrepared / WriteUnprepared chapters do a reasonable job of naming the main internal structures.

The problem is factual reliability. Many of the high-value claims at subsystem boundaries are wrong in the current code: lock-manager selection, deadlock coverage, `two_write_queues`, WritePrepared commit behavior, 2PC naming rules, iterator support, `DeleteRange`, and several newer APIs. The unit tests also disagree with the docs in multiple places, especially around `DeleteRange`, iterator support matrices, timestamped snapshots, and 2PC naming.

## Correctness Issues

### [WRONG] Optimistic transaction status codes and 2PC behavior are misstated
- **File:** `01_types_and_models.md`, `Optimistic Transactions`; `01_types_and_models.md`, `Choosing Between Models`
- **Claim:** "Returns `Status::Busy()` on conflict; caller must retry" and "2PC support | Yes | No (Prepare returns NotSupported)"
- **Reality:** `OptimisticTransaction::Prepare()` returns `Status::InvalidArgument("Two phase commit not supported for optimistic transactions.")`, not `NotSupported`. Conflict checks can return either `Status::Busy()` or `Status::TryAgain()` when memtable history is insufficient.
- **Source:** `utilities/transactions/optimistic_transaction.cc`, `OptimisticTransaction::Prepare`, `OptimisticTransaction::CheckTransactionForConflicts`; `utilities/transactions/transaction_util.cc`, `TransactionUtil::CheckKey`; `include/rocksdb/utilities/transaction.h`, `Transaction::Commit`
- **Fix:** Say that optimistic `Prepare()` fails with `InvalidArgument`, and that optimistic commit retries may be driven by either `Busy` or `TryAgain`.

### [WRONG] Lock-manager selection precedence is documented incorrectly
- **File:** `03_lock_management.md`, `Lock Manager Architecture`
- **Claim:** "Custom `lock_mgr_handle` with range support: Uses the provided range lock manager (e.g., `RangeTreeLockManager`)" and "`use_per_key_point_lock_mgr = true`: Creates a `PerKeyPointLockManager` (experimental)"
- **Reality:** `NewLockManager()` uses `lock_mgr_handle` unconditionally when it is present. `use_per_key_point_lock_mgr` is consulted only when no custom handle is supplied.
- **Source:** `utilities/transactions/lock/lock_manager.cc`, `NewLockManager`
- **Fix:** Document the precedence explicitly: custom `lock_mgr_handle` wins; otherwise RocksDB chooses between `PointLockManager` and `PerKeyPointLockManager`.

### [WRONG] `two_write_queues` is described as a WritePrepared default
- **File:** `02_write_policies.md`, `WritePrepared`; `12_performance_and_best_practices.md`, `WritePrepared/WriteUnprepared Tuning`
- **Claim:** "When `two_write_queues` is enabled (the default for WritePrepared)" and "`two_write_queues` | true (for WritePrepared)"
- **Reality:** `two_write_queues` is a `DBOptions` field that defaults to `false`. The transaction code uses it when configured, but does not auto-enable it for WritePrepared.
- **Source:** `include/rocksdb/options.h`, `DBOptions::two_write_queues`; `utilities/transactions/write_prepared_txn.cc`, `WritePreparedTxn::PrepareInternal`, `WritePreparedTxn::CommitInternal`
- **Fix:** Say that `two_write_queues` lives in `DBOptions`, defaults to `false`, and is an optional optimization for some WritePrepared / WriteUnprepared deployments.

### [WRONG] WritePrepared commit is not always "WAL marker only"
- **File:** `02_write_policies.md`, `Policy Comparison`; `12_performance_and_best_practices.md`, `Write Policy Selection`
- **Claim:** "`Commit cost | Heavy (data + WAL) | Light (WAL marker only) | Light (WAL marker only)`" and "Commit only writes WAL marker; prepare does the heavy lifting"
- **Reality:** WritePrepared commit can carry a non-empty commit-time batch. In that case commit may write data, not just a marker. With `two_write_queues`, that path can also require a second write to publish commit visibility after writing the commit-time batch.
- **Source:** `utilities/transactions/write_prepared_txn.cc`, `WritePreparedTxn::CommitInternal`
- **Fix:** Describe the common case as marker-only commit, but call out the commit-time batch exception and the possible second write when `two_write_queues` is enabled.

### [WRONG] The lock-management chapter conflates default point locks, per-key wait queues, and expiration semantics
- **File:** `03_lock_management.md`, `Point Lock Manager`; `03_lock_management.md`, `Lock Expiration`; `11_advanced_features.md`, `Transaction Expiration`
- **Claim:** "`waiter_queue`: list of transactions waiting for this lock", "If still not available, add to waiter queue, release mutex, and wait on condition variable with timeout", "When a lock's expiration time passes, other transactions can \"steal\" it", and "Subsequent operations on the expired transaction return `Status::Expired()`"
- **Reality:** The default `PointLockManager` waits on stripe condition variables in `AcquireWithTimeout`; the per-key waiter queue and `KeyLockWaiter` machinery belong to `PerKeyPointLockManager`. Expired-lock stealing is implemented in point-lock acquisition via `TryStealingExpiredTransactionLocks()`, but the range-lock manager does not expose the same path. Also, `Expired()` is surfaced on later transactional operations that check state, especially `Prepare()` and `Commit()`, not as a blanket result for every API call after expiry.
- **Source:** `utilities/transactions/lock/point/point_lock_manager.cc`, `PointLockManager::AcquireWithTimeout`, `PointLockManager::IsLockExpired`, `PerKeyPointLockManager::AcquireWithTimeout`; `utilities/transactions/pessimistic_transaction.cc`, `PessimisticTransaction::Prepare`, `PessimisticTransaction::Commit`; `utilities/transactions/lock/range/range_tree/range_tree_lock_manager.cc`, `RangeTreeLockManager::TryLock`
- **Fix:** Split the chapter into separate behavior for the default point-lock manager and the experimental per-key manager, and scope lock stealing to the point-lock implementation.

### [WRONG] Deadlock detection coverage, algorithm, result status, and internal locking are all misstated
- **File:** `04_deadlock_detection.md`, `Overview`; `04_deadlock_detection.md`, `Cycle Detection Algorithm`; `04_deadlock_detection.md`, `Deadlock Timeout`; `04_deadlock_detection.md`, `Lock Timeout vs. Deadlock Detection`; `04_deadlock_detection.md`, `Internal Lock Ordering`; `index.md`, `Chapters`; `index.md`, `Key Characteristics`
- **Claim:** "It is implemented in the `PointLockManager` using a wait-for graph and DFS-based cycle detection", "Perform DFS from the waiting transaction", "`TransactionOptions::deadlock_timeout_us` ... controls the delay before performing deadlock detection", "Deadlock detection | Circular wait detected | `Status::Busy()`", and "Deadlock detection acquires `wait_txn_map_mutex_` after releasing stripe mutexes"
- **Reality:** The point-lock search in `IncrementWaiters()` is breadth-first over an explicit queue, not DFS. Range-lock deadlocks are also reported through the locktree callbacks and then translated into the same public deadlock buffer format. The base `PointLockManager::AcquireWithTimeout()` ignores `deadlock_timeout_us`; the delayed wait-then-detect behavior is implemented only by `PerKeyPointLockManager`. Deadlock status is `Status::Busy(Status::SubCode::kDeadlock)`, and the point-lock path calls `IncrementWaiters()` while the stripe mutex is still held, matching the documented lock order instead of releasing stripes first.
- **Source:** `utilities/transactions/lock/point/point_lock_manager.cc`, `PointLockManager::AcquireWithTimeout`, `PointLockManager::IncrementWaiters`, `PerKeyPointLockManager::AcquireWithTimeout`; `utilities/transactions/lock/range/range_tree/range_tree_lock_manager.cc`, `RangeTreeLockManager::TryLock`, `wait_callback_for_locktree`, `RangeTreeLockManager::GetDeadlockInfoBuffer`
- **Fix:** Rewrite the chapter to distinguish point-lock and range-lock deadlock reporting, change DFS to BFS, document the `kDeadlock` subcode, and scope `deadlock_timeout_us` to the per-key manager.

### [WRONG] 2PC prepare and naming semantics are documented with the wrong status and incomplete preconditions
- **File:** `05_two_phase_commit.md`, `Protocol`; `05_two_phase_commit.md`, `Named Transactions`
- **Claim:** "If `skip_prepare` is false and the transaction has no name, return `Status::TxnNotPrepared()`" and "Important: `SetName()` returns `Status::InvalidArgument()` if the name is already in use. Transaction names must be unique across all active transactions."
- **Reality:** `Prepare()` on an unnamed transaction returns `Status::InvalidArgument("Cannot prepare a transaction that has not been named.")`, not `TxnNotPrepared`. `SetName()` also rejects empty names, names longer than 512 bytes, renaming an already named transaction, and naming after the transaction has left the `STARTED` state.
- **Source:** `utilities/transactions/pessimistic_transaction.cc`, `PessimisticTransaction::Prepare`, `PessimisticTransaction::SetName`; `utilities/transactions/transaction_test.cc`, `TwoPhaseNameTest`
- **Fix:** Say that unnamed prepare fails with `InvalidArgument`, and list the actual `SetName()` constraints.

### [WRONG] Snapshot read visibility and optimistic validation path are both described too broadly
- **File:** `08_snapshot_and_conflicts.md`, `Setting Snapshots`; `08_snapshot_and_conflicts.md`, `Commit-Time Validation`; `09_optimistic_internals.md`, `Serial Validation`; `09_optimistic_internals.md`, `Parallel Validation`
- **Claim:** "`SetSnapshot()`: Captures the current sequence number immediately. All subsequent reads see the database state at this point." and "The validation is performed inside a `WriteCallback` (`OptimisticTransactionCallback`), which is invoked during `DBImpl::WriteImpl()`"
- **Reality:** `SetSnapshot()` affects conflict checking, not what plain `Get()` returns. Read visibility still comes from `ReadOptions::snapshot`. Also, only serial optimistic validation uses `OptimisticTransactionCallback` inside the write group. Parallel validation locks OCC buckets, validates before entering the write group, and then issues a plain write.
- **Source:** `include/rocksdb/utilities/transaction.h`, `Transaction::SetSnapshot`, `Transaction::Get`; `utilities/transactions/optimistic_transaction.cc`, `CommitWithSerialValidate`, `CommitWithParallelValidate`
- **Fix:** Separate read visibility from conflict-validation baseline, and document the two optimistic commit paths separately.

### [WRONG] The iterator support matrix and `DisableIndexing()` semantics are overstated
- **File:** `10_api_and_lifecycle.md`, `Iterators`; `10_api_and_lifecycle.md`, `Indexing Control`
- **Claim:** "`Transaction::GetCoalescingIterator()` and `GetAttributeGroupIterator()` provide multi-column-family iteration with the same merge semantics" and "Disabled writes are NOT visible to `Get()` or `GetIterator()` on the transaction"
- **Reality:** `WritePreparedTxn` overrides both multi-CF iterator APIs to return `NotSupported`, and the transaction tests only expect them to work for WriteCommitted and optimistic transactions. For indexing control, the public API comment says later `Get`, `GetForUpdate`, and iterator results for those keys are undefined, not merely hidden.
- **Source:** `utilities/transactions/transaction_base.cc`, `TransactionBaseImpl::GetCoalescingIterator`, `TransactionBaseImpl::GetAttributeGroupIterator`; `utilities/transactions/write_prepared_txn.cc`, `WritePreparedTxn::GetCoalescingIterator`, `WritePreparedTxn::GetAttributeGroupIterator`; `include/rocksdb/utilities/transaction.h`, `Transaction::DisableIndexing`; `utilities/transactions/transaction_test.cc`, multi-CF iterator tests; `utilities/transactions/optimistic_transaction_test.cc`, multi-CF iterator tests
- **Fix:** Add a support matrix by write policy, and use the stronger "undefined" wording for disabled-indexing reads.

### [WRONG] `TransactionDBOptions::skip_concurrency_control` is documented as a transaction default even though the implementation uses it for DB writes
- **File:** `11_advanced_features.md`, `Skip Concurrency Control`
- **Claim:** "**DB-level:** When true on `TransactionDBOptions`, all transactions skip concurrency control by default unless overridden per-transaction."
- **Reality:** transaction objects initialize `skip_concurrency_control_` from `TransactionOptions::skip_concurrency_control`, not from the DB-level option. The DB-level flag changes the fast path for `TransactionDB::Write()` implementations.
- **Source:** `utilities/transactions/pessimistic_transaction.cc`, `PessimisticTransaction::Initialize`; `utilities/transactions/pessimistic_transaction_db.cc`, `WriteCommittedTxnDB::Write`; `utilities/transactions/write_prepared_txn_db.cc`, `WritePreparedTxnDB::Write`
- **Fix:** Describe the DB-level option as affecting direct `TransactionDB::Write()` calls, not as an implicit default for `BeginTransaction()`.

### [WRONG] Secondary-index support is documented as generic even though only WriteCommitted transactions are wrapped
- **File:** `11_advanced_features.md`, `Secondary Indices`
- **Claim:** "`TransactionDBOptions::secondary_indices` (experimental) allows registering secondary index implementations. These are automatically maintained when transactions modify primary keys."
- **Reality:** `WriteCommittedTxnDB::BeginTransaction()` is the only path that wraps transactions in `SecondaryIndexMixin`. Other pessimistic write policies do not use the mixin, and the tests explicitly bypass non-WriteCommitted combinations.
- **Source:** `utilities/transactions/pessimistic_transaction_db.cc`, `WriteCommittedTxnDB::BeginTransaction`; `utilities/transactions/transaction_test.cc`, `SecondaryIndexPutDelete`
- **Fix:** Scope the feature to WriteCommitted unless additional policies gain explicit support.

### [MISLEADING] Timestamped snapshot semantics are incomplete in ways that matter for callers
- **File:** `11_advanced_features.md`, `Timestamped Snapshots`
- **Claim:** "`GetTimestampedSnapshots(ts_lb, ts_ub)`: get snapshots in a timestamp range" and "`TransactionDB::CreateTimestampedSnapshot()` creates a snapshot associated with an application-provided timestamp"
- **Reality:** `CreateTimestampedSnapshot()` requires the caller to ensure there are no active writes. `GetTimestampedSnapshots()` uses a half-open range `[ts_lb, ts_ub)`, not an arbitrary inclusive range. The tests also enforce ordering constraints between sequence numbers and timestamps.
- **Source:** `include/rocksdb/utilities/transaction_db.h`, `TransactionDB::CreateTimestampedSnapshot`, `TransactionDB::GetTimestampedSnapshots`; `utilities/transactions/timestamped_snapshot_test.cc`, `CreateSnapshot`, `SequenceAndTsOrder`, `MultipleTimestampedSnapshots`
- **Fix:** Document the no-active-writes requirement, the half-open range semantics, and the sequence/timestamp monotonicity expectation.

### [WRONG] DeleteRange and `db_bench` guidance is incorrect
- **File:** `12_performance_and_best_practices.md`, `DeleteRange incompatibility`; `12_performance_and_best_practices.md`, `db_bench Transaction Benchmarks`
- **Claim:** "Users who know their range does not conflict with active transactions can use `TransactionDB::Write()` with `skip_concurrency_control = true`" and "`--transaction_write_policy`: 0=WriteCommitted, 1=WritePrepared, 2=WriteUnprepared"
- **Reality:** For `TransactionDB`, range deletions through `Write()` require `skip_concurrency_control`, and WritePrepared / WriteUnprepared additionally require `skip_duplicate_key_check`. `OptimisticTransactionDB` rejects range deletions even when they are hidden in a `WriteBatch`. Also, `db_bench_tool.cc` has no `transaction_write_policy` flag.
- **Source:** `include/rocksdb/utilities/transaction_db.h`, `TransactionDB::DeleteRange`; `utilities/transactions/write_prepared_txn_db.cc`, `WritePreparedTxnDB::Write`; `utilities/transactions/optimistic_transaction_db_impl.h`, `OptimisticTransactionDBImpl::DeleteRange`, `OptimisticTransactionDBImpl::Write`; `tools/db_bench_tool.cc`, transaction flag definitions and transaction DB open path; `utilities/transactions/transaction_test.cc`, `DeleteRangeSupportTest`; `utilities/transactions/optimistic_transaction_test.cc`, `DeleteRangeSupportTest`
- **Fix:** Replace the prose with a real support matrix by DB type and write policy, and only document flags that actually exist in `db_bench`.

## Completeness Gaps

### Open-time option rewrites and side effects are undocumented
- **Why it matters:** Transaction open paths mutate options and temporarily change compaction behavior. A maintainer debugging validation failures, WAL retention, or startup latency needs this before reading the internals.
- **Where to look:** `utilities/transactions/pessimistic_transaction_db.cc`, `TransactionDB::PrepareWrap`; `utilities/transactions/optimistic_transaction_db_impl.cc`, `OptimisticTransactionDB::Open`
- **Suggested scope:** Add a short open-path subsection in chapter 1 or chapter 8 covering `max_write_buffer_size_to_maintain = -1` when unset, temporary disabling of auto compaction during `TransactionDB::Open()`, and `allow_2pc = true`.

### The docs never present a real DeleteRange support matrix
- **Why it matters:** The behavior depends on DB type and write policy, and the tests already encode that matrix. Without it, users will either assume DeleteRange is impossible everywhere or will use the wrong bypass flags.
- **Where to look:** `include/rocksdb/utilities/transaction_db.h`, `TransactionDB::DeleteRange`; `utilities/transactions/write_prepared_txn_db.cc`, `WritePreparedTxnDB::Write`; `utilities/transactions/optimistic_transaction_db_impl.h`, `OptimisticTransactionDBImpl::Write`; `utilities/transactions/transaction_test.cc`, `DeleteRangeSupportTest`; `utilities/transactions/optimistic_transaction_test.cc`, `DeleteRangeSupportTest`
- **Suggested scope:** Add a table to chapter 10 or chapter 12.

### Public transaction APIs added since 2024 are still largely undocumented
- **Why it matters:** `PutEntity`, `GetEntity`, `MultiGetEntity`, `GetEntityForUpdate`, `GetCoalescingIterator`, and `GetAttributeGroupIterator` are now part of the surface area a reader expects from the transaction docs.
- **Where to look:** `include/rocksdb/utilities/transaction.h`; `utilities/transactions/transaction_base.cc`; `utilities/transactions/pessimistic_transaction.cc`; `utilities/transactions/optimistic_transaction_test.cc`; `utilities/transactions/transaction_test.cc`; recent history in `git log` for `utilities/transactions`
- **Suggested scope:** Expand chapter 10 with a public-API support matrix instead of only describing the classic `Get` / `Put` / `GetForUpdate` trio.

### WriteUnprepared iterator and savepoint caveats are missing
- **Why it matters:** These are not minor edge cases. Iterator lifetime changes flushing behavior, iterator creation can fail after unvalidated writes, and savepoints are split across flushed and unflushed batches.
- **Where to look:** `utilities/transactions/write_unprepared_txn.cc`, `WriteUnpreparedTxn::HandleWrite`, savepoint handling, iterator cleanup; `utilities/transactions/write_unprepared_txn_db.cc`, `WriteUnpreparedTxnDB::NewIterator`; `utilities/transactions/write_unprepared_transaction_test.cc`, `IterateAndWrite`, `IterateAfterClear`
- **Suggested scope:** Add detail to chapters 7 and 10.

### Option sanitization and lower bounds are mostly omitted
- **Why it matters:** The review prompt explicitly asked for option semantics precision, including behavior for zero and too-small values. The docs currently read as if the raw option value is always used as-is.
- **Where to look:** `utilities/transactions/pessimistic_transaction_db.cc`, `PessimisticTransactionDB::ValidateTxnDBOptions`; `utilities/transactions/optimistic_transaction_db_impl.h`, `OptimisticTransactionDBImpl`
- **Suggested scope:** Mention at least `num_stripes == 0` becoming `1` and `occ_lock_buckets` being clamped to a minimum of `16`.

### Error surfaces and subcodes need a single consolidated table
- **Why it matters:** Retry logic depends on distinguishing `Busy`, `Busy(kDeadlock)`, `TimedOut(kLockTimeout)`, `TryAgain`, `Expired`, `TxnNotPrepared`, and `LockLimit`.
- **Where to look:** `include/rocksdb/utilities/transaction.h`, `Transaction::Commit`; `utilities/transactions/transaction_util.cc`, `TransactionUtil::CheckKey`; `utilities/transactions/lock/point/point_lock_manager.cc`; `utilities/transactions/lock/range/range_tree/range_tree_lock_manager.cc`
- **Suggested scope:** Add a compact status-code section to chapters 3, 4, or 8, and cross-reference it from chapter 10.

### `SetSnapshotOnNextOperation()` has an important WriteCommitted-only commit trigger that is not mentioned
- **Why it matters:** The public API comment explicitly says the deferred snapshot can be created by `Commit()` for WriteCommitted. Readers relying on the docs will miss that cross-chapter behavior.
- **Where to look:** `include/rocksdb/utilities/transaction.h`, `Transaction::SetSnapshotOnNextOperation`; `utilities/transactions/transaction_base.cc`, `Transaction::CommitAndTryCreateSnapshot`
- **Suggested scope:** Add a note in chapter 8 and chapter 10.

## Depth Issues

### Lock acquisition needs separate narratives for the default point-lock manager and the per-key manager
- **Current:** Chapter 3 presents one seven-step flow and describes a `waiter_queue` as if it were the default implementation detail.
- **Missing:** The default manager waits on stripe condition variables, while the experimental manager keeps per-key waiters, upgrade priority, and delayed deadlock detection. Those are materially different concurrency stories.
- **Source:** `utilities/transactions/lock/point/point_lock_manager.cc`, `PointLockManager::AcquireWithTimeout`, `PerKeyPointLockManager::AcquireWithTimeout`

### Parallel optimistic validation lacks the actual ordering and bucket-selection mechanics
- **Current:** Chapter 8 says only that each key is locked in a consistent order.
- **Missing:** The code hashes keys with a per-DB / per-column-family seed, deduplicates bucket mutexes into a set, and acquires those mutexes in ascending order before validation and write. That is the reason the no-deadlock claim is credible.
- **Source:** `utilities/transactions/optimistic_transaction.cc`, `OptimisticTransaction::CommitWithParallelValidate`; `utilities/transactions/optimistic_transaction_db_impl.h`, `OptimisticTransactionDBImpl`

### The WriteUnprepared iterator section does not explain why `Prev()` needs extra validation state
- **Current:** Chapter 7 mentions incremental flushing and `unprep_seqs_`, but not the iterator-safety restriction.
- **Missing:** The `largest_validated_seq_ <= snapshot_seq` guard exists specifically because `Prev()` would otherwise stop at the wrong committed version when unprepared writes are interleaved with later commits.
- **Source:** `utilities/transactions/write_unprepared_txn_db.cc`, `WriteUnpreparedTxnDB::NewIterator`

## Structure and Style Violations

### Inline code quotes are used throughout the transaction docs despite the stated style rule
- **File:** repo-wide across `docs/components/transaction/*.md`
- **Details:** The prompt for these docs explicitly said "NO inline code quotes". Every chapter uses inline backticks heavily for API names, status codes, options, and type names. This is a systematic style miss, not an isolated example.

### `index.md` uses "Key Invariants" for a usage constraint rather than a correctness invariant
- **File:** `index.md`
- **Details:** "Transactions are NOT thread-safe; each must be accessed by a single thread at a time" is an important constraint, but it is not a corruption-preventing invariant in the same sense as `prepare_seq < commit_seq`. It should move to a constraints or caveats section.

## Undocumented Complexity

### Open-time compaction suppression during `TransactionDB::Open()`
- **What it is:** `TransactionDB::PrepareWrap()` temporarily disables auto compaction on column families before opening the DB, then re-enables it during initialization.
- **Why it matters:** This is a cross-component coordination point between transaction recovery and compaction. Without it, a reader will not understand why open-time behavior differs from plain `DB::Open()`.
- **Key source:** `utilities/transactions/pessimistic_transaction_db.cc`, `TransactionDB::PrepareWrap`, `PessimisticTransactionDB::Initialize`
- **Suggested placement:** Chapter 1 or chapter 5

### Range-lock deadlocks are translated into the same public deadlock reporting path
- **What it is:** The range-tree lock manager collects deadlock path elements through locktree callbacks, then exposes them through `GetDeadlockInfoBuffer()`.
- **Why it matters:** The current deadlock chapter reads like deadlock reporting is a point-lock-only facility. That is false, and it matters when debugging phantom-protection workloads.
- **Key source:** `utilities/transactions/lock/range/range_tree/range_tree_lock_manager.cc`, `RangeTreeLockManager::TryLock`, `RangeTreeLockManager::GetDeadlockInfoBuffer`
- **Suggested placement:** Chapter 4

### WritePrepared commit-time batches can temporarily create a more complex visibility story than the docs suggest
- **What it is:** When commit-time data is present, commit may write additional DB data and, with `two_write_queues`, may need a second write to publish the commit map update.
- **Why it matters:** This is why `use_only_the_last_commit_time_batch_for_recovery` exists and why the "WAL marker only" simplification is unsafe.
- **Key source:** `utilities/transactions/write_prepared_txn.cc`, `WritePreparedTxn::CommitInternal`
- **Suggested placement:** Chapter 2 and chapter 11

### WriteUnprepared iterators suppress automatic batch flushing while they are alive
- **What it is:** `WriteUnpreparedTxn::HandleWrite()` skips `MaybeFlushWriteBatchToDB()` whenever active iterators exist, and iterator cleanup is tied to transaction clear / rollback / commit.
- **Why it matters:** This is a real performance and memory behavior change that users can hit accidentally by mixing iteration and long transactions.
- **Key source:** `utilities/transactions/write_unprepared_txn.cc`, `WriteUnpreparedTxn::HandleWrite`, iterator cleanup logic; `utilities/transactions/write_unprepared_transaction_test.cc`, `IterateAndWrite`, `IterateAfterClear`
- **Suggested placement:** Chapter 7

### Secondary-index support is injected by a wrapper, not by generic transaction logic
- **What it is:** Secondary index maintenance comes from `SecondaryIndexMixin<WriteCommittedTxn>` in `BeginTransaction()`, not from a write-policy-agnostic transaction layer.
- **Why it matters:** A maintainer extending this feature needs to know immediately that it is a wrapper around a specific concrete transaction type.
- **Key source:** `utilities/transactions/pessimistic_transaction_db.cc`, `WriteCommittedTxnDB::BeginTransaction`
- **Suggested placement:** Chapter 11

### Timestamped snapshots depend on ordering guarantees outside the snapshot map itself
- **What it is:** The APIs rely on the caller maintaining monotonic ordering between sequence numbers and timestamps, and `CreateTimestampedSnapshot()` requires quiescent writes.
- **Why it matters:** Without that context, the API looks like a generic lookup table when it is really a consistency contract with the caller and the DB state.
- **Key source:** `include/rocksdb/utilities/transaction.h`, `Transaction::CommitAndTryCreateSnapshot`; `include/rocksdb/utilities/transaction_db.h`, `TransactionDB::CreateTimestampedSnapshot`
- **Suggested placement:** Chapter 11

## Positive Notes

- `index.md` follows the expected compressed pattern well: overview, key source files, chapter table, characteristics, and invariants, and its length is within the requested range.
- Every chapter has a `Files:` line, which makes code-audit follow-up substantially easier.
- Chapters 6 and 7 name the right internal mechanisms to explain WritePrepared and WriteUnprepared: `PreparedHeap`, `CommitCache`, `unprep_seqs_`, and savepoint handling are all the right topics to cover.

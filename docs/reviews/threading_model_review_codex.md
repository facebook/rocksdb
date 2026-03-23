# Review: threading_model — Codex

## Summary
Overall quality rating: significant issues

The documentation is well organized and covers the right major topics, but too many of the detailed claims are based on stale or simplified mental models rather than the current implementation. The highest-risk problems are in the lock-free read path, subcompaction execution model, shutdown flow, and the semantics of `two_write_queues` / `unordered_write`.

The biggest strength is structure: the index is readable, the chapter breakdown is sensible, and the write-thread chapters are directionally close to the code. The biggest concern is cross-component accuracy. Several sections describe DB-local behavior as if it owned the global Env thread pools, or describe scheduling limits as if they were actual thread counts. Option semantics are also under-specified in exactly the places where sanitization and mode interactions matter.

## Correctness Issues

### [WRONG] Lock-free read path does not validate cached SuperVersions with version numbers
- **File:** docs/components/threading_model/04_lock_free_read_path.md, "Read Acquisition Flow"
- **Claim:** "If the retrieved pointer is a valid SuperVersion (not a sentinel), compare its `version_number` against the global `super_version_number_`. If they match, use it directly -- zero atomic ref-count operations."
- **Reality:** `ColumnFamilyData::GetThreadLocalSuperVersion()` does not compare `version_number` with `super_version_number_`. It uses `ThreadLocalPtr::Swap(kSVInUse)` to take exclusive access to the TLS slot and relies on `ResetThreadLocalSuperVersions()` / `ThreadLocalPtr::Scrape()` to invalidate cached entries when a new SuperVersion is installed.
- **Source:** `db/column_family.cc` `ColumnFamilyData::GetThreadLocalSuperVersion`, `ColumnFamilyData::InstallSuperVersion`, `ColumnFamilyData::ResetThreadLocalSuperVersions`; `util/thread_local.cc` `ThreadLocalPtr::Swap`, `ThreadLocalPtr::Scrape`
- **Fix:** Describe invalidation-by-scrape, not version-number comparison, as the current fast-path freshness mechanism.

### [WRONG] The fast-path read description removes atomic operations that still exist
- **File:** docs/components/threading_model/04_lock_free_read_path.md, "Performance Benefit"
- **Claim:** "Under steady state (no flush/compaction completing), every read is a thread-local load + compare -- no mutex, no atomic increment/decrement."
- **Reality:** The hot path still performs an atomic exchange on acquisition and a compare-and-swap on return to TLS. Point lookups using `GetAndRefSuperVersion()` avoid ref-count churn, but the path is not a plain load+compare. Callers using `GetReferencedSuperVersion()` additionally do `Ref()` and may need `Unref()` if the cached entry was scraped while in use.
- **Source:** `db/column_family.cc` `ColumnFamilyData::GetThreadLocalSuperVersion`, `ColumnFamilyData::GetReferencedSuperVersion`, `ColumnFamilyData::ReturnThreadLocalSuperVersion`; `util/thread_local.cc` `ThreadLocalPtr::Swap`, `ThreadLocalPtr::CompareAndSwap`
- **Fix:** Say the hot path avoids the DB mutex and usually avoids SuperVersion ref-count contention, but still uses atomic TLS operations.

### [MISLEADING] Group commit is documented as amortizing MANIFEST updates
- **File:** docs/components/threading_model/02_write_thread_group_commit.md, "Overview"
- **Claim:** "WriteThread implements a leader/follower pattern to batch concurrent writes into groups, amortizing the cost of WAL fsync and MANIFEST updates across multiple operations."
- **Reality:** The core group-commit path amortizes WAL write/fsync and memtable coordination. MANIFEST writes are not a normal per-group-commit cost and only show up in specific WAL lifecycle paths such as `ApplyWALToManifest()` after a WAL sync boundary.
- **Source:** `db/db_impl/db_impl_write.cc` `DBImpl::WriteImpl`, `DBImpl::PipelinedWriteImpl`
- **Fix:** Focus this overview on WAL batching and memtable coordination; mention MANIFEST interaction only as a separate WAL-management detail if you keep it at all.

### [MISLEADING] `two_write_queues` is treated as a mode that trades consistency guarantees
- **File:** docs/components/threading_model/03_write_modes.md, "Overview"; docs/components/threading_model/index.md, chapter 3 summary
- **Claim:** "RocksDB offers three optional write modes that trade consistency guarantees for higher write throughput."
- **Reality:** `unordered_write` relaxes snapshot/atomic-read guarantees, but `two_write_queues` does not. It routes `disable_memtable` writes to `nonmem_write_thread_` so commit-only work does not wait behind memtable insertions. The documented use case is optimization, not weaker visibility semantics.
- **Source:** `include/rocksdb/options.h` `DBOptions::two_write_queues`; `db/db_impl/db_impl_write.cc` `DBImpl::WriteImpl`, `DBImpl::WriteImplWALOnly`
- **Fix:** Separate "throughput-oriented write modes" from "consistency-relaxing modes", and call out that only `unordered_write` weakens the default read/snapshot contract.

### [MISLEADING] `skip_concurrency_control` is documented as part of the correctness recipe for `unordered_write`
- **File:** docs/components/threading_model/03_write_modes.md, "Restoring Full Guarantees"
- **Claim:** "To get unordered_write throughput with full snapshot immutability, use WritePrepared transactions with `two_write_queues`: set `DBOptions::unordered_write = true` and `DBOptions::two_write_queues = true`, then open via `TransactionDB::Open()` with `TxnDBWritePolicy::WRITE_PREPARED` and `TransactionDBOptions::skip_concurrency_control = true`"
- **Reality:** `WRITE_PREPARED` plus `unordered_write` does require `two_write_queues`, but `skip_concurrency_control` is optional. The option is documented as an optimization for cases where TransactionDB is being used for write ordering rather than concurrency control; it is not required for immutable snapshots.
- **Source:** `utilities/transactions/pessimistic_transaction_db.cc` `TransactionDB::Open`; `include/rocksdb/utilities/transaction_db.h` `TransactionDBOptions::skip_concurrency_control`
- **Fix:** Document `skip_concurrency_control` as an optional optimization, not as a required part of the semantic restoration recipe.

### [WRONG] Pipelined-write incompatibility list is incomplete
- **File:** docs/components/threading_model/03_write_modes.md, "Incompatibilities"
- **Claim:** "Pipelined write is NOT compatible with: `two_write_queues`, `seq_per_batch`, `unordered_write`, `post_memtable_callback`"
- **Reality:** `ValidateOptions()` also rejects `atomic_flush` with `enable_pipelined_write`.
- **Source:** `db/db_impl/db_impl_open.cc` `DBImpl::ValidateOptions`; `db/db_impl/db_impl_write.cc` `DBImpl::WriteImpl`
- **Fix:** Add `atomic_flush` to the incompatibility list and distinguish open-time option validation from write-time request validation.

### [WRONG] Concurrent memtable write support is outdated
- **File:** docs/components/threading_model/07_concurrent_memtable_writes.md, "Configuration"
- **Claim:** "Currently only `SkipListFactory` (the default) supports this via lock-free concurrent insert."
- **Reality:** `VectorRepFactory` also reports concurrent-insert support. There is dedicated test coverage for concurrent writes with vector memtables.
- **Source:** `include/rocksdb/memtablerep.h` `SkipListFactory::IsInsertConcurrentlySupported`, `VectorRepFactory::IsInsertConcurrentlySupported`; `db/db_memtable_test.cc` `DBMemTableTest.VectorConcurrentInsert`
- **Fix:** Update the supported-memtable list to include `VectorRepFactory`, and avoid describing concurrent insert as skiplist-only.

### [STALE] `filter_deletes` is documented as a live compatibility constraint
- **File:** docs/components/threading_model/07_concurrent_memtable_writes.md, "Configuration"
- **Claim:** "Not compatible with `inplace_update_support` or `filter_deletes`"
- **Reality:** `inplace_update_support` is actively validated, but `filter_deletes` is only carried as a deprecated option alias in option metadata and is no longer a live `ColumnFamilyOptions` field developers configure in current code.
- **Source:** `db/column_family.cc` `CheckConcurrentWritesSupported`; `options/cf_options.cc` deprecated option entry for `filter_deletes`
- **Fix:** Keep the active `inplace_update_support` restriction, and either remove `filter_deletes` or clearly mark it as deprecated historical baggage.

### [WRONG] Subcompactions are not scheduled as LOW-priority thread-pool work items
- **File:** docs/components/threading_model/08_subcompactions.md, "Overview" and "Parallel Execution"
- **Claim:** "Subcompactions divide a single compaction job into multiple key-range sub-jobs that execute in parallel on separate thread pool workers." / "Sub-jobs are submitted to the compaction thread pool (LOW priority)"
- **Reality:** The LOW/BOTTOM pool schedules the parent compaction job. `CompactionJob::RunSubcompactions()` then spawns additional `port::Thread`s directly inside that compaction job. Pool capacity is only accounted for separately through `Env::ReserveThreads()` / `ReleaseThreads()`.
- **Source:** `db/compaction/compaction_job.cc` `CompactionJob::RunSubcompactions`, `CompactionJob::AcquireSubcompactionResources`, `CompactionJob::ReleaseSubcompactionResources`
- **Fix:** Describe subcompactions as threads created within a running compaction job, with pool-resource accounting layered around them, not as standalone queue items in the LOW pool.

### [WRONG] `max_subcompactions` is documented as a hard upper bound
- **File:** docs/components/threading_model/08_subcompactions.md, "Configuration"
- **Claim:** "Setting this to N allows each compaction job to be split into up to N parallel sub-ranges."
- **Reality:** This is not a hard cap in all cases. For leveled compaction with `CompactionPri::kRoundRobin`, RocksDB can reserve extra background-thread capacity and plan more subcompactions than the configured `max_subcompactions`. Conversely, some compactions will not split at all even when the option is > 1.
- **Source:** `db/compaction/compaction_job.cc` `CompactionJob::GetSubcompactionsLimit`, `CompactionJob::AcquireSubcompactionResources`, `CompactionJob::GenSubcompactionBoundaries`; `db/db_compaction_test.cc` `RoundRobinSubcompactionsAgainstResources.DISABLED_SubcompactionsUsingResources`
- **Fix:** Describe `max_subcompactions` as the baseline limit, not an unconditional hard cap, and call out the round-robin exception plus the cases where partitioning collapses back to one subcompaction.

### [WRONG] The subcompaction thread bound is stated incorrectly
- **File:** docs/components/threading_model/08_subcompactions.md, "Interaction with Thread Pools"
- **Claim:** "The total number of concurrent compaction threads (including sub-jobs from all compactions) is bounded by `max_background_compactions` (or the compaction share of `max_background_jobs`)."
- **Reality:** Those settings bound compaction-job scheduling, not the total number of OS threads doing subcompaction work. A scheduled compaction job can create additional subcompaction threads up to its computed subcompaction limit, and round-robin compactions can reserve even more background capacity.
- **Source:** `db/compaction/compaction_job.cc` `CompactionJob::RunSubcompactions`, `CompactionJob::GetSubcompactionsLimit`, `CompactionJob::AcquireSubcompactionResources`
- **Fix:** Distinguish compaction-job scheduling limits from per-job subcompaction fan-out.

### [WRONG] The shutdown chapter says `Close()` calls `CancelAllBackgroundWork(wait=true)`
- **File:** docs/components/threading_model/09_shutdown_and_cleanup.md, "DB::Close"
- **Claim:** "Step 1 -- `CancelAllBackgroundWork(wait=true)` to stop all background threads"
- **Reality:** `CloseHelper()` first coordinates with background error recovery, then calls `CancelAllBackgroundWork(false)`, unschedules this DB's queued tasks, and waits for a broader set of conditions than `CancelAllBackgroundWork(true)` covers.
- **Source:** `db/db_impl/db_impl.cc` `DBImpl::CloseHelper`, `DBImpl::CancelAllBackgroundWork`, `DBImpl::WaitForBackgroundWork`
- **Fix:** Rewrite the `Close()` sequence to match `CloseHelper()` rather than the simpler `CancelAllBackgroundWork(true)` helper path.

### [WRONG] The shutdown table conflates memtable flush on close with WAL fsync
- **File:** docs/components/threading_model/09_shutdown_and_cleanup.md, "Graceful vs Immediate Shutdown"
- **Claim:** "`Close()` / Destructor | ... | WAL Flush | Yes (unless `avoid_flush_during_shutdown`)"
- **Reality:** `DB::Close()` does not fsync WAL files. The public API comment explicitly says the caller must use `SyncWAL()` or a sync write first if WAL syncing is required. The `avoid_flush_during_shutdown` option only controls whether RocksDB flushes unpersisted WAL-disabled data from memtables during shutdown.
- **Source:** `include/rocksdb/db.h` `DB::Close`; `db/db_impl/db_impl.cc` `DBImpl::CancelAllBackgroundWork`
- **Fix:** Separate "WAL sync" from "flush WAL-disabled memtable contents" and correct the table accordingly.

### [MISLEADING] `WaitForCompact()` is reduced to "scheduled flush and compaction work"
- **File:** docs/components/threading_model/09_shutdown_and_cleanup.md, "WaitForCompact"
- **Claim:** "`DB::WaitForCompact(const WaitForCompactOptions&)` blocks until all scheduled background work (flush and compaction) completes."
- **Reality:** The implementation waits on scheduled and unscheduled flush/compaction work, optionally waits for purge, can flush first, can abort when background work is paused, can time out, and can close the DB while setting `reject_new_background_jobs_`.
- **Source:** `db/db_impl/db_impl_compaction_flush.cc` `DBImpl::WaitForCompact`; `include/rocksdb/options.h` `WaitForCompactOptions`
- **Fix:** Describe the actual option-controlled behaviors and the fact that queued-unscheduled work is part of the wait condition.

### [MISLEADING] DB shutdown is described as if it terminates Env thread pools
- **File:** docs/components/threading_model/09_shutdown_and_cleanup.md, "Overview" and "Thread Pool Shutdown"
- **Claim:** "RocksDB's shutdown sequence coordinates the termination of background threads..." together with the section describing `ThreadPoolImpl::Impl::JoinThreads(bool wait_for_jobs_to_complete)`
- **Reality:** `DBImpl::Close()` does not join Env thread pools. It cancels or unschedules this DB's work and waits for counters to drain. The underlying HIGH/LOW/BOTTOM pools are Env-global and keep running after a DB is closed.
- **Source:** `db/db_impl/db_impl.cc` `DBImpl::CloseHelper`; `util/threadpool_imp.cc` `ThreadPoolImpl::JoinThreads`
- **Fix:** Present `ThreadPoolImpl::JoinThreads()` as thread-pool or Env lifecycle behavior, not as a normal step of DB shutdown.

### [MISLEADING] The shutdown-flag memory-order story is oversimplified
- **File:** docs/components/threading_model/09_shutdown_and_cleanup.md, "Shutdown Flag Protocol"; docs/components/threading_model/10_thread_safety_reference.md, "Memory Ordering"
- **Claim:** "`shutting_down_` is stored with `memory_order_release` and loaded with `memory_order_acquire` to ensure proper visibility across threads." / "Publish-subscribe | `acquire` / `release` | SuperVersion installation, `shutting_down_`, writer state transitions"
- **Reality:** The code mixes acquire, release, relaxed, and default-ordered atomics depending on the call site. For example, `CancelAllBackgroundWork()` stores `shutting_down_` with release, `MaybeScheduleFlushOrCompaction()` loads it with acquire, but `DelayWrite()` and compaction-iterator shutdown checks use relaxed loads. The `SuperVersion` fast path and writer-state transitions also do not fit the table's simplified acquire/release summary.
- **Source:** `db/db_impl/db_impl.cc` `DBImpl::CancelAllBackgroundWork`; `db/db_impl/db_impl_compaction_flush.cc` `DBImpl::MaybeScheduleFlushOrCompaction`; `db/db_impl/db_impl_write.cc` `DBImpl::DelayWrite`; `db/compaction/compaction_iterator.h` `CompactionIterator::IsShuttingDown`; `db/column_family.cc` `SuperVersion::Ref`, `SuperVersion::Unref`; `util/thread_local.cc` `ThreadLocalPtr::Swap`, `ThreadLocalPtr::CompareAndSwap`
- **Fix:** Either document the specific operations and their actual orderings, or avoid a memory-ordering table that implies uniform acquire/release rules where the code does not use them.

### [MISLEADING] The mutex chapter misstates how `options_mutex_` interacts with `mutex_`
- **File:** docs/components/threading_model/05_mutex_hierarchy.md, "Options Mutex (`options_mutex_`)"
- **Claim:** "Acquired independently of `mutex_` to avoid blocking reads during option changes."
- **Reality:** When both are needed, the current code explicitly requires `options_mutex_` to be acquired before `mutex_`, and the main option-changing and CF-metadata APIs do take both. The benefit is that some slow option-file and periodic-task work can happen outside the DB mutex, not that `options_mutex_` is an independent replacement for it.
- **Source:** `db/db_impl/db_impl.h` comment above `options_mutex_`; `db/db_impl/db_impl.cc` `DBImpl::SetOptions`, `DBImpl::SetDBOptions`, `DBImpl::CreateColumnFamily`, `DBImpl::DropColumnFamily`
- **Fix:** Explain the actual nesting and ordering rule, and what work is intentionally moved out from under `mutex_`.

### [MISLEADING] The BOTTOM pool is described too narrowly
- **File:** docs/components/threading_model/01_thread_pools.md, "Priority Levels"
- **Claim:** "`BOTTOM` | Bottom compaction | Bottommost-level compaction (lower I/O priority)"
- **Reality:** Automatic compactions can also be forwarded from LOW to BOTTOM when they output to the last level or nonzero max output level and BOTTOM threads are configured, not only when the compaction is literally "bottommost" in the simplistic sense described here.
- **Source:** `db/db_impl/db_impl_compaction_flush.cc` `DBImpl::BackgroundCompaction`, `DBImpl::RunManualCompaction`
- **Fix:** Describe BOTTOM as the pool for last-level-oriented compactions when configured, with bottommost manual compactions as one common case.

### [MISLEADING] `max_background_jobs` is documented as if it directly sets thread counts
- **File:** docs/components/threading_model/01_thread_pools.md, "max_background_jobs (Recommended)"
- **Claim:** "Set `DBOptions::max_background_jobs` ... to control total background parallelism. RocksDB internally splits this budget: Flush threads = ... Compaction threads = ..."
- **Reality:** That split comes from `DBImpl::GetBGJobLimits()` and is primarily a scheduling-limit calculation. Actual Env thread pool sizes are separate and are only ensured to be at least those values during open or when limits increase via `SetDBOptions()`. Decreasing the option reduces scheduling limits but does not shrink the Env pools.
- **Source:** `db/db_impl/db_impl_compaction_flush.cc` `DBImpl::GetBGJobLimits`; `db/db_impl/db_impl_open.cc` initialization of HIGH and LOW background threads; `db/db_impl/db_impl.cc` `DBImpl::SetDBOptions`
- **Fix:** Distinguish "job scheduling limits" from "Env thread pool sizes" and note the asymmetry between increasing and decreasing the option.

### [WRONG] The thread-safety reference uses the wrong snapshot API name
- **File:** docs/components/threading_model/10_thread_safety_reference.md, "Thread-Safe Operations"
- **Claim:** "`CreateSnapshot()` / `ReleaseSnapshot()`"
- **Reality:** The public API is `GetSnapshot()` / `ReleaseSnapshot()`.
- **Source:** `include/rocksdb/db.h` `DB::GetSnapshot`, `DB::ReleaseSnapshot`
- **Fix:** Use the actual API names so readers can map the reference to the current public headers.

### [WRONG] The thread-safety chapter's refcount and memory-order description is not what the code does
- **File:** docs/components/threading_model/10_thread_safety_reference.md, "Memory Ordering"
- **Claim:** "Reference counting | `acquire` on load, `release` on store | SuperVersion refs"
- **Reality:** `SuperVersion::Ref()` is a relaxed `fetch_add`, `SuperVersion::Unref()` is a plain `fetch_sub` with default ordering, and the TLS handoff around SuperVersion uses `ThreadLocalPtr` exchange and CAS operations with their own acquire, release, and relaxed choices. The table is not a faithful summary of the current implementation.
- **Source:** `db/column_family.cc` `SuperVersion::Ref`, `SuperVersion::Unref`; `util/thread_local.cc` `ThreadLocalPtr::Swap`, `ThreadLocalPtr::CompareAndSwap`
- **Fix:** Rewrite the table with exact operations or drop this row.

## Completeness Gaps

### `WaitForCompactOptions` behavior is barely documented
- **Why it matters:** Shutdown orchestration, automation, and test code depend on `abort_on_pause`, `wait_for_purge`, `close_db`, `flush`, and `timeout`; these materially change behavior and failure modes.
- **Where to look:** `include/rocksdb/options.h` `WaitForCompactOptions`; `db/db_impl/db_impl_compaction_flush.cc` `DBImpl::WaitForCompact`; `db/deletefile_test.cc` `DeleteFileTest.WaitForCompactWithWaitForPurgeOptionTest`; `utilities/transactions/transaction_test.cc` `TransactionTest.WaitForCompactAbortOnPause`
- **Suggested scope:** expand chapter 9 rather than creating a new chapter

### Shutdown uses three distinct guards, not just `shutting_down_`
- **Why it matters:** The real close path relies on `shutdown_initiated_`, `shutting_down_`, and `reject_new_background_jobs_` for different phases; without that separation it is hard to reason about races around error recovery, waiting, and `WaitForCompact(close_db=true)`.
- **Where to look:** `db/db_impl/db_impl.h` `shutdown_initiated_`, `shutting_down_`, `reject_new_background_jobs_`; `db/db_impl/db_impl.cc` `DBImpl::CloseHelper`; `db/db_impl/db_impl_compaction_flush.cc` `DBImpl::WaitForCompact`, `DBImpl::EnqueuePendingFlush`, `DBImpl::EnqueuePendingCompaction`, `DBImpl::SchedulePendingPurge`
- **Suggested scope:** add to chapter 9

### Object-level thread-safety contracts are missing
- **Why it matters:** Developers frequently confuse DB-level thread safety with object-level rules. Iterators, WriteBatch, and Snapshot each have explicit contracts in public headers, and the current chapter does not cover them.
- **Where to look:** `include/rocksdb/iterator.h` `Iterator`; `include/rocksdb/write_batch.h` `WriteBatch`; `include/rocksdb/snapshot.h` `Snapshot`
- **Suggested scope:** expand chapter 10

### The docs miss the background-job-pressure listener surface
- **Why it matters:** There is now a public listener callback that exposes scheduling pressure and write-stall proximity. That is one of the main integration points between the threading model and observability.
- **Where to look:** `include/rocksdb/listener.h` `BackgroundJobPressure`, `EventListener::OnBackgroundJobPressureChanged`; `db/db_impl/db_impl_compaction_flush.cc` `DBImpl::CaptureBackgroundJobPressure`, `DBImpl::NotifyOnBackgroundJobPressureChanged`; `db/listener_test.cc` `EventListenerTest.BackgroundJobPressure`
- **Suggested scope:** brief mention in chapter 1 or 6

### `LockWAL()` and `UnlockWAL()` reuse the stall machinery but are undocumented here
- **Why it matters:** These APIs intentionally freeze write progress, interact with the stop-token path, and have explicit synchronization guarantees that matter when debugging write stalls or two-write-queue behavior.
- **Where to look:** `include/rocksdb/db.h` `DB::LockWAL`, `DB::UnlockWAL`; `db/db_impl/db_impl_write.cc`; `utilities/transactions/transaction_test.cc` `TransactionTest.StallTwoWriteQueues`, `TransactionTest.UnlockWALStallCleared`
- **Suggested scope:** brief mention in chapter 6 or 9

## Depth Issues

### Write-stall option semantics need sanitization and disable-path details
- **Current:** chapter 6 lists default thresholds and basic trigger conditions.
- **Missing:** FIFO compaction sanitizes L0 triggers to `INT_MAX`; `disable_auto_compactions` suppresses L0 and pending-compaction-based stalls; `soft_pending_compaction_bytes_limit = 0` sanitizes to the hard limit; `hard_pending_compaction_bytes_limit = 0` disables byte-based stop stalls; `max_write_buffer_number` is sanitized to at least 2.
- **Source:** `db/column_family.cc` option-sanitization block; `db/column_family.cc` `ColumnFamilyData::GetWriteStallConditionAndCause`

### The read-path chapter should distinguish cached vs extra-ref SuperVersion acquisition
- **Current:** chapter 4 presents a single acquisition story.
- **Missing:** some callers only need `GetThreadLocalSuperVersion()`, while others use `GetReferencedSuperVersion()` to hold an extra reference across longer-lived operations like iterator refresh or pinned results. That difference is central to understanding why some readers can tolerate a scraped TLS slot and others cannot.
- **Source:** `db/column_family.cc` `ColumnFamilyData::GetThreadLocalSuperVersion`, `ColumnFamilyData::GetReferencedSuperVersion`, `ColumnFamilyData::ReturnThreadLocalSuperVersion`; `db/db_impl/db_impl.h` comments around `MultiCFSnapshot`

### The subcompaction chapter skips the resource-accounting model that actually constrains fan-out
- **Current:** chapter 8 presents subcompactions as a straightforward N-way split.
- **Missing:** round-robin compactions can reserve extra LOW and BOTTOM pool capacity, actual partition count can shrink after anchor analysis, and the parent compaction job is the thing counted in scheduler state while subthreads are created locally.
- **Source:** `db/compaction/compaction_job.cc` `CompactionJob::GetSubcompactionsLimit`, `CompactionJob::AcquireSubcompactionResources`, `CompactionJob::ShrinkSubcompactionResources`, `CompactionJob::RunSubcompactions`

## Structure and Style Violations

### Inline code spans are used pervasively despite the house rule against them
- **File:** all files under `docs/components/threading_model/`
- **Details:** the generated docs heavily use backtick-quoted API names, fields, paths, and keywords. The requested format forbids inline code quotes and wants plain references to headers, structs, and functions instead.

### `10_thread_safety_reference.md` has an incomplete **Files:** line
- **File:** docs/components/threading_model/10_thread_safety_reference.md
- **Details:** the chapter discusses Iterator, WriteBatch, Snapshot, thread-local storage, and thread-pool queue length, but its **Files:** line omits `include/rocksdb/iterator.h`, `include/rocksdb/write_batch.h`, `include/rocksdb/snapshot.h`, `util/thread_local.h`, and `util/threadpool_imp.cc`.

### `index.md` labels non-correctness properties as invariants
- **File:** docs/components/threading_model/index.md
- **Details:** items like "Background jobs release `mutex_` during I/O and reacquire for MANIFEST updates" and "Write stalls are per-column-family triggers but apply to the entire DB" are important design properties, but not correctness invariants in the sense requested by the style guide.

## Undocumented Complexity

### `WaitForCompact(close_db=true)` uses a separate "reject new jobs" phase
- **What it is:** After waiting for outstanding work, `WaitForCompact()` sets `reject_new_background_jobs_` before calling `Close()` so flush, compaction, and purge work do not get re-enqueued in the handoff window.
- **Why it matters:** Without this detail, the `close_db` option looks like "wait then call Close" when it is really "wait, reject requeue, then close." That distinction matters for race analysis and for anyone modifying queueing code.
- **Key source:** `db/db_impl/db_impl_compaction_flush.cc` `DBImpl::WaitForCompact`, `DBImpl::EnqueuePendingFlush`, `DBImpl::EnqueuePendingCompaction`, `DBImpl::SchedulePendingPurge`
- **Suggested placement:** chapter 9

### Round-robin subcompactions can borrow extra resources beyond configured `max_subcompactions`
- **What it is:** Round-robin leveled compactions compute planned subcompactions from input-file count, reserve extra LOW and BOTTOM capacity if possible, and then shrink reservations back down if anchor partitioning produces fewer actual ranges.
- **Why it matters:** It explains why scheduler counters and actual subcompaction fan-out do not line up with the user-visible option alone.
- **Key source:** `db/compaction/compaction_job.cc` `CompactionJob::GenSubcompactionBoundaries`, `CompactionJob::AcquireSubcompactionResources`, `CompactionJob::ShrinkSubcompactionResources`
- **Suggested placement:** chapter 8

### Background-job pressure now has a public observability hook
- **What it is:** RocksDB computes a `BackgroundJobPressure` snapshot combining scheduled and running LOW+BOTTOM compactions, flush pressure, compaction-speedup state, and write-stall proximity, then emits it to listeners.
- **Why it matters:** This is the primary supported way for external systems to observe the runtime consequences of the threading model.
- **Key source:** `include/rocksdb/listener.h` `BackgroundJobPressure`, `EventListener::OnBackgroundJobPressureChanged`; `db/db_impl/db_impl_compaction_flush.cc` `DBImpl::CaptureBackgroundJobPressure`
- **Suggested placement:** chapter 1 or 6

### `LockWAL()` piggybacks on the same write-stop machinery as stalls
- **What it is:** WAL locking acquires a stop token, blocks writes, and has dedicated logic to ensure `UnlockWAL()` does not return before its induced stall has actually cleared from the write queues.
- **Why it matters:** This is a real concurrency mechanism with dedicated regression tests, and it is easy to mis-diagnose as a generic write stall if the docs do not mention it.
- **Key source:** `db/db_impl/db_impl_write.cc`; `utilities/transactions/transaction_test.cc` `TransactionTest.StallTwoWriteQueues`, `TransactionTest.UnlockWALStallCleared`
- **Suggested placement:** chapter 6

### Subcompaction progress persistence and resumption exist now
- **What it is:** Compactions can persist per-subcompaction progress and resume from it, with explicit limitations around supported layouts.
- **Why it matters:** Anyone debugging long-running compactions or remote-compaction behavior needs to know that subcompaction state is no longer purely ephemeral thread-local work.
- **Key source:** `db/compaction/compaction_job.h` `CompactionJob::Prepare`; `db/compaction/compaction_job.cc` progress and resume helpers; recent history including "Resuming and persisting subcompaction progress in CompactionJob"
- **Suggested placement:** chapter 8

## Positive Notes

- The overall chapter layout is good: the index is within the requested size range, the chapter table is easy to navigate, and every chapter has a **Files:** line.
- The write-thread state-machine coverage is one of the stronger parts. The batching asymmetry for sync writers, the three-phase wait strategy, and the parallel-memtable caller and worker split are all close to the current implementation.
- The docs do a good job of connecting write stalls back to per-column-family triggers with whole-DB impact, which is one of the easiest places for readers to build the wrong mental model.

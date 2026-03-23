# Review: db_impl — Codex

## Summary
Overall quality rating: needs work

The db_impl docs are well organized and easy to navigate. The 10-chapter split is sensible, the index is within the target size at 41 lines, and the chapter coverage is broad enough that a maintainer can usually find the right section quickly.

The main problem is factual precision. Several chapters misstate option defaults, close semantics, read-path concurrency mechanisms, scheduler limits, and mode-specific behavior for secondary and compacted DBs. The most concerning errors are in areas where readers rely on the docs to reason about correctness boundaries: snapshots, SuperVersion lifetime, background error callbacks, atomic flush, and shutdown behavior.

## Correctness Issues

### [WRONG] WAL recovery default is misdocumented
- **File:** `02_db_open.md`, section "WAL Recovery Modes"
- **Claim:** "`kTolerateCorruptedTailRecords` | Tolerate corruption/truncation at the tail of the last WAL. Default behavior"
- **Reality:** The default is `WALRecoveryMode::kPointInTimeRecovery`, not `kTolerateCorruptedTailRecords`.
- **Source:** `include/rocksdb/options.h` `WALRecoveryMode`; `DBOptions::wal_recovery_mode`
- **Fix:** Mark `kPointInTimeRecovery` as the default and describe `kTolerateCorruptedTailRecords` as a non-default recovery mode.

### [WRONG] Snapshot close check is described as wrapper-only behavior
- **File:** `03_db_close.md`, section "Snapshot Safety Check"
- **Claim:** "Before shutdown, `CloseHelper()` does not check for unreleased user snapshots. However, `DB::Close()` (the public API wrapper, not shown in `CloseHelper()`) may call `MaybeReleaseTimestampedSnapshotsAndCheck()` ..."
- **Reality:** `DBImpl::Close()` itself calls `MaybeReleaseTimestampedSnapshotsAndCheck()` before `CloseImpl()`, and `DBImpl::~DBImpl()` does the same before invoking `CloseImpl()`. This is not an external wrapper around `CloseHelper()`.
- **Source:** `db/db_impl/db_impl.cc` `DBImpl::Close`; `DBImpl::~DBImpl`; `DBImpl::MaybeReleaseTimestampedSnapshotsAndCheck`
- **Fix:** Describe the snapshot safety check as part of the `DBImpl::Close()` and destructor entry path, not as wrapper-only behavior.

### [WRONG] Unreleased snapshots are not converted to Incomplete on the Close fast-fail path
- **File:** `03_db_close.md`, sections "Snapshot Safety Check" and "Error Handling During Close"
- **Claim:** "This is wrapped as `Status::Incomplete` before returning to the caller." and "If the return status is `Status::Aborted` (from unreleased snapshots), it is converted to `Status::Incomplete` ..."
- **Reality:** If regular snapshots remain, `DBImpl::Close()` returns `Status::Aborted` before `CloseHelper()` runs. The `Status::Aborted` to `Status::Incomplete` conversion at the end of `CloseHelper()` applies only to errors produced during the close sequence itself.
- **Source:** `db/db_impl/db_impl.cc` `DBImpl::Close`; `DBImpl::CloseHelper`
- **Fix:** Separate these cases: unreleased snapshots return `Status::Aborted` from `DBImpl::Close()`, while `CloseHelper()` may wrap its own `Aborted` result as `Incomplete`.

### [WRONG] SuperVersion thread-local algorithm is not a version-number compare
- **File:** `05_version_management.md`, section "Thread-Local Caching"; `07_read_path.md`, section "Get Flow"
- **Claim:** "If the swapped value is a valid SuperVersion pointer, check if its `version_number` matches the current `super_version_number_`" and "If the cached pointer is stale (version has changed), it falls back to `cfd->GetReferencedSuperVersion(this)` ..."
- **Reality:** The fast path swaps the thread-local entry to `kSVInUse`. If it sees `kSVObsolete`, it acquires `super_version_->Ref()` under the DB mutex. There is no `version_number` comparison on the read fast path; staleness is propagated by `ResetThreadLocalSuperVersions()` scraping thread-local entries to `kSVObsolete`. Also `DBImpl::GetAndRefSuperVersion()` simply forwards to `cfd->GetThreadLocalSuperVersion(this)`.
- **Source:** `db/column_family.cc` `ColumnFamilyData::GetThreadLocalSuperVersion`; `ColumnFamilyData::ReturnThreadLocalSuperVersion`; `ColumnFamilyData::ResetThreadLocalSuperVersions`; `db/db_impl/db_impl.cc` `DBImpl::GetAndRefSuperVersion`
- **Fix:** Rewrite this around the actual sentinel-swap protocol and explain that a fresh ref is taken only on the obsolete slow path.

### [WRONG] The read path does not use hazard pointers
- **File:** `07_read_path.md`, section "Overview"
- **Claim:** "The read path is designed to be lock-free on the common path, using reference counting and hazard pointers to avoid holding `mutex_` during data access."
- **Reality:** The DBImpl read path uses refcounted `SuperVersion`s plus thread-local caching. I did not find hazard-pointer machinery in this code path.
- **Source:** `db/db_impl/db_impl.cc` `DBImpl::GetAndRefSuperVersion`; `DBImpl::ReturnAndCleanupSuperVersion`; `db/column_family.cc` `ColumnFamilyData::GetThreadLocalSuperVersion`
- **Fix:** Replace "hazard pointers" with the actual mechanism: thread-local `SuperVersion` caching plus refcounted cleanup.

### [WRONG] GetSnapshot uses the last published sequence, not LastSequence
- **File:** `07_read_path.md`, section "Snapshot Management"
- **Claim:** "`DBImpl::GetSnapshot()` acquires `mutex_`, gets the current `LastSequence()`, and inserts a new `SnapshotImpl` ..."
- **Reality:** `GetSnapshotImpl()` uses `GetLastPublishedSequence()`.
- **Source:** `db/db_impl/db_impl.cc` `DBImpl::GetSnapshotImpl`
- **Fix:** Change the snapshot description to `GetLastPublishedSequence()` and explain that unpublished sequence numbers are excluded.

### [WRONG] Listener auto-recovery control is attributed to the wrong callback
- **File:** `09_background_error_handling.md`, section "Listener Integration"
- **Claim:** "`OnBackgroundError()`: Called when a new background error is detected. The listener can modify the error status and disable auto-recovery by setting `*auto_recovery = false`."
- **Reality:** `OnBackgroundError()` only receives `Status*`. Auto-recovery suppression happens through `OnErrorRecoveryBegin(..., bool* auto_recovery)`, which `EventHelpers::NotifyOnBackgroundError()` invokes after `OnBackgroundError()` when auto-recovery is still enabled.
- **Source:** `include/rocksdb/listener.h` `EventListener::OnBackgroundError`; `EventListener::OnErrorRecoveryBegin`; `db/event_helpers.cc` `EventHelpers::NotifyOnBackgroundError`
- **Fix:** Split the semantics: `OnBackgroundError()` may edit the status; `OnErrorRecoveryBegin()` may disable automatic recovery.

### [WRONG] Dropped column families are removed from the live set at drop time, not destructor time
- **File:** `04_column_families.md`, section "Dropping"
- **Claim:** "When the reference count reaches zero, the `ColumnFamilyData` destructor removes it from the `ColumnFamilySet` linked list and frees associated memory and files."
- **Reality:** `ColumnFamilyData::SetDropped()` removes the CF from `ColumnFamilySet` immediately when the drop edit is applied. The destructor later removes it from the linked list and only calls `RemoveColumnFamily()` if it was not already dropped.
- **Source:** `db/column_family.cc` `ColumnFamilyData::SetDropped`; `ColumnFamilyData::~ColumnFamilyData`
- **Fix:** Distinguish between logical removal from the live set during drop and physical destruction when the last reference disappears.

### [WRONG] Atomic flush does not force every column family into each flush
- **File:** `04_column_families.md`, section "Atomic Flush"
- **Claim:** "When `atomic_flush` is enabled ..., all column families are flushed together atomically."
- **Reality:** `SelectColumnFamiliesForAtomicFlush()` selects only CFs with unflushed immutable memtables, non-empty mutable memtables, non-empty cached recoverable state, or recovery-triggered flushes. Idle CFs are not included in every atomic flush.
- **Source:** `db/db_impl/db_impl_write.cc` `DBImpl::SelectColumnFamiliesForAtomicFlush`
- **Fix:** Say that atomic flush is atomic across the participating CFs selected for that flush request, not across every CF in the DB.

### [WRONG] Row cache key format is incomplete
- **File:** `07_read_path.md`, section "Row Cache"
- **Claim:** "The row cache stores complete key-value pairs keyed by `(file_number, user_key)`."
- **Reality:** The row-cache key prefix includes the row-cache ID, file number, and cache-entry sequence number before the user key.
- **Source:** `db/table_cache.cc` `TableCache::CreateRowCacheKeyPrefix`; `TableCache::GetFromRowCache`
- **Fix:** Document the full key shape and why the sequence component is part of the cache key.

### [WRONG] DeleteRange plus row_cache is not validated on the write path
- **File:** `07_read_path.md`, section "Row Cache"
- **Claim:** "Important: `DeleteRange` is not compatible with `row_cache` -- this is validated at write time."
- **Reality:** `DBImpl::DeleteRange()` only performs timestamp validation before delegating. I did not find a row-cache compatibility check on the write path.
- **Source:** `db/db_impl/db_impl_write.cc` `DBImpl::DeleteRange`
- **Fix:** Rephrase this as a behavioral limitation: the row cache does not account for range tombstones, so users must avoid this feature combination.

### [WRONG] unordered_write relaxes snapshot semantics for Get too
- **File:** `06_write_path.md`, section "Unordered Write Flow"
- **Claim:** "This mode provides the highest throughput but relaxes read-your-own-write guarantees for iterators. Point reads (`Get`) are not affected because they access the memtable directly."
- **Reality:** The option comment explicitly says `unordered_write` relaxes snapshot immutability and can violate repeatability for snapshot-based `Get`, as well as `MultiGet` and iterator consistency.
- **Source:** `include/rocksdb/options.h` `DBOptions::unordered_write`
- **Fix:** Say that `unordered_write` preserves read-your-own-write but weakens snapshot immutability more broadly, including snapshot `Get`.

### [MISLEADING] Write overview overclaims durability on process crash
- **File:** `06_write_path.md`, section "Overview"
- **Claim:** "A process crash (not a machine reboot) will not lose data because the OS buffer survives the process."
- **Reality:** The public API docs place the durability controls on `WriteOptions::sync`, `SyncWAL()`, and `FlushWAL()` when `manual_wal_flush` is enabled. The blanket process-crash guarantee is not the documented contract, and with `manual_wal_flush=true` data can still be sitting in RocksDB's own WAL buffer until `FlushWAL()`.
- **Source:** `include/rocksdb/db.h` `DB::FlushWAL`; `DB::SyncWAL`; `include/rocksdb/options.h` `DBOptions::manual_wal_flush`
- **Fix:** Remove the process-crash guarantee and explain the actual persistence knobs.

### [WRONG] WriteBufferManager-triggered flush does not choose the largest memtable
- **File:** `08_flush_compaction_scheduling.md`, section "Flush Scheduling"
- **Claim:** "When `WriteBufferManager::ShouldFlush()` returns true, `HandleWriteBufferManagerFlush()` picks the column family with the largest memtable for flushing."
- **Reality:** It picks the CF whose mutable memtable has the smallest creation sequence, i.e. the oldest eligible mutable memtable.
- **Source:** `db/db_impl/db_impl_write.cc` `DBImpl::HandleWriteBufferManagerFlush`
- **Fix:** Replace "largest memtable" with "oldest eligible mutable memtable".

### [WRONG] Compaction scheduling limit omits bottom-priority scheduled compactions
- **File:** `08_flush_compaction_scheduling.md`, section "The Central Scheduler"
- **Claim:** "While `unscheduled_compactions_ > 0` and `bg_compaction_scheduled_` is below the limit (`max_compactions`) ..."
- **Reality:** The scheduler enforces `bg_compaction_scheduled_ + bg_bottom_compaction_scheduled_ < max_compactions`.
- **Source:** `db/db_impl/db_impl_compaction_flush.cc` `DBImpl::MaybeScheduleFlushOrCompaction`
- **Fix:** Update the limit description to include both normal and bottom-priority scheduled compactions.

### [WRONG] bg_work_paused_ means pause, not deletion
- **File:** `08_flush_compaction_scheduling.md`, section "The Central Scheduler"
- **Claim:** "DB is being deleted (`bg_work_paused_` > 0)"
- **Reality:** `bg_work_paused_` means background work is paused. Shutdown or deletion is tracked separately by `shutting_down_`.
- **Source:** `db/db_impl/db_impl_compaction_flush.cc` `DBImpl::MaybeScheduleFlushOrCompaction`
- **Fix:** Rename this guard to "background work paused" and keep shutdown as the separate `shutting_down_` condition.

### [MISLEADING] CompactedDBImpl eligibility is more specific than the docs say
- **File:** `01_overview.md`, section "Overview"; `10_secondary_and_readonly.md`, section "Compacted DB > Eligibility"
- **Claim:** "`CompactedDBImpl` | ... | Optimized read-only mode for databases with no L0 files" and "A database is eligible for `CompactedDBImpl` if: - All column families have empty memtables. - There are no WAL files to replay. - There is only the default column family."
- **Reality:** `CompactedDBImpl::Init()` accepts either exactly one L0 file with no other non-empty levels, or one non-empty lower level with all intermediate levels empty. The "no L0 files" summary is false, and the eligibility list omits the actual level-layout restrictions.
- **Source:** `db/db_impl/compacted_db_impl.cc` `CompactedDBImpl::Init`; `CompactedDBImpl::Open`
- **Fix:** Document the real level-layout constraints and soften the shorthand "fully compacted" description.

### [MISLEADING] Secondary reads are not the same as primary reads
- **File:** `10_secondary_and_readonly.md`, section "Secondary Instance > Read Path"
- **Claim:** "`DBImplSecondary::GetImpl()` follows the same pattern as the primary's `GetImpl()`, including `SuperVersion` reference management."
- **Reality:** Secondary point `GetImpl()` uses the current `versions_->LastSequence()` and does not honor `ReadOptions::snapshot`. Secondary iterators explicitly reject `ReadOptions::snapshot` and `tailing`.
- **Source:** `db/db_impl/db_impl_secondary.cc` `DBImplSecondary::GetImpl`; `DBImplSecondary::NewIterator`; `DBImplSecondary::NewIterators`; `DBImplSecondary::NewIteratorImpl`
- **Fix:** Describe the secondary read path as structurally similar but with mode-specific snapshot restrictions: point gets use the current secondary sequence; iterators reject snapshots and tailing.

## Completeness Gaps

### verify_manifest_content_on_close is missing from close/version-management docs
- **Why it matters:** This is a recent mutable DB option that can re-read the MANIFEST on close, detect corruption, and rewrite a fresh MANIFEST from in-memory state. It affects close latency, failure reporting, and recovery expectations.
- **Where to look:** `include/rocksdb/options.h` `DBOptions::verify_manifest_content_on_close`; `db/version_set.cc` `VersionSet::Close`; `db/version_set_test.cc` `VersionSetTest.ManifestContentValidationOnCloseClean`; `VersionSetTest.ManifestContentValidationOnCloseCorruptRecord`
- **Suggested scope:** Add a focused subsection in `03_db_close.md` and a shorter cross-reference in `05_version_management.md`.

### Follower catch-up tuning options are undocumented
- **Why it matters:** The follower section describes auto-tail behavior but omits the actual knobs that control cadence and retry behavior.
- **Where to look:** `include/rocksdb/options.h` `follower_refresh_catchup_period_ms`; `follower_catchup_retry_count`; `follower_catchup_retry_wait_ms`; `db/db_impl/db_impl_follower.cc` `DBImplFollower::PeriodicRefresh`
- **Suggested scope:** Expand the follower subsection in `10_secondary_and_readonly.md` with an options table and retry loop summary.

### Seqno-to-time mapping lifecycle is barely covered
- **Why it matters:** The docs mention periodic seqno-to-time work in passing, but not the open-time seeding, new-DB prepopulation, or config-change integration that determine whether preserve/preclude options behave correctly.
- **Where to look:** `db/db_impl/db_impl.cc` `RegisterRecordSeqnoTimeWorker`; `EnsureSeqnoToTimeMapping`; `PrepopulateSeqnoToTimeMapping`; `InstallSuperVersionForConfigChange`
- **Suggested scope:** Add a subsection to `02_db_open.md` and a cross-reference in `05_version_management.md`.

### Multi-column-family consistent snapshot acquisition is not documented as a mechanism
- **Why it matters:** `MultiGet`, coalescing iterators, and persisted-tier reads rely on a non-trivial algorithm to obtain a consistent cross-CF view. There are dedicated tests for this behavior, which is a strong sign it is part of the contract.
- **Where to look:** `db/db_impl/db_impl.cc` `DBImpl::MultiCFSnapshot`; `db/db_basic_test.cc` `DBMultiGetTestWithParam.MultiGetMultiCFSnapshot`; `db/multi_cf_iterator_test.cc` `CoalescingIteratorTest.ConsistentViewExplicitSnapshot`; `CoalescingIteratorTest.ConsistentViewImplicitSnapshot`
- **Suggested scope:** Expand `07_read_path.md` with a dedicated subsection rather than burying this under the short `MultiGet` summary.

## Depth Issues

### Close flow step 2 hides the flush-before-shutdown behavior
- **Current:** Step 2 says `CancelAllBackgroundWork(false)` sets `shutting_down_` and signals waiters.
- **Missing:** Before setting `shutting_down_`, `CancelAllBackgroundWork(false)` may synchronously flush all column families if there is unpersisted data and `avoid_flush_during_shutdown` is false.
- **Source:** `db/db_impl/db_impl.cc` `DBImpl::CancelAllBackgroundWork`

### Multi-CF snapshot acquisition needs real control-flow detail
- **Current:** The read chapter says `MultiGet` acquires one `SuperVersion` per column family.
- **Missing:** The actual algorithm retries lock-free up to three times for implicit snapshots, falls back to taking `mutex_` on the last try, and forces the mutex path immediately for `kPersistedTier`.
- **Source:** `db/db_impl/db_impl.cc` `DBImpl::MultiCFSnapshot`

### Atomic flush is missing the wait/install/rollback protocol
- **Current:** The atomic flush section gives a short five-step outline.
- **Missing:** It does not explain waiting on `atomic_flush_install_cv_`, the background-error early-abort path used to avoid deadlock, or rollback of already-run flush jobs when installation cannot proceed.
- **Source:** `db/db_impl/db_impl_compaction_flush.cc` `DBImpl::AtomicFlushMemTablesToOutputFiles`

## Structure and Style Violations

### Inline code quotes are pervasive across the whole component doc set
- **File:** `docs/components/db_impl/index.md` and all chapter files under `docs/components/db_impl/`
- **Details:** The review prompt explicitly says "NO inline code quotes," but these docs use inline backticks everywhere for file names, types, functions, options, and even prose emphasis.

### index.md labels non-invariants as invariants
- **File:** `docs/components/db_impl/index.md`
- **Details:** The "Key Invariants" section includes items that are not DBImpl correctness invariants in the requested sense, especially "Lock acquisition order: `options_mutex_` before `mutex_` before `wal_write_mutex_`" and "Checksum on compressed data is verified before decompression (inherited from block format)." The latter is also off-scope for a DBImpl component index.

### Option references are not consistently anchored to their owning option struct/header
- **File:** `02_db_open.md`, `04_column_families.md`, `06_write_path.md`, `08_flush_compaction_scheduling.md`
- **Details:** Many option tables and trigger descriptions mention fields without consistently naming the owning type and header on first mention. The prompt asks for option references as field name plus header path.

## Undocumented Complexity

### MultiCFSnapshot lock-free retry plus mutex fallback
- **What it is:** Cross-CF reads try to take a consistent view without the DB mutex. For implicit snapshots, the code retries up to three times if a memtable switch races the acquisition. On the last try it takes `mutex_`, and `kPersistedTier` takes the mutex path immediately.
- **Why it matters:** This is the real reason cross-CF reads can offer a consistent view without always serializing on the DB mutex. Without documenting it, the current `MultiGet` section sounds much simpler than the implementation and tests.
- **Key source:** `db/db_impl/db_impl.cc` `DBImpl::MultiCFSnapshot`
- **Suggested placement:** Add to `07_read_path.md`

### Atomic flush commit ordering and rollback rules
- **What it is:** After running per-CF flush jobs, atomic flush waits until all earlier immutable memtables are installed, coordinates on `atomic_flush_install_cv_`, and rolls back flush state if it cannot safely install results. It also has a background-error escape hatch to avoid deadlock with recovery.
- **Why it matters:** This is the core correctness mechanism behind atomic flush. The current docs mention the condition variable but not what the wait actually protects or how failures are unwound.
- **Key source:** `db/db_impl/db_impl_compaction_flush.cc` `DBImpl::AtomicFlushMemTablesToOutputFiles`
- **Suggested placement:** Expand the atomic flush subsection in `08_flush_compaction_scheduling.md`

### Seqno-to-time mapping is tied to config changes, not just periodic work
- **What it is:** `InstallSuperVersionForConfigChange()` may reseed seqno-to-time mapping on `SetOptions()` or CF creation. New DBs can prepopulate historical mapping, and the periodic worker only maintains the mapping after initial setup.
- **Why it matters:** Readers trying to understand preserve/preclude options will miss important correctness setup if the docs present this as just a background maintenance task.
- **Key source:** `db/db_impl/db_impl.cc` `InstallSuperVersionForConfigChange`; `EnsureSeqnoToTimeMapping`; `PrepopulateSeqnoToTimeMapping`; `RegisterRecordSeqnoTimeWorker`
- **Suggested placement:** Add to `02_db_open.md` and `05_version_management.md`

### MANIFEST content verification on close can rewrite the MANIFEST
- **What it is:** When `verify_manifest_content_on_close` is enabled, `VersionSet::Close()` re-reads the MANIFEST with checksums enabled, detects decode or CRC problems, and writes a fresh MANIFEST from in-memory state before finishing close.
- **Why it matters:** This is a surprising cross-boundary behavior from a close call: it can do additional read I/O, surface close-time corruption, and rotate the MANIFEST even when shutdown otherwise looks normal.
- **Key source:** `include/rocksdb/options.h` `DBOptions::verify_manifest_content_on_close`; `db/version_set.cc` `VersionSet::Close`
- **Suggested placement:** Add to `03_db_close.md` with a link back to version-management docs

## Positive Notes

- The chapter split is strong. Open, close, column-family management, versioning, read, write, and background work are separated in a way that matches how engineers usually approach DBImpl.
- `index.md` hits the expected size target at 41 lines and gives a good top-level map before readers dive into chapter detail.
- The docs generally choose the right source files to emphasize. Even when behavior is misdescribed, the source-file coverage is pointed at the major implementation units someone would actually read.
- There are no line-number references or box-drawing characters, which matches the requested documentation style.

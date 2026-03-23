# Review: snapshot — Codex

## Summary
Overall quality rating: significant issues.

The doc set has a reasonable chapter breakdown and a few sections are grounded in the current code. The linked-list explanation in the data-structures chapter is mostly accurate, the lifecycle chapter correctly distinguishes `GetLastPublishedSequence()` from `LastSequence()` for public snapshots, and the timestamped-snapshot chapter at least notices the same-timestamp reuse/error cases.

The main problem is that several of the most important cross-component claims are either wrong or scoped too broadly. The docs currently describe snapshot retention as if it were universal, but FIFO deletion compactions ignore snapshots. The iterator lifetime section has the implicit/explicit snapshot story backwards. The timestamped-snapshot chapter names a nonexistent public API. The compaction chapter reduces retention to rules that do not match the actual `CompactionIterator` state machine. Several newer or subtle behaviors are also missing entirely: iterator auto-refresh, multi-CF snapshot capture, UDT interactions, and the flush-ordering tricks that preserve snapshot correctness.

## Correctness Issues

### [WRONG] The docs treat snapshot pinning as universal, but FIFO compaction ignores snapshots
- **File:** `docs/components/snapshot/index.md` / Overview; `docs/components/snapshot/05_compaction.md` / Snapshot-Aware Garbage Collection
- **Claim:** "Snapshots pin old key versions by preventing compaction from deleting data visible to any active snapshot."
- **Reality:** That statement is false for FIFO deletion compactions. FIFO compaction builds deletion compactions with `earliest_snapshot = std::nullopt` and `snapshot_checker = nullptr`, and background compaction has an explicit TODO asking whether snapshots should be honored there. The snapshot-retention rules documented here only apply to compactions that run through the snapshot-aware `CompactionIterator` logic, not FIFO file-deletion compactions.
- **Source:** `db/compaction/compaction_picker_fifo.cc` `PickTTLCompaction`, `PickSizeCompaction`, `PickIntraL0Compaction`; `db/db_impl/db_impl_compaction_flush.cc` `BackgroundCompaction`
- **Fix:** Scope the retention claims to non-FIFO compaction styles, or add a prominent FIFO exception with a cross-reference to the FIFO compaction docs.

### [WRONG] The implicit-snapshot iterator lifetime section has the behavior backwards
- **File:** `docs/components/snapshot/04_reads.md` / Explicit vs. Implicit Snapshot Lifetime Differences
- **Claim:** "The pinned SuperVersion provides read consistency for the duration of the operation, but does not prevent future compactions from removing data. This means a long-lived iterator without an explicit snapshot can encounter 'data not found' situations if compaction drops data between iterator creation and access."
- **Reality:** Snapshot-less iterators keep their original `SuperVersion` and referenced files pinned; they do not auto-refresh. The case that needs special handling is the opposite one: an iterator with an explicit snapshot can use `ReadOptions::auto_refresh_iterator_with_snapshot` so old files can be released while preserving the same snapshot sequence.
- **Source:** `db/db_impl/db_impl.cc` `NewIterator`; `db/arena_wrapped_db_iter.cc` `MaybeAutoRefresh`; `include/rocksdb/options.h` `ReadOptions::auto_refresh_iterator_with_snapshot`; `db/db_iterator_test.cc` `DBIteratorTest.AutoRefreshIterator`
- **Fix:** Reverse the explanation. Say implicit-snapshot iterators pin a `SuperVersion`, while explicit-snapshot iterators can opt into auto-refresh to avoid holding obsolete files indefinitely.

### [WRONG] The timestamped-snapshot chapter names a nonexistent public API and wrong public header
- **File:** `docs/components/snapshot/07_timestamped_snapshots.md` / Files; Creating Timestamped Snapshots
- **Claim:** "`DB::CreateTimestampedSnapshot()` creates a snapshot associated with a specific timestamp."
- **Reality:** The public API is on `TransactionDB`, not `DB`. `include/rocksdb/db.h` does not declare `CreateTimestampedSnapshot()`. The public entry points are `TransactionDB::CreateTimestampedSnapshot()` and `Transaction::CommitAndTryCreateSnapshot()`.
- **Source:** `include/rocksdb/utilities/transaction_db.h` `TransactionDB::CreateTimestampedSnapshot`; `include/rocksdb/utilities/transaction.h` `Transaction::CommitAndTryCreateSnapshot`; `db/db_impl/db_impl.h` internal `DBImpl::CreateTimestampedSnapshot`
- **Fix:** Rename the API references to the `TransactionDB` and `Transaction` APIs, and fix the chapter's `Files:` line to point at the transaction headers.

### [WRONG] The compaction retention table invents a raw `seq < earliest_snapshot_` drop rule
- **File:** `docs/components/snapshot/05_compaction.md` / Key Version Retention Rules
- **Claim:** "`Sequence number < earliest_snapshot_` | `DROP -- no snapshot can see this version`"
- **Reality:** That is not how retention works. Older versions are kept whenever they are the first version visible in a snapshot stripe, when `SingleDelete` handling requires them, when delete-marker conditions do not allow dropping them, when transaction conflict checking needs them, or when UDT/full-history rules prevent GC. The same section's worked example keeps `foo@90` even though `90 < 100`.
- **Source:** `db/compaction/compaction_iterator.cc` `NextFromInput`; `db/compaction/compaction_iterator.cc` `findEarliestVisibleSnapshot`
- **Fix:** Rewrite the rule table around snapshot stripes and record-type-specific rules instead of a standalone comparison to `earliest_snapshot_`.

### [WRONG] The delete-marker section incorrectly restricts obsolete delete dropping to non-bottommost levels
- **File:** `docs/components/snapshot/05_compaction.md` / Deletion Marker Handling
- **Claim:** "A deletion marker at a non-bottommost level can be dropped only if..."
- **Reality:** The obsolete-delete rule is not limited to non-bottommost compactions. The code applies it whenever there is a real compaction, `allow_ingest_behind` is false, the delete is definitely in the earliest snapshot, and the key does not exist beyond the output level. For `kTypeDeletionWithTimestamp`, the key's timestamp must also be older than `full_history_ts_low_`.
- **Source:** `db/compaction/compaction_iterator.cc` `NextFromInput`
- **Fix:** Remove the non-bottommost qualifier and document the real conditions, including the UDT and `allow_ingest_behind` caveats.

### [WRONG] `SetSnapshotOnNextOperation()` is not triggered by arbitrary reads
- **File:** `docs/components/snapshot/06_transactions.md` / Transaction Snapshot Usage Patterns
- **Claim:** "`SetSnapshotOnNextOperation()` -- Defers snapshot creation until the next read/write operation; optionally notifies via `TransactionNotifier`"
- **Reality:** The public contract is narrower. Snapshot creation is deferred until the next `Put`/`PutEntity`/`Merge`/`Delete`/`GetForUpdate`/`MultiGetForUpdate`, and for WriteCommitted it may also happen on `Commit`. Plain `Get()` is not part of the documented trigger set and does not call `SetSnapshotIfNeeded()`.
- **Source:** `include/rocksdb/utilities/transaction.h` `Transaction::SetSnapshotOnNextOperation`; `utilities/transactions/transaction_base.cc` `SetSnapshotIfNeeded`; `utilities/transactions/pessimistic_transaction.cc` `TryLock`; `utilities/transactions/optimistic_transaction.cc` `TryLock`
- **Fix:** Use the exact trigger semantics from the public header, including the WriteCommitted-on-Commit exception.

### [MISLEADING] Timestamped snapshot "automatic cleanup" hides the DB-owned reference
- **File:** `docs/components/snapshot/07_timestamped_snapshots.md` / Creating Timestamped Snapshots; Ownership Model
- **Claim:** "They use `shared_ptr` with a custom deleter that calls `ReleaseSnapshot()`, enabling automatic cleanup when the last reference is dropped."
- **Reality:** The DB itself keeps a `shared_ptr` in `TimestampedSnapshotList`. Dropping the application's `shared_ptr` does not release the snapshot. Cleanup only happens after the DB erases the map entry through `ReleaseTimestampedSnapshotsOlderThan()`, or when the DB is torn down.
- **Source:** `db/db_impl/db_impl.cc` `CreateTimestampedSnapshotImpl`; `db/snapshot_impl.h` `TimestampedSnapshotList::AddSnapshot` and `ReleaseSnapshotsOlderThan`; `utilities/transactions/timestamped_snapshot_test.cc`
- **Fix:** State explicitly that timestamped snapshots remain DB-owned until they are removed from the timestamp index.

### [MISLEADING] Snapshot immutability is overstated
- **File:** `docs/components/snapshot/index.md` / Key Characteristics and Key Invariants; `docs/components/snapshot/03_lifecycle.md` / Thread Safety
- **Claim:** "Snapshot objects are immutable after creation" and "Snapshot reads ... are thread-safe without any lock, because snapshot fields are immutable after creation"
- **Reality:** `WritePreparedTxnDB` mutates `SnapshotImpl::min_uncommitted_` after the `SnapshotImpl` has been created and inserted into the list. The public accessors are still read-only, but the internal object is not globally immutable after `SnapshotList::New()`.
- **Source:** `db/snapshot_impl.h` `SnapshotImpl::min_uncommitted_`; `utilities/transactions/write_prepared_txn_db.cc` `GetSnapshotInternal`; `utilities/transactions/write_prepared_txn_db.h` `EnhanceSnapshot`
- **Fix:** Narrow the statement to the public read accessors, or say immutability holds after transaction-specific snapshot enhancement completes.

### [MISLEADING] The `ReleaseSnapshot()` workflow omits the real gating and the second compaction-trigger path
- **File:** `docs/components/snapshot/03_lifecycle.md` / Releasing a Snapshot; Post-Release Compaction Triggering
- **Claim:** "`ReleaseSnapshot()` ... iterates all column families to call `UpdateOldestSnapshot()` on each"
- **Reality:** The code first checks a DB-wide `bottommost_files_mark_threshold_` and may skip the CF scan entirely. When it does scan, it skips `AllowIngestBehind()` column families. It also has a separate `standalone_range_deletion_files_mark_threshold_` path that can schedule compaction without calling `UpdateOldestSnapshot()`.
- **Source:** `db/db_impl/db_impl.cc` `ReleaseSnapshot`; `db/version_set.cc` `UpdateOldestSnapshot`
- **Fix:** Document the DB-wide threshold fast path, the `allow_ingest_behind` exclusion, and the standalone range tombstone threshold.

### [MISLEADING] The per-key compaction cost is not always `O(log num_snapshots)`
- **File:** `docs/components/snapshot/05_compaction.md` / `findEarliestVisibleSnapshot`; `docs/components/snapshot/08_monitoring_and_best_practices.md` / Performance Characteristics
- **Claim:** "`findEarliestVisibleSnapshot()` ... uses binary search" and "Compaction GC decision per key | O(log num_snapshots)"
- **Reality:** With ordinary snapshots, the search starts with `lower_bound`. With a `SnapshotChecker`, the code then linearly probes later snapshots, skips released snapshots, and may call `CheckInSnapshot()` repeatedly. In that mode, the per-key cost can degrade toward `O(num_snapshots)`.
- **Source:** `db/compaction/compaction_iterator.cc` `findEarliestVisibleSnapshot`
- **Fix:** Split the description into the ordinary case and the `SnapshotChecker` case.

### [MISLEADING] `inplace_update_support` is documented as if it were only a local CF restriction
- **File:** `docs/components/snapshot/index.md` / Key Invariants; `docs/components/snapshot/01_public_api.md` / Creating and Releasing Snapshots; `docs/components/snapshot/08_monitoring_and_best_practices.md` / Snapshot with `inplace_update_support`
- **Claim:** "`GetSnapshot()` returns `nullptr` when `inplace_update_support` is enabled in the column family options"
- **Reality:** Snapshot support is tracked at DB scope. If any live column family's memtable does not support snapshots, `is_snapshot_supported_` becomes false and `DB::GetSnapshot()` returns `nullptr` for the whole DB until the condition is removed.
- **Source:** `db/memtable.h` `MemTable::IsSnapshotSupported`; `db/db_impl/db_impl.cc` CF creation/drop updates of `is_snapshot_supported_`
- **Fix:** Say this is a DB-wide capability derived from all live column families, with `inplace_update_support` being the common built-in reason.

### [UNVERIFIABLE] Historical performance and space numbers are presented as current component facts
- **File:** `docs/components/snapshot/05_compaction.md` / Sequence Number Zeroing and Space Savings; `docs/components/snapshot/08_monitoring_and_best_practices.md` / Impact on Write Throughput and Memory Overhead
- **Claim:** "the internal bytes overhead at the bottommost level is approximately 1.6% with zlib and 5% with LZ4...", "Benchmarks have shown that relaxing this ordering constraint can yield 3-6x higher write throughput", and "Per snapshot: approximately 80 bytes"
- **Reality:** These numbers are not derived from the current snapshot implementation and are not backed by in-tree snapshot tests or constants. They read like historical benchmark results rather than maintained component facts.
- **Source:** current snapshot code paths in `db/compaction/compaction_iterator.cc`, `db/db_impl/db_impl.cc`, and `db/snapshot_impl.h`; no corresponding in-tree constants/tests in the snapshot implementation
- **Fix:** Either cite the external benchmark/design source explicitly and label the numbers as historical examples, or remove them from the component docs.

## Completeness Gaps

### Iterator refresh and auto-refresh are effectively undocumented
- **Why it matters:** This is now a first-class snapshot-read behavior for long-running scans, and the current docs actually contradict it in one place.
- **Where to look:** `include/rocksdb/iterator_base.h` `Iterator::Refresh`; `include/rocksdb/options.h` `ReadOptions::auto_refresh_iterator_with_snapshot`; `db/arena_wrapped_db_iter.cc`; `db/db_iterator_test.cc`; `db/db_range_del_test.cc`
- **Suggested scope:** Add a subsection to chapter 4 covering `Iterator::Refresh(snapshot)`, auto-refresh, and the explicit-vs-implicit snapshot file-lifetime difference.

### Multi-column-family snapshot capture is missing
- **Why it matters:** `MultiGet`, multi-CF scans, and `NewIterators()` do not use the same simple "pin one SuperVersion, then pick a seqno" flow as single-CF reads. They use retries and sometimes the DB mutex to produce a consistent cross-CF view.
- **Where to look:** `db/db_impl/db_impl.cc` `MultiCFSnapshot`
- **Suggested scope:** Add a chapter-4 subsection for multi-CF reads, especially the retry path and the `kPersistedTier` behavior.

### UDT interactions are mostly absent
- **Why it matters:** The snapshot docs currently separate "timestamped snapshots" from "user-defined timestamps," but the actual read/compaction behavior depends heavily on `ReadOptions.timestamp`, `full_history_ts_low_`, and UDT-specific GC rules.
- **Where to look:** `db/db_iter.cc` `IsVisible`; `db/compaction/compaction_iterator.cc`; `include/rocksdb/options.h` comments for `auto_refresh_iterator_with_snapshot`
- **Suggested scope:** Add short sections in chapters 4, 5, and 8 covering UDT-aware visibility, UDT-aware delete/seqno-zeroing rules, and the auto-refresh caveat when UDTs are not persisted.

### Snapshot-triggered compaction exceptions and opt-outs are missing
- **Why it matters:** Operators will not understand why releasing a snapshot sometimes schedules bottommost compaction, sometimes schedules range-tombstone cleanup, and sometimes does nothing at all.
- **Where to look:** `db/db_impl/db_impl.cc` `ReleaseSnapshot`; `db/version_set.cc` `UpdateOldestSnapshot`; `db/db_compaction_test.cc` `BottommostFileCompactionAllowIngestBehind`
- **Suggested scope:** Expand chapters 3 and 5 to cover `allow_ingest_behind`, the standalone range tombstone threshold, and the fact that not all compaction styles honor snapshots the same way.

### Timestamped snapshot API preconditions and current limitations are missing
- **Why it matters:** The public API is easy to misuse without these caveats. `CreateTimestampedSnapshot()` requires no active writes. `CommitAndTryCreateSnapshot()` is currently only supported on WriteCommitted transactions. A transaction can commit successfully without producing a timestamped snapshot.
- **Where to look:** `include/rocksdb/utilities/transaction_db.h` `TransactionDB::CreateTimestampedSnapshot`; `include/rocksdb/utilities/transaction.h` `Transaction::CommitAndTryCreateSnapshot`; `utilities/transactions/timestamped_snapshot_test.cc`
- **Suggested scope:** Add these caveats to chapter 7 and mention that callers must check whether the returned snapshot is non-null.

## Depth Issues

### Transaction-aware compaction needs more than "binary search plus snapshot checker"
- **Current:** Chapter 5 says a snapshot vector and optional `SnapshotChecker` are passed to `CompactionIterator`.
- **Missing:** It does not explain `job_snapshot`, `KeyCommitted()`, `released_snapshots_`, or why released snapshots can still affect the in-progress compaction's visibility checks.
- **Source:** `db/job_context.h` `GetJobSnapshotSequence`; `db/compaction/compaction_iterator.h`; `db/compaction/compaction_iterator.cc` `findEarliestVisibleSnapshot`

### The docs do not explain the snapshot-sensitive flush ordering
- **Current:** The lifecycle/compaction chapters talk about snapshot capture and flushes separately.
- **Missing:** The non-atomic flush path intentionally orders snapshot capture, memtable selection, and `NotifyOnFlushBegin()` to avoid creating a new snapshot that the flush job does not know about.
- **Source:** `db/db_impl/db_impl_compaction_flush.cc` `FlushMemTableToOutputFile`

## Structure and Style Violations

### The index is below the required size floor
- **File:** `docs/components/snapshot/index.md`
- **Details:** The review brief says `index.md` should be 40-80 lines. The current file is 39 lines.

### Inline code quoting is pervasive across the whole doc set
- **File:** `docs/components/snapshot/index.md` and all chapter files
- **Details:** The review brief explicitly says "NO inline code quotes." These docs use inline code formatting for paths, APIs, options, types, enums, and ordinary prose almost everywhere.

### Chapter 7's `Files:` line is incorrect for the public API it describes
- **File:** `docs/components/snapshot/07_timestamped_snapshots.md`
- **Details:** The chapter points at `include/rocksdb/db.h`, but the public APIs it discusses live in `include/rocksdb/utilities/transaction_db.h` and `include/rocksdb/utilities/transaction.h`.

## Undocumented Complexity

### Snapshot-sensitive flush ordering around listener callbacks
- **What it is:** The flush path must avoid letting a new snapshot appear after `snapshot_seqs` are computed but before memtables are chosen for the flush job. The code comments explain why `NotifyOnFlushBegin()` is deliberately placed after memtable picking in the non-atomic path when possible.
- **Why it matters:** Without this ordering, a flush could drop keys needed by a newly created snapshot even though snapshot capture already "happened."
- **Key source:** `db/db_impl/db_impl_compaction_flush.cc` `FlushMemTableToOutputFile`
- **Suggested placement:** Chapter 3 or chapter 5

### Timestamped snapshots are always treated as write-conflict boundaries
- **What it is:** `CreateTimestampedSnapshotImpl()` always sets `is_write_conflict_boundary_ = true`, even for snapshots that are only going to be used by readers.
- **Why it matters:** Timestamped snapshots can pin more history than ordinary read-only snapshots because compaction must preserve enough state for transaction conflict detection as well.
- **Key source:** `db/db_impl/db_impl.h` comment above `CreateTimestampedSnapshot`; `db/db_impl/db_impl.cc` `CreateTimestampedSnapshotImpl`; `db/snapshot_impl.h` `SnapshotList::GetAll`
- **Suggested placement:** Chapter 7

### Multi-CF snapshot acquisition has its own consistency algorithm
- **What it is:** `MultiCFSnapshot()` retries when a memtable switch races with snapshot acquisition and, on the last attempt or for `kPersistedTier`, may take the DB mutex to force a consistent cross-CF view.
- **Why it matters:** Anyone debugging cross-CF read anomalies or performance will not find the real logic from the current single-CF narrative.
- **Key source:** `db/db_impl/db_impl.cc` `MultiCFSnapshot`
- **Suggested placement:** Chapter 4

## Positive Notes

- `docs/components/snapshot/02_data_structures.md` correctly describes the `SnapshotList` sentinel layout and the duplicate-elimination behavior in `GetAll()`.
- `docs/components/snapshot/03_lifecycle.md` usefully distinguishes `GetLastPublishedSequence()` from `LastSequence()` for the ordinary snapshot API.
- `docs/components/snapshot/07_timestamped_snapshots.md` correctly captures the "same timestamp + same sequence reuses the latest snapshot; same timestamp + larger sequence is rejected" behavior from the implementation.

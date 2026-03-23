# Review: snapshot -- Claude Code

## Summary
Overall quality rating: **good**

The snapshot documentation covers the core concepts well: lifecycle, data structures, compaction interaction, and transaction integration are all present and mostly accurate. The doubly-linked list mechanics, `GetSnapshotImpl` workflow, and compaction GC rules are described faithfully. The biggest concern is a factually wrong claim about implicit snapshot safety with iterators, an incorrect function name in the compaction chapter, misattribution of the timestamped snapshot API to the wrong class, and several unverifiable compression ratio numbers that appear fabricated. Structure and style are clean overall.

## Correctness Issues

### [WRONG] Implicit snapshots and iterator "data not found" claim
- **File:** `04_reads.md`, section "Explicit vs. Implicit Snapshot Lifetime Differences"
- **Claim:** "a long-lived iterator without an explicit snapshot can encounter 'data not found' situations if compaction drops data between iterator creation and access"
- **Reality:** Iterators pin the `SuperVersion`, which holds a ref on the `Version`, which ref-counts all SST files. Compaction may logically drop old key versions (since implicit snapshots are not in `SnapshotList`), but the physical SST files containing those versions remain alive on disk as long as the iterator's `Version` reference exists. The iterator reads from its pinned file set, not from the compaction output. Data is never "not found" -- the iterator always sees a consistent view.
- **Source:** `db/db_impl/db_impl.cc` (`NewIteratorImpl` pins SV at line ~4036; `GetReferencedSuperVersion` adds extra Ref), `db/version_set.cc` (`FindObsoleteFiles` only marks files without live version references as deletable)
- **Fix:** Remove the "data not found" claim. Rewrite to: "Implicit snapshots are NOT tracked in the `SnapshotList`, so compaction may drop old versions visible only to implicit snapshots. However, iterators are still safe because the pinned SuperVersion holds a reference to the Version, which prevents the underlying SST files from being deleted. The iterator reads from its original file set regardless of concurrent compactions."

### [WRONG] Deletion marker handling cites wrong function
- **File:** `05_compaction.md`, section "Deletion Marker Handling"
- **Claim:** "The key does not exist in any level beyond the compaction output level (verified via `RangeMightExistAfterSortedRun()`)"
- **Reality:** The actual function used is `KeyNotExistsBeyondOutputLevel()` in `db/compaction/compaction.h`. `RangeMightExistAfterSortedRun()` is a different function in `db/version_set.cc` used for `GenerateBottommostFiles()`, not for the compaction iterator's deletion marker logic.
- **Source:** `db/compaction/compaction_iterator.cc` lines 898-905
- **Fix:** Replace `RangeMightExistAfterSortedRun()` with `KeyNotExistsBeyondOutputLevel()` (defined in `db/compaction/compaction.h`)

### [WRONG] Timestamped snapshot API attributed to wrong class
- **File:** `07_timestamped_snapshots.md`, section "API" and **Files** line
- **Claim:** "Currently, this functionality is exposed through the RocksDB transactions layer (see `TransactionDB::CreateTimestampedSnapshot()` and `Transaction::CommitAndTryCreateSnapshot()`)" -- this sentence is correct, but the **Files** line lists `include/rocksdb/db.h` and the section "Creating Timestamped Snapshots" says `DB::CreateTimestampedSnapshot()`.
- **Reality:** `CreateTimestampedSnapshot()` is declared on `TransactionDB` in `include/rocksdb/utilities/transaction_db.h`, not on `DB` in `include/rocksdb/db.h`. The `DB` class has no timestamped snapshot APIs.
- **Source:** `include/rocksdb/utilities/transaction_db.h` lines 590-621
- **Fix:** Change **Files** to include `include/rocksdb/utilities/transaction_db.h` instead of `include/rocksdb/db.h`. Change `DB::CreateTimestampedSnapshot()` to `TransactionDB::CreateTimestampedSnapshot()` throughout. Also mention the related APIs: `GetTimestampedSnapshot()`, `ReleaseTimestampedSnapshotsOlderThan()`, `GetTimestampedSnapshots()`, `GetAllTimestampedSnapshots()`, `GetLatestTimestampedSnapshot()`.

### [WRONG] GetOldestSnapshotSequence return type
- **File:** `08_monitoring_and_best_practices.md`, DB Properties table
- **Claim:** `rocksdb.oldest-snapshot-sequence` type is `uint64`
- **Reality:** `SnapshotList::GetOldestSnapshotSequence()` returns `int64_t`, not `uint64_t`.
- **Source:** `db/snapshot_impl.h` line 171
- **Fix:** Change type column from `uint64` to `int64`

### [UNVERIFIABLE] Space amplification compression ratio numbers
- **File:** `05_compaction.md`, section "Sequence Number Zeroing and Space Savings"
- **Claim:** "approximately 1.6% with zlib and 5% with LZ4" for zeroed seqno overhead, and "approximately 22% with zlib and 30% with LZ4" for non-zeroed overhead
- **Reality:** These specific percentage numbers do not appear anywhere in the codebase, comments, or documentation. The only relevant comment (`compaction_iterator.cc` line 1279) says "Zeroing out the sequence number leads to better compression" with no quantitative data. These numbers appear to be AI-fabricated.
- **Source:** Searched all source files and comments; no match found
- **Fix:** Either remove the specific percentages entirely or replace with a qualitative statement: "Zeroing sequence numbers at the bottommost level significantly improves compression ratios because the 8-byte internal key metadata (7 bytes sequence number + 1 byte type) becomes highly compressible when all values are zero."

### [MISLEADING] is_snapshot_supported_ controlled by more than inplace_update_support
- **File:** `index.md`, Key Invariants; `01_public_api.md`, section on `GetSnapshot()`
- **Claim:** "`is_snapshot_supported_` is false when `inplace_update_support` is enabled"
- **Reality:** `is_snapshot_supported_` is set to false when `MemTable::IsSnapshotSupported()` returns false, which checks BOTH `inplace_update_support` AND `MemTableRep::IsSnapshotSupported()`. A custom `MemTableRep` could also disable snapshot support by overriding `IsSnapshotSupported()` to return false. All built-in memtable representations return true, so `inplace_update_support` is the only standard mechanism, but the doc should mention the `MemTableRep` path.
- **Source:** `db/memtable.h` line 790-792, `db/db_impl/db_impl_open.cc` lines 2622-2625
- **Fix:** Change to: "`is_snapshot_supported_` is false when any column family's memtable does not support snapshots (either because `inplace_update_support` is enabled or because the `MemTableRep` implementation returns false from `IsSnapshotSupported()`)"

## Completeness Gaps

### Missing PerfContext counter
- **Why it matters:** Developers profiling snapshot overhead need to know about `get_snapshot_time` in PerfContext
- **Where to look:** `include/rocksdb/perf_context.h` line 165: `uint64_t get_snapshot_time; // total nanos spent on getting snapshot`
- **Suggested scope:** Mention in `08_monitoring_and_best_practices.md` alongside the DB Properties table

### Missing TransactionDB timestamped snapshot APIs
- **Why it matters:** The doc covers `TimestampedSnapshotList` internals but omits several public APIs on `TransactionDB`
- **Where to look:** `include/rocksdb/utilities/transaction_db.h` lines 587-621
- **Suggested scope:** Add to `07_timestamped_snapshots.md` API section: `GetLatestTimestampedSnapshot()`, `GetAllTimestampedSnapshots()`, `GetTimestampedSnapshots(ts_lb, ts_ub)`, `ReleaseTimestampedSnapshotsOlderThan(ts)`

### Missing standalone_range_deletion_files_mark_threshold_
- **Why it matters:** `ReleaseSnapshot()` also checks `standalone_range_deletion_files_mark_threshold_` to trigger compaction for files with standalone range deletions, not just `bottommost_files_mark_threshold_`
- **Where to look:** `db/db_impl/db_impl.cc` lines 4529-4543 in `ReleaseSnapshot()`
- **Suggested scope:** Brief mention in `03_lifecycle.md` or `05_compaction.md` under post-release compaction triggering

### Missing TXN_SNAPSHOT_MUTEX_OVERHEAD statistic
- **Why it matters:** WritePrepared transactions have a dedicated statistic for snapshot mutex overhead
- **Where to look:** `include/rocksdb/statistics.h` line 406: `TXN_SNAPSHOT_MUTEX_OVERHEAD`
- **Suggested scope:** Mention in `06_transactions.md`

## Depth Issues

### visible_at_tip_ optimization description imprecise
- **Current:** Doc says "when there are no snapshots at all, `earliest_snapshot_` is set to `kMaxSequenceNumber`"
- **Missing:** `earliest_snapshot_` is a parameter passed by the caller, not automatically set. `visible_at_tip_` is set to `true` when the snapshots vector is empty (line 89 of `compaction_iterator.cc`). When true, `current_user_key_snapshot_` is assigned `earliest_snapshot_` directly without binary search, causing all versions except the first of each key to share the same snapshot boundary and thus get dropped.
- **Source:** `db/compaction/compaction_iterator.cc` lines 89, 597

### ReleaseSnapshot workflow missing detail on oldest_snapshot computation
- **Current:** Doc says "iterates all column families to call `UpdateOldestSnapshot()`"
- **Missing:** Before iterating CFs, `ReleaseSnapshot()` computes `oldest_snapshot` as `GetLastPublishedSequence()` if snapshots list is empty, or `snapshots_.oldest()->number_` otherwise. This value is compared against `bottommost_files_mark_threshold_` BEFORE iterating CFs. Also, after the first pass, a second pass recalculates `bottommost_files_mark_threshold_` across CFs that were not scheduled.
- **Source:** `db/db_impl/db_impl.cc` lines 4485-4524

### ReleaseSnapshotsOlderThan container type
- **Current:** `02_data_structures.md` says "Moves all snapshots... into the output container" without specifying the type
- **Missing:** The actual container type is `autovector<std::shared_ptr<const SnapshotImpl>>`, not `std::vector`. This matters for performance understanding -- `autovector` avoids heap allocation for small counts.
- **Source:** `db/snapshot_impl.h` line 227

## Structure and Style Violations

### index.md line count
- **File:** `index.md`
- **Details:** 40 lines -- at the lower boundary of the 40-80 range. Acceptable but tight.

### No violations found for:
- No line number references anywhere -- PASS
- No inline code quotes -- PASS
- No box-drawing characters -- PASS
- INVARIANT usage appropriate -- PASS (not overused)
- Options referenced with field_name + header path -- PASS
- Each chapter has Files: line -- PASS

## Undocumented Complexity

### InitSnapshotContext idempotency and placement
- **What it is:** `InitSnapshotContext()` has an idempotency guard (`snapshot_context_initialized`). It is called at multiple points: before `PickCompaction()` for universal compaction, before creating `CompactionJob` for non-trivial compactions, and in `CompactFilesImpl()`. The extra snapshot taken when `SnapshotChecker` is present is wrapped in `ManagedSnapshot` stored in `JobContext`.
- **Why it matters:** Understanding when and how snapshot context is initialized is important for anyone modifying compaction flow or transaction integration. The idempotency guard means it can be safely called multiple times.
- **Key source:** `db/db_impl/db_impl_compaction_flush.cc` lines 4979-5006, `db/job_context.h` lines 154-167
- **Suggested placement:** Add detail to `05_compaction.md` section "How Snapshots Reach the Compaction Iterator"

### ForwardIterator exception to snapshot pinning
- **What it is:** `ForwardIterator` (tailing iterators) does NOT pin a fixed SuperVersion and dynamically picks up new versions. This is an exception to the general iterator snapshot safety described in `04_reads.md`.
- **Why it matters:** Users of tailing iterators may assume the same safety guarantees as regular iterators.
- **Key source:** `db/forward_iterator.h`, `db/forward_iterator.cc`
- **Suggested placement:** Add a note in `04_reads.md` section "Iterator Consistency"

### kMinUnCommittedSeq value and semantics
- **What it is:** `kMinUnCommittedSeq` is defined as `1` in `include/rocksdb/types.h` line 28, with the comment "0 is always committed". This means sequence number 0 is universally considered committed, which is the baseline for `min_uncommitted_` optimization.
- **Why it matters:** Understanding the default value of `min_uncommitted_` is needed to understand WritePrepared transaction snapshot behavior.
- **Key source:** `include/rocksdb/types.h` line 28
- **Suggested placement:** Mention value in `02_data_structures.md` `min_uncommitted_` field description

### GetLastPublishedSequence vs LastSequence nuance in iterator path
- **What it is:** `GetImpl` uses `GetLastPublishedSequence()` for implicit snapshots, while `NewIteratorImpl` uses `versions_->LastSequence()` directly. This is deliberate: the comment in `NewIteratorImpl` says "no need to consider the special case of last_seq_same_as_publish_seq_==false since NewIterator is overridden in WritePreparedTxnDB".
- **Why it matters:** The doc describes this difference for `GetSnapshotImpl` but not for the divergence between Get and Iterator paths.
- **Key source:** `db/db_impl/db_impl.cc` lines 2567 (GetImpl), 4079-4089 (NewIteratorImpl), comment at 4060-4062
- **Suggested placement:** Add clarifying note in `04_reads.md` explaining why Get and Iterator paths differ

## Positive Notes

- The `SnapshotList` data structure description in `02_data_structures.md` is excellent -- accurately captures the circular linked list, dummy head sentinel, and all utility methods. Every detail verified against `snapshot_impl.h`.
- The compaction key version retention rules table in `05_compaction.md` with the worked example (snapshots [100, 150, 200] with key "foo") is clear, correct, and pedagogically effective.
- The `GetSnapshotImpl` workflow in `03_lifecycle.md` is accurate in all details: heap allocation before mutex, clock time first, sequence from `GetLastPublishedSequence()`, insertion into list, and the `inplace_update_support` guard.
- Thread safety discussion is appropriately placed and accurate -- immutability after creation, mutex requirements for list operations.
- The `bottommost_files_mark_threshold_` mechanism description is thorough and verified correct against `version_set.h` and `db_impl.cc`.
- Transaction integration chapter correctly explains the `SnapshotChecker` purpose, `DisableGCSnapshotChecker` fallback, and `min_uncommitted_` optimization.

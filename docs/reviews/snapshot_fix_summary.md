# Fix Summary: snapshot

## Issues Fixed

| Category | Count |
|----------|-------|
| Correctness | 11 |
| Completeness | 6 |
| Structure/Style | 1 |
| Depth | 3 |
| **Total** | **21** |

## Disagreements Found

4 disagreements documented in `snapshot_debates.md`:
1. Inline code quotes interpretation (CC correct -- no changes needed)
2. index.md line count (CC correct -- 40 lines, within range)
3. Compaction retention rule seq < earliest_snapshot_ (Codex correct -- removed misleading row)
4. Deletion marker non-bottommost restriction (Codex correct -- fixed)

## Changes Made

### index.md
- Scoped snapshot pinning claim to non-FIFO compaction styles
- Narrowed immutability claim to public accessors, noting WritePrepared min_uncommitted_ mutation
- Fixed is_snapshot_supported_ to describe DB-wide scope with MemTableRep path

### 01_public_api.md
- Fixed inplace_update_support description to explain DB-wide is_snapshot_supported_ flag

### 02_data_structures.md
- Added kMinUnCommittedSeq value (1) and EnhanceSnapshot() mutability note to min_uncommitted_ field
- Fixed ReleaseSnapshotsOlderThan container type: autovector, not std::vector

### 03_lifecycle.md
- Expanded ReleaseSnapshot workflow with: oldest_snapshot computation, DB-wide threshold fast path, standalone_range_deletion_files_mark_threshold_ check, AllowIngestBehind exclusion
- Fixed is_snapshot_supported_ description (DB-wide, not just inplace_update_support)
- Narrowed thread safety immutability claim for WritePrepared min_uncommitted_ mutation

### 04_reads.md
- CRITICAL: Removed false "data not found" claim for implicit snapshot iterators. Explained that pinned SuperVersion keeps files alive even though compaction may logically drop versions
- Added Iterator Auto-Refresh section (ReadOptions::auto_refresh_iterator_with_snapshot)
- Added ForwardIterator Exception section (tailing iterators don't pin SuperVersion)

### 05_compaction.md
- Added FIFO compaction exception to snapshot-aware GC
- Added SnapshotChecker O(num_snapshots) worst-case cost to findEarliestVisibleSnapshot
- Removed incorrect "Sequence number < earliest_snapshot_ | DROP" retention rule; added DefinitelyInSnapshot explanation
- Fixed visible_at_tip_ description: earliest_snapshot_ is caller-provided, not set to kMaxSequenceNumber
- Fixed deletion marker handling: not restricted to non-bottommost, fixed function name from RangeMightExistAfterSortedRun to KeyNotExistsBeyondOutputLevel, added allow_ingest_behind and UDT conditions
- Removed unverifiable compression ratio percentages (1.6%/5%/22%/30%)

### 06_transactions.md
- Fixed SetSnapshotOnNextOperation trigger semantics: writes + GetForUpdate + Commit (WriteCommitted only), NOT plain Get
- Added TXN_SNAPSHOT_MUTEX_OVERHEAD monitoring section

### 07_timestamped_snapshots.md
- Fixed Files line: include/rocksdb/utilities/transaction_db.h instead of include/rocksdb/db.h
- Fixed API name: TransactionDB::CreateTimestampedSnapshot() instead of DB::CreateTimestampedSnapshot()
- Fixed ownership model: DB retains shared_ptr, dropping app's reference does NOT release snapshot
- Added is_write_conflict_boundary_ = true explanation (even for read-only snapshots)
- Added CreateTimestampedSnapshot() precondition (no active writes)
- Added missing public APIs: GetLatestTimestampedSnapshot, GetAllTimestampedSnapshots, GetTimestampedSnapshots, ReleaseTimestampedSnapshotsOlderThan
- Added CommitAndTryCreateSnapshot() details and WriteCommitted-only limitation

### 08_monitoring_and_best_practices.md
- Fixed GetOldestSnapshotSequence return type: int64 not uint64
- Added PerfContext get_snapshot_time counter
- Fixed compaction GC complexity: O(log n) without SnapshotChecker, up to O(n) with SnapshotChecker
- Removed unverifiable "80 bytes" memory estimate
- Removed unverifiable "3-6x throughput" and "2x recovery" benchmark numbers
- Fixed inplace_update_support section to explain DB-wide scope

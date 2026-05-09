# Tombstone Lifecycle

**Files:** `db/db_impl/db_impl_write.cc`, `db/memtable.h`, `db/memtable.cc`, `db/compaction/compaction_iterator.cc`, `db/range_del_aggregator.h`, `db/db_iter.cc`

## Overview

RocksDB uses tombstones to represent deletions. Tombstones flow through the write path like normal keys but are eventually dropped during compaction when it is safe to do so. There are three tombstone types with different semantics and cleanup rules.

## Tombstone Types

| Type | API | ValueType | Scope |
|------|-----|-----------|-------|
| Delete | `Delete(key)` | `kTypeDeletion` | Covers all versions of `key` with `seq < tombstone_seq` |
| SingleDelete | `SingleDelete(key)` | `kTypeSingleDeletion` | Pairs with exactly one `Put` of the same key |
| DeleteRange | `DeleteRange(start, end)` | `kTypeRangeDeletion` | Covers all keys in `[start, end)` with `seq < tombstone_seq` |

## Tombstone Write Path

Tombstones follow the standard write path:

Step 1 - **API**: Application calls `Delete()`, `SingleDelete()`, or `DeleteRange()`, which constructs a `WriteBatch` with the appropriate `ValueType`.

Step 2 - **WAL**: Tombstone is persisted as a regular WAL record.

Step 3 - **MemTable**: Point tombstones (`kTypeDeletion`, `kTypeSingleDeletion`) are inserted into `table_` (the main memtable skiplist). Range tombstones (`kTypeRangeDeletion`) are inserted into `range_del_table_` (a separate skiplist).

Step 4 - **Flush**: During flush to L0 SST, point tombstones are written to data blocks. Range tombstones are written to a dedicated meta-block as a `FragmentedRangeTombstoneList`.

## Tombstone Visibility During Reads

### Point Lookup

During `DBImpl::GetImpl()`, range tombstones are checked first:

Step 1 - Compute `max_covering_tombstone_seq` via `RangeDelAggregator::MaxCoveringTombstoneSeqnum(user_key)`.

Step 2 - Search memtables and SST files for the key.

Step 3 - If a value is found at sequence `seq` and `seq < max_covering_tombstone_seq`, the value is covered by a range tombstone and treated as not-found.

Step 4 - If a point tombstone (`kTypeDeletion`) is found, return not-found immediately.

### Iterator

`DBIter` (see `db/db_iter.cc`) wraps a `MergingIterator` that interleaves range tombstone sentinel keys with point data:

- Point tombstones (`kTypeDeletion`): The iterator skips past the current key's older versions.
- Range tombstones: Integrated via `RangeDelAggregator::ShouldDelete()`, which checks whether the current key falls within a range tombstone and has a lower sequence number.

## Tombstone Cleanup During Compaction

### Point Tombstone Cleanup

`CompactionIterator` (see `db/compaction/compaction_iterator.cc`) drops point tombstones (`kTypeDeletion`) when all conditions hold:

1. **No ingest behind**: `compaction->allow_ingest_behind()` is false
2. **No snapshot needs it**: The tombstone's sequence is below the earliest live snapshot
3. **Key does not exist in lower levels**: `compaction->KeyNotExistsBeyondOutputLevel(user_key)` returns true (implying bottommost or no overlapping files below)

### SingleDelete Cleanup

`kTypeSingleDeletion` has additional constraints: it must pair with exactly one `Put` of the same key. The compaction iterator handles several cases:

- If a matching `Put` is found at an adjacent or nearby position, both the `SingleDelete` and the `Put` are dropped (when no snapshot requires them)
- Two consecutive `SingleDelete`s: the first is dropped, the second is re-evaluated (mismatch counter incremented, no error)
- `SingleDelete` + `Delete`: returns `Status::Corruption()` only if `enforce_single_del_contracts` is true; otherwise logs a warning and drops the SingleDelete
- `SingleDelete` + non-Put types: silently drops both, increments `num_single_del_mismatch`
- Zero `Put`s found: the SingleDelete is either dropped (if the key does not exist beyond the output level) or output as-is -- no corruption is flagged
- Snapshot retention: if an earlier snapshot still needs a conflict marker, the SingleDelete may be kept while the matched Put's value is cleared

The code comments explicitly state that mixing SingleDelete with other operations for a given key produces undefined behavior.

### Range Tombstone Cleanup

Range tombstones have a more complex lifecycle because they span multiple keys and files:

**During flush (L0):** Range tombstones covering the flushed memtable's sequence range are written to the SST meta-block.

**During compaction (Ln to Ln+1):** Range tombstones are fragmented at SST file boundaries via `TruncatedRangeDelIterator` and merged across input files. Fragments fully consumed by the compaction are dropped.

**Bottommost compaction:** Range tombstones with `seq < earliest_snapshot` and no overlapping keys in lower levels are dropped.

**Design property:** Range tombstones are truncated at SST file boundaries via `TruncatedRangeDelIterator` to prevent tombstones from leaking beyond their file's key range scope.

## Space Reclamation Timeline

The time between a deletion and actual space reclamation depends on several factors:

1. **Tombstone must reach bottommost level**: Until the tombstone is compacted to the bottommost level (or there are no overlapping keys below), it cannot be dropped.

2. **Snapshots must release**: If any live snapshot has a sequence number above the tombstone's sequence, the tombstone must be retained to maintain snapshot consistency.

3. **Compaction must be triggered**: Space is only reclaimed when compaction processes the relevant key range.

This means space from deleted keys is not reclaimed immediately; it requires one or more compaction cycles. For workloads with heavy deletions, `CompactRange()` can force compaction of specific ranges to accelerate reclamation.

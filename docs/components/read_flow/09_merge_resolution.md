# Merge Operator Resolution

**Files:** `db/merge_helper.h`, `db/merge_helper.cc`, `db/merge_context.h`, `db/db_iter.cc`, `db/memtable.cc`, `include/rocksdb/merge_operator.h`

## Overview

When a key has merge operands (written via `DB::Merge()`), the read path must collect operands across layers and apply the user's `MergeOperator` to produce a final result. Merge resolution happens in three contexts: point lookups, iteration, and compaction.

## MergeContext: Operand Collection

`MergeContext` (see `MergeContext` in `db/merge_context.h`) accumulates merge operands as the search progresses through layers:

| Aspect | Detail |
|--------|--------|
| Storage | `std::vector<Slice>` backed by `operand_list_` |
| Collection order | Newest to oldest (following the search order) |
| Presentation order | Oldest to newest (reversed before passing to merge operator) |
| Pinning | Operands can be pinned via `PinnedIteratorsManager` to avoid copies |

`PushOperand()` appends each operand as found. `GetOperands()` reverses the list (if needed) before returning to the caller.

## Resolution in Point Lookups

During `Get()`, merge operands are collected as the search moves through layers:

**In MemTable** (see `SaveValue()` in `db/memtable.cc`):
- When `kTypeMerge` is encountered, push the operand and set `merge_in_progress = true`
- If a base value (`kTypeValue`) is found later in the same memtable, call `TimedFullMerge()` immediately
- If no base value found, pass `merge_in_progress` to the next layer

**Across MemTableListVersion** (immutable memtables):
- Search continues newest to oldest
- Each memtable may contribute merge operands
- A base value in any memtable triggers immediate resolution

**In Version::Get()** (SST files):
- `GetContext` tracks merge state via `kMerge` state
- Each SST file may contribute more operands or a base value
- If all files exhausted with `kMerge` state, resolve with no base value

## Resolution During Iteration

`DBIter::MergeValuesNewToOld()` in `db/db_iter.cc` resolves merge chains during forward iteration:

Step 1: Push the first merge operand (the one at the current iterator position)

Step 2: Advance the internal iterator via `Next()`. Since InternalKey ordering is user_key ASC, sequence DESC, subsequent entries for the same user key have older sequences.

Step 3: For each subsequent entry with the same user key:
- `kTypeMerge`: Push another operand, continue
- `kTypeValue` / `kTypeBlobIndex`: Found base value, call `MergeWithPlainBaseValue()`
- `kTypeDeletion`: Stop -- merge without base value

Step 4: If no base value or deletion found (end of key history), call `MergeHelper::TimedFullMerge()` with no base value.

**Key Invariant:** Merge operands are collected newest to oldest, but presented to the merge operator in chronological order (oldest to newest).

## Full Merge vs Partial Merge

**Full Merge** (base value + operands -> result):
- Called via `MergeOperator::FullMergeV3()` in `MergeHelper::TimedFullMerge()`
- Produces a single final value (`kTypeValue` or `kTypeWideColumnEntity`)
- Used during reads (Get/Iterator) when a base value is found
- Also used without base value when all operands are collected

**Partial Merge** (operands -> fewer operands):
- Called via `MergeOperator::PartialMergeMulti()` during compaction
- Combines multiple operands into fewer operands without a base value
- Triggered when operand count >= 2 (or 1 if `MergeOperator::AllowSingleOperand()` returns true)
- Reduces storage and future read cost by pre-combining operands

Note: `max_successive_merges` (see `ColumnFamilyOptions` in `include/rocksdb/advanced_options.h`) is a separate write-path optimization that eagerly merges during memtable insertion. It does not control compaction-side partial merges.

## Snapshot Boundary Constraints

During compaction, merge operands cannot be combined across snapshot boundaries (see `MergeHelper::MergeUntil()` in `db/merge_helper.cc`):

If a snapshot exists between two merge operands' sequence numbers, they must be preserved separately because:
- A reader with the older snapshot must see the pre-merge state
- A reader with the newer snapshot must see the post-merge state

The check uses `SnapshotChecker::CheckInSnapshot()` to determine if an operand's sequence number falls within the range protected by a snapshot.

**Key Invariant:** Cannot merge operands across snapshot boundaries. Individual operands visible to snapshots must be preserved.

## Performance Implications

Merge resolution cost grows with the number of operands:
- **Read amplification**: Each Get must collect operands across all layers before resolving
- **Mitigation via compaction**: Compaction resolves merges (full or partial), reducing the operand chain length
- **Write-path mitigation**: `max_successive_merges` resolves merges at write time when the memtable already contains operands for the same key
- **Monitoring**: Track `NUMBER_MERGE_FAILURES` and merge-related statistics to identify performance issues

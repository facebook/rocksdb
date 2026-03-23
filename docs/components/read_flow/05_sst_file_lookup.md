# SST File Lookup

**Files:** `db/version_set.cc`, `db/version_set.h`, `db/table_cache.cc`, `db/table_cache.h`, `table/block_based/block_based_table_reader.cc`, `table/get_context.cc`, `table/get_context.h`

## FilePicker: Level-by-Level File Selection

`FilePicker` (see `FilePicker` in `db/version_set.cc`) drives the level-by-level search through SST files. It iterates from L0 through the highest non-empty level, selecting candidate files for each level.

### L0 Search Strategy

L0 files have overlapping key ranges (unlike L1+ files), so all L0 files must be checked:

- Files are sorted by `epoch_number` in descending order (newest first)
- `epoch_number` tracks recency/ordering for flushed, ingested, and imported files. Compaction output files inherit the minimum `epoch_number` from their inputs. For L0, larger `epoch_number` indicates a newer file
- For each file, a simple range check (`user_key >= smallest_user_key && user_key <= largest_user_key`) determines if the key might be in the file
- All matching files are checked because key ranges overlap

**Key Invariant:** L0 files must be checked newest-first (by epoch_number) to find the most recent value first.

### L1+ Search Strategy

Files in L1 and higher levels have non-overlapping key ranges, maintained by compaction. This enables binary search:

Step 1: Call `FindFileInRange()` which uses `std::lower_bound` on the level's file list, comparing the lookup key against each file's `largest_key`

Step 2: The result is the first file whose `largest_key >= lookup_key`

Step 3: If no such file exists, skip this level entirely

Step 4: At most one file per level needs to be checked (with the exception noted below)

**Note:** Adjacent L1+ files may share the same user key at file boundaries (e.g., due to merge operands or snapshots preventing compaction from combining them). In this case, a point lookup may need to check subsequent files in the same level.

### FileIndexer Optimization

`FileIndexer` (see `FileIndexer` in `db/file_indexer.h`) narrows the binary search range for L1+ levels. After finding a file in level N, it uses the result to constrain the search bounds in level N+1, reducing the binary search space.

## TableCache::Get()

`TableCache::Get()` handles the per-file lookup:

Step 1: **Find table reader** -- `FindTable()` checks if the table reader is already cached (pinned in memory or in the table cache). If not, opens the SST file and creates a new `BlockBasedTableReader`.

Step 2: **Call table reader Get** -- `BlockBasedTable::Get()` performs the in-file search.

### BlockBasedTable::Get() Flow

Step 1: **Bloom filter check** -- Unless `IsFilterSkipped()` returns true, check the bloom/ribbon filter. If the filter says the key is definitely not in this file, return immediately. The `optimize_filters_for_hits` option (see `ColumnFamilyOptions` in `include/rocksdb/advanced_options.h`) skips bloom filters at the bottommost level since any key reaching the bottom level must exist.

Step 2: **Index block seek** -- Use the index block to locate the data block containing the key. For partitioned indexes, this may involve a two-level lookup (top-level index to partition, then partition to data block).

Step 3: **Retrieve data block** -- Call `RetrieveBlock()` to get the data block from cache or disk. See [Block Cache Integration](06_block_cache.md).

Step 4: **Binary search within block** -- Search the data block for the key using binary search on restart points, then linear search within the restart interval.

Step 5: **GetContext::SaveValue()** -- For each matching entry, invoke the callback which handles type dispatch similar to memtable `SaveValue()`.

## GetContext State Machine

`GetContext` (see `GetContext` in `table/get_context.h`) mediates between the table reader and the caller. It maintains a state that evolves as entries are found:

| State | Meaning |
|-------|---------|
| `kNotFound` | No matching entry found yet |
| `kFound` | Final value found (Put, Entity, or resolved Merge) |
| `kDeleted` | Key was deleted |
| `kMerge` | Merge operand(s) collected, need more data or final resolution |
| `kCorrupt` | Corrupted entry encountered |
| `kUnexpectedBlobIndex` | BlobIndex found but BlobDB not enabled |
| `kMergeOperatorFailed` | Merge operator returned failure |

### GetContext::SaveValue()

When a matching key is found in an SST data block:

1. Check `max_covering_tombstone_seq` -- if a range tombstone with higher sequence covers this key, treat it as deleted
2. Dispatch on value type:
   - `kTypeValue` / `kTypeValuePreferredSeqno`: If state was `kNotFound`, store value and transition to `kFound`. If state was `kMerge`, use as merge base value and call `TimedFullMerge()`.
   - `kTypeBlobIndex`: If state was `kNotFound` with `do_merge=true`, store the raw blob index bytes and set `is_blob_index = true` (lazy resolution). If state was `kMerge`, eagerly fetch blob via `GetBlobValue()` and merge.
   - `kTypeWideColumnEntity`: If caller wants plain value, extract default column via `WideColumnSerialization::GetValueOfDefaultColumn()`. If caller wants full columns, deserialize the entity.
   - `kTypeMerge`: Push operand to `MergeContext`, remain in `kMerge` state, return true to continue searching.
   - `kTypeDeletion` / `kTypeSingleDeletion`: Transition to `kDeleted`, or if in `kMerge` state, resolve merge with no base value.

## Version::Get() Result Processing

After `TableCache::Get()` returns, `Version::Get()` processes the `GetContext` state:

| State | Action |
|-------|--------|
| `kNotFound` | Continue to next file via `FilePicker::GetNextFile()` |
| `kMerge` | Continue searching to collect more operands or find base value |
| `kFound` | If blob index, fetch blob via `GetBlob()`. Record hit-level statistics. Return. |
| `kDeleted` | Return `Status::NotFound()` |
| `kCorrupt` | Return `Status::Corruption()` |
| `kUnexpectedBlobIndex` | Return `Status::NotSupported()` (BlobDB not enabled but blob index encountered) |
| `kMergeOperatorFailed` | Return `Status::Corruption()` with merge operator failure details |

If all files are exhausted with `kMerge` state, call `MergeHelper::TimedFullMerge()` with no base value to resolve the merge.

## Bloom/Ribbon Filter Integration

### Filter Architecture

Filters are loaded via a class hierarchy:

| Class | Role |
|-------|------|
| `FilterBlockReader` (abstract) | Base interface: `KeyMayMatch()`, `PrefixMayMatch()`, `RangeMayExist()` |
| `FullFilterBlockReader` | Full (non-partitioned) filter reader, wraps `ParsedFullFilterBlock` |
| `PartitionedFilterBlockReader` | Two-level partitioned filter: top-level index + partition filter blocks |
| `FilterBitsReader` | Low-level bloom/ribbon bit probing, obtained from `FilterPolicy::GetFilterBitsReader()` |

### Filter Loading

At table open time (`PrefetchIndexAndFilterBlocks` in `block_based_table_reader.cc`):
1. The filter type (`kFullFilter` or `kPartitionedFilter`) is discovered from the metaindex block
2. `CreateFilterBlockReader()` dispatches to the appropriate reader's `Create()` method
3. The filter block is loaded via `RetrieveBlock()` (cache lookup or disk read)
4. If `pin_filter` is true, the filter block is pinned in cache (never evicted)

At query time, `GetOrReadFilterBlock()` returns the pinned filter directly or goes through the cache/disk path.

### Full Filter: Single-Key Check

In `BlockBasedTable::Get()`, the filter check flow is:

Step 1: Check `skip_filters` flag (set by `IsFilterSkipped()`)

Step 2: If not skipping, call `FullFilterKeyMayMatch()`:
- If `whole_key_filtering` is true: `filter->KeyMayMatch(user_key_without_ts)` probes the filter with the full user key
- Else if prefix extractor matches: `filter->PrefixMayMatch(prefix)` probes with the extracted prefix
- If neither applies: returns true (no filtering possible)

Step 3: `FullFilterBlockReader::MayMatch()` calls `filter_bits_reader->MayMatch(entry)` -- the actual bloom/ribbon probe

Step 4: On filter miss (key definitely not in file), return immediately without reading any data block

### Partitioned Filter: Two-Level Lookup

`PartitionedFilterBlockReader` performs a two-level lookup:

Step 1: Load the **top-level index block** (keyed by internal key, pointing to partition `BlockHandle`s)

Step 2: `GetFilterPartitionHandle()` binary-searches the top-level index to find which partition contains the key

Step 3: `GetFilterPartitionBlock()` loads the partition: first checks `filter_map_` (pinned partitions), then falls back to cache/disk

Step 4: Creates a temporary `FullFilterBlockReader` on the partition and delegates to it

For MultiGet, adjacent keys mapping to the same partition are grouped and checked in a single batch probe.

### Whole-Key vs Prefix Filtering

| Mode | Build-time | Query-time |
|------|-----------|------------|
| `whole_key_filtering=true` | Key AND prefix added to filter | `KeyMayMatch(user_key)` probes with full key |
| `whole_key_filtering=false` | Only prefix added to filter | `PrefixMayMatch(prefix)` probes with prefix |

For iterators, `RangeMayExist()` always operates on prefixes, checking if the prefix extractor is compatible with the upper bound before calling `PrefixMayMatch()`.

## Filter Skip Logic

`IsFilterSkipped()` in `Version::Get()` (see `Version::IsFilterSkipped()` in `db/version_set.cc`) decides whether to skip bloom filter checks. All three conditions must hold:

1. `optimize_filters_for_hits` (see `ColumnFamilyOptions` in `include/rocksdb/advanced_options.h`) is enabled
2. The file is at the bottommost non-empty level (the lowest level that currently contains files, not necessarily the absolute bottom level of the LSM tree)
3. For L0 files, only if it's the last file in L0 (since L0 files overlap)

Rationale: at the bottommost level, a key that wasn't found in upper levels is likely present, so the filter check is wasted CPU. This saves one filter probe and potentially one block cache lookup per bottommost-level read.

## InternalKey Ordering

The internal key format determines iteration and search order:

Format: `user_key | [timestamp] | (sequence << 8 | type)`

Comparison rules:
1. Compare `user_key` ascending (using the user-supplied comparator, which includes timestamp if user-defined timestamps are enabled)
2. Compare the packed `(sequence << 8 | type)` as `uint64_t` descending -- so newer entries (higher sequence) come first

This ordering ensures that a forward scan through a sorted structure encounters the newest version of each user key first.

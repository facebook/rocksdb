# Point Lookup (Get)

**Files:** `db/db_impl/db_impl.cc`, `db/db_impl/db_impl.h`, `include/rocksdb/db.h`, `include/rocksdb/options.h`

## Entry Point

The primary point lookup API is `DB::Get()`, which routes through `DBImpl::GetImpl()`. This function supports reading values, wide-column entities, and merge operands depending on `GetImplOptions` configuration.

## End-to-End Flow

Step 1: **Timestamp validation** -- If `ReadOptions::timestamp` is set, validate that it matches the column family's timestamp size via `FailIfTsMismatchCf()`. If timestamps are enabled but not provided, `FailIfCfHasTs()` rejects the call.

Step 2: **Acquire SuperVersion** -- Call `GetAndRefSuperVersion(cfd)` to get a consistent snapshot of the current memtable, immutable memtables, and SST file set. See [SuperVersion and Snapshots](03_superversion_and_snapshots.md) for details.

Step 2a: **Timestamp history check** -- If user-defined timestamps are enabled and `ReadOptions::timestamp` is set, call `FailIfReadCollapsedHistory()` to verify the requested timestamp is not older than `full_history_ts_low` in the SuperVersion. If history has been collapsed past this timestamp, the read is rejected with `Status::InvalidArgument()` before any data lookup occurs.

Step 2b: **Timestamp ReadCallback installation** -- When user-defined timestamps are enabled (comparator `timestamp_size() > 0`), `GetImpl()` installs a `GetWithTimestampReadCallback` that filters results by both sequence number AND timestamp visibility. This temporarily replaces any user-provided callback via a `SaveAndRestore` RAII pattern. Note: combining UDT with an existing `ReadCallback` (e.g., from transactions) is not supported.

Step 3: **Determine snapshot sequence** -- If `ReadOptions::snapshot` is non-null, use the snapshot's sequence number. Otherwise, call `GetLastPublishedSequence()` to get the latest visible sequence. The snapshot is assigned AFTER SuperVersion acquisition to prevent a concurrent flush from compacting away data between the two steps.

Step 4: **Construct LookupKey** -- Build a `LookupKey` from the user key, snapshot sequence, and optional timestamp. The `LookupKey` format is: `klength (varint32) | userkey (includes timestamp bytes if UDT enabled) | tag (sequence << 8 | type, 8 bytes)`. When UDT is enabled, timestamp bytes are appended to the user key bytes before the tag; the `user_key()` accessor returns both the original key and timestamp together as a single slice.

Step 5: **Search mutable memtable** -- Call `sv->mem->Get(lkey, ...)`. If found, the search is complete (MEMTABLE_HIT). See [MemTable Lookup](04_memtable_lookup.md).

Step 6: **Search immutable memtables** -- If not found, call `sv->imm->Get(lkey, ...)`. Immutable memtables are searched newest to oldest.

Step 7: **Search SST files** -- If still not found, call `sv->current->Get(...)` which searches SST files level by level via `FilePicker`. See [SST File Lookup](05_sst_file_lookup.md). Within each file, `TableCache::Get()` checks the row cache first (if `DBOptions::row_cache` is configured) before the normal table reader/block cache path. The row cache stores serialized `GetContext` replay logs keyed by `(row_cache_id, file_number, seq_no, user_key)`. On a row cache hit, the recorded log is replayed into the caller's `GetContext` to reconstruct the result.

Step 8: **Post-processing** -- Record statistics (`RecordTick`, `RecordInHistogram`). If merge operands exceeded `ReadOptions::merge_operand_count_threshold`, return a special OK status with `kMergeOperandThresholdExceeded` subcode. Blob retrieval for BlobDB values happens inside `Version::Get()` when `kFound` state is reached, not in post-processing.

Step 9: **Release SuperVersion** -- Call `ReturnAndCleanupSuperVersion(cfd, sv)`. If this was the last reference to an obsolete SuperVersion, cleanup triggers.

## GetImplOptions

`GetImplOptions` (see `DBImpl` in `db/db_impl/db_impl.h`) controls the behavior of `GetImpl`:

| Field | Purpose |
|-------|---------|
| `column_family` | Target column family handle |
| `value` | Output PinnableSlice for the value |
| `columns` | Output PinnableWideColumns for entity reads |
| `timestamp` | Output string for the key's timestamp |
| `value_found` | Output bool pointer indicating whether a value was found (used by `KeyMayExist()`) |
| `merge_operands` | Output array for `GetMergeOperands()` API |
| `callback` | ReadCallback for transaction visibility checks |
| `is_blob_index` | Output flag indicating a BlobDB index was found |
| `get_value` | Controls whether Get resolves the value (true, default) or returns merge operands (false) |
| `get_merge_operands_options` | If set, retrieves individual merge operands instead of resolving them |
| `number_of_operands` | Output int pointer for the number of merge operands found |

## GetEntity and Wide-Column Reads

`DB::GetEntity()` retrieves a key's value as a `PinnableWideColumns` structure containing named columns. Internally it calls the same `GetImpl()` with `get_impl_options.columns` set instead of `get_impl_options.value`. The routing between `Get()` and `GetEntity()` propagates through to `GetContext`:

- **Get() path** (`pinnable_val_` set): When `kTypeWideColumnEntity` is encountered, only the default (anonymous) column value is extracted via `WideColumnSerialization::GetValueOfDefaultColumn()`. The caller receives a plain `PinnableSlice`.
- **GetEntity() path** (`columns_` set): The full wide-column payload is deserialized into `PinnableWideColumns` containing all named columns.

The V2 wide-column format (see `WideColumnSerialization` in `db/wide/wide_column_serialization.h`) supports individual columns being blob references. `ResolveEntityBlobColumns()` handles fetching all blob columns and re-serializing as V1 format.

## Blob Value Retrieval

When integrated BlobDB is enabled, values exceeding `min_blob_size` (see `AdvancedColumnFamilyOptions` in `include/rocksdb/advanced_options.h`) are stored in separate blob files. Blob resolution is a two-phase process:

**Phase 1 -- During SST scan (lazy):** When `GetContext::SaveValue()` encounters `kTypeBlobIndex` in the common case (`kNotFound` state), it stores the raw blob index bytes in `pinnable_val_` and sets `is_blob_index = true`. The blob is NOT fetched yet. However, in the merge case (`kMerge` state), the blob IS fetched eagerly via `GetContext::GetBlobValue()` because the merge operator needs the actual value as a base.

**Phase 2 -- Post-scan resolution:** After the SST scan completes with `kFound` state, `Version::Get()` checks `is_blob_index`. If true, it calls `Version::GetBlob()` which decodes the `BlobIndex` (containing file number, offset, size, and compression type), validates the blob file exists, and calls `BlobSource::GetBlob()`. The blob source checks the blob cache first, and on miss reads from the blob file.

This lazy two-phase design avoids reading blob data for keys that would be filtered out by tombstones or superseded by newer versions.

**GetMergeOperands path:** When `do_merge = false` (for the `GetMergeOperands` API), blobs ARE fetched eagerly in `SaveValue()` because the caller needs actual values, not blob indices.

## Statistics and Tracing

Get operations record several statistics:

| Statistic | Meaning |
|-----------|---------|
| `GET_HIT_L0` | Value found in L0 SST file |
| `GET_HIT_L1` | Value found in L1 SST file |
| `GET_HIT_L2_AND_UP` | Value found in L2+ SST file |
| `MEMTABLE_HIT` | Value found in memtable (mutable or immutable) |
| `MEMTABLE_MISS` | Value not in any memtable, searched SST files |
| `DB_GET` (histogram) | Latency distribution of Get operations |

When a `Tracer` is configured, Get operations are recorded for replay via `tracer_->Get()`.

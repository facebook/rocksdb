# Compaction

## Overview

Compaction is the background process that merges SST files across levels to reclaim space, reduce read amplification, and enforce size targets. RocksDB supports three compaction styles (Level, Universal, FIFO), each with its own file selection strategy. The execution pipeline is shared: `CompactionPicker` selects files, `CompactionJob` executes the merge using `CompactionIterator`, and the result is installed as a new Version.

### Compaction Data Flow

```
CompactionPicker::PickCompaction()
    |
    v
Compaction (metadata: input files, levels, options)
    |
    v
CompactionJob::Prepare()       -- divide into SubcompactionStates
    |
    v
CompactionJob::Run()           -- per subcompaction:
    |   CompactionMergingIterator -- heap-merge sorted input files + range tombstone start keys
    |       |
    |       v
    |   CompactionIterator      -- dedup, delete, merge, filter, blob GC
    |       |
    |       v
    |   SubcompactionState::AddToOutput()
    |       |
    |       v
    |   CompactionOutputs       -- manage output files, split decisions
    |
    v
CompactionJob::Install()       -- VersionEdit -> LogAndApply
```

---

## 1. CompactionPicker

**Files:** `db/compaction/compaction_picker.h`, `db/compaction/compaction_picker_level.h`, `db/compaction/compaction_picker_universal.h`, `db/compaction/compaction_picker_fifo.h`

### What It Does

CompactionPicker is the strategy interface for selecting which files to compact. Each compaction style implements two pure virtual methods:

| Method | Description |
|--------|-------------|
| `PickCompaction()` | Examines `VersionStorageInfo`, returns a `Compaction*` describing what to compact (or nullptr) |
| `NeedsCompaction()` | Fast check: is there anything worth compacting? |

Additional methods:
- `PickCompactionForCompactRange()` -- for manual `DB::CompactRange()` calls
- `PickCompactionForCompactFiles()` -- for `DB::CompactFiles()` with pre-specified files
- `ExpandInputsToCleanCut()` -- extends input files so no key version boundary is split across levels
- `SetupOtherInputs()` -- expands output-level inputs and optionally re-expands start-level inputs

### LevelCompactionPicker

Standard leveled compaction. Selects the level with the highest compaction score (from `VersionStorageInfo::CompactionScore()`), picks files from that level and overlapping files from the next level.

**Score calculation** (in `VersionStorageInfo::ComputeCompactionScore()`):
- L0: `num_files / l0_compaction_trigger` (also considers L0 size vs base level size for leveled compaction)
- L1+: `level_bytes / target_bytes` (with dynamic-level-bytes scaling when enabled)
- Additional triggers: TTL-based compaction, temperature-change compaction, blob-aware FIFO sizing, universal-specific handling

Supports round-robin file selection via compact cursors.

### UniversalCompactionPicker

Size-tiered compaction. Triggers based on:
- **Size ratio**: Adjacent sorted runs exceed `size_ratio` threshold
- **Space amplification**: Total size / last level size exceeds `max_size_amplification_percent`
- **File count**: Number of sorted runs exceeds `level0_file_num_compaction_trigger`
- **Periodic compaction**: Files older than `periodic_compaction_seconds`
- **Delete-triggered / marked-for-compaction**: Files flagged for compaction by other subsystems

Uses `require_max_output_level` flag via `MeetsOutputLevelRequirements()` to force output to the last level. Applied generically (not only for deletion-triggered compaction), including for bottom-priority background compactions.

### FIFOCompactionPicker

Oldest-first deletion. Primarily targets L0, but during migration from level/universal compaction to FIFO, `PickSizeCompaction()` can select the last non-empty level (which may be non-L0) and build a deletion compaction there. Strategies:

| Strategy | Description |
|----------|-------------|
| TTL compaction | Delete files older than TTL threshold |
| Size compaction | Delete oldest files when total size exceeds `max_table_files_size` |
| Intra-L0 compaction (cost-based) | Merge small L0 files to reduce file count |
| Intra-L0 compaction (kv-ratio) | `PickRatioBasedIntraL0Compaction()` controlled by `compaction_options_fifo.use_kv_ratio_compaction` |
| Temperature change | Move files between temperature tiers |

### NullCompactionPicker

No-op picker used when compaction is disabled. All methods return nullptr/false.

### Tracking Running Compactions

`RegisterCompaction()` / `UnregisterCompaction()` track running compactions to avoid conflicts:
- `level0_compactions_in_progress_` -- ordered set for L0 compactions and all universal compactions (regardless of start level)
- `compactions_in_progress_` -- unordered set for all compactions

---

## 2. Compaction

**Files:** `db/compaction/compaction.h`, `db/compaction/compaction.cc`

### What It Does

A `Compaction` object is a metadata container describing one compaction operation. Created by `CompactionPicker`, consumed by `CompactionJob`.

### Key Fields

| Field | Type | Description |
|-------|------|-------------|
| `inputs_` | `vector<CompactionInputFiles>` | Input files organized by level |
| `start_level_` | `int` | Lowest level being compacted |
| `output_level_` | `int` | Target output level |
| `target_output_file_size_` | `uint64_t` | Target size per output file |
| `max_compaction_bytes_` | `uint64_t` | Max total input bytes |
| `grandparents_` | `vector<FileMetaData*>` | Files at output_level+1 for output splitting |
| `bottommost_level_` | `bool` | No data in the compaction key range exists after the output sorted run (checked via `RangeMightExistAfterSortedRun()`) |
| `deletion_compaction_` | `bool` | Just delete input files, no merge |
| `proximal_level_` | `int` | For per-key-placement: `last_level - 1` |
| `compaction_reason_` | `CompactionReason` | Why this compaction was triggered |

### CompactionInputFiles

```cpp
struct CompactionInputFiles {
    int level;
    std::vector<FileMetaData*> files;
    std::vector<AtomicCompactionUnitBoundary> atomic_compaction_unit_boundaries;
};
```

`AtomicCompactionUnitBoundary` spans neighboring SSTs on the same level whose user keys overlap at boundaries. Used for correct range tombstone truncation.

### Key Methods

| Method | Description |
|--------|-------------|
| `IsTrivialMove()` | True if compaction can move files without merging (may include multiple files in leveled/universal) |
| `ShouldFormSubcompactions()` | Whether to parallelize into subcompactions |
| `KeyNotExistsBeyondOutputLevel()` | Can sequence numbers be zeroed for this key? |
| `AddInputDeletions()` | Adds all input files as deletes to a VersionEdit |
| `SupportsPerKeyPlacement()` | Whether per-key-placement is active |
| `FilterInputsForCompactionIterator()` | Removes files fully covered by a standalone range deletion file (only applies when `earliest_snapshot_` exists, user-defined timestamps are disabled, and the start-level candidate is a standalone range tombstone whose seqno is in a snapshot) |

### Per-Key Placement

When enabled (`SupportsPerKeyPlacement() == true`), compaction can output keys to two different levels:
- **Output level** (last level): cold data
- **Proximal level** (`last_level - 1`): recent data

Enablement requires: the compaction `output_level` is the last level, `preclude_last_level_data_seconds > 0`, a valid proximal level, and a computed proximal output key range. A `keep_in_last_level_through_seqno_` guard prevents data from being moved up prematurely.

The per-key placement decision happens in `CompactionJob::ProcessKeyValue()` via `ikey.sequence > proximal_after_seqno_`, where the threshold is computed in `CompactionJob::Prepare()`. `CompactionIterator::PrepareOutput()` handles blob extraction/GC and sequence number zeroing but not placement.

---

## 3. CompactionJob

**Files:** `db/compaction/compaction_job.h`, `db/compaction/compaction_job.cc`

### What It Does

Executes a compaction. Three-phase lifecycle:

| Phase | Lock | Description |
|-------|------|-------------|
| `Prepare()` | Mutex held | Generate subcompaction boundaries, set up seqno-to-time mapping |
| `Run()` | Mutex NOT held | Execute subcompactions in parallel, verify outputs |
| `Install()` | Mutex held | Apply VersionEdit via `LogAndApply()` |

### Subcompaction Boundary Generation

`GenSubcompactionBoundaries()`:
1. Collects boundary keys from input file boundaries
2. Estimates data size between consecutive boundary keys
3. Groups ranges into subcompactions of similar total size
4. Target: `max_subcompactions` parallel workers

### Core Processing Loop

`ProcessKeyValueCompaction()` (per subcompaction):
1. Create input iterator (`CompactionMergingIterator` via `VersionSet::MakeInputIterator()`, which has compaction-specific behavior: emits range tombstone start keys, skips file-boundary sentinel keys)
2. Wrap in `CompactionIterator` (handles dedup, deletion, merge, filter)
3. For each output key: determine placement via `proximal_after_seqno_`, then `SubcompactionState::AddToOutput()` -> `CompactionOutputs::AddToOutput()`
4. Split output files based on size, grandparent overlap, partitioner

### Output File Lifecycle

| Method | Description |
|--------|-------------|
| `OpenCompactionOutputFile()` | Create new output SST file + TableBuilder |
| `FinishCompactionOutputFile()` | Close current output, add range deletions, verify checksums |
| `InstallCompactionResults()` | Add all outputs to VersionEdit, delete all inputs |

### Compaction Resumption

Compaction resumption is currently narrowly scoped to secondary/remote compaction flows:
- `Prepare()` accepts `CompactionProgress` from a previous run
- `MaybeResumeSubcompactionProgressOnInputIterator()` seeks past completed work
- Progress is persisted to a separate compaction-progress log (not the MANIFEST) via `CompactionJob::PersistSubcompactionProgress()`, which encodes a `VersionEdit` and writes it through `compaction_progress_writer_->AddRecord()`
- Regular background compactions call `Prepare(std::nullopt)` with no progress writer
- Persistence is limited to a single subcompaction and disabled for timestamps, file-boundary/range-delete cases, adjacent output tables sharing a user key, and some lookahead states

### Remote Compaction

`CompactionServiceCompactionJob` (line 694) enables remote compaction execution:
- `CompactionServiceInput` -- serializable input specification
- `CompactionServiceResult` -- serializable output with file metadata

---

## 4. CompactionIterator

**Files:** `db/compaction/compaction_iterator.h`, `db/compaction/compaction_iterator.cc`

### What It Does

The core merging/deduplication engine. Transforms a merged sorted stream of input keys into compacted output by applying:

1. **Deduplication**: For same user key, keeps only versions visible at snapshot boundaries
2. **Deletion handling**: Drops tombstones when safe (no snapshot needs them, key doesn't exist below)
3. **SingleDelete processing**: Pairs SingleDelete with corresponding Put, drops both when safe
4. **Merge operator**: Accumulates merge operands via `MergeHelper`, produces merged values
5. **Range deletion**: Interleaves range tombstone sentinel keys
6. **Blob GC**: Relocates blob values from old blob files
7. **Compaction filter**: Invokes user-defined filter to drop/modify keys
8. **Timestamp GC**: Drops old timestamp versions when `full_history_ts_low` is set
9. **Sequence number zeroing**: At bottommost level, zeros seqnums when no snapshot needs them

### Key State

| Field | Purpose |
|-------|---------|
| `has_current_user_key_` | Tracking whether we're processing the same user key |
| `current_user_key_sequence_` | Seqnum of current user key's first occurrence |
| `current_user_key_snapshot_` | Earliest snapshot this key is visible in |
| `has_outputted_key_` | Already emitted a record for this user key |
| `at_next_` | Already advanced past current during lookahead |

### Core Methods

| Method | Description |
|--------|-------------|
| `NextFromInput()` | Main state machine: reads from input, handles dedup/deletion/merge/SingleDelete logic |
| `PrepareOutput()` | Final preparation: blob extraction, seqnum zeroing |
| `InvokeFilterIfNeeded()` | Calls CompactionFilter (kRemove/kKeep/kChangeValue/kRemoveAndSkipUntil/kPurge/kChangeBlobIndex/kIOError/kChangeWideColumnEntity) |
| `findEarliestVisibleSnapshot()` | Scans snapshot list to find visibility boundary |

### Snapshot Interaction

CompactionIterator preserves key versions that are visible to any live snapshot. For each user key, it keeps:
- The latest version (always)
- One version per snapshot boundary where the key is the newest visible version
- Merge operands that haven't been fully merged due to snapshot boundaries

---

## 5. CompactionOutputs

**Files:** `db/compaction/compaction_outputs.h`, `db/compaction/compaction_outputs.cc`

### What It Does

Manages the output SST files produced by a subcompaction. Each subcompaction has two instances: one for the normal output level, one for the proximal level.

### Output File Splitting

`ShouldStopBefore()` decides when to close the current output file and start a new one:

| Criterion | Description |
|-----------|-------------|
| Target file size | `estimated_file_size >= max_output_file_size()` |
| TTL-based cutting | `files_to_cut_for_ttl_` isolates ranges for TTL merge-down |
| SST partitioner | User-provided `SstPartitioner` callback |
| Round-robin split key | `local_output_split_key_` from compaction |
| Grandparent + max_compaction_bytes | `grandparent_overlapped_bytes_ + current_output_file_size_ > max_compaction_bytes()` |
| Grandparent skippable boundary | For leveled: cuts when crossing grandparent boundaries that would create a skippable file > 1/8 target size |
| Grandparent pre-cut heuristic | For leveled: dynamic threshold (50-90% of target size) based on number of grandparent boundaries seen |

### Grandparent Overlap Tracking

Tracks overlap with files at `output_level + 1` to prevent read amplification in future compactions:
- `grandparent_index_` -- current position in grandparents vector
- `grandparent_overlapped_bytes_` -- accumulated overlap
- `grandparent_boundary_switched_num_` -- number of boundary crossings

File cuts at grandparent boundaries use `max_compaction_bytes()` and adaptive heuristics (see `ShouldStopBefore()` above) rather than a fixed overlap threshold.

### Key Methods

| Method | Description |
|--------|-------------|
| `AddToOutput()` | Add a key to current output file, opening/closing files as needed |
| `Finish()` | Finalize current output (builder->Finish, seqno-to-time mapping) |
| `AddRangeDels()` | Add range deletions respecting boundaries and snapshots |
| `CloseOutput()` | Close builder, handle range-deletion-only output edge case |

---

## 6. CompactionMergingIterator

**Files:** `table/compaction_merging_iterator.h`, `table/compaction_merging_iterator.cc`

### What It Does

A specialized merging iterator used specifically for compaction, created via `NewCompactionMergingIterator()` through `VersionSet::MakeInputIterator()`. Unlike the general `MergingIterator`, it has compaction-specific behavior:

- **Emits range tombstone start keys**: For each range tombstone `[start,end)@seqno`, emits `start@seqno` with op_type `kTypeRangeDeletion` to prevent oversize compactions from overlapping wide key ranges
- **Skips file-boundary sentinel keys**: Filters out internal sentinel keys used for file boundary tracking
- **`IsDeleteRangeSentinelKey()`**: Caller uses this to check if the current key is a range tombstone start key

### MergingIterator (for reads)

**Files:** `table/merging_iterator.h`, `table/merging_iterator.cc`

The general-purpose merging iterator provides the sorted union of N child iterators using a min-heap. Used as the basis for `DBIter` (reads) but **not** for compaction (which uses `CompactionMergingIterator` above).

### Creation

```cpp
InternalIterator* NewMergingIterator(
    const InternalKeyComparator* comparator,
    InternalIterator** children, int n,
    Arena* arena = nullptr,
    bool prefix_seek_mode = false);
```

**No duplicate suppression**: if a key appears in K children, it is yielded K times. Deduplication is handled by `CompactionIterator` (for compaction) or `DBIter` (for reads).

### MergeIteratorBuilder

Builder pattern for constructing merging iterators with range tombstone support:

| Method | Description |
|--------|-------------|
| `AddIterator(iter)` | Add point-key-only iterator |
| `AddPointAndTombstoneIterator(point, tombstone, ptr)` | Add point + range tombstone iterator pair |
| `Finish()` | Return final iterator. Optimizes single-iterator case (no heap needed). |

Range tombstone iterators are used to interleave range deletion sentinel keys into the merge stream, enabling correct range deletion processing in `CompactionIterator` and `DBIter`.

---

## 7. SubcompactionState

**Files:** `db/compaction/subcompaction_state.h`

### What It Does

Maintains all state for a single sub-compaction. Compaction work is divided by key range into non-overlapping `[start, end)` subcompactions that execute in parallel.

### Structure

Each `SubcompactionState` contains two `CompactionOutputs`:
- `compaction_outputs_` -- normal output level
- `proximal_level_outputs_` -- proximal level (per-key-placement)
- `current_outputs_` -- pointer switching between the two based on current key's placement

### Key Fields

| Field | Description |
|-------|-------------|
| `compaction` | Parent `Compaction*` |
| `start` / `end` | Key range boundaries (`const std::optional<Slice>`, `std::nullopt` = unbounded) |
| `status` / `io_status` | Result status |
| `compaction_job_stats` | Per-subcompaction stats |
| `range_del_agg_` | `CompactionRangeDelAggregator`, shared between both output groups |

### Key Methods

| Method | Description |
|--------|-------------|
| `AddToOutput()` | Routes key to correct output group based on placement decision |
| `CloseCompactionFiles()` | Closes both output groups (proximal first, then normal) |
| `AddOutputsEdit()` | Adds all output files to VersionEdit at correct levels |

---

## Key Invariants

| Invariant | Details |
|-----------|---------|
| Input files locked during compaction | `FileMetaData::being_compacted = true` prevents concurrent compaction of same files |
| Atomic compaction unit boundaries respected | Range tombstones are not truncated at SST boundaries that share user keys |
| Snapshot versions preserved | CompactionIterator keeps key versions visible to any live snapshot |
| Output level >= start level (with exceptions) | Compaction generally outputs to the same or lower level, but per-key placement can route keys to the proximal level (`last_level - 1`) |
| Grandparent overlap bounded | Output file splitting prevents unbounded read amplification at grandparent level |
| Trivial move is atomic | File move(s) require no merge; just updates file metadata in VersionEdit. May include multiple files in leveled and universal compaction. |
| Subcompaction ranges are disjoint | Parallel subcompactions have non-overlapping key ranges |

## Interactions With Other Components

- **Version Management** (see [version_management.md](version_management.md)): Compaction reads from a `Version`, produces `VersionEdit`s, installs new Version via `LogAndApply()`.
- **SST Format** (see [sst_table_format.md](sst_table_format.md)): `CompactionJob` reads SST files via `TableReader` iterators and writes new ones via the configured `table_factory` (dispatched through `NewTableBuilder()`).
- **Write Flow** (see [write_flow.md](write_flow.md)): Write stalls occur when compaction falls behind. `WriteController` throttles writes based on compaction pressure.
- **Flush** (see [flush.md](flush.md)): Flush produces L0 files that trigger compaction. Compaction scores are recomputed after each flush.
- **Cache** (see [cache.md](cache.md)): Compaction may bypass block cache (`fill_cache=false`) to avoid polluting the cache with cold data.

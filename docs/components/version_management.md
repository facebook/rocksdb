# Version Management

## Overview

Version management is the metadata backbone of RocksDB. It tracks which SST files and blob files exist at each level, records metadata changes (flushes, compactions, column family operations) durably to the MANIFEST file, and provides point-in-time consistent views of the database to concurrent readers through the SuperVersion mechanism.

### Key Components

| Component | File(s) | Role |
|-----------|---------|------|
| VersionEdit | `db/version_edit.h` | Single metadata change record (add/delete files, update sequence numbers) |
| VersionSet | `db/version_set.h` | Manages all Versions across all column families; owns MANIFEST |
| Version | `db/version_set.h` | Point-in-time snapshot of SST/blob files for one column family |
| VersionStorageInfo | `db/version_set.h` | Per-level file lists, compaction scores, file metadata within a Version |
| VersionBuilder | `db/version_builder.h` | Accumulates VersionEdits and produces a new Version |
| MANIFEST | `db/log_format.h` (reuses WAL log format) | On-disk log of VersionEdit records |
| ColumnFamilyData | `db/column_family.h` | Per-CF state: memtables, versions, options, compaction picker |
| SuperVersion | `db/column_family.h` | Ref-counted bundle of MemTable + ImmutableMemTables + Version |

### How It Fits Together

```
Flush/Compaction completes
    |
    v
Create VersionEdit (add new files, delete old files)
    |
    v
VersionSet::LogAndApply()
    |
    +---> Write VersionEdit to MANIFEST (durable)
    +---> VersionBuilder::Apply(edit) + SaveTo(new_vstorage)
    +---> Install new Version as current
    +---> Install new SuperVersion (readers see new state)
```

---

## 1. VersionEdit

**Files:** `db/version_edit.h`, `db/version_edit.cc`

### What It Does

A VersionEdit represents a single atomic metadata change. It is the unit record written to the MANIFEST file. Every flush, compaction, column family add/drop, or WAL lifecycle event produces one or more VersionEdits.

### Content

A VersionEdit can contain any combination of:

| Field | Purpose |
|-------|---------|
| `new_files_` | SST files added (level + FileMetaData) |
| `deleted_files_` | SST files removed (level + file number) |
| `blob_file_additions_` | New blob files |
| `blob_file_garbages_` | Garbage tracked in existing blob files |
| `wal_additions_` / `wal_deletion_` | WAL lifecycle tracking |
| `compact_cursors_` | Round-robin compaction cursor positions |
| `log_number_` | Minimum WAL number needed for this CF |
| `next_file_number_` | Next available file number |
| `last_sequence_` | Latest sequence number |
| `column_family_` | Which CF this edit applies to (default = 0) |
| `is_column_family_add_` / `is_column_family_drop_` | CF lifecycle |

### FileMetaData

Each SST file is described by `FileMetaData` (`db/version_edit.h:244`):

| Field | Type | Description |
|-------|------|-------------|
| `fd` | `FileDescriptor` | File number, path ID, file size, smallest/largest sequence numbers |
| `smallest` / `largest` | `InternalKey` | Key range boundaries |
| `num_entries` | `uint64_t` | Total entries including deletions |
| `num_deletions` | `uint64_t` | Deletion entries count |
| `compensated_file_size` | `uint64_t` | Size adjusted for deletions (compaction priority) |
| `compensated_range_deletion_size` | `uint64_t` | Estimated impact of range tombstones on next level |
| `being_compacted` | `bool` | Currently involved in a compaction |
| `marked_for_compaction` | `bool` | Client-requested compaction target |
| `epoch_number` | `uint64_t` | Ordering: larger = newer for L0 files |
| `oldest_blob_file_number` | `uint64_t` | Oldest referenced blob file |
| `temperature` | `Temperature` | Hot/warm/cold storage hint |
| `unique_id` | `UniqueId64x2` | Globally unique file identifier |
| `tail_size` | `uint64_t` | Bytes after data blocks (metadata/index/filter) |

### Serialization Format (MANIFEST records)

VersionEdit uses a tag-length-value encoding. Each field is preceded by a varint32 tag from the `Tag` enum:

```
VersionEdit on disk :=
    [kComparator    varint32_tag  length-prefixed-string]  (optional, first edit only)
    [kLogNumber     varint32_tag  varint64]                (optional)
    [kNextFileNumber varint32_tag varint64]                (optional)
    [kLastSequence  varint32_tag  varint64]                (optional)
    [kDeletedFile   varint32_tag  varint32_level varint64_file_number]*
    [kNewFile4      varint32_tag  varint32_level varint64_file_number
                    varint64_file_size  InternalKey_smallest  InternalKey_largest
                    varint64_smallest_seqno  varint64_largest_seqno
                    custom_fields...]*
    [kColumnFamily  varint32_tag  varint32_cf_id]          (unless default CF)
    [kColumnFamilyAdd  varint32_tag  length-prefixed-string]  (CF creation)
    [kColumnFamilyDrop varint32_tag]                        (CF deletion)
    [kBlobFileAddition varint32_tag  blob_file_fields]*
    [kBlobFileGarbage  varint32_tag  garbage_fields]*
    [kInAtomicGroup varint32_tag  varint32_remaining]       (atomic group)
```

Key tag numbers: `kDeletedFile=6`, `kNewFile4=103`, `kColumnFamily=200`, `kColumnFamilyAdd=201`, `kColumnFamilyDrop=202`, `kInAtomicGroup=300`, `kBlobFileAddition=400`.

Tags with `kTagSafeIgnoreMask` (bit 13) set are forward-compatible and can be skipped by older versions.

### Atomic Groups

Multiple VersionEdits can be grouped atomically using `MarkAtomicGroup(remaining_entries)`. All edits in the group must be successfully written before any take effect during recovery. This is used for cross-CF operations like bulk ingestion.

---

## 2. VersionStorageInfo

**Files:** `db/version_set.h:130`

### What It Does

VersionStorageInfo holds the complete set of SST files and blob files for one column family at one point in time. It is the storage-layer portion of a Version.

### Data Structures

| Field | Description |
|-------|-------------|
| `files_[level]` | `vector<FileMetaData*>` per level -- the SST files at each level |
| `blob_files_` | Map of blob file number to `BlobFileMetaData` |
| `level_files_brief_[level]` | `LevelFilesBrief` -- compact array of `FdWithKeyRange` for fast lookup |
| `compaction_score_[i]` | Compaction urgency scores, sorted descending |
| `compaction_level_[i]` | Which level corresponds to score `i` |
| `compact_cursor_[level]` | Round-robin compaction cursor per level |
| `files_by_compaction_pri_[level]` | File indices sorted by compaction priority |
| `files_marked_for_compaction_` | Files needing compaction (e.g., too many deletions) |
| `bottommost_files_marked_for_compaction_` | Bottom-level files with reclaimable sequence numbers |

### Key Operations

**`ComputeCompactionScore()`** (REQUIRES: `db_mutex` held): Calculates compaction urgency for each level. For L0, score = num_files / L0_compaction_trigger. For L1+, score = level_size / target_size. Also calls `ComputeFilesMarkedForCompaction()`, `ComputeExpiredTtlFiles()`, `ComputeBottommostFilesMarkedForCompaction()`, and `ComputeFilesMarkedForForcedBlobGC()`.

**`PrepareForVersionAppend()`**: Called before a new Version is finalized. Generates `LevelFilesBrief` arrays, sorts L0 files by epoch_number, and computes compaction scores.

**`SetFinalized()`**: Marks the storage info as immutable. REQUIRES: `PrepareForVersionAppend()` was called.

### L0 Ordering

L0 files can have overlapping key ranges (unlike L1+). They are sorted by `epoch_number` (descending = newest first) for correct read ordering. The `epoch_number` is assigned at flush time and tracks the creation order of files.

### File Lookup

For L1+ (non-overlapping levels): binary search on `LevelFilesBrief.files[]` using `FindFile()`. For L0: must check all files since key ranges overlap.

---

## 3. Version

**Files:** `db/version_set.h:891`

### What It Does

A Version represents a column family's complete set of SST and blob files at a specific point in time. Versions are ref-counted so that live iterators and ongoing operations can safely access files even after new flushes or compactions install newer Versions.

### Structure

```
Version
  +-- VersionStorageInfo storage_info_  (SST files per level, blob files)
  +-- ColumnFamilyData* cfd_           (owning column family)
  +-- VersionSet* vset_                (parent version set)
  +-- Version* next_ / prev_          (doubly-linked list)
  +-- int refs_                        (reference count)
  +-- uint64_t version_number_         (monotonically increasing)
```

Versions for each column family form a **circular doubly-linked list** anchored by a dummy head (`ColumnFamilyData::dummy_versions_`). The newest Version is `ColumnFamilyData::current_` (i.e., `dummy_versions_->prev_`). Older Versions are kept alive only if referenced by iterators or snapshots.

### Key Methods

**`Get()`**: Point lookup in SST files. Searches L0 files (all, newest first), then L1+ (binary search per level). Returns the first match found. REQUIRES: lock not held, `pinned_iters_mgr != nullptr`.

**`MultiGet()`**: Batched point lookup. Groups keys by level and file for efficient I/O.

**`AddIterators()`**: Creates per-level iterators for a merge iterator. L0 files each get their own iterator; L1+ levels get a concatenating iterator.

**`Ref()` / `Unref()`**: Reference counting. When refs drop to zero, the Version is destroyed (removing it from the linked list and releasing FileMetaData refs).

**`PrepareAppend()`**: Called before appending to the version set. Optionally loads stats from files. Must be called without mutex.

---

## 4. VersionBuilder

**Files:** `db/version_builder.h`, `db/version_builder.cc`

### What It Does

VersionBuilder efficiently accumulates a sequence of VersionEdits and produces a new VersionStorageInfo. It avoids creating intermediate Version objects for each edit (important during recovery when replaying thousands of MANIFEST records).

### How It Works

```
VersionBuilder(base_vstorage)    // Start from an existing Version's storage
    |
    v
Apply(edit1)                     // Accumulate: track added/deleted files
Apply(edit2)                     // Can apply many edits
    ...
    |
    v
SaveTo(new_vstorage)             // Materialize: produce new VersionStorageInfo
```

**`Apply(edit)`**: Records file additions and deletions from the edit. Validates consistency (e.g., deleted file must exist, added file must not already exist).

**`SaveTo(vstorage)`**: Merges the base storage info with accumulated additions/deletions to produce a complete new VersionStorageInfo. Files from the base that weren't deleted are carried forward; new files are inserted at their designated levels.

**`LoadTableHandlers()`**: Opens table readers for newly added SST files. Can be parallelized across threads. Called after `SaveTo()`.

### Save Points

VersionBuilder supports one save point (`CreateOrReplaceSavePoint()`), used by `VersionEditHandlerPointInTime` during best-effort recovery. The save point captures a known-good state that can be restored if later edits reference missing files.

---

## 5. MANIFEST

### What It Does

The MANIFEST file is the durable metadata log of the database. It records every VersionEdit (file additions, deletions, column family changes) so that the database state can be reconstructed on recovery.

### Format

The MANIFEST reuses the same log format as WAL files (`db/log_format.h`):

```
Log file := block*
Block := record* trailer?       (block size = 32KB)
Record := checksum (4B) | length (2B) | type (1B) | data
Type := kFullType | kFirstType | kMiddleType | kLastType
```

Each logical record contains one serialized VersionEdit. Large edits are split across multiple physical records using the First/Middle/Last fragmentation scheme.

The recyclable variants (`kRecyclableFullType`, etc.) add a 4-byte log number to the header, enabling log file reuse.

### MANIFEST Lifecycle

1. **Current MANIFEST** is identified by the `CURRENT` file, which contains the MANIFEST filename (e.g., `MANIFEST-000004`).
2. **Recovery** replays all VersionEdits from the MANIFEST to reconstruct the latest Version for each column family.
3. **Rolling**: When `new_descriptor_log=true` is passed to `LogAndApply()`, a new MANIFEST file is created with a snapshot of the current state, avoiding unbounded growth.
4. **Atomic update**: `LogAndApply()` writes the VersionEdit to MANIFEST, then atomically updates the `CURRENT` file pointer (only when rolling to a new MANIFEST).

### Recovery Process

```
Read CURRENT file -> get MANIFEST filename
    |
    v
Open MANIFEST, create VersionBuilder per column family
    |
    v
For each VersionEdit record:
    - Decode the edit
    - Route to correct CF's VersionBuilder via column_family_ field
    - VersionBuilder::Apply(edit)
    |
    v
For each column family:
    VersionBuilder::SaveTo(new_vstorage)
    -> Install as current Version
```

---

## 6. ColumnFamily

**Files:** `db/column_family.h`, `db/column_family.cc`

### What It Does

Column families provide logical separation of data within a single DB instance. Each column family has its own memtable, immutable memtable list, set of SST files (Version), compaction picker, and options. All column families share the same WAL and the same VersionSet.

### ColumnFamilyData

Core per-CF state holder (`db/column_family.h:298`):

| Field | Description |
|-------|-------------|
| `id_` | Unique CF identifier (0 = default) |
| `name_` | Human-readable CF name |
| `mem_` | Active (mutable) MemTable |
| `imm_` | `MemTableList` of immutable memtables awaiting flush |
| `current_` | Current Version (latest SST file set) |
| `super_version_` | Current SuperVersion |
| `dummy_versions_` | Doubly-linked list head for all Versions |
| `compaction_picker_` | Selects compaction targets (Level/Universal/FIFO) |
| `internal_stats_` | Per-CF statistics |
| `log_number_` | Minimum WAL number needed for this CF |
| `next_epoch_number_` | Monotonic counter for L0 file ordering |
| `ioptions_` | Immutable CF options |
| `mutable_cf_options_` | Latest mutable CF options |
| `table_cache_` | Cache of open SST file readers |
| `blob_file_cache_` / `blob_source_` | Blob file access |

**Thread safety**: Most methods require the DB mutex. `GetID()`, `GetName()`, `NumberLevels()` are thread-safe.

**Lifecycle**: Ref-counted via `Ref()` / `UnrefAndTryDelete()`. A CF can be "dropped" but still alive (reads succeed, writes fail). Dropped CFs are not compacted or flushed. When the last reference is released, the CF and its files are deleted.

### SuperVersion

**`SuperVersion`** (`db/column_family.h:206`) is the key concurrency mechanism for readers:

```cpp
struct SuperVersion {
    ColumnFamilyData* cfd;
    ReadOnlyMemTable* mem;           // active memtable
    MemTableListVersion* imm;        // immutable memtables
    Version* current;                // SST files
    MutableCFOptions mutable_cf_options;
    uint64_t version_number;
    WriteStallCondition write_stall_condition;
    std::string full_history_ts_low;
    std::shared_ptr<const SeqnoToTimeMapping> seqno_to_time_mapping;
};
```

**Purpose**: Bundles all three data sources (active memtable, immutable memtables, SST files) into a single ref-counted object. Readers acquire a SuperVersion to get a consistent view without holding the DB mutex.

**Thread-local caching**: `ColumnFamilyData::GetThreadLocalSuperVersion()` caches the current SuperVersion in thread-local storage for fast access. If the cached version is stale (version number mismatch), a new reference is acquired.

**Lifecycle**:
1. Created by `ColumnFamilyData::InstallSuperVersion()` whenever memtable, imm list, or Version changes.
2. Readers call `Ref()` to acquire, `Unref()` to release.
3. When `Unref()` returns true (last reference), `Cleanup()` must be called with mutex held.
4. Special sentinel values: `kSVInUse` marks thread-local slot as occupied, `kSVObsolete` marks it as needing refresh.

### ColumnFamilySet

**`ColumnFamilySet`** manages all column families in the DB. It maintains:
- A map from CF name to `ColumnFamilyData*`
- A map from CF id to `ColumnFamilyData*`
- A linked list of all CFs for iteration
- A default CF (id = 0, always present)

### ColumnFamilyHandleImpl

User-facing handle (`db/column_family.h:165`). Holds a pointer to `ColumnFamilyData`, `DBImpl`, and the mutex. The destructor unrefs the CFD.

---

## Key Invariants

| Invariant | Details |
|-----------|---------|
| LogAndApply is serialized | REQUIRES: `*mu` is held, no concurrent `LogAndApply()`. Ensures MANIFEST consistency. |
| MANIFEST is append-only | VersionEdits are only appended, never modified. Recovery replays the full log. |
| Older Versions stay alive while referenced | Ref-counting on Version prevents file deletion while iterators use them. |
| SuperVersion is immutable once installed | A new SuperVersion is created for every change; existing ones are never modified. |
| CF in flush_queue iff pending_flush_ is true | Queue membership is tracked by a boolean flag to prevent double-scheduling. |
| CF in compaction_queue iff pending_compaction_ is true | Same pattern for compaction scheduling. |
| Files in pending_outputs_ are protected from deletion | File numbers registered during background jobs are not deleted until the job completes. |
| epoch_number increases monotonically per CF | Ensures correct L0 ordering (newer files have higher epoch numbers). |
| SetFinalized() requires PrepareForVersionAppend() | Storage info must be fully computed before being marked immutable. |

## Interactions With Other Components

- **Write Path** (see [write_path.md](write_path.md)): After flush, the write path creates a VersionEdit adding the new L0 file and calls `LogAndApply()`.
- **Compaction** (see [compaction.md](compaction.md)): CompactionJob creates VersionEdits that add output files and delete input files, then calls `LogAndApply()`.
- **Read Path** (see [flush_and_read_path.md](flush_and_read_path.md)): Readers acquire a SuperVersion to access memtables and Version. `Version::Get()` searches SST files.
- **DB Open** (see [db_impl.md](db_impl.md)): Recovery replays MANIFEST using VersionBuilder to reconstruct current Versions.
- **SST Format** (see [sst_table_format.md](sst_table_format.md)): Version holds FileMetaData for each SST; TableCache opens table readers on demand.

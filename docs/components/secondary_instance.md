# Secondary Instance (Secondary Reader)

## Overview

A **Secondary Instance** is a read-only follower of a primary RocksDB instance that shares access to the same database files without coordination. Secondary instances provide:

- **Read-only access** to the primary database
- **Dynamic catch-up** capability via `TryCatchUpWithPrimary()`
- **No coordination overhead** with the primary
- **Multiple concurrent secondary instances** can co-exist

Secondary instances are useful for:
- Offloading read traffic from primary
- Running analytics queries without impacting production writes
- Remote compaction services (`OpenAndCompact`)
- Cross-region read replicas

**Key source files:**
- `db/db_impl/db_impl_secondary.h`, `db/db_impl/db_impl_secondary.cc` (implementation)
- `include/rocksdb/db.h` (public API)
- `db/version_set.cc` (ReactiveVersionSet for MANIFEST tailing)

---

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                      Primary Instance                        │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐   │
│  │MemTable  │→ │   WAL    │  │ MANIFEST │  │ SST Files│   │
│  └──────────┘  └──────────┘  └──────────┘  └──────────┘   │
│       │            │              │              │          │
│       │Writes      │Writes        │Writes        │Creates   │
│       ▼            ▼              ▼              ▼          │
└───────────────────────────────────────────────────────────┘
         ▲            ▲              ▲              ▲
         │            │              │              │
         │(no access) │(optional)    │(always)      │(always)
         │            │Tail WAL      │Tail MANIFEST │Read SSTs
         │            │              │              │
┌────────┼────────────┼──────────────┼──────────────┼─────────┐
│        │            │              │              │          │
│   Secondary Instance (Read-Only Follower)                   │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐     │
│  │ ManifestReader│→│ LogReaders   │  │ TableCache   │     │
│  │ (tails MANIFEST)│(tails WALs)   │  │ (reads SSTs) │     │
│  └──────────────┘  └──────────────┘  └──────────────┘     │
│         │                │                    │             │
│         └────────────────┴────────────────────┘             │
│                          │                                  │
│                ┌─────────▼─────────┐                        │
│                │ SuperVersion      │                        │
│                │ (point-in-time    │                        │
│                │  view of DB)      │                        │
│                └─────────┬─────────┘                        │
│                          │                                  │
│                  ┌───────▼────────┐                         │
│                  │   Get/Iterate  │                         │
│                  │   (Read Ops)   │                         │
│                  └────────────────┘                         │
└─────────────────────────────────────────────────────────────┘
```

---

## Opening a Secondary Instance

### Basic API

```cpp
// Single column family
Status DB::OpenAsSecondary(
    const Options& options,
    const std::string& name,              // Primary DB path
    const std::string& secondary_path,    // Secondary metadata path
    std::unique_ptr<DB>* dbptr);

// Multiple column families
Status DB::OpenAsSecondary(
    const DBOptions& db_options,
    const std::string& name,
    const std::string& secondary_path,
    const std::vector<ColumnFamilyDescriptor>& column_families,
    std::vector<ColumnFamilyHandle*>* handles,
    std::unique_ptr<DB>* dbptr);
```

**Implementation:** `db/db_impl/db_impl_secondary.cc:706-727`

### Parameters

| Parameter | Description |
|-----------|-------------|
| `name` | Path to the **primary** database directory |
| `secondary_path` | Path where secondary stores its **own info log** |
| `options.max_open_files` | **Must be set to -1** to keep all table files open (see File Deletion Coordination below) |
| `column_families` | Must include at least the default CF; subset of primary's CFs allowed |

⚠️ **INVARIANT:** `Options.max_open_files` should be set to `-1` when opening a secondary instance. This prevents `IOError` when the primary deletes files that the secondary is still reading (via open file descriptors, the secondary can access deleted files on POSIX systems).

### Opening Process

```
db/db_impl/db_impl_secondary.cc:OpenAsSecondaryImpl
  ├─ Create DBImplSecondary instance (db_impl_secondary.cc:27)
  │   └─ Initialize secondary_path_ for metadata
  │
  ├─ Recover() (db_impl_secondary.cc:39-56)
  │   ├─ Create ReactiveVersionSet
  │   ├─ Call ReactiveVersionSet::Recover()
  │   │   └─ Read MANIFEST, create manifest_reader_ (version_set.cc:8007)
  │   └─ Initialize manifest_reader_ for future tailing
  │
  └─ (Optional) Recover WAL logs (if recover_wal=true)
      └─ RecoverLogFiles() (db_impl_secondary.cc:176-329)
          └─ Create log_readers_ for tailing
```

⚠️ **INVARIANT:** During recovery, the secondary only replays the MANIFEST. WAL recovery happens separately if needed. The secondary **never locks** the primary's database directory.

---

## Catching Up with Primary: TryCatchUpWithPrimary()

Secondary instances are **not automatically synchronized**. To catch up with changes from the primary, call:

```cpp
Status DB::TryCatchUpWithPrimary();
```

**Implementation:** `db/db_impl/db_impl_secondary.cc:639-704`

### What TryCatchUpWithPrimary Does

```
TryCatchUpWithPrimary
  │
  ├─ 1. Tail MANIFEST (db_impl_secondary.cc:648-651)
  │   └─ ReactiveVersionSet::ReadAndApply()
  │       ├─ MaybeSwitchManifest() (version_set.cc:8069)
  │       │   └─ Check if CURRENT points to new MANIFEST
  │       ├─ Read new VersionEdits from manifest_reader_
  │       └─ Apply edits → update LSM tree metadata
  │
  ├─ 2. Tail WAL (db_impl_secondary.cc:670)
  │   └─ FindAndRecoverLogFiles()
  │       ├─ List WAL directory for new log files
  │       ├─ MaybeInitLogReader() for each new WAL
  │       └─ RecoverLogFiles() (db_impl_secondary.cc:176)
  │           ├─ Read WriteBatch records from log_readers_
  │           ├─ Apply to in-memory memtables
  │           └─ Update sequence numbers
  │
  ├─ 3. Install new SuperVersions (db_impl_secondary.cc:680-686)
  │   └─ For each changed CF, create new point-in-time view
  │
  └─ 4. Cleanup (db_impl_secondary.cc:691-702)
      └─ PurgeObsoleteFiles() (only file descriptors, not actual files)
```

### MANIFEST Tailing Details

The secondary uses `ReactiveVersionSet::ReadAndApply()` to tail the MANIFEST:

1. **Check for MANIFEST rotation** (`version_set.cc:8069-8131`):
   - Read `CURRENT` file to get current MANIFEST path
   - If MANIFEST path changed, switch to new MANIFEST file
   - Handle race: primary may delete old MANIFEST while secondary reads it

2. **Read VersionEdits** (`version_set.cc:8040-8067`):
   - Use `manifest_reader_` (log::FragmentBufferedReader) to read new records
   - `ManifestTailer::Iterate()` parses VersionEdit records
   - Track which column families changed

3. **Apply edits** (`db/version_builder.cc`):
   - Update VersionStorageInfo (LSM tree structure)
   - Track file additions/deletions
   - Update sequence numbers

⚠️ **INVARIANT:** The secondary reads the MANIFEST **without locking**. The primary may be concurrently writing to the MANIFEST. This is safe because MANIFEST records are append-only and atomic within a VersionEdit group.

### WAL Tailing Details

WAL tailing is **optional** and provides fresher data:

```cpp
// db/db_impl/db_impl_secondary.cc:176-329
Status DBImplSecondary::RecoverLogFiles(
    const std::vector<uint64_t>& log_numbers, ...) {

  // For each WAL file:
  for (auto log_number : log_numbers) {
    log::FragmentBufferedReader* reader = ...;

    while (reader->ReadRecord(&record, ...)) {
      WriteBatch batch;
      WriteBatchInternal::SetContents(&batch, record);

      // Apply to memtables
      WriteBatchInternal::InsertInto(&batch,
                                     column_family_memtables_.get(), ...);
    }
  }
}
```

**Key points:**
- WAL files are discovered by listing the WAL directory
- Each WAL gets a `LogReaderContainer` with a `log::FragmentBufferedReader`
- `log_readers_` map caches readers for incremental tailing
- Old WAL readers are evicted (keeps only latest, `db_impl_secondary.cc:323-327`)

⚠️ **INVARIANT:** The secondary replays WAL records into **in-memory memtables only**. These memtables are never flushed (secondary is read-only). If the primary deletes a WAL, the secondary tolerates `PathNotFound` errors (`db_impl_secondary.cc:671-677`).

---

## Read Operations

### Supported Operations

| Operation | Support | Notes |
|-----------|---------|-------|
| `Get()` | ✅ Yes | `db_impl_secondary.cc:331-421` |
| `MultiGet()` | ✅ Yes | Inherited from DBImpl |
| `NewIterator()` | ✅ Yes | `db_impl_secondary.cc:423-484` |
| `GetSnapshot()` | ✅ Yes | Implicit snapshot at LastSequence |
| `GetProperty()` | ✅ Yes | Read-only properties |

### Read Flow

```
Get/Iterator
  │
  ├─ Acquire SuperVersion (db_impl_secondary.cc:381)
  │   └─ Contains point-in-time view: Version + MemTables
  │
  ├─ Search in memtables (from WAL replay)
  │   └─ Active memtable + immutable memtables
  │
  ├─ Search in SST files (via Version)
  │   └─ TableCache reads from primary's SST files
  │
  └─ Release SuperVersion
```

⚠️ **INVARIANT:** All reads operate on the `SuperVersion` at the time of `TryCatchUpWithPrimary()`. Reads do **not** reflect primary's writes until the next catch-up call.

### File Deletion Coordination

**Problem:** The primary may delete SST files while the secondary is reading them.

**Solutions:**

1. **max_open_files = -1 (Recommended):**
   - Secondary keeps all table files open
   - POSIX allows reading deleted files via open file descriptors
   - Prevents IOError from file deletion

2. **Application-level coordination:**
   - Primary and secondary communicate to prevent premature deletion
   - Use `DisableFileDeletions()` on primary (not recommended)

3. **Custom FileSystem:**
   - Implement reference-counted file deletion
   - Files persist until all instances close them

4. **Limitations:**
   - Cannot use `SstFileManager` with file truncation features
   - Setting `rate_bytes_per_sec > 0` or `bytes_max_delete_chunk > 0` in primary's `NewSstFileManager()` causes truncation → secondary cannot read

⚠️ **INVARIANT:** The secondary **does not own** database files (`OwnTablesAndLogs()` returns `false`, `db_impl_secondary.h:267-273`). File deletion is the primary's responsibility.

---

## Unsupported Operations

All write operations return `Status::NotSupported`:

```cpp
// db/db_impl/db_impl_secondary.h:128-240
Status Put(...) { return Status::NotSupported("Not supported in secondary"); }
Status Delete(...) { return Status::NotSupported(...); }
Status Write(...) { return Status::NotSupported(...); }
Status Flush(...) { return Status::NotSupported(...); }
Status CompactRange(...) { return Status::NotSupported(...); }
Status SetDBOptions(...) { return Status::NotSupported(...); }
Status IngestExternalFile(...) { return Status::NotSupported(...); }
```

**Exceptions:**
- `CompactWithoutInstallation()` for remote compaction (see below)

---

## Consistency Guarantees

### Point-in-Time Consistency

The secondary provides **eventual consistency** with bounded staleness:

| Aspect | Guarantee |
|--------|-----------|
| Snapshot isolation | ✅ Yes, at `TryCatchUpWithPrimary()` time |
| Read-your-writes | ❌ No (reads may lag behind primary writes) |
| Monotonic reads | ✅ Yes (within same SuperVersion) |
| Cross-CF consistency | ✅ Yes (all CFs updated atomically in `TryCatchUpWithPrimary`) |

### Sequence Number Semantics

```cpp
// db/db_impl/db_impl_secondary.cc:368
SequenceNumber snapshot = versions_->LastSequence();
```

- Reads use `LastSequence` from the last MANIFEST/WAL tail
- Primary's writes with higher sequence numbers are invisible
- Calling `TryCatchUpWithPrimary()` advances `LastSequence`

⚠️ **INVARIANT:** The secondary's `LastSequence` is **monotonically increasing**. It never decreases between `TryCatchUpWithPrimary()` calls.

---

## Remote Compaction

Secondary instances support **compaction without installation** for remote compaction services.

### API

```cpp
Status DB::OpenAndCompact(
    const OpenAndCompactOptions& options,
    const std::string& name,              // Primary DB path
    const std::string& output_directory,  // Where to write output
    const std::string& input,             // CompactionServiceInput (serialized)
    std::string* output,                  // CompactionServiceResult (serialized)
    const CompactionServiceOptionsOverride& override_options);
```

**Implementation:** `db/db_impl/db_impl_secondary.cc:1459-1614`

### How It Works

```
OpenAndCompact
  │
  ├─ 1. Open as secondary (WITHOUT WAL recovery)
  │   └─ OpenAsSecondaryImpl(recover_wal=false)
  │       └─ Read MANIFEST only (not WAL)
  │
  ├─ 2. Deserialize CompactionServiceInput
  │   ├─ Input files to compact
  │   ├─ Output level
  │   └─ Compaction options
  │
  ├─ 3. Run CompactWithoutInstallation()
  │   ├─ Create CompactionJob (db_impl_secondary.cc:1293)
  │   ├─ Run compaction in output_directory
  │   ├─ Write SST files to output_directory (not primary DB)
  │   └─ NO installation into LSM tree
  │
  └─ 4. Serialize CompactionServiceResult
      └─ Output file metadata, statistics
```

### Use Cases

- **Disaggregated compaction:** Primary offloads compaction to remote workers
- **Tiered storage:** Compact data to cheaper storage tiers
- **Resource isolation:** Prevent compaction from impacting primary's latency

⚠️ **INVARIANT:** `OpenAndCompact` **does not modify** the primary database. Output files are written to `output_directory`, not the primary's data directory.

### Compaction Progress Tracking

For resumable compaction, the secondary maintains progress files:

```cpp
// db/db_impl/db_impl_secondary.h:308-335
struct CompactionProgressFilesScan {
  std::optional<std::string> latest_progress_filename;
  autovector<std::string> old_progress_filenames;
  autovector<std::string> temp_progress_filenames;
  std::vector<uint64_t> table_file_numbers;
};
```

- **Progress files:** Track which files have been compacted (for resumption)
- **Cleanup:** Old/temporary progress files are deleted
- **Resumption:** `allow_resumption=true` in `OpenAndCompactOptions`

---

## Column Family Handling

### Opening Subset of CFs

```cpp
// You can open only a subset of CFs in secondary
std::vector<ColumnFamilyDescriptor> column_families = {
  {kDefaultColumnFamilyName, cf_options},  // Must always include default
  {"cf1", cf_options}  // Optional: subset of primary's CFs
};
```

### CF Creation/Deletion

| Event | Behavior |
|-------|----------|
| Primary creates new CF after secondary opens | ❌ Ignored by secondary (until restart) |
| Primary drops CF after secondary opens | ✅ Marked as dropped in secondary (`TryCatchUpWithPrimary`) |
| Secondary accesses dropped CF | ✅ Still readable if CF handle not destroyed |

⚠️ **INVARIANT:** The secondary **cannot dynamically track** new CFs created by the primary. To access new CFs, the secondary must close and reopen with the updated CF list.

```cpp
// db/db_impl/db_impl_secondary.cc:655-660
for (ColumnFamilyData* cfd : cfds_changed) {
  if (cfd->IsDropped()) {
    ROCKS_LOG_DEBUG(info_log, "[%s] is dropped\n", cfd->GetName().c_str());
    continue;  // Skip dropped CFs
  }
}
```

---

## Performance Characteristics

### TryCatchUpWithPrimary Latency

| Phase | Typical Latency | Cost |
|-------|-----------------|------|
| MANIFEST tail | O(new VersionEdits) | 10-100 μs (if few edits) |
| WAL tail | O(WAL size) | 1-100 ms (depends on WAL size) |
| SuperVersion install | O(num CFs) | 10-100 μs |
| Total | Variable | **Can be expensive if primary is write-heavy** |

⚠️ **WARNING:** If the primary has a large backlog of MANIFEST/WAL changes, `TryCatchUpWithPrimary()` can take **seconds** due to I/O and CPU costs.

### Read Performance

- **Same as primary** for reads (same TableCache, block cache)
- **No write overhead** (no WAL sync, no memtable flushing)
- **Potential IOError** if files deleted by primary (mitigated by max_open_files=-1)

---

## Limitations

| Limitation | Impact | Mitigation |
|------------|--------|------------|
| **Stale reads** | Secondary lags behind primary | Call `TryCatchUpWithPrimary()` more frequently |
| **File deletion races** | IOError if primary deletes files | Set `max_open_files=-1` |
| **No dynamic CF discovery** | New CFs invisible until restart | Restart secondary when primary adds CFs |
| **MANIFEST rotation race** | `TryAgain` error if MANIFEST switches | Retry `TryCatchUpWithPrimary()` |
| **WAL deletion** | `PathNotFound` when tailing WAL | Tolerated (returns OK, `db_impl_secondary.cc:676`) |
| **No write coordination** | Cannot prevent primary from advancing | Use read-only snapshots in primary |
| **Memory overhead** | Each secondary has own memtables from WAL | Limit WAL tailing or disable it |

---

## Example Usage

### Basic Secondary Instance

```cpp
// Open primary (write instance)
Options options;
std::unique_ptr<DB> primary;
Status s = DB::Open(options, "/data/primary", &primary);

// Open secondary (read instance)
Options secondary_options = options;
secondary_options.max_open_files = -1;  // Critical!
std::unique_ptr<DB> secondary;
s = DB::OpenAsSecondary(secondary_options,
                        "/data/primary",      // Primary's path
                        "/data/secondary",    // Secondary's metadata path
                        &secondary);

// Primary writes
s = primary->Put(WriteOptions(), "key", "value");

// Secondary reads (stale)
std::string value;
s = secondary->Get(ReadOptions(), "key", &value);
// Returns NotFound (stale)

// Catch up with primary
s = secondary->TryCatchUpWithPrimary();

// Now read reflects primary's writes
s = secondary->Get(ReadOptions(), "key", &value);
// Returns "value"
```

### Remote Compaction

```cpp
// Worker receives CompactionServiceInput from primary
CompactionServiceInput input = /* deserialized from primary */;
std::string input_serialized;
input.Write(&input_serialized);

std::string output_serialized;
Status s = DB::OpenAndCompact(
    OpenAndCompactOptions(),
    "/primary/db/path",
    "/worker/output/dir",
    input_serialized,
    &output_serialized,
    CompactionServiceOptionsOverride());

// Worker sends CompactionServiceResult back to primary
CompactionServiceResult result;
result.Read(output_serialized, &result);
```

---

## Comparison with Other Read-Only Modes

| Feature | Secondary | ReadOnly (`OpenForReadOnly`) | Follower (`OpenAsFollower`) |
|---------|-----------|------------------------------|------------------------------|
| Dynamic catch-up | ✅ Manual (`TryCatchUpWithPrimary`) | ❌ No (static snapshot) | ✅ Automatic (periodic tailing) |
| Multiple instances | ✅ Yes | ⚠️ Undefined with concurrent primary | ✅ Yes |
| WAL tailing | ✅ Optional | ❌ No | ✅ Yes (automatic) |
| File deletion tolerance | ⚠️ Requires `max_open_files=-1` | ❌ No | ✅ Yes (uses links) |
| Own directory | ✅ Yes (`secondary_path`) | ❌ No | ✅ Yes |
| EXPERIMENTAL | ❌ Stable | ❌ Stable | ⚠️ **EXPERIMENTAL** |

---

## Debugging

### Logging

```cpp
// Secondary logs MANIFEST switching
ROCKS_LOG_INFO(info_log, "Switched to new manifest: %s\n", manifest_path);

// WAL purging by primary
ROCKS_LOG_INFO(info_log,
    "Secondary tries to read WAL, but WAL file(s) have already been purged");

// Dropped column families
ROCKS_LOG_DEBUG(info_log, "[%s] is dropped\n", cfd->GetName().c_str());
```

### Common Issues

| Symptom | Cause | Solution |
|---------|-------|----------|
| `IOError: No such file` on read | Primary deleted SST | Set `max_open_files=-1` |
| `TryAgain` in `TryCatchUpWithPrimary` | MANIFEST rotation race | Retry the call |
| Stale reads | Haven't called `TryCatchUpWithPrimary` | Call more frequently |
| High `TryCatchUpWithPrimary` latency | Large WAL/MANIFEST backlog | Reduce catch-up frequency or disable WAL tailing |

---

## Key Takeaways

1. **Secondary instances are read-only followers** that tail MANIFEST and optionally WAL.
2. **Set `max_open_files=-1`** to avoid IOError from file deletions.
3. **Call `TryCatchUpWithPrimary()` manually** to catch up with primary (not automatic).
4. **Reads are eventually consistent**, reflecting state at last catch-up.
5. **Remote compaction** uses `OpenAndCompact()` to run compaction without modifying primary.
6. **Cannot dynamically discover new CFs**—must restart secondary to access them.

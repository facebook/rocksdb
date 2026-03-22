# Crash Recovery

## Overview

Crash recovery is the process of restoring a consistent database state after an unexpected shutdown (crash, power loss, or forced termination). RocksDB's recovery happens during `DB::Open()` and reconstructs the database state by:

1. **MANIFEST recovery**: Reading VersionEdits from the MANIFEST file to reconstruct which SST files exist and the structure of the LSM tree
2. **WAL recovery**: Replaying Write-Ahead Log (WAL) records to restore writes that were committed but not yet flushed to SST files
3. **Consistency checks**: Verifying that all referenced SST files exist and match MANIFEST metadata
4. **Post-recovery actions**: Optionally flushing recovered memtables and preparing for normal operation

The recovery process ensures data durability guarantees specified by `WALRecoveryMode` and handles corruptions according to the configured policy.

---

## Files

| File | Responsibility |
|------|---------------|
| `db/db_impl/db_impl_open.cc` | Main recovery orchestration: `DBImpl::Recover()`, `RecoverLogFiles()` |
| `db/version_set.cc` | MANIFEST recovery: `VersionSet::Recover()`, `VersionSet::TryRecover()` (BER) |
| `db/version_edit_handler.h` | `VersionEditHandler`: processes VersionEdits during MANIFEST replay |
| `db/manifest_ops.h` | `GetCurrentManifestPath()`: reads CURRENT file to find active MANIFEST |
| `db/log_reader.cc` | WAL reading during recovery |
| `db/repair.cc` | `RepairDB()` for manual database repair |
| `include/rocksdb/options.h` | `WALRecoveryMode`, `best_efforts_recovery` option definitions |
| `file/filename.h` | File naming conventions (`CURRENT`, `MANIFEST-*`, WAL files) |

---

## 1. Recovery Overview: DB::Open After a Crash

When `DB::Open()` is called after a crash, RocksDB performs recovery to restore the database to a consistent state:

```
DB::Open(options, path, ...)
    |
    v
SanitizeOptions() -- Validate options, adjust incompatible settings
    |
    v
new DBImpl(options, path)
    |
    v
DBImpl::Recover()
    |
    +-- Lock database directory (DB lock file)
    +-- Read CURRENT file -> Find latest MANIFEST-NNNNNN
    +-- VersionSet::Recover()
    |     |
    |     +-- Open MANIFEST file
    |     +-- Replay VersionEdits -> Reconstruct Versions for all column families
    |     +-- Verify SST file existence and metadata (if not best_efforts_recovery)
    |
    +-- RecoverLogFiles()
    |     |
    |     +-- Find all WAL files >= min_log_number_to_keep
    |     +-- For each WAL file:
    |           +-- Read WAL records
    |           +-- Replay WriteBatch into memtables
    |           +-- Handle corruption according to WALRecoveryMode
    |
    +-- Post-recovery flush (if !avoid_flush_during_recovery)
    +-- InstallSuperVersions for all column families
    |
    v
MaybeScheduleFlushOrCompaction()
    |
    v
Return DB handle (ready for reads/writes)
```

**⚠️ INVARIANT:** Recovery is atomic from the user's perspective. Either the database opens successfully with a consistent state, or `DB::Open()` returns an error and the database remains unopened.

**⚠️ INVARIANT:** After successful recovery, all committed writes (those for which `Write()` returned OK) are visible, unless using a recovery mode that tolerates data loss (e.g., `kPointInTimeRecovery` or `kSkipAnyCorruptedRecords`). Note that the default mode (`kPointInTimeRecovery`) stops at the first corruption, so writes after the corruption point may be lost.

---

## 2. MANIFEST Recovery

The MANIFEST is a log file containing `VersionEdit` records that describe incremental changes to the LSM tree structure (file additions, deletions, compaction results). MANIFEST recovery reconstructs the current state of all column families.

### CURRENT File: Finding the Latest MANIFEST

The `CURRENT` file is a small text file containing the name of the active MANIFEST file (e.g., `MANIFEST-000123`). RocksDB uses this to locate the correct MANIFEST after a restart.

```
CURRENT file contents: "MANIFEST-000123\n"
                              |
                              v
                    Read MANIFEST-000123
```

**File:** `file/filename.h:kCurrentFileName`, `db/manifest_ops.h:GetCurrentManifestPath()`

**⚠️ INVARIANT:** The `CURRENT` file always points to a valid MANIFEST file. Updating `CURRENT` is the final step when creating a new MANIFEST (atomic rename operation).

### VersionSet::Recover() Flow

**File:** `db/version_set.cc:6614`

```cpp
Status VersionSet::Recover(
    const std::vector<ColumnFamilyDescriptor>& column_families,
    bool read_only, std::string* db_id, bool no_error_if_files_missing,
    bool is_retry, Status* log_status) {
  // 1. Read CURRENT file to find MANIFEST path
  GetCurrentManifestPath(dbname_, fs_.get(), is_retry, &manifest_path, &manifest_file_number_);

  // 2. Open MANIFEST file for sequential reading
  SequentialFileReader manifest_file_reader;

  // 3. Create VersionEditHandler to process VersionEdits
  VersionEditHandler handler(...);

  // 4. Iterate through MANIFEST records
  log::Reader reader(...);
  handler.Iterate(reader, &log_read_status);

  // 5. Apply VersionEdits to build current Version for each CF
  // handler reconstructs:
  //   - Which SST files exist at each level
  //   - File metadata (smallest/largest key, size, sequence numbers)
  //   - Column family metadata

  // 6. Extract recovered state
  log_number = handler.GetVersionEditParams().GetLogNumber();
  handler.GetDbId(db_id);

  return handler.status();
}
```

**Key Steps:**

1. **Read CURRENT**: Identify the active MANIFEST file
2. **Parse VersionEdits**: Each MANIFEST record is a `VersionEdit` describing a change (add file, delete file, column family modification)
3. **Reconstruct Versions**: Apply edits in order to build the current LSM tree structure for each column family
4. **Extract metadata**: Recover `log_number` (minimum WAL to keep), `db_id`, sequence numbers

**⚠️ INVARIANT:** MANIFEST records are applied in order. The final state represents the LSM tree at the time of the last successful flush or compaction that updated the MANIFEST.

### Best-Efforts Recovery and MANIFEST

With `best_efforts_recovery = true`, RocksDB can recover even if:
- The `CURRENT` file is missing or corrupt
- Some SST files referenced by the MANIFEST are missing
- MANIFEST is truncated or partially corrupt

**File:** `db/db_impl/db_impl_open.cc:444-470`

```cpp
if (!immutable_db_options_.best_efforts_recovery) {
  s = env_->FileExists(current_fname);  // Require CURRENT file
} else {
  // Scan directory for any non-empty MANIFEST-* file, even if CURRENT is missing
  for (const std::string& file : files_in_dbname) {
    uint64_t number = 0;
    FileType type = kWalFile;
    if (ParseFileName(file, &number, &type) && type == kDescriptorFile) {
      uint64_t bytes;
      s = env_->GetFileSize(DescriptorFileName(dbname_, number), &bytes);
      if (s.ok() && bytes != 0) {
        // Found non-empty MANIFEST, use it
        manifest_path = dbname_ + "/" + file;
        break;
      }
    }
  }
}
```

Best-efforts recovery tries to find the most recent valid "point in time" by:
- Scanning for any non-empty MANIFEST files in the directory (skipping empty ones)
- Using `VersionSet::TryRecover()` (instead of the normal `VersionSet::Recover()`) which accepts missing SST files and recovers to the last consistent state where all files exist
- Recovering an incomplete version with only a suffix of L0 files missing (recent writes lost), but only when `atomic_flush` is not enabled (atomic flush requires consistent cross-CF recovery)

See `include/rocksdb/options.h:1561-1608` for full `best_efforts_recovery` documentation.

---

## 3. WAL Recovery

After MANIFEST recovery, RocksDB knows which SST files exist but hasn't yet recovered writes that were committed but not flushed. WAL recovery replays these writes into memtables.

### WAL File Selection

**File:** `db/db_impl/db_impl_open.cc:1147-1183`

```cpp
void DBImpl::SetupLogFilesRecovery(...) {
  uint64_t min_wal_number = MinLogNumberToKeep();

  if (!allow_2pc()) {
    // In non-2PC mode, skip WALs older than the oldest unflushed data
    min_wal_number = std::max(min_wal_number,
                              versions_->MinLogNumberWithUnflushedData());
  }

  // Only recover WAL files with number >= min_wal_number
}
```

**⚠️ INVARIANT:** WAL files older than `min_wal_number` are never needed for recovery because their data has been flushed to SST files and persisted in the MANIFEST.

**Special case for 2PC (two-phase commit):** With `allow_2pc = true`, RocksDB must keep all WALs that might contain prepared (but not committed) transactions, even if the data is older than the last flush. This is because prepared transactions must be recoverable to allow commit or rollback after recovery.

### RecoverLogFiles() Flow

**File:** `db/db_impl/db_impl_open.cc:1128`

```cpp
Status DBImpl::RecoverLogFiles(const std::vector<uint64_t>& wal_numbers, ...) {
  // 1. Setup: Determine min_wal_number, initialize VersionEdits
  SetupLogFilesRecovery(wal_numbers, &version_edits, &job_id, &min_wal_number);

  // 2. Process each WAL file
  Status status = ProcessLogFiles(wal_numbers, read_only, ...);

  // 3. Finalize: Log recovery completion
  FinishLogFilesRecovery(job_id, status);

  return status;
}
```

**ProcessLogFiles() (line 1185-1229) loops over WAL files and calls ProcessLogFile() (singular) for each:**

```
For each WAL file:
    |
    +-- Skip if wal_number < min_wal_number
    |
    +-- Open WAL file with log::Reader
    |
    +-- For each record in WAL:
    |     |
    |     +-- Read WriteBatch
    |     +-- Check for corruption (reporter detects checksum failures, truncation)
    |     +-- Insert WriteBatch into memtables (replay the write)
    |     +-- Update sequence number
    |     +-- Handle corruption according to WALRecoveryMode
    |
    +-- If corruption detected: Apply WALRecoveryMode policy
    |
    v
  Post-WAL-replay flush (if needed)
```

**File:** `db/db_impl/db_impl_open.cc:1231+` (ProcessLogFile, singular - processes one WAL)

Key aspects:
- **Sequential replay**: WAL records are replayed in order
- **WriteBatch replay**: Each WAL record is a `WriteBatch` that gets inserted into the appropriate column family's memtable
- **Sequence number tracking**: Recovery tracks `next_sequence` to detect gaps or inconsistencies
- **Corruption detection**: `log::Reader` reports checksum failures, truncation, or invalid records via a `Reporter`
- **WAL filtering**: If a `WalFilter` is configured (`immutable_db_options_.wal_filter`), it is invoked during recovery to allow the application to inspect, modify, or skip WAL records. The filter's `ColumnFamilyLogNumberMap()` is called before replay, and each record passes through the filter during replay. See `db/db_impl/db_impl_open.cc:995-1012` for `InvokeWalFilterIfNeededOnColumnFamilyToWalNumberMap()` and `db/db_impl/db_impl_open.cc:1015+` for per-record filtering.

**⚠️ INVARIANT:** WAL records are replayed in the order they were written. This ensures writes appear in the same order after recovery as before the crash.

**⚠️ INVARIANT:** If WAL replay succeeds without corruption, the sequence number after recovery equals the sequence number before the crash, ensuring no writes are lost or duplicated.

---

## 4. WALRecoveryMode

`WALRecoveryMode` controls how RocksDB handles WAL corruption during recovery. The mode determines whether to fail recovery, stop at the corruption, or skip corrupted records.

**File:** `include/rocksdb/options.h:414-451`

### Mode Definitions

| Mode | Behavior | Use Case |
|------|----------|----------|
| **kTolerateCorruptedTailRecords** | Tolerate incomplete records at the end of WAL (from crash during write). Zero-padding from preallocation is also tolerated. **Fails on any other corruption.** | Applications that require durability: once `Write()` returns OK and `WritableFile::Append()` is durable, the data must not be lost. |
| **kAbsoluteConsistency** | Fail recovery if **any** corruption is detected in WAL. | Unit tests, applications requiring absolute consistency (no tolerance for corruption). |
| **kPointInTimeRecovery** | Stop WAL replay at the first corruption. Recovers to a valid point-in-time before the corruption. | **Default.** Systems with disk controller caches (SSDs without supercapacitors) where partial writes can occur. |
| **kSkipAnyCorruptedRecords** | Skip **all** corrupted records and continue recovery. Salvages as much data as possible. | Disaster recovery, low-grade data where some loss is acceptable to maximize recovery. |

### kTolerateCorruptedTailRecords

```
WAL file: [Record 1][Record 2][Record 3][Incomplete Record 4 (crash during write)]
                                                |
                                                v
                                        Tolerate incomplete tail
                                        Recover records 1, 2, 3
```

**Guarantees:**
- All writes for which `Write()` returned OK are recovered, provided `WritableFile::Append()` writes are durable (e.g., via `fsync`). Without explicit sync, RocksDB offers mechanisms such as `WriteOptions::sync` or `FlushWAL(true)` to strengthen this guarantee.
- Corruption in the middle of the WAL (not at tail) causes recovery failure
- Distinguishes between crash-during-write (acceptable) and data corruption (unacceptable)

**⚠️ INVARIANT:** With `kTolerateCorruptedTailRecords`, if `Write(batch)` returned `Status::OK()` and the underlying `WritableFile::Append()` was durable, then after recovery, all keys in `batch` are present in the database (unless overwritten by a later write).

**Incompatibility with WAL recycling:** When `recycle_log_file_num > 0`, old WAL data remains at the tail of recycled files. With `kTolerateCorruptedTailRecords`, corruption detection cannot distinguish between crash-during-write corruption (acceptable) and old recycled data (expected). Additionally, `kAbsoluteConsistency` (and `kPointInTimeRecovery`) have a known bug with recycled logs that can introduce holes in recovered data ([PR #7252](https://github.com/facebook/rocksdb/pull/7252#issuecomment-673766236)). RocksDB therefore disables `recycle_log_file_num` when using either `kTolerateCorruptedTailRecords` or `kAbsoluteConsistency`.

**File:** `db/db_impl/db_impl_open.cc:105-122`

### kAbsoluteConsistency

```
WAL file: [Record 1][Record 2][Corrupted Record 3]
                                        |
                                        v
                                Recovery FAILS
                                (returns error from DB::Open)
```

Use for: Unit tests, systems where any WAL corruption is unacceptable.

### kPointInTimeRecovery (Default)

```
WAL file: [Record 1][Record 2][Record 3][Corrupted Record 4][Record 5]
                                                |
                                                v
                                        Stop at corruption
                                        Recover to point-in-time: records 1, 2, 3
                                        (Record 5 is lost)
```

**Guarantees:**
- Recovers to a consistent point-in-time before the corruption
- No guarantee that all committed writes are recovered (some may be lost if corruption occurs)

**Use case:** Systems where disk caches may reorder writes, causing partial WAL writes.

### kSkipAnyCorruptedRecords

```
WAL file: [Record 1][Corrupted Record 2][Record 3][Corrupted Record 4][Record 5]
                            |                              |
                            v                              v
                        Skip                           Skip

Recover records: 1, 3, 5
```

**Guarantees:** Best-effort recovery. Maximizes data recovery at the cost of potential inconsistency.

**Use case:** Disaster recovery, last-ditch effort to salvage data.

**⚠️ INVARIANT:** With `kSkipAnyCorruptedRecords`, the database may be inconsistent after recovery. For example, if a transaction's writes are partially lost, the database may contain only some of the transaction's updates.

---

## 5. Recovery Sequence

The recovery process follows a strict sequence to ensure consistency:

```
1. Lock database directory
      |
      v
2. MANIFEST recovery first
      |
      +-- Read CURRENT file
      +-- Parse MANIFEST-NNNNNN
      +-- Reconstruct Versions (LSM tree structure)
      +-- Extract log_number, db_id, sequence numbers
      |
      v
3. WAL recovery second
      |
      +-- Identify WAL files to replay (>= min_log_number_to_keep)
      +-- Replay each WAL sequentially
      +-- Insert WriteBatches into memtables
      +-- Handle corruption according to WALRecoveryMode
      |
      v
4. Consistency checks
      |
      +-- Verify all SST files referenced by MANIFEST exist
      +-- Verify SST unique IDs match (if tracked)
      +-- Check sequence number consistency
      |
      v
5. Post-recovery actions
      |
      +-- Flush recovered memtables (if !avoid_flush_during_recovery)
      +-- Install SuperVersions
      +-- Schedule background compaction/flush
      |
      v
6. Database ready for operations
```

**Why MANIFEST before WAL?**
- The MANIFEST tells us which SST files exist and what data they contain
- WAL recovery needs to know the last flushed state to avoid replaying already-flushed data
- The MANIFEST provides `min_log_number_to_keep`, which determines which WALs to replay

**⚠️ INVARIANT:** MANIFEST recovery always happens before WAL recovery. This ensures the LSM tree structure is known before replaying writes.

---

## 6. Two-Phase Commit (2PC) Recovery

Two-phase commit allows distributed transactions: a transaction is first **prepared** (logged to WAL but not committed), then later **committed** or **rolled back** based on a distributed coordinator's decision.

### 2PC Recovery Challenge

After a crash, RocksDB must:
1. Recover all prepared transactions (from WAL)
2. Leave them in "prepared" state (not visible to reads)
3. Allow the application to commit or rollback each prepared transaction

**File:** `db/db_impl/db_impl_open.cc:148-153`

```cpp
// Force flush on DB open if 2PC is enabled, since with 2PC we have no
// guarantee that consecutive log files have consecutive sequence id, which
// make recovery complicated.
if (result.allow_2pc) {
  result.avoid_flush_during_recovery = false;
}
```

### 2PC Recovery Flow

```
WAL contains:
  [Write 1]
  [Prepare Transaction T1]  <-- Transaction T1 prepared, not committed
  [Write 2]
  [Commit Transaction T1]   <-- Transaction T1 committed
  [Prepare Transaction T2]  <-- Transaction T2 prepared, not committed
  [CRASH]                   <-- Before T2 could be committed

Recovery:
  |
  +-- Replay all WAL records
  |     +-- [Write 1] -> Insert into memtable
  |     +-- [Prepare T1] -> Mark T1 as prepared (store in DBImpl::recovered_transactions_)
  |     +-- [Write 2] -> Insert into memtable
  |     +-- [Commit T1] -> Commit T1 (now visible)
  |     +-- [Prepare T2] -> Mark T2 as prepared (NOT visible)
  |
  +-- After recovery:
        +-- T1 is committed (visible)
        +-- T2 is prepared but not committed (NOT visible)
        +-- Application can call TransactionDB::GetTransactionByName("T2") and commit or rollback
```

**⚠️ INVARIANT:** Prepared transactions survive crash recovery. After `DB::Open()`, the application can query prepared transactions and decide to commit or rollback.

**Why flush during recovery with 2PC?**
- 2PC can create gaps in sequence numbers (prepared transaction reserves sequence numbers but doesn't make them visible until commit)
- Flushing during recovery ensures all memtable data is persisted before creating new WAL files
- This simplifies subsequent recovery by ensuring WAL files have predictable sequence number ranges

**File:** `db/db_impl/db_impl_open.cc:148-153`

---

## 7. File Consistency Checks

After MANIFEST and WAL recovery, RocksDB verifies that the recovered state is valid.

### SST File Existence

**File:** `db/db_impl/db_impl_open.cc:530-573` (in `DBImpl::Recover()`)

```cpp
if (!immutable_db_options_.best_efforts_recovery) {
  s = versions_->Recover(column_families, read_only, &db_id_,
                         /*no_error_if_files_missing=*/false, ...);
  // Recover() verifies all SST files referenced by MANIFEST exist
} else {
  // BER uses TryRecover() which tolerates missing files
  s = versions_->TryRecover(column_families, read_only, files_in_dbname,
                            &db_id_, &missing_table_file);
}
```

**⚠️ INVARIANT:** Without `best_efforts_recovery`, all SST files referenced by the MANIFEST must exist. Missing files cause recovery failure.

### SST Unique ID Verification

RocksDB can track unique IDs for SST files in the MANIFEST. During recovery, it verifies that files with the same number have matching unique IDs.

**Purpose:** Detect if an SST file was replaced with a different file of the same number (e.g., from restoring a bad backup).

**File:** `include/rocksdb/options.h:1583-1584`

```cpp
// In best_efforts_recovery mode:
// BER can detect when an SST file has been replaced with a different one
// of the same size (assuming SST unique IDs are tracked in DB manifest).
```

### Sequence Number Consistency

Recovery verifies that sequence numbers across WAL files never decrease. After processing each WAL file, `CheckSeqnoNotSetBackDuringRecovery()` confirms the next sequence number has not gone backwards (equal values are permitted for empty WAL files).

**File:** `db/db_impl/db_impl_open.cc:1214-1216`

```cpp
if (status.ok()) {
  status = CheckSeqnoNotSetBackDuringRecovery(prev_next_sequence, *next_sequence);
}
```

**⚠️ INVARIANT:** Sequence numbers must not decrease during WAL replay. `CheckSeqnoNotSetBackDuringRecovery()` returns `Status::Corruption` if the next sequence number goes backwards between WAL files.

---

## 8. Best-Efforts Recovery

`best_efforts_recovery = true` enables aggressive recovery when files are missing or corrupt. Designed for recovering from incomplete physical copies (e.g., partial `rsync`, interrupted backup restore).

**File:** `include/rocksdb/options.h:1561-1608`

### What BER Handles

| Scenario | BER Behavior |
|----------|--------------|
| `CURRENT` file missing | Scan directory for any `MANIFEST-*` file, use the latest |
| SST files missing | Recover to an earlier point-in-time where all files exist |
| Suffix of L0 files missing | Accept missing recent L0 files (recent writes lost), only when `atomic_flush` is disabled |
| MANIFEST truncated | Use the last valid VersionEdit |
| SST file replaced (detected via unique ID) | Treat as missing, recover to earlier point |

### What BER Does NOT Handle

- WAL recovery: BER does not attempt to recover WAL files (`include/rocksdb/options.h:1587`)
- General file corruption: BER is designed for missing/truncated files, not arbitrary corruption
- Atomic flush inconsistency: If atomic flush is enabled, BER cannot recover incomplete atomic groups

### BER Recovery Flow

```
best_efforts_recovery = true:
    |
    +-- CURRENT missing?
    |     +-- YES: Scan for any MANIFEST-* file, use latest
    |     +-- NO: Use CURRENT
    |
    +-- MANIFEST recovery (uses VersionSet::TryRecover() instead of Recover()):
    |     +-- Read MANIFEST records
    |     +-- Apply VersionEdits
    |     +-- If SST file missing:
    |           +-- Try earlier VersionEdit (older point-in-time)
    |           +-- Find latest valid state where all files exist
    |
    +-- Result: Database at some valid point-in-time
    |           (may be older than expected if recent files are missing)
```

**⚠️ INVARIANT:** BER always recovers to a valid point-in-time consistent state, even if data is lost. The recovered state corresponds to some valid MANIFEST snapshot where all referenced files exist.

**Trade-offs:**
- **Pros:** Can recover from incomplete copies, missing files
- **Cons:** May lose recent data, requires careful validation of recovered state

---

## 9. Error Handling During Recovery

Recovery can encounter two classes of errors:

### Fatal Errors (Recovery Fails)

These cause `DB::Open()` to return an error:

| Error | Cause | Recovery Action |
|-------|-------|-----------------|
| `Status::IOError` | Disk I/O failure | Return error, do not open DB |
| `Status::Corruption` (with `kAbsoluteConsistency`) | Any WAL corruption | Return error, do not open DB |
| `Status::NotFound` | SST file missing (without `best_efforts_recovery`) | Return error, do not open DB |
| `Status::InvalidArgument` | Invalid options, incompatible configuration | Return error, do not open DB |

**⚠️ INVARIANT:** If `DB::Open()` returns an error, the database is not opened and no operations are allowed. The database directory is left unchanged (lock released, no new files created).

### Recoverable Errors (Recovery Continues)

These are handled according to configuration:

| Error | Mode | Behavior |
|-------|------|----------|
| Corrupted tail record in WAL | `kTolerateCorruptedTailRecords` | Tolerate, continue recovery |
| WAL corruption | `kPointInTimeRecovery` | Stop at corruption, recover to point-in-time |
| WAL corruption | `kSkipAnyCorruptedRecords` | Skip corrupted record, continue |
| SST file missing | `best_efforts_recovery = true` | Recover to earlier point-in-time |

### Retry on Corruption

RocksDB can retry recovery if the underlying filesystem supports verification and reconstruction reads (e.g., checksummed RAID).

**File:** `db/db_impl/db_impl_open.cc:545-562`

```cpp
if (can_retry) {
  if (!is_retry &&
      (desc_status.IsCorruption() || s.IsNotFound() || s.IsCorruption()) &&
      CheckFSFeatureSupport(fs_.get(),
                            FSSupportedOps::kVerifyAndReconstructRead)) {
    *can_retry = true;  // Retry recovery with FS-level error correction
    ROCKS_LOG_ERROR(immutable_db_options_.info_log,
                    "Possible corruption detected while replaying MANIFEST. "
                    "Will be retried.");
  }
}
```

If the first recovery attempt encounters corruption in the MANIFEST (or missing SST files as a consequence of corrupt MANIFEST records), and the filesystem supports `FSSupportedOps::kVerifyAndReconstructRead` (e.g., RAID with parity checking), RocksDB retries the recovery with filesystem-level error correction enabled.

---

## 10. Atomic Flush Recovery

Atomic flush ensures that multiple column families flush together as an atomic unit. During recovery, atomic flush groups must be recovered atomically (all-or-nothing).

**File:** `include/rocksdb/options.h:1573-1578`

### Atomic Flush and Recovery

```
Atomic flush group: {CF1, CF2, CF3} flushed together
    |
    +-- MANIFEST contains "AtomicGroup" VersionEdit marking the group
    |
    +-- During recovery:
          +-- All CFs in the group must be recovered to the same atomic flush
          +-- If any CF is missing files from the atomic group:
                |
                +-- WITH best_efforts_recovery: Entire atomic group is rejected,
                |                               recover to earlier point
                |
                +-- WITHOUT best_efforts_recovery: Recovery fails
```

**⚠️ INVARIANT:** Atomic flush groups are recovered atomically. Either all column families in the group recover to the atomic flush point, or none do (recover to an earlier point).

**Challenge with best-efforts recovery:**
- If one CF in an atomic group has missing files, the entire group cannot be applied
- All CFs must roll back to the point before the atomic group
- This can block recovery of otherwise-healthy CFs if one CF has invalid state

**File:** `include/rocksdb/options.h:1573-1578`

---

## 11. Post-Recovery Flush

After WAL recovery, RocksDB optionally flushes recovered memtables to SST files.

**File:** `db/db_impl/db_impl_open.cc:148-153`

### avoid_flush_during_recovery

| Setting | Behavior |
|---------|----------|
| `false` (default) | Flush recovered memtables immediately after recovery |
| `true` | Leave recovered memtables in memory, defer flush to normal background operations |

**Why flush during recovery?**
- Durability: Recovered data is persisted to SST files immediately
- WAL cleanup: After flush, old WAL files can be deleted
- Consistent state: Flushing ensures all CFs have the same recovery point

**Why skip flush during recovery (`avoid_flush_during_recovery = true`)?**
- Faster open time: Recovery completes sooner
- Reduced write amplification: Avoid flushing small amounts of recovered data

**Note:** Even with `avoid_flush_during_recovery = true`, flushes may still occur during recovery if `enforce_write_buffer_manager_during_recovery = true` (the default) and a `WriteBufferManager` is configured. When the global memory limit is reached during WAL replay, a flush is triggered. Once any such WBM-triggered flush occurs, all remaining non-empty memtables will also be flushed at the end of recovery.

**File:** `include/rocksdb/options.h:1415-1443`

**⚠️ INVARIANT:** With `allow_2pc = true`, `avoid_flush_during_recovery` is forced to `false`. This ensures prepared transactions are flushed and WAL sequence numbers are consecutive.

---

## Summary

Crash recovery ensures RocksDB can restore a consistent database state after unexpected shutdown:

1. **MANIFEST recovery** reconstructs the LSM tree structure (which SST files exist, at which levels)
2. **WAL recovery** replays committed writes that weren't yet flushed
3. **WALRecoveryMode** controls corruption tolerance (strict vs. point-in-time vs. best-effort)
4. **2PC recovery** restores prepared transactions for distributed commit/rollback
5. **Best-efforts recovery** handles missing files by recovering to an earlier valid point-in-time
6. **Consistency checks** verify SST files exist and sequence numbers are valid
7. **Post-recovery flush** optionally persists recovered data immediately

**Key invariants:**
- ⚠️ All committed writes (with `Write()` returning OK) are recovered (unless using lossy recovery modes like `kPointInTimeRecovery` or `kSkipAnyCorruptedRecords`)
- ⚠️ MANIFEST recovery always precedes WAL recovery
- ⚠️ Sequence numbers never decrease across WAL files during replay
- ⚠️ Atomic flush groups are recovered atomically (all-or-nothing)
- ⚠️ Recovery is atomic: either `DB::Open()` succeeds with a consistent DB, or fails with no side effects

For more details, see:
- [ARCHITECTURE.md](../../ARCHITECTURE.md) for the overall database structure
- [write_flow.md](write_flow.md) for WAL write mechanics
- [version_management.md](version_management.md) for MANIFEST and VersionEdit details
- [transaction.md](transaction.md) for 2PC transaction implementation

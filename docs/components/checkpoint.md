# Checkpoint and Backup System

RocksDB provides two mechanisms for creating consistent snapshots of a database: **Checkpoint** for fast, local, point-in-time snapshots, and **BackupEngine** for durable, incremental backups with remote storage support.

```
┌─────────────────────────────────────────────────────────────────┐
│                    Snapshot Mechanisms                          │
├──────────────────────────────┬──────────────────────────────────┤
│        Checkpoint            │         BackupEngine             │
│  (Local, Fast, Hard-link)    │  (Durable, Incremental, Remote)  │
└──────────────────────────────┴──────────────────────────────────┘
```

---

## 1. Checkpoint: Fast Local Snapshots

Checkpoint creates an **openable snapshot** of RocksDB at a point in time using hard links when possible.

### Public API

**include/rocksdb/utilities/checkpoint.h:22-63**

```cpp
class Checkpoint {
  // Create a Checkpoint object
  static Status Create(DB* db, Checkpoint** checkpoint_ptr);

  // Create an openable snapshot in checkpoint_dir
  virtual Status CreateCheckpoint(
      const std::string& checkpoint_dir,
      uint64_t log_size_for_flush = 0,
      uint64_t* sequence_number_ptr = nullptr);

  // Export a single column family
  virtual Status ExportColumnFamily(
      ColumnFamilyHandle* handle,
      const std::string& export_dir,
      ExportImportFilesMetaData** metadata);
};
```

### How Checkpoint Works

**utilities/checkpoint/checkpoint_impl.cc:92-217**

```
1. Verify checkpoint_dir doesn't exist
2. Create temporary directory (.tmp suffix)
3. DisableFileDeletions()
   └─> Increment disable_delete_obsolete_files_ counter
4. GetLiveFilesStorageInfo()
   └─> Captures all live SST, blob, MANIFEST, CURRENT, OPTIONS files
5. For each file:
   ├─ SST/blob files: Hard link (if same filesystem) or copy
   ├─ MANIFEST/CURRENT: Always copy
   └─ CURRENT file: Replace contents with correct MANIFEST pointer
6. EnableFileDeletions()
7. Atomic rename: .tmp → checkpoint_dir
8. Fsync directory for durability
```

⚠️ **INVARIANT**: Checkpoint directory must NOT exist before creation. The API returns `Status::InvalidArgument("Directory exists")` if it already exists.

⚠️ **INVARIANT**: `db_paths` and `cf_paths` are NOT supported. Using multiple directories for DB data (excluding WAL) will return `Status::NotSupported`.

⚠️ **INVARIANT**: File deletions remain disabled during the entire hard-linking/copying phase to ensure files don't disappear mid-checkpoint.

### Hard Link vs Copy Strategy

**utilities/checkpoint/checkpoint_impl.cc:277-300**

The implementation attempts hard links first, falling back to copy:

```cpp
if (same_fs && !info.trim_to_size) {
  s = link_file_cb(info.directory, info.relative_filename, info.file_type);
  if (s.IsNotSupported()) {
    same_fs = false;  // Cross-device, use copy for remaining files
    s = Status::OK();
  }
}
if (!same_fs || info.trim_to_size) {
  s = copy_file_cb(info.directory, info.relative_filename, info.size, ...);
}
```

- **Hard link**: Fast, no data copy, same inode
- **Copy**: Used when destination is on different filesystem or when file needs trimming (WAL tail)
- **Decision point**: First file determines strategy for all remaining files (except when `trim_to_size=true`)

### WAL Handling and `log_size_for_flush`

**include/rocksdb/utilities/checkpoint.h:34-41**

```cpp
// log_size_for_flush: if total log file size >= this value,
// flush is triggered for all column families.
// Default: 0 (always flush)
```

- **log_size_for_flush=0** (default): Always flush before checkpoint → checkpoint contains all data, minimal WAL
- **log_size_for_flush>0**: Flush only if WAL size exceeds threshold → faster checkpoint, but larger WAL in snapshot
- **2PC mode**: Always flushes regardless of setting to ensure transaction consistency

WAL tail (unflushed portion) is always **copied** (never hard-linked) and may be **trimmed** to the exact sequence number.

### ExportColumnFamily

**utilities/checkpoint/checkpoint_impl.cc:312-449**

Exports a single column family's SST files to a directory:

```
1. Verify export_dir doesn't exist
2. Create temporary directory (.tmp suffix)
3. Flush the column family (always, to ensure completeness)
4. DisableFileDeletions()
5. GetColumnFamilyMetaData() to get SST file list
6. Hard link or copy SST files (same logic as CreateCheckpoint)
7. EnableFileDeletions()
8. Rename .tmp → export_dir
9. Return metadata with file info (sequence numbers, key ranges)
```

**Use case**: Migrating a single column family to another database instance.

⚠️ **INVARIANT**: Always triggers a flush. Unlike `CreateCheckpoint`, there's no option to skip flushing.

---

## 2. BackupEngine: Incremental Durable Backups

BackupEngine provides **incremental backups** with support for remote storage, rate limiting, and parallel operations.

### Public API

**include/rocksdb/utilities/backup_engine.h:703-728**

```cpp
class BackupEngine {
  static IOStatus Open(const BackupEngineOptions& options,
                       Env* db_env,
                       BackupEngine** backup_engine_ptr);

  // Create incremental backup
  IOStatus CreateNewBackup(const CreateBackupOptions& options,
                          DB* db,
                          BackupID* new_backup_id = nullptr);

  // Restore from backup
  IOStatus RestoreDBFromBackup(const RestoreOptions& options,
                              BackupID backup_id,
                              const std::string& db_dir,
                              const std::string& wal_dir) const;

  // Manage backups
  IOStatus PurgeOldBackups(uint32_t num_backups_to_keep);
  IOStatus DeleteBackup(BackupID backup_id);
  IOStatus VerifyBackup(BackupID backup_id, bool verify_with_checksum);
};
```

### BackupEngineOptions

**include/rocksdb/utilities/backup_engine.h:34-265**

| Option | Default | Description |
|--------|---------|-------------|
| `backup_dir` | Required | Directory for storing backups (can be remote) |
| `backup_env` | nullptr | Separate Env for backup I/O (enables remote storage) |
| `share_table_files` | true | Share SST/blob files across backups (incremental) |
| `share_files_with_checksum` | true | Use content-based naming for shared files |
| `backup_rate_limit` | 0 | Max bytes/sec during backup (0=unlimited) |
| `restore_rate_limit` | 0 | Max bytes/sec during restore (0=unlimited) |
| `max_background_operations` | 1 | Thread pool size for parallel file operations |
| `sync` | true | Fsync after each file write for durability |
| `backup_log_files` | true | Include WAL files in backup |
| `schema_version` | 1 | Metadata format version (2 for temperature support) |

⚠️ **INVARIANT**: `share_files_with_checksum` must be true when `share_table_files=true`. Setting it to false is DEPRECATED and can lose data if backing up DBs with divergent history.

### Backup Directory Structure

```
backup_dir/
├── meta/
│   ├── 1              # Backup #1 metadata
│   ├── 2              # Backup #2 metadata
│   └── 3              # Backup #3 metadata
├── shared/            # Shared files (when share_table_files=false)
│   ├── 000001.sst
│   └── 000002.sst
├── shared_checksum/   # Content-addressed shared files (share_files_with_checksum=true)
│   ├── 000123_s<session_id>_<size>.sst
│   ├── 000456_s<session_id>_<size>.sst
│   └── 000789_<crc32c>_<size>.blob    # Blob files use legacy naming
├── private/
│   ├── 1/             # Private files for backup #1 (e.g., MANIFEST, CURRENT, OPTIONS)
│   ├── 2/
│   └── 3/
└── LATEST_BACKUP      # Points to latest valid backup ID
```

### Shared File Naming

**include/rocksdb/utilities/backup_engine.h:154-197**

When `share_files_with_checksum=true`, files are named to ensure uniqueness:

```cpp
enum ShareFilesNaming {
  // Legacy: <file_number>_<crc32c>_<file_size>.sst
  // Problem: Requires reading entire file to compute checksum before knowing if backup needed
  kLegacyCrc32cAndFileSize = 1U,

  // Modern (default): <file_number>_s<db_session_id>_<file_size>.sst
  // Advantage: Uniqueness determined without reading file; faster incremental backups
  // Blob files still use legacy (no session ID in blob format)
  kUseDbSessionId = 2U,
};
```

**Default**: `kUseDbSessionId | kFlagIncludeFileSize`

This naming prevents:
- **Collision**: Different files with same number across different DB histories
- **Unnecessary I/O**: Can determine if file is already backed up without reading it

### Creating Backups

**CreateNewBackup workflow**:

```
1. Lock backup directory (prevents concurrent backup writes)
2. Get live files from DB via GetLiveFilesStorageInfo()
3. For each file:
   ├─ SST/blob (shared):
   │  ├─ Compute name based on ShareFilesNaming scheme
   │  ├─ If file exists in shared_checksum/ → reuse (incremental!)
   │  └─ Else → copy with checksum verification
   ├─ MANIFEST/CURRENT/OPTIONS (private):
   │  └─ Always copy to private/<backup_id>/
   └─ WAL files (if backup_log_files=true):
      └─ Copy to private/<backup_id>/
4. Write metadata file: meta/<backup_id>
5. Update LATEST_BACKUP
6. Fsync directory (if sync=true)
```

**Rate limiting** applies during file copy to avoid overwhelming network/disk I/O.

⚠️ **INVARIANT**: Backup metadata is written **atomically** only after all files are successfully copied. Incomplete backups are detected and cleaned up on next BackupEngine open.

### CreateBackupOptions

**include/rocksdb/utilities/backup_engine.h:308-349**

```cpp
struct CreateBackupOptions {
  // Flush memtable before backup (ensures all data captured)
  // Always true if 2PC enabled
  bool flush_before_backup = false;

  // Progress callback (called every callback_trigger_interval_size bytes)
  std::function<void()> progress_callback = {};

  // Exclude files known to exist in alternate backup (advanced)
  std::function<void(MaybeExcludeBackupFile* files_begin,
                     MaybeExcludeBackupFile* files_end)>
      exclude_files_callback = {};

  // Lower CPU priority for background threads
  bool decrease_background_thread_cpu_priority = false;
  CpuPriority background_thread_cpu_priority = CpuPriority::kNormal;
};
```

**exclude_files_callback** enables **distributed backups**: files can be excluded from this backup if known to exist in another backup directory. Restore requires providing `RestoreOptions::alternate_dirs`.

### Restoring Backups

**include/rocksdb/utilities/backup_engine.h:351-404**

```cpp
struct RestoreOptions {
  enum Mode {
    // Keep existing files with matching db_session_id (fastest, for healthy DB)
    kKeepLatestDbSessionIdFiles = 1U,

    // Verify checksums, replace only corrupted files (for suspected corruption)
    kVerifyChecksum = 2U,

    // Delete everything, restore all files (slowest, zero trust)
    kPurgeAllFiles = 0xffffU,
  };

  bool keep_log_files = false;  // Preserve existing WAL files
  std::forward_list<BackupEngineReadOnlyBase*> alternate_dirs;  // For excluded files
  Mode mode = kPurgeAllFiles;
};
```

**RestoreDBFromBackup workflow**:

```
1. Lock db_dir and wal_dir
2. Based on mode:
   ├─ kPurgeAllFiles: Delete all files in db_dir/wal_dir
   ├─ kVerifyChecksum: Scan existing files, keep if checksum matches
   └─ kKeepLatestDbSessionIdFiles: Keep files with matching session ID
3. Copy/link files from backup_dir to db_dir/wal_dir
   ├─ Shared files: Copy from shared_checksum/
   ├─ Private files: Copy from private/<backup_id>/
   └─ If exclude_files_callback was used: Search alternate_dirs
4. Fsync directories
5. Verify DB can open (optional sanity check)
```

⚠️ **INVARIANT**: Restored DB directory must be **quiesced** (no active DB instance) before restore. Restoring over a live DB will corrupt it.

### Backup Consistency Guarantees

**Atomicity**: Backup metadata (meta/<backup_id>) is written only after all files are successfully copied. If backup is interrupted:
- Partial files in shared_checksum/ are cleaned up by next GarbageCollect()
- Metadata file absence marks backup as incomplete
- `GetBackupInfo()` excludes incomplete/corrupt backups

**Checksums**: Each file's checksum is stored in metadata and verified during:
- **Backup creation**: After copying, compare checksum
- **Restore**: Optionally verify checksums if `verify_with_checksum=true` in `VerifyBackup()`
- **Incremental backup**: Shared files reused only if checksum matches

**Consistency point**: The sequence number stored in backup metadata corresponds to a consistent DB state. For 2PC, this includes all prepared transactions.

---

## 3. Checkpoint vs Backup: When to Use Which

| Aspect | Checkpoint | BackupEngine |
|--------|-----------|--------------|
| **Speed** | Very fast (hard links, no data copy for SSTs) | Slower (always copies data) |
| **Durability** | Local filesystem only | Supports remote storage (S3, HDFS, etc.) |
| **Incremental** | No (every checkpoint is full snapshot) | Yes (shared files reused across backups) |
| **Rate limiting** | No | Yes (backup_rate_limit, restore_rate_limit) |
| **Parallelism** | Single-threaded | Multi-threaded (max_background_operations) |
| **Space efficiency** | Shared inodes (hard links) | Deduplicated by content (shared_checksum) |
| **Remote access** | No | Yes (via backup_env) |
| **WAL handling** | Copies tail, supports log_size_for_flush | Copies all WAL files |
| **Metadata** | None (directory is the checkpoint) | Rich metadata (timestamps, size, checksums) |
| **Atomicity** | Atomic directory rename | Atomic metadata write |
| **Use case** | Local testing, point-in-time restore, replication | Disaster recovery, long-term archival, cloud backup |

**Rule of thumb**:
- Use **Checkpoint** for: Fast local snapshots, testing, replication to nearby nodes, temporary clones
- Use **BackupEngine** for: Production disaster recovery, cross-region backup, compliance/archival, backup to object storage

---

## 4. Common Patterns

### Point-in-Time Snapshot for Testing

```cpp
// Create a checkpoint for testing without disrupting production DB
Checkpoint* checkpoint;
Status s = Checkpoint::Create(db, &checkpoint);
assert(s.ok());

uint64_t snapshot_sequence;
s = checkpoint->CreateCheckpoint("/tmp/test_snapshot", 0, &snapshot_sequence);
assert(s.ok());

// Open the checkpoint as a separate read-only DB
DB* snapshot_db;
s = DB::OpenForReadOnly(options, "/tmp/test_snapshot", &snapshot_db);
// Run tests on snapshot_db without affecting production
```

### Incremental Backup to S3

```cpp
// Set up backup to S3 using custom Env
BackupEngineOptions backup_opts("/mnt/s3/backups");
backup_opts.backup_env = s3_env;  // Custom Env for S3
backup_opts.share_table_files = true;  // Enable incremental backups
backup_opts.backup_rate_limit = 100 * 1024 * 1024;  // 100 MB/s
backup_opts.max_background_operations = 8;  // Parallel uploads

BackupEngine* backup_engine;
IOStatus s = BackupEngine::Open(backup_opts, db->GetEnv(), &backup_engine);
assert(s.ok());

// Create backup (only new SST files uploaded)
BackupID backup_id;
CreateBackupOptions create_opts;
create_opts.flush_before_backup = true;
s = backup_engine->CreateNewBackup(create_opts, db, &backup_id);
```

### Disaster Recovery

```cpp
// Restore latest backup from remote storage
BackupEngineOptions restore_opts("/mnt/s3/backups");
restore_opts.backup_env = s3_env;
restore_opts.restore_rate_limit = 200 * 1024 * 1024;  // 200 MB/s

BackupEngine* backup_engine;
IOStatus s = BackupEngine::Open(restore_opts, env, &backup_engine);

// Restore to local disk
RestoreOptions opts;
opts.mode = RestoreOptions::kPurgeAllFiles;  // Clean restore
s = backup_engine->RestoreDBFromLatestBackup(opts, "/mnt/nvme/db", "/mnt/nvme/wal");
assert(s.ok());

// Open restored DB
DB* restored_db;
Status open_s = DB::Open(db_options, "/mnt/nvme/db", &restored_db);
```

### Database Migration

```cpp
// Export a single column family for migration
Checkpoint* checkpoint;
Checkpoint::Create(db, &checkpoint);

ColumnFamilyHandle* cf_handle = db->GetColumnFamilyHandle("user_data");
ExportImportFilesMetaData* metadata;

Status s = checkpoint->ExportColumnFamily(cf_handle, "/export/user_data", &metadata);
assert(s.ok());

// metadata contains:
// - SST file names and paths
// - Sequence number ranges
// - Key ranges (smallest, largest)
// - Comparator name (for validation)

// Import into another DB using ImportColumnFamily()
```

---

## 5. File Deletion Protection

Both Checkpoint and BackupEngine rely on **DisableFileDeletions()** to prevent files from being deleted during snapshot creation.

**db/db_impl/db_impl_files.cc:54-78**

```cpp
Status DBImpl::DisableFileDeletions() {
  InstrumentedMutexLock l(&mutex_);
  ++disable_delete_obsolete_files_;  // Reference counting
  return Status::OK();
}

Status DBImpl::EnableFileDeletions() {
  JobContext job_context(0);
  InstrumentedMutexLock l(&mutex_);
  if (disable_delete_obsolete_files_ > 0) {
    --disable_delete_obsolete_files_;
  }
  if (disable_delete_obsolete_files_ == 0) {
    FindObsoleteFiles(&job_context, true);
    bg_cv_.SignalAll();
  }
  // ... purge obsolete files if counter reached 0
}
```

⚠️ **INVARIANT**: DisableFileDeletions() uses **reference counting**. Multiple concurrent checkpoints/backups are safe; file deletions resume only when all operations complete.

⚠️ **INVARIANT**: File deletion prevention does **NOT** prevent compaction. Compaction continues, creating new files. Only the deletion of obsolete files is postponed.

**Lifecycle**:
```
Normal operation: Compaction generates new SST files, marks old ones obsolete
                 ↓
DisableFileDeletions(): Obsolete files marked but NOT deleted
                       (accumulate in pending_obsolete_files_)
                 ↓
Checkpoint/Backup: Safe to hard-link or copy files
                 ↓
EnableFileDeletions(): Purge all accumulated obsolete files
                 ↓
Resume normal operation
```

This prevents the race:
```
Thread 1: GetLiveFilesStorageInfo() says file X is live
Thread 2: Compaction completes, marks X obsolete, deletes X
Thread 1: Tries to link/copy X → NotFound error!
```

---

## 6. Advanced Topics

### GetLiveFilesStorageInfo

**include/rocksdb/db.h:1907-1909**

Core API used by both Checkpoint and BackupEngine:

```cpp
virtual Status GetLiveFilesStorageInfo(
    const LiveFilesStorageInfoOptions& opts,
    std::vector<LiveFileStorageInfo>* files);
```

Returns metadata for all live files:
- **SST files**: All files in current Version
- **Blob files**: Referenced by live SSTs
- **MANIFEST**: Current manifest file
- **CURRENT**: Pointer to manifest
- **OPTIONS**: Options file
- **WAL files**: Active and archived WAL files

**LiveFileStorageInfo** includes:
- `directory`: Absolute path to file directory
- `relative_filename`: Filename (e.g., "000123.sst")
- `size`: File size in bytes
- `file_type`: kTableFile, kWalFile, kDescriptorFile, etc.
- `trim_to_size`: True for WAL tail that needs trimming
- `replacement_contents`: For CURRENT file (needs updated MANIFEST pointer)
- `file_checksum`: Checksum value (if requested)
- `temperature`: Storage tier hint (hot/warm/cold)

### Backup Verification

**include/rocksdb/utilities/backup_engine.h:553-569**

```cpp
IOStatus VerifyBackup(BackupID backup_id, bool verify_with_checksum = false);
```

- **verify_with_checksum=false**: Check file existence and size only (fast)
- **verify_with_checksum=true**: Recompute checksums and compare with metadata (slow, thorough)

Detects:
- Missing files
- Truncated files (size mismatch)
- Corrupted files (checksum mismatch)
- Incomplete backups (missing metadata)

### Concurrent Operations

**include/rocksdb/utilities/backup_engine.h:659-701**

BackupEngine uses read-write lock for thread safety:

| Operation Type | Read | Append | Write |
|----------------|------|--------|-------|
| **Read** (GetBackupInfo, RestoreDBFromBackup, VerifyBackup) | ✓ Concurrent | ✗ Blocks | ✗ Blocks |
| **Append** (CreateNewBackup, GarbageCollect) | ✗ Blocks | ✗ Blocks | ✗ Blocks |
| **Write** (DeleteBackup, PurgeOldBackups) | ✗ Blocks | ✗ Blocks | ✗ Blocks |

**Checkpoint** has no built-in concurrency protection. Multiple concurrent `CreateCheckpoint()` calls on the same DB are safe (each uses separate temp directory), but caller must ensure unique `checkpoint_dir` paths.

---

## 7. Error Handling and Edge Cases

### Checkpoint Failures

**utilities/checkpoint/checkpoint_impl.cc:205-216**

If checkpoint creation fails mid-operation:
```cpp
// Clean all files and directory we might have created
Status del_s = CleanStagingDirectory(full_private_path, info_log);
```

Cleanup ensures no partial checkpoint left on disk. Temporary directory (.tmp) is removed.

**Common failures**:
- `InvalidArgument`: checkpoint_dir already exists or invalid path
- `NotSupported`: db_paths/cf_paths used (multiple directories)
- `IOError`: Disk full, permission denied, filesystem error
- `Corruption`: Inconsistent file metadata during GetLiveFilesStorageInfo

### Backup Failures

Incomplete backups are **safe** but occupy space until cleaned up:

```cpp
IOStatus s = backup_engine->CreateNewBackup(opts, db);
if (!s.ok()) {
  // Backup failed, but backup_dir is still consistent
  // Incomplete backup will be cleaned up on next:
  // - CreateNewBackup() attempt
  // - GarbageCollect() call
  // - BackupEngine::Open() with destroy_old_data=true
}
```

**GarbageCollect()** removes:
- Files from incomplete backups (no metadata file)
- Orphaned files not referenced by any backup metadata
- Temporary files from crashed operations

⚠️ **CAUTION**: GarbageCollect() can delete files needed to manually recover corrupt backups or preserve future-version backups. Use with care.

### Session ID Collisions

**include/rocksdb/utilities/backup_engine.h:169-181**

With `kUseDbSessionId` naming, SST files are named `<number>_s<session_id>_<size>.sst`. Session IDs are globally unique (generated from timestamp + random), making collisions astronomically rare.

**Fallback**: SST files without session ID (old RocksDB versions) use legacy `kLegacyCrc32cAndFileSize` naming automatically.

---

## 8. Performance Considerations

### Checkpoint Performance

**Bottlenecks**:
1. **DisableFileDeletions → GetLiveFilesStorageInfo**: O(# live files), typically <100ms
2. **Hard linking SSTs**: O(# SST files), very fast (metadata operation, ~1μs per file)
3. **Copying MANIFEST**: O(MANIFEST size), typically <10ms
4. **Fsync**: O(1), 1-100ms depending on storage

**Optimization**: Use `log_size_for_flush > 0` to skip flush if WAL is small. Reduces checkpoint latency at cost of larger WAL in snapshot.

**Typical latency**: 10-500ms for local NVMe, scales with # of files (metadata operations) not data size.

### BackupEngine Performance

**Bottlenecks**:
1. **First backup**: Copy all SST files → bounded by `backup_rate_limit` or network/disk bandwidth
2. **Incremental backup**: Copy only new SST files since last backup → much faster
3. **Checksum computation**: CPU-bound (crc32c), ~1-2 GB/s per core
4. **Parallel operations**: `max_background_operations` threads, but limited by I/O bandwidth

**Optimization**:
- Set `share_table_files=true` and `share_files_with_checksum=true` for incremental backups
- Use `kUseDbSessionId` naming to avoid re-reading files to compute checksums
- Tune `max_background_operations` based on available I/O bandwidth
- Adjust `backup_rate_limit` to avoid saturating production network

**Typical throughput**:
- Local disk: 500 MB/s - 2 GB/s (NVMe)
- Remote storage: 100 MB/s - 1 GB/s (depends on network, provider)
- Incremental: 10-100× faster than full backup (only new files copied)

### Space Amplification

**Checkpoint**:
- Hard links: ~0 bytes (shares inodes with source DB)
- Cross-device copy: ~DB size
- WAL: log_size_for_flush parameter controls

**BackupEngine**:
- First backup: ~DB size
- Incremental (with share_table_files): ~(new data since last backup)
- Multiple backups with 50% overlap: ~1.5× DB size
- Metadata overhead: ~1KB per file per backup

---

## 9. Debugging and Monitoring

### Checkpoint Logs

```
[INFO] Started the snapshot process -- creating snapshot in directory /path/to/checkpoint
[INFO] Snapshot process -- using temporary directory /path/to/.tmp
[INFO] Hard Linking 000123.sst
[INFO] Copying MANIFEST-000456
[INFO] Creating CURRENT
[INFO] Snapshot DONE. All is good
[INFO] Snapshot sequence number: 123456789
```

### Backup Logs

```
[INFO] Backup Engine: Creating new backup
[INFO] File 000123.sst already in shared_checksum, skipping copy
[INFO] Copying 000456.sst (rate limited to 100 MB/s)
[INFO] Backup 5 complete: 1024 files, 10.5 GB
```

### Key Metrics to Monitor

**Checkpoint**:
- Checkpoint creation latency (p50, p99)
- File deletion disable duration
- Checkpoint directory size

**BackupEngine**:
- Backup creation latency
- Incremental backup ratio (% files reused)
- Backup directory size
- Failed backups (track IOStatus)
- Verification failures

---

## 10. Testing and Validation

### Checkpoint Testing

```cpp
// Verify checkpoint is openable and consistent
uint64_t checkpoint_seq;
checkpoint->CreateCheckpoint("/tmp/chk", 0, &checkpoint_seq);

DB* chk_db;
Status s = DB::OpenForReadOnly(options, "/tmp/chk", &chk_db);
assert(s.ok());

// Verify sequence number
assert(chk_db->GetLatestSequenceNumber() == checkpoint_seq);

// Spot-check key-value pairs
std::string value;
s = chk_db->Get(ReadOptions(), "key1", &value);
assert(s.ok() && value == "expected_value");
```

### Backup Testing

```cpp
// Create backup, corrupt original DB, restore, verify
BackupID backup_id;
backup_engine->CreateNewBackup(cbo, db, &backup_id);

// Simulate disaster: delete DB directory
DestroyDB("/path/to/db", options);

// Restore
backup_engine->RestoreDBFromBackup(RestoreOptions(), backup_id,
                                   "/path/to/db", "/path/to/db");

// Verify restoration
DB* restored_db;
DB::Open(options, "/path/to/db", &restored_db);
// Check key-value pairs, sequence number, etc.
```

### Stress Testing

- **db_stress**: Includes checkpoint and backup verification (--test_checkpoint, --test_backup)
- **Concurrent operations**: Multiple threads creating checkpoints/backups simultaneously
- **Failure injection**: Simulate I/O errors, disk full, crashes mid-operation
- **Cross-platform**: Test on Linux, Windows, macOS to verify filesystem assumptions

---

## Key Takeaways

1. **Checkpoint** is for fast, local, point-in-time snapshots using hard links
2. **BackupEngine** is for durable, incremental, remote-capable backups
3. Both rely on **DisableFileDeletions** for snapshot consistency
4. Checkpoint uses **hard links** (same filesystem) or **copy** (cross-device)
5. BackupEngine uses **content-based deduplication** for incremental backups
6. **Session ID naming** (`kUseDbSessionId`) avoids re-reading files for checksums
7. Restore modes offer trade-offs: **kPurgeAllFiles** (safe), **kVerifyChecksum** (efficient), **kKeepLatestDbSessionIdFiles** (fastest)
8. Both mechanisms are **atomic**: incomplete operations leave DB/backup directory in consistent state

**Related Documentation**:
- `ARCHITECTURE.md`: High-level RocksDB architecture
- `write_flow.md`: WAL and memtable lifecycle
- `flush_and_read_path.md`: FlushJob and file generation
- `version_management.md`: Version, MANIFEST, and file tracking

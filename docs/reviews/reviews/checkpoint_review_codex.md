# Review: checkpoint — Codex

## Summary
Overall quality rating: significant issues

The chapter layout is strong and the index follows the expected overview/source-files/chapter-table/characteristics/invariants pattern. Several workflow descriptions also line up well with current tests, especially around checkpoint openness, CURRENT-file races, and BackupEngine concurrency categories.

The main problem is accuracy in the exact places where a maintainer would rely on this doc for implementation work: which files are linked vs copied, what session-ID naming really saves, what `flush_before_backup` actually changes, how incomplete backups and GC are represented, and what restore/verify modes truly guarantee. The performance chapter also contains many concrete numbers that are not backed by code or in-tree benchmark references.

## Correctness Issues

### [WRONG] Checkpoint docs say the OPTIONS file is always copied
- **File:** `01_checkpoint_api.md`, Hard Link vs Copy Strategy
- **Claim:** "`| OPTIONS | Copy | Metadata file |`"
- **Reality:** During checkpoint creation, OPTIONS behaves like any other non-trimmed file. If `same_fs` is still true and `trim_to_size` is false, `CreateCustomCheckpoint()` will hard-link it. It is only copied after cross-device fallback.
- **Source:** `db/db_filesnapshot.cc` `DBImpl::GetLiveFilesStorageInfo`; `utilities/checkpoint/checkpoint_impl.cc` `CheckpointImpl::CreateCustomCheckpoint`
- **Fix:** Document OPTIONS as hard-linked on same filesystem for Checkpoint, but always copied for BackupEngine because BackupEngine's link callback always returns `NotSupported`.

### [WRONG] Session-ID naming is documented as requiring no file I/O
- **File:** `03_backup_engine_config.md`, SharedFilesNaming; `04_creating_backups.md`, Incremental Backup Mechanism
- **Claim:** "`| `kUseDbSessionId` (default) | `<number>_s<session_id>_<size>.sst` | No file I/O needed to determine name; strong uniqueness via session ID |`" and "`both of which are available without reading the file`"
- **Reality:** The code reads SST table properties to obtain `db_session_id`. `AddBackupFileWorkItem()` calls `GetFileDbIdentities()`, which uses `SstFileDumper` to open the SST and read metadata before naming the backup file.
- **Source:** `utilities/backup/backup_engine.cc` `BackupEngineImpl::AddBackupFileWorkItem`; `utilities/backup/backup_engine.cc` `BackupEngineImpl::GetFileDbIdentities`
- **Fix:** Say session-ID naming avoids reading the whole file just to compute a naming checksum, but it still performs file I/O to read table properties. Also note blob files and old SSTs fall back to legacy checksum-based naming.

### [MISLEADING] The docs describe `shared_checksum/` and default SST naming as content-addressed
- **File:** `index.md`, Overview; `03_backup_engine_config.md`, Backup Directory Layout; `09_performance.md`, Checkpoint vs BackupEngine / Space Efficiency
- **Claim:** "`BackupEngine` ... sharing SST/blob files across backups via content-based naming", "`shared_checksum/` | `Content-addressed SST/blob files`", and "`Deduplicated by content (`shared_checksum/`)`"
- **Reality:** With the default naming scheme, SST names are based on file number, DB session ID, and optionally file size, not content. Blob files still use checksum-and-size naming, but default SST naming is not content-addressed.
- **Source:** `utilities/backup/backup_engine.cc` `BackupEngineImpl::GetSharedFileWithChecksum`
- **Fix:** Describe reuse as shared-file deduplication keyed by the configured naming scheme. Be explicit that default SST naming is session-ID-based, while blob files and old SSTs use legacy checksum-based names.

### [MISLEADING] `flush_before_backup` is described as necessary to capture all data
- **File:** `04_creating_backups.md`, CreateBackupOptions
- **Claim:** "`| `flush_before_backup` | false | Flush memtables before backup to ensure all data is captured |`"
- **Reality:** With WAL enabled, `flush_before_backup = false` still captures committed data because BackupEngine intentionally skips the pre-flush and backs up the relevant WALs. The public header only calls out `flush_before_backup = true` as necessary when WAL writing is disabled.
- **Source:** `include/rocksdb/utilities/backup_engine.h` `CreateBackupOptions`; `utilities/backup/backup_engine.cc` `BackupEngineImpl::CreateNewBackupWithMetadata`; `db/db_filesnapshot.cc` `DBImpl::GetLiveFilesStorageInfo`
- **Fix:** Rephrase this as "force a memtable flush before snapshot." Note that it is required for no-WAL configurations and otherwise mainly reduces WAL dependence in the backup.

### [WRONG] `ExportColumnFamily()` fallback behavior is documented like Checkpoint, but the implementation only probes the first file
- **File:** `02_export_column_family.md`, ExportColumnFamily Workflow
- **Claim:** "`the first `NotSupported` from a hard-link attempt flips the strategy to copy for all remaining files`"
- **Reality:** `ExportFilesInMetaData()` only switches to copy mode when the **first** file returns `NotSupported`. If a later file returns `NotSupported`, export fails instead of falling back.
- **Source:** `utilities/checkpoint/checkpoint_impl.cc` `CheckpointImpl::ExportFilesInMetaData`
- **Fix:** Document that the first table file is the cross-device probe. If later files fail to link, the export fails.

### [WRONG] The docs say missing metadata-backed backups are tracked as incomplete backups and returned by `GetCorruptedBackups()`
- **File:** `08_error_handling.md`, Incomplete Backup Detection; `04_creating_backups.md`, Managing Backups
- **Claim:** "`If the `meta/<backup_id>` metadata file is missing for files found in `private/<backup_id>/` or referenced in `shared_checksum/`, the backup is marked as incomplete.`", "`Incomplete backups are excluded from `GetBackupInfo()` results and included in `GetCorruptedBackups()` results.`", and "`Get corrupt | `GetCorruptedBackups()` | Lists backup IDs that are incomplete or corrupt`"
- **Reality:** Open loads backup IDs only from files found under `meta/`. Orphaned `private/` or `shared*` files with no metadata file are just garbage for GC; they are not represented as a backup ID and cannot appear in `GetCorruptedBackups()`. `GetCorruptedBackups()` simply enumerates `corrupt_backups_`, which comes from failed metadata loads.
- **Source:** `utilities/backup/backup_engine.cc` `BackupEngineImpl::Initialize`; `utilities/backup/backup_engine.cc` `BackupEngineImpl::GetCorruptedBackups`
- **Fix:** Split the behavior into two cases: corrupt backups with metadata vs orphaned files left by interrupted operations.

### [MISLEADING] The docs overstate when BackupEngine automatically runs garbage collection
- **File:** `08_error_handling.md`, Garbage Collection / Backup Creation Failures
- **Claim:** "`Garbage collection is also triggered implicitly during `CreateNewBackup()`, `PurgeOldBackups()`, and `DeleteBackup()`.`"
- **Reality:** `PurgeOldBackups()` and `DeleteBackup()` only call `GarbageCollect()` when `might_need_garbage_collect_` is set. `CreateNewBackup()` only calls `GarbageCollect()` in special cases, such as finding a leftover private directory for the next backup ID.
- **Source:** `utilities/backup/backup_engine.cc` `BackupEngineImpl::CreateNewBackupWithMetadata`; `utilities/backup/backup_engine.cc` `BackupEngineImpl::PurgeOldBackups`; `utilities/backup/backup_engine.cc` `BackupEngineImpl::DeleteBackup`
- **Fix:** Document GC as conditional cleanup, not something every create/delete/purge necessarily runs.

### [MISLEADING] `kKeepLatestDbSessionIdFiles` is documented as matching only session IDs
- **File:** `05_restoring_backups.md`, Restore Modes and Restore Workflow
- **Claim:** "`Keep existing files with matching session ID`" and "`Check existing files for matching session IDs; mark files for retention or replacement`"
- **Reality:** In the default naming mode, restore retention is based on the generated shared filename, which includes file number, DB session ID, and file size. This is not a session-ID-only comparison.
- **Source:** `utilities/backup/backup_engine.cc` `BackupEngineImpl::InferDBFilesToRetainInRestore`; `utilities/backup/backup_engine.cc` `BackupEngineImpl::GenerateSharedFileWithDbSessionIdAndSize`
- **Fix:** Say it matches the backup's session-ID-based shared filename, including size under the default naming flag.

### [MISLEADING] `VerifyBackup(..., true)` is described as a thorough integrity check without noting its scope
- **File:** `05_restoring_backups.md`, Backup Verification
- **Claim:** "`| `verify_with_checksum = true` | Recomputes checksums and compares with metadata (thorough) |`"
- **Reality:** `VerifyBackup()` only checks whole-file size and backup crc32c. It does not open SSTs, validate internal table/block checksums, or prove the backup is a logically healthy RocksDB database. The tests explicitly call this out.
- **Source:** `utilities/backup/backup_engine.cc` `BackupEngineImpl::VerifyBackup`; `utilities/backup/backup_engine_test.cc` `BackupEngineTest::Corruptions` comments around `VerifyBackup`
- **Fix:** Say this verifies backup file bytes, not full RocksDB table integrity. Recommend `DB::VerifyChecksum()` after restore when that stronger guarantee is needed.

### [MISLEADING] The file-deletion section assigns the deferred-delete backlog to the wrong counter
- **File:** `06_file_deletion_protection.md`, DisableFileDeletions
- **Claim:** "`The pending deletions accumulate in `pending_purge_obsolete_files_``"
- **Reality:** `pending_purge_obsolete_files_` counts purge work already discovered by `FindObsoleteFiles()` but not yet finished. While deletions are disabled, `FindObsoleteFiles()` returns early and no new purge work is scheduled. The deferred backlog remains represented in VersionSet/job-context state until deletions are re-enabled.
- **Source:** `db/db_impl/db_impl.h` `pending_purge_obsolete_files_`; `db/db_impl/db_impl_files.cc` `DBImpl::FindObsoleteFiles`; `db/db_impl/db_impl_files.cc` `DBImpl::EnableFileDeletions`
- **Fix:** Describe `pending_purge_obsolete_files_` as a synchronization counter for active purge work, not as the general store of deferred obsolete files.

### [UNVERIFIABLE] The performance chapter contains hardcoded numbers and ratios that are not backed by code, and one of them conflicts with the fallback copy path
- **File:** `09_performance.md`, Checkpoint Performance Characteristics / BackupEngine Performance Characteristics / Space Amplification
- **Claim:** Examples include "`Typical end-to-end latency: 10-500ms on local NVMe, scaling with number of files rather than data size.`", "`500 MB/s - 2 GB/s`", "`10-100x faster than full backup`", and "`using 16 threads can reduce backup time to roughly 1/3 of single-threaded`"
- **Reality:** None of these numbers are verifiable from the source tree. More importantly, the blanket statement that checkpoint latency scales with file count rather than data size conflicts with the actual cross-device fallback, where checkpoint switches from hard-linking to copying and can become O(database size).
- **Source:** `utilities/checkpoint/checkpoint_impl.cc` `CheckpointImpl::CreateCustomCheckpoint`; `utilities/backup/backup_engine.cc` `BackupEngineImpl::Initialize`; no in-tree benchmark source is cited for the numeric claims
- **Fix:** Remove the specific numbers unless they are tied to cited benchmark data, and qualify the checkpoint-latency discussion as "same-filesystem hard-link case."

## Completeness Gaps

### Export/import metadata still depends on deprecated fields that the docs omit
- **Why it matters:** Import currently reconstructs source paths from `LiveFileMetaData::db_path` and `LiveFileMetaData::name`. Someone following the docs could easily assume `relative_filename` and `directory` are the important fields, but that is not what import consumes today.
- **Where to look:** `utilities/checkpoint/checkpoint_impl.cc` `CheckpointImpl::ExportColumnFamily`; `db/import_column_family_job.cc`; `include/rocksdb/metadata.h`
- **Suggested scope:** Add to `02_export_column_family.md`

### ExportColumnFamily multi-path limitations are undocumented
- **Why it matters:** Unlike Checkpoint/BackupEngine, export does not enumerate per-file directories. It reads SSTs from `db_->GetName()`, so `db_paths` / `cf_paths` assumptions matter here too.
- **Where to look:** `utilities/checkpoint/checkpoint_impl.cc` `CheckpointImpl::ExportFilesInMetaData`
- **Suggested scope:** Mention in `02_export_column_family.md` limitations

### Several important option contracts are missing
- **Why it matters:** The review angle asked for exact option semantics. The docs currently miss that `backup_dir` must differ from the DB directory, `exclude_files_callback` requires `schema_version >= 2`, and writable engines ignore `max_valid_backups_to_open`.
- **Where to look:** `include/rocksdb/utilities/backup_engine.h` `BackupEngineOptions`; `utilities/backup/backup_engine.cc` `BackupEngineImpl::Initialize`; `utilities/backup/backup_engine.cc` `BackupEngineImpl::CreateNewBackupWithMetadata`
- **Suggested scope:** Expand `03_backup_engine_config.md`

### Restore optimization scope is narrower than the docs imply
- **Why it matters:** Only SSTs, and in `kVerifyChecksum` mode blobs, participate in file-retention optimization. CURRENT/MANIFEST/OPTIONS are always recreated or recopied, and WAL handling is separate.
- **Where to look:** `utilities/backup/backup_engine.cc` `BackupEngineImpl::InferDBFilesToRetainInRestore`; `utilities/backup/backup_engine.cc` `BackupEngineImpl::RestoreDBFromBackup`
- **Suggested scope:** Add to `05_restoring_backups.md`

## Depth Issues

### WAL handling is flattened into "WAL tail" and loses the real decision tree
- **Current:** The docs mostly talk about "the WAL tail" being copied and "active WAL files" being returned.
- **Missing:** The code distinguishes archived WALs vs live WALs, active/open WALs vs fully synced inactive WALs, recyclable WALs vs normal WALs, and the `LockWAL()` case that suppresses the flush. Those distinctions determine whether a WAL is hard-linked or copied and whether `trim_to_size` is set.
- **Source:** `db/db_filesnapshot.cc` `DBImpl::GetLiveFilesStorageInfo`

### The distributed-backup callback flow needs more precision
- **Current:** The docs say the callback receives shared files and can exclude them.
- **Missing:** The callback only sees shared-checksum files that still need copying. Already-present shared files are not offered, and the feature is gated by `schema_version >= 2`.
- **Source:** `utilities/backup/backup_engine.cc` `BackupEngineImpl::CreateNewBackupWithMetadata`; `utilities/backup/backup_engine.cc` `BackupEngineImpl::AddBackupFileWorkItem`

## Structure and Style Violations

### Inline code quoting is used throughout the whole doc set
- **File:** `index.md` and every chapter file under `docs/components/checkpoint/`
- **Details:** The style contract for these component docs says no inline code quotes, but the current content uses backticked identifiers, paths, enum names, and option names on nearly every page.

## Undocumented Complexity

### `callback_trigger_interval_size = 0` is unsafe
- **What it is:** The progress-callback loop subtracts `callback_trigger_interval_size` until the byte counter drops below the threshold. If the threshold is zero, the loop never makes progress.
- **Why it matters:** This is exactly the kind of 0-value option semantic the prompt asked to verify. Today the docs do not state a valid range, and the implementation does not sanitize it.
- **Key source:** `utilities/backup/backup_engine.cc` `BackupEngineImpl::CopyOrCreateFile`
- **Suggested placement:** `03_backup_engine_config.md`

### `max_background_operations <= 0` is unsafe
- **What it is:** Worker threads are created with a simple `for (int t = 0; t < max_background_operations; ++t)` loop. Zero creates no workers, while create/restore/verify still enqueue work and wait on futures.
- **Why it matters:** This is another missing valid-range constraint. A zero value can deadlock backup/restore/verify.
- **Key source:** `utilities/backup/backup_engine.cc` `BackupEngineImpl::Initialize`; `utilities/backup/backup_engine.cc` `BackupEngineImpl::CreateNewBackupWithMetadata`; `utilities/backup/backup_engine.cc` `BackupEngineImpl::RestoreDBFromBackup`; `utilities/backup/backup_engine.cc` `BackupEngineImpl::VerifyBackup`
- **Suggested placement:** `03_backup_engine_config.md`

### `BackupInfo::file_details` is currently incomplete
- **What it is:** When `include_file_details = true`, the code fills `relative_filename`, `directory`, `size`, `file_number`, and `file_type`, but leaves `temperature`, `file_checksum`, and `file_checksum_func_name` unset.
- **Why it matters:** A reader seeing `BackupFileInfo` in headers could assume those fields are fully populated by `GetBackupInfo()`. They are not.
- **Key source:** `utilities/backup/backup_engine.cc` `BackupEngineImpl::SetBackupInfoFromBackupMeta`
- **Suggested placement:** `05_restoring_backups.md` or `04_creating_backups.md`

### Export temp-dir behavior differs from checkpoint temp-dir behavior
- **What it is:** Checkpoint proactively cleans `<target>.tmp` before use. Export does not; it simply tries to create the temp directory and will fail if a leftover temp dir is already present.
- **Why it matters:** Someone using both APIs could assume the retry/cleanup behavior is symmetrical when it is not.
- **Key source:** `utilities/checkpoint/checkpoint_impl.cc` `CheckpointImpl::CreateCheckpoint`; `utilities/checkpoint/checkpoint_impl.cc` `CheckpointImpl::ExportColumnFamily`
- **Suggested placement:** `02_export_column_family.md`

## Positive Notes

- `index.md` is exactly 40 lines and follows the requested high-level pattern well: overview, key source files, chapter table, key characteristics, and key invariants.
- The checkpoint docs correctly call out the CURRENT-file replacement behavior and the 2PC / `LockWAL()` interactions, which aligns with `utilities/checkpoint/checkpoint_test.cc`.
- The concurrency chapter mirrors the public BackupEngine lock model closely, including the important distinction that `GarbageCollect()` is treated as an Append operation rather than a Write operation.

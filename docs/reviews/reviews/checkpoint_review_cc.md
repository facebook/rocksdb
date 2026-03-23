# Review: checkpoint -- Claude Code

## Summary
Overall quality rating: **good**

The documentation provides a well-structured and largely accurate overview of both Checkpoint and BackupEngine subsystems. The separation into chapters covering API, configuration, workflow, concurrency, error handling, and performance is logical and readable. The factual accuracy is high for the core Checkpoint code, with most claims verified against the implementation. However, there are a few correctness issues -- most notably around the ExportColumnFamily cross-device detection behavior and some misleading claims about file deletion internals. The BackupEngine chapters are harder to fully verify without reading the entire 3000+ line implementation, but the header-verifiable claims are accurate.

## Correctness Issues

### [WRONG] ExportColumnFamily cross-device detection is not "similar" to CreateCheckpoint
- **File:** 02_export_column_family.md, "Link or copy SST files" section
- **Claim:** "The cross-device detection is similar to `CreateCheckpoint()`: the first `NotSupported` from a hard-link attempt flips the strategy to copy for all remaining files."
- **Reality:** In `ExportFilesInMetaData()`, the cross-device fallback only triggers if the **first file** (`num_files == 1`) returns `NotSupported`. If a later file returns `NotSupported`, it is treated as a hard error and the operation fails. In contrast, `CreateCustomCheckpoint()` (used by `CreateCheckpoint()`) flips `same_fs = false` on **any** file's `NotSupported`, allowing subsequent files to be copied.
- **Source:** `utilities/checkpoint/checkpoint_impl.cc`, `ExportFilesInMetaData()` at the `if (num_files == 1 && s.IsNotSupported())` check (vs `CreateCustomCheckpoint()` which uses `if (s.IsNotSupported()) { same_fs = false; }`)
- **Fix:** State that the cross-device detection in `ExportColumnFamily` is more limited: it only falls back to copy if the **first** file fails to hard-link. A `NotSupported` error on any subsequent file is treated as a hard failure.

### [MISLEADING] pending_purge_obsolete_files_ claim in File Deletion Protection
- **File:** 06_file_deletion_protection.md, "DisableFileDeletions" section
- **Claim:** "The pending deletions accumulate in `pending_purge_obsolete_files_`"
- **Reality:** The mechanism is different. When `disable_delete_obsolete_files_` is positive, compaction simply does not call `DeleteObsoleteFiles()`. There is no explicit "pending" list that accumulates during the disabled period. When `EnableFileDeletions()` decrements the counter to zero, it calls `FindObsoleteFiles()` with `force=true` to discover and purge all obsolete files at that point. The `pending_purge_obsolete_files_` field exists in `DBImpl` but serves a different purpose (tracking files currently being purged by background threads).
- **Source:** `db/db_impl/db_impl_files.cc`, `EnableFileDeletions()` calls `FindObsoleteFiles(&job_context, true)` when counter reaches zero.
- **Fix:** Replace with: "When `EnableFileDeletions()` decrements the counter back to zero, `FindObsoleteFiles()` is called with a full scan to discover and purge all obsolete files."

### [MISLEADING] GetLiveFilesStorageInfo field reference to wrong header
- **File:** 06_file_deletion_protection.md, "GetLiveFilesStorageInfo" section
- **Claim:** "`DB::GetLiveFilesStorageInfo()` (see `include/rocksdb/db.h`) ... It accepts `LiveFilesStorageInfoOptions`"
- **Reality:** While `GetLiveFilesStorageInfo()` is declared in `include/rocksdb/db.h`, the `LiveFilesStorageInfoOptions` struct is defined in `include/rocksdb/options.h`, not `include/rocksdb/db.h`. The doc should reference both files.
- **Source:** `include/rocksdb/options.h` contains `struct LiveFilesStorageInfoOptions`
- **Fix:** Add `include/rocksdb/options.h` reference for `LiveFilesStorageInfoOptions`.

### [MISLEADING] "absolute path" enforcement claim
- **File:** 01_checkpoint_api.md, "Preconditions and Limitations" section
- **Claim:** "`checkpoint_dir` must be an absolute path (stated in the public header contract)"
- **Reality:** The header says "checkpoint_dir should contain an absolute path" (recommendation), but the code does NOT enforce this. No check for absolute vs relative path exists in `CreateCheckpoint()`. Using a relative path would work but is not recommended.
- **Source:** `include/rocksdb/utilities/checkpoint.h` header comment and `utilities/checkpoint/checkpoint_impl.cc` `CreateCheckpoint()` implementation
- **Fix:** Soften to: "`checkpoint_dir` should be an absolute path (recommended in the public header, but not enforced by the implementation)"

### [WRONG] ExportColumnFamily Step ordering -- metadata built in wrong place
- **File:** 02_export_column_family.md, "ExportColumnFamily Workflow"
- **Claim:** Steps 8 (atomic rename), 9 (fsync), then 10 (build metadata) -- implying metadata is built AFTER fsync.
- **Reality:** Looking at the code, the metadata is built after fsync (this ordering IS correct in the doc). However, the doc says "On failure, cleanup removes all files in the staging (or final) directory" but doesn't mention that `ExportColumnFamily` only cleans up files in whichever directory the files currently reside (staging or final, depending on whether rename succeeded). This is actually correctly described. No change needed here.

### [MISLEADING] Chapter 1 claims `checkpoint_dir` must be absolute in the parameter table
- **File:** 01_checkpoint_api.md, "Public API" table
- **Claim:** Description says "Absolute path for the snapshot directory (must not exist)"
- **Reality:** As noted above, absolute path is not enforced.
- **Fix:** Change to "Path for the snapshot directory (absolute recommended; must not exist)"

## Completeness Gaps

### Missing: ExportColumnFamily stale data fix (PR #13654)
- **Why it matters:** A bug was fixed in June 2025 where `ExportColumnFamily()` could return stale data because `GetColumnFamilyMetaData()` used the SuperVersion which might not reflect the just-completed flush. The fix switched to getting metadata from the Version directly. This is relevant context for users understanding the reliability of the API.
- **Where to look:** Commit `0119a8c78`, `db/db_impl/db_impl.cc` change to `GetColumnFamilyMetaData()`
- **Suggested scope:** Brief mention in chapter 2 under limitations or as a note about reliability.

### Missing: ExportColumnFamily memory leak fix for empty column families (PR #14458)
- **Why it matters:** A memory leak existed when exporting empty column families (March 2026 fix). Users should be aware that exporting empty CFs is supported but was recently fixed.
- **Where to look:** Commit `e43171d6c`
- **Suggested scope:** Brief mention in chapter 2 limitations.

### Missing: EnableFileDeletions force mode removal (PR #12337)
- **Why it matters:** The force mode for `EnableFileDeletions()` was removed in February 2024. The docs don't mention this but also don't incorrectly reference it, so this is a minor gap. However, users familiar with older APIs might look for the `force` parameter.
- **Where to look:** Commit `4bea83aa4`
- **Suggested scope:** Not critical since docs don't reference the old API, but could add a brief note.

### Missing: Backup verification parallelization (PR #13292)
- **Why it matters:** `VerifyBackup()` was parallelized using `max_background_operations` threads. This is a performance improvement that users should know about.
- **Where to look:** Commit `0e469c7f9`
- **Suggested scope:** Mention in chapter 9 (Performance) or chapter 5 (Restoring) under verification.

### Missing: BackupEngine io_buffer_size option details
- **Why it matters:** The `io_buffer_size` option (PR #13236) was recently added. While it is listed in the options table in chapter 3, its interaction with rate limiter burst size could be explained better.
- **Where to look:** `include/rocksdb/utilities/backup_engine.h`, the `io_buffer_size` field comment
- **Suggested scope:** Expand description in chapter 3 options table.

### Missing: WAL Close-before-Link fix (PR #12734)
- **Why it matters:** A fix ensured WALs are closed before being hard-linked in Checkpoint, preventing hard-linking of inactive but unsynced WALs. This is relevant safety information.
- **Where to look:** Commits `0646ec6e2` and `98393f013`
- **Suggested scope:** Brief mention in chapter 1 under WAL handling.

### Missing: LiveFileStorageInfo is in metadata.h, not options.h
- **Why it matters:** The chapter 6 `Files:` header lists `include/rocksdb/options.h` but `LiveFileStorageInfo` and `FileStorageInfo` are in `include/rocksdb/metadata.h`. The `LiveFilesStorageInfoOptions` is in `include/rocksdb/options.h`. Both should be referenced.
- **Where to look:** `include/rocksdb/metadata.h`
- **Suggested scope:** Add `include/rocksdb/metadata.h` to the Files line in chapter 6.

## Depth Issues

### Chapter 2 should explain the first-file-only cross-device limitation more explicitly
- **Current:** "The cross-device detection is similar to `CreateCheckpoint()`"
- **Missing:** The ExportColumnFamily implementation is strictly limited: cross-device fallback only works on the first file. If a filesystem returns NotSupported on a later file (unlikely but possible with heterogeneous storage), the operation fails. This is a meaningful behavioral difference from CreateCheckpoint.
- **Source:** `utilities/checkpoint/checkpoint_impl.cc`, `ExportFilesInMetaData()` line `if (num_files == 1 && s.IsNotSupported())`

### Chapter 6 could clarify FindObsoleteFiles vs simple pending list
- **Current:** "The pending deletions accumulate in `pending_purge_obsolete_files_`"
- **Missing:** The actual mechanism is scan-based: `FindObsoleteFiles()` discovers obsolete files by comparing the current Version's live file set against what's on disk. There is no queue that builds up during the disabled period.
- **Source:** `db/db_impl/db_impl_files.cc`, `FindObsoleteFiles()` and `EnableFileDeletions()`

### Chapter 4 should mention CreateBackupOptions return type is IOStatus, not Status
- **Current:** The chapter discusses workflows returning Status
- **Missing:** BackupEngine methods return `IOStatus`, not `Status`. This is relevant for callers who need to handle I/O-specific error information.
- **Source:** `include/rocksdb/utilities/backup_engine.h`, all BackupEngine methods return `IOStatus`

## Structure and Style Violations

### index.md line count at minimum boundary
- **File:** index.md
- **Details:** Exactly 40 lines (with the trailing newline being line 41). This is at the very bottom of the recommended 40-80 line range. The index packs a lot of content but could benefit from being slightly expanded. The "Multi-path not supported" invariant is listed under "Key Characteristics" and "Key Invariants" doesn't clearly distinguish between true invariants and limitations.

### "Multi-path not supported" is not an invariant
- **File:** index.md, "Key Characteristics" section
- **Details:** "Both Checkpoint and BackupEngine return `NotSupported` when `db_paths` or `cf_paths` are used" is listed as a key characteristic. This is fine, but note that this is a limitation, not an invariant. The "Key Invariants" section correctly does not include this.

### Chapter 6 Files line missing metadata.h
- **File:** 06_file_deletion_protection.md
- **Details:** The `Files:` line lists `include/rocksdb/db.h`, `include/rocksdb/options.h`, etc., but should also include `include/rocksdb/metadata.h` where `LiveFileStorageInfo` and `FileStorageInfo` are defined.

## Undocumented Complexity

### CleanStagingDirectory failure handling and the Aborted status
- **What it is:** When `CleanStagingDirectory()` fails (returns non-OK), `CreateCheckpoint()` returns `Status::Aborted()` with a descriptive message. However, `CleanStagingDirectory()` itself can fail for various reasons: the staging directory doesn't exist (returns OK), the directory exists but children can't be listed, or individual file deletions fail. The error from the first failed file deletion is propagated, but cleanup continues for remaining files.
- **Why it matters:** Users seeing `Status::Aborted` from `CreateCheckpoint()` need to understand this means a leftover staging directory from a previous failure couldn't be cleaned, not that the current operation was aborted for some other reason. The fix in PR #12894 improved this handling.
- **Key source:** `utilities/checkpoint/checkpoint_impl.cc`, `CleanStagingDirectory()` and its caller in `CreateCheckpoint()`
- **Suggested placement:** Add to chapter 8 (Error Handling), expand the "Aborted" entry in the failure modes table.

### ExportColumnFamily temperature handling FIXME
- **What it is:** The `ExportFilesInMetaData()` copy callback uses `Temperature::kUnknown` for both source and destination, with a FIXME comment "temperature handling". This means exported files lose their temperature metadata.
- **Why it matters:** Users relying on tiered storage will lose temperature information when exporting/importing column families.
- **Key source:** `utilities/checkpoint/checkpoint_impl.cc`, `ExportColumnFamily()` copy callback with `Temperature::kUnknown`
- **Suggested placement:** Add to chapter 2 under Limitations.

### BackupEngine uses CheckpointImpl::CreateCustomCheckpoint internally
- **What it is:** `CreateNewBackup()` delegates to `CheckpointImpl::CreateCustomCheckpoint()` with backup-specific callbacks. The doc mentions this in chapter 4 Step 2 but doesn't explain the implication: any behavioral changes to `CreateCustomCheckpoint()` (like the multi-path check or `same_fs` logic) automatically affect BackupEngine.
- **Why it matters:** Developers modifying checkpoint code need to understand the coupling.
- **Key source:** `utilities/backup/backup_engine.cc` calls into `CheckpointImpl::CreateCustomCheckpoint()`
- **Suggested placement:** Mention in chapter 1 and/or chapter 4 more explicitly.

## Positive Notes

- The chapter structure is logical and comprehensive, covering both Checkpoint (lightweight) and BackupEngine (full-featured) in appropriate depth.
- The options tables in chapters 3 and 5 are accurate and match the header definitions.
- The concurrency chapter (7) accurately captures the read-write lock semantics and inter-instance interference rules, matching the detailed comments in `backup_engine.h`.
- The `StopBackup()` one-way behavior is correctly documented, including the requirement to open a new BackupEngine.
- The `SharedFilesNaming` enum values and their trade-offs are well explained and match the header.
- The WAL handling and `log_size_for_flush` behavior is correctly documented, including the 2PC always-flush exception and the `LockWAL` skip behavior.
- The performance chapter provides actionable guidance with realistic numbers.
- The `ExportImportFilesMetaData` struct fields are accurately documented against `include/rocksdb/metadata.h`.
- The index.md is concise and well-organized with a clear chapter table.

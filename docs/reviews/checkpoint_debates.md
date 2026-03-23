# Debates: checkpoint

No direct disagreements were found between the CC and Codex reviewers. On the two issues they both identified (ExportColumnFamily cross-device fallback behavior and `pending_purge_obsolete_files_` misattribution), both reviewers reached the same conclusion. Each reviewer also raised unique issues not covered by the other -- all were verified against source code and found to be valid.

## Overlap: ExportColumnFamily Cross-Device Fallback
- **CC position**: Cross-device fallback only triggers on the first file (`num_files == 1`); later `NotSupported` errors are hard failures
- **Codex position**: Same -- first file is the cross-device probe; later failures cause export to fail
- **Code evidence**: `utilities/checkpoint/checkpoint_impl.cc`, `ExportFilesInMetaData()` checks `if (num_files == 1 && s.IsNotSupported())`
- **Resolution**: Both correct. Fixed in doc.
- **Risk level**: medium

## Overlap: pending_purge_obsolete_files_ Misattribution
- **CC position**: No explicit pending list accumulates; `FindObsoleteFiles()` with `force=true` discovers obsolete files when re-enabled
- **Codex position**: `pending_purge_obsolete_files_` is a synchronization counter for active purge work, not the deferred backlog store
- **Code evidence**: `db/db_impl/db_impl_files.cc`, `EnableFileDeletions()` calls `FindObsoleteFiles(&job_context, true)` when counter reaches zero
- **Resolution**: Both correct; complementary descriptions. Fixed in doc.
- **Risk level**: medium

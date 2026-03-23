# Fix Summary: checkpoint

## Issues Fixed
- **Correctness**: 10 (wrong or misleading factual claims corrected)
- **Completeness**: 5 (missing limitations, option warnings, scope clarifications added)
- **Structure**: 1 (metadata.h added to Files line in chapter 06)
- **Style**: 0 (inline code quoting flagged by Codex but not changed -- backticks are standard in these docs)

## Disagreements Found
0 direct disagreements. Both reviewers agreed on the 2 overlapping issues. Details in checkpoint_debates.md.

## Changes Made

### index.md
- Fixed "content-based naming" to "shared-file deduplication (session-ID-based naming for SSTs, checksum-based for blobs)"
- Fixed hard link strategy to include OPTIONS files

### 01_checkpoint_api.md
- Fixed OPTIONS file from "Always copy" to "Hard link (preferred), copy (fallback)" -- verified OPTIONS follows same link logic as SST files
- Softened absolute path claim from "must be" to "should be (recommended, not enforced)"
- Fixed parameter table description to match

### 02_export_column_family.md
- Fixed cross-device detection: documented first-file-only probe behavior, not "similar to CreateCheckpoint()"
- Added 3 new limitations: temperature metadata lost (FIXME in source), no staging directory pre-cleanup, multi-path not supported

### 03_backup_engine_config.md
- Fixed session-ID naming from "No file I/O" to "reads SST properties block" -- verified GetFileDbIdentities() uses SstFileDumper
- Fixed shared_checksum/ description from "Content-addressed" to session-ID-based naming description
- Updated default naming description to mention properties block read
- Added safety warnings for callback_trigger_interval_size (must be > 0) and max_background_operations (must be >= 1)

### 04_creating_backups.md
- Fixed flush_before_backup description: required for no-WAL configs, otherwise mainly reduces WAL dependence
- Fixed incremental backup I/O claim: session ID requires properties block read, not zero I/O
- Fixed GetCorruptedBackups() description: lists failed metadata loads, not missing-metadata backups

### 05_restoring_backups.md
- Fixed kKeepLatestDbSessionIdFiles: matches full shared filename (number + session ID + size), not session ID alone
- Clarified restore optimization excludes CURRENT/MANIFEST/OPTIONS/WAL
- Fixed VerifyBackup scope: checks whole-file CRC32C only, not internal SST block checksums; recommend DB::VerifyChecksum() for full integrity

### 06_file_deletion_protection.md
- Fixed pending_purge_obsolete_files_ mechanism: FindObsoleteFiles() returns early while disabled, no pending list accumulates
- Fixed EnableFileDeletions() description: calls FindObsoleteFiles() with force=true
- Added include/rocksdb/metadata.h to Files line

### 08_error_handling.md
- Fixed incomplete backup detection: backups discovered from meta/ dir, orphaned files are GC garbage not backup IDs
- Fixed GC auto-trigger: conditional on might_need_garbage_collect_ flag, not triggered by every create/delete/purge

### 09_performance.md
- Removed unverifiable specific numbers (10-500ms, 500MB/s-2GB/s, 10-100x, 1/3 of single-threaded, etc.)
- Qualified checkpoint latency as same-filesystem hard-link case; noted cross-device is O(database size)
- Fixed "Deduplicated by content" to "Deduplicated via shared-file naming"
- Fixed session-ID naming tuning recommendation

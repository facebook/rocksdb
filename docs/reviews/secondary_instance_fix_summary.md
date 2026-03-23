# Fix Summary: secondary_instance

## Issues Fixed

| Category | Count |
|----------|-------|
| Correctness | 10 |
| Completeness | 9 |
| Structure/Style | 4 |
| Total | 23 |

## Disagreements Found

0 -- CC and Codex raised different issues but never contradicted each other. Details in debates.md.

## Changes Made

### index.md
- Expanded from 36 to 40 lines (was below 40-line minimum)
- Fixed MANIFEST invariant: now mentions retry for MANIFEST-switch races instead of claiming "safe without locking"
- Added invariants: lock-free reads, dropped CF SuperVersion updates
- Added characteristics: eventually consistent, TransactionDB compatible

### 01_opening_and_recovery.md
- Fixed constructor description: removed false "is_secondary" flag claim; documented actual parameter meanings (seq_per_batch, batch_per_txn, read_only) and that secondary behavior comes from the subclass
- Fixed API signatures: changed `DB**` to `unique_ptr<DB>*` in both overloads

### 02_catching_up.md
- Fixed WAL sequence comparison: clarified it checks `l0_files.back()->fd.largest_seqno` (newest L0 file) specifically, not a scan of all L0 files
- Expanded RemoveOldMemTables: documented the log-number-based eviction mechanism and temporary memory growth
- Added Files: line entries for `db/version_edit_handler.h` and `db/version_edit_handler.cc`
- Added WAL Recovery Mode section (interaction with wal_recovery_mode on active WALs)
- Added FindNewLogNumbers optimization section (min-log-number heuristic and reopening implications)
- Added Concurrent Read Safety section (lock-free SuperVersion acquisition during catch-up)

### 03_read_operations.md
- Added GetMergeOperands to supported operations table
- Fixed snapshot semantics: replaced single-row table with per-API matrix showing different behavior for Get, MultiGet, NewIterator, NewIterators
- Fixed cross-CF consistency: now states not guaranteed during concurrent catch-up
- Fixed implicit snapshot description: now describes as best-effort/eventually-consistent
- Documented GetImpl ordering difference (LastSequence read before SuperVersion, unlike base DBImpl)
- Added note that MultiGet inherits base class and can honor explicit snapshots
- Added kPersistedTier behavior section (rejected by iterators, not checked by Get)

### 04_file_deletion.md
- Fixed SstFileManager condition: changed from OR (`rate_bytes_per_sec > 0 or bytes_max_delete_chunk > 0`) to correct conjunction (background deletion active AND `bytes_max_delete_chunk != 0` AND file larger than chunk)

### 05_column_family_handling.md
- Fixed CF subset claim: single-CF overload opens only default CF, not arbitrary subsets
- Fixed dropped CF SuperVersion: they are NOT skipped during installation (no IsDropped check in install loop), enabling existing handles to remain usable
- Added `db/version_edit_handler.cc` to Files: line

### 06_remote_compaction.md
- Added `db/compaction/compaction_service_job.cc` and `include/rocksdb/options.h` to Files: line
- Added second OpenAndCompact overload (legacy, no OpenAndCompactOptions)
- Documented allow_resumption default (false)
- Documented output_directory dual role as secondary_path
- Added subcompaction key bounds (has_begin/begin, has_end/end) to CompactionServiceInput description
- Expanded override list: added DB-level overrides (env, file_checksum_gen_factory, statistics, listeners, info_log) and CF-level (compaction_filter_factory); documented options_map with ignore_unknown_options=true
- Documented DB-level vs CF-level override split
- Fixed priority claim: moved from serialized input to primary-side CompactionServiceJobInfo
- Added Remote Worker Observability section

### 07_comparison.md
- Fixed snapshot support row: now API-specific instead of blanket "Not supported"
- Fixed ReadOnly info log claim: "May create info log files depending on options" instead of "Does not create any files"
- Fixed follower hard links: clarified only SSTs are hard-linked on demand; non-SST files read directly from leader path
- Updated file deletion tolerance column for follower
- Expanded follower File Lifecycle description with OnDemandFileSystem details

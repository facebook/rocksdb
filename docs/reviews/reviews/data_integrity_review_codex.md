# Review: data_integrity — Codex

## Summary
(Overall quality rating: significant issues)

The doc set has a good top-level decomposition and covers most of the right mechanisms: block/WAL/file checksums, in-memory protection, unique IDs, output verification, and recovery. The index is well-shaped and the chapter split is useful for navigation.

The main problem is precision. Several option semantics are stale or wrong, and a few cross-component boundaries are described as if they were simpler than the implementation actually is. The most serious examples are around CRC masking, compaction-vs-flush verification, file-checksum coverage, WAL verification semantics, `paranoid_checks`, and `RepairDB()`. A maintainer could make incorrect operational or debugging decisions from the current text.

## Correctness Issues

### [WRONG] SST CRC32c block checksums are also masked
- **File:** `01_checksum_types.md`, section `CRC32c Masking`
- **Claim:** "Masking is used for WAL record checksums but NOT for SST block checksums (which use `ComputeBuiltinChecksum()` directly)."
- **Reality:** For `kCRC32c`, both `ComputeBuiltinChecksum()` and `ComputeBuiltinChecksumWithLastByte()` call `crc32c::Mask()`. SST block CRC32c values are masked too.
- **Source:** `table/format.cc` `ComputeBuiltinChecksum`; `table/format.cc` `ComputeBuiltinChecksumWithLastByte`; `table/block_based/block_based_table_builder.cc` `BlockBasedTableBuilder::WriteRawBlock`
- **Fix:** State that CRC32c values used in WAL and SST block/footer verification are masked before storage, while the xxHash/XXH3 variants are stored unmasked.

### [STALE] The block-checksum chapter has outdated option semantics
- **File:** `02_block_checksums.md`, sections `Write Path` and `Configuration`
- **Claim:** "`format_version` | `BlockBasedTableOptions` in `include/rocksdb/table.h` | Default | `6`" and "If `verify_compression` is enabled (see `CompressionOptions` in `include/rocksdb/compression_type.h`)..."
- **Reality:** `BlockBasedTableOptions::format_version` defaults to `7`, not `6`. `verify_compression` is also a `BlockBasedTableOptions` field, not a `CompressionOptions` field.
- **Source:** `include/rocksdb/table.h` `BlockBasedTableOptions::format_version`; `include/rocksdb/table.h` `BlockBasedTableOptions::verify_compression`; `table/block_based/block_based_table_builder.cc` `CompressAndVerifyBlock`
- **Fix:** Update the default to `7` and point `verify_compression` at `BlockBasedTableOptions` in `include/rocksdb/table.h`.

### [WRONG] `verify_output_flags` is documented as if it also applies to flush
- **File:** `07_output_verification.md`, sections `paranoid_file_checks (Legacy)` and `Verification Workflow / After Flush`
- **Claim:** "This is equivalent to enabling `kVerifyIteration` for both local and remote compaction." and "If `paranoid_file_checks` is true or appropriate `verify_output_flags` are set, the SST is reopened..."
- **Reality:** `VerifyOutputFlags` is compaction-only today. `include/rocksdb/advanced_options.h` still has `kEnableForFlush` as a TODO. Flush verification uses the older `BuildTable(... paranoid_file_checks ...)` path and does not consult `verify_output_flags`.
- **Source:** `include/rocksdb/advanced_options.h` `VerifyOutputFlags`; `db/builder.cc` `BuildTable`; `db/flush_job.cc` `FlushJob::WriteLevel0Table`; `db/compaction/compaction_job.cc` `CompactionJob::VerifyOutputFiles`; `db/db_compaction_test.cc` `DBCompactionTest.VerifyIterationWithoutParanoidFileChecks`
- **Fix:** Separate flush verification from compaction verification. Say `paranoid_file_checks` affects flush and compaction, while `verify_output_flags` currently affects compaction output only.

### [MISLEADING] `compaction_verify_record_count` is described as input-only
- **File:** `07_output_verification.md`, section `Compaction Record Count`
- **Claim:** "`compaction_verify_record_count` in `DBOptions` ... counts entries read from compaction input files and compares against input file metadata."
- **Reality:** The compaction path verifies both input-record counts and output SST entry counts.
- **Source:** `db/compaction/compaction_job.cc` `VerifyInputRecordCount`; `db/compaction/compaction_job.cc` `VerifyOutputRecordCount`
- **Fix:** Document both checks and keep the existing limitations note for range deletions and `kRemoveAndSkipUntil`.

### [WRONG] `DB::VerifyFileChecksums()` does not recompute every live file unconditionally
- **File:** `04_file_checksums_handoff.md`, section `Verification`
- **Claim:** "`DB::VerifyFileChecksums()` iterates all live SST and blob files, recomputes their checksums using the configured factory, and compares against MANIFEST-stored values."
- **Reality:** It iterates all live files, but files whose expected checksum is `kUnknownFileChecksum` are skipped. Mixed-generation DBs therefore get partial coverage, not complete file-level verification.
- **Source:** `db/db_impl/db_impl.cc` `DBImpl::VerifyChecksumInternal`; `db/db_impl/db_impl.cc` `DBImpl::VerifyFullFileChecksum`; `db/db_basic_test.cc` `DBBasicTest.VerifyFileChecksums`
- **Fix:** Say the API verifies files with recorded file-checksum metadata, and older files written without a checksum factory are skipped.

### [WRONG] WAL handoff does not reuse the full precomputed WAL record CRC
- **File:** `04_file_checksums_handoff.md`, section `Pre-computed Checksums`
- **Claim:** "When handoff is enabled for WAL files, the writer passes the pre-computed WAL record CRC directly to the filesystem layer, avoiding redundant checksum computation."
- **Reality:** The WAL writer appends the header with no handoff checksum and appends the payload with the payload CRC only. `WritableFileWriter` still has a TODO about more effectively reusing existing WAL/MANIFEST CRCs.
- **Source:** `db/log_writer.cc` `Writer::EmitPhysicalRecord`; `file/writable_file_writer.cc` `WritableFileWriter::Crc32cHandoffChecksumCalculation`
- **Fix:** Document the current behavior as payload-level reuse only, not full-record reuse.

### [MISLEADING] In-memory block protection is not limited to block-cache residency
- **File:** `06_memtable_block_protection.md`, section `In-Memory Block Protection / How It Works`
- **Claim:** "When a block is loaded into memory from an SST file ... and loaded into the block cache, per-key checksums are computed..."
- **Reality:** Protection is attached to parsed `Block` objects regardless of whether they are cache-resident. It applies to parsed blocks, not specifically to the cache.
- **Source:** `table/block_based/block.h` `Block::InitializeDataBlockProtection`; `table/block_based/block.h` `Block::InitializeIndexBlockProtection`; `table/block_based/block.h` `Block::InitializeMetaIndexBlockProtection`; `table/block_based/block_based_table_reader.cc` `BlockBasedTable::Open`
- **Fix:** Say protection is initialized when a block is parsed into a `Block` object, and cached blocks keep that parsed representation if they are inserted into cache.

### [WRONG] MANIFEST-based WAL tracking does not require exact size equality
- **File:** `09_wal_tracking.md`, section `MANIFEST-Based WAL Tracking`
- **Claim:** "`WalSet::CheckWals()` ... verifies that every non-obsolete WAL listed in the MANIFEST exists on disk and has the expected size" and "If a synced WAL is missing or its size does not match, recovery reports an error."
- **Reality:** `WalSet::CheckWals()` only errors when the on-disk WAL is smaller than the recorded synced size. Larger files are acceptable.
- **Source:** `db/wal_edit.cc` `WalSet::CheckWals`
- **Fix:** Say the WAL must exist and be at least the recorded synced size.

### [WRONG] The `paranoid_checks` sections attribute the wrong behavior to the option
- **File:** `03_wal_manifest_checksums.md`, section `VersionEdit Integrity`; `10_verification_recovery.md`, section `Paranoid Checks on Open`
- **Claim:** "When `paranoid_checks = true` (default), RocksDB verifies MANIFEST record checksums..." and "With `paranoid_checks = false`, corrupted metadata can cause undefined behavior."
- **Reality:** MANIFEST replay uses `log::Reader(... checksum=true ...)` regardless of `paranoid_checks`. Also, the public option comment explicitly says that with `paranoid_checks = false`, the DB can still open and healthy files can remain accessible while corrupted files are not.
- **Source:** `db/version_set.cc` `VersionSet::Recover`; `include/rocksdb/options.h` `DBOptions::paranoid_checks`; `db/version_set.cc` `VersionSet::LogAndApply`
- **Fix:** Separate MANIFEST checksum verification from `paranoid_checks`, and describe the actual degraded-availability behavior when the option is false.

### [WRONG] The chapter claims a guarantee that the recovery code does not make
- **File:** `10_verification_recovery.md`, section `Error Handling / Background Error Handler`
- **Claim:** "**Key Invariant:** RocksDB never silently discards data. All corruption is either logged, reported via `Status`, or handled by `ErrorHandler` with user-visible effects."
- **Reality:** Several implemented recovery modes intentionally skip or discard data to recover a usable DB: `WALRecoveryMode::kPointInTimeRecovery`, `WALRecoveryMode::kSkipAnyCorruptedRecords`, `best_efforts_recovery`, and `RepairDB()`.
- **Source:** `include/rocksdb/options.h` `WALRecoveryMode`; `include/rocksdb/options.h` `DBOptions::best_efforts_recovery`; `db/db_impl/db_impl_open.cc` WAL recovery flow; `db/repair.cc` `RepairDB`
- **Fix:** Narrow the statement to ordinary read/write paths, and explicitly call out the recovery modes that trade data retention for availability.

### [WRONG] `RepairDB()` is described as SST-only salvage, but it also converts WALs
- **File:** `10_verification_recovery.md`, section `RepairDB`
- **Claim:** "Step 1 -- Scans the database directory for SST files" and "Step 2 -- Reads each SST file and extracts valid key-value pairs"
- **Reality:** Repair first scans both SSTs and WALs, converts WALs to tables, archives the original WAL files, and then scans table files.
- **Source:** `db/repair.cc` `Repairer::FindFiles`; `db/repair.cc` `Repairer::ConvertLogFilesToTables`; `db/repair.cc` `Repairer::ConvertLogToTable`; `db/repair.cc` `Repairer::ScanTable`
- **Fix:** Rewrite the `RepairDB()` flow to include WAL discovery/conversion before the table-salvage phase.

### [UNVERIFIABLE] The exact throughput and overhead numbers are not grounded in the checked-in tree
- **File:** `01_checksum_types.md`, sections `ChecksumType Enum`, `xxHash / XXH3`, `Platform Performance Notes`; `02_block_checksums.md`, section `Configuration`
- **Claim:** Examples include "~20 GB/s with SSE4.2", "~60 GB/s with AVX2", "roughly 3x", and "approximately 1-2%".
- **Reality:** I could not find these numbers in the cited code, in the nearby tests, or in checked-in benchmark artifacts. Some may be historically true, but they are not verifiable from the current tree.
- **Source:** checked cited source files plus recent history touching these areas; no in-tree benchmark citation is attached to the claims
- **Fix:** Remove the exact figures or replace them with a maintained benchmark reference.

## Completeness Gaps

### `verify_manifest_content_on_close` is missing entirely
- **Why it matters:** This is a recent MANIFEST-integrity feature that re-reads MANIFEST content on close and rewrites a fresh MANIFEST from in-memory state if corruption is detected.
- **Where to look:** `include/rocksdb/options.h` `DBOptions::verify_manifest_content_on_close`; `db/version_set.cc` `VersionSet::Close`
- **Suggested scope:** Add to chapter 3 or chapter 10.

### Async file opening changes when integrity failures surface
- **Why it matters:** The current unique-ID/open-time verification text assumes validation happens during `DB::Open()`, but `open_files_async` moves SST open/validation into background work and later read/compaction paths.
- **Where to look:** `include/rocksdb/options.h` `DBOptions::open_files_async`; `db/db_impl/db_impl_open.cc` async file-open flow; `db/db_test.cc` `OpenFilesAsyncTest`
- **Suggested scope:** Expand chapter 8 or chapter 10.

### Best-efforts recovery limitations are under-documented
- **Why it matters:** The current text talks about older MANIFESTs and missing SST/blob files, but omits two important constraints: BER does not attempt WAL recovery, and atomic-flush/AtomicGroup history can block some partial recoveries.
- **Where to look:** `include/rocksdb/options.h` `DBOptions::best_efforts_recovery`; `db/version_set.h` `VersionSet::TryRecover`; `db/db_impl/db_impl_open.cc` BER open flow
- **Suggested scope:** Expand chapter 10.

### File-checksum ingestion behavior is much more nuanced than the chapter suggests
- **Why it matters:** `verify_file_checksum=false` does not simply mean "skip recomputation." RocksDB still recomputes when metadata is absent/incomplete, and `write_global_seqno=true` forces a different post-ingestion checksum story.
- **Where to look:** `db/external_sst_file_ingestion_job.cc` checksum-generation and verification logic; `db/external_sst_file_basic_test.cc` ingestion checksum tests
- **Suggested scope:** Add a focused subsection in chapter 4.

## Depth Issues

### The predecessor-WAL section stops before the real cross-component call chain
- **Current:** Chapter 9 says a new WAL records predecessor info and recovery cross-checks it.
- **Missing:** The actual mechanism is WAL-to-WAL state threading during recovery: the previous recovered WAL produces `PredecessorWALInfo(log_number, size_bytes, last_seqno)`, the next WAL carries a recorded predecessor record, and `log::Reader` compares all three fields. The outcome then depends on `WALRecoveryMode`.
- **Source:** `db/db_impl/db_impl_open.cc` `ProcessLogFile`; `db/db_impl/db_impl_open.cc` `UpdatePredecessorWALInfo`; `db/log_reader.cc` `Reader::MaybeVerifyPredecessorWALInfo`; `db/db_wal_test.cc` `DBWALTrackAndVerifyWALsWithParamsTest.Basic`

### The output-verification chapter needs the exact compaction-path gating and dependencies
- **Current:** Chapter 7 explains the flags at a high level.
- **Missing:** `kVerifyIteration` only works because the write side conditionally computes an `OutputValidator` hash even when `paranoid_file_checks=false`; `kVerifyFileChecksum` is skipped unless a checksum factory is configured and the output file metadata actually contains a checksum; block-checksum verification does meta-block-only work when combined with full iteration to avoid redundant data-block reads.
- **Source:** `db/compaction/compaction_job.cc` `CompactionJob::VerifyOutputFiles`; `db/compaction/compaction_job.cc` hash-enable sites near `VerifyOutputFlags::kVerifyIteration`; `db/db_compaction_test.cc` `DBCompactionTest.VerifyIterationWithoutParanoidFileChecks`

## Structure and Style Violations

### Inline code quoting is used throughout despite the local style rule
- **File:** `index.md` and all chapter files
- **Details:** The local review prompt explicitly says "NO inline code quotes", but inline backticks are used extensively for options, enums, functions, and file paths in every chapter.

### "Key Invariant" is used for a statement that is not an invariant and is false
- **File:** `10_verification_recovery.md`
- **Details:** "RocksDB never silently discards data" is neither a true correctness invariant nor compatible with the documented recovery modes that intentionally drop data.

## Undocumented Complexity

### Close-time MANIFEST validation can rewrite the MANIFEST on the way out
- **What it is:** When `verify_manifest_content_on_close=true`, `VersionSet::Close` re-reads the current MANIFEST, re-validates CRCs and logical record decoding, and can rewrite a fresh MANIFEST before closing if corruption is detected.
- **Why it matters:** This is an unusual integrity mechanism: it changes close semantics, can surface I/O errors from close, and gives operators another place to look when MANIFEST corruption is intermittent.
- **Key source:** `include/rocksdb/options.h` `DBOptions::verify_manifest_content_on_close`; `db/version_set.cc` `VersionSet::Close`
- **Suggested placement:** Chapter 3 or chapter 10.

### SST validation timing depends on `max_open_files` and `open_files_async`
- **What it is:** Unique-ID verification and other SST-open validation are no longer just a `DB::Open()` concern. They may happen eagerly at open, lazily on first access, or asynchronously in background file-open work.
- **Why it matters:** It changes where corruption shows up: `DB::Open()`, background error handling, later reads, or later compactions.
- **Key source:** `include/rocksdb/options.h` `DBOptions::max_open_files`; `include/rocksdb/options.h` `DBOptions::open_files_async`; `db/db_impl/db_impl_open.cc` async file-open scheduling
- **Suggested placement:** Chapter 8 or chapter 10.

### Parsed-block protection coverage is broader and narrower than the chapter implies
- **What it is:** The protection applies to parsed data/index/meta blocks and related parsed structures, but not to raw compressed blocks, filter blocks/partitions, or compression dictionaries that are not represented as `Block`.
- **Why it matters:** Readers trying to reason about protection coverage need to know which in-memory surfaces are actually protected and which still rely only on on-disk checksums.
- **Key source:** `table/block_based/block.h` class comments and `Initialize*Protection` methods
- **Suggested placement:** Expand chapter 6.

### Mixed-generation file-checksum deployments only get partial audit coverage
- **What it is:** After turning on `file_checksum_gen_factory`, older SST/blob files without recorded checksum metadata are skipped by `VerifyFileChecksums()`.
- **Why it matters:** Operators can otherwise mistake a successful `VerifyFileChecksums()` run for whole-DB coverage when it is only covering files written after checksum metadata started being recorded.
- **Key source:** `db/db_impl/db_impl.cc` `DBImpl::VerifyFullFileChecksum`; `db/db_basic_test.cc` `DBBasicTest.VerifyFileChecksums`
- **Suggested placement:** Chapter 4 and chapter 10.

## Positive Notes

- The index is in the right shape for this doc family: overview, source-file map, chapter table, key characteristics, and key invariants, and it is exactly 40 lines.
- The chapter split is strong. The docs separate block/WAL/file/in-memory/output/recovery integrity layers in a way that mirrors the code structure well.
- Several foundational facts checked out cleanly, including `ChecksumType` values, `BlockBasedTableOptions::checksum = kXXH3`, `ReadOptions::verify_checksums = true`, `DBOptions::verify_sst_unique_id_in_manifest = true`, and the WAL header layout and CRC coverage rules.

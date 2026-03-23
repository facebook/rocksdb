# Fix Summary: data_integrity

## Issues Fixed

| Category | Count |
|----------|-------|
| Correctness | 13 |
| Completeness | 5 |
| Structure/style | 0 |

## Disagreements Found

3 disagreements documented in `data_integrity_debates.md`:
1. **Inline backtick usage** (low risk) -- CC says no violation, Codex says violation. CC is correct: backticks for identifiers is standard practice.
2. **paranoid_checks and MANIFEST checksums** (medium risk) -- CC implies paranoid_checks controls MANIFEST CRC. Codex correctly notes MANIFEST CRC is always on. Codex is correct.
3. **"Never silently discards data" invariant** (high risk) -- CC approved it. Codex correctly identified recovery modes that intentionally discard data. Codex is correct.

## Changes Made

### 01_checksum_types.md
- Fixed CRC masking claim: SST block CRC32c values ARE masked via `ComputeBuiltinChecksum()` (both CC and Codex flagged)

### 02_block_checksums.md
- Fixed `format_version` default from 6 to 7 (both CC and Codex flagged)
- Fixed `verify_compression` location from `CompressionOptions` to `BlockBasedTableOptions` (both CC and Codex flagged)
- Added `block_based_table_reader.h` to Files line for `kBlockTrailerSize` definition (CC flagged)

### 03_wal_manifest_checksums.md
- Added recyclable record types: `kRecyclableUserDefinedTimestampSizeType` (11) and `kRecyclePredecessorWALInfoType` (131) (CC flagged)
- Fixed `paranoid_checks` description: MANIFEST CRC verification is always on; paranoid_checks controls SST open failure handling (Codex flagged)
- Added `verify_manifest_content_on_close` documentation with close-time MANIFEST validation and self-healing behavior (both CC and Codex flagged as missing)

### 04_file_checksums_handoff.md
- Fixed `VerifyFileChecksums()` to document that files with `kUnknownFileChecksum` are skipped (Codex flagged)
- Added note about mixed-generation partial coverage (Codex flagged)
- Fixed WAL handoff: payload CRC only is passed, not full record CRC; header passes 0 (Codex flagged)

### 05_write_path_protection.md
- Clarified protection width support: WriteBatch only supports 0 and 8; memtable and block protection support 0/1/2/4/8 (CC flagged)

### 06_memtable_block_protection.md
- Fixed block protection description: applies to parsed `Block` objects, not tied to block cache residency (Codex flagged)
- Added specific initialization methods and clarified scope (data/index/meta blocks but not filter/compression dict)

### 07_output_verification.md
- Separated flush from compaction for `verify_output_flags` (compaction-only; flush uses `paranoid_file_checks`) (Codex flagged)
- Fixed `compaction_verify_record_count` to document both input AND output verification (Codex flagged)
- Fixed flush verification workflow to reference only `paranoid_file_checks` (Codex flagged)

### 08_unique_id_verification.md
- Clarified `orig_file_number` table property is passed as `file_number` parameter to `GetSstInternalUniqueId()` (CC flagged)

### 09_wal_tracking.md
- Fixed WAL size check: on-disk WAL must be at least the recorded synced size, not exact match (Codex flagged)

### 10_verification_recovery.md
- Fixed `paranoid_checks` description: MANIFEST CRC always verified; option controls SST open failure handling (Codex flagged)
- Narrowed "never silently discards data" claim to normal operations; explicitly listed recovery modes that trade data for availability (Codex flagged)
- Fixed `RepairDB()` flow to include WAL discovery and conversion before table salvage (Codex flagged)
- Added `max_bgerror_resume_count` default value (`INT_MAX`) (CC flagged)
- Added note that `best_efforts_recovery` does not attempt WAL recovery (Codex flagged)

### index.md
- Added close-time MANIFEST validation characteristic
- Added file checksum MANIFEST-only storage invariant

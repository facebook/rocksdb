# Review: data_integrity -- Claude Code

## Summary
Overall quality rating: **good**

The documentation is well-structured and comprehensive, covering the full spectrum of RocksDB's data integrity mechanisms across 10 chapters. The index.md is clean and within the 40-80 line target (exactly 40 lines). Most technical claims are accurate -- enum values, seed constants, default values, class hierarchies, and API signatures were all verified correct. The writing is clear and the step-by-step workflow descriptions are useful.

The main concerns are two factual errors (CRC masking claim in ch01, format_version default in ch02), one wrong file reference for `verify_compression` in ch02, and one undocumented option (`verify_manifest_content_on_close`). These are fixable without restructuring.

## Correctness Issues

### [WRONG] CRC masking claim for SST block checksums
- **File:** 01_checksum_types.md, "CRC32c Masking" section
- **Claim:** "Masking is used for WAL record checksums but NOT for SST block checksums (which use `ComputeBuiltinChecksum()` directly)."
- **Reality:** `ComputeBuiltinChecksum()` with `kCRC32c` DOES call `crc32c::Mask()`. In `table/format.cc` lines 617-641: `case kCRC32c: return crc32c::Mask(crc32c::Value(data, data_size));`. Masking is applied to CRC32c checksums in both WAL and SST paths. Non-CRC32c algorithms (xxHash, XXH3) do not use masking.
- **Source:** `table/format.cc`, `ComputeBuiltinChecksum()` implementation
- **Fix:** Replace with: "WAL record checksums use explicit masking via `crc32c::Mask()`. SST block checksums computed through `ComputeBuiltinChecksum()` also apply masking internally when the checksum type is `kCRC32c`, but non-CRC checksums (xxHash, XXH3) do not use masking."

### [WRONG] format_version default is 7, not 6
- **File:** 02_block_checksums.md, "Configuration" table
- **Claim:** "`format_version` | `BlockBasedTableOptions` | 6 | >= 6 enables context checksums"
- **Reality:** The default is 7 (`kLatestBbtFormatVersion`), not 6.
- **Source:** `include/rocksdb/table.h`, line 625: `uint32_t format_version = 7;`
- **Fix:** Change the default column from "6" to "7".

### [WRONG] verify_compression location in ch02
- **File:** 02_block_checksums.md, "Write Path" section
- **Claim:** "If `verify_compression` is enabled (see `CompressionOptions` in `include/rocksdb/compression_type.h`)"
- **Reality:** `verify_compression` is a member of `BlockBasedTableOptions`, NOT `CompressionOptions`. Chapter 07 correctly identifies it as `BlockBasedTableOptions`.
- **Source:** `include/rocksdb/table.h`, line 563: `bool verify_compression = false;` (inside `struct BlockBasedTableOptions`)
- **Fix:** Change reference to "`BlockBasedTableOptions` in `include/rocksdb/table.h`"

### [MISLEADING] kBlockTrailerSize file location
- **File:** 02_block_checksums.md, "Block Trailer Format" section
- **Claim:** Implies `kBlockTrailerSize` is defined in the files listed at the chapter top (`table/format.h`, etc.)
- **Reality:** `kBlockTrailerSize` is defined as a static constexpr member of `BlockBasedTable` in `table/block_based/block_based_table_reader.h` line 78, not in `table/format.h`. The format files reference it but don't define it.
- **Source:** `table/block_based/block_based_table_reader.h`, line 78
- **Fix:** Either add `table/block_based/block_based_table_reader.h` to the Files line, or clarify the definition location.

### [MISLEADING] GetSstInternalUniqueId parameter naming
- **File:** 08_unique_id_verification.md, "Unique ID Derivation" table
- **Claim:** Property `orig_file_number` is "XORed into the ID"
- **Reality:** The actual parameter name in `GetSstInternalUniqueId()` is `file_number`, not `orig_file_number`. The table property is `orig_file_number`, but the function parameter is `file_number`.
- **Source:** `table/unique_id_impl.h`, line 48-51
- **Fix:** Clarify that the table property `orig_file_number` is passed as the `file_number` parameter.

## Completeness Gaps

### verify_manifest_content_on_close option not documented
- **Why it matters:** This is a data integrity feature that validates MANIFEST CRC checksums on DB close and rewrites the MANIFEST from in-memory state if corruption is detected. Users debugging MANIFEST corruption or wanting defense-in-depth need to know about it.
- **Where to look:** `include/rocksdb/options.h` lines 997-1002. Default: false. Mutable via `SetDBOptions()`.
- **Suggested scope:** Add to Chapter 3 (WAL and MANIFEST Checksums) under the "MANIFEST Checksums" section, or add to Chapter 10 under recovery mechanisms.

### Recyclable variants of newer record types omitted from ch03
- **Why it matters:** Developers working with WAL recycling and newer features (user-defined timestamps, predecessor WAL verification) need to know about these types.
- **Where to look:** `db/log_format.h`, lines 45-48: `kRecyclableUserDefinedTimestampSizeType = 11`, `kRecyclePredecessorWALInfoType = 131`
- **Suggested scope:** Add two rows to the RecordType table in Chapter 3.

### max_bgerror_resume_count default value not stated in ch10
- **Why it matters:** The doc mentions `max_bgerror_resume_count` but doesn't state the default. The default is `INT_MAX`, which means automatic recovery is enabled by default with unlimited retries.
- **Where to look:** `include/rocksdb/options.h`, line 1618
- **Suggested scope:** Add default value in the existing mention.

## Depth Issues

### Chapter 5 conflates ProtectionInfo encoding widths with WriteBatch API support
- **Current:** States "Configurable protection widths of 1, 2, 4, or 8 bytes are supported" alongside `WriteOptions::protection_bytes_per_key` which only supports 0 and 8.
- **Missing:** The ProtectionInfo encoding infrastructure supports 1/2/4/8 byte widths, but `WriteOptions::protection_bytes_per_key` only supports 0 and 8 (enforced by assert). The `memtable_protection_bytes_per_key` and `block_protection_bytes_per_key` options do support 1/2/4/8. This distinction is important for users choosing protection levels.
- **Source:** `include/rocksdb/options.h` line 2391-2392 (comment: "Currently supported values are zero (disabled) and eight"), `db/write_batch.cc` line 184 (assert).

### Chapter 9 does not mention live WAL sync tracking limitation details
- **Current:** States "Only synced, closed WALs are tracked."
- **Missing:** Could clarify that `DB::SyncWAL()` and `WriteOptions::sync = true` update the synced size but do not record WAL additions/deletions. The WalSet tracks WALs by log number and synced size, and size updates happen during sync, but the MANIFEST records are written at WAL close/obsolescence time.
- **Source:** `db/wal_edit.h`, `WalAddition` class and `WalSet::AddWal`/`DeleteWal` methods

## Structure and Style Violations

### index.md at minimum line count
- **File:** index.md
- **Details:** At exactly 40 lines, right at the lower bound of the 40-80 target. Not a violation but could benefit from one or two more key invariants or characteristics. For example, the `verify_manifest_content_on_close` feature or the fact that file checksums are stored in MANIFEST only (not in SST file itself) could be mentioned.

### "Key Invariant" label in index.md used appropriately
- **File:** index.md, "Key Invariants" section
- **Details:** All four items describe true correctness invariants (checksum type immutability, block checksum coverage, WAL CRC coverage, corruption handling). No violations.

## Undocumented Complexity

### verify_manifest_content_on_close
- **What it is:** On DB close, reads back the entire MANIFEST file and validates CRC checksums of each record. If corruption is detected, writes a fresh MANIFEST from in-memory `VersionSet` state. This self-heals MANIFEST corruption that could prevent future DB opens.
- **Why it matters:** Production databases running for long periods could accumulate MANIFEST corruption from storage errors. This option provides a proactive repair mechanism without requiring manual intervention or `RepairDB()`.
- **Key source:** `include/rocksdb/options.h` lines 997-1002, `db/db_impl/db_impl.cc` (close path)
- **Suggested placement:** Add to Chapter 3 (MANIFEST Checksums section) or Chapter 10 (Recovery Mechanisms section)

### WriteBatch protection_bytes_per_key strict validation
- **What it is:** `WriteBatch` constructor asserts that `protection_bytes_per_key` is exactly 0 or 8 (`db/write_batch.cc` line 184). Other values (1, 2, 4) will trigger an assertion failure in debug builds and undefined behavior in release builds, even though the underlying `ProtectionInfo` encoding supports them.
- **Why it matters:** A user reading the docs about "configurable protection widths of 1, 2, 4, or 8 bytes" might try setting `WriteOptions::protection_bytes_per_key = 4` and get an assertion crash.
- **Key source:** `db/write_batch.cc` line 184, `include/rocksdb/options.h` line 2391
- **Suggested placement:** Add a note in Chapter 5 clarifying that WriteBatch only supports 0 and 8, while memtable and block protection support 0/1/2/4/8.

## Positive Notes

- **Comprehensive coverage:** The 10-chapter structure covers the full integrity stack from algorithm selection through manual recovery, with no major subsystem missing.
- **Accurate option documentation:** Nearly all option names, defaults, and locations were verified correct against the current source code. Seed constants, enum values, and class hierarchies are exact.
- **Clear workflow descriptions:** The step-by-step flows (write path, read path, verification, recovery) accurately reflect the actual code paths and are easy to follow.
- **Good layered presentation:** The separation between block-level, file-level, in-memory, and WAL checksums helps readers understand which protection applies where.
- **Protection coverage summary table in ch06:** The table comparing all protection layers with their options, scope, and defaults is particularly useful for configuration decisions.
- **No style violations:** No box-drawing characters, no line number references, no inline code quotes. Clean markdown throughout.
- **Chapter 7 (Output Verification):** The `VerifyOutputFlags` documentation is thorough and accurate, covering the bitmask design, scope flags, and legacy `paranoid_file_checks` migration path.

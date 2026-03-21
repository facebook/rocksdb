# Data Integrity & Checksum Verification

**Purpose**: This document describes RocksDB's data integrity mechanisms that detect corruption at rest and in transit. RocksDB employs multiple layers of checksums—from individual blocks to entire files—combined with paranoid verification options to ensure data correctness.

**Scope**: Covers checksum types, per-block checksums, WAL checksums, MANIFEST integrity, handoff checksums, file checksums, paranoid checks, and corruption detection/recovery.

---

## Overview

RocksDB protects data integrity through several independent mechanisms:

```
┌─────────────────────────────────────────────────────────────┐
│                     Data Integrity Layers                    │
├─────────────────────────────────────────────────────────────┤
│  1. Block Checksum (SST data/index/filter blocks)           │
│  2. WAL Record Checksum (per-record CRC32c)                 │
│  3. MANIFEST Record Checksum (VersionEdit integrity)        │
│  4. Handoff Checksum (writer → filesystem)                  │
│  5. File Checksum (full-file verification)                  │
│  6. MemTable Protection (per-key checksums)                 │
│  7. Paranoid Verification (extra checks at runtime)         │
└─────────────────────────────────────────────────────────────┘
```

**⚠️ INVARIANT**: All checksums use big-endian encoding for cross-platform consistency, except block-level CRC32c which uses `crc32c::Mask()` to prevent all-zeros checksums from silent corruption.

**⚠️ INVARIANT**: Corruption detected during read operations returns `Status::Corruption()` rather than crashing, allowing applications to implement recovery strategies.

---

## 1. Checksum Types

RocksDB supports multiple checksum algorithms with different performance/security tradeoffs.

### Supported Algorithms

```cpp
// include/rocksdb/table.h:55
enum ChecksumType : char {
  kNoChecksum = 0x0,  // No checksum (not recommended)
  kCRC32c = 0x1,      // CRC32c with hardware acceleration (SSE4.2, ARM64 CRC)
  kxxHash = 0x2,      // xxHash 32-bit
  kxxHash64 = 0x3,    // xxHash 64-bit (truncated to 32 bits)
  kXXH3 = 0x4,        // XXH3 64-bit (default, fastest on modern CPUs)
};
```

**Default**: `kXXH3` (since RocksDB 6.27) provides best performance on modern hardware.

**Configuration**:
```cpp
// Set checksum type for new SST files
BlockBasedTableOptions table_options;
table_options.checksum = kXXH3;  // or kCRC32c, kxxHash, kxxHash64
```

### Implementation Details

**CRC32c** (`util/crc32c.cc`):
- Uses hardware acceleration when available (SSE4.2 `_mm_crc32_u*`, ARM64 CRC instructions, PowerPC crypto extensions)
- Falls back to software implementation with slice-by-8 algorithm
- Applies masking: `((crc >> 15) | (crc << 17)) + 0xa282ead8` to avoid all-zeros
- **Performance**: ~20 GB/s on modern x86 CPUs with SSE4.2

**xxHash/xxHash64/XXH3** (`util/xxhash.cc`):
- Uses upstream xxHash library (https://github.com/Cyan4973/xxHash)
- XXH3 leverages SIMD (AVX2, AVX512, NEON) for peak performance
- No masking applied (collision resistance from algorithm design)
- **Performance**: XXH3 achieves ~60 GB/s on modern CPUs with AVX2

**⚠️ INVARIANT**: Checksum type is stored in the SST footer and cannot change after file creation. Reading old files with different checksums is supported.

---

## 2. Block Checksums (SST Files)

Every block in an SST file (data, index, filter, compression dictionary) has a 5-byte trailer containing checksum + compression type.

### Block Trailer Format

```
┌──────────────────┬─────────────┬──────────────┐
│   Block Data     │  Checksum   │ Compression  │
│   (variable)     │  (4 bytes)  │   (1 byte)   │
└──────────────────┴─────────────┴──────────────┘
                    └── 5-byte Block Trailer ──┘
```

**Trailer Layout**:
- **Checksum (4 bytes)**: Computed over `compression_type + block_data`
- **Compression Type (1 byte)**: `kNoCompression`, `kSnappyCompression`, `kZSTD`, etc.

**⚠️ INVARIANT**: The checksum covers BOTH the compression type byte AND the compressed/uncompressed block data. This prevents compression type corruption from going undetected.

### Writing Checksums

```cpp
// table/block_based/block_based_table_builder.cc
// When WriteBlock() is called:

Status WriteBlock(const Slice& block_contents,
                  BlockHandle* handle,
                  CompressionType compression_type) {
  // 1. Compress block if needed
  Slice compressed = MaybeCompress(block_contents, compression_type);

  // 2. Compute checksum over compression_type + data
  char trailer[kBlockTrailerSize];
  trailer[0] = static_cast<char>(compression_type);
  uint32_t checksum = ComputeBuiltinChecksum(
      checksum_type_,
      compressed.data(),
      compressed.size(),
      /*include_trailer_byte=*/true,
      trailer[0]);

  EncodeFixed32(trailer + 1, checksum);

  // 3. Write: [block_data][5-byte trailer]
  file_->Append(compressed);
  file_->Append(Slice(trailer, kBlockTrailerSize));
}
```

**Context Checksums** (Format Version ≥ 6):
For format version 6+, RocksDB adds context to checksums to prevent cross-file and cross-block corruption:

```cpp
// table/format.h:136
inline uint32_t ChecksumModifierForContext(uint32_t base_context_checksum,
                                           uint64_t offset) {
  // Mix base checksum (derived from unique_id) with block offset
  uint32_t modifier =
      base_context_checksum ^ (Lower32of64(offset) + Upper32of64(offset));
  return modifier;  // XORed with raw checksum
}
```

This prevents:
- Blocks from different files appearing valid if copied incorrectly
- Blocks from the same file being swapped without detection

**⚠️ INVARIANT**: Format version ≥ 6 files MUST have a unique ID in the footer. Context checksums are derived from `unique_id ⊕ block_offset`.

### Reading and Verifying Checksums

```cpp
// table/block_fetcher.cc:62
if (read_options_.verify_checksums) {
  io_status_ = VerifyBlockChecksum(
      footer_,               // Provides checksum type and context
      slice_.data(),         // Block data + trailer
      block_size_,           // Block size (excluding trailer)
      file_->file_name(),    // For error reporting
      handle_.offset(),      // Block offset (for context checksum)
      block_type_);          // For error messages

  RecordTick(ioptions_.stats, BLOCK_CHECKSUM_COMPUTE_COUNT);

  if (!io_status_.ok()) {
    return Status::Corruption("Block checksum mismatch: " +
                               file_->file_name());
  }
}
```

**Read Options**:
```cpp
ReadOptions read_opts;
read_opts.verify_checksums = true;  // Default: true
// Checksum verification happens on every Get(), Iterator operation
```

**Performance**: With `verify_checksums = true` and XXH3, checksum overhead is ~1-2% on typical workloads.

**⚠️ INVARIANT**: `verify_checksums = false` disables checksum verification for READS ONLY. Writes always compute and store checksums.

---

## 3. WAL Checksums

The Write-Ahead Log (WAL) uses per-record CRC32c checksums to detect corruption during recovery.

### WAL Record Format

```
┌───────────┬────────┬────────┬────────┬─────────────┐
│ Checksum  │ Length │ Length │  Type  │   Payload   │
│ (4 bytes) │  Low   │  High  │(1 byte)│  (variable) │
│           │(1 byte)│(1 byte)│        │             │
└───────────┴────────┴────────┴────────┴─────────────┘
 └─────────── kHeaderSize = 7 bytes ────────────────┘
```

**Checksum Coverage**: Covers `Type + Length + Payload` (NOT the checksum field itself).

**Recyclable WAL Format** (recycled log files add 4 more bytes):
```
┌───────────┬────────┬────────┬────────┬────────────┬─────────────┐
│ Checksum  │ Length │ Length │  Type  │ Log Number │   Payload   │
│ (4 bytes) │  Low   │  High  │(1 byte)│  (4 bytes) │  (variable) │
└───────────┴────────┴────────┴────────┴────────────┴─────────────┘
 └──────────── kRecyclableHeaderSize = 11 bytes ──────────────────┘
```

**Record Types**:
- `kFullType`: Complete record in single chunk
- `kFirstType`, `kMiddleType`, `kLastType`: Fragmented records spanning multiple 32KB blocks
- `kRecyclableFullType`, etc.: Same as above but for recycled WAL files
- `kSetCompressionType`, `kPredecessorWALInfoType`: Metadata records

**⚠️ INVARIANT**: WAL records ALWAYS use CRC32c regardless of SST checksum settings. This ensures fast hardware-accelerated verification during crash recovery.

### WAL Checksum Verification

```cpp
// db/log_reader.cc:625
if (checksum_) {
  uint32_t expected_crc = crc32c::Unmask(DecodeFixed32(header));
  uint32_t actual_crc = crc32c::Value(header + 6, length + header_size - 6);

  if (actual_crc != expected_crc) {
    *drop_size = buffer_.size();
    buffer_.clear();
    return kBadRecordChecksum;  // Triggers corruption handling
  }
}
```

**Recovery Modes**:
```cpp
DBOptions options;
options.wal_recovery_mode = WALRecoveryMode::kTolerateCorruptedTailRecords;
// Options:
// - kAbsoluteConsistency: Any corruption is fatal
// - kPointInTimeRecovery: Tolerates incomplete tail records
// - kSkipAnyCorruptedRecords: Skips all corrupted records (data loss risk)
// - kTolerateCorruptedTailRecords: Default, tolerates incomplete writes
```

**Corruption Handling**:
```cpp
// db/log_reader.cc:333
case kBadRecordChecksum:
  ReportCorruption(drop_size, "checksum mismatch");
  // Higher layer (VersionSet, DBImpl) decides whether to:
  // 1. Return error to user
  // 2. Skip corrupted record
  // 3. Replay up to corruption point
```

**⚠️ INVARIANT**: WAL corruption during recovery is ONLY tolerated for the last record in the last WAL file (incomplete write). Corruption in the middle triggers `Status::Corruption()`.

### WAL Handoff Checksums (End-to-End)

When `checksum_handoff_file_types` includes WAL files, RocksDB computes XXH3 checksums during writes and passes them to the filesystem for verification.

```cpp
// file/writable_file_writer.cc
Status WritableFileWriter::Append(const Slice& data) {
  if (checksum_generator_) {
    // Update end-to-end checksum
    checksum_generator_->Update(data.data(), data.size());
  }

  // Pass checksum to filesystem
  DataVerificationInfo verify_info;
  verify_info.checksum = Slice(reinterpret_cast<char*>(&checksum),
                               sizeof(checksum));
  return writable_file_->Append(data, io_options, verify_info, io_dbg);
}
```

This protects against corruption in the I/O path between RocksDB and disk.

---

## 4. MANIFEST Checksums

The MANIFEST file (tracking SST file metadata via `VersionEdit` records) uses the same WAL record format with CRC32c checksums.

```
MANIFEST-XXXXXX
└── Sequence of VersionEdit records (each checksummed like WAL)
    ├── AddFile: SST file metadata + checksum info
    ├── DeleteFile: File deletions
    ├── ColumnFamilyAdd/Drop: CF lifecycle
    └── LogNumber: Current WAL number
```

**⚠️ INVARIANT**: MANIFEST corruption is ALWAYS fatal. RocksDB cannot open a DB with a corrupted MANIFEST (no recovery mode).

**VersionEdit Integrity**:
Each `VersionEdit` record stores SST file checksums:
```cpp
// db/version_edit.h
struct FileMetaData {
  uint64_t file_number;
  std::string file_checksum;        // Full-file checksum (if enabled)
  std::string file_checksum_func_name;  // e.g., "FileChecksumCrc32c"
  std::string unique_id;            // 192-bit unique ID
  // ...
};
```

When paranoid_checks = true, RocksDB verifies:
1. MANIFEST record checksums (always)
2. SST file existence (on Open)
3. SST unique_id matches MANIFEST (if `verify_sst_unique_id_in_manifest = true`)

---

## 5. Handoff Checksums (Writer → Filesystem)

Handoff checksums provide end-to-end data integrity from RocksDB's write buffer to the storage layer.

### Configuration

```cpp
DBOptions options;
options.checksum_handoff_file_types = {kWALFile, kTableFile, kDescriptorFile};
// Default: empty set (disabled)
// Types: kWALFile, kTableFile, kDescriptorFile, kTempFile, kBlobFile
```

**Checksum Type**: Always `ChecksumType::kCRC32c` (matches filesystem support).

### Write Path

```
┌─────────────┐   Compute CRC32c   ┌──────────────┐   Verify    ┌──────────┐
│  RocksDB    │ ─────────────────> │  Filesystem  │ ─────────>  │   Disk   │
│  (Writer)   │   DataVerificationInfo│  (Storage) │             │          │
└─────────────┘                    └──────────────┘             └──────────┘
```

**Implementation**:
```cpp
// file/writable_file_writer.cc
IOStatus WritableFileWriter::Append(const Slice& data) {
  DataVerificationInfo verify_info;

  if (perform_checksum_handoff_) {
    // Compute checksum for this write
    verify_info.checksum = crc32c::Value(data.data(), data.size());
  }

  // Filesystem can verify checksum before persisting
  return writable_file_->Append(data, io_options, verify_info, dbg);
}
```

**Filesystem Requirements**:
- Must support `DataVerificationInfo` parameter in `Append()` / `PositionedAppend()`
- If unsupported, checksum is silently ignored (no error)
- Checksum verification happens in storage firmware or filesystem layer (e.g., ext4 with data=journal, ZFS)

**⚠️ INVARIANT**: If the storage layer has DIFFERENT checksum support (e.g., xxHash), set `checksum_handoff_file_types` to empty set to avoid write failures.

---

## 6. File Checksums (Full-File Verification)

RocksDB can compute and store a checksum for the ENTIRE SST file (not just per-block).

### Configuration

```cpp
DBOptions options;
options.file_checksum_gen_factory = GetFileChecksumGenCrc32cFactory();
// Default: nullptr (disabled)
```

**Builtin Factory**: `GetFileChecksumGenCrc32cFactory()` provides CRC32c-based file checksums.

**Custom Checksums**:
```cpp
class MyFileChecksumGenerator : public FileChecksumGenerator {
 public:
  void Update(const char* data, size_t n) override {
    // Update checksum state (called during writes)
  }

  void Finalize() override {
    // Compute final checksum
  }

  std::string GetChecksum() const override {
    // Return binary checksum (can include non-printable chars)
  }

  const char* Name() const override {
    return "MyChecksum";
  }
};
```

### File Checksum Lifecycle

**Write Time**:
```cpp
// table/table_builder.cc
TableBuilder::Finish() {
  if (file_checksum_generator) {
    // Called for every write to the file
    file_checksum_generator->Update(data, size);
  }

  // At file close:
  file_checksum_generator->Finalize();
  std::string checksum = file_checksum_generator->GetChecksum();

  // Store in TableProperties
  table_properties.file_checksum = checksum;
  table_properties.file_checksum_func_name =
      file_checksum_generator->Name();
}
```

**Metadata Storage**:
File checksums are stored in:
1. **SST TableProperties** (inside the file)
2. **MANIFEST** (VersionEdit::AddFile record)

**Verification**:
```cpp
// Verify during file ingestion
IngestExternalFileOptions ingest_opts;
ingest_opts.verify_file_checksum = true;  // Default: true

// Verify existing files
ReadOptions read_opts;
Status s = db->VerifyFileChecksums(read_opts);
// Iterates all SST files, recomputes checksums, compares with metadata
```

**⚠️ INVARIANT**: File checksums stored in MANIFEST are treated as ground truth. If SST's embedded checksum disagrees with MANIFEST, MANIFEST wins.

---

## 7. Paranoid Checks

Paranoid options enable extra integrity verification at the cost of performance.

### paranoid_checks (DBOptions)

```cpp
DBOptions options;
options.paranoid_checks = true;  // Default: true
```

**Effects**:
- Verifies MANIFEST and WAL checksums during DB::Open()
- Checks SST file existence and basic metadata consistency
- Validates VersionEdit record integrity
- **Does NOT**: Verify all SST block checksums (use `paranoid_file_checks` instead)

**⚠️ INVARIANT**: With `paranoid_checks = false`, corrupted metadata can cause undefined behavior. Only disable for bulk loading scenarios.

### paranoid_file_checks (ColumnFamilyOptions)

```cpp
ColumnFamilyOptions cf_options;
cf_options.paranoid_file_checks = false;  // Default: false
```

**Effects when enabled**:
1. **After Flush**: Reads all blocks of newly created SST file to verify checksums
2. **After Compaction**: Reads all blocks of compaction outputs
3. **During DB::Open**: Optionally verifies SST files (controlled by `verify_sst_unique_id_in_manifest`)

**Performance Impact**: Doubles I/O for flushes/compactions (write + verify read).

**⚠️ INVARIANT**: `paranoid_file_checks = true` is incompatible with compaction resumption (`CompactionServiceOptions::allow_resumption = true`).

### verify_sst_unique_id_in_manifest (DBOptions)

```cpp
DBOptions options;
options.verify_sst_unique_id_in_manifest = true;  // Default: true
```

**Effects**:
- During DB::Open(), verifies that each SST's `unique_id` (read from file footer) matches the `unique_id` stored in MANIFEST
- Detects if SST files were swapped, copied from another DB, or corrupted

**Unique ID Generation**:
```cpp
// table/unique_id_impl.h
// Unique ID = 192 bits derived from:
// - DB session ID (120 bits)
// - File number (64 bits, truncated to fit)
// Encoded as: session_lower64 ^ file_number, session_upper64, session_mid64
```

**⚠️ INVARIANT**: SST files copied between databases will have mismatched unique IDs, causing DB::Open() to fail with `Status::Corruption()`.

### paranoid_memory_checks (ColumnFamilyOptions)

```cpp
ColumnFamilyOptions cf_options;
cf_options.paranoid_memory_checks = false;  // Default: false
```

**Effects**:
- Enables per-key checksum validation during MemTable seeks (requires `memtable_protection_bytes_per_key > 0`)
- Validates KV checksum consistency before writes

**Related Option**:
```cpp
cf_options.memtable_protection_bytes_per_key = 8;  // 0 = disabled (default)
// Adds per-key checksums in MemTable (8 bytes overhead per key)
```

**Performance**: ~5-10% overhead for MemTable operations when enabled.

---

## 8. Compaction Output Verification

Compaction jobs can optionally verify the integrity of output SST files.

### paranoid_file_checks (Compaction Context)

Already covered in Section 7. When enabled, compaction outputs are read and checksummed before being added to the version.

### verify_table_property_collectors (Advanced)

```cpp
// Not a user option; always enabled
// During compaction, RocksDB verifies:
// 1. Output file statistics match TablePropertiesCollector aggregates
// 2. No data loss during compaction (entry count consistency)
```

**Implementation**:
```cpp
// db/compaction/compaction_job.cc
Status CompactionJob::FinishCompactionOutputFile() {
  if (paranoid_file_checks_) {
    // Read entire file to verify
    std::unique_ptr<Iterator> iter(
        table_cache_->NewIterator(read_options, ...));

    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
      // Checksum verification happens automatically via ReadOptions
    }

    if (!iter->status().ok()) {
      return Status::Corruption("Compaction output verification failed");
    }
  }
}
```

---

## 9. Error Handling and Corruption Recovery

RocksDB detects corruption at multiple levels and provides recovery mechanisms.

### Corruption Detection

**Read Path**:
```cpp
Status s = db->Get(read_options, key, &value);
if (s.IsCorruption()) {
  // Block checksum mismatch, file read error, or metadata corruption
  // Possible causes:
  // 1. Disk bit flip
  // 2. Filesystem bug
  // 3. Memory corruption
  // 4. Software bug (data race, buffer overflow)
}
```

**Background Jobs**:
```cpp
// Compaction encounters corruption
listeners.OnBackgroundError(BackgroundErrorReason::kCompaction,
                            Status::Corruption("Block checksum mismatch"),
                            db);
// Triggers error handler (may pause writes, mark DB read-only, etc.)
```

### Error Handler (Background Corruption)

```cpp
// db/error_handler.h
class ErrorHandler {
  // Decides how to handle background errors:
  // - Pause writes (wait for intervention)
  // - Mark DB read-only
  // - Attempt automatic recovery (discard corrupted SST)
};
```

**Automatic Recovery** (for compaction corruption):
1. Mark corrupted SST as "bad" in MANIFEST
2. Schedule new compaction excluding bad file
3. Resume normal operations

**⚠️ INVARIANT**: RocksDB NEVER silently discards data. All corruption is either logged, reported via `Status`, or handled by `ErrorHandler` with user-visible effects.

### Manual Verification

**Verify All SST Checksums**:
```cpp
ReadOptions read_opts;
Status s = db->VerifyChecksum(read_opts);
// Reads all blocks of all SST files
// Returns first error encountered
```

**Verify File Checksums Only**:
```cpp
ReadOptions read_opts;
Status s = db->VerifyFileChecksums(read_opts);
// Faster: only checks full-file checksums (if enabled)
// Skips per-block verification
```

**Repair Corrupted DB**:
```cpp
#include "rocksdb/db.h"

Options options;
Status s = RepairDB("/path/to/db", options);
// Attempts to salvage uncorrupted data
// WARNING: May result in data loss
```

---

## 10. Best Practices

### Production Deployments

1. **Enable paranoid_checks**: Always `true` unless bulk loading
2. **Use XXH3 checksums**: Fastest on modern CPUs
3. **Enable file checksums**: Set `file_checksum_gen_factory` for auditability
4. **Monitor corruption events**: Implement `EventListener::OnBackgroundError()`
5. **Verify backups**: Run `VerifyChecksum()` before and after backups

### Performance-Critical Workloads

```cpp
ReadOptions read_opts;
read_opts.verify_checksums = true;  // 1-2% overhead, worth it

ColumnFamilyOptions cf_opts;
cf_opts.paranoid_file_checks = false;  // Skip double I/O for compaction
cf_opts.paranoid_memory_checks = false;  // Skip MemTable checksums
```

### Maximum Paranoia (Development/Testing)

```cpp
DBOptions db_opts;
db_opts.paranoid_checks = true;
db_opts.verify_sst_unique_id_in_manifest = true;
db_opts.file_checksum_gen_factory = GetFileChecksumGenCrc32cFactory();
db_opts.checksum_handoff_file_types = {kWALFile, kTableFile};

ColumnFamilyOptions cf_opts;
cf_opts.paranoid_file_checks = true;
cf_opts.paranoid_memory_checks = true;
cf_opts.memtable_protection_bytes_per_key = 8;
cf_opts.memtable_veirfy_per_key_checksum_on_seek = true;

BlockBasedTableOptions table_opts;
table_opts.checksum = kXXH3;
```

---

## 11. Debugging Corruption Issues

### Identifying the Source

**Checksum Mismatch in SST Block**:
```
Status::Corruption: "Corrupted block read from /path/to/file.sst:
                     offset 12345, size 4096, checksum mismatch"
```
→ Run `db->VerifyFileChecksums()` to check if entire file is corrupted
→ If file checksum OK but block fails: likely memory corruption or storage bug
→ If file checksum fails: disk/filesystem corruption

**WAL Corruption**:
```
Status::Corruption: "log::Reader 000123.log EOF encountered in middle of record"
```
→ Check `wal_recovery_mode` setting
→ Incomplete writes (power loss) vs. mid-record corruption (disk failure)

**MANIFEST Corruption**:
```
Status::Corruption: "MANIFEST checksum mismatch"
```
→ FATAL: Cannot open DB
→ Restore from backup or attempt `RepairDB()`

### Reproducing Corruption

**Inject Block Corruption** (testing):
```cpp
// test_util/fault_injection_test_fs.h
FaultInjectionTestFS fault_fs(base_fs);
fault_fs.InjectCorruption(file_name, offset, num_bytes);
```

**Stress Testing**:
```bash
# db_stress with checksum verification
./db_stress --verify_checksum=1 --paranoid_file_checks=1 \
  --enable_compaction_filter=0 --mmap_read=0
```

---

## Related Components

- **Block Cache** (`cache/`): Caches blocks AFTER checksum verification
- **File I/O** (`file/`): Implements handoff checksums
- **Compaction** (`db/compaction/`): Uses paranoid_file_checks for output verification
- **Version Management** (`db/version_*.cc`): Stores file checksums in MANIFEST
- **Table Format** (`table/`): Encodes/decodes block trailers with checksums

---

## Key Source Files

| File | Purpose |
|------|---------|
| `util/crc32c.cc` | CRC32c implementation with hardware acceleration |
| `util/xxhash.cc` | xxHash/XXH3 implementation |
| `table/block_based/block_based_table_builder.cc` | Writes block checksums to SST files |
| `table/block_based/block_based_table_reader.cc` | Verifies block checksums on reads |
| `table/block_fetcher.cc` | Fetches blocks and verifies checksums |
| `table/format.h` | Footer format, checksum types, context checksums |
| `db/log_reader.cc` | WAL record checksum verification |
| `db/log_writer.cc` | WAL record checksum computation |
| `file/file_checksum_helper.cc` | File-level checksum generators |
| `include/rocksdb/file_checksum.h` | FileChecksumGen* interfaces |
| `include/rocksdb/options.h` | paranoid_checks, verify_sst_unique_id_in_manifest |
| `include/rocksdb/advanced_options.h` | paranoid_file_checks, paranoid_memory_checks |
| `include/rocksdb/table.h` | ChecksumType enum, BlockBasedTableOptions::checksum |

---

## Revision History

- **2026-03-21**: Initial version covering all checksum mechanisms

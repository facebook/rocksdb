# Data Integrity & Checksum Verification

**Purpose**: This document describes RocksDB's data integrity mechanisms that detect corruption at rest and in transit. RocksDB employs multiple layers of checksums—from individual blocks to entire files—combined with paranoid verification options to ensure data correctness.

**Scope**: Covers checksum types, per-block checksums, WAL checksums, MANIFEST integrity, handoff checksums, file checksums, paranoid checks, and corruption detection/recovery.

---

## Overview

RocksDB protects data integrity through several independent mechanisms:

```
Data Integrity Layers:
  1. Block Checksum (SST data/index/filter blocks)
  2. SST Footer Checksum (format version >= 6)
  3. WAL Record Checksum (per-record CRC32c)
  4. WAL Tracking & Verification (MANIFEST/predecessor WAL)
  5. MANIFEST Record Checksum (VersionEdit integrity)
  6. Handoff Checksum (writer -> filesystem)
  7. File Checksum (full-file verification)
  8. Blob File Checksums (header/blob/footer CRCs)
  9. WriteBatch Protection (per-key checksums in write path)
 10. MemTable Protection (per-key checksums)
 11. Block Protection (in-memory per-key checksums)
 12. Paranoid Verification (extra checks at runtime)
```

**⚠️ INVARIANT**: Most on-disk checksums (SST block trailers, WAL headers, SST footer checksums) use little-endian encoding via `EncodeFixed32`/`DecodeFixed32`. The exception is the built-in full-file checksum generator (`FileChecksumGenCrc32c`), which explicitly stores big-endian raw bytes via `EndianSwapValue`. Block-level CRC32c uses `crc32c::Mask()` to prevent all-zeros checksums from silent corruption.

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
Block Data (variable) | Compression (1 byte) | Checksum (4 bytes)
                       <--- 5-byte Block Trailer --->
```

**Trailer Layout** (offset within trailer):
- **Compression Type (1 byte, offset 0)**: `kNoCompression`, `kSnappyCompression`, `kZSTD`, etc.
- **Checksum (4 bytes, offset 1)**: Computed over `block_data + compression_type_byte`

**⚠️ INVARIANT**: The checksum covers BOTH the compressed/uncompressed block data AND the compression type byte (trailer[0]). This prevents compression type corruption from going undetected.

### Writing Checksums

```cpp
// table/block_based/block_based_table_builder.cc
// When WriteBlock() is called:

Status WriteBlock(const Slice& block_contents,
                  BlockHandle* handle,
                  CompressionType compression_type) {
  // 1. Compress block if needed
  Slice compressed = MaybeCompress(block_contents, compression_type);

  // 2. Compute checksum over block_data + compression_type
  char trailer[kBlockTrailerSize];
  trailer[0] = static_cast<char>(compression_type);
  uint32_t checksum = ComputeBuiltinChecksum(
      checksum_type_,
      compressed.data(),
      compressed.size(),
      /*include_trailer_byte=*/true,
      trailer[0]);

  // Context checksum modifier (format version >= 6)
  checksum += ChecksumModifierForContext(...);

  EncodeFixed32(trailer + 1, checksum);  // little-endian at trailer[1..4]

  // 3. Write: [block_data][compression_type(1)][checksum(4)]
  file_->Append(compressed);
  file_->Append(Slice(trailer, kBlockTrailerSize));
}
```

**Context Checksums** (Format Version ≥ 6):
For format version 6+, RocksDB adds context to checksums to prevent cross-file and cross-block corruption:

```cpp
// table/block_based/block_based_table_builder.cc
// base_context_checksum is a random non-zero 32-bit value chosen at
// table-build time and stored in the SST footer.
base_context_checksum_ = Random::GetTLSInstance()->Next() | 1;

// table/format.h:136
inline uint32_t ChecksumModifierForContext(uint32_t base_context_checksum,
                                           uint64_t offset) {
  // Mix base checksum with block offset
  uint32_t modifier =
      base_context_checksum ^ (Lower32of64(offset) + Upper32of64(offset));
  return modifier;  // Added to checksum on write, subtracted on read
}
```

The modifier is **added** to the raw checksum during writes and **subtracted** during reads (not XORed). `base_context_checksum` is stored in the SST footer via `EncodeFixed32`.

This prevents:
- Blocks from different files appearing valid if copied incorrectly
- Blocks from the same file being swapped without detection

**Note**: `base_context_checksum` is a random value, unrelated to the SST unique ID mechanism (which is derived from table properties).

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
// Checksum verification happens when blocks are read from underlying storage.
// Blocks served from the block cache are NOT re-verified on each read.
```

**Performance**: With `verify_checksums = true` and XXH3, checksum overhead is ~1-2% on typical workloads.

**⚠️ INVARIANT**: `verify_checksums = false` disables checksum verification for READS ONLY. Writes always compute and store checksums.

---

## 3. WAL Checksums

The Write-Ahead Log (WAL) uses per-record CRC32c checksums to detect corruption during recovery.

### WAL Record Format

```
Checksum (4 bytes) | Length Low (1 byte) | Length High (1 byte) | Type (1 byte) | Payload (variable)
<------------------------- kHeaderSize = 7 bytes --------------------------->
```

**Checksum Coverage**: Covers `Type + Payload` only. The length bytes are NOT included in the CRC. The writer precomputes CRC over the record type byte, then combines it with the payload CRC using `crc32c::Crc32cCombine()`. For recyclable records, the 4-byte log number is also included in the CRC.

**Recyclable WAL Format** (recycled log files add 4 more bytes):
```
Checksum (4 bytes) | Length Low (1 byte) | Length High (1 byte) | Type (1 byte) | Log Number (4 bytes) | Payload (variable)
<------------------------------ kRecyclableHeaderSize = 11 bytes ------------------------------>
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
options.wal_recovery_mode = WALRecoveryMode::kPointInTimeRecovery;
// Options:
// - kTolerateCorruptedTailRecords: Tolerates incomplete tail records/trailing zeros
// - kAbsoluteConsistency: Any corruption is fatal
// - kPointInTimeRecovery: Default. Stops replay before corruption point
// - kSkipAnyCorruptedRecords: Skips all corrupted records (data loss risk)
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

**⚠️ INVARIANT**: Corruption tolerance depends on the recovery mode. `kTolerateCorruptedTailRecords` tolerates incomplete tail records and trailing zeros. `kPointInTimeRecovery` (default) stops replaying before the first corruption. `kSkipAnyCorruptedRecords` can skip corrupted records anywhere. The reader also has special handling for recycled log files.

### WAL Handoff Checksums (End-to-End)

When `checksum_handoff_file_types` includes WAL files, RocksDB computes CRC32c checksums during writes and passes them to the filesystem for verification.

```cpp
// file/writable_file_writer.h
// Handoff is always CRC32c regardless of SST checksum type.
IOStatus Append(const IOOptions& opts, const Slice& data,
                uint32_t crc32c_checksum = 0);

// file/writable_file_writer.cc
// When perform_data_verification_ is true, CRC32c is computed and
// passed to the filesystem via DataVerificationInfo.
DataVerificationInfo verify_info;
verify_info.checksum = Slice(reinterpret_cast<char*>(&checksum),
                             sizeof(checksum));
return writable_file_->Append(data, io_options, verify_info, io_dbg);
```

This protects against corruption in the I/O path between RocksDB and disk.

---

## 4. MANIFEST Checksums

The MANIFEST file (tracking SST file metadata via `VersionEdit` records) uses the same WAL record format with CRC32c checksums.

```
MANIFEST-XXXXXX
  Sequence of VersionEdit records (each checksummed like WAL):
    - AddFile: SST file metadata + checksum info
    - DeleteFile: File deletions
    - ColumnFamilyAdd/Drop: CF lifecycle
    - LogNumber: Current WAL number
```

**⚠️ INVARIANT**: MANIFEST corruption is fatal by default. However, with `best_efforts_recovery = true`, RocksDB can try older MANIFEST files in reverse chronological order and recover to the most recent consistent point-in-time state.

**VersionEdit Integrity**:
Each `VersionEdit` record stores SST file checksums:
```cpp
// db/version_edit.h
struct FileMetaData {
  FileDescriptor fd;                      // Contains file number, path, size
  std::string file_checksum;              // Full-file checksum (if enabled)
  std::string file_checksum_func_name;    // e.g., "FileChecksumCrc32c"
  UniqueId64x2 unique_id{};              // 128-bit unique ID
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
options.checksum_handoff_file_types = {kWalFile, kTableFile, kDescriptorFile};
// Default: empty set (disabled)
// Types: kWalFile, kTableFile, kDescriptorFile, kTempFile, kBlobFile
```

**Checksum Type**: Always CRC32c for handoff (see `options.h`: "currently RocksDB only generates crc32c based checksum for the handoff").

### Write Path

```
RocksDB (Writer) --[Compute CRC32c, DataVerificationInfo]--> Filesystem (Storage) --[Verify]--> Disk
```

**Implementation**:
```cpp
// file/writable_file_writer.cc
// WritableFileWriter::Append computes CRC32c and passes it to the
// filesystem via DataVerificationInfo for end-to-end verification.
IOStatus WritableFileWriter::Append(const IOOptions& opts,
                                    const Slice& data,
                                    uint32_t crc32c_checksum) {
  DataVerificationInfo verify_info;

  if (perform_data_verification_) {
    // Compute or use provided CRC32c checksum
    verify_info.checksum = ...;
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
// file/writable_file_writer.cc
// The full-file checksum generator (checksum_generator_) is updated on
// every Append() call. On Close(), the generator is finalized:
WritableFileWriter::Close() {
  if (checksum_generator_ != nullptr && !checksum_finalized_) {
    checksum_generator_->Finalize();
    checksum_finalized_ = true;
  }
}

// db/builder.cc
// After file close, the checksum is copied into FileMetaData:
meta->file_checksum = file_writer->GetFileChecksum();
meta->file_checksum_func_name = file_writer->GetFileChecksumFuncName();
```

**Metadata Storage**:
File checksums are stored in:
1. **MANIFEST** (VersionEdit::AddFile record, in `FileMetaData`)
2. Note: Full-file checksums are NOT stored inside the SST file itself (not in `TableProperties`)

**Verification**:
```cpp
// Verify during file ingestion
IngestExternalFileOptions ingest_opts;
ingest_opts.verify_file_checksum = true;  // Default: true

// Verify existing files (SST and blob files)
ReadOptions read_opts;
Status s = db->VerifyFileChecksums(read_opts);
// Iterates all live SST and blob files, recomputes checksums, compares with MANIFEST metadata
// Returns InvalidArgument if file_checksum_gen_factory is not configured
```

**⚠️ INVARIANT**: File checksums stored in MANIFEST are treated as ground truth. However, during external SST ingestion with `verify_file_checksum=false`, caller-supplied checksum metadata may be accepted without recomputation (only the checksum function name is validated).

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
1. **After Flush**: Reopens the newly created SST file, reads all keys, and compares a rolling key/value hash against what was computed during the write (`OutputValidator`)
2. **After Compaction**: Maps to `VerifyOutputFlags::kVerifyIteration` for local/remote compaction
3. **Purpose**: Detects data loss or corruption by verifying every key/value survives the write path

**Performance Impact**: Doubles I/O for flushes/compactions (write + verify read).

**⚠️ INVARIANT**: `paranoid_file_checks = true` is incompatible with compaction resumption (`CompactionServiceOptions::allow_resumption = true`).

### verify_sst_unique_id_in_manifest (DBOptions)

```cpp
DBOptions options;
options.verify_sst_unique_id_in_manifest = true;  // Default: true
```

**Effects**:
- Each time an SST file is opened, RocksDB verifies that the file's unique ID (recomputed from table properties) matches the `unique_id` stored in MANIFEST
- Detects if SST files were swapped, copied from another DB, or corrupted
- Note: an early version opened all SST files at `DB::Open()` time; this is no longer guaranteed (depends on `max_open_files`)

**Unique ID Generation**:
```cpp
// table/unique_id.cc
// Unique ID is derived from three table properties:
// - db_id (hashed)
// - db_session_id (decoded from base-36)
// - orig_file_number (XORed in)
// Verification recomputes the ID from these properties each time an SST is opened:
//   GetSstInternalUniqueId(props->db_id, props->db_session_id,
//                          props->orig_file_number, &actual_unique_id);
```

**⚠️ INVARIANT**: SST files copied between databases will have mismatched unique IDs, causing DB::Open() to fail with `Status::Corruption()`.

### paranoid_memory_checks (ColumnFamilyOptions)

```cpp
ColumnFamilyOptions cf_options;
cf_options.paranoid_memory_checks = false;  // Default: false
```

**Effects**:
- Enables key-ordering validation during MemTable reads/scans on skiplist-based memtables
- Uses `GetAndValidate()` / `NextAndValidate()` / `PrevAndValidate()` on the skiplist iterator

**Related Options**:
```cpp
// Per-key checksum verification during seek (separate from paranoid_memory_checks)
cf_options.memtable_veirfy_per_key_checksum_on_seek = false;  // Default: false
// Requires memtable_protection_bytes_per_key > 0

cf_options.memtable_protection_bytes_per_key = 8;  // 0 = disabled (default)
// Adds per-key checksums in MemTable (8 bytes overhead per key)
```

**Performance**: ~5-10% overhead for MemTable operations when enabled.

---

## 8. Compaction Output Verification

Compaction jobs can optionally verify the integrity of output SST files.

### paranoid_file_checks (Compaction Context)

Already covered in Section 7. When enabled, compaction outputs are read and checksummed before being added to the version.

### Compaction/Flush Output Verification

RocksDB provides several knobs for verifying output file integrity:

```cpp
// include/rocksdb/options.h
DBOptions options;
options.flush_verify_memtable_count = true;     // Default: true
options.compaction_verify_record_count = true;   // Default: true

// include/rocksdb/advanced_options.h
// Bitmask controlling post-compaction verification:
enum class VerifyOutputFlags : uint32_t {
  kVerifyNone = 0,
  kVerifyBlockChecksum = 1 << 0,   // Verify block checksums
  kVerifyIteration = 1 << 1,       // Reopen and iterate all keys (rolling hash)
  kVerifyFileChecksum = 1 << 2,    // Verify full-file checksum
  kEnableForLocalCompaction = 1 << 3,
  kEnableForRemoteCompaction = 1 << 4,
};
ColumnFamilyOptions cf_opts;
cf_opts.verify_output_flags = VerifyOutputFlags::kVerifyNone;  // Default
```

**Implementation**:
```cpp
// db/compaction/compaction_job.cc
// CompactionJob::VerifyOutputFiles() performs post-compaction verification
// based on verify_output_flags. When kVerifyIteration is set, it reopens
// the output file, iterates all keys, and compares a rolling key/value
// hash against what was computed during the write (OutputValidator).
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
db_opts.checksum_handoff_file_types = {kWalFile, kTableFile};

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
→ Fatal by default: Cannot open DB
→ With `best_efforts_recovery = true`: may recover from older MANIFEST files
→ Otherwise restore from backup or attempt `RepairDB()`

### Reproducing Corruption

**Inject Block Corruption** (testing):
```cpp
// utilities/fault_injection_fs.h
FaultInjectionTestFS fault_fs(base_fs);
fault_fs.IngestDataCorruptionBeforeWrite();
// Other fault APIs: SetFilesystemActive(), DropUnsyncedFileData()
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
| `util/file_checksum_helper.cc` | File-level checksum generators |
| `include/rocksdb/file_checksum.h` | FileChecksumGen* interfaces |
| `include/rocksdb/options.h` | paranoid_checks, verify_sst_unique_id_in_manifest |
| `include/rocksdb/advanced_options.h` | paranoid_file_checks, paranoid_memory_checks |
| `include/rocksdb/table.h` | ChecksumType enum, BlockBasedTableOptions::checksum |

---

## Revision History

- **2026-03-21**: Initial version covering all checksum mechanisms

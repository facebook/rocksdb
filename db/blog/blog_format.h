//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <cstdint>
#include <string>
#include <utility>
#include <vector>

#include "rocksdb/rocksdb_namespace.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "rocksdb/table.h"

namespace ROCKSDB_NAMESPACE {

// Record types used in blog file body and footer.
// Types 0x01-0x7F are for body records.
// Types 0xF0-0xFF are for footer records (in expected order of occurrence,
// with gaps for future additions).
enum BlogRecordType : uint8_t {
  kBlogBlobRecord = 0x01,
  kBlogPreambleStartRecord = 0x02,
  kBlogWriteBatchRecord = 0x03,
  // 0x04-0x7F: reserved for future body record types

  kBlogFooterIndexRecord = 0xF0,  // sparse index or similar large metadata
  // 0xF1-0xF3: reserved for future footer data records
  kBlogFooterPropertiesRecord = 0xF8,  // small metadata (named properties)
  // 0xF9-0xFB: reserved
  kBlogFooterFileChecksumInfo = 0xFC,  // full file checksum state dump, so
                                       // the file checksum can be inferred
                                       // from just reading the tail
  // 0xFD-0xFE: reserved
  kBlogFooterLocatorRecord = 0xFF,  // relative offsets to other footer
                                    // records; must be last
};

// --- Constants ---

// 12-byte file magic: "Blog" (ASCII) followed by XXH3_64bits("Blog", 4).
// Used as an opaque identifier for blog format files.
static constexpr char kBlogFileMagic[12] = {
    '\x42', '\x6C', '\x6F', '\x67',                                 // "Blog"
    '\x9F', '\xFC', '\x41', '\x52', '\xF7', '\xF2', '\x2C', '\x49'  // XXH3_64
};
static constexpr size_t kBlogFileMagicSize = 12;

// Fixed header size in bytes (before property section).
static constexpr size_t kBlogFileFixedHeaderSize = 40;

// Escape sequence: 6 random bytes + 4 derived bytes.
static constexpr size_t kBlogEscapeSequenceSize = 10;
static constexpr size_t kBlogEscapeSeqRandomPartSize = 6;

// Seed for deriving bytes [6-9] of escape sequence via
// XXH3_64bits_withSeed(random_6_bytes, 6, kBlogEscapeSeqSeed).
// Chosen arbitrarily; must never change once the format is released.
static constexpr uint64_t kBlogEscapeSeqSeed =
    0x52636B73426C6F67ULL;  // "RcksBlg\0"

// Sentinel for unspecified-size records. Encodes to a 9-byte varint,
// which always triggers the full record format.
static constexpr uint64_t kBlogUnspecifiedSize = (uint64_t{1} << 63) - 1;

// Maximum payload size that encodes to a compact-format varint (<= 2 bytes).
// varint of 16383 (0x3FFF) uses 2 bytes; 16384 uses 3 bytes.
// Records larger than this get full format with prefix checksum protection.
static constexpr uint64_t kBlogMaxCompactPayloadSize = (1u << 14) - 1;

// Compact format varint threshold: varints of this many bytes or fewer
// use compact format (no type byte, no prefix checksum).
static constexpr size_t kBlogCompactVarintMaxBytes = 2;

// Block trailer size: 1 byte compression_type + 4 bytes checksum.
static constexpr size_t kBlogBlockTrailerSize = 5;

// kStreamingCompressionSentinel (0x7F) is defined in compression_type.h
// and used in blog record trailers to indicate streaming compression.
// A record with kNoCompression in the trailer remains individually
// uncompressed even when streaming is active (dynamic on/off).

// --- Named Properties ---

// Named property: string key -> string value.
// Case convention: Uppercase first letter = required (reader must recognize
// or reject the file). Lowercase first letter = ignorable.
using BlogPropertyMap = std::vector<std::pair<std::string, std::string>>;

// Encode properties as a sequence of length-prefixed name/value pairs.
// No count prefix; section is terminated by reaching the end of the slice.
void EncodeBlogProperties(const BlogPropertyMap& props, std::string* dst);

// Decode properties from a slice, consuming all bytes.
Status DecodeBlogProperties(Slice* input, BlogPropertyMap* props);

// Returns true if a property name starts with an uppercase letter,
// meaning the reader must recognize it or reject the file.
inline bool IsBlogRequiredProperty(const Slice& name) {
  return name.size() > 0 && name[0] >= 'A' && name[0] <= 'Z';
}

// Known header property names
static constexpr const char* kBlogPropCompressionCompatibilityName =
    "CompressionCompatibilityName";
static constexpr const char* kBlogPropTimestampSize = "TimestampSize";
static constexpr const char* kBlogPropPredecessorWalInfo = "PredecessorWalInfo";
// Required (uppercase) when streaming compression is active for WriteBatch
// records. Value is a 2-digit hex encoding of the CompressionType used
// (e.g. "07" for kZSTD). The presence of this property means WriteBatch
// record payloads with kStreamingCompressionSentinel in their trailer
// are part of a streaming compression context that spans across records.
static constexpr const char* kBlogPropWriteBatchStreamingCompressionType =
    "WriteBatchStreamingCompressionType";
static constexpr const char* kBlogPropRole = "role";
static constexpr const char* kBlogPropCompressionSettings =
    "compressionSettings";
static constexpr const char* kBlogPropCreationTime = "creationTime";
static constexpr const char* kBlogPropCreator = "creator";
// Ignorable properties for file identity and diagnostics.
static constexpr const char* kBlogPropDbId = "dbId";
static constexpr const char* kBlogPropDbSessionId = "dbSessionId";
static constexpr const char* kBlogPropFileNumber = "fileNumber";
static constexpr const char* kBlogPropDbHostId = "dbHostId";
static constexpr const char* kBlogPropColumnFamilyId = "columnFamilyId";
static constexpr const char* kBlogPropColumnFamilyName = "columnFamilyName";

// --- BlogFileHeader ---

// Bit flags for BlogFileHeader::flags (byte [15] in the fixed header).
enum BlogFileHeaderFlags : uint8_t {
  kBlogFileRecycled = 0x01,  // File may contain trailing stale data from a
                             // previous incarnation. Recovery should tolerate
                             // trailing data that doesn't match our escape
                             // sequence (recycled/stale, not corruption).
};

static constexpr uint8_t kBlogCurrentSchemaVersion = 0;

struct BlogFileHeader {
  uint8_t schema_version = kBlogCurrentSchemaVersion;
  ChecksumType checksum_type = kXXH3;
  BlogRecordType compact_record_type = kBlogBlobRecord;
  uint8_t flags = 0;  // BlogFileHeaderFlags, byte [15] in fixed header
  // 10-byte escape sequence: bytes [0-5] random (byte[0] not 0x00/0xFF),
  // bytes [6-9] = lower32(XXH3_64bits_withSeed(bytes[0-5],
  // kBlogEscapeSeqSeed)).
  char escape_sequence[kBlogEscapeSequenceSize] = {};

  bool is_recycled() const { return flags & kBlogFileRecycled; }

  BlogPropertyMap properties;

  // Incarnation ID = first 4 bytes of escape_sequence (little-endian via
  // DecodeFixed32). Guaranteed nonzero since byte[0] is not 0x00.
  uint32_t incarnation_id() const;

  // Encode the full header (fixed prefix + property section + checksums)
  // into dst.
  void EncodeTo(std::string* dst) const;

  // Decode from input. Consumes the bytes read.
  Status DecodeFrom(Slice* input);

  // Generate escape_sequence from the file's unique ID (db_id, db_session_id,
  // file_number), using the same scheme as SST internal unique IDs. Falls back
  // to GenerateRandomFields() if any input is empty/zero. Also sets the
  // ignorable dbId/dbSessionId/fileNumber header properties.
  void GenerateFromUniqueId(const std::string& db_id,
                            const std::string& db_session_id,
                            uint64_t file_number);

  // Generate escape_sequence from random bytes. Appropriate when unique ID
  // inputs are not available.
  void GenerateRandomFields();

 private:
  // Populate escape_sequence from a 64 bits of entropy.
  void SetEscapeSequenceFromU64(uint64_t v);

 public:
  // Verify that the escape sequence's derived bytes match.
  static bool VerifyEscapeSequence(const char* seq);

  // Check if a buffer starts with the blog file magic number.
  static bool IsBlogFormat(const char* data, size_t size) {
    return size >= kBlogFileMagicSize &&
           memcmp(data, kBlogFileMagic, kBlogFileMagicSize) == 0;
  }

  // Decode a blog file header from a buffer that may or may not contain
  // the full header. Returns OK and consumes the header bytes on success.
  // If the buffer contains the fixed header but not the full property
  // section, returns the number of additional bytes needed in
  // *additional_bytes_needed and returns Status::Incomplete().
  // This enables single-read header parsing from a ~4 KiB buffer.
  Status DecodeFromBuffer(Slice* buffer, size_t* additional_bytes_needed);

  // Raw property helpers (string values)
  void SetProperty(const std::string& name, const std::string& value);
  std::string GetProperty(const std::string& name) const;

  // Typed property setters/getters for properties that aren't natively strings.
  void SetUint64Property(const std::string& name, uint64_t value);
  void SetUint32Property(const std::string& name, uint32_t value);
  bool GetUint64Property(const std::string& name, uint64_t* value) const;
  bool GetUint32Property(const std::string& name, uint32_t* value) const;
  bool HasProperty(const std::string& name) const;
};

// --- Footer types ---

// Entry in a footer locator record. Offsets are in units of 4 bytes,
// sequential relative to each other in reverse order of footer records.
// First entry: offset from locator's escape_seq to the preceding footer
// record's escape_seq. Subsequent entries: offset from the previous
// entry's escape_seq to the next earlier footer record's escape_seq.
// Verify that a blog file's trailing data contains a valid footer locator
// record. Scans backward from the end of the buffer for the escape sequence,
// then parses and verifies the full record (varint length, type, prefix
// checksum, payload, trailer checksum). The buffer must start at a 4-byte-
// aligned file offset. Returns true if a valid footer locator record is found.
bool VerifyBlogFooterLocator(const char* buffer, size_t buffer_size,
                             uint64_t buffer_file_offset,
                             const char* expected_escape_seq,
                             ChecksumType checksum_type,
                             uint32_t incarnation_id);

struct BlogFooterLocatorEntry {
  BlogRecordType record_type;
  uint32_t relative_offset_4B;
};

struct BlogFileFooterLocator {
  std::vector<BlogFooterLocatorEntry> entries;

  // Encode as the payload of a footer locator record.
  void EncodeTo(std::string* dst) const;
  Status DecodeFrom(const Slice& input);
};

struct BlogFileFooterProperties {
  BlogPropertyMap properties;

  // Convenience setters that encode values into the property map.
  void SetBlobCount(uint64_t count);
  void SetTotalBlobBytes(uint64_t bytes);
  void SetSequenceRange(uint64_t min_seq, uint64_t max_seq);

  // Encode as named properties (no 5-byte trailer; that comes from
  // the enclosing full record format).
  void EncodeTo(std::string* dst) const;
  Status DecodeFrom(const Slice& input);
};

// --- Padding helpers ---

// Compute padding needed to reach the next 4-byte-aligned offset,
// choosing the pad byte value to differ from last_meaningful_byte.
// Appends padding bytes to dst.
void ComputeBlogPadding(uint8_t last_meaningful_byte, size_t current_offset,
                        std::string* dst);

// Same as above, but returns the pad byte value and count without
// allocating. For use on hot paths.
void ComputeBlogPaddingParams(uint8_t last_meaningful_byte,
                              size_t current_offset, uint8_t* pad_byte,
                              size_t* pad_count);

// --- Checksum helpers ---

// Compute a context-aware checksum over data + compression_type byte,
// suitable for a blog record trailer. Uses the same algorithm and context
// checksum approach as block-based SST files.
uint32_t ComputeBlogRecordChecksum(ChecksumType type, const char* data,
                                   size_t data_size, char compression_type_byte,
                                   uint32_t base_context_checksum,
                                   uint64_t record_offset);

// Verify a blog record's 5-byte trailer (compression_type + checksum).
// payload points to the record payload of payload_size bytes; the trailer
// immediately follows. Returns OK if checksum matches, Corruption otherwise.
// On success, *actual_compression_type is set from the trailer.
Status VerifyBlogRecordTrailer(ChecksumType checksum_type, const char* payload,
                               size_t payload_size, uint32_t incarnation_id,
                               uint64_t payload_file_offset,
                               CompressionType* actual_compression_type);

// Encode an irregular varint: a value encoded with more bytes than the
// minimum required, ensuring varint length > 3 bytes to trigger full
// record format. The value is encoded correctly and will decode to the
// same value via standard GetVarint64.
void PutBlogIrregularVarint64(std::string* dst, uint64_t value,
                              size_t min_encoded_bytes);

}  // namespace ROCKSDB_NAMESPACE

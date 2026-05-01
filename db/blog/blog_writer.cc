//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/blog/blog_writer.h"

#include <cassert>

#include "table/format.h"
#include "util/coding.h"
#include "util/string_util.h"

namespace ROCKSDB_NAMESPACE {

BlogFileWriter::BlogFileWriter(std::unique_ptr<WritableFileWriter>&& dest,
                               const BlogFileHeader& header, bool manual_flush)
    : dest_(std::move(dest)), header_(header), manual_flush_(manual_flush) {}

BlogFileWriter::~BlogFileWriter() = default;

IOStatus BlogFileWriter::WriteHeader(const WriteOptions& wo) {
  assert(!header_written_);
  std::string buf;
  header_.EncodeTo(&buf);
  // Pad after header properties using the same padding scheme as records.
  uint8_t last_byte = static_cast<uint8_t>(buf.back());
  ComputeBlogPadding(last_byte, buf.size(), &buf);
  IOStatus s = EmitBytes(wo, buf);
  if (s.ok()) {
    header_written_ = true;
  }
  if (s.ok() && !manual_flush_) {
    IOOptions opts;
    s = WritableFileWriter::PrepareIOOptions(wo, opts);
    if (s.ok()) {
      s = dest_->Flush(opts);
    }
  }
  return s;
}

IOStatus BlogFileWriter::AddBlobRecord(const WriteOptions& wo,
                                       const Slice& payload,
                                       CompressionType comp_type,
                                       uint64_t* blob_offset) {
  return AddRecord(wo, kBlogBlobRecord, payload, comp_type, blob_offset);
}

IOStatus BlogFileWriter::AddWriteBatchRecord(const WriteOptions& wo,
                                             const Slice& wb_data,
                                             CompressionType comp_type) {
  // Stub: WriteBatch record writing is not yet integrated (WAL integration
  // deferred). The record format is the same as blob records with type 0x03.
  return AddRecord(wo, kBlogWriteBatchRecord, wb_data, comp_type, nullptr);
}

IOStatus BlogFileWriter::AddPreambleStartRecord(const WriteOptions& wo) {
  return AddRecord(wo, kBlogPreambleStartRecord, Slice(), kNoCompression,
                   nullptr, /*force_full=*/true);
}

IOStatus BlogFileWriter::AddFooterIndexRecord(const WriteOptions& wo,
                                              const Slice& index_data) {
  return AddRecord(wo, kBlogFooterIndexRecord, index_data, kNoCompression,
                   nullptr, /*force_full=*/true);
}

IOStatus BlogFileWriter::AddFooterPropertiesRecord(
    const WriteOptions& wo, const BlogFileFooterProperties& props) {
  std::string payload;
  props.EncodeTo(&payload);
  return AddRecord(wo, kBlogFooterPropertiesRecord, payload, kNoCompression,
                   nullptr, /*force_full=*/true);
}

IOStatus BlogFileWriter::AddFooterLocatorRecord(
    const WriteOptions& wo, const BlogFileFooterLocator& locator) {
  std::string payload;
  locator.EncodeTo(&payload);
  return AddRecord(wo, kBlogFooterLocatorRecord, payload, kNoCompression,
                   nullptr, /*force_full=*/true);
}

IOStatus BlogFileWriter::WriteBuffer(const WriteOptions& wo) {
  IOOptions opts;
  IOStatus s = WritableFileWriter::PrepareIOOptions(wo, opts);
  if (s.ok()) {
    s = dest_->Flush(opts);
  }
  return s;
}

IOStatus BlogFileWriter::Sync(const WriteOptions& wo, bool use_fsync) {
  IOOptions opts;
  IOStatus s = WritableFileWriter::PrepareIOOptions(wo, opts);
  if (s.ok()) {
    s = dest_->Sync(opts, use_fsync);
  }
  return s;
}

IOStatus BlogFileWriter::Close(const WriteOptions& wo) {
  IOOptions opts;
  IOStatus s = WritableFileWriter::PrepareIOOptions(wo, opts);
  if (s.ok()) {
    s = dest_->Close(opts);
  }
  return s;
}

IOStatus BlogFileWriter::AddRecord(const WriteOptions& wo, BlogRecordType type,
                                   const Slice& payload,
                                   CompressionType comp_type,
                                   uint64_t* payload_offset, bool force_full) {
  assert(header_written_);

  // Determine varint length of payload size
  uint16_t varint_len = VarintLength(payload.size());

  // Use compact format when:
  // 1. varint fits in <= kBlogCompactVarintMaxBytes (payload < ~16 KiB)
  // 2. record type matches the header's compact_record_type
  // 3. not forced to full format (e.g. footer/preamble records)
  bool use_compact =
      !force_full && (varint_len <= kBlogCompactVarintMaxBytes) &&
      (type == header_.compact_record_type) && (payload.size() > 0);

  IOStatus s;
  if (use_compact) {
    s = EmitCompactRecord(wo, payload, comp_type, payload_offset);
  } else {
    s = EmitFullRecord(wo, type, payload, comp_type, payload_offset);
  }

  if (s.ok() && !manual_flush_) {
    IOOptions opts;
    s = WritableFileWriter::PrepareIOOptions(wo, opts);
    if (s.ok()) {
      s = dest_->Flush(opts);
    }
  }
  return s;
}

IOStatus BlogFileWriter::EmitCompactRecord(const WriteOptions& wo,
                                           const Slice& payload,
                                           CompressionType comp_type,
                                           uint64_t* payload_offset) {
  // Compact format:
  //   [escape_seq: 10B] [varint length: 1-3B]
  //   [payload: length bytes]
  //   [compression_type: 1B] [checksum: 4B]
  //   [padding: 0+B]
  assert(payload.size() > 0);  // length=0 compact records not expected

  std::string prefix;
  // Escape sequence
  prefix.append(header_.escape_sequence, kBlogEscapeSequenceSize);
  // Varint length
  PutVarint64(&prefix, payload.size());

  IOStatus s = EmitBytes(wo, prefix);
  if (!s.ok()) {
    return s;
  }

  // Record payload offset for caller
  if (payload_offset) {
    *payload_offset = offset_;
  }

  // Payload
  s = EmitBytes(wo, payload);
  if (!s.ok()) {
    return s;
  }

  // 5-byte trailer: compression_type + checksum
  char trailer[kBlogBlockTrailerSize];
  trailer[0] = static_cast<char>(comp_type);
  uint32_t checksum = ComputeBlogRecordChecksum(
      header_.checksum_type, payload.data(), payload.size(), trailer[0],
      header_.incarnation_id(),
      offset_ - payload.size());  // record offset = start of payload
  EncodeFixed32(trailer + 1, checksum);
  s = EmitBytes(wo, trailer, kBlogBlockTrailerSize);
  if (!s.ok()) {
    return s;
  }

  // Padding (stack-allocated, no heap allocation)
  uint8_t last_byte = static_cast<uint8_t>(trailer[kBlogBlockTrailerSize - 1]);
  uint8_t pad_byte;
  size_t pad_count;
  ComputeBlogPaddingParams(last_byte, offset_, &pad_byte, &pad_count);
  if (pad_count > 0) {
    char pad_buf[4];
    memset(pad_buf, pad_byte, pad_count);
    s = EmitBytes(wo, pad_buf, pad_count);
  }
  return s;
}

IOStatus BlogFileWriter::EmitFullRecord(const WriteOptions& wo,
                                        BlogRecordType type,
                                        const Slice& payload,
                                        CompressionType comp_type,
                                        uint64_t* payload_offset) {
  // Full format:
  //   [escape_seq: 10B] [varint length: 4+B] [type: 1B] [compression_type: 1B]
  //   [prefix_checksum: 4B]
  //   (if length > 0):
  //     [payload: length bytes]
  //     [compression_type: 1B] [checksum: 4B]
  //   [padding: 0+B]

  std::string prefix;
  // Escape sequence
  prefix.append(header_.escape_sequence, kBlogEscapeSequenceSize);

  // Varint length (must be > kBlogCompactVarintMaxBytes to trigger full format)
  constexpr size_t kFullVarintMinBytes = kBlogCompactVarintMaxBytes + 1;
  size_t varint_start = prefix.size();
  if (payload.size() == 0) {
    PutBlogIrregularVarint64(&prefix, 0, kFullVarintMinBytes);
  } else {
    uint16_t natural_len = VarintLength(payload.size());
    if (natural_len >= kFullVarintMinBytes) {
      PutVarint64(&prefix, payload.size());
    } else {
      PutBlogIrregularVarint64(&prefix, payload.size(), kFullVarintMinBytes);
    }
  }

  // Type byte
  prefix.push_back(static_cast<char>(type));
  // Pre-payload compression type
  prefix.push_back(static_cast<char>(comp_type));

  // Prefix checksum covers (varint length + type + compression_type),
  // with context modifier for defense-in-depth.
  const char* prefix_data = prefix.data() + varint_start;
  size_t prefix_data_size = prefix.size() - varint_start;
  uint32_t prefix_checksum =
      ComputeBuiltinChecksum(header_.checksum_type, prefix_data,
                             prefix_data_size) +
      ChecksumModifierForContext(header_.incarnation_id(), offset_);
  PutFixed32(&prefix, prefix_checksum);

  IOStatus s = EmitBytes(wo, prefix);
  if (!s.ok()) {
    return s;
  }

  // Track the last meaningful byte for padding computation.
  // EncodeFixed32 is little-endian: last byte written = (value >> 24) & 0xFF.
  uint8_t last_byte;

  if (payload.size() > 0) {
    // Record payload offset for caller
    if (payload_offset) {
      *payload_offset = offset_;
    }

    // Payload
    s = EmitBytes(wo, payload);
    if (!s.ok()) {
      return s;
    }

    // 5-byte trailer: compression_type + checksum
    char trailer[kBlogBlockTrailerSize];
    trailer[0] = static_cast<char>(comp_type);
    uint32_t checksum = ComputeBlogRecordChecksum(
        header_.checksum_type, payload.data(), payload.size(), trailer[0],
        header_.incarnation_id(), offset_ - payload.size());
    EncodeFixed32(trailer + 1, checksum);
    s = EmitBytes(wo, trailer, kBlogBlockTrailerSize);
    if (!s.ok()) {
      return s;
    }

    last_byte = static_cast<uint8_t>(trailer[kBlogBlockTrailerSize - 1]);
  } else {
    // No payload, no trailer. Last meaningful byte is the last byte of
    // prefix_checksum (little-endian).
    last_byte = static_cast<uint8_t>(prefix_checksum >> 24);
  }

  uint8_t pad_byte;
  size_t pad_count;
  ComputeBlogPaddingParams(last_byte, offset_, &pad_byte, &pad_count);
  if (pad_count > 0) {
    char pad_buf[4];
    memset(pad_buf, pad_byte, pad_count);
    s = EmitBytes(wo, pad_buf, pad_count);
  }
  return s;
}

IOStatus BlogFileWriter::EmitBytes(const WriteOptions& wo, const Slice& data) {
  return EmitBytes(wo, data.data(), data.size());
}

IOStatus BlogFileWriter::EmitBytes(const WriteOptions& wo, const char* data,
                                   size_t len) {
  IOOptions opts;
  IOStatus s = WritableFileWriter::PrepareIOOptions(wo, opts);
  if (!s.ok()) {
    return s;
  }
  s = dest_->Append(opts, Slice(data, len));
  if (s.ok()) {
    offset_ += len;
  }
  return s;
}

}  // namespace ROCKSDB_NAMESPACE

//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/blog/blog_writer.h"

#include <cassert>

#include "table/format.h"
#include "util/coding.h"
#include "util/prefix_varint.h"
#include "util/string_util.h"

namespace ROCKSDB_NAMESPACE {

BlogFileWriter::BlogFileWriter(std::unique_ptr<WritableFileWriter>&& dest,
                               const BlogFileHeader& header, bool manual_flush)
    : dest_(std::move(dest)), header_(header), manual_flush_(manual_flush) {}

BlogFileWriter::~BlogFileWriter() = default;

IOStatus BlogFileWriter::WriteHeader(const WriteOptions& wo) {
  assert(!header_written_);

  // Split properties: required (uppercase) go in the header property section,
  // ignorable (lowercase) go in a kBlogIgnorablePropertiesRecord after.
  BlogPropertyMap ignorable_props;
  BlogFileHeader header_copy = header_;
  header_copy.properties.clear();
  for (auto& [name, value] : header_.properties) {
    if (IsBlogRequiredProperty(name)) {
      header_copy.properties.emplace_back(std::move(name), std::move(value));
    } else {
      ignorable_props.emplace_back(std::move(name), std::move(value));
    }
  }

  std::string buf;
  header_copy.EncodeTo(&buf);
  BlogPadding header_pad(static_cast<uint8_t>(buf.back()), buf.size());
  header_pad.AppendTo(&buf);
  IOStatus s = EmitBytes(wo, buf);
  if (s.ok()) {
    header_written_ = true;
  }

  // Emit ignorable properties as a separate record.
  if (s.ok() && !ignorable_props.empty()) {
    std::string payload;
    EncodeBlogProperties(ignorable_props, &payload);
    s = AddRecord(wo, kBlogIgnorablePropertiesRecord, payload, kNoCompression,
                  nullptr, /*force_full=*/true);
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
                                       uint64_t* blob_offset,
                                       uint64_t uncompressed_size) {
  uint64_t offset_before = offset_;
  IOStatus s = AddRecord(wo, kBlogBlobRecord, payload, comp_type, blob_offset);
  if (s.ok()) {
    blob_stats_.count++;
    blob_stats_.payload_bytes += payload.size();
    blob_stats_.overhead_bytes += (offset_ - offset_before) - payload.size();
    if (comp_type != kNoCompression) {
      blob_stats_.compressed_bytes += payload.size();
      blob_stats_.uncompressed_bytes += uncompressed_size;
    }
  }
  return s;
}

IOStatus BlogFileWriter::AddWriteBatchRecord(const WriteOptions& wo,
                                             const Slice& wb_data,
                                             CompressionType comp_type) {
  // Stub: WriteBatch record writing is not yet integrated (WAL integration
  // deferred). The record format is the same as blob records with type 0x03.
  return AddRecord(wo, kBlogWriteBatchRecord, wb_data, comp_type, nullptr);
}

IOStatus BlogFileWriter::AddIgnorablePropertiesRecord(
    const WriteOptions& wo, const BlogPropertyMap& props) {
  for (const auto& [name, value] : props) {
    if (IsBlogRequiredProperty(name)) {
      return IOStatus::InvalidArgument(
          "Required properties not allowed in ignorable properties record: " +
          name);
    }
  }
  std::string payload;
  EncodeBlogProperties(props, &payload);
  return AddRecord(wo, kBlogIgnorablePropertiesRecord, payload, kNoCompression,
                   nullptr, /*force_full=*/true);
}

IOStatus BlogFileWriter::AddPreambleStartRecord(const WriteOptions& wo) {
  return AddRecord(wo, kBlogPreambleStartRecord, Slice(), kNoCompression,
                   nullptr);
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

  if (comp_type != kNoCompression &&
      comp_type != kStreamingCompressionSentinel) {
    compression_type_set_.Add(comp_type);
  }

  IOStatus s;
  if (payload.size() == 0) {
    // Trivial format for length=0 records (preamble-start, etc.)
    s = EmitTrivialRecord(wo, type);
  } else {
    // Use compact format when:
    // 1. prefix varint fits in <= kBlogCompactVarintMaxBytes (payload < ~16
    // KiB)
    // 2. record type matches the header's compact_record_type
    // 3. not forced to full format (e.g. footer records)
    uint32_t varint_len = PrefixVarint64Length(payload.size());
    bool use_compact = !force_full &&
                       (varint_len <= kBlogCompactVarintMaxBytes) &&
                       (type == header_.compact_record_type);

    if (use_compact) {
      s = EmitCompactRecord(wo, payload, comp_type, payload_offset);
    } else {
      s = EmitFullRecord(wo, type, payload, comp_type, payload_offset);
    }
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

IOStatus BlogFileWriter::EmitTrivialRecord(const WriteOptions& wo,
                                           BlogRecordType type) {
  // Trivial format (length=0):
  //   [escape_seq: 10B] [prefix_varint(0): 1B] [type: 1B] [checksum: 4B]
  //   [padding: 0+B]
  std::string buf;
  buf.append(header_.escape_sequence, kBlogEscapeSequenceSize);
  PutPrefixVarint64(&buf, 0);  // prefix varint(0) = 0x01
  buf.push_back(static_cast<char>(type));

  // Checksum covers (varint + type), with context modifier.
  const char* cksum_data = buf.data() + kBlogEscapeSequenceSize;
  size_t cksum_data_size = buf.size() - kBlogEscapeSequenceSize;
  uint32_t checksum =
      ComputeBuiltinChecksum(header_.checksum_type, cksum_data,
                             cksum_data_size) +
      ChecksumModifierForContext(header_.incarnation_id(), offset_);
  PutFixed32(&buf, checksum);

  IOStatus s = EmitBytes(wo, buf);
  if (!s.ok()) {
    return s;
  }

  // Padding (not needed for last record in file, but trivial records
  // are never the footer locator, so always pad)
  uint8_t last_byte = static_cast<uint8_t>(checksum >> 24);
  BlogPadding pad(last_byte, offset_);
  if (pad.count > 0) {
    s = EmitBytes(wo, pad.bytes, pad.count);
  }
  return s;
}

IOStatus BlogFileWriter::EmitCompactRecord(const WriteOptions& wo,
                                           const Slice& payload,
                                           CompressionType comp_type,
                                           uint64_t* payload_offset) {
  // Compact format:
  //   [escape_seq: 10B] [prefix_varint length: 1-2B]
  //   [payload: length bytes]
  //   [compression_type: 1B] [checksum: 4B]
  //   [padding: 0+B]
  assert(payload.size() > 0);

  std::string prefix;
  prefix.append(header_.escape_sequence, kBlogEscapeSequenceSize);
  PutPrefixVarint64(&prefix, payload.size());

  IOStatus s = EmitBytes(wo, prefix);
  if (!s.ok()) {
    return s;
  }

  return EmitPayloadTrailerPadding(wo, payload, comp_type, payload_offset,
                                   /*skip_padding=*/false);
}

IOStatus BlogFileWriter::EmitFullRecord(const WriteOptions& wo,
                                        BlogRecordType type,
                                        const Slice& payload,
                                        CompressionType comp_type,
                                        uint64_t* payload_offset) {
  // Full format:
  //   [escape_seq: 10B] [prefix_varint length: 3+B] [type: 1B]
  //   [compression_type: 1B] [prefix_checksum: 4B]
  //   [payload: length bytes]
  //   [compression_type: 1B] [checksum: 4B]
  //   [padding: 0+B]
  assert(payload.size() > 0);

  std::string prefix;
  prefix.append(header_.escape_sequence, kBlogEscapeSequenceSize);

  // Prefix varint length (must be > kBlogCompactVarintMaxBytes for full format)
  constexpr uint32_t kFullVarintMinBytes = kBlogCompactVarintMaxBytes + 1;
  size_t varint_start = prefix.size();
  {
    char buf[kMaxPrefixVarint64Length];
    char* end = EncodePrefixVarint64<kFullVarintMinBytes>(buf, payload.size());
    prefix.append(buf, static_cast<size_t>(end - buf));
  }

  prefix.push_back(static_cast<char>(type));
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

  bool skip_padding = (type == kBlogFooterLocatorRecord);
  return EmitPayloadTrailerPadding(wo, payload, comp_type, payload_offset,
                                   skip_padding);
}

IOStatus BlogFileWriter::EmitPayloadTrailerPadding(const WriteOptions& wo,
                                                   const Slice& payload,
                                                   CompressionType comp_type,
                                                   uint64_t* payload_offset,
                                                   bool skip_padding) {
  if (payload_offset) {
    *payload_offset = offset_;
  }

  IOStatus s = EmitBytes(wo, payload);
  if (!s.ok()) {
    return s;
  }

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

  if (!skip_padding) {
    BlogPadding pad(static_cast<uint8_t>(trailer[kBlogBlockTrailerSize - 1]),
                    offset_);
    if (pad.count > 0) {
      s = EmitBytes(wo, pad.bytes, pad.count);
    }
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

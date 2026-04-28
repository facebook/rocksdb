//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <cstdint>
#include <memory>
#include <string>

#include "db/blog/blog_format.h"
#include "file/writable_file_writer.h"
#include "rocksdb/data_structure.h"
#include "rocksdb/io_status.h"
#include "rocksdb/options.h"

namespace ROCKSDB_NAMESPACE {

// Writes blog-format files (WAL or blob). The caller constructs a
// BlogFileHeader, calls WriteHeader(), then adds records via
// AddBlobRecord/AddWriteBatchRecord, and optionally appends footer records
// before closing.
//
// Format selection (compact vs full) is automatic based on payload size and
// record type. See blog_format.h for the wire format details.
class BlogFileWriter {
 public:
  BlogFileWriter(std::unique_ptr<WritableFileWriter>&& dest,
                 const BlogFileHeader& header, bool manual_flush = false);
  ~BlogFileWriter();

  // Write the file header. Must be called exactly once, before any records.
  IOStatus WriteHeader(const WriteOptions& wo);

  // Write a blob record. Selects compact or full format automatically.
  // On success, *blob_offset is the file offset of the payload start.
  // uncompressed_size: original size before compression (for footer stats).
  // Pass payload.size() when comp_type == kNoCompression.
  IOStatus AddBlobRecord(const WriteOptions& wo, const Slice& payload,
                         CompressionType comp_type, uint64_t* blob_offset,
                         uint64_t uncompressed_size);

  // Write a WriteBatch record. Selects compact or full format automatically.
  IOStatus AddWriteBatchRecord(const WriteOptions& wo, const Slice& wb_data,
                               CompressionType comp_type = kNoCompression);

  // Write a preamble-start record (stub). Uses trivial format (length=0).
  IOStatus AddPreambleStartRecord(const WriteOptions& wo);

  // Write an ignorable properties record. Can appear anywhere in the file
  // body, including when re-opening an existing file (e.g. manifest reuse)
  // to record updated diagnostic properties. Only ignorable (lowercase)
  // properties are allowed; required (uppercase) properties are rejected.
  IOStatus AddIgnorablePropertiesRecord(const WriteOptions& wo,
                                        const BlogPropertyMap& props);

  // Write a footer index record (full format).
  IOStatus AddFooterIndexRecord(const WriteOptions& wo,
                                const Slice& index_data);

  // Write a footer properties record (full format).
  IOStatus AddFooterPropertiesRecord(const WriteOptions& wo,
                                     const BlogFileFooterProperties& props);

  // Write a footer locator record (full format). Must be the last record.
  IOStatus AddFooterLocatorRecord(const WriteOptions& wo,
                                  const BlogFileFooterLocator& locator);

  IOStatus WriteBuffer(const WriteOptions& wo);
  IOStatus Sync(const WriteOptions& wo, bool use_fsync = false);
  IOStatus Close(const WriteOptions& wo);

  uint64_t current_offset() const { return offset_; }
  WritableFileWriter* file() { return dest_.get(); }
  const BlogFileHeader& header() const { return header_; }

  // Accumulated blob record stats for footer properties.
  // Accumulated blob record stats for footer properties.
  struct BlobStats {
    uint64_t count = 0;
    uint64_t payload_bytes =
        0;  // on-disk payload (compressed where applicable)
    uint64_t compressed_bytes =
        0;  // compressed size (only actually-compressed blobs)
    uint64_t uncompressed_bytes = 0;  // original size of those same blobs
    uint64_t overhead_bytes =
        0;  // framing: escape seq, varint, trailer, padding
  };
  const BlobStats& blob_stats() const { return blob_stats_; }

  using CompressionTypeSet =
      SmallEnumSet<CompressionType, kDisableCompressionOption>;

  // Set of CompressionType values used across all records in the file
  // (excluding kNoCompression and kStreamingCompressionSentinel).
  const CompressionTypeSet& compression_type_set() const {
    return compression_type_set_;
  }

 private:
  // Write a record. Chooses compact format when varint fits in <= 3 bytes
  // and type matches compact_record_type, unless force_full is true.
  IOStatus AddRecord(const WriteOptions& wo, BlogRecordType type,
                     const Slice& payload, CompressionType comp_type,
                     uint64_t* payload_offset, bool force_full = false);

  // Emit a trivial-format record (length=0, no payload).
  IOStatus EmitTrivialRecord(const WriteOptions& wo, BlogRecordType type);

  // Emit a compact-format record.
  IOStatus EmitCompactRecord(const WriteOptions& wo, const Slice& payload,
                             CompressionType comp_type,
                             uint64_t* payload_offset);

  // Emit a full-format record.
  IOStatus EmitFullRecord(const WriteOptions& wo, BlogRecordType type,
                          const Slice& payload, CompressionType comp_type,
                          uint64_t* payload_offset);

  // Emit payload, 5-byte trailer (compression_type + checksum), and padding.
  // If skip_padding is true, no padding is emitted (used for the last record).
  IOStatus EmitPayloadTrailerPadding(const WriteOptions& wo,
                                     const Slice& payload,
                                     CompressionType comp_type,
                                     uint64_t* payload_offset,
                                     bool skip_padding);

  // Append raw bytes to the file and advance offset_.
  IOStatus EmitBytes(const WriteOptions& wo, const Slice& data);
  IOStatus EmitBytes(const WriteOptions& wo, const char* data, size_t len);

  std::unique_ptr<WritableFileWriter> dest_;
  BlogFileHeader header_;
  uint64_t offset_ = 0;
  bool manual_flush_;
  bool header_written_ = false;
  BlobStats blob_stats_;
  CompressionTypeSet compression_type_set_;
};

}  // namespace ROCKSDB_NAMESPACE

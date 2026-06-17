//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <cstddef>
#include <cstdint>
#include <limits>
#include <string>

#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "util/coding.h"

namespace ROCKSDB_NAMESPACE {

struct SstFileWriterEmbeddedBlobOptions;

// Internal builder-side spelling for the public SstFileWriter options. Keeping
// this as an alias makes the table-builder wrapper's contract explicit without
// forking the option definition.
using EmbeddedBlobSstBuilderOptions = SstFileWriterEmbeddedBlobOptions;

// Metaindex entry that locates the embedded blob record range. The value is a
// raw BlockHandle, but it points to blob records rather than a block. The data
// is not parseable through this BlockHandle; it is just a locator for tracking
// and sanity checking.
inline constexpr char kEmbeddedBlobSstRecordRangeName[] =
    "rocksdb.embedded.blob.records";

// User property with best-effort diagnostic counters for the embedded blob
// records. Readers must not depend on this property for correctness.
inline constexpr char kEmbeddedBlobSstStatsPropertyName[] =
    "rocksdb.embedded.blob.stats";

// Embedded blob records are encoded as payload bytes followed by a block-like
// trailer: one compression marker byte and a four-byte checksum.
inline constexpr size_t kEmbeddedBlobSstRecordTrailerSize = 5;

// Byte range within the SST file containing embedded blob records. This is the
// reader's sanity-checking locator for same-file BlobIndex references.
struct EmbeddedBlobRecordRange {
  uint64_t offset = 0;
  uint64_t size = 0;

  // Whether the range names at least one embedded blob record.
  bool HasRecords() const { return size > 0; }

  // Returns true when a record with the given payload size and trailer size is
  // fully contained in this range.
  bool ContainsRecord(
      uint64_t record_offset, uint64_t payload_size,
      uint64_t trailer_size = kEmbeddedBlobSstRecordTrailerSize) const {
    if (record_offset < offset) {
      return false;
    }
    const uint64_t relative_offset = record_offset - offset;
    if (relative_offset > size) {
      return false;
    }
    if (payload_size > std::numeric_limits<uint64_t>::max() - trailer_size) {
      return false;
    }
    const uint64_t record_size = payload_size + trailer_size;
    return record_size <= size - relative_offset;
  }
};

// Diagnostic counters for embedded blob records. Stored as a user property and
// intentionally ignored by readers.
struct EmbeddedBlobStats {
  uint64_t blob_count = 0;
  uint64_t payload_bytes = 0;

  // Whether any embedded blob records contributed to the counters.
  bool HasRecords() const { return blob_count > 0; }
};

// Encodes EmbeddedBlobStats for kEmbeddedBlobSstStatsPropertyName.
inline void EncodeEmbeddedBlobStats(const EmbeddedBlobStats& stats,
                                    std::string* dst) {
  dst->clear();
  PutVarint64(dst, stats.blob_count);
  PutVarint64(dst, stats.payload_bytes);
}

// Decodes EmbeddedBlobStats from kEmbeddedBlobSstStatsPropertyName.
inline Status DecodeEmbeddedBlobStats(Slice input, EmbeddedBlobStats* stats) {
  if (stats == nullptr) {
    return Status::InvalidArgument("Missing embedded blob stats output");
  }

  EmbeddedBlobStats decoded;
  if (!GetVarint64(&input, &decoded.blob_count) ||
      !GetVarint64(&input, &decoded.payload_bytes)) {
    return Status::Corruption("Error decoding embedded blob stats");
  }

  if (decoded.blob_count == 0 && decoded.payload_bytes != 0) {
    return Status::Corruption("Embedded blob stats have bytes but no records");
  }

  *stats = decoded;
  return Status::OK();
}

}  // namespace ROCKSDB_NAMESPACE

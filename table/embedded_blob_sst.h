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
using EmbeddedBlobSstBuilderOptions = SstFileWriterEmbeddedBlobOptions;

inline constexpr char kEmbeddedBlobSstRecordRangeName[] =
    "rocksdb.embedded.blob.records";
inline constexpr char kEmbeddedBlobSstStatsPropertyName[] =
    "rocksdb.embedded.blob.stats";
inline constexpr size_t kEmbeddedBlobSstRecordTrailerSize = 5;

struct EmbeddedBlobRecordRange {
  uint64_t offset = 0;
  uint64_t size = 0;

  bool HasRecords() const { return size > 0; }

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

struct EmbeddedBlobStats {
  uint64_t blob_count = 0;
  uint64_t payload_bytes = 0;

  bool HasRecords() const { return blob_count > 0; }
};

inline void EncodeEmbeddedBlobStats(const EmbeddedBlobStats& stats,
                                    std::string* dst) {
  dst->clear();
  PutVarint64(dst, stats.blob_count);
  PutVarint64(dst, stats.payload_bytes);
}

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

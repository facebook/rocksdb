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
#include <string>

#include "db/blob/blob_gen2_format.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "util/coding.h"

namespace ROCKSDB_NAMESPACE {

struct SstFileWriterEmbeddedBlobOptions;

// Internal builder-side spelling for the public SstFileWriter options. Keeping
// this as an alias makes the table builder's contract explicit without forking
// the option definition.
using EmbeddedBlobSstBuilderOptions = SstFileWriterEmbeddedBlobOptions;

// User property with best-effort diagnostic counters for the embedded blob
// records. The presence of this property is also the reader's signal that the
// SST contains embedded blob records; readers must not depend on the counter
// values for correctness.
inline constexpr char kEmbeddedBlobSstStatsPropertyName[] =
    "rocksdb.embedded.blob.stats";

// Embedded blob records use the SimpleGen2Blob record format (payload bytes
// followed by a compression-marker byte and a four-byte checksum); see
// db/blob/blob_gen2_format.h.

// Diagnostic counters for embedded blob records. Stored as a user property and
// intentionally ignored (other than for presence detection) by readers.
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

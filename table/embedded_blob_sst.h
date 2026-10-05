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
#include "rocksdb/table.h"
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

// A relocation file is a block-based-table blob file produced by standalone
// Blob GC that stores the surviving values for exactly one stable origin. It
// replaces that origin's identity file as physical storage while BlobIndexes
// continue to name the origin. The MANIFEST supplies the expected origin;
// readers require an exact match with this checksummed property before using
// the relocation file.
inline constexpr char kBlobGcRelocationFileOriginPropertyName[] =
    "rocksdb.blob.gc.relocation_file.origin";

// Relocation files put embedded payloads before their data blocks. Prefix the
// file with an engine-owned discriminator so obsolete-file cleanup never
// mistakes user-controlled payload bytes for a v1 blob-log header.
inline constexpr char kBlobGcRelocationFilePrefix[] = "RDBGC001";
inline constexpr size_t kBlobGcRelocationFilePrefixSize =
    sizeof(kBlobGcRelocationFilePrefix) - 1;

inline void EncodeBlobGcRelocationFileOrigin(uint64_t origin_file_number,
                                             std::string* dst) {
  assert(dst != nullptr);
  dst->clear();
  PutFixed64(dst, origin_file_number);
}

inline Status DecodeBlobGcRelocationFileOrigin(Slice input,
                                               uint64_t* origin_file_number) {
  if (origin_file_number == nullptr || input.size() != sizeof(uint64_t) ||
      !GetFixed64(&input, origin_file_number) || !input.empty() ||
      *origin_file_number == 0) {
    return Status::Corruption(
        "Invalid Blob GC relocation file origin property");
  }
  return Status::OK();
}

// Fixed-width big-endian encoding preserves numeric offset order under the
// bytewise comparator used by relocation files.
inline std::string EncodeBlobGcRelocationFileKey(uint64_t origin_offset) {
  std::string key(sizeof(origin_offset), '\0');
  for (size_t i = 0; i < sizeof(origin_offset); ++i) {
    key[i] = static_cast<char>(origin_offset >>
                               (8 * (sizeof(origin_offset) - 1 - i)));
  }
  return key;
}

// Relocation files use only the standard bytewise index. Preserve the column
// family's block sizing and block cache while removing user-key-specific
// filters, flush policies, and custom indexes that do not apply to encoded
// offsets.
inline BlockBasedTableOptions MakeBlobGcRelocationFileTableOptions(
    const BlockBasedTableOptions& source) {
  BlockBasedTableOptions options(source);
  options.index_type = BlockBasedTableOptions::kBinarySearch;
  options.data_block_index_type =
      BlockBasedTableOptions::kDataBlockBinarySearch;
  options.flush_block_policy_factory.reset();
  options.filter_policy.reset();
  options.partition_filters = false;
  options.user_defined_index_factory.reset();
  options.use_udi_as_primary_index = false;
  options.fail_if_no_udi_on_open = false;
  return options;
}

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
      !GetVarint64(&input, &decoded.payload_bytes) || !input.empty()) {
    return Status::Corruption("Error decoding embedded blob stats");
  }

  if (decoded.blob_count == 0 && decoded.payload_bytes != 0) {
    return Status::Corruption("Embedded blob stats have bytes but no records");
  }

  *stats = decoded;
  return Status::OK();
}

}  // namespace ROCKSDB_NAMESPACE

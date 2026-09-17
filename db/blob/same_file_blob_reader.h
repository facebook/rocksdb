//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <cstddef>
#include <cstdint>

#include "db/blob/blob_constants.h"
#include "rocksdb/rocksdb_namespace.h"
#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {

class BlobIndex;
class PinnableSlice;
struct ReadOptions;

// Narrow interface for resolving a same-file ("embedded") blob reference
// against the physical file that currently holds the table entry. Implemented
// by BlockBasedTable, which owns the reader context (file, footer, blob cache)
// needed to read an embedded blob record.
//
// Kept in its own low-level header, free of any table-layer dependency, so that
// db/blob (EmbeddedAwareBlobFetcher) and table/get_context can depend on just
// this interface without pulling in BlockBasedTable. ReadPathBlobResolver (the
// lazy-resolution read path) can likewise hold a const SameFileBlobReader*.
class SameFileBlobReader {
 public:
  virtual ~SameFileBlobReader() = default;

  // Resolves a same-file ("embedded") blob reference (one for which
  // blob_index.IsSameFile() is true) into `value`, pinning the payload
  // zero-copy (from the blob cache or an owned buffer).
  //
  // range_length == kWholeBlobLength resolves the whole value. Any other length
  // resolves only the sub-range [range_offset, range_offset + range_length),
  // reading just those bytes on a cache miss -- skipping whole-record checksum
  // verification (a strict sub-range cannot cover it) and never populating the
  // blob cache; valid only for an uncompressed record and a verify_policy other
  // than kVerifyIfPresent, with range_offset + range_length <=
  // blob_index.size().
  //
  // verify_policy controls whole-record checksum verification (see
  // BlobVerifyPolicy); a whole read under kVerifyIfPresent verifies even when
  // ReadOptions::verify_checksums is off. read_tier == kBlockCacheTier without
  // a cached record yields Status::Incomplete; a bad record yields
  // Status::Corruption.
  virtual Status GetSameFileBlob(const ReadOptions& read_options,
                                 const BlobIndex& blob_index,
                                 uint64_t range_offset, size_t range_length,
                                 BlobVerifyPolicy verify_policy,
                                 PinnableSlice* value) const = 0;
};

}  // namespace ROCKSDB_NAMESPACE

//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <memory>
#include <utility>
#include <vector>

#include "db/blob/blob_index.h"
#include "rocksdb/slice.h"

namespace ROCKSDB_NAMESPACE {

class PinnableSlice;

// Shared helper functions for blob column resolution, used by both
// ReadPathBlobResolver and CompactionBlobResolver. These operate on the
// common data structures: a vector of (column_index, BlobIndex) pairs
// for blob column metadata, and a vector of (column_index, PinnableSlice)
// pairs for resolved value caching.
//
// All functions use linear scan, which is optimal for the typical case
// of few blob columns (<5) per entity.
namespace blob_resolver_util {

// Find the BlobIndex for a given column index in the blob columns list.
// Returns nullptr if the column is not a blob column.
inline const BlobIndex* FindBlobColumn(
    const std::vector<std::pair<size_t, BlobIndex>>* blob_columns,
    size_t column_index) {
  if (blob_columns == nullptr) {
    return nullptr;
  }
  for (const auto& bc : *blob_columns) {
    if (bc.first == column_index) {
      return &bc.second;
    }
  }
  return nullptr;
}

// Find a previously resolved value in the cache.
// Returns nullptr if the column has not been resolved yet.
inline PinnableSlice* FindInCache(
    const std::vector<std::pair<size_t, std::unique_ptr<PinnableSlice>>>& cache,
    size_t column_index) {
  for (const auto& entry : cache) {
    if (entry.first == column_index) {
      return entry.second.get();
    }
  }
  return nullptr;
}

// Cache an inlined blob value. Creates a new cache entry with the blob's
// inline value pinned. Returns a Slice pointing to the cached value.
inline Slice CacheInlinedBlob(
    std::vector<std::pair<size_t, std::unique_ptr<PinnableSlice>>>& cache,
    size_t column_index, const BlobIndex& blob_index) {
  cache.emplace_back(column_index, std::make_unique<PinnableSlice>());
  cache.back().second->PinSelf(blob_index.value());
  return Slice(*cache.back().second);
}

// Check if a column index is a blob column.
inline bool IsBlobColumnIndex(
    const std::vector<std::pair<size_t, BlobIndex>>* blob_columns,
    size_t column_index) {
  return FindBlobColumn(blob_columns, column_index) != nullptr;
}

}  // namespace blob_resolver_util
}  // namespace ROCKSDB_NAMESPACE

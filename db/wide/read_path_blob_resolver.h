//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "db/blob/blob_index.h"
#include "rocksdb/cleanable.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "rocksdb/wide_columns.h"

namespace ROCKSDB_NAMESPACE {

class Version;

// Enables lazy (on-demand) resolution of blob column values in the read path.
// When a wide-column entity contains blob references (V2 format), the resolver
// stores the blob metadata and fetches blob values only when explicitly
// requested via ResolveColumn(). Resolved values are cached to avoid
// re-fetching.
//
// Used by both the iterator path (DBIter) and the point-lookup path
// (GetEntity via PinnableWideColumns).
//
// Thread safety: not thread-safe. A resolver instance is used by a single
// thread at a time. The Version* must remain valid for the lifetime of the
// resolver (ensured by SuperVersion pinning in the caller).
class ReadPathBlobResolver {
 public:
  ReadPathBlobResolver(const Version* version, ReadTier read_tier,
                       bool verify_checksums, bool fill_cache,
                       Env::IOActivity io_activity);

  // Reset the resolver for a new entity. Clears all cached values.
  // The columns and blob_columns pointers must remain valid for the lifetime
  // of the resolver (or until the next Reset/ResetWithOwnedData call).
  void Reset(const Slice& user_key,
             const std::vector<WideColumn>* columns,
             const std::vector<std::pair<size_t, BlobIndex>>* blob_columns);

  // Reset the resolver and take ownership of the column data. Use this when
  // the column vectors would otherwise go out of scope (e.g., GetEntity path
  // where the resolver outlives the function that parsed the entity).
  // Also takes ownership of the serialized entity data to keep Slice
  // references valid.
  void ResetWithOwnedData(
      const Slice& user_key,
      std::string&& serialized_entity,
      std::unique_ptr<std::vector<WideColumn>>&& columns,
      std::unique_ptr<std::vector<std::pair<size_t, BlobIndex>>>&& blob_columns);

  // Resolve the value for the column at the given index.
  // For blob columns, fetches the blob value from the blob file (or returns
  // from cache if already resolved). For inline columns, returns the inline
  // value directly.
  // Returns an error status if:
  // - column_index is out of bounds
  // - I/O error occurred while fetching the blob
  Status ResolveColumn(size_t column_index, Slice* resolved_value);

  // Resolve all unresolved blob columns at once.
  Status ResolveAllColumns();

  // Check if the column at the given index is an unresolved blob reference.
  // Returns false if column_index is out of bounds or the column is inline
  // or already resolved.
  bool IsUnresolvedColumn(size_t column_index) const;

  // Returns true if any blob columns have not yet been resolved.
  bool HasUnresolvedColumns() const;

  // Returns the total number of columns in the entity.
  size_t NumColumns() const;

  // Register a cleanup function that will be called when the resolver is
  // destroyed. Used to pin resources (e.g., SuperVersion) that must remain
  // alive while the resolver exists.
  void RegisterCleanup(Cleanable::CleanupFunction function, void* arg1,
                       void* arg2) {
    cleanable_.RegisterCleanup(function, arg1, arg2);
  }

 private:
  const Version* version_;
  ReadTier read_tier_;
  bool verify_checksums_;
  bool fill_cache_;
  Env::IOActivity io_activity_;

  Slice user_key_;
  const std::vector<WideColumn>* columns_ = nullptr;
  const std::vector<std::pair<size_t, BlobIndex>>* blob_columns_ = nullptr;

  // Cache for resolved blob values to avoid re-fetching.
  // Key: column index, Value: resolved blob value.
  std::unordered_map<size_t, PinnableSlice> resolved_cache_;

  // Map from column index to blob_columns_ vector index for O(1) lookup.
  std::unordered_map<size_t, size_t> blob_column_index_map_;

  // Owned data for the GetEntity path where column vectors must outlive
  // the function that parsed the entity. These are set by
  // ResetWithOwnedData().
  std::string owned_user_key_;
  std::string owned_serialized_entity_;
  std::unique_ptr<std::vector<WideColumn>> owned_columns_;
  std::unique_ptr<std::vector<std::pair<size_t, BlobIndex>>>
      owned_blob_columns_;

  // Cleanable for pinning resources (e.g., SuperVersion).
  Cleanable cleanable_;
};

}  // namespace ROCKSDB_NAMESPACE

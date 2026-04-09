//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "db/blob/blob_fetcher.h"
#include "db/blob/blob_index.h"
#include "rocksdb/cleanable.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "rocksdb/wide_columns.h"

namespace ROCKSDB_NAMESPACE {

class Version;

// TODO: ReadPathBlobResolver and CompactionBlobResolver (in
// compaction_iterator.h) share significant logic for blob column resolution
// and caching. A refactoring into a common base class or shared utility
// could reduce duplication. The two classes differ in their blob-fetching
// backends (Version::GetBlob vs BlobFetcher::FetchBlob) and contexts
// (read path vs compaction path with stats tracking).
//
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
                       Env::IOActivity io_activity,
                       BlobFileCache* blob_file_cache = nullptr,
                       bool allow_write_path_fallback = false);

  // Reset the resolver for a new entity. Clears all cached values.
  // The columns and blob_columns pointers must remain valid for the lifetime
  // of the resolver (or until the next Reset call).
  void Reset(const Slice& user_key, const std::vector<WideColumn>* columns,
             const std::vector<std::pair<size_t, BlobIndex>>* blob_columns);

  // Resolve the value for the column at the given index.
  // For blob columns, fetches the blob value from the blob file (or returns
  // from cache if already resolved). For inline columns, returns the inline
  // value directly.
  // Returns an error status if:
  // - column_index is out of bounds
  // - I/O error occurred while fetching the blob
  Status ResolveColumn(size_t column_index, Slice* resolved_value);

  // Resolve multiple columns in the order provided by `column_indices`.
  // Resolved blob values are cached exactly as if ResolveColumn() were called
  // repeatedly.
  Status ResolveColumns(const std::vector<size_t>& column_indices,
                        std::vector<Slice>* resolved_values);

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
  BlobFetcher blob_fetcher_;

  Slice user_key_;
  const std::vector<WideColumn>* columns_ = nullptr;
  const std::vector<std::pair<size_t, BlobIndex>>* blob_columns_ = nullptr;

  // Cache for resolved blob values to avoid re-fetching.
  // Uses a vector of (column_index, PinnableSlice) pairs. Typical entities
  // have few blob columns (<5), making linear scan cheaper than hash map
  // overhead. PinnableSlice values need stable addresses, so we use
  // unique_ptr to prevent invalidation when the vector grows.
  std::vector<std::pair<size_t, std::unique_ptr<PinnableSlice>>>
      resolved_cache_;

  // Cleanable for pinning resources (e.g., SuperVersion).
  Cleanable cleanable_;
};

}  // namespace ROCKSDB_NAMESPACE

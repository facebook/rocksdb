//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/wide/read_path_blob_resolver.h"

#include <cassert>

#include "db/version_set.h"
#include "db/wide/blob_column_resolver_util.h"

namespace ROCKSDB_NAMESPACE {

namespace {

ReadOptions BuildReadPathBlobResolverReadOptions(ReadTier read_tier,
                                                 bool verify_checksums,
                                                 bool fill_cache,
                                                 Env::IOActivity io_activity) {
  ReadOptions read_options;
  read_options.read_tier = read_tier;
  read_options.verify_checksums = verify_checksums;
  read_options.fill_cache = fill_cache;
  read_options.io_activity = io_activity;
  return read_options;
}

}  // namespace

ReadPathBlobResolver::ReadPathBlobResolver(
    const Version* version, ReadTier read_tier, bool verify_checksums,
    bool fill_cache, Env::IOActivity io_activity,
    BlobFileCache* blob_file_cache, bool allow_write_path_fallback)
    : blob_fetcher_(version,
                    BuildReadPathBlobResolverReadOptions(
                        read_tier, verify_checksums, fill_cache, io_activity),
                    blob_file_cache, allow_write_path_fallback) {}

void ReadPathBlobResolver::Reset(
    const Slice& user_key, const std::vector<WideColumn>* columns,
    const std::vector<std::pair<size_t, BlobIndex>>* blob_columns) {
  user_key_ = user_key;
  columns_ = columns;
  blob_columns_ = blob_columns;
  resolved_cache_.clear();
}

Status ReadPathBlobResolver::ResolveColumn(size_t column_index,
                                           Slice* resolved_value) {
  assert(columns_);
  assert(resolved_value);

  Status status = Status::OK();
  if (column_index >= columns_->size()) {
    status = Status::InvalidArgument("Column index out of bounds");
  } else {
    const BlobIndex* blob_index_ptr =
        blob_resolver_util::FindBlobColumn(blob_columns_, column_index);

    if (blob_index_ptr == nullptr) {
      // Inline column — return the value directly
      *resolved_value = (*columns_)[column_index].value();
    } else {
      // Check if already resolved
      PinnableSlice* cached =
          blob_resolver_util::FindInCache(resolved_cache_, column_index);
      if (cached != nullptr) {
        *resolved_value = *cached;
      } else {
        const BlobIndex& blob_index = *blob_index_ptr;

        // Handle inlined blobs
        if (blob_index.IsInlined()) {
          *resolved_value = blob_resolver_util::CacheInlinedBlob(
              resolved_cache_, column_index, blob_index);
        } else {
          resolved_cache_.emplace_back(column_index,
                                       std::make_unique<PinnableSlice>());
          auto& new_entry = resolved_cache_.back();

          constexpr FilePrefetchBuffer* prefetch_buffer = nullptr;
          constexpr uint64_t* bytes_read = nullptr;

          status =
              blob_fetcher_.FetchBlob(user_key_, blob_index, prefetch_buffer,
                                      new_entry.second.get(), bytes_read);
          if (!status.ok()) {
            resolved_cache_.pop_back();
          } else {
            *resolved_value = *new_entry.second;
          }
        }
      }
    }
  }
  return status;
}

Status ReadPathBlobResolver::ResolveColumns(
    const std::vector<size_t>& column_indices,
    std::vector<Slice>* resolved_values) {
  assert(resolved_values != nullptr);

  resolved_values->clear();
  resolved_values->reserve(column_indices.size());

  for (size_t column_index : column_indices) {
    Slice resolved_value;
    Status s = ResolveColumn(column_index, &resolved_value);
    if (!s.ok()) {
      resolved_values->clear();
      return s;
    }
    resolved_values->push_back(resolved_value);
  }

  return Status::OK();
}

Status ReadPathBlobResolver::ResolveAllColumns() {
  assert(columns_);

  if (!blob_columns_) {
    return Status::OK();
  }

  for (const auto& blob_col : *blob_columns_) {
    // ResolveColumn internally checks the cache, so we can just call it
    // directly; it will no-op for already resolved columns.
    Slice resolved_value;
    Status s = ResolveColumn(blob_col.first, &resolved_value);
    if (!s.ok()) {
      return s;
    }
  }

  return Status::OK();
}

bool ReadPathBlobResolver::IsUnresolvedColumn(size_t column_index) const {
  if (!columns_ || column_index >= columns_->size()) {
    return false;
  }

  if (!blob_resolver_util::IsBlobColumnIndex(blob_columns_, column_index)) {
    return false;
  }

  return blob_resolver_util::FindInCache(resolved_cache_, column_index) ==
         nullptr;
}

bool ReadPathBlobResolver::HasUnresolvedColumns() const {
  if (!blob_columns_ || blob_columns_->empty()) {
    return false;
  }

  for (const auto& blob_col : *blob_columns_) {
    if (blob_resolver_util::FindInCache(resolved_cache_, blob_col.first) ==
        nullptr) {
      return true;
    }
  }

  return false;
}

size_t ReadPathBlobResolver::NumColumns() const {
  if (!columns_) {
    return 0;
  }
  return columns_->size();
}

}  // namespace ROCKSDB_NAMESPACE

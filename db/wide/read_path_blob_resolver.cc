//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/wide/read_path_blob_resolver.h"

#include <cassert>

#include "db/version_set.h"

namespace ROCKSDB_NAMESPACE {

ReadPathBlobResolver::ReadPathBlobResolver(const Version* version,
                                           ReadTier read_tier,
                                           bool verify_checksums,
                                           bool fill_cache,
                                           Env::IOActivity io_activity)
    : version_(version),
      read_tier_(read_tier),
      verify_checksums_(verify_checksums),
      fill_cache_(fill_cache),
      io_activity_(io_activity) {}

void ReadPathBlobResolver::Reset(
    const Slice& user_key, const std::vector<WideColumn>* columns,
    const std::vector<std::pair<size_t, BlobIndex>>* blob_columns) {
  user_key_ = user_key;
  columns_ = columns;
  blob_columns_ = blob_columns;
  resolved_cache_.clear();
  blob_column_index_map_.clear();

  // Clear any owned data from a previous ResetWithOwnedData call
  owned_user_key_.clear();
  owned_serialized_entity_.clear();
  owned_columns_.reset();
  owned_blob_columns_.reset();

  if (blob_columns_) {
    for (size_t i = 0; i < blob_columns_->size(); ++i) {
      blob_column_index_map_[(*blob_columns_)[i].first] = i;
    }
  }
}

void ReadPathBlobResolver::ResetWithOwnedData(
    const Slice& user_key, std::string&& serialized_entity,
    std::unique_ptr<std::vector<WideColumn>>&& columns,
    std::unique_ptr<std::vector<std::pair<size_t, BlobIndex>>>&& blob_columns) {
  resolved_cache_.clear();
  blob_column_index_map_.clear();

  // Take ownership of the data
  owned_user_key_.assign(user_key.data(), user_key.size());
  owned_serialized_entity_ = std::move(serialized_entity);
  owned_columns_ = std::move(columns);
  owned_blob_columns_ = std::move(blob_columns);

  // Point to owned data
  user_key_ = owned_user_key_;
  columns_ = owned_columns_.get();
  blob_columns_ = owned_blob_columns_.get();

  if (blob_columns_) {
    for (size_t i = 0; i < blob_columns_->size(); ++i) {
      blob_column_index_map_[(*blob_columns_)[i].first] = i;
    }
  }
}

Status ReadPathBlobResolver::ResolveColumn(size_t column_index,
                                           Slice* resolved_value) {
  assert(columns_);
  assert(resolved_value);

  if (column_index >= columns_->size()) {
    return Status::InvalidArgument("Column index out of bounds");
  }

  // Check if it's a blob column
  auto it = blob_column_index_map_.find(column_index);
  if (it == blob_column_index_map_.end()) {
    // Inline column — return the value directly
    *resolved_value = (*columns_)[column_index].value();
    return Status::OK();
  }

  // Check if already resolved
  auto cache_it = resolved_cache_.find(column_index);
  if (cache_it != resolved_cache_.end()) {
    *resolved_value = cache_it->second;
    return Status::OK();
  }

  // Fetch the blob value
  assert(version_);
  const size_t blob_idx = it->second;
  assert(blob_idx < blob_columns_->size());
  const BlobIndex& blob_index = (*blob_columns_)[blob_idx].second;

  // Handle inlined blobs
  if (blob_index.IsInlined()) {
    auto [emplace_it, inserted] =
        resolved_cache_.emplace(column_index, PinnableSlice());
    assert(inserted);
    emplace_it->second.PinSelf(blob_index.value());
    *resolved_value = emplace_it->second;
    return Status::OK();
  }

  ReadOptions read_options;
  read_options.read_tier = read_tier_;
  read_options.verify_checksums = verify_checksums_;
  read_options.fill_cache = fill_cache_;
  read_options.io_activity = io_activity_;

  auto [emplace_it, inserted] =
      resolved_cache_.emplace(column_index, PinnableSlice());
  assert(inserted);

  constexpr FilePrefetchBuffer* prefetch_buffer = nullptr;
  constexpr uint64_t* bytes_read = nullptr;

  Status s = version_->GetBlob(read_options, user_key_, blob_index,
                                prefetch_buffer, &emplace_it->second,
                                bytes_read);
  if (!s.ok()) {
    resolved_cache_.erase(emplace_it);
    return s;
  }

  *resolved_value = emplace_it->second;
  return Status::OK();
}

Status ReadPathBlobResolver::ResolveAllColumns() {
  assert(columns_);

  if (!blob_columns_) {
    return Status::OK();
  }

  for (const auto& blob_col : *blob_columns_) {
    const size_t col_idx = blob_col.first;
    if (resolved_cache_.find(col_idx) != resolved_cache_.end()) {
      continue;  // Already resolved
    }
    Slice resolved_value;
    Status s = ResolveColumn(col_idx, &resolved_value);
    if (!s.ok()) {
      return s;
    }
  }

  return Status::OK();
}

bool ReadPathBlobResolver::IsUnresolvedColumn(size_t column_index) const {
  if (!blob_columns_ || !columns_ || column_index >= columns_->size()) {
    return false;
  }

  auto it = blob_column_index_map_.find(column_index);
  if (it == blob_column_index_map_.end()) {
    return false;  // Not a blob column
  }

  return resolved_cache_.find(column_index) == resolved_cache_.end();
}

bool ReadPathBlobResolver::HasUnresolvedColumns() const {
  if (!blob_columns_ || blob_columns_->empty()) {
    return false;
  }

  for (const auto& blob_col : *blob_columns_) {
    if (resolved_cache_.find(blob_col.first) == resolved_cache_.end()) {
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

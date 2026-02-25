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
}

Status ReadPathBlobResolver::ResolveColumn(size_t column_index,
                                           Slice* resolved_value) {
  assert(columns_);
  assert(resolved_value);

  if (column_index >= columns_->size()) {
    return Status::InvalidArgument("Column index out of bounds");
  }

  // Linear scan to find if this is a blob column. Typical entities have
  // few blob columns (<5), so this is cheaper than hash map overhead.
  const BlobIndex* blob_index_ptr = nullptr;
  if (blob_columns_) {
    for (const auto& bc : *blob_columns_) {
      if (bc.first == column_index) {
        blob_index_ptr = &bc.second;
        break;
      }
    }
  }

  if (blob_index_ptr == nullptr) {
    // Inline column — return the value directly
    *resolved_value = (*columns_)[column_index].value();
    return Status::OK();
  }

  // Check if already resolved
  for (const auto& entry : resolved_cache_) {
    if (entry.first == column_index) {
      *resolved_value = *entry.second;
      return Status::OK();
    }
  }

  // Fetch the blob value
  assert(version_);
  const BlobIndex& blob_index = *blob_index_ptr;

  // Handle inlined blobs
  if (blob_index.IsInlined()) {
    resolved_cache_.emplace_back(column_index,
                                 std::make_unique<PinnableSlice>());
    resolved_cache_.back().second->PinSelf(blob_index.value());
    *resolved_value = *resolved_cache_.back().second;
    return Status::OK();
  }

  ReadOptions read_options;
  read_options.read_tier = read_tier_;
  read_options.verify_checksums = verify_checksums_;
  read_options.fill_cache = fill_cache_;
  read_options.io_activity = io_activity_;

  resolved_cache_.emplace_back(column_index, std::make_unique<PinnableSlice>());
  auto& new_entry = resolved_cache_.back();

  constexpr FilePrefetchBuffer* prefetch_buffer = nullptr;
  constexpr uint64_t* bytes_read = nullptr;

  Status s =
      version_->GetBlob(read_options, user_key_, blob_index, prefetch_buffer,
                        new_entry.second.get(), bytes_read);
  if (!s.ok()) {
    resolved_cache_.pop_back();
    return s;
  }

  *resolved_value = *new_entry.second;
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
  if (!blob_columns_ || !columns_ || column_index >= columns_->size()) {
    return false;
  }

  // Check if this is a blob column via linear scan
  bool is_blob = false;
  for (const auto& bc : *blob_columns_) {
    if (bc.first == column_index) {
      is_blob = true;
      break;
    }
  }
  if (!is_blob) {
    return false;
  }

  // Check if already resolved
  for (const auto& entry : resolved_cache_) {
    if (entry.first == column_index) {
      return false;
    }
  }
  return true;
}

bool ReadPathBlobResolver::HasUnresolvedColumns() const {
  if (!blob_columns_ || blob_columns_->empty()) {
    return false;
  }

  for (const auto& blob_col : *blob_columns_) {
    bool found = false;
    for (const auto& entry : resolved_cache_) {
      if (entry.first == blob_col.first) {
        found = true;
        break;
      }
    }
    if (!found) {
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

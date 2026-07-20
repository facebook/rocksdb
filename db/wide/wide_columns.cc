//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "rocksdb/wide_columns.h"

#include <iterator>

#include "db/blob/blob_index.h"
#include "db/wide/wide_column_serialization.h"

namespace ROCKSDB_NAMESPACE {

const Slice kDefaultWideColumnName;

const WideColumns kNoWideColumns;

Status PinnableWideColumns::BuildColumnsForEntity() {
  columns_.clear();
  unresolved_blob_column_indices_.clear();

  // Called right after a Set*() has placed the serialized entity in a single
  // backing buffer; the whole entity must live in backing_.front().
  assert(!backing_.empty());
  assert(std::next(backing_.begin()) == backing_.end());

  // Collect any blob column references; a resolved entity simply yields none.
  std::vector<std::pair<size_t, BlobIndex>> blob_columns;
  Status status = WideColumnSerialization::Deserialize(backing_.front(),
                                                       columns_, &blob_columns);
  if (status.ok()) {
    unresolved_blob_column_indices_.reserve(blob_columns.size());
    for (const auto& blob_column : blob_columns) {
      unresolved_blob_column_indices_.push_back(blob_column.first);
    }
  }

  return status;
}

size_t PinnableWideColumns::serialized_size() const {
  return WideColumnSerialization::SerializedSizeV1(columns_);
}

}  // namespace ROCKSDB_NAMESPACE

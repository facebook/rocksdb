//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "rocksdb/wide_columns.h"

#include "db/blob/blob_index.h"
#include "db/wide/wide_column_serialization.h"

namespace ROCKSDB_NAMESPACE {

const Slice kDefaultWideColumnName;

const WideColumns kNoWideColumns;

Status PinnableWideColumns::CreateIndexForWideColumns() {
  columns_.clear();
  unresolved_blob_column_indices_.clear();

  Slice value_copy = value_;
  Status status = WideColumnSerialization::Deserialize(value_copy, columns_);
  if (status.IsNotSupported()) {
    // Deserialize() may already populate columns_ before discovering V2 blob
    // references and returning NotSupported. Clear the partial result before
    // falling back to DeserializeV2(), which requires an empty output vector.
    columns_.clear();
    value_copy = value_;
    std::vector<std::pair<size_t, BlobIndex>> blob_columns;
    status = WideColumnSerialization::DeserializeV2(value_copy, columns_,
                                                    blob_columns);
    if (status.ok()) {
      unresolved_blob_column_indices_.reserve(blob_columns.size());
      for (const auto& blob_column : blob_columns) {
        unresolved_blob_column_indices_.push_back(blob_column.first);
      }
    }
  }

  return status;
}

}  // namespace ROCKSDB_NAMESPACE

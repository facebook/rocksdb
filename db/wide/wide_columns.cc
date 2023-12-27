//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "rocksdb/wide_columns.h"

#include "db/wide/wide_column_serialization.h"

namespace ROCKSDB_NAMESPACE {

const Slice kDefaultWideColumnName;

const WideColumns kNoWideColumns;

Status PinnableWideColumns::CreateIndexForWideColumns() {
  Slice value_copy = value_;

  return WideColumnSerialization::Deserialize(value_copy, columns_);
}

}  // namespace ROCKSDB_NAMESPACE

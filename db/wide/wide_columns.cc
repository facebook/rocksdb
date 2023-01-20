//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "rocksdb/wide_columns.h"

#include <algorithm>

#include "db/wide/wide_column_serialization.h"

namespace ROCKSDB_NAMESPACE {

const Slice kDefaultWideColumnName;

const WideColumns kNoWideColumns;

Status ToPinnableWideColumns(const WideColumns& columns,
                             PinnableWideColumns& pinnable_columns) {
  WideColumns sorted_columns(columns);
  std::sort(sorted_columns.begin(), sorted_columns.end(),
            [](const WideColumn& lhs, const WideColumn& rhs) {
              return lhs.name().compare(rhs.name()) < 0;
            });

  std::string serialized_columns;

  {
    const Status s =
        WideColumnSerialization::Serialize(sorted_columns, serialized_columns);
    if (!s.ok()) {
      return s;
    }
  }

  {
    const Status s = pinnable_columns.SetWideColumnValue(serialized_columns);
    if (!s.ok()) {
      return s;
    }
  }

  return Status::OK();
}

Status PinnableWideColumns::CreateIndexForWideColumns() {
  Slice buf_copy = buf_;

  return WideColumnSerialization::Deserialize(buf_copy, columns_);
}

}  // namespace ROCKSDB_NAMESPACE

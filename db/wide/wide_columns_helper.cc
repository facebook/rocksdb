//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/wide/wide_columns_helper.h"

#include "db/wide/wide_column_serialization.h"

namespace ROCKSDB_NAMESPACE {
void WideColumnsHelper::DumpWideColumns(const WideColumns& columns,
                                        std::ostream& oss, bool hex) {
  if (hex) {
    oss << std::hex;
  }
  auto it = columns.begin();
  oss << *it;
  for (++it; it != columns.end(); ++it) {
    oss << ' ' << *it;
  }
}
Status WideColumnsHelper::DumpSliceAsWideColumns(const Slice& value,
                                                 std::ostream& oss, bool hex) {
  WideColumns columns;
  Slice value_copy = value;
  const Status s = WideColumnSerialization::Deserialize(value_copy, columns);
  if (s.ok()) {
    DumpWideColumns(columns, oss, hex);
  }
  return s;
}

}  // namespace ROCKSDB_NAMESPACE

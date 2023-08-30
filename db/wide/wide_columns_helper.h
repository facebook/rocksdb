//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once
#include <ostream>
#include <string>

#include "rocksdb/rocksdb_namespace.h"
#include "rocksdb/wide_columns.h"

namespace ROCKSDB_NAMESPACE {
class WideColumnsHelper {
 public:
  static void DumpWideColumns(const WideColumns& columns, std::ostream& oss,
                              bool hex);
  static Status DumpSliceAsWideColumns(const Slice& value, std::ostream& oss,
                                       bool hex);
};
}  // namespace ROCKSDB_NAMESPACE

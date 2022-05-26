//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <cstdint>
#include <string>

#include "rocksdb/rocksdb_namespace.h"
#include "rocksdb/status.h"
#include "rocksdb/wide_columns.h"

namespace ROCKSDB_NAMESPACE {

class Slice;

class WideColumnSerialization {
 public:
  static Status Serialize(const WideColumns& columns, std::string& output);
  static Status Deserialize(Slice& input, WideColumns& columns);

  static WideColumns::const_iterator Find(const WideColumns& columns,
                                          const Slice& column_name);

  static constexpr uint32_t kCurrentVersion = 1;
};

}  // namespace ROCKSDB_NAMESPACE

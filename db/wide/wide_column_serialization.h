//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <string>
#include <utility>
#include <vector>

#include "rocksdb/rocksdb_namespace.h"
#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {

class Slice;

class WideColumnSerialization {
 public:
  using ColumnDesc = std::pair<Slice, Slice>;
  using ColumnDescs = std::vector<ColumnDesc>;

  static Status Serialize(const ColumnDescs& column_descs, std::string* output);
  static Status Deserialize(Slice* input, ColumnDescs* column_descs);
};

}  // namespace ROCKSDB_NAMESPACE

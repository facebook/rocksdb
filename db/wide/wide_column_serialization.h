//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <cstdint>
#include <string>

#include "rocksdb/rocksdb_namespace.h"
#include "rocksdb/status.h"
#include "rocksdb/types.h"

namespace ROCKSDB_NAMESPACE {

class Slice;

class WideColumnSerialization {
 public:
  static Status Serialize(const WideColumnDescs& column_descs,
                          std::string* output);
  static Status DeserializeOne(Slice* input, const Slice& column_name,
                               WideColumnDesc* column_desc);
  static Status DeserializeAll(Slice* input, WideColumnDescs* column_descs);

 private:
  static Status DeserializeIndex(Slice* input, WideColumnDescs* column_descs);

  static constexpr uint32_t kCurrentVersion = 1;
};

}  // namespace ROCKSDB_NAMESPACE

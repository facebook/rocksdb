//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "rocksdb/rocksdb_namespace.h"
#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {

class Slice;

struct WideColumnDesc {
  WideColumnDesc() = default;

  template <typename N, typename V>
  WideColumnDesc(N&& n, V&& v)
      : name(std::forward<N>(n)), value(std::forward<V>(v)) {}

  Slice name;
  Slice value;
};

bool operator==(const WideColumnDesc& lhs, const WideColumnDesc& rhs);
bool operator!=(const WideColumnDesc& lhs, const WideColumnDesc& rhs);

using WideColumnDescs = std::vector<WideColumnDesc>;

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

inline bool operator==(const WideColumnDesc& lhs, const WideColumnDesc& rhs) {
  return lhs.name == rhs.name && lhs.value == rhs.value;
}

inline bool operator!=(const WideColumnDesc& lhs, const WideColumnDesc& rhs) {
  return !(lhs == rhs);
}

}  // namespace ROCKSDB_NAMESPACE

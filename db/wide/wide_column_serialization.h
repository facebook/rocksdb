//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "rocksdb/rocksdb_namespace.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {

class WideColumnDesc {
 public:
  WideColumnDesc() = default;

  template <typename N, typename V>
  WideColumnDesc(N&& name, V&& value)
      : name_(std::forward<N>(name)), value_(std::forward<V>(value)) {}

  const Slice& name() const { return name_; }
  const Slice& value() const { return value_; }

  Slice& name() { return name_; }
  Slice& value() { return value_; }

 private:
  Slice name_;
  Slice value_;
};

bool operator==(const WideColumnDesc& lhs, const WideColumnDesc& rhs);
bool operator!=(const WideColumnDesc& lhs, const WideColumnDesc& rhs);

using WideColumnDescs = std::vector<WideColumnDesc>;

class WideColumnSerialization {
 public:
  static Status Serialize(const WideColumnDescs& column_descs,
                          std::string& output);
  static Status DeserializeOne(Slice& input, const Slice& column_name,
                               WideColumnDesc& column_desc);
  static Status DeserializeAll(Slice& input, WideColumnDescs& column_descs);

  static constexpr uint32_t kCurrentVersion = 1;

 private:
  static Status DeserializeIndex(Slice& input, WideColumnDescs& column_descs);
};

inline bool operator==(const WideColumnDesc& lhs, const WideColumnDesc& rhs) {
  return lhs.name() == rhs.name() && lhs.value() == rhs.value();
}

inline bool operator!=(const WideColumnDesc& lhs, const WideColumnDesc& rhs) {
  return !(lhs == rhs);
}

}  // namespace ROCKSDB_NAMESPACE

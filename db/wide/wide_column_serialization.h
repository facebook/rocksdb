//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <cstdint>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include "rocksdb/rocksdb_namespace.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {

class WideColumnDesc {
 public:
  WideColumnDesc() = default;

  // Initializes a WideColumnDesc object by forwarding the name and value
  // arguments to the corresponding member Slices. This makes it possible to
  // construct a WideColumnDesc using combinations of const char*, const
  // std::string&, const Slice& etc., for example:
  //
  // constexpr char foo[] = "foo";
  // const std::string bar("bar");
  // WideColumnDesc desc(foo, bar);
  template <typename N, typename V>
  WideColumnDesc(N&& name, V&& value)
      : name_(std::forward<N>(name)), value_(std::forward<V>(value)) {}

  // Initializes a WideColumnDesc object by forwarding the elements of
  // name_tuple and value_tuple to the constructors of the corresponding member
  // Slices. This makes it possible to initialize the Slices using the Slice
  // constructors that take more than one argument, for example:
  //
  // constexpr char foo_name[] = "foo_name";
  // constexpr char bar_value[] = "bar_value";
  // WideColumnDesc desc(std::piecewise_construct,
  //                     std::forward_as_tuple(foo_name, 3),
  //                     std::forward_as_tuple(bar_value, 3));
  template <typename NTuple, typename VTuple>
  WideColumnDesc(std::piecewise_construct_t, NTuple&& name_tuple,
                 VTuple&& value_tuple)
      : name_(std::make_from_tuple<Slice>(std::forward<NTuple>(name_tuple))),
        value_(std::make_from_tuple<Slice>(std::forward<VTuple>(value_tuple))) {
  }

  const Slice& name() const { return name_; }
  const Slice& value() const { return value_; }

  Slice& name() { return name_; }
  Slice& value() { return value_; }

 private:
  Slice name_;
  Slice value_;
};

// Note: column names and values are compared bytewise.
bool operator==(const WideColumnDesc& lhs, const WideColumnDesc& rhs);
bool operator!=(const WideColumnDesc& lhs, const WideColumnDesc& rhs);

using WideColumnDescs = std::vector<WideColumnDesc>;

class WideColumnSerialization {
 public:
  static Status Serialize(const WideColumnDescs& column_descs,
                          std::string& output);
  static Status Deserialize(Slice& input, WideColumnDescs& column_descs);

  static WideColumnDescs::const_iterator Find(
      const WideColumnDescs& column_descs, const Slice& column_name);

  static constexpr uint32_t kCurrentVersion = 1;
};

inline bool operator==(const WideColumnDesc& lhs, const WideColumnDesc& rhs) {
  return lhs.name() == rhs.name() && lhs.value() == rhs.value();
}

inline bool operator!=(const WideColumnDesc& lhs, const WideColumnDesc& rhs) {
  return !(lhs == rhs);
}

}  // namespace ROCKSDB_NAMESPACE

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

// Wide-column serialization/deserialization primitives.
//
// The two main parts of the layout are 1) a sorted index containing the column
// names and column value sizes and 2) the column values themselves. Keeping the
// index and the values separate will enable selectively reading column values
// down the line. Note that currently the index has to be fully parsed in order
// to find out the offset of each column value.
//
// Legend: cn = column name, cv = column value, cns = column name size, cvs =
// column value size.
//
//      +----------+--------------+----------+-------+----------+---...
//      | version  | # of columns |  cns 1   | cn 1  |  cvs 1   |
//      +----------+--------------+------------------+--------- +---...
//      | varint32 |   varint32   | varint32 | bytes | varint32 |
//      +----------+--------------+----------+-------+----------+---...
//
//      ... continued ...
//
//          ...---+----------+-------+----------+-------+---...---+-------+
//                |  cns N   | cn N  |  cvs N   | cv 1  |         | cv N  |
//          ...---+----------+-------+----------+-------+---...---+-------+
//                | varint32 | bytes | varint32 | bytes |         | bytes |
//          ...---+----------+-------+----------+-------+---...---+-------+

class WideColumnSerialization {
 public:
  static Status Serialize(const WideColumns& columns, std::string& output);
  static Status Serialize(const Slice& value_of_default,
                          const WideColumns& other_columns,
                          std::string& output);

  static Status Deserialize(Slice& input, WideColumns& columns);

  static WideColumns::const_iterator Find(const WideColumns& columns,
                                          const Slice& column_name);
  static Status GetValueOfDefaultColumn(Slice& input, Slice& value);

  static constexpr uint32_t kCurrentVersion = 1;

 private:
  static Status SerializeImpl(const Slice* value_of_default,
                              const WideColumns& columns, std::string& output);
};

inline Status WideColumnSerialization::Serialize(const WideColumns& columns,
                                                 std::string& output) {
  constexpr Slice* value_of_default = nullptr;

  return SerializeImpl(value_of_default, columns, output);
}

inline Status WideColumnSerialization::Serialize(
    const Slice& value_of_default, const WideColumns& other_columns,
    std::string& output) {
  return SerializeImpl(&value_of_default, other_columns, output);
}

}  // namespace ROCKSDB_NAMESPACE

//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <algorithm>
#include <cassert>
#include <ostream>

#include "rocksdb/rocksdb_namespace.h"
#include "rocksdb/wide_columns.h"

namespace ROCKSDB_NAMESPACE {

class WideColumnsHelper {
 public:
  static void DumpWideColumns(const WideColumns& columns, std::ostream& os,
                              bool hex);

  static Status DumpSliceAsWideColumns(const Slice& value, std::ostream& os,
                                       bool hex);

  static bool HasDefaultColumn(const WideColumns& columns) {
    return !columns.empty() && columns.front().name() == kDefaultWideColumnName;
  }

  static bool HasDefaultColumnOnly(const WideColumns& columns) {
    return columns.size() == 1 &&
           columns.front().name() == kDefaultWideColumnName;
  }

  static const Slice& GetDefaultColumn(const WideColumns& columns) {
    assert(HasDefaultColumn(columns));
    return columns.front().value();
  }

  static void SortColumns(WideColumns& columns) {
    std::sort(columns.begin(), columns.end(),
              [](const WideColumn& lhs, const WideColumn& rhs) {
                return lhs.name().compare(rhs.name()) < 0;
              });
  }

  template <typename Iterator>
  static Iterator Find(Iterator begin, Iterator end, const Slice& column_name) {
    assert(std::is_sorted(begin, end,
                          [](const WideColumn& lhs, const WideColumn& rhs) {
                            return lhs.name().compare(rhs.name()) < 0;
                          }));

    auto it = std::lower_bound(begin, end, column_name,
                               [](const WideColumn& lhs, const Slice& rhs) {
                                 return lhs.name().compare(rhs) < 0;
                               });

    if (it == end || it->name() != column_name) {
      return end;
    }

    return it;
  }
};

}  // namespace ROCKSDB_NAMESPACE

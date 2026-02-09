//  Copyright (c) Meta Platforms, Inc. and affiliates.
//
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <string>
#include <variant>

#include "rocksdb/rocksdb_namespace.h"
#include "rocksdb/slice.h"
#include "util/overload.h"

namespace ROCKSDB_NAMESPACE {

class SecondaryIndexHelper {
 public:
  static Slice AsSlice(const std::variant<Slice, std::string>& var) {
    return std::visit([](const auto& value) -> Slice { return value; }, var);
  }

  static std::string AsString(const std::variant<Slice, std::string>& var) {
    return std::visit(
        overload{
            [](const Slice& value) -> std::string { return value.ToString(); },
            [](const std::string& value) -> std::string { return value; }},
        var);
  }
};

}  // namespace ROCKSDB_NAMESPACE

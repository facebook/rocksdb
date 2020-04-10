// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once

#include <string>
#include <unordered_map>
#include <unordered_set>

#include "rocksdb/configurable.h"
#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {
class OptionTypeInfo;
struct DBOptions;

#ifndef ROCKSDB_LITE
// Wrapper class for configuring structs.
// This class can be used to configure classes that
// do not inherit from Configurable (such as structs)
// and that do not need special handling of options
template <typename T>
class ConfigurableStruct : public Configurable {
 public:
  ConfigurableStruct(
      const std::string& opt_name, const T& options,
      const std::unordered_map<std::string, OptionTypeInfo>* opt_map,
      bool is_mutable)
      : ConfigurableStruct(opt_name, opt_map, is_mutable) {
    options_ = options;
  }

  ConfigurableStruct(
      const std::string& opt_name,
      const std::unordered_map<std::string, OptionTypeInfo>* opt_map,
      bool is_mutable)
      : is_mutable_(is_mutable) {
    RegisterOptions(opt_name, &options_, opt_map);
  }

  bool IsMutable() const override { return is_mutable_; }

 private:
  bool is_mutable_;
  T options_;
};
#endif  // ROCKSDB_LITE
}  // namespace ROCKSDB_NAMESPACE

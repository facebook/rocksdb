// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#pragma once

#include <unordered_map>
#include <string>
#include "rocksdb/options.h"

namespace rocksdb {

#ifndef ROCKSDB_LITE
// Take a map of option name and option value, apply them into the
// base_options, and return the new options as a result
bool GetColumnFamilyOptionsFromMap(
    const ColumnFamilyOptions& base_options,
    const std::unordered_map<std::string, std::string>& opts_map,
    ColumnFamilyOptions* new_options);

bool GetDBOptionsFromMap(
    const DBOptions& base_options,
    const std::unordered_map<std::string, std::string>& opts_map,
    DBOptions* new_options);

// Take a string representation of option names and  values, apply them into the
// base_options, and return the new options as a result. The string has the
// following format:
//   "write_buffer_size=1024;max_write_buffer_number=2"
bool GetColumnFamilyOptionsFromString(
    const ColumnFamilyOptions& base_options,
    const std::string& opts_str,
    ColumnFamilyOptions* new_options);

bool GetDBOptionsFromString(
    const DBOptions& base_options,
    const std::string& opts_str,
    DBOptions* new_options);
#endif  // ROCKSDB_LITE

}  // namespace rocksdb

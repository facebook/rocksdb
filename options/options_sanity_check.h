// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <string>
#include <unordered_map>

#ifndef ROCKSDB_LITE
#include "rocksdb/utilities/options_type.h"

namespace rocksdb {
// This enum defines the RocksDB options sanity level.

// The sanity check level for DB options
static const std::unordered_map<std::string, OptionsSanityCheckLevel>
    sanity_level_db_options {};

// The sanity check level for block-based table options
static const std::unordered_map<std::string, OptionsSanityCheckLevel>
    sanity_level_bbt_options {};

OptionsSanityCheckLevel DBOptionSanityCheckLevel(
    const std::string& options_name);
OptionsSanityCheckLevel BBTOptionSanityCheckLevel(
    const std::string& options_name);

}  // namespace rocksdb

#endif  // !ROCKSDB_LITE

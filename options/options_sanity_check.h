// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <string>
#include <unordered_map>

#include "rocksdb/convenience.h"
#include "rocksdb/rocksdb_namespace.h"

#ifndef ROCKSDB_LITE
namespace ROCKSDB_NAMESPACE {
// This enum defines the RocksDB options sanity level.

// The sanity check level for DB options
static const std::unordered_map<std::string, ConfigOptions::SanityLevel>
    sanity_level_db_options{};

// The sanity check level for column-family options
static const std::unordered_map<std::string, ConfigOptions::SanityLevel>
    sanity_level_cf_options = {
        {"comparator",
         ConfigOptions::SanityLevel::kSanityLevelLooselyCompatible},
        {"table_factory",
         ConfigOptions::SanityLevel::kSanityLevelLooselyCompatible},
        {"merge_operator",
         ConfigOptions::SanityLevel::kSanityLevelLooselyCompatible}};

// The sanity check level for block-based table options
static const std::unordered_map<std::string, ConfigOptions::SanityLevel>
    sanity_level_bbt_options{};

ConfigOptions::SanityLevel DBOptionSanityCheckLevel(
    const std::string& options_name);
ConfigOptions::SanityLevel CFOptionSanityCheckLevel(
    const std::string& options_name);
ConfigOptions::SanityLevel BBTOptionSanityCheckLevel(
    const std::string& options_name);

}  // namespace ROCKSDB_NAMESPACE

#endif  // !ROCKSDB_LITE

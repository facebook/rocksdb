//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include "rocksdb/table_properties.h"

namespace ROCKSDB_NAMESPACE {
#ifndef NDEBUG
void TEST_SetRandomTableProperties(TableProperties* props);
#endif

// REQUIRED:
// The following TEST_GetXXXPropStartEndPosition() funcitons require the
// following layout of TableProperties being true:
//
// struct TableProperties {
//    int64_t orig_file_number = 0;
//    ...
//    ... int64_t properties only
//    ...
//    std::string db_id;
//    ...
//    ... std::string properties only
//    ...
//    UserCollectedProperties user_collected_properties;
//    ...
//    ... non-int64_t/std::string/UserCollectedProperties only
//    ...
// }
//
// WARNING: minimize usage of these functions as they assume some fixed layout
// of TableProperties
std::pair<const uint64_t*, const uint64_t*> GetUint64TPropStartEndPosition(
    const TableProperties* props);
std::pair<const std::string*, const std::string*> GetStringPropStartEndPosition(
    const TableProperties* props);
}  // namespace ROCKSDB_NAMESPACE

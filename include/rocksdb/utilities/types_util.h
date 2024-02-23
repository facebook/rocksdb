//  Copyright (c) Meta Platforms, Inc. and affiliates.
//
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "rocksdb/types.h"

namespace ROCKSDB_NAMESPACE {

// Util method that takes an internal key and parse it to get `ParsedEntryInfo`.
// Such an internal key usually comes from a table iterator.
Status ParseEntry(const Slice& internal_key, ParsedEntryInfo* parsed_entry);

}  // namespace ROCKSDB_NAMESPACE

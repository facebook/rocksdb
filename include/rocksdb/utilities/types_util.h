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

// Given a user key, create the internal key used for `Seek` operation for a
// raw table iterator. The internal key is stored in `buf` and a Slice
// representing the internal key is returned.
Slice GetInternalKeyForSeek(const Slice& user_key, std::string* buf);

// Given a user key, create the internal key used for `SeekForPrev` operation
// for a raw table iterator. The internal key is stored in `buf` and a Slice
// representing the internal key is returned.
Slice GetInternalKeyForSeekForPrev(const Slice& user_key, std::string* buf);

// Util method that takes an internal key and parse it to get `ParsedEntryInfo`.
// Such an internal key usually comes from a table iterator.
Status ParseEntry(const Slice& internal_key, ParsedEntryInfo* parsed_entry);

}  // namespace ROCKSDB_NAMESPACE

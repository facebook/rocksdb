//  Copyright (c) Meta Platforms, Inc. and affiliates.
//
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include "rocksdb/comparator.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "rocksdb/types.h"

namespace ROCKSDB_NAMESPACE {

// Given a user key, creates the internal key used for `Seek` operation for a
// raw table iterator. The internal key is stored in `buf`.
// `comparator` should be the same as the `Options.comparator` used to create
// the column family or the `SstFileWriter`.
Status GetInternalKeyForSeek(const Slice& user_key,
                             const Comparator* comparator, std::string* buf);

// Given a user key, creates the internal key used for `SeekForPrev` operation
// for a raw table iterator. The internal key is stored in `buf`.
// `comparator`: see doc for `GetInternalKeyForSeek`.
Status GetInternalKeyForSeekForPrev(const Slice& user_key,
                                    const Comparator* comparator,
                                    std::string* buf);

// Util method that takes an internal key and parse it to get `ParsedEntryInfo`.
// Such an internal key usually comes from a table iterator.
// `comparator`: see doc for `GetInternalKeyForSeek`.
Status ParseEntry(const Slice& internal_key, const Comparator* comparator,
                  ParsedEntryInfo* parsed_entry);

}  // namespace ROCKSDB_NAMESPACE

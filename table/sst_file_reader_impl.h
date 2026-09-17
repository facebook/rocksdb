// Copyright (c) Meta Platforms, Inc. and affiliates.
// This source code is licensed under both the GPLv2 (found in the
// COPYING file in the root directory) and Apache 2.0 License
// (found in the LICENSE.Apache file in the root directory).

#pragma once

#include "options/cf_options.h"
#include "rocksdb/sst_file_reader.h"
#include "table/table_reader.h"
#include "util/coro_utils.h"
#if USE_COROUTINES
#include "folly/executors/IOExecutor.h"
#endif  // USE_COROUTINES

namespace ROCKSDB_NAMESPACE {

struct SstFileReader::Rep {
  Options options;
  EnvOptions soptions;
  ImmutableOptions ioptions;
  MutableCFOptions moptions;
  // Keep a member variable for this, since `NewIterator()` uses a const
  // reference of `ReadOptions`.
  ReadOptions roptions_for_table_iter;

  std::unique_ptr<TableReader> table_reader;
#if USE_COROUTINES
  folly::IOExecutor* read_executor = nullptr;
#endif  // USE_COROUTINES

  DECLARE_SYNC_AND_ASYNC(std::vector<Status>, MultiGet,
                         const ReadOptions& roptions,
                         const std::vector<Slice>& keys,
                         std::vector<PinnableSlice>* values);
  DECLARE_SYNC_AND_ASYNC(Status, Get, const ReadOptions& roptions,
                         const Slice& key, PinnableSlice* value);

  explicit Rep(const Options& opts)
      : options(opts),
        soptions(options),
        ioptions(options),
        moptions(ColumnFamilyOptions(options)) {
    roptions_for_table_iter =
        ReadOptions(/*_verify_checksums=*/true, /*_fill_cache=*/false);
  }
};

}  // namespace ROCKSDB_NAMESPACE

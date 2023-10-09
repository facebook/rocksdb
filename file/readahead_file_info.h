// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <cstddef>
#include <cstdint>

#include "rocksdb/rocksdb_namespace.h"

namespace ROCKSDB_NAMESPACE {

// struct ReadaheadFileInfo contains readahead information that is passed from
// one file to another file per level during iterations. This information helps
// iterators to carry forward the internal automatic prefetching readahead value
// to next file during sequential reads instead of starting from the scratch.

struct ReadaheadFileInfo {
  struct ReadaheadInfo {
    size_t readahead_size = 0;
    int64_t num_file_reads = 0;
  };

  // Used by Data block iterators to update readahead info.
  ReadaheadInfo data_block_readahead_info;

  // Used by Index block iterators to update readahead info.
  ReadaheadInfo index_block_readahead_info;
};

}  // namespace ROCKSDB_NAMESPACE

// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//

#pragma once

#include <cstddef>
#include <cstdint>

namespace ROCKSDB_NAMESPACE {

struct ReadaheadInfo {
  struct ReadPattern {
    size_t readahead_size = 0;
    int64_t num_file_reads = 0;
  };

  ReadPattern data_block_pattern;
  ReadPattern index_block_pattern;
};

}  // namespace ROCKSDB_NAMESPACE

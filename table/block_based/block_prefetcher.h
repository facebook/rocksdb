//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#pragma once
#include "table/block_based/block_based_table_reader.h"

namespace ROCKSDB_NAMESPACE {
class BlockPrefetcher {
 public:
  explicit BlockPrefetcher(size_t compaction_readahead_size)
      : compaction_readahead_size_(compaction_readahead_size) {}
  void PrefetchIfNeeded(const BlockBasedTable::Rep* rep,
                        const BlockHandle& handle, size_t readahead_size,
                        bool is_for_compaction);
  FilePrefetchBuffer* prefetch_buffer() { return prefetch_buffer_.get(); }

  void UpdateReadPattern(const size_t& offset, const size_t& len) {
    prev_offset_ = offset;
    prev_len_ = len;
  }

  bool IsBlockSequential(const size_t& offset) {
    return (prev_len_ == 0 || (prev_offset_ + prev_len_ == offset));
  }

  void ResetValues() {
    num_file_reads_ = 1;
    readahead_size_ = BlockBasedTable::kInitAutoReadaheadSize;
    readahead_limit_ = 0;
    return;
  }

 private:
  // Readahead size used in compaction, its value is used only if
  // lookup_context_.caller = kCompaction.
  size_t compaction_readahead_size_;

  size_t readahead_size_ = BlockBasedTable::kInitAutoReadaheadSize;
  size_t readahead_limit_ = 0;
  int64_t num_file_reads_ = 0;
  size_t prev_offset_ = 0;
  size_t prev_len_ = 0;
  std::unique_ptr<FilePrefetchBuffer> prefetch_buffer_;
};
}  // namespace ROCKSDB_NAMESPACE

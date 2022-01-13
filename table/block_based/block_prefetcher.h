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

  void UpdateReadPattern(const uint64_t& offset, const size_t& len) {
    prev_offset_ = offset;
    prev_len_ = len;
  }

  bool IsBlockSequential(const uint64_t& offset) {
    return (prev_len_ == 0 || (prev_offset_ + prev_len_ == offset));
  }

  void ResetValues() {
    num_file_reads_ = 1;
    // Since initial_auto_readahead_size_ can be different from
    // kInitAutoReadaheadSize in case of adaptive_readahead, so fallback the
    // readahead_size_ to kInitAutoReadaheadSize in case of reset.
    initial_auto_readahead_size_ = BlockBasedTable::kInitAutoReadaheadSize;
    readahead_size_ = initial_auto_readahead_size_;
    readahead_limit_ = 0;
    return;
  }

  void SetReadaheadState(ReadaheadFileInfo::ReadaheadInfo* readahead_info) {
    num_file_reads_ = readahead_info->num_file_reads;
    initial_auto_readahead_size_ = readahead_info->readahead_size;
    TEST_SYNC_POINT_CALLBACK("BlockPrefetcher::SetReadaheadState",
                             &initial_auto_readahead_size_);
  }

 private:
  // Readahead size used in compaction, its value is used only if
  // lookup_context_.caller = kCompaction.
  size_t compaction_readahead_size_;

  // readahead_size_ is used if underlying FS supports prefetching.
  size_t readahead_size_ = BlockBasedTable::kInitAutoReadaheadSize;
  size_t readahead_limit_ = 0;
  // initial_auto_readahead_size_ is used if RocksDB uses internal prefetch
  // buffer.
  uint64_t initial_auto_readahead_size_ =
      BlockBasedTable::kInitAutoReadaheadSize;
  int64_t num_file_reads_ = 0;
  uint64_t prev_offset_ = 0;
  size_t prev_len_ = 0;
  std::unique_ptr<FilePrefetchBuffer> prefetch_buffer_;
};
}  // namespace ROCKSDB_NAMESPACE

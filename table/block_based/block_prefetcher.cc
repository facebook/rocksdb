//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#include "table/block_based/block_prefetcher.h"

namespace ROCKSDB_NAMESPACE {
void BlockPrefetcher::PrefetchIfNeeded(const BlockBasedTable::Rep* rep,
                                       const BlockHandle& handle,
                                       size_t readahead_size,
                                       bool is_for_compaction) {
  if (is_for_compaction) {
    rep->CreateFilePrefetchBufferIfNotExists(compaction_readahead_size_,
                                             compaction_readahead_size_,
                                             &prefetch_buffer_);
    return;
  }

  // Explicit user requested readahead
  if (readahead_size > 0) {
    rep->CreateFilePrefetchBufferIfNotExists(readahead_size, readahead_size,
                                             &prefetch_buffer_);
    return;
  }

  // Implicit auto readahead, which will be enabled if the number of reads
  // reached `kMinNumFileReadsToStartAutoReadahead` (default: 2).
  num_file_reads_++;
  if (num_file_reads_ <=
      BlockBasedTable::kMinNumFileReadsToStartAutoReadahead) {
    return;
  }

  if (rep->file->use_direct_io()) {
    rep->CreateFilePrefetchBufferIfNotExists(
        BlockBasedTable::kInitAutoReadaheadSize,
        BlockBasedTable::kMaxAutoReadaheadSize, &prefetch_buffer_);
    return;
  }

  if (handle.offset() + static_cast<size_t>(block_size(handle)) <=
      readahead_limit_) {
    return;
  }

  // If prefetch is not supported, fall back to use internal prefetch buffer.
  // Discarding other return status of Prefetch calls intentionally, as
  // we can fallback to reading from disk if Prefetch fails.
  Status s = rep->file->Prefetch(handle.offset(), readahead_size_);
  if (s.IsNotSupported()) {
    rep->CreateFilePrefetchBufferIfNotExists(
        BlockBasedTable::kInitAutoReadaheadSize,
        BlockBasedTable::kMaxAutoReadaheadSize, &prefetch_buffer_);
    return;
  }
  readahead_limit_ = static_cast<size_t>(handle.offset() + readahead_size_);
  // Keep exponentially increasing readahead size until
  // kMaxAutoReadaheadSize.
  readahead_size_ =
      std::min(BlockBasedTable::kMaxAutoReadaheadSize, readahead_size_ * 2);
}
}  // namespace ROCKSDB_NAMESPACE

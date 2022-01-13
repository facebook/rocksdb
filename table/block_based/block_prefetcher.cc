//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#include "table/block_based/block_prefetcher.h"

#include "table/block_based/block_based_table_reader.h"

namespace ROCKSDB_NAMESPACE {
void BlockPrefetcher::PrefetchIfNeeded(const BlockBasedTable::Rep* rep,
                                       const BlockHandle& handle,
                                       size_t readahead_size,
                                       bool is_for_compaction) {
  if (is_for_compaction) {
    rep->CreateFilePrefetchBufferIfNotExists(compaction_readahead_size_,
                                             compaction_readahead_size_,
                                             &prefetch_buffer_, false);
    return;
  }

  // Explicit user requested readahead.
  if (readahead_size > 0) {
    rep->CreateFilePrefetchBufferIfNotExists(readahead_size, readahead_size,
                                             &prefetch_buffer_, false);
    return;
  }

  // Implicit readahead.

  // If max_auto_readahead_size is set to be 0 by user, no data will be
  // prefetched.
  size_t max_auto_readahead_size = rep->table_options.max_auto_readahead_size;
  if (max_auto_readahead_size == 0) {
    return;
  }

  size_t len = BlockBasedTable::BlockSizeWithTrailer(handle);
  size_t offset = handle.offset();

  // If FS supports prefetching (readahead_limit_ will be non zero in that case)
  // and current block exists in prefetch buffer then return.
  if (offset + len <= readahead_limit_) {
    UpdateReadPattern(offset, len);
    return;
  }

  if (!IsBlockSequential(offset)) {
    UpdateReadPattern(offset, len);
    ResetValues();
    return;
  }
  UpdateReadPattern(offset, len);

  // Implicit auto readahead, which will be enabled if the number of reads
  // reached `kMinNumFileReadsToStartAutoReadahead` (default: 2)  and scans are
  // sequential.
  num_file_reads_++;
  if (num_file_reads_ <=
      BlockBasedTable::kMinNumFileReadsToStartAutoReadahead) {
    return;
  }

  if (initial_auto_readahead_size_ > max_auto_readahead_size) {
    initial_auto_readahead_size_ = max_auto_readahead_size;
  }

  if (rep->file->use_direct_io()) {
    rep->CreateFilePrefetchBufferIfNotExists(initial_auto_readahead_size_,
                                             max_auto_readahead_size,
                                             &prefetch_buffer_, true);
    return;
  }

  if (readahead_size_ > max_auto_readahead_size) {
    readahead_size_ = max_auto_readahead_size;
  }

  // If prefetch is not supported, fall back to use internal prefetch buffer.
  // Discarding other return status of Prefetch calls intentionally, as
  // we can fallback to reading from disk if Prefetch fails.
  Status s = rep->file->Prefetch(
      handle.offset(),
      BlockBasedTable::BlockSizeWithTrailer(handle) + readahead_size_);
  if (s.IsNotSupported()) {
    rep->CreateFilePrefetchBufferIfNotExists(initial_auto_readahead_size_,
                                             max_auto_readahead_size,
                                             &prefetch_buffer_, true);
    return;
  }

  readahead_limit_ = offset + len + readahead_size_;
  // Keep exponentially increasing readahead size until
  // max_auto_readahead_size.
  readahead_size_ = std::min(max_auto_readahead_size, readahead_size_ * 2);
}
}  // namespace ROCKSDB_NAMESPACE

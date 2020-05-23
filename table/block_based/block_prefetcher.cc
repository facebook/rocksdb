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
  if (!is_for_compaction) {
    if (readahead_size == 0) {
      // Implicit auto readahead
      num_file_reads_++;
      if (num_file_reads_ >
          BlockBasedTable::kMinNumFileReadsToStartAutoReadahead) {
        if (!rep->file->use_direct_io() &&
            (handle.offset() + static_cast<size_t>(block_size(handle)) >
             readahead_limit_)) {
          // Buffered I/O
          // Discarding the return status of Prefetch calls intentionally, as
          // we can fallback to reading from disk if Prefetch fails.
          rep->file->Prefetch(handle.offset(), readahead_size_);
          readahead_limit_ =
              static_cast<size_t>(handle.offset() + readahead_size_);
          // Keep exponentially increasing readahead size until
          // kMaxAutoReadaheadSize.
          readahead_size_ = std::min(BlockBasedTable::kMaxAutoReadaheadSize,
                                     readahead_size_ * 2);
        } else if (rep->file->use_direct_io() && !prefetch_buffer_) {
          // Direct I/O
          // Let FilePrefetchBuffer take care of the readahead.
          rep->CreateFilePrefetchBuffer(BlockBasedTable::kInitAutoReadaheadSize,
                                        BlockBasedTable::kMaxAutoReadaheadSize,
                                        &prefetch_buffer_);
        }
      }
    } else if (!prefetch_buffer_) {
      // Explicit user requested readahead
      // The actual condition is:
      // if (readahead_size != 0 && !prefetch_buffer_)
      rep->CreateFilePrefetchBuffer(readahead_size, readahead_size,
                                    &prefetch_buffer_);
    }
  } else if (!prefetch_buffer_) {
    rep->CreateFilePrefetchBuffer(compaction_readahead_size_,
                                  compaction_readahead_size_,
                                  &prefetch_buffer_);
  }
}
}  // namespace ROCKSDB_NAMESPACE

//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/blob/prefetch_buffer_collection.h"

namespace ROCKSDB_NAMESPACE {

FilePrefetchBuffer* PrefetchBufferCollection::GetOrCreatePrefetchBuffer(
    uint64_t file_number) {
  auto& prefetch_buffer = prefetch_buffers_[file_number];
  if (!prefetch_buffer) {
    ReadaheadParams readahead_params;
    readahead_params.initial_readahead_size = readahead_size_;
    readahead_params.max_readahead_size = readahead_size_;
    prefetch_buffer.reset(new FilePrefetchBuffer(readahead_params));
  }

  return prefetch_buffer.get();
}

}  // namespace ROCKSDB_NAMESPACE

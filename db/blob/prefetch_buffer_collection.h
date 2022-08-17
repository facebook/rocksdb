//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <cassert>
#include <cstdint>
#include <memory>
#include <unordered_map>

#include "file/file_prefetch_buffer.h"
#include "rocksdb/rocksdb_namespace.h"

namespace ROCKSDB_NAMESPACE {

// A class that owns a collection of FilePrefetchBuffers using the file number
// as key. Used for implementing compaction readahead for blob files. Designed
// to be accessed by a single thread only: every (sub)compaction needs its own
// buffers since they are guaranteed to read different blobs from different
// positions even when reading the same file.
class PrefetchBufferCollection {
 public:
  explicit PrefetchBufferCollection(uint64_t readahead_size)
      : readahead_size_(readahead_size) {
    assert(readahead_size_ > 0);
  }

  FilePrefetchBuffer* GetOrCreatePrefetchBuffer(uint64_t file_number);

 private:
  uint64_t readahead_size_;
  std::unordered_map<uint64_t, std::unique_ptr<FilePrefetchBuffer>>
      prefetch_buffers_;  // maps file number to prefetch buffer
};

}  // namespace ROCKSDB_NAMESPACE

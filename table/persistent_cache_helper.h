//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
#pragma once

#include <string>

#include "monitoring/statistics.h"
#include "table/format.h"
#include "table/persistent_cache_options.h"

namespace ROCKSDB_NAMESPACE {

struct BlockContents;

// PersistentCacheHelper
//
// Encapsulates  some of the helper logic for read and writing from the cache
class PersistentCacheHelper {
 public:
  // Insert block into cache of serialized blocks. Size includes block trailer
  // (if applicable).
  static void InsertSerialized(const PersistentCacheOptions& cache_options,
                               const BlockHandle& handle, const char* data,
                               const size_t size);

  // Insert block into cache of uncompressed blocks. No block trailer.
  static void InsertUncompressed(const PersistentCacheOptions& cache_options,
                                 const BlockHandle& handle,
                                 const BlockContents& contents);

  // Lookup block from cache of serialized blocks. Size includes block trailer
  // (if applicable).
  static Status LookupSerialized(const PersistentCacheOptions& cache_options,
                                 const BlockHandle& handle,
                                 std::unique_ptr<char[]>* out_data,
                                 const size_t expected_data_size);

  // Lookup block from uncompressed cache. No block trailer.
  static Status LookupUncompressed(const PersistentCacheOptions& cache_options,
                                   const BlockHandle& handle,
                                   BlockContents* contents);
};

}  // namespace ROCKSDB_NAMESPACE

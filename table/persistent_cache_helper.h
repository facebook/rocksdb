//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
#pragma once

#include <string>

#include "table/block_based_table_reader.h"
#include "util/statistics.h"

namespace rocksdb {

struct BlockContents;

// PersistentCacheOptions
//
// This describe the caching behavior for page cache
// This is used to pass the context for caching and the cache handle
struct PersistentCacheOptions {
  PersistentCacheOptions() {}
  explicit PersistentCacheOptions(
      const std::shared_ptr<PersistentCache>& _persistent_cache,
      const std::string _key_prefix, Statistics* const _statistics)
      : persistent_cache(_persistent_cache),
        key_prefix(_key_prefix),
        statistics(_statistics) {}

  virtual ~PersistentCacheOptions() {}

  std::shared_ptr<PersistentCache> persistent_cache;
  std::string key_prefix;
  Statistics* statistics = nullptr;
};

// PersistentCacheHelper
//
// Encapsulates  some of the helper logic for read and writing from the cache
class PersistentCacheHelper {
 public:
  // insert block into raw page cache
  static void InsertRawPage(const PersistentCacheOptions& cache_options,
                            const BlockHandle& handle, const char* data,
                            const size_t size);

  // insert block into uncompressed cache
  static void InsertUncompressedPage(
      const PersistentCacheOptions& cache_options, const BlockHandle& handle,
      const BlockContents& contents);

  // lookup block from raw page cacge
  static Status LookupRawPage(const PersistentCacheOptions& cache_options,
                              const BlockHandle& handle,
                              std::unique_ptr<char[]>* raw_data,
                              const size_t raw_data_size);

  // lookup block from uncompressed cache
  static Status LookupUncompressedPage(
      const PersistentCacheOptions& cache_options, const BlockHandle& handle,
      BlockContents* contents);
};

}  // namespace rocksdb

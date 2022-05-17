//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
#pragma once

#include <string>

#include "cache/cache_key.h"
#include "monitoring/statistics.h"
#include "rocksdb/persistent_cache.h"

namespace ROCKSDB_NAMESPACE {

// PersistentCacheOptions
//
// This describe the caching behavior for page cache
// This is used to pass the context for caching and the cache handle
struct PersistentCacheOptions {
  PersistentCacheOptions() {}
  explicit PersistentCacheOptions(
      const std::shared_ptr<PersistentCache>& _persistent_cache,
      const OffsetableCacheKey& _base_cache_key, Statistics* const _statistics)
      : persistent_cache(_persistent_cache),
        base_cache_key(_base_cache_key),
        statistics(_statistics) {}
  std::shared_ptr<PersistentCache> persistent_cache;
  OffsetableCacheKey base_cache_key;
  Statistics* statistics = nullptr;

  static const PersistentCacheOptions kEmpty;
};

}  // namespace ROCKSDB_NAMESPACE

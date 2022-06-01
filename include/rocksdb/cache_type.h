//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include "rocksdb/cache.h"

namespace ROCKSDB_NAMESPACE {

enum CacheType : unsigned char {
  kLRUCache,
  kFastLRUCache,
  kClockCache
};

// Public string representation of the cache types.
// Command line options should use these values.
const std::string kLRUCacheString = "lru_cache";
const std::string kFastLRUCacheString = "fast_lru_cache";
const std::string kClockCacheString = "clock_cache";

inline enum CacheType StringToCacheType(const std::string& ctype) {
  CacheType ret_cache_type;
  if (ctype == kLRUCacheString) {
    ret_cache_type = CacheType::kLRUCache;
  } else if (ctype == kFastLRUCacheString) {
    ret_cache_type = CacheType::kFastLRUCache;
  } else if (ctype == kClockCacheString) {
    ret_cache_type = CacheType::kClockCache;
  } else {
      fprintf(stderr, "Cannot parse cache type '%s'\n", ctype.c_str());
      ret_cache_type = CacheType::kLRUCache;  // Default value.
  }
  return ret_cache_type;
}

}  // namespace ROCKSDB_NAMESPACE

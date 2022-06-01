//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <cassert>

#include "rocksdb/cache.h"

namespace ROCKSDB_NAMESPACE {

enum CacheType : unsigned char { kLRUCache, kFastLRUCache, kClockCache };

inline enum CacheType StringToCacheType(const char* ctype) {
  assert(ctype);
  CacheType ret_cache_type;
  if (!strcasecmp(ctype, "lru_cache")) {
    ret_cache_type = CacheType::kLRUCache;
  } else if (!strcasecmp(ctype, "fast_lru_cache")) {
    ret_cache_type = CacheType::kFastLRUCache;
  } else if (!strcasecmp(ctype, "clock_cache")) {
    ret_cache_type = CacheType::kClockCache;
  } else {
    fprintf(stderr, "Cannot parse cache type '%s'\n", ctype);
    ret_cache_type = CacheType::kLRUCache;  // default value
  }
  return ret_cache_type;
}

}  // namespace ROCKSDB_NAMESPACE

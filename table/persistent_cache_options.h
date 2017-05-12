//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//  This source code is also licensed under the GPLv2 license found in the
//  COPYING file in the root directory of this source tree.
#pragma once

#include <string>

#include "monitoring/statistics.h"
#include "rocksdb/persistent_cache.h"

namespace rocksdb {

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

}  // namespace rocksdb

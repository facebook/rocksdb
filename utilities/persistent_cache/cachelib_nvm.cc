//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
#ifndef ROCKSDB_LITE

#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif

#include "utilities/persistent_cache/cachelib_nvm.h"

namespace rocksdb {

Status CacheLibNvm::Insert(const Slice& key, const char* data,
                           const size_t size) {
  // TODO(idemura): implement
  return Status();
}

Status CacheLibNvm::Lookup(const Slice& key, std::unique_ptr<char[]>* data,
                           size_t* size) {
  // TODO(idemura): implement
  return Status();
}

auto CacheLibNvm::Stats() -> StatsType {
  // TODO(idemura): implement
  return {};
}

std::string CacheLibNvm::GetPrintableOptions() const {
  // TODO(idemura): implement
  return "CacheLibNvm";
}

Status NewCacheLibNvm(Env* const env, const std::string& path,
                      const uint64_t size, const std::shared_ptr<Logger>& log,
                      std::shared_ptr<PersistentCache>* cache) {
  return Status();
}

}  // namespace rocksdb

#endif

//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "cache/cache_helpers.h"

namespace ROCKSDB_NAMESPACE {

void ReleaseCacheHandleCleanup(void* arg1, void* arg2) {
  Cache* const cache = static_cast<Cache*>(arg1);
  assert(cache);

  Cache::Handle* const cache_handle = static_cast<Cache::Handle*>(arg2);
  assert(cache_handle);

  cache->Release(cache_handle);
}

Status WarmInCache(Cache* cache, const Slice& key, const Slice& saved,
                   Cache::CreateContext* create_context,
                   const Cache::CacheItemHelper* helper,
                   Cache::Priority priority, size_t* out_charge) {
  assert(helper);
  assert(helper->create_cb);
  Cache::ObjectPtr value;
  size_t charge;
  Status st = helper->create_cb(saved, create_context,
                                cache->memory_allocator(), &value, &charge);
  if (st.ok()) {
    st =
        cache->Insert(key, value, helper, charge, /*handle*/ nullptr, priority);
    if (out_charge) {
      *out_charge = charge;
    }
  }
  return st;
}

}  // namespace ROCKSDB_NAMESPACE

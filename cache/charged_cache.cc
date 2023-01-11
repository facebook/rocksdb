//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "cache/charged_cache.h"

#include "cache/cache_reservation_manager.h"

namespace ROCKSDB_NAMESPACE {

ChargedCache::ChargedCache(std::shared_ptr<Cache> cache,
                           std::shared_ptr<Cache> block_cache)
    : cache_(cache),
      cache_res_mgr_(std::make_shared<ConcurrentCacheReservationManager>(
          std::make_shared<
              CacheReservationManagerImpl<CacheEntryRole::kBlobCache>>(
              block_cache))) {}

Status ChargedCache::Insert(const Slice& key, ObjectPtr obj,
                            const CacheItemHelper* helper, size_t charge,
                            Handle** handle, Priority priority) {
  Status s = cache_->Insert(key, obj, helper, charge, handle, priority);
  if (s.ok()) {
    // Insert may cause the cache entry eviction if the cache is full. So we
    // directly call the reservation manager to update the total memory used
    // in the cache.
    assert(cache_res_mgr_);
    cache_res_mgr_->UpdateCacheReservation(cache_->GetUsage())
        .PermitUncheckedError();
  }
  return s;
}

Cache::Handle* ChargedCache::Lookup(const Slice& key,
                                    const CacheItemHelper* helper,
                                    CreateContext* create_context,
                                    Priority priority, bool wait,
                                    Statistics* stats) {
  auto handle =
      cache_->Lookup(key, helper, create_context, priority, wait, stats);
  // Lookup may promote the KV pair from the secondary cache to the primary
  // cache. So we directly call the reservation manager to update the total
  // memory used in the cache.
  if (helper && helper->create_cb) {
    assert(cache_res_mgr_);
    cache_res_mgr_->UpdateCacheReservation(cache_->GetUsage())
        .PermitUncheckedError();
  }
  return handle;
}

bool ChargedCache::Release(Cache::Handle* handle, bool useful,
                           bool erase_if_last_ref) {
  size_t memory_used_delta = cache_->GetUsage(handle);
  bool erased = cache_->Release(handle, useful, erase_if_last_ref);
  if (erased) {
    assert(cache_res_mgr_);
    cache_res_mgr_
        ->UpdateCacheReservation(memory_used_delta, /* increase */ false)
        .PermitUncheckedError();
  }
  return erased;
}

bool ChargedCache::Release(Cache::Handle* handle, bool erase_if_last_ref) {
  size_t memory_used_delta = cache_->GetUsage(handle);
  bool erased = cache_->Release(handle, erase_if_last_ref);
  if (erased) {
    assert(cache_res_mgr_);
    cache_res_mgr_
        ->UpdateCacheReservation(memory_used_delta, /* increase */ false)
        .PermitUncheckedError();
  }
  return erased;
}

void ChargedCache::Erase(const Slice& key) {
  cache_->Erase(key);
  assert(cache_res_mgr_);
  cache_res_mgr_->UpdateCacheReservation(cache_->GetUsage())
      .PermitUncheckedError();
}

void ChargedCache::EraseUnRefEntries() {
  cache_->EraseUnRefEntries();
  assert(cache_res_mgr_);
  cache_res_mgr_->UpdateCacheReservation(cache_->GetUsage())
      .PermitUncheckedError();
}

void ChargedCache::SetCapacity(size_t capacity) {
  cache_->SetCapacity(capacity);
  // SetCapacity can result in evictions when the cache capacity is decreased,
  // so we would want to update the cache reservation here as well.
  assert(cache_res_mgr_);
  cache_res_mgr_->UpdateCacheReservation(cache_->GetUsage())
      .PermitUncheckedError();
}

}  // namespace ROCKSDB_NAMESPACE

//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "cache/charged_cache.h"

#include "cache/cache_reservation_manager.h"

namespace ROCKSDB_NAMESPACE {

ChargedCache::ChargedCache(std::shared_ptr<Cache> cache,
                           std::shared_ptr<Cache> block_cache)
    : CacheWrapper(cache),
      cache_res_mgr_(std::make_shared<ConcurrentCacheReservationManager>(
          std::make_shared<
              CacheReservationManagerImpl<CacheEntryRole::kBlobCache>>(
              block_cache))) {}

Status ChargedCache::Insert(const Slice& key, ObjectPtr obj,
                            const CacheItemHelper* helper, size_t charge,
                            Handle** handle, Priority priority,
                            const Slice& compressed_val, CompressionType type) {
  Status s = target_->Insert(key, obj, helper, charge, handle, priority,
                             compressed_val, type);
  if (s.ok()) {
    // Insert may cause the cache entry eviction if the cache is full. So we
    // directly call the reservation manager to update the total memory used
    // in the cache.
    assert(cache_res_mgr_);
    cache_res_mgr_->UpdateCacheReservation(target_->GetUsage())
        .PermitUncheckedError();
  }
  return s;
}

Cache::Handle* ChargedCache::Lookup(const Slice& key,
                                    const CacheItemHelper* helper,
                                    CreateContext* create_context,
                                    Priority priority, Statistics* stats) {
  auto handle = target_->Lookup(key, helper, create_context, priority, stats);
  // Lookup may promote the KV pair from the secondary cache to the primary
  // cache. So we directly call the reservation manager to update the total
  // memory used in the cache.
  if (helper && helper->create_cb) {
    assert(cache_res_mgr_);
    cache_res_mgr_->UpdateCacheReservation(target_->GetUsage())
        .PermitUncheckedError();
  }
  return handle;
}

void ChargedCache::WaitAll(AsyncLookupHandle* async_handles, size_t count) {
  target_->WaitAll(async_handles, count);
  // In case of any promotions. Although some could finish by return of
  // StartAsyncLookup, Wait/WaitAll will generally be used, so simpler to
  // update here.
  assert(cache_res_mgr_);
  cache_res_mgr_->UpdateCacheReservation(target_->GetUsage())
      .PermitUncheckedError();
}

bool ChargedCache::Release(Cache::Handle* handle, bool useful,
                           bool erase_if_last_ref) {
  size_t memory_used_delta = target_->GetUsage(handle);
  bool erased = target_->Release(handle, useful, erase_if_last_ref);
  if (erased) {
    assert(cache_res_mgr_);
    cache_res_mgr_
        ->UpdateCacheReservation(memory_used_delta, /* increase */ false)
        .PermitUncheckedError();
  }
  return erased;
}

bool ChargedCache::Release(Cache::Handle* handle, bool erase_if_last_ref) {
  size_t memory_used_delta = target_->GetUsage(handle);
  bool erased = target_->Release(handle, erase_if_last_ref);
  if (erased) {
    assert(cache_res_mgr_);
    cache_res_mgr_
        ->UpdateCacheReservation(memory_used_delta, /* increase */ false)
        .PermitUncheckedError();
  }
  return erased;
}

void ChargedCache::Erase(const Slice& key) {
  target_->Erase(key);
  assert(cache_res_mgr_);
  cache_res_mgr_->UpdateCacheReservation(target_->GetUsage())
      .PermitUncheckedError();
}

void ChargedCache::EraseUnRefEntries() {
  target_->EraseUnRefEntries();
  assert(cache_res_mgr_);
  cache_res_mgr_->UpdateCacheReservation(target_->GetUsage())
      .PermitUncheckedError();
}

void ChargedCache::SetCapacity(size_t capacity) {
  target_->SetCapacity(capacity);
  // SetCapacity can result in evictions when the cache capacity is decreased,
  // so we would want to update the cache reservation here as well.
  assert(cache_res_mgr_);
  cache_res_mgr_->UpdateCacheReservation(target_->GetUsage())
      .PermitUncheckedError();
}

}  // namespace ROCKSDB_NAMESPACE

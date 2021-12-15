//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#include "cache/cache_reservation_manager.h"

#include <cassert>
#include <cstddef>
#include <cstring>
#include <memory>

#include "cache/cache_entry_roles.h"
#include "rocksdb/cache.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "table/block_based/block_based_table_reader.h"
#include "util/coding.h"

namespace ROCKSDB_NAMESPACE {
CacheReservationManager::CacheReservationManager(std::shared_ptr<Cache> cache,
                                                 bool delayed_decrease)
    : delayed_decrease_(delayed_decrease),
      cache_allocated_size_(0),
      memory_used_(0) {
  assert(cache != nullptr);
  cache_ = cache;
}

CacheReservationManager::~CacheReservationManager() {
  for (auto* handle : dummy_handles_) {
    cache_->Release(handle, true);
  }
}

template <CacheEntryRole R>
Status CacheReservationManager::UpdateCacheReservation(
    std::size_t new_mem_used) {
  memory_used_ = new_mem_used;
  std::size_t cur_cache_allocated_size =
      cache_allocated_size_.load(std::memory_order_relaxed);
  if (new_mem_used == cur_cache_allocated_size) {
    return Status::OK();
  } else if (new_mem_used > cur_cache_allocated_size) {
    Status s = IncreaseCacheReservation<R>(new_mem_used);
    return s;
  } else {
    // In delayed decrease mode, we don't decrease cache reservation
    // untill the memory usage is less than 3/4 of what we reserve
    // in the cache.
    // We do this because
    // (1) Dummy entry insertion is expensive in block cache
    // (2) Delayed releasing previously inserted dummy entries can save such
    // expensive dummy entry insertion on memory increase in the near future,
    // which is likely to happen when the memory usage is greater than or equal
    // to 3/4 of what we reserve
    if (delayed_decrease_ && new_mem_used >= cur_cache_allocated_size / 4 * 3) {
      return Status::OK();
    } else {
      Status s = DecreaseCacheReservation(new_mem_used);
      return s;
    }
  }
}

// Explicitly instantiate templates for "CacheEntryRole" values we use.
// This makes it possible to keep the template definitions in the .cc file.
template Status CacheReservationManager::UpdateCacheReservation<
    CacheEntryRole::kWriteBuffer>(std::size_t new_mem_used);
template Status CacheReservationManager::UpdateCacheReservation<
    CacheEntryRole::kCompressionDictionaryBuildingBuffer>(
    std::size_t new_mem_used);
// For cache reservation manager unit tests
template Status CacheReservationManager::UpdateCacheReservation<
    CacheEntryRole::kMisc>(std::size_t new_mem_used);

template <CacheEntryRole R>
Status CacheReservationManager::MakeCacheReservation(
    std::size_t incremental_memory_used,
    std::unique_ptr<CacheReservationHandle<R>>* handle) {
  assert(handle != nullptr);
  Status s =
      UpdateCacheReservation<R>(GetTotalMemoryUsed() + incremental_memory_used);
  (*handle).reset(new CacheReservationHandle<R>(incremental_memory_used,
                                                shared_from_this()));
  return s;
}

template Status
CacheReservationManager::MakeCacheReservation<CacheEntryRole::kMisc>(
    std::size_t incremental_memory_used,
    std::unique_ptr<CacheReservationHandle<CacheEntryRole::kMisc>>* handle);
template Status CacheReservationManager::MakeCacheReservation<
    CacheEntryRole::kFilterConstruction>(
    std::size_t incremental_memory_used,
    std::unique_ptr<
        CacheReservationHandle<CacheEntryRole::kFilterConstruction>>* handle);

template <CacheEntryRole R>
Status CacheReservationManager::IncreaseCacheReservation(
    std::size_t new_mem_used) {
  Status return_status = Status::OK();
  while (new_mem_used > cache_allocated_size_.load(std::memory_order_relaxed)) {
    Cache::Handle* handle = nullptr;
    return_status = cache_->Insert(GetNextCacheKey(), nullptr, kSizeDummyEntry,
                                   GetNoopDeleterForRole<R>(), &handle);

    if (return_status != Status::OK()) {
      return return_status;
    }

    dummy_handles_.push_back(handle);
    cache_allocated_size_ += kSizeDummyEntry;
  }
  return return_status;
}

Status CacheReservationManager::DecreaseCacheReservation(
    std::size_t new_mem_used) {
  Status return_status = Status::OK();

  // Decrease to the smallest multiple of kSizeDummyEntry that is greater than
  // or equal to new_mem_used We do addition instead of new_mem_used <=
  // cache_allocated_size_.load(std::memory_order_relaxed) - kSizeDummyEntry to
  // avoid underflow of size_t when cache_allocated_size_ = 0
  while (new_mem_used + kSizeDummyEntry <=
         cache_allocated_size_.load(std::memory_order_relaxed)) {
    assert(!dummy_handles_.empty());
    auto* handle = dummy_handles_.back();
    cache_->Release(handle, true);
    dummy_handles_.pop_back();
    cache_allocated_size_ -= kSizeDummyEntry;
  }
  return return_status;
}

std::size_t CacheReservationManager::GetTotalReservedCacheSize() {
  return cache_allocated_size_.load(std::memory_order_relaxed);
}

std::size_t CacheReservationManager::GetTotalMemoryUsed() {
  return memory_used_;
}

Slice CacheReservationManager::GetNextCacheKey() {
  // Calling this function will have the side-effect of changing the
  // underlying cache_key_ that is shared among other keys generated from this
  // fucntion. Therefore please make sure the previous keys are saved/copied
  // before calling this function.
  cache_key_ = CacheKey::CreateUniqueForCacheLifetime(cache_.get());
  return cache_key_.AsSlice();
}

template <CacheEntryRole R>
Cache::DeleterFn CacheReservationManager::TEST_GetNoopDeleterForRole() {
  return GetNoopDeleterForRole<R>();
}

template Cache::DeleterFn CacheReservationManager::TEST_GetNoopDeleterForRole<
    CacheEntryRole::kFilterConstruction>();

template <CacheEntryRole R>
CacheReservationHandle<R>::CacheReservationHandle(
    std::size_t incremental_memory_used,
    std::shared_ptr<CacheReservationManager> cache_res_mgr)
    : incremental_memory_used_(incremental_memory_used) {
  assert(cache_res_mgr != nullptr);
  cache_res_mgr_ = cache_res_mgr;
}

template <CacheEntryRole R>
CacheReservationHandle<R>::~CacheReservationHandle() {
  assert(cache_res_mgr_ != nullptr);
  assert(cache_res_mgr_->GetTotalMemoryUsed() >= incremental_memory_used_);

  Status s = cache_res_mgr_->UpdateCacheReservation<R>(
      cache_res_mgr_->GetTotalMemoryUsed() - incremental_memory_used_);
  s.PermitUncheckedError();
}

// Explicitly instantiate templates for "CacheEntryRole" values we use.
// This makes it possible to keep the template definitions in the .cc file.
template class CacheReservationHandle<CacheEntryRole::kMisc>;
template class CacheReservationHandle<CacheEntryRole::kFilterConstruction>;
}  // namespace ROCKSDB_NAMESPACE

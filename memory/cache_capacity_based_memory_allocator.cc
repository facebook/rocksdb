//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "memory/cache_capacity_based_memory_allocator.h"

#include "cache/cache_reservation_manager.h"
#include "port/malloc.h"

namespace ROCKSDB_NAMESPACE {
CacheCapacityBasedMemoryAllocator::CacheCapacityBasedMemoryAllocator(
    std::shared_ptr<Cache> cache)
    : memory_allocated_(0) {
  assert(cache);
  cache_res_mngr_.reset(new CacheReservationManager(cache));
}

template <CacheEntryRole R>
void CacheCapacityBasedMemoryAllocator::Allocate(size_t size, Status* s) {
  Status cache_res_status = Status::OK();
  size_t usable_size = size;

  std::lock_guard<std::mutex> lock(cache_res_mu_);
  cache_res_status = cache_res_mngr_->UpdateCacheReservation<R>(
      memory_allocated_ + usable_size);

  if (cache_res_status.ok()) {
    cache_res_status = Status::OK();
  } else {
    usable_size = 0;
    Status reset_cache_res_status =
        cache_res_mngr_->UpdateCacheReservation<R>(memory_allocated_);
    reset_cache_res_status.PermitUncheckedError();

    cache_res_status = Status::MemoryLimit(
        "Reach the memory limit based on cache capacity for memory allocation");
  }

  memory_allocated_ += usable_size;
  *s = cache_res_status;
}

template <CacheEntryRole R>
void CacheCapacityBasedMemoryAllocator::Deallocate(size_t size) {
  size_t usable_size = size;

  std::lock_guard<std::mutex> lock(cache_res_mu_);
  Status cache_res_status = cache_res_mngr_->UpdateCacheReservation<R>(
      memory_allocated_ >= usable_size ? memory_allocated_ - usable_size : 0);
  cache_res_status.PermitUncheckedError();
  memory_allocated_ -= usable_size;
}

// Explicitly instantiate templates for "CacheEntryRole" values we use.
// This makes it possible to keep the template definitions in the .cc file.
template void CacheCapacityBasedMemoryAllocator::Allocate<
    CacheEntryRole::kBlockBasedTableReader>(size_t size, Status* s);
template void CacheCapacityBasedMemoryAllocator::Deallocate<
    CacheEntryRole::kBlockBasedTableReader>(size_t size);
}  // namespace ROCKSDB_NAMESPACE

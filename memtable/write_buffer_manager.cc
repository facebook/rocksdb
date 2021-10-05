//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "rocksdb/write_buffer_manager.h"

#include "cache/cache_entry_roles.h"
#include "cache/cache_reservation_manager.h"
#include "db/db_impl/db_impl.h"
#include "rocksdb/status.h"
#include "util/coding.h"

namespace ROCKSDB_NAMESPACE {
WriteBufferManager::WriteBufferManager(size_t _buffer_size,
                                       std::shared_ptr<Cache> cache,
                                       bool allow_stall)
    : buffer_size_(_buffer_size),
      mutable_limit_(buffer_size_ * 7 / 8),
      memory_used_(0),
      memory_active_(0),
      cache_rev_mng_(nullptr),
      allow_stall_(allow_stall),
      stall_active_(false) {
#ifndef ROCKSDB_LITE
  if (cache) {
    // Memtable's memory usage tends to fluctuate frequently
    // therefore we set delayed_decrease = true to save some dummy entry
    // insertion on memory increase right after memory decrease
    cache_rev_mng_.reset(
        new CacheReservationManager(cache, true /* delayed_decrease */));
  }
#else
  (void)cache;
#endif  // ROCKSDB_LITE
}

WriteBufferManager::~WriteBufferManager() = default;

std::size_t WriteBufferManager::dummy_entries_in_cache_usage() const {
  if (cache_rev_mng_ != nullptr) {
    return cache_rev_mng_->GetTotalReservedCacheSize();
  } else {
    return 0;
  }
}

void WriteBufferManager::ReserveMem(size_t mem) {
  if (cache_rev_mng_ != nullptr) {
    ReserveMemWithCache(mem);
  } else if (enabled()) {
    memory_used_.fetch_add(mem, std::memory_order_relaxed);
  }
  if (enabled()) {
    memory_active_.fetch_add(mem, std::memory_order_relaxed);
  }
}

// Should only be called from write thread
void WriteBufferManager::ReserveMemWithCache(size_t mem) {
#ifndef ROCKSDB_LITE
  assert(cache_rev_mng_ != nullptr);
  // Use a mutex to protect various data structures. Can be optimized to a
  // lock-free solution if it ends up with a performance bottleneck.
  std::lock_guard<std::mutex> lock(cache_rev_mng_mu_);

  size_t new_mem_used = memory_used_.load(std::memory_order_relaxed) + mem;
  memory_used_.store(new_mem_used, std::memory_order_relaxed);
  Status s =
      cache_rev_mng_->UpdateCacheReservation<CacheEntryRole::kWriteBuffer>(
          new_mem_used);

  // We absorb the error since WriteBufferManager is not able to handle
  // this failure properly. Ideallly we should prevent this allocation
  // from happening if this cache reservation fails.
  // [TODO] We'll need to improve it in the future and figure out what to do on
  // error
  s.PermitUncheckedError();
#else
  (void)mem;
#endif  // ROCKSDB_LITE
}

void WriteBufferManager::ScheduleFreeMem(size_t mem) {
  if (enabled()) {
    memory_active_.fetch_sub(mem, std::memory_order_relaxed);
  }
}

void WriteBufferManager::FreeMem(size_t mem) {
  if (cache_rev_mng_ != nullptr) {
    FreeMemWithCache(mem);
  } else if (enabled()) {
    memory_used_.fetch_sub(mem, std::memory_order_relaxed);
  }
  // Check if stall is active and can be ended.
  if (allow_stall_) {
    EndWriteStall();
  }
}

void WriteBufferManager::FreeMemWithCache(size_t mem) {
#ifndef ROCKSDB_LITE
  assert(cache_rev_mng_ != nullptr);
  // Use a mutex to protect various data structures. Can be optimized to a
  // lock-free solution if it ends up with a performance bottleneck.
  std::lock_guard<std::mutex> lock(cache_rev_mng_mu_);
  size_t new_mem_used = memory_used_.load(std::memory_order_relaxed) - mem;
  memory_used_.store(new_mem_used, std::memory_order_relaxed);
  Status s =
      cache_rev_mng_->UpdateCacheReservation<CacheEntryRole::kWriteBuffer>(
          new_mem_used);

  // We absorb the error since WriteBufferManager is not able to handle
  // this failure properly.
  // [TODO] We'll need to improve it in the future and figure out what to do on
  // error
  s.PermitUncheckedError();
#else
  (void)mem;
#endif  // ROCKSDB_LITE
}

void WriteBufferManager::BeginWriteStall(StallInterface* wbm_stall) {
  assert(wbm_stall != nullptr);
  if (wbm_stall) {
    std::unique_lock<std::mutex> lock(mu_);
    queue_.push_back(wbm_stall);
  }
  // In case thread enqueue itself and memory got freed in parallel, end the
  // stall.
  if (!ShouldStall()) {
    EndWriteStall();
  }
}

// Called when memory is freed in FreeMem.
void WriteBufferManager::EndWriteStall() {
  if (enabled() && !IsStallThresholdExceeded()) {
    {
      std::unique_lock<std::mutex> lock(mu_);
      stall_active_.store(false, std::memory_order_relaxed);
      if (queue_.empty()) {
        return;
      }
    }

    // Get the instances from the list and call WBMStallInterface::Signal to
    // change the state to running and unblock the DB instances.
    // Check ShouldStall() incase stall got active by other DBs.
    while (!ShouldStall() && !queue_.empty()) {
      std::unique_lock<std::mutex> lock(mu_);
      StallInterface* wbm_stall = queue_.front();
      queue_.pop_front();
      wbm_stall->Signal();
    }
  }
}

void WriteBufferManager::RemoveDBFromQueue(StallInterface* wbm_stall) {
  assert(wbm_stall != nullptr);
  if (enabled() && allow_stall_) {
    std::unique_lock<std::mutex> lock(mu_);
    queue_.remove(wbm_stall);
    wbm_stall->Signal();
  }
}

}  // namespace ROCKSDB_NAMESPACE

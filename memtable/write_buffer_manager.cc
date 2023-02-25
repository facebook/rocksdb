//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "rocksdb/write_buffer_manager.h"

#include <atomic>
#include <memory>

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
      cache_res_mgr_(nullptr),
      allow_stall_(allow_stall) {
  if (cache) {
    // Memtable's memory usage tends to fluctuate frequently
    // therefore we set delayed_decrease = true to save some dummy entry
    // insertion on memory increase right after memory decrease
    cache_res_mgr_ = std::make_shared<
        CacheReservationManagerImpl<CacheEntryRole::kWriteBuffer>>(
        cache, true /* delayed_decrease */);
  }
}

WriteBufferManager::~WriteBufferManager() {
#ifndef NDEBUG
  std::unique_lock<std::mutex> lock(wbm_mutex_);
  assert(queue_.empty());
#endif
}

std::size_t WriteBufferManager::dummy_entries_in_cache_usage() const {
  if (cache_res_mgr_ != nullptr) {
    return cache_res_mgr_->GetTotalReservedCacheSize();
  } else {
    return 0;
  }
}

void WriteBufferManager::SetBufferSize(size_t new_size) {
  if (new_size == 0) {
    assert(false);
    return;
  }
  std::unique_lock<std::mutex> lock(wbm_mutex_);
  buffer_size_.store(new_size, std::memory_order_relaxed);
  mutable_limit_.store(new_size * 7 / 8, std::memory_order_relaxed);
  MaybeEndWriteStall();
}

void WriteBufferManager::SetAllowStall(bool new_allow_stall) {
  std::unique_lock<std::mutex> lock(wbm_mutex_);
  allow_stall_.store(new_allow_stall, std::memory_order_relaxed);
  MaybeEndWriteStall();
}

bool WriteBufferManager::ShouldFlush() const {
  std::unique_lock<std::mutex> lock(wbm_mutex_);
  if (enabled()) {
    if (mutable_memtable_memory_usage() >
        mutable_limit_.load(std::memory_order_relaxed)) {
      return true;
    }
    size_t local_size = buffer_size();
    if (memory_usage() >= local_size &&
        mutable_memtable_memory_usage() >= local_size / 2) {
      // If the memory exceeds the buffer size, we trigger more aggressive
      // flush. But if already more than half memory is being flushed,
      // triggering more flush may not help. We will hold it instead.
      return true;
    }
  }
  return false;
}

void WriteBufferManager::ReserveMem(size_t mem) {
  std::unique_lock<std::mutex> lock(wbm_mutex_);
  if (cache_res_mgr_ != nullptr) {
    ReserveMemWithCache(mem);
  } else if (enabled()) {
    memory_used_.fetch_add(mem, std::memory_order_relaxed);
  }
  if (enabled()) {
    memory_active_.fetch_add(mem, std::memory_order_relaxed);
  }
}

void WriteBufferManager::ReserveMemWithCache(size_t mem) {
  assert(cache_res_mgr_ != nullptr);

  size_t new_mem_used = memory_used_.load(std::memory_order_relaxed) + mem;
  memory_used_.store(new_mem_used, std::memory_order_relaxed);
  Status s = cache_res_mgr_->UpdateCacheReservation(new_mem_used);

  // We absorb the error since WriteBufferManager is not able to handle
  // this failure properly. Ideallly we should prevent this allocation
  // from happening if this cache charging fails.
  // [TODO] We'll need to improve it in the future and figure out what to do on
  // error
  s.PermitUncheckedError();
}

void WriteBufferManager::ScheduleFreeMem(size_t mem) {
  std::unique_lock<std::mutex> lock(wbm_mutex_);
  if (enabled()) {
    memory_active_.fetch_sub(mem, std::memory_order_relaxed);
  }
}

void WriteBufferManager::FreeMem(size_t mem) {
  std::unique_lock<std::mutex> lock(wbm_mutex_);
  if (cache_res_mgr_ != nullptr) {
    FreeMemWithCache(mem);
  } else if (enabled()) {
    memory_used_.fetch_sub(mem, std::memory_order_relaxed);
  }
  MaybeEndWriteStall();
}

void WriteBufferManager::FreeMemWithCache(size_t mem) {
  assert(cache_res_mgr_ != nullptr);

  size_t new_mem_used = memory_used_.load(std::memory_order_relaxed) - mem;
  memory_used_.store(new_mem_used, std::memory_order_relaxed);
  Status s = cache_res_mgr_->UpdateCacheReservation(new_mem_used);

  // We absorb the error since WriteBufferManager is not able to handle
  // this failure properly.
  // [TODO] We'll need to improve it in the future and figure out what to do on
  // error
  s.PermitUncheckedError();
}

bool WriteBufferManager::MaybeBeginWriteStall(StallInterface* wbm_stall) {
  assert(wbm_stall != nullptr);
  std::list<StallInterface*> new_node = {wbm_stall};
  std::unique_lock<std::mutex> lock(wbm_mutex_);
  if (ShouldStall()) {
    wbm_stall->SetState(StallInterface::State::BLOCKED);
    queue_.splice(queue_.end(), std::move(new_node));
    return true;
  } else {
    return false;
  }
}

void WriteBufferManager::MaybeEndWriteStall() {
  if (ShouldStall()) {
    return;
  }

  std::list<StallInterface*> cleanup;

  // Unblock the writers in the queue.
  for (StallInterface* wbm_stall : queue_) {
    wbm_stall->Signal();
  }
  cleanup = std::move(queue_);
}

void WriteBufferManager::RemoveDBFromQueue(StallInterface* wbm_stall) {
  assert(wbm_stall != nullptr);

  // Deallocate the removed nodes outside of the lock.
  std::list<StallInterface*> cleanup;
  {
    std::unique_lock<std::mutex> lock(wbm_mutex_);
    for (auto it = queue_.begin(); it != queue_.end();) {
      auto next = std::next(it);
      if (*it == wbm_stall) {
        cleanup.splice(cleanup.end(), queue_, std::move(it));
      }
      it = next;
    }
  }
  wbm_stall->Signal();
}
}  // namespace ROCKSDB_NAMESPACE

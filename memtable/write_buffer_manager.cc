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
#include "db/db_impl/db_impl.h"
#include "util/coding.h"

namespace ROCKSDB_NAMESPACE {
#ifndef ROCKSDB_LITE
namespace {
const size_t kSizeDummyEntry = 256 * 1024;
// The key will be longer than keys for blocks in SST files so they won't
// conflict.
const size_t kCacheKeyPrefix = kMaxVarint64Length * 4 + 1;
}  // namespace

struct WriteBufferManager::CacheRep {
  std::shared_ptr<Cache> cache_;
  std::mutex cache_mutex_;
  std::atomic<size_t> cache_allocated_size_;
  // The non-prefix part will be updated according to the ID to use.
  char cache_key_[kCacheKeyPrefix + kMaxVarint64Length];
  uint64_t next_cache_key_id_ = 0;
  std::vector<Cache::Handle*> dummy_handles_;

  explicit CacheRep(std::shared_ptr<Cache> cache)
      : cache_(cache), cache_allocated_size_(0) {
    memset(cache_key_, 0, kCacheKeyPrefix);
    size_t pointer_size = sizeof(const void*);
    assert(pointer_size <= kCacheKeyPrefix);
    memcpy(cache_key_, static_cast<const void*>(this), pointer_size);
  }

  Slice GetNextCacheKey() {
    memset(cache_key_ + kCacheKeyPrefix, 0, kMaxVarint64Length);
    char* end =
        EncodeVarint64(cache_key_ + kCacheKeyPrefix, next_cache_key_id_++);
    return Slice(cache_key_, static_cast<size_t>(end - cache_key_));
  }
};
#else
struct WriteBufferManager::CacheRep {};
#endif  // ROCKSDB_LITE

WriteBufferManager::WriteBufferManager(size_t _buffer_size,
                                       std::shared_ptr<Cache> cache,
                                       bool allow_stall)
    : buffer_size_(_buffer_size),
      mutable_limit_(buffer_size_ * 7 / 8),
      memory_used_(0),
      memory_active_(0),
      dummy_size_(0),
      cache_rep_(nullptr),
      allow_stall_(allow_stall),
      stall_active_(false) {
#ifndef ROCKSDB_LITE
  if (cache) {
    // Construct the cache key using the pointer to this.
    cache_rep_.reset(new CacheRep(cache));
  }
#else
  (void)cache;
#endif  // ROCKSDB_LITE
}

WriteBufferManager::~WriteBufferManager() {
#ifndef ROCKSDB_LITE
  if (cache_rep_) {
    for (auto* handle : cache_rep_->dummy_handles_) {
      if (handle != nullptr) {
        cache_rep_->cache_->Release(handle, true);
      }
    }
  }
#endif  // ROCKSDB_LITE
}

void WriteBufferManager::ReserveMem(size_t mem) {
  if (cache_rep_ != nullptr) {
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
  assert(cache_rep_ != nullptr);
  // Use a mutex to protect various data structures. Can be optimized to a
  // lock-free solution if it ends up with a performance bottleneck.
  std::lock_guard<std::mutex> lock(cache_rep_->cache_mutex_);

  size_t new_mem_used = memory_used_.load(std::memory_order_relaxed) + mem;
  memory_used_.store(new_mem_used, std::memory_order_relaxed);
  while (new_mem_used > cache_rep_->cache_allocated_size_) {
    // Expand size by at least 256KB.
    // Add a dummy record to the cache
    Cache::Handle* handle = nullptr;
    Status s = cache_rep_->cache_->Insert(
        cache_rep_->GetNextCacheKey(), nullptr, kSizeDummyEntry,
        GetNoopDeleterForRole<CacheEntryRole::kWriteBuffer>(), &handle);
    s.PermitUncheckedError();  // TODO: What to do on error?
    // We keep the handle even if insertion fails and a null handle is
    // returned, so that when memory shrinks, we don't release extra
    // entries from cache.
    // Ideallly we should prevent this allocation from happening if
    // this insertion fails. However, the callers to this code path
    // are not able to handle failures properly. We'll need to improve
    // it in the future.
    cache_rep_->dummy_handles_.push_back(handle);
    cache_rep_->cache_allocated_size_ += kSizeDummyEntry;
    dummy_size_.fetch_add(kSizeDummyEntry, std::memory_order_relaxed);
  }
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
  if (cache_rep_ != nullptr) {
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
  assert(cache_rep_ != nullptr);
  // Use a mutex to protect various data structures. Can be optimized to a
  // lock-free solution if it ends up with a performance bottleneck.
  std::lock_guard<std::mutex> lock(cache_rep_->cache_mutex_);
  size_t new_mem_used = memory_used_.load(std::memory_order_relaxed) - mem;
  memory_used_.store(new_mem_used, std::memory_order_relaxed);
  // Gradually shrink memory costed in the block cache if the actual
  // usage is less than 3/4 of what we reserve from the block cache.
  // We do this because:
  // 1. we don't pay the cost of the block cache immediately a memtable is
  //    freed, as block cache insert is expensive;
  // 2. eventually, if we walk away from a temporary memtable size increase,
  //    we make sure shrink the memory costed in block cache over time.
  // In this way, we only shrink costed memory showly even there is enough
  // margin.
  if (new_mem_used < cache_rep_->cache_allocated_size_ / 4 * 3 &&
      cache_rep_->cache_allocated_size_ - kSizeDummyEntry > new_mem_used) {
    assert(!cache_rep_->dummy_handles_.empty());
    auto* handle = cache_rep_->dummy_handles_.back();
    // If insert failed, handle is null so we should not release.
    if (handle != nullptr) {
      cache_rep_->cache_->Release(handle, true);
    }
    cache_rep_->dummy_handles_.pop_back();
    cache_rep_->cache_allocated_size_ -= kSizeDummyEntry;
    dummy_size_.fetch_sub(kSizeDummyEntry, std::memory_order_relaxed);
  }
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

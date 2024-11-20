//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// WriteBufferManager is for managing memory allocation for one or more
// MemTables.

#pragma once

#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <list>
#include <mutex>
#include <vector>
#include <iostream>
#include "rocksdb/cache.h"
#include "rocksdb/tg_thread_local.h"

namespace ROCKSDB_NAMESPACE {

class CacheReservationManager;

// Interface to block and signal DB instances, intended for RocksDB
// internal use only. Each DB instance contains ptr to StallInterface.
class StallInterface {
 public:
  virtual ~StallInterface() {}

  virtual void Block() = 0;

  virtual void Signal() = 0;
};

class WriteBufferManager final {
 public:
  // Parameters:
  // buffer_size: buffer_size = 0 indicates no limit. Memory won't be capped.
  // memory_usage() won't be valid and ShouldFlush() will always return false.
  //
  // cache: if `cache` is provided, we'll put dummy entries in the cache and
  // cost the memory allocated to the cache. It can be used even if buffer_size
  // = 0.
  //
  // allow_stall: if set true, it will enable stalling of writes when
  // memory_usage() exceeds buffer_size. It will wait for flush to complete and
  // memory usage to drop down.
  explicit WriteBufferManager(size_t buffer_size,
                              std::shared_ptr<Cache> cache = {},
                              bool allow_stall = true,
                              size_t num_clients = 16);

  // No copying allowed
  WriteBufferManager(const WriteBufferManager&) = delete;
  WriteBufferManager& operator=(const WriteBufferManager&) = delete;

  ~WriteBufferManager();

  // Returns true if buffer_limit is passed to limit the total memory usage and
  // is greater than 0.
  bool enabled() const { return buffer_size() > 0; }

  // Returns true if pointer to cache is passed.
  bool cost_to_cache() const { return cache_res_mgr_ != nullptr; }

  // Returns the total memory used by memtables.
  // Only valid if enabled()
  size_t memory_usage() const {
    return memory_used_.load(std::memory_order_relaxed);
  }

  // Returns the per-client memory usage.
  size_t per_client_memory_usage(int client_id) const {
    return per_client_memory_used_[client_id].load(std::memory_order_relaxed);
  }

  // Returns the total memory used by active memtables.
  size_t mutable_memtable_memory_usage() const {
    return memory_active_.load(std::memory_order_relaxed);
  }

  size_t dummy_entries_in_cache_usage() const;

  // Returns the buffer_size.
  size_t buffer_size() const {
    return buffer_size_.load(std::memory_order_relaxed);
  }

  size_t num_clients() const {
    return per_client_memory_used_.size();
  }

  // Sets the per-client buffer size (memory usage threshold) for stalling.
  void SetPerClientBufferSize(int client_id, size_t buffer_size);

  // REQUIRED: `new_size` > 0
  void SetBufferSize(size_t new_size) {
    assert(new_size > 0);
    buffer_size_.store(new_size, std::memory_order_relaxed);
    mutable_limit_.store(new_size * 7 / 8, std::memory_order_relaxed);
    // Check if stall is active and can be ended.
    MaybeEndWriteStall();
  }

  void SetAllowStall(bool new_allow_stall) {
    allow_stall_.store(new_allow_stall, std::memory_order_relaxed);
    MaybeEndWriteStall();
  }

  // Below functions should be called by RocksDB internally.

  // Should only be called from write thread
bool ShouldFlush() const {
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


  // Returns true if total memory usage exceeded buffer_size.
  // We stall the writes until memory_usage drops below buffer_size. When the
  // function returns true, the writer thread for the given client will be stalled.
  // Stall is allowed only if user passes allow_stall = true during
  // WriteBufferManager instance creation.
  //
  // Should only be called by RocksDB internally.
  bool ShouldStall(int client_id) const {
    if (client_id < 0) {
      std::cout << "[FAIRDB_LOG] Unaccounted ShouldStall " << client_id << std::endl;
      return false;
    }
    if (!allow_stall_.load(std::memory_order_relaxed) || !enabled()) {
      return false;
    }

  return IsStallActive(client_id) || IsStallThresholdExceeded(client_id);
}

  // Returns true if stall is active for the given client.
  bool IsStallActive(int client_id) const {
    return per_client_stall_active_[client_id].load(std::memory_order_relaxed);
  }

  // Returns true if stalling condition is met for the given client.
  bool IsStallThresholdExceeded(int client_id) const {
    // std::cout << "IsStallThresholdExceeded for " << client_id << ": " << per_client_memory_used_[client_id].load(std::memory_order_relaxed) << " vs " << per_client_buffer_size_[client_id] << std::endl;

    return per_client_memory_used_[client_id].load(std::memory_order_relaxed) >=
           per_client_buffer_size_[client_id];
  }

  void ReserveMem(size_t mem);

  // We are in the process of freeing `mem` bytes, so it is not considered
  // when checking the soft limit.
  void ScheduleFreeMem(size_t mem);

  void FreeMem(size_t mem);

  // Add the DB instance to the per-client queue and block the DB.
  // Should only be called by RocksDB internally.
  void BeginWriteStall(StallInterface* wbm_stall, int client_id);

  // If stall conditions have resolved, remove DB instances from queue and
  // signal them to continue.
  void MaybeEndWriteStall();

  void RemoveDBFromQueue(StallInterface* wbm_stall, int client_id);

 private:
  std::atomic<size_t> buffer_size_;
  std::atomic<size_t> mutable_limit_;
  std::atomic<size_t> memory_used_;
  std::vector<std::atomic<size_t>> per_client_memory_used_;
  std::vector<size_t> per_client_buffer_size_;
  // Memory that hasn't been scheduled to free.
  std::atomic<size_t> memory_active_;
  std::shared_ptr<CacheReservationManager> cache_res_mgr_;
  // Protects cache_res_mgr_
  std::mutex cache_res_mgr_mu_;

  std::vector<std::list<StallInterface*>> per_client_queue_;
  // Protects the per-client queues and stall_active_ flags.
  std::mutex mu_;
  std::atomic<bool> allow_stall_;
  // Value should only be changed by BeginWriteStall() and MaybeEndWriteStall()
  // while holding mu_, but it can be read without a lock.
  std::vector<std::atomic<bool>> per_client_stall_active_;

  void ReserveMemWithCache(size_t mem);
  void FreeMemWithCache(size_t mem);
};

}  // namespace ROCKSDB_NAMESPACE

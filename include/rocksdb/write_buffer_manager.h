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

#include "rocksdb/cache.h"

namespace ROCKSDB_NAMESPACE {
class CacheReservationManager;

// Interface to block and signal DB instances, intended for RocksDB
// internal use only. Each DB instance contains ptr to StallInterface.
class StallInterface {
 public:
  enum State {
    BLOCKED = 0,
    RUNNING,
  };

  virtual ~StallInterface() {}

  virtual void Block() = 0;

  virtual void Signal() = 0;

  virtual void SetState(State /* state */) {}
};

// This class is thread-safe
class WriteBufferManager final {
 public:
  // Parameters:
  // _buffer_size: the memory limit. _buffer_size = 0 indicates no limit where
  // memory won't be capped, memory_usage() won't be valid and ShouldFlush()
  // will always return false.
  //
  // cache_: if `cache` is provided, we'll put dummy entries in the cache and
  // cost the memory allocated to the cache. It can be used even if _buffer_size
  // = 0.
  //
  // allow_stall: if set true, it will enable stalling of writes when
  // memory_usage() exceeds buffer_size. It will wait for flush to complete and
  // memory usage to drop down.
  explicit WriteBufferManager(size_t _buffer_size,
                              std::shared_ptr<Cache> cache = {},
                              bool allow_stall = false);
  // No copying allowed
  WriteBufferManager(const WriteBufferManager&) = delete;
  WriteBufferManager& operator=(const WriteBufferManager&) = delete;

  ~WriteBufferManager();

  // Returns true if memory limit exixts (i.e, buffer size > 0);
  //
  // WARNING: If running without syncronization with `SetBufferSize()`, this
  // function might not return the latest result changed by `SetBufferSize()`
  // but an old result.
  bool enabled() const { return buffer_size() > 0; }

  // Returns true if pointer to cache is passed.
  bool cost_to_cache() const { return cache_res_mgr_ != nullptr; }

  // Returns the total memory used by memtables.  Only valid if enabled()=true
  //
  // WARNING: If running without syncronization with `ReserveMem()/FreeMem()`,
  // this function might not return the latest result changed by these functions
  // but an old result.
  size_t memory_usage() const {
    return memory_used_.load(std::memory_order_relaxed);
  }

  // Returns the total memory used by active memtables.
  //
  // WARNING: If running without syncronization with
  // `ReserveMem()/ScheduleFreeMem()`, this function might not return the latest
  // result changed by these functions but an old result.
  size_t mutable_memtable_memory_usage() const {
    return memory_active_.load(std::memory_order_relaxed);
  }

  // Return the number of dummy entries put in the cache used to cost the memory
  // accounted by this WriteBufferManager to the cache
  //
  // WARNING: If running without syncronization with `ReserveMem()/FreeMem()`,
  // this function might not return the latest result changed by these functions
  // but an old result.
  size_t dummy_entries_in_cache_usage() const;

  // Returns the buffer_size.
  //
  // WARNING: If running without syncronization with `SetBufferSize()`, this
  // function might not return the latest result changed by `SetBufferSize()`
  // but an old result.
  size_t buffer_size() const {
    return buffer_size_.load(std::memory_order_relaxed);
  }

  // REQUIRED: new_size != 0
  void SetBufferSize(size_t new_size);

  void SetAllowStall(bool new_allow_stall);

  // WARNING: Should only be called from write thread
  bool ShouldFlush() const;

  // Returns true if stall conditions are met.
  // Stall conditions: stall is allowed AND memory limit is set (i.e, buffer
  // size > 0) AND total memory usage accounted by this WriteBufferManager
  // exceeds the memory limit.
  //
  // WARNING: Should only be called by RocksDB internally .
  //
  // WARNING: If running without syncronization with any functions that could
  // change the stall conditions above, this function might not return the
  // latest result changed by these functions but an old result.
  bool ShouldStall() const {
    if (allow_stall_.load(std::memory_order_relaxed) && enabled() &&
        IsStallThresholdExceeded()) {
      return true;
    } else {
      return false;
    }
  }

  // WARNING: Should only be called by RocksDB internally.
  void ReserveMem(size_t mem);

  // We are in the process of freeing `mem` bytes, so it is not considered
  // when checking the soft limit.
  //
  // WARNING: Should only be called by RocksDB internally.
  void ScheduleFreeMem(size_t mem);

  // WARNING: Should only be called by RocksDB internally.
  void FreeMem(size_t mem);

  // If stall conditions are met, WriteBufferManager
  // will prepare for write stall (including changing `wbm_stall`'s state
  // to be `State::Blocked`). Otherwise, this function does nothing.
  //
  // WARNING: Should only be called by RocksDB internally.
  void MaybeBeginWriteStall(StallInterface* wbm_stall);

  // WARNING: Should only be called by RocksDB internally.
  void RemoveDBFromQueue(StallInterface* wbm_stall);

 private:
  // If stall conditions have resolved, remove DB instances from queue and
  // signal them to continue.
  //
  // Called when stall conditions (see `ShouldStall()` API) might have been
  // changed
  //
  // REQUIRED: wbm_mutex_ held
  void MaybeEndWriteStall();

  // REQUIRED: wbm_mutex_ held
  bool IsStallThresholdExceeded() const {
    return memory_usage() >= buffer_size_;
  }

  // WARNING:  Should only be called from write thread
  // REQUIRED: wbm_mutex_ held
  void ReserveMemWithCache(size_t mem);

  // REQUIRED: wbm_mutex_ held
  void FreeMemWithCache(size_t mem);

  // Mutex used to protect WriteBufferManager's data variables.
  mutable std::mutex wbm_mutex_;

  std::atomic<size_t> buffer_size_;
  std::atomic<size_t> mutable_limit_;
  std::atomic<size_t> memory_used_;
  // Memory that hasn't been scheduled to free.
  std::atomic<size_t> memory_active_;
  std::shared_ptr<CacheReservationManager> cache_res_mgr_;

  std::list<StallInterface*> queue_;
  std::atomic<bool> allow_stall_;
};
}  // namespace ROCKSDB_NAMESPACE

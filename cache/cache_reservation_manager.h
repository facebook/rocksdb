//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <vector>

#include "cache/cache_entry_roles.h"
#include "cache/cache_key.h"
#include "rocksdb/cache.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "util/coding.h"

namespace ROCKSDB_NAMESPACE {
// CacheReservationManager is for reserving cache space for the memory used
// through inserting/releasing dummy entries in the cache.
//
// This class is NOT thread-safe, except that GetTotalReservedCacheSize()
// can be called without external synchronization.
class CacheReservationManager
    : public std::enable_shared_from_this<CacheReservationManager> {
 public:
  // CacheReservationHandle is for managing the lifetime of a cache reservation
  // for an incremental amount of memory used (i.e, incremental_memory_used)
  template <CacheEntryRole R>
  class CacheReservationHandle {
   public:
    // REQUIRES: cache_res_mgr != nullptr
    explicit CacheReservationHandle(
        std::size_t incremental_memory_used,
        std::shared_ptr<CacheReservationManager> cache_res_mgr);

    ~CacheReservationHandle();

    std::size_t GetIncrementalMemoryUsed() { return incremental_memory_used_; }

   private:
    std::size_t incremental_memory_used_;
    std::shared_ptr<CacheReservationManager> cache_res_mgr_;
  };

  // Construct a CacheReservationManager
  // @param cache The cache where dummy entries are inserted and released for
  // reserving cache space
  // @param delayed_decrease If set true, then dummy entries won't be released
  // immediately when memory usage decreases.
  //                         Instead, it will be released when the memory usage
  //                         decreases to 3/4 of what we have reserved so far.
  //                         This is for saving some future dummy entry
  //                         insertion when memory usage increases are likely to
  //                         happen in the near future.
  //
  // REQUIRED: cache is not nullptr
  explicit CacheReservationManager(std::shared_ptr<Cache> cache,
                                   bool delayed_decrease = false);

  // no copy constructor, copy assignment, move constructor, move assignment
  CacheReservationManager(const CacheReservationManager &) = delete;
  CacheReservationManager &operator=(const CacheReservationManager &) = delete;
  CacheReservationManager(CacheReservationManager &&) = delete;
  CacheReservationManager &operator=(CacheReservationManager &&) = delete;

  ~CacheReservationManager();

  template <CacheEntryRole R>

  // One of the two ways of reserving/releasing cache,
  // see CacheReservationManager::MakeCacheReservation() for the other.
  // Use ONLY one of them to prevent unexpected behavior.
  //
  // Insert and release dummy entries in the cache to
  // match the size of total dummy entries with the least multiple of
  // kSizeDummyEntry greater than or equal to new_mem_used
  //
  // Insert dummy entries if new_memory_used > cache_allocated_size_;
  //
  // Release dummy entries if new_memory_used < cache_allocated_size_
  // (and new_memory_used < cache_allocated_size_ * 3/4
  // when delayed_decrease is set true);
  //
  // Keey dummy entries the same if (1) new_memory_used == cache_allocated_size_
  // or (2) new_memory_used is in the interval of
  // [cache_allocated_size_ * 3/4, cache_allocated_size) when delayed_decrease
  // is set true.
  //
  // @param new_memory_used The number of bytes used by new memory
  //        The most recent new_memoy_used passed in will be returned
  //        in GetTotalMemoryUsed() even when the call return non-ok status.
  //
  //        Since the class is NOT thread-safe, external synchronization on the
  //        order of calling UpdateCacheReservation() is needed if you want
  //        GetTotalMemoryUsed() indeed returns the latest memory used.
  //
  // @return On inserting dummy entries, it returns Status::OK() if all dummy
  //         entry insertions succeed.
  //         Otherwise, it returns the first non-ok status;
  //         On releasing dummy entries, it always returns Status::OK().
  //         On keeping dummy entries the same, it always returns Status::OK().
  Status UpdateCacheReservation(std::size_t new_memory_used);

  // One of the two ways of reserving cache, used with
  // ReleaseCacheReservation().
  // See CacheReservationManager::UpdateCacheReservation()
  // for the other way.
  //
  // Use ONLY one of them to prevent unexpected behavior.
  //
  // Insert dummy entries in the cache for the incremental memory usage
  // to match the size of total dummy entries with the least multiple of
  // kSizeDummyEntry greater than or equal to the total memory used.
  //
  // A CacheReservationHandle is returned as an output parameter.
  // The reserved dummy entries are automatically released on the destruction of
  // this handle, which achieves better RAII per cache reservation.
  //
  // WARNING: Deallocate all the handles of the CacheReservationManager object
  //          before deallocating the object to prevent unexpected behavior.
  //
  // @param incremental_memory_used The number of bytes increased in memory
  //        usage.
  //
  //        Calling GetTotalMemoryUsed() afterward will return the total memory
  //        increased by this number, even when calling MakeCacheReservation()
  //        returns non-ok status.
  //
  //        Since the class is NOT thread-safe, external synchronization in
  //        calling MakeCacheReservation() is needed if you want
  //        GetTotalMemoryUsed() indeed returns the latest memory used.
  //
  // @param handle An pointer to std::unique_ptr<CacheReservationHandle<R>> that
  //        manages the lifetime of the handle and its cache reservation.
  //
  // @return It returns Status::OK() if all dummy
  //         entry insertions succeed.
  //         Otherwise, it returns the first non-ok status;
  //
  // REQUIRES: handle != nullptr
  // REQUIRES: The CacheReservationManager object is NOT managed by
  //           std::unique_ptr as CacheReservationHandle needs to
  //           shares ownership to the CacheReservationManager object.
  template <CacheEntryRole R>
  Status MakeCacheReservation(
      std::size_t incremental_memory_used,
      std::unique_ptr<CacheReservationManager::CacheReservationHandle<R>>
          *handle);

  // One of the two ways of releasing cache reservation, used with
  // MakeCacheReservation(). It's usually called on the destruction of a
  // CacheReservationManager::CacheReservationHandle<R> handle, which achieves
  // better RAII per cache reservation.
  // See CacheReservationManager::UpdateCacheReservation()
  // for the other way.
  //
  // Use ONLY one of them to prevent unexpected behavior.
  //
  // Release dummy entries in the cache for the incremental memory usage
  // to match the size of total dummy entries with the least multiple of
  // kSizeDummyEntry greater than or equal to the total memory used.
  //
  // WARNING: The passed-in incremental_memory_used should be resulted from
  //          calling GetIncrementalMemoryUsed() on some
  //          CacheReservationManager::CacheReservationHandle<R>> object
  //          created by MakeCacheReservation() and hasn't had its cache
  //          reservation released
  //
  // @param incremental_memory_used The number of bytes previously increased in
  //        memory usage.
  //
  //        Calling GetTotalMemoryUsed() afterward will return the total memory
  //        decreased by this number.
  //
  //        Since the class is NOT thread-safe, external synchronization in
  //        calling ReleaseCacheReservation() is needed if you want
  //        GetTotalMemoryUsed() indeed returns the latest memory used.
  //
  // @return It always returns Status::OK()
  template <CacheEntryRole R>
  Status ReleaseCacheReservation(std::size_t incremental_memory_used);

  // Return the size of the cache (which is a multiple of kSizeDummyEntry)
  // successfully reserved by calling UpdateCacheReservation().
  //
  // When UpdateCacheReservation() returns non-ok status,
  // calling GetTotalReservedCacheSize() after that might return a slightly
  // smaller number than the actual reserved cache size due to
  // the returned number will always be a multiple of kSizeDummyEntry
  // and cache full might happen in the middle of inserting a dummy entry.
  std::size_t GetTotalReservedCacheSize();

  // Return the latest total memory used indicated by the most recent call of
  // UpdateCacheReservation(std::size_t new_memory_used);
  std::size_t GetTotalMemoryUsed();

  static constexpr std::size_t GetDummyEntrySize() { return kSizeDummyEntry; }

  // For testing only - it is to help ensure the NoopDeleterForRole<R>
  // accessed from CacheReservationManager and the one accessed from the test
  // are from the same translation units
  template <CacheEntryRole R>
  static Cache::DeleterFn TEST_GetNoopDeleterForRole();

 private:
  static constexpr std::size_t kSizeDummyEntry = 256 * 1024;

  Slice GetNextCacheKey();
  template <CacheEntryRole R>
  Status IncreaseCacheReservation(std::size_t new_mem_used);
  Status DecreaseCacheReservation(std::size_t new_mem_used);

  std::shared_ptr<Cache> cache_;
  bool delayed_decrease_;
  std::atomic<std::size_t> cache_allocated_size_;
  std::size_t memory_used_;
  std::vector<Cache::Handle *> dummy_handles_;
  CacheKey cache_key_;
};

// Same as CacheReservationManager, except that
// it's thread-safe in calling all the APIs excluding construction and
// destruction.
//
// See CacheReservationManager for more API usage.
class CacheReservationManagerThreadSafeWrapper
    : public std::enable_shared_from_this<
          CacheReservationManagerThreadSafeWrapper> {
 public:
  // Same as CacheReservationManager::CacheReservationHandle
  // expect that it's for CacheReservationManagerThreadSafeWrapper
  //
  // See CacheReservationManager::CacheReservationManager for more API usage.
  template <CacheEntryRole R>
  class CacheReservationHandle {
   public:
    // REQUIRES: cache_res_mgr_threadsafe_wrapper != nullptr
    explicit CacheReservationHandle(
        std::size_t incremental_memory_used,
        std::shared_ptr<CacheReservationManagerThreadSafeWrapper>
            cache_res_mgr_threadsafe_wrapper);

    ~CacheReservationHandle();

    std::size_t GetIncrementalMemoryUsed() { return incremental_memory_used_; }

   private:
    std::size_t incremental_memory_used_;
    std::shared_ptr<CacheReservationManagerThreadSafeWrapper>
        cache_res_mgr_threadsafe_wrapper_;
  };

  // REQUIRED: cache is not nullptr
  explicit CacheReservationManagerThreadSafeWrapper(
      std::shared_ptr<Cache> cache, bool delayed_decrease = false);

  // no copy constructor, copy assignment, move constructor, move assignment
  CacheReservationManagerThreadSafeWrapper(
      const CacheReservationManagerThreadSafeWrapper &) = delete;
  CacheReservationManagerThreadSafeWrapper &operator=(
      const CacheReservationManagerThreadSafeWrapper &) = delete;
  CacheReservationManagerThreadSafeWrapper(
      CacheReservationManagerThreadSafeWrapper &&) = delete;
  CacheReservationManagerThreadSafeWrapper &operator=(
      CacheReservationManagerThreadSafeWrapper &&) = delete;

  ~CacheReservationManagerThreadSafeWrapper() {}

  template <CacheEntryRole R>
  Status UpdateCacheReservation(std::size_t new_memory_used);

  template <CacheEntryRole R>
  Status MakeCacheReservation(
      std::size_t incremental_memory_used,
      std::unique_ptr<
          CacheReservationManagerThreadSafeWrapper::CacheReservationHandle<R>>
          *handle);

  template <CacheEntryRole R>
  Status ReleaseCacheReservation(std::size_t incremental_memory_used);

  std::size_t GetTotalReservedCacheSize();

  std::size_t GetTotalMemoryUsed();

 private:
  std::mutex cache_res_mgr_mu_;
  std::shared_ptr<CacheReservationManager> cache_res_mgr_;
};
}  // namespace ROCKSDB_NAMESPACE

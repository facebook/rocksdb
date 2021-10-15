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
#include <vector>

#include "cache/cache_entry_roles.h"
#include "rocksdb/cache.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "table/block_based/block_based_table_reader.h"
#include "util/coding.h"

namespace ROCKSDB_NAMESPACE {

// CacheReservationManager is for reserving cache space for the memory used
// through inserting/releasing dummy entries in the cache.
// This class is not thread-safe.
class CacheReservationManager {
 public:
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
  explicit CacheReservationManager(std::shared_ptr<Cache> cache,
                                   bool delayed_decrease = false);

  // no copy constructor, copy assignment, move constructor, move assignment
  CacheReservationManager(const CacheReservationManager &) = delete;
  CacheReservationManager &operator=(const CacheReservationManager &) = delete;
  CacheReservationManager(CacheReservationManager &&) = delete;
  CacheReservationManager &operator=(CacheReservationManager &&) = delete;

  ~CacheReservationManager();

  template <CacheEntryRole R>

  // Insert and release dummy entries in the cache to
  // match the size of total dummy entries with the smallest multiple of
  // kSizeDummyEntry that is greater than or equal to new_mem_used
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
  // @return On inserting dummy entries, it returns Status::OK() if all dummy
  // entry insertions succeed. Otherwise, it returns the first non-ok status;
  // On releasing dummy entries, it always returns Status::OK().
  // On keeping dummy entries the same, it always returns Status::OK().
  Status UpdateCacheReservation(std::size_t new_memory_used);
  std::size_t GetTotalReservedCacheSize();

 private:
  static constexpr std::size_t kSizeDummyEntry = 256 * 1024;
  // The key will be longer than keys for blocks in SST files so they won't
  // conflict.
  static const std::size_t kCacheKeyPrefixSize =
      BlockBasedTable::kMaxCacheKeyPrefixSize + kMaxVarint64Length;

  Slice GetNextCacheKey();
  template <CacheEntryRole R>
  Status IncreaseCacheReservation(std::size_t new_mem_used);
  Status DecreaseCacheReservation(std::size_t new_mem_used);

  std::shared_ptr<Cache> cache_;
  bool delayed_decrease_;
  std::atomic<std::size_t> cache_allocated_size_;
  std::vector<Cache::Handle *> dummy_handles_;
  std::uint64_t next_cache_key_id_ = 0;
  // The non-prefix part will be updated according to the ID to use.
  char cache_key_[kCacheKeyPrefixSize + kMaxVarint64Length];
};
}  // namespace ROCKSDB_NAMESPACE
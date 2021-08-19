//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// CacheReservationManager is for reserving cache space through
// inserting/removing dummy entries based on the memory used.
// This class is not thread-safe.
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

class CacheReservationManager {
 public:
  explicit CacheReservationManager(std::shared_ptr<Cache> cache,
                                   bool delayed_decrease = false);

  // no copy constructor, copy assignment, move constructor, move assignment
  CacheReservationManager(const CacheReservationManager &) = delete;
  CacheReservationManager &operator=(const CacheReservationManager &) = delete;
  CacheReservationManager(const CacheReservationManager &&) = delete;
  CacheReservationManager &operator=(const CacheReservationManager &&) = delete;

  ~CacheReservationManager();

  template <CacheEntryRole R>

  // Insert and remove dummy entries in the cache based on latest used memory.
  //
  // Insert dummy entries into cache_ if new_memory_used > cache_allocated_size_
  // and the difference is greater than one dummy entry size;
  // Release dummy entries from cache_ if new_memory_used <
  // cache_allocated_size_ and the difference is greater that one dummy entry
  // size (and new_memory_used < cache_allocated_size_ * 3 / 4 if
  // delayed_decrease = true);
  // Otherwise, keep dummy entries in the cache_ the same.
  //
  // For dummy entry insertion, it returns Status::OK() if all dummy entry
  // insertion succeed. Otherwise, it returns the first non-ok status;
  // For dummy entry release, it always return Status::OK().
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
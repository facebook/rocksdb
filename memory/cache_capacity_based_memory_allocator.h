//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once

#include <mutex>

#include "cache/cache_reservation_manager.h"
#include "utilities/memory_allocators.h"

namespace ROCKSDB_NAMESPACE {

// CacheCapacityBasedMemoryAllocator is for allocating memory based on available
// cache space by charging the memory to the cache.
// For now, it does not support the actual memory allocation and only the
// memory charging.
//
// This class is thread-safe
class CacheCapacityBasedMemoryAllocator : public BaseMemoryAllocator {
 public:
  static const char *kClassName() {
    return "CacheCapacityBasedMemoryAllocator";
  }
  const char *Name() const override { return kClassName(); }

  // REQUIRED: cache is not nullptr
  explicit CacheCapacityBasedMemoryAllocator(std::shared_ptr<Cache> cache);
  ~CacheCapacityBasedMemoryAllocator(){};

  // no copy constructor, copy assignment, move constructor, move assignment
  CacheCapacityBasedMemoryAllocator(const CacheCapacityBasedMemoryAllocator &) =
      delete;
  CacheCapacityBasedMemoryAllocator &operator=(
      const CacheCapacityBasedMemoryAllocator &) = delete;
  CacheCapacityBasedMemoryAllocator(CacheCapacityBasedMemoryAllocator &&) =
      delete;
  CacheCapacityBasedMemoryAllocator &operator=(
      CacheCapacityBasedMemoryAllocator &&) = delete;

  using BaseMemoryAllocator::Allocate;
  void *Allocate(size_t /* size */) override {
    assert(false);
    return nullptr;
  }

  // If charging `size` amount of memory to underlying cache
  // succeeds, (*status) be set to Status::OK(). Otherwise,
  // it will be set to Status::MemoryLimit().
  template <CacheEntryRole R>
  void Allocate(size_t size, Status *status);

  using BaseMemoryAllocator::Deallocate;
  void Deallocate(void * /*p */) override { assert(false); }

  // Release `size` amount of memory charges from underlying cache.
  // This operation always succeeds.
  template <CacheEntryRole R>
  void Deallocate(size_t size);

 private:
  std::mutex cache_res_mu_;
  std::unique_ptr<CacheReservationManager> cache_res_mngr_;
  std::size_t memory_allocated_;
};
}  // namespace ROCKSDB_NAMESPACE

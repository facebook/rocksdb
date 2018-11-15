//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <atomic>

#include "rocksdb/memory_allocator.h"

#if defined(ROCKSDB_JEMALLOC) && defined(ROCKSDB_PLATFORM_POSIX)

#include <jemalloc/jemalloc.h>
#include <sys/mman.h>

#if (JEMALLOC_VERSION_MAJOR >= 5) && defined(MADV_DONTDUMP)
#define ROCKSDB_JEMALLOC_NODUMP_ALLOCATOR

namespace rocksdb {

class JemallocNodumpAllocator : public MemoryAllocator {
 public:
  JemallocNodumpAllocator(unsigned arena_index, int flags,
                          std::unique_ptr<extent_hooks_t>&& hooks);
  ~JemallocNodumpAllocator();

  const char* Name() const override { return "JemallocNodumpAllocator"; }
  void* Allocate(size_t size) override;
  void Deallocate(void* p) override;
  size_t UsableSize(void* p, size_t allocation_size) const override;

 private:
  friend Status NewJemallocNodumpAllocator(
      std::shared_ptr<MemoryAllocator>* memory_allocator);

  // Custom alloc hook to replace jemalloc default alloc.
  static void* Alloc(extent_hooks_t* extent, void* new_addr, size_t size,
                     size_t alignment, bool* zero, bool* commit,
                     unsigned arena_ind);

  // A function pointer to jemalloc default alloc. Use atomic to make sure
  // NewJemallocNodumpAllocator is thread-safe.
  //
  // Hack: original_alloc_ needs to be static for Alloc() to access it.
  // alloc needs to be static to pass to jemalloc as function pointer.
  static std::atomic<extent_alloc_t*> original_alloc_;

  unsigned arena_index_;
  int flags_;
  const std::unique_ptr<extent_hooks_t> hooks_;
};

}  // namespace rocksdb
#endif  // (JEMALLOC_VERSION_MAJOR >= 5) && MADV_DONTDUMP
#endif  // ROCKSDB_JEMALLOC && ROCKSDB_PLATFORM_POSIX

//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <atomic>
#include <vector>

#include "port/jemalloc_helper.h"
#include "port/likely.h"
#include "port/port.h"
#include "rocksdb/memory_allocator.h"
#include "rocksdb/utilities/options_type.h"
#include "util/random.h"
#include "util/thread_local.h"
#include "utilities/memory_allocators.h"

#if defined(ROCKSDB_JEMALLOC) && defined(ROCKSDB_PLATFORM_POSIX)

#include <sys/mman.h>

#if (JEMALLOC_VERSION_MAJOR >= 5) && defined(MADV_DONTDUMP)
#define ROCKSDB_JEMALLOC_NODUMP_ALLOCATOR
#endif  // (JEMALLOC_VERSION_MAJOR >= 5) && MADV_DONTDUMP
#endif  // ROCKSDB_JEMALLOC && ROCKSDB_PLATFORM_POSIX

namespace ROCKSDB_NAMESPACE {

// Allocation requests are randomly sharded across `2^kLog2NumArenas` arenas to
// reduce contention on per-arena mutexes.
template <size_t kLog2NumArenas>
class JemallocNodumpAllocator : public BaseMemoryAllocator {
  static const size_t kNumArenas = 1 << kLog2NumArenas;

 public:
  explicit JemallocNodumpAllocator(JemallocAllocatorOptions& options);
#ifdef ROCKSDB_JEMALLOC_NODUMP_ALLOCATOR
  ~JemallocNodumpAllocator();
#endif  // ROCKSDB_JEMALLOC_NODUMP_ALLOCATOR

  static const char* kClassName() {
    if (kNumArenas == 1) {
      // Omit the number of arenas suffix for compatibility.
      return "JemallocNodumpAllocator";
    }
    static const std::string name =
        "JemallocNodumpAllocator" + std::to_string(kNumArenas);
    return name.c_str();
  }

  const char* Name() const override { return kClassName(); }
  static bool IsSupported() {
    std::string unused;
    return IsSupported(&unused);
  }
  static bool IsSupported(std::string* why);
  bool IsMutable() const { return !init_; }

  Status PrepareOptions(const ConfigOptions& config_options) override;

#ifdef ROCKSDB_JEMALLOC_NODUMP_ALLOCATOR
  void* Allocate(size_t size) override;
  void Deallocate(void* p) override;
  size_t UsableSize(void* p, size_t allocation_size) const override;
#endif  // ROCKSDB_JEMALLOC_NODUMP_ALLOCATOR

 private:
#ifdef ROCKSDB_JEMALLOC_NODUMP_ALLOCATOR
  Status InitializeArenas();

  friend Status NewJemallocNodumpAllocator(
      JemallocAllocatorOptions& options,
      std::shared_ptr<MemoryAllocator>* memory_allocator);

  // Custom alloc hook to replace jemalloc default alloc.
  static void* Alloc(extent_hooks_t* extent, void* new_addr, size_t size,
                     size_t alignment, bool* zero, bool* commit,
                     unsigned arena_ind);

  // Destroy arena on destruction of the allocator, or on failure.
  static Status DestroyArena(unsigned arena_index);

  // Destroy tcache on destruction of the allocator, or thread exit.
  static void DestroyThreadSpecificCache(void* ptr);

  // Get or create tcache. Return flag suitable to use with `mallocx`:
  // either MALLOCX_TCACHE_NONE or MALLOCX_TCACHE(tc).
  int GetThreadSpecificCache(size_t size);
#endif  // ROCKSDB_JEMALLOC_NODUMP_ALLOCATOR
  JemallocAllocatorOptions options_;

#ifdef ROCKSDB_JEMALLOC_NODUMP_ALLOCATOR
  // A function pointer to jemalloc default alloc. Use atomic to make sure
  // NewJemallocNodumpAllocator is thread-safe.
  //
  // Hack: original_alloc_ needs to be static for Alloc() to access it.
  // alloc needs to be static to pass to jemalloc as function pointer. We can
  // use a single process-wide value as long as we assume that any newly created
  // arena has the same original value in its `alloc` function pointer.
  static std::atomic<extent_alloc_t*> original_alloc_;

  // Custom hooks has to outlive corresponding arena.
  std::array<std::unique_ptr<extent_hooks_t>, kNumArenas> per_arena_hooks_;

  // Hold thread-local tcache index.
  ThreadLocalPtr tcache_;

  std::array<unsigned, kNumArenas> arena_indexes_ = {0};
#endif  // ROCKSDB_JEMALLOC_NODUMP_ALLOCATOR

  bool init_ = false;
};

}  // namespace ROCKSDB_NAMESPACE

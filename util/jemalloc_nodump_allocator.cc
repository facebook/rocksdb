//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "util/jemalloc_nodump_allocator.h"

#include <string>
#include <thread>

#include "port/likely.h"
#include "port/port.h"
#include "util/string_util.h"

namespace rocksdb {
namespace jemalloc {

#ifdef ROCKSDB_JEMALLOC_NODUMP_ALLOCATOR

std::atomic<extent_alloc_t*> JemallocNodumpAllocator::original_alloc_{nullptr};

JemallocNodumpAllocator::JemallocNodumpAllocator(
    PerCPUArena per_cpu_arena, unsigned num_cpus,
    std::vector<std::unique_ptr<extent_hooks_t>>&& arena_hooks,
    std::vector<unsigned>&& arena_indices)
    : per_cpu_arena_(per_cpu_arena),
      num_cpus_(num_cpus),
      arena_hooks_(std::move(arena_hooks)),
      arena_indices_(std::move(arena_indices)),
      tcache_(&JemallocNodumpAllocator::DestroyThreadSpecificCache) {
  switch (per_cpu_arena) {
    case PerCPUArena::kDisabled:
      assert(arena_indices_.size() == 1);
      break;
    case PerCPUArena::kPerCPU:
      assert(arena_indices_.size() == num_cpus_);
      break;
    case PerCPUArena::kPerPhysicalCPU:
      assert(arena_indices_.size() == (num_cpus_ + 1) / 2);
      break;
  }
  assert(arena_hooks.size() == arena_indices.size());
  for (unsigned __attribute__((__unused__)) arena_index : arena_indices) {
    assert(arena_index != 0);
  }
}

int JemallocNodumpAllocator::GetThreadSpecificCache() {
  // We always enable tcache. The only corner case is when there are a ton of
  // threads accessing with low frequency, then it could consume a lot of
  // memory (may reach # threads * ~1MB) without bringing too much benefit.
  unsigned* tcache_index = reinterpret_cast<unsigned*>(tcache_.Get());
  if (UNLIKELY(tcache_index == nullptr)) {
    // Instantiate tcache.
    tcache_index = new unsigned(0);
    size_t tcache_index_size = sizeof(unsigned);
    int ret =
        mallctl("tcache.create", tcache_index, &tcache_index_size, nullptr, 0);
    if (ret != 0) {
      // No good way to expose the error. Silently disable tcache.
      delete tcache_index;
      return MALLOCX_TCACHE_NONE;
    }
    tcache_.Reset(static_cast<void*>(tcache_index));
  }
  return MALLOCX_TCACHE(*tcache_index);
}

void* JemallocNodumpAllocator::Allocate(size_t size) {
  // Obtain tcache.
  int tcache_flag = GetThreadSpecificCache();

  // Choose arena.
  unsigned arena_id = 0;
  if (per_cpu_arena_ != PerCPUArena::kDisabled) {
    int cpu_id = port::PhysicalCoreID();
    if (cpu_id < 0) {
      // Failed to get CPU id.
      arena_id = 0;
    } else if (per_cpu_arena_ == PerCPUArena::kPerCPU ||
               static_cast<unsigned>(cpu_id) < (num_cpus_ + 1) / 2) {
      arena_id = static_cast<unsigned>(cpu_id);
    } else {
      arena_id = static_cast<unsigned>(cpu_id) - (num_cpus_ + 1) / 2;
    }
  }
  assert(arena_id < arena_indices_.size());
  assert(arena_indices_[arena_id] != 0);

  int flags = MALLOCX_ARENA(arena_indices_[arena_id]) | tcache_flag;
  return mallocx(size, flags);
}

void JemallocNodumpAllocator::Deallocate(void* p) {
  // Obtain tcache.
  int tcache_flag = GetThreadSpecificCache();
  // No need to pass arena index to dallocx(). Jemalloc will find arena index
  // from its own metadata.
  dallocx(p, tcache_flag);
}

void* JemallocNodumpAllocator::Alloc(extent_hooks_t* extent, void* new_addr,
                                     size_t size, size_t alignment, bool* zero,
                                     bool* commit, unsigned arena_ind) {
  extent_alloc_t* original_alloc =
      original_alloc_.load(std::memory_order_relaxed);
  assert(original_alloc != nullptr);
  void* result = original_alloc(extent, new_addr, size, alignment, zero, commit,
                                arena_ind);
  if (result != nullptr) {
    int ret = madvise(result, size, MADV_DONTDUMP);
    if (ret != 0) {
      fprintf(
          stderr,
          "JemallocNodumpAllocator failed to set MADV_DONTDUMP, error code: %d",
          ret);
      assert(false);
    }
  }
  return result;
}

Status JemallocNodumpAllocator::DestroyArena(unsigned arena_index) {
  assert(arena_index != 0);
  std::string key = "arena." + ToString(arena_index) + ".destroy";
  int ret = mallctl(key.c_str(), nullptr, 0, nullptr, 0);
  if (ret != 0) {
    return Status::Incomplete("Failed to destroy jemalloc arena, error code: " +
                              ToString(ret));
  }
  return Status::OK();
}

void JemallocNodumpAllocator::DestroyThreadSpecificCache(void* ptr) {
  assert(ptr != nullptr);
  unsigned* tcache_index = static_cast<unsigned*>(ptr);
  size_t tcache_index_size = sizeof(unsigned);
  int ret __attribute__((__unused__)) =
      mallctl("tcache.destroy", tcache_index, &tcache_index_size, nullptr, 0);
  // Silently ignore error.
  assert(ret == 0);
  delete tcache_index;
}

JemallocNodumpAllocator::~JemallocNodumpAllocator() {
  // Destroy tcache before destroying arena.
  autovector<void*> tcache_list;
  tcache_.Scrape(&tcache_list, nullptr);
  for (void* tcache_index : tcache_list) {
    DestroyThreadSpecificCache(tcache_index);
  }
  // Destroy arena.
  for (unsigned arena_id = 0; arena_id < arena_indices_.size(); arena_id++) {
    Status s = DestroyArena(arena_indices_[arena_id]);
    if (!s.ok()) {
      fprintf(stderr, "%s\n", s.ToString().c_str());
    }
  }
}

size_t JemallocNodumpAllocator::UsableSize(void* p,
                                           size_t /*allocation_size*/) const {
  return malloc_usable_size(static_cast<void*>(p));
}
#endif  // ROCKSDB_JEMALLOC_NODUMP_ALLOCATOR

Status NewJemallocNodumpAllocator(
    const jemalloc::JemallocAllocatorOptions& options,
    std::shared_ptr<MemoryAllocator>* memory_allocator) {
#ifndef ROCKSDB_JEMALLOC_NODUMP_ALLOCATOR
  (void)options;
  *memory_allocator = nullptr;
  return Status::NotSupported(
      "JemallocNodumpAllocator only available with jemalloc version >= 5 "
      "and MADV_DONTDUMP is available.");
#else
  if (memory_allocator == nullptr) {
    return Status::InvalidArgument("memory_allocator must be non-null.");
  }
  unsigned num_cpus = std::thread::hardware_concurrency();
  unsigned num_arenas = 1;
  switch (options.per_cpu_arena) {
    case PerCPUArena::kDisabled:
      num_arenas = 1;
      break;
    case PerCPUArena::kPerCPU:
      num_arenas = num_cpus;
      break;
    case PerCPUArena::kPerPhysicalCPU:
      // We don't verify if num_cpus is multiple of 2 in kPerPhysicalCPU
      // mode. We simply handle the case when it is not.
      num_arenas = (num_cpus + 1) / 2;
      break;
  }
  std::vector<std::unique_ptr<extent_hooks_t>> arena_hooks;
  std::vector<unsigned> arena_indices;
  Status s;
  for (unsigned arena_id = 0; arena_id < num_arenas; arena_id++) {
    // Create arena.
    unsigned arena_index = 0;
    size_t arena_index_size = sizeof(arena_index);
    int ret =
        mallctl("arenas.create", &arena_index, &arena_index_size, nullptr, 0);
    if (ret != 0) {
      s = Status::Incomplete("Failed to create jemalloc arena, error code: " +
                             ToString(ret));
      break;
    }
    assert(arena_index != 0);
    arena_indices.push_back(arena_index);

    // Read existing hooks.
    std::string key = "arena." + ToString(arena_index) + ".extent_hooks";
    extent_hooks_t* hooks;
    size_t hooks_size = sizeof(hooks);
    ret = mallctl(key.c_str(), &hooks, &hooks_size, nullptr, 0);
    if (ret != 0) {
      s = Status::Incomplete("Failed to read existing hooks, error code: " +
                             ToString(ret));
      break;
    }

    // Store existing alloc.
    extent_alloc_t* original_alloc = hooks->alloc;
    extent_alloc_t* expected = nullptr;
    bool success =
        JemallocNodumpAllocator::original_alloc_.compare_exchange_strong(
            expected, original_alloc);
    if (!success && original_alloc != expected) {
      s = Status::Incomplete("Original alloc conflict.");
      break;
    }

    // Set the custom hook.
    std::unique_ptr<extent_hooks_t> new_hooks(new extent_hooks_t(*hooks));
    new_hooks->alloc = &JemallocNodumpAllocator::Alloc;
    extent_hooks_t* hooks_ptr = new_hooks.get();
    ret = mallctl(key.c_str(), nullptr, nullptr, &hooks_ptr, sizeof(hooks_ptr));
    if (ret != 0) {
      s = Status::Incomplete("Failed to set custom hook, error code: " +
                             ToString(ret));
      break;
    }
    arena_hooks.emplace_back(std::move(new_hooks));
  }
  if (s.ok()) {
    // Create cache allocator.
    memory_allocator->reset(new JemallocNodumpAllocator(
        options.per_cpu_arena, num_cpus,
        std::move(arena_hooks), std::move(arena_indices)));
  } else {
    for (unsigned arena_id = 0; arena_id < arena_indices.size(); arena_id++) {
      // Ignore status.
      JemallocNodumpAllocator::DestroyArena(arena_indices[arena_id]);
    }
  }
  return s;
#endif  // ROCKSDB_JEMALLOC_NODUMP_ALLOCATOR
}

}  // namespace jemalloc
}  // namespace rocksdb

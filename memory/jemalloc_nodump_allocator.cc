//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "memory/jemalloc_nodump_allocator.h"

#include <string>
#include <thread>

#include "port/likely.h"
#include "port/port.h"
#include "rocksdb/convenience.h"
#include "rocksdb/utilities/customizable_util.h"
#include "rocksdb/utilities/object_registry.h"
#include "rocksdb/utilities/options_type.h"
#include "util/string_util.h"

namespace ROCKSDB_NAMESPACE {

#ifdef ROCKSDB_JEMALLOC_NODUMP_ALLOCATOR
std::atomic<extent_alloc_t*> JemallocNodumpAllocator::original_alloc_{nullptr};
#endif  // ROCKSDB_JEMALLOC_NODUMP_ALLOCATOR

static std::unordered_map<std::string, OptionTypeInfo> jemalloc_type_info = {
#ifndef ROCKSDB_LITE
    {"limit_tcache_size",
     {offsetof(struct JemallocAllocatorOptions, limit_tcache_size),
      OptionType::kBoolean, OptionVerificationType::kNormal,
      OptionTypeFlags::kNone}},
    {"tcache_size_lower_bound",
     {offsetof(struct JemallocAllocatorOptions, tcache_size_lower_bound),
      OptionType::kSizeT, OptionVerificationType::kNormal,
      OptionTypeFlags::kNone}},
    {"tcache_size_upper_bound",
     {offsetof(struct JemallocAllocatorOptions, tcache_size_upper_bound),
      OptionType::kSizeT, OptionVerificationType::kNormal,
      OptionTypeFlags::kNone}},
#endif  // ROCKSDB_LITE
};
bool JemallocNodumpAllocator::IsSupported(std::string* why) {
#ifndef ROCKSDB_JEMALLOC
  *why = "Not compiled with ROCKSDB_JEMALLOC";
  return false;
#else
  static const std::string unsupported =
      "JemallocNodumpAllocator only available with jemalloc version >= 5 "
      "and MADV_DONTDUMP is available.";
  if (!HasJemalloc()) {
    *why = unsupported;
    return false;
  }
#ifndef ROCKSDB_JEMALLOC_NODUMP_ALLOCATOR
  *why = unsupported;
  return false;
#else
  return true;
#endif  // ROCKSDB_JEMALLOC_NODUMP_ALLOCATOR
#endif  // ROCKSDB_MALLOC
}

JemallocNodumpAllocator::JemallocNodumpAllocator(
    JemallocAllocatorOptions& options)
    : options_(options),
#ifdef ROCKSDB_JEMALLOC_NODUMP_ALLOCATOR
      tcache_(&JemallocNodumpAllocator::DestroyThreadSpecificCache),
#endif  // ROCKSDB_JEMALLOC_NODUMP_ALLOCATOR
      arena_index_(0) {
  RegisterOptions(&options_, &jemalloc_type_info);
}

#ifdef ROCKSDB_JEMALLOC_NODUMP_ALLOCATOR
JemallocNodumpAllocator::~JemallocNodumpAllocator() {
  // Destroy tcache before destroying arena.
  autovector<void*> tcache_list;
  tcache_.Scrape(&tcache_list, nullptr);
  for (void* tcache_index : tcache_list) {
    DestroyThreadSpecificCache(tcache_index);
  }
  if (arena_index_ > 0) {
    // Destroy arena. Silently ignore error.
    Status s = DestroyArena(arena_index_);
    assert(s.ok());
    s.PermitUncheckedError();
  }
}

size_t JemallocNodumpAllocator::UsableSize(void* p,
                                           size_t /*allocation_size*/) const {
  return malloc_usable_size(static_cast<void*>(p));
}

void* JemallocNodumpAllocator::Allocate(size_t size) {
  int tcache_flag = GetThreadSpecificCache(size);
  return mallocx(size, MALLOCX_ARENA(arena_index_) | tcache_flag);
}

void JemallocNodumpAllocator::Deallocate(void* p) {
  // Obtain tcache.
  size_t size = 0;
  if (options_.limit_tcache_size) {
    size = malloc_usable_size(p);
  }
  int tcache_flag = GetThreadSpecificCache(size);
  // No need to pass arena index to dallocx(). Jemalloc will find arena index
  // from its own metadata.
  dallocx(p, tcache_flag);
}

Status JemallocNodumpAllocator::InitializeArenas() {
  // Create arena.
  size_t arena_index_size = sizeof(arena_index_);
  int ret =
      mallctl("arenas.create", &arena_index_, &arena_index_size, nullptr, 0);
  if (ret != 0) {
    return Status::Incomplete("Failed to create jemalloc arena, error code: " +
                              ROCKSDB_NAMESPACE::ToString(ret));
  }
  assert(arena_index_ != 0);

  // Read existing hooks.
  std::string key =
      "arena." + ROCKSDB_NAMESPACE::ToString(arena_index_) + ".extent_hooks";
  extent_hooks_t* hooks;
  size_t hooks_size = sizeof(hooks);
  ret = mallctl(key.c_str(), &hooks, &hooks_size, nullptr, 0);
  if (ret != 0) {
    return Status::Incomplete("Failed to read existing hooks, error code: " +
                              ROCKSDB_NAMESPACE::ToString(ret));
  }

  // Store existing alloc.
  extent_alloc_t* original_alloc = hooks->alloc;
  extent_alloc_t* expected = nullptr;
  bool success =
      JemallocNodumpAllocator::original_alloc_.compare_exchange_strong(
          expected, original_alloc);
  if (!success && original_alloc != expected) {
    return Status::Incomplete("Original alloc conflict.");
  }

  // Set the custom hook.
  arena_hooks_.reset(new extent_hooks_t(*hooks));
  arena_hooks_->alloc = &JemallocNodumpAllocator::Alloc;
  extent_hooks_t* hooks_ptr = arena_hooks_.get();
  ret = mallctl(key.c_str(), nullptr, nullptr, &hooks_ptr, sizeof(hooks_ptr));
  if (ret != 0) {
    return Status::Incomplete("Failed to set custom hook, error code: " +
                              ROCKSDB_NAMESPACE::ToString(ret));
  }
  return Status::OK();
}

#endif  // ROCKSDB_JEMALLOC_NODUMP_ALLOCATOR

Status JemallocNodumpAllocator::PrepareOptions(
    const ConfigOptions& config_options) {
  std::string message;

  if (!IsSupported(&message)) {
    return Status::NotSupported(message);
  } else if (options_.limit_tcache_size &&
             options_.tcache_size_lower_bound >=
                 options_.tcache_size_upper_bound) {
    return Status::InvalidArgument(
        "tcache_size_lower_bound larger or equal to tcache_size_upper_bound.");
  } else if (IsMutable()) {
    Status s = MemoryAllocator::PrepareOptions(config_options);
#ifdef ROCKSDB_JEMALLOC_NODUMP_ALLOCATOR
    if (s.ok()) {
      s = InitializeArenas();
    }
#endif  // ROCKSDB_JEMALLOC_NODUMP_ALLOCATOR
    return s;
  } else {
    // Already prepared
    return Status::OK();
  }
}

#ifdef ROCKSDB_JEMALLOC_NODUMP_ALLOCATOR
int JemallocNodumpAllocator::GetThreadSpecificCache(size_t size) {
  // We always enable tcache. The only corner case is when there are a ton of
  // threads accessing with low frequency, then it could consume a lot of
  // memory (may reach # threads * ~1MB) without bringing too much benefit.
  if (options_.limit_tcache_size && (size <= options_.tcache_size_lower_bound ||
                                     size > options_.tcache_size_upper_bound)) {
    return MALLOCX_TCACHE_NONE;
  }
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
  std::string key =
      "arena." + ROCKSDB_NAMESPACE::ToString(arena_index) + ".destroy";
  int ret = mallctl(key.c_str(), nullptr, 0, nullptr, 0);
  if (ret != 0) {
    return Status::Incomplete("Failed to destroy jemalloc arena, error code: " +
                              ROCKSDB_NAMESPACE::ToString(ret));
  }
  return Status::OK();
}

void JemallocNodumpAllocator::DestroyThreadSpecificCache(void* ptr) {
  assert(ptr != nullptr);
  unsigned* tcache_index = static_cast<unsigned*>(ptr);
  size_t tcache_index_size = sizeof(unsigned);
  int ret __attribute__((__unused__)) =
      mallctl("tcache.destroy", nullptr, 0, tcache_index, tcache_index_size);
  // Silently ignore error.
  assert(ret == 0);
  delete tcache_index;
}

#endif  // ROCKSDB_JEMALLOC_NODUMP_ALLOCATOR

Status NewJemallocNodumpAllocator(
    JemallocAllocatorOptions& options,
    std::shared_ptr<MemoryAllocator>* memory_allocator) {
  if (memory_allocator == nullptr) {
    return Status::InvalidArgument("memory_allocator must be non-null.");
  }
#ifndef ROCKSDB_JEMALLOC
  (void)options;
  return Status::NotSupported("Not compiled with JEMALLOC");
#else
  std::unique_ptr<MemoryAllocator> allocator(
      new JemallocNodumpAllocator(options));
  Status s = allocator->PrepareOptions(ConfigOptions());
  if (s.ok()) {
    memory_allocator->reset(allocator.release());
  }
  return s;
#endif
}
}  // namespace ROCKSDB_NAMESPACE

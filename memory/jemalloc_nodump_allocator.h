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

#ifdef ROCKSDB_JEMALLOC_NODUMP_ALLOCATOR
template <size_t kLog2NumArenas>
std::atomic<extent_alloc_t*>
    JemallocNodumpAllocator<kLog2NumArenas>::original_alloc_{nullptr};
#endif  // ROCKSDB_JEMALLOC_NODUMP_ALLOCATOR

static std::unordered_map<std::string, OptionTypeInfo> jemalloc_type_info = {
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
};

template <size_t kLog2NumArenas>
bool JemallocNodumpAllocator<kLog2NumArenas>::IsSupported(std::string* why) {
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

template <size_t kLog2NumArenas>
JemallocNodumpAllocator<kLog2NumArenas>::JemallocNodumpAllocator(
    JemallocAllocatorOptions& options)
    : options_(options)
#ifdef ROCKSDB_JEMALLOC_NODUMP_ALLOCATOR
      ,
      tcache_(&JemallocNodumpAllocator<
              kLog2NumArenas>::DestroyThreadSpecificCache) {
#else   // ROCKSDB_JEMALLOC_NODUMP_ALLOCATOR
{
#endif  // ROCKSDB_JEMALLOC_NODUMP_ALLOCATOR
  RegisterOptions(&options_, &jemalloc_type_info);
}

#ifdef ROCKSDB_JEMALLOC_NODUMP_ALLOCATOR
template <size_t kLog2NumArenas>
JemallocNodumpAllocator<kLog2NumArenas>::~JemallocNodumpAllocator() {
  // Destroy tcache before destroying arena.
  autovector<void*> tcache_list;
  tcache_.Scrape(&tcache_list, nullptr);
  for (void* tcache_index : tcache_list) {
    DestroyThreadSpecificCache(tcache_index);
  }
  // Destroy created arenas (assumed to have nonzero indexes). Silently ignore
  // errors.
  for (auto arena_index : arena_indexes_) {
    if (arena_index == 0) {
      continue;
    }
    assert(init_);
    Status s = DestroyArena(arena_index);
    assert(s.ok());
    s.PermitUncheckedError();
  }
}

template <size_t kLog2NumArenas>
size_t JemallocNodumpAllocator<kLog2NumArenas>::UsableSize(
    void* p, size_t /*allocation_size*/) const {
  return malloc_usable_size(static_cast<void*>(p));
}

template <size_t kLog2NumArenas>
void* JemallocNodumpAllocator<kLog2NumArenas>::Allocate(size_t size) {
  // `size` is used as a source of entropy to initialize each thread's arena
  // selection.
  thread_local uint32_t tl_rng_seed = static_cast<uint32_t>(size);

  int tcache_flag = GetThreadSpecificCache(size);

  // `tl_rng_seed` is used directly for arena selection and updated afterwards
  // in order to avoid a close data dependency.
  void* ret = mallocx(
      size,
      MALLOCX_ARENA(arena_indexes_[tl_rng_seed & ((1 << kLog2NumArenas) - 1)]) |
          tcache_flag);
  Random rng(tl_rng_seed);
  tl_rng_seed = rng.Next();

  return ret;
}

template <>
inline void* JemallocNodumpAllocator<0>::Allocate(size_t size) {
  int tcache_flag = GetThreadSpecificCache(size);
  return mallocx(size, MALLOCX_ARENA(arena_indexes_[0]) | tcache_flag);
}

template <size_t kLog2NumArenas>
void JemallocNodumpAllocator<kLog2NumArenas>::Deallocate(void* p) {
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

template <size_t kLog2NumArenas>
Status JemallocNodumpAllocator<kLog2NumArenas>::InitializeArenas() {
  assert(!init_);
  init_ = true;

  for (size_t i = 0; i < kNumArenas; i++) {
    // Create arena.
    size_t arena_index_size = sizeof(arena_indexes_[i]);
    int ret = mallctl("arenas.create", &arena_indexes_[i], &arena_index_size,
                      nullptr, 0);
    if (ret != 0) {
      return Status::Incomplete(
          "Failed to create jemalloc arena, error code: " +
          std::to_string(ret));
    }
    assert(arena_indexes_[i] != 0);

    // Read existing hooks.
    std::string key =
        "arena." + std::to_string(arena_indexes_[i]) + ".extent_hooks";
    extent_hooks_t* hooks;
    size_t hooks_size = sizeof(hooks);
    ret = mallctl(key.c_str(), &hooks, &hooks_size, nullptr, 0);
    if (ret != 0) {
      return Status::Incomplete("Failed to read existing hooks, error code: " +
                                std::to_string(ret));
    }

    // Store existing alloc.
    extent_alloc_t* original_alloc = hooks->alloc;
    extent_alloc_t* expected = nullptr;
    bool success = JemallocNodumpAllocator<kLog2NumArenas>::original_alloc_
                       .compare_exchange_strong(expected, original_alloc);
    if (!success && original_alloc != expected) {
      // This could happen if jemalloc creates new arenas with different initial
      // values in their `alloc` function pointers. See `original_alloc_` API
      // doc for more details.
      return Status::Incomplete("Original alloc conflict.");
    }

    // Set the custom hook.
    per_arena_hooks_[i].reset(new extent_hooks_t(*hooks));
    per_arena_hooks_[i]->alloc =
        &JemallocNodumpAllocator<kLog2NumArenas>::Alloc;
    extent_hooks_t* hooks_ptr = per_arena_hooks_[i].get();
    ret = mallctl(key.c_str(), nullptr, nullptr, &hooks_ptr, sizeof(hooks_ptr));
    if (ret != 0) {
      return Status::Incomplete("Failed to set custom hook, error code: " +
                                std::to_string(ret));
    }
  }
  return Status::OK();
}

#endif  // ROCKSDB_JEMALLOC_NODUMP_ALLOCATOR

template <size_t kLog2NumArenas>
Status JemallocNodumpAllocator<kLog2NumArenas>::PrepareOptions(
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
template <size_t kLog2NumArenas>
int JemallocNodumpAllocator<kLog2NumArenas>::GetThreadSpecificCache(
    size_t size) {
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

template <size_t kLog2NumArenas>
void* JemallocNodumpAllocator<kLog2NumArenas>::Alloc(
    extent_hooks_t* extent, void* new_addr, size_t size, size_t alignment,
    bool* zero, bool* commit, unsigned arena_ind) {
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

template <size_t kLog2NumArenas>
Status JemallocNodumpAllocator<kLog2NumArenas>::DestroyArena(
    unsigned arena_index) {
  assert(arena_index != 0);
  std::string key = "arena." + std::to_string(arena_index) + ".destroy";
  int ret = mallctl(key.c_str(), nullptr, 0, nullptr, 0);
  if (ret != 0) {
    return Status::Incomplete("Failed to destroy jemalloc arena, error code: " +
                              std::to_string(ret));
  }
  return Status::OK();
}

template <size_t kLog2NumArenas>
void JemallocNodumpAllocator<kLog2NumArenas>::DestroyThreadSpecificCache(
    void* ptr) {
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
}  // namespace ROCKSDB_NAMESPACE

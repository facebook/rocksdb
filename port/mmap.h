//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#ifdef OS_WIN
#include "port/win/port_win.h"
// ^^^ For proper/safe inclusion of windows.h. Must come first.
#include <memoryapi.h>
#else
#include <sys/mman.h>
#endif  // OS_WIN

#include <cstdint>

#include "rocksdb/rocksdb_namespace.h"

namespace ROCKSDB_NAMESPACE {

// An RAII wrapper for mmaped memory
class MemMapping {
 public:
  static constexpr bool kHugePageSupported =
#if defined(MAP_HUGETLB) || defined(FILE_MAP_LARGE_PAGES)
      true;
#else
      false;
#endif

  // Allocate memory requesting to be backed by huge pages
  static MemMapping AllocateHuge(size_t length);

  // Allocate memory that is only lazily mapped to resident memory and
  // guaranteed to be zero-initialized. Note that some platforms like
  // Linux allow memory over-commit, where only the used portion of memory
  // matters, while other platforms require enough swap space (page file) to
  // back the full mapping.
  static MemMapping AllocateLazyZeroed(size_t length);

  // No copies
  MemMapping(const MemMapping&) = delete;
  MemMapping& operator=(const MemMapping&) = delete;
  // Move
  MemMapping(MemMapping&&) noexcept;
  MemMapping& operator=(MemMapping&&) noexcept;

  // Releases the mapping
  ~MemMapping();

  inline void* Get() const { return addr_; }
  inline size_t Length() const { return length_; }

 private:
  MemMapping() {}

  // The mapped memory, or nullptr on failure / not supported
  void* addr_ = nullptr;
  // The known usable number of bytes starting at that address
  size_t length_ = 0;

#ifdef OS_WIN
  HANDLE page_file_handle_ = NULL;
#endif  // OS_WIN

  static MemMapping AllocateAnonymous(size_t length, bool huge);
};

}  // namespace ROCKSDB_NAMESPACE

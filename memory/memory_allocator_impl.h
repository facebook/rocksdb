//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//

#pragma once

#include <algorithm>

#include "rocksdb/memory_allocator.h"

namespace ROCKSDB_NAMESPACE {

struct CustomDeleter {
  CustomDeleter(MemoryAllocator* a = nullptr) : allocator(a) {}

  void operator()(char* ptr) const {
    if (allocator) {
      allocator->Deallocate(reinterpret_cast<void*>(ptr));
    } else {
      delete[] ptr;
    }
  }

  MemoryAllocator* allocator;
};

using CacheAllocationPtr = std::unique_ptr<char[], CustomDeleter>;

inline CacheAllocationPtr AllocateBlock(size_t size,
                                        MemoryAllocator* allocator) {
  if (allocator) {
    auto block = reinterpret_cast<char*>(allocator->Allocate(size));
    return CacheAllocationPtr(block, allocator);
  }
  return CacheAllocationPtr(new char[size]);
}

inline CacheAllocationPtr AllocateAndCopyBlock(const Slice& data,
                                               MemoryAllocator* allocator) {
  CacheAllocationPtr cap = AllocateBlock(data.size(), allocator);
  std::copy_n(data.data(), data.size(), cap.get());
  return cap;
}

template <class T, class... Params>
T* AllocatorNew(MemoryAllocator* allocator, Params&&... params) {
  if (allocator) {
    auto mem = allocator->Allocate(sizeof(T));
    return new (mem) T(std::forward<Params>(params)...);
  }
  return new T(std::forward<Params>(params)...);
}

}  // namespace ROCKSDB_NAMESPACE

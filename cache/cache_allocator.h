//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include "rocksdb/cache_allocator.h"

#include <memory>

namespace rocksdb {

struct CustomDeleter {
  CustomDeleter(CacheAllocator* a = nullptr) : allocator(a) {}

  void operator()(char* ptr) const {
    if (allocator) {
      allocator->Deallocate(reinterpret_cast<void*>(ptr));
    } else {
      delete[] ptr;
    }
  }

  CacheAllocator* allocator;
};

using CacheAllocationPtr = std::unique_ptr<char[], CustomDeleter>;

inline CacheAllocationPtr AllocateBlock(size_t size,
                                        CacheAllocator* allocator) {
  if (allocator) {
    auto block = reinterpret_cast<char*>(allocator->Allocate(size));
    return CacheAllocationPtr(block, allocator);
  }
  return CacheAllocationPtr(new char[size]);
}

}  // namespace rocksdb

// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

namespace rocksdb {

// MemoryAllocator is an interface that a client can implement to supply custom
// memory allocation and deallocation methods. See rocksdb/cache.h for more
// information.
// All methods should be thread-safe.
class MemoryAllocator {
 public:
  virtual ~MemoryAllocator() = default;

  // Name of the cache allocator, printed in the log
  virtual const char* Name() const = 0;

  // Allocate a block of at least size size
  virtual void* Allocate(size_t size) = 0;
  // Deallocate previously allocated block
  virtual void Deallocate(void* p) = 0;
  // Returns the memory size of the block allocated at p. The default
  // implementation that just returns the original allocation_size is fine.
  virtual size_t UsableSize(void* /*p*/, size_t allocation_size) const {
    // default implementation just returns the allocation size
    return allocation_size;
  }
};

}  // namespace rocksdb

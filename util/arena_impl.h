//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

// ArenaImpl is an implementation of Arena class. For a request of small size,
// it allocates a block with pre-defined block size. For a request of big
// size, it uses malloc to directly get the requested size.

#pragma once
#include <cstddef>
#include <vector>
#include <assert.h>
#include <stdint.h>
#include "rocksdb/arena.h"

namespace rocksdb {

class ArenaImpl : public Arena {
 public:
  explicit ArenaImpl(size_t block_size = kMinBlockSize);
  virtual ~ArenaImpl();

  virtual char* Allocate(size_t bytes);

  virtual char* AllocateAligned(size_t bytes);

  // Returns an estimate of the total memory usage of data allocated
  // by the arena (including space allocated but not yet used for user
  // allocations).
  //
  // TODO: Do we need to exclude space allocated but not used?
  virtual const size_t ApproximateMemoryUsage() {
    return blocks_memory_ + blocks_.capacity() * sizeof(char*);
  }

  virtual const size_t MemoryAllocatedBytes() {
    return blocks_memory_;
  }

 private:
  char* AllocateFallback(size_t bytes);
  char* AllocateNewBlock(size_t block_bytes);

  static const size_t kMinBlockSize = 4096;
  static const size_t kMaxBlockSize = 2 << 30;

  // Number of bytes allocated in one block
  size_t block_size_;

  // Allocation state
  char* alloc_ptr_;
  size_t alloc_bytes_remaining_;

  // Array of new[] allocated memory blocks
  std::vector<char*> blocks_;

  // Bytes of memory in blocks allocated so far
  size_t blocks_memory_;

  // No copying allowed
  ArenaImpl(const ArenaImpl&);
  void operator=(const ArenaImpl&);
};

inline char* ArenaImpl::Allocate(size_t bytes) {
  // The semantics of what to return are a bit messy if we allow
  // 0-byte allocations, so we disallow them here (we don't need
  // them for our internal use).
  assert(bytes > 0);
  if (bytes <= alloc_bytes_remaining_) {
    char* result = alloc_ptr_;
    alloc_ptr_ += bytes;
    alloc_bytes_remaining_ -= bytes;
    return result;
  }
  return AllocateFallback(bytes);
}

}  // namespace rocksdb

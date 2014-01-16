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
  // No copying allowed
  ArenaImpl(const ArenaImpl&) = delete;
  void operator=(const ArenaImpl&) = delete;

  static const size_t kMinBlockSize;
  static const size_t kMaxBlockSize;

  explicit ArenaImpl(size_t block_size = kMinBlockSize);
  virtual ~ArenaImpl();

  virtual char* Allocate(size_t bytes) override;

  virtual char* AllocateAligned(size_t bytes) override;

  // Returns an estimate of the total memory usage of data allocated
  // by the arena (exclude the space allocated but not yet used for future
  // allocations).
  virtual const size_t ApproximateMemoryUsage() {
    return blocks_memory_ + blocks_.capacity() * sizeof(char*) -
           alloc_bytes_remaining_;
  }

  virtual const size_t MemoryAllocatedBytes() override {
    return blocks_memory_;
  }

 private:
  // Number of bytes allocated in one block
  const size_t kBlockSize;
  // Array of new[] allocated memory blocks
  typedef std::vector<char*> Blocks;
  Blocks blocks_;

  // Stats for current active block.
  // For each block, we allocate aligned memory chucks from one end and
  // allocate unaligned memory chucks from the other end. Otherwise the
  // memory waste for alignment will be higher if we allocate both types of
  // memory from one direction.
  char* unaligned_alloc_ptr_ = nullptr;
  char* aligned_alloc_ptr_ = nullptr;
  // How many bytes left in currently active block?
  size_t alloc_bytes_remaining_ = 0;

  char* AllocateFallback(size_t bytes, bool aligned);
  char* AllocateNewBlock(size_t block_bytes);

  // Bytes of memory in blocks allocated so far
  size_t blocks_memory_ = 0;
};

inline char* ArenaImpl::Allocate(size_t bytes) {
  // The semantics of what to return are a bit messy if we allow
  // 0-byte allocations, so we disallow them here (we don't need
  // them for our internal use).
  assert(bytes > 0);
  if (bytes <= alloc_bytes_remaining_) {
    unaligned_alloc_ptr_ -= bytes;
    alloc_bytes_remaining_ -= bytes;
    return unaligned_alloc_ptr_;
  }
  return AllocateFallback(bytes, false /* unaligned */);
}

// check and adjust the block_size so that the return value is
//  1. in the range of [kMinBlockSize, kMaxBlockSize].
//  2. the multiple of align unit.
extern size_t OptimizeBlockSize(size_t block_size);

}  // namespace rocksdb

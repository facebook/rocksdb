//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "util/arena_impl.h"
#include <algorithm>

namespace rocksdb {

const size_t ArenaImpl::kMinBlockSize = 4096;
const size_t ArenaImpl::kMaxBlockSize = 2 << 30;
static const int kAlignUnit = sizeof(void*);

size_t OptimizeBlockSize(size_t block_size) {
  // Make sure block_size is in optimal range
  block_size = std::max(ArenaImpl::kMinBlockSize, block_size);
  block_size = std::min(ArenaImpl::kMaxBlockSize, block_size);

  // make sure block_size is the multiple of kAlignUnit
  if (block_size % kAlignUnit != 0) {
    block_size = (1 + block_size / kAlignUnit) * kAlignUnit;
  }

  return block_size;
}

ArenaImpl::ArenaImpl(size_t block_size)
    : kBlockSize(OptimizeBlockSize(block_size)) {
  assert(kBlockSize >= kMinBlockSize && kBlockSize <= kMaxBlockSize &&
         kBlockSize % kAlignUnit == 0);
}

ArenaImpl::~ArenaImpl() {
  for (const auto& block : blocks_) {
    delete[] block;
  }
}

char* ArenaImpl::AllocateFallback(size_t bytes, bool aligned) {
  if (bytes > kBlockSize / 4) {
    // Object is more than a quarter of our block size.  Allocate it separately
    // to avoid wasting too much space in leftover bytes.
    return AllocateNewBlock(bytes);
  }

  // We waste the remaining space in the current block.
  auto block_head = AllocateNewBlock(kBlockSize);
  alloc_bytes_remaining_ = kBlockSize - bytes;

  if (aligned) {
    aligned_alloc_ptr_ = block_head + bytes;
    unaligned_alloc_ptr_ = block_head + kBlockSize;
    return block_head;
  } else {
    aligned_alloc_ptr_ = block_head;
    unaligned_alloc_ptr_ = block_head + kBlockSize - bytes;
    return unaligned_alloc_ptr_;
  }
}

char* ArenaImpl::AllocateAligned(size_t bytes) {
  assert((kAlignUnit & (kAlignUnit - 1)) ==
         0);  // Pointer size should be a power of 2
  size_t current_mod =
      reinterpret_cast<uintptr_t>(aligned_alloc_ptr_) & (kAlignUnit - 1);
  size_t slop = (current_mod == 0 ? 0 : kAlignUnit - current_mod);
  size_t needed = bytes + slop;
  char* result;
  if (needed <= alloc_bytes_remaining_) {
    result = aligned_alloc_ptr_ + slop;
    aligned_alloc_ptr_ += needed;
    alloc_bytes_remaining_ -= needed;
  } else {
    // AllocateFallback always returned aligned memory
    result = AllocateFallback(bytes, true /* aligned */);
  }
  assert((reinterpret_cast<uintptr_t>(result) & (kAlignUnit - 1)) == 0);
  return result;
}

char* ArenaImpl::AllocateNewBlock(size_t block_bytes) {
  char* block = new char[block_bytes];
  blocks_memory_ += block_bytes;
  blocks_.push_back(block);
  return block;
}

}  // namespace rocksdb

//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "memory/arena.h"
#ifndef OS_WIN
#include <sys/mman.h>
#endif
#include <algorithm>
#include "logging/logging.h"
#include "port/malloc.h"
#include "port/port.h"
#include "rocksdb/env.h"
#include "test_util/sync_point.h"

namespace rocksdb {

// MSVC complains that it is already defined since it is static in the header.
#ifndef _MSC_VER
const size_t Arena::kInlineSize;
#endif

const size_t Arena::kMinBlockSize = 4096;
const size_t Arena::kMaxBlockSize = 2u << 30;
static const int kAlignUnit = alignof(max_align_t);
static const int64_t kMaxSize = 8L << 30;

size_t OptimizeBlockSize(size_t block_size) {
  // Make sure block_size is in optimal range
  block_size = std::max(Arena::kMinBlockSize, block_size);
  block_size = std::min(Arena::kMaxBlockSize, block_size);

  // make sure block_size is the multiple of kAlignUnit
  if (block_size % kAlignUnit != 0) {
    block_size = (1 + block_size / kAlignUnit) * kAlignUnit;
  }

  return block_size;
}

// This is run only once so we don't need to overoptimize.
uint32_t CalcShift(size_t block_size) {
  for (uint32_t shift = 0; shift < 32; ++shift) {
    if ((1u << shift) >= block_size) {
      return shift;
    }
  }
  // We should not have blocks this large
  assert(false);
  return 31;
}

Arena::Arena(size_t block_size, AllocTracker* tracker, size_t huge_page_size)
    : kBlockSize(OptimizeBlockSize(block_size)),
      kBlockShift(CalcShift(std::max(kBlockSize, kInlineSize))),
      tracker_(tracker) {
  assert(kBlockSize >= kMinBlockSize && kBlockSize <= kMaxBlockSize &&
         kBlockSize % kAlignUnit == 0);
  assert(kBlockShift < 32 && kBlockShift > 10);
  TEST_SYNC_POINT_CALLBACK("Arena::Arena:0", const_cast<size_t*>(&kBlockSize));
  alloc_bytes_remaining_ = sizeof(inline_block_);
  blocks_memory_ += alloc_bytes_remaining_;
  aligned_alloc_ptr_ = inline_block_;
  unaligned_alloc_ptr_ = inline_block_ + alloc_bytes_remaining_;
#ifdef MAP_HUGETLB
  hugetlb_size_ = huge_page_size;
  if (hugetlb_size_ && kBlockSize > hugetlb_size_) {
    hugetlb_size_ = ((kBlockSize - 1U) / hugetlb_size_ + 1U) * hugetlb_size_;
  }
#else
  (void)huge_page_size;
#endif
  if (tracker_ != nullptr) {
    tracker_->Allocate(kInlineSize);
  }
  blocks_ = nullptr;
  blocks_capacity_ = 0;
  blocks_size_ = 0;
}

Arena::~Arena() {
  if (tracker_ != nullptr) {
    assert(tracker_->is_freed());
    tracker_->FreeMem();
  }
  for (size_t i = 0; i < blocks_size_; ++i) {
    delete[] blocks_[i];
  }
  for (const auto& block_vector : blocks_to_delete_) {
    delete[] block_vector;
  }

#ifdef MAP_HUGETLB
  for (const auto& mmap_info : huge_blocks_) {
    if (mmap_info.addr_ == nullptr) {
      continue;
    }
    auto ret = munmap(mmap_info.addr_, mmap_info.length_);
    if (ret != 0) {
      // TODO(sdong): Better handling
    }
  }
#endif
}

char* Arena::AllocateFallback(size_t bytes, bool aligned) {
  if (bytes > kBlockSize / 4) {
    ++irregular_block_num;
    // Object is more than a quarter of our block size.  Allocate it separately
    // to avoid wasting too much space in leftover bytes.
    return AllocateNewBlock(bytes);
  }

  // We waste the remaining space in the current block.
  size_t size = 0;
  char* block_head = nullptr;
#ifdef MAP_HUGETLB
  if (hugetlb_size_) {
    size = hugetlb_size_;
    block_head = AllocateFromHugePage(size);
  }
#endif
  if (!block_head) {
    size = kBlockSize;
    block_head = AllocateNewBlock(size);
  }
  alloc_bytes_remaining_ = size - bytes;

  if (aligned) {
    aligned_alloc_ptr_ = block_head + bytes;
    unaligned_alloc_ptr_ = block_head + size;
    return block_head;
  } else {
    aligned_alloc_ptr_ = block_head;
    unaligned_alloc_ptr_ = block_head + size - bytes;
    return unaligned_alloc_ptr_;
  }
}

char* Arena::AllocateFromHugePage(size_t bytes) {
#ifdef MAP_HUGETLB
  if (hugetlb_size_ == 0) {
    return nullptr;
  }
  // Reserve space in `huge_blocks_` before calling `mmap`.
  // Use `emplace_back()` instead of `reserve()` to let std::vector manage its
  // own memory and do fewer reallocations.
  //
  // - If `emplace_back` throws, no memory leaks because we haven't called
  //   `mmap` yet.
  // - If `mmap` throws, no memory leaks because the vector will be cleaned up
  //   via RAII.
  huge_blocks_.emplace_back(nullptr /* addr */, 0 /* length */);

  void* addr = mmap(nullptr, bytes, (PROT_READ | PROT_WRITE),
                    (MAP_PRIVATE | MAP_ANONYMOUS | MAP_HUGETLB), -1, 0);

  if (addr == MAP_FAILED) {
    return nullptr;
  }
  huge_blocks_.back() = MmapInfo(addr, bytes);
  blocks_memory_ += bytes;
  if (tracker_ != nullptr) {
    tracker_->Allocate(bytes);
  }
  return reinterpret_cast<char*>(addr);
#else
  (void)bytes;
  return nullptr;
#endif
}

char* Arena::AllocateAligned(size_t bytes, size_t huge_page_size,
                             Logger* logger, size_t align_unit) {
  // Pointer size should be a power of 2
  assert((align_unit & (align_unit - 1)) == 0);

#ifdef MAP_HUGETLB
  if (huge_page_size > 0 && bytes > 0) {
    // Allocate from a huge page TBL table.
    assert(logger != nullptr);  // logger need to be passed in.
    size_t reserved_size =
        ((bytes - 1U) / huge_page_size + 1U) * huge_page_size;
    assert(reserved_size >= bytes);

    char* addr = AllocateFromHugePage(reserved_size);
    if (addr == nullptr) {
      ROCKS_LOG_WARN(logger,
                     "AllocateAligned fail to allocate huge TLB pages: %s",
                     strerror(errno));
      // fail back to malloc
    } else {
      return addr;
    }
  }
#else
  (void)huge_page_size;
  (void)logger;
#endif

  size_t current_mod =
      reinterpret_cast<uintptr_t>(aligned_alloc_ptr_) & (align_unit - 1);
  size_t slop = (current_mod == 0 ? 0 : align_unit - current_mod);
  size_t needed = bytes + slop;
  char* result;
  if (needed <= alloc_bytes_remaining_) {
    result = aligned_alloc_ptr_ + slop;
    aligned_alloc_ptr_ += needed;
    alloc_bytes_remaining_ -= needed;
  } else {
    // AllocateFallback always returns aligned memory
    result = AllocateFallback(bytes, true /* aligned */);
  }
  assert((reinterpret_cast<uintptr_t>(result) & (align_unit - 1)) == 0);
  return result;
}

uint32_t Arena::GetRef(char* ptr) {
  // We encode the block index in the upper part of the code, and the
  // offset within the block in the lower part (of size kBlockAlign - 3).
  // We save 3 trailing bits as they are always 0 for aligned pointers.
  uint32_t offset;
  uint32_t index;
  if (ptr >= &inline_block_[0] && ptr <= &inline_block_[kInlineSize - 1]) {
    index = 0;
    offset = ptr - &inline_block_[0];
    assert(offset < kInlineSize);
  } else {
    index = blocks_size_;
    while (
        index > 0 &&
        !(ptr >= blocks_[index - 1] && ptr < blocks_[index - 1] + kBlockSize)) {
      --index;
    }
    assert(index > 0);
    offset = ptr - blocks_[index - 1];
    assert(offset < kBlockSize);
  }
  uint32_t ref = (index << (kBlockShift - kRefShift)) + (offset >> kRefShift);
  assert(ConvertRef(ref) == ptr);
  return ref;
}

char* Arena::AllocateNewBlock(size_t block_bytes) {
  if (blocks_size_ >= blocks_capacity_) {
    // Because lock-free reads may reference blocks_, we have to do a little
    // bit of memory management ourselves. Nothing more sophisiticated than
    // what std::vector would do, but we keep the old memory around.
    const uint32_t new_blocks_capacity  = std::max(blocks_capacity_ * 2, 32u);
    Blocks new_blocks = new char*[new_blocks_capacity];
    blocks_to_delete_.push_back(new_blocks);
    if (blocks_ != nullptr) {
      memcpy(new_blocks, blocks_, blocks_capacity_ * sizeof(char*));
    }
    blocks_ = new_blocks;
    blocks_capacity_ = new_blocks_capacity;
  }

  char* block = new char[block_bytes];
  size_t allocated_size;
#ifdef ROCKSDB_MALLOC_USABLE_SIZE
  allocated_size = malloc_usable_size(block);
#ifndef NDEBUG
  // It's hard to predict what malloc_usable_size() returns.
  // A callback can allow users to change the costed size.
  std::pair<size_t*, size_t*> pair(&allocated_size, &block_bytes);
  TEST_SYNC_POINT_CALLBACK("Arena::AllocateNewBlock:0", &pair);
#endif  // NDEBUG
#else
  allocated_size = block_bytes;
#endif  // ROCKSDB_MALLOC_USABLE_SIZE
  blocks_memory_ += allocated_size;
  if (tracker_ != nullptr) {
    tracker_->Allocate(allocated_size);
  }
  blocks_[blocks_size_] = block;
  blocks_size_++;
  return block;
}

}  // namespace rocksdb

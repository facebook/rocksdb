//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/memtable_allocator.h"

#include <assert.h>
#include "rocksdb/write_buffer_manager.h"
#include "util/arena.h"

namespace rocksdb {

MemTableAllocator::MemTableAllocator(Allocator* allocator,
                                     WriteBufferManager* write_buffer_manager)
    : allocator_(allocator),
      write_buffer_manager_(write_buffer_manager),
      bytes_allocated_(0) {}

MemTableAllocator::~MemTableAllocator() { DoneAllocating(); }

char* MemTableAllocator::Allocate(size_t bytes) {
  assert(write_buffer_manager_ != nullptr);
  if (write_buffer_manager_->enabled()) {
    bytes_allocated_.fetch_add(bytes, std::memory_order_relaxed);
    write_buffer_manager_->ReserveMem(bytes);
  }
  return allocator_->Allocate(bytes);
}

char* MemTableAllocator::AllocateAligned(size_t bytes, size_t huge_page_size,
                                         Logger* logger) {
  assert(write_buffer_manager_ != nullptr);
  if (write_buffer_manager_->enabled()) {
    bytes_allocated_.fetch_add(bytes, std::memory_order_relaxed);
    write_buffer_manager_->ReserveMem(bytes);
  }
  return allocator_->AllocateAligned(bytes, huge_page_size, logger);
}

void MemTableAllocator::DoneAllocating() {
  if (write_buffer_manager_ != nullptr) {
    if (write_buffer_manager_->enabled()) {
      write_buffer_manager_->FreeMem(
          bytes_allocated_.load(std::memory_order_relaxed));
    } else {
      assert(bytes_allocated_.load(std::memory_order_relaxed) == 0);
    }
    write_buffer_manager_ = nullptr;
  }
}

size_t MemTableAllocator::BlockSize() const { return allocator_->BlockSize(); }

}  // namespace rocksdb

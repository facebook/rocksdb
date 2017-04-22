//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// This is used by the MemTable to allocate write buffer memory. It connects
// to WriteBufferManager so we can track and enforce overall write buffer
// limits.

#pragma once

#include <atomic>
#include "rocksdb/write_buffer_manager.h"
#include "util/allocator.h"

namespace rocksdb {

class Logger;

class MemTableAllocator : public Allocator {
 public:
  explicit MemTableAllocator(Allocator* allocator,
                             WriteBufferManager* write_buffer_manager);
  ~MemTableAllocator();

  // Allocator interface
  char* Allocate(size_t bytes) override;
  char* AllocateAligned(size_t bytes, size_t huge_page_size = 0,
                        Logger* logger = nullptr) override;
  size_t BlockSize() const override;

  // Call when we're finished allocating memory so we can free it from
  // the write buffer's limit.
  void DoneAllocating();

 private:
  Allocator* allocator_;
  WriteBufferManager* write_buffer_manager_;
  std::atomic<size_t> bytes_allocated_;

  // No copying allowed
  MemTableAllocator(const MemTableAllocator&);
  void operator=(const MemTableAllocator&);
};

}  // namespace rocksdb

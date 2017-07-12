//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//  This source code is also licensed under the GPLv2 license found in the
//  COPYING file in the root directory of this source tree.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// Abstract interface for allocating memory in blocks. This memory is freed
// when the allocator object is destroyed. See the Arena class for more info.

#pragma once
#include <cerrno>
#include <cstddef>
#include "rocksdb/write_buffer_manager.h"

namespace rocksdb {

class Logger;

class Allocator {
 public:
  virtual ~Allocator() {}

  virtual char* Allocate(size_t bytes) = 0;
  virtual char* AllocateAligned(size_t bytes, size_t huge_page_size = 0,
                                Logger* logger = nullptr) = 0;

  virtual size_t BlockSize() const = 0;
};

class AllocTracker {
 public:
  explicit AllocTracker(WriteBufferManager* write_buffer_manager);
  ~AllocTracker();
  void Allocate(size_t bytes);
  // Call when we're finished allocating memory so we can free it from
  // the write buffer's limit.
  void DoneAllocating();

  void FreeMem();

  bool is_freed() const { return write_buffer_manager_ == nullptr || freed_; }

 private:
  WriteBufferManager* write_buffer_manager_;
  std::atomic<size_t> bytes_allocated_;
  bool done_allocating_;
  bool freed_;

  // No copying allowed
  AllocTracker(const AllocTracker&);
  void operator=(const AllocTracker&);
};

}  // namespace rocksdb

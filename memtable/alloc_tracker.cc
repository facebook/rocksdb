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

#include <assert.h>
#include "rocksdb/write_buffer_manager.h"
#include "util/allocator.h"
#include "util/arena.h"

namespace rocksdb {

AllocTracker::AllocTracker(WriteBufferManager* write_buffer_manager)
    : write_buffer_manager_(write_buffer_manager),
      bytes_allocated_(0),
      done_allocating_(false),
      freed_(false) {}

AllocTracker::~AllocTracker() { FreeMem(); }

void AllocTracker::Allocate(size_t bytes) {
  assert(write_buffer_manager_ != nullptr);
  if (write_buffer_manager_->enabled()) {
    bytes_allocated_.fetch_add(bytes, std::memory_order_relaxed);
    write_buffer_manager_->ReserveMem(bytes);
  }
}

void AllocTracker::DoneAllocating() {
  if (write_buffer_manager_ != nullptr && !done_allocating_) {
    if (write_buffer_manager_->enabled()) {
      write_buffer_manager_->ScheduleFreeMem(
          bytes_allocated_.load(std::memory_order_relaxed));
    } else {
      assert(bytes_allocated_.load(std::memory_order_relaxed) == 0);
    }
    done_allocating_ = true;
  }
}

void AllocTracker::FreeMem() {
  if (!done_allocating_) {
    DoneAllocating();
  }
  if (write_buffer_manager_ != nullptr && !freed_) {
    if (write_buffer_manager_->enabled()) {
      write_buffer_manager_->FreeMem(
          bytes_allocated_.load(std::memory_order_relaxed));
    } else {
      assert(bytes_allocated_.load(std::memory_order_relaxed) == 0);
    }
    freed_ = true;
  }
}
}  // namespace rocksdb

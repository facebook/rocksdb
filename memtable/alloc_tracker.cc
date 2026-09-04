//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <cassert>

#include "memory/allocator.h"
#include "memory/arena.h"
#include "rocksdb/write_buffer_manager.h"

namespace ROCKSDB_NAMESPACE {

AllocTracker::AllocTracker(WriteBufferManager* write_buffer_manager,
                           FlushInitiator* flush_initiator)
    : write_buffer_manager_(write_buffer_manager),
      flush_initiator_(flush_initiator),
      bytes_allocated_(0),
      bytes_reported_(0),
      flush_initiator_active_(false),
      done_allocating_(false),
      freed_(false) {}

AllocTracker::~AllocTracker() { FreeMem(); }

void AllocTracker::Allocate(size_t bytes) {
  assert(write_buffer_manager_ != nullptr);
  if (write_buffer_manager_->enabled() ||
      write_buffer_manager_->cost_to_cache()) {
    const size_t memtable_mem =
        bytes_allocated_.fetch_add(bytes, std::memory_order_relaxed) + bytes;
    write_buffer_manager_->ReserveMem(bytes);
    if (flush_initiator_ != nullptr &&
        flush_initiator_active_.load(std::memory_order_acquire)) {
      ReportAllocations(memtable_mem);
    }
  }
}

void AllocTracker::ActivateFlushInitiator() {
  if (flush_initiator_ == nullptr) {
    return;
  }
  flush_initiator_active_.store(true, std::memory_order_release);
  ReportAllocations(bytes_allocated_.load(std::memory_order_relaxed));
}

void AllocTracker::DeactivateFlushInitiator() {
  if (flush_initiator_ != nullptr &&
      flush_initiator_active_.exchange(false, std::memory_order_acq_rel)) {
    const size_t mem = bytes_allocated_.load(std::memory_order_relaxed);
    ReportAllocations(mem);
    flush_initiator_->ScheduleFreeMem(
        bytes_reported_.load(std::memory_order_relaxed));
  }
}

void AllocTracker::ReportAllocations(size_t bytes_allocated) {
  size_t bytes_reported = bytes_reported_.load(std::memory_order_relaxed);
  while (bytes_reported < bytes_allocated) {
    if (bytes_reported_.compare_exchange_weak(
            bytes_reported, bytes_allocated, std::memory_order_relaxed)) {
      flush_initiator_->ReserveMem(bytes_allocated - bytes_reported,
                                   bytes_allocated);
      return;
    }
  }
}

void AllocTracker::DoneAllocating() {
  if (write_buffer_manager_ != nullptr && !done_allocating_) {
    if (write_buffer_manager_->enabled() ||
        write_buffer_manager_->cost_to_cache()) {
      const size_t mem = bytes_allocated_.load(std::memory_order_relaxed);
      write_buffer_manager_->ScheduleFreeMem(mem);
      DeactivateFlushInitiator();
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
    if (write_buffer_manager_->enabled() ||
        write_buffer_manager_->cost_to_cache()) {
      write_buffer_manager_->FreeMem(
          bytes_allocated_.load(std::memory_order_relaxed));
    } else {
      assert(bytes_allocated_.load(std::memory_order_relaxed) == 0);
    }
    freed_ = true;
  }
}
}  // namespace ROCKSDB_NAMESPACE

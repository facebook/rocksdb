//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// WriteBufferManager is for managing memory allocation for one or more
// MemTables.

#pragma once

#include <atomic>
#include <cstddef>

namespace rocksdb {

class WriteBufferManager {
 public:
  // _buffer_size = 0 indicates no limit. Memory won't be tracked,
  // memory_usage() won't be valid and ShouldFlush() will always return true.
  explicit WriteBufferManager(size_t _buffer_size)
      : buffer_size_(_buffer_size), memory_used_(0) {}

  ~WriteBufferManager() {}

  bool enabled() const { return buffer_size_ != 0; }

  // Only valid if enabled()
  size_t memory_usage() const {
    return memory_used_.load(std::memory_order_relaxed);
  }
  size_t buffer_size() const { return buffer_size_; }

  // Should only be called from write thread
  bool ShouldFlush() const {
    return enabled() && memory_usage() >= buffer_size();
  }

  // Should only be called from write thread
  void ReserveMem(size_t mem) {
    if (enabled()) {
      memory_used_.fetch_add(mem, std::memory_order_relaxed);
    }
  }
  void FreeMem(size_t mem) {
    if (enabled()) {
      memory_used_.fetch_sub(mem, std::memory_order_relaxed);
    }
  }

 private:
  const size_t buffer_size_;
  std::atomic<size_t> memory_used_;

  // No copying allowed
  WriteBufferManager(const WriteBufferManager&) = delete;
  WriteBufferManager& operator=(const WriteBufferManager&) = delete;
};
}  // namespace rocksdb

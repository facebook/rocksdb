//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
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

// Utility helpers to allocate on the managed allocator if possible
// if not default on heap allocation
namespace allocator_details {
// Destroy, do not free
template<class T>
struct Destructor {
  bool managed_ = false;
  void operator()(T* t) const {
    assert(t != nullptr);
    if (managed_) {
      t->~T();
    } else {
      delete t;
    }
  }
};
}

template<class T, class ...Args> inline
std::unique_ptr<T, allocator_details::Destructor<T>> TryAllocateOnAllocator(
    Allocator* allocator, Args... args) {

  using D = allocator_details::Destructor<T>;
  D d;
  T* v = nullptr;

  if (allocator != nullptr) {
    auto* mem = allocator->AllocateAligned(sizeof(T));
    v = new (mem) T(args..., true);
    d.managed_ = true;
  } else {
    v = new T(args..., false);
  }

  std::unique_ptr<T, D> result(v, d);
  return result;
}


}  // namespace rocksdb

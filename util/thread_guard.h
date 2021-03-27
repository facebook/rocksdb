//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include "port/port.h"
#include "rocksdb/rocksdb_namespace.h"

namespace ROCKSDB_NAMESPACE {

// Resource management object for threads that joins the thread upon
// destruction. Has unique ownership of the thread object, so copying it is not
// allowed, while moving it transfers ownership.
class ThreadGuard {
 public:
  ThreadGuard() = default;

  explicit ThreadGuard(port::Thread&& thread) : thread_(std::move(thread)) {}

  ThreadGuard(const ThreadGuard&) = delete;
  ThreadGuard& operator=(const ThreadGuard&) = delete;

  ThreadGuard(ThreadGuard&&) noexcept = default;
  ThreadGuard& operator=(ThreadGuard&&) noexcept = default;

  ~ThreadGuard() {
    if (thread_.joinable()) {
      thread_.join();
    }
  }

  const port::Thread& GetThread() const { return thread_; }
  port::Thread& GetThread() { return thread_; }

 private:
  port::Thread thread_;
};

}  // namespace ROCKSDB_NAMESPACE

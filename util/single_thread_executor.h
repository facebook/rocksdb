//  Copyright (c) Meta Platforms, Inc. and affiliates.
//
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
#pragma once

#if USE_COROUTINES
#include <atomic>

#include "folly/CPortability.h"
#include "folly/CppAttributes.h"
#include "folly/Executor.h"
#include "util/async_file_reader.h"

namespace ROCKSDB_NAMESPACE {
// Implements a simple executor that runs callback functions in the same
// thread, unlike CPUThreadExecutor which may schedule the callback on
// another thread. Runs in a tight loop calling the queued callbacks,
// and polls for async IO completions when idle. The completions will
// resume suspended coroutines and they get added to the queue, which
// will get picked up by this loop.
// Any possibility of deadlock is precluded because the file system
// guarantees that async IO completion callbacks will not be scheduled
// to run in this thread or this executor.
class SingleThreadExecutor : public folly::Executor {
 public:
  explicit SingleThreadExecutor(AsyncFileReader& reader)
      : reader_(reader), busy_(false) {}

  void add(folly::Func callback) override {
    auto& q = q_;
    q.push(std::move(callback));
    if (q.size() == 1 && !busy_) {
      while (!q.empty()) {
        q.front()();
        q.pop();

        if (q.empty()) {
          // Prevent recursion, as the Wait may queue resumed coroutines
          busy_ = true;
          reader_.Wait();
          busy_ = false;
        }
      }
    }
  }

 private:
  std::queue<folly::Func> q_;
  AsyncFileReader& reader_;
  bool busy_;
};
}  // namespace ROCKSDB_NAMESPACE
#endif  // USE_COROUTINES

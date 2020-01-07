// Copyright (c) 2011-present, Facebook, Inc. All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <memory>
#include <mutex>
#include <queue>

namespace rocksdb {
template <typename T>
class SafeQueue {
 public:
  SafeQueue() {}

  ~SafeQueue() {}

  bool PopFront(T &ret) {
    if (0 == que_len_.load(std::memory_order_relaxed)) {
      return false;
    }
    std::lock_guard<std::mutex> lock(mu_);
    if (que_.empty()) {
      return false;
    }
    ret = std::move(que_.front());
    que_.pop_front();
    que_len_.fetch_sub(1, std::memory_order_relaxed);
    return true;
  }

  void PushBack(T &&v) {
    std::lock_guard<std::mutex> lock(mu_);
    que_.push_back(v);
    que_len_.fetch_add(1, std::memory_order_relaxed);
  }

 private:
  std::deque<T> que_;
  std::mutex mu_;
  std::atomic<uint64_t> que_len_;
};
}  // namespace rocksdb

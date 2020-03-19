// Copyright (c) 2011-present, Facebook, Inc. All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <deque>
#include <memory>
#include <mutex>
#include <queue>

namespace rocksdb {
class SafeFuncQueue {
 private:
  struct Item {
    std::function<void()> func;
  };

 public:
  SafeFuncQueue() {}

  ~SafeFuncQueue() {}

  bool RunFunc() {
    mu_.lock();
    if (que_.empty()) {
      mu_.unlock();
      return false;
    }
    auto func = std::move(que_.front().func);
    que_.pop_front();
    mu_.unlock();
    func();
    return true;
  }

  void Push(std::function<void()> &&v) {
    std::lock_guard<std::mutex> _guard(mu_);
    que_.emplace_back();
    que_.back().func = std::move(v);
  }

 private:
  std::deque<Item> que_;
  std::mutex mu_;
};

}  // namespace rocksdb

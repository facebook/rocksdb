//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//

#pragma once

#include <functional>
#include <memory>
#include <queue>
#include <unordered_map>
#include <vector>

#include "port/port.h"
#include "rocksdb/env.h"
#include "util/mutexlock.h"

namespace ROCKSDB_NAMESPACE {

// A Timer class to handle repeated work.
//
// A single timer instance can handle multiple functions via a single thread.
// It is better to leave long running work to a dedicated thread pool.
//
// Timer can be started by calling `Start()`, and ended by calling `Shutdown()`.
// A new function can be added by called `Add` with a unique function name.
// Many functions can be added. 
//
// Impl Details:
// A heap is used to keep track of when the next timer goes off.
// A map from a function name to the function is keep track of all the functions.
class Timer {
 public:
  Timer(Env* env)
      : env_(env),
        mutex_(env),
        cond_var_(&mutex_),
        running_(false) {
  }

  ~Timer() {}

  void Add(std::function<void()> fn,
           const std::string& fn_name,
           uint64_t start_after_us,
           uint64_t repeat_every_us) {
    std::unique_ptr<FunctionInfo> fn_info(new FunctionInfo(
        std::move(fn), 
        fn_name,
        env_->NowMicros() + start_after_us,
        repeat_every_us));
    
    MutexLock l(&mutex_);
    map_[fn_name] = fn_info.get();
    heap_.push(std::move(fn_info));
  }

  void Cancel(std::string& fn_name) {
    MutexLock l(&mutex_);
    auto it = map_.find(fn_name);
    if (it != map_.end()) {
      it->second->Cancel();
      map_.erase(it);
    }
  }

  void CancelAll() {
    MutexLock l(&mutex_);
    CancelAllWithLock();
  }

  // Start the Timer
  bool Start() {
    MutexLock l(&mutex_);
    if (running_) {
      return false;
    }

    thread_ = std::thread([&] { this->Run(); });
    running_ = true;
    return true;
  }

  // Shutdown the Timer
  bool Shutdown() {
    {
      MutexLock l(&mutex_);
      if (!running_) {
        return false;
      }
      CancelAllWithLock();
      running_ = false;
      cond_var_.SignalAll();
    }

    thread_.join();
    return true;
  }

 private:

  void Run() {
    MutexLock l(&mutex_);

    while (running_) {
      if (heap_.empty()) {
        // wait
        cond_var_.Wait();
        continue;
      }

      FunctionInfo *current_fn = heap_.top().get();

      if (!current_fn->IsValid()) {
        map_.erase(current_fn->name);
        heap_.pop();
        continue; 
      }

      if (current_fn->next_run_time_us <= env_->NowMicros()) {
        // execute the function
        current_fn->fn();
        heap_.pop();
        if (current_fn->repeat_every_us > 0) {
          current_fn->next_run_time_us = env_->NowMicros() +
              current_fn->repeat_every_us;
          heap_.push(std::unique_ptr<FunctionInfo>(current_fn));
        }
      } else {
        cond_var_.TimedWait(current_fn->next_run_time_us);
      }
    }
  }

  void CancelAllWithLock() {
   for (auto it = map_.cbegin(); it != map_.cend(); ++it) {
      it->second->Cancel();
      map_.erase(it);
    }
  }

  // A wrapper around std::function to keep track when it should run next
  // and at what frequency.
  struct FunctionInfo {
    std::function<void()> fn;
    std::string name;
    uint64_t next_run_time_us;
    uint64_t repeat_every_us;

    FunctionInfo(std::function<void()>&& _fn,
               const std::string& _name,
               const uint64_t _next_run_time_us,
               uint64_t _repeat_every_us)
      : fn(std::move(_fn)),
        name(_name),
        next_run_time_us(_next_run_time_us),
        repeat_every_us(_repeat_every_us) {}

    void Cancel() {
      fn = {};
    }

    bool IsValid() {
      return bool(fn);
    }
  };

  struct fnHeapOrdering {
    bool operator()(const std::unique_ptr<FunctionInfo>& f1,
                    const std::unique_ptr<FunctionInfo>& f2) {
      return f1->next_run_time_us > f2->next_run_time_us;
    }
  };

  Env* const env_;
  port::Mutex mutex_;
  port::CondVar cond_var_;
  port::Thread thread_;
  bool running_;

  std::priority_queue<std::unique_ptr<FunctionInfo>,
                      std::vector<std::unique_ptr<FunctionInfo>>,
                      fnHeapOrdering> heap_;

  std::unordered_map<std::string, FunctionInfo*> map_;
};

}  // namespace ROCKSDB_NAMESPACE
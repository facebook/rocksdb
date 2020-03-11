//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//

#pragma once

#include <functional>
#include <queue>
#include <vector>

#include "port/port.h"
#include "rocksdb/env.h"
#include "util/mutexlock.h"

namespace ROCKSDB_NAMESPACE {

class FunctionScheduler {
 public:
  FunctionScheduler(Env* env)
      : env_(env),
        mutex_(env),
        cond_var_(&mutex_),
        running_(false) {
  }

  ~FunctionScheduler() {}

  void Schedule(std::function<void()> fn,
                const std::string& fnName,
                uint64_t delay_us,
                uint64_t repeat_us) {
    RepeatFunc repeatFn(std::move(fn), fnName, env_->NowMicros() + delay_us, repeat_us);
    heap_.push(repeatFn);
  }

  bool cancelFunction(/*std::string& fnName*/) {
    return true;
  }

  // Start the Scheduler
  bool Start() {
    MutexLock l(&mutex_);
    if (running_) {
      return false;
    }

    thread_ = std::thread([&] { this->Run(); });
    running_ = true;
    return true;
  }

  // Stop the Scheduler
  bool Stop() {
    {
      MutexLock l(&mutex_);
      if (!running_) {
        return false;
      }
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

      // 1. check the top el.
      // 2. Run or wait
      //   a. if it is time to run this func, run it.
      //   b. else wait.
      auto topEl = heap_.top();
      if (topEl.next_run_time < env_->NowMicros()) {
        // execute the function
        topEl.fn();
        heap_.pop();
        if (topEl.repeat_us > 0) {
          topEl.next_run_time = env_->NowMicros() + topEl.repeat_us;
          heap_.push(topEl);
        }
      } else {
        cond_var_.TimedWait(topEl.next_run_time);
      }
    }
  }

  struct RepeatFunc {
    std::function<void()> fn;
    std::string fnName;
    uint64_t next_run_time; // microseconds
    uint64_t repeat_us;

    RepeatFunc(std::function<void()>&& _fn,
        const std::string& _fnName,
        const uint64_t _next_run_time,
        uint64_t _repeat_us) 
      : fn(std::move(_fn)),
        fnName(_fnName),
        next_run_time(_next_run_time),
        repeat_us(_repeat_us) {}
  };

  // auto cmp = [](const RepeatFunc& f1, const RepeatFunc& f2) { return f1.next_run_time > f2.next_run_time;}
  struct fnHeapOrdering {
    bool operator()(const RepeatFunc& f1, const RepeatFunc& f2) {
      return f1.next_run_time > f2.next_run_time;
    }
  };

  Env* const env_;
  port::Mutex mutex_;
  port::CondVar cond_var_;
  port::Thread thread_;
  bool running_;

  std::priority_queue<RepeatFunc, std::vector<RepeatFunc>, fnHeapOrdering> heap_;
};

}  // namespace ROCKSDB_NAMESPACE
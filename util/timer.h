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
#include <utility>
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
// Work (in terms of a `void function`) can be scheduled by calling `Add` with
// a unique function name and de-scheduled by calling `Cancel`.
// Many functions can be added.
//
// Impl Details:
// A heap is used to keep track of when the next timer goes off.
// A map from a function name to the function keeps track of all the functions.
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
    heap_.push(fn_info.get());
    map_.emplace(std::make_pair(fn_name, std::move(fn_info)));
  }

  void Cancel(const std::string& fn_name) {
    MutexLock l(&mutex_);

    auto it = map_.find(fn_name);
    if (it != map_.end()) {
      if (it->second) {
        it->second->Cancel();
      }
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

    thread_.reset(new port::Thread(&Timer::Run, this));
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

    if (thread_) {
      thread_->join();
    }
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

      FunctionInfo* current_fn = heap_.top();

      if (!current_fn->IsValid()) {
        heap_.pop();
        map_.erase(current_fn->name);
        continue;
      }

      if (current_fn->next_run_time_us <= env_->NowMicros()) {
        // Execute the work
        current_fn->fn();

        // Remove the work from the heap once it is done executing.
        // Note that we are just removing the pointer from the heap. Its
        // memory is still managed in the map (as it holds a unique ptr).
        // So current_fn is still a valid ptr.
        heap_.pop();

        if (current_fn->repeat_every_us > 0) {
          current_fn->next_run_time_us = env_->NowMicros() +
              current_fn->repeat_every_us;

          // Schedule new work into the heap with new time.
          heap_.push(current_fn);
        }
      } else {
        cond_var_.TimedWait(current_fn->next_run_time_us);
      }
    }
  }

  void CancelAllWithLock() {
    if (map_.empty() && heap_.empty()) {
      return;
    }

    while (!heap_.empty()) {
      heap_.pop();
    }

    map_.clear();
  }

  // A wrapper around std::function to keep track when it should run next
  // and at what frequency.
  struct FunctionInfo {
    // the actual work
    std::function<void()> fn;
    // name of the function
    std::string name;
    // when the function should run next
    uint64_t next_run_time_us;
    // repeat interval
    uint64_t repeat_every_us;
    // controls whether this function is valid.
    // A function is valid upon construction and until someone explicitly
    // calls `Cancel()`.
    bool valid;

    FunctionInfo(std::function<void()>&& _fn,
                 const std::string& _name,
                 const uint64_t _next_run_time_us,
                 uint64_t _repeat_every_us)
      : fn(std::move(_fn)),
        name(_name),
        next_run_time_us(_next_run_time_us),
        repeat_every_us(_repeat_every_us),
        valid(true) {}

    void Cancel() {
      valid = false;
    }

    bool IsValid() {
      return valid;
    }
  };

  struct RunTimeOrder {
    bool operator()(const FunctionInfo* f1,
                    const FunctionInfo* f2) {
      return f1->next_run_time_us > f2->next_run_time_us;
    }
  };

  Env* const env_;
  // This mutex controls both the heap_ and the map_. It needs to be held for
  // making any changes in them.
  port::Mutex mutex_;
  port::CondVar cond_var_;
  std::unique_ptr<port::Thread> thread_;
  bool running_;


  std::priority_queue<FunctionInfo*,
                      std::vector<FunctionInfo*>,
                      RunTimeOrder> heap_;

  // In addition to providing a mapping from a function name to a function,
  // it is also responsible for memory management.
  std::unordered_map<std::string, std::unique_ptr<FunctionInfo>> map_;
};

}  // namespace ROCKSDB_NAMESPACE

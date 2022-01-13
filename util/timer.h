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

#include "monitoring/instrumented_mutex.h"
#include "rocksdb/system_clock.h"
#include "test_util/sync_point.h"
#include "util/mutexlock.h"

namespace ROCKSDB_NAMESPACE {

// A Timer class to handle repeated work.
//
// `Start()` and `Shutdown()` are currently not thread-safe. The client must
// serialize calls to these two member functions.
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
  explicit Timer(SystemClock* clock)
      : clock_(clock),
        mutex_(clock),
        cond_var_(&mutex_),
        running_(false),
        executing_task_(false) {}

  ~Timer() { Shutdown(); }

  // Add a new function to run.
  // fn_name has to be identical, otherwise, the new one overrides the existing
  // one, regardless if the function is pending removed (invalid) or not.
  // start_after_us is the initial delay.
  // repeat_every_us is the interval between ending time of the last call and
  // starting time of the next call. For example, repeat_every_us = 2000 and
  // the function takes 1000us to run. If it starts at time [now]us, then it
  // finishes at [now]+1000us, 2nd run starting time will be at [now]+3000us.
  // repeat_every_us == 0 means do not repeat.
  void Add(std::function<void()> fn,
           const std::string& fn_name,
           uint64_t start_after_us,
           uint64_t repeat_every_us) {
    std::unique_ptr<FunctionInfo> fn_info(new FunctionInfo(
        std::move(fn), fn_name, clock_->NowMicros() + start_after_us,
        repeat_every_us));
    {
      InstrumentedMutexLock l(&mutex_);
      auto it = map_.find(fn_name);
      if (it == map_.end()) {
        heap_.push(fn_info.get());
        map_.emplace(std::make_pair(fn_name, std::move(fn_info)));
      } else {
        // If it already exists, overriding it.
        it->second->fn = std::move(fn_info->fn);
        it->second->valid = true;
        it->second->next_run_time_us = clock_->NowMicros() + start_after_us;
        it->second->repeat_every_us = repeat_every_us;
      }
    }
    cond_var_.SignalAll();
  }

  void Cancel(const std::string& fn_name) {
    InstrumentedMutexLock l(&mutex_);

    // Mark the function with fn_name as invalid so that it will not be
    // requeued.
    auto it = map_.find(fn_name);
    if (it != map_.end() && it->second) {
      it->second->Cancel();
    }

    // If the currently running function is fn_name, then we need to wait
    // until it finishes before returning to caller.
    while (!heap_.empty() && executing_task_) {
      FunctionInfo* func_info = heap_.top();
      assert(func_info);
      if (func_info->name == fn_name) {
        WaitForTaskCompleteIfNecessary();
      } else {
        break;
      }
    }
  }

  void CancelAll() {
    InstrumentedMutexLock l(&mutex_);
    CancelAllWithLock();
  }

  // Start the Timer
  bool Start() {
    InstrumentedMutexLock l(&mutex_);
    if (running_) {
      return false;
    }

    running_ = true;
    thread_.reset(new port::Thread(&Timer::Run, this));
    return true;
  }

  // Shutdown the Timer
  bool Shutdown() {
    {
      InstrumentedMutexLock l(&mutex_);
      if (!running_) {
        return false;
      }
      running_ = false;
      CancelAllWithLock();
      cond_var_.SignalAll();
    }

    if (thread_) {
      thread_->join();
    }
    return true;
  }

  bool HasPendingTask() const {
    InstrumentedMutexLock l(&mutex_);
    for (auto it = map_.begin(); it != map_.end(); it++) {
      if (it->second->IsValid()) {
        return true;
      }
    }
    return false;
  }

#ifndef NDEBUG
  // Wait until Timer starting waiting, call the optional callback, then wait
  // for Timer waiting again.
  // Tests can provide a custom Clock object to mock time, and use the callback
  // here to bump current time and trigger Timer. See timer_test for example.
  //
  // Note: only support one caller of this method.
  void TEST_WaitForRun(std::function<void()> callback = nullptr) {
    InstrumentedMutexLock l(&mutex_);
    // It act as a spin lock
    while (executing_task_ ||
           (!heap_.empty() &&
            heap_.top()->next_run_time_us <= clock_->NowMicros())) {
      cond_var_.TimedWait(clock_->NowMicros() + 1000);
    }
    if (callback != nullptr) {
      callback();
    }
    cond_var_.SignalAll();
    do {
      cond_var_.TimedWait(clock_->NowMicros() + 1000);
    } while (executing_task_ ||
             (!heap_.empty() &&
              heap_.top()->next_run_time_us <= clock_->NowMicros()));
  }

  size_t TEST_GetPendingTaskNum() const {
    InstrumentedMutexLock l(&mutex_);
    size_t ret = 0;
    for (auto it = map_.begin(); it != map_.end(); it++) {
      if (it->second->IsValid()) {
        ret++;
      }
    }
    return ret;
  }
#endif  // NDEBUG

 private:

  void Run() {
    InstrumentedMutexLock l(&mutex_);

    while (running_) {
      if (heap_.empty()) {
        // wait
        TEST_SYNC_POINT("Timer::Run::Waiting");
        cond_var_.Wait();
        continue;
      }

      FunctionInfo* current_fn = heap_.top();
      assert(current_fn);

      if (!current_fn->IsValid()) {
        heap_.pop();
        map_.erase(current_fn->name);
        continue;
      }

      if (current_fn->next_run_time_us <= clock_->NowMicros()) {
        // make a copy of the function so it won't be changed after
        // mutex_.unlock.
        std::function<void()> fn = current_fn->fn;
        executing_task_ = true;
        mutex_.Unlock();
        // Execute the work
        fn();
        mutex_.Lock();
        executing_task_ = false;
        cond_var_.SignalAll();

        // Remove the work from the heap once it is done executing.
        // Note that we are just removing the pointer from the heap. Its
        // memory is still managed in the map (as it holds a unique ptr).
        // So current_fn is still a valid ptr.
        heap_.pop();

        // current_fn may be cancelled already.
        if (current_fn->IsValid() && current_fn->repeat_every_us > 0) {
          assert(running_);
          current_fn->next_run_time_us =
              clock_->NowMicros() + current_fn->repeat_every_us;

          // Schedule new work into the heap with new time.
          heap_.push(current_fn);
        }
      } else {
        cond_var_.TimedWait(current_fn->next_run_time_us);
      }
    }
  }

  void CancelAllWithLock() {
    mutex_.AssertHeld();
    if (map_.empty() && heap_.empty()) {
      return;
    }

    // With mutex_ held, set all tasks to invalid so that they will not be
    // re-queued.
    for (auto& elem : map_) {
      auto& func_info = elem.second;
      assert(func_info);
      func_info->Cancel();
    }

    // WaitForTaskCompleteIfNecessary() may release mutex_
    WaitForTaskCompleteIfNecessary();

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

    FunctionInfo(std::function<void()>&& _fn, const std::string& _name,
                 const uint64_t _next_run_time_us, uint64_t _repeat_every_us)
        : fn(std::move(_fn)),
          name(_name),
          next_run_time_us(_next_run_time_us),
          repeat_every_us(_repeat_every_us),
          valid(true) {}

    void Cancel() {
      valid = false;
    }

    bool IsValid() const { return valid; }
  };

  void WaitForTaskCompleteIfNecessary() {
    mutex_.AssertHeld();
    while (executing_task_) {
      TEST_SYNC_POINT("Timer::WaitForTaskCompleteIfNecessary:TaskExecuting");
      cond_var_.Wait();
    }
  }

  struct RunTimeOrder {
    bool operator()(const FunctionInfo* f1,
                    const FunctionInfo* f2) {
      return f1->next_run_time_us > f2->next_run_time_us;
    }
  };

  SystemClock* clock_;
  // This mutex controls both the heap_ and the map_. It needs to be held for
  // making any changes in them.
  mutable InstrumentedMutex mutex_;
  InstrumentedCondVar cond_var_;
  std::unique_ptr<port::Thread> thread_;
  bool running_;
  bool executing_task_;

  std::priority_queue<FunctionInfo*,
                      std::vector<FunctionInfo*>,
                      RunTimeOrder> heap_;

  // In addition to providing a mapping from a function name to a function,
  // it is also responsible for memory management.
  std::unordered_map<std::string, std::unique_ptr<FunctionInfo>> map_;
};

}  // namespace ROCKSDB_NAMESPACE

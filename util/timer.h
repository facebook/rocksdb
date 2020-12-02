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
#include "rocksdb/env.h"
#include "test_util/sync_point.h"
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
  explicit Timer(Env* env)
      : env_(env),
        state_mutex_(env),
        task_complete_condvar_(&state_mutex_),
        no_tasks_mutex_(env),
        no_tasks_condvar_(&no_tasks_mutex_),
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
    std::unique_ptr<FunctionInfo> fn_info(
      new FunctionInfo(std::move(fn), fn_name, 
      env_->NowMicros() + start_after_us, repeat_every_us));
    
    InstrumentedMutexLock l(&state_mutex_);
    
    auto it = map_.find(fn_name);
    if (it == map_.end()) {
      heap_.push(fn_info.get());
      map_.emplace(std::make_pair(fn_name, std::move(fn_info)));
    } else {
      // If it already exists, overriding it.
      it->second->fn = std::move(fn_info->fn);
      it->second->valid = true;
      it->second->next_run_time_us = env_->NowMicros() + start_after_us;
      it->second->repeat_every_us = repeat_every_us;
    }

    // Signal our thread that new task is added.
    InstrumentedMutexLock no_tasks_lock(&no_tasks_mutex_);
    no_tasks_condvar_.SignalAll();
  }

  void Cancel(const std::string& fn_name) {
    InstrumentedMutexLock l(&state_mutex_);

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
    InstrumentedMutexLock l(&state_mutex_);
    CancelAllWithLock();
  }

  // Start the Timer
  bool Start() {
    InstrumentedMutexLock l(&state_mutex_);
    if (running_) {
      return false;
    }
    running_ = true;
    thread_.reset(new port::Thread(&Timer::Run, this));
    return true;
  }

  // If Shutdown() returns true - no background thread is running.
  bool Shutdown() {
    InstrumentedMutexLock l(&state_mutex_);
    if (!running_) {
      return false;
    }
    running_ = false;
    CancelAllWithLock();

    // Thread may wait only for a new task.
    {
      InstrumentedMutexLock no_tasks_lock(&state_mutex_);
      no_tasks_condvar_.SignalAll();
    }

    if (thread_) {
      thread_->join();
    }
    return true;
  }

  bool HasPendingTask() const {
    InstrumentedMutexLock l(&state_mutex_);

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
  // Tests can provide a custom env object to mock time, and use the callback
  // here to bump current time and trigger Timer. See timer_test for example.
  //
  // Note: only support one caller of this method.
  void TEST_WaitForRun(std::function<void()> callback = nullptr) {
    InstrumentedMutexLock l(&state_mutex_);
    // It act as a spin lock
    while (executing_task_ ||
           (!heap_.empty() &&
            heap_.top()->next_run_time_us <= env_->NowMicros())) {
      task_complete_condvar_.TimedWait(env_->NowMicros() + 1000);
    }
    if (callback != nullptr) {
      callback();
    }
    task_complete_condvar_.SignalAll();
    do {
      task_complete_condvar_.TimedWait(env_->NowMicros() + 1000);
    } while (
        executing_task_ ||
        (!heap_.empty() && heap_.top()->next_run_time_us <= env_->NowMicros()));
  }

  size_t TEST_GetPendingTaskNum() const {
    InstrumentedMutexLock l(&state_mutex_);
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
    while (running_) {
      bool is_heap_empty = false;
      {
        InstrumentedMutexLock l(&state_mutex_);
        is_heap_empty = heap_.empty();
      }

      if (is_heap_empty) {
        // Wait for somebody to add new task or request shutdown.
        InstrumentedMutexLock l(&no_tasks_mutex_);
        TEST_SYNC_POINT("Timer::Run::Waiting");
        no_tasks_condvar_.Wait();
        continue;
      }

      FunctionInfo* current_fn = nullptr;
      // Make a copy of the function so it won't be changed while the execution.
      std::function<  void()> func_to_execute{};
      bool can_run_task = true;
      uint64_t now = 0;

      // Choose a function, check the conditions.
      {
        InstrumentedMutexLock l(&state_mutex_);

        current_fn = heap_.top();
        assert(current_fn);

        if (!current_fn->IsValid()) {
          heap_.pop();
          map_.erase(current_fn->name);
          continue;
        }

        now = env_->NowMicros();
        can_run_task = (current_fn->next_run_time_us <= now);
      }

      if (!can_run_task) {
        InstrumentedMutexLock no_tasks_lock(&no_tasks_mutex_);
        // No tasks to execute, we have to wait until the deadline is reached or
        // somebody will add new task (probably we need to execute it firstly)
        no_tasks_condvar_.TimedWait(current_fn->next_run_time_us - now);
        continue;
      }

      {
        InstrumentedMutexLock l(&state_mutex_);
        func_to_execute = current_fn->fn;
        executing_task_ = true;
      }
      
      // Execute the task without any lock.
      func_to_execute();

      // Reshedule the current task.
      {
        InstrumentedMutexLock l(&state_mutex_);
        executing_task_ = false;
        task_complete_condvar_.SignalAll();

        // Remove the work from the heap once it is done executing.
        // Note that we are just removing the pointer from the heap. Its
        // memory is still managed in the map (as it holds a unique ptr).
        // So current_fn is still a valid ptr.
        heap_.pop();

        // current_fn may be cancelled already.
        if (current_fn->IsValid() && current_fn->repeat_every_us > 0) {
          assert(running_);
          current_fn->next_run_time_us = env_->NowMicros() +
              current_fn->repeat_every_us;

          // Schedule new work into the heap with new time.
          heap_.push(current_fn);
        }
      }
    }
  }

  void CancelAllWithLock() {
    state_mutex_.AssertHeld();
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
    state_mutex_.AssertHeld();
    while (executing_task_) {
      TEST_SYNC_POINT("Timer::WaitForTaskCompleteIfNecessary:TaskExecuting");
      task_complete_condvar_.Wait();
    }
  }

  struct RunTimeOrder {
    bool operator()(const FunctionInfo* f1,
                    const FunctionInfo* f2) {
      return f1->next_run_time_us > f2->next_run_time_us;
    }
  };

  Env* const env_;
  // This mutex controls both the heap_ and the map_. It needs to be held for
  // making any changes in them.
  mutable InstrumentedMutex state_mutex_;
  InstrumentedCondVar task_complete_condvar_;

  mutable InstrumentedMutex no_tasks_mutex_;
  InstrumentedCondVar no_tasks_condvar_;

  std::unique_ptr<port::Thread> thread_;
  // This is atomic, because internal thread doesn't acquire a lock.
  std::atomic_bool running_;
  bool executing_task_;

  std::priority_queue<FunctionInfo*,
                      std::vector<FunctionInfo*>,
                      RunTimeOrder> heap_;

  // In addition to providing a mapping from a function name to a function,
  // it is also responsible for memory management.
  std::unordered_map<std::string, std::unique_ptr<FunctionInfo>> map_;
};

}  // namespace ROCKSDB_NAMESPACE

//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#pragma once
#include "rocksdb/env.h"
#include "util/thread_status_util.h"

namespace rocksdb {

class ThreadPool {
 public:
  ThreadPool();
  ~ThreadPool();

  void JoinAllThreads();
  void LowerIOPriority();
  void BGThread(size_t thread_id);
  void WakeUpAllThreads();
  void IncBackgroundThreadsIfNeeded(int num);
  void SetBackgroundThreads(int num);
  void StartBGThreads();
  void Schedule(void (*function)(void* arg1), void* arg, void* tag);
  int UnSchedule(void* arg);

  unsigned int GetQueueLen() const {
    return queue_len_.load(std::memory_order_relaxed);
  }

  void SetHostEnv(Env* env) { env_ = env; }
  Env* GetHostEnv() { return env_; }

  // Return true if there is at least one thread needs to terminate.
  bool HasExcessiveThread() {
    return static_cast<int>(bgthreads_.size()) > total_threads_limit_;
  }

  // Return true iff the current thread is the excessive thread to terminate.
  // Always terminate the running thread that is added last, even if there are
  // more than one thread to terminate.
  bool IsLastExcessiveThread(size_t thread_id) {
    return HasExcessiveThread() && thread_id == bgthreads_.size() - 1;
  }

  // Is one of the threads to terminate.
  bool IsExcessiveThread(size_t thread_id) {
    return static_cast<int>(thread_id) >= total_threads_limit_;
  }

  // Return the thread priority.
  // This would allow its member-thread to know its priority.
  Env::Priority GetThreadPriority() { return priority_; }

  // Set the thread priority.
  void SetThreadPriority(Env::Priority priority) { priority_ = priority; }

  static void PthreadCall(const char* label, int result);

 private:
  // Entry per Schedule() call
  struct BGItem {
    void* arg;
    void (*function)(void*);
    void* tag;
  };
  typedef std::deque<BGItem> BGQueue;

  pthread_mutex_t mu_;
  pthread_cond_t bgsignal_;
  int total_threads_limit_;
  std::vector<pthread_t> bgthreads_;
  BGQueue queue_;
  std::atomic_uint queue_len_;  // Queue length. Used for stats reporting
  bool exit_all_threads_;
  bool low_io_priority_;
  Env::Priority priority_;
  Env* env_;

  void SetBackgroundThreadsInternal(int num, bool allow_reduce);
};

}  // namespace rocksdb

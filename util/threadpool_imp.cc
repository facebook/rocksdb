//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "util/threadpool_imp.h"
#include <algorithm>
#include <atomic>

#ifndef OS_WIN
#  include <unistd.h>
#endif

#ifdef OS_LINUX
#  include <sys/syscall.h>
#endif

#ifdef OS_FREEBSD
#  include <stdlib.h>
#endif


namespace rocksdb {

void ThreadPoolImpl::PthreadCall(const char* label, int result) {
  if (result != 0) {
    fprintf(stderr, "pthread %s: %s\n", label, strerror(result));
    abort();
  }
}

namespace {
#ifdef ROCKSDB_STD_THREADPOOL

struct Lock {
  std::unique_lock<std::mutex> ul_;
  explicit Lock(std::mutex& m) : ul_(m, std::defer_lock) {}
};

using Condition = std::condition_variable;

inline int ThreadPoolMutexLock(Lock& mutex) {
  mutex.ul_.lock();
  return 0;
}

inline
int ConditionWait(Condition& condition, Lock& lock) {
  condition.wait(lock.ul_);
  return 0;
}

inline
int ConditionSignalAll(Condition& condition) {
  condition.notify_all();
  return 0;
}

inline
int ConditionSignal(Condition& condition) {
  condition.notify_one();
  return 0;
}

inline
int MutexUnlock(Lock& mutex) {
  mutex.ul_.unlock();
  return 0;
}

inline
void ThreadJoin(std::thread& thread) {
  thread.join();
}

inline
int ThreadDetach(std::thread& thread) {
  thread.detach();
  return 0;
}

#else

using Lock = pthread_mutex_t&;
using Condition = pthread_cond_t&;

inline int ThreadPoolMutexLock(Lock mutex) {
  return pthread_mutex_lock(&mutex);
}

inline
int ConditionWait(Condition condition, Lock lock) {
  return pthread_cond_wait(&condition, &lock);
}

inline
int ConditionSignalAll(Condition condition) {
  return pthread_cond_broadcast(&condition);
}

inline
int ConditionSignal(Condition condition) {
  return pthread_cond_signal(&condition);
}

inline
int MutexUnlock(Lock mutex) {
  return pthread_mutex_unlock(&mutex);
}

inline
void ThreadJoin(pthread_t& thread) {
  pthread_join(thread, nullptr);
}

inline
int ThreadDetach(pthread_t& thread) {
  return pthread_detach(thread);
}
#endif
}

ThreadPoolImpl::ThreadPoolImpl()
    : total_threads_limit_(1),
      bgthreads_(0),
      queue_(),
      queue_len_(),
      exit_all_threads_(false),
      low_io_priority_(false),
      env_(nullptr) {
#ifndef ROCKSDB_STD_THREADPOOL
  PthreadCall("mutex_init", pthread_mutex_init(&mu_, nullptr));
  PthreadCall("cvar_init", pthread_cond_init(&bgsignal_, nullptr));
#endif
}

ThreadPoolImpl::~ThreadPoolImpl() { assert(bgthreads_.size() == 0U); }

void ThreadPoolImpl::JoinAllThreads() {
  Lock lock(mu_);
  PthreadCall("lock", ThreadPoolMutexLock(lock));
  assert(!exit_all_threads_);
  exit_all_threads_ = true;
  PthreadCall("signalall", ConditionSignalAll(bgsignal_));
  PthreadCall("unlock", MutexUnlock(lock));

  for (auto& th : bgthreads_) {
    ThreadJoin(th);
  }

  bgthreads_.clear();
}

void ThreadPoolImpl::LowerIOPriority() {
#ifdef OS_LINUX
  PthreadCall("lock", pthread_mutex_lock(&mu_));
  low_io_priority_ = true;
  PthreadCall("unlock", pthread_mutex_unlock(&mu_));
#endif
}

void ThreadPoolImpl::BGThread(size_t thread_id) {
  bool low_io_priority = false;
  while (true) {
// Wait until there is an item that is ready to run
    Lock uniqueLock(mu_);
    PthreadCall("lock", ThreadPoolMutexLock(uniqueLock));
    // Stop waiting if the thread needs to do work or needs to terminate.
    while (!exit_all_threads_ && !IsLastExcessiveThread(thread_id) &&
           (queue_.empty() || IsExcessiveThread(thread_id))) {
      PthreadCall("wait", ConditionWait(bgsignal_, uniqueLock));
    }

    if (exit_all_threads_) {  // mechanism to let BG threads exit safely
      PthreadCall("unlock", MutexUnlock(uniqueLock));
      break;
    }

    if (IsLastExcessiveThread(thread_id)) {
      // Current thread is the last generated one and is excessive.
      // We always terminate excessive thread in the reverse order of
      // generation time.
      auto& terminating_thread = bgthreads_.back();
      PthreadCall("detach", ThreadDetach(terminating_thread));
      bgthreads_.pop_back();
      if (HasExcessiveThread()) {
        // There is still at least more excessive thread to terminate.
        WakeUpAllThreads();
      }
      PthreadCall("unlock", MutexUnlock(uniqueLock));
      break;
    }

    void (*function)(void*) = queue_.front().function;
    void* arg = queue_.front().arg;
    queue_.pop_front();
    queue_len_.store(static_cast<unsigned int>(queue_.size()),
                     std::memory_order_relaxed);

    bool decrease_io_priority = (low_io_priority != low_io_priority_);
    PthreadCall("unlock", MutexUnlock(uniqueLock));

#ifdef OS_LINUX
    if (decrease_io_priority) {
#define IOPRIO_CLASS_SHIFT (13)
#define IOPRIO_PRIO_VALUE(class, data) (((class) << IOPRIO_CLASS_SHIFT) | data)
      // Put schedule into IOPRIO_CLASS_IDLE class (lowest)
      // These system calls only have an effect when used in conjunction
      // with an I/O scheduler that supports I/O priorities. As at
      // kernel 2.6.17 the only such scheduler is the Completely
      // Fair Queuing (CFQ) I/O scheduler.
      // To change scheduler:
      //  echo cfq > /sys/block/<device_name>/queue/schedule
      // Tunables to consider:
      //  /sys/block/<device_name>/queue/slice_idle
      //  /sys/block/<device_name>/queue/slice_sync
      syscall(SYS_ioprio_set, 1,  // IOPRIO_WHO_PROCESS
              0,                  // current thread
              IOPRIO_PRIO_VALUE(3, 0));
      low_io_priority = true;
    }
#else
    (void)decrease_io_priority;  // avoid 'unused variable' error
#endif
    (*function)(arg);
  }
}

// Helper struct for passing arguments when creating threads.
struct BGThreadMetadata {
  ThreadPoolImpl* thread_pool_;
  size_t thread_id_;  // Thread count in the thread.
  BGThreadMetadata(ThreadPoolImpl* thread_pool, size_t thread_id)
      : thread_pool_(thread_pool), thread_id_(thread_id) {}
};

static void* BGThreadWrapper(void* arg) {
  BGThreadMetadata* meta = reinterpret_cast<BGThreadMetadata*>(arg);
  size_t thread_id = meta->thread_id_;
  ThreadPoolImpl* tp = meta->thread_pool_;
#if ROCKSDB_USING_THREAD_STATUS
  // for thread-status
  ThreadStatusUtil::RegisterThread(
      tp->GetHostEnv(), (tp->GetThreadPriority() == Env::Priority::HIGH
                             ? ThreadStatus::HIGH_PRIORITY
                             : ThreadStatus::LOW_PRIORITY));
#endif
  delete meta;
  tp->BGThread(thread_id);
#if ROCKSDB_USING_THREAD_STATUS
  ThreadStatusUtil::UnregisterThread();
#endif
  return nullptr;
}

void ThreadPoolImpl::WakeUpAllThreads() {
  PthreadCall("signalall", ConditionSignalAll(bgsignal_));
}

void ThreadPoolImpl::SetBackgroundThreadsInternal(int num, bool allow_reduce) {
  Lock lock(mu_);
  PthreadCall("lock", ThreadPoolMutexLock(lock));
  if (exit_all_threads_) {
    PthreadCall("unlock", MutexUnlock(lock));
    return;
  }
  if (num > total_threads_limit_ ||
      (num < total_threads_limit_ && allow_reduce)) {
    total_threads_limit_ = std::max(1, num);
    WakeUpAllThreads();
    StartBGThreads();
  }
  PthreadCall("unlock", MutexUnlock(lock));
}

void ThreadPoolImpl::IncBackgroundThreadsIfNeeded(int num) {
  SetBackgroundThreadsInternal(num, false);
}

void ThreadPoolImpl::SetBackgroundThreads(int num) {
  SetBackgroundThreadsInternal(num, true);
}

void ThreadPoolImpl::StartBGThreads() {
  // Start background thread if necessary
  while ((int)bgthreads_.size() < total_threads_limit_) {
#ifdef ROCKSDB_STD_THREADPOOL
    std::thread p_t(&BGThreadWrapper,
      new BGThreadMetadata(this, bgthreads_.size()));
    bgthreads_.push_back(std::move(p_t));
#else
    pthread_t t;
    PthreadCall("create thread",
                pthread_create(&t, nullptr, &BGThreadWrapper,
                               new BGThreadMetadata(this, bgthreads_.size())));
// Set the thread name to aid debugging
#if defined(_GNU_SOURCE) && defined(__GLIBC_PREREQ)
#if __GLIBC_PREREQ(2, 12)
    char name_buf[16];
    snprintf(name_buf, sizeof name_buf, "rocksdb:bg%" ROCKSDB_PRIszt,
             bgthreads_.size());
    name_buf[sizeof name_buf - 1] = '\0';
    pthread_setname_np(t, name_buf);
#endif
#endif
    bgthreads_.push_back(t);
#endif
  }
}

void ThreadPoolImpl::Schedule(void (*function)(void* arg1), void* arg,
                              void* tag, void (*unschedFunction)(void* arg)) {
  Lock lock(mu_);
  PthreadCall("lock", ThreadPoolMutexLock(lock));

  if (exit_all_threads_) {
    PthreadCall("unlock", MutexUnlock(lock));
    return;
  }

  StartBGThreads();

  // Add to priority queue
  queue_.push_back(BGItem());
  queue_.back().function = function;
  queue_.back().arg = arg;
  queue_.back().tag = tag;
  queue_.back().unschedFunction = unschedFunction;
  queue_len_.store(static_cast<unsigned int>(queue_.size()),
                   std::memory_order_relaxed);

  if (!HasExcessiveThread()) {
    // Wake up at least one waiting thread.
    PthreadCall("signal", ConditionSignal(bgsignal_));
  } else {
    // Need to wake up all threads to make sure the one woken
    // up is not the one to terminate.
    WakeUpAllThreads();
  }

  PthreadCall("unlock", MutexUnlock(lock));
}

int ThreadPoolImpl::UnSchedule(void* arg) {
  int count = 0;

  Lock lock(mu_);
  PthreadCall("lock", ThreadPoolMutexLock(lock));

  // Remove from priority queue
  BGQueue::iterator it = queue_.begin();
  while (it != queue_.end()) {
    if (arg == (*it).tag) {
      void (*unschedFunction)(void*) = (*it).unschedFunction;
      void* arg1 = (*it).arg;
      if (unschedFunction != nullptr) {
        (*unschedFunction)(arg1);
      }
      it = queue_.erase(it);
      count++;
    } else {
      ++it;
    }
  }
  queue_len_.store(static_cast<unsigned int>(queue_.size()),
                   std::memory_order_relaxed);
  PthreadCall("unlock", MutexUnlock(lock));
  return count;
}

ThreadPool* NewThreadPool(int num_threads) {
  ThreadPoolImpl* thread_pool = new ThreadPoolImpl();
  thread_pool->SetBackgroundThreads(num_threads);
  return thread_pool;
}

}  // namespace rocksdb

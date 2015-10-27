//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <atomic>
#include "util/thread_posix.h"
#include <unistd.h>
#ifdef OS_LINUX
#include <sys/syscall.h>
#endif

namespace rocksdb {

void ThreadPool::PthreadCall(const char* label, int result) {
  if (result != 0) {
    fprintf(stderr, "pthread %s: %s\n", label, strerror(result));
    abort();
  }
}

ThreadPool::ThreadPool()
    : total_threads_limit_(1),
      bgthreads_(0),
      queue_(),
      queue_len_(0),
      exit_all_threads_(false),
      low_io_priority_(false),
      env_(nullptr) {
  PthreadCall("mutex_init", pthread_mutex_init(&mu_, nullptr));
  PthreadCall("cvar_init", pthread_cond_init(&bgsignal_, nullptr));
}

ThreadPool::~ThreadPool() { assert(bgthreads_.size() == 0U); }

void ThreadPool::JoinAllThreads() {
  PthreadCall("lock", pthread_mutex_lock(&mu_));
  assert(!exit_all_threads_);
  exit_all_threads_ = true;
  PthreadCall("signalall", pthread_cond_broadcast(&bgsignal_));
  PthreadCall("unlock", pthread_mutex_unlock(&mu_));
  for (const auto tid : bgthreads_) {
    pthread_join(tid, nullptr);
  }
  bgthreads_.clear();
}

void ThreadPool::LowerIOPriority() {
#ifdef OS_LINUX
  PthreadCall("lock", pthread_mutex_lock(&mu_));
  low_io_priority_ = true;
  PthreadCall("unlock", pthread_mutex_unlock(&mu_));
#endif
}

void ThreadPool::BGThread(size_t thread_id) {
  bool low_io_priority = false;
  while (true) {
    // Wait until there is an item that is ready to run
    PthreadCall("lock", pthread_mutex_lock(&mu_));
    // Stop waiting if the thread needs to do work or needs to terminate.
    while (!exit_all_threads_ && !IsLastExcessiveThread(thread_id) &&
           (queue_.empty() || IsExcessiveThread(thread_id))) {
      PthreadCall("wait", pthread_cond_wait(&bgsignal_, &mu_));
    }
    if (exit_all_threads_) {  // mechanism to let BG threads exit safely
      PthreadCall("unlock", pthread_mutex_unlock(&mu_));
      break;
    }
    if (IsLastExcessiveThread(thread_id)) {
      // Current thread is the last generated one and is excessive.
      // We always terminate excessive thread in the reverse order of
      // generation time.
      auto terminating_thread = bgthreads_.back();
      pthread_detach(terminating_thread);
      bgthreads_.pop_back();
      if (HasExcessiveThread()) {
        // There is still at least more excessive thread to terminate.
        WakeUpAllThreads();
      }
      PthreadCall("unlock", pthread_mutex_unlock(&mu_));
      break;
    }
    void (*function)(void*) = queue_.front().function;
    void* arg = queue_.front().arg;
    queue_.pop_front();
    queue_len_.store(static_cast<unsigned int>(queue_.size()),
                     std::memory_order_relaxed);

    bool decrease_io_priority = (low_io_priority != low_io_priority_);
    PthreadCall("unlock", pthread_mutex_unlock(&mu_));

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
  ThreadPool* thread_pool_;
  size_t thread_id_;  // Thread count in the thread.
  explicit BGThreadMetadata(ThreadPool* thread_pool, size_t thread_id)
      : thread_pool_(thread_pool), thread_id_(thread_id) {}
};

static void* BGThreadWrapper(void* arg) {
  BGThreadMetadata* meta = reinterpret_cast<BGThreadMetadata*>(arg);
  size_t thread_id = meta->thread_id_;
  ThreadPool* tp = meta->thread_pool_;
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

void ThreadPool::WakeUpAllThreads() {
  PthreadCall("signalall", pthread_cond_broadcast(&bgsignal_));
}

void ThreadPool::SetBackgroundThreadsInternal(int num, bool allow_reduce) {
  PthreadCall("lock", pthread_mutex_lock(&mu_));
  if (exit_all_threads_) {
    PthreadCall("unlock", pthread_mutex_unlock(&mu_));
    return;
  }
  if (num > total_threads_limit_ ||
      (num < total_threads_limit_ && allow_reduce)) {
    total_threads_limit_ = std::max(1, num);
    WakeUpAllThreads();
    StartBGThreads();
  }
  PthreadCall("unlock", pthread_mutex_unlock(&mu_));
}

void ThreadPool::IncBackgroundThreadsIfNeeded(int num) {
  SetBackgroundThreadsInternal(num, false);
}

void ThreadPool::SetBackgroundThreads(int num) {
  SetBackgroundThreadsInternal(num, true);
}

void ThreadPool::StartBGThreads() {
  // Start background thread if necessary
  while ((int)bgthreads_.size() < total_threads_limit_) {
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
  }
}

void ThreadPool::Schedule(void (*function)(void* arg1), void* arg, void* tag) {
  PthreadCall("lock", pthread_mutex_lock(&mu_));

  if (exit_all_threads_) {
    PthreadCall("unlock", pthread_mutex_unlock(&mu_));
    return;
  }

  StartBGThreads();

  // Add to priority queue
  queue_.push_back(BGItem());
  queue_.back().function = function;
  queue_.back().arg = arg;
  queue_.back().tag = tag;
  queue_len_.store(static_cast<unsigned int>(queue_.size()),
                   std::memory_order_relaxed);

  if (!HasExcessiveThread()) {
    // Wake up at least one waiting thread.
    PthreadCall("signal", pthread_cond_signal(&bgsignal_));
  } else {
    // Need to wake up all threads to make sure the one woken
    // up is not the one to terminate.
    WakeUpAllThreads();
  }

  PthreadCall("unlock", pthread_mutex_unlock(&mu_));
}

int ThreadPool::UnSchedule(void* arg) {
  int count = 0;
  PthreadCall("lock", pthread_mutex_lock(&mu_));

  // Remove from priority queue
  BGQueue::iterator it = queue_.begin();
  while (it != queue_.end()) {
    if (arg == (*it).tag) {
      it = queue_.erase(it);
      count++;
    } else {
      it++;
    }
  }
  queue_len_.store(static_cast<unsigned int>(queue_.size()),
                   std::memory_order_relaxed);
  PthreadCall("unlock", pthread_mutex_unlock(&mu_));
  return count;
}

}  // namespace rocksdb

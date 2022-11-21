//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include "monitoring/statistics.h"
#include "port/port.h"
#include "rocksdb/statistics.h"
#include "rocksdb/system_clock.h"
#include "rocksdb/thread_status.h"
#include "util/stop_watch.h"

namespace ROCKSDB_NAMESPACE {
class InstrumentedCondVar;

// A wrapper class for port::Mutex that provides additional layer
// for collecting stats and instrumentation.
class InstrumentedMutex {
 public:
  explicit InstrumentedMutex(bool adaptive = false, bool track_owner = false)
      : mutex_(adaptive),
        stats_(nullptr),
        clock_(nullptr),
        stats_code_(0),
        track_owner_(track_owner) {}

  explicit InstrumentedMutex(SystemClock* clock, bool adaptive = false,
                             bool track_owner = false)
      : mutex_(adaptive),
        stats_(nullptr),
        clock_(clock),
        stats_code_(0),
        track_owner_(track_owner) {}

  InstrumentedMutex(Statistics* stats, SystemClock* clock, int stats_code,
                    bool adaptive = false, bool track_owner = false)
      : mutex_(adaptive),
        stats_(stats),
        clock_(clock),
        stats_code_(stats_code),
        track_owner_(track_owner) {}

#ifdef COERCE_CONTEXT_SWITCH
  InstrumentedMutex(Statistics* stats, SystemClock* clock, int stats_code,
                    InstrumentedCondVar* bg_cv, bool adaptive = false,
                    bool track_owner = false)
      : mutex_(adaptive),
        stats_(stats),
        clock_(clock),
        stats_code_(stats_code),
        bg_cv_(bg_cv),
        track_owner_(track_owner) {}
#endif

  void Lock();

  void Unlock();

  void AssertHeld() { mutex_.AssertHeld(); }

  bool LockedByThisThread();

 private:
#if defined(OS_WIN) && !defined(_POSIX_THREADS)
  // https://learn.microsoft.com/en-us/windows/win32/procthread/thread-handles-and-identifiers
  // "Note that no thread identifier will ever be 0."
  // Therefore, we use 0 here as the dummy owner.
  static constexpr port::ThreadId kDummyOwner = 0;
#else
  // Assumption: a default-constructed std::thread::id does not correspond to
  // the id of any thread. See
  // https://en.cppreference.com/w/cpp/thread/thread/id/id:
  // "Default-constructs a new thread identifier. The identifier does not
  // represent a thread."
  static const std::thread::id& dummy_owner();
#endif  // !defined(OS_WIN) || defined(_POSIX_THREADS)

  void LockInternal();
  friend class InstrumentedCondVar;
  port::Mutex mutex_;
  Statistics* stats_;
  SystemClock* clock_;
  int stats_code_;
#ifdef COERCE_CONTEXT_SWITCH
  InstrumentedCondVar* bg_cv_ = nullptr;
#endif
  const bool track_owner_ = false;

#if defined(OS_WIN) && !defined(_POSIX_THREADS)
  std::atomic<port::ThreadId> owner_{kDummyOwner};
#else
  // Invariant: if any thread is holding mutex_, then owner_ is set to the
  // result of calling `std::this_thread::get_id()` within that thread.
  std::atomic<std::thread::id> owner_{dummy_owner()};
#endif  // !defined(OS_WIN) || defined(_POSIX_THREADS)
};

class ALIGN_AS(CACHE_LINE_SIZE) CacheAlignedInstrumentedMutex
    : public InstrumentedMutex {
  using InstrumentedMutex::InstrumentedMutex;
};
static_assert(alignof(CacheAlignedInstrumentedMutex) != CACHE_LINE_SIZE ||
              sizeof(CacheAlignedInstrumentedMutex) % CACHE_LINE_SIZE == 0);

// RAII wrapper for InstrumentedMutex
class InstrumentedMutexLock {
 public:
  explicit InstrumentedMutexLock(InstrumentedMutex* mutex) : mutex_(mutex) {
    mutex_->Lock();
  }

  ~InstrumentedMutexLock() { mutex_->Unlock(); }

 private:
  InstrumentedMutex* const mutex_;
  InstrumentedMutexLock(const InstrumentedMutexLock&) = delete;
  void operator=(const InstrumentedMutexLock&) = delete;
};

// RAII wrapper for temporary releasing InstrumentedMutex inside
// InstrumentedMutexLock
class InstrumentedMutexUnlock {
 public:
  explicit InstrumentedMutexUnlock(InstrumentedMutex* mutex) : mutex_(mutex) {
    mutex_->Unlock();
  }

  ~InstrumentedMutexUnlock() { mutex_->Lock(); }

 private:
  InstrumentedMutex* const mutex_;
  InstrumentedMutexUnlock(const InstrumentedMutexUnlock&) = delete;
  void operator=(const InstrumentedMutexUnlock&) = delete;
};

class InstrumentedCondVar {
 public:
  explicit InstrumentedCondVar(InstrumentedMutex* instrumented_mutex)
      : cond_(&(instrumented_mutex->mutex_)),
        stats_(instrumented_mutex->stats_),
        clock_(instrumented_mutex->clock_),
        stats_code_(instrumented_mutex->stats_code_) {}

  void Wait();

  bool TimedWait(uint64_t abs_time_us);

  void Signal() { cond_.Signal(); }

  void SignalAll() { cond_.SignalAll(); }

 private:
  void WaitInternal();
  bool TimedWaitInternal(uint64_t abs_time_us);
  port::CondVar cond_;
  Statistics* stats_;
  SystemClock* clock_;
  int stats_code_;
};

}  // namespace ROCKSDB_NAMESPACE

//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <cassert>
#include <condition_variable>
#include <mutex>
#ifdef ROCKSDB_USE_STD_SEMAPHORES
#include <semaphore>
#endif

#include "port/port.h"
#include "rocksdb/rocksdb_namespace.h"

namespace ROCKSDB_NAMESPACE {

// Wrapper providing a chosen counting semaphore implementation. The default
// implementation based on a mutex and condvar unfortunately can result in
// Release() temporarily waiting on another thread to make progress (if that
// other thread is preempted while holding the mutex), but that should be rare.
// However, alternative implementations may have correctness issues or even
// worse performance. See std::counting_semaphore for general contract.
//
// NOTE1: std::counting_semaphore is known to be buggy on many std library
// implementations, so be cautious about enabling it. Reportedly, an acquire()
// can falsely block indefinitely. And we can't easily work around that with
// try_acquire_for because another common bug has that function consistently
// sleeping for the entire timeout duration even if a release() happens earlier.
// Therefore, using std::counting_semaphore/binary_semaphore is strictly opt-in
// for now.
//
// NOTE2: Also tried wrapping folly::fibers::Semaphore here but it was not as
// efficient (for parallel compression) as even the mutex+condvar version.
class ALIGN_AS(CACHE_LINE_SIZE) CountingSemaphore {
 public:
  explicit CountingSemaphore(std::ptrdiff_t starting_count)
#ifdef ROCKSDB_USE_STD_SEMAPHORES
      : sem_(starting_count)
#else
      : count_(static_cast<int32_t>(starting_count))
#endif  // ROCKSDB_USE_STD_SEMAPHORES
  {
    assert(starting_count >= 0);
    assert(starting_count <= INT32_MAX);
  }
  void Acquire() {
#ifdef ROCKSDB_USE_STD_SEMAPHORES
    sem_.acquire();
#else
    std::unique_lock<std::mutex> lock(mutex_);
    assert(count_ >= 0);
    cv_.wait(lock, [this] { return count_ > 0; });
    --count_;
#endif  // ROCKSDB_USE_STD_SEMAPHORES
  }
  bool TryAcquire() {
#ifdef ROCKSDB_USE_STD_SEMAPHORES
    return sem_.try_acquire();
#else
    std::unique_lock<std::mutex> lock(mutex_);
    assert(count_ >= 0);
    if (count_ == 0) {
      return false;
    } else {
      --count_;
      return true;
    }
#endif  // ROCKSDB_USE_STD_SEMAPHORES
  }
  void Release(std::ptrdiff_t n = 1) {
#ifdef ROCKSDB_USE_STD_SEMAPHORES
    sem_.release(n);
#else
    assert(n >= 0);
    assert(n <= INT32_MAX);
    if (n > 0) {
      std::unique_lock<std::mutex> lock(mutex_);
      assert(count_ >= 0);
      count_ += static_cast<int32_t>(n);
      assert(count_ >= 0);  // no overflow
      if (n == 1) {
        cv_.notify_one();
      } else {
        cv_.notify_all();
      }
    }
#endif  // ROCKSDB_USE_STD_SEMAPHORES
  }

 private:
#ifdef ROCKSDB_USE_STD_SEMAPHORES
  std::counting_semaphore<INT32_MAX> sem_;
#else
  int32_t count_;
  std::mutex mutex_;
  std::condition_variable cv_;
#endif  // ROCKSDB_USE_STD_SEMAPHORES
};  // namespace ROCKSDB_NAMESPACE

// Wrapper providing a chosen binary semaphore implementation. See notes on
// CountingSemaphore above, and on Release() below.
class BinarySemaphore {
 public:
  explicit BinarySemaphore(std::ptrdiff_t starting_count)
#ifdef ROCKSDB_USE_STD_SEMAPHORES
      : sem_(starting_count)
#else
      : state_(starting_count > 0)
#endif  // ROCKSDB_USE_STD_SEMAPHORES
  {
    assert(starting_count >= 0);
  }
  void Acquire() {
#ifdef ROCKSDB_USE_STD_SEMAPHORES
    sem_.acquire();
#else
    std::unique_lock<std::mutex> lock(mutex_);
    cv_.wait(lock, [this] { return state_; });
    state_ = false;
#endif  // ROCKSDB_USE_STD_SEMAPHORES
  }
  bool TryAcquire() {
#ifdef ROCKSDB_USE_STD_SEMAPHORES
    return sem_.try_acquire();
#else
    std::unique_lock<std::mutex> lock(mutex_);
    if (state_) {
      state_ = false;
      return true;
    } else {
      return false;
    }
#endif  // ROCKSDB_USE_STD_SEMAPHORES
  }
  void Release() {
    // NOTE: implementations of std::binary_semaphore::release() tend to behave
    // like counting semaphores in the case of multiple Release() calls without
    // Acquire() in between, though it is undefined behavior. It is also OK to
    // cap the count at 1.
#ifdef ROCKSDB_USE_STD_SEMAPHORES
    sem_.release();
#else
    std::unique_lock<std::mutex> lock(mutex_);
    // check precondition to avoid UB in std implementation
    assert(state_ == false);
    state_ = true;
    cv_.notify_one();
#endif  // ROCKSDB_USE_STD_SEMAPHORES
  }

 private:
#ifdef ROCKSDB_USE_STD_SEMAPHORES
  std::binary_semaphore sem_;
#else
  bool state_;
  std::mutex mutex_;
  std::condition_variable cv_;
#endif  // ROCKSDB_USE_STD_SEMAPHORES
};

}  // namespace ROCKSDB_NAMESPACE

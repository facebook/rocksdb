//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once
#include <assert.h>

#include <atomic>
#include <mutex>
#include <thread>

#include "port/port.h"

namespace ROCKSDB_NAMESPACE {

// Helper class that locks a mutex on construction and unlocks the mutex when
// the destructor of the MutexLock object is invoked.
//
// Typical usage:
//
//   void MyClass::MyMethod() {
//     MutexLock l(&mu_);       // mu_ is an instance variable
//     ... some complex code, possibly with multiple return paths ...
//   }

class MutexLock {
 public:
  explicit MutexLock(port::Mutex *mu) : mu_(mu) { this->mu_->Lock(); }
  // No copying allowed
  MutexLock(const MutexLock &) = delete;
  void operator=(const MutexLock &) = delete;

  ~MutexLock() { this->mu_->Unlock(); }

 private:
  port::Mutex *const mu_;
};

//
// Acquire a ReadLock on the specified RWMutex.
// The Lock will be automatically released when the
// object goes out of scope.
//
class ReadLock {
 public:
  explicit ReadLock(port::RWMutex *mu) : mu_(mu) { this->mu_->ReadLock(); }
  // No copying allowed
  ReadLock(const ReadLock &) = delete;
  void operator=(const ReadLock &) = delete;

  ~ReadLock() { this->mu_->ReadUnlock(); }

 private:
  port::RWMutex *const mu_;
};

//
// Automatically unlock a locked mutex when the object is destroyed
//
class ReadUnlock {
 public:
  explicit ReadUnlock(port::RWMutex *mu) : mu_(mu) { mu->AssertHeld(); }
  // No copying allowed
  ReadUnlock(const ReadUnlock &) = delete;
  ReadUnlock &operator=(const ReadUnlock &) = delete;

  ~ReadUnlock() { mu_->ReadUnlock(); }

 private:
  port::RWMutex *const mu_;
};

//
// Acquire a WriteLock on the specified RWMutex.
// The Lock will be automatically released then the
// object goes out of scope.
//
class WriteLock {
 public:
  explicit WriteLock(port::RWMutex *mu) : mu_(mu) { this->mu_->WriteLock(); }
  // No copying allowed
  WriteLock(const WriteLock &) = delete;
  void operator=(const WriteLock &) = delete;

  ~WriteLock() { this->mu_->WriteUnlock(); }

 private:
  port::RWMutex *const mu_;
};

//
// SpinMutex has very low overhead for low-contention cases.  Method names
// are chosen so you can use std::unique_lock or std::lock_guard with it.
//
class SpinMutex {
 public:
  SpinMutex() : locked_(false) {}

  bool try_lock() {
    auto currently_locked = locked_.load(std::memory_order_relaxed);
    return !currently_locked &&
           locked_.compare_exchange_weak(currently_locked, true,
                                         std::memory_order_acquire,
                                         std::memory_order_relaxed);
  }

  void lock() {
    for (size_t tries = 0;; ++tries) {
      if (try_lock()) {
        // success
        break;
      }
      port::AsmVolatilePause();
      if (tries > 100) {
        std::this_thread::yield();
      }
    }
  }

  void unlock() { locked_.store(false, std::memory_order_release); }

 private:
  std::atomic<bool> locked_;
};

// We want to prevent false sharing
template <class T>
struct ALIGN_AS(CACHE_LINE_SIZE) LockData {
  T lock_;
};

//
// Inspired by Guava: https://github.com/google/guava/wiki/StripedExplained
// A striped Lock. This offers the underlying lock striping similar
// to that of ConcurrentHashMap in a reusable form, and extends it for
// semaphores and read-write locks. Conceptually, lock striping is the technique
// of dividing a lock into many <i>stripes</i>, increasing the granularity of a
// single lock and allowing independent operations to lock different stripes and
// proceed concurrently, instead of creating contention for a single lock.
//
template <class T, class P>
class Striped {
 public:
  Striped(size_t stripes, std::function<uint64_t(const P &)> hash)
      : stripes_(stripes), hash_(hash) {
    locks_ = reinterpret_cast<LockData<T> *>(
        port::cacheline_aligned_alloc(sizeof(LockData<T>) * stripes));
    for (size_t i = 0; i < stripes; i++) {
      new (&locks_[i]) LockData<T>();
    }
  }

  virtual ~Striped() {
    if (locks_ != nullptr) {
      assert(stripes_ > 0);
      for (size_t i = 0; i < stripes_; i++) {
        locks_[i].~LockData<T>();
      }
      port::cacheline_aligned_free(locks_);
    }
  }

  T *get(const P &key) {
    uint64_t h = hash_(key);
    size_t index = h % stripes_;
    return &reinterpret_cast<LockData<T> *>(&locks_[index])->lock_;
  }

 private:
  size_t stripes_;
  LockData<T> *locks_;
  std::function<uint64_t(const P &)> hash_;
};

}  // namespace ROCKSDB_NAMESPACE

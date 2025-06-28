//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "utilities/transactions/transaction_db_mutex_impl.h"

#include <chrono>
#include <condition_variable>
#include <mutex>
#ifdef DEBUG_MUTEX_IN_TRX_DB_MUTEX
#include <sstream>
#include <thread>
#endif

#include "rocksdb/utilities/transaction_db_mutex.h"

namespace ROCKSDB_NAMESPACE {

// Helper function to log lock/unlock events on a mutex for debugging purposes
void logLockEventOnThread(std::mutex* mutex, bool isLock, bool success = true) {
#ifdef DEBUG_MUTEX_IN_TRX_DB_MUTEX
  // get thread id
  std::thread::id this_id = std::this_thread::get_id();
  std::stringstream ss;
  ss << this_id;
  std::string thread_id = ss.str();
  if (isLock) {
    if (success) {
      fprintf(stderr, "Locking mutex %p, thread id %s\n", mutex,
              thread_id.c_str());
    } else {
      fprintf(stderr, "Failed to lock mutex %p, thread id %s\n", mutex,
              thread_id.c_str());
    }
  } else {
    fprintf(stderr, "Unlocking mutex %p, thread id %s\n", mutex,
            thread_id.c_str());
  }
#else
  (void)mutex;
  (void)isLock;
  (void)success;
#endif
}

class TransactionDBMutexImpl : public TransactionDBMutex {
 public:
  TransactionDBMutexImpl() = default;
  ~TransactionDBMutexImpl() override = default;

  Status Lock() override;

  Status TryLockFor(int64_t timeout_time) override;

  void UnLock() override {
    logLockEventOnThread(&mutex_, false);
    mutex_.unlock();
  }

  friend class TransactionDBCondVarImpl;

 private:
  std::mutex mutex_;
};

class TransactionDBCondVarImpl : public TransactionDBCondVar {
 public:
  TransactionDBCondVarImpl() = default;
  ~TransactionDBCondVarImpl() override = default;

  // Note that the behaivor of Wait and WaitFor are different from
  // std::condition_variable It does not internall lock the mutex, nor release
  // it when it is woken up. Instead, it assumes that the mutex is already
  // locked before calling Wait, and it is caller's responsibility to release it
  // after Wait returns.
  Status Wait(std::shared_ptr<TransactionDBMutex> mutex,
              std::function<bool()>* predicate) override;
  Status WaitFor(std::shared_ptr<TransactionDBMutex> mutex,
                 int64_t timeout_time,
                 std::function<bool()>* predicate) override;

  void Notify() override { cv_.notify_one(); }

  void NotifyAll() override { cv_.notify_all(); }

 private:
  std::condition_variable cv_;
};

std::shared_ptr<TransactionDBMutex>
TransactionDBMutexFactoryImpl::AllocateMutex() {
  return std::shared_ptr<TransactionDBMutex>(new TransactionDBMutexImpl());
}

std::shared_ptr<TransactionDBCondVar>
TransactionDBMutexFactoryImpl::AllocateCondVar() {
  return std::shared_ptr<TransactionDBCondVar>(new TransactionDBCondVarImpl());
}

Status TransactionDBMutexImpl::Lock() {
  mutex_.lock();
  logLockEventOnThread(&mutex_, true);
  return Status::OK();
}

Status TransactionDBMutexImpl::TryLockFor(int64_t timeout_time) {
  bool locked = true;

  if (timeout_time == 0) {
    locked = mutex_.try_lock();
  } else {
    // Previously, this code used a std::timed_mutex.  However, this was changed
    // due to known bugs in gcc versions < 4.9.
    // https://gcc.gnu.org/bugzilla/show_bug.cgi?id=54562
    //
    // Since this mutex isn't held for long and only a single mutex is ever
    // held at a time, it is reasonable to ignore the lock timeout_time here
    // and only check it when waiting on the condition_variable.
    mutex_.lock();
  }

  if (!locked) {
    // timeout acquiring mutex
    logLockEventOnThread(&mutex_, true, false);
    return Status::TimedOut(Status::SubCode::kMutexTimeout);
  }

  logLockEventOnThread(&mutex_, true);

  return Status::OK();
}

Status TransactionDBCondVarImpl::Wait(std::shared_ptr<TransactionDBMutex> mutex,
                                      std::function<bool()>* predicate) {
  auto mutex_impl = static_cast<TransactionDBMutexImpl*>(mutex.get());

  std::unique_lock<std::mutex> lock(mutex_impl->mutex_, std::adopt_lock);
  logLockEventOnThread(&mutex_impl->mutex_, true);

  logLockEventOnThread(&mutex_impl->mutex_, false);
  // If timeout is negative, do not use a timeout
  if (predicate != nullptr) {
    cv_.wait(lock, *predicate);
  } else {
    cv_.wait(lock);
  }
  logLockEventOnThread(&mutex_impl->mutex_, true);

  // Make sure unique_lock doesn't unlock mutex when it destructs
  lock.release();

  return Status::OK();
}

Status TransactionDBCondVarImpl::WaitFor(
    std::shared_ptr<TransactionDBMutex> mutex, int64_t timeout_time,
    std::function<bool()>* predicate) {
  Status s;

  auto mutex_impl = static_cast<TransactionDBMutexImpl*>(mutex.get());
  std::unique_lock<std::mutex> lock(mutex_impl->mutex_, std::adopt_lock);

  logLockEventOnThread(&mutex_impl->mutex_, false);
  if (timeout_time < 0) {
    // If timeout is negative, do not use a timeout
    if (predicate != nullptr) {
      cv_.wait(lock, *predicate);
    } else {
      cv_.wait(lock);
    }
  } else {
    auto duration = std::chrono::microseconds(timeout_time);
    if (predicate != nullptr) {
      if (!(*predicate)()) {
        auto cv_status = cv_.wait_for(lock, duration, *predicate);
        if (!cv_status) {
          s = Status::TimedOut(Status::SubCode::kMutexTimeout);
        }
      }
    } else {
      auto cv_status = cv_.wait_for(lock, duration);
      // Check if the wait stopped due to timing out.
      if (cv_status == std::cv_status::timeout) {
        s = Status::TimedOut(Status::SubCode::kMutexTimeout);
      }
    }
  }
  logLockEventOnThread(&mutex_impl->mutex_, true);

  // Make sure unique_lock doesn't unlock mutex when it destructs
  lock.release();

  // CV was signaled, or we spuriously woke up (but didn't time out)
  return s;
}

}  // namespace ROCKSDB_NAMESPACE

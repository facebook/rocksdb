//  Copyright (c) Meta Platforms, Inc. and affiliates.
//
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <cinttypes>
#include <cstddef>
#include <cstdio>
#include <iostream>
#include <memory>
#include <sstream>

#include "rocksdb/convenience.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/utilities/transaction_db.h"
#include "utilities/transactions/lock/lock_manager.h"
#include "utilities/transactions/lock/point/point_lock_manager_test_common.h"
#include "utilities/transactions/pessimistic_transaction.h"

namespace ROCKSDB_NAMESPACE {

// Since this code is executed both with and without gtest, it support assert
// with different way.
#ifdef ASSERT_TRUE
#define ASSERT_TRUE_WITH_MSG(expr, errmsg) ASSERT_TRUE(expr) << (errmsg)
#else
#define ASSERT_TRUE_WITH_MSG(expr, errmsg)                             \
  if (!(expr)) {                                                       \
    std::cerr << "Assert true failed with error message: " << (errmsg) \
              << std::endl;                                            \
    abort();                                                           \
  }
#endif

#ifndef ASSERT_OK
#define ASSERT_OK(s) \
  ASSERT_TRUE_WITH_MSG(s.ok(), "Failed with " + s.ToString());
#endif

#define ASSERT_TRUE_WITH_INFO(X)                                         \
  ASSERT_TRUE_WITH_MSG((X), "Thd " + std::to_string(thd_idx) + " Txn " + \
                                std::to_string(txn_id) + " key " +       \
                                std::to_string(key))

#define ASSERT_EQ_WITH_INFO(X, Y) ASSERT_TRUE_WITH_INFO((X) == (Y))

constexpr bool kDebugLog = false;

#define DEBUG_LOG(...)            \
  if (kDebugLog) {                \
    fprintf(stderr, __VA_ARGS__); \
    fflush(stderr);               \
  }

#define DEBUG_LOG_PREFIX(format, ...) \
  DEBUG_LOG("Thd %zu Txn %" PRIu64 " " format, thd_idx, txn_id, ##__VA_ARGS__);

enum class LockTypeToTest : int8_t {
  EXCLUSIVE_ONLY = 0,
  SHARED_ONLY = 1,
  EXCLUSIVE_AND_SHARED = 2,
};

struct KeyStatus {
  KeyStatus(uint32_t k, bool ex, int v) : key(k), exclusive(ex), value(v) {}
  uint32_t key;
  bool exclusive;
  int value;
};

class PointLockValidationTestRunner {
 public:
  PointLockValidationTestRunner(Env* env, TransactionDBOptions txndb_opt,
                                std::shared_ptr<LockManager> locker,
                                TransactionDB* db, TransactionOptions txn_opt,
                                size_t thd_cnt, size_t key_cnt,
                                size_t max_num_keys_to_lock_per_txn,
                                uint32_t execution_time_sec,
                                LockTypeToTest lock_type,
                                bool allow_non_deadlock_error,
                                int sleep_after_lock_acquisition_ms)
      : env_(env),
        txndb_opt_(txndb_opt),
        locker_(locker),
        db_(db),
        txn_opt_(txn_opt),
        thread_count_(thd_cnt),
        key_count_(key_cnt),
        max_num_keys_to_lock_per_txn_(max_num_keys_to_lock_per_txn),
        execution_time_sec_(execution_time_sec),
        lock_type_(lock_type),
        allow_non_deadlock_error_(allow_non_deadlock_error),
        sleep_after_lock_acquisition_ms_(sleep_after_lock_acquisition_ms),
        num_of_locks_acquired_(0),
        num_of_shared_locks_acquired_(0),
        num_of_exclusive_locks_acquired_(0),
        num_of_deadlock_detected_(0),
        shutdown_(false) {
    values_.resize(key_count_, 0);
    exclusive_lock_status_.resize(key_count_, 0);

    // init counters and values
    for (size_t i = 0; i < key_count_; i++) {
      counters_.emplace_back(std::make_unique<std::atomic_int>(0));
      shared_lock_count_.emplace_back(std::make_unique<std::atomic_int>(0));
    }
  }

  PessimisticTransaction* NewTxn(
      TransactionOptions txn_opt = TransactionOptions()) {
    Transaction* txn = db_->BeginTransaction(WriteOptions(), txn_opt);
    return static_cast<PessimisticTransaction*>(txn);
  }

  void run() {
    // Verify lock guarantee. Exclusive lock provide unique access guarantee.
    // Shared lock provide shared access guarantee.
    // Create multiple threads. Each try to grab a lock with random type on
    // random key.

    // To validate lock exclusive guarantee, each key has a value and a counter
    // used for tracking the number of exclusive locks have been acquired on it
    // in each test run across all threads.

    // Every time an exclusive lock is acquired, both the counter and the value
    // are bumped by 1. The difference between the counter and the value is that
    // counter is atomic, so it is guaranteed that it would not lose update,
    // while value is not atomic. Its correctness is only guaranteed by the
    // exclusiveness provided by the lock manager which is being tested. If the
    // lock manager does not guarantee exclusiveness, the value would lose
    // update, and the counter would mismatch with the value, which fails the
    // test.

    // To validate lock shared guarantee, after a shared lock is acquired, the
    // counter and value are read and stored in a local variable inside the
    // thread. Before the lock is released, the local copy is compared against
    // the counter and value. If they mismatch, it means the shared lock
    // guaranteed is violated.

    MockColumnFamilyHandle cf(1);
    locker_->AddColumnFamily(&cf);

    for (size_t thd_idx = 0; thd_idx < thread_count_; thd_idx++) {
      threads_.emplace_back([this, thd_idx]() {
        auto txn = NewTxn(txn_opt_);
        auto txn_id = txn->GetID();
        while (!shutdown_) {
          DEBUG_LOG("Thd %zu Txn %" PRIu64 "new txn\n", thd_idx, txn_id);
          std::vector<KeyStatus> locked_key_status;
          auto num_key_to_lock = max_num_keys_to_lock_per_txn_;
          Status s;

          for (uint32_t j = 0; j < num_key_to_lock; j++) {
            uint32_t key = 0;
            key = Random::GetTLSInstance()->Uniform(
                static_cast<uint32_t>(key_count_));
            auto key_str = std::to_string(key);
            bool isUpgrade = false;
            bool isDowngrade = false;

            // Decide lock type
            auto exclusive_lock_type = Random::GetTLSInstance()->OneIn(2);
            // check whether a lock on the same key is already held
            auto it =
                std::find_if(locked_key_status.begin(), locked_key_status.end(),
                             [&key](KeyStatus& e) { return e.key == key; });
            auto lock_type_to_test = static_cast<LockTypeToTest>(lock_type_);
            if (it != locked_key_status.end()) {
              // a lock on the same key is already held.
              if (lock_type_to_test == LockTypeToTest::EXCLUSIVE_AND_SHARED) {
                // if test both shared and exclusive locks, switch their type
                if (it->exclusive == false) {
                  // If it is a shared lock, switch to an exclusive lock
                  exclusive_lock_type = true;
                  isUpgrade = true;
                } else {
                  // If it is an exclusive lock, downgrade to a shared lock
                  exclusive_lock_type = false;
                  isDowngrade = true;
                }
              } else {
                // try to lock a different key
                j--;
                continue;
              }
            }
            if (lock_type_to_test != LockTypeToTest::EXCLUSIVE_AND_SHARED) {
              // if only one type of locks to be acquired, update its type
              exclusive_lock_type =
                  (lock_type_to_test == LockTypeToTest::EXCLUSIVE_ONLY);
            }

            if (!allow_non_deadlock_error_) {
              if (isDowngrade) {
                // Before downgrade, validate the lock is in exlusive status
                // This could not be done after downgrade, as another thread
                // could take a shared lock and update lock status
                ASSERT_TRUE_WITH_INFO(exclusive_lock_status_[key]);
                ASSERT_EQ_WITH_INFO(*shared_lock_count_[key], 0);
                // for downgrade, update the lock status before acquiring the
                // lock, as afterwards, it will not have exclusive access to it
                exclusive_lock_status_[key] = 0;
              }
            }

            // try to acquire the lock
            DEBUG_LOG_PREFIX("try to acquire lock %u type %s\n", key,
                             exclusive_lock_type ? "exclusive" : "shared");
            s = locker_->TryLock(txn, 1, key_str, env_, exclusive_lock_type);
            if (s.ok()) {
              DEBUG_LOG_PREFIX("acquired lock %u type %s\n", key,
                               exclusive_lock_type ? "exclusive" : "shared");

              // update local lock status
              if (exclusive_lock_type) {
                if (isUpgrade) {
                  it->exclusive = true;
                } else {
                  locked_key_status.emplace_back(key, exclusive_lock_type,
                                                 values_[key]);
                }
                num_of_exclusive_locks_acquired_++;
              } else {
                if (isDowngrade) {
                  it->exclusive = false;
                } else {
                  // Could not validate status was not in exclusive status, as
                  // the lock could be downgraded by another thread.
                  locked_key_status.emplace_back(key, exclusive_lock_type,
                                                 values_[key]);
                }
                num_of_shared_locks_acquired_++;
              }
              num_of_locks_acquired_++;

              // Check and update global lock status
              if (!allow_non_deadlock_error_) {
                // Validate lock status, if deadlock is the only allowed error.
                // otherwise, lock could be expired and stolen
                if (exclusive_lock_type) {
                  // validate the lock is not in exclusive status
                  ASSERT_TRUE_WITH_INFO(!exclusive_lock_status_[key]);
                  if (isUpgrade) {
                    // validate the lock is in shared status and only had one
                    // shared lock
                    ASSERT_EQ_WITH_INFO(*shared_lock_count_[key], 1);
                    shared_lock_count_[key]->fetch_sub(1);
                  } else {
                    ASSERT_EQ_WITH_INFO(*shared_lock_count_[key], 0);
                  }
                  // update the lock status
                  exclusive_lock_status_[key] = 1;
                } else {
                  shared_lock_count_[key]->fetch_add(1);
                  ASSERT_TRUE_WITH_INFO(!exclusive_lock_status_[key]);
                }
              }
            } else {
              if (!allow_non_deadlock_error_) {
                ASSERT_TRUE_WITH_INFO(s.IsDeadlock());
              }
              if (s.IsDeadlock()) {
                DEBUG_LOG_PREFIX("detected deadlock on key %u, abort\n", key);
                num_of_deadlock_detected_++;
                // for deadlock, release all locks acquired
                break;
              } else {
                // for other errors, try again
                DEBUG_LOG_PREFIX(
                    "failed to acquire lock on key %u, due to "
                    "%s, "
                    "abort\n",
                    key, s.ToString().c_str());
              }
            }
          }

          if (sleep_after_lock_acquisition_ms_ != 0 && s.ok()) {
            auto sleep_time_us = Random::GetTLSInstance()->Uniform(
                static_cast<uint32_t>(sleep_after_lock_acquisition_ms_));
            std::this_thread::sleep_for(
                std::chrono::milliseconds(sleep_time_us));
          }

          // release all locks
          for (auto& key_status : locked_key_status) {
            auto key = key_status.key;
            ASSERT_TRUE_WITH_INFO(key < key_count_);
            // Check global lock status
            if (!allow_non_deadlock_error_) {
              ASSERT_EQ_WITH_INFO(counters_[key]->load(), values_[key]);
              auto exclusive = key_status.exclusive;
              if (exclusive) {
                // exclusive lock
                // bump the value by 1
                (*counters_[key])++;
                values_[key]++;
                DEBUG_LOG_PREFIX("bump key %u by 1 to %d\n", key, values_[key]);
                ASSERT_EQ_WITH_INFO(counters_[key]->load(), values_[key]);
              } else {
                // shared lock, validate the value has not changed since it was
                // read
                ASSERT_EQ_WITH_INFO(counters_[key]->load(), key_status.value);
                ASSERT_EQ_WITH_INFO(values_[key], key_status.value);
              }
              // Validate lock status, if deadlock is the only allowed error.
              // otherwise, lock could be expired and stolen
              if (exclusive) {
                // exclusive lock
                ASSERT_TRUE_WITH_INFO(exclusive_lock_status_[key]);
                ASSERT_EQ_WITH_INFO(*shared_lock_count_[key], 0);
                exclusive_lock_status_[key] = 0;
              } else {
                // shared lock
                ASSERT_TRUE_WITH_INFO(!exclusive_lock_status_[key]);
                ASSERT_TRUE_WITH_INFO(shared_lock_count_[key]->fetch_sub(1) >=
                                      1);
              }
            }
            DEBUG_LOG_PREFIX("release lock %u\n", key);
            locker_->UnLock(txn, 1, std::to_string(key), env_);
          }
        }
        delete txn;
      });
    }

    // run test for a few seconds
    // print progress
    auto prev_num_of_locks_acquired = num_of_locks_acquired_.load();
    int64_t measured_locks_acquired = 0;
    for (size_t i = 0; i < execution_time_sec_; i++) {
      std::this_thread::sleep_for(std::chrono::seconds(1));
      auto num_of_locks_acquired = num_of_locks_acquired_.load();
      DEBUG_LOG("num_of_locks_acquired: %" PRId64 "\n", num_of_locks_acquired);
      DEBUG_LOG("num_of_exclusive_locks_acquired: %" PRId64 "\n",
                num_of_exclusive_locks_acquired_.load());
      DEBUG_LOG("num_of_shared_locks_acquired: %" PRId64 "\n",
                num_of_shared_locks_acquired_.load());
      DEBUG_LOG("num_of_deadlock_detected: %" PRId64 "\n",
                num_of_deadlock_detected_.load());
      ASSERT_TRUE_WITH_MSG(num_of_locks_acquired > prev_num_of_locks_acquired,
                           "No locks were acquired in the last 1 second");
      prev_num_of_locks_acquired = num_of_locks_acquired;
      if (i == 0) {
        measured_locks_acquired = num_of_locks_acquired;
      }
      if (i == execution_time_sec_ - 1) {
        measured_locks_acquired =
            num_of_locks_acquired - measured_locks_acquired;
        // Skip the first second, as threads are warming up
        printf("measured_num_of_locks_acquired: %" PRId64 "\n",
               measured_locks_acquired / (execution_time_sec_ - 1));
      }
    }

    shutdown_ = true;
    for (auto& t : threads_) {
      t.join();
    }

    // validate values against counters
    for (size_t i = 0; i < key_count_; i++) {
      ASSERT_TRUE_WITH_MSG(counters_[i]->load() == values_[i],
                           "Exclusive lock guarantee is violated.");
    }

    ASSERT_TRUE_WITH_MSG(num_of_locks_acquired_.load() >= 0,
                         "No lock were acquired at all");
    printf("num_of_locks_acquired: %" PRId64 "\n",
           num_of_locks_acquired_.load());

    // Validate no lock was held at the end of the test
    auto lock_status = locker_->GetPointLockStatus();
    // print the lock status for debugging
    std::stringstream ss;
    for (auto& s : lock_status) {
      ss << "id " << s.first;
      ss << " key " << s.second.key;
      ss << " type " << (s.second.exclusive ? "exclusive" : "shared");
      ss << " txn ids [";
      for (auto& t : s.second.ids) {
        ss << t << ",";
      }
      ss << "]";
      ss << std::endl;
    }
    ASSERT_TRUE_WITH_MSG(lock_status.empty(),
                         std::to_string(lock_status.size()) +
                             " locks were held at the end. " + ss.str());
  }

 private:
  // test configuration
  Env* env_;
  TransactionDBOptions txndb_opt_;
  std::shared_ptr<LockManager> locker_;

  TransactionDB* db_;
  TransactionOptions txn_opt_;

  size_t thread_count_;
  uint32_t key_count_;
  uint32_t max_num_keys_to_lock_per_txn_;
  uint32_t execution_time_sec_;
  LockTypeToTest lock_type_;
  bool allow_non_deadlock_error_;
  uint32_t sleep_after_lock_acquisition_ms_;

  // Internal test variables
  std::vector<std::thread> threads_;

  // test statistics
  std::atomic_int64_t num_of_locks_acquired_ = 0;
  std::atomic_int64_t num_of_shared_locks_acquired_ = 0;
  std::atomic_int64_t num_of_exclusive_locks_acquired_ = 0;
  std::atomic_int64_t num_of_deadlock_detected_ = 0;

  std::vector<std::unique_ptr<std::atomic_int>> counters_;
  std::vector<int> values_;

  // track whether the lock is in exclusive status or
  // not. vector<bool> does something special underneath, causing consistency
  // issue. Therefore int64_t is used.
  std::vector<int64_t> exclusive_lock_status_;

  // A counter to track number of shared locks for tracking shared lock status
  std::vector<std::unique_ptr<std::atomic_int>> shared_lock_count_;

  // shutdown flag to signal threads to exit
  std::atomic_bool shutdown_ = false;
};

}  // namespace ROCKSDB_NAMESPACE

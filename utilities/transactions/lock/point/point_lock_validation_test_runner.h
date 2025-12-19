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

#include "rocksdb/convenience.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/utilities/transaction_db.h"
#include "utilities/transactions/lock/lock_manager.h"
#include "utilities/transactions/lock/point/point_lock_manager_test_common.h"
#include "utilities/transactions/pessimistic_transaction.h"

namespace ROCKSDB_NAMESPACE {

constexpr bool kDebugLog = false;

// Since this code is executed both with and without gtest, it supports assert
// with different ways.
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

#define ASSERT_TRUE_WITH_INFO(X) \
  ASSERT_TRUE_WITH_MSG(          \
      (X), " Txn " + std::to_string(txn_id) + " key " + std::to_string(key))

#define ASSERT_EQ_WITH_INFO(X, Y) ASSERT_TRUE_WITH_INFO((X) == (Y))

#define DEBUG_LOG(...)            \
  if (kDebugLog) {                \
    fprintf(stderr, __VA_ARGS__); \
    fflush(stderr);               \
  }

#define DEBUG_LOG_WITH_PREFIX(format, ...) \
  DEBUG_LOG("Txn %" PRIu64 " " format, txn_id, ##__VA_ARGS__);

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
  PointLockValidationTestRunner(
      Env* env, TransactionDBOptions txndb_opt,
      std::shared_ptr<LockManager> locker, TransactionDB* db,
      TransactionOptions txn_opt, uint32_t thd_cnt, uint32_t key_cnt,
      uint32_t max_num_keys_to_lock_per_txn, uint32_t execution_time_sec,
      LockTypeToTest lock_type, bool allow_non_deadlock_error,
      uint32_t max_sleep_after_lock_acquisition_ms,
      bool enable_per_thread_lock_count_assertion = false)
      : env_(env),
        txndb_opt_(std::move(txndb_opt)),
        locker_(std::move(locker)),
        db_(db),
        txn_opt_(std::move(txn_opt)),
        thread_count_(thd_cnt),
        key_count_(key_cnt),
        max_num_keys_to_lock_per_txn_(max_num_keys_to_lock_per_txn),
        execution_time_sec_(execution_time_sec),
        lock_type_(lock_type),
        allow_non_deadlock_error_(allow_non_deadlock_error),
        max_sleep_after_lock_acquisition_ms_(
            max_sleep_after_lock_acquisition_ms),
        enable_per_thread_lock_count_assertion_(
            enable_per_thread_lock_count_assertion),
        shutdown_(false) {
    // Only enable lock status validation when lock expiration/stealing isk
    // disabled.
    enable_lock_status_validation_ = txn_opt_.expiration == -1;
    values_.resize(key_count_, 0);
    exclusive_lock_status_.resize(key_count_, 0);

    // init counters and values
    for (size_t i = 0; i < key_count_; i++) {
      counters_.emplace_back(std::make_unique<std::atomic_int>(0));
      shared_lock_count_.emplace_back(std::make_unique<std::atomic_int>(0));
    }

    for (size_t i = 0; i < thread_count_; i++) {
      num_of_locks_acquired_per_thread_.emplace_back(
          std::make_unique<std::atomic_int64_t>(0));
    }
  }

  // Decide which lock type to acquire
  // If the key is already locked and only one type of locks to be tested,
  // return false, so caller could try to lock a different key.
  // Otherwise, return true.
  bool DecideLockType(
      bool& acquire_exclusive_lock, uint32_t key,
      std::unordered_map<uint32_t, KeyStatus>& locked_key_status,
      bool& isUpgrade, bool& isDowngrade) {
    // Decide lock type
    acquire_exclusive_lock = Random::GetTLSInstance()->OneIn(2);

    // check whether a lock on the same key is already held
    auto it = locked_key_status.find(key);
    if (it != locked_key_status.end()) {
      // a lock on the same key is already held.
      if (lock_type_ == LockTypeToTest::EXCLUSIVE_AND_SHARED) {
        // if test both shared and exclusive locks, switch their type
        if (it->second.exclusive == false) {
          // If it is a shared lock, upgrade to an exclusive lock
          acquire_exclusive_lock = true;
          isUpgrade = true;
        } else {
          // If it is an exclusive lock, downgrade to a shared lock
          acquire_exclusive_lock = false;
          isDowngrade = true;
        }
      } else {
        // Only one type of lock to test, and the key is already locked,
        return false;
      }
    }

    // This is a new key to lock or the lock type is switched.
    if (lock_type_ != LockTypeToTest::EXCLUSIVE_AND_SHARED) {
      // if only one type of locks to be acquired, update its type
      acquire_exclusive_lock = (lock_type_ == LockTypeToTest::EXCLUSIVE_ONLY);
    }
    return true;
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

    for (uint32_t thd_idx = 0; thd_idx < thread_count_; thd_idx++) {
      threads_.emplace_back([this, thd_idx]() {
        auto txn = static_cast<PessimisticTransaction*>(
            db_->BeginTransaction(WriteOptions(), txn_opt_));
        auto txn_id = txn->GetID();
        DEBUG_LOG_WITH_PREFIX("Thd %" PRIu32 " new txn\n", thd_idx);
        while (!shutdown_) {
          std::unordered_map<uint32_t, KeyStatus> locked_key_status;
          auto num_key_to_lock = max_num_keys_to_lock_per_txn_;
          Status s;

          for (uint32_t j = 0; j < num_key_to_lock; j++) {
            uint32_t key = 0;
            key = Random::GetTLSInstance()->Uniform(key_count_);
            auto key_str = std::to_string(key);
            bool isUpgrade = false;
            bool isDowngrade = false;
            bool exclusive_lock_type;

            if (!DecideLockType(exclusive_lock_type, key, locked_key_status,
                                isUpgrade, isDowngrade)) {
              // try a different key
              j--;
              continue;
            }

            if (enable_lock_status_validation_) {
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
            DEBUG_LOG_WITH_PREFIX("try to acquire lock %" PRIu32 " type %s\n",
                                  key,
                                  exclusive_lock_type ? "exclusive" : "shared");
            s = locker_->TryLock(txn, 1, key_str, env_, exclusive_lock_type);

            if (s.ok()) {
              DEBUG_LOG_WITH_PREFIX(
                  "acquired lock %" PRIu32 " type %s\n", key,
                  exclusive_lock_type ? "exclusive" : "shared");

              auto it = locked_key_status.find(key);
              if (isUpgrade || isDowngrade) {
                // If it is either upgrade or downgrade, the key should exist
                // already.
                ASSERT_TRUE_WITH_INFO(it != locked_key_status.end());
              } else {
                locked_key_status.emplace(
                    std::piecewise_construct, std::forward_as_tuple(key),
                    std::forward_as_tuple(key, exclusive_lock_type,
                                          values_[key]));
              }
              // update local lock status
              if (exclusive_lock_type) {
                if (isUpgrade) {
                  it->second.exclusive = true;
                }
                num_of_exclusive_locks_acquired_++;
              } else {
                if (isDowngrade) {
                  it->second.exclusive = false;
                }
                num_of_shared_locks_acquired_++;
              }
              num_of_locks_acquired_++;
              (*num_of_locks_acquired_per_thread_[thd_idx])++;

              if (enable_lock_status_validation_) {
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
                DEBUG_LOG_WITH_PREFIX(
                    "detected deadlock on key %" PRIu32 ", abort\n", key);
                num_of_deadlock_detected_++;
                // for deadlock, release all locks acquired
                break;
              } else {
                // for other errors, try again
                DEBUG_LOG_WITH_PREFIX("failed to acquire lock on key %" PRIu32
                                      ", due to "
                                      "%s, "
                                      "abort\n",
                                      key, s.ToString().c_str());
              }
            }
          }

          // After all of the locks are acquired, try to sleep a bit to simulate
          // some useful work to be done
          if (max_sleep_after_lock_acquisition_ms_ != 0 && s.ok()) {
            auto sleep_time_us = Random::GetTLSInstance()->Uniform(
                static_cast<uint32_t>(max_sleep_after_lock_acquisition_ms_));
            std::this_thread::sleep_for(
                std::chrono::milliseconds(sleep_time_us));
          }

          // release all locks
          for (const auto& pair : locked_key_status) {
            auto key_status = pair.second;
            auto key = key_status.key;
            ASSERT_TRUE_WITH_INFO(key < key_count_);
            if (enable_lock_status_validation_) {
              ASSERT_EQ_WITH_INFO(counters_[key]->load(), values_[key]);
              auto exclusive = key_status.exclusive;
              if (exclusive) {
                // for exclusive lock, bump the value by 1
                (*counters_[key])++;
                values_[key]++;
                DEBUG_LOG_WITH_PREFIX("bump key %" PRIu32 " by 1 to %d\n", key,
                                      values_[key]);
                ASSERT_EQ_WITH_INFO(counters_[key]->load(), values_[key]);
              } else {
                // shared lock, validate the value has not changed since it was
                // read
                ASSERT_EQ_WITH_INFO(counters_[key]->load(), key_status.value);
                ASSERT_EQ_WITH_INFO(values_[key], key_status.value);
              }
              if (exclusive) {
                ASSERT_TRUE_WITH_INFO(exclusive_lock_status_[key]);
                ASSERT_EQ_WITH_INFO(*shared_lock_count_[key], 0);
                exclusive_lock_status_[key] = 0;
              } else {
                ASSERT_TRUE_WITH_INFO(!exclusive_lock_status_[key]);
                ASSERT_TRUE_WITH_INFO(shared_lock_count_[key]->fetch_sub(1) >=
                                      1);
              }
            }
            DEBUG_LOG_WITH_PREFIX("release lock %" PRIu32 "\n", key);
            locker_->UnLock(txn, 1, std::to_string(key), env_);
          }
        }
        delete txn;
      });
    }

    // run test for a few seconds
    // print progress
    auto prev_num_of_locks_acquired = num_of_locks_acquired_.load();
    std::vector<int64_t> prev_num_of_locks_acquired_per_thread(thread_count_,
                                                               0);
    int64_t measured_locks_acquired = 0;
    for (uint32_t i = 0; i < execution_time_sec_; i++) {
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
      for (uint32_t thd_idx = 0; thd_idx < thread_count_; thd_idx++) {
        auto num_of_locks_acquired_per_thread =
            num_of_locks_acquired_per_thread_[thd_idx]->load();
        DEBUG_LOG("thread: %" PRIu32 " acquired %" PRId64 " locks\n", thd_idx,
                  num_of_locks_acquired_per_thread);
        if (enable_per_thread_lock_count_assertion_) {
          ASSERT_TRUE_WITH_MSG(
              num_of_locks_acquired_per_thread >
                  prev_num_of_locks_acquired_per_thread[thd_idx],
              "No locks were acquired in the last 1 second on thread " +
                  std::to_string(thd_idx));
        }
        prev_num_of_locks_acquired_per_thread[thd_idx] =
            num_of_locks_acquired_per_thread;
      }
      prev_num_of_locks_acquired = num_of_locks_acquired;
      if (i == 0) {
        measured_locks_acquired = num_of_locks_acquired;
      }
      if (i == execution_time_sec_ - 1) {
        measured_locks_acquired =
            num_of_locks_acquired - measured_locks_acquired;
        // Skip the first second, as threads are warming up
        auto measured_execution_time_sec = execution_time_sec_ - 1;
        if (measured_execution_time_sec > 0) {
          printf("measured_num_of_locks_acquired: %" PRId64 "\n",
                 measured_locks_acquired / (measured_execution_time_sec));
        }
      }
    }

    shutdown_ = true;
    for (auto& t : threads_) {
      t.join();
    }

    // validate values against counters
    for (uint32_t i = 0; i < key_count_; i++) {
      ASSERT_TRUE_WITH_MSG(counters_[i]->load() == values_[i],
                           "Exclusive lock guarantee is violated.");
    }

    ASSERT_TRUE_WITH_MSG(num_of_locks_acquired_.load() >= 0,
                         "No lock were acquired at all");
    printf("num_of_locks_acquired: %" PRId64 "\n",
           num_of_locks_acquired_.load());

    std::string errmsg;
    auto no_lock_held = verifyNoLocksHeld(locker_, errmsg);
    ASSERT_TRUE_WITH_MSG(no_lock_held, errmsg);
  }

 private:
  // test configuration
  Env* env_;
  TransactionDBOptions txndb_opt_;
  std::shared_ptr<LockManager> locker_;

  TransactionDB* db_;
  TransactionOptions txn_opt_;

  uint32_t thread_count_;
  uint32_t key_count_;
  uint32_t max_num_keys_to_lock_per_txn_;
  uint32_t execution_time_sec_;
  LockTypeToTest lock_type_;
  bool allow_non_deadlock_error_;
  uint32_t max_sleep_after_lock_acquisition_ms_;

  // In some of the test run, due to debug or ASAN build and short lock timeout,
  // a thread may not be able to acquire any lock within a second. So skip this
  // assertion by default. However, this could be useful for quickly detecting
  // stuck thread, when running locally with longer timeout.
  bool enable_per_thread_lock_count_assertion_;

  // Internal test variables

  bool enable_lock_status_validation_;
  std::vector<std::thread> threads_;
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

  // test statistics
  std::atomic_int64_t num_of_locks_acquired_ = 0;
  std::atomic_int64_t num_of_shared_locks_acquired_ = 0;
  std::atomic_int64_t num_of_exclusive_locks_acquired_ = 0;
  std::atomic_int64_t num_of_deadlock_detected_ = 0;
  std::vector<std::unique_ptr<std::atomic_int64_t>>
      num_of_locks_acquired_per_thread_;
};

}  // namespace ROCKSDB_NAMESPACE

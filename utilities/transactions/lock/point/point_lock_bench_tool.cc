//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#ifdef GFLAGS

#include <cstddef>
#include <cstdio>
#include <iostream>
#include <memory>
#include <sstream>

#include "db/db_impl/db_impl.h"
#include "rocksdb/convenience.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/utilities/transaction_db.h"
#include "util/gflags_compat.h"
#include "util/random.h"
#include "utilities/transactions/lock/point/point_lock_manager.h"
#include "utilities/transactions/pessimistic_transaction_db.h"

using GFLAGS_NAMESPACE::ParseCommandLineFlags;

namespace ROCKSDB_NAMESPACE {

constexpr bool kDebugLog = false;

#define DEBUG_LOG(...)            \
  if (kDebugLog) {                \
    fprintf(stderr, __VA_ARGS__); \
    fflush(stderr);               \
  }

#define DEBUG_LOG_PREFIX(format, ...) \
  DEBUG_LOG("Thd %lu Txn %lu " format, thd_idx, txn_id, ##__VA_ARGS__);

enum class LockTypeToTest : int8_t {
  EXCLUSIVE_ONLY = 0,
  SHARED_ONLY = 1,
  EXCLUSIVE_AND_SHARED = 2,
};

struct PointLockCorrectnessCheckTestFLAGS_ {
  bool is_per_key_point_lock_manager;
  size_t thread_count;
  size_t key_count;
  size_t max_num_keys_to_lock_per_txn;
  size_t execution_time_sec;
  LockTypeToTest lock_type;
  int64_t lock_timeout_us;
  int64_t lock_expiration_us;
  bool allow_non_deadlock_error;
  // to simulate some useful work
  bool sleep_after_lock_acquisition;
};

DEFINE_string(db_dir, "/tmp/point_lock_manager_test",
              "DB path for running the benchmark");
DEFINE_uint32(stripe_count, 16, "Number of stripes in point lock manager");
DEFINE_bool(is_per_key_point_lock_manager, false,
            "Use PerKeyPointLockManager or PointLockManager");
DEFINE_uint32(thread_count, 64,
              "Number of threads to acquire release locks concurrently");
DEFINE_uint32(key_count, 16, "Number of keys to acquire release locks upon");
DEFINE_uint32(max_num_keys_to_lock_per_txn, 8,
              "Max Number of keys to lock in a transaction");
DEFINE_uint32(execution_time_sec, 10,
              "Number of seconds to execute the benchmark");
DEFINE_int32(lock_type, 0,
             "Lock type to test, 0: exclusive lock only; 1: shared lock only; "
             "2: both shared and exclusive locks");
DEFINE_int64(lock_timeout_ms, 1000,
             "Lock acquisition request timeout in milliseconds.");
DEFINE_int64(lock_expiration_ms, 100,
             "Acquired Lock expiration time in milliseconds.");
DEFINE_bool(allow_non_deadlock_error, false,
            "Allow returned error code other than deadlock, such as timeout.");
DEFINE_uint32(
    max_sleep_ms_after_lock_acquisition, 0,
    "Max number of milliseconds to sleep after acquiring all the locks in the "
    "transaction. The actuall sleep time will be randomized from 0 to max. It "
    "is used to simulate some useful work performed.");

#define ASSERT_TRUE(expr, errmsg)                                    \
  if (!(expr)) {                                                     \
    std::cerr << "Assert true failed with error message: " << errmsg \
              << std::endl;                                          \
    abort();                                                         \
  }

#define ASSERT_OK(s)                                          \
  if (!(s).ok()) {                                            \
    std::cerr << "Failed with " << s.ToString() << std::endl; \
    abort();                                                  \
  }

#define ASSERT_INFO_TRUE(X)                                     \
  ASSERT_TRUE((X), "Thd " + std::to_string(thd_idx) + " Txn " + \
                           std::to_string(txn_id) + " key "     \
                       << key)

#define ASSERT_INFO_EQ(X, Y) ASSERT_INFO_TRUE((X) == (Y))

class PointLockManagerBenchmark {
 public:
  void SetUp() {
    env_ = Env::Default();
    env_->CreateDir(FLAGS_db_dir);

    Options opt;
    opt.create_if_missing = true;
    txndb_opt_.num_stripes = FLAGS_stripe_count;

    auto s = TransactionDB::Open(opt, txndb_opt_, FLAGS_db_dir, &db_);
    ASSERT_OK(s);

    if (FLAGS_is_per_key_point_lock_manager) {
      printf("use PerKeyPointLockManager\n");
      locker_.reset(new PerKeyPointLockManager(
          static_cast<PessimisticTransactionDB*>(db_), txndb_opt_));
    } else {
      printf("use PointLockManager\n");
      locker_.reset(new PointLockManager(
          static_cast<PessimisticTransactionDB*>(db_), txndb_opt_));
    }

    txn_opt_.deadlock_detect = true;
    txn_opt_.lock_timeout = FLAGS_lock_timeout_ms;
    txn_opt_.expiration = FLAGS_lock_expiration_ms;

    values_.resize(FLAGS_key_count, 0);
    exclusive_lock_status_.resize(FLAGS_key_count, 0);

    // init counters and values
    for (size_t i = 0; i < FLAGS_key_count; i++) {
      counters_.emplace_back(std::make_unique<std::atomic_int>(0));
      shared_lock_count_.emplace_back(std::make_unique<std::atomic_int>(0));
    }
  }

  void TearDown() {
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
    ASSERT_TRUE(lock_status.empty(), std::to_string(lock_status.size()) +
                                         " locks were held at the end. " +
                                         ss.str());

    delete db_;
    auto s = DestroyDir(env_, FLAGS_db_dir);
    ASSERT_OK(s);
  }

  PessimisticTransaction* NewTxn(
      TransactionOptions txn_opt = TransactionOptions()) {
    Transaction* txn = db_->BeginTransaction(WriteOptions(), txn_opt);
    return static_cast<PessimisticTransaction*>(txn);
  }

  void BenchmarkPointLockManager();

 protected:
  Env* env_;
  TransactionDBOptions txndb_opt_;
  std::shared_ptr<LockManager> locker_;

  TransactionDB* db_;

  TransactionOptions txn_opt_;
  std::vector<std::thread> threads_;
  std::atomic_int num_of_locks_acquired_ = 0;
  std::atomic_int num_of_shared_locks_acquired_ = 0;
  std::atomic_int num_of_exclusive_locks_acquired_ = 0;
  std::atomic_int num_of_deadlock_detected_ = 0;

  // Lock status only tracks whether
  std::vector<std::unique_ptr<std::atomic_int>> counters_;
  // values are read/write protected by the locks
  std::vector<int> values_;
  // use int64_t for boolean to track exclusive lock status. vector<bool> does
  // something special underneath, causes consistency issue.
  std::vector<int64_t> exclusive_lock_status_;
  // use counter to track number of shared locks to track shared lock status
  std::vector<std::unique_ptr<std::atomic_int>> shared_lock_count_;

  // shutdown flag
  std::atomic_bool shutdown_ = false;
};

class MockColumnFamilyHandle : public ColumnFamilyHandle {
 public:
  explicit MockColumnFamilyHandle(ColumnFamilyId cf_id) : cf_id_(cf_id) {}

  ~MockColumnFamilyHandle() override {}

  const std::string& GetName() const override { return name_; }

  ColumnFamilyId GetID() const override { return cf_id_; }

  Status GetDescriptor(ColumnFamilyDescriptor*) override {
    return Status::OK();
  }

  const Comparator* GetComparator() const override {
    return BytewiseComparator();
  }

 private:
  ColumnFamilyId cf_id_;
  std::string name_ = "MockCF";
};

void PointLockManagerBenchmark::BenchmarkPointLockManager() {
  // Verify lock guarantee. Exclusive lock provide unique access guarantee.
  // Shared lock provide shared access guarantee.
  // Create multiple threads. Each try to grab a lock with random type on
  // random key. On exclusive lock, bump the value by 1. Meantime, update a
  // global counter for validation On shared lock, read the value and compare
  // it against the global counter to make sure its value matches At the end,
  // validate the value against the global counter.

  MockColumnFamilyHandle cf(1);
  locker_->AddColumnFamily(&cf);

  for (size_t thd_idx = 0; thd_idx < FLAGS_thread_count; thd_idx++) {
    threads_.emplace_back([this, thd_idx]() {
      while (!shutdown_) {
        auto txn = NewTxn(txn_opt_);
        auto txn_id = txn->GetID();
        DEBUG_LOG_PREFIX("new txn\n");
        std::vector<std::pair<uint32_t, bool>> locked_key_with_types;
        // try to grab a random number of locks
        auto num_key_to_lock =
            Random::GetTLSInstance()->Uniform(
                static_cast<uint32_t>(FLAGS_max_num_keys_to_lock_per_txn)) +
            1;
        Status s;

        for (uint32_t j = 0; j < num_key_to_lock; j++) {
          uint32_t key = 0;
          key = Random::GetTLSInstance()->Uniform(
              static_cast<uint32_t>(FLAGS_key_count));
          auto key_str = std::to_string(key);
          bool isUpgrade = false;
          bool isDowngrade = false;

          // Decide lock type
          auto exclusive_lock_type = Random::GetTLSInstance()->OneIn(2);
          // check whether a lock on the same key is already held
          auto it = std::find_if(
              locked_key_with_types.begin(), locked_key_with_types.end(),
              [&key](std::pair<uint32_t, bool>& e) { return e.first == key; });
          auto lock_type_to_test = static_cast<LockTypeToTest>(FLAGS_lock_type);
          if (it != locked_key_with_types.end()) {
            // a lock on the same key is already held.
            if (lock_type_to_test == LockTypeToTest::EXCLUSIVE_AND_SHARED) {
              // if test both shared and exclusive locks, switch their type
              if (it->second == false) {
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

          if (!FLAGS_allow_non_deadlock_error) {
            if (isDowngrade) {
              // Before downgrade, validate the lock is in exlusive status
              // This could not be done after downgrade, as another thread could
              // take a shared lock and update lock status
              ASSERT_INFO_TRUE(exclusive_lock_status_[key]);
              ASSERT_INFO_EQ(*shared_lock_count_[key], 0);
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
                it->second = true;
              } else {
                locked_key_with_types.emplace_back(key, exclusive_lock_type);
              }
              num_of_exclusive_locks_acquired_++;
            } else {
              if (isDowngrade) {
                it->second = false;
              } else {
                // Could not validate status was not in exclusive status, as
                // the lock could be downgraded by another thread.
                locked_key_with_types.emplace_back(key, exclusive_lock_type);
              }
              num_of_shared_locks_acquired_++;
            }
            num_of_locks_acquired_++;

            // Check and update global lock status
            if (!FLAGS_allow_non_deadlock_error) {
              // Validate lock status, if deadlock is the only allowed error.
              // otherwise, lock could be expired and stolen
              if (exclusive_lock_type) {
                // validate the lock is not in exclusive status
                ASSERT_INFO_TRUE(!exclusive_lock_status_[key]);
                if (isUpgrade) {
                  // validate the lock is in shared status and only had one
                  // shared lock
                  ASSERT_INFO_EQ(*shared_lock_count_[key], 1);
                  shared_lock_count_[key]->fetch_sub(1);
                } else {
                  ASSERT_INFO_EQ(*shared_lock_count_[key], 0);
                }
                // update the lock status
                exclusive_lock_status_[key] = 1;
              } else {
                shared_lock_count_[key]->fetch_add(1);
                ASSERT_INFO_TRUE(!exclusive_lock_status_[key]);
              }
            }
          } else {
            if (!FLAGS_allow_non_deadlock_error) {
              ASSERT_INFO_TRUE(s.IsDeadlock());
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

        if (FLAGS_max_sleep_ms_after_lock_acquisition != 0 && s.ok()) {
          auto sleep_time_us = Random::GetTLSInstance()->Uniform(
              static_cast<uint32_t>(FLAGS_max_sleep_ms_after_lock_acquisition));
          std::this_thread::sleep_for(std::chrono::milliseconds(sleep_time_us));
        }

        // release all locks
        for (auto& key_type : locked_key_with_types) {
          auto key = key_type.first;
          ASSERT_INFO_TRUE(key < FLAGS_key_count);
          // Check global lock status
          if (!FLAGS_allow_non_deadlock_error) {
            ASSERT_INFO_EQ(counters_[key]->load(), values_[key]);
            auto exclusive = key_type.second;
            if (exclusive) {
              // exclusive lock
              // bump the value by 1
              (*counters_[key])++;
              values_[key]++;
              DEBUG_LOG_PREFIX("bump key %u by 1 to %d\n", key, values_[key]);
              ASSERT_INFO_EQ(counters_[key]->load(), values_[key]);
            }
            // Validate lock status, if deadlock is the only allowed error.
            // otherwise, lock could be expired and stolen
            if (exclusive) {
              // exclusive lock
              ASSERT_INFO_TRUE(exclusive_lock_status_[key]);
              ASSERT_INFO_EQ(*shared_lock_count_[key], 0);
              exclusive_lock_status_[key] = 0;
            } else {
              // shared lock
              ASSERT_INFO_TRUE(!exclusive_lock_status_[key]);
              ASSERT_INFO_TRUE(shared_lock_count_[key]->fetch_sub(1) >= 1);
            }
          }
          DEBUG_LOG_PREFIX("release lock %u\n", key);
          locker_->UnLock(txn, 1, std::to_string(key), env_);
        }
        delete txn;
      }
    });
  }

  // run test for a few seconds
  // print progress
  auto prev_num_of_locks_acquired = num_of_locks_acquired_.load();
  for (size_t i = 0; i < FLAGS_execution_time_sec; i++) {
    std::this_thread::sleep_for(std::chrono::seconds(1));
    auto num_of_locks_acquired = num_of_locks_acquired_.load();
    DEBUG_LOG("num_of_locks_acquired: %d\n", num_of_locks_acquired);
    DEBUG_LOG("num_of_exclusive_locks_acquired: %d\n",
              num_of_exclusive_locks_acquired_.load());
    DEBUG_LOG("num_of_shared_locks_acquired: %d\n",
              num_of_shared_locks_acquired_.load());
    DEBUG_LOG("num_of_deadlock_detected: %d\n",
              num_of_deadlock_detected_.load());
    ASSERT_TRUE(num_of_locks_acquired > prev_num_of_locks_acquired,
                "No locks were acquired in the last 1 second");
    prev_num_of_locks_acquired = num_of_locks_acquired;
  }

  shutdown_ = true;
  for (auto& t : threads_) {
    t.join();
  }

  // validate values against counters
  for (size_t i = 0; i < FLAGS_key_count; i++) {
    ASSERT_TRUE(counters_[i]->load() == values_[i],
                "Exclusive lock guarantee is violated.");
  }

  ASSERT_TRUE(num_of_locks_acquired_.load() >= 0,
              "No lock were acquired at all");
  printf("num_of_locks_acquired: %d\n", num_of_locks_acquired_.load());
}

int point_lock_bench_tool(int argc, char** argv) {
  ParseCommandLineFlags(&argc, &argv, true);

  // Iterate through all flags and print their values
  std::vector<gflags::CommandLineFlagInfo> all_flags;
  gflags::GetAllFlags(&all_flags);  // Get information about all flags

  for (const auto& flag : all_flags) {
    if (flag.filename.find("point_lock_bench_tool.cc") != std::string::npos) {
      std::cout << "-" << flag.name << "=";
      if (flag.type == "bool") {
        std::cout << (gflags::GetCommandLineFlagInfoOrDie(flag.name.c_str())
                                  .current_value == "true"
                          ? "true"
                          : "false");
      } else {
        std::cout << gflags::GetCommandLineFlagInfoOrDie(flag.name.c_str())
                         .current_value;
      }
      std::cout << " ";
    }
  }
  std::cout << std::endl;

  PointLockManagerBenchmark benchmark;

  benchmark.SetUp();
  benchmark.BenchmarkPointLockManager();
  benchmark.TearDown();
  return 0;
}

}  // namespace ROCKSDB_NAMESPACE

#endif  // GFLAGS

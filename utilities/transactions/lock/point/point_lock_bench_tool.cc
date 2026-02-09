//  Copyright (c) Meta Platforms, Inc. and affiliates.
//
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#ifdef GFLAGS

#include <cstdio>
#include <iostream>
#include <memory>

#include "port/stack_trace.h"
#include "rocksdb/convenience.h"
#include "rocksdb/env.h"
#include "rocksdb/utilities/transaction_db.h"
#include "util/gflags_compat.h"
#include "utilities/transactions/lock/point/point_lock_manager.h"
#include "utilities/transactions/lock/point/point_lock_validation_test_runner.h"
#include "utilities/transactions/pessimistic_transaction_db.h"

using GFLAGS_NAMESPACE::ParseCommandLineFlags;

namespace ROCKSDB_NAMESPACE {

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
DEFINE_uint32(lock_type, 2,
              "Lock type to test, 0: exclusive lock only; 1: shared lock only; "
              "2: both shared and exclusive locks");
DEFINE_int64(lock_timeout_ms, 1000,
             "Lock acquisition request timeout in milliseconds.");
DEFINE_int64(deadlock_timeout_us, 500,
             "DeadLock detection timeout in microseconds.");
DEFINE_int64(lock_expiration_ms, 100,
             "Acquired Lock expiration time in milliseconds.");
DEFINE_bool(allow_non_deadlock_error, true,
            "Allow returned error code other than deadlock, such as timeout.");
DEFINE_uint32(
    max_sleep_after_lock_acquisition_ms, 5,
    "Max number of milliseconds to sleep after acquiring all the locks in the "
    "transaction. The actuall sleep time will be randomized from 0 to max. It "
    "is used to simulate some useful work performed.");
DEFINE_bool(check_thread_stuck, false,
            "Check thread periodically to see whether they are stuck or not. "
            "This is useful for detecting stuck transaction quickly. But it "
            "could have false-positive when running with ASAN or running with "
            "high thread count on a small number of CPUs");

namespace {  // anonymous namespace

class PointLockManagerBenchmark {
 public:
  PointLockManagerBenchmark() {
    env_ = Env::Default();
    env_->CreateDir(FLAGS_db_dir);

    Options opt;
    opt.create_if_missing = true;
    txndb_opt_.num_stripes = FLAGS_stripe_count;

    db_ = nullptr;

    auto s = TransactionDB::Open(opt, txndb_opt_, FLAGS_db_dir, &db_);
    ASSERT_OK(s);

    if (FLAGS_is_per_key_point_lock_manager) {
      locker_ = std::make_shared<PerKeyPointLockManager>(
          static_cast<PessimisticTransactionDB*>(db_), txndb_opt_);
    } else {
      locker_ = std::make_shared<PointLockManager>(
          static_cast<PessimisticTransactionDB*>(db_), txndb_opt_);
    }

    txn_opt_.deadlock_detect = true;
    txn_opt_.lock_timeout = FLAGS_lock_timeout_ms;
    txn_opt_.deadlock_timeout_us = FLAGS_deadlock_timeout_us;
    txn_opt_.expiration = FLAGS_lock_expiration_ms;
  }

  // Disable copy and assignment
  PointLockManagerBenchmark(const PointLockManagerBenchmark&) = delete;
  PointLockManagerBenchmark& operator=(const PointLockManagerBenchmark&) =
      delete;
  PointLockManagerBenchmark(PointLockManagerBenchmark&&) = delete;
  PointLockManagerBenchmark& operator=(PointLockManagerBenchmark&&) = delete;

  ~PointLockManagerBenchmark() {
    delete db_;
    auto s = DestroyDir(env_, FLAGS_db_dir);
    ASSERT_OK(s);
  }

  void run() {
    PointLockValidationTestRunner test_runner(
        env_, txndb_opt_, locker_, db_, txn_opt_, FLAGS_thread_count,
        FLAGS_key_count, FLAGS_max_num_keys_to_lock_per_txn,
        FLAGS_execution_time_sec, static_cast<LockTypeToTest>(FLAGS_lock_type),
        FLAGS_allow_non_deadlock_error,
        FLAGS_max_sleep_after_lock_acquisition_ms, FLAGS_check_thread_stuck);
    test_runner.run();
  }

 private:
  Env* env_;
  TransactionDBOptions txndb_opt_;
  std::shared_ptr<LockManager> locker_;
  TransactionDB* db_;
  TransactionOptions txn_opt_;
};

}  // anonymous namespace

int point_lock_bench_tool(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ParseCommandLineFlags(&argc, &argv, true);

  // Print test configuration
  std::vector<gflags::CommandLineFlagInfo> all_flags;
  gflags::GetAllFlags(&all_flags);

  for (const auto& flag : all_flags) {
    // only show the flags defined in this file
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

  // Run the benchmark
  PointLockManagerBenchmark benchmark;
  benchmark.run();

  return 0;
}

}  // namespace ROCKSDB_NAMESPACE

#endif  // GFLAGS

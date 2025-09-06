//  Copyright (c) Meta Platforms, Inc. and affiliates.
//
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "utilities/transactions/lock/point/point_lock_manager_test.h"
#include "utilities/transactions/lock/point/point_lock_validation_test_runner.h"

namespace ROCKSDB_NAMESPACE {

struct PointLockCorrectnessCheckTestParam {
  bool is_per_key_point_lock_manager;
  uint32_t thread_count;
  uint32_t key_count;
  uint32_t max_num_keys_to_lock_per_txn;
  uint32_t execution_time_sec;
  LockTypeToTest lock_type;
  int64_t lock_timeout_us;
  int64_t lock_expiration_us;
  bool allow_non_deadlock_error;
  // to simulate some useful work
  uint32_t max_sleep_after_lock_acquisition_ms;
};

class PointLockCorrectnessCheckTest
    : public PointLockManagerTest,
      public testing::WithParamInterface<PointLockCorrectnessCheckTestParam> {
 public:
  void SetUp() override {
    init();
    auto const& param = GetParam();
    auto per_key_lock_manager = param.is_per_key_point_lock_manager;
    if (per_key_lock_manager) {
      locker_ = std::make_shared<PerKeyPointLockManager>(
          static_cast<PessimisticTransactionDB*>(db_), txndb_opt_);
    } else {
      locker_ = std::make_shared<PointLockManager>(
          static_cast<PessimisticTransactionDB*>(db_), txndb_opt_);
    }

    txn_opt_.deadlock_detect = true;
    txn_opt_.lock_timeout = param.lock_timeout_us;
    txn_opt_.expiration = param.lock_expiration_us;
  }

 protected:
  TransactionOptions txn_opt_;
};

TEST_P(PointLockCorrectnessCheckTest, LockCorrectnessValidation) {
  auto const& param = GetParam();
  PointLockValidationTestRunner test_runner(
      env_, txndb_opt_, locker_, db_, txn_opt_, param.thread_count,
      param.key_count, param.max_num_keys_to_lock_per_txn,
      param.execution_time_sec, static_cast<LockTypeToTest>(param.lock_type),
      param.allow_non_deadlock_error,
      param.max_sleep_after_lock_acquisition_ms);
  test_runner.run();
}

constexpr auto X_S_LOCK = LockTypeToTest::EXCLUSIVE_AND_SHARED;
constexpr auto X_LOCK = LockTypeToTest::EXCLUSIVE_ONLY;
constexpr auto S_LOCK = LockTypeToTest::SHARED_ONLY;

INSTANTIATE_TEST_CASE_P(
    PointLockCorrectnessCheckTestSuite, PointLockCorrectnessCheckTest,
    ::testing::ValuesIn(std::vector<PointLockCorrectnessCheckTestParam>{
        // 2 second timeout and no expiration simulates myrocks default
        // configuration
        {true, 16, 16, 8, 10, X_S_LOCK, 2000, -1, true, 0},
        {false, 16, 16, 8, 10, X_S_LOCK, 2000, -1, true, 0},
        {true, 16, 16, 8, 10, X_LOCK, 2000, -1, true, 0},
        {false, 16, 16, 8, 10, X_LOCK, 2000, -1, true, 0},
        {true, 16, 16, 8, 10, S_LOCK, 2000, -1, true, 0},
        {false, 16, 16, 8, 10, S_LOCK, 2000, -1, true, 0},
        // short timeout and expiration to test lock stealing
        {true, 16, 16, 8, 10, X_S_LOCK, 10, 10, true, 10},
        {false, 16, 16, 8, 10, X_S_LOCK, 10, 10, true, 10},
        {true, 16, 16, 8, 10, X_LOCK, 10, 10, true, 10},
        {false, 16, 16, 8, 10, X_LOCK, 10, 10, true, 10},
        {true, 16, 16, 8, 10, S_LOCK, 10, 10, true, 10},
        {false, 16, 16, 8, 10, S_LOCK, 10, 10, true, 10},
        // long timeout and expiration to test deadlock detection without
        // timeout
        {true, 16, 16, 8, 10, X_S_LOCK, 100000, 100000, false, 0},
        {false, 16, 16, 8, 10, X_S_LOCK, 100000, 100000, false, 0},
        {true, 16, 16, 8, 10, X_LOCK, 100000, 100000, false, 0},
        {false, 16, 16, 8, 10, X_LOCK, 100000, 100000, false, 0},
        {true, 16, 16, 8, 10, S_LOCK, 100000, 100000, false, 0},
        {false, 16, 16, 8, 10, S_LOCK, 100000, 100000, false, 0},
        // Low lock contention
        {true, 4, 1024 * 1024, 2, 10, S_LOCK, 100000, 100000, false, 0},
        {false, 4, 1024 * 1024, 2, 10, S_LOCK, 100000, 100000, false, 0},
    }));

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

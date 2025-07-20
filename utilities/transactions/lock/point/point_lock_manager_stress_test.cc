//  Copyright (c) Meta Platforms, Inc. and affiliates.
//
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "utilities/transactions/lock/point/point_lock_manager_test.h"
#include "utilities/transactions/lock/point/point_lock_manager_test_common.h"
#include "utilities/transactions/lock/point/point_lock_validation_test_runner.h"

namespace ROCKSDB_NAMESPACE {

struct PointLockCorrectnessCheckTestParam {
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

class PointLockCorrectnessCheckTest
    : public PointLockManagerTest,
      public testing::WithParamInterface<PointLockCorrectnessCheckTestParam> {
 public:
  void SetUp() override {
    init();
    auto const& param = GetParam();
    auto per_key_lock_manager = param.is_per_key_point_lock_manager;
    if (per_key_lock_manager) {
      locker_.reset(new PerKeyPointLockManager(
          static_cast<PessimisticTransactionDB*>(db_), txndb_opt_));
    } else {
      locker_.reset(new PointLockManager(
          static_cast<PessimisticTransactionDB*>(db_), txndb_opt_));
    }

    txn_opt_.deadlock_detect = true;
    txn_opt_.lock_timeout = param.lock_timeout_us;
    txn_opt_.expiration = param.lock_expiration_us;
  }

 protected:
  TransactionOptions txn_opt_;
};

#define ASSERT_INFO(X) \
  ASSERT_##X << "Thd " << thd_idx << " Txn " << txn_id << " key " << key;

TEST_P(PointLockCorrectnessCheckTest, LockCorrectnessValidation) {
  auto const& param = GetParam();
  std::unique_ptr<PointLockValidationTestRunner> test_runner =
      std::make_unique<PointLockValidationTestRunner>(
          env_, txndb_opt_, locker_, db_, txn_opt_, param.thread_count,
          param.key_count, param.max_num_keys_to_lock_per_txn,
          param.execution_time_sec,
          static_cast<LockTypeToTest>(param.lock_type),
          param.allow_non_deadlock_error, param.sleep_after_lock_acquisition);
  test_runner->run();
}

INSTANTIATE_TEST_CASE_P(
    PointLockCorrectnessCheckTestSuite, PointLockCorrectnessCheckTest,
    ::testing::ValuesIn(std::vector<PointLockCorrectnessCheckTestParam>{
        // 2 second timeout and no expiration simulating mysql default
        // configuration
        {true, 64, 16, 8, 10, LockTypeToTest::EXCLUSIVE_AND_SHARED, 2000, -1,
         true, false},
        {false, 64, 16, 8, 10, LockTypeToTest::EXCLUSIVE_AND_SHARED, 2000, -1,
         true, false},
        {true, 64, 16, 8, 10, LockTypeToTest::EXCLUSIVE_ONLY, 2000, -1, true,
         false},
        {false, 64, 16, 8, 10, LockTypeToTest::EXCLUSIVE_ONLY, 2000, -1, true,
         false},
        {true, 64, 16, 8, 10, LockTypeToTest::SHARED_ONLY, 2000, -1, true,
         false},
        {false, 64, 16, 8, 10, LockTypeToTest::SHARED_ONLY, 2000, -1, true,
         false},
        // short timeout and expiration to test lock stealing
        {true, 64, 16, 8, 10, LockTypeToTest::EXCLUSIVE_AND_SHARED, 10, 10,
         true, true},
        {false, 64, 16, 8, 10, LockTypeToTest::EXCLUSIVE_AND_SHARED, 10, 10,
         true, true},
        {true, 64, 16, 8, 10, LockTypeToTest::EXCLUSIVE_ONLY, 10, 10, true,
         true},
        {false, 64, 16, 8, 10, LockTypeToTest::EXCLUSIVE_ONLY, 10, 10, true,
         true},
        {true, 64, 16, 8, 10, LockTypeToTest::SHARED_ONLY, 10, 10, true, true},
        {false, 64, 16, 8, 10, LockTypeToTest::SHARED_ONLY, 10, 10, true, true},
        // long timeout and expiration to test deadlock detection without
        // timeout
        {true, 64, 16, 8, 10, LockTypeToTest::EXCLUSIVE_AND_SHARED,
         kLongTxnTimeoutMs, kLongTxnTimeoutMs, false, false},
        {false, 64, 16, 8, 10, LockTypeToTest::EXCLUSIVE_AND_SHARED,
         kLongTxnTimeoutMs, kLongTxnTimeoutMs, false, false},
        {true, 64, 16, 8, 10, LockTypeToTest::EXCLUSIVE_ONLY, kLongTxnTimeoutMs,
         kLongTxnTimeoutMs, false, false},
        {false, 64, 16, 8, 10, LockTypeToTest::EXCLUSIVE_ONLY,
         kLongTxnTimeoutMs, kLongTxnTimeoutMs, false, false},
        {true, 64, 16, 8, 10, LockTypeToTest::SHARED_ONLY, kLongTxnTimeoutMs,
         kLongTxnTimeoutMs, false, false},
        {false, 64, 16, 8, 10, LockTypeToTest::SHARED_ONLY, kLongTxnTimeoutMs,
         kLongTxnTimeoutMs, false, false},
        // Low lock contention
        {true, 16, 1024 * 1024, 2, 10, LockTypeToTest::SHARED_ONLY,
         kLongTxnTimeoutMs, kLongTxnTimeoutMs, false, false},
        {false, 16, 1024 * 1024, 2, 10, LockTypeToTest::SHARED_ONLY,
         kLongTxnTimeoutMs, kLongTxnTimeoutMs, false, false},
    }));

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

//  Copyright (c) Meta Platforms, Inc. and affiliates.
//
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include "file/file_util.h"
#include "rocksdb/utilities/transaction_db.h"
#include "test_util/testharness.h"
#include "utilities/transactions/lock/point/point_lock_manager.h"
#include "utilities/transactions/pessimistic_transaction_db.h"

namespace ROCKSDB_NAMESPACE {

class PointLockManagerTest : public testing::Test {
 public:
  void init() {
    env_ = Env::Default();
    db_dir_ = test::PerThreadDBPath("point_lock_manager_test");
    ASSERT_OK(env_->CreateDir(db_dir_));

    Options opt;
    opt.create_if_missing = true;
    // Reduce the number of stripes to 4 to increase contention in test
    txndb_opt_.num_stripes = 4;
    txndb_opt_.transaction_lock_timeout = 0;

    ASSERT_OK(TransactionDB::Open(opt, txndb_opt_, db_dir_, &db_));

    wait_sync_point_name_ = "PointLockManager::AcquireWithTimeout:WaitingTxn";
  }

  void SetUp() override {
    init();
    // CAUTION: This test creates a separate lock manager object (right, NOT
    // the one that the TransactionDB is using!), and runs tests on it.
    locker_.reset(new PointLockManager(
        static_cast<PessimisticTransactionDB*>(db_), txndb_opt_));
  }

  void TearDown() override {
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
    ASSERT_TRUE(lock_status.empty())
        << lock_status.size() << " locks were held at the end of the test"
        << ss.str();

    delete db_;
    EXPECT_OK(DestroyDir(env_, db_dir_));
  }

  PessimisticTransaction* NewTxn(
      TransactionOptions txn_opt = TransactionOptions()) {
    Transaction* txn = db_->BeginTransaction(WriteOptions(), txn_opt);
    return static_cast<PessimisticTransaction*>(txn);
  }

 protected:
  Env* env_;
  TransactionDBOptions txndb_opt_;
  std::shared_ptr<LockManager> locker_;
  const char* wait_sync_point_name_;
  friend void PointLockManagerTestExternalSetup(PointLockManagerTest*);
  friend void PerLockPointLockManagerAnyLockManagerTestSetup(
      PointLockManagerTest*);

  std::string db_dir_;
  TransactionDB* db_;
};

// Try to block until transaction enters waiting state.
// However due to timing, it could fail, so return true if succeeded, false
// otherwise.
bool TryBlockUntilWaitingTxn(const char* sync_point_name, port::Thread& t,
                             std::function<void()> function) {
  std::atomic<bool> reached(false);
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      sync_point_name, [&](void* /*arg*/) { reached.store(true); });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  // As the lifetime of the complete variable could go beyond the scope of this
  // function, so we wrap it in a shared_ptr, and copy it into the lambda
  std::shared_ptr<std::atomic<bool>> complete =
      std::make_shared<std::atomic<bool>>(false);
  t = port::Thread([complete, &function]() {
    function();
    complete->store(true);
  });

  auto ret = false;

  while (true) {
    if (complete->load()) {
      // function completed, before sync point was reached, return false
      t.join();
      ret = false;
      break;
    }
    if (reached.load()) {
      // sync point was reached before function completed, return true
      ret = true;
      break;
    }
  }

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearAllCallBacks();
  return ret;
}

void BlockUntilWaitingTxn(const char* sync_point_name, port::Thread& t,
                          std::function<void()> f) {
  std::atomic<bool> reached(false);
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      sync_point_name, [&](void* /*arg*/) { reached.store(true); });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  t = port::Thread(f);

  // timeout after 30 seconds, so test does not hang forever
  // 30 seconds should be enough for the test to reach the expected state
  // without causing too much flakiness
  for (int i = 0; i < 3000; i++) {
    if (reached.load()) {
      break;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }

  ASSERT_TRUE(reached.load());
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearAllCallBacks();
}

}  // namespace ROCKSDB_NAMESPACE

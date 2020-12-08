//  Copyright (c) 2020-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#ifndef ROCKSDB_LITE

#include "utilities/transactions/lock/point/point_lock_manager_test.h"

namespace ROCKSDB_NAMESPACE {

// This test is not applicable for Range Lock manager as Range Lock Manager
// operates on Column Families, not their ids.
TEST_F(PointLockManagerTest, LockNonExistingColumnFamily) {
  MockColumnFamilyHandle cf(1024);
  locker_->RemoveColumnFamily(&cf);
  auto txn = NewTxn();
  auto s = locker_->TryLock(txn, 1024, "k", env_, true);
  ASSERT_TRUE(s.IsInvalidArgument());
  ASSERT_STREQ(s.getState(), "Column family id not found: 1024");
  delete txn;
}

TEST_F(PointLockManagerTest, LockStatus) {
  MockColumnFamilyHandle cf1(1024), cf2(2048);
  locker_->AddColumnFamily(&cf1);
  locker_->AddColumnFamily(&cf2);

  auto txn1 = NewTxn();
  ASSERT_OK(locker_->TryLock(txn1, 1024, "k1", env_, true));
  ASSERT_OK(locker_->TryLock(txn1, 2048, "k1", env_, true));

  auto txn2 = NewTxn();
  ASSERT_OK(locker_->TryLock(txn2, 1024, "k2", env_, false));
  ASSERT_OK(locker_->TryLock(txn2, 2048, "k2", env_, false));

  auto s = locker_->GetPointLockStatus();
  ASSERT_EQ(s.size(), 4u);
  for (uint32_t cf_id : {1024, 2048}) {
    ASSERT_EQ(s.count(cf_id), 2u);
    auto range = s.equal_range(cf_id);
    for (auto it = range.first; it != range.second; it++) {
      ASSERT_TRUE(it->second.key == "k1" || it->second.key == "k2");
      if (it->second.key == "k1") {
        ASSERT_EQ(it->second.exclusive, true);
        ASSERT_EQ(it->second.ids.size(), 1u);
        ASSERT_EQ(it->second.ids[0], txn1->GetID());
      } else if (it->second.key == "k2") {
        ASSERT_EQ(it->second.exclusive, false);
        ASSERT_EQ(it->second.ids.size(), 1u);
        ASSERT_EQ(it->second.ids[0], txn2->GetID());
      }
    }
  }

  // Cleanup
  locker_->UnLock(txn1, 1024, "k1", env_);
  locker_->UnLock(txn1, 2048, "k1", env_);
  locker_->UnLock(txn2, 1024, "k2", env_);
  locker_->UnLock(txn2, 2048, "k2", env_);

  delete txn1;
  delete txn2;
}

TEST_F(PointLockManagerTest, UnlockExclusive) {
  MockColumnFamilyHandle cf(1);
  locker_->AddColumnFamily(&cf);

  auto txn1 = NewTxn();
  ASSERT_OK(locker_->TryLock(txn1, 1, "k", env_, true));
  locker_->UnLock(txn1, 1, "k", env_);

  auto txn2 = NewTxn();
  ASSERT_OK(locker_->TryLock(txn2, 1, "k", env_, true));

  // Cleanup
  locker_->UnLock(txn2, 1, "k", env_);

  delete txn1;
  delete txn2;
}

TEST_F(PointLockManagerTest, UnlockShared) {
  MockColumnFamilyHandle cf(1);
  locker_->AddColumnFamily(&cf);

  auto txn1 = NewTxn();
  ASSERT_OK(locker_->TryLock(txn1, 1, "k", env_, false));
  locker_->UnLock(txn1, 1, "k", env_);

  auto txn2 = NewTxn();
  ASSERT_OK(locker_->TryLock(txn2, 1, "k", env_, true));

  // Cleanup
  locker_->UnLock(txn2, 1, "k", env_);

  delete txn1;
  delete txn2;
}

// This test doesn't work with Range Lock Manager, because Range Lock Manager
// doesn't support deadlock_detect_depth.

TEST_F(PointLockManagerTest, DeadlockDepthExceeded) {
  // Tests that when detecting deadlock, if the detection depth is exceeded,
  // it's also viewed as deadlock.
  MockColumnFamilyHandle cf(1);
  locker_->AddColumnFamily(&cf);
  TransactionOptions txn_opt;
  txn_opt.deadlock_detect = true;
  txn_opt.deadlock_detect_depth = 1;
  txn_opt.lock_timeout = 1000000;
  auto txn1 = NewTxn(txn_opt);
  auto txn2 = NewTxn(txn_opt);
  auto txn3 = NewTxn(txn_opt);
  auto txn4 = NewTxn(txn_opt);
  // "a ->(k) b" means transaction a is waiting for transaction b to release
  // the held lock on key k.
  // txn4 ->(k3) -> txn3 ->(k2) txn2 ->(k1) txn1
  // txn3's deadlock detection will exceed the detection depth 1,
  // which will be viewed as a deadlock.
  // NOTE:
  // txn4 ->(k3) -> txn3 must be set up before
  // txn3 ->(k2) -> txn2, because to trigger deadlock detection for txn3,
  // it must have another txn waiting on it, which is txn4 in this case.
  ASSERT_OK(locker_->TryLock(txn1, 1, "k1", env_, true));

  port::Thread t1 = BlockUntilWaitingTxn(wait_sync_point_name_, [&]() {
    ASSERT_OK(locker_->TryLock(txn2, 1, "k2", env_, true));
    // block because txn1 is holding a lock on k1.
    locker_->TryLock(txn2, 1, "k1", env_, true);
  });

  ASSERT_OK(locker_->TryLock(txn3, 1, "k3", env_, true));

  port::Thread t2 = BlockUntilWaitingTxn(wait_sync_point_name_, [&]() {
    // block because txn3 is holding a lock on k1.
    locker_->TryLock(txn4, 1, "k3", env_, true);
  });

  auto s = locker_->TryLock(txn3, 1, "k2", env_, true);
  ASSERT_TRUE(s.IsBusy());
  ASSERT_EQ(s.subcode(), Status::SubCode::kDeadlock);

  std::vector<DeadlockPath> deadlock_paths = locker_->GetDeadlockInfoBuffer();
  ASSERT_EQ(deadlock_paths.size(), 1u);
  ASSERT_TRUE(deadlock_paths[0].limit_exceeded);

  locker_->UnLock(txn1, 1, "k1", env_);
  locker_->UnLock(txn3, 1, "k3", env_);
  t1.join();
  t2.join();

  delete txn4;
  delete txn3;
  delete txn2;
  delete txn1;
}

INSTANTIATE_TEST_CASE_P(PointLockManager, AnyLockManagerTest,
                        ::testing::Values(nullptr));

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

#else
#include <stdio.h>

int main(int /*argc*/, char** /*argv*/) {
  fprintf(stderr,
          "SKIPPED because Transactions are not supported in ROCKSDB_LITE\n");
  return 0;
}

#endif  // ROCKSDB_LITE

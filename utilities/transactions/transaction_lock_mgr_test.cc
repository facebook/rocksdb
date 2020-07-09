//  Copyright (c) 2020-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#ifndef ROCKSDB_LITE

#include "utilities/transactions/transaction_lock_mgr.h"

#include "file/file_util.h"
#include "port/port.h"
#include "port/stack_trace.h"
#include "rocksdb/utilities/transaction_db.h"
#include "test_util/testharness.h"
#include "test_util/testutil.h"
#include "utilities/transactions/transaction_db_mutex_impl.h"

namespace ROCKSDB_NAMESPACE {

class TransactionLockMgrTest : public testing::Test {
 public:
  void SetUp() override {
    env_ = Env::Default();
    db_dir_ = test::PerThreadDBPath("transaction_lock_mgr_test");
    ASSERT_OK(env_->CreateDir(db_dir_));
    mutex_factory_ = std::make_shared<TransactionDBMutexFactoryImpl>();

    Options opt;
    opt.create_if_missing = true;
    TransactionDBOptions txn_opt;
    txn_opt.transaction_lock_timeout = 0;
    ASSERT_OK(TransactionDB::Open(opt, txn_opt, db_dir_, &db_));

    locker_.reset(
        new TransactionLockMgr(db_, txn_opt.num_stripes, txn_opt.max_num_locks,
                               txn_opt.max_num_deadlocks, mutex_factory_));
  }

  void TearDown() override {
    delete db_;
    EXPECT_OK(DestroyDir(env_, db_dir_));
  }

  PessimisticTransaction* NewTxn(
      TransactionOptions txn_opt = TransactionOptions()) {
    Transaction* txn = db_->BeginTransaction(WriteOptions(), txn_opt);
    return reinterpret_cast<PessimisticTransaction*>(txn);
  }

 protected:
  Env* env_;
  std::unique_ptr<TransactionLockMgr> locker_;

 private:
  std::string db_dir_;
  std::shared_ptr<TransactionDBMutexFactory> mutex_factory_;
  TransactionDB* db_;
};

TEST_F(TransactionLockMgrTest, LockNonExistingColumnFamily) {
  locker_->RemoveColumnFamily(1024);
  auto txn = NewTxn();
  auto s = locker_->TryLock(txn, 1024, "k", env_, true);
  ASSERT_TRUE(s.IsInvalidArgument());
  ASSERT_STREQ(s.getState(), "Column family id not found: 1024");
  delete txn;
}

TEST_F(TransactionLockMgrTest, LockStatus) {
  locker_->AddColumnFamily(1024);
  locker_->AddColumnFamily(2048);

  auto txn1 = NewTxn();
  ASSERT_OK(locker_->TryLock(txn1, 1024, "k1", env_, true));
  ASSERT_OK(locker_->TryLock(txn1, 2048, "k1", env_, true));

  auto txn2 = NewTxn();
  ASSERT_OK(locker_->TryLock(txn2, 1024, "k2", env_, false));
  ASSERT_OK(locker_->TryLock(txn2, 2048, "k2", env_, false));

  auto s = locker_->GetLockStatusData();
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

  delete txn1;
  delete txn2;
}

TEST_F(TransactionLockMgrTest, UnlockExclusive) {
  locker_->AddColumnFamily(1);

  auto txn1 = NewTxn();
  ASSERT_OK(locker_->TryLock(txn1, 1, "k", env_, true));
  locker_->UnLock(txn1, 1, "k", env_);

  auto txn2 = NewTxn();
  ASSERT_OK(locker_->TryLock(txn2, 1, "k", env_, true));

  delete txn1;
  delete txn2;
}

TEST_F(TransactionLockMgrTest, UnlockShared) {
  locker_->AddColumnFamily(1);

  auto txn1 = NewTxn();
  ASSERT_OK(locker_->TryLock(txn1, 1, "k", env_, false));
  locker_->UnLock(txn1, 1, "k", env_);

  auto txn2 = NewTxn();
  ASSERT_OK(locker_->TryLock(txn2, 1, "k", env_, true));

  delete txn1;
  delete txn2;
}

TEST_F(TransactionLockMgrTest, ReentrantExclusiveLock) {
  // Tests that a txn can acquire exclusive lock on the same key repeatedly.
  locker_->AddColumnFamily(1);
  auto txn = NewTxn();
  ASSERT_OK(locker_->TryLock(txn, 1, "k", env_, true));
  ASSERT_OK(locker_->TryLock(txn, 1, "k", env_, true));
  delete txn;
}

TEST_F(TransactionLockMgrTest, ReentrantSharedLock) {
  // Tests that a txn can acquire shared lock on the same key repeatedly.
  locker_->AddColumnFamily(1);
  auto txn = NewTxn();
  ASSERT_OK(locker_->TryLock(txn, 1, "k", env_, false));
  ASSERT_OK(locker_->TryLock(txn, 1, "k", env_, false));
  delete txn;
}

TEST_F(TransactionLockMgrTest, LockUpgrade) {
  // Tests that a txn can upgrade from a shared lock to an exclusive lock.
  locker_->AddColumnFamily(1);
  auto txn = NewTxn();
  ASSERT_OK(locker_->TryLock(txn, 1, "k", env_, false));
  ASSERT_OK(locker_->TryLock(txn, 1, "k", env_, true));
  delete txn;
}

TEST_F(TransactionLockMgrTest, LockDowngrade) {
  // Tests that a txn can acquire a shared lock after acquiring an exclusive
  // lock on the same key.
  locker_->AddColumnFamily(1);
  auto txn = NewTxn();
  ASSERT_OK(locker_->TryLock(txn, 1, "k", env_, true));
  ASSERT_OK(locker_->TryLock(txn, 1, "k", env_, false));
  delete txn;
}

TEST_F(TransactionLockMgrTest, LockConflict) {
  // Tests that lock conflicts lead to lock timeout.
  locker_->AddColumnFamily(1);
  auto txn1 = NewTxn();
  auto txn2 = NewTxn();

  {
    // exclusive-exclusive conflict.
    ASSERT_OK(locker_->TryLock(txn1, 1, "k1", env_, true));
    auto s = locker_->TryLock(txn2, 1, "k1", env_, true);
    ASSERT_TRUE(s.IsTimedOut());
  }

  {
    // exclusive-shared conflict.
    ASSERT_OK(locker_->TryLock(txn1, 1, "k2", env_, true));
    auto s = locker_->TryLock(txn2, 1, "k2", env_, false);
    ASSERT_TRUE(s.IsTimedOut());
  }

  {
    // shared-exclusive conflict.
    ASSERT_OK(locker_->TryLock(txn1, 1, "k2", env_, false));
    auto s = locker_->TryLock(txn2, 1, "k2", env_, true);
    ASSERT_TRUE(s.IsTimedOut());
  }

  delete txn1;
  delete txn2;
}

port::Thread BlockUntilWaitingTxn(std::function<void()> f) {
  std::atomic<bool> reached(false);
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "TransactionLockMgr::AcquireWithTimeout:WaitingTxn",
      [&](void* /*arg*/) { reached.store(true); });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  port::Thread t(f);

  while (!reached.load()) {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearAllCallBacks();

  return t;
}

TEST_F(TransactionLockMgrTest, SharedLocks) {
  // Tests that shared locks can be concurrently held by multiple transactions.
  locker_->AddColumnFamily(1);
  auto txn1 = NewTxn();
  auto txn2 = NewTxn();
  ASSERT_OK(locker_->TryLock(txn1, 1, "k", env_, false));
  ASSERT_OK(locker_->TryLock(txn2, 1, "k", env_, false));
  delete txn1;
  delete txn2;
}

TEST_F(TransactionLockMgrTest, Deadlock) {
  // Tests that deadlock can be detected.
  // Deadlock scenario:
  // txn1 exclusively locks k1, and wants to lock k2;
  // txn2 exclusively locks k2, and wants to lock k1.
  locker_->AddColumnFamily(1);
  TransactionOptions txn_opt;
  txn_opt.deadlock_detect = true;
  txn_opt.lock_timeout = 1000000;
  auto txn1 = NewTxn(txn_opt);
  auto txn2 = NewTxn(txn_opt);

  ASSERT_OK(locker_->TryLock(txn1, 1, "k1", env_, true));
  ASSERT_OK(locker_->TryLock(txn2, 1, "k2", env_, true));

  // txn1 tries to lock k2, will block forever.
  port::Thread t = BlockUntilWaitingTxn([&]() {
    // block because txn2 is holding a lock on k2.
    locker_->TryLock(txn1, 1, "k2", env_, true);
  });

  auto s = locker_->TryLock(txn2, 1, "k1", env_, true);
  ASSERT_TRUE(s.IsBusy());
  ASSERT_EQ(s.subcode(), Status::SubCode::kDeadlock);

  std::vector<DeadlockPath> deadlock_paths = locker_->GetDeadlockInfoBuffer();
  ASSERT_EQ(deadlock_paths.size(), 1u);
  ASSERT_FALSE(deadlock_paths[0].limit_exceeded);

  std::vector<DeadlockInfo> deadlocks = deadlock_paths[0].path;
  ASSERT_EQ(deadlocks.size(), 2u);

  ASSERT_EQ(deadlocks[0].m_txn_id, txn1->GetID());
  ASSERT_EQ(deadlocks[0].m_cf_id, 1u);
  ASSERT_TRUE(deadlocks[0].m_exclusive);
  ASSERT_EQ(deadlocks[0].m_waiting_key, "k2");

  ASSERT_EQ(deadlocks[1].m_txn_id, txn2->GetID());
  ASSERT_EQ(deadlocks[1].m_cf_id, 1u);
  ASSERT_TRUE(deadlocks[1].m_exclusive);
  ASSERT_EQ(deadlocks[1].m_waiting_key, "k1");

  locker_->UnLock(txn2, 1, "k2", env_);
  t.join();

  delete txn2;
  delete txn1;
}

TEST_F(TransactionLockMgrTest, DeadlockDepthExceeded) {
  // Tests that when detecting deadlock, if the detection depth is exceeded,
  // it's also viewed as deadlock.
  locker_->AddColumnFamily(1);
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

  port::Thread t1 = BlockUntilWaitingTxn([&]() {
    ASSERT_OK(locker_->TryLock(txn2, 1, "k2", env_, true));
    // block because txn1 is holding a lock on k1.
    locker_->TryLock(txn2, 1, "k1", env_, true);
  });

  ASSERT_OK(locker_->TryLock(txn3, 1, "k3", env_, true));

  port::Thread t2 = BlockUntilWaitingTxn([&]() {
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

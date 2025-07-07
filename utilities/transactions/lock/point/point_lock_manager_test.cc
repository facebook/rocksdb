//  Copyright (c) 2020-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "utilities/transactions/lock/point/point_lock_manager_test.h"

namespace ROCKSDB_NAMESPACE {

constexpr auto kLongTxnTimeoutUs = 10000000;
constexpr auto kShortTxnTimeoutUs = 100;

// including test for both PointLockManager and PerKeyPointLockManager
class SpotLockManagerTest : public PointLockManagerTest,
                            public testing::WithParamInterface<bool> {
 public:
  void SetUp() override {
    init();
    // If a custom setup function was provided, use it. Otherwise, use what we
    // have inherited.
    auto per_key_lock_manager = GetParam();
    if (per_key_lock_manager) {
      locker_.reset(new PerKeyPointLockManager(
          static_cast<PessimisticTransactionDB*>(db_), txndb_opt_));
    } else {
      locker_.reset(new PointLockManager(
          static_cast<PessimisticTransactionDB*>(db_), txndb_opt_));
    }
  }
};

// This test is not applicable for Range Lock manager as Range Lock Manager
// operates on Column Families, not their ids.
TEST_P(SpotLockManagerTest, LockNonExistingColumnFamily) {
  MockColumnFamilyHandle cf(1024);
  locker_->RemoveColumnFamily(&cf);
  auto txn = NewTxn();
  auto s = locker_->TryLock(txn, 1024, "k", env_, true);
  ASSERT_TRUE(s.IsInvalidArgument());
  ASSERT_STREQ(s.getState(), "Column family id not found: 1024");
  delete txn;
}

TEST_P(SpotLockManagerTest, LockStatus) {
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

TEST_P(SpotLockManagerTest, UnlockExclusive) {
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

TEST_P(SpotLockManagerTest, UnlockShared) {
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

TEST_P(SpotLockManagerTest, DeadlockDepthExceeded) {
  // Tests that when detecting deadlock, if the detection depth is exceeded,
  // it's also viewed as deadlock.
  MockColumnFamilyHandle cf(1);
  locker_->AddColumnFamily(&cf);
  TransactionOptions txn_opt;
  txn_opt.deadlock_detect = true;
  txn_opt.deadlock_detect_depth = 1;
  txn_opt.lock_timeout = kLongTxnTimeoutUs;
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
    ASSERT_OK(locker_->TryLock(txn2, 1, "k1", env_, true));
  });

  ASSERT_OK(locker_->TryLock(txn3, 1, "k3", env_, true));

  port::Thread t2 = BlockUntilWaitingTxn(wait_sync_point_name_, [&]() {
    // block because txn3 is holding a lock on k1.
    ASSERT_OK(locker_->TryLock(txn4, 1, "k3", env_, true));
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

  locker_->UnLock(txn2, 1, "k2", env_);
  locker_->UnLock(txn2, 1, "k1", env_);
  locker_->UnLock(txn4, 1, "k3", env_);

  delete txn4;
  delete txn3;
  delete txn2;
  delete txn1;
}

TEST_P(SpotLockManagerTest, PrioritizedLockUpgradeWithExclusiveLock) {
  // Tests that a lock upgrade request is prioritized over other lock requests.

  // txn1 acquires shared lock on k1.
  // txn2 acquires exclusive lock on k1.
  // txn1 acquires exclusive locks k1 successfully

  MockColumnFamilyHandle cf(1);
  locker_->AddColumnFamily(&cf);
  TransactionOptions txn_opt;
  txn_opt.deadlock_detect = true;
  txn_opt.lock_timeout = kLongTxnTimeoutUs;
  auto txn1 = NewTxn(txn_opt);
  auto txn2 = NewTxn(txn_opt);

  ASSERT_OK(locker_->TryLock(txn1, 1, "k1", env_, false));

  // txn2 tries to lock k1 exclusively, will block forever.
  port::Thread t = BlockUntilWaitingTxn(wait_sync_point_name_, [this, &txn2]() {
    // block because txn1 is holding a shared lock on k1.
    ASSERT_OK(locker_->TryLock(txn2, 1, "k1", env_, true));
  });

  // verify lock upgrade successfully
  ASSERT_OK(locker_->TryLock(txn1, 1, "k1", env_, true));

  // unlock txn1, so txn2 could proceed
  locker_->UnLock(txn1, 1, "k1", env_);

  // Cleanup
  t.join();

  // Cleanup
  locker_->UnLock(txn2, 1, "k1", env_);
  delete txn2;
  delete txn1;
}

TEST_P(SpotLockManagerTest,
       PrioritizedLockUpgradeWithExclusiveLockAndSharedLock) {
  // Tests that lock upgrade is prioritized when mixed with shared and exclusive
  // locks requests

  // txn1 acquires shared lock on k1.
  // txn2 acquires shared lock on k1.
  // txn3 acquires exclusive lock on k1.
  // txn1 acquires exclusive locks k1 <- request granted after txn2 release the
  // lock

  MockColumnFamilyHandle cf(1);
  locker_->AddColumnFamily(&cf);
  TransactionOptions txn_opt;
  txn_opt.deadlock_detect = true;
  txn_opt.lock_timeout = kLongTxnTimeoutUs;
  auto txn1 = NewTxn(txn_opt);
  auto txn2 = NewTxn(txn_opt);
  auto txn3 = NewTxn(txn_opt);

  ASSERT_OK(locker_->TryLock(txn1, 1, "k1", env_, false));
  ASSERT_OK(locker_->TryLock(txn2, 1, "k1", env_, false));

  // txn3 tries to lock k1 exclusively, will block forever.
  port::Thread txn3_thread =
      BlockUntilWaitingTxn(wait_sync_point_name_, [this, &txn3]() {
        // block because txn1 and txn2 are holding a shared lock on k1.
        ASSERT_OK(locker_->TryLock(txn3, 1, "k1", env_, true));
      });
  // Verify txn3 is blocked
  ASSERT_TRUE(txn3_thread.joinable());

  // txn1 tries to lock k1 exclusively, will block forever.
  port::Thread txn1_thread =
      BlockUntilWaitingTxn(wait_sync_point_name_, [this, &txn1]() {
        // block because txn1 and txn2 are holding a shared lock on k1.
        ASSERT_OK(locker_->TryLock(txn1, 1, "k1", env_, true));
      });
  // Verify txn1 is blocked
  ASSERT_TRUE(txn1_thread.joinable());

  // Unlock txn2, so txn1 could proceed
  locker_->UnLock(txn2, 1, "k1", env_);
  txn1_thread.join();

  // Unlock txn1, so txn3 could proceed
  locker_->UnLock(txn1, 1, "k1", env_);
  txn3_thread.join();

  // Cleanup
  locker_->UnLock(txn3, 1, "k1", env_);
  delete txn3;
  delete txn2;
  delete txn1;
}

TEST_P(SpotLockManagerTest, Deadlock_MultipleUpgrade) {
  // Tests that deadlock can be detected for shared locks and exclusive locks
  // mixed Deadlock scenario:

  // txn1 acquires shared lock on k1.
  // txn2 acquires shared lock on k1.
  // txn1 acquires exclusive locks k1
  // txn2 acquires exclusive locks k1 <- dead lock detected

  MockColumnFamilyHandle cf(1);
  locker_->AddColumnFamily(&cf);
  TransactionOptions txn_opt;
  txn_opt.deadlock_detect = true;
  txn_opt.lock_timeout = kLongTxnTimeoutUs;
  auto txn1 = NewTxn(txn_opt);
  auto txn2 = NewTxn(txn_opt);

  ASSERT_OK(locker_->TryLock(txn1, 1, "k1", env_, false));
  ASSERT_OK(locker_->TryLock(txn2, 1, "k1", env_, false));

  // txn1 tries to lock k1 exclusively, will block forever.
  port::Thread t = BlockUntilWaitingTxn(wait_sync_point_name_, [this, &txn1]() {
    // block because txn2 is holding a shared lock on k1.
    ASSERT_OK(locker_->TryLock(txn1, 1, "k1", env_, true));
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
  ASSERT_EQ(deadlocks[0].m_waiting_key, "k1");

  ASSERT_EQ(deadlocks[1].m_txn_id, txn2->GetID());
  ASSERT_EQ(deadlocks[1].m_cf_id, 1u);
  ASSERT_TRUE(deadlocks[1].m_exclusive);
  ASSERT_EQ(deadlocks[1].m_waiting_key, "k1");

  locker_->UnLock(txn2, 1, "k1", env_);
  t.join();

  // Cleanup
  locker_->UnLock(txn1, 1, "k1", env_);
  delete txn2;
  delete txn1;
}

TEST_P(SpotLockManagerTest, Deadlock_MultipleUpgradeInterleaveExclusive) {
  // Tests that deadlock can be detected for shared locks and exclusive locks
  // mixed Deadlock scenario:

  // txn1 acquires shared lock on k1.
  // txn2 acquires shared lock on k1.
  // txn3 acquires exclusive lock on k1.
  // txn1 acquires exclusive locks k1 <- request granted after txn2 release the
  // lock.
  // txn2 acquires exclusive locks k1 <- dead lock detected

  MockColumnFamilyHandle cf(1);
  locker_->AddColumnFamily(&cf);
  TransactionOptions txn_opt;
  txn_opt.deadlock_detect = true;
  txn_opt.lock_timeout = kLongTxnTimeoutUs;
  auto txn1 = NewTxn(txn_opt);
  auto txn2 = NewTxn(txn_opt);
  auto txn3 = NewTxn(txn_opt);

  ASSERT_OK(locker_->TryLock(txn1, 1, "k1", env_, false));
  ASSERT_OK(locker_->TryLock(txn2, 1, "k1", env_, false));

  // txn3 tries to lock k1 exclusively, will block forever.
  port::Thread txn3_thread =
      BlockUntilWaitingTxn(wait_sync_point_name_, [this, &txn3]() {
        // block because txn1 and txn2 are holding a shared lock on k1.
        ASSERT_OK(locker_->TryLock(txn3, 1, "k1", env_, true));
      });
  // Verify txn3 is blocked
  ASSERT_TRUE(txn3_thread.joinable());

  // txn1 tries to lock k1 exclusively, will block forever.
  port::Thread txn1_thread =
      BlockUntilWaitingTxn(wait_sync_point_name_, [this, &txn1]() {
        // block because txn1 and txn2 are holding a shared lock on k1.
        ASSERT_OK(locker_->TryLock(txn1, 1, "k1", env_, true));
      });
  // Verify txn1 is blocked
  ASSERT_TRUE(txn1_thread.joinable());

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
  ASSERT_EQ(deadlocks[0].m_waiting_key, "k1");

  ASSERT_EQ(deadlocks[1].m_txn_id, txn2->GetID());
  ASSERT_EQ(deadlocks[1].m_cf_id, 1u);
  ASSERT_TRUE(deadlocks[1].m_exclusive);
  ASSERT_EQ(deadlocks[1].m_waiting_key, "k1");

  // Unlock txn2, so txn1 could proceed
  locker_->UnLock(txn2, 1, "k1", env_);
  txn1_thread.join();

  // Unlock txn1, so txn3 could proceed
  locker_->UnLock(txn1, 1, "k1", env_);
  txn3_thread.join();

  // Cleanup
  locker_->UnLock(txn3, 1, "k1", env_);
  delete txn3;
  delete txn2;
  delete txn1;
}

class PerKeyPointLockManagerTest : public PointLockManagerTest {
 public:
  void SetUp() override {
    init();
    // CAUTION: This test creates a separate lock manager object (right, NOT
    // the one that the TransactionDB is using!), and runs tests on it.
    locker_.reset(new PerKeyPointLockManager(
        static_cast<PessimisticTransactionDB*>(db_), txndb_opt_));
  }
};

TEST_F(PerKeyPointLockManagerTest, LockEfficiency) {
  // Create multiple transactions, each acquire exclusive lock on the same key
  MockColumnFamilyHandle cf(1);
  locker_->AddColumnFamily(&cf);
  TransactionOptions txn_opt;
  txn_opt.deadlock_detect = true;
  txn_opt.lock_timeout = kLongTxnTimeoutUs;
  std::vector<PessimisticTransaction*> txns;
  std::vector<port::Thread> blockingThreads;

  // Count the total number of wait sync point calls
  std::atomic_int wait_sync_point_times = 0;
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      wait_sync_point_name_,
      [&wait_sync_point_times](void* /*arg*/) { wait_sync_point_times++; });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  constexpr auto num_of_txn = 10;
  // create 10 transactions, each of them try to acquire exclusive lock on the
  // same key
  for (int i = 0; i < num_of_txn; i++) {
    auto txn = NewTxn(txn_opt);
    txns.push_back(txn);

    if (i == 0) {
      // txn0 acquires the lock, so the rest of the transactions could block
      ASSERT_OK(locker_->TryLock(txn, 1, "k1", env_, true));
    } else {
      blockingThreads.emplace_back([this, txn]() {
        // block because txn1 and txn2 are holding a shared lock on k1.
        ASSERT_OK(locker_->TryLock(txn, 1, "k1", env_, true));
      });
    }

    // wait for transaction i to be blocked
    while (wait_sync_point_times.load() < i) {
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
  }

  // unlock the key, so next transaction could take the lock.
  locker_->UnLock(txns[0], 1, "k1", env_);

  auto num_of_blocking_thread = num_of_txn - 1;

  for (int i = 0; i < num_of_blocking_thread; i++) {
    // validate the thread is finished
    blockingThreads[i].join();
    auto num_of_threads_completed = i + 1;
    for (int j = 0; j < num_of_blocking_thread; j++) {
      if (j < num_of_threads_completed) {
        // validate the thread is no longer joinable
        ASSERT_FALSE(blockingThreads[j].joinable());
      } else {
        // validate the rest of the threads are still joinable
        ASSERT_TRUE(blockingThreads[j].joinable());
      }
    }
    // unlock the key, so next transaction could take the lock.
    locker_->UnLock(txns[i + 1], 1, "k1", env_);
  }

  ASSERT_EQ(wait_sync_point_times.load(), num_of_blocking_thread);
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearAllCallBacks();

  for (int i = 0; i < num_of_txn; i++) {
    delete txns[num_of_txn - i - 1];
  }
}

TEST_F(PerKeyPointLockManagerTest, LockFairness) {
  // Create multiple transactions requesting locks on the same key, validate
  // that they are executed in FIFO order

  // txn0 acquires exclusive lock on k1.
  // txn1 acquires shared lock on k1.
  // txn2 acquires shared lock on k1.
  // txn3 acquires exclusive lock on k1.
  // txn4 acquires shared lock on k1.
  // txn5 acquires exclusive lock on k1.
  // txn6 acquires exclusive lock on k1.
  // txn7 acquires shared lock on k1.
  // txn8 acquires shared lock on k1.
  // txn9 acquires exclusive lock on k1.

  MockColumnFamilyHandle cf(1);
  locker_->AddColumnFamily(&cf);
  TransactionOptions txn_opt;
  txn_opt.deadlock_detect = true;
  txn_opt.lock_timeout = kLongTxnTimeoutUs;
  std::vector<PessimisticTransaction*> txns;
  std::vector<port::Thread> blockingThreads;

  // Count the total number of wait sync point calls
  std::atomic_int wait_sync_point_times = 0;
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      wait_sync_point_name_,
      [&wait_sync_point_times](void* /*arg*/) { wait_sync_point_times++; });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  constexpr auto num_of_txn = 10;
  std::vector<bool> txn_lock_types = {true, false, false, true,  false,
                                      true, true,  false, false, true};
  // create 10 transactions, each of them try to acquire exclusive lock on the
  // same key
  for (int i = 0; i < num_of_txn; i++) {
    auto txn = NewTxn(txn_opt);
    txns.push_back(txn);

    if (i == 0) {
      // txn0 acquires the lock, so the rest of the transactions would block
      ASSERT_OK(locker_->TryLock(txn, 1, "k1", env_, txn_lock_types[0]));
    } else {
      blockingThreads.emplace_back([this, txn, type = txn_lock_types[i]]() {
        ASSERT_OK(locker_->TryLock(txn, 1, "k1", env_, type));
      });
    }

    // wait for transaction i to be blocked
    while (wait_sync_point_times.load() < i) {
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
  }

  auto num_of_blocking_thread = num_of_txn - 1;

  auto thread_idx = 0;
  auto txn_idx = 0;

  auto unlockTxn = [&]() {
    // unlock the key in transaction.
    locker_->UnLock(txns[txn_idx++], 1, "k1", env_);
  };

  auto validateLockTakenByNextTxn = [&]() {
    // validate the thread is finished
    blockingThreads[thread_idx++].join();
  };

  auto stillWaitingForLock = [&]() {
    // validate the thread is no longer joinable
    ASSERT_TRUE(blockingThreads[thread_idx].joinable());
  };

  // unlock the key, so next group of transactions could take the lock.
  unlockTxn();

  // txn1 acquires shared lock on k1.
  // txn2 acquires shared lock on k1.
  validateLockTakenByNextTxn();
  validateLockTakenByNextTxn();

  // txn3 acquires exclusive lock on k1.
  stillWaitingForLock();
  unlockTxn();
  unlockTxn();
  validateLockTakenByNextTxn();

  // txn4 acquires shared lock on k1.
  stillWaitingForLock();
  unlockTxn();
  validateLockTakenByNextTxn();

  // txn5 acquires exclusive lock on k1.
  stillWaitingForLock();
  unlockTxn();
  validateLockTakenByNextTxn();

  // txn6 acquires exclusive lock on k1.
  stillWaitingForLock();
  unlockTxn();
  validateLockTakenByNextTxn();

  // txn7 acquires shared lock on k1.
  // txn8 acquires shared lock on k1.
  stillWaitingForLock();
  unlockTxn();
  validateLockTakenByNextTxn();
  validateLockTakenByNextTxn();

  // txn9 acquires exclusive lock on k1.
  stillWaitingForLock();
  unlockTxn();
  unlockTxn();
  validateLockTakenByNextTxn();

  // clean up
  unlockTxn();

  ASSERT_EQ(wait_sync_point_times.load(), num_of_blocking_thread);
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearAllCallBacks();

  for (int i = 0; i < num_of_txn; i++) {
    delete txns[num_of_txn - i - 1];
  }
}

TEST_F(PerKeyPointLockManagerTest, FIFO) {
  // validate S, X, S lock order would be executed in FIFO order
  // txn1 acquires shared lock on k1.
  // txn2 acquires exclusive lock on k1.
  // txn3 acquires shared lock on k1.

  MockColumnFamilyHandle cf(1);
  locker_->AddColumnFamily(&cf);
  TransactionOptions txn_opt;
  txn_opt.deadlock_detect = true;
  txn_opt.lock_timeout = kLongTxnTimeoutUs;
  std::vector<PessimisticTransaction*> txns;
  std::vector<port::Thread> blockingThreads;

  // Count the total number of wait sync point calls
  std::atomic_int wait_sync_point_times = 0;
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      wait_sync_point_name_,
      [&wait_sync_point_times](void* /*arg*/) { wait_sync_point_times++; });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  constexpr auto num_of_txn = 3;
  std::vector<bool> txn_lock_types = {false, true, false};
  // create 3 transactions, each of them try to acquire exclusive lock on the
  // same key
  for (int i = 0; i < num_of_txn; i++) {
    auto txn = NewTxn(txn_opt);
    txns.push_back(txn);

    if (i == 0) {
      // txn0 acquires the lock, so the rest of the transactions would block
      ASSERT_OK(locker_->TryLock(txn, 1, "k1", env_, txn_lock_types[0]));
    } else {
      blockingThreads.emplace_back([this, txn, type = txn_lock_types[i]]() {
        ASSERT_OK(locker_->TryLock(txn, 1, "k1", env_, type));
      });
    }

    // wait for transaction i to be blocked
    while (wait_sync_point_times.load() < i) {
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
  }

  auto num_of_blocking_thread = num_of_txn - 1;

  auto thread_idx = 0;
  auto txn_idx = 0;

  auto unlockTxn = [&]() {
    // unlock the key in transaction.
    locker_->UnLock(txns[txn_idx++], 1, "k1", env_);
  };

  auto validateLockTakenByNextTxn = [&]() {
    // validate the thread is finished
    blockingThreads[thread_idx++].join();
  };

  auto stillWaitingForLock = [&]() {
    // validate the thread is no longer joinable
    ASSERT_TRUE(blockingThreads[thread_idx].joinable());
  };

  // unlock the key, so next group of transactions could take the lock.
  stillWaitingForLock();
  unlockTxn();

  // txn1 acquires exclusive lock on k1.
  validateLockTakenByNextTxn();

  // txn2 acquires shared lock on k1.
  stillWaitingForLock();
  unlockTxn();
  validateLockTakenByNextTxn();

  // clean up
  unlockTxn();

  ASSERT_EQ(wait_sync_point_times.load(), num_of_blocking_thread);
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearAllCallBacks();

  for (int i = 0; i < num_of_txn; i++) {
    delete txns[num_of_txn - i - 1];
  }
}

TEST_P(SpotLockManagerTest, LockDownGradeWithOtherLockRequests) {
  // Test lock down grade always succeeds, even if there are other lock requests
  // waiting for the same lock.
  MockColumnFamilyHandle cf(1);
  locker_->AddColumnFamily(&cf);
  TransactionOptions txn_opt;
  txn_opt.deadlock_detect = true;
  txn_opt.lock_timeout = kLongTxnTimeoutUs;
  auto txn1 = NewTxn(txn_opt);
  auto txn2 = NewTxn(txn_opt);

  for (bool exclusive : {true, false}) {
    ASSERT_OK(locker_->TryLock(txn1, 1, "k1", env_, true));

    auto t =
        BlockUntilWaitingTxn(wait_sync_point_name_, [this, &txn2, exclusive]() {
          // block because txn1 is holding a exclusive lock on k1.
          ASSERT_OK(locker_->TryLock(txn2, 1, "k1", env_, exclusive));
        });

    // txn1 downgrades the lock to shared lock, so txn2 could proceed
    ASSERT_OK(locker_->TryLock(txn1, 1, "k1", env_, false));

    locker_->UnLock(txn1, 1, "k1", env_);
    t.join();
    locker_->UnLock(txn2, 1, "k1", env_);
  }

  // clean up
  delete txn2;
  delete txn1;
}

TEST_P(SpotLockManagerTest, LockTimeout) {
  // Test lock timeout
  // txn1 acquires an exclusive lock on k1 successfully.
  // txn2 try to acquire a lock on k1, but timedout.

  MockColumnFamilyHandle cf(1);
  locker_->AddColumnFamily(&cf);
  TransactionOptions txn_opt;
  txn_opt.deadlock_detect = true;
  txn_opt.lock_timeout = kShortTxnTimeoutUs;
  auto txn1 = NewTxn(txn_opt);
  auto txn2 = NewTxn(txn_opt);

  ASSERT_OK(locker_->TryLock(txn1, 1, "k1", env_, true));

  for (bool exclusive : {true, false}) {
    auto ret = locker_->TryLock(txn2, 1, "k1", env_, exclusive);
    ASSERT_TRUE(ret.IsTimedOut());
  }

  // clean up
  locker_->UnLock(txn1, 1, "k1", env_);
  delete txn2;
  delete txn1;
}

TEST_F(PerKeyPointLockManagerTest, SharedLockTimeoutInTheMiddle) {
  // There are multiple transactions waiting for the same lock.
  // txn1 acquires a shared lock on k1 successfully.
  // txn2 try to acquire an exclusive lock on k1.
  // txn3 try to acquire a shared lock on k1 with a very short timeout.
  // Verify timeout is returned.
  // txn4 try to acquire an exclusive lock on k1.
  // unlock txn1, both txn2 and txn4 should proceed.

  MockColumnFamilyHandle cf(1);
  locker_->AddColumnFamily(&cf);
  TransactionOptions txn_opt;
  txn_opt.deadlock_detect = true;
  txn_opt.lock_timeout = kLongTxnTimeoutUs;
  auto txn1 = NewTxn(txn_opt);
  auto txn2 = NewTxn(txn_opt);
  txn_opt.lock_timeout = kShortTxnTimeoutUs;
  auto txn3 = NewTxn(txn_opt);
  txn_opt.lock_timeout = kLongTxnTimeoutUs;
  auto txn4 = NewTxn(txn_opt);

  // Count the total number of wait sync point calls
  std::atomic_int wait_sync_point_times = 0;
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      wait_sync_point_name_,
      [&wait_sync_point_times](void* /*arg*/) { wait_sync_point_times++; });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  auto wait_for_txn_to_be_blocked = [&](int expected_sync_point_call_times) {
    while (wait_sync_point_times.load() < expected_sync_point_call_times) {
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    ASSERT_EQ(wait_sync_point_times.load(), expected_sync_point_call_times);
  };

  // txn1 acquire a shared lock on k1
  ASSERT_OK(locker_->TryLock(txn1, 1, "k1", env_, false));

  auto t1 = port::Thread([this, &txn2]() {
    // block because txn1 is holding a shared lock on k1.
    ASSERT_OK(locker_->TryLock(txn2, 1, "k1", env_, true));
  });

  wait_for_txn_to_be_blocked(1);

  auto t2 = port::Thread([this, &txn3]() {
    // block due to txn2 is trying to acquire an exclusive lock on k1.
    // respect FIFO order.
    auto s = locker_->TryLock(txn3, 1, "k1", env_, false);
    ASSERT_TRUE(s.IsTimedOut());
  });

  wait_for_txn_to_be_blocked(2);

  auto t3 = port::Thread([this, &txn4]() {
    // block because txn1 is holding a shared lock on k1.
    ASSERT_OK(locker_->TryLock(txn4, 1, "k1", env_, true));
  });

  wait_for_txn_to_be_blocked(3);

  // wait until t2 finished
  t2.join();

  // unlock txn1, so txn2 could proceed
  locker_->UnLock(txn1, 1, "k1", env_);
  t1.join();

  // unlock txn2, so txn4 could proceed
  locker_->UnLock(txn2, 1, "k1", env_);
  t3.join();

  // clean up
  locker_->UnLock(txn4, 1, "k1", env_);

  // Validate wait sync point didn't get called again
  ASSERT_EQ(wait_sync_point_times.load(), 3);
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearAllCallBacks();

  delete txn4;
  delete txn3;
  delete txn2;
  delete txn1;
}

TEST_F(PerKeyPointLockManagerTest, ExclusiveLockTimeoutInTheMiddle) {
  // There are multiple transactions waiting for the same lock.
  // txn1 acquires an exclusive lock on k1 successfully.
  // txn2 try to acquire a shared lock on k1.
  // txn3 try to acquire an exclusive lock on k1 with short timeout.
  // txn4 try to acquire a shared lock on k1.
  // Verify lock are granted in FIFO order and only one exclusive lock waiter is
  // woken up at a time.

  MockColumnFamilyHandle cf(1);
  locker_->AddColumnFamily(&cf);
  TransactionOptions txn_opt;
  txn_opt.deadlock_detect = true;
  txn_opt.lock_timeout = kLongTxnTimeoutUs;
  auto txn1 = NewTxn(txn_opt);
  auto txn2 = NewTxn(txn_opt);
  txn_opt.lock_timeout = kShortTxnTimeoutUs;
  auto txn3 = NewTxn(txn_opt);
  txn_opt.lock_timeout = kLongTxnTimeoutUs;
  auto txn4 = NewTxn(txn_opt);

  ASSERT_OK(locker_->TryLock(txn1, 1, "k1", env_, true));

  auto t1 = BlockUntilWaitingTxn(wait_sync_point_name_, [this, &txn2]() {
    // block because txn1 is holding an exclusive lock on k1.
    ASSERT_OK(locker_->TryLock(txn2, 1, "k1", env_, false));
  });

  auto t2 = BlockUntilWaitingTxn(wait_sync_point_name_, [this, &txn3]() {
    // block because txn1 is holding an exclusive lock on k1.
    auto s = locker_->TryLock(txn3, 1, "k1", env_, true);
    ASSERT_TRUE(s.IsTimedOut());
  });

  auto t3 = BlockUntilWaitingTxn(wait_sync_point_name_, [this, &txn4]() {
    // block because txn1 is holding an exclusive lock on k1.
    ASSERT_OK(locker_->TryLock(txn4, 1, "k1", env_, false));
  });

  // wait until t2 finished
  t2.join();

  // unlock txn1, so txn2 and txn4 could proceed
  locker_->UnLock(txn1, 1, "k1", env_);

  // wait until t1 and t3 finished
  t1.join();
  t3.join();

  // clean up
  locker_->UnLock(txn2, 1, "k1", env_);
  locker_->UnLock(txn4, 1, "k1", env_);

  delete txn4;
  delete txn3;
  delete txn2;
  delete txn1;
}

TEST_P(SpotLockManagerTest, ExpiredLockStolenAfterTimeout) {
  // validate an expired lock can be stolen by another transaction that timed
  // out on the lock.
  // txn1 acquires an exclusive lock on k1 successfully with a short expiration
  // time.
  // txn2 try to acquire a shared lock on k1 with timeout that is slightly
  // longer than the txn1 expiration.
  // Validate txn2 will take the lock.

  MockColumnFamilyHandle cf(1);
  locker_->AddColumnFamily(&cf);
  TransactionOptions txn_opt;
  txn_opt.deadlock_detect = true;
  txn_opt.expiration = kShortTxnTimeoutUs;
  txn_opt.lock_timeout = kShortTxnTimeoutUs * 2;
  auto txn1 = NewTxn(txn_opt);
  auto txn2 = NewTxn(txn_opt);

  ASSERT_OK(locker_->TryLock(txn1, 1, "k1", env_, true));

  auto t1 = BlockUntilWaitingTxn(wait_sync_point_name_, [this, &txn2]() {
    // block because txn1 is holding an exclusive lock on k1.
    ASSERT_OK(locker_->TryLock(txn2, 1, "k1", env_, false));
  });

  t1.join();

  // clean up
  locker_->UnLock(txn2, 1, "k1", env_);
  locker_->UnLock(txn1, 1, "k1", env_);

  delete txn2;
  delete txn1;
}

TEST_F(PerKeyPointLockManagerTest, LockStealAfterTimeoutExclusive) {
  // There are multiple transactions waiting for the same lock.
  // txn1 acquires an exclusive lock on k1 successfully with a short expiration
  // time.
  // txn2 try to acquire an exclusive lock on k1, before expiration time,
  // so it is blocked and waits for txn1 lock expired.
  // txn3 try to acquire an exclusive lock on k1 after txn1 lock expires, FIFO
  // order is respected.
  // txn2 is woken up and takes the lock. unlock txn2, txn3 should proceed.

  MockColumnFamilyHandle cf(1);
  locker_->AddColumnFamily(&cf);
  TransactionOptions txn_opt;
  txn_opt.deadlock_detect = true;
  txn_opt.lock_timeout = kLongTxnTimeoutUs;
  txn_opt.expiration = kShortTxnTimeoutUs;
  auto txn1 = NewTxn(txn_opt);
  txn_opt.expiration = -1;
  auto txn2 = NewTxn(txn_opt);
  auto txn3 = NewTxn(txn_opt);

  ASSERT_OK(locker_->TryLock(txn1, 1, "k1", env_, true));

  auto t1 = BlockUntilWaitingTxn(wait_sync_point_name_, [this, &txn2]() {
    // block because txn1 is holding an exclusive lock on k1.
    ASSERT_OK(locker_->TryLock(txn2, 1, "k1", env_, true));
  });

  // wait until txn1 lock expired
  std::this_thread::sleep_for(std::chrono::milliseconds(kShortTxnTimeoutUs));

  // txn3 try to acquire an exclusive lock on k1, FIFO order is respected.
  auto t2 = BlockUntilWaitingTxn(wait_sync_point_name_, [this, &txn3]() {
    // block because txn1 is holding an exclusive lock on k1.
    ASSERT_OK(locker_->TryLock(txn3, 1, "k1", env_, true));
  });

  // validate txn2 is woken up and takes the lock
  t1.join();

  // unlock txn2, txn3 should proceed
  locker_->UnLock(txn2, 1, "k1", env_);
  t2.join();

  // clean up
  locker_->UnLock(txn3, 1, "k1", env_);

  delete txn3;
  delete txn2;
  delete txn1;
}

TEST_F(PerKeyPointLockManagerTest, LockStealAfterTimeoutShared) {
  // There are multiple transactions waiting for the same lock.
  // txn1 acquires a shared lock on k1 successfully with a short expiration
  // time.
  // txn2 try to acquire an exclusive lock on k1, before expiration time,
  // so it is blocked and waits for txn1 lock expired.
  // txn3 try to acquire a shared lock on k1 after txn1 lock expires, FIFO
  // order is respected.
  // txn2 is woken up and takes the lock. unlock txn2, txn3 should proceed.

  MockColumnFamilyHandle cf(1);
  locker_->AddColumnFamily(&cf);
  TransactionOptions txn_opt;
  txn_opt.deadlock_detect = true;
  txn_opt.lock_timeout = kLongTxnTimeoutUs;
  txn_opt.expiration = kShortTxnTimeoutUs;
  auto txn1 = NewTxn(txn_opt);
  txn_opt.expiration = -1;
  auto txn2 = NewTxn(txn_opt);
  auto txn3 = NewTxn(txn_opt);

  ASSERT_OK(locker_->TryLock(txn1, 1, "k1", env_, false));

  auto t1 = BlockUntilWaitingTxn(wait_sync_point_name_, [this, &txn2]() {
    // block because txn1 is holding an exclusive lock on k1.
    ASSERT_OK(locker_->TryLock(txn2, 1, "k1", env_, true));
  });

  // wait until txn1 lock expired
  std::this_thread::sleep_for(std::chrono::milliseconds(kShortTxnTimeoutUs));

  // txn3 try to acquire an exclusive lock on k1, FIFO order is respected.
  auto t2 = BlockUntilWaitingTxn(wait_sync_point_name_, [this, &txn3]() {
    // block because txn1 is holding an exclusive lock on k1.
    ASSERT_OK(locker_->TryLock(txn3, 1, "k1", env_, false));
  });

  // validate txn2 is woken up and takes the lock
  t1.join();

  // unlock txn2, txn3 should proceed
  locker_->UnLock(txn2, 1, "k1", env_);
  t2.join();

  // clean up
  locker_->UnLock(txn3, 1, "k1", env_);

  delete txn3;
  delete txn2;
  delete txn1;
}

TEST_F(PerKeyPointLockManagerTest, DeadLockOnWaiter) {
  // Txn1 acquires exclusive lock on k1
  // Txn3 acquires shared lock on k2
  // Txn2 tries to acquire exclusive lock on k1, waiting in the waiter queue.
  // Txn3 tries to acquire exclusive lock on k1, waiting in the waiter queue.
  // Txn3 depends on both Txn1 and Txn2. Txn1 unlocks k1.
  // Txn2 takes the lock k1, and tries to acquire lock k2.
  // Now Txn2 depends on Txn3.
  // Deadlock is detected, and Txn2 is aborted.

  MockColumnFamilyHandle cf(1);
  locker_->AddColumnFamily(&cf);
  TransactionOptions txn_opt;
  txn_opt.deadlock_detect = true;
  txn_opt.lock_timeout = kLongTxnTimeoutUs;
  auto txn1 = NewTxn(txn_opt);
  auto txn2 = NewTxn(txn_opt);
  auto txn3 = NewTxn(txn_opt);

  ASSERT_OK(locker_->TryLock(txn1, 1, "k1", env_, true));
  ASSERT_OK(locker_->TryLock(txn3, 1, "k2", env_, false));

  auto t1 = BlockUntilWaitingTxn(wait_sync_point_name_, [this, &txn2]() {
    ASSERT_OK(locker_->TryLock(txn2, 1, "k1", env_, true));
    auto s = locker_->TryLock(txn2, 1, "k2", env_, true);
    ASSERT_TRUE(s.IsDeadlock());
  });

  auto t2 = BlockUntilWaitingTxn(wait_sync_point_name_, [this, &txn3]() {
    ASSERT_OK(locker_->TryLock(txn3, 1, "k1", env_, true));
  });

  locker_->UnLock(txn1, 1, "k1", env_);

  t1.join();

  locker_->UnLock(txn2, 1, "k1", env_);
  t2.join();

  // clean up
  locker_->UnLock(txn3, 1, "k1", env_);
  locker_->UnLock(txn3, 1, "k2", env_);

  delete txn3;
  delete txn2;
  delete txn1;
}

TEST_F(PerKeyPointLockManagerTest, SharedLockRaceCondition) {
  // Verify a shared lock race condition is handled properly.
  // When there are waiters in the queue, and all of them are shared waiters,
  // and no one has taken the lock and all of them just got woken up and not yet
  // taken the lock yet. A new shared lock request should be granted directly,
  // without wait in the queue. If it did, It would not be woken up until the
  // last shared lock is released.

  MockColumnFamilyHandle cf(1);
  locker_->AddColumnFamily(&cf);
  TransactionOptions txn_opt;
  txn_opt.deadlock_detect = true;
  txn_opt.lock_timeout = kLongTxnTimeoutUs;
  txn_opt.expiration = kLongTxnTimeoutUs;

  auto txn1 = NewTxn(txn_opt);
  auto txn2 = NewTxn(txn_opt);
  auto txn3 = NewTxn(txn_opt);

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->LoadDependency(
      {{"LockMapStrpe::WaitOnKeyInternal:AfterWokenUp",
        "PerKeyPointLockManagerTest::SharedLockRaceCondition:"
        "BeforeNewSharedLockRequest"},
       {"PerKeyPointLockManagerTest::SharedLockRaceCondition:"
        "AfterNewSharedLockRequest",
        "LockMapStrpe::WaitOnKeyInternal:BeforeTakeLock"}});

  std::atomic<bool> reached(false);
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      wait_sync_point_name_,
      [&reached](void* /*arg*/) { reached.store(true); });

  SyncPoint::GetInstance()->EnableProcessing();

  // txn1 acquires an exclusive lock on k1, so that the following shared lock
  // request would be blocked
  ASSERT_OK(locker_->TryLock(txn1, 1, "k1", env_, true));

  // txn2 try to acquire a shared lock on k1, and get blocked
  auto t1 = port::Thread([this, &txn2]() {
    ASSERT_OK(locker_->TryLock(txn2, 1, "k1", env_, false));
  });

  while (!reached.load()) {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }

  // unlock txn1, txn2 should be woken up, but txn2 stops on the sync point
  locker_->UnLock(txn1, 1, "k1", env_);

  // Use sync point to simulate the race condition.
  // txn3 tries to take the lock right after txn2 is woken up, but before it
  // takes the lock
  TEST_SYNC_POINT(
      "PerKeyPointLockManagerTest::SharedLockRaceCondition:"
      "BeforeNewSharedLockRequest");

  // txn3 try to acquire a shared lock on k1, and get granted immediately
  ASSERT_OK(locker_->TryLock(txn3, 1, "k1", env_, false));

  TEST_SYNC_POINT(
      "PerKeyPointLockManagerTest::SharedLockRaceCondition:"
      "AfterNewSharedLockRequest");

  // validate txn2 is woken up and takes the lock
  t1.join();

  // cleanup
  locker_->UnLock(txn2, 1, "k1", env_);
  locker_->UnLock(txn3, 1, "k1", env_);

  delete txn3;
  delete txn2;
  delete txn1;
}

TEST_F(PerKeyPointLockManagerTest, UpgradeLockRaceCondition) {
  // Verify an upgrade lock race condition is handled properly.
  // When a key is locked in exlusive mode, shared lock waiters will be enqueued
  // as waiters. When the exclusive lock holder release the lock. The shared
  // lock waiters are waked up to take the lock. At this point, when a new
  // shared lock requester comes in, it will take the lock directly without
  // waiting or queueing. This requester then immediately upgrade the lock to
  // exclusive lock. This request will be prioritized to the head of the queue.
  // Meantime, it should also depend on the shared lock waiters which are still
  // in the queue that are ready to take the lock. Later, when one of the reader
  // lock want to also upgrade its lock, it will detect a dead lock and abort.

  MockColumnFamilyHandle cf(1);
  locker_->AddColumnFamily(&cf);
  TransactionOptions txn_opt;
  txn_opt.deadlock_detect = true;
  txn_opt.lock_timeout = kLongTxnTimeoutUs;
  txn_opt.expiration = kLongTxnTimeoutUs;

  auto txn1 = NewTxn(txn_opt);
  auto txn2 = NewTxn(txn_opt);
  auto txn3 = NewTxn(txn_opt);

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->LoadDependency(
      {{"LockMapStrpe::WaitOnKeyInternal:AfterWokenUp",
        "PerKeyPointLockManagerTest::UpgradeLockRaceCondition:"
        "BeforeNewSharedLockRequest"},
       {"PerKeyPointLockManagerTest::UpgradeLockRaceCondition:"
        "AfterNewSharedLockRequest",
        "LockMapStrpe::WaitOnKeyInternal:BeforeTakeLock"}});

  std::atomic<bool> reached(false);
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      wait_sync_point_name_,
      [&reached](void* /*arg*/) { reached.store(true); });

  SyncPoint::GetInstance()->EnableProcessing();

  // txn1 acquires an exclusive lock on k1, so that the following shared lock
  // request would be blocked
  ASSERT_OK(locker_->TryLock(txn1, 1, "k1", env_, true));

  auto t1 = port::Thread([this, &txn2]() {
    // txn2 try to acquire a shared lock on k1, and get blocked
    ASSERT_OK(locker_->TryLock(txn2, 1, "k1", env_, false));
  });

  while (!reached.load()) {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }

  // unlock txn1, txn2 should be woken up, but txn2 stops on the sync point
  locker_->UnLock(txn1, 1, "k1", env_);

  // Use sync point to simulate the race condition.
  // txn3 tries to take the lock right after txn2 is woken up, but before it
  // takes the lock
  TEST_SYNC_POINT(
      "PerKeyPointLockManagerTest::UpgradeLockRaceCondition:"
      "BeforeNewSharedLockRequest");

  // txn3 try to acquire a shared lock on k1, and get granted immediately
  ASSERT_OK(locker_->TryLock(txn3, 1, "k1", env_, false));

  // txn3 try to upgrade its lock to exclusive lock and get blocked.
  reached = false;
  auto t2 = port::Thread([this, &txn3]() {
    ASSERT_OK(locker_->TryLock(txn3, 1, "k1", env_, true));
  });

  while (!reached.load()) {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }

  TEST_SYNC_POINT(
      "PerKeyPointLockManagerTest::UpgradeLockRaceCondition:"
      "AfterNewSharedLockRequest");

  // validate txn2 is woken up and takes the shared lock
  t1.join();

  // validate txn2 would get deadlock when it try to upgrade its lock to
  // exclusive
  auto s = locker_->TryLock(txn2, 1, "k1", env_, true);
  ASSERT_TRUE(s.IsDeadlock());

  // cleanup
  locker_->UnLock(txn2, 1, "k1", env_);
  t2.join();
  locker_->UnLock(txn3, 1, "k1", env_);

  delete txn3;
  delete txn2;
  delete txn1;
}

TEST_P(SpotLockManagerTest, Catch22) {
  // Benchmark the overhead of one transaction depends on another in a circle
  // repeatedly

  MockColumnFamilyHandle cf(1);
  locker_->AddColumnFamily(&cf);
  TransactionOptions txn_opt;
  txn_opt.deadlock_detect = true;
  txn_opt.lock_timeout = kLongTxnTimeoutUs;
  txn_opt.expiration = kLongTxnTimeoutUs;

  auto txn1 = NewTxn(txn_opt);
  auto txn2 = NewTxn(txn_opt);

  // use a wait count to count the number of times the lock is waited inside
  // transaction lock
  std::atomic_int wait_count(0);

  SyncPoint::GetInstance()->DisableProcessing();
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      wait_sync_point_name_, [&wait_count](void* /*arg*/) { wait_count++; });
  SyncPoint::GetInstance()->EnableProcessing();

  // txn1 X lock
  ASSERT_OK(locker_->TryLock(txn1, 1, "k1", env_, true));

  std::mutex coordinator_mutex;

  // txn1 try to lock X lock in a loop
  auto t1 = port::Thread([this, &txn1, &wait_count, &coordinator_mutex]() {
    while (wait_count.load() < 100000) {
      // spin wait until the other thread enters the lock waiter queue.
      while (wait_count.load() % 2 == 0);
      // unlock the lock, so that the other thread can acquire the lock
      locker_->UnLock(txn1, 1, "k1", env_);
      {
        // Use the coordinator mutex to make sure the other thread has been
        // waked up and acquired the lock, before this thread try to acquire
        // the lock again.
        std::scoped_lock<std::mutex> lock(coordinator_mutex);
        ASSERT_OK(locker_->TryLock(txn1, 1, "k1", env_, true));
      }
    }
    locker_->UnLock(txn1, 1, "k1", env_);
  });

  // txn2 try to lock X lock in a loop
  auto t2 = port::Thread([this, &txn2, &wait_count, &coordinator_mutex]() {
    while (wait_count.load() < 100000) {
      {
        // Use the coordinator mutex to make sure the other thread has been
        // waked up and acquired the lock, before this thread try to acquire
        // the lock again.
        std::scoped_lock<std::mutex> lock(coordinator_mutex);
        ASSERT_OK(locker_->TryLock(txn2, 1, "k1", env_, true));
      }
      // spin wait until the other thread enters the lock waiter queue.
      while (wait_count.load() % 2 == 1);
      // unlock the lock, so that the other thread can acquire the lock
      locker_->UnLock(txn2, 1, "k1", env_);
    }
  });

  // clean up
  t1.join();
  t2.join();

  delete txn2;
  delete txn1;
}

TEST_F(PerKeyPointLockManagerTest, LockUpgradeOrdering) {
  // When lock is upgraded, verify that it will only upgrade its lock after all
  // the shared lock that are before the first exclusive lock in the lock wait
  // queue.

  MockColumnFamilyHandle cf(1);
  locker_->AddColumnFamily(&cf);
  TransactionOptions txn_opt;
  txn_opt.deadlock_detect = true;
  txn_opt.lock_timeout = kLongTxnTimeoutUs;
  txn_opt.expiration = kLongTxnTimeoutUs;

  auto txn1 = NewTxn(txn_opt);
  auto txn2 = NewTxn(txn_opt);
  auto txn3 = NewTxn(txn_opt);
  auto txn4 = NewTxn(txn_opt);

  std::mutex txn4_mutex;
  std::unique_lock<std::mutex> txn4_lock(txn4_mutex);

  SyncPoint::GetInstance()->DisableProcessing();
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "LockMapStrpe::WaitOnKeyInternal:AfterWokenUp",
      [&txn4, &txn4_mutex](void* arg) {
        auto transaction_id = *(static_cast<TransactionID*>(arg));
        if (transaction_id == txn4->GetId()) {
          // wait for txn4 mutex to be released, so that this thread will be
          // blocked.
          std::scoped_lock<std::mutex> lock(txn4_mutex);
        }
      });
  SyncPoint::GetInstance()->EnableProcessing();

  // Txn1 X lock
  ASSERT_OK(locker_->TryLock(txn1, 1, "k1", env_, true));

  // Txn2,3,4 try S lock
  auto t1 = BlockUntilWaitingTxn(wait_sync_point_name_, [this, &txn2]() {
    ASSERT_OK(locker_->TryLock(txn2, 1, "k1", env_, false));
  });
  auto t2 = BlockUntilWaitingTxn(wait_sync_point_name_, [this, &txn3]() {
    ASSERT_OK(locker_->TryLock(txn3, 1, "k1", env_, false));
  });
  auto t3 = BlockUntilWaitingTxn(wait_sync_point_name_, [this, &txn4]() {
    ASSERT_OK(locker_->TryLock(txn4, 1, "k1", env_, false));
  });

  // Txn1 unlock
  locker_->UnLock(txn1, 1, "k1", env_);

  // Txn2,3 take S lock
  t1.join();
  t2.join();

  // Txn2 try X lock
  std::atomic_bool txn2_exclusive_lock_acquired(false);
  auto t4 = BlockUntilWaitingTxn(
      wait_sync_point_name_, [this, &txn2, &txn2_exclusive_lock_acquired]() {
        ASSERT_OK(locker_->TryLock(txn2, 1, "k1", env_, true));
        txn2_exclusive_lock_acquired.store(true);
      });

  // Txn3 release S lock
  locker_->UnLock(txn3, 1, "k1", env_);

  // Txn4 take S lock
  t3.join();

  // Validate Txn2 has not acquired the lock yet
  ASSERT_FALSE(txn2_exclusive_lock_acquired.load());

  // Txn4 release S lock Txn2 upgraded to X lock Txn2
  locker_->UnLock(txn4, 1, "k1", env_);
  t4.join();
  ASSERT_TRUE(txn2_exclusive_lock_acquired.load());

  // release lock clean up
  locker_->UnLock(txn2, 1, "k1", env_);

  delete txn4;
  delete txn3;
  delete txn2;
  delete txn1;
}

TEST_F(PerKeyPointLockManagerTest, LockDownGradeRaceCondition) {
  // When a lock is downgraded, it should notify all the shared waiters in the
  // queue to take the lock.

  MockColumnFamilyHandle cf(1);
  locker_->AddColumnFamily(&cf);
  TransactionOptions txn_opt;
  txn_opt.deadlock_detect = true;
  txn_opt.lock_timeout = kLongTxnTimeoutUs;
  txn_opt.expiration = kLongTxnTimeoutUs;

  auto txn1 = NewTxn(txn_opt);
  auto txn2 = NewTxn(txn_opt);

  // Txn1 X lock
  ASSERT_OK(locker_->TryLock(txn1, 1, "k1", env_, true));

  // Txn2 try S lock
  auto t1 = BlockUntilWaitingTxn(wait_sync_point_name_, [this, &txn2]() {
    ASSERT_OK(locker_->TryLock(txn2, 1, "k1", env_, false));
  });

  // Txn1 downgrade to S lock
  ASSERT_OK(locker_->TryLock(txn1, 1, "k1", env_, false));

  // Txn2 take S lock
  t1.join();

  // clean up
  locker_->UnLock(txn1, 1, "k1", env_);
  locker_->UnLock(txn2, 1, "k1", env_);

  delete txn2;
  delete txn1;
}

#ifdef DEBUG_POINT_LOCK_MANAGER_TEST
#define DEBUG_LOG(...)            \
  do {                            \
    fprintf(stderr, __VA_ARGS__); \
    fflush(stderr);               \
  } while (0)
#else
#define DEBUG_LOG(...)
#endif

enum class LockTypeToTest : int8_t {
  EXCLUSIVE_ONLY = 0,
  SHARED_ONLY = 1,
  EXCLUSIVE_AND_SHARED = 2,
};

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

    values_.resize(param.key_count, 0);
    exclusive_lock_status_.resize(param.key_count, 0);

    // init counters and values
    for (size_t i = 0; i < param.key_count; i++) {
      counters_.emplace_back(std::make_unique<std::atomic_int>(0));
      shared_lock_count_.emplace_back(std::make_unique<std::atomic_int>(0));
    }
  }

 protected:
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

TEST_P(PointLockCorrectnessCheckTest, LockCorrectnessValidation) {
  // Verify lock guarantee. Exclusive lock provide unique access guarantee.
  // Shared lock provide shared access guarantee.
  // Create multiple threads. Each try to grab a lock with random type on
  // random key. On exclusive lock, bump the value by 1. Meantime, update a
  // global counter for validation On shared lock, read the value and compare
  // it against the global counter to make sure its value matches At the end,
  // validate the value against the global counter.

  auto const& param = GetParam();

  MockColumnFamilyHandle cf(1);
  locker_->AddColumnFamily(&cf);

  for (size_t thd_idx = 0; thd_idx < param.thread_count; thd_idx++) {
    threads_.emplace_back([this, &param, thd_idx]() {
      while (!shutdown_) {
        DEBUG_LOG("Thd %lu new txn\n", thd_idx);
        auto txn = NewTxn(txn_opt_);
        std::vector<std::pair<uint32_t, bool>> locked_key_with_types;
        // try to grab a random number of locks
        auto num_key_to_lock =
            Random::GetTLSInstance()->Uniform(
                static_cast<uint32_t>(param.max_num_keys_to_lock_per_txn)) +
            1;
        Status s;

        for (uint32_t j = 0; j < num_key_to_lock; j++) {
          uint32_t key = 0;
          key = Random::GetTLSInstance()->Uniform(
              static_cast<uint32_t>(param.key_count));
          auto key_str = std::to_string(key);
          bool isUpgrade = false;
          bool isDowngrade = false;

          // Decide lock type
          auto exclusive_lock_type = Random::GetTLSInstance()->OneIn(2);
          // check whether a lock on the same key is already held
          auto it = std::find_if(
              locked_key_with_types.begin(), locked_key_with_types.end(),
              [&key](std::pair<uint32_t, bool>& e) { return e.first == key; });
          if (it != locked_key_with_types.end()) {
            // a lock on the same key is already held.
            if (param.lock_type == LockTypeToTest::EXCLUSIVE_AND_SHARED) {
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
          if (param.lock_type != LockTypeToTest::EXCLUSIVE_AND_SHARED) {
            // if only one type of locks to be acquired, update its type
            exclusive_lock_type =
                (param.lock_type == LockTypeToTest::EXCLUSIVE_ONLY);
          }

          if (!param.allow_non_deadlock_error) {
            if (isDowngrade) {
              // Before downgrade, validate the lock is in exlusive status
              // This could not be done after downgrade, as another thread could
              // take a shared lock and update lock status
              ASSERT_TRUE(exclusive_lock_status_[key])
                  << "Thd " << thd_idx << " key " << key;
              ASSERT_EQ(*shared_lock_count_[key], 0)
                  << "Thd " << thd_idx << " key " << key;
            }
          }

          // try to acquire the lock
          DEBUG_LOG("Thd %lu try to acquire lock %u type %s\n", thd_idx, key,
                    exclusive_lock_type ? "exclusive" : "shared");
          s = locker_->TryLock(txn, 1, key_str, env_, exclusive_lock_type);
          if (s.ok()) {
            DEBUG_LOG("Thd %lu acquired lock %u type %s\n", thd_idx, key,
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
            if (!param.allow_non_deadlock_error) {
              // Validate lock status, if deadlock is the only allowed error.
              // otherwise, lock could be expired and stolen
              if (exclusive_lock_type) {
                // validate the lock is not in exclusive status
                ASSERT_FALSE(exclusive_lock_status_[key])
                    << "Thd " << thd_idx << " key " << key;
                if (isUpgrade) {
                  // validate the lock is in shared status and only had one
                  // shared lock
                  ASSERT_EQ(*shared_lock_count_[key], 1)
                      << "Thd " << thd_idx << " key " << key;
                  shared_lock_count_[key]->fetch_sub(1);
                } else {
                  ASSERT_EQ(*shared_lock_count_[key], 0)
                      << "Thd " << thd_idx << " key " << key;
                }
                // update the lock status
                exclusive_lock_status_[key] = 1;
              } else {
                shared_lock_count_[key]->fetch_add(1);
                exclusive_lock_status_[key] = 0;
              }
            }
          } else {
            if (!param.allow_non_deadlock_error) {
              ASSERT_TRUE(s.IsDeadlock());
            }
            if (s.IsDeadlock()) {
              DEBUG_LOG("Thd %lu detected deadlock on key %u, abort\n", thd_idx,
                        key);
              num_of_deadlock_detected_++;
              // for deadlock, release all locks acquired
              break;
            } else {
              // for other errors, try again
              DEBUG_LOG(
                  "Thd %lu failed to acquire lock on key %u, due to %s, "
                  "abort\n",
                  thd_idx, key, s.ToString().c_str());
            }
          }
        }

        if (param.sleep_after_lock_acquisition && s.ok()) {
          // sleep for a random time between 0.5 and 1.5 times of the
          // expiration time to pretend to do some work, and allow some of
          // the lock expires
          auto sleep_time_us =
              param.lock_expiration_us / 2 +
              Random::GetTLSInstance()->Uniform(
                  static_cast<uint32_t>(param.lock_expiration_us));
          std::this_thread::sleep_for(std::chrono::microseconds(sleep_time_us));
        }

        // release all locks
        for (auto& key_type : locked_key_with_types) {
          auto key = key_type.first;
          ASSERT_TRUE(key < param.key_count);
          // Check global lock status
          if (!param.allow_non_deadlock_error) {
            ASSERT_EQ(counters_[key]->load(), values_[key])
                << "Thd " << thd_idx << " key " << key;
            auto exclusive = key_type.second;
            if (exclusive) {
              // exclusive lock
              // bump the value by 1
              (*counters_[key])++;
              values_[key]++;
              DEBUG_LOG("Thd %lu bump key %u by 1 to %d\n", thd_idx, key,
                        values[key]);
              ASSERT_EQ(counters_[key]->load(), values_[key])
                  << "Thd " << thd_idx << " key " << key;
            }
            // Validate lock status, if deadlock is the only allowed error.
            // otherwise, lock could be expired and stolen
            if (exclusive) {
              // exclusive lock
              ASSERT_TRUE(exclusive_lock_status_[key])
                  << "Thd " << thd_idx << " key " << key;
              ASSERT_EQ(*shared_lock_count_[key], 0)
                  << "Thd " << thd_idx << " key " << key;
              exclusive_lock_status_[key] = 0;
            } else {
              // shared lock
              ASSERT_FALSE(exclusive_lock_status_[key])
                  << "Thd " << thd_idx << " key " << key;
              ASSERT_GE(shared_lock_count_[key]->fetch_sub(1), 1)
                  << "Thd " << thd_idx << " key " << key;
            }
          }
          DEBUG_LOG("Thd %lu release lock %u\n", thd_idx, key);
          locker_->UnLock(txn, 1, std::to_string(key), env_);
        }
        delete txn;
      }
    });
  }

  // run test for a few seconds
  // print progress
  auto prev_num_of_locks_acquired = num_of_locks_acquired_.load();
  for (size_t i = 0; i < param.execution_time_sec; i++) {
    std::this_thread::sleep_for(std::chrono::seconds(1));
    ASSERT_GT(num_of_locks_acquired_.load(), prev_num_of_locks_acquired);
    prev_num_of_locks_acquired = num_of_locks_acquired_.load();
    DEBUG_LOG("num_of_locks_acquired: %d\n", num_of_locks_acquired_.load());
    DEBUG_LOG("num_of_exclusive_locks_acquired: %d\n",
              num_of_exclusive_locks_acquired_.load());
    DEBUG_LOG("num_of_shared_locks_acquired: %d\n",
              num_of_shared_locks_acquired_.load());
    DEBUG_LOG("num_of_deadlock_detected: %d\n",
              num_of_deadlock_detected_.load());
  }

  shutdown_ = true;
  for (auto& t : threads_) {
    t.join();
  }

  // validate values against counters
  for (size_t i = 0; i < param.key_count; i++) {
    ASSERT_EQ(counters_[i]->load(), values_[i]);
  }

  ASSERT_GE(num_of_locks_acquired_.load(), 0);
  printf("num_of_locks_acquired: %d\n", num_of_locks_acquired_.load());
}

INSTANTIATE_TEST_CASE_P(
    PointLockCorrectnessCheckTestSuite, PointLockCorrectnessCheckTest,
    ::testing::ValuesIn(std::vector<PointLockCorrectnessCheckTestParam>{
        // short timeout and expiration
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
        // long timeout and expiration
        {true, 64, 16, 8, 10, LockTypeToTest::EXCLUSIVE_AND_SHARED,
         kLongTxnTimeoutUs, kLongTxnTimeoutUs, false, false},
        {false, 64, 16, 8, 10, LockTypeToTest::EXCLUSIVE_AND_SHARED,
         kLongTxnTimeoutUs, kLongTxnTimeoutUs, false, false},
        {true, 64, 16, 8, 10, LockTypeToTest::EXCLUSIVE_ONLY, kLongTxnTimeoutUs,
         kLongTxnTimeoutUs, false, false},
        {false, 64, 16, 8, 10, LockTypeToTest::EXCLUSIVE_ONLY,
         kLongTxnTimeoutUs, kLongTxnTimeoutUs, false, false},
        {true, 64, 16, 8, 10, LockTypeToTest::SHARED_ONLY, kLongTxnTimeoutUs,
         kLongTxnTimeoutUs, false, false},
        {false, 64, 16, 8, 10, LockTypeToTest::SHARED_ONLY, kLongTxnTimeoutUs,
         kLongTxnTimeoutUs, false, false},
        // Low lock contention
        {true, 16, 4096, 2, 10, LockTypeToTest::SHARED_ONLY, kLongTxnTimeoutUs,
         kLongTxnTimeoutUs, false, false},
        {false, 16, 4096, 2, 10, LockTypeToTest::SHARED_ONLY, kLongTxnTimeoutUs,
         kLongTxnTimeoutUs, false, false},
    }));

// Run AnyLockManagerTest with PointLockManager
INSTANTIATE_TEST_CASE_P(PointLockManager, AnyLockManagerTest,
                        ::testing::Values(nullptr));

// Run AnyLockManagerTest with PerKeyPointLockManager
void PerLockPointLockManagerAnyLockManagerTestSetup(
    PointLockManagerTest* self) {
  self->init();
  self->locker_.reset(new PerKeyPointLockManager(
      static_cast<PessimisticTransactionDB*>(self->db_), self->txndb_opt_));
}

INSTANTIATE_TEST_CASE_P(
    PerLockPointLockManager, AnyLockManagerTest,
    ::testing::Values(PerLockPointLockManagerAnyLockManagerTestSetup));

// Run PointLockManagerTest with PerLockPointLockManager and PointLockManager
INSTANTIATE_TEST_CASE_P(PointLockCorrectnessCheckTestSuite, SpotLockManagerTest,
                        ::testing::ValuesIn(std::vector<bool>{true, false}));

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

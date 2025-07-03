//  Copyright (c) 2020-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "utilities/transactions/lock/point/point_lock_manager_test.h"

namespace ROCKSDB_NAMESPACE {

constexpr auto kLongTxnTimeoutUs = 10000000;
constexpr auto kShortTxnTimeoutUs = 100;

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

  delete txn4;
  delete txn3;
  delete txn2;
  delete txn1;
}

TEST_F(PointLockManagerTest, PrioritizedLockUpgradeWithExclusiveLock) {
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

TEST_F(PointLockManagerTest,
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

TEST_F(PointLockManagerTest, Deadlock_MultipleUpgrade) {
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

TEST_F(PointLockManagerTest, Deadlock_MultipleUpgradeInterleaveExclusive) {
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

TEST_F(PointLockManagerTest, LockEfficiency) {
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
      wait_sync_point_name_, [&](void* /*arg*/) { wait_sync_point_times++; });
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

TEST_F(PointLockManagerTest, LockFairness) {
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
      wait_sync_point_name_, [&](void* /*arg*/) { wait_sync_point_times++; });
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

TEST_F(PointLockManagerTest, FIFO) {
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
      wait_sync_point_name_, [&](void* /*arg*/) { wait_sync_point_times++; });
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

TEST_F(PointLockManagerTest, LockDownGradeWithOtherLockRequests) {
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

TEST_F(PointLockManagerTest, LockTimeout) {
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

TEST_F(PointLockManagerTest, SharedLockTimeoutInTheMiddle) {
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
      wait_sync_point_name_, [&](void* /*arg*/) { wait_sync_point_times++; });
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

TEST_F(PointLockManagerTest, ExclusiveLockTimeoutInTheMiddle) {
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

TEST_F(PointLockManagerTest, ExpiredLockStolenAfterTimeout) {
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

TEST_F(PointLockManagerTest, LockStealAfterTimeoutExclusive) {
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

TEST_F(PointLockManagerTest, LockStealAfterTimeoutShared) {
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

TEST_F(PointLockManagerTest, DeadLockOnWaiter) {
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

#ifdef DEBUG_POINT_LOCK_MANAGER_TEST
#define DEBUG_LOG(...)            \
  do {                            \
    fprintf(stderr, __VA_ARGS__); \
  } while (0)
#else
#define DEBUG_LOG(...)
#endif

TEST_F(PointLockManagerTest, SharedLockRaceCondition) {
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
        "PointLockManagerTest::SharedLockRaceCondition:"
        "BeforeNewSharedLockRequest"},
       {"PointLockManagerTest::SharedLockRaceCondition:"
        "AfterNewSharedLockRequest",
        "LockMapStrpe::WaitOnKeyInternal:BeforeTakeLock"}});

  std::atomic<bool> reached(false);
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      wait_sync_point_name_, [&](void* /*arg*/) { reached.store(true); });

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
      "PointLockManagerTest::SharedLockRaceCondition:"
      "BeforeNewSharedLockRequest");

  // txn3 try to acquire a shared lock on k1, and get granted immediately
  ASSERT_OK(locker_->TryLock(txn3, 1, "k1", env_, false));

  TEST_SYNC_POINT(
      "PointLockManagerTest::SharedLockRaceCondition:"
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

TEST_F(PointLockManagerTest, LockGuaranteeValidation) {
  // Verify lock guarantee. Exclusive lock provide unique access guarantee.
  // Shared lock provide shared access guarantee.
  // Create multiple threads. Each try to grab a lock with random type on random
  // key.
  // On exclusive lock, bump the value by 1. Meantime, update a global
  // counter for validation On shared lock, read the value and compare it
  // against the global counter to make sure its value matches At the end,
  // validate the value against the global counter.

  MockColumnFamilyHandle cf(1);
  locker_->AddColumnFamily(&cf);
  TransactionOptions txn_opt;
  txn_opt.deadlock_detect = true;
  txn_opt.lock_timeout = kLongTxnTimeoutUs;
  txn_opt.expiration = kLongTxnTimeoutUs;
  std::atomic_int kNumOfThreads = 64;
  std::vector<std::thread> threads;
  std::atomic_int num_of_locks_acquired = 0;
  constexpr auto kNumOfKeys = 16;

  std::array<std::atomic_int, kNumOfKeys> counters{0};
  // values are read/write protected by the locks
  std::array<int, kNumOfKeys> values{0};

  // shutdown flag
  std::atomic_bool shutdown = 0;

  for (int thd_idx = 0; thd_idx < kNumOfThreads; thd_idx++) {
    threads.emplace_back(
        [this, &txn_opt, &shutdown, &num_of_locks_acquired, thd_idx, &counters,
         &values,
         /* Have to use this workaround to make both clang and microsoft
            compiler happy until we upgrade microsoft compiler. See
            https://developercommunity.visualstudio.com/t/
            // problems-with-capturing-constexpr-in-lambda/367326 */
         key_count = kNumOfKeys]() {
          while (!shutdown) {
            DEBUG_LOG("Thd %d new txn\n", thd_idx);
            auto txn = NewTxn(txn_opt);
            std::vector<std::pair<std::string, bool>> locked_key_with_types;
            // try to grab a random number of locks
            auto num_key_to_lock =
                Random::GetTLSInstance()->Uniform(key_count / 2) + 1;
            Status s;
            for (uint32_t j = 0; j < num_key_to_lock; j++) {
              auto key =
                  std::to_string(Random::GetTLSInstance()->Uniform(key_count));
              auto lock_type = true;
              // check whether a lock on the same key is already held
              auto it = std::find_if(locked_key_with_types.begin(),
                                     locked_key_with_types.end(),
                                     [&key](std::pair<std::string, bool>& e) {
                                       return e.first == key;
                                     });
              bool isUpgrade = false;
              if (it != locked_key_with_types.end()) {
                // a lock on the same key is already held.
                if (it->second == false) {
                  // If it is a shared lock, switch to an exclusive lock
                  lock_type = true;
                  isUpgrade = true;
                } else {
                  // If it is an exclusive lock, skip it
                  j--;
                  continue;
                }
              }
              // try to acquire the lock
              DEBUG_LOG("Thd %d try to acquire lock %s type %s\n", thd_idx,
                        key.c_str(), lock_type ? "exclusive" : "shared");
              s = locker_->TryLock(txn, 1, key, env_, lock_type);
              if (s.ok()) {
                DEBUG_LOG("Thd %d acquired lock %s type %s\n", thd_idx,
                          key.c_str(), lock_type ? "exclusive" : "shared");
                num_of_locks_acquired++;
                if (!isUpgrade) {
                  locked_key_with_types.emplace_back(key, lock_type);
                } else {
                  it->second = true;
                }
              } else {
                ASSERT_TRUE(s.IsDeadlock());
                break;
              }
            }

            // release all locks
            for (auto& key_type : locked_key_with_types) {
              auto key = key_type.first;
              auto key_num = std::stoi(key);
              ASSERT_TRUE(key_num >= 0 && key_num < key_count);
              ASSERT_EQ(counters[key_num].load(), values[key_num])
                  << "Thd " << thd_idx << " key " << std::to_string(key_num);
              if (key_type.second) {
                // exclusive lock
                // bump the value by 1
                counters[key_num]++;
                values[key_num]++;
                DEBUG_LOG("Thd %d bump key %s by 1 to %d\n", thd_idx,
                          key_type.first.c_str(), values[key_num]);
                ASSERT_EQ(counters[key_num].load(), values[key_num])
                    << "Thd " << thd_idx << " key " << std::to_string(key_num);
              }
              locker_->UnLock(txn, 1, key_type.first, env_);
              DEBUG_LOG("Thd %d release lock %s\n", thd_idx,
                        key_type.first.c_str());
            }
            delete txn;
          }
        });
  }

  // run test for 10 seconds
  // print progress
  for (int i = 0; i < 10; i++) {
    std::this_thread::sleep_for(std::chrono::seconds(1));
    printf("num_of_locks_acquired: %d\n", num_of_locks_acquired.load());
  }

  shutdown = true;
  for (auto& t : threads) {
    t.join();
  }

  // validate values against counters
  for (int i = 0; i < kNumOfKeys; i++) {
    ASSERT_EQ(counters[i].load(), values[i]);
  }

  ASSERT_TRUE(num_of_locks_acquired.load() > 0);
}

TEST_F(PointLockManagerTest, ExpirationAndTimeout) {
  // start many threads, each thread randomly acquire and release locks.
  // Use short lock timeout and lock expiration time to validate lock stealing
  // If a deadlock is detected, release the lock.
  // If a lock is acquired, sleep for a random time, then release it
  // At the end, validate threads could finish successfully.
  // validate no runtime corruption.
  // validate locks could be acquired and released successfully.
  // validate no assertion was violated.

  MockColumnFamilyHandle cf(1);
  locker_->AddColumnFamily(&cf);
  TransactionOptions txn_opt;
  txn_opt.deadlock_detect = true;
  txn_opt.lock_timeout = kShortTxnTimeoutUs * 2;
  txn_opt.expiration = kShortTxnTimeoutUs;
  std::atomic_int kNumOfThreads = 64;
  std::vector<std::thread> threads;
  std::atomic_int num_of_locks_acquired = 0;
  int kNumOfKeys = 16;

  // shutdown flag
  std::atomic_bool shutdown = 0;

  for (int thd_idx = 0; thd_idx < kNumOfThreads; thd_idx++) {
    threads.emplace_back([this, &txn_opt, &shutdown, &num_of_locks_acquired,
#ifdef DEBUG_POINT_LOCK_MANAGER_TEST
                          thd_idx,
#endif
                          &kNumOfKeys]() {
      DEBUG_LOG("Thd %d new txn\n", thd_idx);
      auto txn = NewTxn(txn_opt);
      while (!shutdown) {
        std::vector<std::pair<std::string, bool>> locked_key_with_types;
        // try to grab a random number of locks
        auto num_key_to_lock = Random::GetTLSInstance()->Uniform(4) + 1;
        Status s;
        for (uint32_t j = 0; j < num_key_to_lock; j++) {
          auto key = "k" + std::to_string(
                               Random::GetTLSInstance()->Uniform(kNumOfKeys));
          auto lock_type = Random::GetTLSInstance()->OneIn(2);
          // check whether a lock on the same key is already held
          auto it = std::find_if(locked_key_with_types.begin(),
                                 locked_key_with_types.end(),
                                 [&key](std::pair<std::string, bool>& e) {
                                   return e.first == key;
                                 });
          bool isUpgrade = false;
          if (it != locked_key_with_types.end()) {
            // a lock on the same key is already held.
            if (it->second == false) {
              // If it is a shared lock, switch to an exclusive lock
              lock_type = true;
              isUpgrade = true;
            } else {
              // If it is an exclusive lock, skip it
              j--;
              continue;
            }
          }
          // try to acquire the lock
          s = locker_->TryLock(txn, 1, key, env_, lock_type);
          if (s.ok()) {
            DEBUG_LOG("Thd %d acquired lock %s type %s\n", thd_idx, key.c_str(),
                      lock_type ? "exclusive" : "shared");
            num_of_locks_acquired++;
            if (!isUpgrade) {
              locked_key_with_types.emplace_back(key, lock_type);
            } else {
              it->second = true;
            }
          } else {
            if (s.IsDeadlock()) {
              DEBUG_LOG("Thd %d detected deadlock on key %s, abort\n", thd_idx,
                        key.c_str());
              break;
            }
            DEBUG_LOG(
                "Thd %d failed to acquire lock on key %s due to "
                "%s, retry\n",
                thd_idx, key.c_str(), s.ToString().c_str());
            // For other errors, retry again
          }
        }

        if (s.ok()) {
          // sleep for a random time between 0.5 and 1.5 times of the
          // expiration time to pretend to do some work, and allow some of the
          // lock expires
          auto sleep_time_us =
              kShortTxnTimeoutUs / 2 +
              Random::GetTLSInstance()->Uniform(kShortTxnTimeoutUs);
          std::this_thread::sleep_for(std::chrono::milliseconds(sleep_time_us));
        }

        // release all locks
        for (auto& key_type : locked_key_with_types) {
          locker_->UnLock(txn, 1, key_type.first, env_);
          DEBUG_LOG("Thd %d release lock %s\n", thd_idx,
                    key_type.first.c_str());
        }
      }
      delete txn;
    });
  }

  // run test for 10 seconds
  // print progress
  for (int i = 0; i < 10; i++) {
    std::this_thread::sleep_for(std::chrono::seconds(1));
    printf("num_of_locks_acquired: %d\n", num_of_locks_acquired.load());
  }

  shutdown = true;
  for (auto& t : threads) {
    t.join();
  }

  ASSERT_TRUE(num_of_locks_acquired.load() > 0);
}

INSTANTIATE_TEST_CASE_P(PointLockManager, AnyLockManagerTest,
                        ::testing::Values(nullptr));

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

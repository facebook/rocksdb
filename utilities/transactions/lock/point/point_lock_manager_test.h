
#include "file/file_util.h"
#include "port/port.h"
#include "port/stack_trace.h"
#include "rocksdb/utilities/transaction_db.h"
#include "test_util/testharness.h"
#include "test_util/testutil.h"
#include "utilities/transactions/lock/point/point_lock_manager.h"
#include "utilities/transactions/pessimistic_transaction_db.h"
#include "utilities/transactions/transaction_db_mutex_impl.h"

namespace ROCKSDB_NAMESPACE {

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

class PointLockManagerTest : public testing::Test {
 public:
  void SetUp() override {
    env_ = Env::Default();
    db_dir_ = test::PerThreadDBPath("point_lock_manager_test");
    ASSERT_OK(env_->CreateDir(db_dir_));

    Options opt;
    opt.create_if_missing = true;
    TransactionDBOptions txn_opt;
    txn_opt.transaction_lock_timeout = 0;

    ASSERT_OK(TransactionDB::Open(opt, txn_opt, db_dir_, &db_));

    // CAUTION: This test creates a separate lock manager object (right, NOT
    // the one that the TransactionDB is using!), and runs tests on it.
    locker_.reset(new PointLockManager(
        static_cast<PessimisticTransactionDB*>(db_), txn_opt));

    wait_sync_point_name_ = "PointLockManager::AcquireWithTimeout:WaitingTxn";
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
  std::shared_ptr<LockManager> locker_;
  const char* wait_sync_point_name_;
  friend void PointLockManagerTestExternalSetup(PointLockManagerTest*);

 private:
  std::string db_dir_;
  TransactionDB* db_;
};

using init_func_t = void (*)(PointLockManagerTest*);

class AnyLockManagerTest : public PointLockManagerTest,
                           public testing::WithParamInterface<init_func_t> {
 public:
  void SetUp() override {
    // If a custom setup function was provided, use it. Otherwise, use what we
    // have inherited.
    auto init_func = GetParam();
    if (init_func)
      (*init_func)(this);
    else
      PointLockManagerTest::SetUp();
  }
};

TEST_P(AnyLockManagerTest, ReentrantExclusiveLock) {
  // Tests that a txn can acquire exclusive lock on the same key repeatedly.
  MockColumnFamilyHandle cf(1);
  locker_->AddColumnFamily(&cf);
  auto txn = NewTxn();
  ASSERT_OK(locker_->TryLock(txn, 1, "k", env_, true));
  ASSERT_OK(locker_->TryLock(txn, 1, "k", env_, true));

  // Cleanup
  locker_->UnLock(txn, 1, "k", env_);

  delete txn;
}

TEST_P(AnyLockManagerTest, ReentrantSharedLock) {
  // Tests that a txn can acquire shared lock on the same key repeatedly.
  MockColumnFamilyHandle cf(1);
  locker_->AddColumnFamily(&cf);
  auto txn = NewTxn();
  ASSERT_OK(locker_->TryLock(txn, 1, "k", env_, false));
  ASSERT_OK(locker_->TryLock(txn, 1, "k", env_, false));

  // Cleanup
  locker_->UnLock(txn, 1, "k", env_);

  delete txn;
}

TEST_P(AnyLockManagerTest, LockUpgrade) {
  // Tests that a txn can upgrade from a shared lock to an exclusive lock.
  MockColumnFamilyHandle cf(1);
  locker_->AddColumnFamily(&cf);
  auto txn = NewTxn();
  ASSERT_OK(locker_->TryLock(txn, 1, "k", env_, false));
  ASSERT_OK(locker_->TryLock(txn, 1, "k", env_, true));

  // Cleanup
  locker_->UnLock(txn, 1, "k", env_);
  delete txn;
}

TEST_P(AnyLockManagerTest, LockDowngrade) {
  // Tests that a txn can acquire a shared lock after acquiring an exclusive
  // lock on the same key.
  MockColumnFamilyHandle cf(1);
  locker_->AddColumnFamily(&cf);
  auto txn = NewTxn();
  ASSERT_OK(locker_->TryLock(txn, 1, "k", env_, true));
  ASSERT_OK(locker_->TryLock(txn, 1, "k", env_, false));

  // Cleanup
  locker_->UnLock(txn, 1, "k", env_);
  delete txn;
}

TEST_P(AnyLockManagerTest, LockConflict) {
  // Tests that lock conflicts lead to lock timeout.
  MockColumnFamilyHandle cf(1);
  locker_->AddColumnFamily(&cf);
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

  // Cleanup
  locker_->UnLock(txn1, 1, "k1", env_);
  locker_->UnLock(txn1, 1, "k2", env_);

  delete txn1;
  delete txn2;
}

port::Thread BlockUntilWaitingTxn(const char* sync_point_name,
                                  std::function<void()> f) {
  std::atomic<bool> reached(false);
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      sync_point_name, [&](void* /*arg*/) { reached.store(true); });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  port::Thread t(f);

  while (!reached.load()) {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearAllCallBacks();

  return t;
}

TEST_P(AnyLockManagerTest, SharedLocks) {
  // Tests that shared locks can be concurrently held by multiple transactions.
  MockColumnFamilyHandle cf(1);
  locker_->AddColumnFamily(&cf);
  auto txn1 = NewTxn();
  auto txn2 = NewTxn();
  ASSERT_OK(locker_->TryLock(txn1, 1, "k", env_, false));
  ASSERT_OK(locker_->TryLock(txn2, 1, "k", env_, false));

  // Cleanup
  locker_->UnLock(txn1, 1, "k", env_);
  locker_->UnLock(txn2, 1, "k", env_);

  delete txn1;
  delete txn2;
}

TEST_P(AnyLockManagerTest, Deadlock) {
  // Tests that deadlock can be detected.
  // Deadlock scenario:
  // txn1 exclusively locks k1, and wants to lock k2;
  // txn2 exclusively locks k2, and wants to lock k1.
  MockColumnFamilyHandle cf(1);
  locker_->AddColumnFamily(&cf);
  TransactionOptions txn_opt;
  txn_opt.deadlock_detect = true;
  txn_opt.lock_timeout = 1000000;
  auto txn1 = NewTxn(txn_opt);
  auto txn2 = NewTxn(txn_opt);

  ASSERT_OK(locker_->TryLock(txn1, 1, "k1", env_, true));
  ASSERT_OK(locker_->TryLock(txn2, 1, "k2", env_, true));

  // txn1 tries to lock k2, will block forever.
  port::Thread t = BlockUntilWaitingTxn(wait_sync_point_name_, [&]() {
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

  // Cleanup
  locker_->UnLock(txn1, 1, "k1", env_);
  locker_->UnLock(txn1, 1, "k2", env_);
  delete txn2;
  delete txn1;
}

TEST_P(AnyLockManagerTest, GetWaitingTxns_MultipleTxns) {
  MockColumnFamilyHandle cf(1);
  locker_->AddColumnFamily(&cf);

  auto txn1 = NewTxn();
  ASSERT_OK(locker_->TryLock(txn1, 1, "k", env_, false));

  auto txn2 = NewTxn();
  ASSERT_OK(locker_->TryLock(txn2, 1, "k", env_, false));

  auto txn3 = NewTxn();
  txn3->SetLockTimeout(10000);
  port::Thread t1 = BlockUntilWaitingTxn(wait_sync_point_name_, [&]() {
    ASSERT_OK(locker_->TryLock(txn3, 1, "k", env_, true));
    locker_->UnLock(txn3, 1, "k", env_);
  });

  // Ok, now txn3 is waiting for lock on "k", which is owned by two
  // transactions. Check that GetWaitingTxns reports this correctly
  uint32_t wait_cf_id;
  std::string wait_key;
  auto waiters = txn3->GetWaitingTxns(&wait_cf_id, &wait_key);

  ASSERT_EQ(wait_cf_id, 1u);
  ASSERT_EQ(wait_key, "k");
  ASSERT_EQ(waiters.size(), 2);
  bool waits_correct =
      (waiters[0] == txn1->GetID() && waiters[1] == txn2->GetID()) ||
      (waiters[1] == txn1->GetID() && waiters[0] == txn2->GetID());
  ASSERT_EQ(waits_correct, true);

  // Release locks so txn3 can proceed with execution
  locker_->UnLock(txn1, 1, "k", env_);
  locker_->UnLock(txn2, 1, "k", env_);

  // Wait until txn3 finishes
  t1.join();

  delete txn1;
  delete txn2;
  delete txn3;
}

}  // namespace ROCKSDB_NAMESPACE

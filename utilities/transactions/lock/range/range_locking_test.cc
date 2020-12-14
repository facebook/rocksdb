#ifndef ROCKSDB_LITE
#ifndef OS_WIN

#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif

#include <algorithm>
#include <functional>
#include <string>
#include <thread>

#include "db/db_impl/db_impl.h"
#include "port/port.h"
#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/perf_context.h"
#include "rocksdb/utilities/transaction.h"
#include "rocksdb/utilities/transaction_db.h"
#include "utilities/transactions/lock/point/point_lock_manager_test.h"
#include "utilities/transactions/pessimistic_transaction_db.h"
#include "utilities/transactions/transaction_test.h"

using std::string;

namespace ROCKSDB_NAMESPACE {

class RangeLockingTest : public ::testing::Test {
 public:
  TransactionDB* db;
  std::string dbname;
  Options options;

  std::shared_ptr<RangeLockManagerHandle> range_lock_mgr;
  TransactionDBOptions txn_db_options;

  RangeLockingTest() : db(nullptr) {
    options.create_if_missing = true;
    dbname = test::PerThreadDBPath("range_locking_testdb");

    DestroyDB(dbname, options);
    Status s;

    range_lock_mgr.reset(NewRangeLockManager(nullptr));
    txn_db_options.lock_mgr_handle = range_lock_mgr;

    s = TransactionDB::Open(options, txn_db_options, dbname, &db);
    assert(s.ok());
  }

  ~RangeLockingTest() {
    delete db;
    db = nullptr;
    // This is to skip the assert statement in FaultInjectionTestEnv. There
    // seems to be a bug in btrfs that the makes readdir return recently
    // unlink-ed files. By using the default fs we simply ignore errors resulted
    // from attempting to delete such files in DestroyDB.
    DestroyDB(dbname, options);
  }

  PessimisticTransaction* NewTxn(
      TransactionOptions txn_opt = TransactionOptions()) {
    Transaction* txn = db->BeginTransaction(WriteOptions(), txn_opt);
    return reinterpret_cast<PessimisticTransaction*>(txn);
  }
};

// TODO: set a smaller lock wait timeout so that the test runs faster.
TEST_F(RangeLockingTest, BasicRangeLocking) {
  WriteOptions write_options;
  TransactionOptions txn_options;
  std::string value;
  ReadOptions read_options;
  auto cf = db->DefaultColumnFamily();

  Transaction* txn0 = db->BeginTransaction(write_options, txn_options);
  Transaction* txn1 = db->BeginTransaction(write_options, txn_options);

  // Get a range lock
  {
    auto s = txn0->GetRangeLock(cf, Endpoint("a"), Endpoint("c"));
    ASSERT_EQ(s, Status::OK());
  }

  // Check that range Lock inhibits an overlapping range lock
  {
    auto s = txn1->GetRangeLock(cf, Endpoint("b"), Endpoint("z"));
    ASSERT_TRUE(s.IsTimedOut());
  }

  // Check that range Lock inhibits an overlapping point lock
  {
    auto s = txn1->GetForUpdate(read_options, cf, Slice("b"), &value);
    ASSERT_TRUE(s.IsTimedOut());
  }

  // Get a point lock, check that it inhibits range locks
  {
    auto s = txn0->Put(cf, Slice("n"), Slice("value"));
    ASSERT_EQ(s, Status::OK());

    auto s2 = txn1->GetRangeLock(cf, Endpoint("m"), Endpoint("p"));
    ASSERT_TRUE(s2.IsTimedOut());
  }

  ASSERT_OK(txn0->Commit());
  txn1->Rollback();

  delete txn0;
  delete txn1;
}

TEST_F(RangeLockingTest, MyRocksLikeUpdate) {
  WriteOptions write_options;
  TransactionOptions txn_options;
  Transaction* txn0 = db->BeginTransaction(write_options, txn_options);
  auto cf = db->DefaultColumnFamily();
  Status s;

  // Get a range lock for the range we are about to update
  s = txn0->GetRangeLock(cf, Endpoint("a"), Endpoint("c"));
  ASSERT_EQ(s, Status::OK());

  bool try_range_lock_called = false;

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "RangeTreeLockManager::TryRangeLock:enter",
      [&](void* /*arg*/) { try_range_lock_called = true; });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  // For performance reasons, the following must NOT call lock_mgr->TryLock():
  // We verify that by checking the value of try_range_lock_called.
  s = txn0->Put(cf, Slice("b"), Slice("value"), /*assume_tracked=*/true);
  ASSERT_EQ(s, Status::OK());

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearAllCallBacks();
  ASSERT_EQ(try_range_lock_called, false);

  txn0->Rollback();

  delete txn0;
}

TEST_F(RangeLockingTest, SnapshotValidation) {
  Status s;
  Slice key_slice = Slice("k");
  ColumnFamilyHandle* cfh = db->DefaultColumnFamily();

  auto txn0 = NewTxn();
  txn0->Put(key_slice, Slice("initial"));
  txn0->Commit();

  // txn1
  auto txn1 = NewTxn();
  txn1->SetSnapshot();
  std::string val1;
  s = txn1->Get(ReadOptions(), cfh, key_slice, &val1);
  ASSERT_TRUE(s.ok());
  val1 = val1 + std::string("-txn1");

  s = txn1->Put(cfh, key_slice, Slice(val1));
  ASSERT_TRUE(s.ok());

  // txn2
  auto txn2 = NewTxn();
  txn2->SetSnapshot();
  std::string val2;
  // This will see the original value as nothing is committed
  // This is also Get, so it is doesn't acquire any locks.
  s = txn2->Get(ReadOptions(), cfh, key_slice, &val2);
  ASSERT_TRUE(s.ok());

  // txn1
  s = txn1->Commit();
  ASSERT_TRUE(s.ok());

  // txn2
  val2 = val2 + std::string("-txn2");
  // Now, this call should do Snapshot Validation and fail:
  s = txn2->Put(cfh, key_slice, Slice(val2));
  ASSERT_TRUE(s.IsBusy());

  s = txn2->Commit();
  ASSERT_TRUE(s.ok());

  /*
    // Not meaningful if s.IsBusy() above is true:
    // Examine the result
    auto txn3= NewTxn();
    std::string val3;
    Status s3 = txn3->Get(ReadOptions(), cfh, key_slice, &val3);
    fprintf(stderr, "Final: %s\n", val3.c_str());
  */
  delete txn0;
  delete txn1;
  delete txn2;
}

TEST_F(RangeLockingTest, MultipleTrxLockStatusData) {
  WriteOptions write_options;
  TransactionOptions txn_options;
  auto cf = db->DefaultColumnFamily();
  Status s;

  Transaction* txn0 = db->BeginTransaction(write_options, txn_options);
  Transaction* txn1 = db->BeginTransaction(write_options, txn_options);

  // Get a range lock
  s = txn0->GetRangeLock(cf, Endpoint("z"), Endpoint("z"));
  ASSERT_EQ(s, Status::OK());

  s = txn1->GetRangeLock(cf, Endpoint("b"), Endpoint("e"));
  ASSERT_EQ(s, Status::OK());

  auto s2 = range_lock_mgr->GetRangeLockStatusData();

  ASSERT_EQ(s2.size(), 2);
  delete txn0;
  delete txn1;
}

TEST_F(RangeLockingTest, BasicLockEscalation) {
  auto cf = db->DefaultColumnFamily();

  auto counters = range_lock_mgr->GetStatus();

  // Initially not using any lock memory
  ASSERT_EQ(counters.current_lock_memory, 0);
  ASSERT_EQ(counters.escalation_count, 0);

  ASSERT_EQ(0, range_lock_mgr->SetMaxLockMemory(2000));

  // Insert until we see lock escalations
  auto txn0= NewTxn();

  // Get the locks until we hit an escalation
  for (int i=0; i < 2020; i++) {
    char buf[32];
    snprintf(buf, sizeof(buf)-1, "%08d", i);
    auto s = txn0->GetRangeLock(cf, Endpoint(buf), Endpoint(buf));
    ASSERT_EQ(s, Status::OK());
  }
  counters = range_lock_mgr->GetStatus();
  ASSERT_GE(counters.escalation_count, 0);
  ASSERT_LE(counters.current_lock_memory, 2000);

  delete txn0;
}

void PointLockManagerTestExternalSetup(PointLockManagerTest* self) {
  self->env_ = Env::Default();
  self->db_dir_ = test::PerThreadDBPath("point_lock_manager_test");
  ASSERT_OK(self->env_->CreateDir(self->db_dir_));

  Options opt;
  opt.create_if_missing = true;
  TransactionDBOptions txn_opt;
  txn_opt.transaction_lock_timeout = 0;

  auto mutex_factory = std::make_shared<TransactionDBMutexFactoryImpl>();
  self->locker_.reset(NewRangeLockManager(mutex_factory)->getLockManager());
  std::shared_ptr<RangeLockManagerHandle> range_lock_mgr =
      std::dynamic_pointer_cast<RangeLockManagerHandle>(self->locker_);
  txn_opt.lock_mgr_handle = range_lock_mgr;

  ASSERT_OK(TransactionDB::Open(opt, txn_opt, self->db_dir_, &self->db_));
  self->wait_sync_point_name_ = "RangeTreeLockManager::TryRangeLock:WaitingTxn";
}

INSTANTIATE_TEST_CASE_P(RangeLockManager, AnyLockManagerTest,
                        ::testing::Values(PointLockManagerTestExternalSetup));

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

#else  // OS_WIN

#include <stdio.h>
int main(int /*argc*/, char** /*argv*/) {
  fprintf(stderr, "skipped as Range Locking is not supported on Windows\n");
  return 0;
}

#endif  // OS_WIN

#else
#include <stdio.h>

int main(int /*argc*/, char** /*argv*/) {
  fprintf(stderr,
          "skipped as transactions are not supported in rocksdb_lite\n");
  return 0;
}

#endif  // ROCKSDB_LITE

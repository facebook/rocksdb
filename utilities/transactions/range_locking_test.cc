#ifndef ROCKSDB_LITE

#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif

#include "utilities/transactions/transaction_test.h"

#include <algorithm>
#include <functional>
#include <string>
#include <thread>

#include "db/db_impl/db_impl.h"
#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/perf_context.h"
#include "rocksdb/utilities/transaction.h"
#include "rocksdb/utilities/transaction_db.h"
#include "table/mock_table.h"
#include "test_util/fault_injection_test_env.h"
#include "util/random.h"
#include "util/string_util.h"
#include "test_util/sync_point.h"
#include "test_util/testharness.h"
#include "test_util/testutil.h"
#include "test_util/transaction_test_util.h"
#include "utilities/merge_operators.h"
#include "utilities/merge_operators/string_append/stringappend.h"
#include "utilities/transactions/pessimistic_transaction_db.h"

#include "port/port.h"

using std::string;

namespace rocksdb {


class RangeLockingTest : public ::testing::Test {
 public:
  TransactionDB* db;
  std::string dbname;
  Options options;

  std::shared_ptr<RangeLockMgrHandle> range_lock_mgr;
  TransactionDBOptions txn_db_options;

  RangeLockingTest()
      : db(nullptr)  {
    options.create_if_missing = true;
    dbname = test::PerThreadDBPath("range_locking_testdb");

    DestroyDB(dbname, options);
    Status s;

    range_lock_mgr.reset(rocksdb::NewRangeLockManager(nullptr));
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

  Transaction* txn0 = db->BeginTransaction(write_options, txn_options);
  Transaction* txn1 = db->BeginTransaction(write_options, txn_options);

  // Get a range lock
  {
    auto s= txn0->GetRangeLock(db->DefaultColumnFamily(), 
                               Endpoint("a"), Endpoint("c"));
    ASSERT_EQ(s, Status::OK());
  }
 

  // Check that range Lock inhibits an overlapping range lock
  {
    auto s= txn1->GetRangeLock(db->DefaultColumnFamily(), 
                                Endpoint("b"), Endpoint("z"));
    ASSERT_TRUE(s.IsTimedOut());
  }

  // Check that range Lock inhibits an overlapping point lock
  {
    auto s= txn1->GetForUpdate(read_options, db->DefaultColumnFamily(),
                               Slice("b"), &value);
    ASSERT_TRUE(s.IsTimedOut());
  }

  // Get a point lock, check that it inhibits range locks
  {
    auto s= txn0->Put(db->DefaultColumnFamily(),
                      Slice("d"), Slice("value"));
    ASSERT_EQ(s, Status::OK());

    auto s2= txn1->GetRangeLock(db->DefaultColumnFamily(),
                                Endpoint("c"), Endpoint("e"));
    ASSERT_TRUE(s2.IsTimedOut());
  }

  ASSERT_OK(txn0->Commit());
  txn1->Rollback();

  delete txn0;
  delete txn1;
}

TEST_F(RangeLockingTest, SnapshotValidation) {
  Status s;
  Slice key_slice= Slice("k");
  ColumnFamilyHandle *cfh= db->DefaultColumnFamily();

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
}

}  // namespace rocksdb

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

#else
#include <stdio.h>

int main(int /*argc*/, char** /*argv*/) {
  fprintf(stderr,
          "SKIPPED as Transactions are not supported in ROCKSDB_LITE\n");
  return 0;
}

#endif  // ROCKSDB_LITE

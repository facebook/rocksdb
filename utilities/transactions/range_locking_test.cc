#ifndef ROCKSDB_LITE

#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif

#include "utilities/transactions/transaction_test.h"

#include <algorithm>
#include <functional>
#include <string>
#include <thread>

#include "db/db_impl.h"
#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/perf_context.h"
#include "rocksdb/utilities/transaction.h"
#include "rocksdb/utilities/transaction_db.h"
#include "table/mock_table.h"
#include "util/fault_injection_test_env.h"
#include "util/random.h"
#include "util/string_util.h"
#include "util/sync_point.h"
#include "util/testharness.h"
#include "util/testutil.h"
#include "util/transaction_test_util.h"
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
    dbname = test::PerThreadDBPath("transaction_testdb");

    DestroyDB(dbname, options);
    Status s;

    range_lock_mgr.reset(rocksdb::NewRangeLockManager(nullptr));
    txn_db_options.lock_mgr_handle = range_lock_mgr;

    s = TransactionDB::Open(options, txn_db_options, dbname, &db);
    assert(s.ok());

  }

  void VerifyLocks(ColumnFamilyHandle *cf, Transaction* txn,
                   const char *json, const std::string& ignore_lock);

  ~RangeLockingTest() {
    delete db;
    db = nullptr;
    // This is to skip the assert statement in FaultInjectionTestEnv. There
    // seems to be a bug in btrfs that the makes readdir return recently
    // unlink-ed files. By using the default fs we simply ignore errors resulted
    // from attempting to delete such files in DestroyDB.
    DestroyDB(dbname, options);
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

  txn0->SetLockTimeout(50);
  txn1->SetLockTimeout(50);

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

// Print a string into stream, with non-printable character escaped
void escape_print_str(std::ostringstream& oss, const std::string &s) {
  const char *hexstr= "0123456789ABCDEF";
  for (char c : s) {
    if (isprint(c))
      oss << c;
    else
      oss << "\\x" << hexstr[((c >> 8) & 0xF)] << hexstr[c & 0xF];
  }
}


using AcquiredLockMap = std::unordered_multimap<uint32_t, KeyLockInfo>;

// Dump the output of db-GetLockStatusData() into a siccint JSON-like format
//
//  @param default_cf   Assume normally locks are from this CF and don't
//                      mention it in the output
//  @param default_trx  Assume locks are from this TRX and don't mention it
//                      in the output
//  @param ignore_lock  Don't print the lock on this key

std::string DumpLocksToJson(AcquiredLockMap& lock_data, uint32_t default_cf,
                            TransactionID default_trx,
                            const std::string& ignore_lock) {
  std::ostringstream oss;
  bool first= true;
  for (auto iter: lock_data) {

    assert(iter.second.ids.size() > 0);
    if (iter.second.key == ignore_lock)
      continue;

    oss << "lock:{";
    if (iter.first != default_cf)
      oss << "cf:" << iter.first << ",";
    if (iter.second.has_key2) {
      oss << "start:'";
      escape_print_str(oss, iter.second.key);
      oss << "',end:'";
      escape_print_str(oss, iter.second.key2);
      oss << "'";
    } else {
      oss << "key:'";
      escape_print_str(oss, iter.second.key);
      oss << "'";
    }
    if (iter.second.ids.size() != 1 ||
        iter.second.ids.front() != default_trx) {
      oss << ",owners[";
      bool first_owner= true;
      for (auto o: iter.second.ids) {
        if (first_owner)
          first_owner= false;
        else
          oss << ",";
        oss << o;
      }
      oss << "]";
    }
    oss << "}";
    if (first)
      first= false;
    else
      oss << std::endl;
  }
  return oss.str();
}

// Verify that locks match the specified JSON-like description
void RangeLockingTest::VerifyLocks(ColumnFamilyHandle *cf, Transaction* txn,
                                   const char *json,
                                   const std::string& ignore_lock) {
  AcquiredLockMap lock_data = db->GetLockStatusData();
  std::string s = DumpLocksToJson(lock_data, cf->GetID(), txn->GetID(),
                                  ignore_lock);
  if (s != json) {
    std::cerr << "FAILURE: expected " << json << " ,got " << s << std::endl;
    assert(0);
  }
}

// Test for LockingIterator (aka SeekForUpdate) feature
TEST_F(RangeLockingTest, SeekForUpdate) {

  WriteOptions write_options;
  TransactionOptions txn_options;
  auto cf= db->DefaultColumnFamily();

  Transaction* txn0 = db->BeginTransaction(write_options, txn_options);
  // Insert some initial contents into the database
  txn0->Put(cf, "00", "value");
  txn0->Put(cf, "02", "value");
  txn0->Put(cf, "04", "value");
  txn0->Put(cf, "06", "value");
  txn0->Put(cf, "08", "value");
  txn0->Put(cf, "10", "value");

  txn0->Commit();
  delete txn0;
  txn0 = db->BeginTransaction(write_options, txn_options);
  txn0->Put(cf, "FF", "value");
  std::string ignored_key;
  ignored_key.push_back(0);
  ignored_key.append("FF");

  Transaction* txn1 = db->BeginTransaction(write_options, txn_options);

  // TODO: Does Locking + using snapshot make any sense? If not, should we
  // disallow it?
  ReadOptions read_options;
  Iterator *it = txn1->GetLockingIterator(read_options, cf);

  it->Seek("03");
  ASSERT_TRUE(it->Valid());
  ASSERT_EQ(it->key(), "04");
  VerifyLocks(cf, txn1, "lock:{start:'\\x0003',end:'\\x0004'}", ignored_key);

  it->Next();
  ASSERT_TRUE(it->Valid());
  ASSERT_EQ(it->key(), "06");
  VerifyLocks(cf, txn1, "lock:{start:'\\x0003',end:'\\x0006'}", ignored_key);

  it->Next();
  ASSERT_TRUE(it->Valid());
  ASSERT_EQ(it->key(), "08");
  VerifyLocks(cf, txn1, "lock:{start:'\\x0003',end:'\\x0008'}", ignored_key);

  // Try seeking where we won't find any data:
  it->Seek("AA");
  ASSERT_FALSE(it->Valid());
  VerifyLocks(cf, txn1, "lock:{start:'\\x0003',end:'\\x0008'}", ignored_key);

  delete it;
  txn1->Rollback();
  delete txn1;

  // Restart the transaction.
  txn1 = db->BeginTransaction(write_options, txn_options);
  it = txn1->GetLockingIterator(ReadOptions(), cf);


  // Now, try the backward scans.
  it->SeekForPrev("03");
  ASSERT_TRUE(it->Valid());
  ASSERT_EQ(it->key(), "02");
  VerifyLocks(cf, txn1, "lock:{start:'\\x0002',end:'\\x0003'}", ignored_key);

  delete it;
  txn1->Rollback();
  delete txn1;

  txn0->Rollback();
  delete txn0;
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

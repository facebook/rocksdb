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


void range_endpoint_convert_same(const rocksdb::Slice &key,
                                 std::string *res)
{
  res->clear();
  res->append(key.data(), key.size());
}

int range_endpoints_compare_default(const char *a, size_t a_len,
                                    const char *b, size_t b_len)
{
  return Slice(a, a_len).compare(Slice(b, b_len));
}

class RangeLockingTest : public ::testing::Test {
 public:
  TransactionDB* db;
  std::string dbname;
  Options options;

  TransactionDBOptions txn_db_options;

  RangeLockingTest()
      : db(nullptr)  {
    options.create_if_missing = true;
    dbname = test::PerThreadDBPath("transaction_testdb");

    DestroyDB(dbname, options);
    Status s;
    txn_db_options.use_range_locking = true;
    txn_db_options.range_locking_opts.cvt_func =
      range_endpoint_convert_same;
    txn_db_options.range_locking_opts.cmp_func =
      range_endpoints_compare_default;
    s = TransactionDB::Open(options, txn_db_options, dbname, &db);
    assert(s.ok());

    db->use_range_locking= true;
    rocksdb::RangeLockMgrControl *mgr= db->get_range_lock_manager();
    assert(mgr);
    // can also: mgr->set_max_lock_memory(rocksdb_max_lock_memory);
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
                               Slice("a"), Slice("c"));
    ASSERT_EQ(s, Status::OK());
  }
 

  // Check that range Lock inhibits an overlapping range lock
  {
    auto s= txn1->GetRangeLock(db->DefaultColumnFamily(), 
                                Slice("b"), Slice("z"));
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
                                Slice("c"), Slice("e"));
    ASSERT_TRUE(s2.IsTimedOut());
  }

  ASSERT_OK(txn0->Commit());
  txn1->Rollback();

  delete txn0;
  delete txn1;
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

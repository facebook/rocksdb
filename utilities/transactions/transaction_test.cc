//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#ifndef ROCKSDB_LITE

#include <algorithm>
#include <functional>
#include <string>
#include <thread>

#include "db/db_impl.h"
#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/utilities/transaction.h"
#include "rocksdb/utilities/transaction_db.h"
#include "table/mock_table.h"
#include "util/fault_injection_test_env.h"
#include "util/logging.h"
#include "util/random.h"
#include "util/string_util.h"
#include "util/sync_point.h"
#include "util/testharness.h"
#include "util/testutil.h"
#include "util/transaction_test_util.h"
#include "utilities/merge_operators.h"
#include "utilities/merge_operators/string_append/stringappend.h"

using std::string;

namespace rocksdb {

class TransactionTest : public ::testing::TestWithParam<bool> {
 public:
  TransactionDB* db;
  FaultInjectionTestEnv* env;
  string dbname;
  Options options;

  TransactionDBOptions txn_db_options;

  TransactionTest() {
    options.create_if_missing = true;
    options.max_write_buffer_number = 2;
    options.write_buffer_size = 4 * 1024;
    options.level0_file_num_compaction_trigger = 2;
    options.merge_operator = MergeOperators::CreateFromStringId("stringappend");
    env = new FaultInjectionTestEnv(Env::Default());
    options.env = env;
    dbname = test::TmpDir() + "/transaction_testdb";

    DestroyDB(dbname, options);
    txn_db_options.transaction_lock_timeout = 0;
    txn_db_options.default_lock_timeout = 0;
    Status s;
    if (GetParam() == false) {
      s = TransactionDB::Open(options, txn_db_options, dbname, &db);
    } else {
      s = OpenWithStackableDB();
    }
    assert(s.ok());
  }

  ~TransactionTest() {
    delete db;
    DestroyDB(dbname, options);
    delete env;
  }

  Status ReOpenNoDelete() {
    delete db;
    db = nullptr;
    env->AssertNoOpenFile();
    env->DropUnsyncedFileData();
    env->ResetState();
    Status s;
    if (GetParam() == false) {
      s = TransactionDB::Open(options, txn_db_options, dbname, &db);
    } else {
      s = OpenWithStackableDB();
    }
    return s;
  }

  Status ReOpen() {
    delete db;
    DestroyDB(dbname, options);
    Status s;
    if (GetParam() == false) {
      s = TransactionDB::Open(options, txn_db_options, dbname, &db);
    } else {
      s = OpenWithStackableDB();
    }
    return s;
  }

  Status OpenWithStackableDB() {
    std::vector<size_t> compaction_enabled_cf_indices;
    std::vector<ColumnFamilyDescriptor> column_families{ColumnFamilyDescriptor(
        kDefaultColumnFamilyName, ColumnFamilyOptions(options))};

    TransactionDB::PrepareWrap(&options, &column_families,
                               &compaction_enabled_cf_indices);
    std::vector<ColumnFamilyHandle*> handles;
    DB* root_db;
    Options options_copy(options);
    Status s =
        DB::Open(options_copy, dbname, column_families, &handles, &root_db);
    if (s.ok()) {
      assert(handles.size() == 1);
      s = TransactionDB::WrapStackableDB(
          new StackableDB(root_db), txn_db_options,
          compaction_enabled_cf_indices, handles, &db);
      delete handles[0];
    }
    return s;
  }
};

INSTANTIATE_TEST_CASE_P(DBAsBaseDB, TransactionTest, ::testing::Values(false));
INSTANTIATE_TEST_CASE_P(StackableDBAsBaseDB, TransactionTest,
                        ::testing::Values(true));

TEST_P(TransactionTest, DoubleEmptyWrite) {
  WriteOptions write_options;
  write_options.sync = true;
  write_options.disableWAL = false;

  WriteBatch batch;

  ASSERT_OK(db->Write(write_options, &batch));
  ASSERT_OK(db->Write(write_options, &batch));
}

TEST_P(TransactionTest, SuccessTest) {
  WriteOptions write_options;
  ReadOptions read_options;
  string value;
  Status s;

  db->Put(write_options, Slice("foo"), Slice("bar"));
  db->Put(write_options, Slice("foo2"), Slice("bar"));

  Transaction* txn = db->BeginTransaction(write_options, TransactionOptions());
  ASSERT_TRUE(txn);

  ASSERT_EQ(0, txn->GetNumPuts());
  ASSERT_LE(0, txn->GetID());

  s = txn->GetForUpdate(read_options, "foo", &value);
  ASSERT_OK(s);
  ASSERT_EQ(value, "bar");

  s = txn->Put(Slice("foo"), Slice("bar2"));
  ASSERT_OK(s);

  ASSERT_EQ(1, txn->GetNumPuts());

  s = txn->GetForUpdate(read_options, "foo", &value);
  ASSERT_OK(s);
  ASSERT_EQ(value, "bar2");

  s = txn->Commit();
  ASSERT_OK(s);

  s = db->Get(read_options, "foo", &value);
  ASSERT_OK(s);
  ASSERT_EQ(value, "bar2");

  delete txn;
}

TEST_P(TransactionTest, WaitingTxn) {
  WriteOptions write_options;
  ReadOptions read_options;
  TransactionOptions txn_options;
  string value;
  Status s;

  txn_options.lock_timeout = 1;
  s = db->Put(write_options, Slice("foo"), Slice("bar"));
  ASSERT_OK(s);

  /* create second cf */
  ColumnFamilyHandle* cfa;
  ColumnFamilyOptions cf_options;
  s = db->CreateColumnFamily(cf_options, "CFA", &cfa);
  ASSERT_OK(s);
  s = db->Put(write_options, cfa, Slice("foo"), Slice("bar"));
  ASSERT_OK(s);

  Transaction* txn1 = db->BeginTransaction(write_options, txn_options);
  Transaction* txn2 = db->BeginTransaction(write_options, txn_options);
  TransactionID id1 = txn1->GetID();
  ASSERT_TRUE(txn1);
  ASSERT_TRUE(txn2);

  rocksdb::SyncPoint::GetInstance()->SetCallBack(
      "TransactionLockMgr::AcquireWithTimeout:WaitingTxn", [&](void* arg) {
        std::string key;
        uint32_t cf_id;
        std::vector<TransactionID> wait = txn2->GetWaitingTxns(&cf_id, &key);
        ASSERT_EQ(key, "foo");
        ASSERT_EQ(wait.size(), 1);
        ASSERT_EQ(wait[0], id1);
        ASSERT_EQ(cf_id, 0);
      });

  // lock key in default cf
  s = txn1->GetForUpdate(read_options, "foo", &value);
  ASSERT_OK(s);
  ASSERT_EQ(value, "bar");

  // lock key in cfa
  s = txn1->GetForUpdate(read_options, cfa, "foo", &value);
  ASSERT_OK(s);
  ASSERT_EQ(value, "bar");

  auto lock_data = db->GetLockStatusData();
  // Locked keys exist in both column family.
  ASSERT_EQ(lock_data.size(), 2);

  auto cf_iterator = lock_data.begin();

  // The iterator points to an unordered_multimap
  // thus the test can not assume any particular order.

  // Column family is 1 or 0 (cfa).
  if (cf_iterator->first != 1 && cf_iterator->first != 0) {
    ASSERT_FALSE(true);
  }
  // The locked key is "foo" and is locked by txn1
  ASSERT_EQ(cf_iterator->second.key, "foo");
  ASSERT_EQ(cf_iterator->second.ids.size(), 1);
  ASSERT_EQ(cf_iterator->second.ids[0], txn1->GetID());

  cf_iterator++;

  // Column family is 0 (default) or 1.
  if (cf_iterator->first != 1 && cf_iterator->first != 0) {
    ASSERT_FALSE(true);
  }
  // The locked key is "foo" and is locked by txn1
  ASSERT_EQ(cf_iterator->second.key, "foo");
  ASSERT_EQ(cf_iterator->second.ids.size(), 1);
  ASSERT_EQ(cf_iterator->second.ids[0], txn1->GetID());

  rocksdb::SyncPoint::GetInstance()->EnableProcessing();

  s = txn2->GetForUpdate(read_options, "foo", &value);
  ASSERT_TRUE(s.IsTimedOut());
  ASSERT_EQ(s.ToString(), "Operation timed out: Timeout waiting to lock key");

  rocksdb::SyncPoint::GetInstance()->DisableProcessing();
  rocksdb::SyncPoint::GetInstance()->ClearAllCallBacks();

  delete cfa;
  delete txn1;
  delete txn2;
}

TEST_P(TransactionTest, SharedLocks) {
  WriteOptions write_options;
  ReadOptions read_options;
  TransactionOptions txn_options;
  Status s;

  txn_options.lock_timeout = 1;
  s = db->Put(write_options, Slice("foo"), Slice("bar"));
  ASSERT_OK(s);

  Transaction* txn1 = db->BeginTransaction(write_options, txn_options);
  Transaction* txn2 = db->BeginTransaction(write_options, txn_options);
  Transaction* txn3 = db->BeginTransaction(write_options, txn_options);
  ASSERT_TRUE(txn1);
  ASSERT_TRUE(txn2);
  ASSERT_TRUE(txn3);

  // Test shared access between txns
  s = txn1->GetForUpdate(read_options, "foo", nullptr, false /* exclusive */);
  ASSERT_OK(s);

  s = txn2->GetForUpdate(read_options, "foo", nullptr, false /* exclusive */);
  ASSERT_OK(s);

  s = txn3->GetForUpdate(read_options, "foo", nullptr, false /* exclusive */);
  ASSERT_OK(s);

  auto lock_data = db->GetLockStatusData();
  ASSERT_EQ(lock_data.size(), 1);

  auto cf_iterator = lock_data.begin();
  ASSERT_EQ(cf_iterator->second.key, "foo");

  // We compare whether the set of txns locking this key is the same. To do
  // this, we need to sort both vectors so that the comparison is done
  // correctly.
  std::vector<TransactionID> expected_txns = {txn1->GetID(), txn2->GetID(),
                                              txn3->GetID()};
  std::vector<TransactionID> lock_txns = cf_iterator->second.ids;
  ASSERT_EQ(expected_txns, lock_txns);
  ASSERT_FALSE(cf_iterator->second.exclusive);

  txn1->Rollback();
  txn2->Rollback();
  txn3->Rollback();

  // Test txn1 and txn2 sharing a lock and txn3 trying to obtain it.
  s = txn1->GetForUpdate(read_options, "foo", nullptr, false /* exclusive */);
  ASSERT_OK(s);

  s = txn2->GetForUpdate(read_options, "foo", nullptr, false /* exclusive */);
  ASSERT_OK(s);

  s = txn3->GetForUpdate(read_options, "foo", nullptr);
  ASSERT_TRUE(s.IsTimedOut());
  ASSERT_EQ(s.ToString(), "Operation timed out: Timeout waiting to lock key");

  txn1->UndoGetForUpdate("foo");
  s = txn3->GetForUpdate(read_options, "foo", nullptr);
  ASSERT_TRUE(s.IsTimedOut());
  ASSERT_EQ(s.ToString(), "Operation timed out: Timeout waiting to lock key");

  txn2->UndoGetForUpdate("foo");
  s = txn3->GetForUpdate(read_options, "foo", nullptr);
  ASSERT_OK(s);

  txn1->Rollback();
  txn2->Rollback();
  txn3->Rollback();

  // Test txn1 holding an exclusive lock and txn2 trying to obtain shared
  // access.
  s = txn1->GetForUpdate(read_options, "foo", nullptr);
  ASSERT_OK(s);

  s = txn2->GetForUpdate(read_options, "foo", nullptr, false /* exclusive */);
  ASSERT_TRUE(s.IsTimedOut());
  ASSERT_EQ(s.ToString(), "Operation timed out: Timeout waiting to lock key");

  txn1->UndoGetForUpdate("foo");
  s = txn2->GetForUpdate(read_options, "foo", nullptr, false /* exclusive */);
  ASSERT_OK(s);

  delete txn1;
  delete txn2;
  delete txn3;
}

TEST_P(TransactionTest, DeadlockCycleShared) {
  WriteOptions write_options;
  ReadOptions read_options;
  TransactionOptions txn_options;

  txn_options.lock_timeout = 1000000;
  txn_options.deadlock_detect = true;

  // Set up a wait for chain like this:
  //
  // Tn -> T(n*2)
  // Tn -> T(n*2 + 1)
  //
  // So we have:
  // T1 -> T2 -> T4 ...
  //    |     |> T5 ...
  //    |> T3 -> T6 ...
  //          |> T7 ...
  // up to T31, then T[16 - 31] -> T1.
  // Note that Tn holds lock on floor(n / 2).

  std::vector<Transaction*> txns(31);

  for (uint32_t i = 0; i < 31; i++) {
    txns[i] = db->BeginTransaction(write_options, txn_options);
    ASSERT_TRUE(txns[i]);
    auto s = txns[i]->GetForUpdate(read_options, ToString((i + 1) / 2), nullptr,
                                   false /* exclusive */);
    ASSERT_OK(s);
  }

  std::atomic<uint32_t> checkpoints(0);
  rocksdb::SyncPoint::GetInstance()->SetCallBack(
      "TransactionLockMgr::AcquireWithTimeout:WaitingTxn",
      [&](void* arg) { checkpoints.fetch_add(1); });
  rocksdb::SyncPoint::GetInstance()->EnableProcessing();

  // We want the leaf transactions to block and hold everyone back.
  std::vector<std::thread> threads;
  for (uint32_t i = 0; i < 15; i++) {
    std::function<void()> blocking_thread = [&, i] {
      auto s = txns[i]->GetForUpdate(read_options, ToString(i + 1), nullptr,
                                     true /* exclusive */);
      ASSERT_OK(s);
      txns[i]->Rollback();
      delete txns[i];
    };
    threads.emplace_back(blocking_thread);
  }

  // Wait until all threads are waiting on each other.
  while (checkpoints.load() != 15) {
    /* sleep override */
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
  rocksdb::SyncPoint::GetInstance()->DisableProcessing();
  rocksdb::SyncPoint::GetInstance()->ClearAllCallBacks();

  // Complete the cycle T[16 - 31] -> T1
  for (uint32_t i = 15; i < 31; i++) {
    auto s =
        txns[i]->GetForUpdate(read_options, "0", nullptr, true /* exclusive */);
    ASSERT_TRUE(s.IsDeadlock());
  }

  // Rollback the leaf transaction.
  for (uint32_t i = 15; i < 31; i++) {
    txns[i]->Rollback();
    delete txns[i];
  }

  for (auto& t : threads) {
    t.join();
  }
}

TEST_P(TransactionTest, DeadlockCycle) {
  WriteOptions write_options;
  ReadOptions read_options;
  TransactionOptions txn_options;

  const uint32_t kMaxCycleLength = 50;

  txn_options.lock_timeout = 1000000;
  txn_options.deadlock_detect = true;

  for (uint32_t len = 2; len < kMaxCycleLength; len++) {
    // Set up a long wait for chain like this:
    //
    // T1 -> T2 -> T3 -> ... -> Tlen
    std::vector<Transaction*> txns(len);

    for (uint32_t i = 0; i < len; i++) {
      txns[i] = db->BeginTransaction(write_options, txn_options);
      ASSERT_TRUE(txns[i]);
      auto s = txns[i]->GetForUpdate(read_options, ToString(i), nullptr);
      ASSERT_OK(s);
    }

    std::atomic<uint32_t> checkpoints(0);
    rocksdb::SyncPoint::GetInstance()->SetCallBack(
        "TransactionLockMgr::AcquireWithTimeout:WaitingTxn",
        [&](void* arg) { checkpoints.fetch_add(1); });
    rocksdb::SyncPoint::GetInstance()->EnableProcessing();

    // We want the last transaction in the chain to block and hold everyone
    // back.
    std::vector<std::thread> threads;
    for (uint32_t i = 0; i < len - 1; i++) {
      std::function<void()> blocking_thread = [&, i] {
        auto s =
            txns[i]->GetForUpdate(read_options, ToString(i + 1), nullptr);
        ASSERT_OK(s);
        txns[i]->Rollback();
        delete txns[i];
      };
      threads.emplace_back(blocking_thread);
    }

    // Wait until all threads are waiting on each other.
    while (checkpoints.load() != len - 1) {
      /* sleep override */
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    rocksdb::SyncPoint::GetInstance()->DisableProcessing();
    rocksdb::SyncPoint::GetInstance()->ClearAllCallBacks();

    // Complete the cycle Tlen -> T1
    auto s = txns[len - 1]->GetForUpdate(read_options, "0", nullptr);
    ASSERT_TRUE(s.IsDeadlock());

    // Rollback the last transaction.
    txns[len - 1]->Rollback();
    delete txns[len - 1];

    for (auto& t : threads) {
      t.join();
    }
  }
}

TEST_P(TransactionTest, DeadlockStress) {
  const uint32_t NUM_TXN_THREADS = 10;
  const uint32_t NUM_KEYS = 100;
  const uint32_t NUM_ITERS = 100000;

  WriteOptions write_options;
  ReadOptions read_options;
  TransactionOptions txn_options;

  txn_options.lock_timeout = 1000000;
  txn_options.deadlock_detect = true;
  std::vector<std::string> keys;

  for (uint32_t i = 0; i < NUM_KEYS; i++) {
    db->Put(write_options, Slice(ToString(i)), Slice(""));
    keys.push_back(ToString(i));
  }

  size_t tid = std::hash<std::thread::id>()(std::this_thread::get_id());
  Random rnd(static_cast<uint32_t>(tid));
  std::function<void(uint32_t)> stress_thread = [&](uint32_t seed) {
    std::default_random_engine g(seed);

    Transaction* txn;
    for (uint32_t i = 0; i < NUM_ITERS; i++) {
      txn = db->BeginTransaction(write_options, txn_options);
      auto random_keys = keys;
      std::shuffle(random_keys.begin(), random_keys.end(), g);

      // Lock keys in random order.
      for (const auto& k : random_keys) {
        // Lock mostly for shared access, but exclusive 1/4 of the time.
        auto s =
            txn->GetForUpdate(read_options, k, nullptr, txn->GetID() % 4 == 0);
        if (!s.ok()) {
          ASSERT_TRUE(s.IsDeadlock());
          txn->Rollback();
          break;
        }
      }

      delete txn;
    }
  };

  std::vector<std::thread> threads;
  for (uint32_t i = 0; i < NUM_TXN_THREADS; i++) {
    threads.emplace_back(stress_thread, rnd.Next());
  }

  for (auto& t : threads) {
    t.join();
  }
}

TEST_P(TransactionTest, CommitTimeBatchFailTest) {
  WriteOptions write_options;
  TransactionOptions txn_options;

  string value;
  Status s;

  Transaction* txn1 = db->BeginTransaction(write_options, txn_options);
  ASSERT_TRUE(txn1);

  txn1->GetCommitTimeWriteBatch()->Put("cat", "dog");

  s = txn1->Put("foo", "bar");
  ASSERT_OK(s);

  // fails due to non-empty commit-time batch
  s = txn1->Commit();
  ASSERT_EQ(s, Status::InvalidArgument());

  delete txn1;
}

TEST_P(TransactionTest, SimpleTwoPhaseTransactionTest) {
  WriteOptions write_options;
  ReadOptions read_options;

  TransactionOptions txn_options;

  string value;
  Status s;

  DBImpl* db_impl = reinterpret_cast<DBImpl*>(db->GetRootDB());

  Transaction* txn = db->BeginTransaction(write_options, txn_options);
  s = txn->SetName("xid");
  ASSERT_OK(s);

  ASSERT_EQ(db->GetTransactionByName("xid"), txn);

  // transaction put
  s = txn->Put(Slice("foo"), Slice("bar"));
  ASSERT_OK(s);
  ASSERT_EQ(1, txn->GetNumPuts());

  // regular db put
  s = db->Put(write_options, Slice("foo2"), Slice("bar2"));
  ASSERT_OK(s);
  ASSERT_EQ(1, txn->GetNumPuts());

  // regular db read
  db->Get(read_options, "foo2", &value);
  ASSERT_EQ(value, "bar2");

  // commit time put
  txn->GetCommitTimeWriteBatch()->Put(Slice("gtid"), Slice("dogs"));
  txn->GetCommitTimeWriteBatch()->Put(Slice("gtid2"), Slice("cats"));

  // nothing has been prepped yet
  ASSERT_EQ(db_impl->TEST_FindMinLogContainingOutstandingPrep(), 0);

  s = txn->Prepare();
  ASSERT_OK(s);

  // data not im mem yet
  s = db->Get(read_options, Slice("foo"), &value);
  ASSERT_TRUE(s.IsNotFound());
  s = db->Get(read_options, Slice("gtid"), &value);
  ASSERT_TRUE(s.IsNotFound());

  // find trans in list of prepared transactions
  std::vector<Transaction*> prepared_trans;
  db->GetAllPreparedTransactions(&prepared_trans);
  ASSERT_EQ(prepared_trans.size(), 1);
  ASSERT_EQ(prepared_trans.front()->GetName(), "xid");

  auto log_containing_prep =
      db_impl->TEST_FindMinLogContainingOutstandingPrep();
  ASSERT_GT(log_containing_prep, 0);

  // make commit
  s = txn->Commit();
  ASSERT_OK(s);

  // value is now available
  s = db->Get(read_options, "foo", &value);
  ASSERT_OK(s);
  ASSERT_EQ(value, "bar");

  s = db->Get(read_options, "gtid", &value);
  ASSERT_OK(s);
  ASSERT_EQ(value, "dogs");

  s = db->Get(read_options, "gtid2", &value);
  ASSERT_OK(s);
  ASSERT_EQ(value, "cats");

  // we already committed
  s = txn->Commit();
  ASSERT_EQ(s, Status::InvalidArgument());

  // no longer is prpared results
  db->GetAllPreparedTransactions(&prepared_trans);
  ASSERT_EQ(prepared_trans.size(), 0);
  ASSERT_EQ(db->GetTransactionByName("xid"), nullptr);

  // heap should not care about prepared section anymore
  ASSERT_EQ(db_impl->TEST_FindMinLogContainingOutstandingPrep(), 0);

  // but now our memtable should be referencing the prep section
  ASSERT_EQ(log_containing_prep,
            db_impl->TEST_FindMinPrepLogReferencedByMemTable());

  db_impl->TEST_FlushMemTable(true);

  // after memtable flush we can now relese the log
  ASSERT_EQ(0, db_impl->TEST_FindMinPrepLogReferencedByMemTable());

  delete txn;
}

TEST_P(TransactionTest, TwoPhaseNameTest) {
  Status s;

  WriteOptions write_options;
  TransactionOptions txn_options;
  Transaction* txn1 = db->BeginTransaction(write_options, txn_options);
  Transaction* txn2 = db->BeginTransaction(write_options, txn_options);
  Transaction* txn3 = db->BeginTransaction(write_options, txn_options);
  ASSERT_TRUE(txn3);
  delete txn3;

  // cant prepare txn without name
  s = txn1->Prepare();
  ASSERT_EQ(s, Status::InvalidArgument());

  // name too short
  s = txn1->SetName("");
  ASSERT_EQ(s, Status::InvalidArgument());

  // name too long
  s = txn1->SetName(std::string(513, 'x'));
  ASSERT_EQ(s, Status::InvalidArgument());

  // valid set name
  s = txn1->SetName("name1");
  ASSERT_OK(s);

  // cant have duplicate name
  s = txn2->SetName("name1");
  ASSERT_EQ(s, Status::InvalidArgument());

  // shouldn't be able to prepare
  s = txn2->Prepare();
  ASSERT_EQ(s, Status::InvalidArgument());

  // valid name set
  s = txn2->SetName("name2");
  ASSERT_OK(s);

  // cant reset name
  s = txn2->SetName("name3");
  ASSERT_EQ(s, Status::InvalidArgument());

  ASSERT_EQ(txn1->GetName(), "name1");
  ASSERT_EQ(txn2->GetName(), "name2");

  s = txn1->Prepare();
  ASSERT_OK(s);

  // can't rename after prepare
  s = txn1->SetName("name4");
  ASSERT_EQ(s, Status::InvalidArgument());

  delete txn1;
  delete txn2;
}

TEST_P(TransactionTest, TwoPhaseEmptyWriteTest) {
  Status s;
  std::string value;

  WriteOptions write_options;
  ReadOptions read_options;
  TransactionOptions txn_options;
  Transaction* txn1 = db->BeginTransaction(write_options, txn_options);
  ASSERT_TRUE(txn1);
  Transaction* txn2 = db->BeginTransaction(write_options, txn_options);
  ASSERT_TRUE(txn2);

  s = txn1->SetName("joe");
  ASSERT_OK(s);

  s = txn2->SetName("bob");
  ASSERT_OK(s);

  s = txn1->Prepare();
  ASSERT_OK(s);

  s = txn1->Commit();
  ASSERT_OK(s);

  delete txn1;

  txn2->GetCommitTimeWriteBatch()->Put(Slice("foo"), Slice("bar"));

  s = txn2->Prepare();
  ASSERT_OK(s);

  s = txn2->Commit();
  ASSERT_OK(s);

  s = db->Get(read_options, "foo", &value);
  ASSERT_OK(s);
  ASSERT_EQ(value, "bar");

  delete txn2;
}

TEST_P(TransactionTest, TwoPhaseExpirationTest) {
  Status s;

  WriteOptions write_options;
  TransactionOptions txn_options;
  txn_options.expiration = 500;  // 500ms
  Transaction* txn1 = db->BeginTransaction(write_options, txn_options);
  Transaction* txn2 = db->BeginTransaction(write_options, txn_options);
  ASSERT_TRUE(txn1);
  ASSERT_TRUE(txn1);

  s = txn1->SetName("joe");
  ASSERT_OK(s);
  s = txn2->SetName("bob");
  ASSERT_OK(s);

  s = txn1->Prepare();
  ASSERT_OK(s);

  /* sleep override */
  std::this_thread::sleep_for(std::chrono::milliseconds(1000));

  s = txn1->Commit();
  ASSERT_OK(s);

  s = txn2->Prepare();
  ASSERT_EQ(s, Status::Expired());

  delete txn1;
  delete txn2;
}

TEST_P(TransactionTest, TwoPhaseRollbackTest) {
  WriteOptions write_options;
  ReadOptions read_options;

  TransactionOptions txn_options;

  string value;
  Status s;

  DBImpl* db_impl = reinterpret_cast<DBImpl*>(db->GetRootDB());
  Transaction* txn = db->BeginTransaction(write_options, txn_options);
  s = txn->SetName("xid");
  ASSERT_OK(s);

  // transaction put
  s = txn->Put(Slice("tfoo"), Slice("tbar"));
  ASSERT_OK(s);

  // value is readable form txn
  s = txn->Get(read_options, Slice("tfoo"), &value);
  ASSERT_OK(s);
  ASSERT_EQ(value, "tbar");

  // issue rollback
  s = txn->Rollback();
  ASSERT_OK(s);

  // value is nolonger readable
  s = txn->Get(read_options, Slice("tfoo"), &value);
  ASSERT_TRUE(s.IsNotFound());
  ASSERT_EQ(txn->GetNumPuts(), 0);

  // put new txn values
  s = txn->Put(Slice("tfoo2"), Slice("tbar2"));
  ASSERT_OK(s);

  // new value is readable from txn
  s = txn->Get(read_options, Slice("tfoo2"), &value);
  ASSERT_OK(s);
  ASSERT_EQ(value, "tbar2");

  s = txn->Prepare();
  ASSERT_OK(s);

  // flush to next wal
  s = db->Put(write_options, Slice("foo"), Slice("bar"));
  ASSERT_OK(s);
  db_impl->TEST_FlushMemTable(true);

  // issue rollback (marker written to WAL)
  s = txn->Rollback();
  ASSERT_OK(s);

  // value is nolonger readable
  s = txn->Get(read_options, Slice("tfoo2"), &value);
  ASSERT_TRUE(s.IsNotFound());
  ASSERT_EQ(txn->GetNumPuts(), 0);

  // make commit
  s = txn->Commit();
  ASSERT_EQ(s, Status::InvalidArgument());

  // try rollback again
  s = txn->Rollback();
  ASSERT_EQ(s, Status::InvalidArgument());

  delete txn;
}

TEST_P(TransactionTest, PersistentTwoPhaseTransactionTest) {
  WriteOptions write_options;
  write_options.sync = true;
  write_options.disableWAL = false;
  ReadOptions read_options;

  TransactionOptions txn_options;

  string value;
  Status s;

  DBImpl* db_impl = reinterpret_cast<DBImpl*>(db->GetRootDB());

  Transaction* txn = db->BeginTransaction(write_options, txn_options);
  s = txn->SetName("xid");
  ASSERT_OK(s);

  ASSERT_EQ(db->GetTransactionByName("xid"), txn);

  // transaction put
  s = txn->Put(Slice("foo"), Slice("bar"));
  ASSERT_OK(s);
  ASSERT_EQ(1, txn->GetNumPuts());

  // txn read
  s = txn->Get(read_options, "foo", &value);
  ASSERT_OK(s);
  ASSERT_EQ(value, "bar");

  // regular db put
  s = db->Put(write_options, Slice("foo2"), Slice("bar2"));
  ASSERT_OK(s);
  ASSERT_EQ(1, txn->GetNumPuts());

  db_impl->TEST_FlushMemTable(true);

  // regular db read
  db->Get(read_options, "foo2", &value);
  ASSERT_EQ(value, "bar2");

  // nothing has been prepped yet
  ASSERT_EQ(db_impl->TEST_FindMinLogContainingOutstandingPrep(), 0);

  // prepare
  s = txn->Prepare();
  ASSERT_OK(s);

  // still not available to db
  s = db->Get(read_options, Slice("foo"), &value);
  ASSERT_TRUE(s.IsNotFound());

  delete txn;
  // kill and reopen
  s = ReOpenNoDelete();
  ASSERT_OK(s);
  db_impl = reinterpret_cast<DBImpl*>(db->GetRootDB());

  // find trans in list of prepared transactions
  std::vector<Transaction*> prepared_trans;
  db->GetAllPreparedTransactions(&prepared_trans);
  ASSERT_EQ(prepared_trans.size(), 1);

  txn = prepared_trans.front();
  ASSERT_TRUE(txn);
  ASSERT_EQ(txn->GetName(), "xid");
  ASSERT_EQ(db->GetTransactionByName("xid"), txn);

  // log has been marked
  auto log_containing_prep =
      db_impl->TEST_FindMinLogContainingOutstandingPrep();
  ASSERT_GT(log_containing_prep, 0);

  // value is readable from txn
  s = txn->Get(read_options, "foo", &value);
  ASSERT_OK(s);
  ASSERT_EQ(value, "bar");

  // make commit
  s = txn->Commit();
  ASSERT_OK(s);

  // value is now available
  db->Get(read_options, "foo", &value);
  ASSERT_EQ(value, "bar");

  // we already committed
  s = txn->Commit();
  ASSERT_EQ(s, Status::InvalidArgument());

  // no longer is prpared results
  prepared_trans.clear();
  db->GetAllPreparedTransactions(&prepared_trans);
  ASSERT_EQ(prepared_trans.size(), 0);

  // transaction should no longer be visible
  ASSERT_EQ(db->GetTransactionByName("xid"), nullptr);

  // heap should not care about prepared section anymore
  ASSERT_EQ(db_impl->TEST_FindMinLogContainingOutstandingPrep(), 0);

  // but now our memtable should be referencing the prep section
  ASSERT_EQ(log_containing_prep,
            db_impl->TEST_FindMinPrepLogReferencedByMemTable());

  db_impl->TEST_FlushMemTable(true);

  // after memtable flush we can now relese the log
  ASSERT_EQ(0, db_impl->TEST_FindMinPrepLogReferencedByMemTable());

  delete txn;

  // deleting transaction should unregister transaction
  ASSERT_EQ(db->GetTransactionByName("xid"), nullptr);
}

TEST_P(TransactionTest, TwoPhaseMultiThreadTest) {
  // mix transaction writes and regular writes
  const uint32_t NUM_TXN_THREADS = 50;
  std::atomic<uint32_t> txn_thread_num(0);

  std::function<void()> txn_write_thread = [&]() {
    uint32_t id = txn_thread_num.fetch_add(1);

    WriteOptions write_options;
    write_options.sync = true;
    write_options.disableWAL = false;
    TransactionOptions txn_options;
    txn_options.lock_timeout = 1000000;
    if (id % 2 == 0) {
      txn_options.expiration = 1000000;
    }
    TransactionName name("xid_" + std::string(1, 'A' + id));
    Transaction* txn = db->BeginTransaction(write_options, txn_options);
    ASSERT_OK(txn->SetName(name));
    for (int i = 0; i < 10; i++) {
      std::string key(name + "_" + std::string(1, 'A' + i));
      ASSERT_OK(txn->Put(key, "val"));
    }
    ASSERT_OK(txn->Prepare());
    ASSERT_OK(txn->Commit());
    delete txn;
  };

  // assure that all thread are in the same write group
  std::atomic<uint32_t> t_wait_on_prepare(0);
  std::atomic<uint32_t> t_wait_on_commit(0);

  rocksdb::SyncPoint::GetInstance()->SetCallBack(
      "WriteThread::JoinBatchGroup:Wait", [&](void* arg) {
        auto* writer = reinterpret_cast<WriteThread::Writer*>(arg);

        if (writer->ShouldWriteToWAL()) {
          t_wait_on_prepare.fetch_add(1);
          // wait for friends
          while (t_wait_on_prepare.load() < NUM_TXN_THREADS) {
            env->SleepForMicroseconds(10);
          }
        } else if (writer->ShouldWriteToMemtable()) {
          t_wait_on_commit.fetch_add(1);
          // wait for friends
          while (t_wait_on_commit.load() < NUM_TXN_THREADS) {
            env->SleepForMicroseconds(10);
          }
        } else {
          ASSERT_TRUE(false);
        }
      });

  rocksdb::SyncPoint::GetInstance()->EnableProcessing();

  // do all the writes
  std::vector<std::thread> threads;
  for (uint32_t i = 0; i < NUM_TXN_THREADS; i++) {
    threads.emplace_back(txn_write_thread);
  }
  for (auto& t : threads) {
    t.join();
  }

  rocksdb::SyncPoint::GetInstance()->DisableProcessing();
  rocksdb::SyncPoint::GetInstance()->ClearAllCallBacks();

  ReadOptions read_options;
  std::string value;
  Status s;
  for (uint32_t t = 0; t < NUM_TXN_THREADS; t++) {
    TransactionName name("xid_" + std::string(1, 'A' + t));
    for (int i = 0; i < 10; i++) {
      std::string key(name + "_" + std::string(1, 'A' + i));
      s = db->Get(read_options, key, &value);
      ASSERT_OK(s);
      ASSERT_EQ(value, "val");
    }
  }
}

TEST_P(TransactionTest, TwoPhaseLongPrepareTest) {
  WriteOptions write_options;
  write_options.sync = true;
  write_options.disableWAL = false;
  ReadOptions read_options;
  TransactionOptions txn_options;

  std::string value;
  Status s;

  Transaction* txn = db->BeginTransaction(write_options, txn_options);
  s = txn->SetName("bob");
  ASSERT_OK(s);

  // transaction put
  s = txn->Put(Slice("foo"), Slice("bar"));
  ASSERT_OK(s);

  // prepare
  s = txn->Prepare();
  ASSERT_OK(s);

  delete txn;

  for (int i = 0; i < 1000; i++) {
    std::string key(i, 'k');
    std::string val(1000, 'v');
    s = db->Put(write_options, key, val);
    ASSERT_OK(s);

    if (i % 29 == 0) {
      // crash
      env->SetFilesystemActive(false);
      ReOpenNoDelete();
    } else if (i % 37 == 0) {
      // close
      ReOpenNoDelete();
    }
  }

  // commit old txn
  txn = db->GetTransactionByName("bob");
  ASSERT_TRUE(txn);
  s = txn->Commit();
  ASSERT_OK(s);

  // verify data txn data
  s = db->Get(read_options, "foo", &value);
  ASSERT_EQ(s, Status::OK());
  ASSERT_EQ(value, "bar");

  // verify non txn data
  for (int i = 0; i < 1000; i++) {
    std::string key(i, 'k');
    std::string val(1000, 'v');
    s = db->Get(read_options, key, &value);
    ASSERT_EQ(s, Status::OK());
    ASSERT_EQ(value, val);
  }

  delete txn;
}

TEST_P(TransactionTest, TwoPhaseSequenceTest) {
  WriteOptions write_options;
  write_options.sync = true;
  write_options.disableWAL = false;
  ReadOptions read_options;

  TransactionOptions txn_options;

  std::string value;
  Status s;

  Transaction* txn = db->BeginTransaction(write_options, txn_options);
  s = txn->SetName("xid");
  ASSERT_OK(s);

  // transaction put
  s = txn->Put(Slice("foo"), Slice("bar"));
  ASSERT_OK(s);
  s = txn->Put(Slice("foo2"), Slice("bar2"));
  ASSERT_OK(s);
  s = txn->Put(Slice("foo3"), Slice("bar3"));
  ASSERT_OK(s);
  s = txn->Put(Slice("foo4"), Slice("bar4"));
  ASSERT_OK(s);

  // prepare
  s = txn->Prepare();
  ASSERT_OK(s);

  // make commit
  s = txn->Commit();
  ASSERT_OK(s);

  delete txn;

  // kill and reopen
  env->SetFilesystemActive(false);
  ReOpenNoDelete();

  // value is now available
  s = db->Get(read_options, "foo4", &value);
  ASSERT_EQ(s, Status::OK());
  ASSERT_EQ(value, "bar4");
}

TEST_P(TransactionTest, TwoPhaseDoubleRecoveryTest) {
  WriteOptions write_options;
  write_options.sync = true;
  write_options.disableWAL = false;
  ReadOptions read_options;

  TransactionOptions txn_options;

  std::string value;
  Status s;

  Transaction* txn = db->BeginTransaction(write_options, txn_options);
  s = txn->SetName("a");
  ASSERT_OK(s);

  // transaction put
  s = txn->Put(Slice("foo"), Slice("bar"));
  ASSERT_OK(s);

  // prepare
  s = txn->Prepare();
  ASSERT_OK(s);

  delete txn;

  // kill and reopen
  env->SetFilesystemActive(false);
  ReOpenNoDelete();

  // commit old txn
  txn = db->GetTransactionByName("a");
  s = txn->Commit();
  ASSERT_OK(s);

  s = db->Get(read_options, "foo", &value);
  ASSERT_EQ(s, Status::OK());
  ASSERT_EQ(value, "bar");

  delete txn;

  txn = db->BeginTransaction(write_options, txn_options);
  s = txn->SetName("b");
  ASSERT_OK(s);

  s = txn->Put(Slice("foo2"), Slice("bar2"));
  ASSERT_OK(s);

  s = txn->Prepare();
  ASSERT_OK(s);

  s = txn->Commit();
  ASSERT_OK(s);

  delete txn;

  // kill and reopen
  env->SetFilesystemActive(false);
  ReOpenNoDelete();

  // value is now available
  s = db->Get(read_options, "foo", &value);
  ASSERT_EQ(s, Status::OK());
  ASSERT_EQ(value, "bar");

  s = db->Get(read_options, "foo2", &value);
  ASSERT_EQ(s, Status::OK());
  ASSERT_EQ(value, "bar2");
}

TEST_P(TransactionTest, TwoPhaseLogRollingTest) {
  DBImpl* db_impl = reinterpret_cast<DBImpl*>(db->GetRootDB());

  Status s;
  string v;
  ColumnFamilyHandle *cfa, *cfb;

  // Create 2 new column families
  ColumnFamilyOptions cf_options;
  s = db->CreateColumnFamily(cf_options, "CFA", &cfa);
  ASSERT_OK(s);
  s = db->CreateColumnFamily(cf_options, "CFB", &cfb);
  ASSERT_OK(s);

  WriteOptions wopts;
  wopts.disableWAL = false;
  wopts.sync = true;

  TransactionOptions topts1;
  Transaction* txn1 = db->BeginTransaction(wopts, topts1);
  s = txn1->SetName("xid1");
  ASSERT_OK(s);

  TransactionOptions topts2;
  Transaction* txn2 = db->BeginTransaction(wopts, topts2);
  s = txn2->SetName("xid2");
  ASSERT_OK(s);

  // transaction put in two column families
  s = txn1->Put(cfa, "ka1", "va1");
  ASSERT_OK(s);

  // transaction put in two column families
  s = txn2->Put(cfa, "ka2", "va2");
  ASSERT_OK(s);
  s = txn2->Put(cfb, "kb2", "vb2");
  ASSERT_OK(s);

  // write prep section to wal
  s = txn1->Prepare();
  ASSERT_OK(s);

  // our log should be in the heap
  ASSERT_EQ(db_impl->TEST_FindMinLogContainingOutstandingPrep(),
            txn1->GetLogNumber());
  ASSERT_EQ(db_impl->TEST_LogfileNumber(), txn1->GetLogNumber());

  // flush default cf to crate new log
  s = db->Put(wopts, "foo", "bar");
  ASSERT_OK(s);
  s = db_impl->TEST_FlushMemTable(true);
  ASSERT_OK(s);

  // make sure we are on a new log
  ASSERT_GT(db_impl->TEST_LogfileNumber(), txn1->GetLogNumber());

  // put txn2 prep section in this log
  s = txn2->Prepare();
  ASSERT_OK(s);
  ASSERT_EQ(db_impl->TEST_LogfileNumber(), txn2->GetLogNumber());

  // heap should still see first log
  ASSERT_EQ(db_impl->TEST_FindMinLogContainingOutstandingPrep(),
            txn1->GetLogNumber());

  // commit txn1
  s = txn1->Commit();
  ASSERT_OK(s);

  // heap should now show txn2s log
  ASSERT_EQ(db_impl->TEST_FindMinLogContainingOutstandingPrep(),
            txn2->GetLogNumber());

  // we should see txn1s log refernced by the memtables
  ASSERT_EQ(db_impl->TEST_FindMinPrepLogReferencedByMemTable(),
            txn1->GetLogNumber());

  // flush default cf to crate new log
  s = db->Put(wopts, "foo", "bar2");
  ASSERT_OK(s);
  s = db_impl->TEST_FlushMemTable(true);
  ASSERT_OK(s);

  // make sure we are on a new log
  ASSERT_GT(db_impl->TEST_LogfileNumber(), txn2->GetLogNumber());

  // commit txn2
  s = txn2->Commit();
  ASSERT_OK(s);

  // heap should not show any logs
  ASSERT_EQ(db_impl->TEST_FindMinLogContainingOutstandingPrep(), 0);

  // should show the first txn log
  ASSERT_EQ(db_impl->TEST_FindMinPrepLogReferencedByMemTable(),
            txn1->GetLogNumber());

  // flush only cfa memtable
  s = db_impl->TEST_FlushMemTable(true, cfa);
  ASSERT_OK(s);

  // should show the first txn log
  ASSERT_EQ(db_impl->TEST_FindMinPrepLogReferencedByMemTable(),
            txn2->GetLogNumber());

  // flush only cfb memtable
  s = db_impl->TEST_FlushMemTable(true, cfb);
  ASSERT_OK(s);

  // should show not dependency on logs
  ASSERT_EQ(db_impl->TEST_FindMinPrepLogReferencedByMemTable(), 0);
  ASSERT_EQ(db_impl->TEST_FindMinLogContainingOutstandingPrep(), 0);

  delete txn1;
  delete txn2;
  delete cfa;
  delete cfb;
}

TEST_P(TransactionTest, TwoPhaseLogRollingTest2) {
  DBImpl* db_impl = reinterpret_cast<DBImpl*>(db->GetRootDB());

  Status s;
  ColumnFamilyHandle *cfa, *cfb;

  ColumnFamilyOptions cf_options;
  s = db->CreateColumnFamily(cf_options, "CFA", &cfa);
  ASSERT_OK(s);
  s = db->CreateColumnFamily(cf_options, "CFB", &cfb);
  ASSERT_OK(s);

  WriteOptions wopts;
  wopts.disableWAL = false;
  wopts.sync = true;

  auto cfh_a = reinterpret_cast<ColumnFamilyHandleImpl*>(cfa);
  auto cfh_b = reinterpret_cast<ColumnFamilyHandleImpl*>(cfb);

  TransactionOptions topts1;
  Transaction* txn1 = db->BeginTransaction(wopts, topts1);
  s = txn1->SetName("xid1");
  ASSERT_OK(s);
  s = txn1->Put(cfa, "boys", "girls1");
  ASSERT_OK(s);

  Transaction* txn2 = db->BeginTransaction(wopts, topts1);
  s = txn2->SetName("xid2");
  ASSERT_OK(s);
  s = txn2->Put(cfb, "up", "down1");
  ASSERT_OK(s);

  // prepre transaction in LOG A
  s = txn1->Prepare();
  ASSERT_OK(s);

  // prepre transaction in LOG A
  s = txn2->Prepare();
  ASSERT_OK(s);

  // regular put so that mem table can actually be flushed for log rolling
  s = db->Put(wopts, "cats", "dogs1");
  ASSERT_OK(s);

  auto prepare_log_no = txn1->GetLogNumber();

  // roll to LOG B
  s = db_impl->TEST_FlushMemTable(true);
  ASSERT_OK(s);

  // now we pause background work so that
  // imm()s are not flushed before we can check their status
  s = db_impl->PauseBackgroundWork();
  ASSERT_OK(s);

  ASSERT_GT(db_impl->TEST_LogfileNumber(), prepare_log_no);
  ASSERT_GT(cfh_a->cfd()->GetLogNumber(), prepare_log_no);
  ASSERT_EQ(cfh_a->cfd()->GetLogNumber(), db_impl->TEST_LogfileNumber());
  ASSERT_EQ(db_impl->TEST_FindMinLogContainingOutstandingPrep(),
            prepare_log_no);
  ASSERT_EQ(db_impl->TEST_FindMinPrepLogReferencedByMemTable(), 0);

  // commit in LOG B
  s = txn1->Commit();
  ASSERT_OK(s);

  ASSERT_EQ(db_impl->TEST_FindMinPrepLogReferencedByMemTable(), prepare_log_no);

  ASSERT_TRUE(!db_impl->TEST_UnableToFlushOldestLog());

  // request a flush for all column families such that the earliest
  // alive log file can be killed
  db_impl->TEST_MaybeFlushColumnFamilies();
  // log cannot be flushed because txn2 has not been commited
  ASSERT_TRUE(!db_impl->TEST_IsLogGettingFlushed());
  ASSERT_TRUE(db_impl->TEST_UnableToFlushOldestLog());

  // assert that cfa has a flush requested
  ASSERT_TRUE(cfh_a->cfd()->imm()->HasFlushRequested());

  // cfb should not be flushed becuse it has no data from LOG A
  ASSERT_TRUE(!cfh_b->cfd()->imm()->HasFlushRequested());

  // cfb now has data from LOG A
  s = txn2->Commit();
  ASSERT_OK(s);

  db_impl->TEST_MaybeFlushColumnFamilies();
  ASSERT_TRUE(!db_impl->TEST_UnableToFlushOldestLog());

  // we should see that cfb now has a flush requested
  ASSERT_TRUE(cfh_b->cfd()->imm()->HasFlushRequested());

  // all data in LOG A resides in a memtable that has been
  // requested for a flush
  ASSERT_TRUE(db_impl->TEST_IsLogGettingFlushed());

  delete txn1;
  delete txn2;
  delete cfa;
  delete cfb;
}
/*
 * 1) use prepare to keep first log around to determine starting sequence
 * during recovery.
 * 2) insert many values, skipping wal, to increase seqid.
 * 3) insert final value into wal
 * 4) recover and see that final value was properly recovered - not
 * hidden behind improperly summed sequence ids
 */
TEST_P(TransactionTest, TwoPhaseOutOfOrderDelete) {
  DBImpl* db_impl = reinterpret_cast<DBImpl*>(db->GetRootDB());
  WriteOptions wal_on, wal_off;
  wal_on.sync = true;
  wal_on.disableWAL = false;
  wal_off.disableWAL = true;
  ReadOptions read_options;
  TransactionOptions txn_options;

  std::string value;
  Status s;

  Transaction* txn1 = db->BeginTransaction(wal_on, txn_options);

  s = txn1->SetName("1");
  ASSERT_OK(s);

  s = db->Put(wal_on, "first", "first");
  ASSERT_OK(s);

  s = txn1->Put(Slice("dummy"), Slice("dummy"));
  ASSERT_OK(s);
  s = txn1->Prepare();
  ASSERT_OK(s);

  s = db->Put(wal_off, "cats", "dogs1");
  ASSERT_OK(s);
  s = db->Put(wal_off, "cats", "dogs2");
  ASSERT_OK(s);
  s = db->Put(wal_off, "cats", "dogs3");
  ASSERT_OK(s);

  s = db_impl->TEST_FlushMemTable(true);
  ASSERT_OK(s);

  s = db->Put(wal_on, "cats", "dogs4");
  ASSERT_OK(s);

  // kill and reopen
  env->SetFilesystemActive(false);
  ReOpenNoDelete();

  s = db->Get(read_options, "first", &value);
  ASSERT_OK(s);
  ASSERT_EQ(value, "first");

  s = db->Get(read_options, "cats", &value);
  ASSERT_OK(s);
  ASSERT_EQ(value, "dogs4");
}

TEST_P(TransactionTest, FirstWriteTest) {
  WriteOptions write_options;

  // Test conflict checking against the very first write to a db.
  // The transaction's snapshot will have seq 1 and the following write
  // will have sequence 1.
  Status s = db->Put(write_options, "A", "a");

  Transaction* txn = db->BeginTransaction(write_options);
  txn->SetSnapshot();

  ASSERT_OK(s);

  s = txn->Put("A", "b");
  ASSERT_OK(s);

  delete txn;
}

TEST_P(TransactionTest, FirstWriteTest2) {
  WriteOptions write_options;

  Transaction* txn = db->BeginTransaction(write_options);
  txn->SetSnapshot();

  // Test conflict checking against the very first write to a db.
  // The transaction's snapshot is a seq 0 while the following write
  // will have sequence 1.
  Status s = db->Put(write_options, "A", "a");
  ASSERT_OK(s);

  s = txn->Put("A", "b");
  ASSERT_TRUE(s.IsBusy());

  delete txn;
}

TEST_P(TransactionTest, WriteOptionsTest) {
  WriteOptions write_options;
  write_options.sync = true;
  write_options.disableWAL = true;

  Transaction* txn = db->BeginTransaction(write_options);
  ASSERT_TRUE(txn);

  ASSERT_TRUE(txn->GetWriteOptions()->sync);

  write_options.sync = false;
  txn->SetWriteOptions(write_options);
  ASSERT_FALSE(txn->GetWriteOptions()->sync);
  ASSERT_TRUE(txn->GetWriteOptions()->disableWAL);

  delete txn;
}

TEST_P(TransactionTest, WriteConflictTest) {
  WriteOptions write_options;
  ReadOptions read_options;
  string value;
  Status s;

  db->Put(write_options, "foo", "A");
  db->Put(write_options, "foo2", "B");

  Transaction* txn = db->BeginTransaction(write_options);
  ASSERT_TRUE(txn);

  s = txn->Put("foo", "A2");
  ASSERT_OK(s);

  s = txn->Put("foo2", "B2");
  ASSERT_OK(s);

  // This Put outside of a transaction will conflict with the previous write
  s = db->Put(write_options, "foo", "xxx");
  ASSERT_TRUE(s.IsTimedOut());

  s = db->Get(read_options, "foo", &value);
  ASSERT_EQ(value, "A");

  s = txn->Commit();
  ASSERT_OK(s);

  db->Get(read_options, "foo", &value);
  ASSERT_EQ(value, "A2");
  db->Get(read_options, "foo2", &value);
  ASSERT_EQ(value, "B2");

  delete txn;
}

TEST_P(TransactionTest, WriteConflictTest2) {
  WriteOptions write_options;
  ReadOptions read_options;
  TransactionOptions txn_options;
  string value;
  Status s;

  db->Put(write_options, "foo", "bar");

  txn_options.set_snapshot = true;
  Transaction* txn = db->BeginTransaction(write_options, txn_options);
  ASSERT_TRUE(txn);

  // This Put outside of a transaction will conflict with a later write
  s = db->Put(write_options, "foo", "barz");
  ASSERT_OK(s);

  s = txn->Put("foo2", "X");
  ASSERT_OK(s);

  s = txn->Put("foo",
               "bar2");  // Conflicts with write done after snapshot taken
  ASSERT_TRUE(s.IsBusy());

  s = txn->Put("foo3", "Y");
  ASSERT_OK(s);

  s = db->Get(read_options, "foo", &value);
  ASSERT_EQ(value, "barz");

  ASSERT_EQ(2, txn->GetNumKeys());

  s = txn->Commit();
  ASSERT_OK(s);  // Txn should commit, but only write foo2 and foo3

  // Verify that transaction wrote foo2 and foo3 but not foo
  db->Get(read_options, "foo", &value);
  ASSERT_EQ(value, "barz");

  db->Get(read_options, "foo2", &value);
  ASSERT_EQ(value, "X");

  db->Get(read_options, "foo3", &value);
  ASSERT_EQ(value, "Y");

  delete txn;
}

TEST_P(TransactionTest, ReadConflictTest) {
  WriteOptions write_options;
  ReadOptions read_options, snapshot_read_options;
  TransactionOptions txn_options;
  string value;
  Status s;

  db->Put(write_options, "foo", "bar");
  db->Put(write_options, "foo2", "bar");

  txn_options.set_snapshot = true;
  Transaction* txn = db->BeginTransaction(write_options, txn_options);
  ASSERT_TRUE(txn);

  txn->SetSnapshot();
  snapshot_read_options.snapshot = txn->GetSnapshot();

  txn->GetForUpdate(snapshot_read_options, "foo", &value);
  ASSERT_EQ(value, "bar");

  // This Put outside of a transaction will conflict with the previous read
  s = db->Put(write_options, "foo", "barz");
  ASSERT_TRUE(s.IsTimedOut());

  s = db->Get(read_options, "foo", &value);
  ASSERT_EQ(value, "bar");

  s = txn->Get(read_options, "foo", &value);
  ASSERT_EQ(value, "bar");

  s = txn->Commit();
  ASSERT_OK(s);

  delete txn;
}

TEST_P(TransactionTest, TxnOnlyTest) {
  // Test to make sure transactions work when there are no other writes in an
  // empty db.

  WriteOptions write_options;
  ReadOptions read_options;
  string value;
  Status s;

  Transaction* txn = db->BeginTransaction(write_options);
  ASSERT_TRUE(txn);

  s = txn->Put("x", "y");
  ASSERT_OK(s);

  s = txn->Commit();
  ASSERT_OK(s);

  delete txn;
}

TEST_P(TransactionTest, FlushTest) {
  WriteOptions write_options;
  ReadOptions read_options, snapshot_read_options;
  string value;
  Status s;

  db->Put(write_options, Slice("foo"), Slice("bar"));
  db->Put(write_options, Slice("foo2"), Slice("bar"));

  Transaction* txn = db->BeginTransaction(write_options);
  ASSERT_TRUE(txn);

  snapshot_read_options.snapshot = txn->GetSnapshot();

  txn->GetForUpdate(snapshot_read_options, "foo", &value);
  ASSERT_EQ(value, "bar");

  s = txn->Put(Slice("foo"), Slice("bar2"));
  ASSERT_OK(s);

  txn->GetForUpdate(snapshot_read_options, "foo", &value);
  ASSERT_EQ(value, "bar2");

  // Put a random key so we have a memtable to flush
  s = db->Put(write_options, "dummy", "dummy");
  ASSERT_OK(s);

  // force a memtable flush
  FlushOptions flush_ops;
  db->Flush(flush_ops);

  s = txn->Commit();
  // txn should commit since the flushed table is still in MemtableList History
  ASSERT_OK(s);

  db->Get(read_options, "foo", &value);
  ASSERT_EQ(value, "bar2");

  delete txn;
}

TEST_P(TransactionTest, FlushTest2) {
  const size_t num_tests = 3;

  for (size_t n = 0; n < num_tests; n++) {
    // Test different table factories
    switch (n) {
      case 0:
        break;
      case 1:
        options.table_factory.reset(new mock::MockTableFactory());
        break;
      case 2: {
        PlainTableOptions pt_opts;
        pt_opts.hash_table_ratio = 0;
        options.table_factory.reset(NewPlainTableFactory(pt_opts));
        break;
      }
    }

    Status s = ReOpen();
    ASSERT_OK(s);

    WriteOptions write_options;
    ReadOptions read_options, snapshot_read_options;
    TransactionOptions txn_options;
    string value;

    DBImpl* db_impl = reinterpret_cast<DBImpl*>(db->GetRootDB());

    db->Put(write_options, Slice("foo"), Slice("bar"));
    db->Put(write_options, Slice("foo2"), Slice("bar2"));
    db->Put(write_options, Slice("foo3"), Slice("bar3"));

    txn_options.set_snapshot = true;
    Transaction* txn = db->BeginTransaction(write_options, txn_options);
    ASSERT_TRUE(txn);

    snapshot_read_options.snapshot = txn->GetSnapshot();

    txn->GetForUpdate(snapshot_read_options, "foo", &value);
    ASSERT_EQ(value, "bar");

    s = txn->Put(Slice("foo"), Slice("bar2"));
    ASSERT_OK(s);

    txn->GetForUpdate(snapshot_read_options, "foo", &value);
    ASSERT_EQ(value, "bar2");
    // verify foo is locked by txn
    s = db->Delete(write_options, "foo");
    ASSERT_TRUE(s.IsTimedOut());

    s = db->Put(write_options, "Z", "z");
    ASSERT_OK(s);
    s = db->Put(write_options, "dummy", "dummy");
    ASSERT_OK(s);

    s = db->Put(write_options, "S", "s");
    ASSERT_OK(s);
    s = db->SingleDelete(write_options, "S");
    ASSERT_OK(s);

    s = txn->Delete("S");
    // Should fail after encountering a write to S in memtable
    ASSERT_TRUE(s.IsBusy());

    // force a memtable flush
    s = db_impl->TEST_FlushMemTable(true);
    ASSERT_OK(s);

    // Put a random key so we have a MemTable to flush
    s = db->Put(write_options, "dummy", "dummy2");
    ASSERT_OK(s);

    // force a memtable flush
    ASSERT_OK(db_impl->TEST_FlushMemTable(true));

    s = db->Put(write_options, "dummy", "dummy3");
    ASSERT_OK(s);

    // force a memtable flush
    // Since our test db has max_write_buffer_number=2, this flush will cause
    // the first memtable to get purged from the MemtableList history.
    ASSERT_OK(db_impl->TEST_FlushMemTable(true));

    s = txn->Put("X", "Y");
    // Should succeed after verifying there is no write to X in SST file
    ASSERT_OK(s);

    s = txn->Put("Z", "zz");
    // Should fail after encountering a write to Z in SST file
    ASSERT_TRUE(s.IsBusy());

    s = txn->GetForUpdate(read_options, "foo2", &value);
    // should succeed since key was written before txn started
    ASSERT_OK(s);
    // verify foo2 is locked by txn
    s = db->Delete(write_options, "foo2");
    ASSERT_TRUE(s.IsTimedOut());

    s = txn->Delete("S");
    // Should fail after encountering a write to S in SST file
    ASSERT_TRUE(s.IsBusy());

    // Write a bunch of keys to db to force a compaction
    Random rnd(47);
    for (int i = 0; i < 1000; i++) {
      s = db->Put(write_options, std::to_string(i),
                  test::CompressibleString(&rnd, 0.8, 100, &value));
      ASSERT_OK(s);
    }

    s = txn->Put("X", "yy");
    // Should succeed after verifying there is no write to X in SST file
    ASSERT_OK(s);

    s = txn->Put("Z", "zzz");
    // Should fail after encountering a write to Z in SST file
    ASSERT_TRUE(s.IsBusy());

    s = txn->Delete("S");
    // Should fail after encountering a write to S in SST file
    ASSERT_TRUE(s.IsBusy());

    s = txn->GetForUpdate(read_options, "foo3", &value);
    // should succeed since key was written before txn started
    ASSERT_OK(s);
    // verify foo3 is locked by txn
    s = db->Delete(write_options, "foo3");
    ASSERT_TRUE(s.IsTimedOut());

    db_impl->TEST_WaitForCompact();

    s = txn->Commit();
    ASSERT_OK(s);

    // Transaction should only write the keys that succeeded.
    s = db->Get(read_options, "foo", &value);
    ASSERT_EQ(value, "bar2");

    s = db->Get(read_options, "X", &value);
    ASSERT_OK(s);
    ASSERT_EQ("yy", value);

    s = db->Get(read_options, "Z", &value);
    ASSERT_OK(s);
    ASSERT_EQ("z", value);

  delete txn;
  }
}

TEST_P(TransactionTest, NoSnapshotTest) {
  WriteOptions write_options;
  ReadOptions read_options;
  string value;
  Status s;

  db->Put(write_options, "AAA", "bar");

  Transaction* txn = db->BeginTransaction(write_options);
  ASSERT_TRUE(txn);

  // Modify key after transaction start
  db->Put(write_options, "AAA", "bar1");

  // Read and write without a snap
  txn->GetForUpdate(read_options, "AAA", &value);
  ASSERT_EQ(value, "bar1");
  s = txn->Put("AAA", "bar2");
  ASSERT_OK(s);

  // Should commit since read/write was done after data changed
  s = txn->Commit();
  ASSERT_OK(s);

  txn->GetForUpdate(read_options, "AAA", &value);
  ASSERT_EQ(value, "bar2");

  delete txn;
}

TEST_P(TransactionTest, MultipleSnapshotTest) {
  WriteOptions write_options;
  ReadOptions read_options, snapshot_read_options;
  string value;
  Status s;

  db->Put(write_options, "AAA", "bar");
  db->Put(write_options, "BBB", "bar");
  db->Put(write_options, "CCC", "bar");

  Transaction* txn = db->BeginTransaction(write_options);
  ASSERT_TRUE(txn);

  db->Put(write_options, "AAA", "bar1");

  // Read and write without a snapshot
  txn->GetForUpdate(read_options, "AAA", &value);
  ASSERT_EQ(value, "bar1");
  s = txn->Put("AAA", "bar2");
  ASSERT_OK(s);

  // Modify BBB before snapshot is taken
  db->Put(write_options, "BBB", "bar1");

  txn->SetSnapshot();
  snapshot_read_options.snapshot = txn->GetSnapshot();

  // Read and write with snapshot
  txn->GetForUpdate(snapshot_read_options, "BBB", &value);
  ASSERT_EQ(value, "bar1");
  s = txn->Put("BBB", "bar2");
  ASSERT_OK(s);

  db->Put(write_options, "CCC", "bar1");

  // Set a new snapshot
  txn->SetSnapshot();
  snapshot_read_options.snapshot = txn->GetSnapshot();

  // Read and write with snapshot
  txn->GetForUpdate(snapshot_read_options, "CCC", &value);
  ASSERT_EQ(value, "bar1");
  s = txn->Put("CCC", "bar2");
  ASSERT_OK(s);

  s = txn->GetForUpdate(read_options, "AAA", &value);
  ASSERT_OK(s);
  ASSERT_EQ(value, "bar2");
  s = txn->GetForUpdate(read_options, "BBB", &value);
  ASSERT_OK(s);
  ASSERT_EQ(value, "bar2");
  s = txn->GetForUpdate(read_options, "CCC", &value);
  ASSERT_OK(s);
  ASSERT_EQ(value, "bar2");

  s = db->Get(read_options, "AAA", &value);
  ASSERT_OK(s);
  ASSERT_EQ(value, "bar1");
  s = db->Get(read_options, "BBB", &value);
  ASSERT_OK(s);
  ASSERT_EQ(value, "bar1");
  s = db->Get(read_options, "CCC", &value);
  ASSERT_OK(s);
  ASSERT_EQ(value, "bar1");

  s = txn->Commit();
  ASSERT_OK(s);

  s = db->Get(read_options, "AAA", &value);
  ASSERT_OK(s);
  ASSERT_EQ(value, "bar2");
  s = db->Get(read_options, "BBB", &value);
  ASSERT_OK(s);
  ASSERT_EQ(value, "bar2");
  s = db->Get(read_options, "CCC", &value);
  ASSERT_OK(s);
  ASSERT_EQ(value, "bar2");

  // verify that we track multiple writes to the same key at different snapshots
  delete txn;
  txn = db->BeginTransaction(write_options);

  // Potentially conflicting writes
  db->Put(write_options, "ZZZ", "zzz");
  db->Put(write_options, "XXX", "xxx");

  txn->SetSnapshot();

  TransactionOptions txn_options;
  txn_options.set_snapshot = true;
  Transaction* txn2 = db->BeginTransaction(write_options, txn_options);
  txn2->SetSnapshot();

  // This should not conflict in txn since the snapshot is later than the
  // previous write (spoiler alert:  it will later conflict with txn2).
  s = txn->Put("ZZZ", "zzzz");
  ASSERT_OK(s);

  s = txn->Commit();
  ASSERT_OK(s);

  delete txn;

  // This will conflict since the snapshot is earlier than another write to ZZZ
  s = txn2->Put("ZZZ", "xxxxx");
  ASSERT_TRUE(s.IsBusy());

  s = txn2->Commit();
  ASSERT_OK(s);

  s = db->Get(read_options, "ZZZ", &value);
  ASSERT_OK(s);
  ASSERT_EQ(value, "zzzz");

  delete txn2;
}

TEST_P(TransactionTest, ColumnFamiliesTest) {
  WriteOptions write_options;
  ReadOptions read_options, snapshot_read_options;
  TransactionOptions txn_options;
  string value;
  Status s;

  ColumnFamilyHandle *cfa, *cfb;
  ColumnFamilyOptions cf_options;

  // Create 2 new column families
  s = db->CreateColumnFamily(cf_options, "CFA", &cfa);
  ASSERT_OK(s);
  s = db->CreateColumnFamily(cf_options, "CFB", &cfb);
  ASSERT_OK(s);

  delete cfa;
  delete cfb;
  delete db;

  // open DB with three column families
  std::vector<ColumnFamilyDescriptor> column_families;
  // have to open default column family
  column_families.push_back(
      ColumnFamilyDescriptor(kDefaultColumnFamilyName, ColumnFamilyOptions()));
  // open the new column families
  column_families.push_back(
      ColumnFamilyDescriptor("CFA", ColumnFamilyOptions()));
  column_families.push_back(
      ColumnFamilyDescriptor("CFB", ColumnFamilyOptions()));

  std::vector<ColumnFamilyHandle*> handles;

  s = TransactionDB::Open(options, txn_db_options, dbname, column_families,
                          &handles, &db);
  ASSERT_OK(s);

  Transaction* txn = db->BeginTransaction(write_options);
  ASSERT_TRUE(txn);

  txn->SetSnapshot();
  snapshot_read_options.snapshot = txn->GetSnapshot();

  txn_options.set_snapshot = true;
  Transaction* txn2 = db->BeginTransaction(write_options, txn_options);
  ASSERT_TRUE(txn2);

  // Write some data to the db
  WriteBatch batch;
  batch.Put("foo", "foo");
  batch.Put(handles[1], "AAA", "bar");
  batch.Put(handles[1], "AAAZZZ", "bar");
  s = db->Write(write_options, &batch);
  ASSERT_OK(s);
  db->Delete(write_options, handles[1], "AAAZZZ");

  // These keys do not conflict with existing writes since they're in
  // different column families
  s = txn->Delete("AAA");
  ASSERT_OK(s);
  s = txn->GetForUpdate(snapshot_read_options, handles[1], "foo", &value);
  ASSERT_TRUE(s.IsNotFound());
  Slice key_slice("AAAZZZ");
  Slice value_slices[2] = {Slice("bar"), Slice("bar")};
  s = txn->Put(handles[2], SliceParts(&key_slice, 1),
               SliceParts(value_slices, 2));
  ASSERT_OK(s);
  ASSERT_EQ(3, txn->GetNumKeys());

  s = txn->Commit();
  ASSERT_OK(s);
  s = db->Get(read_options, "AAA", &value);
  ASSERT_TRUE(s.IsNotFound());
  s = db->Get(read_options, handles[2], "AAAZZZ", &value);
  ASSERT_EQ(value, "barbar");

  Slice key_slices[3] = {Slice("AAA"), Slice("ZZ"), Slice("Z")};
  Slice value_slice("barbarbar");

  s = txn2->Delete(handles[2], "XXX");
  ASSERT_OK(s);
  s = txn2->Delete(handles[1], "XXX");
  ASSERT_OK(s);

  // This write will cause a conflict with the earlier batch write
  s = txn2->Put(handles[1], SliceParts(key_slices, 3),
                SliceParts(&value_slice, 1));
  ASSERT_TRUE(s.IsBusy());

  s = txn2->Commit();
  ASSERT_OK(s);
  // In the above the latest change to AAAZZZ in handles[1] is delete.
  s = db->Get(read_options, handles[1], "AAAZZZ", &value);
  ASSERT_TRUE(s.IsNotFound());

  delete txn;
  delete txn2;

  txn = db->BeginTransaction(write_options, txn_options);
  snapshot_read_options.snapshot = txn->GetSnapshot();

  txn2 = db->BeginTransaction(write_options, txn_options);
  ASSERT_TRUE(txn);

  std::vector<ColumnFamilyHandle*> multiget_cfh = {handles[1], handles[2],
                                                   handles[0], handles[2]};
  std::vector<Slice> multiget_keys = {"AAA", "AAAZZZ", "foo", "foo"};
  std::vector<std::string> values(4);

  std::vector<Status> results = txn->MultiGetForUpdate(
      snapshot_read_options, multiget_cfh, multiget_keys, &values);
  ASSERT_OK(results[0]);
  ASSERT_OK(results[1]);
  ASSERT_OK(results[2]);
  ASSERT_TRUE(results[3].IsNotFound());
  ASSERT_EQ(values[0], "bar");
  ASSERT_EQ(values[1], "barbar");
  ASSERT_EQ(values[2], "foo");

  s = txn->SingleDelete(handles[2], "ZZZ");
  ASSERT_OK(s);
  s = txn->Put(handles[2], "ZZZ", "YYY");
  ASSERT_OK(s);
  s = txn->Put(handles[2], "ZZZ", "YYYY");
  ASSERT_OK(s);
  s = txn->Delete(handles[2], "ZZZ");
  ASSERT_OK(s);
  s = txn->Put(handles[2], "AAAZZZ", "barbarbar");
  ASSERT_OK(s);

  ASSERT_EQ(5, txn->GetNumKeys());

  // Txn should commit
  s = txn->Commit();
  ASSERT_OK(s);
  s = db->Get(read_options, handles[2], "ZZZ", &value);
  ASSERT_TRUE(s.IsNotFound());

  // Put a key which will conflict with the next txn using the previous snapshot
  db->Put(write_options, handles[2], "foo", "000");

  results = txn2->MultiGetForUpdate(snapshot_read_options, multiget_cfh,
                                    multiget_keys, &values);
  // All results should fail since there was a conflict
  ASSERT_TRUE(results[0].IsBusy());
  ASSERT_TRUE(results[1].IsBusy());
  ASSERT_TRUE(results[2].IsBusy());
  ASSERT_TRUE(results[3].IsBusy());

  s = db->Get(read_options, handles[2], "foo", &value);
  ASSERT_EQ(value, "000");

  s = txn2->Commit();
  ASSERT_OK(s);

  s = db->DropColumnFamily(handles[1]);
  ASSERT_OK(s);
  s = db->DropColumnFamily(handles[2]);
  ASSERT_OK(s);

  delete txn;
  delete txn2;

  for (auto handle : handles) {
    delete handle;
  }
}

TEST_P(TransactionTest, ColumnFamiliesTest2) {
  WriteOptions write_options;
  ReadOptions read_options, snapshot_read_options;
  TransactionOptions txn_options;
  string value;
  Status s;

  ColumnFamilyHandle *one, *two;
  ColumnFamilyOptions cf_options;

  // Create 2 new column families
  s = db->CreateColumnFamily(cf_options, "ONE", &one);
  ASSERT_OK(s);
  s = db->CreateColumnFamily(cf_options, "TWO", &two);
  ASSERT_OK(s);

  Transaction* txn1 = db->BeginTransaction(write_options);
  ASSERT_TRUE(txn1);
  Transaction* txn2 = db->BeginTransaction(write_options);
  ASSERT_TRUE(txn2);

  s = txn1->Put(one, "X", "1");
  ASSERT_OK(s);
  s = txn1->Put(two, "X", "2");
  ASSERT_OK(s);
  s = txn1->Put("X", "0");
  ASSERT_OK(s);

  s = txn2->Put(one, "X", "11");
  ASSERT_TRUE(s.IsTimedOut());

  s = txn1->Commit();
  ASSERT_OK(s);

  // Drop first column family
  s = db->DropColumnFamily(one);
  ASSERT_OK(s);

  // Should fail since column family was dropped.
  s = txn2->Commit();
  ASSERT_OK(s);

  delete txn1;
  txn1 = db->BeginTransaction(write_options);
  ASSERT_TRUE(txn1);

  // Should fail since column family was dropped
  s = txn1->Put(one, "X", "111");
  ASSERT_TRUE(s.IsInvalidArgument());

  s = txn1->Put(two, "X", "222");
  ASSERT_OK(s);

  s = txn1->Put("X", "000");
  ASSERT_OK(s);

  s = txn1->Commit();
  ASSERT_OK(s);

  s = db->Get(read_options, two, "X", &value);
  ASSERT_OK(s);
  ASSERT_EQ("222", value);

  s = db->Get(read_options, "X", &value);
  ASSERT_OK(s);
  ASSERT_EQ("000", value);

  s = db->DropColumnFamily(two);
  ASSERT_OK(s);

  delete txn1;
  delete txn2;

  delete one;
  delete two;
}

TEST_P(TransactionTest, EmptyTest) {
  WriteOptions write_options;
  ReadOptions read_options;
  string value;
  Status s;

  s = db->Put(write_options, "aaa", "aaa");
  ASSERT_OK(s);

  Transaction* txn = db->BeginTransaction(write_options);
  s = txn->Commit();
  ASSERT_OK(s);
  delete txn;

  txn = db->BeginTransaction(write_options);
  txn->Rollback();
  delete txn;

  txn = db->BeginTransaction(write_options);
  s = txn->GetForUpdate(read_options, "aaa", &value);
  ASSERT_EQ(value, "aaa");

  s = txn->Commit();
  ASSERT_OK(s);
  delete txn;

  txn = db->BeginTransaction(write_options);
  txn->SetSnapshot();

  s = txn->GetForUpdate(read_options, "aaa", &value);
  ASSERT_EQ(value, "aaa");

  // Conflicts with previous GetForUpdate
  s = db->Put(write_options, "aaa", "xxx");
  ASSERT_TRUE(s.IsTimedOut());

  // transaction expired!
  s = txn->Commit();
  ASSERT_OK(s);
  delete txn;
}

TEST_P(TransactionTest, PredicateManyPreceders) {
  WriteOptions write_options;
  ReadOptions read_options1, read_options2;
  TransactionOptions txn_options;
  string value;
  Status s;

  txn_options.set_snapshot = true;
  Transaction* txn1 = db->BeginTransaction(write_options, txn_options);
  read_options1.snapshot = txn1->GetSnapshot();

  Transaction* txn2 = db->BeginTransaction(write_options);
  txn2->SetSnapshot();
  read_options2.snapshot = txn2->GetSnapshot();

  std::vector<Slice> multiget_keys = {"1", "2", "3"};
  std::vector<std::string> multiget_values;

  std::vector<Status> results =
      txn1->MultiGetForUpdate(read_options1, multiget_keys, &multiget_values);
  ASSERT_TRUE(results[1].IsNotFound());

  s = txn2->Put("2", "x");  // Conflict's with txn1's MultiGetForUpdate
  ASSERT_TRUE(s.IsTimedOut());

  txn2->Rollback();

  multiget_values.clear();
  results =
      txn1->MultiGetForUpdate(read_options1, multiget_keys, &multiget_values);
  ASSERT_TRUE(results[1].IsNotFound());

  s = txn1->Commit();
  ASSERT_OK(s);

  delete txn1;
  delete txn2;

  txn1 = db->BeginTransaction(write_options, txn_options);
  read_options1.snapshot = txn1->GetSnapshot();

  txn2 = db->BeginTransaction(write_options, txn_options);
  read_options2.snapshot = txn2->GetSnapshot();

  s = txn1->Put("4", "x");
  ASSERT_OK(s);

  s = txn2->Delete("4");  // conflict
  ASSERT_TRUE(s.IsTimedOut());

  s = txn1->Commit();
  ASSERT_OK(s);

  s = txn2->GetForUpdate(read_options2, "4", &value);
  ASSERT_TRUE(s.IsBusy());

  txn2->Rollback();

  delete txn1;
  delete txn2;
}

TEST_P(TransactionTest, LostUpdate) {
  WriteOptions write_options;
  ReadOptions read_options, read_options1, read_options2;
  TransactionOptions txn_options;
  string value;
  Status s;

  // Test 2 transactions writing to the same key in multiple orders and
  // with/without snapshots

  Transaction* txn1 = db->BeginTransaction(write_options);
  Transaction* txn2 = db->BeginTransaction(write_options);

  s = txn1->Put("1", "1");
  ASSERT_OK(s);

  s = txn2->Put("1", "2");  // conflict
  ASSERT_TRUE(s.IsTimedOut());

  s = txn2->Commit();
  ASSERT_OK(s);

  s = txn1->Commit();
  ASSERT_OK(s);

  s = db->Get(read_options, "1", &value);
  ASSERT_OK(s);
  ASSERT_EQ("1", value);

  delete txn1;
  delete txn2;

  txn_options.set_snapshot = true;
  txn1 = db->BeginTransaction(write_options, txn_options);
  read_options1.snapshot = txn1->GetSnapshot();

  txn2 = db->BeginTransaction(write_options, txn_options);
  read_options2.snapshot = txn2->GetSnapshot();

  s = txn1->Put("1", "3");
  ASSERT_OK(s);
  s = txn2->Put("1", "4");  // conflict
  ASSERT_TRUE(s.IsTimedOut());

  s = txn1->Commit();
  ASSERT_OK(s);

  s = txn2->Commit();
  ASSERT_OK(s);

  s = db->Get(read_options, "1", &value);
  ASSERT_OK(s);
  ASSERT_EQ("3", value);

  delete txn1;
  delete txn2;

  txn1 = db->BeginTransaction(write_options, txn_options);
  read_options1.snapshot = txn1->GetSnapshot();

  txn2 = db->BeginTransaction(write_options, txn_options);
  read_options2.snapshot = txn2->GetSnapshot();

  s = txn1->Put("1", "5");
  ASSERT_OK(s);

  s = txn1->Commit();
  ASSERT_OK(s);

  s = txn2->Put("1", "6");
  ASSERT_TRUE(s.IsBusy());
  s = txn2->Commit();
  ASSERT_OK(s);

  s = db->Get(read_options, "1", &value);
  ASSERT_OK(s);
  ASSERT_EQ("5", value);

  delete txn1;
  delete txn2;

  txn1 = db->BeginTransaction(write_options, txn_options);
  read_options1.snapshot = txn1->GetSnapshot();

  txn2 = db->BeginTransaction(write_options, txn_options);
  read_options2.snapshot = txn2->GetSnapshot();

  s = txn1->Put("1", "7");
  ASSERT_OK(s);
  s = txn1->Commit();
  ASSERT_OK(s);

  txn2->SetSnapshot();
  s = txn2->Put("1", "8");
  ASSERT_OK(s);
  s = txn2->Commit();
  ASSERT_OK(s);

  s = db->Get(read_options, "1", &value);
  ASSERT_OK(s);
  ASSERT_EQ("8", value);

  delete txn1;
  delete txn2;

  txn1 = db->BeginTransaction(write_options);
  txn2 = db->BeginTransaction(write_options);

  s = txn1->Put("1", "9");
  ASSERT_OK(s);
  s = txn1->Commit();
  ASSERT_OK(s);

  s = txn2->Put("1", "10");
  ASSERT_OK(s);
  s = txn2->Commit();
  ASSERT_OK(s);

  delete txn1;
  delete txn2;

  s = db->Get(read_options, "1", &value);
  ASSERT_OK(s);
  ASSERT_EQ(value, "10");
}

TEST_P(TransactionTest, UntrackedWrites) {
  WriteOptions write_options;
  ReadOptions read_options;
  string value;
  Status s;

  // Verify transaction rollback works for untracked keys.
  Transaction* txn = db->BeginTransaction(write_options);
  txn->SetSnapshot();

  s = txn->PutUntracked("untracked", "0");
  ASSERT_OK(s);
  txn->Rollback();
  s = db->Get(read_options, "untracked", &value);
  ASSERT_TRUE(s.IsNotFound());

  delete txn;
  txn = db->BeginTransaction(write_options);
  txn->SetSnapshot();

  s = db->Put(write_options, "untracked", "x");
  ASSERT_OK(s);

  // Untracked writes should succeed even though key was written after snapshot
  s = txn->PutUntracked("untracked", "1");
  ASSERT_OK(s);
  s = txn->MergeUntracked("untracked", "2");
  ASSERT_OK(s);
  s = txn->DeleteUntracked("untracked");
  ASSERT_OK(s);

  // Conflict
  s = txn->Put("untracked", "3");
  ASSERT_TRUE(s.IsBusy());

  s = txn->Commit();
  ASSERT_OK(s);

  s = db->Get(read_options, "untracked", &value);
  ASSERT_TRUE(s.IsNotFound());

  delete txn;
}

TEST_P(TransactionTest, ExpiredTransaction) {
  WriteOptions write_options;
  ReadOptions read_options;
  TransactionOptions txn_options;
  string value;
  Status s;

  // Set txn expiration timeout to 0 microseconds (expires instantly)
  txn_options.expiration = 0;
  Transaction* txn1 = db->BeginTransaction(write_options, txn_options);

  s = txn1->Put("X", "1");
  ASSERT_OK(s);

  s = txn1->Put("Y", "1");
  ASSERT_OK(s);

  Transaction* txn2 = db->BeginTransaction(write_options);

  // txn2 should be able to write to X since txn1 has expired
  s = txn2->Put("X", "2");
  ASSERT_OK(s);

  s = txn2->Commit();
  ASSERT_OK(s);
  s = db->Get(read_options, "X", &value);
  ASSERT_OK(s);
  ASSERT_EQ("2", value);

  s = txn1->Put("Z", "1");
  ASSERT_OK(s);

  // txn1 should fail to commit since it is expired
  s = txn1->Commit();
  ASSERT_TRUE(s.IsExpired());

  s = db->Get(read_options, "Y", &value);
  ASSERT_TRUE(s.IsNotFound());

  s = db->Get(read_options, "Z", &value);
  ASSERT_TRUE(s.IsNotFound());

  delete txn1;
  delete txn2;
}

TEST_P(TransactionTest, ReinitializeTest) {
  WriteOptions write_options;
  ReadOptions read_options;
  TransactionOptions txn_options;
  string value;
  Status s;

  // Set txn expiration timeout to 0 microseconds (expires instantly)
  txn_options.expiration = 0;
  Transaction* txn1 = db->BeginTransaction(write_options, txn_options);

  // Reinitialize transaction to no long expire
  txn_options.expiration = -1;
  txn1 = db->BeginTransaction(write_options, txn_options, txn1);

  s = txn1->Put("Z", "z");
  ASSERT_OK(s);

  // Should commit since not expired
  s = txn1->Commit();
  ASSERT_OK(s);

  txn1 = db->BeginTransaction(write_options, txn_options, txn1);

  s = txn1->Put("Z", "zz");
  ASSERT_OK(s);

  // Reinitilize txn1 and verify that Z gets unlocked
  txn1 = db->BeginTransaction(write_options, txn_options, txn1);

  Transaction* txn2 = db->BeginTransaction(write_options, txn_options, nullptr);
  s = txn2->Put("Z", "zzz");
  ASSERT_OK(s);
  s = txn2->Commit();
  ASSERT_OK(s);
  delete txn2;

  s = db->Get(read_options, "Z", &value);
  ASSERT_OK(s);
  ASSERT_EQ(value, "zzz");

  // Verify snapshots get reinitialized correctly
  txn1->SetSnapshot();
  s = txn1->Put("Z", "zzzz");
  ASSERT_OK(s);

  s = txn1->Commit();
  ASSERT_OK(s);

  s = db->Get(read_options, "Z", &value);
  ASSERT_OK(s);
  ASSERT_EQ(value, "zzzz");

  txn1 = db->BeginTransaction(write_options, txn_options, txn1);
  const Snapshot* snapshot = txn1->GetSnapshot();
  ASSERT_FALSE(snapshot);

  txn_options.set_snapshot = true;
  txn1 = db->BeginTransaction(write_options, txn_options, txn1);
  snapshot = txn1->GetSnapshot();
  ASSERT_TRUE(snapshot);

  s = txn1->Put("Z", "a");
  ASSERT_OK(s);

  txn1->Rollback();

  s = txn1->Put("Y", "y");
  ASSERT_OK(s);

  txn_options.set_snapshot = false;
  txn1 = db->BeginTransaction(write_options, txn_options, txn1);
  snapshot = txn1->GetSnapshot();
  ASSERT_FALSE(snapshot);

  s = txn1->Put("X", "x");
  ASSERT_OK(s);

  s = txn1->Commit();
  ASSERT_OK(s);

  s = db->Get(read_options, "Z", &value);
  ASSERT_OK(s);
  ASSERT_EQ(value, "zzzz");

  s = db->Get(read_options, "Y", &value);
  ASSERT_TRUE(s.IsNotFound());

  txn1 = db->BeginTransaction(write_options, txn_options, txn1);

  s = txn1->SetName("name");
  ASSERT_OK(s);

  s = txn1->Prepare();
  ASSERT_OK(s);
  s = txn1->Commit();
  ASSERT_OK(s);

  txn1 = db->BeginTransaction(write_options, txn_options, txn1);

  s = txn1->SetName("name");
  ASSERT_OK(s);

  delete txn1;
}

TEST_P(TransactionTest, Rollback) {
  WriteOptions write_options;
  ReadOptions read_options;
  TransactionOptions txn_options;
  string value;
  Status s;

  Transaction* txn1 = db->BeginTransaction(write_options, txn_options);

  ASSERT_OK(s);

  s = txn1->Put("X", "1");
  ASSERT_OK(s);

  Transaction* txn2 = db->BeginTransaction(write_options);

  // txn2 should not be able to write to X since txn1 has it locked
  s = txn2->Put("X", "2");
  ASSERT_TRUE(s.IsTimedOut());

  txn1->Rollback();
  delete txn1;

  // txn2 should now be able to write to X
  s = txn2->Put("X", "3");
  ASSERT_OK(s);

  s = txn2->Commit();
  ASSERT_OK(s);

  s = db->Get(read_options, "X", &value);
  ASSERT_OK(s);
  ASSERT_EQ("3", value);

  delete txn2;
}

TEST_P(TransactionTest, LockLimitTest) {
  WriteOptions write_options;
  ReadOptions read_options, snapshot_read_options;
  TransactionOptions txn_options;
  string value;
  Status s;

  delete db;

  // Open DB with a lock limit of 3
  txn_db_options.max_num_locks = 3;
  s = TransactionDB::Open(options, txn_db_options, dbname, &db);
  ASSERT_OK(s);

  // Create a txn and verify we can only lock up to 3 keys
  Transaction* txn = db->BeginTransaction(write_options);
  ASSERT_TRUE(txn);

  s = txn->Put("X", "x");
  ASSERT_OK(s);

  s = txn->Put("Y", "y");
  ASSERT_OK(s);

  s = txn->Put("Z", "z");
  ASSERT_OK(s);

  // lock limit reached
  s = txn->Put("W", "w");
  ASSERT_TRUE(s.IsBusy());

  // re-locking same key shouldn't put us over the limit
  s = txn->Put("X", "xx");
  ASSERT_OK(s);

  s = txn->GetForUpdate(read_options, "W", &value);
  ASSERT_TRUE(s.IsBusy());
  s = txn->GetForUpdate(read_options, "V", &value);
  ASSERT_TRUE(s.IsBusy());

  // re-locking same key shouldn't put us over the limit
  s = txn->GetForUpdate(read_options, "Y", &value);
  ASSERT_OK(s);
  ASSERT_EQ("y", value);

  s = txn->Get(read_options, "W", &value);
  ASSERT_TRUE(s.IsNotFound());

  Transaction* txn2 = db->BeginTransaction(write_options);
  ASSERT_TRUE(txn2);

  // "X" currently locked
  s = txn2->Put("X", "x");
  ASSERT_TRUE(s.IsTimedOut());

  // lock limit reached
  s = txn2->Put("M", "m");
  ASSERT_TRUE(s.IsBusy());

  s = txn->Commit();
  ASSERT_OK(s);

  s = db->Get(read_options, "X", &value);
  ASSERT_OK(s);
  ASSERT_EQ("xx", value);

  s = db->Get(read_options, "W", &value);
  ASSERT_TRUE(s.IsNotFound());

  // Committing txn should release its locks and allow txn2 to proceed
  s = txn2->Put("X", "x2");
  ASSERT_OK(s);

  s = txn2->Delete("X");
  ASSERT_OK(s);

  s = txn2->Put("M", "m");
  ASSERT_OK(s);

  s = txn2->Put("Z", "z2");
  ASSERT_OK(s);

  // lock limit reached
  s = txn2->Delete("Y");
  ASSERT_TRUE(s.IsBusy());

  s = txn2->Commit();
  ASSERT_OK(s);

  s = db->Get(read_options, "Z", &value);
  ASSERT_OK(s);
  ASSERT_EQ("z2", value);

  s = db->Get(read_options, "Y", &value);
  ASSERT_OK(s);
  ASSERT_EQ("y", value);

  s = db->Get(read_options, "X", &value);
  ASSERT_TRUE(s.IsNotFound());

  delete txn;
  delete txn2;
}

TEST_P(TransactionTest, IteratorTest) {
  WriteOptions write_options;
  ReadOptions read_options, snapshot_read_options;
  TransactionOptions txn_options;
  string value;
  Status s;

  // Write some keys to the db
  s = db->Put(write_options, "A", "a");
  ASSERT_OK(s);

  s = db->Put(write_options, "G", "g");
  ASSERT_OK(s);

  s = db->Put(write_options, "F", "f");
  ASSERT_OK(s);

  s = db->Put(write_options, "C", "c");
  ASSERT_OK(s);

  s = db->Put(write_options, "D", "d");
  ASSERT_OK(s);

  Transaction* txn = db->BeginTransaction(write_options);
  ASSERT_TRUE(txn);

  // Write some keys in a txn
  s = txn->Put("B", "b");
  ASSERT_OK(s);

  s = txn->Put("H", "h");
  ASSERT_OK(s);

  s = txn->Delete("D");
  ASSERT_OK(s);

  s = txn->Put("E", "e");
  ASSERT_OK(s);

  txn->SetSnapshot();
  const Snapshot* snapshot = txn->GetSnapshot();

  // Write some keys to the db after the snapshot
  s = db->Put(write_options, "BB", "xx");
  ASSERT_OK(s);

  s = db->Put(write_options, "C", "xx");
  ASSERT_OK(s);

  read_options.snapshot = snapshot;
  Iterator* iter = txn->GetIterator(read_options);
  ASSERT_OK(iter->status());
  iter->SeekToFirst();

  // Read all keys via iter and lock them all
  std::string results[] = {"a", "b", "c", "e", "f", "g", "h"};
  for (int i = 0; i < 7; i++) {
    ASSERT_OK(iter->status());
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(results[i], iter->value().ToString());

    s = txn->GetForUpdate(read_options, iter->key(), nullptr);
    if (i == 2) {
      // "C" was modified after txn's snapshot
      ASSERT_TRUE(s.IsBusy());
    } else {
      ASSERT_OK(s);
    }

    iter->Next();
  }
  ASSERT_FALSE(iter->Valid());

  iter->Seek("G");
  ASSERT_OK(iter->status());
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ("g", iter->value().ToString());

  iter->Prev();
  ASSERT_OK(iter->status());
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ("f", iter->value().ToString());

  iter->Seek("D");
  ASSERT_OK(iter->status());
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ("e", iter->value().ToString());

  iter->Seek("C");
  ASSERT_OK(iter->status());
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ("c", iter->value().ToString());

  iter->Next();
  ASSERT_OK(iter->status());
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ("e", iter->value().ToString());

  iter->Seek("");
  ASSERT_OK(iter->status());
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ("a", iter->value().ToString());

  iter->Seek("X");
  ASSERT_OK(iter->status());
  ASSERT_FALSE(iter->Valid());

  iter->SeekToLast();
  ASSERT_OK(iter->status());
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ("h", iter->value().ToString());

  s = txn->Commit();
  ASSERT_OK(s);

  delete iter;
  delete txn;
}

TEST_P(TransactionTest, DisableIndexingTest) {
  WriteOptions write_options;
  ReadOptions read_options;
  string value;
  Status s;

  Transaction* txn = db->BeginTransaction(write_options);
  ASSERT_TRUE(txn);

  s = txn->Put("A", "a");
  ASSERT_OK(s);

  s = txn->Get(read_options, "A", &value);
  ASSERT_OK(s);
  ASSERT_EQ("a", value);

  txn->DisableIndexing();

  s = txn->Put("B", "b");
  ASSERT_OK(s);

  s = txn->Get(read_options, "B", &value);
  ASSERT_TRUE(s.IsNotFound());

  Iterator* iter = txn->GetIterator(read_options);
  ASSERT_OK(iter->status());

  iter->Seek("B");
  ASSERT_OK(iter->status());
  ASSERT_FALSE(iter->Valid());

  s = txn->Delete("A");

  s = txn->Get(read_options, "A", &value);
  ASSERT_OK(s);
  ASSERT_EQ("a", value);

  txn->EnableIndexing();

  s = txn->Put("B", "bb");
  ASSERT_OK(s);

  iter->Seek("B");
  ASSERT_OK(iter->status());
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ("bb", iter->value().ToString());

  s = txn->Get(read_options, "B", &value);
  ASSERT_OK(s);
  ASSERT_EQ("bb", value);

  s = txn->Put("A", "aa");
  ASSERT_OK(s);

  s = txn->Get(read_options, "A", &value);
  ASSERT_OK(s);
  ASSERT_EQ("aa", value);

  delete iter;
  delete txn;
}

TEST_P(TransactionTest, SavepointTest) {
  WriteOptions write_options;
  ReadOptions read_options, snapshot_read_options;
  TransactionOptions txn_options;
  string value;
  Status s;

  Transaction* txn = db->BeginTransaction(write_options);
  ASSERT_TRUE(txn);

  ASSERT_EQ(0, txn->GetNumPuts());

  s = txn->RollbackToSavePoint();
  ASSERT_TRUE(s.IsNotFound());

  txn->SetSavePoint();  // 1

  ASSERT_OK(txn->RollbackToSavePoint());  // Rollback to beginning of txn
  s = txn->RollbackToSavePoint();
  ASSERT_TRUE(s.IsNotFound());

  s = txn->Put("B", "b");
  ASSERT_OK(s);

  ASSERT_EQ(1, txn->GetNumPuts());
  ASSERT_EQ(0, txn->GetNumDeletes());

  s = txn->Commit();
  ASSERT_OK(s);

  s = db->Get(read_options, "B", &value);
  ASSERT_OK(s);
  ASSERT_EQ("b", value);

  delete txn;
  txn = db->BeginTransaction(write_options);
  ASSERT_TRUE(txn);

  s = txn->Put("A", "a");
  ASSERT_OK(s);

  s = txn->Put("B", "bb");
  ASSERT_OK(s);

  s = txn->Put("C", "c");
  ASSERT_OK(s);

  txn->SetSavePoint();  // 2

  s = txn->Delete("B");
  ASSERT_OK(s);

  s = txn->Put("C", "cc");
  ASSERT_OK(s);

  s = txn->Put("D", "d");
  ASSERT_OK(s);

  ASSERT_EQ(5, txn->GetNumPuts());
  ASSERT_EQ(1, txn->GetNumDeletes());

  ASSERT_OK(txn->RollbackToSavePoint());  // Rollback to 2

  ASSERT_EQ(3, txn->GetNumPuts());
  ASSERT_EQ(0, txn->GetNumDeletes());

  s = txn->Get(read_options, "A", &value);
  ASSERT_OK(s);
  ASSERT_EQ("a", value);

  s = txn->Get(read_options, "B", &value);
  ASSERT_OK(s);
  ASSERT_EQ("bb", value);

  s = txn->Get(read_options, "C", &value);
  ASSERT_OK(s);
  ASSERT_EQ("c", value);

  s = txn->Get(read_options, "D", &value);
  ASSERT_TRUE(s.IsNotFound());

  s = txn->Put("A", "a");
  ASSERT_OK(s);

  s = txn->Put("E", "e");
  ASSERT_OK(s);

  ASSERT_EQ(5, txn->GetNumPuts());
  ASSERT_EQ(0, txn->GetNumDeletes());

  // Rollback to beginning of txn
  s = txn->RollbackToSavePoint();
  ASSERT_TRUE(s.IsNotFound());
  txn->Rollback();

  ASSERT_EQ(0, txn->GetNumPuts());
  ASSERT_EQ(0, txn->GetNumDeletes());

  s = txn->Get(read_options, "A", &value);
  ASSERT_TRUE(s.IsNotFound());

  s = txn->Get(read_options, "B", &value);
  ASSERT_OK(s);
  ASSERT_EQ("b", value);

  s = txn->Get(read_options, "D", &value);
  ASSERT_TRUE(s.IsNotFound());

  s = txn->Get(read_options, "D", &value);
  ASSERT_TRUE(s.IsNotFound());

  s = txn->Get(read_options, "E", &value);
  ASSERT_TRUE(s.IsNotFound());

  s = txn->Put("A", "aa");
  ASSERT_OK(s);

  s = txn->Put("F", "f");
  ASSERT_OK(s);

  ASSERT_EQ(2, txn->GetNumPuts());
  ASSERT_EQ(0, txn->GetNumDeletes());

  txn->SetSavePoint();  // 3
  txn->SetSavePoint();  // 4

  s = txn->Put("G", "g");
  ASSERT_OK(s);

  s = txn->SingleDelete("F");
  ASSERT_OK(s);

  s = txn->Delete("B");
  ASSERT_OK(s);

  s = txn->Get(read_options, "A", &value);
  ASSERT_OK(s);
  ASSERT_EQ("aa", value);

  s = txn->Get(read_options, "F", &value);
  // According to db.h, doing a SingleDelete on a key that has been
  // overwritten will have undefinied behavior.  So it is unclear what the
  // result of fetching "F" should be. The current implementation will
  // return NotFound in this case.
  ASSERT_TRUE(s.IsNotFound());

  s = txn->Get(read_options, "B", &value);
  ASSERT_TRUE(s.IsNotFound());

  ASSERT_EQ(3, txn->GetNumPuts());
  ASSERT_EQ(2, txn->GetNumDeletes());

  ASSERT_OK(txn->RollbackToSavePoint());  // Rollback to 3

  ASSERT_EQ(2, txn->GetNumPuts());
  ASSERT_EQ(0, txn->GetNumDeletes());

  s = txn->Get(read_options, "F", &value);
  ASSERT_OK(s);
  ASSERT_EQ("f", value);

  s = txn->Get(read_options, "G", &value);
  ASSERT_TRUE(s.IsNotFound());

  s = txn->Commit();
  ASSERT_OK(s);

  s = db->Get(read_options, "F", &value);
  ASSERT_OK(s);
  ASSERT_EQ("f", value);

  s = db->Get(read_options, "G", &value);
  ASSERT_TRUE(s.IsNotFound());

  s = db->Get(read_options, "A", &value);
  ASSERT_OK(s);
  ASSERT_EQ("aa", value);

  s = db->Get(read_options, "B", &value);
  ASSERT_OK(s);
  ASSERT_EQ("b", value);

  s = db->Get(read_options, "C", &value);
  ASSERT_TRUE(s.IsNotFound());

  s = db->Get(read_options, "D", &value);
  ASSERT_TRUE(s.IsNotFound());

  s = db->Get(read_options, "E", &value);
  ASSERT_TRUE(s.IsNotFound());

  delete txn;
}

TEST_P(TransactionTest, SavepointTest2) {
  WriteOptions write_options;
  ReadOptions read_options;
  TransactionOptions txn_options;
  Status s;

  txn_options.lock_timeout = 1;  // 1 ms
  Transaction* txn1 = db->BeginTransaction(write_options, txn_options);
  ASSERT_TRUE(txn1);

  s = txn1->Put("A", "");
  ASSERT_OK(s);

  txn1->SetSavePoint();  // 1

  s = txn1->Put("A", "a");
  ASSERT_OK(s);

  s = txn1->Put("C", "c");
  ASSERT_OK(s);

  txn1->SetSavePoint();  // 2

  s = txn1->Put("A", "a");
  ASSERT_OK(s);
  s = txn1->Put("B", "b");
  ASSERT_OK(s);

  ASSERT_OK(txn1->RollbackToSavePoint());  // Rollback to 2

  // Verify that "A" and "C" is still locked while "B" is not
  Transaction* txn2 = db->BeginTransaction(write_options, txn_options);
  ASSERT_TRUE(txn2);

  s = txn2->Put("A", "a2");
  ASSERT_TRUE(s.IsTimedOut());
  s = txn2->Put("C", "c2");
  ASSERT_TRUE(s.IsTimedOut());
  s = txn2->Put("B", "b2");
  ASSERT_OK(s);

  s = txn1->Put("A", "aa");
  ASSERT_OK(s);
  s = txn1->Put("B", "bb");
  ASSERT_TRUE(s.IsTimedOut());

  s = txn2->Commit();
  ASSERT_OK(s);
  delete txn2;

  s = txn1->Put("A", "aaa");
  ASSERT_OK(s);
  s = txn1->Put("B", "bbb");
  ASSERT_OK(s);
  s = txn1->Put("C", "ccc");
  ASSERT_OK(s);

  txn1->SetSavePoint();                    // 3
  ASSERT_OK(txn1->RollbackToSavePoint());  // Rollback to 3

  // Verify that "A", "B", "C" are still locked
  txn2 = db->BeginTransaction(write_options, txn_options);
  ASSERT_TRUE(txn2);

  s = txn2->Put("A", "a2");
  ASSERT_TRUE(s.IsTimedOut());
  s = txn2->Put("B", "b2");
  ASSERT_TRUE(s.IsTimedOut());
  s = txn2->Put("C", "c2");
  ASSERT_TRUE(s.IsTimedOut());

  ASSERT_OK(txn1->RollbackToSavePoint());  // Rollback to 1

  // Verify that only "A" is locked
  s = txn2->Put("A", "a3");
  ASSERT_TRUE(s.IsTimedOut());
  s = txn2->Put("B", "b3");
  ASSERT_OK(s);
  s = txn2->Put("C", "c3po");
  ASSERT_OK(s);

  s = txn1->Commit();
  ASSERT_OK(s);
  delete txn1;

  // Verify "A" "C" "B" are no longer locked
  s = txn2->Put("A", "a4");
  ASSERT_OK(s);
  s = txn2->Put("B", "b4");
  ASSERT_OK(s);
  s = txn2->Put("C", "c4");
  ASSERT_OK(s);

  s = txn2->Commit();
  ASSERT_OK(s);
  delete txn2;
}

TEST_P(TransactionTest, UndoGetForUpdateTest) {
  WriteOptions write_options;
  ReadOptions read_options;
  TransactionOptions txn_options;
  string value;
  Status s;

  txn_options.lock_timeout = 1;  // 1 ms
  Transaction* txn1 = db->BeginTransaction(write_options, txn_options);
  ASSERT_TRUE(txn1);

  txn1->UndoGetForUpdate("A");

  s = txn1->Commit();
  ASSERT_OK(s);
  delete txn1;

  txn1 = db->BeginTransaction(write_options, txn_options);

  txn1->UndoGetForUpdate("A");
  s = txn1->GetForUpdate(read_options, "A", &value);
  ASSERT_TRUE(s.IsNotFound());

  // Verify that A is locked
  Transaction* txn2 = db->BeginTransaction(write_options, txn_options);
  s = txn2->Put("A", "a");
  ASSERT_TRUE(s.IsTimedOut());

  txn1->UndoGetForUpdate("A");

  // Verify that A is now unlocked
  s = txn2->Put("A", "a2");
  ASSERT_OK(s);
  txn2->Commit();
  delete txn2;
  s = db->Get(read_options, "A", &value);
  ASSERT_OK(s);
  ASSERT_EQ("a2", value);

  s = txn1->Delete("A");
  ASSERT_OK(s);
  s = txn1->GetForUpdate(read_options, "A", &value);
  ASSERT_TRUE(s.IsNotFound());

  s = txn1->Put("B", "b3");
  ASSERT_OK(s);
  s = txn1->GetForUpdate(read_options, "B", &value);
  ASSERT_OK(s);

  txn1->UndoGetForUpdate("A");
  txn1->UndoGetForUpdate("B");

  // Verify that A and B are still locked
  txn2 = db->BeginTransaction(write_options, txn_options);
  s = txn2->Put("A", "a4");
  ASSERT_TRUE(s.IsTimedOut());
  s = txn2->Put("B", "b4");
  ASSERT_TRUE(s.IsTimedOut());

  txn1->Rollback();
  delete txn1;

  // Verify that A and B are no longer locked
  s = txn2->Put("A", "a5");
  ASSERT_OK(s);
  s = txn2->Put("B", "b5");
  ASSERT_OK(s);
  s = txn2->Commit();
  delete txn2;
  ASSERT_OK(s);

  txn1 = db->BeginTransaction(write_options, txn_options);

  s = txn1->GetForUpdate(read_options, "A", &value);
  ASSERT_OK(s);
  s = txn1->GetForUpdate(read_options, "A", &value);
  ASSERT_OK(s);
  s = txn1->GetForUpdate(read_options, "C", &value);
  ASSERT_TRUE(s.IsNotFound());
  s = txn1->GetForUpdate(read_options, "A", &value);
  ASSERT_OK(s);
  s = txn1->GetForUpdate(read_options, "C", &value);
  ASSERT_TRUE(s.IsNotFound());
  s = txn1->GetForUpdate(read_options, "B", &value);
  ASSERT_OK(s);
  s = txn1->Put("B", "b5");
  s = txn1->GetForUpdate(read_options, "B", &value);
  ASSERT_OK(s);

  txn1->UndoGetForUpdate("A");
  txn1->UndoGetForUpdate("B");
  txn1->UndoGetForUpdate("C");
  txn1->UndoGetForUpdate("X");

  // Verify A,B,C are locked
  txn2 = db->BeginTransaction(write_options, txn_options);
  s = txn2->Put("A", "a6");
  ASSERT_TRUE(s.IsTimedOut());
  s = txn2->Delete("B");
  ASSERT_TRUE(s.IsTimedOut());
  s = txn2->Put("C", "c6");
  ASSERT_TRUE(s.IsTimedOut());
  s = txn2->Put("X", "x6");
  ASSERT_OK(s);

  txn1->UndoGetForUpdate("A");
  txn1->UndoGetForUpdate("B");
  txn1->UndoGetForUpdate("C");
  txn1->UndoGetForUpdate("X");

  // Verify A,B are locked and C is not
  s = txn2->Put("A", "a6");
  ASSERT_TRUE(s.IsTimedOut());
  s = txn2->Delete("B");
  ASSERT_TRUE(s.IsTimedOut());
  s = txn2->Put("C", "c6");
  ASSERT_OK(s);
  s = txn2->Put("X", "x6");
  ASSERT_OK(s);

  txn1->UndoGetForUpdate("A");
  txn1->UndoGetForUpdate("B");
  txn1->UndoGetForUpdate("C");
  txn1->UndoGetForUpdate("X");

  // Verify B is locked and A and C are not
  s = txn2->Put("A", "a7");
  ASSERT_OK(s);
  s = txn2->Delete("B");
  ASSERT_TRUE(s.IsTimedOut());
  s = txn2->Put("C", "c7");
  ASSERT_OK(s);
  s = txn2->Put("X", "x7");
  ASSERT_OK(s);

  s = txn2->Commit();
  ASSERT_OK(s);
  delete txn2;

  s = txn1->Commit();
  ASSERT_OK(s);
  delete txn1;
}

TEST_P(TransactionTest, UndoGetForUpdateTest2) {
  WriteOptions write_options;
  ReadOptions read_options;
  TransactionOptions txn_options;
  string value;
  Status s;

  s = db->Put(write_options, "A", "");
  ASSERT_OK(s);

  txn_options.lock_timeout = 1;  // 1 ms
  Transaction* txn1 = db->BeginTransaction(write_options, txn_options);
  ASSERT_TRUE(txn1);

  s = txn1->GetForUpdate(read_options, "A", &value);
  ASSERT_OK(s);
  s = txn1->GetForUpdate(read_options, "B", &value);
  ASSERT_TRUE(s.IsNotFound());

  s = txn1->Put("F", "f");
  ASSERT_OK(s);

  txn1->SetSavePoint();  // 1

  txn1->UndoGetForUpdate("A");

  s = txn1->GetForUpdate(read_options, "C", &value);
  ASSERT_TRUE(s.IsNotFound());
  s = txn1->GetForUpdate(read_options, "D", &value);
  ASSERT_TRUE(s.IsNotFound());

  s = txn1->Put("E", "e");
  ASSERT_OK(s);
  s = txn1->GetForUpdate(read_options, "E", &value);
  ASSERT_OK(s);

  s = txn1->GetForUpdate(read_options, "F", &value);
  ASSERT_OK(s);

  // Verify A,B,C,D,E,F are still locked
  Transaction* txn2 = db->BeginTransaction(write_options, txn_options);
  s = txn2->Put("A", "a1");
  ASSERT_TRUE(s.IsTimedOut());
  s = txn2->Put("B", "b1");
  ASSERT_TRUE(s.IsTimedOut());
  s = txn2->Put("C", "c1");
  ASSERT_TRUE(s.IsTimedOut());
  s = txn2->Put("D", "d1");
  ASSERT_TRUE(s.IsTimedOut());
  s = txn2->Put("E", "e1");
  ASSERT_TRUE(s.IsTimedOut());
  s = txn2->Put("F", "f1");
  ASSERT_TRUE(s.IsTimedOut());

  txn1->UndoGetForUpdate("C");
  txn1->UndoGetForUpdate("E");

  // Verify A,B,D,E,F are still locked and C is not.
  s = txn2->Put("A", "a2");
  ASSERT_TRUE(s.IsTimedOut());
  s = txn2->Put("B", "b2");
  ASSERT_TRUE(s.IsTimedOut());
  s = txn2->Put("D", "d2");
  ASSERT_TRUE(s.IsTimedOut());
  s = txn2->Put("E", "e2");
  ASSERT_TRUE(s.IsTimedOut());
  s = txn2->Put("F", "f2");
  ASSERT_TRUE(s.IsTimedOut());
  s = txn2->Put("C", "c2");
  ASSERT_OK(s);

  txn1->SetSavePoint();  // 2

  s = txn1->Put("H", "h");
  ASSERT_OK(s);

  txn1->UndoGetForUpdate("A");
  txn1->UndoGetForUpdate("B");
  txn1->UndoGetForUpdate("C");
  txn1->UndoGetForUpdate("D");
  txn1->UndoGetForUpdate("E");
  txn1->UndoGetForUpdate("F");
  txn1->UndoGetForUpdate("G");
  txn1->UndoGetForUpdate("H");

  // Verify A,B,D,E,F,H are still locked and C,G are not.
  s = txn2->Put("A", "a3");
  ASSERT_TRUE(s.IsTimedOut());
  s = txn2->Put("B", "b3");
  ASSERT_TRUE(s.IsTimedOut());
  s = txn2->Put("D", "d3");
  ASSERT_TRUE(s.IsTimedOut());
  s = txn2->Put("E", "e3");
  ASSERT_TRUE(s.IsTimedOut());
  s = txn2->Put("F", "f3");
  ASSERT_TRUE(s.IsTimedOut());
  s = txn2->Put("H", "h3");
  ASSERT_TRUE(s.IsTimedOut());
  s = txn2->Put("C", "c3");
  ASSERT_OK(s);
  s = txn2->Put("G", "g3");
  ASSERT_OK(s);

  txn1->RollbackToSavePoint();  // rollback to 2

  // Verify A,B,D,E,F are still locked and C,G,H are not.
  s = txn2->Put("A", "a3");
  ASSERT_TRUE(s.IsTimedOut());
  s = txn2->Put("B", "b3");
  ASSERT_TRUE(s.IsTimedOut());
  s = txn2->Put("D", "d3");
  ASSERT_TRUE(s.IsTimedOut());
  s = txn2->Put("E", "e3");
  ASSERT_TRUE(s.IsTimedOut());
  s = txn2->Put("F", "f3");
  ASSERT_TRUE(s.IsTimedOut());
  s = txn2->Put("C", "c3");
  ASSERT_OK(s);
  s = txn2->Put("G", "g3");
  ASSERT_OK(s);
  s = txn2->Put("H", "h3");
  ASSERT_OK(s);

  txn1->UndoGetForUpdate("A");
  txn1->UndoGetForUpdate("B");
  txn1->UndoGetForUpdate("C");
  txn1->UndoGetForUpdate("D");
  txn1->UndoGetForUpdate("E");
  txn1->UndoGetForUpdate("F");
  txn1->UndoGetForUpdate("G");
  txn1->UndoGetForUpdate("H");

  // Verify A,B,E,F are still locked and C,D,G,H are not.
  s = txn2->Put("A", "a3");
  ASSERT_TRUE(s.IsTimedOut());
  s = txn2->Put("B", "b3");
  ASSERT_TRUE(s.IsTimedOut());
  s = txn2->Put("E", "e3");
  ASSERT_TRUE(s.IsTimedOut());
  s = txn2->Put("F", "f3");
  ASSERT_TRUE(s.IsTimedOut());
  s = txn2->Put("C", "c3");
  ASSERT_OK(s);
  s = txn2->Put("D", "d3");
  ASSERT_OK(s);
  s = txn2->Put("G", "g3");
  ASSERT_OK(s);
  s = txn2->Put("H", "h3");
  ASSERT_OK(s);

  txn1->RollbackToSavePoint();  // rollback to 1

  // Verify A,B,F are still locked and C,D,E,G,H are not.
  s = txn2->Put("A", "a3");
  ASSERT_TRUE(s.IsTimedOut());
  s = txn2->Put("B", "b3");
  ASSERT_TRUE(s.IsTimedOut());
  s = txn2->Put("F", "f3");
  ASSERT_TRUE(s.IsTimedOut());
  s = txn2->Put("C", "c3");
  ASSERT_OK(s);
  s = txn2->Put("D", "d3");
  ASSERT_OK(s);
  s = txn2->Put("E", "e3");
  ASSERT_OK(s);
  s = txn2->Put("G", "g3");
  ASSERT_OK(s);
  s = txn2->Put("H", "h3");
  ASSERT_OK(s);

  txn1->UndoGetForUpdate("A");
  txn1->UndoGetForUpdate("B");
  txn1->UndoGetForUpdate("C");
  txn1->UndoGetForUpdate("D");
  txn1->UndoGetForUpdate("E");
  txn1->UndoGetForUpdate("F");
  txn1->UndoGetForUpdate("G");
  txn1->UndoGetForUpdate("H");

  // Verify F is still locked and A,B,C,D,E,G,H are not.
  s = txn2->Put("F", "f3");
  ASSERT_TRUE(s.IsTimedOut());
  s = txn2->Put("A", "a3");
  ASSERT_OK(s);
  s = txn2->Put("B", "b3");
  ASSERT_OK(s);
  s = txn2->Put("C", "c3");
  ASSERT_OK(s);
  s = txn2->Put("D", "d3");
  ASSERT_OK(s);
  s = txn2->Put("E", "e3");
  ASSERT_OK(s);
  s = txn2->Put("G", "g3");
  ASSERT_OK(s);
  s = txn2->Put("H", "h3");
  ASSERT_OK(s);

  s = txn1->Commit();
  ASSERT_OK(s);
  s = txn2->Commit();
  ASSERT_OK(s);

  delete txn1;
  delete txn2;
}

TEST_P(TransactionTest, TimeoutTest) {
  WriteOptions write_options;
  ReadOptions read_options;
  string value;
  Status s;

  delete db;

  // transaction writes have an infinite timeout,
  // but we will override this when we start a txn
  // db writes have infinite timeout
  txn_db_options.transaction_lock_timeout = -1;
  txn_db_options.default_lock_timeout = -1;

  s = TransactionDB::Open(options, txn_db_options, dbname, &db);
  ASSERT_OK(s);

  s = db->Put(write_options, "aaa", "aaa");
  ASSERT_OK(s);

  TransactionOptions txn_options0;
  txn_options0.expiration = 100;  // 100ms
  txn_options0.lock_timeout = 50;  // txn timeout no longer infinite
  Transaction* txn1 = db->BeginTransaction(write_options, txn_options0);

  s = txn1->GetForUpdate(read_options, "aaa", nullptr);
  ASSERT_OK(s);

  // Conflicts with previous GetForUpdate.
  // Since db writes do not have a timeout, this should eventually succeed when
  // the transaction expires.
  s = db->Put(write_options, "aaa", "xxx");
  ASSERT_OK(s);

  ASSERT_GE(txn1->GetElapsedTime(),
            static_cast<uint64_t>(txn_options0.expiration));

  s = txn1->Commit();
  ASSERT_TRUE(s.IsExpired());  // expired!

  s = db->Get(read_options, "aaa", &value);
  ASSERT_OK(s);
  ASSERT_EQ("xxx", value);

  delete txn1;
  delete db;

  // transaction writes have 10ms timeout,
  // db writes have infinite timeout
  txn_db_options.transaction_lock_timeout = 50;
  txn_db_options.default_lock_timeout = -1;

  s = TransactionDB::Open(options, txn_db_options, dbname, &db);
  ASSERT_OK(s);

  s = db->Put(write_options, "aaa", "aaa");
  ASSERT_OK(s);

  TransactionOptions txn_options;
  txn_options.expiration = 100;  // 100ms
  txn1 = db->BeginTransaction(write_options, txn_options);

  s = txn1->GetForUpdate(read_options, "aaa", nullptr);
  ASSERT_OK(s);

  // Conflicts with previous GetForUpdate.
  // Since db writes do not have a timeout, this should eventually succeed when
  // the transaction expires.
  s = db->Put(write_options, "aaa", "xxx");
  ASSERT_OK(s);

  s = txn1->Commit();
  ASSERT_NOK(s);  // expired!

  s = db->Get(read_options, "aaa", &value);
  ASSERT_OK(s);
  ASSERT_EQ("xxx", value);

  delete txn1;
  txn_options.expiration = 6000000;  // 100 minutes
  txn_options.lock_timeout = 1;      // 1ms
  txn1 = db->BeginTransaction(write_options, txn_options);
  txn1->SetLockTimeout(100);

  TransactionOptions txn_options2;
  txn_options2.expiration = 10;  // 10ms
  Transaction* txn2 = db->BeginTransaction(write_options, txn_options2);
  ASSERT_OK(s);

  s = txn2->Put("a", "2");
  ASSERT_OK(s);

  // txn1 has a lock timeout longer than txn2's expiration, so it will win
  s = txn1->Delete("a");
  ASSERT_OK(s);

  s = txn1->Commit();
  ASSERT_OK(s);

  // txn2 should be expired out since txn1 waiting until its timeout expired.
  s = txn2->Commit();
  ASSERT_TRUE(s.IsExpired());

  delete txn1;
  delete txn2;
  txn_options.expiration = 6000000;  // 100 minutes
  txn1 = db->BeginTransaction(write_options, txn_options);
  txn_options2.expiration = 100000000;
  txn2 = db->BeginTransaction(write_options, txn_options2);

  s = txn1->Delete("asdf");
  ASSERT_OK(s);

  // txn2 has a smaller lock timeout than txn1's expiration, so it will time out
  s = txn2->Delete("asdf");
  ASSERT_TRUE(s.IsTimedOut());
  ASSERT_EQ(s.ToString(), "Operation timed out: Timeout waiting to lock key");

  s = txn1->Commit();
  ASSERT_OK(s);

  s = txn2->Put("asdf", "asdf");
  ASSERT_OK(s);

  s = txn2->Commit();
  ASSERT_OK(s);

  s = db->Get(read_options, "asdf", &value);
  ASSERT_OK(s);
  ASSERT_EQ("asdf", value);

  delete txn1;
  delete txn2;
}

TEST_P(TransactionTest, SingleDeleteTest) {
  WriteOptions write_options;
  ReadOptions read_options;
  string value;
  Status s;

  Transaction* txn = db->BeginTransaction(write_options);
  ASSERT_TRUE(txn);

  s = txn->SingleDelete("A");
  ASSERT_OK(s);

  s = txn->Get(read_options, "A", &value);
  ASSERT_TRUE(s.IsNotFound());

  s = txn->Commit();
  ASSERT_OK(s);
  delete txn;

  txn = db->BeginTransaction(write_options);

  s = txn->SingleDelete("A");
  ASSERT_OK(s);

  s = txn->Put("A", "a");
  ASSERT_OK(s);

  s = txn->Get(read_options, "A", &value);
  ASSERT_OK(s);
  ASSERT_EQ("a", value);

  s = txn->Commit();
  ASSERT_OK(s);
  delete txn;

  s = db->Get(read_options, "A", &value);
  ASSERT_OK(s);
  ASSERT_EQ("a", value);

  txn = db->BeginTransaction(write_options);

  s = txn->SingleDelete("A");
  ASSERT_OK(s);

  s = txn->Get(read_options, "A", &value);
  ASSERT_TRUE(s.IsNotFound());

  s = txn->Commit();
  ASSERT_OK(s);
  delete txn;

  s = db->Get(read_options, "A", &value);
  ASSERT_TRUE(s.IsNotFound());

  txn = db->BeginTransaction(write_options);
  Transaction* txn2 = db->BeginTransaction(write_options);
  txn2->SetSnapshot();

  s = txn->Put("A", "a");
  ASSERT_OK(s);

  s = txn->Put("A", "a2");
  ASSERT_OK(s);

  s = txn->SingleDelete("A");
  ASSERT_OK(s);

  s = txn->SingleDelete("B");
  ASSERT_OK(s);

  // According to db.h, doing a SingleDelete on a key that has been
  // overwritten will have undefinied behavior.  So it is unclear what the
  // result of fetching "A" should be. The current implementation will
  // return NotFound in this case.
  s = txn->Get(read_options, "A", &value);
  ASSERT_TRUE(s.IsNotFound());

  s = txn2->Put("B", "b");
  ASSERT_TRUE(s.IsTimedOut());
  s = txn2->Commit();
  ASSERT_OK(s);
  delete txn2;

  s = txn->Commit();
  ASSERT_OK(s);
  delete txn;

  // According to db.h, doing a SingleDelete on a key that has been
  // overwritten will have undefinied behavior.  So it is unclear what the
  // result of fetching "A" should be. The current implementation will
  // return NotFound in this case.
  s = db->Get(read_options, "A", &value);
  ASSERT_TRUE(s.IsNotFound());

  s = db->Get(read_options, "B", &value);
  ASSERT_TRUE(s.IsNotFound());
}

TEST_P(TransactionTest, MergeTest) {
  WriteOptions write_options;
  ReadOptions read_options;
  string value;
  Status s;

  Transaction* txn = db->BeginTransaction(write_options, TransactionOptions());
  ASSERT_TRUE(txn);

  s = db->Put(write_options, "A", "a0");
  ASSERT_OK(s);

  s = txn->Merge("A", "1");
  ASSERT_OK(s);

  s = txn->Merge("A", "2");
  ASSERT_OK(s);

  s = txn->Get(read_options, "A", &value);
  ASSERT_TRUE(s.IsMergeInProgress());

  s = txn->Put("A", "a");
  ASSERT_OK(s);

  s = txn->Get(read_options, "A", &value);
  ASSERT_OK(s);
  ASSERT_EQ("a", value);

  s = txn->Merge("A", "3");
  ASSERT_OK(s);

  s = txn->Get(read_options, "A", &value);
  ASSERT_TRUE(s.IsMergeInProgress());

  TransactionOptions txn_options;
  txn_options.lock_timeout = 1;  // 1 ms
  Transaction* txn2 = db->BeginTransaction(write_options, txn_options);
  ASSERT_TRUE(txn2);

  // verify that txn has "A" locked
  s = txn2->Merge("A", "4");
  ASSERT_TRUE(s.IsTimedOut());

  s = txn2->Commit();
  ASSERT_OK(s);
  delete txn2;

  s = txn->Commit();
  ASSERT_OK(s);
  delete txn;

  s = db->Get(read_options, "A", &value);
  ASSERT_OK(s);
  ASSERT_EQ("a,3", value);
}

TEST_P(TransactionTest, DeferSnapshotTest) {
  WriteOptions write_options;
  ReadOptions read_options;
  string value;
  Status s;

  s = db->Put(write_options, "A", "a0");
  ASSERT_OK(s);

  Transaction* txn1 = db->BeginTransaction(write_options);
  Transaction* txn2 = db->BeginTransaction(write_options);

  txn1->SetSnapshotOnNextOperation();
  auto snapshot = txn1->GetSnapshot();
  ASSERT_FALSE(snapshot);

  s = txn2->Put("A", "a2");
  ASSERT_OK(s);
  s = txn2->Commit();
  ASSERT_OK(s);
  delete txn2;

  s = txn1->GetForUpdate(read_options, "A", &value);
  // Should not conflict with txn2 since snapshot wasn't set until
  // GetForUpdate was called.
  ASSERT_OK(s);
  ASSERT_EQ("a2", value);

  s = txn1->Put("A", "a1");
  ASSERT_OK(s);

  s = db->Put(write_options, "B", "b0");
  ASSERT_OK(s);

  // Cannot lock B since it was written after the snapshot was set
  s = txn1->Put("B", "b1");
  ASSERT_TRUE(s.IsBusy());

  s = txn1->Commit();
  ASSERT_OK(s);
  delete txn1;

  s = db->Get(read_options, "A", &value);
  ASSERT_OK(s);
  ASSERT_EQ("a1", value);

  s = db->Get(read_options, "B", &value);
  ASSERT_OK(s);
  ASSERT_EQ("b0", value);
}

TEST_P(TransactionTest, DeferSnapshotTest2) {
  WriteOptions write_options;
  ReadOptions read_options, snapshot_read_options;
  string value;
  Status s;

  Transaction* txn1 = db->BeginTransaction(write_options);

  txn1->SetSnapshot();

  s = txn1->Put("A", "a1");
  ASSERT_OK(s);

  s = db->Put(write_options, "C", "c0");
  ASSERT_OK(s);
  s = db->Put(write_options, "D", "d0");
  ASSERT_OK(s);

  snapshot_read_options.snapshot = txn1->GetSnapshot();

  txn1->SetSnapshotOnNextOperation();

  s = txn1->Get(snapshot_read_options, "C", &value);
  // Snapshot was set before C was written
  ASSERT_TRUE(s.IsNotFound());
  s = txn1->Get(snapshot_read_options, "D", &value);
  // Snapshot was set before D was written
  ASSERT_TRUE(s.IsNotFound());

  // Snapshot should not have changed yet.
  snapshot_read_options.snapshot = txn1->GetSnapshot();

  s = txn1->Get(snapshot_read_options, "C", &value);
  // Snapshot was set before C was written
  ASSERT_TRUE(s.IsNotFound());
  s = txn1->Get(snapshot_read_options, "D", &value);
  // Snapshot was set before D was written
  ASSERT_TRUE(s.IsNotFound());

  s = txn1->GetForUpdate(read_options, "C", &value);
  ASSERT_OK(s);
  ASSERT_EQ("c0", value);

  s = db->Put(write_options, "D", "d00");
  ASSERT_OK(s);

  // Snapshot is now set
  snapshot_read_options.snapshot = txn1->GetSnapshot();
  s = txn1->Get(snapshot_read_options, "D", &value);
  ASSERT_OK(s);
  ASSERT_EQ("d0", value);

  s = txn1->Commit();
  ASSERT_OK(s);
  delete txn1;
}

TEST_P(TransactionTest, DeferSnapshotSavePointTest) {
  WriteOptions write_options;
  ReadOptions read_options, snapshot_read_options;
  string value;
  Status s;

  Transaction* txn1 = db->BeginTransaction(write_options);

  txn1->SetSavePoint();  // 1

  s = db->Put(write_options, "T", "1");
  ASSERT_OK(s);

  txn1->SetSnapshotOnNextOperation();

  s = db->Put(write_options, "T", "2");
  ASSERT_OK(s);

  txn1->SetSavePoint();  // 2

  s = db->Put(write_options, "T", "3");
  ASSERT_OK(s);

  s = txn1->Put("A", "a");
  ASSERT_OK(s);

  txn1->SetSavePoint();  // 3

  s = db->Put(write_options, "T", "4");
  ASSERT_OK(s);

  txn1->SetSnapshot();
  txn1->SetSnapshotOnNextOperation();

  txn1->SetSavePoint();  // 4

  s = db->Put(write_options, "T", "5");
  ASSERT_OK(s);

  snapshot_read_options.snapshot = txn1->GetSnapshot();
  s = txn1->Get(snapshot_read_options, "T", &value);
  ASSERT_OK(s);
  ASSERT_EQ("4", value);

  s = txn1->Put("A", "a1");
  ASSERT_OK(s);

  snapshot_read_options.snapshot = txn1->GetSnapshot();
  s = txn1->Get(snapshot_read_options, "T", &value);
  ASSERT_OK(s);
  ASSERT_EQ("5", value);

  s = txn1->RollbackToSavePoint();  // Rollback to 4
  ASSERT_OK(s);

  snapshot_read_options.snapshot = txn1->GetSnapshot();
  s = txn1->Get(snapshot_read_options, "T", &value);
  ASSERT_OK(s);
  ASSERT_EQ("4", value);

  s = txn1->RollbackToSavePoint();  // Rollback to 3
  ASSERT_OK(s);

  snapshot_read_options.snapshot = txn1->GetSnapshot();
  s = txn1->Get(snapshot_read_options, "T", &value);
  ASSERT_OK(s);
  ASSERT_EQ("3", value);

  s = txn1->Get(read_options, "T", &value);
  ASSERT_OK(s);
  ASSERT_EQ("5", value);

  s = txn1->RollbackToSavePoint();  // Rollback to 2
  ASSERT_OK(s);

  snapshot_read_options.snapshot = txn1->GetSnapshot();
  ASSERT_FALSE(snapshot_read_options.snapshot);
  s = txn1->Get(snapshot_read_options, "T", &value);
  ASSERT_OK(s);
  ASSERT_EQ("5", value);

  s = txn1->Delete("A");
  ASSERT_OK(s);

  snapshot_read_options.snapshot = txn1->GetSnapshot();
  ASSERT_TRUE(snapshot_read_options.snapshot);
  s = txn1->Get(snapshot_read_options, "T", &value);
  ASSERT_OK(s);
  ASSERT_EQ("5", value);

  s = txn1->RollbackToSavePoint();  // Rollback to 1
  ASSERT_OK(s);

  s = txn1->Delete("A");
  ASSERT_OK(s);

  snapshot_read_options.snapshot = txn1->GetSnapshot();
  ASSERT_FALSE(snapshot_read_options.snapshot);
  s = txn1->Get(snapshot_read_options, "T", &value);
  ASSERT_OK(s);
  ASSERT_EQ("5", value);

  s = txn1->Commit();
  ASSERT_OK(s);

  delete txn1;
}

TEST_P(TransactionTest, SetSnapshotOnNextOperationWithNotification) {
  WriteOptions write_options;
  ReadOptions read_options;
  string value;

  class Notifier : public TransactionNotifier {
   private:
    const Snapshot** snapshot_ptr_;

   public:
    explicit Notifier(const Snapshot** snapshot_ptr)
        : snapshot_ptr_(snapshot_ptr) {}

    void SnapshotCreated(const Snapshot* newSnapshot) {
      *snapshot_ptr_ = newSnapshot;
    }
  };

  std::shared_ptr<Notifier> notifier =
      std::make_shared<Notifier>(&read_options.snapshot);
  Status s;

  s = db->Put(write_options, "B", "0");
  ASSERT_OK(s);

  Transaction* txn1 = db->BeginTransaction(write_options);

  txn1->SetSnapshotOnNextOperation(notifier);
  ASSERT_FALSE(read_options.snapshot);

  s = db->Put(write_options, "B", "1");
  ASSERT_OK(s);

  // A Get does not generate the snapshot
  s = txn1->Get(read_options, "B", &value);
  ASSERT_OK(s);
  ASSERT_FALSE(read_options.snapshot);
  ASSERT_EQ(value, "1");

  // Any other operation does
  s = txn1->Put("A", "0");
  ASSERT_OK(s);

  // Now change "B".
  s = db->Put(write_options, "B", "2");
  ASSERT_OK(s);

  // The original value should still be read
  s = txn1->Get(read_options, "B", &value);
  ASSERT_OK(s);
  ASSERT_TRUE(read_options.snapshot);
  ASSERT_EQ(value, "1");

  s = txn1->Commit();
  ASSERT_OK(s);

  delete txn1;
}

TEST_P(TransactionTest, ClearSnapshotTest) {
  WriteOptions write_options;
  ReadOptions read_options, snapshot_read_options;
  string value;
  Status s;

  s = db->Put(write_options, "foo", "0");
  ASSERT_OK(s);

  Transaction* txn = db->BeginTransaction(write_options);
  ASSERT_TRUE(txn);

  s = db->Put(write_options, "foo", "1");
  ASSERT_OK(s);

  snapshot_read_options.snapshot = txn->GetSnapshot();
  ASSERT_FALSE(snapshot_read_options.snapshot);

  // No snapshot created yet
  s = txn->Get(snapshot_read_options, "foo", &value);
  ASSERT_EQ(value, "1");

  txn->SetSnapshot();
  snapshot_read_options.snapshot = txn->GetSnapshot();
  ASSERT_TRUE(snapshot_read_options.snapshot);

  s = db->Put(write_options, "foo", "2");
  ASSERT_OK(s);

  // Snapshot was created before change to '2'
  s = txn->Get(snapshot_read_options, "foo", &value);
  ASSERT_EQ(value, "1");

  txn->ClearSnapshot();
  snapshot_read_options.snapshot = txn->GetSnapshot();
  ASSERT_FALSE(snapshot_read_options.snapshot);

  // Snapshot has now been cleared
  s = txn->Get(snapshot_read_options, "foo", &value);
  ASSERT_EQ(value, "2");

  s = txn->Commit();
  ASSERT_OK(s);

  delete txn;
}

TEST_P(TransactionTest, ToggleAutoCompactionTest) {
  Status s;

  TransactionOptions txn_options;
  ColumnFamilyHandle *cfa, *cfb;
  ColumnFamilyOptions cf_options;

  // Create 2 new column families
  s = db->CreateColumnFamily(cf_options, "CFA", &cfa);
  ASSERT_OK(s);
  s = db->CreateColumnFamily(cf_options, "CFB", &cfb);
  ASSERT_OK(s);

  delete cfa;
  delete cfb;
  delete db;

  // open DB with three column families
  std::vector<ColumnFamilyDescriptor> column_families;
  // have to open default column family
  column_families.push_back(
      ColumnFamilyDescriptor(kDefaultColumnFamilyName, ColumnFamilyOptions()));
  // open the new column families
  column_families.push_back(
      ColumnFamilyDescriptor("CFA", ColumnFamilyOptions()));
  column_families.push_back(
      ColumnFamilyDescriptor("CFB", ColumnFamilyOptions()));

  ColumnFamilyOptions* cf_opt_default = &column_families[0].options;
  ColumnFamilyOptions* cf_opt_cfa = &column_families[1].options;
  ColumnFamilyOptions* cf_opt_cfb = &column_families[2].options;
  cf_opt_default->disable_auto_compactions = false;
  cf_opt_cfa->disable_auto_compactions = true;
  cf_opt_cfb->disable_auto_compactions = false;

  std::vector<ColumnFamilyHandle*> handles;

  s = TransactionDB::Open(options, txn_db_options, dbname, column_families,
                          &handles, &db);
  ASSERT_OK(s);

  auto cfh_default = reinterpret_cast<ColumnFamilyHandleImpl*>(handles[0]);
  auto opt_default = *cfh_default->cfd()->GetLatestMutableCFOptions();

  auto cfh_a = reinterpret_cast<ColumnFamilyHandleImpl*>(handles[1]);
  auto opt_a = *cfh_a->cfd()->GetLatestMutableCFOptions();

  auto cfh_b = reinterpret_cast<ColumnFamilyHandleImpl*>(handles[2]);
  auto opt_b = *cfh_b->cfd()->GetLatestMutableCFOptions();

  ASSERT_EQ(opt_default.disable_auto_compactions, false);
  ASSERT_EQ(opt_a.disable_auto_compactions, true);
  ASSERT_EQ(opt_b.disable_auto_compactions, false);

  for (auto handle : handles) {
    delete handle;
  }
}

TEST_P(TransactionTest, ExpiredTransactionDataRace1) {
  // In this test, txn1 should succeed committing,
  // as the callback is called after txn1 starts committing.
  rocksdb::SyncPoint::GetInstance()->LoadDependency(
      {{"TransactionTest::ExpirableTransactionDataRace:1"}});
  rocksdb::SyncPoint::GetInstance()->SetCallBack(
      "TransactionTest::ExpirableTransactionDataRace:1", [&](void* arg) {
        WriteOptions write_options;
        TransactionOptions txn_options;

        // Force txn1 to expire
        /* sleep override */
        std::this_thread::sleep_for(std::chrono::milliseconds(150));

        Transaction* txn2 = db->BeginTransaction(write_options, txn_options);
        Status s;
        s = txn2->Put("X", "2");
        ASSERT_TRUE(s.IsTimedOut());
        s = txn2->Commit();
        ASSERT_OK(s);
        delete txn2;
      });

  rocksdb::SyncPoint::GetInstance()->EnableProcessing();

  WriteOptions write_options;
  TransactionOptions txn_options;

  txn_options.expiration = 100;
  Transaction* txn1 = db->BeginTransaction(write_options, txn_options);

  Status s;
  s = txn1->Put("X", "1");
  ASSERT_OK(s);
  s = txn1->Commit();
  ASSERT_OK(s);

  ReadOptions read_options;
  string value;
  s = db->Get(read_options, "X", &value);
  ASSERT_EQ("1", value);

  delete txn1;
  rocksdb::SyncPoint::GetInstance()->DisableProcessing();
}

namespace {
Status TransactionStressTestInserter(TransactionDB* db,
                                     const size_t num_transactions,
                                     const size_t num_sets,
                                     const size_t num_keys_per_set) {
  size_t seed = std::hash<std::thread::id>()(std::this_thread::get_id());
  Random64 _rand(seed);
  WriteOptions write_options;
  ReadOptions read_options;
  TransactionOptions txn_options;
  txn_options.set_snapshot = true;

  RandomTransactionInserter inserter(&_rand, write_options, read_options,
                                     num_keys_per_set,
                                     static_cast<uint16_t>(num_sets));

  for (size_t t = 0; t < num_transactions; t++) {
    bool success = inserter.TransactionDBInsert(db, txn_options);
    if (!success) {
      // unexpected failure
      return inserter.GetLastStatus();
    }
  }

  // Make sure at least some of the transactions succeeded.  It's ok if
  // some failed due to write-conflicts.
  if (inserter.GetFailureCount() > num_transactions / 2) {
    return Status::TryAgain("Too many transactions failed! " +
                            std::to_string(inserter.GetFailureCount()) + " / " +
                            std::to_string(num_transactions));
  }

  return Status::OK();
}
}  // namespace

TEST_P(TransactionTest, TransactionStressTest) {
  const size_t num_threads = 4;
  const size_t num_transactions_per_thread = 10000;
  const size_t num_sets = 3;
  const size_t num_keys_per_set = 100;
  // Setting the key-space to be 100 keys should cause enough write-conflicts
  // to make this test interesting.

  std::vector<std::thread> threads;

  std::function<void()> call_inserter = [&] {
    ASSERT_OK(TransactionStressTestInserter(db, num_transactions_per_thread,
                                            num_sets, num_keys_per_set));
  };

  // Create N threads that use RandomTransactionInserter to write
  // many transactions.
  for (uint32_t i = 0; i < num_threads; i++) {
    threads.emplace_back(call_inserter);
  }

  // Wait for all threads to run
  for (auto& t : threads) {
    t.join();
  }

  // Verify that data is consistent
  Status s = RandomTransactionInserter::Verify(db, num_sets);
  ASSERT_OK(s);
}

}  // namespace rocksdb

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

#else
#include <stdio.h>

int main(int argc, char** argv) {
  fprintf(stderr,
          "SKIPPED as Transactions are not supported in ROCKSDB_LITE\n");
  return 0;
}

#endif  // ROCKSDB_LITE

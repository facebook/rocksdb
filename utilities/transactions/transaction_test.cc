//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "utilities/transactions/transaction_test.h"

#include <algorithm>
#include <array>
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
#include "table/mock_table.h"
#include "test_util/sync_point.h"
#include "test_util/testharness.h"
#include "test_util/testutil.h"
#include "test_util/transaction_test_util.h"
#include "util/random.h"
#include "util/string_util.h"
#include "utilities/merge_operators.h"
#include "utilities/merge_operators/string_append/stringappend.h"
#include "utilities/transactions/pessimistic_transaction_db.h"

namespace ROCKSDB_NAMESPACE {

INSTANTIATE_TEST_CASE_P(
    DBAsBaseDB, TransactionTest,
    ::testing::Values(
        std::make_tuple(false, false, WRITE_COMMITTED, kOrderedWrite),
        std::make_tuple(false, true, WRITE_COMMITTED, kOrderedWrite),
        std::make_tuple(false, false, WRITE_PREPARED, kOrderedWrite),
        std::make_tuple(false, true, WRITE_PREPARED, kOrderedWrite),
        std::make_tuple(false, true, WRITE_PREPARED, kUnorderedWrite),
        std::make_tuple(false, false, WRITE_UNPREPARED, kOrderedWrite),
        std::make_tuple(false, true, WRITE_UNPREPARED, kOrderedWrite)));
INSTANTIATE_TEST_CASE_P(
    DBAsBaseDB, TransactionStressTest,
    ::testing::Values(
        std::make_tuple(false, false, WRITE_COMMITTED, kOrderedWrite),
        std::make_tuple(false, true, WRITE_COMMITTED, kOrderedWrite),
        std::make_tuple(false, false, WRITE_PREPARED, kOrderedWrite),
        std::make_tuple(false, true, WRITE_PREPARED, kOrderedWrite),
        std::make_tuple(false, true, WRITE_PREPARED, kUnorderedWrite),
        std::make_tuple(false, false, WRITE_UNPREPARED, kOrderedWrite),
        std::make_tuple(false, true, WRITE_UNPREPARED, kOrderedWrite)));
INSTANTIATE_TEST_CASE_P(
    StackableDBAsBaseDB, TransactionTest,
    ::testing::Values(
        std::make_tuple(true, true, WRITE_COMMITTED, kOrderedWrite),
        std::make_tuple(true, true, WRITE_PREPARED, kOrderedWrite),
        std::make_tuple(true, true, WRITE_UNPREPARED, kOrderedWrite)));

// MySQLStyleTransactionTest takes far too long for valgrind to run. Only do it
// in full mode (`ROCKSDB_FULL_VALGRIND_RUN` compiler flag is set).
#if !defined(ROCKSDB_VALGRIND_RUN) || defined(ROCKSDB_FULL_VALGRIND_RUN)
INSTANTIATE_TEST_CASE_P(
    MySQLStyleTransactionTest, MySQLStyleTransactionTest,
    ::testing::Values(
        std::make_tuple(false, false, WRITE_COMMITTED, kOrderedWrite, false),
        std::make_tuple(false, true, WRITE_COMMITTED, kOrderedWrite, false),
        std::make_tuple(false, false, WRITE_PREPARED, kOrderedWrite, false),
        std::make_tuple(false, false, WRITE_PREPARED, kOrderedWrite, true),
        std::make_tuple(false, true, WRITE_PREPARED, kOrderedWrite, false),
        std::make_tuple(false, true, WRITE_PREPARED, kOrderedWrite, true),
        std::make_tuple(false, false, WRITE_UNPREPARED, kOrderedWrite, false),
        std::make_tuple(false, false, WRITE_UNPREPARED, kOrderedWrite, true),
        std::make_tuple(false, true, WRITE_UNPREPARED, kOrderedWrite, false),
        std::make_tuple(false, true, WRITE_UNPREPARED, kOrderedWrite, true),
        std::make_tuple(false, true, WRITE_PREPARED, kUnorderedWrite, false),
        std::make_tuple(false, true, WRITE_PREPARED, kUnorderedWrite, true)));
#endif  // !defined(ROCKSDB_VALGRIND_RUN) || defined(ROCKSDB_FULL_VALGRIND_RUN)

TEST_P(TransactionTest, TestUpperBoundUponDeletion) {
  // Reproduction from the original bug report, 11606
  // This test does writes without snapshot validation, and then tries to create
  // iterator later, which is unsupported in write unprepared.
  if (txn_db_options.write_policy == WRITE_UNPREPARED) {
    return;
  }

  WriteOptions write_options;
  ReadOptions read_options;
  Status s;

  Transaction* txn = db->BeginTransaction(write_options);
  ASSERT_TRUE(txn);

  // Write some keys in a txn
  s = txn->Put("2", "2");
  ASSERT_OK(s);

  s = txn->Put("1", "1");
  ASSERT_OK(s);

  s = txn->Delete("2");
  ASSERT_OK(s);

  read_options.iterate_upper_bound = new Slice("2", 1);
  Iterator* iter = txn->GetIterator(read_options);
  ASSERT_OK(iter->status());
  iter->SeekToFirst();
  while (iter->Valid()) {
    ASSERT_EQ("1", iter->key().ToString());
    iter->Next();
  }
  delete iter;
  delete txn;
  delete read_options.iterate_upper_bound;
}

TEST_P(TransactionTest, TestTxnRespectBoundsInReadOption) {
  if (txn_db_options.write_policy == WRITE_UNPREPARED) {
    return;
  }

  WriteOptions write_options;

  {
    std::unique_ptr<Transaction> txn(db->BeginTransaction(write_options));
    // writes that should be observed by base_iterator_ in BaseDeltaIterator
    ASSERT_OK(txn->Put("a", "aa"));
    ASSERT_OK(txn->Put("c", "cc"));
    ASSERT_OK(txn->Put("e", "ee"));
    ASSERT_OK(txn->Put("f", "ff"));
    ASSERT_TRUE(txn->Commit().ok());
  }

  std::unique_ptr<Transaction> txn2(db->BeginTransaction(write_options));
  // writes that should be observed by delta_iterator_ in BaseDeltaIterator
  ASSERT_OK(txn2->Put("b", "bb"));
  ASSERT_OK(txn2->Put("c", "cc"));
  ASSERT_OK(txn2->Put("f", "ff"));

  // delta_iterator_:   b c   f
  //  base_iterator_: a   c e f
  //
  // given range [c, f)
  // assert only {c, e} can be seen

  ReadOptions ro;
  ro.iterate_lower_bound = new Slice("c");
  ro.iterate_upper_bound = new Slice("f");
  std::unique_ptr<Iterator> iter(txn2->GetIterator(ro));

  iter->Seek(Slice("b"));
  ASSERT_EQ("c", iter->key());  // lower bound capping
  iter->Seek(Slice("f"));
  ASSERT_FALSE(iter->Valid());  // out of bound

  iter->SeekForPrev(Slice("f"));
  ASSERT_EQ("e", iter->key());  // upper bound capping
  iter->SeekForPrev(Slice("b"));
  ASSERT_FALSE(iter->Valid());  // out of bound

  // move to the lower bound
  iter->SeekToFirst();
  ASSERT_EQ("c", iter->key());
  iter->Prev();
  ASSERT_FALSE(iter->Valid());

  // move to the upper bound
  iter->SeekToLast();
  ASSERT_EQ("e", iter->key());
  iter->Next();
  ASSERT_FALSE(iter->Valid());

  // reversely walk to the beginning
  iter->SeekToLast();
  ASSERT_EQ("e", iter->key());
  iter->Prev();
  ASSERT_EQ("c", iter->key());
  iter->Prev();
  ASSERT_FALSE(iter->Valid());

  delete ro.iterate_lower_bound;
  delete ro.iterate_upper_bound;
}

TEST_P(TransactionTest, DoubleEmptyWrite) {
  WriteOptions write_options;
  write_options.sync = true;
  write_options.disableWAL = false;

  WriteBatch batch;

  ASSERT_OK(db->Write(write_options, &batch));
  ASSERT_OK(db->Write(write_options, &batch));

  // Also test committing empty transactions in 2PC
  TransactionOptions txn_options;
  Transaction* txn0 = db->BeginTransaction(write_options, txn_options);
  ASSERT_OK(txn0->SetName("xid"));
  ASSERT_OK(txn0->Prepare());
  ASSERT_OK(txn0->Commit());
  delete txn0;

  // Also test that it works during recovery
  txn0 = db->BeginTransaction(write_options, txn_options);
  ASSERT_OK(txn0->SetName("xid2"));
  ASSERT_OK(txn0->Put(Slice("foo0"), Slice("bar0a")));
  ASSERT_OK(txn0->Prepare());
  delete txn0;
  static_cast<PessimisticTransactionDB*>(db)->TEST_Crash();
  ASSERT_OK(ReOpenNoDelete());
  assert(db != nullptr);
  txn0 = db->GetTransactionByName("xid2");
  ASSERT_OK(txn0->Commit());
  delete txn0;
}

TEST_P(TransactionTest, SuccessTest) {
  ASSERT_OK(db->ResetStats());

  WriteOptions write_options;
  ReadOptions read_options;
  std::string value;

  ASSERT_OK(db->Put(write_options, Slice("foo"), Slice("bar")));
  ASSERT_OK(db->Put(write_options, Slice("foo2"), Slice("bar")));

  Transaction* txn = db->BeginTransaction(write_options, TransactionOptions());
  ASSERT_TRUE(txn);

  ASSERT_EQ(0, txn->GetNumPuts());
  ASSERT_LE(0, txn->GetID());

  ASSERT_OK(txn->GetForUpdate(read_options, "foo", &value));
  ASSERT_EQ(value, "bar");

  ASSERT_OK(txn->Put(Slice("foo"), Slice("bar2")));

  ASSERT_EQ(1, txn->GetNumPuts());

  ASSERT_OK(txn->GetForUpdate(read_options, "foo", &value));
  ASSERT_EQ(value, "bar2");

  ASSERT_OK(txn->Commit());

  ASSERT_OK(db->Get(read_options, "foo", &value));
  ASSERT_EQ(value, "bar2");

  delete txn;
}

// Test the basic API of the pinnable slice overload of GetForUpdate()
TEST_P(TransactionTest, SuccessTestPinnable) {
  ASSERT_OK(db->ResetStats());

  WriteOptions write_options;
  ReadOptions read_options;
  PinnableSlice pinnable_val;

  ASSERT_OK(db->Put(write_options, Slice("foo"), Slice("bar")));
  ASSERT_OK(db->Put(write_options, Slice("foo2"), Slice("bar")));

  Transaction* txn = db->BeginTransaction(write_options, TransactionOptions());
  ASSERT_TRUE(txn);

  ASSERT_EQ(0, txn->GetNumPuts());
  ASSERT_LE(0, txn->GetID());

  ASSERT_OK(txn->GetForUpdate(read_options, "foo", &pinnable_val));
  ASSERT_EQ(*pinnable_val.GetSelf(), std::string("bar"));

  ASSERT_OK(txn->Put(Slice("foo"), Slice("bar2")));

  ASSERT_EQ(1, txn->GetNumPuts());

  ASSERT_OK(txn->GetForUpdate(read_options, "foo", &pinnable_val));
  ASSERT_EQ(*pinnable_val.GetSelf(), std::string("bar2"));

  ASSERT_OK(txn->Commit());

  ASSERT_OK(
      db->Get(read_options, db->DefaultColumnFamily(), "foo", &pinnable_val));
  ASSERT_EQ(*pinnable_val.GetSelf(), std::string("bar2"));

  delete txn;
}

TEST_P(TransactionTest, SwitchMemtableDuringPrepareAndCommit_WC) {
  const TxnDBWritePolicy write_policy = std::get<2>(GetParam());

  if (write_policy != TxnDBWritePolicy::WRITE_COMMITTED) {
    ROCKSDB_GTEST_BYPASS("Test applies to write-committed only");
    return;
  }

  ASSERT_OK(db->Put(WriteOptions(), "key0", "value"));

  TransactionOptions txn_opts;
  txn_opts.use_only_the_last_commit_time_batch_for_recovery = true;
  Transaction* txn = db->BeginTransaction(WriteOptions(), txn_opts);
  assert(txn);

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  SyncPoint::GetInstance()->SetCallBack(
      "FlushJob::WriteLevel0Table", [&](void* arg) {
        // db mutex not held.
        auto* mems = static_cast<autovector<MemTable*>*>(arg);
        assert(mems);
        ASSERT_EQ(1, mems->size());
        auto* ctwb = txn->GetCommitTimeWriteBatch();
        ASSERT_OK(ctwb->Put("gtid", "123"));
        ASSERT_OK(txn->Commit());
        delete txn;
      });
  SyncPoint::GetInstance()->EnableProcessing();

  ASSERT_OK(txn->Put("key1", "value"));
  ASSERT_OK(txn->SetName("txn1"));

  ASSERT_OK(txn->Prepare());

  auto dbimpl = static_cast_with_check<DBImpl>(db->GetRootDB());
  ASSERT_OK(dbimpl->TEST_SwitchMemtable(nullptr));
  ASSERT_OK(dbimpl->TEST_FlushMemTable(
      /*wait=*/false, /*allow_write_stall=*/true, /*cfh=*/nullptr));

  ASSERT_OK(dbimpl->TEST_WaitForFlushMemTable());

  {
    std::string value;
    ASSERT_OK(db->Get(ReadOptions(), "key1", &value));
    ASSERT_EQ("value", value);
  }

  ASSERT_OK(dbimpl->SyncWAL());
  delete db;
  db = nullptr;
  Status s;
  if (use_stackable_db_ == false) {
    s = TransactionDB::Open(options, txn_db_options, dbname, &db);
  } else {
    s = OpenWithStackableDB();
  }
  ASSERT_OK(s);
  assert(db);

  {
    std::string value;
    ASSERT_OK(db->Get(ReadOptions(), "gtid", &value));
    ASSERT_EQ("123", value);

    ASSERT_OK(db->Get(ReadOptions(), "key1", &value));
    ASSERT_EQ("value", value);
  }

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
}

// The test clarifies the contract of do_validate and assume_tracked
// in GetForUpdate and Put/Merge/Delete
TEST_P(TransactionTest, AssumeExclusiveTracked) {
  WriteOptions write_options;
  ReadOptions read_options;
  std::string value;
  Status s;
  TransactionOptions txn_options;
  txn_options.lock_timeout = 1;
  const bool EXCLUSIVE = true;
  const bool DO_VALIDATE = true;
  const bool ASSUME_LOCKED = true;

  Transaction* txn = db->BeginTransaction(write_options, txn_options);
  ASSERT_TRUE(txn);
  txn->SetSnapshot();

  // commit a value after the snapshot is taken
  ASSERT_OK(db->Put(write_options, Slice("foo"), Slice("bar")));

  // By default write should fail to the commit after our snapshot
  s = txn->GetForUpdate(read_options, "foo", &value, EXCLUSIVE);
  ASSERT_TRUE(s.IsBusy());
  // But the user could direct the db to skip validating the snapshot. The read
  // value then should be the most recently committed
  ASSERT_OK(
      txn->GetForUpdate(read_options, "foo", &value, EXCLUSIVE, !DO_VALIDATE));
  ASSERT_EQ(value, "bar");

  // Although ValidateSnapshot is skipped the key must have still got locked
  s = db->Put(write_options, Slice("foo"), Slice("bar"));
  ASSERT_TRUE(s.IsTimedOut());

  // By default the write operations should fail due to the commit after the
  // snapshot
  s = txn->Put(Slice("foo"), Slice("bar1"));
  ASSERT_TRUE(s.IsBusy());
  s = txn->Put(db->DefaultColumnFamily(), Slice("foo"), Slice("bar1"),
               !ASSUME_LOCKED);
  ASSERT_TRUE(s.IsBusy());
  // But the user could direct the db that it already assumes exclusive lock on
  // the key due to the previous GetForUpdate call.
  ASSERT_OK(txn->Put(db->DefaultColumnFamily(), Slice("foo"), Slice("bar1"),
                     ASSUME_LOCKED));
  ASSERT_OK(txn->Merge(db->DefaultColumnFamily(), Slice("foo"), Slice("bar2"),
                       ASSUME_LOCKED));
  ASSERT_OK(
      txn->Delete(db->DefaultColumnFamily(), Slice("foo"), ASSUME_LOCKED));
  ASSERT_OK(txn->SingleDelete(db->DefaultColumnFamily(), Slice("foo"),
                              ASSUME_LOCKED));

  ASSERT_OK(txn->Rollback());
  delete txn;
}

// This test clarifies the contract of ValidateSnapshot
TEST_P(TransactionTest, ValidateSnapshotTest) {
  for (bool with_flush : {true}) {
    for (bool with_2pc : {true}) {
      ASSERT_OK(ReOpen());
      WriteOptions write_options;
      ReadOptions read_options;
      std::string value;

      assert(db != nullptr);
      Transaction* txn1 =
          db->BeginTransaction(write_options, TransactionOptions());
      ASSERT_TRUE(txn1);
      ASSERT_OK(txn1->Put(Slice("foo"), Slice("bar1")));
      if (with_2pc) {
        ASSERT_OK(txn1->SetName("xid1"));
        ASSERT_OK(txn1->Prepare());
      }

      if (with_flush) {
        auto db_impl = static_cast_with_check<DBImpl>(db->GetRootDB());
        ASSERT_OK(db_impl->TEST_FlushMemTable(true));
        // Make sure the flushed memtable is not kept in memory
        int max_memtable_in_history =
            std::max(
                options.max_write_buffer_number,
                static_cast<int>(options.max_write_buffer_size_to_maintain) /
                    static_cast<int>(options.write_buffer_size)) +
            1;
        for (int i = 0; i < max_memtable_in_history; i++) {
          ASSERT_OK(db->Put(write_options, Slice("key"), Slice("value")));
          ASSERT_OK(db_impl->TEST_FlushMemTable(true));
        }
      }

      Transaction* txn2 =
          db->BeginTransaction(write_options, TransactionOptions());
      ASSERT_TRUE(txn2);
      txn2->SetSnapshot();

      ASSERT_OK(txn1->Commit());
      delete txn1;

      auto pes_txn2 = dynamic_cast<PessimisticTransaction*>(txn2);
      // Test the simple case where the key is not tracked yet
      auto trakced_seq = kMaxSequenceNumber;
      auto s = pes_txn2->ValidateSnapshot(db->DefaultColumnFamily(), "foo",
                                          &trakced_seq);
      ASSERT_TRUE(s.IsBusy());
      delete txn2;
    }
  }
}

TEST_P(TransactionTest, WaitingTxn) {
  WriteOptions write_options;
  ReadOptions read_options;
  TransactionOptions txn_options;
  std::string value;
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

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "PointLockManager::AcquireWithTimeout:WaitingTxn", [&](void* /*arg*/) {
        std::string key;
        uint32_t cf_id;
        std::vector<TransactionID> wait = txn2->GetWaitingTxns(&cf_id, &key);
        ASSERT_EQ(key, "foo");
        ASSERT_EQ(wait.size(), 1);
        ASSERT_EQ(wait[0], id1);
        ASSERT_EQ(cf_id, 0U);
      });

  get_perf_context()->Reset();
  // lock key in default cf
  s = txn1->GetForUpdate(read_options, "foo", &value);
  ASSERT_OK(s);
  ASSERT_EQ(value, "bar");
  ASSERT_EQ(get_perf_context()->key_lock_wait_count, 0);

  // lock key in cfa
  s = txn1->GetForUpdate(read_options, cfa, "foo", &value);
  ASSERT_OK(s);
  ASSERT_EQ(value, "bar");
  ASSERT_EQ(get_perf_context()->key_lock_wait_count, 0);

  auto lock_data = db->GetLockStatusData();
  // Locked keys exist in both column family.
  ASSERT_EQ(lock_data.size(), 2);

  auto cf_iterator = lock_data.begin();

  // The iterator points to an unordered_multimap
  // thus the test can not assume any particular order.

  // Column family is 1 or 0 (cfa).
  if (cf_iterator->first != 1 && cf_iterator->first != 0) {
    FAIL();
  }
  // The locked key is "foo" and is locked by txn1
  ASSERT_EQ(cf_iterator->second.key, "foo");
  ASSERT_EQ(cf_iterator->second.ids.size(), 1);
  ASSERT_EQ(cf_iterator->second.ids[0], txn1->GetID());

  cf_iterator++;

  // Column family is 0 (default) or 1.
  if (cf_iterator->first != 1 && cf_iterator->first != 0) {
    FAIL();
  }
  // The locked key is "foo" and is locked by txn1
  ASSERT_EQ(cf_iterator->second.key, "foo");
  ASSERT_EQ(cf_iterator->second.ids.size(), 1);
  ASSERT_EQ(cf_iterator->second.ids[0], txn1->GetID());

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  s = txn2->GetForUpdate(read_options, "foo", &value);
  ASSERT_TRUE(s.IsTimedOut());
  ASSERT_EQ(s.ToString(), "Operation timed out: Timeout waiting to lock key");
  ASSERT_EQ(get_perf_context()->key_lock_wait_count, 1);
  ASSERT_GE(get_perf_context()->key_lock_wait_time, 0);

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearAllCallBacks();

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
  s = txn1->GetForUpdate(read_options, "foo", (std::string*)nullptr,
                         false /* exclusive */);
  ASSERT_OK(s);

  s = txn2->GetForUpdate(read_options, "foo", (std::string*)nullptr,
                         false /* exclusive */);
  ASSERT_OK(s);

  s = txn3->GetForUpdate(read_options, "foo", (std::string*)nullptr,
                         false /* exclusive */);
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

  ASSERT_OK(txn1->Rollback());
  ASSERT_OK(txn2->Rollback());
  ASSERT_OK(txn3->Rollback());

  // Test txn1 and txn2 sharing a lock and txn3 trying to obtain it.
  s = txn1->GetForUpdate(read_options, "foo", (std::string*)nullptr,
                         false /* exclusive */);
  ASSERT_OK(s);

  s = txn2->GetForUpdate(read_options, "foo", (std::string*)nullptr,
                         false /* exclusive */);
  ASSERT_OK(s);

  s = txn3->GetForUpdate(read_options, "foo", (std::string*)nullptr);
  ASSERT_TRUE(s.IsTimedOut());
  ASSERT_EQ(s.ToString(), "Operation timed out: Timeout waiting to lock key");

  txn1->UndoGetForUpdate("foo");
  s = txn3->GetForUpdate(read_options, "foo", (std::string*)nullptr);
  ASSERT_TRUE(s.IsTimedOut());
  ASSERT_EQ(s.ToString(), "Operation timed out: Timeout waiting to lock key");

  txn2->UndoGetForUpdate("foo");
  s = txn3->GetForUpdate(read_options, "foo", (std::string*)nullptr);
  ASSERT_OK(s);

  ASSERT_OK(txn1->Rollback());
  ASSERT_OK(txn2->Rollback());
  ASSERT_OK(txn3->Rollback());

  // Test txn1 and txn2 sharing a lock and txn2 trying to upgrade lock.
  s = txn1->GetForUpdate(read_options, "foo", (std::string*)nullptr,
                         false /* exclusive */);
  ASSERT_OK(s);

  s = txn2->GetForUpdate(read_options, "foo", (std::string*)nullptr,
                         false /* exclusive */);
  ASSERT_OK(s);

  s = txn2->GetForUpdate(read_options, "foo", (std::string*)nullptr);
  ASSERT_TRUE(s.IsTimedOut());
  ASSERT_EQ(s.ToString(), "Operation timed out: Timeout waiting to lock key");

  txn1->UndoGetForUpdate("foo");
  s = txn2->GetForUpdate(read_options, "foo", (std::string*)nullptr);
  ASSERT_OK(s);

  ASSERT_OK(txn1->Rollback());
  ASSERT_OK(txn2->Rollback());

  // Test txn1 trying to downgrade its lock.
  s = txn1->GetForUpdate(read_options, "foo", (std::string*)nullptr,
                         true /* exclusive */);
  ASSERT_OK(s);

  s = txn2->GetForUpdate(read_options, "foo", (std::string*)nullptr,
                         false /* exclusive */);
  ASSERT_TRUE(s.IsTimedOut());
  ASSERT_EQ(s.ToString(), "Operation timed out: Timeout waiting to lock key");

  // Should still fail after "downgrading".
  s = txn1->GetForUpdate(read_options, "foo", (std::string*)nullptr,
                         false /* exclusive */);
  ASSERT_OK(s);

  s = txn2->GetForUpdate(read_options, "foo", (std::string*)nullptr,
                         false /* exclusive */);
  ASSERT_TRUE(s.IsTimedOut());
  ASSERT_EQ(s.ToString(), "Operation timed out: Timeout waiting to lock key");

  ASSERT_OK(txn1->Rollback());
  ASSERT_OK(txn2->Rollback());

  // Test txn1 holding an exclusive lock and txn2 trying to obtain shared
  // access.
  s = txn1->GetForUpdate(read_options, "foo", (std::string*)nullptr);
  ASSERT_OK(s);

  s = txn2->GetForUpdate(read_options, "foo", (std::string*)nullptr,
                         false /* exclusive */);
  ASSERT_TRUE(s.IsTimedOut());
  ASSERT_EQ(s.ToString(), "Operation timed out: Timeout waiting to lock key");

  txn1->UndoGetForUpdate("foo");
  s = txn2->GetForUpdate(read_options, "foo", (std::string*)nullptr,
                         false /* exclusive */);
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
    auto s =
        txns[i]->GetForUpdate(read_options, std::to_string((i + 1) / 2),
                              (std::string*)nullptr, false /* exclusive */);
    ASSERT_OK(s);
  }

  std::atomic<uint32_t> checkpoints(0);
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "PointLockManager::AcquireWithTimeout:WaitingTxn",
      [&](void* /*arg*/) { checkpoints.fetch_add(1); });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  // We want the leaf transactions to block and hold everyone back.
  std::vector<port::Thread> threads;
  for (uint32_t i = 0; i < 15; i++) {
    std::function<void()> blocking_thread = [&, i] {
      auto s =
          txns[i]->GetForUpdate(read_options, std::to_string(i + 1),
                                (std::string*)nullptr, true /* exclusive */);
      ASSERT_OK(s);
      ASSERT_OK(txns[i]->Rollback());
      delete txns[i];
    };
    threads.emplace_back(blocking_thread);
  }

  // Wait until all threads are waiting on each other.
  while (checkpoints.load() != 15) {
    /* sleep override */
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearAllCallBacks();

  // Complete the cycle T[16 - 31] -> T1
  for (uint32_t i = 15; i < 31; i++) {
    auto s = txns[i]->GetForUpdate(read_options, "0", (std::string*)nullptr,
                                   true /* exclusive */);
    ASSERT_TRUE(s.IsDeadlock());

    // Calculate next buffer len, plateau at 5 when 5 records are inserted.
    const uint32_t curr_dlock_buffer_len_ =
        (i - 14 > kInitialMaxDeadlocks) ? kInitialMaxDeadlocks : (i - 14);

    auto dlock_buffer = db->GetDeadlockInfoBuffer();
    ASSERT_EQ(dlock_buffer.size(), curr_dlock_buffer_len_);
    auto dlock_entry = dlock_buffer[0].path;
    ASSERT_EQ(dlock_entry.size(), kInitialMaxDeadlocks);
    int64_t pre_deadlock_time = dlock_buffer[0].deadlock_time;
    int64_t cur_deadlock_time = 0;
    for (auto const& dl_path_rec : dlock_buffer) {
      cur_deadlock_time = dl_path_rec.deadlock_time;
      ASSERT_NE(cur_deadlock_time, 0);
      ASSERT_TRUE(cur_deadlock_time <= pre_deadlock_time);
      pre_deadlock_time = cur_deadlock_time;
    }

    int64_t curr_waiting_key = 0;

    // Offset of each txn id from the root of the shared dlock tree's txn id.
    int64_t offset_root = dlock_entry[0].m_txn_id - 1;
    // Offset of the final entry in the dlock path from the root's txn id.
    TransactionID leaf_id =
        dlock_entry[dlock_entry.size() - 1].m_txn_id - offset_root;

    for (auto it = dlock_entry.rbegin(); it != dlock_entry.rend(); ++it) {
      auto dl_node = *it;
      ASSERT_EQ(dl_node.m_txn_id, offset_root + leaf_id);
      ASSERT_EQ(dl_node.m_cf_id, 0U);
      ASSERT_EQ(dl_node.m_waiting_key, std::to_string(curr_waiting_key));
      ASSERT_EQ(dl_node.m_exclusive, true);

      if (curr_waiting_key == 0) {
        curr_waiting_key = leaf_id;
      }
      curr_waiting_key /= 2;
      leaf_id /= 2;
    }
  }

  // Rollback the leaf transaction.
  for (uint32_t i = 15; i < 31; i++) {
    ASSERT_OK(txns[i]->Rollback());
    delete txns[i];
  }

  for (auto& t : threads) {
    t.join();
  }

  // Downsize the buffer and verify the 3 latest deadlocks are preserved.
  auto dlock_buffer_before_resize = db->GetDeadlockInfoBuffer();
  db->SetDeadlockInfoBufferSize(3);
  auto dlock_buffer_after_resize = db->GetDeadlockInfoBuffer();
  ASSERT_EQ(dlock_buffer_after_resize.size(), 3);

  for (uint32_t i = 0; i < dlock_buffer_after_resize.size(); i++) {
    for (uint32_t j = 0; j < dlock_buffer_after_resize[i].path.size(); j++) {
      ASSERT_EQ(dlock_buffer_after_resize[i].path[j].m_txn_id,
                dlock_buffer_before_resize[i].path[j].m_txn_id);
    }
  }

  // Upsize the buffer and verify the 3 latest dealocks are preserved.
  dlock_buffer_before_resize = db->GetDeadlockInfoBuffer();
  db->SetDeadlockInfoBufferSize(5);
  dlock_buffer_after_resize = db->GetDeadlockInfoBuffer();
  ASSERT_EQ(dlock_buffer_after_resize.size(), 3);

  for (uint32_t i = 0; i < dlock_buffer_before_resize.size(); i++) {
    for (uint32_t j = 0; j < dlock_buffer_before_resize[i].path.size(); j++) {
      ASSERT_EQ(dlock_buffer_after_resize[i].path[j].m_txn_id,
                dlock_buffer_before_resize[i].path[j].m_txn_id);
    }
  }

  // Downsize to 0 and verify the size is consistent.
  dlock_buffer_before_resize = db->GetDeadlockInfoBuffer();
  db->SetDeadlockInfoBufferSize(0);
  dlock_buffer_after_resize = db->GetDeadlockInfoBuffer();
  ASSERT_EQ(dlock_buffer_after_resize.size(), 0);

  // Upsize from 0 to verify the size is persistent.
  dlock_buffer_before_resize = db->GetDeadlockInfoBuffer();
  db->SetDeadlockInfoBufferSize(3);
  dlock_buffer_after_resize = db->GetDeadlockInfoBuffer();
  ASSERT_EQ(dlock_buffer_after_resize.size(), 0);

  // Contrived case of shared lock of cycle size 2 to verify that a shared
  // lock causing a deadlock is correctly reported as "shared" in the buffer.
  std::vector<Transaction*> txns_shared(2);

  // Create a cycle of size 2.
  for (uint32_t i = 0; i < 2; i++) {
    txns_shared[i] = db->BeginTransaction(write_options, txn_options);
    ASSERT_TRUE(txns_shared[i]);
    auto s = txns_shared[i]->GetForUpdate(read_options, std::to_string(i),
                                          (std::string*)nullptr);
    ASSERT_OK(s);
  }

  std::atomic<uint32_t> checkpoints_shared(0);
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "PointLockManager::AcquireWithTimeout:WaitingTxn",
      [&](void* /*arg*/) { checkpoints_shared.fetch_add(1); });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  std::vector<port::Thread> threads_shared;
  for (uint32_t i = 0; i < 1; i++) {
    std::function<void()> blocking_thread = [&, i] {
      auto s = txns_shared[i]->GetForUpdate(read_options, std::to_string(i + 1),
                                            (std::string*)nullptr);
      ASSERT_OK(s);
      ASSERT_OK(txns_shared[i]->Rollback());
      delete txns_shared[i];
    };
    threads_shared.emplace_back(blocking_thread);
  }

  // Wait until all threads are waiting on each other.
  while (checkpoints_shared.load() != 1) {
    /* sleep override */
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearAllCallBacks();

  // Complete the cycle T2 -> T1 with a shared lock.
  auto s = txns_shared[1]->GetForUpdate(read_options, "0",
                                        (std::string*)nullptr, false);
  ASSERT_TRUE(s.IsDeadlock());

  auto dlock_buffer = db->GetDeadlockInfoBuffer();

  // Verify the size of the buffer and the single path.
  ASSERT_EQ(dlock_buffer.size(), 1);
  ASSERT_EQ(dlock_buffer[0].path.size(), 2);

  // Verify the exclusivity field of the transactions in the deadlock path.
  ASSERT_TRUE(dlock_buffer[0].path[0].m_exclusive);
  ASSERT_FALSE(dlock_buffer[0].path[1].m_exclusive);
  ASSERT_OK(txns_shared[1]->Rollback());
  delete txns_shared[1];

  for (auto& t : threads_shared) {
    t.join();
  }
}

#if !defined(ROCKSDB_VALGRIND_RUN) || defined(ROCKSDB_FULL_VALGRIND_RUN)
TEST_P(TransactionStressTest, DeadlockCycle) {
  WriteOptions write_options;
  ReadOptions read_options;
  TransactionOptions txn_options;

  // offset by 2 from the max depth to test edge case
  const uint32_t kMaxCycleLength = 52;

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
      auto s = txns[i]->GetForUpdate(read_options, std::to_string(i),
                                     (std::string*)nullptr);
      ASSERT_OK(s);
    }

    std::atomic<uint32_t> checkpoints(0);
    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
        "PointLockManager::AcquireWithTimeout:WaitingTxn",
        [&](void* /*arg*/) { checkpoints.fetch_add(1); });
    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

    // We want the last transaction in the chain to block and hold everyone
    // back.
    std::vector<port::Thread> threads;
    for (uint32_t i = 0; i + 1 < len; i++) {
      std::function<void()> blocking_thread = [&, i] {
        auto s = txns[i]->GetForUpdate(read_options, std::to_string(i + 1),
                                       (std::string*)nullptr);
        ASSERT_OK(s);
        ASSERT_OK(txns[i]->Rollback());
        delete txns[i];
      };
      threads.emplace_back(blocking_thread);
    }

    // Wait until all threads are waiting on each other.
    while (checkpoints.load() != len - 1) {
      /* sleep override */
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearAllCallBacks();

    // Complete the cycle Tlen -> T1
    auto s =
        txns[len - 1]->GetForUpdate(read_options, "0", (std::string*)nullptr);
    ASSERT_TRUE(s.IsDeadlock());

    const uint32_t dlock_buffer_size_ = (len - 1 > 5) ? 5 : (len - 1);
    uint32_t curr_waiting_key = 0;
    TransactionID curr_txn_id = txns[0]->GetID();

    auto dlock_buffer = db->GetDeadlockInfoBuffer();
    ASSERT_EQ(dlock_buffer.size(), dlock_buffer_size_);
    uint32_t check_len = len;
    bool check_limit_flag = false;

    // Special case for a deadlock path that exceeds the maximum depth.
    if (len > 50) {
      check_len = 0;
      check_limit_flag = true;
    }
    auto dlock_entry = dlock_buffer[0].path;
    ASSERT_EQ(dlock_entry.size(), check_len);
    ASSERT_EQ(dlock_buffer[0].limit_exceeded, check_limit_flag);

    int64_t pre_deadlock_time = dlock_buffer[0].deadlock_time;
    int64_t cur_deadlock_time = 0;
    for (auto const& dl_path_rec : dlock_buffer) {
      cur_deadlock_time = dl_path_rec.deadlock_time;
      ASSERT_NE(cur_deadlock_time, 0);
      ASSERT_TRUE(cur_deadlock_time <= pre_deadlock_time);
      pre_deadlock_time = cur_deadlock_time;
    }

    // Iterates backwards over path verifying decreasing txn_ids.
    for (auto it = dlock_entry.rbegin(); it != dlock_entry.rend(); ++it) {
      auto dl_node = *it;
      ASSERT_EQ(dl_node.m_txn_id, len + curr_txn_id - 1);
      ASSERT_EQ(dl_node.m_cf_id, 0u);
      ASSERT_EQ(dl_node.m_waiting_key, std::to_string(curr_waiting_key));
      ASSERT_EQ(dl_node.m_exclusive, true);

      curr_txn_id--;
      if (curr_waiting_key == 0) {
        curr_waiting_key = len;
      }
      curr_waiting_key--;
    }

    // Rollback the last transaction.
    ASSERT_OK(txns[len - 1]->Rollback());
    delete txns[len - 1];

    for (auto& t : threads) {
      t.join();
    }
  }
}

TEST_P(TransactionStressTest, DeadlockStress) {
  const uint32_t NUM_TXN_THREADS = 10;
  const uint32_t NUM_KEYS = 100;
  const uint32_t NUM_ITERS = 1000;

  WriteOptions write_options;
  ReadOptions read_options;
  TransactionOptions txn_options;

  txn_options.lock_timeout = 1000000;
  txn_options.deadlock_detect = true;
  std::vector<std::string> keys;

  for (uint32_t i = 0; i < NUM_KEYS; i++) {
    ASSERT_OK(db->Put(write_options, Slice(std::to_string(i)), Slice("")));
    keys.push_back(std::to_string(i));
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
        auto s = txn->GetForUpdate(read_options, k, (std::string*)nullptr,
                                   txn->GetID() % 4 == 0);
        if (!s.ok()) {
          ASSERT_TRUE(s.IsDeadlock());
          ASSERT_OK(txn->Rollback());
          break;
        }
      }

      delete txn;
    }
  };

  std::vector<port::Thread> threads;
  for (uint32_t i = 0; i < NUM_TXN_THREADS; i++) {
    threads.emplace_back(stress_thread, rnd.Next());
  }

  for (auto& t : threads) {
    t.join();
  }
}
#endif  // !defined(ROCKSDB_VALGRIND_RUN) || defined(ROCKSDB_FULL_VALGRIND_RUN)

TEST_P(TransactionTest, CommitTimeBatchFailTest) {
  WriteOptions write_options;
  TransactionOptions txn_options;

  std::string value;
  Status s;

  Transaction* txn1 = db->BeginTransaction(write_options, txn_options);
  ASSERT_TRUE(txn1);

  ASSERT_OK(txn1->GetCommitTimeWriteBatch()->Put("cat", "dog"));

  s = txn1->Put("foo", "bar");
  ASSERT_OK(s);

  // fails due to non-empty commit-time batch
  s = txn1->Commit();
  ASSERT_EQ(s, Status::InvalidArgument());

  delete txn1;
}

TEST_P(TransactionTest, LogMarkLeakTest) {
  TransactionOptions txn_options;
  WriteOptions write_options;
  options.write_buffer_size = 1024;
  ASSERT_OK(ReOpenNoDelete());
  assert(db != nullptr);
  Random rnd(47);
  std::vector<Transaction*> txns;
  DBImpl* db_impl = static_cast_with_check<DBImpl>(db->GetRootDB());
  // At the beginning there should be no log containing prepare data
  ASSERT_EQ(db_impl->TEST_FindMinLogContainingOutstandingPrep(), 0);
  for (size_t i = 0; i < 100; i++) {
    Transaction* txn = db->BeginTransaction(write_options, txn_options);
    ASSERT_OK(txn->SetName("xid" + std::to_string(i)));
    ASSERT_OK(txn->Put(Slice("foo" + std::to_string(i)), Slice("bar")));
    ASSERT_OK(txn->Prepare());
    ASSERT_GT(db_impl->TEST_FindMinLogContainingOutstandingPrep(), 0);
    if (rnd.OneIn(5)) {
      txns.push_back(txn);
    } else {
      ASSERT_OK(txn->Commit());
      delete txn;
    }
    ASSERT_OK(db_impl->TEST_FlushMemTable(true));
  }
  for (auto txn : txns) {
    ASSERT_OK(txn->Commit());
    delete txn;
  }
  // At the end there should be no log left containing prepare data
  ASSERT_EQ(db_impl->TEST_FindMinLogContainingOutstandingPrep(), 0);
  // Make sure that the underlying data structures are properly truncated and
  // cause not leak
  ASSERT_EQ(db_impl->TEST_PreparedSectionCompletedSize(), 0);
  ASSERT_EQ(db_impl->TEST_LogsWithPrepSize(), 0);
}

TEST_P(TransactionTest, SimpleTwoPhaseTransactionTest) {
  for (bool cwb4recovery : {true, false}) {
    ASSERT_OK(ReOpen());
    WriteOptions write_options;
    ReadOptions read_options;

    TransactionOptions txn_options;
    txn_options.use_only_the_last_commit_time_batch_for_recovery = cwb4recovery;

    std::string value;
    Status s;

    DBImpl* db_impl = static_cast_with_check<DBImpl>(db->GetRootDB());

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
    ASSERT_OK(db->Get(read_options, "foo2", &value));
    ASSERT_EQ(value, "bar2");

    // commit time put
    if (cwb4recovery) {
      ASSERT_OK(
          txn->GetCommitTimeWriteBatch()->Put(Slice("gtid"), Slice("dogs")));
      ASSERT_OK(
          txn->GetCommitTimeWriteBatch()->Put(Slice("gtid2"), Slice("cats")));
    }

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

    // we already committed
    s = txn->Commit();
    ASSERT_EQ(s, Status::InvalidArgument());

    // no longer is prepared results
    db->GetAllPreparedTransactions(&prepared_trans);
    ASSERT_EQ(prepared_trans.size(), 0);
    ASSERT_EQ(db->GetTransactionByName("xid"), nullptr);

    // heap should not care about prepared section anymore
    ASSERT_EQ(db_impl->TEST_FindMinLogContainingOutstandingPrep(), 0);

    switch (txn_db_options.write_policy) {
      case WRITE_COMMITTED:
        // but now our memtable should be referencing the prep section
        ASSERT_GE(log_containing_prep, db_impl->MinLogNumberToKeep());
        ASSERT_EQ(log_containing_prep,
                  db_impl->TEST_FindMinPrepLogReferencedByMemTable());
        break;
      case WRITE_PREPARED:
      case WRITE_UNPREPARED:
        // In these modes memtable do not ref the prep sections
        ASSERT_EQ(0, db_impl->TEST_FindMinPrepLogReferencedByMemTable());
        break;
      default:
        assert(false);
    }

    ASSERT_OK(db_impl->TEST_FlushMemTable(true));
    // After flush the recoverable state must be visible
    if (cwb4recovery) {
      s = db->Get(read_options, "gtid", &value);
      ASSERT_OK(s);
      ASSERT_EQ(value, "dogs");

      s = db->Get(read_options, "gtid2", &value);
      ASSERT_OK(s);
      ASSERT_EQ(value, "cats");
    }

    // after memtable flush we can now relese the log
    ASSERT_GT(db_impl->MinLogNumberToKeep(), log_containing_prep);
    ASSERT_EQ(0, db_impl->TEST_FindMinPrepLogReferencedByMemTable());

    delete txn;

    if (cwb4recovery) {
      // kill and reopen to trigger recovery
      s = ReOpenNoDelete();
      ASSERT_OK(s);
      assert(db != nullptr);
      s = db->Get(read_options, "gtid", &value);
      ASSERT_OK(s);
      ASSERT_EQ(value, "dogs");

      s = db->Get(read_options, "gtid2", &value);
      ASSERT_OK(s);
      ASSERT_EQ(value, "cats");
    }
  }
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

  ASSERT_OK(txn1->Rollback());
  ASSERT_OK(txn2->Rollback());
  delete txn1;
  delete txn2;
}

TEST_P(TransactionTest, TwoPhaseEmptyWriteTest) {
  for (bool cwb4recovery : {true, false}) {
    for (bool test_with_empty_wal : {true, false}) {
      if (!cwb4recovery && test_with_empty_wal) {
        continue;
      }
      ASSERT_OK(ReOpen());
      Status s;
      std::string value;

      WriteOptions write_options;
      ReadOptions read_options;
      TransactionOptions txn_options;
      txn_options.use_only_the_last_commit_time_batch_for_recovery =
          cwb4recovery;
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

      if (cwb4recovery) {
        ASSERT_OK(
            txn2->GetCommitTimeWriteBatch()->Put(Slice("foo"), Slice("bar")));
      }

      s = txn2->Prepare();
      ASSERT_OK(s);

      s = txn2->Commit();
      ASSERT_OK(s);

      delete txn2;
      if (cwb4recovery) {
        if (test_with_empty_wal) {
          DBImpl* db_impl = static_cast_with_check<DBImpl>(db->GetRootDB());
          ASSERT_OK(db_impl->TEST_FlushMemTable(true));
          // After flush the state must be visible
          s = db->Get(read_options, "foo", &value);
          ASSERT_OK(s);
          ASSERT_EQ(value, "bar");
        }
        ASSERT_OK(db->FlushWAL(true));
        // kill and reopen to trigger recovery
        s = ReOpenNoDelete();
        ASSERT_OK(s);
        assert(db != nullptr);
        s = db->Get(read_options, "foo", &value);
        ASSERT_OK(s);
        ASSERT_EQ(value, "bar");
      }
    }
  }
}

#if !defined(ROCKSDB_VALGRIND_RUN) || defined(ROCKSDB_FULL_VALGRIND_RUN)
TEST_P(TransactionStressTest, TwoPhaseExpirationTest) {
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

  std::string value;
  Status s;

  DBImpl* db_impl = static_cast_with_check<DBImpl>(db->GetRootDB());
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
  ASSERT_OK(db_impl->TEST_FlushMemTable(true));

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

  std::string value;
  Status s;

  DBImpl* db_impl = static_cast_with_check<DBImpl>(db->GetRootDB());

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

  ASSERT_OK(db_impl->TEST_FlushMemTable(true));

  // regular db read
  ASSERT_OK(db->Get(read_options, "foo2", &value));
  ASSERT_EQ(value, "bar2");

  // nothing has been prepped yet
  ASSERT_EQ(db_impl->TEST_FindMinLogContainingOutstandingPrep(), 0);

  // prepare
  s = txn->Prepare();
  ASSERT_OK(s);

  // still not available to db
  s = db->Get(read_options, Slice("foo"), &value);
  ASSERT_TRUE(s.IsNotFound());

  ASSERT_OK(db->FlushWAL(false));
  delete txn;
  // kill and reopen
  static_cast<PessimisticTransactionDB*>(db)->TEST_Crash();
  s = ReOpenNoDelete();
  ASSERT_OK(s);
  assert(db != nullptr);
  db_impl = static_cast_with_check<DBImpl>(db->GetRootDB());

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
  ASSERT_OK(db->Get(read_options, "foo", &value));
  ASSERT_EQ(value, "bar");

  // we already committed
  s = txn->Commit();
  ASSERT_EQ(s, Status::InvalidArgument());

  // no longer is prepared results
  prepared_trans.clear();
  db->GetAllPreparedTransactions(&prepared_trans);
  ASSERT_EQ(prepared_trans.size(), 0);

  // transaction should no longer be visible
  ASSERT_EQ(db->GetTransactionByName("xid"), nullptr);

  // heap should not care about prepared section anymore
  ASSERT_EQ(db_impl->TEST_FindMinLogContainingOutstandingPrep(), 0);

  switch (txn_db_options.write_policy) {
    case WRITE_COMMITTED:
      // but now our memtable should be referencing the prep section
      ASSERT_EQ(log_containing_prep,
                db_impl->TEST_FindMinPrepLogReferencedByMemTable());
      ASSERT_GE(log_containing_prep, db_impl->MinLogNumberToKeep());

      break;
    case WRITE_PREPARED:
    case WRITE_UNPREPARED:
      // In these modes memtable do not ref the prep sections
      ASSERT_EQ(0, db_impl->TEST_FindMinPrepLogReferencedByMemTable());
      break;
    default:
      assert(false);
  }

  // Add a dummy record to memtable before a flush. Otherwise, the
  // memtable will be empty and flush will be skipped.
  s = db->Put(write_options, Slice("foo3"), Slice("bar3"));
  ASSERT_OK(s);

  ASSERT_OK(db_impl->TEST_FlushMemTable(true));

  // after memtable flush we can now release the log
  ASSERT_GT(db_impl->MinLogNumberToKeep(), log_containing_prep);
  ASSERT_EQ(0, db_impl->TEST_FindMinPrepLogReferencedByMemTable());

  delete txn;

  // deleting transaction should unregister transaction
  ASSERT_EQ(db->GetTransactionByName("xid"), nullptr);
}
#endif  // !defined(ROCKSDB_VALGRIND_RUN) || defined(ROCKSDB_FULL_VALGRIND_RUN)

// TODO this test needs to be updated with serial commits
TEST_P(TransactionTest, DISABLED_TwoPhaseMultiThreadTest) {
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
    TransactionName name("xid_" + std::string(1, 'A' + static_cast<char>(id)));
    Transaction* txn = db->BeginTransaction(write_options, txn_options);
    ASSERT_OK(txn->SetName(name));
    for (int i = 0; i < 10; i++) {
      std::string key(name + "_" + std::string(1, static_cast<char>('A' + i)));
      ASSERT_OK(txn->Put(key, "val"));
    }
    ASSERT_OK(txn->Prepare());
    ASSERT_OK(txn->Commit());
    delete txn;
  };

  // assure that all thread are in the same write group
  std::atomic<uint32_t> t_wait_on_prepare(0);
  std::atomic<uint32_t> t_wait_on_commit(0);

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "WriteThread::JoinBatchGroup:Wait", [&](void* arg) {
        auto* writer = static_cast<WriteThread::Writer*>(arg);

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
          FAIL();
        }
      });

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  // do all the writes
  std::vector<port::Thread> threads;
  for (uint32_t i = 0; i < NUM_TXN_THREADS; i++) {
    threads.emplace_back(txn_write_thread);
  }
  for (auto& t : threads) {
    t.join();
  }

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearAllCallBacks();

  ReadOptions read_options;
  std::string value;
  Status s;
  for (uint32_t t = 0; t < NUM_TXN_THREADS; t++) {
    TransactionName name("xid_" + std::string(1, 'A' + static_cast<char>(t)));
    for (int i = 0; i < 10; i++) {
      std::string key(name + "_" + std::string(1, static_cast<char>('A' + i)));
      s = db->Get(read_options, key, &value);
      ASSERT_OK(s);
      ASSERT_EQ(value, "val");
    }
  }
}

#if !defined(ROCKSDB_VALGRIND_RUN) || defined(ROCKSDB_FULL_VALGRIND_RUN)
TEST_P(TransactionStressTest, TwoPhaseLongPrepareTest) {
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
    assert(db != nullptr);
    s = db->Put(write_options, key, val);
    ASSERT_OK(s);

    if (i % 29 == 0) {
      // crash
      fault_fs->SetFilesystemActive(false);
      static_cast<PessimisticTransactionDB*>(db)->TEST_Crash();
      ASSERT_OK(ReOpenNoDelete());
    } else if (i % 37 == 0) {
      // close
      ASSERT_OK(ReOpenNoDelete());
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
  fault_fs->SetFilesystemActive(false);
  ASSERT_OK(ReOpenNoDelete());
  assert(db != nullptr);

  // value is now available
  s = db->Get(read_options, "foo4", &value);
  ASSERT_EQ(s, Status::OK());
  ASSERT_EQ(value, "bar4");
}
#endif  // !defined(ROCKSDB_VALGRIND_RUN) || defined(ROCKSDB_FULL_VALGRIND_RUN)

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
  fault_fs->SetFilesystemActive(false);
  static_cast<PessimisticTransactionDB*>(db)->TEST_Crash();
  ASSERT_OK(ReOpenNoDelete());

  // commit old txn
  assert(db != nullptr);  // Make clang analyze happy.
  txn = db->GetTransactionByName("a");
  assert(txn != nullptr);
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
  fault_fs->SetFilesystemActive(false);
  ASSERT_OK(ReOpenNoDelete());
  assert(db != nullptr);

  // value is now available
  s = db->Get(read_options, "foo", &value);
  ASSERT_EQ(s, Status::OK());
  ASSERT_EQ(value, "bar");

  s = db->Get(read_options, "foo2", &value);
  ASSERT_EQ(s, Status::OK());
  ASSERT_EQ(value, "bar2");
}

TEST_P(TransactionTest, TwoPhaseLogRollingTest) {
  DBImpl* db_impl = static_cast_with_check<DBImpl>(db->GetRootDB());

  Status s;
  std::string v;
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
  ASSERT_EQ(db_impl->TEST_LogfileNumber(), txn1->GetLastLogNumber());

  // flush default cf to crate new log
  s = db->Put(wopts, "foo", "bar");
  ASSERT_OK(s);
  s = db_impl->TEST_FlushMemTable(true);
  ASSERT_OK(s);

  // make sure we are on a new log
  ASSERT_GT(db_impl->TEST_LogfileNumber(), txn1->GetLastLogNumber());

  // put txn2 prep section in this log
  s = txn2->Prepare();
  ASSERT_OK(s);
  ASSERT_EQ(db_impl->TEST_LogfileNumber(), txn2->GetLastLogNumber());

  // heap should still see first log
  ASSERT_EQ(db_impl->TEST_FindMinLogContainingOutstandingPrep(),
            txn1->GetLogNumber());

  // commit txn1
  s = txn1->Commit();
  ASSERT_OK(s);

  // heap should now show txn2s log
  ASSERT_EQ(db_impl->TEST_FindMinLogContainingOutstandingPrep(),
            txn2->GetLogNumber());

  switch (txn_db_options.write_policy) {
    case WRITE_COMMITTED:
      // we should see txn1s log refernced by the memtables
      ASSERT_EQ(txn1->GetLogNumber(),
                db_impl->TEST_FindMinPrepLogReferencedByMemTable());
      break;
    case WRITE_PREPARED:
    case WRITE_UNPREPARED:
      // In these modes memtable do not ref the prep sections
      ASSERT_EQ(0, db_impl->TEST_FindMinPrepLogReferencedByMemTable());
      break;
    default:
      assert(false);
  }

  // flush default cf to crate new log
  s = db->Put(wopts, "foo", "bar2");
  ASSERT_OK(s);
  s = db_impl->TEST_FlushMemTable(true);
  ASSERT_OK(s);

  // make sure we are on a new log
  ASSERT_GT(db_impl->TEST_LogfileNumber(), txn2->GetLastLogNumber());

  // commit txn2
  s = txn2->Commit();
  ASSERT_OK(s);

  // heap should not show any logs
  ASSERT_EQ(db_impl->TEST_FindMinLogContainingOutstandingPrep(), 0);

  switch (txn_db_options.write_policy) {
    case WRITE_COMMITTED:
      // should show the first txn log
      ASSERT_EQ(txn1->GetLogNumber(),
                db_impl->TEST_FindMinPrepLogReferencedByMemTable());
      break;
    case WRITE_PREPARED:
    case WRITE_UNPREPARED:
      // In these modes memtable do not ref the prep sections
      ASSERT_EQ(0, db_impl->TEST_FindMinPrepLogReferencedByMemTable());
      break;
    default:
      assert(false);
  }

  // flush only cfa memtable
  s = db_impl->TEST_FlushMemTable(true, false, cfa);
  ASSERT_OK(s);

  switch (txn_db_options.write_policy) {
    case WRITE_COMMITTED:
      // should show the first txn log
      ASSERT_EQ(txn2->GetLogNumber(),
                db_impl->TEST_FindMinPrepLogReferencedByMemTable());
      break;
    case WRITE_PREPARED:
    case WRITE_UNPREPARED:
      // In these modes memtable do not ref the prep sections
      ASSERT_EQ(0, db_impl->TEST_FindMinPrepLogReferencedByMemTable());
      break;
    default:
      assert(false);
  }

  // flush only cfb memtable
  s = db_impl->TEST_FlushMemTable(true, false, cfb);
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
  DBImpl* db_impl = static_cast_with_check<DBImpl>(db->GetRootDB());

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

  auto cfh_a = static_cast_with_check<ColumnFamilyHandleImpl>(cfa);
  auto cfh_b = static_cast_with_check<ColumnFamilyHandleImpl>(cfb);

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

  auto prepare_log_no = txn1->GetLastLogNumber();

  // roll to LOG B
  s = db_impl->TEST_FlushMemTable(true);
  ASSERT_OK(s);

  // now we pause background work so that
  // imm()s are not flushed before we can check their status
  s = db_impl->PauseBackgroundWork();
  ASSERT_OK(s);

  ASSERT_GT(db_impl->TEST_LogfileNumber(), prepare_log_no);
  switch (txn_db_options.write_policy) {
    case WRITE_COMMITTED:
      // This cf is empty and should ref the latest log
      ASSERT_GT(cfh_a->cfd()->GetLogNumber(), prepare_log_no);
      ASSERT_EQ(cfh_a->cfd()->GetLogNumber(), db_impl->TEST_LogfileNumber());
      break;
    case WRITE_PREPARED:
    case WRITE_UNPREPARED:
      // This cf is not flushed yet and should ref the log that has its data
      ASSERT_EQ(cfh_a->cfd()->GetLogNumber(), prepare_log_no);
      break;
    default:
      assert(false);
  }
  ASSERT_EQ(db_impl->TEST_FindMinLogContainingOutstandingPrep(),
            txn1->GetLogNumber());
  ASSERT_EQ(db_impl->TEST_FindMinPrepLogReferencedByMemTable(), 0);

  // commit in LOG B
  s = txn1->Commit();
  ASSERT_OK(s);

  switch (txn_db_options.write_policy) {
    case WRITE_COMMITTED:
      ASSERT_EQ(db_impl->TEST_FindMinPrepLogReferencedByMemTable(),
                prepare_log_no);
      break;
    case WRITE_PREPARED:
    case WRITE_UNPREPARED:
      // In these modes memtable do not ref the prep sections
      ASSERT_EQ(db_impl->TEST_FindMinPrepLogReferencedByMemTable(), 0);
      break;
    default:
      assert(false);
  }

  ASSERT_TRUE(!db_impl->TEST_UnableToReleaseOldestLog());

  // request a flush for all column families such that the earliest
  // alive log file can be killed
  ASSERT_OK(db_impl->TEST_SwitchWAL());
  // log cannot be flushed because txn2 has not been commited
  ASSERT_TRUE(!db_impl->TEST_IsLogGettingFlushed());
  ASSERT_TRUE(db_impl->TEST_UnableToReleaseOldestLog());

  // assert that cfa has a flush requested
  ASSERT_TRUE(cfh_a->cfd()->imm()->HasFlushRequested());

  switch (txn_db_options.write_policy) {
    case WRITE_COMMITTED:
      // cfb should not be flushed becuse it has no data from LOG A
      ASSERT_TRUE(!cfh_b->cfd()->imm()->HasFlushRequested());
      break;
    case WRITE_PREPARED:
    case WRITE_UNPREPARED:
      // cfb should be flushed becuse it has prepared data from LOG A
      ASSERT_TRUE(cfh_b->cfd()->imm()->HasFlushRequested());
      break;
    default:
      assert(false);
  }

  // cfb now has data from LOG A
  s = txn2->Commit();
  ASSERT_OK(s);

  ASSERT_OK(db_impl->TEST_SwitchWAL());
  ASSERT_TRUE(!db_impl->TEST_UnableToReleaseOldestLog());

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
  // WAL recycling incompatible with disableWAL (below)
  options.recycle_log_file_num = 0;
  ASSERT_OK(ReOpenNoDelete());

  DBImpl* db_impl = static_cast_with_check<DBImpl>(db->GetRootDB());
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

  ASSERT_OK(db->FlushWAL(false));

  // kill and reopen
  fault_fs->SetFilesystemActive(false);
  static_cast<PessimisticTransactionDB*>(db)->TEST_Crash();
  ASSERT_OK(ReOpenNoDelete());
  assert(db != nullptr);

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
  std::string value;
  Status s;

  ASSERT_OK(db->Put(write_options, "foo", "A"));
  ASSERT_OK(db->Put(write_options, "foo2", "B"));

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

  ASSERT_OK(db->Get(read_options, "foo", &value));
  ASSERT_EQ(value, "A2");
  ASSERT_OK(db->Get(read_options, "foo2", &value));
  ASSERT_EQ(value, "B2");

  delete txn;
}

TEST_P(TransactionTest, WriteConflictTest2) {
  WriteOptions write_options;
  ReadOptions read_options;
  TransactionOptions txn_options;
  std::string value;
  Status s;

  ASSERT_OK(db->Put(write_options, "foo", "bar"));

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
  ASSERT_OK(db->Get(read_options, "foo", &value));
  ASSERT_EQ(value, "barz");

  ASSERT_OK(db->Get(read_options, "foo2", &value));
  ASSERT_EQ(value, "X");

  ASSERT_OK(db->Get(read_options, "foo3", &value));
  ASSERT_EQ(value, "Y");

  delete txn;
}

TEST_P(TransactionTest, ReadConflictTest) {
  WriteOptions write_options;
  ReadOptions read_options, snapshot_read_options;
  TransactionOptions txn_options;
  std::string value;
  Status s;

  ASSERT_OK(db->Put(write_options, "foo", "bar"));
  ASSERT_OK(db->Put(write_options, "foo2", "bar"));

  txn_options.set_snapshot = true;
  Transaction* txn = db->BeginTransaction(write_options, txn_options);
  ASSERT_TRUE(txn);

  txn->SetSnapshot();
  snapshot_read_options.snapshot = txn->GetSnapshot();

  ASSERT_OK(txn->GetForUpdate(snapshot_read_options, "foo", &value));
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
  std::string value;
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
  std::string value;
  Status s;

  ASSERT_OK(db->Put(write_options, Slice("foo"), Slice("bar")));
  ASSERT_OK(db->Put(write_options, Slice("foo2"), Slice("bar")));

  Transaction* txn = db->BeginTransaction(write_options);
  ASSERT_TRUE(txn);

  snapshot_read_options.snapshot = txn->GetSnapshot();

  ASSERT_OK(txn->GetForUpdate(snapshot_read_options, "foo", &value));
  ASSERT_EQ(value, "bar");

  s = txn->Put(Slice("foo"), Slice("bar2"));
  ASSERT_OK(s);

  ASSERT_OK(txn->GetForUpdate(snapshot_read_options, "foo", &value));
  ASSERT_EQ(value, "bar2");

  // Put a random key so we have a memtable to flush
  s = db->Put(write_options, "dummy", "dummy");
  ASSERT_OK(s);

  // force a memtable flush
  FlushOptions flush_ops;
  ASSERT_OK(db->Flush(flush_ops));

  s = txn->Commit();
  // txn should commit since the flushed table is still in MemtableList History
  ASSERT_OK(s);

  ASSERT_OK(db->Get(read_options, "foo", &value));
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
    assert(db != nullptr);

    WriteOptions write_options;
    ReadOptions read_options, snapshot_read_options;
    TransactionOptions txn_options;
    std::string value;

    DBImpl* db_impl = static_cast_with_check<DBImpl>(db->GetRootDB());

    ASSERT_OK(db->Put(write_options, Slice("foo"), Slice("bar")));
    ASSERT_OK(db->Put(write_options, Slice("foo2"), Slice("bar2")));
    ASSERT_OK(db->Put(write_options, Slice("foo3"), Slice("bar3")));

    txn_options.set_snapshot = true;
    Transaction* txn = db->BeginTransaction(write_options, txn_options);
    ASSERT_TRUE(txn);

    snapshot_read_options.snapshot = txn->GetSnapshot();

    ASSERT_OK(txn->GetForUpdate(snapshot_read_options, "foo", &value));
    ASSERT_EQ(value, "bar");

    s = txn->Put(Slice("foo"), Slice("bar2"));
    ASSERT_OK(s);

    ASSERT_OK(txn->GetForUpdate(snapshot_read_options, "foo", &value));
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

    ASSERT_OK(db_impl->TEST_WaitForCompact());

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

TEST_P(TransactionTest, WaitForCompactAbortOnPause) {
  Status s = ReOpen();
  ASSERT_OK(s);
  assert(db != nullptr);

  DBImpl* db_impl = static_cast_with_check<DBImpl>(db->GetRootDB());

  // Pause the background jobs.
  ASSERT_OK(db_impl->PauseBackgroundWork());

  WaitForCompactOptions waitForCompactOptions = WaitForCompactOptions();
  waitForCompactOptions.abort_on_pause = true;
  s = db->WaitForCompact(waitForCompactOptions);
  ASSERT_NOK(s);
  ASSERT_FALSE(s.IsNotSupported());
  ASSERT_TRUE(s.IsAborted());
}

TEST_P(TransactionTest, NoSnapshotTest) {
  WriteOptions write_options;
  ReadOptions read_options;
  std::string value;
  Status s;

  ASSERT_OK(db->Put(write_options, "AAA", "bar"));

  Transaction* txn = db->BeginTransaction(write_options);
  ASSERT_TRUE(txn);

  // Modify key after transaction start
  ASSERT_OK(db->Put(write_options, "AAA", "bar1"));

  // Read and write without a snap
  ASSERT_OK(txn->GetForUpdate(read_options, "AAA", &value));
  ASSERT_EQ(value, "bar1");
  s = txn->Put("AAA", "bar2");
  ASSERT_OK(s);

  // Should commit since read/write was done after data changed
  s = txn->Commit();
  ASSERT_OK(s);

  ASSERT_OK(txn->GetForUpdate(read_options, "AAA", &value));
  ASSERT_EQ(value, "bar2");

  delete txn;
}

TEST_P(TransactionTest, MultipleSnapshotTest) {
  WriteOptions write_options;
  ReadOptions read_options, snapshot_read_options;
  std::string value;
  Status s;

  ASSERT_OK(db->Put(write_options, "AAA", "bar"));
  ASSERT_OK(db->Put(write_options, "BBB", "bar"));
  ASSERT_OK(db->Put(write_options, "CCC", "bar"));

  Transaction* txn = db->BeginTransaction(write_options);
  ASSERT_TRUE(txn);

  ASSERT_OK(db->Put(write_options, "AAA", "bar1"));

  // Read and write without a snapshot
  ASSERT_OK(txn->GetForUpdate(read_options, "AAA", &value));
  ASSERT_EQ(value, "bar1");
  s = txn->Put("AAA", "bar2");
  ASSERT_OK(s);

  // Modify BBB before snapshot is taken
  ASSERT_OK(db->Put(write_options, "BBB", "bar1"));

  txn->SetSnapshot();
  snapshot_read_options.snapshot = txn->GetSnapshot();

  // Read and write with snapshot
  ASSERT_OK(txn->GetForUpdate(snapshot_read_options, "BBB", &value));
  ASSERT_EQ(value, "bar1");
  s = txn->Put("BBB", "bar2");
  ASSERT_OK(s);

  ASSERT_OK(db->Put(write_options, "CCC", "bar1"));

  // Set a new snapshot
  txn->SetSnapshot();
  snapshot_read_options.snapshot = txn->GetSnapshot();

  // Read and write with snapshot
  ASSERT_OK(txn->GetForUpdate(snapshot_read_options, "CCC", &value));
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
  ASSERT_OK(db->Put(write_options, "ZZZ", "zzz"));
  ASSERT_OK(db->Put(write_options, "XXX", "xxx"));

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
  std::string value;
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
  db = nullptr;

  // open DB with three column families
  std::vector<ColumnFamilyDescriptor> column_families;
  // have to open default column family
  column_families.emplace_back(kDefaultColumnFamilyName, ColumnFamilyOptions());
  // open the new column families
  column_families.emplace_back("CFA", ColumnFamilyOptions());
  column_families.emplace_back("CFB", ColumnFamilyOptions());

  std::vector<ColumnFamilyHandle*> handles;

  ASSERT_OK(ReOpenNoDelete(column_families, &handles));
  assert(db != nullptr);

  Transaction* txn = db->BeginTransaction(write_options);
  ASSERT_TRUE(txn);

  txn->SetSnapshot();
  snapshot_read_options.snapshot = txn->GetSnapshot();

  txn_options.set_snapshot = true;
  Transaction* txn2 = db->BeginTransaction(write_options, txn_options);
  ASSERT_TRUE(txn2);

  // Write some data to the db
  WriteBatch batch;
  ASSERT_OK(batch.Put("foo", "foo"));
  ASSERT_OK(batch.Put(handles[1], "AAA", "bar"));
  ASSERT_OK(batch.Put(handles[1], "AAAZZZ", "bar"));
  s = db->Write(write_options, &batch);
  ASSERT_OK(s);
  ASSERT_OK(db->Delete(write_options, handles[1], "AAAZZZ"));

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
  ASSERT_OK(db->Put(write_options, handles[2], "foo", "000"));

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

TEST_P(TransactionTest, WriteImportedColumnFamilyTest) {
  WriteOptions write_options;
  ReadOptions read_options;
  ColumnFamilyOptions cf_options;
  ImportColumnFamilyOptions import_options;
  ExportImportFilesMetaData* metadata_ptr = nullptr;
  ColumnFamilyHandle* import_cf = nullptr;
  ColumnFamilyHandle* export_cf = nullptr;
  std::string export_files_dir = test::PerThreadDBPath(env.get(), "cf_export");
  std::string value;
  Status s;

  {
    // Create a column family to export
    s = db->CreateColumnFamily(cf_options, "CF_EXPORT", &export_cf);
    ASSERT_OK(s);

    // Write some data to the db
    WriteBatch batch;
    ASSERT_OK(batch.Put(export_cf, "K1", "V1"));
    ASSERT_OK(batch.Put(export_cf, "K2", "V2"));
    s = db->Write(write_options, &batch);
    ASSERT_OK(s);

    Checkpoint* checkpoint = nullptr;
    s = Checkpoint::Create(db, &checkpoint);
    ASSERT_OK(s);
    s = checkpoint->ExportColumnFamily(export_cf, export_files_dir,
                                       &metadata_ptr);
    ASSERT_OK(s);
    ASSERT_NE(metadata_ptr, nullptr);
    delete checkpoint;

    s = db->DropColumnFamily(export_cf);
    ASSERT_OK(s);
    delete export_cf;
  }

  {
    // Create a new column family with import
    s = db->CreateColumnFamilyWithImport(
        cf_options, "CF_IMPORT", import_options, *metadata_ptr, &import_cf);
    ASSERT_OK(s);
    s = db->Get(read_options, import_cf, "K1", &value);
    ASSERT_OK(s);
    ASSERT_EQ(value, "V1");
    s = db->Get(read_options, import_cf, "K2", &value);
    ASSERT_OK(s);
    ASSERT_EQ(value, "V2");

    // Wirte a new key-value pair
    Transaction* txn = db->BeginTransaction(write_options);
    ASSERT_TRUE(txn);
    s = txn->Put(import_cf, "K3", "V3");
    ASSERT_OK(s);
    s = txn->Commit();
    ASSERT_OK(s);
    delete txn;

    s = db->Get(read_options, import_cf, "K3", &value);
    ASSERT_OK(s);
    ASSERT_EQ(value, "V3");

    s = db->DropColumnFamily(import_cf);
    ASSERT_OK(s);
    delete import_cf;
  }

  delete metadata_ptr;

  EXPECT_OK(DestroyDir(env.get(), export_files_dir));
}

TEST_P(TransactionTest, MultiGetBatchedTest) {
  WriteOptions write_options;
  ReadOptions read_options, snapshot_read_options;
  TransactionOptions txn_options;
  std::string value;
  Status s;

  ColumnFamilyHandle* cf;
  ColumnFamilyOptions cf_options;

  // Create a new column families
  s = db->CreateColumnFamily(cf_options, "CF", &cf);
  ASSERT_OK(s);

  delete cf;
  delete db;
  db = nullptr;

  // open DB with three column families
  std::vector<ColumnFamilyDescriptor> column_families;
  // have to open default column family
  column_families.emplace_back(kDefaultColumnFamilyName, ColumnFamilyOptions());
  // open the new column families
  cf_options.merge_operator = MergeOperators::CreateStringAppendOperator();
  column_families.emplace_back("CF", cf_options);

  std::vector<ColumnFamilyHandle*> handles;

  options.merge_operator = MergeOperators::CreateStringAppendOperator();
  ASSERT_OK(ReOpenNoDelete(column_families, &handles));
  assert(db != nullptr);

  // Write some data to the db
  WriteBatch batch;
  ASSERT_OK(batch.Put(handles[1], "aaa", "val1"));
  ASSERT_OK(batch.Put(handles[1], "bbb", "val2"));
  ASSERT_OK(batch.Put(handles[1], "ccc", "val3"));
  ASSERT_OK(batch.Put(handles[1], "ddd", "foo"));
  ASSERT_OK(batch.Put(handles[1], "eee", "val5"));
  ASSERT_OK(batch.Put(handles[1], "fff", "val6"));
  ASSERT_OK(batch.Merge(handles[1], "ggg", "foo"));
  s = db->Write(write_options, &batch);
  ASSERT_OK(s);

  Transaction* txn = db->BeginTransaction(write_options);
  ASSERT_TRUE(txn);

  txn->SetSnapshot();
  snapshot_read_options.snapshot = txn->GetSnapshot();

  txn_options.set_snapshot = true;
  // Write some data to the db
  s = txn->Delete(handles[1], "bbb");
  ASSERT_OK(s);
  s = txn->Put(handles[1], "ccc", "val3_new");
  ASSERT_OK(s);
  s = txn->Merge(handles[1], "ddd", "bar");
  ASSERT_OK(s);

  std::vector<Slice> keys = {"aaa", "bbb", "ccc", "ddd", "eee", "fff", "ggg"};
  std::vector<PinnableSlice> values(keys.size());
  std::vector<Status> statuses(keys.size());

  txn->MultiGet(snapshot_read_options, handles[1], keys.size(), keys.data(),
                values.data(), statuses.data());
  ASSERT_TRUE(statuses[0].ok());
  ASSERT_EQ(values[0], "val1");
  ASSERT_TRUE(statuses[1].IsNotFound());
  ASSERT_TRUE(statuses[2].ok());
  ASSERT_EQ(values[2], "val3_new");
  ASSERT_TRUE(statuses[3].ok());
  ASSERT_EQ(values[3], "foo,bar");
  ASSERT_TRUE(statuses[4].ok());
  ASSERT_EQ(values[4], "val5");
  ASSERT_TRUE(statuses[5].ok());
  ASSERT_EQ(values[5], "val6");
  ASSERT_TRUE(statuses[6].ok());
  ASSERT_EQ(values[6], "foo");
  delete txn;
  for (auto handle : handles) {
    delete handle;
  }
}

// This test calls WriteBatchWithIndex::MultiGetFromBatchAndDB with a large
// number of keys, i.e greater than MultiGetContext::MAX_BATCH_SIZE, which is
// is 32. This forces autovector allocations in the MultiGet code paths
// to use std::vector in addition to stack allocations. The MultiGet keys
// includes Merges, which are handled specially in MultiGetFromBatchAndDB by
// allocating an autovector of MergeContexts
TEST_P(TransactionTest, MultiGetLargeBatchedTest) {
  WriteOptions write_options;
  ReadOptions read_options, snapshot_read_options;
  std::string value;
  Status s;

  ColumnFamilyHandle* cf;
  ColumnFamilyOptions cf_options;

  std::vector<std::string> key_str;
  for (int i = 0; i < 100; ++i) {
    key_str.emplace_back(std::to_string(i));
  }
  // Create a new column families
  s = db->CreateColumnFamily(cf_options, "CF", &cf);
  ASSERT_OK(s);

  delete cf;
  delete db;
  db = nullptr;

  // open DB with three column families
  std::vector<ColumnFamilyDescriptor> column_families;
  // have to open default column family
  column_families.emplace_back(kDefaultColumnFamilyName, ColumnFamilyOptions());
  // open the new column families
  cf_options.merge_operator = MergeOperators::CreateStringAppendOperator();
  column_families.emplace_back("CF", cf_options);

  std::vector<ColumnFamilyHandle*> handles;

  options.merge_operator = MergeOperators::CreateStringAppendOperator();
  ASSERT_OK(ReOpenNoDelete(column_families, &handles));
  assert(db != nullptr);

  // Write some data to the db
  WriteBatch batch;
  for (int i = 0; i < 3 * MultiGetContext::MAX_BATCH_SIZE; ++i) {
    std::string val = "val" + std::to_string(i);
    ASSERT_OK(batch.Put(handles[1], key_str[i], val));
  }
  s = db->Write(write_options, &batch);
  ASSERT_OK(s);

  WriteBatchWithIndex wb;
  // Write some data to the db
  s = wb.Delete(handles[1], std::to_string(1));
  ASSERT_OK(s);
  s = wb.Put(handles[1], std::to_string(2), "new_val" + std::to_string(2));
  ASSERT_OK(s);
  // Write a lot of merges so when we call MultiGetFromBatchAndDB later on,
  // it is forced to use std::vector in ROCKSDB_NAMESPACE::autovector to
  // allocate MergeContexts. The number of merges needs to be >
  // MultiGetContext::MAX_BATCH_SIZE
  for (int i = 8; i < MultiGetContext::MAX_BATCH_SIZE + 24; ++i) {
    s = wb.Merge(handles[1], std::to_string(i), "merge");
    ASSERT_OK(s);
  }

  // MultiGet a lot of keys in order to force std::vector reallocations
  std::vector<Slice> keys;
  for (int i = 0; i < MultiGetContext::MAX_BATCH_SIZE + 32; ++i) {
    keys.emplace_back(key_str[i]);
  }
  std::vector<PinnableSlice> values(keys.size());
  std::vector<Status> statuses(keys.size());

  wb.MultiGetFromBatchAndDB(db, snapshot_read_options, handles[1], keys.size(),
                            keys.data(), values.data(), statuses.data(), false);
  for (size_t i = 0; i < keys.size(); ++i) {
    if (i == 1) {
      ASSERT_TRUE(statuses[1].IsNotFound());
    } else if (i == 2) {
      ASSERT_TRUE(statuses[2].ok());
      ASSERT_EQ(values[2], "new_val" + std::to_string(2));
    } else if (i >= 8 && i < 56) {
      ASSERT_TRUE(statuses[i].ok());
      ASSERT_EQ(values[i], "val" + std::to_string(i) + ",merge");
    } else {
      ASSERT_TRUE(statuses[i].ok());
      if (values[i] != "val" + std::to_string(i)) {
        ASSERT_EQ(values[i], "val" + std::to_string(i));
      }
    }
  }

  for (auto handle : handles) {
    delete handle;
  }
}

TEST_P(TransactionTest, MultiGetSnapshot) {
  WriteOptions write_options;
  TransactionOptions transaction_options;
  Transaction* txn1 = db->BeginTransaction(write_options, transaction_options);

  Slice key = "foo";

  Status s = txn1->Put(key, "bar");
  ASSERT_OK(s);

  s = txn1->SetName("test");
  ASSERT_OK(s);

  s = txn1->Prepare();
  ASSERT_OK(s);

  // Get snapshot between prepare and commit
  // Un-committed data should be invisible to other transactions
  const Snapshot* s1 = db->GetSnapshot();

  s = txn1->Commit();
  ASSERT_OK(s);
  delete txn1;

  Transaction* txn2 = db->BeginTransaction(write_options, transaction_options);
  ReadOptions read_options;
  read_options.snapshot = s1;

  std::vector<Slice> keys;
  std::vector<PinnableSlice> values(1);
  std::vector<Status> statuses(1);
  keys.push_back(key);
  auto cfd = db->DefaultColumnFamily();
  txn2->MultiGet(read_options, cfd, 1, keys.data(), values.data(),
                 statuses.data());
  ASSERT_TRUE(statuses[0].IsNotFound());
  delete txn2;

  db->ReleaseSnapshot(s1);
}

TEST_P(TransactionTest, ColumnFamiliesTest2) {
  WriteOptions write_options;
  ReadOptions read_options, snapshot_read_options;
  std::string value;
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
  std::string value;
  Status s;

  s = db->Put(write_options, "aaa", "aaa");
  ASSERT_OK(s);

  Transaction* txn = db->BeginTransaction(write_options);
  s = txn->Commit();
  ASSERT_OK(s);
  delete txn;

  txn = db->BeginTransaction(write_options);
  ASSERT_OK(txn->Rollback());
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
  std::string value;
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
  ASSERT_EQ(results.size(), 3);
  ASSERT_TRUE(results[0].IsNotFound());
  ASSERT_TRUE(results[1].IsNotFound());
  ASSERT_TRUE(results[2].IsNotFound());

  s = txn2->Put("2", "x");  // Conflict's with txn1's MultiGetForUpdate
  ASSERT_TRUE(s.IsTimedOut());

  ASSERT_OK(txn2->Rollback());

  multiget_values.clear();
  results =
      txn1->MultiGetForUpdate(read_options1, multiget_keys, &multiget_values);
  ASSERT_EQ(results.size(), 3);
  ASSERT_TRUE(results[0].IsNotFound());
  ASSERT_TRUE(results[1].IsNotFound());
  ASSERT_TRUE(results[2].IsNotFound());

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

  ASSERT_OK(txn2->Rollback());

  delete txn1;
  delete txn2;
}

TEST_P(TransactionTest, LostUpdate) {
  WriteOptions write_options;
  ReadOptions read_options, read_options1, read_options2;
  TransactionOptions txn_options;
  std::string value;
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
  if (txn_db_options.write_policy == WRITE_UNPREPARED) {
    // TODO(lth): For WriteUnprepared, validate that untracked writes are
    // not supported.
    return;
  }

  WriteOptions write_options;
  ReadOptions read_options;
  std::string value;
  Status s;

  // Verify transaction rollback works for untracked keys.
  Transaction* txn = db->BeginTransaction(write_options);
  txn->SetSnapshot();

  s = txn->PutUntracked("untracked", "0");
  ASSERT_OK(s);
  ASSERT_OK(txn->Rollback());
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
  s = txn->PutEntityUntracked(db->DefaultColumnFamily(), "untracked",
                              {{"hello", "world"}});
  ASSERT_OK(s);
  s = txn->MergeUntracked("untracked", "2");
  ASSERT_OK(s);
  s = txn->DeleteUntracked("untracked");
  ASSERT_OK(s);

  // Conflict
  s = txn->Put("untracked", "3");
  ASSERT_TRUE(s.IsBusy());
  s = txn->PutEntity(db->DefaultColumnFamily(), "untracked", {{"foo", "bar"}});
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
  std::string value;
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
  std::string value;
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

  ASSERT_OK(txn1->Rollback());

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
  std::string value;
  Status s;

  Transaction* txn1 = db->BeginTransaction(write_options, txn_options);

  ASSERT_OK(s);

  s = txn1->Put("X", "1");
  ASSERT_OK(s);

  Transaction* txn2 = db->BeginTransaction(write_options);

  // txn2 should not be able to write to X since txn1 has it locked
  s = txn2->Put("X", "2");
  ASSERT_TRUE(s.IsTimedOut());

  ASSERT_OK(txn1->Rollback());
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
  std::string value;
  Status s;

  delete db;
  db = nullptr;

  // Open DB with a lock limit of 3
  txn_db_options.max_num_locks = 3;
  ASSERT_OK(ReOpen());
  assert(db != nullptr);
  ASSERT_OK(s);

  // Create a txn and verify we can only lock up to 3 keys
  Transaction* txn = db->BeginTransaction(write_options, txn_options);
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

  Transaction* txn2 = db->BeginTransaction(write_options, txn_options);
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
  // This test does writes without snapshot validation, and then tries to create
  // iterator later, which is unsupported in write unprepared.
  if (txn_db_options.write_policy == WRITE_UNPREPARED) {
    return;
  }

  WriteOptions write_options;
  ReadOptions read_options, snapshot_read_options;
  std::string value;
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

    s = txn->GetForUpdate(read_options, iter->key(), (std::string*)nullptr);
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
  // Skip this test for write unprepared. It does not solely rely on WBWI for
  // read your own writes, so depending on whether batches are flushed or not,
  // only some writes will be visible.
  //
  // Also, write unprepared does not support creating iterators if there has
  // been txn->Put() without snapshot validation.
  if (txn_db_options.write_policy == WRITE_UNPREPARED) {
    return;
  }

  WriteOptions write_options;
  ReadOptions read_options;
  std::string value;
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
  std::string value;
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
  ASSERT_OK(txn->Rollback());

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

TEST_P(TransactionTest, SavepointTest3) {
  WriteOptions write_options;
  ReadOptions read_options;
  TransactionOptions txn_options;
  Status s;

  txn_options.lock_timeout = 1;  // 1 ms
  Transaction* txn1 = db->BeginTransaction(write_options, txn_options);
  ASSERT_TRUE(txn1);

  s = txn1->PopSavePoint();  // No SavePoint present
  ASSERT_TRUE(s.IsNotFound());

  s = txn1->Put("A", "");
  ASSERT_OK(s);

  s = txn1->PopSavePoint();  // Still no SavePoint present
  ASSERT_TRUE(s.IsNotFound());

  txn1->SetSavePoint();  // 1

  s = txn1->Put("A", "a");
  ASSERT_OK(s);

  s = txn1->PopSavePoint();  // Remove 1
  ASSERT_TRUE(txn1->RollbackToSavePoint().IsNotFound());

  // Verify that "A" is still locked
  Transaction* txn2 = db->BeginTransaction(write_options, txn_options);
  ASSERT_TRUE(txn2);

  s = txn2->Put("A", "a2");
  ASSERT_TRUE(s.IsTimedOut());
  delete txn2;

  txn1->SetSavePoint();  // 2

  s = txn1->Put("B", "b");
  ASSERT_OK(s);

  txn1->SetSavePoint();  // 3

  s = txn1->Put("B", "b2");
  ASSERT_OK(s);

  ASSERT_OK(txn1->RollbackToSavePoint());  // Roll back to 2

  s = txn1->PopSavePoint();
  ASSERT_OK(s);

  s = txn1->PopSavePoint();
  ASSERT_TRUE(s.IsNotFound());

  s = txn1->Commit();
  ASSERT_OK(s);
  delete txn1;

  std::string value;

  // tnx1 should have modified "A" to "a"
  s = db->Get(read_options, "A", &value);
  ASSERT_OK(s);
  ASSERT_EQ("a", value);

  // tnx1 should have set "B" to just "b"
  s = db->Get(read_options, "B", &value);
  ASSERT_OK(s);
  ASSERT_EQ("b", value);

  s = db->Get(read_options, "C", &value);
  ASSERT_TRUE(s.IsNotFound());
}

TEST_P(TransactionTest, SavepointTest4) {
  WriteOptions write_options;
  ReadOptions read_options;
  TransactionOptions txn_options;
  Status s;

  txn_options.lock_timeout = 1;  // 1 ms
  Transaction* txn1 = db->BeginTransaction(write_options, txn_options);
  ASSERT_TRUE(txn1);

  txn1->SetSavePoint();  // 1
  s = txn1->Put("A", "a");
  ASSERT_OK(s);

  txn1->SetSavePoint();  // 2
  s = txn1->Put("B", "b");
  ASSERT_OK(s);

  s = txn1->PopSavePoint();  // Remove 2
  ASSERT_OK(s);

  // Verify that A/B still exists.
  std::string value;
  ASSERT_OK(txn1->Get(read_options, "A", &value));
  ASSERT_EQ("a", value);

  ASSERT_OK(txn1->Get(read_options, "B", &value));
  ASSERT_EQ("b", value);

  ASSERT_OK(txn1->RollbackToSavePoint());  // Rollback to 1

  // Verify that everything was rolled back.
  s = txn1->Get(read_options, "A", &value);
  ASSERT_TRUE(s.IsNotFound());

  s = txn1->Get(read_options, "B", &value);
  ASSERT_TRUE(s.IsNotFound());

  // Nothing should be locked
  Transaction* txn2 = db->BeginTransaction(write_options, txn_options);
  ASSERT_TRUE(txn2);

  s = txn2->Put("A", "");
  ASSERT_OK(s);

  s = txn2->Put("B", "");
  ASSERT_OK(s);

  delete txn2;
  delete txn1;
}

TEST_P(TransactionTest, UndoGetForUpdateTest) {
  WriteOptions write_options;
  ReadOptions read_options;
  TransactionOptions txn_options;
  std::string value;
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
  ASSERT_OK(txn2->Commit());
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

  ASSERT_OK(txn1->Rollback());
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
  std::string value;
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

  ASSERT_OK(txn1->RollbackToSavePoint());  // rollback to 2

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

  ASSERT_OK(txn1->RollbackToSavePoint());  // rollback to 1

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
  std::string value;
  Status s;

  delete db;
  db = nullptr;

  // transaction writes have an infinite timeout,
  // but we will override this when we start a txn
  // db writes have infinite timeout
  txn_db_options.transaction_lock_timeout = -1;
  txn_db_options.default_lock_timeout = -1;

  s = TransactionDB::Open(options, txn_db_options, dbname, &db);
  assert(db != nullptr);
  ASSERT_OK(s);

  s = db->Put(write_options, "aaa", "aaa");
  ASSERT_OK(s);

  TransactionOptions txn_options0;
  txn_options0.expiration = 100;   // 100ms
  txn_options0.lock_timeout = 50;  // txn timeout no longer infinite
  Transaction* txn1 = db->BeginTransaction(write_options, txn_options0);

  s = txn1->GetForUpdate(read_options, "aaa", (std::string*)nullptr);
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

  s = txn1->GetForUpdate(read_options, "aaa", (std::string*)nullptr);
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
  std::string value;
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
  std::string value;
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
  ASSERT_OK(s);
  ASSERT_EQ("a0,1,2", value);

  s = txn->Put("A", "a");
  ASSERT_OK(s);

  s = txn->Get(read_options, "A", &value);
  ASSERT_OK(s);
  ASSERT_EQ("a", value);

  s = txn->Merge("A", "3");
  ASSERT_OK(s);

  s = txn->Get(read_options, "A", &value);
  ASSERT_OK(s);
  ASSERT_EQ("a,3", value);

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

TEST_P(TransactionTest, DeleteRangeSupportTest) {
  // The `DeleteRange()` API is banned everywhere.
  ASSERT_TRUE(
      db->DeleteRange(WriteOptions(), db->DefaultColumnFamily(), "a", "b")
          .IsNotSupported());

  // But range deletions can be added via the `Write()` API by specifying the
  // proper flags to promise there are no conflicts according to the DB type
  // (see `TransactionDB::DeleteRange()` API doc for details).
  for (bool skip_concurrency_control : {false, true}) {
    for (bool skip_duplicate_key_check : {false, true}) {
      ASSERT_OK(db->Put(WriteOptions(), "a", "val"));
      WriteBatch wb;
      ASSERT_OK(wb.DeleteRange("a", "b"));
      TransactionDBWriteOptimizations flags;
      flags.skip_concurrency_control = skip_concurrency_control;
      flags.skip_duplicate_key_check = skip_duplicate_key_check;
      Status s = db->Write(WriteOptions(), flags, &wb);
      std::string value;
      switch (txn_db_options.write_policy) {
        case WRITE_COMMITTED:
          if (skip_concurrency_control) {
            ASSERT_OK(s);
            ASSERT_TRUE(db->Get(ReadOptions(), "a", &value).IsNotFound());
          } else {
            ASSERT_NOK(s);
            ASSERT_OK(db->Get(ReadOptions(), "a", &value));
          }
          break;
        case WRITE_PREPARED:
          FALLTHROUGH_INTENDED;
        case WRITE_UNPREPARED:
          if (skip_concurrency_control && skip_duplicate_key_check) {
            ASSERT_OK(s);
            ASSERT_TRUE(db->Get(ReadOptions(), "a", &value).IsNotFound());
          } else {
            ASSERT_NOK(s);
            ASSERT_OK(db->Get(ReadOptions(), "a", &value));
          }
          break;
      }
      // Without any promises from the user, range deletion via other `Write()`
      // APIs are still banned.
      ASSERT_OK(db->Put(WriteOptions(), "a", "val"));
      ASSERT_NOK(db->Write(WriteOptions(), &wb));
      ASSERT_OK(db->Get(ReadOptions(), "a", &value));
    }
  }
}

TEST_P(TransactionTest, DeferSnapshotTest) {
  WriteOptions write_options;
  ReadOptions read_options;
  std::string value;
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
  std::string value;
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
  std::string value;
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
  std::string value;

  class Notifier : public TransactionNotifier {
   private:
    const Snapshot** snapshot_ptr_;

   public:
    explicit Notifier(const Snapshot** snapshot_ptr)
        : snapshot_ptr_(snapshot_ptr) {}

    void SnapshotCreated(const Snapshot* newSnapshot) override {
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
  std::string value;
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
  column_families.emplace_back(kDefaultColumnFamilyName, ColumnFamilyOptions());
  // open the new column families
  column_families.emplace_back("CFA", ColumnFamilyOptions());
  column_families.emplace_back("CFB", ColumnFamilyOptions());

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

  auto cfh_default = static_cast_with_check<ColumnFamilyHandleImpl>(handles[0]);
  auto opt_default = *cfh_default->cfd()->GetLatestMutableCFOptions();

  auto cfh_a = static_cast_with_check<ColumnFamilyHandleImpl>(handles[1]);
  auto opt_a = *cfh_a->cfd()->GetLatestMutableCFOptions();

  auto cfh_b = static_cast_with_check<ColumnFamilyHandleImpl>(handles[2]);
  auto opt_b = *cfh_b->cfd()->GetLatestMutableCFOptions();

  ASSERT_EQ(opt_default.disable_auto_compactions, false);
  ASSERT_EQ(opt_a.disable_auto_compactions, true);
  ASSERT_EQ(opt_b.disable_auto_compactions, false);

  for (auto handle : handles) {
    delete handle;
  }
}

TEST_P(TransactionStressTest, ExpiredTransactionDataRace1) {
  // In this test, txn1 should succeed committing,
  // as the callback is called after txn1 starts committing.
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency(
      {{"TransactionTest::ExpirableTransactionDataRace:1"}});
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "TransactionTest::ExpirableTransactionDataRace:1", [&](void* /*arg*/) {
        WriteOptions write_options;
        TransactionOptions txn_options;

        // Force txn1 to expire
        /* sleep override */
        std::this_thread::sleep_for(std::chrono::milliseconds(1500));

        Transaction* txn2 = db->BeginTransaction(write_options, txn_options);
        Status s;
        s = txn2->Put("X", "2");
        ASSERT_TRUE(s.IsTimedOut());
        s = txn2->Commit();
        ASSERT_OK(s);
        delete txn2;
      });

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  WriteOptions write_options;
  TransactionOptions txn_options;

  txn_options.expiration = 1000;  // 1 second
  Transaction* txn1 = db->BeginTransaction(write_options, txn_options);

  Status s;
  s = txn1->Put("X", "1");
  ASSERT_OK(s);
  s = txn1->Commit();
  ASSERT_OK(s);

  ReadOptions read_options;
  std::string value;
  s = db->Get(read_options, "X", &value);
  ASSERT_OK(s);
  ASSERT_EQ("1", value);

  delete txn1;
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
}

#if !defined(ROCKSDB_VALGRIND_RUN) || defined(ROCKSDB_FULL_VALGRIND_RUN)
namespace {
// cmt_delay_ms is the delay between prepare and commit
// first_id is the id of the first transaction
Status TransactionStressTestInserter(
    TransactionDB* db, const size_t num_transactions, const size_t num_sets,
    const size_t num_keys_per_set, Random64* rand,
    const uint64_t cmt_delay_ms = 0, const uint64_t first_id = 0) {
  WriteOptions write_options;
  ReadOptions read_options;
  TransactionOptions txn_options;
  txn_options.use_only_the_last_commit_time_batch_for_recovery = true;

  // Inside the inserter we might also retake the snapshot. We do both since two
  // separte functions are engaged for each.
  txn_options.set_snapshot = rand->OneIn(2);

  RandomTransactionInserter inserter(
      rand, write_options, read_options, num_keys_per_set,
      static_cast<uint16_t>(num_sets), cmt_delay_ms, first_id);

  for (size_t t = 0; t < num_transactions; t++) {
    bool success = inserter.TransactionDBInsert(db, txn_options);
    if (!success) {
      // unexpected failure
      return inserter.GetLastStatus();
    }
  }
  inserter.GetLastStatus().PermitUncheckedError();

  // Make sure at least some of the transactions succeeded.  It's ok if
  // some failed due to write-conflicts.
  if (num_transactions != 1 &&
      inserter.GetFailureCount() > num_transactions / 2) {
    return Status::TryAgain("Too many transactions failed! " +
                            std::to_string(inserter.GetFailureCount()) + " / " +
                            std::to_string(num_transactions));
  }

  return Status::OK();
}
}  // namespace

// Worker threads add a number to a key from each set of keys. The checker
// threads verify that the sum of all keys in each set are equal.
TEST_P(MySQLStyleTransactionTest, TransactionStressTest) {
  // Small write buffer to trigger more compactions
  options.write_buffer_size = 1024;
  txn_db_options.rollback_deletion_type_callback =
      [](TransactionDB*, ColumnFamilyHandle*, const Slice& key) {
        return RandomTransactionInserter::RollbackDeletionTypeCallback(key);
      };
  ASSERT_OK(ReOpenNoDelete());
  constexpr size_t num_workers = 4;        // worker threads count
  constexpr size_t num_checkers = 2;       // checker threads count
  constexpr size_t num_slow_checkers = 2;  // checker threads emulating backups
  constexpr size_t num_slow_workers = 1;   // slow worker threads count
  constexpr size_t num_transactions_per_thread = 1000;
  constexpr uint16_t num_sets = 3;
  constexpr size_t num_keys_per_set = 100;
  // Setting the key-space to be 100 keys should cause enough write-conflicts
  // to make this test interesting.

  std::vector<port::Thread> threads;
  std::atomic<uint32_t> finished = {0};
  constexpr bool TAKE_SNAPSHOT = true;
  uint64_t time_seed = env->NowMicros();
  printf("time_seed is %" PRIu64 "\n", time_seed);  // would help to reproduce

  std::function<void()> call_inserter = [&] {
    size_t thd_seed = std::hash<std::thread::id>()(std::this_thread::get_id());
    Random64 rand(time_seed * thd_seed);
    ASSERT_OK(TransactionStressTestInserter(db, num_transactions_per_thread,
                                            num_sets, num_keys_per_set, &rand));
    finished++;
  };
  std::function<void()> call_checker = [&] {
    size_t thd_seed = std::hash<std::thread::id>()(std::this_thread::get_id());
    Random64 rand(time_seed * thd_seed);
    // Verify that data is consistent
    while (finished < num_workers) {
      ASSERT_OK(RandomTransactionInserter::Verify(
          db, num_sets, num_keys_per_set, TAKE_SNAPSHOT, &rand));
    }
  };
  std::function<void()> call_slow_checker = [&] {
    size_t thd_seed = std::hash<std::thread::id>()(std::this_thread::get_id());
    Random64 rand(time_seed * thd_seed);
    // Verify that data is consistent
    while (finished < num_workers) {
      uint64_t delay_ms = rand.Uniform(100) + 1;
      Status s = RandomTransactionInserter::Verify(
          db, num_sets, num_keys_per_set, TAKE_SNAPSHOT, &rand, delay_ms);
      ASSERT_OK(s);
    }
  };
  std::function<void()> call_slow_inserter = [&] {
    size_t thd_seed = std::hash<std::thread::id>()(std::this_thread::get_id());
    Random64 rand(time_seed * thd_seed);
    uint64_t id = 0;
    // Verify that data is consistent
    while (finished < num_workers) {
      uint64_t delay_ms = rand.Uniform(500) + 1;
      ASSERT_OK(TransactionStressTestInserter(db, 1, num_sets, num_keys_per_set,
                                              &rand, delay_ms, id++));
    }
  };

  for (uint32_t i = 0; i < num_workers; i++) {
    threads.emplace_back(call_inserter);
  }
  for (uint32_t i = 0; i < num_checkers; i++) {
    threads.emplace_back(call_checker);
  }
  if (with_slow_threads_) {
    for (uint32_t i = 0; i < num_slow_checkers; i++) {
      threads.emplace_back(call_slow_checker);
    }
    for (uint32_t i = 0; i < num_slow_workers; i++) {
      threads.emplace_back(call_slow_inserter);
    }
  }

  // Wait for all threads to finish
  for (auto& t : threads) {
    t.join();
  }

  // Verify that data is consistent
  Status s = RandomTransactionInserter::Verify(db, num_sets, num_keys_per_set,
                                               !TAKE_SNAPSHOT);
  ASSERT_OK(s);
}
#endif  // !defined(ROCKSDB_VALGRIND_RUN) || defined(ROCKSDB_FULL_VALGRIND_RUN)

TEST_P(TransactionTest, MemoryLimitTest) {
  TransactionOptions txn_options;
  // Header (12 bytes) + NOOP (1 byte) + 2 * 8 bytes for data.
  txn_options.max_write_batch_size = 29;
  // Set threshold to unlimited so that the write batch does not get flushed,
  // and can hit the memory limit.
  txn_options.write_batch_flush_threshold = 0;
  std::string value;
  Status s;

  Transaction* txn = db->BeginTransaction(WriteOptions(), txn_options);
  ASSERT_TRUE(txn);

  ASSERT_EQ(0, txn->GetNumPuts());
  ASSERT_LE(0, txn->GetID());

  s = txn->Put(Slice("a"), Slice("...."));
  ASSERT_OK(s);
  ASSERT_EQ(1, txn->GetNumPuts());

  s = txn->Put(Slice("b"), Slice("...."));
  ASSERT_OK(s);
  ASSERT_EQ(2, txn->GetNumPuts());

  s = txn->Put(Slice("b"), Slice("...."));
  ASSERT_TRUE(s.IsMemoryLimit());
  ASSERT_EQ(2, txn->GetNumPuts());

  ASSERT_OK(txn->Rollback());
  delete txn;
}

#if !defined(ROCKSDB_VALGRIND_RUN) || defined(ROCKSDB_FULL_VALGRIND_RUN)
// This test clarifies the existing expectation from the sequence number
// algorithm. It could detect mistakes in updating the code but it is not
// necessarily the one acceptable way. If the algorithm is legitimately changed,
// this unit test should be updated as well.
TEST_P(TransactionStressTest, SeqAdvanceTest) {
  // TODO(myabandeh): must be test with false before new releases
  const bool short_test = true;
  WriteOptions wopts;
  FlushOptions fopt;

  options.disable_auto_compactions = true;
  ASSERT_OK(ReOpen());

  // Do the test with NUM_BRANCHES branches in it. Each run of a test takes some
  // of the branches. This is the same as counting a binary number where i-th
  // bit represents whether we take branch i in the represented by the number.
  const size_t NUM_BRANCHES = short_test ? 6 : 10;
  // Helper function that shows if the branch is to be taken in the run
  // represented by the number n.
  auto branch_do = [&](size_t n, size_t* branch) {
    assert(*branch < NUM_BRANCHES);
    const size_t filter = static_cast<size_t>(1) << *branch;
    return n & filter;
  };
  const size_t max_n = static_cast<size_t>(1) << NUM_BRANCHES;
  for (size_t n = 0; n < max_n; n++) {
    DBImpl* db_impl = static_cast_with_check<DBImpl>(db->GetRootDB());
    size_t branch = 0;
    auto seq = db_impl->GetLatestSequenceNumber();
    exp_seq = seq;
    TestTxn0(0);
    seq = db_impl->TEST_GetLastVisibleSequence();
    ASSERT_EQ(exp_seq, seq);

    if (branch_do(n, &branch)) {
      ASSERT_OK(db_impl->Flush(fopt));
      seq = db_impl->TEST_GetLastVisibleSequence();
      ASSERT_EQ(exp_seq, seq);
    }
    if (!short_test && branch_do(n, &branch)) {
      ASSERT_OK(db_impl->FlushWAL(true));
      ASSERT_OK(ReOpenNoDelete());
      db_impl = static_cast_with_check<DBImpl>(db->GetRootDB());
      seq = db_impl->GetLatestSequenceNumber();
      ASSERT_EQ(exp_seq, seq);
    }

    // Doing it twice might detect some bugs
    TestTxn0(1);
    seq = db_impl->TEST_GetLastVisibleSequence();
    ASSERT_EQ(exp_seq, seq);

    TestTxn1(0);
    seq = db_impl->TEST_GetLastVisibleSequence();
    ASSERT_EQ(exp_seq, seq);

    if (branch_do(n, &branch)) {
      ASSERT_OK(db_impl->Flush(fopt));
      seq = db_impl->TEST_GetLastVisibleSequence();
      ASSERT_EQ(exp_seq, seq);
    }
    if (!short_test && branch_do(n, &branch)) {
      ASSERT_OK(db_impl->FlushWAL(true));
      ASSERT_OK(ReOpenNoDelete());
      db_impl = static_cast_with_check<DBImpl>(db->GetRootDB());
      seq = db_impl->GetLatestSequenceNumber();
      ASSERT_EQ(exp_seq, seq);
    }

    TestTxn3(0);
    seq = db_impl->TEST_GetLastVisibleSequence();
    ASSERT_EQ(exp_seq, seq);

    if (branch_do(n, &branch)) {
      ASSERT_OK(db_impl->Flush(fopt));
      seq = db_impl->TEST_GetLastVisibleSequence();
      ASSERT_EQ(exp_seq, seq);
    }
    if (!short_test && branch_do(n, &branch)) {
      ASSERT_OK(db_impl->FlushWAL(true));
      ASSERT_OK(ReOpenNoDelete());
      db_impl = static_cast_with_check<DBImpl>(db->GetRootDB());
      seq = db_impl->GetLatestSequenceNumber();
      ASSERT_EQ(exp_seq, seq);
    }

    TestTxn4(0);
    seq = db_impl->TEST_GetLastVisibleSequence();

    ASSERT_EQ(exp_seq, seq);

    if (branch_do(n, &branch)) {
      ASSERT_OK(db_impl->Flush(fopt));
      seq = db_impl->TEST_GetLastVisibleSequence();
      ASSERT_EQ(exp_seq, seq);
    }
    if (!short_test && branch_do(n, &branch)) {
      ASSERT_OK(db_impl->FlushWAL(true));
      ASSERT_OK(ReOpenNoDelete());
      db_impl = static_cast_with_check<DBImpl>(db->GetRootDB());
      seq = db_impl->GetLatestSequenceNumber();
      ASSERT_EQ(exp_seq, seq);
    }

    TestTxn2(0);
    seq = db_impl->TEST_GetLastVisibleSequence();
    ASSERT_EQ(exp_seq, seq);

    if (branch_do(n, &branch)) {
      ASSERT_OK(db_impl->Flush(fopt));
      seq = db_impl->TEST_GetLastVisibleSequence();
      ASSERT_EQ(exp_seq, seq);
    }
    if (!short_test && branch_do(n, &branch)) {
      ASSERT_OK(db_impl->FlushWAL(true));
      ASSERT_OK(ReOpenNoDelete());
      db_impl = static_cast_with_check<DBImpl>(db->GetRootDB());
      seq = db_impl->GetLatestSequenceNumber();
      ASSERT_EQ(exp_seq, seq);
    }
    ASSERT_OK(ReOpen());
  }
}
#endif  // !defined(ROCKSDB_VALGRIND_RUN) || defined(ROCKSDB_FULL_VALGRIND_RUN)

// Verify that the optimization would not compromize the correctness
TEST_P(TransactionTest, Optimizations) {
  size_t comb_cnt = size_t(1) << 2;  // 2 is number of optimization vars
  for (size_t new_comb = 0; new_comb < comb_cnt; new_comb++) {
    TransactionDBWriteOptimizations optimizations;
    optimizations.skip_concurrency_control = IsInCombination(0, new_comb);
    optimizations.skip_duplicate_key_check = IsInCombination(1, new_comb);

    ASSERT_OK(ReOpen());
    WriteOptions write_options;
    WriteBatch batch;
    ASSERT_OK(batch.Put(Slice("k"), Slice("v1")));
    ASSERT_OK(db->Write(write_options, &batch));

    ReadOptions ropt;
    PinnableSlice pinnable_val;
    ASSERT_OK(db->Get(ropt, db->DefaultColumnFamily(), "k", &pinnable_val));
    ASSERT_TRUE(pinnable_val == ("v1"));
  }
}

// A comparator that uses only the first three bytes
class ThreeBytewiseComparator : public Comparator {
 public:
  ThreeBytewiseComparator() = default;
  const char* Name() const override { return "test.ThreeBytewiseComparator"; }
  int Compare(const Slice& a, const Slice& b) const override {
    Slice na = Slice(a.data(), a.size() < 3 ? a.size() : 3);
    Slice nb = Slice(b.data(), b.size() < 3 ? b.size() : 3);
    return na.compare(nb);
  }
  bool Equal(const Slice& a, const Slice& b) const override {
    Slice na = Slice(a.data(), a.size() < 3 ? a.size() : 3);
    Slice nb = Slice(b.data(), b.size() < 3 ? b.size() : 3);
    return na == nb;
  }
  // These methods below don't seem relevant to this test. Implement them if
  // proven othersize.
  void FindShortestSeparator(std::string* start,
                             const Slice& limit) const override {
    const Comparator* bytewise_comp = BytewiseComparator();
    bytewise_comp->FindShortestSeparator(start, limit);
  }
  void FindShortSuccessor(std::string* key) const override {
    const Comparator* bytewise_comp = BytewiseComparator();
    bytewise_comp->FindShortSuccessor(key);
  }
};

#if !defined(ROCKSDB_VALGRIND_RUN) || defined(ROCKSDB_FULL_VALGRIND_RUN)
TEST_P(TransactionTest, GetWithoutSnapshot) {
  WriteOptions write_options;
  std::atomic<bool> finish = {false};
  ASSERT_OK(db->Put(write_options, "key", "value"));
  ROCKSDB_NAMESPACE::port::Thread commit_thread([&]() {
    for (int i = 0; i < 100; i++) {
      TransactionOptions txn_options;
      Transaction* txn = db->BeginTransaction(write_options, txn_options);
      ASSERT_OK(txn->SetName("xid"));
      ASSERT_OK(txn->Put("key", "overridedvalue"));
      ASSERT_OK(txn->Put("key", "value"));
      ASSERT_OK(txn->Prepare());
      ASSERT_OK(txn->Commit());
      delete txn;
    }
    finish = true;
  });
  ROCKSDB_NAMESPACE::port::Thread read_thread([&]() {
    while (!finish) {
      ReadOptions ropt;
      PinnableSlice pinnable_val;
      ASSERT_OK(db->Get(ropt, db->DefaultColumnFamily(), "key", &pinnable_val));
      ASSERT_TRUE(pinnable_val == ("value"));
    }
  });
  commit_thread.join();
  read_thread.join();
}
#endif  // !defined(ROCKSDB_VALGRIND_RUN) || defined(ROCKSDB_FULL_VALGRIND_RUN)

// Test that the transactional db can handle duplicate keys in the write batch
TEST_P(TransactionTest, DuplicateKeys) {
  ColumnFamilyOptions cf_options;
  std::string cf_name = "two";
  ColumnFamilyHandle* cf_handle = nullptr;
  {
    ASSERT_OK(db->CreateColumnFamily(cf_options, cf_name, &cf_handle));
    WriteOptions write_options;
    WriteBatch batch;
    ASSERT_OK(batch.Put(Slice("key"), Slice("value")));
    ASSERT_OK(batch.Put(Slice("key2"), Slice("value2")));
    // duplicate the keys
    ASSERT_OK(batch.Put(Slice("key"), Slice("value3")));
    // duplicate the 2nd key. It should not be counted duplicate since a
    // sub-patch is cut after the last duplicate.
    ASSERT_OK(batch.Put(Slice("key2"), Slice("value4")));
    // duplicate the keys but in a different cf. It should not be counted as
    // duplicate keys
    ASSERT_OK(batch.Put(cf_handle, Slice("key"), Slice("value5")));

    ASSERT_OK(db->Write(write_options, &batch));

    ReadOptions ropt;
    PinnableSlice pinnable_val;
    auto s = db->Get(ropt, db->DefaultColumnFamily(), "key", &pinnable_val);
    ASSERT_OK(s);
    ASSERT_TRUE(pinnable_val == ("value3"));
    s = db->Get(ropt, db->DefaultColumnFamily(), "key2", &pinnable_val);
    ASSERT_OK(s);
    ASSERT_TRUE(pinnable_val == ("value4"));
    s = db->Get(ropt, cf_handle, "key", &pinnable_val);
    ASSERT_OK(s);
    ASSERT_TRUE(pinnable_val == ("value5"));

    delete cf_handle;
  }

  // Test with non-bytewise comparator
  {
    ASSERT_OK(ReOpen());
    std::unique_ptr<const Comparator> comp_gc(new ThreeBytewiseComparator());
    cf_options.comparator = comp_gc.get();
    ASSERT_OK(db->CreateColumnFamily(cf_options, cf_name, &cf_handle));
    WriteOptions write_options;
    WriteBatch batch;
    ASSERT_OK(batch.Put(cf_handle, Slice("key"), Slice("value")));
    // The first three bytes are the same, do it must be counted as duplicate
    ASSERT_OK(batch.Put(cf_handle, Slice("key2"), Slice("value2")));
    // check for 2nd duplicate key in cf with non-default comparator
    ASSERT_OK(batch.Put(cf_handle, Slice("key2b"), Slice("value2b")));
    ASSERT_OK(db->Write(write_options, &batch));

    // The value must be the most recent value for all the keys equal to "key",
    // including "key2"
    ReadOptions ropt;
    PinnableSlice pinnable_val;
    ASSERT_OK(db->Get(ropt, cf_handle, "key", &pinnable_val));
    ASSERT_TRUE(pinnable_val == ("value2b"));

    // Test duplicate keys with rollback
    TransactionOptions txn_options;
    Transaction* txn0 = db->BeginTransaction(write_options, txn_options);
    ASSERT_OK(txn0->SetName("xid"));
    ASSERT_OK(txn0->Put(cf_handle, Slice("key3"), Slice("value3")));
    ASSERT_OK(txn0->Merge(cf_handle, Slice("key4"), Slice("value4")));
    ASSERT_OK(txn0->Rollback());
    ASSERT_OK(db->Get(ropt, cf_handle, "key5", &pinnable_val));
    ASSERT_TRUE(pinnable_val == ("value2b"));
    delete txn0;

    delete cf_handle;
    cf_options.comparator = BytewiseComparator();
  }

  for (bool do_prepare : {true, false}) {
    for (bool do_rollback : {true, false}) {
      for (bool with_commit_batch : {true, false}) {
        if (with_commit_batch && !do_prepare) {
          continue;
        }
        if (with_commit_batch && do_rollback) {
          continue;
        }
        ASSERT_OK(ReOpen());
        ASSERT_OK(db->CreateColumnFamily(cf_options, cf_name, &cf_handle));
        TransactionOptions txn_options;
        txn_options.use_only_the_last_commit_time_batch_for_recovery = true;
        WriteOptions write_options;
        Transaction* txn0 = db->BeginTransaction(write_options, txn_options);
        auto s = txn0->SetName("xid");
        ASSERT_OK(s);
        s = txn0->Put(Slice("foo0"), Slice("bar0a"));
        ASSERT_OK(s);
        s = txn0->Put(Slice("foo0"), Slice("bar0b"));
        ASSERT_OK(s);
        s = txn0->Put(Slice("foo1"), Slice("bar1"));
        ASSERT_OK(s);
        s = txn0->Merge(Slice("foo2"), Slice("bar2a"));
        ASSERT_OK(s);
        // Repeat a key after the start of a sub-patch. This should not cause a
        // duplicate in the most recent sub-patch and hence not creating a new
        // sub-patch.
        s = txn0->Put(Slice("foo0"), Slice("bar0c"));
        ASSERT_OK(s);
        s = txn0->Merge(Slice("foo2"), Slice("bar2b"));
        ASSERT_OK(s);
        // duplicate the keys but in a different cf. It should not be counted as
        // duplicate.
        s = txn0->Put(cf_handle, Slice("foo0"), Slice("bar0-cf1"));
        ASSERT_OK(s);
        s = txn0->Put(Slice("foo3"), Slice("bar3"));
        ASSERT_OK(s);
        s = txn0->Merge(Slice("foo3"), Slice("bar3"));
        ASSERT_OK(s);
        s = txn0->Put(Slice("foo4"), Slice("bar4"));
        ASSERT_OK(s);
        s = txn0->Delete(Slice("foo4"));
        ASSERT_OK(s);
        s = txn0->SingleDelete(Slice("foo4"));
        ASSERT_OK(s);
        if (do_prepare) {
          s = txn0->Prepare();
          ASSERT_OK(s);
        }
        if (do_rollback) {
          // Test rolling back the batch with duplicates
          s = txn0->Rollback();
          ASSERT_OK(s);
        } else {
          if (with_commit_batch) {
            assert(do_prepare);
            auto cb = txn0->GetCommitTimeWriteBatch();
            // duplicate a key in the original batch
            // TODO(myabandeh): the behavior of GetCommitTimeWriteBatch
            // conflicting with the prepared batch is currently undefined and
            // gives different results in different implementations.

            // s = cb->Put(Slice("foo0"), Slice("bar0d"));
            // ASSERT_OK(s);
            // add a new duplicate key
            s = cb->Put(Slice("foo6"), Slice("bar6a"));
            ASSERT_OK(s);
            s = cb->Put(Slice("foo6"), Slice("bar6b"));
            ASSERT_OK(s);
            // add a duplicate key that is removed in the same batch
            s = cb->Put(Slice("foo7"), Slice("bar7a"));
            ASSERT_OK(s);
            s = cb->Delete(Slice("foo7"));
            ASSERT_OK(s);
          }
          s = txn0->Commit();
          ASSERT_OK(s);
        }
        delete txn0;
        ReadOptions ropt;
        PinnableSlice pinnable_val;

        if (do_rollback) {
          s = db->Get(ropt, db->DefaultColumnFamily(), "foo0", &pinnable_val);
          ASSERT_TRUE(s.IsNotFound());
          s = db->Get(ropt, cf_handle, "foo0", &pinnable_val);
          ASSERT_TRUE(s.IsNotFound());
          s = db->Get(ropt, db->DefaultColumnFamily(), "foo1", &pinnable_val);
          ASSERT_TRUE(s.IsNotFound());
          s = db->Get(ropt, db->DefaultColumnFamily(), "foo2", &pinnable_val);
          ASSERT_TRUE(s.IsNotFound());
          s = db->Get(ropt, db->DefaultColumnFamily(), "foo3", &pinnable_val);
          ASSERT_TRUE(s.IsNotFound());
          s = db->Get(ropt, db->DefaultColumnFamily(), "foo4", &pinnable_val);
          ASSERT_TRUE(s.IsNotFound());
        } else {
          s = db->Get(ropt, db->DefaultColumnFamily(), "foo0", &pinnable_val);
          ASSERT_OK(s);
          ASSERT_TRUE(pinnable_val == ("bar0c"));
          s = db->Get(ropt, cf_handle, "foo0", &pinnable_val);
          ASSERT_OK(s);
          ASSERT_TRUE(pinnable_val == ("bar0-cf1"));
          s = db->Get(ropt, db->DefaultColumnFamily(), "foo1", &pinnable_val);
          ASSERT_OK(s);
          ASSERT_TRUE(pinnable_val == ("bar1"));
          s = db->Get(ropt, db->DefaultColumnFamily(), "foo2", &pinnable_val);
          ASSERT_OK(s);
          ASSERT_TRUE(pinnable_val == ("bar2a,bar2b"));
          s = db->Get(ropt, db->DefaultColumnFamily(), "foo3", &pinnable_val);
          ASSERT_OK(s);
          ASSERT_TRUE(pinnable_val == ("bar3,bar3"));
          s = db->Get(ropt, db->DefaultColumnFamily(), "foo4", &pinnable_val);
          ASSERT_TRUE(s.IsNotFound());
          if (with_commit_batch) {
            s = db->Get(ropt, db->DefaultColumnFamily(), "foo6", &pinnable_val);
            if (txn_db_options.write_policy ==
                TxnDBWritePolicy::WRITE_COMMITTED) {
              ASSERT_OK(s);
              ASSERT_TRUE(pinnable_val == ("bar6b"));
            } else {
              ASSERT_TRUE(s.IsNotFound());
            }
            s = db->Get(ropt, db->DefaultColumnFamily(), "foo7", &pinnable_val);
            ASSERT_TRUE(s.IsNotFound());
          }
        }
        delete cf_handle;
      }  // with_commit_batch
    }    // do_rollback
  }      // do_prepare

  if (!options.unordered_write) {
    // Also test with max_successive_merges > 0. max_successive_merges will not
    // affect our algorithm for duplicate key insertion but we add the test to
    // verify that.
    cf_options.max_successive_merges = 2;
    cf_options.merge_operator = MergeOperators::CreateStringAppendOperator();
    ASSERT_OK(ReOpen());
    ASSERT_OK(db->CreateColumnFamily(cf_options, cf_name, &cf_handle));
    WriteOptions write_options;
    // Ensure one value for the key
    ASSERT_OK(db->Put(write_options, cf_handle, Slice("key"), Slice("value")));
    WriteBatch batch;
    // Merge more than max_successive_merges times
    ASSERT_OK(batch.Merge(cf_handle, Slice("key"), Slice("1")));
    ASSERT_OK(batch.Merge(cf_handle, Slice("key"), Slice("2")));
    ASSERT_OK(batch.Merge(cf_handle, Slice("key"), Slice("3")));
    ASSERT_OK(batch.Merge(cf_handle, Slice("key"), Slice("4")));
    ASSERT_OK(db->Write(write_options, &batch));
    ReadOptions read_options;
    std::string value;
    ASSERT_OK(db->Get(read_options, cf_handle, "key", &value));
    ASSERT_EQ(value, "value,1,2,3,4");
    delete cf_handle;
  }

  {
    // Test that the duplicate detection is not compromised after rolling back
    // to a save point
    TransactionOptions txn_options;
    WriteOptions write_options;
    Transaction* txn0 = db->BeginTransaction(write_options, txn_options);
    ASSERT_OK(txn0->Put(Slice("foo0"), Slice("bar0a")));
    ASSERT_OK(txn0->Put(Slice("foo0"), Slice("bar0b")));
    txn0->SetSavePoint();
    ASSERT_OK(txn0->RollbackToSavePoint());
    ASSERT_OK(txn0->Commit());
    delete txn0;
  }

  // Test sucessfull recovery after a crash
  {
    ASSERT_OK(ReOpen());
    TransactionOptions txn_options;
    WriteOptions write_options;
    ReadOptions ropt;
    Transaction* txn0;
    PinnableSlice pinnable_val;
    Status s;

    std::unique_ptr<const Comparator> comp_gc(new ThreeBytewiseComparator());
    cf_options.comparator = comp_gc.get();
    cf_options.merge_operator = MergeOperators::CreateStringAppendOperator();
    ASSERT_OK(db->CreateColumnFamily(cf_options, cf_name, &cf_handle));
    delete cf_handle;
    std::vector<ColumnFamilyDescriptor> cfds{
        ColumnFamilyDescriptor(kDefaultColumnFamilyName,
                               ColumnFamilyOptions(options)),
        ColumnFamilyDescriptor(cf_name, cf_options),
    };
    std::vector<ColumnFamilyHandle*> handles;
    ASSERT_OK(ReOpenNoDelete(cfds, &handles));

    assert(db != nullptr);
    ASSERT_OK(db->Put(write_options, "foo0", "init"));
    ASSERT_OK(db->Put(write_options, "foo1", "init"));
    ASSERT_OK(db->Put(write_options, handles[1], "foo0", "init"));
    ASSERT_OK(db->Put(write_options, handles[1], "foo1", "init"));

    // one entry
    txn0 = db->BeginTransaction(write_options, txn_options);
    ASSERT_OK(txn0->SetName("xid"));
    ASSERT_OK(txn0->Put(Slice("foo0"), Slice("bar0a")));
    ASSERT_OK(txn0->Prepare());
    delete txn0;
    // This will check the asserts inside recovery code
    ASSERT_OK(db->FlushWAL(true));
    static_cast<PessimisticTransactionDB*>(db)->TEST_Crash();
    ASSERT_OK(ReOpenNoDelete(cfds, &handles));
    txn0 = db->GetTransactionByName("xid");
    ASSERT_TRUE(txn0 != nullptr);
    ASSERT_OK(txn0->Commit());
    delete txn0;
    s = db->Get(ropt, db->DefaultColumnFamily(), "foo0", &pinnable_val);
    ASSERT_OK(s);
    ASSERT_TRUE(pinnable_val == ("bar0a"));

    // two entries, no duplicate
    txn0 = db->BeginTransaction(write_options, txn_options);
    ASSERT_OK(txn0->SetName("xid"));
    ASSERT_OK(txn0->Put(handles[1], Slice("foo0"), Slice("bar0b")));
    ASSERT_OK(txn0->Put(handles[1], Slice("fol1"), Slice("bar1b")));
    ASSERT_OK(txn0->Put(Slice("foo0"), Slice("bar0b")));
    ASSERT_OK(txn0->Put(Slice("foo1"), Slice("bar1b")));
    ASSERT_OK(txn0->Prepare());
    delete txn0;
    // This will check the asserts inside recovery code
    ASSERT_OK(db->FlushWAL(true));
    // Flush only cf 1
    ASSERT_OK(static_cast_with_check<DBImpl>(db->GetRootDB())
                  ->TEST_FlushMemTable(true, false, handles[1]));
    static_cast<PessimisticTransactionDB*>(db)->TEST_Crash();
    ASSERT_OK(ReOpenNoDelete(cfds, &handles));
    txn0 = db->GetTransactionByName("xid");
    ASSERT_TRUE(txn0 != nullptr);
    ASSERT_OK(txn0->Commit());
    delete txn0;
    pinnable_val.Reset();
    s = db->Get(ropt, db->DefaultColumnFamily(), "foo0", &pinnable_val);
    ASSERT_OK(s);
    ASSERT_TRUE(pinnable_val == ("bar0b"));
    pinnable_val.Reset();
    s = db->Get(ropt, db->DefaultColumnFamily(), "foo1", &pinnable_val);
    ASSERT_OK(s);
    ASSERT_TRUE(pinnable_val == ("bar1b"));
    pinnable_val.Reset();
    s = db->Get(ropt, handles[1], "foo0", &pinnable_val);
    ASSERT_OK(s);
    ASSERT_TRUE(pinnable_val == ("bar0b"));
    pinnable_val.Reset();
    s = db->Get(ropt, handles[1], "fol1", &pinnable_val);
    ASSERT_OK(s);
    ASSERT_TRUE(pinnable_val == ("bar1b"));

    // one duplicate with ::Put
    txn0 = db->BeginTransaction(write_options, txn_options);
    ASSERT_OK(txn0->SetName("xid"));
    ASSERT_OK(txn0->Put(handles[1], Slice("key-nonkey0"), Slice("bar0c")));
    ASSERT_OK(txn0->Put(handles[1], Slice("key-nonkey1"), Slice("bar1d")));
    ASSERT_OK(txn0->Put(Slice("foo0"), Slice("bar0c")));
    ASSERT_OK(txn0->Put(Slice("foo1"), Slice("bar1c")));
    ASSERT_OK(txn0->Put(Slice("foo0"), Slice("bar0d")));
    ASSERT_OK(txn0->Prepare());
    delete txn0;
    // This will check the asserts inside recovery code
    ASSERT_OK(db->FlushWAL(true));
    // Flush only cf 1
    ASSERT_OK(static_cast_with_check<DBImpl>(db->GetRootDB())
                  ->TEST_FlushMemTable(true, false, handles[1]));
    static_cast<PessimisticTransactionDB*>(db)->TEST_Crash();
    ASSERT_OK(ReOpenNoDelete(cfds, &handles));
    txn0 = db->GetTransactionByName("xid");
    ASSERT_TRUE(txn0 != nullptr);
    ASSERT_OK(txn0->Commit());
    delete txn0;
    pinnable_val.Reset();
    s = db->Get(ropt, db->DefaultColumnFamily(), "foo0", &pinnable_val);
    ASSERT_OK(s);
    ASSERT_TRUE(pinnable_val == ("bar0d"));
    pinnable_val.Reset();
    s = db->Get(ropt, db->DefaultColumnFamily(), "foo1", &pinnable_val);
    ASSERT_OK(s);
    ASSERT_TRUE(pinnable_val == ("bar1c"));
    pinnable_val.Reset();
    s = db->Get(ropt, handles[1], "key-nonkey2", &pinnable_val);
    ASSERT_OK(s);
    ASSERT_TRUE(pinnable_val == ("bar1d"));

    // Duplicate with ::Put, ::Delete
    txn0 = db->BeginTransaction(write_options, txn_options);
    ASSERT_OK(txn0->SetName("xid"));
    ASSERT_OK(txn0->Put(handles[1], Slice("key-nonkey0"), Slice("bar0e")));
    ASSERT_OK(txn0->Delete(handles[1], Slice("key-nonkey1")));
    ASSERT_OK(txn0->Put(Slice("foo0"), Slice("bar0e")));
    ASSERT_OK(txn0->Delete(Slice("foo0")));
    ASSERT_OK(txn0->Prepare());
    delete txn0;
    // This will check the asserts inside recovery code
    ASSERT_OK(db->FlushWAL(true));
    // Flush only cf 1
    ASSERT_OK(static_cast_with_check<DBImpl>(db->GetRootDB())
                  ->TEST_FlushMemTable(true, false, handles[1]));
    static_cast<PessimisticTransactionDB*>(db)->TEST_Crash();
    ASSERT_OK(ReOpenNoDelete(cfds, &handles));
    txn0 = db->GetTransactionByName("xid");
    ASSERT_TRUE(txn0 != nullptr);
    ASSERT_OK(txn0->Commit());
    delete txn0;
    pinnable_val.Reset();
    s = db->Get(ropt, db->DefaultColumnFamily(), "foo0", &pinnable_val);
    ASSERT_TRUE(s.IsNotFound());
    pinnable_val.Reset();
    s = db->Get(ropt, handles[1], "key-nonkey2", &pinnable_val);
    ASSERT_TRUE(s.IsNotFound());

    // Duplicate with ::Put, ::SingleDelete
    txn0 = db->BeginTransaction(write_options, txn_options);
    ASSERT_OK(txn0->SetName("xid"));
    ASSERT_OK(txn0->Put(handles[1], Slice("key-nonkey0"), Slice("bar0g")));
    ASSERT_OK(txn0->SingleDelete(handles[1], Slice("key-nonkey1")));
    ASSERT_OK(txn0->Put(Slice("foo0"), Slice("bar0e")));
    ASSERT_OK(txn0->SingleDelete(Slice("foo0")));
    ASSERT_OK(txn0->Prepare());
    delete txn0;
    // This will check the asserts inside recovery code
    ASSERT_OK(db->FlushWAL(true));
    // Flush only cf 1
    ASSERT_OK(static_cast_with_check<DBImpl>(db->GetRootDB())
                  ->TEST_FlushMemTable(true, false, handles[1]));
    static_cast<PessimisticTransactionDB*>(db)->TEST_Crash();
    ASSERT_OK(ReOpenNoDelete(cfds, &handles));
    txn0 = db->GetTransactionByName("xid");
    ASSERT_TRUE(txn0 != nullptr);
    ASSERT_OK(txn0->Commit());
    delete txn0;
    pinnable_val.Reset();
    s = db->Get(ropt, db->DefaultColumnFamily(), "foo0", &pinnable_val);
    ASSERT_TRUE(s.IsNotFound());
    pinnable_val.Reset();
    s = db->Get(ropt, handles[1], "key-nonkey2", &pinnable_val);
    ASSERT_TRUE(s.IsNotFound());

    // Duplicate with ::Put, ::Merge
    txn0 = db->BeginTransaction(write_options, txn_options);
    ASSERT_OK(txn0->SetName("xid"));
    ASSERT_OK(txn0->Put(handles[1], Slice("key-nonkey0"), Slice("bar1i")));
    ASSERT_OK(txn0->Merge(handles[1], Slice("key-nonkey1"), Slice("bar1j")));
    ASSERT_OK(txn0->Put(Slice("foo0"), Slice("bar0f")));
    ASSERT_OK(txn0->Merge(Slice("foo0"), Slice("bar0g")));
    ASSERT_OK(txn0->Prepare());
    delete txn0;
    // This will check the asserts inside recovery code
    ASSERT_OK(db->FlushWAL(true));
    // Flush only cf 1
    ASSERT_OK(static_cast_with_check<DBImpl>(db->GetRootDB())
                  ->TEST_FlushMemTable(true, false, handles[1]));
    static_cast<PessimisticTransactionDB*>(db)->TEST_Crash();
    ASSERT_OK(ReOpenNoDelete(cfds, &handles));
    txn0 = db->GetTransactionByName("xid");
    ASSERT_TRUE(txn0 != nullptr);
    ASSERT_OK(txn0->Commit());
    delete txn0;
    pinnable_val.Reset();
    s = db->Get(ropt, db->DefaultColumnFamily(), "foo0", &pinnable_val);
    ASSERT_OK(s);
    ASSERT_TRUE(pinnable_val == ("bar0f,bar0g"));
    pinnable_val.Reset();
    s = db->Get(ropt, handles[1], "key-nonkey2", &pinnable_val);
    ASSERT_OK(s);
    ASSERT_TRUE(pinnable_val == ("bar1i,bar1j"));

    for (auto h : handles) {
      delete h;
    }
    delete db;
    db = nullptr;
  }
}

// Test that the reseek optimization in iterators will not result in an infinite
// loop if there are too many uncommitted entries before the snapshot.
TEST_P(TransactionTest, ReseekOptimization) {
  WriteOptions write_options;
  write_options.sync = true;
  write_options.disableWAL = false;
  ColumnFamilyDescriptor cfd;
  ASSERT_OK(db->DefaultColumnFamily()->GetDescriptor(&cfd));
  auto max_skip = cfd.options.max_sequential_skip_in_iterations;

  ASSERT_OK(db->Put(write_options, Slice("foo0"), Slice("initv")));

  TransactionOptions txn_options;
  Transaction* txn0 = db->BeginTransaction(write_options, txn_options);
  ASSERT_OK(txn0->SetName("xid"));
  // Duplicate keys will result into separate sequence numbers in WritePrepared
  // and WriteUnPrepared
  for (size_t i = 0; i < 2 * max_skip; i++) {
    ASSERT_OK(txn0->Put(Slice("foo1"), Slice("bar")));
  }
  ASSERT_OK(txn0->Prepare());
  ASSERT_OK(db->Put(write_options, Slice("foo2"), Slice("initv")));

  ReadOptions read_options;
  // To avoid loops
  read_options.max_skippable_internal_keys = 10 * max_skip;
  Iterator* iter = db->NewIterator(read_options);
  ASSERT_OK(iter->status());
  size_t cnt = 0;
  iter->SeekToFirst();
  while (iter->Valid()) {
    iter->Next();
    ASSERT_OK(iter->status());
    cnt++;
  }
  ASSERT_EQ(cnt, 2);
  cnt = 0;
  iter->SeekToLast();
  while (iter->Valid()) {
    iter->Prev();
    ASSERT_OK(iter->status());
    cnt++;
  }
  ASSERT_EQ(cnt, 2);
  delete iter;
  ASSERT_OK(txn0->Rollback());
  delete txn0;
}

// After recovery in kPointInTimeRecovery mode, the corrupted log file remains
// there. The new log files should be still read succesfully during recovery of
// the 2nd crash.
TEST_P(TransactionTest, DoubleCrashInRecovery) {
  for (const bool manual_wal_flush : {false, true}) {
    for (const bool write_after_recovery : {false, true}) {
      options.wal_recovery_mode = WALRecoveryMode::kPointInTimeRecovery;
      options.manual_wal_flush = manual_wal_flush;
      ASSERT_OK(ReOpen());
      std::string cf_name = "two";
      ColumnFamilyOptions cf_options;
      ColumnFamilyHandle* cf_handle = nullptr;
      ASSERT_OK(db->CreateColumnFamily(cf_options, cf_name, &cf_handle));

      // Add a prepare entry to prevent the older logs from being deleted.
      WriteOptions write_options;
      TransactionOptions txn_options;
      Transaction* txn = db->BeginTransaction(write_options, txn_options);
      ASSERT_OK(txn->SetName("xid"));
      ASSERT_OK(txn->Put(Slice("foo-prepare"), Slice("bar-prepare")));
      ASSERT_OK(txn->Prepare());

      FlushOptions flush_ops;
      ASSERT_OK(db->Flush(flush_ops));
      // Now we have a log that cannot be deleted

      ASSERT_OK(db->Put(write_options, cf_handle, "foo1", "bar1"));
      // Flush only the 2nd cf
      ASSERT_OK(db->Flush(flush_ops, cf_handle));

      // The value is large enough to be touched by the corruption we ingest
      // below.
      std::string large_value(400, ' ');
      // key/value not touched by corruption
      ASSERT_OK(db->Put(write_options, "foo2", "bar2"));
      // key/value touched by corruption
      ASSERT_OK(db->Put(write_options, "foo3", large_value));
      // key/value not touched by corruption
      ASSERT_OK(db->Put(write_options, "foo4", "bar4"));

      ASSERT_OK(db->FlushWAL(true));
      DBImpl* db_impl = static_cast_with_check<DBImpl>(db->GetRootDB());
      uint64_t wal_file_id = db_impl->TEST_LogfileNumber();
      std::string fname = LogFileName(dbname, wal_file_id);
      static_cast<PessimisticTransactionDB*>(db)->TEST_Crash();
      delete txn;
      delete cf_handle;
      delete db;
      db = nullptr;

      // Corrupt the last log file in the middle, so that it is not corrupted
      // in the tail.
      std::string file_content;
      ASSERT_OK(ReadFileToString(env.get(), fname, &file_content));
      file_content[400] = 'h';
      file_content[401] = 'a';
      ASSERT_OK(env->DeleteFile(fname));
      ASSERT_OK(WriteStringToFile(env.get(), file_content, fname, true));

      // Recover from corruption
      std::vector<ColumnFamilyHandle*> handles;
      std::vector<ColumnFamilyDescriptor> column_families;
      column_families.emplace_back(kDefaultColumnFamilyName,
                                   ColumnFamilyOptions());
      column_families.emplace_back("two", ColumnFamilyOptions());
      ASSERT_OK(ReOpenNoDelete(column_families, &handles));
      assert(db != nullptr);

      if (write_after_recovery) {
        // Write data to the log right after the corrupted log
        ASSERT_OK(db->Put(write_options, "foo5", large_value));
      }

      // Persist data written to WAL during recovery or by the last Put
      ASSERT_OK(db->FlushWAL(true));
      // 2nd crash to recover while having a valid log after the corrupted one.
      ASSERT_OK(ReOpenNoDelete(column_families, &handles));
      assert(db != nullptr);
      txn = db->GetTransactionByName("xid");
      ASSERT_TRUE(txn != nullptr);
      ASSERT_OK(txn->Commit());
      delete txn;
      for (auto handle : handles) {
        delete handle;
      }
    }
  }
}

TEST_P(TransactionTest, CommitWithoutPrepare) {
  {
    // skip_prepare = false.
    WriteOptions write_options;
    TransactionOptions txn_options;
    txn_options.skip_prepare = false;
    Transaction* txn = db->BeginTransaction(write_options, txn_options);
    ASSERT_TRUE(txn->Commit().IsTxnNotPrepared());
    delete txn;
  }

  {
    // skip_prepare = true.
    WriteOptions write_options;
    TransactionOptions txn_options;
    txn_options.skip_prepare = true;
    Transaction* txn = db->BeginTransaction(write_options, txn_options);
    ASSERT_OK(txn->Commit());
    delete txn;
  }
}

TEST_P(TransactionTest, OpenAndEnableU64Timestamp) {
  ASSERT_OK(ReOpenNoDelete());

  assert(db);

  const std::string test_cf_name = "test_cf";
  ColumnFamilyOptions cf_opts;
  cf_opts.comparator = test::BytewiseComparatorWithU64TsWrapper();
  {
    ColumnFamilyHandle* cfh = nullptr;
    const Status s = db->CreateColumnFamily(cf_opts, test_cf_name, &cfh);
    if (txn_db_options.write_policy == WRITE_COMMITTED) {
      ASSERT_OK(s);
      delete cfh;
    } else {
      ASSERT_TRUE(s.IsNotSupported());
      assert(!cfh);
    }
  }

  // Bypass transaction db layer.
  if (txn_db_options.write_policy != WRITE_COMMITTED) {
    DBImpl* db_impl = static_cast_with_check<DBImpl>(db->GetRootDB());
    assert(db_impl);
    ColumnFamilyHandle* cfh = nullptr;
    ASSERT_OK(db_impl->CreateColumnFamily(cf_opts, test_cf_name, &cfh));
    delete cfh;
  }

  {
    std::vector<ColumnFamilyDescriptor> cf_descs;
    cf_descs.emplace_back(kDefaultColumnFamilyName, options);
    cf_descs.emplace_back(test_cf_name, cf_opts);
    std::vector<ColumnFamilyHandle*> handles;
    const Status s = ReOpenNoDelete(cf_descs, &handles);
    if (txn_db_options.write_policy == WRITE_COMMITTED) {
      ASSERT_OK(s);
      for (auto* h : handles) {
        delete h;
      }
    } else {
      ASSERT_TRUE(s.IsNotSupported());
    }
  }
}

TEST_P(TransactionTest, OpenAndEnableU32Timestamp) {
  class DummyComparatorWithU32Ts : public Comparator {
   public:
    DummyComparatorWithU32Ts() : Comparator(sizeof(uint32_t)) {}
    const char* Name() const override { return "DummyComparatorWithU32Ts"; }
    void FindShortSuccessor(std::string*) const override {}
    void FindShortestSeparator(std::string*, const Slice&) const override {}
    int Compare(const Slice&, const Slice&) const override { return 0; }
  };

  std::unique_ptr<Comparator> dummy_ucmp(new DummyComparatorWithU32Ts());

  ASSERT_OK(ReOpenNoDelete());

  assert(db);

  const std::string test_cf_name = "test_cf";

  ColumnFamilyOptions cf_opts;
  cf_opts.comparator = dummy_ucmp.get();
  {
    ColumnFamilyHandle* cfh = nullptr;
    ASSERT_TRUE(db->CreateColumnFamily(cf_opts, test_cf_name, &cfh)
                    .IsInvalidArgument());
  }

  // Bypass transaction db layer.
  {
    ColumnFamilyHandle* cfh = nullptr;
    DBImpl* db_impl = static_cast_with_check<DBImpl>(db->GetRootDB());
    assert(db_impl);
    ASSERT_OK(db_impl->CreateColumnFamily(cf_opts, test_cf_name, &cfh));
    delete cfh;
  }

  {
    std::vector<ColumnFamilyDescriptor> cf_descs;
    cf_descs.emplace_back(kDefaultColumnFamilyName, options);
    cf_descs.emplace_back(test_cf_name, cf_opts);
    std::vector<ColumnFamilyHandle*> handles;
    ASSERT_TRUE(ReOpenNoDelete(cf_descs, &handles).IsInvalidArgument());
  }
}

TEST_P(TransactionTest, WriteWithBulkCreatedColumnFamilies) {
  ColumnFamilyOptions cf_options;
  WriteOptions write_options;

  std::vector<std::string> cf_names;
  std::vector<ColumnFamilyHandle*> cf_handles;

  cf_names.emplace_back("test_cf");

  ASSERT_OK(db->CreateColumnFamilies(cf_options, cf_names, &cf_handles));
  ASSERT_OK(db->Put(write_options, cf_handles[0], "foo", "bar"));
  ASSERT_OK(db->DropColumnFamilies(cf_handles));

  for (auto* h : cf_handles) {
    delete h;
  }
  cf_handles.clear();

  std::vector<ColumnFamilyDescriptor> cf_descriptors;

  cf_descriptors.emplace_back("test_cf", ColumnFamilyOptions());

  ASSERT_OK(db->CreateColumnFamilies(cf_options, cf_names, &cf_handles));
  ASSERT_OK(db->Put(write_options, cf_handles[0], "foo", "bar"));
  ASSERT_OK(db->DropColumnFamilies(cf_handles));
  for (auto* h : cf_handles) {
    delete h;
  }
  cf_handles.clear();
}

TEST_P(TransactionTest, LockWal) {
  const TxnDBWritePolicy write_policy = std::get<2>(GetParam());
  if (TxnDBWritePolicy::WRITE_COMMITTED != write_policy) {
    ROCKSDB_GTEST_BYPASS("Test only write-committed for now");
    return;
  }
  ASSERT_OK(ReOpen());

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->LoadDependency(
      {{"TransactionTest::LockWal:AfterLockWal",
        "TransactionTest::LockWal:BeforePrepareTxn2"}});
  SyncPoint::GetInstance()->EnableProcessing();

  std::unique_ptr<Transaction> txn0;
  WriteOptions wopts;
  wopts.no_slowdown = true;
  txn0.reset(db->BeginTransaction(wopts, TransactionOptions()));
  ASSERT_OK(txn0->SetName("txn0"));
  ASSERT_OK(txn0->Put("foo", "v0"));

  std::unique_ptr<Transaction> txn1;
  txn1.reset(db->BeginTransaction(wopts, TransactionOptions()));
  ASSERT_OK(txn1->SetName("txn1"));
  ASSERT_OK(txn1->Put("dummy", "v0"));
  ASSERT_OK(txn1->Prepare());

  std::unique_ptr<Transaction> txn2;
  port::Thread worker([&]() {
    txn2.reset(db->BeginTransaction(WriteOptions(), TransactionOptions()));
    ASSERT_OK(txn2->SetName("txn2"));
    ASSERT_OK(txn2->Put("bar", "v0"));
    TEST_SYNC_POINT("TransactionTest::LockWal:BeforePrepareTxn2");
    ASSERT_OK(txn2->Prepare());
    ASSERT_OK(txn2->Commit());
  });
  ASSERT_OK(db->LockWAL());
  // txn0 cannot prepare
  Status s = txn0->Prepare();
  ASSERT_TRUE(s.IsIncomplete());
  // txn1 cannot commit
  s = txn1->Commit();
  ASSERT_TRUE(s.IsIncomplete());

  TEST_SYNC_POINT("TransactionTest::LockWal:AfterLockWal");

  ASSERT_OK(db->UnlockWAL());
  txn0.reset();

  txn0.reset(db->BeginTransaction(wopts, TransactionOptions()));
  ASSERT_OK(txn0->SetName("txn0_1"));
  ASSERT_OK(txn0->Put("foo", "v1"));
  ASSERT_OK(txn0->Prepare());
  ASSERT_OK(txn0->Commit());
  worker.join();

  SyncPoint::GetInstance()->DisableProcessing();
}

TEST_P(TransactionTest, StallTwoWriteQueues) {
  // There was a two_write_queues bug in which both write thread leaders (for
  // each queue) would attempt to own the stopping of writes in the primary
  // write queue. This nearly worked but could lead to some broken assertions
  // and a kind of deadlock in the test below. (Would resume if someone
  // eventually signalled bg_cv_ again.)
  if (!options.two_write_queues) {
    ROCKSDB_GTEST_BYPASS("Test only needed with two_write_queues");
    return;
  }

  // Stop writes
  ASSERT_OK(db->LockWAL());

  WriteOptions wopts;
  wopts.sync = true;
  wopts.disableWAL = false;

  // Create one write thread that blocks in the primary write queue and one
  // that blocks in the nonmem queue.
  bool t1_completed = false;
  bool t2_completed = false;
  port::Thread t1{[&]() {
    ASSERT_OK(db->Put(wopts, "x", "y"));
    t1_completed = true;
  }};
  port::Thread t2{[&]() {
    std::unique_ptr<Transaction> txn0{db->BeginTransaction(wopts, {})};
    ASSERT_OK(txn0->SetName("xid"));
    ASSERT_OK(txn0->Prepare());  // nonmem
    ASSERT_OK(txn0->Commit());
    t2_completed = true;
  }};

  // Sleep long enough to that above threads can usually reach a waiting point,
  // to usually reveal deadlock if the bug is present.
  std::this_thread::sleep_for(std::chrono::milliseconds(100));
  // Ensure proper test setup
  ASSERT_FALSE(t1_completed);
  ASSERT_FALSE(t2_completed);

  // Resume writes
  ASSERT_OK(db->UnlockWAL());

  // Wait for writes to finish
  t1.join();
  t2.join();
  // Ensure proper test setup
  ASSERT_TRUE(t1_completed);
  ASSERT_TRUE(t2_completed);
}

// Make sure UnlockWAL does not return until the stall it controls is cleared.
TEST_P(TransactionTest, UnlockWALStallCleared) {
  auto dbimpl = static_cast_with_check<DBImpl>(db->GetRootDB());
  for (bool external_stall : {false, true}) {
    WriteOptions wopts;
    wopts.sync = true;
    wopts.disableWAL = false;

    ASSERT_OK(db->Put(wopts, "k1", "val1"));

    // Stall writes
    ASSERT_OK(db->LockWAL());

    std::unique_ptr<WriteControllerToken> token;
    if (external_stall) {
      // Also make sure UnlockWAL can return despite another stall being in
      // effect.
      token = dbimpl->TEST_write_controler().GetStopToken();
    }

    SyncPoint::GetInstance()->DisableProcessing();
    std::vector<SyncPoint::SyncPointPair> sync_deps;
    sync_deps.push_back(
        {"DBImpl::DelayWrite:Wait",
         "TransactionTest::UnlockWALStallCleared:BeforeUnlockWAL1"});
    if (options.two_write_queues &&
        txn_db_options.write_policy == WRITE_COMMITTED) {
      sync_deps.push_back(
          {"DBImpl::DelayWrite:NonmemWait",
           "TransactionTest::UnlockWALStallCleared:BeforeUnlockWAL2"});
    }
    SyncPoint::GetInstance()->LoadDependency(sync_deps);
    SyncPoint::GetInstance()->SetCallBack(
        "DBImpl::DelayWrite:AfterWait", [](void* arg) {
          auto& mu = *static_cast<CacheAlignedInstrumentedMutex*>(arg);
          mu.AssertHeld();
          // Pretend we are slow waking up from bg_cv_, to give a chance for the
          // bug to occur if it can. Randomly prefer one queue over the other.
          mu.Unlock();
          if (Random::GetTLSInstance()->OneIn(2)) {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
          } else {
            std::this_thread::yield();
          }
          mu.Lock();
        });
    SyncPoint::GetInstance()->EnableProcessing();

    // Create blocking writes (for both queues) in background and use
    // sync point dependency to get the stall into the write queue(s)
    std::atomic<bool> t1_completed{false};
    port::Thread t1{[&]() {
      ASSERT_OK(db->Put(wopts, "k2", "val2"));
      t1_completed = true;
    }};

    std::atomic<bool> t2_completed{false};
    port::Thread t2{[&]() {
      std::unique_ptr<Transaction> txn0{db->BeginTransaction(wopts, {})};
      ASSERT_OK(txn0->SetName("x1"));
      ASSERT_OK(txn0->Put("k3", "val3"));
      ASSERT_OK(txn0->Prepare());  // nonmem
      ASSERT_OK(txn0->Commit());
    }};

    // Be sure the test is set up appropriately
    TEST_SYNC_POINT("TransactionTest::UnlockWALStallCleared:BeforeUnlockWAL1");
    TEST_SYNC_POINT("TransactionTest::UnlockWALStallCleared:BeforeUnlockWAL2");
    ASSERT_FALSE(t1_completed.load());
    ASSERT_FALSE(t2_completed.load());

    // Clear the stall
    ASSERT_OK(db->UnlockWAL());

    WriteOptions wopts2 = wopts;
    if (external_stall) {
      // We did not deadlock in UnlockWAL, so now async clear the external
      // stall and then do a blocking write.
      // DB mutex acquire+release is needed to ensure we don't reset token and
      // signal while DelayWrite() is between IsStopped() and
      // BeginWriteStall().
      token.reset();
      dbimpl->TEST_LockMutex();
      dbimpl->TEST_UnlockMutex();
      dbimpl->TEST_SignalAllBgCv();
    } else {
      // To verify the LockWAL stall is guaranteed cleared, do a non-blocking
      // write that is attempting to catch a bug by attempting to come before
      // the thread that did BeginWriteStall() can do EndWriteStall()
      wopts2.no_slowdown = true;
    }
    std::unique_ptr<Transaction> txn0{db->BeginTransaction(wopts2, {})};
    ASSERT_OK(txn0->SetName("x2"));
    ASSERT_OK(txn0->Put("k1", "val4"));
    ASSERT_OK(txn0->Prepare());  // nonmem
    ASSERT_OK(txn0->Commit());

    t1.join();
    t2.join();
  }
}

TEST_P(TransactionTest, PutEntitySuccess) {
  const TxnDBWritePolicy write_policy = std::get<2>(GetParam());
  if (write_policy != TxnDBWritePolicy::WRITE_COMMITTED) {
    ROCKSDB_GTEST_BYPASS("Test only WriteCommitted for now");
    return;
  }

  constexpr char foo[] = "foo";
  const WideColumns foo_columns{
      {kDefaultWideColumnName, "bar"}, {"col1", "val1"}, {"col2", "val2"}};
  const WideColumns foo_new_columns{
      {kDefaultWideColumnName, "baz"}, {"colA", "valA"}, {"colB", "valB"}};

  ASSERT_OK(db->PutEntity(WriteOptions(), db->DefaultColumnFamily(), foo,
                          foo_columns));

  {
    std::unique_ptr<Transaction> txn(
        db->BeginTransaction(WriteOptions(), TransactionOptions()));

    ASSERT_NE(txn, nullptr);
    ASSERT_NE(txn->GetID(), 0);
    ASSERT_EQ(txn->GetNumPutEntities(), 0);

    {
      PinnableWideColumns columns;
      ASSERT_OK(txn->GetEntity(ReadOptions(), db->DefaultColumnFamily(), foo,
                               &columns));
      ASSERT_EQ(columns.columns(), foo_columns);
    }

    {
      PinnableWideColumns columns;
      ASSERT_OK(txn->GetEntityForUpdate(
          ReadOptions(), db->DefaultColumnFamily(), foo, &columns));
      ASSERT_EQ(columns.columns(), foo_columns);
    }

    ASSERT_OK(txn->PutEntity(db->DefaultColumnFamily(), foo, foo_new_columns));

    ASSERT_EQ(txn->GetNumPutEntities(), 1);

    {
      PinnableWideColumns columns;
      ASSERT_OK(txn->GetEntity(ReadOptions(), db->DefaultColumnFamily(), foo,
                               &columns));
      ASSERT_EQ(columns.columns(), foo_new_columns);
    }

    {
      PinnableWideColumns columns;
      ASSERT_OK(txn->GetEntityForUpdate(
          ReadOptions(), db->DefaultColumnFamily(), foo, &columns));
      ASSERT_EQ(columns.columns(), foo_new_columns);
    }

    ASSERT_OK(txn->Commit());
  }

  {
    PinnableWideColumns columns;
    ASSERT_OK(
        db->GetEntity(ReadOptions(), db->DefaultColumnFamily(), foo, &columns));
    ASSERT_EQ(columns.columns(), foo_new_columns);
  }
}

TEST_P(TransactionTest, PutEntityWriteConflict) {
  const TxnDBWritePolicy write_policy = std::get<2>(GetParam());
  if (write_policy != TxnDBWritePolicy::WRITE_COMMITTED) {
    ROCKSDB_GTEST_BYPASS("Test only WriteCommitted for now");
    return;
  }

  constexpr char foo[] = "foo";
  const WideColumns foo_columns{
      {kDefaultWideColumnName, "bar"}, {"col1", "val1"}, {"col2", "val2"}};
  constexpr char baz[] = "baz";
  const WideColumns baz_columns{
      {kDefaultWideColumnName, "quux"}, {"colA", "valA"}, {"colB", "valB"}};

  ASSERT_OK(db->PutEntity(WriteOptions(), db->DefaultColumnFamily(), foo,
                          foo_columns));
  ASSERT_OK(db->PutEntity(WriteOptions(), db->DefaultColumnFamily(), baz,
                          baz_columns));

  std::unique_ptr<Transaction> txn(db->BeginTransaction(WriteOptions()));
  ASSERT_NE(txn, nullptr);

  {
    PinnableWideColumns columns;
    ASSERT_OK(txn->GetEntity(ReadOptions(), db->DefaultColumnFamily(), foo,
                             &columns));
    ASSERT_EQ(columns.columns(), foo_columns);
  }

  {
    PinnableWideColumns columns;
    ASSERT_OK(txn->GetEntity(ReadOptions(), db->DefaultColumnFamily(), baz,
                             &columns));
    ASSERT_EQ(columns.columns(), baz_columns);
  }

  {
    constexpr size_t num_keys = 2;

    std::array<Slice, num_keys> keys{{foo, baz}};
    std::array<PinnableWideColumns, num_keys> results;
    std::array<Status, num_keys> statuses;

    txn->MultiGetEntity(ReadOptions(), db->DefaultColumnFamily(), num_keys,
                        keys.data(), results.data(), statuses.data());

    ASSERT_OK(statuses[0]);
    ASSERT_OK(statuses[1]);

    ASSERT_EQ(results[0].columns(), foo_columns);
    ASSERT_EQ(results[1].columns(), baz_columns);
  }

  const WideColumns foo_new_columns{{kDefaultWideColumnName, "FOO"},
                                    {"hello", "world"}};
  const WideColumns baz_new_columns{{kDefaultWideColumnName, "BAZ"},
                                    {"ping", "pong"}};

  ASSERT_OK(txn->PutEntity(db->DefaultColumnFamily(), foo, foo_new_columns));
  ASSERT_OK(txn->PutEntity(db->DefaultColumnFamily(), baz, baz_new_columns));

  {
    PinnableWideColumns columns;
    ASSERT_OK(txn->GetEntity(ReadOptions(), db->DefaultColumnFamily(), foo,
                             &columns));
    ASSERT_EQ(columns.columns(), foo_new_columns);
  }

  {
    PinnableWideColumns columns;
    ASSERT_OK(txn->GetEntity(ReadOptions(), db->DefaultColumnFamily(), baz,
                             &columns));
    ASSERT_EQ(columns.columns(), baz_new_columns);
  }

  {
    constexpr size_t num_keys = 2;

    std::array<Slice, num_keys> keys{{foo, baz}};
    std::array<PinnableWideColumns, num_keys> results;
    std::array<Status, num_keys> statuses;

    txn->MultiGetEntity(ReadOptions(), db->DefaultColumnFamily(), num_keys,
                        keys.data(), results.data(), statuses.data());

    ASSERT_OK(statuses[0]);
    ASSERT_OK(statuses[1]);

    ASSERT_EQ(results[0].columns(), foo_new_columns);
    ASSERT_EQ(results[1].columns(), baz_new_columns);
  }

  // This PutEntity outside of a transaction will conflict with the previous
  // write
  const WideColumns foo_conflict_columns{{kDefaultWideColumnName, "X"},
                                         {"conflicting", "write"}};
  ASSERT_TRUE(db->PutEntity(WriteOptions(), db->DefaultColumnFamily(), foo,
                            foo_conflict_columns)
                  .IsTimedOut());

  {
    PinnableWideColumns columns;
    ASSERT_OK(
        db->GetEntity(ReadOptions(), db->DefaultColumnFamily(), foo, &columns));
    ASSERT_EQ(columns.columns(), foo_columns);
  }

  {
    PinnableWideColumns columns;
    ASSERT_OK(
        db->GetEntity(ReadOptions(), db->DefaultColumnFamily(), baz, &columns));
    ASSERT_EQ(columns.columns(), baz_columns);
  }

  {
    constexpr size_t num_keys = 2;

    std::array<Slice, num_keys> keys{{foo, baz}};
    std::array<PinnableWideColumns, num_keys> results;
    std::array<Status, num_keys> statuses;
    constexpr bool sorted_input = false;

    db->MultiGetEntity(ReadOptions(), db->DefaultColumnFamily(), num_keys,
                       keys.data(), results.data(), statuses.data(),
                       sorted_input);

    ASSERT_OK(statuses[0]);
    ASSERT_OK(statuses[1]);

    ASSERT_EQ(results[0].columns(), foo_columns);
    ASSERT_EQ(results[1].columns(), baz_columns);
  }

  ASSERT_OK(txn->Commit());

  {
    PinnableWideColumns columns;
    ASSERT_OK(
        db->GetEntity(ReadOptions(), db->DefaultColumnFamily(), foo, &columns));
    ASSERT_EQ(columns.columns(), foo_new_columns);
  }

  {
    PinnableWideColumns columns;
    ASSERT_OK(
        db->GetEntity(ReadOptions(), db->DefaultColumnFamily(), baz, &columns));
    ASSERT_EQ(columns.columns(), baz_new_columns);
  }

  {
    constexpr size_t num_keys = 2;

    std::array<Slice, num_keys> keys{{foo, baz}};
    std::array<PinnableWideColumns, num_keys> results;
    std::array<Status, num_keys> statuses;
    constexpr bool sorted_input = false;

    db->MultiGetEntity(ReadOptions(), db->DefaultColumnFamily(), num_keys,
                       keys.data(), results.data(), statuses.data(),
                       sorted_input);

    ASSERT_OK(statuses[0]);
    ASSERT_OK(statuses[1]);

    ASSERT_EQ(results[0].columns(), foo_new_columns);
    ASSERT_EQ(results[1].columns(), baz_new_columns);
  }
}

TEST_P(TransactionTest, PutEntityReadConflict) {
  const TxnDBWritePolicy write_policy = std::get<2>(GetParam());
  if (write_policy != TxnDBWritePolicy::WRITE_COMMITTED) {
    ROCKSDB_GTEST_BYPASS("Test only WriteCommitted for now");
    return;
  }

  constexpr char foo[] = "foo";
  const WideColumns foo_columns{
      {kDefaultWideColumnName, "bar"}, {"col1", "val1"}, {"col2", "val2"}};

  ASSERT_OK(db->PutEntity(WriteOptions(), db->DefaultColumnFamily(), foo,
                          foo_columns));

  std::unique_ptr<Transaction> txn(db->BeginTransaction(WriteOptions()));
  ASSERT_NE(txn, nullptr);

  txn->SetSnapshot();

  ReadOptions snapshot_read_options;
  snapshot_read_options.snapshot = txn->GetSnapshot();

  {
    PinnableWideColumns columns;
    ASSERT_OK(txn->GetEntityForUpdate(
        snapshot_read_options, db->DefaultColumnFamily(), foo, &columns));
    ASSERT_EQ(columns.columns(), foo_columns);
  }

  // This PutEntity outside of a transaction will conflict with the previous
  // write
  const WideColumns foo_conflict_columns{{kDefaultWideColumnName, "X"},
                                         {"conflicting", "write"}};
  ASSERT_TRUE(db->PutEntity(WriteOptions(), db->DefaultColumnFamily(), foo,
                            foo_conflict_columns)
                  .IsTimedOut());

  {
    PinnableWideColumns columns;
    ASSERT_OK(
        db->GetEntity(ReadOptions(), db->DefaultColumnFamily(), foo, &columns));
    ASSERT_EQ(columns.columns(), foo_columns);
  }

  {
    PinnableWideColumns columns;
    ASSERT_OK(txn->GetEntity(ReadOptions(), db->DefaultColumnFamily(), foo,
                             &columns));
    ASSERT_EQ(columns.columns(), foo_columns);
  }

  ASSERT_OK(txn->Commit());

  {
    PinnableWideColumns columns;
    ASSERT_OK(
        db->GetEntity(ReadOptions(), db->DefaultColumnFamily(), foo, &columns));
    ASSERT_EQ(columns.columns(), foo_columns);
  }
}

TEST_P(TransactionTest, EntityReadSanityChecks) {
  constexpr char foo[] = "foo";
  constexpr char bar[] = "bar";
  constexpr size_t num_keys = 2;

  std::unique_ptr<Transaction> txn(db->BeginTransaction(WriteOptions()));
  ASSERT_NE(txn, nullptr);

  {
    constexpr ColumnFamilyHandle* column_family = nullptr;
    PinnableWideColumns columns;
    ASSERT_TRUE(txn->GetEntity(ReadOptions(), column_family, foo, &columns)
                    .IsInvalidArgument());
  }

  {
    constexpr PinnableWideColumns* columns = nullptr;
    ASSERT_TRUE(
        txn->GetEntity(ReadOptions(), db->DefaultColumnFamily(), foo, columns)
            .IsInvalidArgument());
  }

  {
    ReadOptions read_options;
    read_options.io_activity = Env::IOActivity::kGet;

    PinnableWideColumns columns;
    ASSERT_TRUE(
        txn->GetEntity(read_options, db->DefaultColumnFamily(), foo, &columns)
            .IsInvalidArgument());
  }

  {
    constexpr ColumnFamilyHandle* column_family = nullptr;
    std::array<Slice, num_keys> keys{{foo, bar}};
    std::array<PinnableWideColumns, num_keys> results;
    std::array<Status, num_keys> statuses;
    constexpr bool sorted_input = false;

    txn->MultiGetEntity(ReadOptions(), column_family, num_keys, keys.data(),
                        results.data(), statuses.data(), sorted_input);

    ASSERT_TRUE(statuses[0].IsInvalidArgument());
    ASSERT_TRUE(statuses[1].IsInvalidArgument());
  }

  {
    constexpr Slice* keys = nullptr;
    std::array<PinnableWideColumns, num_keys> results;
    std::array<Status, num_keys> statuses;
    constexpr bool sorted_input = false;

    txn->MultiGetEntity(ReadOptions(), db->DefaultColumnFamily(), num_keys,
                        keys, results.data(), statuses.data(), sorted_input);

    ASSERT_TRUE(statuses[0].IsInvalidArgument());
    ASSERT_TRUE(statuses[1].IsInvalidArgument());
  }

  {
    std::array<Slice, num_keys> keys{{foo, bar}};
    constexpr PinnableWideColumns* results = nullptr;
    std::array<Status, num_keys> statuses;
    constexpr bool sorted_input = false;

    txn->MultiGetEntity(ReadOptions(), db->DefaultColumnFamily(), num_keys,
                        keys.data(), results, statuses.data(), sorted_input);

    ASSERT_TRUE(statuses[0].IsInvalidArgument());
    ASSERT_TRUE(statuses[1].IsInvalidArgument());
  }

  {
    ReadOptions read_options;
    read_options.io_activity = Env::IOActivity::kMultiGet;

    std::array<Slice, num_keys> keys{{foo, bar}};
    std::array<PinnableWideColumns, num_keys> results;
    std::array<Status, num_keys> statuses;
    constexpr bool sorted_input = false;

    txn->MultiGetEntity(read_options, db->DefaultColumnFamily(), num_keys,
                        keys.data(), results.data(), statuses.data(),
                        sorted_input);
    ASSERT_TRUE(statuses[0].IsInvalidArgument());
    ASSERT_TRUE(statuses[1].IsInvalidArgument());
  }

  {
    constexpr ColumnFamilyHandle* column_family = nullptr;
    PinnableWideColumns columns;
    ASSERT_TRUE(
        txn->GetEntityForUpdate(ReadOptions(), column_family, foo, &columns)
            .IsInvalidArgument());
  }

  {
    constexpr PinnableWideColumns* columns = nullptr;
    ASSERT_TRUE(txn->GetEntityForUpdate(ReadOptions(),
                                        db->DefaultColumnFamily(), foo, columns)
                    .IsInvalidArgument());
  }

  {
    ReadOptions read_options;
    read_options.io_activity = Env::IOActivity::kGet;

    PinnableWideColumns columns;
    ASSERT_TRUE(txn->GetEntityForUpdate(read_options, db->DefaultColumnFamily(),
                                        foo, &columns)
                    .IsInvalidArgument());
  }

  {
    txn->SetSnapshot();

    ReadOptions read_options;
    read_options.snapshot = txn->GetSnapshot();

    PinnableWideColumns columns;
    constexpr bool exclusive = true;
    constexpr bool do_validate = false;

    ASSERT_TRUE(txn->GetEntityForUpdate(read_options, db->DefaultColumnFamily(),
                                        foo, &columns, exclusive, do_validate)
                    .IsInvalidArgument());
  }
}

TEST_P(TransactionTest, PutEntityRecovery) {
  const TxnDBWritePolicy write_policy = std::get<2>(GetParam());
  if (write_policy != TxnDBWritePolicy::WRITE_COMMITTED) {
    ROCKSDB_GTEST_BYPASS("Test only WriteCommitted for now");
    return;
  }

  constexpr char foo[] = "foo";
  const WideColumns foo_columns{
      {kDefaultWideColumnName, "bar"}, {"col1", "val1"}, {"col2", "val2"}};

  constexpr char xid[] = "xid";

  {
    WriteOptions write_options;
    write_options.sync = true;
    write_options.disableWAL = false;

    std::unique_ptr<Transaction> txn(db->BeginTransaction(write_options));
    ASSERT_NE(txn, nullptr);

    ASSERT_OK(txn->SetName(xid));

    ASSERT_OK(txn->PutEntity(db->DefaultColumnFamily(), foo, foo_columns));

    ASSERT_OK(txn->Prepare());
  }

  ASSERT_OK(ReOpenNoDelete());

  {
    std::unique_ptr<Transaction> txn(db->GetTransactionByName(xid));
    ASSERT_NE(txn, nullptr);

    ASSERT_OK(txn->Commit());
  }

  {
    PinnableWideColumns columns;
    ASSERT_OK(
        db->GetEntity(ReadOptions(), db->DefaultColumnFamily(), foo, &columns));
    ASSERT_EQ(columns.columns(), foo_columns);
  }
}

TEST_F(TransactionDBTest, CollapseKey) {
  ASSERT_OK(ReOpen());
  ASSERT_OK(db->Put({}, "hello", "world"));
  ASSERT_OK(db->Flush({}));
  ASSERT_OK(db->Merge({}, "hello", "world"));
  ASSERT_OK(db->Flush({}));
  ASSERT_OK(db->Merge({}, "hello", "world"));
  ASSERT_OK(db->Flush({}));

  std::string value;
  ASSERT_OK(db->Get({}, "hello", &value));
  ASSERT_EQ("world,world,world", value);

  // get merge op info
  std::vector<PinnableSlice> operands(3);
  GetMergeOperandsOptions mergeOperandOptions;
  mergeOperandOptions.expected_max_number_of_operands = 3;
  int numOperands;
  ASSERT_OK(db->GetMergeOperands({}, db->DefaultColumnFamily(), "hello",
                                 operands.data(), &mergeOperandOptions,
                                 &numOperands));
  ASSERT_EQ(3, numOperands);

  // collapse key
  {
    std::unique_ptr<Transaction> txn0{
        db->BeginTransaction(WriteOptions{}, TransactionOptions{})};
    ASSERT_OK(txn0->CollapseKey(ReadOptions{}, "hello"));
    ASSERT_OK(txn0->Commit());
  }

  // merge operands should be 1
  ASSERT_OK(db->GetMergeOperands({}, db->DefaultColumnFamily(), "hello",
                                 operands.data(), &mergeOperandOptions,
                                 &numOperands));
  ASSERT_EQ(1, numOperands);

  // get again after collapse
  ASSERT_OK(db->Get({}, "hello", &value));
  ASSERT_EQ("world,world,world", value);

  // collapse of non-existent key
  {
    std::unique_ptr<Transaction> txn1{
        db->BeginTransaction(WriteOptions{}, TransactionOptions{})};
    ASSERT_TRUE(txn1->CollapseKey(ReadOptions{}, "dummy").IsNotFound());
  }
}

TEST_F(TransactionDBTest, FlushedLogWithPendingPrepareIsSynced) {
  // Repro for a bug where we missed a necessary sync of the old WAL during
  // memtable flush. It happened due to applying an optimization to skip syncing
  // the old WAL in too many scenarios (all memtable flushes on single CF
  // databases). That optimization is only valid when memtable flush can
  // guarantee the old WAL will not be read by crash-recovery. When the old WAL
  // contains a prepare record without its corresponding commit, that WAL will
  // be read by crash-recovery and therefore it must be synced.
  const int kStartIndex = 1000;
  int next_index = kStartIndex;
  ASSERT_OK(ReOpen());

  ASSERT_OK(
      db->Put(WriteOptions(), "key" + std::to_string(next_index), "value"));
  next_index++;

  Transaction* txn = db->BeginTransaction(WriteOptions(), TransactionOptions());
  ASSERT_OK(txn->SetName("xid" + std::to_string(next_index)));
  ASSERT_OK(
      txn->Put(Slice("key" + std::to_string(next_index)), Slice("value")));
  next_index++;
  ASSERT_OK(txn->Prepare());

  // Set it directly writable so new WAL containing the commit record will be
  // recovered despite not being explicitly synced.
  fault_fs->SetFilesystemDirectWritable(true);
  ASSERT_OK(db->Flush(FlushOptions()));

  ASSERT_OK(txn->Commit());
  delete txn;

  ASSERT_OK(
      db->Put(WriteOptions(), "key" + std::to_string(next_index), "value"));
  next_index++;

  ASSERT_OK(ReOpenNoDelete());

  for (int i = kStartIndex; i < next_index; i++) {
    PinnableSlice value;
    ASSERT_OK(db->Get(ReadOptions(), db->DefaultColumnFamily(),
                      "key" + std::to_string(i), &value));
    ASSERT_EQ("value", value);
  }
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

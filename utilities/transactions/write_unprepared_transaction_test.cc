//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#ifndef ROCKSDB_LITE

#include "utilities/transactions/transaction_test.h"
#include "utilities/transactions/write_unprepared_txn.h"
#include "utilities/transactions/write_unprepared_txn_db.h"

namespace ROCKSDB_NAMESPACE {

class WriteUnpreparedTransactionTestBase : public TransactionTestBase {
 public:
  WriteUnpreparedTransactionTestBase(bool use_stackable_db,
                                     bool two_write_queue,
                                     TxnDBWritePolicy write_policy)
      : TransactionTestBase(use_stackable_db, two_write_queue, write_policy,
                            kOrderedWrite) {}
};

class WriteUnpreparedTransactionTest
    : public WriteUnpreparedTransactionTestBase,
      virtual public ::testing::WithParamInterface<
          std::tuple<bool, bool, TxnDBWritePolicy>> {
 public:
  WriteUnpreparedTransactionTest()
      : WriteUnpreparedTransactionTestBase(std::get<0>(GetParam()),
                                           std::get<1>(GetParam()),
                                           std::get<2>(GetParam())) {}
};

INSTANTIATE_TEST_CASE_P(
    WriteUnpreparedTransactionTest, WriteUnpreparedTransactionTest,
    ::testing::Values(std::make_tuple(false, false, WRITE_UNPREPARED),
                      std::make_tuple(false, true, WRITE_UNPREPARED)));

enum StressAction { NO_SNAPSHOT, RO_SNAPSHOT, REFRESH_SNAPSHOT };
class WriteUnpreparedStressTest : public WriteUnpreparedTransactionTestBase,
                                  virtual public ::testing::WithParamInterface<
                                      std::tuple<bool, StressAction>> {
 public:
  WriteUnpreparedStressTest()
      : WriteUnpreparedTransactionTestBase(false, std::get<0>(GetParam()),
                                           WRITE_UNPREPARED),
        action_(std::get<1>(GetParam())) {}
  StressAction action_;
};

INSTANTIATE_TEST_CASE_P(
    WriteUnpreparedStressTest, WriteUnpreparedStressTest,
    ::testing::Values(std::make_tuple(false, NO_SNAPSHOT),
                      std::make_tuple(false, RO_SNAPSHOT),
                      std::make_tuple(false, REFRESH_SNAPSHOT),
                      std::make_tuple(true, NO_SNAPSHOT),
                      std::make_tuple(true, RO_SNAPSHOT),
                      std::make_tuple(true, REFRESH_SNAPSHOT)));

TEST_P(WriteUnpreparedTransactionTest, ReadYourOwnWrite) {
  // The following tests checks whether reading your own write for
  // a transaction works for write unprepared, when there are uncommitted
  // values written into DB.
  auto verify_state = [](Iterator* iter, const std::string& key,
                         const std::string& value) {
    ASSERT_TRUE(iter->Valid());
    ASSERT_OK(iter->status());
    ASSERT_EQ(key, iter->key().ToString());
    ASSERT_EQ(value, iter->value().ToString());
  };

  // Test always reseeking vs never reseeking.
  for (uint64_t max_skip : {0, std::numeric_limits<int>::max()}) {
    options.max_sequential_skip_in_iterations = max_skip;
    options.disable_auto_compactions = true;
    ASSERT_OK(ReOpen());

    TransactionOptions txn_options;
    WriteOptions woptions;
    ReadOptions roptions;

    ASSERT_OK(db->Put(woptions, "a", ""));
    ASSERT_OK(db->Put(woptions, "b", ""));

    Transaction* txn = db->BeginTransaction(woptions, txn_options);
    WriteUnpreparedTxn* wup_txn = dynamic_cast<WriteUnpreparedTxn*>(txn);
    txn->SetSnapshot();

    for (int i = 0; i < 5; i++) {
      std::string stored_value = "v" + std::to_string(i);
      ASSERT_OK(txn->Put("a", stored_value));
      ASSERT_OK(txn->Put("b", stored_value));
      ASSERT_OK(wup_txn->FlushWriteBatchToDB(false));

      // Test Get()
      std::string value;
      ASSERT_OK(txn->Get(roptions, "a", &value));
      ASSERT_EQ(value, stored_value);
      ASSERT_OK(txn->Get(roptions, "b", &value));
      ASSERT_EQ(value, stored_value);

      // Test Next()
      auto iter = txn->GetIterator(roptions);
      iter->Seek("a");
      verify_state(iter, "a", stored_value);

      iter->Next();
      verify_state(iter, "b", stored_value);

      iter->SeekToFirst();
      verify_state(iter, "a", stored_value);

      iter->Next();
      verify_state(iter, "b", stored_value);

      delete iter;

      // Test Prev()
      iter = txn->GetIterator(roptions);
      iter->SeekForPrev("b");
      verify_state(iter, "b", stored_value);

      iter->Prev();
      verify_state(iter, "a", stored_value);

      iter->SeekToLast();
      verify_state(iter, "b", stored_value);

      iter->Prev();
      verify_state(iter, "a", stored_value);

      delete iter;
    }

    delete txn;
  }
}

#if !defined(ROCKSDB_VALGRIND_RUN) || defined(ROCKSDB_FULL_VALGRIND_RUN)
TEST_P(WriteUnpreparedStressTest, ReadYourOwnWriteStress) {
  // This is a stress test where different threads are writing random keys, and
  // then before committing or aborting the transaction, it validates to see
  // that it can read the keys it wrote, and the keys it did not write respect
  // the snapshot. To avoid row lock contention (and simply stressing the
  // locking system), each thread is mostly only writing to its own set of keys.
  const uint32_t kNumIter = 1000;
  const uint32_t kNumThreads = 10;
  const uint32_t kNumKeys = 5;

  // Test with
  // 1. no snapshots set
  // 2. snapshot set on ReadOptions
  // 3. snapshot set, and refreshing after every write.
  StressAction a = action_;
  WriteOptions write_options;
  txn_db_options.transaction_lock_timeout = -1;
  options.disable_auto_compactions = true;
  ASSERT_OK(ReOpen());

  std::vector<std::string> keys;
  for (uint32_t k = 0; k < kNumKeys * kNumThreads; k++) {
    keys.push_back("k" + std::to_string(k));
  }
  RandomShuffle(keys.begin(), keys.end());

  // This counter will act as a "sequence number" to help us validate
  // visibility logic with snapshots. If we had direct access to the seqno of
  // snapshots and key/values, then we should directly compare those instead.
  std::atomic<int64_t> counter(0);

  std::function<void(uint32_t)> stress_thread = [&](int id) {
    size_t tid = std::hash<std::thread::id>()(std::this_thread::get_id());
    Random64 rnd(static_cast<uint32_t>(tid));

    Transaction* txn;
    TransactionOptions txn_options;
    // batch_size of 1 causes writes to DB for every marker.
    txn_options.write_batch_flush_threshold = 1;
    ReadOptions read_options;

    for (uint32_t i = 0; i < kNumIter; i++) {
      std::set<std::string> owned_keys(keys.begin() + id * kNumKeys,
                                       keys.begin() + (id + 1) * kNumKeys);
      // Add unowned keys to make the workload more interesting, but this
      // increases row lock contention, so just do it sometimes.
      if (rnd.OneIn(2)) {
        owned_keys.insert(keys[rnd.Uniform(kNumKeys * kNumThreads)]);
      }

      txn = db->BeginTransaction(write_options, txn_options);
      ASSERT_OK(txn->SetName(std::to_string(id)));
      txn->SetSnapshot();
      if (a >= RO_SNAPSHOT) {
        read_options.snapshot = txn->GetSnapshot();
        ASSERT_TRUE(read_options.snapshot != nullptr);
      }

      uint64_t buf[2];
      buf[0] = id;

      // When scanning through the database, make sure that all unprepared
      // keys have value >= snapshot and all other keys have value < snapshot.
      int64_t snapshot_num = counter.fetch_add(1);

      Status s;
      for (const auto& key : owned_keys) {
        buf[1] = counter.fetch_add(1);
        s = txn->Put(key, Slice((const char*)buf, sizeof(buf)));
        if (!s.ok()) {
          break;
        }
        if (a == REFRESH_SNAPSHOT) {
          txn->SetSnapshot();
          read_options.snapshot = txn->GetSnapshot();
          snapshot_num = counter.fetch_add(1);
        }
      }

      // Failure is possible due to snapshot validation. In this case,
      // rollback and move onto next iteration.
      if (!s.ok()) {
        ASSERT_TRUE(s.IsBusy());
        ASSERT_OK(txn->Rollback());
        delete txn;
        continue;
      }

      auto verify_key = [&owned_keys, &a, &id, &snapshot_num](
                            const std::string& key, const std::string& value) {
        if (owned_keys.count(key) > 0) {
          ASSERT_EQ(value.size(), 16);

          // Since this key is part of owned_keys, then this key must be
          // unprepared by this transaction identified by 'id'
          ASSERT_EQ(((int64_t*)value.c_str())[0], id);
          if (a == REFRESH_SNAPSHOT) {
            // If refresh snapshot is true, then the snapshot is refreshed
            // after every Put(), meaning that the current snapshot in
            // snapshot_num must be greater than the "seqno" of any keys
            // written by the current transaction.
            ASSERT_LT(((int64_t*)value.c_str())[1], snapshot_num);
          } else {
            // If refresh snapshot is not on, then the snapshot was taken at
            // the beginning of the transaction, meaning all writes must come
            // after snapshot_num
            ASSERT_GT(((int64_t*)value.c_str())[1], snapshot_num);
          }
        } else if (a >= RO_SNAPSHOT) {
          // If this is not an unprepared key, just assert that the key
          // "seqno" is smaller than the snapshot seqno.
          ASSERT_EQ(value.size(), 16);
          ASSERT_LT(((int64_t*)value.c_str())[1], snapshot_num);
        }
      };

      // Validate Get()/Next()/Prev(). Do only one of them to save time, and
      // reduce lock contention.
      switch (rnd.Uniform(3)) {
        case 0:  // Validate Get()
        {
          for (const auto& key : keys) {
            std::string value;
            s = txn->Get(read_options, Slice(key), &value);
            if (!s.ok()) {
              ASSERT_TRUE(s.IsNotFound());
              ASSERT_EQ(owned_keys.count(key), 0);
            } else {
              verify_key(key, value);
            }
          }
          break;
        }
        case 1:  // Validate Next()
        {
          Iterator* iter = txn->GetIterator(read_options);
          ASSERT_OK(iter->status());
          for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
            verify_key(iter->key().ToString(), iter->value().ToString());
          }
          ASSERT_OK(iter->status());
          delete iter;
          break;
        }
        case 2:  // Validate Prev()
        {
          Iterator* iter = txn->GetIterator(read_options);
          ASSERT_OK(iter->status());
          for (iter->SeekToLast(); iter->Valid(); iter->Prev()) {
            verify_key(iter->key().ToString(), iter->value().ToString());
          }
          ASSERT_OK(iter->status());
          delete iter;
          break;
        }
        default:
          FAIL();
      }

      if (rnd.OneIn(2)) {
        ASSERT_OK(txn->Commit());
      } else {
        ASSERT_OK(txn->Rollback());
      }
      delete txn;
    }
  };

  std::vector<port::Thread> threads;
  for (uint32_t i = 0; i < kNumThreads; i++) {
    threads.emplace_back(stress_thread, i);
  }

  for (auto& t : threads) {
    t.join();
  }
}
#endif  // !defined(ROCKSDB_VALGRIND_RUN) || defined(ROCKSDB_FULL_VALGRIND_RUN)

// This tests how write unprepared behaves during recovery when the DB crashes
// after a transaction has either been unprepared or prepared, and tests if
// the changes are correctly applied for prepared transactions if we decide to
// rollback/commit.
TEST_P(WriteUnpreparedTransactionTest, RecoveryTest) {
  WriteOptions write_options;
  write_options.disableWAL = false;
  TransactionOptions txn_options;
  std::vector<Transaction*> prepared_trans;
  WriteUnpreparedTxnDB* wup_db;
  options.disable_auto_compactions = true;

  enum Action { UNPREPARED, ROLLBACK, COMMIT };

  // batch_size of 1 causes writes to DB for every marker.
  for (size_t batch_size : {1, 1000000}) {
    txn_options.write_batch_flush_threshold = batch_size;
    for (bool empty : {true, false}) {
      for (Action a : {UNPREPARED, ROLLBACK, COMMIT}) {
        for (int num_batches = 1; num_batches < 10; num_batches++) {
          // Reset database.
          prepared_trans.clear();
          ASSERT_OK(ReOpen());
          wup_db = dynamic_cast<WriteUnpreparedTxnDB*>(db);
          if (!empty) {
            for (int i = 0; i < num_batches; i++) {
              ASSERT_OK(db->Put(WriteOptions(), "k" + std::to_string(i),
                                "before value" + std::to_string(i)));
            }
          }

          // Write num_batches unprepared batches.
          Transaction* txn = db->BeginTransaction(write_options, txn_options);
          WriteUnpreparedTxn* wup_txn = dynamic_cast<WriteUnpreparedTxn*>(txn);
          ASSERT_OK(txn->SetName("xid"));
          for (int i = 0; i < num_batches; i++) {
            ASSERT_OK(
                txn->Put("k" + std::to_string(i), "value" + std::to_string(i)));
            if (txn_options.write_batch_flush_threshold == 1) {
              // WriteUnprepared will check write_batch_flush_threshold and
              // possibly flush before appending to the write batch. No flush
              // will happen at the first write because the batch is still
              // empty, so after k puts, there should be k-1 flushed batches.
              ASSERT_EQ(wup_txn->GetUnpreparedSequenceNumbers().size(), i);
            } else {
              ASSERT_EQ(wup_txn->GetUnpreparedSequenceNumbers().size(), 0);
            }
          }
          if (a == UNPREPARED) {
            // This is done to prevent the destructor from rolling back the
            // transaction for us, since we want to pretend we crashed and
            // test that recovery does the rollback.
            wup_txn->unprep_seqs_.clear();
          } else {
            ASSERT_OK(txn->Prepare());
          }
          delete txn;

          // Crash and run recovery code paths.
          ASSERT_OK(wup_db->db_impl_->FlushWAL(true));
          wup_db->TEST_Crash();
          ASSERT_OK(ReOpenNoDelete());
          assert(db != nullptr);

          db->GetAllPreparedTransactions(&prepared_trans);
          ASSERT_EQ(prepared_trans.size(), a == UNPREPARED ? 0 : 1);
          if (a == ROLLBACK) {
            ASSERT_OK(prepared_trans[0]->Rollback());
            delete prepared_trans[0];
          } else if (a == COMMIT) {
            ASSERT_OK(prepared_trans[0]->Commit());
            delete prepared_trans[0];
          }

          Iterator* iter = db->NewIterator(ReadOptions());
          ASSERT_OK(iter->status());
          iter->SeekToFirst();
          // Check that DB has before values.
          if (!empty || a == COMMIT) {
            for (int i = 0; i < num_batches; i++) {
              ASSERT_TRUE(iter->Valid());
              ASSERT_EQ(iter->key().ToString(), "k" + std::to_string(i));
              if (a == COMMIT) {
                ASSERT_EQ(iter->value().ToString(),
                          "value" + std::to_string(i));
              } else {
                ASSERT_EQ(iter->value().ToString(),
                          "before value" + std::to_string(i));
              }
              iter->Next();
            }
          }
          ASSERT_FALSE(iter->Valid());
          ASSERT_OK(iter->status());
          delete iter;
        }
      }
    }
  }
}

// Basic test to see that unprepared batch gets written to DB when batch size
// is exceeded. It also does some basic checks to see if commit/rollback works
// as expected for write unprepared.
TEST_P(WriteUnpreparedTransactionTest, UnpreparedBatch) {
  WriteOptions write_options;
  TransactionOptions txn_options;
  const int kNumKeys = 10;

  // batch_size of 1 causes writes to DB for every marker.
  for (size_t batch_size : {1, 1000000}) {
    txn_options.write_batch_flush_threshold = batch_size;
    for (bool prepare : {false, true}) {
      for (bool commit : {false, true}) {
        ASSERT_OK(ReOpen());
        Transaction* txn = db->BeginTransaction(write_options, txn_options);
        WriteUnpreparedTxn* wup_txn = dynamic_cast<WriteUnpreparedTxn*>(txn);
        ASSERT_OK(txn->SetName("xid"));

        for (int i = 0; i < kNumKeys; i++) {
          ASSERT_OK(txn->Put("k" + std::to_string(i), "v" + std::to_string(i)));
          if (txn_options.write_batch_flush_threshold == 1) {
            // WriteUnprepared will check write_batch_flush_threshold and
            // possibly flush before appending to the write batch. No flush will
            // happen at the first write because the batch is still empty, so
            // after k puts, there should be k-1 flushed batches.
            ASSERT_EQ(wup_txn->GetUnpreparedSequenceNumbers().size(), i);
          } else {
            ASSERT_EQ(wup_txn->GetUnpreparedSequenceNumbers().size(), 0);
          }
        }

        if (prepare) {
          ASSERT_OK(txn->Prepare());
        }

        Iterator* iter = db->NewIterator(ReadOptions());
        ASSERT_OK(iter->status());
        iter->SeekToFirst();
        assert(!iter->Valid());
        ASSERT_FALSE(iter->Valid());
        ASSERT_OK(iter->status());
        delete iter;

        if (commit) {
          ASSERT_OK(txn->Commit());
        } else {
          ASSERT_OK(txn->Rollback());
        }
        delete txn;

        iter = db->NewIterator(ReadOptions());
        ASSERT_OK(iter->status());
        iter->SeekToFirst();

        for (int i = 0; i < (commit ? kNumKeys : 0); i++) {
          ASSERT_TRUE(iter->Valid());
          ASSERT_EQ(iter->key().ToString(), "k" + std::to_string(i));
          ASSERT_EQ(iter->value().ToString(), "v" + std::to_string(i));
          iter->Next();
        }
        ASSERT_FALSE(iter->Valid());
        ASSERT_OK(iter->status());
        delete iter;
      }
    }
  }
}

// Test whether logs containing unprepared/prepared batches are kept even
// after memtable finishes flushing, and whether they are removed when
// transaction commits/aborts.
//
// TODO(lth): Merge with TransactionTest/TwoPhaseLogRollingTest tests.
TEST_P(WriteUnpreparedTransactionTest, MarkLogWithPrepSection) {
  WriteOptions write_options;
  TransactionOptions txn_options;
  // batch_size of 1 causes writes to DB for every marker.
  txn_options.write_batch_flush_threshold = 1;
  const int kNumKeys = 10;

  WriteOptions wopts;
  wopts.sync = true;

  for (bool prepare : {false, true}) {
    for (bool commit : {false, true}) {
      ASSERT_OK(ReOpen());
      auto wup_db = dynamic_cast<WriteUnpreparedTxnDB*>(db);
      auto db_impl = wup_db->db_impl_;

      Transaction* txn1 = db->BeginTransaction(write_options, txn_options);
      ASSERT_OK(txn1->SetName("xid1"));

      Transaction* txn2 = db->BeginTransaction(write_options, txn_options);
      ASSERT_OK(txn2->SetName("xid2"));

      // Spread this transaction across multiple log files.
      for (int i = 0; i < kNumKeys; i++) {
        ASSERT_OK(txn1->Put("k1" + std::to_string(i), "v" + std::to_string(i)));
        if (i >= kNumKeys / 2) {
          ASSERT_OK(
              txn2->Put("k2" + std::to_string(i), "v" + std::to_string(i)));
        }

        if (i > 0) {
          ASSERT_OK(db_impl->TEST_SwitchWAL());
        }
      }

      ASSERT_GT(txn1->GetLogNumber(), 0);
      ASSERT_GT(txn2->GetLogNumber(), 0);

      ASSERT_EQ(db_impl->TEST_FindMinLogContainingOutstandingPrep(),
                txn1->GetLogNumber());
      ASSERT_GT(db_impl->TEST_LogfileNumber(), txn1->GetLogNumber());

      if (prepare) {
        ASSERT_OK(txn1->Prepare());
        ASSERT_OK(txn2->Prepare());
      }

      ASSERT_GE(db_impl->TEST_LogfileNumber(), txn1->GetLogNumber());
      ASSERT_GE(db_impl->TEST_LogfileNumber(), txn2->GetLogNumber());

      ASSERT_EQ(db_impl->TEST_FindMinLogContainingOutstandingPrep(),
                txn1->GetLogNumber());
      if (commit) {
        ASSERT_OK(txn1->Commit());
      } else {
        ASSERT_OK(txn1->Rollback());
      }

      ASSERT_EQ(db_impl->TEST_FindMinLogContainingOutstandingPrep(),
                txn2->GetLogNumber());

      if (commit) {
        ASSERT_OK(txn2->Commit());
      } else {
        ASSERT_OK(txn2->Rollback());
      }

      ASSERT_EQ(db_impl->TEST_FindMinLogContainingOutstandingPrep(), 0);

      delete txn1;
      delete txn2;
    }
  }
}

TEST_P(WriteUnpreparedTransactionTest, NoSnapshotWrite) {
  WriteOptions woptions;
  TransactionOptions txn_options;
  txn_options.write_batch_flush_threshold = 1;

  Transaction* txn = db->BeginTransaction(woptions, txn_options);

  // Do some writes with no snapshot
  ASSERT_OK(txn->Put("a", "a"));
  ASSERT_OK(txn->Put("b", "b"));
  ASSERT_OK(txn->Put("c", "c"));

  // Test that it is still possible to create iterators after writes with no
  // snapshot, if iterator snapshot is fresh enough.
  ReadOptions roptions;
  auto iter = txn->GetIterator(roptions);
  ASSERT_OK(iter->status());
  int keys = 0;
  for (iter->SeekToLast(); iter->Valid(); iter->Prev(), keys++) {
    ASSERT_OK(iter->status());
    ASSERT_EQ(iter->key().ToString(), iter->value().ToString());
  }
  ASSERT_EQ(keys, 3);
  ASSERT_OK(iter->status());

  delete iter;
  delete txn;
}

// Test whether write to a transaction while iterating is supported.
TEST_P(WriteUnpreparedTransactionTest, IterateAndWrite) {
  WriteOptions woptions;
  TransactionOptions txn_options;
  txn_options.write_batch_flush_threshold = 1;

  enum Action { DO_DELETE, DO_UPDATE };

  for (Action a : {DO_DELETE, DO_UPDATE}) {
    for (int i = 0; i < 100; i++) {
      ASSERT_OK(db->Put(woptions, std::to_string(i), std::to_string(i)));
    }

    Transaction* txn = db->BeginTransaction(woptions, txn_options);
    // write_batch_ now contains 1 key.
    ASSERT_OK(txn->Put("9", "a"));

    ReadOptions roptions;
    auto iter = txn->GetIterator(roptions);
    ASSERT_OK(iter->status());
    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
      ASSERT_OK(iter->status());
      if (iter->key() == "9") {
        ASSERT_EQ(iter->value().ToString(), "a");
      } else {
        ASSERT_EQ(iter->key().ToString(), iter->value().ToString());
      }

      if (a == DO_DELETE) {
        ASSERT_OK(txn->Delete(iter->key()));
      } else {
        ASSERT_OK(txn->Put(iter->key(), "b"));
      }
    }
    ASSERT_OK(iter->status());

    delete iter;
    ASSERT_OK(txn->Commit());

    iter = db->NewIterator(roptions);
    ASSERT_OK(iter->status());
    if (a == DO_DELETE) {
      // Check that db is empty.
      iter->SeekToFirst();
      ASSERT_FALSE(iter->Valid());
    } else {
      int keys = 0;
      // Check that all values are updated to b.
      for (iter->SeekToFirst(); iter->Valid(); iter->Next(), keys++) {
        ASSERT_OK(iter->status());
        ASSERT_EQ(iter->value().ToString(), "b");
      }
      ASSERT_EQ(keys, 100);
    }
    ASSERT_OK(iter->status());

    delete iter;
    delete txn;
  }
}

// Test that using an iterator after transaction clear is not supported
TEST_P(WriteUnpreparedTransactionTest, IterateAfterClear) {
  WriteOptions woptions;
  TransactionOptions txn_options;
  txn_options.write_batch_flush_threshold = 1;

  enum Action { kCommit, kRollback };

  for (Action a : {kCommit, kRollback}) {
    for (int i = 0; i < 100; i++) {
      ASSERT_OK(db->Put(woptions, std::to_string(i), std::to_string(i)));
    }

    Transaction* txn = db->BeginTransaction(woptions, txn_options);
    ASSERT_OK(txn->Put("9", "a"));

    ReadOptions roptions;
    auto iter1 = txn->GetIterator(roptions);
    auto iter2 = txn->GetIterator(roptions);
    iter1->SeekToFirst();
    iter2->Seek("9");

    // Check that iterators are valid before transaction finishes.
    ASSERT_TRUE(iter1->Valid());
    ASSERT_TRUE(iter2->Valid());
    ASSERT_OK(iter1->status());
    ASSERT_OK(iter2->status());

    if (a == kCommit) {
      ASSERT_OK(txn->Commit());
    } else {
      ASSERT_OK(txn->Rollback());
    }

    // Check that iterators are invalidated after transaction finishes.
    ASSERT_FALSE(iter1->Valid());
    ASSERT_FALSE(iter2->Valid());
    ASSERT_TRUE(iter1->status().IsInvalidArgument());
    ASSERT_TRUE(iter2->status().IsInvalidArgument());

    delete iter1;
    delete iter2;
    delete txn;
  }
}

TEST_P(WriteUnpreparedTransactionTest, SavePoint) {
  WriteOptions woptions;
  TransactionOptions txn_options;
  txn_options.write_batch_flush_threshold = 1;

  Transaction* txn = db->BeginTransaction(woptions, txn_options);
  txn->SetSavePoint();
  ASSERT_OK(txn->Put("a", "a"));
  ASSERT_OK(txn->Put("b", "b"));
  ASSERT_OK(txn->Commit());

  ReadOptions roptions;
  std::string value;
  ASSERT_OK(txn->Get(roptions, "a", &value));
  ASSERT_EQ(value, "a");
  ASSERT_OK(txn->Get(roptions, "b", &value));
  ASSERT_EQ(value, "b");
  delete txn;
}

TEST_P(WriteUnpreparedTransactionTest, UntrackedKeys) {
  WriteOptions woptions;
  TransactionOptions txn_options;
  txn_options.write_batch_flush_threshold = 1;

  Transaction* txn = db->BeginTransaction(woptions, txn_options);
  auto wb = txn->GetWriteBatch()->GetWriteBatch();
  ASSERT_OK(txn->Put("a", "a"));
  ASSERT_OK(wb->Put("a_untrack", "a_untrack"));
  txn->SetSavePoint();
  ASSERT_OK(txn->Put("b", "b"));
  ASSERT_OK(txn->Put("b_untrack", "b_untrack"));

  ReadOptions roptions;
  std::string value;
  ASSERT_OK(txn->Get(roptions, "a", &value));
  ASSERT_EQ(value, "a");
  ASSERT_OK(txn->Get(roptions, "a_untrack", &value));
  ASSERT_EQ(value, "a_untrack");
  ASSERT_OK(txn->Get(roptions, "b", &value));
  ASSERT_EQ(value, "b");
  ASSERT_OK(txn->Get(roptions, "b_untrack", &value));
  ASSERT_EQ(value, "b_untrack");

  // b and b_untrack should be rolled back.
  ASSERT_OK(txn->RollbackToSavePoint());
  ASSERT_OK(txn->Get(roptions, "a", &value));
  ASSERT_EQ(value, "a");
  ASSERT_OK(txn->Get(roptions, "a_untrack", &value));
  ASSERT_EQ(value, "a_untrack");
  auto s = txn->Get(roptions, "b", &value);
  ASSERT_TRUE(s.IsNotFound());
  s = txn->Get(roptions, "b_untrack", &value);
  ASSERT_TRUE(s.IsNotFound());

  // Everything should be rolled back.
  ASSERT_OK(txn->Rollback());
  s = txn->Get(roptions, "a", &value);
  ASSERT_TRUE(s.IsNotFound());
  s = txn->Get(roptions, "a_untrack", &value);
  ASSERT_TRUE(s.IsNotFound());
  s = txn->Get(roptions, "b", &value);
  ASSERT_TRUE(s.IsNotFound());
  s = txn->Get(roptions, "b_untrack", &value);
  ASSERT_TRUE(s.IsNotFound());

  delete txn;
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
          "SKIPPED as Transactions are not supported in ROCKSDB_LITE\n");
  return 0;
}

#endif  // ROCKSDB_LITE

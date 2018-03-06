//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif

#include <inttypes.h>
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

namespace rocksdb {

// Return true if the ith bit is set in combination represented by comb
bool IsInCombination(size_t i, size_t comb) { return comb & (size_t(1) << i); }

class TransactionTestBase : public ::testing::Test {
 public:
  TransactionDB* db;
  FaultInjectionTestEnv* env;
  std::string dbname;
  Options options;

  TransactionDBOptions txn_db_options;
  bool use_stackable_db_;

  TransactionTestBase(bool use_stackable_db, bool two_write_queue,
                      TxnDBWritePolicy write_policy)
      : use_stackable_db_(use_stackable_db) {
    options.create_if_missing = true;
    options.max_write_buffer_number = 2;
    options.write_buffer_size = 4 * 1024;
    options.level0_file_num_compaction_trigger = 2;
    options.merge_operator = MergeOperators::CreateFromStringId("stringappend");
    env = new FaultInjectionTestEnv(Env::Default());
    options.env = env;
    options.two_write_queues = two_write_queue;
    dbname = test::TmpDir() + "/transaction_testdb";

    DestroyDB(dbname, options);
    txn_db_options.transaction_lock_timeout = 0;
    txn_db_options.default_lock_timeout = 0;
    txn_db_options.write_policy = write_policy;
    Status s;
    if (use_stackable_db == false) {
      s = TransactionDB::Open(options, txn_db_options, dbname, &db);
    } else {
      s = OpenWithStackableDB();
    }
    assert(s.ok());
  }

  ~TransactionTestBase() {
    delete db;
    // This is to skip the assert statement in FaultInjectionTestEnv. There
    // seems to be a bug in btrfs that the makes readdir return recently
    // unlink-ed files. By using the default fs we simply ignore errors resulted
    // from attempting to delete such files in DestroyDB.
    options.env = Env::Default();
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
    if (use_stackable_db_ == false) {
      s = TransactionDB::Open(options, txn_db_options, dbname, &db);
    } else {
      s = OpenWithStackableDB();
    }
    return s;
  }

  Status ReOpenNoDelete(std::vector<ColumnFamilyDescriptor>& cfs,
                        std::vector<ColumnFamilyHandle*>* handles) {
    for (auto h : *handles) {
      delete h;
    }
    handles->clear();
    delete db;
    db = nullptr;
    env->AssertNoOpenFile();
    env->DropUnsyncedFileData();
    env->ResetState();
    Status s;
    if (use_stackable_db_ == false) {
      s = TransactionDB::Open(options, txn_db_options, dbname, cfs, handles,
                              &db);
    } else {
      s = OpenWithStackableDB(cfs, handles);
    }
    return s;
  }

  Status ReOpen() {
    delete db;
    DestroyDB(dbname, options);
    Status s;
    if (use_stackable_db_ == false) {
      s = TransactionDB::Open(options, txn_db_options, dbname, &db);
    } else {
      s = OpenWithStackableDB();
    }
    return s;
  }

  Status OpenWithStackableDB(std::vector<ColumnFamilyDescriptor>& cfs,
                             std::vector<ColumnFamilyHandle*>* handles) {
    std::vector<size_t> compaction_enabled_cf_indices;
    TransactionDB::PrepareWrap(&options, &cfs, &compaction_enabled_cf_indices);
    DB* root_db;
    Options options_copy(options);
    const bool use_seq_per_batch =
        txn_db_options.write_policy == WRITE_PREPARED;
    Status s = DBImpl::Open(options_copy, dbname, cfs, handles, &root_db,
                            use_seq_per_batch);
    if (s.ok()) {
      s = TransactionDB::WrapStackableDB(
          new StackableDB(root_db), txn_db_options,
          compaction_enabled_cf_indices, *handles, &db);
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
    const bool use_seq_per_batch =
        txn_db_options.write_policy == WRITE_PREPARED;
    Status s = DBImpl::Open(options_copy, dbname, column_families, &handles,
                            &root_db, use_seq_per_batch);
    if (s.ok()) {
      assert(handles.size() == 1);
      s = TransactionDB::WrapStackableDB(
          new StackableDB(root_db), txn_db_options,
          compaction_enabled_cf_indices, handles, &db);
      delete handles[0];
    }
    return s;
  }

  std::atomic<size_t> linked = {0};
  std::atomic<size_t> exp_seq = {0};
  std::atomic<size_t> commit_writes = {0};
  std::atomic<size_t> expected_commits = {0};
  std::function<void(size_t, Status)> txn_t0_with_status = [&](size_t index,
                                                               Status exp_s) {
    // Test DB's internal txn. It involves no prepare phase nor a commit marker.
    WriteOptions wopts;
    auto s = db->Put(wopts, "key" + std::to_string(index), "value");
    ASSERT_EQ(exp_s, s);
    if (txn_db_options.write_policy == TxnDBWritePolicy::WRITE_COMMITTED) {
      // Consume one seq per key
      exp_seq++;
    } else {
      // Consume one seq per batch
      exp_seq++;
      if (options.two_write_queues) {
        // Consume one seq for commit
        exp_seq++;
      }
    }
  };
  std::function<void(size_t)> txn_t0 = [&](size_t index) {
    return txn_t0_with_status(index, Status::OK());
  };
  std::function<void(size_t)> txn_t1 = [&](size_t index) {
    // Testing directly writing a write batch. Functionality-wise it is
    // equivalent to commit without prepare.
    WriteBatch wb;
    auto istr = std::to_string(index);
    wb.Put("k1" + istr, "v1");
    wb.Put("k2" + istr, "v2");
    wb.Put("k3" + istr, "v3");
    WriteOptions wopts;
    auto s = db->Write(wopts, &wb);
    if (txn_db_options.write_policy == TxnDBWritePolicy::WRITE_COMMITTED) {
      // Consume one seq per key
      exp_seq += 3;
    } else {
      // Consume one seq per batch
      exp_seq++;
      if (options.two_write_queues) {
        // Consume one seq for commit
        exp_seq++;
      }
    }
    ASSERT_OK(s);
  };
  std::function<void(size_t)> txn_t2 = [&](size_t index) {
    // Commit without prepare. It should write to DB without a commit marker.
    TransactionOptions txn_options;
    WriteOptions write_options;
    Transaction* txn = db->BeginTransaction(write_options, txn_options);
    auto istr = std::to_string(index);
    auto s = txn->SetName("xid" + istr);
    ASSERT_OK(s);
    s = txn->Put(Slice("foo" + istr), Slice("bar"));
    s = txn->Put(Slice("foo2" + istr), Slice("bar2"));
    s = txn->Put(Slice("foo3" + istr), Slice("bar3"));
    s = txn->Put(Slice("foo4" + istr), Slice("bar4"));
    ASSERT_OK(s);
    s = txn->Commit();
    ASSERT_OK(s);
    if (txn_db_options.write_policy == TxnDBWritePolicy::WRITE_COMMITTED) {
      // Consume one seq per key
      exp_seq += 4;
    } else {
      // Consume one seq per batch
      exp_seq++;
      if (options.two_write_queues) {
        // Consume one seq for commit
        exp_seq++;
      }
    }
    auto pdb = reinterpret_cast<PessimisticTransactionDB*>(db);
    pdb->UnregisterTransaction(txn);
    delete txn;
  };
  std::function<void(size_t)> txn_t3 = [&](size_t index) {
    // A full 2pc txn that also involves a commit marker.
    TransactionOptions txn_options;
    WriteOptions write_options;
    Transaction* txn = db->BeginTransaction(write_options, txn_options);
    auto istr = std::to_string(index);
    auto s = txn->SetName("xid" + istr);
    ASSERT_OK(s);
    s = txn->Put(Slice("foo" + istr), Slice("bar"));
    s = txn->Put(Slice("foo2" + istr), Slice("bar2"));
    s = txn->Put(Slice("foo3" + istr), Slice("bar3"));
    s = txn->Put(Slice("foo4" + istr), Slice("bar4"));
    s = txn->Put(Slice("foo5" + istr), Slice("bar5"));
    ASSERT_OK(s);
    expected_commits++;
    s = txn->Prepare();
    ASSERT_OK(s);
    commit_writes++;
    s = txn->Commit();
    ASSERT_OK(s);
    if (txn_db_options.write_policy == TxnDBWritePolicy::WRITE_COMMITTED) {
      // Consume one seq per key
      exp_seq += 5;
    } else {
      // Consume one seq per batch
      exp_seq++;
      // Consume one seq per commit marker
      exp_seq++;
    }
    delete txn;
  };
  std::function<void(size_t)> txn_t4 = [&](size_t index) {
    // A full 2pc txn that also involves a commit marker.
    TransactionOptions txn_options;
    WriteOptions write_options;
    Transaction* txn = db->BeginTransaction(write_options, txn_options);
    auto istr = std::to_string(index);
    auto s = txn->SetName("xid" + istr);
    ASSERT_OK(s);
    s = txn->Put(Slice("foo" + istr), Slice("bar"));
    s = txn->Put(Slice("foo2" + istr), Slice("bar2"));
    s = txn->Put(Slice("foo3" + istr), Slice("bar3"));
    s = txn->Put(Slice("foo4" + istr), Slice("bar4"));
    s = txn->Put(Slice("foo5" + istr), Slice("bar5"));
    ASSERT_OK(s);
    expected_commits++;
    s = txn->Prepare();
    ASSERT_OK(s);
    commit_writes++;
    s = txn->Rollback();
    ASSERT_OK(s);
    if (txn_db_options.write_policy == TxnDBWritePolicy::WRITE_COMMITTED) {
      // No seq is consumed for deleting the txn buffer
      exp_seq += 0;
    } else {
      // Consume one seq per batch
      exp_seq++;
      // Consume one seq per rollback batch
      exp_seq++;
      if (options.two_write_queues) {
        // Consume one seq for rollback commit
        exp_seq++;
      }
    }
    delete txn;
  };

  // Test that we can change write policy after a clean shutdown (which would
  // empty the WAL)
  void CrossCompatibilityTest(TxnDBWritePolicy from_policy,
                              TxnDBWritePolicy to_policy, bool empty_wal) {
    TransactionOptions txn_options;
    ReadOptions read_options;
    WriteOptions write_options;
    uint32_t index = 0;
    Random rnd(1103);
    options.write_buffer_size = 1024;  // To create more sst files
    std::unordered_map<std::string, std::string> committed_kvs;
    Transaction* txn;

    txn_db_options.write_policy = from_policy;
    ReOpen();

    for (int i = 0; i < 1024; i++) {
      auto istr = std::to_string(index);
      auto k = Slice("foo-" + istr).ToString();
      auto v = Slice("bar-" + istr).ToString();
      // For test the duplicate keys
      auto v2 = Slice("bar2-" + istr).ToString();
      auto type = rnd.Uniform(4);
      Status s;
      switch (type) {
        case 0:
          committed_kvs[k] = v;
          s = db->Put(write_options, k, v);
          ASSERT_OK(s);
          committed_kvs[k] = v2;
          s = db->Put(write_options, k, v2);
          ASSERT_OK(s);
          break;
        case 1: {
          WriteBatch wb;
          committed_kvs[k] = v;
          wb.Put(k, v);
          // TODO(myabandeh): remove this when we supprot duplicate keys in
          // db->Write method
          if (false) {
            committed_kvs[k] = v2;
            wb.Put(k, v2);
          }
          s = db->Write(write_options, &wb);
          ASSERT_OK(s);
        } break;
        case 2:
        case 3:
          txn = db->BeginTransaction(write_options, txn_options);
          s = txn->SetName("xid" + istr);
          ASSERT_OK(s);
          committed_kvs[k] = v;
          s = txn->Put(k, v);
          ASSERT_OK(s);
          // TODO(myabandeh): remove this when we supprot duplicate keys in
          // db->Write method
          if (false) {
            committed_kvs[k] = v2;
            s = txn->Put(k, v2);
          }
          ASSERT_OK(s);
          if (type == 3) {
            s = txn->Prepare();
            ASSERT_OK(s);
          }
          s = txn->Commit();
          ASSERT_OK(s);
          if (type == 2) {
            auto pdb = reinterpret_cast<PessimisticTransactionDB*>(db);
            // TODO(myabandeh): this is counter-intuitive. The destructor should
            // also do the unregistering.
            pdb->UnregisterTransaction(txn);
          }
          delete txn;
          break;
        default:
          assert(0);
      }

      index++;
    }  // for i

    txn_db_options.write_policy = to_policy;
    auto db_impl = reinterpret_cast<DBImpl*>(db->GetRootDB());
    // Before upgrade/downgrade the WAL must be emptied
    if (empty_wal) {
      db_impl->TEST_FlushMemTable();
    } else {
      db_impl->FlushWAL(true);
    }
    auto s = ReOpenNoDelete();
    if (empty_wal) {
      ASSERT_OK(s);
    } else {
      // Test that we can detect the WAL that is produced by an incompatbile
      // WritePolicy and fail fast before mis-interpreting the WAL.
      ASSERT_TRUE(s.IsNotSupported());
      return;
    }
    db_impl = reinterpret_cast<DBImpl*>(db->GetRootDB());
    // Check that WAL is empty
    VectorLogPtr log_files;
    db_impl->GetSortedWalFiles(log_files);
    ASSERT_EQ(0, log_files.size());

    for (auto& kv : committed_kvs) {
      std::string value;
      s = db->Get(read_options, kv.first, &value);
      if (s.IsNotFound()) {
        printf("key = %s\n", kv.first.c_str());
      }
      ASSERT_OK(s);
      if (kv.second != value) {
        printf("key = %s\n", kv.first.c_str());
      }
      ASSERT_EQ(kv.second, value);
    }
  }
};

class TransactionTest : public TransactionTestBase,
                        virtual public ::testing::WithParamInterface<
                            std::tuple<bool, bool, TxnDBWritePolicy>> {
 public:
  TransactionTest()
      : TransactionTestBase(std::get<0>(GetParam()), std::get<1>(GetParam()),
                            std::get<2>(GetParam())){};
};

class MySQLStyleTransactionTest : public TransactionTest {};

}  // namespace rocksdb

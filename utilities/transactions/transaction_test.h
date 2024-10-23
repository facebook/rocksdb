//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <algorithm>
#include <cinttypes>
#include <functional>
#include <string>
#include <thread>

#include "db/db_impl/db_impl.h"
#include "db/db_test_util.h"
#include "port/port.h"
#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/utilities/transaction.h"
#include "rocksdb/utilities/transaction_db.h"
#include "table/mock_table.h"
#include "test_util/sync_point.h"
#include "test_util/testharness.h"
#include "test_util/testutil.h"
#include "test_util/transaction_test_util.h"
#include "util/random.h"
#include "util/string_util.h"
#include "utilities/fault_injection_fs.h"
#include "utilities/merge_operators.h"
#include "utilities/merge_operators/string_append/stringappend.h"
#include "utilities/transactions/pessimistic_transaction_db.h"
#include "utilities/transactions/write_unprepared_txn_db.h"

namespace ROCKSDB_NAMESPACE {

// Return true if the ith bit is set in combination represented by comb
bool IsInCombination(size_t i, size_t comb) { return comb & (size_t(1) << i); }

enum WriteOrdering : bool { kOrderedWrite, kUnorderedWrite };

class TransactionTestBase : public ::testing::Test {
 public:
  TransactionDB* db;
  SpecialEnv special_env;
  std::shared_ptr<FaultInjectionTestFS> fault_fs;
  std::unique_ptr<Env> env;
  std::string dbname;
  Options options;

  TransactionDBOptions txn_db_options;
  bool use_stackable_db_;

  TransactionTestBase(bool use_stackable_db, bool two_write_queue,
                      TxnDBWritePolicy write_policy,
                      WriteOrdering write_ordering)
      : db(nullptr),
        special_env(Env::Default()),
        env(nullptr),
        use_stackable_db_(use_stackable_db) {
    options.create_if_missing = true;
    options.max_write_buffer_number = 2;
    options.write_buffer_size = 4 * 1024;
    options.unordered_write = write_ordering == kUnorderedWrite;
    options.level0_file_num_compaction_trigger = 2;
    options.merge_operator = MergeOperators::CreateFromStringId("stringappend");
    // Recycling log file is generally more challenging for correctness
    options.recycle_log_file_num = 2;
    special_env.skip_fsync_ = true;
    fault_fs.reset(new FaultInjectionTestFS(FileSystem::Default()));
    env.reset(new CompositeEnvWrapper(&special_env, fault_fs));
    options.env = env.get();
    options.two_write_queues = two_write_queue;
    dbname = test::PerThreadDBPath("transaction_testdb");

    EXPECT_OK(DestroyDB(dbname, options));
    txn_db_options.transaction_lock_timeout = 0;
    txn_db_options.default_lock_timeout = 0;
    txn_db_options.write_policy = write_policy;
    txn_db_options.rollback_merge_operands = true;
    // This will stress write unprepared, by forcing write batch flush on every
    // write.
    txn_db_options.default_write_batch_flush_threshold = 1;
    // Write unprepared requires all transactions to be named. This setting
    // autogenerates the name so that existing tests can pass.
    txn_db_options.autogenerate_name = true;
    Status s;
    if (use_stackable_db == false) {
      s = TransactionDB::Open(options, txn_db_options, dbname, &db);
    } else {
      s = OpenWithStackableDB();
    }
    EXPECT_OK(s);
  }

  ~TransactionTestBase() {
    delete db;
    db = nullptr;
    // This is to skip the assert statement in FaultInjectionTestEnv. There
    // seems to be a bug in btrfs that the makes readdir return recently
    // unlink-ed files. By using the default fs we simply ignore errors resulted
    // from attempting to delete such files in DestroyDB.
    if (getenv("KEEP_DB") == nullptr) {
      options.env = Env::Default();
      EXPECT_OK(DestroyDB(dbname, options));
    } else {
      fprintf(stdout, "db is still in %s\n", dbname.c_str());
    }
  }

  Status ReOpenNoDelete() {
    delete db;
    db = nullptr;
    fault_fs->AssertNoOpenFile();
    EXPECT_OK(fault_fs->DropUnsyncedFileData());
    fault_fs->ResetState();
    Status s;
    if (use_stackable_db_ == false) {
      s = TransactionDB::Open(options, txn_db_options, dbname, &db);
    } else {
      s = OpenWithStackableDB();
    }
    assert(!s.ok() || db != nullptr);
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
    fault_fs->AssertNoOpenFile();
    EXPECT_OK(fault_fs->DropUnsyncedFileData());
    fault_fs->ResetState();
    Status s;
    if (use_stackable_db_ == false) {
      s = TransactionDB::Open(options, txn_db_options, dbname, cfs, handles,
                              &db);
    } else {
      s = OpenWithStackableDB(cfs, handles);
    }
    assert(!s.ok() || db != nullptr);
    return s;
  }

  Status ReOpen() {
    delete db;
    db = nullptr;
    EXPECT_OK(DestroyDB(dbname, options));
    Status s;
    if (use_stackable_db_ == false) {
      s = TransactionDB::Open(options, txn_db_options, dbname, &db);
    } else {
      s = OpenWithStackableDB();
    }
    assert(db != nullptr);
    return s;
  }

  Status OpenWithStackableDB(std::vector<ColumnFamilyDescriptor>& cfs,
                             std::vector<ColumnFamilyHandle*>* handles) {
    std::vector<size_t> compaction_enabled_cf_indices;
    TransactionDB::PrepareWrap(&options, &cfs, &compaction_enabled_cf_indices);
    DB* root_db = nullptr;
    Options options_copy(options);
    const bool use_seq_per_batch =
        txn_db_options.write_policy == WRITE_PREPARED ||
        txn_db_options.write_policy == WRITE_UNPREPARED;
    const bool use_batch_per_txn =
        txn_db_options.write_policy == WRITE_COMMITTED ||
        txn_db_options.write_policy == WRITE_PREPARED;
    Status s = DBImpl::Open(options_copy, dbname, cfs, handles, &root_db,
                            use_seq_per_batch, use_batch_per_txn,
                            /*is_retry=*/false, /*can_retry=*/nullptr);
    auto stackable_db = std::make_unique<StackableDB>(root_db);
    if (s.ok()) {
      assert(root_db != nullptr);
      // If WrapStackableDB() returns non-ok, then stackable_db is already
      // deleted within WrapStackableDB().
      s = TransactionDB::WrapStackableDB(stackable_db.release(), txn_db_options,
                                         compaction_enabled_cf_indices,
                                         *handles, &db);
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
    DB* root_db = nullptr;
    Options options_copy(options);
    const bool use_seq_per_batch =
        txn_db_options.write_policy == WRITE_PREPARED ||
        txn_db_options.write_policy == WRITE_UNPREPARED;
    const bool use_batch_per_txn =
        txn_db_options.write_policy == WRITE_COMMITTED ||
        txn_db_options.write_policy == WRITE_PREPARED;
    Status s = DBImpl::Open(options_copy, dbname, column_families, &handles,
                            &root_db, use_seq_per_batch, use_batch_per_txn,
                            /*is_retry=*/false, /*can_retry=*/nullptr);
    if (!s.ok()) {
      delete root_db;
      return s;
    }
    StackableDB* stackable_db = new StackableDB(root_db);
    assert(root_db != nullptr);
    assert(handles.size() == 1);
    s = TransactionDB::WrapStackableDB(stackable_db, txn_db_options,
                                       compaction_enabled_cf_indices, handles,
                                       &db);
    delete handles[0];
    if (!s.ok()) {
      delete stackable_db;
    }
    return s;
  }

  std::atomic<size_t> linked = {0};
  std::atomic<size_t> exp_seq = {0};
  std::atomic<size_t> commit_writes = {0};
  std::atomic<size_t> expected_commits = {0};
  // Without Prepare, the commit does not write to WAL
  std::atomic<size_t> with_empty_commits = {0};
  void TestTxn0(size_t index) {
    // Test DB's internal txn. It involves no prepare phase nor a commit marker.
    auto s = db->Put(WriteOptions(), "key" + std::to_string(index), "value");
    ASSERT_OK(s);
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
    with_empty_commits++;
  }

  void TestTxn1(size_t index) {
    // Testing directly writing a write batch. Functionality-wise it is
    // equivalent to commit without prepare.
    WriteBatch wb;
    auto istr = std::to_string(index);
    ASSERT_OK(wb.Put("k1" + istr, "v1"));
    ASSERT_OK(wb.Put("k2" + istr, "v2"));
    ASSERT_OK(wb.Put("k3" + istr, "v3"));
    auto s = db->Write(WriteOptions(), &wb);
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
    with_empty_commits++;
  }

  void TestTxn2(size_t index) {
    // Commit without prepare. It should write to DB without a commit marker.
    Transaction* txn =
        db->BeginTransaction(WriteOptions(), TransactionOptions());
    auto istr = std::to_string(index);
    ASSERT_OK(txn->SetName("xid" + istr));
    ASSERT_OK(txn->Put(Slice("foo" + istr), Slice("bar")));
    ASSERT_OK(txn->Put(Slice("foo2" + istr), Slice("bar2")));
    ASSERT_OK(txn->Put(Slice("foo3" + istr), Slice("bar3")));
    ASSERT_OK(txn->Put(Slice("foo4" + istr), Slice("bar4")));
    ASSERT_OK(txn->Commit());
    if (txn_db_options.write_policy == TxnDBWritePolicy::WRITE_COMMITTED) {
      // Consume one seq per key
      exp_seq += 4;
    } else if (txn_db_options.write_policy ==
               TxnDBWritePolicy::WRITE_PREPARED) {
      // Consume one seq per batch
      exp_seq++;
      if (options.two_write_queues) {
        // Consume one seq for commit
        exp_seq++;
      }
    } else {
      // Flushed after each key, consume one seq per flushed batch
      exp_seq += 4;
      // WriteUnprepared implements CommitWithoutPrepareInternal by simply
      // calling Prepare then Commit. Consume one seq for the prepare.
      exp_seq++;
    }
    delete txn;
    with_empty_commits++;
  }

  void TestTxn3(size_t index) {
    // A full 2pc txn that also involves a commit marker.
    Transaction* txn =
        db->BeginTransaction(WriteOptions(), TransactionOptions());
    auto istr = std::to_string(index);
    ASSERT_OK(txn->SetName("xid" + istr));
    ASSERT_OK(txn->Put(Slice("foo" + istr), Slice("bar")));
    ASSERT_OK(txn->Put(Slice("foo2" + istr), Slice("bar2")));
    ASSERT_OK(txn->Put(Slice("foo3" + istr), Slice("bar3")));
    ASSERT_OK(txn->Put(Slice("foo4" + istr), Slice("bar4")));
    ASSERT_OK(txn->Put(Slice("foo5" + istr), Slice("bar5")));
    expected_commits++;
    ASSERT_OK(txn->Prepare());
    commit_writes++;
    ASSERT_OK(txn->Commit());
    if (txn_db_options.write_policy == TxnDBWritePolicy::WRITE_COMMITTED) {
      // Consume one seq per key
      exp_seq += 5;
    } else if (txn_db_options.write_policy ==
               TxnDBWritePolicy::WRITE_PREPARED) {
      // Consume one seq per batch
      exp_seq++;
      // Consume one seq per commit marker
      exp_seq++;
    } else {
      // Flushed after each key, consume one seq per flushed batch
      exp_seq += 5;
      // Consume one seq per commit marker
      exp_seq++;
    }
    delete txn;
  }

  void TestTxn4(size_t index) {
    // A full 2pc txn that also involves a commit marker.
    Transaction* txn =
        db->BeginTransaction(WriteOptions(), TransactionOptions());
    auto istr = std::to_string(index);
    ASSERT_OK(txn->SetName("xid" + istr));
    ASSERT_OK(txn->Put(Slice("foo" + istr), Slice("bar")));
    ASSERT_OK(txn->Put(Slice("foo2" + istr), Slice("bar2")));
    ASSERT_OK(txn->Put(Slice("foo3" + istr), Slice("bar3")));
    ASSERT_OK(txn->Put(Slice("foo4" + istr), Slice("bar4")));
    ASSERT_OK(txn->Put(Slice("foo5" + istr), Slice("bar5")));
    expected_commits++;
    ASSERT_OK(txn->Prepare());
    commit_writes++;
    ASSERT_OK(txn->Rollback());
    if (txn_db_options.write_policy == TxnDBWritePolicy::WRITE_COMMITTED) {
      // No seq is consumed for deleting the txn buffer
      exp_seq += 0;
    } else if (txn_db_options.write_policy ==
               TxnDBWritePolicy::WRITE_PREPARED) {
      // Consume one seq per batch
      exp_seq++;
      // Consume one seq per rollback batch
      exp_seq++;
      if (options.two_write_queues) {
        // Consume one seq for rollback commit
        exp_seq++;
      }
    } else {
      // Flushed after each key, consume one seq per flushed batch
      exp_seq += 5;
      // Consume one seq per rollback batch
      exp_seq++;
      if (options.two_write_queues) {
        // Consume one seq for rollback commit
        exp_seq++;
      }
    }
    delete txn;
  }

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
    if (txn_db_options.write_policy == WRITE_COMMITTED) {
      options.unordered_write = false;
    }
    ASSERT_OK(ReOpen());

    for (int i = 0; i < 1024; i++) {
      auto istr = std::to_string(index);
      auto k = Slice("foo-" + istr).ToString();
      auto v = Slice("bar-" + istr).ToString();
      // For test the duplicate keys
      auto v2 = Slice("bar2-" + istr).ToString();
      auto type = rnd.Uniform(4);
      switch (type) {
        case 0:
          committed_kvs[k] = v;
          ASSERT_OK(db->Put(write_options, k, v));
          committed_kvs[k] = v2;
          ASSERT_OK(db->Put(write_options, k, v2));
          break;
        case 1: {
          WriteBatch wb;
          committed_kvs[k] = v;
          ASSERT_OK(wb.Put(k, v));
          committed_kvs[k] = v2;
          ASSERT_OK(wb.Put(k, v2));
          ASSERT_OK(db->Write(write_options, &wb));

        } break;
        case 2:
        case 3:
          txn = db->BeginTransaction(write_options, txn_options);
          ASSERT_OK(txn->SetName("xid" + istr));
          committed_kvs[k] = v;
          ASSERT_OK(txn->Put(k, v));
          committed_kvs[k] = v2;
          ASSERT_OK(txn->Put(k, v2));

          if (type == 3) {
            ASSERT_OK(txn->Prepare());
          }
          ASSERT_OK(txn->Commit());
          delete txn;
          break;
        default:
          FAIL();
      }

      index++;
    }  // for i

    txn_db_options.write_policy = to_policy;
    if (txn_db_options.write_policy == WRITE_COMMITTED) {
      options.unordered_write = false;
    }
    auto db_impl = static_cast_with_check<DBImpl>(db->GetRootDB());
    // Before upgrade/downgrade the WAL must be emptied
    if (empty_wal) {
      ASSERT_OK(db_impl->TEST_FlushMemTable());
    } else {
      ASSERT_OK(db_impl->FlushWAL(true));
    }
    auto s = ReOpenNoDelete();
    if (empty_wal) {
      ASSERT_OK(s);
    } else {
      // Test that we can detect the WAL that is produced by an incompatible
      // WritePolicy and fail fast before mis-interpreting the WAL.
      ASSERT_TRUE(s.IsNotSupported());
      return;
    }
    db_impl = static_cast_with_check<DBImpl>(db->GetRootDB());
    // Check that WAL is empty
    VectorWalPtr log_files;
    ASSERT_OK(db_impl->GetSortedWalFiles(log_files));
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

class TransactionTest
    : public TransactionTestBase,
      virtual public ::testing::WithParamInterface<
          std::tuple<bool, bool, TxnDBWritePolicy, WriteOrdering>> {
 public:
  TransactionTest()
      : TransactionTestBase(std::get<0>(GetParam()), std::get<1>(GetParam()),
                            std::get<2>(GetParam()), std::get<3>(GetParam())){};
};

class TransactionDBTest : public TransactionTestBase {
 public:
  TransactionDBTest()
      : TransactionTestBase(false, false, WRITE_COMMITTED, kOrderedWrite) {}
};

class TransactionStressTest : public TransactionTest {};

class MySQLStyleTransactionTest
    : public TransactionTestBase,
      virtual public ::testing::WithParamInterface<
          std::tuple<bool, bool, TxnDBWritePolicy, WriteOrdering, bool>> {
 public:
  MySQLStyleTransactionTest()
      : TransactionTestBase(std::get<0>(GetParam()), std::get<1>(GetParam()),
                            std::get<2>(GetParam()), std::get<3>(GetParam())),
        with_slow_threads_(std::get<4>(GetParam())) {
    if (with_slow_threads_ &&
        (txn_db_options.write_policy == WRITE_PREPARED ||
         txn_db_options.write_policy == WRITE_UNPREPARED)) {
      // The corner case with slow threads involves the caches filling
      // over which would not happen even with artifial delays. To help
      // such cases to show up we lower the size of the cache-related data
      // structures.
      txn_db_options.wp_snapshot_cache_bits = 1;
      txn_db_options.wp_commit_cache_bits = 10;
      options.write_buffer_size = 1024;
      EXPECT_OK(ReOpen());
    }
  };

 protected:
  // Also emulate slow threads by addin artiftial delays
  const bool with_slow_threads_;
};

class WriteCommittedTxnWithTsTest
    : public TransactionTestBase,
      public ::testing::WithParamInterface<std::tuple<bool, bool, bool>> {
 public:
  WriteCommittedTxnWithTsTest()
      : TransactionTestBase(std::get<0>(GetParam()), std::get<1>(GetParam()),
                            WRITE_COMMITTED, kOrderedWrite) {}
  ~WriteCommittedTxnWithTsTest() override {
    for (auto* h : handles_) {
      delete h;
    }
  }

  Status GetFromDb(ReadOptions read_opts, ColumnFamilyHandle* column_family,
                   const Slice& key, TxnTimestamp ts, std::string* value) {
    std::string ts_buf;
    PutFixed64(&ts_buf, ts);
    Slice ts_slc = ts_buf;
    read_opts.timestamp = &ts_slc;
    assert(db);
    return db->Get(read_opts, column_family, key, value);
  }

  Transaction* NewTxn(WriteOptions write_opts, TransactionOptions txn_opts) {
    assert(db);
    auto* txn = db->BeginTransaction(write_opts, txn_opts);
    assert(txn);
    const bool enable_indexing = std::get<2>(GetParam());
    if (enable_indexing) {
      txn->EnableIndexing();
    } else {
      txn->DisableIndexing();
    }
    return txn;
  }

 protected:
  std::vector<ColumnFamilyHandle*> handles_{};
};

class TimestampedSnapshotWithTsSanityCheck
    : public TransactionTestBase,
      public ::testing::WithParamInterface<
          std::tuple<bool, bool, TxnDBWritePolicy, WriteOrdering>> {
 public:
  explicit TimestampedSnapshotWithTsSanityCheck()
      : TransactionTestBase(std::get<0>(GetParam()), std::get<1>(GetParam()),
                            std::get<2>(GetParam()), std::get<3>(GetParam())) {}
  ~TimestampedSnapshotWithTsSanityCheck() override {
    for (auto* h : handles_) {
      delete h;
    }
  }

 protected:
  std::vector<ColumnFamilyHandle*> handles_{};
};

}  // namespace ROCKSDB_NAMESPACE

//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
#include "db/write_callback.h"

#include <atomic>
#include <functional>
#include <string>
#include <utility>
#include <vector>

#include "db/db_impl/db_impl.h"
#include "port/port.h"
#include "rocksdb/db.h"
#include "rocksdb/user_write_callback.h"
#include "rocksdb/write_batch.h"
#include "test_util/sync_point.h"
#include "test_util/testharness.h"
#include "util/random.h"

using std::string;

namespace ROCKSDB_NAMESPACE {

class WriteCallbackTest : public testing::Test {
 public:
  string dbname;

  WriteCallbackTest() {
    dbname = test::PerThreadDBPath("write_callback_testdb");
  }
};

class WriteCallbackTestWriteCallback1 : public WriteCallback {
 public:
  bool was_called = false;

  Status Callback(DB* db) override {
    was_called = true;

    // Make sure db is a DBImpl
    DBImpl* db_impl = dynamic_cast<DBImpl*>(db);
    if (db_impl == nullptr) {
      return Status::InvalidArgument("");
    }

    return Status::OK();
  }

  bool AllowWriteBatching() override { return true; }
};

class WriteCallbackTestWriteCallback2 : public WriteCallback {
 public:
  Status Callback(DB* /*db*/) override { return Status::Busy(); }
  bool AllowWriteBatching() override { return true; }
};

class MockWriteCallback : public WriteCallback {
 public:
  bool should_fail_ = false;
  bool allow_batching_ = false;
  std::atomic<bool> was_called_{false};

  MockWriteCallback() = default;

  MockWriteCallback(const MockWriteCallback& other) {
    should_fail_ = other.should_fail_;
    allow_batching_ = other.allow_batching_;
    was_called_.store(other.was_called_.load());
  }

  Status Callback(DB* /*db*/) override {
    was_called_.store(true);
    if (should_fail_) {
      return Status::Busy();
    } else {
      return Status::OK();
    }
  }

  bool AllowWriteBatching() override { return allow_batching_; }
};

class MockUserWriteCallback : public UserWriteCallback {
 public:
  std::atomic<bool> write_enqueued_{false};
  std::atomic<bool> wal_write_done_{false};

  MockUserWriteCallback() = default;

  MockUserWriteCallback(const MockUserWriteCallback& other) {
    write_enqueued_.store(other.write_enqueued_.load());
    wal_write_done_.store(other.wal_write_done_.load());
  }

  void OnWriteEnqueued() override { write_enqueued_.store(true); }

  void OnWalWriteFinish() override { wal_write_done_.store(true); }

  void Reset() {
    write_enqueued_.store(false);
    wal_write_done_.store(false);
  }
};

#if !defined(ROCKSDB_VALGRIND_RUN) || defined(ROCKSDB_FULL_VALGRIND_RUN)
class WriteCallbackPTest
    : public WriteCallbackTest,
      public ::testing::WithParamInterface<
          std::tuple<bool, bool, bool, bool, bool, bool, bool>> {
 public:
  WriteCallbackPTest() {
    std::tie(unordered_write_, seq_per_batch_, two_queues_, allow_parallel_,
             allow_batching_, enable_WAL_, enable_pipelined_write_) =
        GetParam();
  }

 protected:
  bool unordered_write_;
  bool seq_per_batch_;
  bool two_queues_;
  bool allow_parallel_;
  bool allow_batching_;
  bool enable_WAL_;
  bool enable_pipelined_write_;
};

TEST_P(WriteCallbackPTest, WriteWithCallbackTest) {
  struct WriteOP {
    WriteOP(bool should_fail = false) { callback_.should_fail_ = should_fail; }

    void Put(const string& key, const string& val) {
      kvs_.emplace_back(key, val);
      ASSERT_OK(write_batch_.Put(key, val));
    }

    void Clear() {
      kvs_.clear();
      write_batch_.Clear();
      callback_.was_called_.store(false);
      user_write_cb_.Reset();
    }

    MockWriteCallback callback_;
    MockUserWriteCallback user_write_cb_;
    WriteBatch write_batch_;
    std::vector<std::pair<string, string>> kvs_;
  };

  // In each scenario we'll launch multiple threads to write.
  // The size of each array equals to number of threads, and
  // each boolean in it denote whether callback of corresponding
  // thread should succeed or fail.
  std::vector<std::vector<WriteOP>> write_scenarios = {
      {true},
      {false},
      {false, false},
      {true, true},
      {true, false},
      {false, true},
      {false, false, false},
      {true, true, true},
      {false, true, false},
      {true, false, true},
      {true, false, false, false, false},
      {false, false, false, false, true},
      {false, false, true, false, true},
  };

  for (auto& write_group : write_scenarios) {
    Options options;
    options.create_if_missing = true;
    options.unordered_write = unordered_write_;
    options.allow_concurrent_memtable_write = allow_parallel_;
    options.enable_pipelined_write = enable_pipelined_write_;
    options.two_write_queues = two_queues_;
    // Skip unsupported combinations
    if (options.enable_pipelined_write && seq_per_batch_) {
      continue;
    }
    if (options.enable_pipelined_write && options.two_write_queues) {
      continue;
    }
    if (options.unordered_write && !options.allow_concurrent_memtable_write) {
      continue;
    }
    if (options.unordered_write && options.enable_pipelined_write) {
      continue;
    }

    ReadOptions read_options;
    DB* db;
    DBImpl* db_impl;

    ASSERT_OK(DestroyDB(dbname, options));

    DBOptions db_options(options);
    ColumnFamilyOptions cf_options(options);
    std::vector<ColumnFamilyDescriptor> column_families;
    column_families.emplace_back(kDefaultColumnFamilyName, cf_options);
    std::vector<ColumnFamilyHandle*> handles;
    auto open_s = DBImpl::Open(db_options, dbname, column_families, &handles,
                               &db, seq_per_batch_, true /* batch_per_txn */,
                               false /* is_retry */, nullptr /* can_retry */);
    ASSERT_OK(open_s);
    assert(handles.size() == 1);
    delete handles[0];

    db_impl = dynamic_cast<DBImpl*>(db);
    ASSERT_TRUE(db_impl);

    // Writers that have called JoinBatchGroup.
    std::atomic<uint64_t> threads_joining(0);
    // Writers that have linked to the queue
    std::atomic<uint64_t> threads_linked(0);
    // Writers that pass WriteThread::JoinBatchGroup:Wait sync-point.
    std::atomic<uint64_t> threads_verified(0);

    std::atomic<uint64_t> seq(db_impl->GetLatestSequenceNumber());
    ASSERT_EQ(db_impl->GetLatestSequenceNumber(), 0);

    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
        "WriteThread::JoinBatchGroup:Start", [&](void*) {
          uint64_t cur_threads_joining = threads_joining.fetch_add(1);
          // Wait for the last joined writer to link to the queue.
          // In this way the writers link to the queue one by one.
          // This allows us to confidently detect the first writer
          // who increases threads_linked as the leader.
          while (threads_linked.load() < cur_threads_joining) {
          }
        });

    // Verification once writers call JoinBatchGroup.
    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
        "WriteThread::JoinBatchGroup:Wait", [&](void* arg) {
          uint64_t cur_threads_linked = threads_linked.fetch_add(1);
          bool is_leader = false;
          bool is_last = false;

          // who am i
          is_leader = (cur_threads_linked == 0);
          is_last = (cur_threads_linked == write_group.size() - 1);

          // check my state
          auto* writer = static_cast<WriteThread::Writer*>(arg);

          if (is_leader) {
            ASSERT_TRUE(writer->state ==
                        WriteThread::State::STATE_GROUP_LEADER);
          } else {
            ASSERT_TRUE(writer->state == WriteThread::State::STATE_INIT);
          }

          // (meta test) the first WriteOP should indeed be the first
          // and the last should be the last (all others can be out of
          // order)
          if (is_leader) {
            ASSERT_TRUE(writer->callback->Callback(nullptr).ok() ==
                        !write_group.front().callback_.should_fail_);
          } else if (is_last) {
            ASSERT_TRUE(writer->callback->Callback(nullptr).ok() ==
                        !write_group.back().callback_.should_fail_);
          }

          threads_verified.fetch_add(1);
          // Wait here until all verification in this sync-point
          // callback finish for all writers.
          while (threads_verified.load() < write_group.size()) {
          }
        });

    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
        "WriteThread::JoinBatchGroup:DoneWaiting", [&](void* arg) {
          // check my state
          auto* writer = static_cast<WriteThread::Writer*>(arg);

          if (!allow_batching_) {
            // no batching so everyone should be a leader
            ASSERT_TRUE(writer->state ==
                        WriteThread::State::STATE_GROUP_LEADER);
          } else if (!allow_parallel_) {
            ASSERT_TRUE(writer->state == WriteThread::State::STATE_COMPLETED ||
                        (enable_pipelined_write_ &&
                         writer->state ==
                             WriteThread::State::STATE_MEMTABLE_WRITER_LEADER));
          }
        });

    std::atomic<uint32_t> thread_num(0);
    std::atomic<char> dummy_key(0);

    // Each write thread create a random write batch and write to DB
    // with a write callback.
    std::function<void()> write_with_callback_func = [&]() {
      uint32_t i = thread_num.fetch_add(1);
      Random rnd(i);

      // leaders gotta lead
      while (i > 0 && threads_verified.load() < 1) {
      }

      // loser has to lose
      while (i == write_group.size() - 1 &&
             threads_verified.load() < write_group.size() - 1) {
      }

      auto& write_op = write_group.at(i);
      write_op.Clear();
      write_op.callback_.allow_batching_ = allow_batching_;

      // insert some keys
      for (uint32_t j = 0; j < rnd.Next() % 50; j++) {
        // grab unique key
        char my_key = dummy_key.fetch_add(1);

        string skey(5, my_key);
        string sval(10, my_key);
        write_op.Put(skey, sval);

        if (!write_op.callback_.should_fail_ && !seq_per_batch_) {
          seq.fetch_add(1);
        }
      }
      if (!write_op.callback_.should_fail_ && seq_per_batch_) {
        seq.fetch_add(1);
      }

      WriteOptions woptions;
      woptions.disableWAL = !enable_WAL_;
      woptions.sync = enable_WAL_;
      if (woptions.protection_bytes_per_key > 0) {
        ASSERT_OK(WriteBatchInternal::UpdateProtectionInfo(
            &write_op.write_batch_, woptions.protection_bytes_per_key));
      }
      Status s;
      if (seq_per_batch_) {
        class PublishSeqCallback : public PreReleaseCallback {
         public:
          PublishSeqCallback(DBImpl* db_impl_in) : db_impl_(db_impl_in) {}
          Status Callback(SequenceNumber last_seq, bool /*not used*/, uint64_t,
                          size_t /*index*/, size_t /*total*/) override {
            db_impl_->SetLastPublishedSequence(last_seq);
            return Status::OK();
          }
          DBImpl* db_impl_;
        } publish_seq_callback(db_impl);
        // seq_per_batch_ requires a natural batch separator or Noop
        ASSERT_OK(WriteBatchInternal::InsertNoop(&write_op.write_batch_));
        const size_t ONE_BATCH = 1;
        s = db_impl->WriteImpl(woptions, &write_op.write_batch_,
                               &write_op.callback_, &write_op.user_write_cb_,
                               nullptr, 0, false, nullptr, ONE_BATCH,
                               two_queues_ ? &publish_seq_callback : nullptr);
      } else {
        s = db_impl->WriteWithCallback(woptions, &write_op.write_batch_,
                                       &write_op.callback_,
                                       &write_op.user_write_cb_);
      }

      ASSERT_TRUE(write_op.user_write_cb_.write_enqueued_.load());
      if (write_op.callback_.should_fail_) {
        ASSERT_TRUE(s.IsBusy());
        ASSERT_FALSE(write_op.user_write_cb_.wal_write_done_.load());
      } else {
        ASSERT_OK(s);
        if (enable_WAL_) {
          ASSERT_TRUE(write_op.user_write_cb_.wal_write_done_.load());
        } else {
          ASSERT_FALSE(write_op.user_write_cb_.wal_write_done_.load());
        }
      }
    };

    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

    // do all the writes
    std::vector<port::Thread> threads;
    for (uint32_t i = 0; i < write_group.size(); i++) {
      threads.emplace_back(write_with_callback_func);
    }
    for (auto& t : threads) {
      t.join();
    }

    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();

    // check for keys
    string value;
    for (auto& w : write_group) {
      ASSERT_TRUE(w.callback_.was_called_.load());
      for (auto& kvp : w.kvs_) {
        if (w.callback_.should_fail_) {
          ASSERT_TRUE(db->Get(read_options, kvp.first, &value).IsNotFound());
        } else {
          ASSERT_OK(db->Get(read_options, kvp.first, &value));
          ASSERT_EQ(value, kvp.second);
        }
      }
    }

    ASSERT_EQ(seq.load(), db_impl->TEST_GetLastVisibleSequence());

    delete db;
    ASSERT_OK(DestroyDB(dbname, options));
  }
}

INSTANTIATE_TEST_CASE_P(WriteCallbackPTest, WriteCallbackPTest,
                        ::testing::Combine(::testing::Bool(), ::testing::Bool(),
                                           ::testing::Bool(), ::testing::Bool(),
                                           ::testing::Bool(), ::testing::Bool(),
                                           ::testing::Bool()));
#endif  // !defined(ROCKSDB_VALGRIND_RUN) || defined(ROCKSDB_FULL_VALGRIND_RUN)

TEST_F(WriteCallbackTest, WriteCallBackTest) {
  Options options;
  WriteOptions write_options;
  ReadOptions read_options;
  string value;
  DB* db;
  DBImpl* db_impl;

  ASSERT_OK(DestroyDB(dbname, options));

  options.create_if_missing = true;
  Status s = DB::Open(options, dbname, &db);
  ASSERT_OK(s);

  db_impl = dynamic_cast<DBImpl*>(db);
  ASSERT_TRUE(db_impl);

  WriteBatch wb;

  ASSERT_OK(wb.Put("a", "value.a"));
  ASSERT_OK(wb.Delete("x"));

  // Test a simple Write
  s = db->Write(write_options, &wb);
  ASSERT_OK(s);

  s = db->Get(read_options, "a", &value);
  ASSERT_OK(s);
  ASSERT_EQ("value.a", value);

  // Test WriteWithCallback
  WriteCallbackTestWriteCallback1 callback1;
  WriteBatch wb2;

  ASSERT_OK(wb2.Put("a", "value.a2"));

  s = db_impl->WriteWithCallback(write_options, &wb2, &callback1);
  ASSERT_OK(s);
  ASSERT_TRUE(callback1.was_called);

  s = db->Get(read_options, "a", &value);
  ASSERT_OK(s);
  ASSERT_EQ("value.a2", value);

  // Test WriteWithCallback for a callback that fails
  WriteCallbackTestWriteCallback2 callback2;
  WriteBatch wb3;

  ASSERT_OK(wb3.Put("a", "value.a3"));

  s = db_impl->WriteWithCallback(write_options, &wb3, &callback2);
  ASSERT_NOK(s);

  s = db->Get(read_options, "a", &value);
  ASSERT_OK(s);
  ASSERT_EQ("value.a2", value);

  MockUserWriteCallback user_write_cb;
  WriteBatch wb4;
  ASSERT_OK(wb4.Put("a", "value.a4"));

  ASSERT_OK(db->WriteWithCallback(write_options, &wb4, &user_write_cb));
  ASSERT_OK(db->Get(read_options, "a", &value));
  ASSERT_EQ(value, "value.a4");
  ASSERT_TRUE(user_write_cb.write_enqueued_.load());
  ASSERT_TRUE(user_write_cb.wal_write_done_.load());

  delete db;
  ASSERT_OK(DestroyDB(dbname, options));
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

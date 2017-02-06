//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#ifndef ROCKSDB_LITE

#include <functional>
#include <string>
#include <utility>
#include <vector>

#include "db/db_impl.h"
#include "db/write_callback.h"
#include "rocksdb/db.h"
#include "rocksdb/write_batch.h"
#include "port/port.h"
#include "util/logging.h"
#include "util/sync_point.h"
#include "util/testharness.h"

using std::string;

namespace rocksdb {

class WriteCallbackTest : public testing::Test {
 public:
  string dbname;

  WriteCallbackTest() {
    dbname = test::TmpDir() + "/write_callback_testdb";
  }
};

class WriteCallbackTestWriteCallback1 : public WriteCallback {
 public:
  bool was_called = false;

  Status Callback(DB *db) override {
    was_called = true;

    // Make sure db is a DBImpl
    DBImpl* db_impl = dynamic_cast<DBImpl*> (db);
    if (db_impl == nullptr) {
      return Status::InvalidArgument("");
    }

    return Status::OK();
  }

  bool AllowWriteBatching() override { return true; }
};

class WriteCallbackTestWriteCallback2 : public WriteCallback {
 public:
  Status Callback(DB *db) override {
    return Status::Busy();
  }
  bool AllowWriteBatching() override { return true; }
};

class MockWriteCallback : public WriteCallback {
 public:
  bool should_fail_ = false;
  bool was_called_ = false;
  bool allow_batching_ = false;

  Status Callback(DB* db) override {
    was_called_ = true;
    if (should_fail_) {
      return Status::Busy();
    } else {
      return Status::OK();
    }
  }

  bool AllowWriteBatching() override { return allow_batching_; }
};

TEST_F(WriteCallbackTest, WriteWithCallbackTest) {
  struct WriteOP {
    WriteOP(bool should_fail = false) { callback_.should_fail_ = should_fail; }

    void Put(const string& key, const string& val) {
      kvs_.push_back(std::make_pair(key, val));
      write_batch_.Put(key, val);
    }

    void Clear() {
      kvs_.clear();
      write_batch_.Clear();
      callback_.was_called_ = false;
    }

    MockWriteCallback callback_;
    WriteBatch write_batch_;
    std::vector<std::pair<string, string>> kvs_;
  };

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

  for (auto& allow_parallel : {true, false}) {
    for (auto& allow_batching : {true, false}) {
      for (auto& enable_WAL : {true, false}) {
        for (auto& write_group : write_scenarios) {
          Options options;
          options.create_if_missing = true;
          options.allow_concurrent_memtable_write = allow_parallel;

          ReadOptions read_options;
          DB* db;
          DBImpl* db_impl;

          DestroyDB(dbname, options);
          ASSERT_OK(DB::Open(options, dbname, &db));

          db_impl = dynamic_cast<DBImpl*>(db);
          ASSERT_TRUE(db_impl);

          std::atomic<uint64_t> threads_waiting(0);
          std::atomic<uint64_t> seq(db_impl->GetLatestSequenceNumber());
          ASSERT_EQ(db_impl->GetLatestSequenceNumber(), 0);

          rocksdb::SyncPoint::GetInstance()->SetCallBack(
              "WriteThread::JoinBatchGroup:Wait", [&](void* arg) {
                uint64_t cur_threads_waiting = 0;
                bool is_leader = false;
                bool is_last = false;

                // who am i
                do {
                  cur_threads_waiting = threads_waiting.load();
                  is_leader = (cur_threads_waiting == 0);
                  is_last = (cur_threads_waiting == write_group.size() - 1);
                } while (!threads_waiting.compare_exchange_strong(
                    cur_threads_waiting, cur_threads_waiting + 1));

                // check my state
                auto* writer = reinterpret_cast<WriteThread::Writer*>(arg);

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

                // wait for friends
                while (threads_waiting.load() < write_group.size()) {
                }
              });

          rocksdb::SyncPoint::GetInstance()->SetCallBack(
              "WriteThread::JoinBatchGroup:DoneWaiting", [&](void* arg) {
                // check my state
                auto* writer = reinterpret_cast<WriteThread::Writer*>(arg);

                if (!allow_batching) {
                  // no batching so everyone should be a leader
                  ASSERT_TRUE(writer->state ==
                              WriteThread::State::STATE_GROUP_LEADER);
                } else if (!allow_parallel) {
                  ASSERT_TRUE(writer->state ==
                              WriteThread::State::STATE_COMPLETED);
                }
              });

          std::atomic<uint32_t> thread_num(0);
          std::atomic<char> dummy_key(0);
          std::function<void()> write_with_callback_func = [&]() {
            uint32_t i = thread_num.fetch_add(1);
            Random rnd(i);

            // leaders gotta lead
            while (i > 0 && threads_waiting.load() < 1) {
            }

            // loser has to lose
            while (i == write_group.size() - 1 &&
                   threads_waiting.load() < write_group.size() - 1) {
            }

            auto& write_op = write_group.at(i);
            write_op.Clear();
            write_op.callback_.allow_batching_ = allow_batching;

            // insert some keys
            for (uint32_t j = 0; j < rnd.Next() % 50; j++) {
              // grab unique key
              char my_key = 0;
              do {
                my_key = dummy_key.load();
              } while (!dummy_key.compare_exchange_strong(my_key, my_key + 1));

              string skey(5, my_key);
              string sval(10, my_key);
              write_op.Put(skey, sval);

              if (!write_op.callback_.should_fail_) {
                seq.fetch_add(1);
              }
            }

            WriteOptions woptions;
            woptions.disableWAL = !enable_WAL;
            woptions.sync = enable_WAL;
            Status s = db_impl->WriteWithCallback(
                woptions, &write_op.write_batch_, &write_op.callback_);

            if (write_op.callback_.should_fail_) {
              ASSERT_TRUE(s.IsBusy());
            } else {
              ASSERT_OK(s);
            }
          };

          rocksdb::SyncPoint::GetInstance()->EnableProcessing();

          // do all the writes
          std::vector<port::Thread> threads;
          for (uint32_t i = 0; i < write_group.size(); i++) {
            threads.emplace_back(write_with_callback_func);
          }
          for (auto& t : threads) {
            t.join();
          }

          rocksdb::SyncPoint::GetInstance()->DisableProcessing();

          // check for keys
          string value;
          for (auto& w : write_group) {
            ASSERT_TRUE(w.callback_.was_called_);
            for (auto& kvp : w.kvs_) {
              if (w.callback_.should_fail_) {
                ASSERT_TRUE(
                    db->Get(read_options, kvp.first, &value).IsNotFound());
              } else {
                ASSERT_OK(db->Get(read_options, kvp.first, &value));
                ASSERT_EQ(value, kvp.second);
              }
            }
          }

          ASSERT_EQ(seq.load(), db_impl->GetLatestSequenceNumber());

          delete db;
          DestroyDB(dbname, options);
        }
      }
    }
  }
}

TEST_F(WriteCallbackTest, WriteCallBackTest) {
  Options options;
  WriteOptions write_options;
  ReadOptions read_options;
  string value;
  DB* db;
  DBImpl* db_impl;

  DestroyDB(dbname, options);

  options.create_if_missing = true;
  Status s = DB::Open(options, dbname, &db);
  ASSERT_OK(s);

  db_impl = dynamic_cast<DBImpl*> (db);
  ASSERT_TRUE(db_impl);

  WriteBatch wb;

  wb.Put("a", "value.a");
  wb.Delete("x");

  // Test a simple Write
  s = db->Write(write_options, &wb);
  ASSERT_OK(s);

  s = db->Get(read_options, "a", &value);
  ASSERT_OK(s);
  ASSERT_EQ("value.a", value);

  // Test WriteWithCallback
  WriteCallbackTestWriteCallback1 callback1;
  WriteBatch wb2;

  wb2.Put("a", "value.a2");

  s = db_impl->WriteWithCallback(write_options, &wb2, &callback1);
  ASSERT_OK(s);
  ASSERT_TRUE(callback1.was_called);

  s = db->Get(read_options, "a", &value);
  ASSERT_OK(s);
  ASSERT_EQ("value.a2", value);

  // Test WriteWithCallback for a callback that fails
  WriteCallbackTestWriteCallback2 callback2;
  WriteBatch wb3;

  wb3.Put("a", "value.a3");

  s = db_impl->WriteWithCallback(write_options, &wb3, &callback2);
  ASSERT_NOK(s);

  s = db->Get(read_options, "a", &value);
  ASSERT_OK(s);
  ASSERT_EQ("value.a2", value);

  delete db;
  DestroyDB(dbname, options);
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
          "SKIPPED as WriteWithCallback is not supported in ROCKSDB_LITE\n");
  return 0;
}

#endif  // !ROCKSDB_LITE

//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/db_test_util.h"
#include "db/write_thread.h"
#include "port/stack_trace.h"

namespace ROCKSDB_NAMESPACE {

class DBWriteBufferManagerTest : public DBTestBase,
                                 public testing::WithParamInterface<bool> {
 public:
  DBWriteBufferManagerTest()
      : DBTestBase("db_write_buffer_manager_test", /*env_do_fsync=*/false) {}
  bool cost_cache_;
};

TEST_P(DBWriteBufferManagerTest, SharedBufferAcrossCFs1) {
  Options options = CurrentOptions();
  options.arena_block_size = 4096;
  options.write_buffer_size = 500000;  // this is never hit
  std::shared_ptr<Cache> cache = NewLRUCache(4 * 1024 * 1024, 2);
  ASSERT_LT(cache->GetUsage(), 256 * 1024);
  cost_cache_ = GetParam();

  if (cost_cache_) {
    options.write_buffer_manager.reset(
        new WriteBufferManager(100000, cache, true));
  } else {
    options.write_buffer_manager.reset(
        new WriteBufferManager(100000, nullptr, true));
  }

  WriteOptions wo;
  wo.disableWAL = true;

  CreateAndReopenWithCF({"cf1", "cf2", "cf3"}, options);
  ASSERT_OK(Put(3, Key(1), DummyString(1), wo));
  ASSERT_OK(Flush(3));
  ASSERT_OK(Put(3, Key(1), DummyString(1), wo));
  ASSERT_OK(Put(0, Key(1), DummyString(1), wo));
  ASSERT_OK(Flush(0));

  // Write to "Default", "cf2" and "cf3".
  ASSERT_OK(Put(3, Key(1), DummyString(30000), wo));
  ASSERT_OK(Put(0, Key(1), DummyString(40000), wo));
  ASSERT_OK(Put(2, Key(1), DummyString(1), wo));

  ASSERT_OK(Put(3, Key(2), DummyString(40000), wo));
  // WriteBufferManager::buffer_size_ has exceeded after the previous write is
  // completed.

  // This make sures write will go through and if stall was in effect, it will
  // end.
  ASSERT_OK(Put(0, Key(2), DummyString(1), wo));
}

// Test Single DB with multiple writer threads get blocked when
// WriteBufferManager execeeds buffer_size_ and flush is waiting to be
// finished.
TEST_P(DBWriteBufferManagerTest, SharedWriteBufferAcrossCFs2) {
  Options options = CurrentOptions();
  options.arena_block_size = 4096;
  options.write_buffer_size = 500000;  // this is never hit
  std::shared_ptr<Cache> cache = NewLRUCache(4 * 1024 * 1024, 2);
  ASSERT_LT(cache->GetUsage(), 256 * 1024);
  cost_cache_ = GetParam();

  if (cost_cache_) {
    options.write_buffer_manager.reset(
        new WriteBufferManager(100000, cache, true));
  } else {
    options.write_buffer_manager.reset(
        new WriteBufferManager(100000, nullptr, true));
  }
  WriteOptions wo;
  wo.disableWAL = true;

  CreateAndReopenWithCF({"cf1", "cf2", "cf3"}, options);
  ASSERT_OK(Put(3, Key(1), DummyString(1), wo));
  ASSERT_OK(Flush(3));
  ASSERT_OK(Put(3, Key(1), DummyString(1), wo));
  ASSERT_OK(Put(0, Key(1), DummyString(1), wo));
  ASSERT_OK(Flush(0));

  // Write to "Default", "cf2" and "cf3". No flush will be triggered.
  ASSERT_OK(Put(3, Key(1), DummyString(30000), wo));
  ASSERT_OK(Put(0, Key(1), DummyString(40000), wo));
  ASSERT_OK(Put(2, Key(1), DummyString(1), wo));

  ASSERT_OK(Put(3, Key(2), DummyString(40000), wo));
  // WriteBufferManager::buffer_size_ has exceeded after the previous write is
  // completed.

  std::unordered_set<WriteThread::Writer*> w_set;
  std::vector<port::Thread> threads;
  int wait_count_db = 0;
  int num_writers = 4;
  InstrumentedMutex mutex;
  InstrumentedCondVar cv(&mutex);
  std::atomic<int> thread_num(0);

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency(
      {{"DBWriteBufferManagerTest::SharedWriteBufferAcrossCFs:0",
        "DBImpl::BackgroundCallFlush:start"}});

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "WBMStallInterface::BlockDB", [&](void*) {
        InstrumentedMutexLock lock(&mutex);
        wait_count_db++;
        cv.SignalAll();
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "WriteThread::WriteStall::Wait", [&](void* arg) {
        InstrumentedMutexLock lock(&mutex);
        WriteThread::Writer* w = static_cast<WriteThread::Writer*>(arg);
        w_set.insert(w);
        // Allow the flush to continue if all writer threads are blocked.
        if (w_set.size() == (unsigned long)num_writers) {
          TEST_SYNC_POINT(
              "DBWriteBufferManagerTest::SharedWriteBufferAcrossCFs:0");
        }
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  bool s = true;

  std::function<void(int)> writer = [&](int cf) {
    int a = thread_num.fetch_add(1);
    std::string key = "foo" + std::to_string(a);
    Status tmp = Put(cf, Slice(key), DummyString(1), wo);
    InstrumentedMutexLock lock(&mutex);
    s = s && tmp.ok();
  };

  // Flow:
  // main_writer thread will write but will be blocked (as Flush will on hold,
  // buffer_size_ has exceeded, thus will create stall in effect).
  //  |
  //  |
  //  multiple writer threads will be created to write across multiple columns
  //  and they will be blocked.
  //  |
  //  |
  //  Last writer thread will write and when its blocked it will signal Flush to
  //  continue to clear the stall.

  threads.emplace_back(writer, 1);
  // Wait untill first thread (main_writer) writing to DB is blocked and then
  // create the multiple writers which will be blocked from getting added to the
  // queue because stall is in effect.
  {
    InstrumentedMutexLock lock(&mutex);
    while (wait_count_db != 1) {
      cv.Wait();
    }
  }
  for (int i = 0; i < num_writers; i++) {
    threads.emplace_back(writer, i % 4);
  }
  for (auto& t : threads) {
    t.join();
  }

  ASSERT_TRUE(s);

  // Number of DBs blocked.
  ASSERT_EQ(wait_count_db, 1);
  // Number of Writer threads blocked.
  ASSERT_EQ(w_set.size(), num_writers);

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearAllCallBacks();
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
}

// Test multiple DBs get blocked when WriteBufferManager limit exceeds and flush
// is waiting to be finished but DBs tries to write meanwhile.
TEST_P(DBWriteBufferManagerTest, SharedWriteBufferLimitAcrossDB) {
  std::vector<std::string> dbnames;
  std::vector<DB*> dbs;
  int num_dbs = 3;

  for (int i = 0; i < num_dbs; i++) {
    dbs.push_back(nullptr);
    dbnames.push_back(
        test::PerThreadDBPath("db_shared_wb_db" + std::to_string(i)));
  }

  Options options = CurrentOptions();
  options.arena_block_size = 4096;
  options.write_buffer_size = 500000;  // this is never hit
  std::shared_ptr<Cache> cache = NewLRUCache(4 * 1024 * 1024, 2);
  ASSERT_LT(cache->GetUsage(), 256 * 1024);
  cost_cache_ = GetParam();

  if (cost_cache_) {
    options.write_buffer_manager.reset(
        new WriteBufferManager(100000, cache, true));
  } else {
    options.write_buffer_manager.reset(
        new WriteBufferManager(100000, nullptr, true));
  }
  CreateAndReopenWithCF({"cf1", "cf2"}, options);

  for (int i = 0; i < num_dbs; i++) {
    ASSERT_OK(DestroyDB(dbnames[i], options));
    ASSERT_OK(DB::Open(options, dbnames[i], &(dbs[i])));
  }
  WriteOptions wo;
  wo.disableWAL = true;

  for (int i = 0; i < num_dbs; i++) {
    ASSERT_OK(dbs[i]->Put(wo, Key(1), DummyString(20000)));
  }
  // Insert to db_.
  ASSERT_OK(Put(0, Key(1), DummyString(30000), wo));

  // WriteBufferManager Limit exceeded.
  std::vector<port::Thread> threads;
  int wait_count_db = 0;
  InstrumentedMutex mutex;
  InstrumentedCondVar cv(&mutex);

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency(
      {{"DBWriteBufferManagerTest::SharedWriteBufferAcrossCFs:0",
        "DBImpl::BackgroundCallFlush:start"}});

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "WBMStallInterface::BlockDB", [&](void*) {
        {
          InstrumentedMutexLock lock(&mutex);
          wait_count_db++;
          cv.Signal();
          // Since this is the last DB, signal Flush to continue.
          if (wait_count_db == num_dbs + 1) {
            TEST_SYNC_POINT(
                "DBWriteBufferManagerTest::SharedWriteBufferAcrossCFs:0");
          }
        }
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  bool s = true;

  // Write to DB.
  std::function<void(DB*)> write_db = [&](DB* db) {
    Status tmp = db->Put(wo, Key(3), DummyString(1));
    InstrumentedMutexLock lock(&mutex);
    s = s && tmp.ok();
  };

  // Flow:
  // db_ will write and will be blocked (as Flush will on hold and will create
  // stall in effect).
  //  |
  //  multiple dbs writers will be created to write to that db and they will be
  //  blocked.
  //  |
  //  |
  //  Last writer will write and when its blocked it will signal Flush to
  //  continue to clear the stall.

  threads.emplace_back(write_db, db_);
  // Wait untill first DB is blocked and then create the multiple writers for
  // different DBs which will be blocked from getting added to the queue because
  // stall is in effect.
  {
    InstrumentedMutexLock lock(&mutex);
    while (wait_count_db != 1) {
      cv.Wait();
    }
  }
  for (int i = 0; i < num_dbs; i++) {
    threads.emplace_back(write_db, dbs[i]);
  }
  for (auto& t : threads) {
    t.join();
  }

  ASSERT_TRUE(s);
  ASSERT_EQ(num_dbs + 1, wait_count_db);
  // Clean up DBs.
  for (int i = 0; i < num_dbs; i++) {
    ASSERT_OK(dbs[i]->Close());
    ASSERT_OK(DestroyDB(dbnames[i], options));
    delete dbs[i];
  }

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearAllCallBacks();
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
}

// Test multiple threads writing across multiple DBs and multiple columns get
// blocked when stall by WriteBufferManager is in effect.
TEST_P(DBWriteBufferManagerTest, SharedWriteBufferLimitAcrossDB1) {
  std::vector<std::string> dbnames;
  std::vector<DB*> dbs;
  int num_dbs = 3;

  for (int i = 0; i < num_dbs; i++) {
    dbs.push_back(nullptr);
    dbnames.push_back(
        test::PerThreadDBPath("db_shared_wb_db" + std::to_string(i)));
  }

  Options options = CurrentOptions();
  options.arena_block_size = 4096;
  options.write_buffer_size = 500000;  // this is never hit
  std::shared_ptr<Cache> cache = NewLRUCache(4 * 1024 * 1024, 2);
  ASSERT_LT(cache->GetUsage(), 256 * 1024);
  cost_cache_ = GetParam();

  if (cost_cache_) {
    options.write_buffer_manager.reset(
        new WriteBufferManager(100000, cache, true));
  } else {
    options.write_buffer_manager.reset(
        new WriteBufferManager(100000, nullptr, true));
  }
  CreateAndReopenWithCF({"cf1", "cf2"}, options);

  for (int i = 0; i < num_dbs; i++) {
    ASSERT_OK(DestroyDB(dbnames[i], options));
    ASSERT_OK(DB::Open(options, dbnames[i], &(dbs[i])));
  }
  WriteOptions wo;
  wo.disableWAL = true;

  for (int i = 0; i < num_dbs; i++) {
    ASSERT_OK(dbs[i]->Put(wo, Key(1), DummyString(20000)));
  }
  // Insert to db_.
  ASSERT_OK(Put(0, Key(1), DummyString(30000), wo));

  // WriteBufferManager::buffer_size_ has exceeded after the previous write to
  // dbs[0] is completed.
  std::vector<port::Thread> threads;
  int wait_count_db = 0;
  InstrumentedMutex mutex;
  InstrumentedCondVar cv(&mutex);
  std::unordered_set<WriteThread::Writer*> w_set;
  std::vector<port::Thread> writer_threads;
  std::atomic<int> thread_num(0);

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency(
      {{"DBWriteBufferManagerTest::SharedWriteBufferAcrossCFs:0",
        "DBImpl::BackgroundCallFlush:start"}});

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "WBMStallInterface::BlockDB", [&](void*) {
        {
          InstrumentedMutexLock lock(&mutex);
          wait_count_db++;
          thread_num.fetch_add(1);
          cv.Signal();
          // Allow the flush to continue if all writer threads are blocked.
          if (thread_num.load(std::memory_order_relaxed) == 2 * num_dbs + 1) {
            TEST_SYNC_POINT(
                "DBWriteBufferManagerTest::SharedWriteBufferAcrossCFs:0");
          }
        }
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "WriteThread::WriteStall::Wait", [&](void* arg) {
        WriteThread::Writer* w = static_cast<WriteThread::Writer*>(arg);
        {
          InstrumentedMutexLock lock(&mutex);
          w_set.insert(w);
          thread_num.fetch_add(1);
          // Allow the flush continue if all writer threads are blocked.
          if (thread_num.load(std::memory_order_relaxed) == 2 * num_dbs + 1) {
            TEST_SYNC_POINT(
                "DBWriteBufferManagerTest::SharedWriteBufferAcrossCFs:0");
          }
        }
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  bool s1 = true, s2 = true;
  // Write to multiple columns of db_.
  std::function<void(int)> write_cf = [&](int cf) {
    Status tmp = Put(cf, Key(3), DummyString(1), wo);
    InstrumentedMutexLock lock(&mutex);
    s1 = s1 && tmp.ok();
  };
  // Write to multiple DBs.
  std::function<void(DB*)> write_db = [&](DB* db) {
    Status tmp = db->Put(wo, Key(3), DummyString(1));
    InstrumentedMutexLock lock(&mutex);
    s2 = s2 && tmp.ok();
  };

  // Flow:
  // thread will write to db_ will be blocked (as Flush will on hold,
  // buffer_size_ has exceeded and will create stall in effect).
  //  |
  //  |
  //  multiple writers threads writing to different DBs and to db_ across
  //  multiple columns will be created and they will be blocked due to stall.
  //  |
  //  |
  //  Last writer thread will write and when its blocked it will signal Flush to
  //  continue to clear the stall.
  threads.emplace_back(write_db, db_);
  // Wait untill first thread is blocked and then create the multiple writer
  // threads.
  {
    InstrumentedMutexLock lock(&mutex);
    while (wait_count_db != 1) {
      cv.Wait();
    }
  }

  for (int i = 0; i < num_dbs; i++) {
    // Write to multiple columns of db_.
    writer_threads.emplace_back(write_cf, i % 3);
    // Write to different dbs.
    threads.emplace_back(write_db, dbs[i]);
  }
  for (auto& t : threads) {
    t.join();
  }
  for (auto& t : writer_threads) {
    t.join();
  }

  ASSERT_TRUE(s1);
  ASSERT_TRUE(s2);

  // Number of DBs blocked.
  ASSERT_EQ(num_dbs + 1, wait_count_db);
  // Number of Writer threads blocked.
  ASSERT_EQ(w_set.size(), num_dbs);
  // Clean up DBs.
  for (int i = 0; i < num_dbs; i++) {
    ASSERT_OK(dbs[i]->Close());
    ASSERT_OK(DestroyDB(dbnames[i], options));
    delete dbs[i];
  }

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearAllCallBacks();
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
}

// Test multiple threads writing across multiple columns of db_ by passing
// different values to WriteOption.no_slown_down.
TEST_P(DBWriteBufferManagerTest, MixedSlowDownOptionsSingleDB) {
  Options options = CurrentOptions();
  options.arena_block_size = 4096;
  options.write_buffer_size = 500000;  // this is never hit
  std::shared_ptr<Cache> cache = NewLRUCache(4 * 1024 * 1024, 2);
  ASSERT_LT(cache->GetUsage(), 256 * 1024);
  cost_cache_ = GetParam();

  if (cost_cache_) {
    options.write_buffer_manager.reset(
        new WriteBufferManager(100000, cache, true));
  } else {
    options.write_buffer_manager.reset(
        new WriteBufferManager(100000, nullptr, true));
  }
  WriteOptions wo;
  wo.disableWAL = true;

  CreateAndReopenWithCF({"cf1", "cf2", "cf3"}, options);

  ASSERT_OK(Put(3, Key(1), DummyString(1), wo));
  ASSERT_OK(Flush(3));
  ASSERT_OK(Put(3, Key(1), DummyString(1), wo));
  ASSERT_OK(Put(0, Key(1), DummyString(1), wo));
  ASSERT_OK(Flush(0));

  // Write to "Default", "cf2" and "cf3". No flush will be triggered.
  ASSERT_OK(Put(3, Key(1), DummyString(30000), wo));
  ASSERT_OK(Put(0, Key(1), DummyString(40000), wo));
  ASSERT_OK(Put(2, Key(1), DummyString(1), wo));
  ASSERT_OK(Put(3, Key(2), DummyString(40000), wo));

  // WriteBufferManager::buffer_size_ has exceeded after the previous write to
  // db_ is completed.

  std::unordered_set<WriteThread::Writer*> w_slowdown_set;
  std::vector<port::Thread> threads;
  int wait_count_db = 0;
  int num_writers = 4;
  InstrumentedMutex mutex;
  InstrumentedCondVar cv(&mutex);
  std::atomic<int> thread_num(0);
  std::atomic<int> w_no_slowdown(0);

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency(
      {{"DBWriteBufferManagerTest::SharedWriteBufferAcrossCFs:0",
        "DBImpl::BackgroundCallFlush:start"}});

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "WBMStallInterface::BlockDB", [&](void*) {
        {
          InstrumentedMutexLock lock(&mutex);
          wait_count_db++;
          cv.SignalAll();
        }
      });

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "WriteThread::WriteStall::Wait", [&](void* arg) {
        {
          InstrumentedMutexLock lock(&mutex);
          WriteThread::Writer* w = static_cast<WriteThread::Writer*>(arg);
          w_slowdown_set.insert(w);
          // Allow the flush continue if all writer threads are blocked.
          if (w_slowdown_set.size() + (unsigned long)w_no_slowdown.load(
                                          std::memory_order_relaxed) ==
              (unsigned long)num_writers) {
            TEST_SYNC_POINT(
                "DBWriteBufferManagerTest::SharedWriteBufferAcrossCFs:0");
          }
        }
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  bool s1 = true, s2 = true;

  std::function<void(int)> write_slow_down = [&](int cf) {
    int a = thread_num.fetch_add(1);
    std::string key = "foo" + std::to_string(a);
    WriteOptions write_op;
    write_op.no_slowdown = false;
    Status tmp = Put(cf, Slice(key), DummyString(1), write_op);
    InstrumentedMutexLock lock(&mutex);
    s1 = s1 && tmp.ok();
  };

  std::function<void(int)> write_no_slow_down = [&](int cf) {
    int a = thread_num.fetch_add(1);
    std::string key = "foo" + std::to_string(a);
    WriteOptions write_op;
    write_op.no_slowdown = true;
    Status tmp = Put(cf, Slice(key), DummyString(1), write_op);
    {
      InstrumentedMutexLock lock(&mutex);
      s2 = s2 && !tmp.ok();
      w_no_slowdown.fetch_add(1);
      // Allow the flush continue if all writer threads are blocked.
      if (w_slowdown_set.size() +
              (unsigned long)w_no_slowdown.load(std::memory_order_relaxed) ==
          (unsigned long)num_writers) {
        TEST_SYNC_POINT(
            "DBWriteBufferManagerTest::SharedWriteBufferAcrossCFs:0");
      }
    }
  };

  // Flow:
  // main_writer thread will write but will be blocked (as Flush will on hold,
  // buffer_size_ has exceeded, thus will create stall in effect).
  //  |
  //  |
  //  multiple writer threads will be created to write across multiple columns
  //  with different values of WriteOptions.no_slowdown. Some of them will
  //  be blocked and some of them will return with Incomplete status.
  //  |
  //  |
  //  Last writer thread will write and when its blocked/return it will signal
  //  Flush to continue to clear the stall.
  threads.emplace_back(write_slow_down, 1);
  // Wait untill first thread (main_writer) writing to DB is blocked and then
  // create the multiple writers which will be blocked from getting added to the
  // queue because stall is in effect.
  {
    InstrumentedMutexLock lock(&mutex);
    while (wait_count_db != 1) {
      cv.Wait();
    }
  }

  for (int i = 0; i < num_writers; i += 2) {
    threads.emplace_back(write_no_slow_down, (i) % 4);
    threads.emplace_back(write_slow_down, (i + 1) % 4);
  }
  for (auto& t : threads) {
    t.join();
  }

  ASSERT_TRUE(s1);
  ASSERT_TRUE(s2);
  // Number of DBs blocked.
  ASSERT_EQ(wait_count_db, 1);
  // Number of Writer threads blocked.
  ASSERT_EQ(w_slowdown_set.size(), num_writers / 2);
  // Number of Writer threads with WriteOptions.no_slowdown = true.
  ASSERT_EQ(w_no_slowdown.load(std::memory_order_relaxed), num_writers / 2);

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearAllCallBacks();
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
}

// Test multiple threads writing across multiple columns of db_ and different
// dbs by passing different values to WriteOption.no_slown_down.
TEST_P(DBWriteBufferManagerTest, MixedSlowDownOptionsMultipleDB) {
  std::vector<std::string> dbnames;
  std::vector<DB*> dbs;
  int num_dbs = 4;

  for (int i = 0; i < num_dbs; i++) {
    dbs.push_back(nullptr);
    dbnames.push_back(
        test::PerThreadDBPath("db_shared_wb_db" + std::to_string(i)));
  }

  Options options = CurrentOptions();
  options.arena_block_size = 4096;
  options.write_buffer_size = 500000;  // this is never hit
  std::shared_ptr<Cache> cache = NewLRUCache(4 * 1024 * 1024, 2);
  ASSERT_LT(cache->GetUsage(), 256 * 1024);
  cost_cache_ = GetParam();

  if (cost_cache_) {
    options.write_buffer_manager.reset(
        new WriteBufferManager(100000, cache, true));
  } else {
    options.write_buffer_manager.reset(
        new WriteBufferManager(100000, nullptr, true));
  }
  CreateAndReopenWithCF({"cf1", "cf2"}, options);

  for (int i = 0; i < num_dbs; i++) {
    ASSERT_OK(DestroyDB(dbnames[i], options));
    ASSERT_OK(DB::Open(options, dbnames[i], &(dbs[i])));
  }
  WriteOptions wo;
  wo.disableWAL = true;

  for (int i = 0; i < num_dbs; i++) {
    ASSERT_OK(dbs[i]->Put(wo, Key(1), DummyString(20000)));
  }
  // Insert to db_.
  ASSERT_OK(Put(0, Key(1), DummyString(30000), wo));

  // WriteBufferManager::buffer_size_ has exceeded after the previous write to
  // dbs[0] is completed.
  std::vector<port::Thread> threads;
  int wait_count_db = 0;
  InstrumentedMutex mutex;
  InstrumentedCondVar cv(&mutex);
  std::unordered_set<WriteThread::Writer*> w_slowdown_set;
  std::vector<port::Thread> writer_threads;
  std::atomic<int> thread_num(0);
  std::atomic<int> w_no_slowdown(0);

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency(
      {{"DBWriteBufferManagerTest::SharedWriteBufferAcrossCFs:0",
        "DBImpl::BackgroundCallFlush:start"}});

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "WBMStallInterface::BlockDB", [&](void*) {
        InstrumentedMutexLock lock(&mutex);
        wait_count_db++;
        cv.Signal();
        // Allow the flush continue if all writer threads are blocked.
        if (w_slowdown_set.size() +
                (unsigned long)(w_no_slowdown.load(std::memory_order_relaxed) +
                                wait_count_db) ==
            (unsigned long)(2 * num_dbs + 1)) {
          TEST_SYNC_POINT(
              "DBWriteBufferManagerTest::SharedWriteBufferAcrossCFs:0");
        }
      });

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "WriteThread::WriteStall::Wait", [&](void* arg) {
        WriteThread::Writer* w = static_cast<WriteThread::Writer*>(arg);
        InstrumentedMutexLock lock(&mutex);
        w_slowdown_set.insert(w);
        // Allow the flush continue if all writer threads are blocked.
        if (w_slowdown_set.size() +
                (unsigned long)(w_no_slowdown.load(std::memory_order_relaxed) +
                                wait_count_db) ==
            (unsigned long)(2 * num_dbs + 1)) {
          TEST_SYNC_POINT(
              "DBWriteBufferManagerTest::SharedWriteBufferAcrossCFs:0");
        }
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  bool s1 = true, s2 = true;
  std::function<void(DB*)> write_slow_down = [&](DB* db) {
    int a = thread_num.fetch_add(1);
    std::string key = "foo" + std::to_string(a);
    WriteOptions write_op;
    write_op.no_slowdown = false;
    Status tmp = db->Put(write_op, Slice(key), DummyString(1));
    InstrumentedMutexLock lock(&mutex);
    s1 = s1 && tmp.ok();
  };

  std::function<void(DB*)> write_no_slow_down = [&](DB* db) {
    int a = thread_num.fetch_add(1);
    std::string key = "foo" + std::to_string(a);
    WriteOptions write_op;
    write_op.no_slowdown = true;
    Status tmp = db->Put(write_op, Slice(key), DummyString(1));
    {
      InstrumentedMutexLock lock(&mutex);
      s2 = s2 && !tmp.ok();
      w_no_slowdown.fetch_add(1);
      if (w_slowdown_set.size() +
              (unsigned long)(w_no_slowdown.load(std::memory_order_relaxed) +
                              wait_count_db) ==
          (unsigned long)(2 * num_dbs + 1)) {
        TEST_SYNC_POINT(
            "DBWriteBufferManagerTest::SharedWriteBufferAcrossCFs:0");
      }
    }
  };

  // Flow:
  // first thread will write but will be blocked (as Flush will on hold,
  // buffer_size_ has exceeded, thus will create stall in effect).
  //  |
  //  |
  //  multiple writer threads will be created to write across multiple columns
  //  of db_ and different DBs with different values of
  //  WriteOptions.no_slowdown. Some of them will be blocked and some of them
  //  will return with Incomplete status.
  //  |
  //  |
  //  Last writer thread will write and when its blocked/return it will signal
  //  Flush to continue to clear the stall.
  threads.emplace_back(write_slow_down, db_);
  // Wait untill first thread writing to DB is blocked and then
  // create the multiple writers.
  {
    InstrumentedMutexLock lock(&mutex);
    while (wait_count_db != 1) {
      cv.Wait();
    }
  }

  for (int i = 0; i < num_dbs; i += 2) {
    // Write to multiple columns of db_.
    writer_threads.emplace_back(write_slow_down, db_);
    writer_threads.emplace_back(write_no_slow_down, db_);
    // Write to different DBs.
    threads.emplace_back(write_slow_down, dbs[i]);
    threads.emplace_back(write_no_slow_down, dbs[i + 1]);
  }

  for (auto& t : threads) {
    t.join();
  }

  for (auto& t : writer_threads) {
    t.join();
  }

  ASSERT_TRUE(s1);
  ASSERT_TRUE(s2);
  // Number of DBs blocked.
  ASSERT_EQ((num_dbs / 2) + 1, wait_count_db);
  // Number of writer threads writing to db_ blocked from getting added to the
  // queue.
  ASSERT_EQ(w_slowdown_set.size(), num_dbs / 2);
  // Number of threads with WriteOptions.no_slowdown = true.
  ASSERT_EQ(w_no_slowdown.load(std::memory_order_relaxed), num_dbs);

  // Clean up DBs.
  for (int i = 0; i < num_dbs; i++) {
    ASSERT_OK(dbs[i]->Close());
    ASSERT_OK(DestroyDB(dbnames[i], options));
    delete dbs[i];
  }

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearAllCallBacks();
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
}

// Tests a `WriteBufferManager` constructed with `allow_stall == false` does not
// thrash memtable switching when full and a CF receives multiple writes.
// Instead, we expect to switch a CF's memtable for flush only when that CF does
// not have any pending or running flush.
//
// This test uses multiple DBs each with a single CF instead of a single DB
// with multiple CFs. That way we can control which CF is considered for switch
// by writing to that CF's DB.
//
// Not supported in LITE mode due to `GetProperty()` unavailable.
TEST_P(DBWriteBufferManagerTest, StopSwitchingMemTablesOnceFlushing) {
  Options options = CurrentOptions();
  options.arena_block_size = 4 << 10;   // 4KB
  options.write_buffer_size = 1 << 20;  // 1MB
  std::shared_ptr<Cache> cache =
      NewLRUCache(4 << 20 /* capacity (4MB) */, 2 /* num_shard_bits */);
  ASSERT_LT(cache->GetUsage(), 256 << 10 /* 256KB */);
  cost_cache_ = GetParam();
  if (cost_cache_) {
    options.write_buffer_manager.reset(new WriteBufferManager(
        512 << 10 /* buffer_size (512KB) */, cache, false /* allow_stall */));
  } else {
    options.write_buffer_manager.reset(
        new WriteBufferManager(512 << 10 /* buffer_size (512KB) */,
                               nullptr /* cache */, false /* allow_stall */));
  }

  Reopen(options);
  std::string dbname = test::PerThreadDBPath("db_shared_wbm_db");
  DB* shared_wbm_db = nullptr;

  ASSERT_OK(DestroyDB(dbname, options));
  ASSERT_OK(DB::Open(options, dbname, &shared_wbm_db));

  // The last write will make WBM need flush, but it won't flush yet.
  ASSERT_OK(Put(Key(1), DummyString(256 << 10 /* 256KB */), WriteOptions()));
  ASSERT_FALSE(options.write_buffer_manager->ShouldFlush());
  ASSERT_OK(Put(Key(1), DummyString(256 << 10 /* 256KB */), WriteOptions()));
  ASSERT_TRUE(options.write_buffer_manager->ShouldFlush());

  // Flushes will be pending, not running because flush threads are blocked.
  test::SleepingBackgroundTask sleeping_task_high;
  env_->Schedule(&test::SleepingBackgroundTask::DoSleepTask,
                 &sleeping_task_high, Env::Priority::HIGH);

  for (int i = 0; i < 3; ++i) {
    ASSERT_OK(
        shared_wbm_db->Put(WriteOptions(), Key(1), DummyString(1 /* len */)));
    std::string prop;
    ASSERT_TRUE(
        shared_wbm_db->GetProperty("rocksdb.num-immutable-mem-table", &prop));
    ASSERT_EQ(std::to_string(i > 0 ? 1 : 0), prop);
    ASSERT_TRUE(
        shared_wbm_db->GetProperty("rocksdb.mem-table-flush-pending", &prop));
    ASSERT_EQ(std::to_string(i > 0 ? 1 : 0), prop);
  }

  // Clean up DBs.
  sleeping_task_high.WakeUp();
  sleeping_task_high.WaitUntilDone();
  ASSERT_OK(shared_wbm_db->Close());
  ASSERT_OK(DestroyDB(dbname, options));
  delete shared_wbm_db;
}

TEST_F(DBWriteBufferManagerTest, RuntimeChangeableAllowStall) {
  constexpr int kBigValue = 10000;

  Options options = CurrentOptions();
  options.write_buffer_manager.reset(
      new WriteBufferManager(1, nullptr /* cache */, true /* allow_stall */));
  DestroyAndReopen(options);

  // Pause flush thread so that
  // (a) the only way to exist write stall below is to change the `allow_stall`
  // (b) the write stall is "stable" without being interfered by flushes so that
  // we can check it without flakiness
  std::unique_ptr<test::SleepingBackgroundTask> sleeping_task(
      new test::SleepingBackgroundTask());
  env_->SetBackgroundThreads(1, Env::HIGH);
  env_->Schedule(&test::SleepingBackgroundTask::DoSleepTask,
                 sleeping_task.get(), Env::Priority::HIGH);
  sleeping_task->WaitUntilSleeping();

  // Test 1: test setting `allow_stall` from true to false
  //
  // Assert existence of a write stall
  WriteOptions wo_no_slowdown;
  wo_no_slowdown.no_slowdown = true;
  Status s = Put(Key(0), DummyString(kBigValue), wo_no_slowdown);
  ASSERT_TRUE(s.IsIncomplete());
  ASSERT_TRUE(s.ToString().find("Write stall") != std::string::npos);

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency(
      {{"WBMStallInterface::BlockDB",
        "DBWriteBufferManagerTest::RuntimeChangeableThreadSafeParameters::"
        "ChangeParameter"}});
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  // Test `SetAllowStall()`
  port::Thread thread1([&] { ASSERT_OK(Put(Key(0), DummyString(kBigValue))); });
  port::Thread thread2([&] {
    TEST_SYNC_POINT(
        "DBWriteBufferManagerTest::RuntimeChangeableThreadSafeParameters::"
        "ChangeParameter");
    options.write_buffer_manager->SetAllowStall(false);
  });

  // Verify `allow_stall` is successfully set to false in thread2.
  // Othwerwise, thread1's write will be stalled and this test will hang
  // forever.
  thread1.join();
  thread2.join();

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearAllCallBacks();
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();

  // Test 2: test setting `allow_stall` from false to true
  //
  // Assert no write stall
  ASSERT_OK(Put(Key(0), DummyString(kBigValue), wo_no_slowdown));

  // Test `SetAllowStall()`
  options.write_buffer_manager->SetAllowStall(true);

  // Verify `allow_stall` is successfully set to true.
  // Otherwise the following write will not be stalled and therefore succeed.
  s = Put(Key(0), DummyString(kBigValue), wo_no_slowdown);
  ASSERT_TRUE(s.IsIncomplete());
  ASSERT_TRUE(s.ToString().find("Write stall") != std::string::npos);
  sleeping_task->WakeUp();
}

INSTANTIATE_TEST_CASE_P(DBWriteBufferManagerTest, DBWriteBufferManagerTest,
                        testing::Bool());

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  RegisterCustomObjects(argc, argv);
  return RUN_ALL_TESTS();
}

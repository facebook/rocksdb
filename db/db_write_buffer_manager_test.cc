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
      : DBTestBase("/db_write_buffer_manager_test", /*env_do_fsync=*/false) {}
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
    options.write_buffer_manager.reset(new WriteBufferManager(100000, cache));
  } else {
    options.write_buffer_manager.reset(new WriteBufferManager(100000));
  }

  WriteOptions wo;
  wo.disableWAL = true;

  int wait_count = 0;
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::BlockDB", [&](void* /*arg*/) { wait_count++; });

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  CreateAndReopenWithCF({"cf1", "cf2", "cf3"}, options);
  ASSERT_OK(Put(3, Key(1), DummyString(1), wo));
  Flush(3);
  ASSERT_OK(Put(3, Key(1), DummyString(1), wo));
  ASSERT_OK(Put(0, Key(1), DummyString(1), wo));
  Flush(0);

  // Write to "Default", "cf2" and "cf3".
  ASSERT_OK(Put(3, Key(1), DummyString(30000), wo));
  ASSERT_OK(Put(0, Key(1), DummyString(40000), wo));
  ASSERT_OK(Put(2, Key(1), DummyString(1), wo));

  ASSERT_OK(Put(3, Key(2), DummyString(40000), wo));
  // WriteBufferManager::buffer_limit_ has exceeded after the previous write is
  // completed.

  // Next write will be blocked as WriteBufferManager::buffer_limit_ has
  // exceeded and will be unblocked after flush releases the memory.
  ASSERT_OK(Put(0, Key(2), DummyString(1), wo));

  ASSERT_EQ(wait_count, 1);
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearAllCallBacks();

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
}

// Test Single DB with multiple writer threads get blocked when
// WriteBufferManager execeeds buffer_limit_ and flush is waiting to be
// finished.
TEST_P(DBWriteBufferManagerTest, SharedWriteBufferAcrossCFs2) {
  Options options = CurrentOptions();
  options.arena_block_size = 4096;
  options.write_buffer_size = 500000;  // this is never hit
  std::shared_ptr<Cache> cache = NewLRUCache(4 * 1024 * 1024, 2);
  ASSERT_LT(cache->GetUsage(), 256 * 1024);
  cost_cache_ = GetParam();

  if (cost_cache_) {
    options.write_buffer_manager.reset(new WriteBufferManager(100000, cache));
  } else {
    options.write_buffer_manager.reset(new WriteBufferManager(100000));
  }
  WriteOptions wo;
  wo.disableWAL = true;

  CreateAndReopenWithCF({"cf1", "cf2", "cf3"}, options);
  ASSERT_OK(Put(3, Key(1), DummyString(1), wo));
  Flush(3);
  ASSERT_OK(Put(3, Key(1), DummyString(1), wo));
  ASSERT_OK(Put(0, Key(1), DummyString(1), wo));
  Flush(0);

  // Write to "Default", "cf2" and "cf3". No flush will be triggered.
  ASSERT_OK(Put(3, Key(1), DummyString(30000), wo));
  ASSERT_OK(Put(0, Key(1), DummyString(40000), wo));
  ASSERT_OK(Put(2, Key(1), DummyString(1), wo));

  ASSERT_OK(Put(3, Key(2), DummyString(40000), wo));
  // WriteBufferManager::buffer_limit_ has exceeded after the previous write is
  // completed.

  std::unordered_set<WriteThread::Writer*> w_set;
  std::vector<port::Thread> threads;
  int wait_count_db = 0;
  int num_writers = 4;
  InstrumentedMutex mutex;
  InstrumentedCondVar cv(&mutex);

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency(
      {{"DBWriteBufferManagerTest::SharedWriteBufferAcrossCFs:0",
        "DBWriteBufferManagerTest::SharedWriteBufferAcrossCFs:1"}});

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::BlockDB", [&](void*) {
        {
          InstrumentedMutexLock lock(&mutex);
          wait_count_db++;
          cv.SignalAll();
        }
      });

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "WriteThread::WriteStall::Wait", [&](void* arg) {
        InstrumentedMutexLock lock(&mutex);
        WriteThread::Writer* w = reinterpret_cast<WriteThread::Writer*>(arg);
        w_set.insert(w);
        // Allow the flush continue if all writer threads are blocked.
        if (w_set.size() == (unsigned long)num_writers) {
          TEST_SYNC_POINT(
              "DBWriteBufferManagerTest::SharedWriteBufferAcrossCFs:0");
        }
      });

  // Flush is paused to create stall.
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::BackgroundCallFlush:start", [&](void*) {
        // Wait until all writer threads are blocked.
        TEST_SYNC_POINT(
            "DBWriteBufferManagerTest::SharedWriteBufferAcrossCFs:1");
      });

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  std::function<void()> main_writer = [&]() {
    ASSERT_OK(Put(1, Key(3), DummyString(1), wo));
  };

  std::function<void(int)> write_cf2 = [&](int cf) {
    ASSERT_OK(Put(cf, Key(4), DummyString(1), wo));
  };

  // Flow:
  // main_writer thread will write but will be blocked (as Flush will on hold,
  // buffer_limit_ has exceeded, thus will create stall in effect).
  //  |
  //  |
  //  multiple writer threads will be created to write across multiple columns
  //  and they will be blocked.
  //  |
  //  |
  //  Last writer thread will write and when its blocked it will signal Flush to
  //  continue to clear the stall.

  threads.emplace_back(main_writer);
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
    threads.emplace_back(write_cf2, i % 4);
  }

  for (auto& t : threads) {
    t.join();
  }

  // Number of DBs blocked.
  ASSERT_EQ(wait_count_db, 1);
  // Number of Writer threads blocked.
  ASSERT_EQ(w_set.size(), num_writers);

  ASSERT_EQ(Get(2, Key(4)), DummyString(1));
  ASSERT_EQ(Get(3, Key(4)), DummyString(1));

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
    options.write_buffer_manager.reset(new WriteBufferManager(100000, cache));
  } else {
    options.write_buffer_manager.reset(new WriteBufferManager(100000));
  }
  CreateAndReopenWithCF({"cf1", "cf2"}, options);

  for (int i = 0; i < num_dbs; i++) {
    ASSERT_OK(DestroyDB(dbnames[i], options));
    ASSERT_OK(DB::Open(options, dbnames[i], &(dbs[i])));
  }

  WriteOptions wo;
  wo.disableWAL = true;

  // Insert to db_.
  ASSERT_OK(Put(0, Key(1), DummyString(70000), wo));

  // Insert to dbs[0].
  ASSERT_OK(dbs[0]->Put(wo, Key(1), DummyString(30000)));

  // WriteBufferManager Limit exceeded.
  std::vector<port::Thread> threads;
  int wait_count_db = 0;
  InstrumentedMutex mutex;
  InstrumentedCondVar cv(&mutex);

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency(
      {{"DBWriteBufferManagerTest::SharedWriteBufferAcrossCFs:0",
        "DBWriteBufferManagerTest::SharedWriteBufferAcrossCFs:1"}});

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::BlockDB", [&](void*) {
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

  // Flush is paused in between to execute stall. It will be signalled when all
  // DBs and writer threads will be blocked.
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::BackgroundCallFlush:start", [&](void*) {
        TEST_SYNC_POINT(
            "DBWriteBufferManagerTest::SharedWriteBufferAcrossCFs:1");
      });

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  // Write to DB.
  std::function<void(DB*)> write_db = [&](DB* db) {
    ASSERT_OK(db->Put(wo, Key(3), DummyString(1)));
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

  ASSERT_EQ(num_dbs + 1, wait_count_db);

  // Clean up DBs.
  for (int i = 0; i < num_dbs; i++) {
    ASSERT_OK(dbs[i]->Close());
    ASSERT_OK(DestroyDB(dbnames[i], options));
  }

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearAllCallBacks();
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
}

// Test multiple threads writing across mutiple DBs and multiple columns get
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
    options.write_buffer_manager.reset(new WriteBufferManager(100000, cache));
  } else {
    options.write_buffer_manager.reset(new WriteBufferManager(100000));
  }
  CreateAndReopenWithCF({"cf1", "cf2"}, options);

  for (int i = 0; i < num_dbs; i++) {
    ASSERT_OK(DestroyDB(dbnames[i], options));
    ASSERT_OK(DB::Open(options, dbnames[i], &(dbs[i])));
  }

  WriteOptions wo;
  wo.disableWAL = true;

  // Insert to db_.
  ASSERT_OK(Put(0, Key(1), DummyString(70000), wo));

  // Insert to dbs[0].
  ASSERT_OK(dbs[0]->Put(wo, Key(1), DummyString(30000)));

  // WriteBufferManager::buffer_limit_ has exceeded after the previous write to
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
        "DBWriteBufferManagerTest::SharedWriteBufferAcrossCFs:1"}});

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::BlockDB", [&](void*) {
        {
          InstrumentedMutexLock lock(&mutex);
          wait_count_db++;
          thread_num.fetch_add(1);
          cv.Signal();
          // Allow the flush continue if all writer threads are blocked.
          if (thread_num.load(std::memory_order_relaxed) == 2 * num_dbs + 1) {
            TEST_SYNC_POINT(
                "DBWriteBufferManagerTest::SharedWriteBufferAcrossCFs:0");
          }
        }
      });

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "WriteThread::WriteStall::Wait", [&](void* arg) {
        WriteThread::Writer* w = reinterpret_cast<WriteThread::Writer*>(arg);
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

  // Flush is paused to create stall.
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::BackgroundCallFlush:start", [&](void*) {
        // Wait until all writer threads are blocked.
        TEST_SYNC_POINT(
            "DBWriteBufferManagerTest::SharedWriteBufferAcrossCFs:1");
      });

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  // Write to multiple columns of db_.
  std::function<void(int)> write_cf = [&](int cf) {
    ASSERT_OK(Put(cf, Key(3), DummyString(1), wo));
  };

  // Write to mutiple DBs.
  std::function<void(DB*)> write_db = [&](DB* db) {
    ASSERT_OK(db->Put(wo, Key(3), DummyString(1)));
  };

  // Flow:
  // thread will write to db_ will be blocked (as Flush will on hold,
  // buffer_limit_ has exceeded and will create stall in effect).
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

  // Number of DBs blocked.
  ASSERT_EQ(num_dbs + 1, wait_count_db);
  // Number of Writer threads blocked.
  ASSERT_EQ(w_set.size(), num_dbs);

  // Clean up DBs.
  for (int i = 0; i < num_dbs; i++) {
    ASSERT_OK(dbs[i]->Close());
    ASSERT_OK(DestroyDB(dbnames[i], options));
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
    options.write_buffer_manager.reset(new WriteBufferManager(100000, cache));
  } else {
    options.write_buffer_manager.reset(new WriteBufferManager(100000));
  }
  WriteOptions wo;
  wo.disableWAL = true;

  CreateAndReopenWithCF({"cf1", "cf2", "cf3"}, options);

  ASSERT_OK(Put(3, Key(1), DummyString(1), wo));
  Flush(3);
  ASSERT_OK(Put(3, Key(1), DummyString(1), wo));
  ASSERT_OK(Put(0, Key(1), DummyString(1), wo));
  Flush(0);

  // Write to "Default", "cf2" and "cf3". No flush will be triggered.
  ASSERT_OK(Put(3, Key(1), DummyString(30000), wo));
  ASSERT_OK(Put(0, Key(1), DummyString(40000), wo));
  ASSERT_OK(Put(2, Key(1), DummyString(1), wo));
  ASSERT_OK(Put(3, Key(2), DummyString(40000), wo));

  // WriteBufferManager::buffer_limit_ has exceeded after the previous write to
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
        "DBWriteBufferManagerTest::SharedWriteBufferAcrossCFs:1"}});

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::BlockDB", [&](void*) {
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
          WriteThread::Writer* w = reinterpret_cast<WriteThread::Writer*>(arg);
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

  // Flush is paused to create stall.
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::BackgroundCallFlush:start", [&](void*) {
        // Continue flush once all writers are blocked.
        TEST_SYNC_POINT(
            "DBWriteBufferManagerTest::SharedWriteBufferAcrossCFs:1");
      });

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  std::function<void()> main_writer = [&]() {
    ASSERT_OK(Put(1, Key(3), DummyString(1), wo));
  };

  std::function<void(int)> write_slow_down = [&](int cf) {
    int a = thread_num.fetch_add(1);
    std::string key = "foo" + std::to_string(a);
    WriteOptions write_op;
    write_op.no_slowdown = false;
    ASSERT_OK(Put(cf, Slice(key), DummyString(1), write_op));
  };

  std::function<void(int)> write_no_slow_down = [&](int cf) {
    int a = thread_num.fetch_add(1);
    std::string key = "foo" + std::to_string(a);
    WriteOptions write_op;
    write_op.no_slowdown = true;
    ASSERT_NOK(Put(cf, Slice(key), DummyString(1), write_op));
    {
      InstrumentedMutexLock lock(&mutex);
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
  // buffer_limit_ has exceeded, thus will create stall in effect).
  //  |
  //  |
  //  multiple writer threads will be created to write across multiple columns
  //  with different values of WriteOptions.no_slowdown. Some of them will
  //  be blocked and some of them will return with Incomplete status.
  //  |
  //  |
  //  Last writer thread will write and when its blocked/return it will signal
  //  Flush to continue to clear the stall.
  threads.emplace_back(main_writer);
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
    options.write_buffer_manager.reset(new WriteBufferManager(100000, cache));
  } else {
    options.write_buffer_manager.reset(new WriteBufferManager(100000));
  }
  CreateAndReopenWithCF({"cf1", "cf2"}, options);

  for (int i = 0; i < num_dbs; i++) {
    ASSERT_OK(DestroyDB(dbnames[i], options));
    ASSERT_OK(DB::Open(options, dbnames[i], &(dbs[i])));
  }

  WriteOptions wo;
  wo.disableWAL = true;

  // Insert to db_.
  ASSERT_OK(Put(0, Key(1), DummyString(70000), wo));

  // Insert to dbs[0].
  ASSERT_OK(dbs[0]->Put(wo, Key(1), DummyString(30000)));

  // WriteBufferManager::buffer_limit_ has exceeded after the previous write to
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
        "DBWriteBufferManagerTest::SharedWriteBufferAcrossCFs:1"}});

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::BlockDB", [&](void*) {
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
        WriteThread::Writer* w = reinterpret_cast<WriteThread::Writer*>(arg);
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

  // Flush is paused to create stall.
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::BackgroundCallFlush:start", [&](void*) {
        TEST_SYNC_POINT(
            "DBWriteBufferManagerTest::SharedWriteBufferAcrossCFs:1");
      });

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  // Write to different dbs.
  std::function<void(DB*)> write_db = [&](DB* db) {
    ASSERT_OK(db->Put(wo, Key(3), DummyString(1)));
  };

  // Write to different columns of db_.
  std::function<void(int)> write_cf = [&](int cf) {
    ASSERT_OK(Put(cf, Key(3), DummyString(1), wo));
  };

  std::function<void(DB*)> write_slow_down = [&](DB* db) {
    int a = thread_num.fetch_add(1);
    std::string key = "foo" + std::to_string(a);
    WriteOptions write_op;
    write_op.no_slowdown = false;
    ASSERT_OK(db->Put(write_op, Slice(key), DummyString(1)));
  };

  std::function<void(DB*)> write_no_slow_down = [&](DB* db) {
    int a = thread_num.fetch_add(1);
    std::string key = "foo" + std::to_string(a);
    WriteOptions write_op;
    write_op.no_slowdown = true;
    ASSERT_NOK(db->Put(write_op, Slice(key), DummyString(1)));
    {
      InstrumentedMutexLock lock(&mutex);
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
  // buffer_limit_ has exceeded, thus will create stall in effect).
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
    // Write to mutiple columns of db_.
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
  }

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearAllCallBacks();
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
}

INSTANTIATE_TEST_CASE_P(DBWriteBufferManagerTest, DBWriteBufferManagerTest,
                        testing::Bool());

}  // namespace ROCKSDB_NAMESPACE

#ifdef ROCKSDB_UNITTESTS_WITH_CUSTOM_OBJECTS_FROM_STATIC_LIBS
extern "C" {
void RegisterCustomObjects(int argc, char** argv);
}
#else
void RegisterCustomObjects(int /*argc*/, char** /*argv*/) {}
#endif  // !ROCKSDB_UNITTESTS_WITH_CUSTOM_OBJECTS_FROM_STATIC_LIBS

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  RegisterCustomObjects(argc, argv);
  return RUN_ALL_TESTS();
}

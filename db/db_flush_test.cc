//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <atomic>

#include "db/db_impl/db_impl.h"
#include "db/db_test_util.h"
#include "port/port.h"
#include "port/stack_trace.h"
#include "test_util/sync_point.h"
#include "util/cast_util.h"
#include "util/mutexlock.h"
#include "utilities/fault_injection_env.h"

namespace ROCKSDB_NAMESPACE {

class DBFlushTest : public DBTestBase {
 public:
  DBFlushTest() : DBTestBase("/db_flush_test", /*env_do_fsync=*/true) {}
};

class DBFlushDirectIOTest : public DBFlushTest,
                            public ::testing::WithParamInterface<bool> {
 public:
  DBFlushDirectIOTest() : DBFlushTest() {}
};

class DBAtomicFlushTest : public DBFlushTest,
                          public ::testing::WithParamInterface<bool> {
 public:
  DBAtomicFlushTest() : DBFlushTest() {}
};

// We had issue when two background threads trying to flush at the same time,
// only one of them get committed. The test verifies the issue is fixed.
TEST_F(DBFlushTest, FlushWhileWritingManifest) {
  Options options;
  options.disable_auto_compactions = true;
  options.max_background_flushes = 2;
  options.env = env_;
  Reopen(options);
  FlushOptions no_wait;
  no_wait.wait = false;
  no_wait.allow_write_stall=true;

  SyncPoint::GetInstance()->LoadDependency(
      {{"VersionSet::LogAndApply:WriteManifest",
        "DBFlushTest::FlushWhileWritingManifest:1"},
       {"MemTableList::TryInstallMemtableFlushResults:InProgress",
        "VersionSet::LogAndApply:WriteManifestDone"}});
  SyncPoint::GetInstance()->EnableProcessing();

  ASSERT_OK(Put("foo", "v"));
  ASSERT_OK(dbfull()->Flush(no_wait));
  TEST_SYNC_POINT("DBFlushTest::FlushWhileWritingManifest:1");
  ASSERT_OK(Put("bar", "v"));
  ASSERT_OK(dbfull()->Flush(no_wait));
  // If the issue is hit we will wait here forever.
  dbfull()->TEST_WaitForFlushMemTable();
#ifndef ROCKSDB_LITE
  ASSERT_EQ(2, TotalTableFiles());
#endif  // ROCKSDB_LITE
}

// Disable this test temporarily on Travis as it fails intermittently.
// Github issue: #4151
TEST_F(DBFlushTest, SyncFail) {
  std::unique_ptr<FaultInjectionTestEnv> fault_injection_env(
      new FaultInjectionTestEnv(env_));
  Options options;
  options.disable_auto_compactions = true;
  options.env = fault_injection_env.get();

  SyncPoint::GetInstance()->LoadDependency(
      {{"DBFlushTest::SyncFail:GetVersionRefCount:1",
        "DBImpl::FlushMemTableToOutputFile:BeforePickMemtables"},
       {"DBImpl::FlushMemTableToOutputFile:AfterPickMemtables",
        "DBFlushTest::SyncFail:GetVersionRefCount:2"},
       {"DBFlushTest::SyncFail:1", "DBImpl::SyncClosedLogs:Start"},
       {"DBImpl::SyncClosedLogs:Failed", "DBFlushTest::SyncFail:2"}});
  SyncPoint::GetInstance()->EnableProcessing();

  CreateAndReopenWithCF({"pikachu"}, options);
  Put("key", "value");
  auto* cfd =
      static_cast_with_check<ColumnFamilyHandleImpl>(db_->DefaultColumnFamily())
          ->cfd();
  FlushOptions flush_options;
  flush_options.wait = false;
  ASSERT_OK(dbfull()->Flush(flush_options));
  // Flush installs a new super-version. Get the ref count after that.
  auto current_before = cfd->current();
  int refs_before = cfd->current()->TEST_refs();
  TEST_SYNC_POINT("DBFlushTest::SyncFail:GetVersionRefCount:1");
  TEST_SYNC_POINT("DBFlushTest::SyncFail:GetVersionRefCount:2");
  int refs_after_picking_memtables = cfd->current()->TEST_refs();
  ASSERT_EQ(refs_before + 1, refs_after_picking_memtables);
  fault_injection_env->SetFilesystemActive(false);
  TEST_SYNC_POINT("DBFlushTest::SyncFail:1");
  TEST_SYNC_POINT("DBFlushTest::SyncFail:2");
  fault_injection_env->SetFilesystemActive(true);
  // Now the background job will do the flush; wait for it.
  dbfull()->TEST_WaitForFlushMemTable();
#ifndef ROCKSDB_LITE
  ASSERT_EQ("", FilesPerLevel());  // flush failed.
#endif                             // ROCKSDB_LITE
  // Backgroun flush job should release ref count to current version.
  ASSERT_EQ(current_before, cfd->current());
  ASSERT_EQ(refs_before, cfd->current()->TEST_refs());
  Destroy(options);
}

TEST_F(DBFlushTest, SyncSkip) {
  Options options = CurrentOptions();

  SyncPoint::GetInstance()->LoadDependency(
      {{"DBFlushTest::SyncSkip:1", "DBImpl::SyncClosedLogs:Skip"},
       {"DBImpl::SyncClosedLogs:Skip", "DBFlushTest::SyncSkip:2"}});
  SyncPoint::GetInstance()->EnableProcessing();

  Reopen(options);
  Put("key", "value");

  FlushOptions flush_options;
  flush_options.wait = false;
  ASSERT_OK(dbfull()->Flush(flush_options));

  TEST_SYNC_POINT("DBFlushTest::SyncSkip:1");
  TEST_SYNC_POINT("DBFlushTest::SyncSkip:2");

  // Now the background job will do the flush; wait for it.
  dbfull()->TEST_WaitForFlushMemTable();

  Destroy(options);
}

TEST_F(DBFlushTest, FlushInLowPriThreadPool) {
  // Verify setting an empty high-pri (flush) thread pool causes flushes to be
  // scheduled in the low-pri (compaction) thread pool.
  Options options = CurrentOptions();
  options.level0_file_num_compaction_trigger = 4;
  options.memtable_factory.reset(new SpecialSkipListFactory(1));
  Reopen(options);
  env_->SetBackgroundThreads(0, Env::HIGH);

  std::thread::id tid;
  int num_flushes = 0, num_compactions = 0;
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::BGWorkFlush", [&](void* /*arg*/) {
        if (tid == std::thread::id()) {
          tid = std::this_thread::get_id();
        } else {
          ASSERT_EQ(tid, std::this_thread::get_id());
        }
        ++num_flushes;
      });
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::BGWorkCompaction", [&](void* /*arg*/) {
        ASSERT_EQ(tid, std::this_thread::get_id());
        ++num_compactions;
      });
  SyncPoint::GetInstance()->EnableProcessing();

  ASSERT_OK(Put("key", "val"));
  for (int i = 0; i < 4; ++i) {
    ASSERT_OK(Put("key", "val"));
    dbfull()->TEST_WaitForFlushMemTable();
  }
  dbfull()->TEST_WaitForCompact();
  ASSERT_EQ(4, num_flushes);
  ASSERT_EQ(1, num_compactions);
}

TEST_F(DBFlushTest, ManualFlushWithMinWriteBufferNumberToMerge) {
  Options options = CurrentOptions();
  options.write_buffer_size = 100;
  options.max_write_buffer_number = 4;
  options.min_write_buffer_number_to_merge = 3;
  Reopen(options);

  SyncPoint::GetInstance()->LoadDependency(
      {{"DBImpl::BGWorkFlush",
        "DBFlushTest::ManualFlushWithMinWriteBufferNumberToMerge:1"},
       {"DBFlushTest::ManualFlushWithMinWriteBufferNumberToMerge:2",
        "FlushJob::WriteLevel0Table"}});
  SyncPoint::GetInstance()->EnableProcessing();

  ASSERT_OK(Put("key1", "value1"));

  port::Thread t([&]() {
    // The call wait for flush to finish, i.e. with flush_options.wait = true.
    ASSERT_OK(Flush());
  });

  // Wait for flush start.
  TEST_SYNC_POINT("DBFlushTest::ManualFlushWithMinWriteBufferNumberToMerge:1");
  // Insert a second memtable before the manual flush finish.
  // At the end of the manual flush job, it will check if further flush
  // is needed, but it will not trigger flush of the second memtable because
  // min_write_buffer_number_to_merge is not reached.
  ASSERT_OK(Put("key2", "value2"));
  ASSERT_OK(dbfull()->TEST_SwitchMemtable());
  TEST_SYNC_POINT("DBFlushTest::ManualFlushWithMinWriteBufferNumberToMerge:2");

  // Manual flush should return, without waiting for flush indefinitely.
  t.join();
}

TEST_F(DBFlushTest, ScheduleOnlyOneBgThread) {
  Options options = CurrentOptions();
  Reopen(options);
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  int called = 0;
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::MaybeScheduleFlushOrCompaction:AfterSchedule:0", [&](void* arg) {
        ASSERT_NE(nullptr, arg);
        auto unscheduled_flushes = *reinterpret_cast<int*>(arg);
        ASSERT_EQ(0, unscheduled_flushes);
        ++called;
      });
  SyncPoint::GetInstance()->EnableProcessing();

  ASSERT_OK(Put("a", "foo"));
  FlushOptions flush_opts;
  ASSERT_OK(dbfull()->Flush(flush_opts));
  ASSERT_EQ(1, called);

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
}

TEST_P(DBFlushDirectIOTest, DirectIO) {
  Options options;
  options.create_if_missing = true;
  options.disable_auto_compactions = true;
  options.max_background_flushes = 2;
  options.use_direct_io_for_flush_and_compaction = GetParam();
  options.env = new MockEnv(Env::Default());
  SyncPoint::GetInstance()->SetCallBack(
      "BuildTable:create_file", [&](void* arg) {
        bool* use_direct_writes = static_cast<bool*>(arg);
        ASSERT_EQ(*use_direct_writes,
                  options.use_direct_io_for_flush_and_compaction);
      });

  SyncPoint::GetInstance()->EnableProcessing();
  Reopen(options);
  ASSERT_OK(Put("foo", "v"));
  FlushOptions flush_options;
  flush_options.wait = true;
  ASSERT_OK(dbfull()->Flush(flush_options));
  Destroy(options);
  delete options.env;
}

TEST_F(DBFlushTest, FlushError) {
  Options options;
  std::unique_ptr<FaultInjectionTestEnv> fault_injection_env(
      new FaultInjectionTestEnv(env_));
  options.write_buffer_size = 100;
  options.max_write_buffer_number = 4;
  options.min_write_buffer_number_to_merge = 3;
  options.disable_auto_compactions = true;
  options.env = fault_injection_env.get();
  Reopen(options);

  ASSERT_OK(Put("key1", "value1"));
  ASSERT_OK(Put("key2", "value2"));
  fault_injection_env->SetFilesystemActive(false);
  Status s = dbfull()->TEST_SwitchMemtable();
  fault_injection_env->SetFilesystemActive(true);
  Destroy(options);
  ASSERT_NE(s, Status::OK());
}

TEST_F(DBFlushTest, ManualFlushFailsInReadOnlyMode) {
  // Regression test for bug where manual flush hangs forever when the DB
  // is in read-only mode. Verify it now at least returns, despite failing.
  Options options;
  std::unique_ptr<FaultInjectionTestEnv> fault_injection_env(
      new FaultInjectionTestEnv(env_));
  options.env = fault_injection_env.get();
  options.max_write_buffer_number = 2;
  Reopen(options);

  // Trigger a first flush but don't let it run
  ASSERT_OK(db_->PauseBackgroundWork());
  ASSERT_OK(Put("key1", "value1"));
  FlushOptions flush_opts;
  flush_opts.wait = false;
  ASSERT_OK(db_->Flush(flush_opts));

  // Write a key to the second memtable so we have something to flush later
  // after the DB is in read-only mode.
  ASSERT_OK(Put("key2", "value2"));

  // Let the first flush continue, hit an error, and put the DB in read-only
  // mode.
  fault_injection_env->SetFilesystemActive(false);
  ASSERT_OK(db_->ContinueBackgroundWork());
  dbfull()->TEST_WaitForFlushMemTable();
#ifndef ROCKSDB_LITE
  uint64_t num_bg_errors;
  ASSERT_TRUE(db_->GetIntProperty(DB::Properties::kBackgroundErrors,
                                  &num_bg_errors));
  ASSERT_GT(num_bg_errors, 0);
#endif  // ROCKSDB_LITE

  // In the bug scenario, triggering another flush would cause the second flush
  // to hang forever. After the fix we expect it to return an error.
  ASSERT_NOK(db_->Flush(FlushOptions()));

  Close();
}

TEST_F(DBFlushTest, CFDropRaceWithWaitForFlushMemTables) {
  Options options = CurrentOptions();
  options.create_if_missing = true;
  CreateAndReopenWithCF({"pikachu"}, options);
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->LoadDependency(
      {{"DBImpl::FlushMemTable:AfterScheduleFlush",
        "DBFlushTest::CFDropRaceWithWaitForFlushMemTables:BeforeDrop"},
       {"DBFlushTest::CFDropRaceWithWaitForFlushMemTables:AfterFree",
        "DBImpl::BackgroundCallFlush:start"},
       {"DBImpl::BackgroundCallFlush:start",
        "DBImpl::FlushMemTable:BeforeWaitForBgFlush"}});
  SyncPoint::GetInstance()->EnableProcessing();
  ASSERT_EQ(2, handles_.size());
  ASSERT_OK(Put(1, "key", "value"));
  auto* cfd = static_cast<ColumnFamilyHandleImpl*>(handles_[1])->cfd();
  port::Thread drop_cf_thr([&]() {
    TEST_SYNC_POINT(
        "DBFlushTest::CFDropRaceWithWaitForFlushMemTables:BeforeDrop");
    ASSERT_OK(dbfull()->DropColumnFamily(handles_[1]));
    ASSERT_OK(dbfull()->DestroyColumnFamilyHandle(handles_[1]));
    handles_.resize(1);
    TEST_SYNC_POINT(
        "DBFlushTest::CFDropRaceWithWaitForFlushMemTables:AfterFree");
  });
  FlushOptions flush_opts;
  flush_opts.allow_write_stall = true;
  ASSERT_NOK(dbfull()->TEST_FlushMemTable(cfd, flush_opts));
  drop_cf_thr.join();
  Close();
  SyncPoint::GetInstance()->DisableProcessing();
}

#ifndef ROCKSDB_LITE
TEST_F(DBFlushTest, FireOnFlushCompletedAfterCommittedResult) {
  class TestListener : public EventListener {
   public:
    void OnFlushCompleted(DB* db, const FlushJobInfo& info) override {
      // There's only one key in each flush.
      ASSERT_EQ(info.smallest_seqno, info.largest_seqno);
      ASSERT_NE(0, info.smallest_seqno);
      if (info.smallest_seqno == seq1) {
        // First flush completed
        ASSERT_FALSE(completed1);
        completed1 = true;
        CheckFlushResultCommitted(db, seq1);
      } else {
        // Second flush completed
        ASSERT_FALSE(completed2);
        completed2 = true;
        ASSERT_EQ(info.smallest_seqno, seq2);
        CheckFlushResultCommitted(db, seq2);
      }
    }

    void CheckFlushResultCommitted(DB* db, SequenceNumber seq) {
      DBImpl* db_impl = static_cast_with_check<DBImpl>(db);
      InstrumentedMutex* mutex = db_impl->mutex();
      mutex->Lock();
      auto* cfd = static_cast_with_check<ColumnFamilyHandleImpl>(
                      db->DefaultColumnFamily())
                      ->cfd();
      ASSERT_LT(seq, cfd->imm()->current()->GetEarliestSequenceNumber());
      mutex->Unlock();
    }

    std::atomic<SequenceNumber> seq1{0};
    std::atomic<SequenceNumber> seq2{0};
    std::atomic<bool> completed1{false};
    std::atomic<bool> completed2{false};
  };
  std::shared_ptr<TestListener> listener = std::make_shared<TestListener>();

  SyncPoint::GetInstance()->LoadDependency(
      {{"DBImpl::BackgroundCallFlush:start",
        "DBFlushTest::FireOnFlushCompletedAfterCommittedResult:WaitFirst"},
       {"DBImpl::FlushMemTableToOutputFile:Finish",
        "DBFlushTest::FireOnFlushCompletedAfterCommittedResult:WaitSecond"}});
  SyncPoint::GetInstance()->SetCallBack(
      "FlushJob::WriteLevel0Table", [&listener](void* arg) {
        // Wait for the second flush finished, out of mutex.
        auto* mems = reinterpret_cast<autovector<MemTable*>*>(arg);
        if (mems->front()->GetEarliestSequenceNumber() == listener->seq1 - 1) {
          TEST_SYNC_POINT(
              "DBFlushTest::FireOnFlushCompletedAfterCommittedResult:"
              "WaitSecond");
        }
      });

  Options options = CurrentOptions();
  options.create_if_missing = true;
  options.listeners.push_back(listener);
  // Setting max_flush_jobs = max_background_jobs / 4 = 2.
  options.max_background_jobs = 8;
  // Allow 2 immutable memtables.
  options.max_write_buffer_number = 3;
  Reopen(options);
  SyncPoint::GetInstance()->EnableProcessing();
  ASSERT_OK(Put("foo", "v"));
  listener->seq1 = db_->GetLatestSequenceNumber();
  // t1 will wait for the second flush complete before committing flush result.
  auto t1 = port::Thread([&]() {
    // flush_opts.wait = true
    ASSERT_OK(db_->Flush(FlushOptions()));
  });
  // Wait for first flush started.
  TEST_SYNC_POINT(
      "DBFlushTest::FireOnFlushCompletedAfterCommittedResult:WaitFirst");
  // The second flush will exit early without commit its result. The work
  // is delegated to the first flush.
  ASSERT_OK(Put("bar", "v"));
  listener->seq2 = db_->GetLatestSequenceNumber();
  FlushOptions flush_opts;
  flush_opts.wait = false;
  ASSERT_OK(db_->Flush(flush_opts));
  t1.join();
  ASSERT_TRUE(listener->completed1);
  ASSERT_TRUE(listener->completed2);
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
}
#endif  // !ROCKSDB_LITE

TEST_P(DBAtomicFlushTest, ManualAtomicFlush) {
  Options options = CurrentOptions();
  options.create_if_missing = true;
  options.atomic_flush = GetParam();
  options.write_buffer_size = (static_cast<size_t>(64) << 20);

  CreateAndReopenWithCF({"pikachu", "eevee"}, options);
  size_t num_cfs = handles_.size();
  ASSERT_EQ(3, num_cfs);
  WriteOptions wopts;
  wopts.disableWAL = true;
  for (size_t i = 0; i != num_cfs; ++i) {
    ASSERT_OK(Put(static_cast<int>(i) /*cf*/, "key", "value", wopts));
  }
  std::vector<int> cf_ids;
  for (size_t i = 0; i != num_cfs; ++i) {
    cf_ids.emplace_back(static_cast<int>(i));
  }
  ASSERT_OK(Flush(cf_ids));
  for (size_t i = 0; i != num_cfs; ++i) {
    auto cfh = static_cast<ColumnFamilyHandleImpl*>(handles_[i]);
    ASSERT_EQ(0, cfh->cfd()->imm()->NumNotFlushed());
    ASSERT_TRUE(cfh->cfd()->mem()->IsEmpty());
  }
}

TEST_P(DBAtomicFlushTest, AtomicFlushTriggeredByMemTableFull) {
  Options options = CurrentOptions();
  options.create_if_missing = true;
  options.atomic_flush = GetParam();
  // 4KB so that we can easily trigger auto flush.
  options.write_buffer_size = 4096;

  SyncPoint::GetInstance()->LoadDependency(
      {{"DBImpl::BackgroundCallFlush:FlushFinish:0",
        "DBAtomicFlushTest::AtomicFlushTriggeredByMemTableFull:BeforeCheck"}});
  SyncPoint::GetInstance()->EnableProcessing();

  CreateAndReopenWithCF({"pikachu", "eevee"}, options);
  size_t num_cfs = handles_.size();
  ASSERT_EQ(3, num_cfs);
  WriteOptions wopts;
  wopts.disableWAL = true;
  for (size_t i = 0; i != num_cfs; ++i) {
    ASSERT_OK(Put(static_cast<int>(i) /*cf*/, "key", "value", wopts));
  }
  // Keep writing to one of them column families to trigger auto flush.
  for (int i = 0; i != 4000; ++i) {
    ASSERT_OK(Put(static_cast<int>(num_cfs) - 1 /*cf*/,
                  "key" + std::to_string(i), "value" + std::to_string(i),
                  wopts));
  }

  TEST_SYNC_POINT(
      "DBAtomicFlushTest::AtomicFlushTriggeredByMemTableFull:BeforeCheck");
  if (options.atomic_flush) {
    for (size_t i = 0; i + 1 != num_cfs; ++i) {
      auto cfh = static_cast<ColumnFamilyHandleImpl*>(handles_[i]);
      ASSERT_EQ(0, cfh->cfd()->imm()->NumNotFlushed());
      ASSERT_TRUE(cfh->cfd()->mem()->IsEmpty());
    }
  } else {
    for (size_t i = 0; i + 1 != num_cfs; ++i) {
      auto cfh = static_cast<ColumnFamilyHandleImpl*>(handles_[i]);
      ASSERT_EQ(0, cfh->cfd()->imm()->NumNotFlushed());
      ASSERT_FALSE(cfh->cfd()->mem()->IsEmpty());
    }
  }
  SyncPoint::GetInstance()->DisableProcessing();
}

TEST_P(DBAtomicFlushTest, AtomicFlushRollbackSomeJobs) {
  bool atomic_flush = GetParam();
  if (!atomic_flush) {
    return;
  }
  std::unique_ptr<FaultInjectionTestEnv> fault_injection_env(
      new FaultInjectionTestEnv(env_));
  Options options = CurrentOptions();
  options.create_if_missing = true;
  options.atomic_flush = atomic_flush;
  options.env = fault_injection_env.get();
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->LoadDependency(
      {{"DBImpl::AtomicFlushMemTablesToOutputFiles:SomeFlushJobsComplete:1",
        "DBAtomicFlushTest::AtomicFlushRollbackSomeJobs:1"},
       {"DBAtomicFlushTest::AtomicFlushRollbackSomeJobs:2",
        "DBImpl::AtomicFlushMemTablesToOutputFiles:SomeFlushJobsComplete:2"}});
  SyncPoint::GetInstance()->EnableProcessing();

  CreateAndReopenWithCF({"pikachu", "eevee"}, options);
  size_t num_cfs = handles_.size();
  ASSERT_EQ(3, num_cfs);
  WriteOptions wopts;
  wopts.disableWAL = true;
  for (size_t i = 0; i != num_cfs; ++i) {
    int cf_id = static_cast<int>(i);
    ASSERT_OK(Put(cf_id, "key", "value", wopts));
  }
  FlushOptions flush_opts;
  flush_opts.wait = false;
  ASSERT_OK(dbfull()->Flush(flush_opts, handles_));
  TEST_SYNC_POINT("DBAtomicFlushTest::AtomicFlushRollbackSomeJobs:1");
  fault_injection_env->SetFilesystemActive(false);
  TEST_SYNC_POINT("DBAtomicFlushTest::AtomicFlushRollbackSomeJobs:2");
  for (auto* cfh : handles_) {
    dbfull()->TEST_WaitForFlushMemTable(cfh);
  }
  for (size_t i = 0; i != num_cfs; ++i) {
    auto cfh = static_cast<ColumnFamilyHandleImpl*>(handles_[i]);
    ASSERT_EQ(1, cfh->cfd()->imm()->NumNotFlushed());
    ASSERT_TRUE(cfh->cfd()->mem()->IsEmpty());
  }
  fault_injection_env->SetFilesystemActive(true);
  Destroy(options);
}

TEST_P(DBAtomicFlushTest, FlushMultipleCFs_DropSomeBeforeRequestFlush) {
  bool atomic_flush = GetParam();
  if (!atomic_flush) {
    return;
  }
  Options options = CurrentOptions();
  options.create_if_missing = true;
  options.atomic_flush = atomic_flush;
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  SyncPoint::GetInstance()->EnableProcessing();

  CreateAndReopenWithCF({"pikachu", "eevee"}, options);
  size_t num_cfs = handles_.size();
  ASSERT_EQ(3, num_cfs);
  WriteOptions wopts;
  wopts.disableWAL = true;
  std::vector<int> cf_ids;
  for (size_t i = 0; i != num_cfs; ++i) {
    int cf_id = static_cast<int>(i);
    ASSERT_OK(Put(cf_id, "key", "value", wopts));
    cf_ids.push_back(cf_id);
  }
  ASSERT_OK(dbfull()->DropColumnFamily(handles_[1]));
  ASSERT_TRUE(Flush(cf_ids).IsColumnFamilyDropped());
  Destroy(options);
}

TEST_P(DBAtomicFlushTest,
       FlushMultipleCFs_DropSomeAfterScheduleFlushBeforeFlushJobRun) {
  bool atomic_flush = GetParam();
  if (!atomic_flush) {
    return;
  }
  Options options = CurrentOptions();
  options.create_if_missing = true;
  options.atomic_flush = atomic_flush;

  CreateAndReopenWithCF({"pikachu", "eevee"}, options);

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  SyncPoint::GetInstance()->LoadDependency(
      {{"DBImpl::AtomicFlushMemTables:AfterScheduleFlush",
        "DBAtomicFlushTest::BeforeDropCF"},
       {"DBAtomicFlushTest::AfterDropCF",
        "DBImpl::BackgroundCallFlush:start"}});
  SyncPoint::GetInstance()->EnableProcessing();

  size_t num_cfs = handles_.size();
  ASSERT_EQ(3, num_cfs);
  WriteOptions wopts;
  wopts.disableWAL = true;
  for (size_t i = 0; i != num_cfs; ++i) {
    int cf_id = static_cast<int>(i);
    ASSERT_OK(Put(cf_id, "key", "value", wopts));
  }
  port::Thread user_thread([&]() {
    TEST_SYNC_POINT("DBAtomicFlushTest::BeforeDropCF");
    ASSERT_OK(dbfull()->DropColumnFamily(handles_[1]));
    TEST_SYNC_POINT("DBAtomicFlushTest::AfterDropCF");
  });
  FlushOptions flush_opts;
  flush_opts.wait = true;
  ASSERT_OK(dbfull()->Flush(flush_opts, handles_));
  user_thread.join();
  for (size_t i = 0; i != num_cfs; ++i) {
    int cf_id = static_cast<int>(i);
    ASSERT_EQ("value", Get(cf_id, "key"));
  }

  ReopenWithColumnFamilies({kDefaultColumnFamilyName, "eevee"}, options);
  num_cfs = handles_.size();
  ASSERT_EQ(2, num_cfs);
  for (size_t i = 0; i != num_cfs; ++i) {
    int cf_id = static_cast<int>(i);
    ASSERT_EQ("value", Get(cf_id, "key"));
  }
  Destroy(options);
}

TEST_P(DBAtomicFlushTest, TriggerFlushAndClose) {
  bool atomic_flush = GetParam();
  if (!atomic_flush) {
    return;
  }
  const int kNumKeysTriggerFlush = 4;
  Options options = CurrentOptions();
  options.create_if_missing = true;
  options.atomic_flush = atomic_flush;
  options.memtable_factory.reset(
      new SpecialSkipListFactory(kNumKeysTriggerFlush));
  CreateAndReopenWithCF({"pikachu"}, options);

  for (int i = 0; i != kNumKeysTriggerFlush; ++i) {
    ASSERT_OK(Put(0, "key" + std::to_string(i), "value" + std::to_string(i)));
  }
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  SyncPoint::GetInstance()->EnableProcessing();
  ASSERT_OK(Put(0, "key", "value"));
  Close();

  ReopenWithColumnFamilies({kDefaultColumnFamilyName, "pikachu"}, options);
  ASSERT_EQ("value", Get(0, "key"));
}

TEST_P(DBAtomicFlushTest, PickMemtablesRaceWithBackgroundFlush) {
  bool atomic_flush = GetParam();
  Options options = CurrentOptions();
  options.create_if_missing = true;
  options.atomic_flush = atomic_flush;
  options.max_write_buffer_number = 4;
  // Set min_write_buffer_number_to_merge to be greater than 1, so that
  // a column family with one memtable in the imm will not cause IsFlushPending
  // to return true when flush_requested_ is false.
  options.min_write_buffer_number_to_merge = 2;
  CreateAndReopenWithCF({"pikachu"}, options);
  ASSERT_EQ(2, handles_.size());
  ASSERT_OK(dbfull()->PauseBackgroundWork());
  ASSERT_OK(Put(0, "key00", "value00"));
  ASSERT_OK(Put(1, "key10", "value10"));
  FlushOptions flush_opts;
  flush_opts.wait = false;
  ASSERT_OK(dbfull()->Flush(flush_opts, handles_));
  ASSERT_OK(Put(0, "key01", "value01"));
  // Since max_write_buffer_number is 4, the following flush won't cause write
  // stall.
  ASSERT_OK(dbfull()->Flush(flush_opts));
  ASSERT_OK(dbfull()->DropColumnFamily(handles_[1]));
  ASSERT_OK(dbfull()->DestroyColumnFamilyHandle(handles_[1]));
  handles_[1] = nullptr;
  ASSERT_OK(dbfull()->ContinueBackgroundWork());
  ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable(handles_[0]));
  delete handles_[0];
  handles_.clear();
}

TEST_P(DBAtomicFlushTest, CFDropRaceWithWaitForFlushMemTables) {
  bool atomic_flush = GetParam();
  if (!atomic_flush) {
    return;
  }
  Options options = CurrentOptions();
  options.create_if_missing = true;
  options.atomic_flush = atomic_flush;
  CreateAndReopenWithCF({"pikachu"}, options);
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->LoadDependency(
      {{"DBImpl::AtomicFlushMemTables:AfterScheduleFlush",
        "DBAtomicFlushTest::CFDropRaceWithWaitForFlushMemTables:BeforeDrop"},
       {"DBAtomicFlushTest::CFDropRaceWithWaitForFlushMemTables:AfterFree",
        "DBImpl::BackgroundCallFlush:start"},
       {"DBImpl::BackgroundCallFlush:start",
        "DBImpl::AtomicFlushMemTables:BeforeWaitForBgFlush"}});
  SyncPoint::GetInstance()->EnableProcessing();
  ASSERT_EQ(2, handles_.size());
  ASSERT_OK(Put(0, "key", "value"));
  ASSERT_OK(Put(1, "key", "value"));
  auto* cfd_default =
      static_cast<ColumnFamilyHandleImpl*>(dbfull()->DefaultColumnFamily())
          ->cfd();
  auto* cfd_pikachu = static_cast<ColumnFamilyHandleImpl*>(handles_[1])->cfd();
  port::Thread drop_cf_thr([&]() {
    TEST_SYNC_POINT(
        "DBAtomicFlushTest::CFDropRaceWithWaitForFlushMemTables:BeforeDrop");
    ASSERT_OK(dbfull()->DropColumnFamily(handles_[1]));
    delete handles_[1];
    handles_.resize(1);
    TEST_SYNC_POINT(
        "DBAtomicFlushTest::CFDropRaceWithWaitForFlushMemTables:AfterFree");
  });
  FlushOptions flush_opts;
  flush_opts.allow_write_stall = true;
  ASSERT_OK(dbfull()->TEST_AtomicFlushMemTables({cfd_default, cfd_pikachu},
                                                flush_opts));
  drop_cf_thr.join();
  Close();
  SyncPoint::GetInstance()->DisableProcessing();
}

TEST_P(DBAtomicFlushTest, RollbackAfterFailToInstallResults) {
  bool atomic_flush = GetParam();
  if (!atomic_flush) {
    return;
  }
  auto fault_injection_env = std::make_shared<FaultInjectionTestEnv>(env_);
  Options options = CurrentOptions();
  options.env = fault_injection_env.get();
  options.create_if_missing = true;
  options.atomic_flush = atomic_flush;
  CreateAndReopenWithCF({"pikachu"}, options);
  ASSERT_EQ(2, handles_.size());
  for (size_t cf = 0; cf < handles_.size(); ++cf) {
    ASSERT_OK(Put(static_cast<int>(cf), "a", "value"));
  }
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  SyncPoint::GetInstance()->SetCallBack(
      "VersionSet::ProcessManifestWrites:BeforeWriteLastVersionEdit:0",
      [&](void* /*arg*/) { fault_injection_env->SetFilesystemActive(false); });
  SyncPoint::GetInstance()->EnableProcessing();
  FlushOptions flush_opts;
  Status s = db_->Flush(flush_opts, handles_);
  ASSERT_NOK(s);
  fault_injection_env->SetFilesystemActive(true);
  Close();
  SyncPoint::GetInstance()->ClearAllCallBacks();
}

INSTANTIATE_TEST_CASE_P(DBFlushDirectIOTest, DBFlushDirectIOTest,
                        testing::Bool());

INSTANTIATE_TEST_CASE_P(DBAtomicFlushTest, DBAtomicFlushTest, testing::Bool());

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

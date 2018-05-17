//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/db_test_util.h"
#include "port/stack_trace.h"
#include "util/fault_injection_test_env.h"
#include "util/sync_point.h"

namespace rocksdb {

class DBFlushTest : public DBTestBase {
 public:
  DBFlushTest() : DBTestBase("/db_flush_test") {}
};

class DBFlushDirectIOTest : public DBFlushTest,
                            public ::testing::WithParamInterface<bool> {
 public:
  DBFlushDirectIOTest() : DBFlushTest() {}
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

  SyncPoint::GetInstance()->LoadDependency(
      {{"VersionSet::LogAndApply:WriteManifest",
        "DBFlushTest::FlushWhileWritingManifest:1"},
       {"MemTableList::InstallMemtableFlushResults:InProgress",
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

TEST_F(DBFlushTest, SyncFail) {
  std::unique_ptr<FaultInjectionTestEnv> fault_injection_env(
      new FaultInjectionTestEnv(env_));
  Options options;
  options.disable_auto_compactions = true;
  options.env = fault_injection_env.get();

  SyncPoint::GetInstance()->LoadDependency(
      {{"DBFlushTest::SyncFail:1", "DBImpl::SyncClosedLogs:Start"},
       {"DBImpl::SyncClosedLogs:Failed", "DBFlushTest::SyncFail:2"}});
  SyncPoint::GetInstance()->EnableProcessing();

  Reopen(options);
  Put("key", "value");
  auto* cfd =
      reinterpret_cast<ColumnFamilyHandleImpl*>(db_->DefaultColumnFamily())
          ->cfd();
  FlushOptions flush_options;
  flush_options.wait = false;
  ASSERT_OK(dbfull()->Flush(flush_options));
  // Flush installs a new super-version. Get the ref count after that.
  auto current_before = cfd->current();
  int refs_before = cfd->current()->TEST_refs();
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

INSTANTIATE_TEST_CASE_P(DBFlushDirectIOTest, DBFlushDirectIOTest,
                        testing::Bool());

TEST_F(DBFlushTest, AtomicFlushEmpty) {
  std::vector<Options> options_list(2);
  for (size_t i = 0; i != options_list.size(); ++i) {
    options_list[i].atomic_flush = true;
    options_list[i].disable_auto_compactions = true;
    options_list[i].env = env_;
  }

  // Create non-default column families.
  CreateColumnFamilies({"cf1", "cf2"}, options_list);
  // Reopen with all column families.
  Options default_cf_options;
  default_cf_options = options_list[0];
  options_list.insert(options_list.begin(), default_cf_options);
  ReopenWithColumnFamilies({"default", "cf1", "cf2"}, options_list);

  // All column families are empty.
  // If we flush now, we should see no sst file generated.
  FlushOptions flush_options;
  flush_options.wait = true;
  ASSERT_OK(dbfull()->Flush(flush_options));

  for (int cf = 0; cf != 3; ++cf) {
    ASSERT_EQ(0, NumTableFilesAtLevel(0, cf));
  }
}

TEST_F(DBFlushTest, AtomicFlushAllDirtyColumnFamilies) {
  std::vector<Options> options_list(2);
  for (size_t i = 0; i != options_list.size(); ++i) {
    options_list[i].atomic_flush = true;
    options_list[i].disable_auto_compactions = true;
    options_list[i].env = env_;
  }

  // Create non-default column families.
  CreateColumnFamilies({"cf1", "cf2"}, options_list);
  // Reopen with all column families.
  Options default_cf_options;
  default_cf_options = options_list[0];
  options_list.insert(options_list.begin(), default_cf_options);
  ReopenWithColumnFamilies({"default", "cf1", "cf2"}, options_list);

  WriteOptions write_options;
  write_options.disableWAL = true;
  ASSERT_OK(Put(0 /* cf */, "key0", "value0", write_options));
  ASSERT_OK(Put(1 /* cf */, "key1", "value1", write_options));

  // If we flush one cf, we should see all dirty cfs are flushed to L0.
  FlushOptions flush_options;
  flush_options.wait = true;
  ASSERT_OK(dbfull()->Flush(flush_options));

  ASSERT_EQ(1, NumTableFilesAtLevel(0 /* level */, 0 /* cf */));
  ASSERT_EQ(1, NumTableFilesAtLevel(0 /* level */, 1 /* cf */));
  ASSERT_EQ(0, NumTableFilesAtLevel(0 /* level */, 2 /* cf */));
}

TEST_F(DBFlushTest, AtomicFlushPointInTimeConsistentView) {
  std::vector<Options> options_list(2);
  for (size_t i = 0; i != options_list.size(); ++i) {
    options_list[i].atomic_flush = true;
    options_list[i].disable_auto_compactions = true;
    options_list[i].env = env_;
  }

  // Create non-default column families.
  CreateColumnFamilies({"cf1", "cf2"}, options_list);
  // Reopen with all column families.
  Options default_cf_options;
  default_cf_options = options_list[0];
  options_list.insert(options_list.begin(), default_cf_options);
  ReopenWithColumnFamilies({"default", "cf1", "cf2"}, options_list);

  SequenceNumber latest_seqno;
  rocksdb::SyncPoint::GetInstance()->SetCallBack(
      "BatchFlushJob::Run:CollectLargestSeqs", [&](void* arg) {
        std::map<std::string, uint64_t>* largest_seqs =
            reinterpret_cast<std::map<std::string, uint64_t>*>(arg);
        ASSERT_EQ(2, largest_seqs->size());
        for (const auto& iter : *largest_seqs) {
          ASSERT_TRUE(iter.second <= latest_seqno);
        }
      });
  typedef std::pair<std::string, std::string> KeyPair;
  std::map<std::string, KeyPair> saved_key_pairs;
  rocksdb::SyncPoint::GetInstance()->SetCallBack(
      "BatchFlushJob::Run:CollectKeyPairs", [&](void* arg) {
        auto key_pairs =
            reinterpret_cast<std::map<std::string, KeyPair>*>(arg);
        ASSERT_EQ(2, key_pairs->size());
        for (const auto& iter : *key_pairs) {
          const std::string& cf_name = iter.first;
          const std::pair<std::string, std::string>& key_pair = iter.second;
          auto it = saved_key_pairs.find(cf_name);
          ASSERT_NE(saved_key_pairs.end(), it);
          ASSERT_EQ(it->second, key_pair);
        }
      });
  rocksdb::SyncPoint::GetInstance()->EnableProcessing();

  WriteOptions write_options;
  write_options.disableWAL = true;
  for (int i = 0; i < 100; ++i) {
    ASSERT_OK(Put(0 /* cf */, Key(i), "value0", write_options));
    ASSERT_OK(Put(1 /* cf */, Key(i), "value1", write_options));
  }
  saved_key_pairs.insert({"default", {Key(0), Key(99)}});
  saved_key_pairs.insert({"cf1", {Key(0), Key(99)}});

  latest_seqno = dbfull()->GetLatestSequenceNumber();

  ASSERT_OK(dbfull()->Flush(FlushOptions()));
}

}  // namespace rocksdb

int main(int argc, char** argv) {
  rocksdb::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

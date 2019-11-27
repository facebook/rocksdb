//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef ROCKSDB_LITE

#include <stdlib.h>
#include <map>
#include <string>
#include <vector>
#include "db/db_impl/db_impl.h"
#include "db/db_test_util.h"
#include "db/version_set.h"
#include "db/write_batch_internal.h"
#include "file/filename.h"
#include "port/stack_trace.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/transaction_log.h"
#include "test_util/sync_point.h"
#include "test_util/testharness.h"
#include "test_util/testutil.h"
#include "util/string_util.h"

namespace rocksdb {

class DeleteFileTest : public DBTestBase {
 public:
  const int numlevels_;
  const std::string wal_dir_;

  DeleteFileTest()
      : DBTestBase("/deletefile_test"),
        numlevels_(7),
        wal_dir_(dbname_ + "/wal_files") {}

  void SetOptions(Options* options) {
    assert(options);
    options->delete_obsolete_files_period_micros = 0;  // always do full purge
    options->enable_thread_tracking = true;
    options->write_buffer_size = 1024 * 1024 * 1000;
    options->target_file_size_base = 1024 * 1024 * 1000;
    options->max_bytes_for_level_base = 1024 * 1024 * 1000;
    options->WAL_ttl_seconds = 300;     // Used to test log files
    options->WAL_size_limit_MB = 1024;  // Used to test log files
    options->wal_dir = wal_dir_;
  }

  void AddKeys(int numkeys, int startkey = 0) {
    WriteOptions options;
    options.sync = false;
    ReadOptions roptions;
    for (int i = startkey; i < (numkeys + startkey) ; i++) {
      std::string temp = ToString(i);
      Slice key(temp);
      Slice value(temp);
      ASSERT_OK(db_->Put(options, key, value));
    }
  }

  int numKeysInLevels(
    std::vector<LiveFileMetaData> &metadata,
    std::vector<int> *keysperlevel = nullptr) {

    if (keysperlevel != nullptr) {
      keysperlevel->resize(numlevels_);
    }

    int numKeys = 0;
    for (size_t i = 0; i < metadata.size(); i++) {
      int startkey = atoi(metadata[i].smallestkey.c_str());
      int endkey = atoi(metadata[i].largestkey.c_str());
      int numkeysinfile = (endkey - startkey + 1);
      numKeys += numkeysinfile;
      if (keysperlevel != nullptr) {
        (*keysperlevel)[(int)metadata[i].level] += numkeysinfile;
      }
      fprintf(stderr, "level %d name %s smallest %s largest %s\n",
              metadata[i].level, metadata[i].name.c_str(),
              metadata[i].smallestkey.c_str(),
              metadata[i].largestkey.c_str());
    }
    return numKeys;
  }

  void CreateTwoLevels() {
    AddKeys(50000, 10000);
    ASSERT_OK(dbfull()->TEST_FlushMemTable());
    ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());
    for (int i = 0; i < 2; ++i) {
      ASSERT_OK(dbfull()->TEST_CompactRange(i, nullptr, nullptr));
    }

    AddKeys(50000, 10000);
    ASSERT_OK(dbfull()->TEST_FlushMemTable());
    ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());
    ASSERT_OK(dbfull()->TEST_CompactRange(0, nullptr, nullptr));
  }

  void CheckFileTypeCounts(const std::string& dir, int required_log,
                           int required_sst, int required_manifest) {
    std::vector<std::string> filenames;
    env_->GetChildren(dir, &filenames);

    int log_cnt = 0, sst_cnt = 0, manifest_cnt = 0;
    for (auto file : filenames) {
      uint64_t number;
      FileType type;
      if (ParseFileName(file, &number, &type)) {
        log_cnt += (type == kLogFile);
        sst_cnt += (type == kTableFile);
        manifest_cnt += (type == kDescriptorFile);
      }
    }
    ASSERT_EQ(required_log, log_cnt);
    ASSERT_EQ(required_sst, sst_cnt);
    ASSERT_EQ(required_manifest, manifest_cnt);
  }

  static void DoSleep(void* arg) {
    auto test = reinterpret_cast<DeleteFileTest*>(arg);
    test->env_->SleepForMicroseconds(2 * 1000 * 1000);
  }

  // An empty job to guard all jobs are processed
  static void GuardFinish(void* /*arg*/) {
    TEST_SYNC_POINT("DeleteFileTest::GuardFinish");
  }
};

TEST_F(DeleteFileTest, AddKeysAndQueryLevels) {
  Options options = CurrentOptions();
  SetOptions(&options);
  Destroy(options);
  options.create_if_missing = true;
  Reopen(options);

  CreateTwoLevels();
  std::vector<LiveFileMetaData> metadata;
  db_->GetLiveFilesMetaData(&metadata);

  std::string level1file = "";
  int level1keycount = 0;
  std::string level2file = "";
  int level2keycount = 0;
  int level1index = 0;
  int level2index = 1;

  ASSERT_EQ((int)metadata.size(), 2);
  if (metadata[0].level == 2) {
    level1index = 1;
    level2index = 0;
  }

  level1file = metadata[level1index].name;
  int startkey = atoi(metadata[level1index].smallestkey.c_str());
  int endkey = atoi(metadata[level1index].largestkey.c_str());
  level1keycount = (endkey - startkey + 1);
  level2file = metadata[level2index].name;
  startkey = atoi(metadata[level2index].smallestkey.c_str());
  endkey = atoi(metadata[level2index].largestkey.c_str());
  level2keycount = (endkey - startkey + 1);

  // COntrolled setup. Levels 1 and 2 should both have 50K files.
  // This is a little fragile as it depends on the current
  // compaction heuristics.
  ASSERT_EQ(level1keycount, 50000);
  ASSERT_EQ(level2keycount, 50000);

  Status status = db_->DeleteFile("0.sst");
  ASSERT_TRUE(status.IsInvalidArgument());

  // intermediate level files cannot be deleted.
  status = db_->DeleteFile(level1file);
  ASSERT_TRUE(status.IsInvalidArgument());

  // Lowest level file deletion should succeed.
  ASSERT_OK(db_->DeleteFile(level2file));
}

TEST_F(DeleteFileTest, PurgeObsoleteFilesTest) {
  Options options = CurrentOptions();
  SetOptions(&options);
  Destroy(options);
  options.create_if_missing = true;
  Reopen(options);

  CreateTwoLevels();
  // there should be only one (empty) log file because CreateTwoLevels()
  // flushes the memtables to disk
  CheckFileTypeCounts(wal_dir_, 1, 0, 0);
  // 2 ssts, 1 manifest
  CheckFileTypeCounts(dbname_, 0, 2, 1);
  std::string first("0"), last("999999");
  CompactRangeOptions compact_options;
  compact_options.change_level = true;
  compact_options.target_level = 2;
  Slice first_slice(first), last_slice(last);
  db_->CompactRange(compact_options, &first_slice, &last_slice);
  // 1 sst after compaction
  CheckFileTypeCounts(dbname_, 0, 1, 1);

  // this time, we keep an iterator alive
  Reopen(options);
  Iterator *itr = nullptr;
  CreateTwoLevels();
  itr = db_->NewIterator(ReadOptions());
  db_->CompactRange(compact_options, &first_slice, &last_slice);
  // 3 sst after compaction with live iterator
  CheckFileTypeCounts(dbname_, 0, 3, 1);
  delete itr;
  // 1 sst after iterator deletion
  CheckFileTypeCounts(dbname_, 0, 1, 1);
}

TEST_F(DeleteFileTest, BackgroundPurgeIteratorTest) {
  Options options = CurrentOptions();
  SetOptions(&options);
  Destroy(options);
  options.create_if_missing = true;
  Reopen(options);

  std::string first("0"), last("999999");
  CompactRangeOptions compact_options;
  compact_options.change_level = true;
  compact_options.target_level = 2;
  Slice first_slice(first), last_slice(last);

  // We keep an iterator alive
  Iterator* itr = nullptr;
  CreateTwoLevels();
  ReadOptions read_options;
  read_options.background_purge_on_iterator_cleanup = true;
  itr = db_->NewIterator(read_options);
  db_->CompactRange(compact_options, &first_slice, &last_slice);
  // 3 sst after compaction with live iterator
  CheckFileTypeCounts(dbname_, 0, 3, 1);
  test::SleepingBackgroundTask sleeping_task_before;
  env_->Schedule(&test::SleepingBackgroundTask::DoSleepTask,
                 &sleeping_task_before, Env::Priority::HIGH);
  delete itr;
  test::SleepingBackgroundTask sleeping_task_after;
  env_->Schedule(&test::SleepingBackgroundTask::DoSleepTask,
                 &sleeping_task_after, Env::Priority::HIGH);

  // Make sure no purges are executed foreground
  CheckFileTypeCounts(dbname_, 0, 3, 1);
  sleeping_task_before.WakeUp();
  sleeping_task_before.WaitUntilDone();

  // Make sure all background purges are executed
  sleeping_task_after.WakeUp();
  sleeping_task_after.WaitUntilDone();
  // 1 sst after iterator deletion
  CheckFileTypeCounts(dbname_, 0, 1, 1);
}

TEST_F(DeleteFileTest, BackgroundPurgeCFDropTest) {
  Options options = CurrentOptions();
  SetOptions(&options);
  Destroy(options);
  options.create_if_missing = true;
  Reopen(options);

  auto do_test = [&](bool bg_purge) {
    ColumnFamilyOptions co;
    co.max_write_buffer_size_to_maintain =
        static_cast<int64_t>(co.write_buffer_size);
    WriteOptions wo;
    FlushOptions fo;
    ColumnFamilyHandle* cfh = nullptr;

    ASSERT_OK(db_->CreateColumnFamily(co, "dropme", &cfh));

    ASSERT_OK(db_->Put(wo, cfh, "pika", "chu"));
    ASSERT_OK(db_->Flush(fo, cfh));
    // Expect 1 sst file.
    CheckFileTypeCounts(dbname_, 0, 1, 1);

    ASSERT_OK(db_->DropColumnFamily(cfh));
    // Still 1 file, it won't be deleted while ColumnFamilyHandle is alive.
    CheckFileTypeCounts(dbname_, 0, 1, 1);

    delete cfh;
    test::SleepingBackgroundTask sleeping_task_after;
    env_->Schedule(&test::SleepingBackgroundTask::DoSleepTask,
                   &sleeping_task_after, Env::Priority::HIGH);
    // If background purge is enabled, the file should still be there.
    CheckFileTypeCounts(dbname_, 0, bg_purge ? 1 : 0, 1);
    TEST_SYNC_POINT("DeleteFileTest::BackgroundPurgeCFDropTest:1");

    // Execute background purges.
    sleeping_task_after.WakeUp();
    sleeping_task_after.WaitUntilDone();
    // The file should have been deleted.
    CheckFileTypeCounts(dbname_, 0, 0, 1);
  };

  {
    SCOPED_TRACE("avoid_unnecessary_blocking_io = false");
    do_test(false);
  }

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  SyncPoint::GetInstance()->LoadDependency(
      {{"DeleteFileTest::BackgroundPurgeCFDropTest:1",
        "DBImpl::BGWorkPurge:start"}});
  SyncPoint::GetInstance()->EnableProcessing();

  options.avoid_unnecessary_blocking_io = true;
  options.create_if_missing = false;
  Reopen(options);
  {
    SCOPED_TRACE("avoid_unnecessary_blocking_io = true");
    do_test(true);
  }
}

// This test is to reproduce a bug that read invalid ReadOption in iterator
// cleanup function
TEST_F(DeleteFileTest, BackgroundPurgeCopyOptions) {
  Options options = CurrentOptions();
  SetOptions(&options);
  Destroy(options);
  options.create_if_missing = true;
  Reopen(options);

  std::string first("0"), last("999999");
  CompactRangeOptions compact_options;
  compact_options.change_level = true;
  compact_options.target_level = 2;
  Slice first_slice(first), last_slice(last);

  // We keep an iterator alive
  Iterator* itr = nullptr;
  CreateTwoLevels();
  {
    ReadOptions read_options;
    read_options.background_purge_on_iterator_cleanup = true;
    itr = db_->NewIterator(read_options);
    // ReadOptions is deleted, but iterator cleanup function should not be
    // affected
  }

  db_->CompactRange(compact_options, &first_slice, &last_slice);
  // 3 sst after compaction with live iterator
  CheckFileTypeCounts(dbname_, 0, 3, 1);
  delete itr;

  test::SleepingBackgroundTask sleeping_task_after;
  env_->Schedule(&test::SleepingBackgroundTask::DoSleepTask,
                 &sleeping_task_after, Env::Priority::HIGH);

  // Make sure all background purges are executed
  sleeping_task_after.WakeUp();
  sleeping_task_after.WaitUntilDone();
  // 1 sst after iterator deletion
  CheckFileTypeCounts(dbname_, 0, 1, 1);
}

TEST_F(DeleteFileTest, BackgroundPurgeTestMultipleJobs) {
  Options options = CurrentOptions();
  SetOptions(&options);
  Destroy(options);
  options.create_if_missing = true;
  Reopen(options);

  std::string first("0"), last("999999");
  CompactRangeOptions compact_options;
  compact_options.change_level = true;
  compact_options.target_level = 2;
  Slice first_slice(first), last_slice(last);

  // We keep an iterator alive
  CreateTwoLevels();
  ReadOptions read_options;
  read_options.background_purge_on_iterator_cleanup = true;
  Iterator* itr1 = db_->NewIterator(read_options);
  CreateTwoLevels();
  Iterator* itr2 = db_->NewIterator(read_options);
  db_->CompactRange(compact_options, &first_slice, &last_slice);
  // 5 sst files after 2 compactions with 2 live iterators
  CheckFileTypeCounts(dbname_, 0, 5, 1);

  rocksdb::SyncPoint::GetInstance()->DisableProcessing();
  // ~DBImpl should wait until all BGWorkPurge are finished
  rocksdb::SyncPoint::GetInstance()->LoadDependency(
      {{"DBImpl::~DBImpl:WaitJob", "DBImpl::BGWorkPurge"},
       {"DeleteFileTest::GuardFinish",
        "DeleteFileTest::BackgroundPurgeTestMultipleJobs:DBClose"}});
  rocksdb::SyncPoint::GetInstance()->EnableProcessing();

  delete itr1;
  env_->Schedule(&DeleteFileTest::DoSleep, this, Env::Priority::HIGH);
  delete itr2;
  env_->Schedule(&DeleteFileTest::GuardFinish, nullptr, Env::Priority::HIGH);
  Close();

  TEST_SYNC_POINT("DeleteFileTest::BackgroundPurgeTestMultipleJobs:DBClose");
  // 1 sst after iterator deletion
  CheckFileTypeCounts(dbname_, 0, 1, 1);
}

TEST_F(DeleteFileTest, DeleteFileWithIterator) {
  Options options = CurrentOptions();
  SetOptions(&options);
  Destroy(options);
  options.create_if_missing = true;
  Reopen(options);

  CreateTwoLevels();
  ReadOptions read_options;
  Iterator* it = db_->NewIterator(read_options);
  std::vector<LiveFileMetaData> metadata;
  db_->GetLiveFilesMetaData(&metadata);

  std::string level2file;

  ASSERT_EQ(metadata.size(), static_cast<size_t>(2));
  if (metadata[0].level == 1) {
    level2file = metadata[1].name;
  } else {
    level2file = metadata[0].name;
  }

  Status status = db_->DeleteFile(level2file);
  fprintf(stdout, "Deletion status %s: %s\n",
          level2file.c_str(), status.ToString().c_str());
  ASSERT_TRUE(status.ok());
  it->SeekToFirst();
  int numKeysIterated = 0;
  while(it->Valid()) {
    numKeysIterated++;
    it->Next();
  }
  ASSERT_EQ(numKeysIterated, 50000);
  delete it;
}

TEST_F(DeleteFileTest, DeleteLogFiles) {
  Options options = CurrentOptions();
  SetOptions(&options);
  Destroy(options);
  options.create_if_missing = true;
  Reopen(options);

  AddKeys(10, 0);
  VectorLogPtr logfiles;
  db_->GetSortedWalFiles(logfiles);
  ASSERT_GT(logfiles.size(), 0UL);
  // Take the last log file which is expected to be alive and try to delete it
  // Should not succeed because live logs are not allowed to be deleted
  std::unique_ptr<LogFile> alive_log = std::move(logfiles.back());
  ASSERT_EQ(alive_log->Type(), kAliveLogFile);
  ASSERT_OK(env_->FileExists(wal_dir_ + "/" + alive_log->PathName()));
  fprintf(stdout, "Deleting alive log file %s\n",
          alive_log->PathName().c_str());
  ASSERT_TRUE(!db_->DeleteFile(alive_log->PathName()).ok());
  ASSERT_OK(env_->FileExists(wal_dir_ + "/" + alive_log->PathName()));
  logfiles.clear();

  // Call Flush to bring about a new working log file and add more keys
  // Call Flush again to flush out memtable and move alive log to archived log
  // and try to delete the archived log file
  FlushOptions fopts;
  db_->Flush(fopts);
  AddKeys(10, 0);
  db_->Flush(fopts);
  db_->GetSortedWalFiles(logfiles);
  ASSERT_GT(logfiles.size(), 0UL);
  std::unique_ptr<LogFile> archived_log = std::move(logfiles.front());
  ASSERT_EQ(archived_log->Type(), kArchivedLogFile);
  ASSERT_OK(env_->FileExists(wal_dir_ + "/" + archived_log->PathName()));
  fprintf(stdout, "Deleting archived log file %s\n",
          archived_log->PathName().c_str());
  ASSERT_OK(db_->DeleteFile(archived_log->PathName()));
  ASSERT_EQ(Status::NotFound(),
            env_->FileExists(wal_dir_ + "/" + archived_log->PathName()));
}

TEST_F(DeleteFileTest, DeleteNonDefaultColumnFamily) {
  Options options = CurrentOptions();
  SetOptions(&options);
  Destroy(options);
  options.create_if_missing = true;
  Reopen(options);
  CreateAndReopenWithCF({"new_cf"}, options);

  Random rnd(5);
  for (int i = 0; i < 1000; ++i) {
    ASSERT_OK(db_->Put(WriteOptions(), handles_[1], test::RandomKey(&rnd, 10),
                       test::RandomKey(&rnd, 10)));
  }
  ASSERT_OK(db_->Flush(FlushOptions(), handles_[1]));
  for (int i = 0; i < 1000; ++i) {
    ASSERT_OK(db_->Put(WriteOptions(), handles_[1], test::RandomKey(&rnd, 10),
                       test::RandomKey(&rnd, 10)));
  }
  ASSERT_OK(db_->Flush(FlushOptions(), handles_[1]));

  std::vector<LiveFileMetaData> metadata;
  db_->GetLiveFilesMetaData(&metadata);
  ASSERT_EQ(2U, metadata.size());
  ASSERT_EQ("new_cf", metadata[0].column_family_name);
  ASSERT_EQ("new_cf", metadata[1].column_family_name);
  auto old_file = metadata[0].smallest_seqno < metadata[1].smallest_seqno
                      ? metadata[0].name
                      : metadata[1].name;
  auto new_file = metadata[0].smallest_seqno > metadata[1].smallest_seqno
                      ? metadata[0].name
                      : metadata[1].name;
  ASSERT_TRUE(db_->DeleteFile(new_file).IsInvalidArgument());
  ASSERT_OK(db_->DeleteFile(old_file));

  {
    std::unique_ptr<Iterator> itr(db_->NewIterator(ReadOptions(), handles_[1]));
    int count = 0;
    for (itr->SeekToFirst(); itr->Valid(); itr->Next()) {
      ASSERT_OK(itr->status());
      ++count;
    }
    ASSERT_EQ(count, 1000);
  }

  Close();
  ReopenWithColumnFamilies({kDefaultColumnFamilyName, "new_cf"}, options);

  {
    std::unique_ptr<Iterator> itr(db_->NewIterator(ReadOptions(), handles_[1]));
    int count = 0;
    for (itr->SeekToFirst(); itr->Valid(); itr->Next()) {
      ASSERT_OK(itr->status());
      ++count;
    }
    ASSERT_EQ(count, 1000);
  }
}

} //namespace rocksdb

#ifdef ROCKSDB_UNITTESTS_WITH_CUSTOM_OBJECTS_FROM_STATIC_LIBS
extern "C" {
void RegisterCustomObjects(int argc, char** argv);
}
#else
void RegisterCustomObjects(int /*argc*/, char** /*argv*/) {}
#endif  // !ROCKSDB_UNITTESTS_WITH_CUSTOM_OBJECTS_FROM_STATIC_LIBS

int main(int argc, char** argv) {
  rocksdb::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  RegisterCustomObjects(argc, argv);
  return RUN_ALL_TESTS();
}

#else
#include <stdio.h>

int main(int /*argc*/, char** /*argv*/) {
  fprintf(stderr,
          "SKIPPED as DBImpl::DeleteFile is not supported in ROCKSDB_LITE\n");
  return 0;
}

#endif  // !ROCKSDB_LITE

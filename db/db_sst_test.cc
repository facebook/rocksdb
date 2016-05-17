//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/db_test_util.h"
#include "port/stack_trace.h"
#include "rocksdb/sst_file_manager.h"
#include "rocksdb/sst_file_writer.h"
#include "util/sst_file_manager_impl.h"

namespace rocksdb {

class DBSSTTest : public DBTestBase {
 public:
  DBSSTTest() : DBTestBase("/db_sst_test") {}
};

TEST_F(DBSSTTest, DontDeletePendingOutputs) {
  Options options;
  options.env = env_;
  options.create_if_missing = true;
  DestroyAndReopen(options);

  // Every time we write to a table file, call FOF/POF with full DB scan. This
  // will make sure our pending_outputs_ protection work correctly
  std::function<void()> purge_obsolete_files_function = [&]() {
    JobContext job_context(0);
    dbfull()->TEST_LockMutex();
    dbfull()->FindObsoleteFiles(&job_context, true /*force*/);
    dbfull()->TEST_UnlockMutex();
    dbfull()->PurgeObsoleteFiles(job_context);
    job_context.Clean();
  };

  env_->table_write_callback_ = &purge_obsolete_files_function;

  for (int i = 0; i < 2; ++i) {
    ASSERT_OK(Put("a", "begin"));
    ASSERT_OK(Put("z", "end"));
    ASSERT_OK(Flush());
  }

  // If pending output guard does not work correctly, PurgeObsoleteFiles() will
  // delete the file that Compaction is trying to create, causing this: error
  // db/db_test.cc:975: IO error:
  // /tmp/rocksdbtest-1552237650/db_test/000009.sst: No such file or directory
  Compact("a", "b");
}

#ifndef ROCKSDB_LITE
TEST_F(DBSSTTest, DontDeleteMovedFile) {
  // This test triggers move compaction and verifies that the file is not
  // deleted when it's part of move compaction
  Options options = CurrentOptions();
  options.env = env_;
  options.create_if_missing = true;
  options.max_bytes_for_level_base = 1024 * 1024;  // 1 MB
  options.level0_file_num_compaction_trigger =
      2;  // trigger compaction when we have 2 files
  DestroyAndReopen(options);

  Random rnd(301);
  // Create two 1MB sst files
  for (int i = 0; i < 2; ++i) {
    // Create 1MB sst file
    for (int j = 0; j < 100; ++j) {
      ASSERT_OK(Put(Key(i * 50 + j), RandomString(&rnd, 10 * 1024)));
    }
    ASSERT_OK(Flush());
  }
  // this should execute both L0->L1 and L1->(move)->L2 compactions
  dbfull()->TEST_WaitForCompact();
  ASSERT_EQ("0,0,1", FilesPerLevel(0));

  // If the moved file is actually deleted (the move-safeguard in
  // ~Version::Version() is not there), we get this failure:
  // Corruption: Can't access /000009.sst
  Reopen(options);
}

// This reproduces a bug where we don't delete a file because when it was
// supposed to be deleted, it was blocked by pending_outputs
// Consider:
// 1. current file_number is 13
// 2. compaction (1) starts, blocks deletion of all files starting with 13
// (pending outputs)
// 3. file 13 is created by compaction (2)
// 4. file 13 is consumed by compaction (3) and file 15 was created. Since file
// 13 has no references, it is put into VersionSet::obsolete_files_
// 5. FindObsoleteFiles() gets file 13 from VersionSet::obsolete_files_. File 13
// is deleted from obsolete_files_ set.
// 6. PurgeObsoleteFiles() tries to delete file 13, but this file is blocked by
// pending outputs since compaction (1) is still running. It is not deleted and
// it is not present in obsolete_files_ anymore. Therefore, we never delete it.
TEST_F(DBSSTTest, DeleteObsoleteFilesPendingOutputs) {
  Options options = CurrentOptions();
  options.env = env_;
  options.write_buffer_size = 2 * 1024 * 1024;     // 2 MB
  options.max_bytes_for_level_base = 1024 * 1024;  // 1 MB
  options.level0_file_num_compaction_trigger =
      2;  // trigger compaction when we have 2 files
  options.max_background_flushes = 2;
  options.max_background_compactions = 2;

  OnFileDeletionListener* listener = new OnFileDeletionListener();
  options.listeners.emplace_back(listener);

  Reopen(options);

  Random rnd(301);
  // Create two 1MB sst files
  for (int i = 0; i < 2; ++i) {
    // Create 1MB sst file
    for (int j = 0; j < 100; ++j) {
      ASSERT_OK(Put(Key(i * 50 + j), RandomString(&rnd, 10 * 1024)));
    }
    ASSERT_OK(Flush());
  }
  // this should execute both L0->L1 and L1->(move)->L2 compactions
  dbfull()->TEST_WaitForCompact();
  ASSERT_EQ("0,0,1", FilesPerLevel(0));

  test::SleepingBackgroundTask blocking_thread;
  port::Mutex mutex_;
  bool already_blocked(false);

  // block the flush
  std::function<void()> block_first_time = [&]() {
    bool blocking = false;
    {
      MutexLock l(&mutex_);
      if (!already_blocked) {
        blocking = true;
        already_blocked = true;
      }
    }
    if (blocking) {
      blocking_thread.DoSleep();
    }
  };
  env_->table_write_callback_ = &block_first_time;
  // Create 1MB sst file
  for (int j = 0; j < 256; ++j) {
    ASSERT_OK(Put(Key(j), RandomString(&rnd, 10 * 1024)));
  }
  // this should trigger a flush, which is blocked with block_first_time
  // pending_file is protecting all the files created after

  ASSERT_OK(dbfull()->TEST_CompactRange(2, nullptr, nullptr));

  ASSERT_EQ("0,0,0,1", FilesPerLevel(0));
  std::vector<LiveFileMetaData> metadata;
  db_->GetLiveFilesMetaData(&metadata);
  ASSERT_EQ(metadata.size(), 1U);
  auto file_on_L2 = metadata[0].name;
  listener->SetExpectedFileName(dbname_ + file_on_L2);

  ASSERT_OK(dbfull()->TEST_CompactRange(3, nullptr, nullptr, nullptr,
                                        true /* disallow trivial move */));
  ASSERT_EQ("0,0,0,0,1", FilesPerLevel(0));

  // finish the flush!
  blocking_thread.WakeUp();
  blocking_thread.WaitUntilDone();
  dbfull()->TEST_WaitForFlushMemTable();
  ASSERT_EQ("1,0,0,0,1", FilesPerLevel(0));

  metadata.clear();
  db_->GetLiveFilesMetaData(&metadata);
  ASSERT_EQ(metadata.size(), 2U);

  // This file should have been deleted during last compaction
  ASSERT_EQ(Status::NotFound(), env_->FileExists(dbname_ + file_on_L2));
  listener->VerifyMatchedCount(1);
}

#endif  // ROCKSDB_LITE

TEST_F(DBSSTTest, DBWithSstFileManager) {
  std::shared_ptr<SstFileManager> sst_file_manager(NewSstFileManager(env_));
  auto sfm = static_cast<SstFileManagerImpl*>(sst_file_manager.get());

  int files_added = 0;
  int files_deleted = 0;
  int files_moved = 0;
  rocksdb::SyncPoint::GetInstance()->SetCallBack(
      "SstFileManagerImpl::OnAddFile", [&](void* arg) { files_added++; });
  rocksdb::SyncPoint::GetInstance()->SetCallBack(
      "SstFileManagerImpl::OnDeleteFile", [&](void* arg) { files_deleted++; });
  rocksdb::SyncPoint::GetInstance()->SetCallBack(
      "SstFileManagerImpl::OnMoveFile", [&](void* arg) { files_moved++; });
  rocksdb::SyncPoint::GetInstance()->EnableProcessing();

  Options options = CurrentOptions();
  options.sst_file_manager = sst_file_manager;
  DestroyAndReopen(options);

  Random rnd(301);
  for (int i = 0; i < 25; i++) {
    GenerateNewRandomFile(&rnd);
    ASSERT_OK(Flush());
    dbfull()->TEST_WaitForFlushMemTable();
    dbfull()->TEST_WaitForCompact();
    // Verify that we are tracking all sst files in dbname_
    ASSERT_EQ(sfm->GetTrackedFiles(), GetAllSSTFiles());
  }
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));

  auto files_in_db = GetAllSSTFiles();
  // Verify that we are tracking all sst files in dbname_
  ASSERT_EQ(sfm->GetTrackedFiles(), files_in_db);
  // Verify the total files size
  uint64_t total_files_size = 0;
  for (auto& file_to_size : files_in_db) {
    total_files_size += file_to_size.second;
  }
  ASSERT_EQ(sfm->GetTotalSize(), total_files_size);
  // We flushed at least 25 files
  ASSERT_GE(files_added, 25);
  // Compaction must have deleted some files
  ASSERT_GT(files_deleted, 0);
  // No files were moved
  ASSERT_EQ(files_moved, 0);

  Close();
  Reopen(options);
  ASSERT_EQ(sfm->GetTrackedFiles(), files_in_db);
  ASSERT_EQ(sfm->GetTotalSize(), total_files_size);

  // Verify that we track all the files again after the DB is closed and opened
  Close();
  sst_file_manager.reset(NewSstFileManager(env_));
  options.sst_file_manager = sst_file_manager;
  sfm = static_cast<SstFileManagerImpl*>(sst_file_manager.get());

  Reopen(options);
  ASSERT_EQ(sfm->GetTrackedFiles(), files_in_db);
  ASSERT_EQ(sfm->GetTotalSize(), total_files_size);

  rocksdb::SyncPoint::GetInstance()->DisableProcessing();
}

#ifndef ROCKSDB_LITE
TEST_F(DBSSTTest, RateLimitedDelete) {
  Destroy(last_options_);
  rocksdb::SyncPoint::GetInstance()->LoadDependency({
      {"DBSSTTest::RateLimitedDelete:1",
       "DeleteScheduler::BackgroundEmptyTrash"},
  });

  std::vector<uint64_t> penalties;
  rocksdb::SyncPoint::GetInstance()->SetCallBack(
      "DeleteScheduler::BackgroundEmptyTrash:Wait",
      [&](void* arg) { penalties.push_back(*(static_cast<int*>(arg))); });
  rocksdb::SyncPoint::GetInstance()->EnableProcessing();

  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  options.env = env_;

  std::string trash_dir = test::TmpDir(env_) + "/trash";
  int64_t rate_bytes_per_sec = 1024 * 10;  // 10 Kbs / Sec
  Status s;
  options.sst_file_manager.reset(NewSstFileManager(
      env_, nullptr, trash_dir, rate_bytes_per_sec, false, &s));
  ASSERT_OK(s);
  auto sfm = static_cast<SstFileManagerImpl*>(options.sst_file_manager.get());

  ASSERT_OK(TryReopen(options));
  // Create 4 files in L0
  for (char v = 'a'; v <= 'd'; v++) {
    ASSERT_OK(Put("Key2", DummyString(1024, v)));
    ASSERT_OK(Put("Key3", DummyString(1024, v)));
    ASSERT_OK(Put("Key4", DummyString(1024, v)));
    ASSERT_OK(Put("Key1", DummyString(1024, v)));
    ASSERT_OK(Put("Key4", DummyString(1024, v)));
    ASSERT_OK(Flush());
  }
  // We created 4 sst files in L0
  ASSERT_EQ("4", FilesPerLevel(0));

  std::vector<LiveFileMetaData> metadata;
  db_->GetLiveFilesMetaData(&metadata);

  // Compaction will move the 4 files in L0 to trash and create 1 L1 file
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  ASSERT_EQ("0,1", FilesPerLevel(0));

  uint64_t delete_start_time = env_->NowMicros();
  // Hold BackgroundEmptyTrash
  TEST_SYNC_POINT("DBSSTTest::RateLimitedDelete:1");
  sfm->WaitForEmptyTrash();
  uint64_t time_spent_deleting = env_->NowMicros() - delete_start_time;

  uint64_t total_files_size = 0;
  uint64_t expected_penlty = 0;
  ASSERT_EQ(penalties.size(), metadata.size());
  for (size_t i = 0; i < metadata.size(); i++) {
    total_files_size += metadata[i].size;
    expected_penlty = ((total_files_size * 1000000) / rate_bytes_per_sec);
    ASSERT_EQ(expected_penlty, penalties[i]);
  }
  ASSERT_GT(time_spent_deleting, expected_penlty * 0.9);

  rocksdb::SyncPoint::GetInstance()->DisableProcessing();
}

// Create a DB with 2 db_paths, and generate multiple files in the 2
// db_paths using CompactRangeOptions, make sure that files that were
// deleted from first db_path were deleted using DeleteScheduler and
// files in the second path were not.
TEST_F(DBSSTTest, DeleteSchedulerMultipleDBPaths) {
  int bg_delete_file = 0;
  rocksdb::SyncPoint::GetInstance()->SetCallBack(
      "DeleteScheduler::DeleteTrashFile:DeleteFile",
      [&](void* arg) { bg_delete_file++; });
  rocksdb::SyncPoint::GetInstance()->EnableProcessing();

  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  options.db_paths.emplace_back(dbname_, 1024 * 100);
  options.db_paths.emplace_back(dbname_ + "_2", 1024 * 100);
  options.env = env_;

  std::string trash_dir = test::TmpDir(env_) + "/trash";
  int64_t rate_bytes_per_sec = 1024 * 1024;  // 1 Mb / Sec
  Status s;
  options.sst_file_manager.reset(NewSstFileManager(
      env_, nullptr, trash_dir, rate_bytes_per_sec, false, &s));
  ASSERT_OK(s);
  auto sfm = static_cast<SstFileManagerImpl*>(options.sst_file_manager.get());

  DestroyAndReopen(options);

  // Create 4 files in L0
  for (int i = 0; i < 4; i++) {
    ASSERT_OK(Put("Key" + ToString(i), DummyString(1024, 'A')));
    ASSERT_OK(Flush());
  }
  // We created 4 sst files in L0
  ASSERT_EQ("4", FilesPerLevel(0));
  // Compaction will delete files from L0 in first db path and generate a new
  // file in L1 in second db path
  CompactRangeOptions compact_options;
  compact_options.target_path_id = 1;
  Slice begin("Key0");
  Slice end("Key3");
  ASSERT_OK(db_->CompactRange(compact_options, &begin, &end));
  ASSERT_EQ("0,1", FilesPerLevel(0));

  // Create 4 files in L0
  for (int i = 4; i < 8; i++) {
    ASSERT_OK(Put("Key" + ToString(i), DummyString(1024, 'B')));
    ASSERT_OK(Flush());
  }
  ASSERT_EQ("4,1", FilesPerLevel(0));

  // Compaction will delete files from L0 in first db path and generate a new
  // file in L1 in second db path
  begin = "Key4";
  end = "Key7";
  ASSERT_OK(db_->CompactRange(compact_options, &begin, &end));
  ASSERT_EQ("0,2", FilesPerLevel(0));

  sfm->WaitForEmptyTrash();
  ASSERT_EQ(bg_delete_file, 8);

  compact_options.bottommost_level_compaction =
      BottommostLevelCompaction::kForce;
  ASSERT_OK(db_->CompactRange(compact_options, nullptr, nullptr));
  ASSERT_EQ("0,1", FilesPerLevel(0));

  sfm->WaitForEmptyTrash();
  ASSERT_EQ(bg_delete_file, 8);

  rocksdb::SyncPoint::GetInstance()->DisableProcessing();
}

TEST_F(DBSSTTest, DestroyDBWithRateLimitedDelete) {
  int bg_delete_file = 0;
  rocksdb::SyncPoint::GetInstance()->SetCallBack(
      "DeleteScheduler::DeleteTrashFile:DeleteFile",
      [&](void* arg) { bg_delete_file++; });
  rocksdb::SyncPoint::GetInstance()->EnableProcessing();

  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  options.env = env_;
  DestroyAndReopen(options);

  // Create 4 files in L0
  for (int i = 0; i < 4; i++) {
    ASSERT_OK(Put("Key" + ToString(i), DummyString(1024, 'A')));
    ASSERT_OK(Flush());
  }
  // We created 4 sst files in L0
  ASSERT_EQ("4", FilesPerLevel(0));

  // Close DB and destroy it using DeleteScheduler
  Close();
  std::string trash_dir = test::TmpDir(env_) + "/trash";
  int64_t rate_bytes_per_sec = 1024 * 1024;  // 1 Mb / Sec
  Status s;
  options.sst_file_manager.reset(NewSstFileManager(
      env_, nullptr, trash_dir, rate_bytes_per_sec, false, &s));
  ASSERT_OK(s);
  ASSERT_OK(DestroyDB(dbname_, options));

  auto sfm = static_cast<SstFileManagerImpl*>(options.sst_file_manager.get());
  sfm->WaitForEmptyTrash();
  // We have deleted the 4 sst files in the delete_scheduler
  ASSERT_EQ(bg_delete_file, 4);
}
#endif  // ROCKSDB_LITE

TEST_F(DBSSTTest, DBWithMaxSpaceAllowed) {
  std::shared_ptr<SstFileManager> sst_file_manager(NewSstFileManager(env_));
  auto sfm = static_cast<SstFileManagerImpl*>(sst_file_manager.get());

  Options options = CurrentOptions();
  options.sst_file_manager = sst_file_manager;
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);

  Random rnd(301);

  // Generate a file containing 100 keys.
  for (int i = 0; i < 100; i++) {
    ASSERT_OK(Put(Key(i), RandomString(&rnd, 50)));
  }
  ASSERT_OK(Flush());

  uint64_t first_file_size = 0;
  auto files_in_db = GetAllSSTFiles(&first_file_size);
  ASSERT_EQ(sfm->GetTotalSize(), first_file_size);

  // Set the maximum allowed space usage to the current total size
  sfm->SetMaxAllowedSpaceUsage(first_file_size + 1);

  ASSERT_OK(Put("key1", "val1"));
  // This flush will cause bg_error_ and will fail
  ASSERT_NOK(Flush());
}

TEST_F(DBSSTTest, DBWithMaxSpaceAllowedRandomized) {
  // This test will set a maximum allowed space for the DB, then it will
  // keep filling the DB until the limit is reached and bg_error_ is set.
  // When bg_error_ is set we will verify that the DB size is greater
  // than the limit.

  std::vector<int> max_space_limits_mbs = {1, 2, 4, 8, 10};

  bool bg_error_set = false;
  uint64_t total_sst_files_size = 0;

  int reached_max_space_on_flush = 0;
  int reached_max_space_on_compaction = 0;
  rocksdb::SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::FlushMemTableToOutputFile:MaxAllowedSpaceReached",
      [&](void* arg) {
        bg_error_set = true;
        GetAllSSTFiles(&total_sst_files_size);
        reached_max_space_on_flush++;
      });

  rocksdb::SyncPoint::GetInstance()->SetCallBack(
      "CompactionJob::FinishCompactionOutputFile:MaxAllowedSpaceReached",
      [&](void* arg) {
        bg_error_set = true;
        GetAllSSTFiles(&total_sst_files_size);
        reached_max_space_on_compaction++;
      });

  for (auto limit_mb : max_space_limits_mbs) {
    bg_error_set = false;
    total_sst_files_size = 0;
    rocksdb::SyncPoint::GetInstance()->ClearTrace();
    rocksdb::SyncPoint::GetInstance()->EnableProcessing();
    std::shared_ptr<SstFileManager> sst_file_manager(NewSstFileManager(env_));
    auto sfm = static_cast<SstFileManagerImpl*>(sst_file_manager.get());

    Options options = CurrentOptions();
    options.sst_file_manager = sst_file_manager;
    options.write_buffer_size = 1024 * 512;  // 512 Kb
    DestroyAndReopen(options);
    Random rnd(301);

    sfm->SetMaxAllowedSpaceUsage(limit_mb * 1024 * 1024);

    int keys_written = 0;
    uint64_t estimated_db_size = 0;
    while (true) {
      auto s = Put(RandomString(&rnd, 10), RandomString(&rnd, 50));
      if (!s.ok()) {
        break;
      }
      keys_written++;
      // Check the estimated db size vs the db limit just to make sure we
      // dont run into an infinite loop
      estimated_db_size = keys_written * 60;  // ~60 bytes per key
      ASSERT_LT(estimated_db_size, limit_mb * 1024 * 1024 * 2);
    }
    ASSERT_TRUE(bg_error_set);
    ASSERT_GE(total_sst_files_size, limit_mb * 1024 * 1024);
    rocksdb::SyncPoint::GetInstance()->DisableProcessing();
  }

  ASSERT_GT(reached_max_space_on_flush, 0);
  ASSERT_GT(reached_max_space_on_compaction, 0);
}

#ifndef ROCKSDB_LITE
TEST_F(DBSSTTest, OpenDBWithInfiniteMaxOpenFiles) {
  // Open DB with infinite max open files
  //  - First iteration use 1 thread to open files
  //  - Second iteration use 5 threads to open files
  for (int iter = 0; iter < 2; iter++) {
    Options options;
    options.create_if_missing = true;
    options.write_buffer_size = 100000;
    options.disable_auto_compactions = true;
    options.max_open_files = -1;
    if (iter == 0) {
      options.max_file_opening_threads = 1;
    } else {
      options.max_file_opening_threads = 5;
    }
    options = CurrentOptions(options);
    DestroyAndReopen(options);

    // Create 12 Files in L0 (then move then to L2)
    for (int i = 0; i < 12; i++) {
      std::string k = "L2_" + Key(i);
      ASSERT_OK(Put(k, k + std::string(1000, 'a')));
      ASSERT_OK(Flush());
    }
    CompactRangeOptions compact_options;
    compact_options.change_level = true;
    compact_options.target_level = 2;
    db_->CompactRange(compact_options, nullptr, nullptr);

    // Create 12 Files in L0
    for (int i = 0; i < 12; i++) {
      std::string k = "L0_" + Key(i);
      ASSERT_OK(Put(k, k + std::string(1000, 'a')));
      ASSERT_OK(Flush());
    }
    Close();

    // Reopening the DB will load all exisitng files
    Reopen(options);
    ASSERT_EQ("12,0,12", FilesPerLevel(0));
    std::vector<std::vector<FileMetaData>> files;
    dbfull()->TEST_GetFilesMetaData(db_->DefaultColumnFamily(), &files);

    for (const auto& level : files) {
      for (const auto& file : level) {
        ASSERT_TRUE(file.table_reader_handle != nullptr);
      }
    }

    for (int i = 0; i < 12; i++) {
      ASSERT_EQ(Get("L0_" + Key(i)), "L0_" + Key(i) + std::string(1000, 'a'));
      ASSERT_EQ(Get("L2_" + Key(i)), "L2_" + Key(i) + std::string(1000, 'a'));
    }
  }
}

TEST_F(DBSSTTest, GetTotalSstFilesSize) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  options.compression = kNoCompression;
  DestroyAndReopen(options);
  // Generate 5 files in L0
  for (int i = 0; i < 5; i++) {
    for (int j = 0; j < 10; j++) {
      std::string val = "val_file_" + ToString(i);
      ASSERT_OK(Put(Key(j), val));
    }
    Flush();
  }
  ASSERT_EQ("5", FilesPerLevel(0));

  std::vector<LiveFileMetaData> live_files_meta;
  dbfull()->GetLiveFilesMetaData(&live_files_meta);
  ASSERT_EQ(live_files_meta.size(), 5);
  uint64_t single_file_size = live_files_meta[0].size;

  uint64_t live_sst_files_size = 0;
  uint64_t total_sst_files_size = 0;
  for (const auto& file_meta : live_files_meta) {
    live_sst_files_size += file_meta.size;
  }

  ASSERT_TRUE(dbfull()->GetIntProperty("rocksdb.total-sst-files-size",
                                       &total_sst_files_size));
  // Live SST files = 5
  // Total SST files = 5
  ASSERT_EQ(live_sst_files_size, 5 * single_file_size);
  ASSERT_EQ(total_sst_files_size, 5 * single_file_size);

  // hold current version
  std::unique_ptr<Iterator> iter1(dbfull()->NewIterator(ReadOptions()));

  // Compact 5 files into 1 file in L0
  ASSERT_OK(dbfull()->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  ASSERT_EQ("0,1", FilesPerLevel(0));

  live_files_meta.clear();
  dbfull()->GetLiveFilesMetaData(&live_files_meta);
  ASSERT_EQ(live_files_meta.size(), 1);

  live_sst_files_size = 0;
  total_sst_files_size = 0;
  for (const auto& file_meta : live_files_meta) {
    live_sst_files_size += file_meta.size;
  }
  ASSERT_TRUE(dbfull()->GetIntProperty("rocksdb.total-sst-files-size",
                                       &total_sst_files_size));
  // Live SST files = 1 (compacted file)
  // Total SST files = 6 (5 original files + compacted file)
  ASSERT_EQ(live_sst_files_size, 1 * single_file_size);
  ASSERT_EQ(total_sst_files_size, 6 * single_file_size);

  // hold current version
  std::unique_ptr<Iterator> iter2(dbfull()->NewIterator(ReadOptions()));

  // Delete all keys and compact, this will delete all live files
  for (int i = 0; i < 10; i++) {
    ASSERT_OK(Delete(Key(i)));
  }
  Flush();
  ASSERT_OK(dbfull()->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  ASSERT_EQ("", FilesPerLevel(0));

  live_files_meta.clear();
  dbfull()->GetLiveFilesMetaData(&live_files_meta);
  ASSERT_EQ(live_files_meta.size(), 0);

  ASSERT_TRUE(dbfull()->GetIntProperty("rocksdb.total-sst-files-size",
                                       &total_sst_files_size));
  // Live SST files = 0
  // Total SST files = 6 (5 original files + compacted file)
  ASSERT_EQ(total_sst_files_size, 6 * single_file_size);

  iter1.reset();
  ASSERT_TRUE(dbfull()->GetIntProperty("rocksdb.total-sst-files-size",
                                       &total_sst_files_size));
  // Live SST files = 0
  // Total SST files = 1 (compacted file)
  ASSERT_EQ(total_sst_files_size, 1 * single_file_size);

  iter2.reset();
  ASSERT_TRUE(dbfull()->GetIntProperty("rocksdb.total-sst-files-size",
                                       &total_sst_files_size));
  // Live SST files = 0
  // Total SST files = 0
  ASSERT_EQ(total_sst_files_size, 0);
}

TEST_F(DBSSTTest, GetTotalSstFilesSizeVersionsFilesShared) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  options.compression = kNoCompression;
  DestroyAndReopen(options);
  // Generate 5 files in L0
  for (int i = 0; i < 5; i++) {
    ASSERT_OK(Put(Key(i), "val"));
    Flush();
  }
  ASSERT_EQ("5", FilesPerLevel(0));

  std::vector<LiveFileMetaData> live_files_meta;
  dbfull()->GetLiveFilesMetaData(&live_files_meta);
  ASSERT_EQ(live_files_meta.size(), 5);
  uint64_t single_file_size = live_files_meta[0].size;

  uint64_t live_sst_files_size = 0;
  uint64_t total_sst_files_size = 0;
  for (const auto& file_meta : live_files_meta) {
    live_sst_files_size += file_meta.size;
  }

  ASSERT_TRUE(dbfull()->GetIntProperty("rocksdb.total-sst-files-size",
                                       &total_sst_files_size));

  // Live SST files = 5
  // Total SST files = 5
  ASSERT_EQ(live_sst_files_size, 5 * single_file_size);
  ASSERT_EQ(total_sst_files_size, 5 * single_file_size);

  // hold current version
  std::unique_ptr<Iterator> iter1(dbfull()->NewIterator(ReadOptions()));

  // Compaction will do trivial move from L0 to L1
  ASSERT_OK(dbfull()->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  ASSERT_EQ("0,5", FilesPerLevel(0));

  live_files_meta.clear();
  dbfull()->GetLiveFilesMetaData(&live_files_meta);
  ASSERT_EQ(live_files_meta.size(), 5);

  live_sst_files_size = 0;
  total_sst_files_size = 0;
  for (const auto& file_meta : live_files_meta) {
    live_sst_files_size += file_meta.size;
  }
  ASSERT_TRUE(dbfull()->GetIntProperty("rocksdb.total-sst-files-size",
                                       &total_sst_files_size));
  // Live SST files = 5
  // Total SST files = 5 (used in 2 version)
  ASSERT_EQ(live_sst_files_size, 5 * single_file_size);
  ASSERT_EQ(total_sst_files_size, 5 * single_file_size);

  // hold current version
  std::unique_ptr<Iterator> iter2(dbfull()->NewIterator(ReadOptions()));

  // Delete all keys and compact, this will delete all live files
  for (int i = 0; i < 5; i++) {
    ASSERT_OK(Delete(Key(i)));
  }
  Flush();
  ASSERT_OK(dbfull()->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  ASSERT_EQ("", FilesPerLevel(0));

  live_files_meta.clear();
  dbfull()->GetLiveFilesMetaData(&live_files_meta);
  ASSERT_EQ(live_files_meta.size(), 0);

  ASSERT_TRUE(dbfull()->GetIntProperty("rocksdb.total-sst-files-size",
                                       &total_sst_files_size));
  // Live SST files = 0
  // Total SST files = 5 (used in 2 version)
  ASSERT_EQ(total_sst_files_size, 5 * single_file_size);

  iter1.reset();
  iter2.reset();

  ASSERT_TRUE(dbfull()->GetIntProperty("rocksdb.total-sst-files-size",
                                       &total_sst_files_size));
  // Live SST files = 0
  // Total SST files = 0
  ASSERT_EQ(total_sst_files_size, 0);
}

TEST_F(DBSSTTest, AddExternalSstFile) {
  do {
    std::string sst_files_folder = test::TmpDir(env_) + "/sst_files/";
    env_->CreateDir(sst_files_folder);
    Options options = CurrentOptions();
    options.env = env_;

    SstFileWriter sst_file_writer(EnvOptions(), options, options.comparator);

    // file1.sst (0 => 99)
    std::string file1 = sst_files_folder + "file1.sst";
    ASSERT_OK(sst_file_writer.Open(file1));
    for (int k = 0; k < 100; k++) {
      ASSERT_OK(sst_file_writer.Add(Key(k), Key(k) + "_val"));
    }
    ExternalSstFileInfo file1_info;
    Status s = sst_file_writer.Finish(&file1_info);
    ASSERT_TRUE(s.ok()) << s.ToString();
    ASSERT_EQ(file1_info.file_path, file1);
    ASSERT_EQ(file1_info.num_entries, 100);
    ASSERT_EQ(file1_info.smallest_key, Key(0));
    ASSERT_EQ(file1_info.largest_key, Key(99));
    // sst_file_writer already finished, cannot add this value
    s = sst_file_writer.Add(Key(100), "bad_val");
    ASSERT_FALSE(s.ok()) << s.ToString();

    // file2.sst (100 => 199)
    std::string file2 = sst_files_folder + "file2.sst";
    ASSERT_OK(sst_file_writer.Open(file2));
    for (int k = 100; k < 200; k++) {
      ASSERT_OK(sst_file_writer.Add(Key(k), Key(k) + "_val"));
    }
    // Cannot add this key because it's not after last added key
    s = sst_file_writer.Add(Key(99), "bad_val");
    ASSERT_FALSE(s.ok()) << s.ToString();
    ExternalSstFileInfo file2_info;
    s = sst_file_writer.Finish(&file2_info);
    ASSERT_TRUE(s.ok()) << s.ToString();
    ASSERT_EQ(file2_info.file_path, file2);
    ASSERT_EQ(file2_info.num_entries, 100);
    ASSERT_EQ(file2_info.smallest_key, Key(100));
    ASSERT_EQ(file2_info.largest_key, Key(199));

    // file3.sst (195 => 299)
    // This file values overlap with file2 values
    std::string file3 = sst_files_folder + "file3.sst";
    ASSERT_OK(sst_file_writer.Open(file3));
    for (int k = 195; k < 300; k++) {
      ASSERT_OK(sst_file_writer.Add(Key(k), Key(k) + "_val_overlap"));
    }
    ExternalSstFileInfo file3_info;
    s = sst_file_writer.Finish(&file3_info);
    ASSERT_TRUE(s.ok()) << s.ToString();
    ASSERT_EQ(file3_info.file_path, file3);
    ASSERT_EQ(file3_info.num_entries, 105);
    ASSERT_EQ(file3_info.smallest_key, Key(195));
    ASSERT_EQ(file3_info.largest_key, Key(299));

    // file4.sst (30 => 39)
    // This file values overlap with file1 values
    std::string file4 = sst_files_folder + "file4.sst";
    ASSERT_OK(sst_file_writer.Open(file4));
    for (int k = 30; k < 40; k++) {
      ASSERT_OK(sst_file_writer.Add(Key(k), Key(k) + "_val_overlap"));
    }
    ExternalSstFileInfo file4_info;
    s = sst_file_writer.Finish(&file4_info);
    ASSERT_TRUE(s.ok()) << s.ToString();
    ASSERT_EQ(file4_info.file_path, file4);
    ASSERT_EQ(file4_info.num_entries, 10);
    ASSERT_EQ(file4_info.smallest_key, Key(30));
    ASSERT_EQ(file4_info.largest_key, Key(39));

    // file5.sst (400 => 499)
    std::string file5 = sst_files_folder + "file5.sst";
    ASSERT_OK(sst_file_writer.Open(file5));
    for (int k = 400; k < 500; k++) {
      ASSERT_OK(sst_file_writer.Add(Key(k), Key(k) + "_val"));
    }
    ExternalSstFileInfo file5_info;
    s = sst_file_writer.Finish(&file5_info);
    ASSERT_TRUE(s.ok()) << s.ToString();
    ASSERT_EQ(file5_info.file_path, file5);
    ASSERT_EQ(file5_info.num_entries, 100);
    ASSERT_EQ(file5_info.smallest_key, Key(400));
    ASSERT_EQ(file5_info.largest_key, Key(499));

    // Cannot create an empty sst file
    std::string file_empty = sst_files_folder + "file_empty.sst";
    ExternalSstFileInfo file_empty_info;
    s = sst_file_writer.Finish(&file_empty_info);
    ASSERT_NOK(s);

    DestroyAndReopen(options);
    // Add file using file path
    s = db_->AddFile(file1);
    ASSERT_TRUE(s.ok()) << s.ToString();
    ASSERT_EQ(db_->GetLatestSequenceNumber(), 0U);
    for (int k = 0; k < 100; k++) {
      ASSERT_EQ(Get(Key(k)), Key(k) + "_val");
    }

    // Add file while holding a snapshot will fail
    const Snapshot* s1 = db_->GetSnapshot();
    if (s1 != nullptr) {
      ASSERT_NOK(db_->AddFile(&file2_info));
      db_->ReleaseSnapshot(s1);
    }
    // We can add the file after releaseing the snapshot
    ASSERT_OK(db_->AddFile(&file2_info));

    ASSERT_EQ(db_->GetLatestSequenceNumber(), 0U);
    for (int k = 0; k < 200; k++) {
      ASSERT_EQ(Get(Key(k)), Key(k) + "_val");
    }

    // This file have overlapping values with the exisitng data
    s = db_->AddFile(file3);
    ASSERT_FALSE(s.ok()) << s.ToString();

    // This file have overlapping values with the exisitng data
    s = db_->AddFile(&file4_info);
    ASSERT_FALSE(s.ok()) << s.ToString();

    // Overwrite values of keys divisible by 5
    for (int k = 0; k < 200; k += 5) {
      ASSERT_OK(Put(Key(k), Key(k) + "_val_new"));
    }
    ASSERT_NE(db_->GetLatestSequenceNumber(), 0U);

    // Key range of file5 (400 => 499) dont overlap with any keys in DB
    ASSERT_OK(db_->AddFile(file5));

    // Make sure values are correct before and after flush/compaction
    for (int i = 0; i < 2; i++) {
      for (int k = 0; k < 200; k++) {
        std::string value = Key(k) + "_val";
        if (k % 5 == 0) {
          value += "_new";
        }
        ASSERT_EQ(Get(Key(k)), value);
      }
      for (int k = 400; k < 500; k++) {
        std::string value = Key(k) + "_val";
        ASSERT_EQ(Get(Key(k)), value);
      }
      ASSERT_OK(Flush());
      ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
    }

    Close();
    options.disable_auto_compactions = true;
    Reopen(options);

    // Delete keys in range (400 => 499)
    for (int k = 400; k < 500; k++) {
      ASSERT_OK(Delete(Key(k)));
    }
    // We deleted range (400 => 499) but cannot add file5 because
    // of the range tombstones
    ASSERT_NOK(db_->AddFile(file5));

    // Compacting the DB will remove the tombstones
    ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));

    // Now we can add the file
    ASSERT_OK(db_->AddFile(file5));

    // Verify values of file5 in DB
    for (int k = 400; k < 500; k++) {
      std::string value = Key(k) + "_val";
      ASSERT_EQ(Get(Key(k)), value);
    }
  } while (ChangeOptions(kSkipPlainTable | kSkipUniversalCompaction |
                         kSkipFIFOCompaction));
}

// This test reporduce a bug that can happen in some cases if the DB started
// purging obsolete files when we are adding an external sst file.
// This situation may result in deleting the file while it's being added.
TEST_F(DBSSTTest, AddExternalSstFilePurgeObsoleteFilesBug) {
  std::string sst_files_folder = test::TmpDir(env_) + "/sst_files/";
  env_->CreateDir(sst_files_folder);
  Options options = CurrentOptions();
  options.env = env_;
  SstFileWriter sst_file_writer(EnvOptions(), options, options.comparator);

  // file1.sst (0 => 500)
  std::string sst_file_path = sst_files_folder + "file1.sst";
  Status s = sst_file_writer.Open(sst_file_path);
  ASSERT_OK(s);
  for (int i = 0; i < 500; i++) {
    std::string k = Key(i);
    s = sst_file_writer.Add(k, k + "_val");
    ASSERT_OK(s);
  }

  ExternalSstFileInfo sst_file_info;
  s = sst_file_writer.Finish(&sst_file_info);
  ASSERT_OK(s);

  options.delete_obsolete_files_period_micros = 0;
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);

  rocksdb::SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::AddFile:FileCopied", [&](void* arg) {
        ASSERT_OK(Put("aaa", "bbb"));
        ASSERT_OK(Flush());
        ASSERT_OK(Put("aaa", "xxx"));
        ASSERT_OK(Flush());
        db_->CompactRange(CompactRangeOptions(), nullptr, nullptr);
      });
  rocksdb::SyncPoint::GetInstance()->EnableProcessing();

  s = db_->AddFile(sst_file_path);
  ASSERT_OK(s);

  for (int i = 0; i < 500; i++) {
    std::string k = Key(i);
    std::string v = k + "_val";
    ASSERT_EQ(Get(k), v);
  }

  rocksdb::SyncPoint::GetInstance()->DisableProcessing();
}

TEST_F(DBSSTTest, AddExternalSstFileNoCopy) {
  std::string sst_files_folder = test::TmpDir(env_) + "/sst_files/";
  env_->CreateDir(sst_files_folder);
  Options options = CurrentOptions();
  options.env = env_;
  const ImmutableCFOptions ioptions(options);

  SstFileWriter sst_file_writer(EnvOptions(), options, options.comparator);

  // file1.sst (0 => 99)
  std::string file1 = sst_files_folder + "file1.sst";
  ASSERT_OK(sst_file_writer.Open(file1));
  for (int k = 0; k < 100; k++) {
    ASSERT_OK(sst_file_writer.Add(Key(k), Key(k) + "_val"));
  }
  ExternalSstFileInfo file1_info;
  Status s = sst_file_writer.Finish(&file1_info);
  ASSERT_TRUE(s.ok()) << s.ToString();
  ASSERT_EQ(file1_info.file_path, file1);
  ASSERT_EQ(file1_info.num_entries, 100);
  ASSERT_EQ(file1_info.smallest_key, Key(0));
  ASSERT_EQ(file1_info.largest_key, Key(99));

  // file2.sst (100 => 299)
  std::string file2 = sst_files_folder + "file2.sst";
  ASSERT_OK(sst_file_writer.Open(file2));
  for (int k = 100; k < 300; k++) {
    ASSERT_OK(sst_file_writer.Add(Key(k), Key(k) + "_val"));
  }
  ExternalSstFileInfo file2_info;
  s = sst_file_writer.Finish(&file2_info);
  ASSERT_TRUE(s.ok()) << s.ToString();
  ASSERT_EQ(file2_info.file_path, file2);
  ASSERT_EQ(file2_info.num_entries, 200);
  ASSERT_EQ(file2_info.smallest_key, Key(100));
  ASSERT_EQ(file2_info.largest_key, Key(299));

  // file3.sst (110 => 124) .. overlap with file2.sst
  std::string file3 = sst_files_folder + "file3.sst";
  ASSERT_OK(sst_file_writer.Open(file3));
  for (int k = 110; k < 125; k++) {
    ASSERT_OK(sst_file_writer.Add(Key(k), Key(k) + "_val_overlap"));
  }
  ExternalSstFileInfo file3_info;
  s = sst_file_writer.Finish(&file3_info);
  ASSERT_TRUE(s.ok()) << s.ToString();
  ASSERT_EQ(file3_info.file_path, file3);
  ASSERT_EQ(file3_info.num_entries, 15);
  ASSERT_EQ(file3_info.smallest_key, Key(110));
  ASSERT_EQ(file3_info.largest_key, Key(124));

  s = db_->AddFile(&file1_info, true /* move file */);
  ASSERT_TRUE(s.ok()) << s.ToString();
  ASSERT_EQ(Status::NotFound(), env_->FileExists(file1));

  s = db_->AddFile(&file2_info, false /* copy file */);
  ASSERT_TRUE(s.ok()) << s.ToString();
  ASSERT_OK(env_->FileExists(file2));

  // This file have overlapping values with the exisitng data
  s = db_->AddFile(&file3_info, true /* move file */);
  ASSERT_FALSE(s.ok()) << s.ToString();
  ASSERT_OK(env_->FileExists(file3));

  for (int k = 0; k < 300; k++) {
    ASSERT_EQ(Get(Key(k)), Key(k) + "_val");
  }
}

TEST_F(DBSSTTest, AddExternalSstFileMultiThreaded) {
  std::string sst_files_folder = test::TmpDir(env_) + "/sst_files/";
  // Bulk load 10 files every file contain 1000 keys
  int num_files = 10;
  int keys_per_file = 1000;

  // Generate file names
  std::vector<std::string> file_names;
  for (int i = 0; i < num_files; i++) {
    std::string file_name = "file_" + ToString(i) + ".sst";
    file_names.push_back(sst_files_folder + file_name);
  }

  do {
    env_->CreateDir(sst_files_folder);
    Options options = CurrentOptions();

    std::atomic<int> thread_num(0);
    std::function<void()> write_file_func = [&]() {
      int file_idx = thread_num.fetch_add(1);
      int range_start = file_idx * keys_per_file;
      int range_end = range_start + keys_per_file;

      SstFileWriter sst_file_writer(EnvOptions(), options, options.comparator);

      ASSERT_OK(sst_file_writer.Open(file_names[file_idx]));

      for (int k = range_start; k < range_end; k++) {
        ASSERT_OK(sst_file_writer.Add(Key(k), Key(k)));
      }

      Status s = sst_file_writer.Finish();
      ASSERT_TRUE(s.ok()) << s.ToString();
    };
    // Write num_files files in parallel
    std::vector<std::thread> sst_writer_threads;
    for (int i = 0; i < num_files; ++i) {
      sst_writer_threads.emplace_back(write_file_func);
    }

    for (auto& t : sst_writer_threads) {
      t.join();
    }

    fprintf(stderr, "Wrote %d files (%d keys)\n", num_files,
            num_files * keys_per_file);

    thread_num.store(0);
    std::atomic<int> files_added(0);
    std::function<void()> load_file_func = [&]() {
      // We intentionally add every file twice, and assert that it was added
      // only once and the other add failed
      int thread_id = thread_num.fetch_add(1);
      int file_idx = thread_id / 2;
      // sometimes we use copy, sometimes link .. the result should be the same
      bool move_file = (thread_id % 3 == 0);

      Status s = db_->AddFile(file_names[file_idx], move_file);
      if (s.ok()) {
        files_added++;
      }
    };
    // Bulk load num_files files in parallel
    std::vector<std::thread> add_file_threads;
    DestroyAndReopen(options);
    for (int i = 0; i < num_files * 2; ++i) {
      add_file_threads.emplace_back(load_file_func);
    }

    for (auto& t : add_file_threads) {
      t.join();
    }
    ASSERT_EQ(files_added.load(), num_files);
    fprintf(stderr, "Loaded %d files (%d keys)\n", num_files,
            num_files * keys_per_file);

    // Overwrite values of keys divisible by 100
    for (int k = 0; k < num_files * keys_per_file; k += 100) {
      std::string key = Key(k);
      Status s = Put(key, key + "_new");
      ASSERT_TRUE(s.ok());
    }

    for (int i = 0; i < 2; i++) {
      // Make sure the values are correct before and after flush/compaction
      for (int k = 0; k < num_files * keys_per_file; ++k) {
        std::string key = Key(k);
        std::string value = (k % 100 == 0) ? (key + "_new") : key;
        ASSERT_EQ(Get(key), value);
      }
      ASSERT_OK(Flush());
      ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
    }

    fprintf(stderr, "Verified %d values\n", num_files * keys_per_file);
  } while (ChangeOptions(kSkipPlainTable | kSkipUniversalCompaction |
                         kSkipFIFOCompaction));
}

TEST_F(DBSSTTest, AddExternalSstFileOverlappingRanges) {
  std::string sst_files_folder = test::TmpDir(env_) + "/sst_files/";
  Random rnd(301);
  do {
    env_->CreateDir(sst_files_folder);
    Options options = CurrentOptions();
    DestroyAndReopen(options);

    SstFileWriter sst_file_writer(EnvOptions(), options, options.comparator);

    printf("Option config = %d\n", option_config_);
    std::vector<std::pair<int, int>> key_ranges;
    for (int i = 0; i < 500; i++) {
      int range_start = rnd.Uniform(20000);
      int keys_per_range = 10 + rnd.Uniform(41);

      key_ranges.emplace_back(range_start, range_start + keys_per_range);
    }

    int memtable_add = 0;
    int success_add_file = 0;
    int failed_add_file = 0;
    std::map<std::string, std::string> true_data;
    for (size_t i = 0; i < key_ranges.size(); i++) {
      int range_start = key_ranges[i].first;
      int range_end = key_ranges[i].second;

      Status s;
      std::string range_val = "range_" + ToString(i);

      // For 20% of ranges we use DB::Put, for 80% we use DB::AddFile
      if (i && i % 5 == 0) {
        // Use DB::Put to insert range (insert into memtable)
        range_val += "_put";
        for (int k = range_start; k <= range_end; k++) {
          s = Put(Key(k), range_val);
          ASSERT_OK(s);
        }
        memtable_add++;
      } else {
        // Use DB::AddFile to insert range
        range_val += "_add_file";

        // Generate the file containing the range
        std::string file_name = sst_files_folder + env_->GenerateUniqueId();
        ASSERT_OK(sst_file_writer.Open(file_name));
        for (int k = range_start; k <= range_end; k++) {
          s = sst_file_writer.Add(Key(k), range_val);
          ASSERT_OK(s);
        }
        ExternalSstFileInfo file_info;
        s = sst_file_writer.Finish(&file_info);
        ASSERT_OK(s);

        // Insert the generated file
        s = db_->AddFile(&file_info);

        auto it = true_data.lower_bound(Key(range_start));
        if (it != true_data.end() && it->first <= Key(range_end)) {
          // This range overlap with data already exist in DB
          ASSERT_NOK(s);
          failed_add_file++;
        } else {
          ASSERT_OK(s);
          success_add_file++;
        }
      }

      if (s.ok()) {
        // Update true_data map to include the new inserted data
        for (int k = range_start; k <= range_end; k++) {
          true_data[Key(k)] = range_val;
        }
      }

      // Flush / Compact the DB
      if (i && i % 50 == 0) {
        Flush();
      }
      if (i && i % 75 == 0) {
        db_->CompactRange(CompactRangeOptions(), nullptr, nullptr);
      }
    }

    printf(
        "Total: %zu ranges\n"
        "AddFile()|Success: %d ranges\n"
        "AddFile()|RangeConflict: %d ranges\n"
        "Put(): %d ranges\n",
        key_ranges.size(), success_add_file, failed_add_file, memtable_add);

    // Verify the correctness of the data
    for (const auto& kv : true_data) {
      ASSERT_EQ(Get(kv.first), kv.second);
    }
    printf("keys/values verified\n");
  } while (ChangeOptions(kSkipPlainTable | kSkipUniversalCompaction |
                         kSkipFIFOCompaction));
}

#endif  // ROCKSDB_LITE

// 1 Create some SST files by inserting K-V pairs into DB
// 2 Close DB and change suffix from ".sst" to ".ldb" for every other SST file
// 3 Open DB and check if all key can be read
TEST_F(DBSSTTest, SSTsWithLdbSuffixHandling) {
  Options options = CurrentOptions();
  options.write_buffer_size = 110 << 10;  // 110KB
  options.num_levels = 4;
  DestroyAndReopen(options);

  Random rnd(301);
  int key_id = 0;
  for (int i = 0; i < 10; ++i) {
    GenerateNewFile(&rnd, &key_id, false);
  }
  Flush();
  Close();
  int const num_files = GetSstFileCount(dbname_);
  ASSERT_GT(num_files, 0);

  std::vector<std::string> filenames;
  GetSstFiles(dbname_, &filenames);
  int num_ldb_files = 0;
  for (size_t i = 0; i < filenames.size(); ++i) {
    if (i & 1) {
      continue;
    }
    std::string const rdb_name = dbname_ + "/" + filenames[i];
    std::string const ldb_name = Rocks2LevelTableFileName(rdb_name);
    ASSERT_TRUE(env_->RenameFile(rdb_name, ldb_name).ok());
    ++num_ldb_files;
  }
  ASSERT_GT(num_ldb_files, 0);
  ASSERT_EQ(num_files, GetSstFileCount(dbname_));

  Reopen(options);
  for (int k = 0; k < key_id; ++k) {
    ASSERT_NE("NOT_FOUND", Get(Key(k)));
  }
  Destroy(options);
}

}  // namespace rocksdb

int main(int argc, char** argv) {
  rocksdb::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

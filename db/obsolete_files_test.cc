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

using std::cerr;
using std::cout;
using std::endl;
using std::flush;

namespace rocksdb {

class ObsoleteFilesTest : public DBTestBase {
 public:
  ObsoleteFilesTest()
      : DBTestBase("/obsolete_files_test"), wal_dir_(dbname_ + "/wal_files") {}

  void AddKeys(int numkeys, int startkey) {
    WriteOptions options;
    options.sync = false;
    for (int i = startkey; i < (numkeys + startkey) ; i++) {
      std::string temp = ToString(i);
      Slice key(temp);
      Slice value(temp);
      ASSERT_OK(db_->Put(options, key, value));
    }
  }

  void createLevel0Files(int numFiles, int numKeysPerFile) {
    int startKey = 0;
    for (int i = 0; i < numFiles; i++) {
      AddKeys(numKeysPerFile, startKey);
      startKey += numKeysPerFile;
      ASSERT_OK(dbfull()->TEST_FlushMemTable());
      ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());
    }
  }

  void CheckFileTypeCounts(const std::string& dir, int required_log,
                           int required_sst, int required_manifest) {
    std::vector<std::string> filenames;
    env_->GetChildren(dir, &filenames);

    int log_cnt = 0;
    int sst_cnt = 0;
    int manifest_cnt = 0;
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

  void ReopenDB() {
    Options options = CurrentOptions();
    // Trigger compaction when the number of level 0 files reaches 2.
    options.create_if_missing = true;
    options.level0_file_num_compaction_trigger = 2;
    options.disable_auto_compactions = false;
    options.delete_obsolete_files_period_micros = 0;  // always do full purge
    options.enable_thread_tracking = true;
    options.write_buffer_size = 1024 * 1024 * 1000;
    options.target_file_size_base = 1024 * 1024 * 1000;
    options.max_bytes_for_level_base = 1024 * 1024 * 1000;
    options.WAL_ttl_seconds = 300;     // Used to test log files
    options.WAL_size_limit_MB = 1024;  // Used to test log files
    options.wal_dir = wal_dir_;
    Destroy(options);
    Reopen(options);
  }

  const std::string wal_dir_;
};

TEST_F(ObsoleteFilesTest, RaceForObsoleteFileDeletion) {
  ReopenDB();
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->LoadDependency({
      {"DBImpl::BackgroundCallCompaction:FoundObsoleteFiles",
       "ObsoleteFilesTest::RaceForObsoleteFileDeletion:1"},
      {"DBImpl::BackgroundCallCompaction:PurgedObsoleteFiles",
       "ObsoleteFilesTest::RaceForObsoleteFileDeletion:2"},
      });
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::DeleteObsoleteFileImpl:AfterDeletion", [&](void* arg) {
        Status* p_status = reinterpret_cast<Status*>(arg);
        ASSERT_OK(*p_status);
      });
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::CloseHelper:PendingPurgeFinished", [&](void* arg) {
        std::unordered_set<uint64_t>* files_grabbed_for_purge_ptr =
            reinterpret_cast<std::unordered_set<uint64_t>*>(arg);
        ASSERT_TRUE(files_grabbed_for_purge_ptr->empty());
      });
  SyncPoint::GetInstance()->EnableProcessing();

  createLevel0Files(2, 50000);
  CheckFileTypeCounts(wal_dir_, 1, 0, 0);

  port::Thread user_thread([this]() {
    JobContext jobCxt(0);
    TEST_SYNC_POINT("ObsoleteFilesTest::RaceForObsoleteFileDeletion:1");
    dbfull()->TEST_LockMutex();
    dbfull()->FindObsoleteFiles(&jobCxt, true /* force=true */,
                                false /* no_full_scan=false */);
    dbfull()->TEST_UnlockMutex();
    TEST_SYNC_POINT("ObsoleteFilesTest::RaceForObsoleteFileDeletion:2");
    dbfull()->PurgeObsoleteFiles(jobCxt);
    jobCxt.Clean();
  });

  user_thread.join();
}

TEST_F(ObsoleteFilesTest, DeleteObsoleteOptionsFile) {
  ReopenDB();
  SyncPoint::GetInstance()->DisableProcessing();
  std::vector<uint64_t> optsfiles_nums;
  std::vector<bool> optsfiles_keep;
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::PurgeObsoleteFiles:CheckOptionsFiles:1", [&](void* arg) {
        optsfiles_nums.push_back(*reinterpret_cast<uint64_t*>(arg));
      });
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::PurgeObsoleteFiles:CheckOptionsFiles:2", [&](void* arg) {
        optsfiles_keep.push_back(*reinterpret_cast<bool*>(arg));
      });
  SyncPoint::GetInstance()->EnableProcessing();

  createLevel0Files(2, 50000);
  CheckFileTypeCounts(wal_dir_, 1, 0, 0);

  ASSERT_OK(dbfull()->DisableFileDeletions());
  for (int i = 0; i != 4; ++i) {
    if (i % 2) {
      ASSERT_OK(dbfull()->SetOptions(dbfull()->DefaultColumnFamily(),
                                     {{"paranoid_file_checks", "false"}}));
    } else {
      ASSERT_OK(dbfull()->SetOptions(dbfull()->DefaultColumnFamily(),
                                     {{"paranoid_file_checks", "true"}}));
    }
  }
  ASSERT_OK(dbfull()->EnableFileDeletions(true /* force */));
  ASSERT_EQ(optsfiles_nums.size(), optsfiles_keep.size());

  Close();

  std::vector<std::string> files;
  int opts_file_count = 0;
  ASSERT_OK(env_->GetChildren(dbname_, &files));
  for (const auto& file : files) {
    uint64_t file_num;
    Slice dummy_info_log_name_prefix;
    FileType type;
    WalFileType log_type;
    if (ParseFileName(file, &file_num, dummy_info_log_name_prefix, &type,
                      &log_type) &&
        type == kOptionsFile) {
      opts_file_count++;
    }
  }
  ASSERT_EQ(2, opts_file_count);
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

//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "rocksdb/db.h"
#include "db/db_impl.h"
#include "db/filename.h"
#include "db/version_set.h"
#include "db/write_batch_internal.h"
#include "util/testharness.h"
#include "util/testutil.h"
#include "rocksdb/env.h"
#include "rocksdb/transaction_log.h"
#include <vector>
#include <stdlib.h>
#include <map>
#include <string>

namespace rocksdb {

class DeleteFileTest {
 public:
  std::string dbname_;
  Options options_;
  DB* db_;
  Env* env_;
  int numlevels_;

  DeleteFileTest() {
    db_ = nullptr;
    env_ = Env::Default();
    options_.write_buffer_size = 1024*1024*1000;
    options_.target_file_size_base = 1024*1024*1000;
    options_.max_bytes_for_level_base = 1024*1024*1000;
    options_.WAL_ttl_seconds = 300; // Used to test log files
    options_.WAL_size_limit_MB = 1024; // Used to test log files
    dbname_ = test::TmpDir() + "/deletefile_test";
    options_.wal_dir = dbname_ + "/wal_files";
    DestroyDB(dbname_, options_);
    numlevels_ = 7;
    ASSERT_OK(ReopenDB(true));
  }

  Status ReopenDB(bool create) {
    delete db_;
    if (create) {
      DestroyDB(dbname_, options_);
    }
    db_ = nullptr;
    options_.create_if_missing = create;
    return DB::Open(options_, dbname_, &db_);
  }

  void CloseDB() {
    delete db_;
  }

  void AddKeys(int numkeys, int startkey = 0) {
    WriteOptions options;
    options.sync = false;
    ReadOptions roptions;
    for (int i = startkey; i < (numkeys + startkey) ; i++) {
      std::string temp = std::to_string(i);
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
    DBImpl* dbi = reinterpret_cast<DBImpl*>(db_);
    ASSERT_OK(dbi->TEST_FlushMemTable());
    ASSERT_OK(dbi->TEST_WaitForFlushMemTable());

    AddKeys(50000, 10000);
    ASSERT_OK(dbi->TEST_FlushMemTable());
    ASSERT_OK(dbi->TEST_WaitForFlushMemTable());
  }

  void CheckFileTypeCounts(std::string& dir,
                            int required_log,
                            int required_sst,
                            int required_manifest) {
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

};

TEST(DeleteFileTest, AddKeysAndQueryLevels) {
  CreateTwoLevels();
  std::vector<LiveFileMetaData> metadata;
  std::vector<int> keysinlevel;
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

  CloseDB();
}

TEST(DeleteFileTest, PurgeObsoleteFilesTest) {
  CreateTwoLevels();
  // there should be only one (empty) log file because CreateTwoLevels()
  // flushes the memtables to disk
  CheckFileTypeCounts(options_.wal_dir, 1, 0, 0);
  // 2 ssts, 1 manifest
  CheckFileTypeCounts(dbname_, 0, 2, 1);
  std::string first("0"), last("999999");
  Slice first_slice(first), last_slice(last);
  db_->CompactRange(&first_slice, &last_slice, true, 2);
  // 1 sst after compaction
  CheckFileTypeCounts(dbname_, 0, 1, 1);

  // this time, we keep an iterator alive
  ReopenDB(true);
  Iterator *itr = 0;
  CreateTwoLevels();
  itr = db_->NewIterator(ReadOptions());
  db_->CompactRange(&first_slice, &last_slice, true, 2);
  // 3 sst after compaction with live iterator
  CheckFileTypeCounts(dbname_, 0, 3, 1);
  delete itr;
  // 1 sst after iterator deletion
  CheckFileTypeCounts(dbname_, 0, 1, 1);

  CloseDB();
}

TEST(DeleteFileTest, DeleteFileWithIterator) {
  CreateTwoLevels();
  ReadOptions options;
  Iterator* it = db_->NewIterator(options);
  std::vector<LiveFileMetaData> metadata;
  db_->GetLiveFilesMetaData(&metadata);

  std::string level2file = "";

  ASSERT_EQ((int)metadata.size(), 2);
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
  CloseDB();
}

TEST(DeleteFileTest, DeleteLogFiles) {
  AddKeys(10, 0);
  VectorLogPtr logfiles;
  db_->GetSortedWalFiles(logfiles);
  ASSERT_GT(logfiles.size(), 0UL);
  // Take the last log file which is expected to be alive and try to delete it
  // Should not succeed because live logs are not allowed to be deleted
  std::unique_ptr<LogFile> alive_log = std::move(logfiles.back());
  ASSERT_EQ(alive_log->Type(), kAliveLogFile);
  ASSERT_TRUE(env_->FileExists(options_.wal_dir + "/" + alive_log->PathName()));
  fprintf(stdout, "Deleting alive log file %s\n",
          alive_log->PathName().c_str());
  ASSERT_TRUE(!db_->DeleteFile(alive_log->PathName()).ok());
  ASSERT_TRUE(env_->FileExists(options_.wal_dir + "/" + alive_log->PathName()));
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
  ASSERT_TRUE(env_->FileExists(options_.wal_dir + "/" +
        archived_log->PathName()));
  fprintf(stdout, "Deleting archived log file %s\n",
          archived_log->PathName().c_str());
  ASSERT_OK(db_->DeleteFile(archived_log->PathName()));
  ASSERT_TRUE(!env_->FileExists(options_.wal_dir + "/" +
        archived_log->PathName()));
  CloseDB();
}

} //namespace rocksdb

int main(int argc, char** argv) {
  return rocksdb::test::RunAllTests();
}


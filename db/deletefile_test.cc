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
#include <vector>
#include <stdlib.h>
#include <map>
#include <string>

namespace leveldb {

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
    dbname_ = test::TmpDir() + "/deletefile_test";
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
    ASSERT_OK(dbi->TEST_CompactMemTable());
    ASSERT_OK(dbi->TEST_WaitForCompactMemTable());

    AddKeys(50000, 10000);
    ASSERT_OK(dbi->TEST_CompactMemTable());
    ASSERT_OK(dbi->TEST_WaitForCompactMemTable());
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
} //namespace leveldb

int main(int argc, char** argv) {
  return leveldb::test::RunAllTests();
}


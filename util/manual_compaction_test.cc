//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Test for issue 178: a manual compaction causes deleted data to reappear.
#include <iostream>
#include <sstream>
#include <cstdlib>

#include "rocksdb/db.h"
#include "rocksdb/write_batch.h"
#include "util/testharness.h"

namespace {

const int kNumKeys = 1100000;

std::string Key1(int i) {
  char buf[100];
  snprintf(buf, sizeof(buf), "my_key_%d", i);
  return buf;
}

std::string Key2(int i) {
  return Key1(i) + "_xxx";
}

class ManualCompactionTest { };

TEST(ManualCompactionTest, Test) {
  // Get rid of any state from an old run.
  std::string dbpath = rocksdb::test::TmpDir() + "/rocksdb_cbug_test";
  DestroyDB(dbpath, rocksdb::Options());

  // Open database.  Disable compression since it affects the creation
  // of layers and the code below is trying to test against a very
  // specific scenario.
  rocksdb::DB* db;
  rocksdb::Options db_options;
  db_options.create_if_missing = true;
  db_options.compression = rocksdb::kNoCompression;
  ASSERT_OK(rocksdb::DB::Open(db_options, dbpath, &db));

  // create first key range
  rocksdb::WriteBatch batch;
  for (int i = 0; i < kNumKeys; i++) {
    batch.Put(Key1(i), "value for range 1 key");
  }
  ASSERT_OK(db->Write(rocksdb::WriteOptions(), &batch));

  // create second key range
  batch.Clear();
  for (int i = 0; i < kNumKeys; i++) {
    batch.Put(Key2(i), "value for range 2 key");
  }
  ASSERT_OK(db->Write(rocksdb::WriteOptions(), &batch));

  // delete second key range
  batch.Clear();
  for (int i = 0; i < kNumKeys; i++) {
    batch.Delete(Key2(i));
  }
  ASSERT_OK(db->Write(rocksdb::WriteOptions(), &batch));

  // compact database
  std::string start_key = Key1(0);
  std::string end_key = Key1(kNumKeys - 1);
  rocksdb::Slice least(start_key.data(), start_key.size());
  rocksdb::Slice greatest(end_key.data(), end_key.size());

  // commenting out the line below causes the example to work correctly
  db->CompactRange(&least, &greatest);

  // count the keys
  rocksdb::Iterator* iter = db->NewIterator(rocksdb::ReadOptions());
  int num_keys = 0;
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    num_keys++;
  }
  delete iter;
  ASSERT_EQ(kNumKeys, num_keys) << "Bad number of keys";

  // close database
  delete db;
  DestroyDB(dbpath, rocksdb::Options());
}

}  // anonymous namespace

int main(int argc, char** argv) {
  return rocksdb::test::RunAllTests();
}

// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef ROCKSDB_LITE

#include "utilities/blob_db/blob_db.h"
#include "util/random.h"
#include "util/testharness.h"
#include "util/testutil.h"

namespace rocksdb {
class BlobDBTest : public testing::Test {
 public:
  BlobDBTest() : db_(nullptr) {
    dbname_ = test::TmpDir() + "/blob_db_test";
    Options options;
    options.create_if_missing = true;
    BlobDBOptions bdb_options;
    bdb_options.blob_dir = "blob_dir";
    EXPECT_TRUE(BlobDB::Open(options, bdb_options, dbname_, &db_).ok());
  }

  ~BlobDBTest() {
   if (db_) 
     delete db_;
  }

  BlobDB* db_;
  std::string dbname_;
};  // class BlobDBTest

TEST_F(BlobDBTest, Basic) {
  ASSERT_TRUE(db_ != nullptr);

  WriteOptions wo;
  ReadOptions ro;
  std::string value;

  ColumnFamilyHandle* dcfh = db_->DefaultColumnFamily();

  ASSERT_OK(db_->PutWithTTL(wo, dcfh, "foo", "v1", 3600));
  ASSERT_OK(db_->PutWithTTL(wo, dcfh, "bar", "v2", 60));

  ASSERT_OK(db_->Get(ro, dcfh, "foo", &value));
  ASSERT_EQ("v1", value);
  ASSERT_OK(db_->Get(ro, dcfh, "bar", &value));
  ASSERT_EQ("v2", value);
}

TEST_F(BlobDBTest, Large) {
  ASSERT_TRUE(db_ != nullptr);

  WriteOptions wo;
  ReadOptions ro;
  std::string value1, value2, value3;
  Random rnd(301);
  ColumnFamilyHandle* dcfh = db_->DefaultColumnFamily();

  value1.assign(8999, '1');
  ASSERT_OK(db_->PutWithTTL(wo, dcfh, "foo", value1, 3600));
  value2.assign(9001, '2');
  ASSERT_OK(db_->PutWithTTL(wo, dcfh, "bar", value2, 3600));
  test::RandomString(&rnd, 13333, &value3);
  ASSERT_OK(db_->PutWithTTL(wo, dcfh, "barfoo", value3, 3600));

  std::string value;
  ASSERT_OK(db_->Get(ro, dcfh, "foo", &value));
  ASSERT_EQ(value1, value);
  ASSERT_OK(db_->Get(ro, dcfh, "bar", &value));
  ASSERT_EQ(value2, value);
  ASSERT_OK(db_->Get(ro, dcfh, "barfoo", &value));
  ASSERT_EQ(value3, value);
}

}  //  namespace rocksdb

// A black-box test for the ttl wrapper around rocksdb
int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

#else
#include <stdio.h>

int main(int argc, char** argv) {
  fprintf(stderr, "SKIPPED as BlobDB is not supported in ROCKSDB_LITE\n");
  return 0;
}

#endif  // !ROCKSDB_LITE

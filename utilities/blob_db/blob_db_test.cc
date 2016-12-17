// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef ROCKSDB_LITE

#include <cstdlib>
#include "db/db_test_util.h"
#include "util/random.h"
#include "util/testharness.h"
#include "util/testutil.h"
#include "utilities/blob_db/blob_db.h"

namespace rocksdb {
Random s_rnd(301);

void gen_random(char *s, const int len) {
    static const char alphanum[] =
        "0123456789"
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        "abcdefghijklmnopqrstuvwxyz";

    for (int i = 0; i < len; ++i) {
        s[i] = alphanum[s_rnd.Next() % (sizeof(alphanum) - 1)];
    }

    s[len] = 0;
}

class BlobDBTest : public testing::Test {
 public:
  BlobDBTest() :
    blobdb_(nullptr) {
    dbname_ = test::TmpDir() + "/blob_db_test";
    //Reopen1(BlobDBOptions());
  }

  ~BlobDBTest() {
    if (blobdb_) {
      delete blobdb_;
      blobdb_ = nullptr;
    }
  }

  void Reopen1(const BlobDBOptions &bdboptions, const Options& options = Options()) {
    if (blobdb_) {
      delete blobdb_;
      blobdb_ = nullptr;
    }

    BlobDBOptions bblobdb_options = bdboptions;
    Options myoptions = options;
    BlobDB::DestroyBlobDB(dbname_, myoptions,
      bblobdb_options);

    DestroyDB(dbname_, myoptions);

    myoptions.create_if_missing = true;
    EXPECT_TRUE(BlobDB::Open(myoptions, bblobdb_options, dbname_, &blobdb_).ok());
  }

  void insert_blobs() {
    WriteOptions wo;
    ReadOptions ro;
    std::string value;

    ColumnFamilyHandle* dcfh = blobdb_->DefaultColumnFamily();

    Random rnd(301);
    for (size_t i = 0; i < 100000; i++) {
      int len = rnd.Next() % 16384;
      if (!len)
        continue;

      char *val = new char[len+1];
      gen_random(val, len);

      std::string key("key");
      key += std::to_string(i % 500);

      Slice keyslice(key);
      Slice valslice(val, len+1);

      int ttl = rnd.Next() % 86400;

      ASSERT_OK(blobdb_->PutWithTTL(wo, dcfh, keyslice, valslice, ttl));
      delete [] val;
    }

    for (size_t i = 0; i < 10; i++) {
      std::string key("key");
      key += std::to_string(i % 500);
      Slice keyslice(key);
      blobdb_->Delete(wo, dcfh, keyslice);
    }
  }

  BlobDB* blobdb_;
  std::string dbname_;
};  // class BlobDBTest

TEST_F(BlobDBTest, DeleteComplex) {
  BlobDBOptions bdboptions;
  bdboptions.partial_expiration_pct = 75;
  bdboptions.gc_check_period = 20;
  bdboptions.blob_file_size = 219 * 1024;

  Reopen1(bdboptions);

  WriteOptions wo;
  ReadOptions ro;
  std::string value;

  ColumnFamilyHandle* dcfh = blobdb_->DefaultColumnFamily();

  Random rnd(301);
  for (size_t i = 0; i < 100; i++) {
    int len = rnd.Next() % 16384;
    if (!len)
      continue;

    char *val = new char[len+1];
    gen_random(val, len);

    std::string key("key");
    key += std::to_string(i);

    Slice keyslice(key);
    Slice valslice(val, len+1);

    ASSERT_OK(blobdb_->Put(wo, dcfh, keyslice, valslice));
    delete [] val;
  }

  for (size_t i = 0; i < 99; i++) {
    std::string key("key");
    key += std::to_string(i);

    Slice keyslice(key);
    blobdb_->Delete(wo, dcfh, keyslice);
  }

  sleep(60);
}

TEST_F(BlobDBTest, OverrideTest) {
  BlobDBOptions bdboptions;
  bdboptions.ttl_range = 30;
  bdboptions.gc_file_pct = 100;
  bdboptions.gc_check_period = 20;
  bdboptions.num_concurrent_simple_blobs = 2;
  bdboptions.blob_file_size = 876 * 1024 * 10;

  Options options;
  options.write_buffer_size = 256 * 1024;
  options.info_log_level = INFO_LEVEL;

  Reopen1(bdboptions, options);

  WriteOptions wo;
  ReadOptions ro;
  std::string value;

  Random rnd(301);
  ColumnFamilyHandle* dcfh = blobdb_->DefaultColumnFamily();

  for (int i = 0; i < 10000; i++) {
    int len = rnd.Next() % 16384;
    if (!len)
      continue;

    char *val = new char[len+1];
    gen_random(val, len);

    std::string key("key");
    char x[10];
    std::sprintf(x, "%04d", i);
    key += std::string(x);

    Slice keyslice(key);
    Slice valslice(val, len+1);

    ASSERT_OK(blobdb_->Put(wo, dcfh, keyslice, valslice));
    delete [] val;
  }

  // override all the keys
  for (int i = 0; i < 10000; i++) {
    int len = rnd.Next() % 16384;
    if (!len)
      continue;

    char *val = new char[len+1];
    gen_random(val, len);

    std::string key("key");
    char x[10];
    std::sprintf(x, "%04d", i);
    key += std::string(x);

    Slice keyslice(key);
    Slice valslice(val, len+1);

    ASSERT_OK(blobdb_->Put(wo, dcfh, keyslice, valslice));
    delete [] val;
  }

  blobdb_->Flush(FlushOptions());

#if 1
  blobdb_->GetBaseDB()->CompactRange(CompactRangeOptions(), nullptr, nullptr);
  reinterpret_cast<DBImpl*>(blobdb_->GetBaseDB())->TEST_WaitForFlushMemTable();
  reinterpret_cast<DBImpl*>(blobdb_->GetBaseDB())->TEST_WaitForCompact();
#endif

  sleep(120);
}

TEST_F(BlobDBTest, DeleteTest) {
  BlobDBOptions bdboptions;
  bdboptions.ttl_range = 30;
  bdboptions.gc_file_pct = 100;
  bdboptions.partial_expiration_pct = 18;
  bdboptions.gc_check_period = 20;
  bdboptions.num_concurrent_simple_blobs = 1;
  bdboptions.blob_file_size = 876 * 1024;

  Reopen1(bdboptions);

  WriteOptions wo;
  ReadOptions ro;
  std::string value;

  Random rnd(301);
  ColumnFamilyHandle* dcfh = blobdb_->DefaultColumnFamily();

  for (size_t i = 0; i < 100; i++) {
    int len = rnd.Next() % 16384;
    if (!len)
      continue;

    char *val = new char[len+1];
    gen_random(val, len);

    std::string key("key");
    key += std::to_string(i);

    Slice keyslice(key);
    Slice valslice(val, len+1);

    ASSERT_OK(blobdb_->Put(wo, dcfh, keyslice, valslice));
    delete [] val;
  }

  for (size_t i = 0; i < 100; i+= 5) {
    std::string key("key");
    key += std::to_string(i);

    Slice keyslice(key);
    blobdb_->Delete(wo, dcfh, keyslice);
  }

  sleep(60);
}

TEST_F(BlobDBTest, GCTestWithWrite) {
  BlobDBOptions bdboptions;
  bdboptions.ttl_range = 30;
  bdboptions.gc_file_pct = 100;
  bdboptions.gc_check_period = 20;

  Reopen1(bdboptions);

  WriteOptions wo;
  ReadOptions ro;
  std::string value;

  ColumnFamilyHandle* dcfh = blobdb_->DefaultColumnFamily();

  WriteBatch WB;

  Random rnd(301);
  for (size_t i = 0; i < 100; i++) {
    int len = rnd.Next() % 16384;
    if (!len)
      continue;

    int ttl = 30;

    char *val = new char[len + BlobDB::kTTLSuffixLength];
    gen_random(val, len);
    strncpy(val+len, "ttl:", 4);
    EncodeFixed32(val + len + 4, ttl);

    std::string key("key");
    key += std::to_string(i);

    Slice keyslice(key);
    Slice valslice(val, len + BlobDB::kTTLSuffixLength);

    WB.Put(dcfh, keyslice, valslice);
    delete [] val;
  }

  ASSERT_OK(blobdb_->Write(wo, &WB));

  sleep(120);
}

TEST_F(BlobDBTest, GCTestWithPut) {
  BlobDBOptions bdboptions;
  bdboptions.ttl_range = 30;
  bdboptions.gc_file_pct = 100;
  bdboptions.gc_check_period = 20;
  bdboptions.default_ttl_extractor = true;

  Reopen1(bdboptions);

  WriteOptions wo;
  ReadOptions ro;
  std::string value;
  Random rnd(301);

  ColumnFamilyHandle* dcfh = blobdb_->DefaultColumnFamily();

  for (size_t i = 0; i < 100; i++) {
    int len = rnd.Next() % 16384;
    if (!len)
      continue;

    int ttl = 30;

    char *val = new char[len + BlobDB::kTTLSuffixLength];
    gen_random(val, len);
    strncpy(val+len, "ttl:", 4);
    EncodeFixed32(val + len + 4, ttl);

    std::string key("key");
    key += std::to_string(i);

    Slice keyslice(key);
    Slice valslice(val, len + BlobDB::kTTLSuffixLength);

    ASSERT_OK(blobdb_->Put(wo, dcfh, keyslice, valslice));
    delete [] val;
  }

  sleep(120);
}

TEST_F(BlobDBTest, GCTest) {
  BlobDBOptions bdboptions;
  bdboptions.ttl_range = 30;
  bdboptions.gc_file_pct = 100;

  Reopen1(bdboptions);

  WriteOptions wo;
  ReadOptions ro;
  std::string value;
  Random rnd(301);

  ColumnFamilyHandle* dcfh = blobdb_->DefaultColumnFamily();

  for (size_t i = 0; i < 100; i++) {
    int len = rnd.Next() % 16384;
    if (!len)
      continue;

    char *val = new char[len+1];
    gen_random(val, len);

    std::string key("key");
    key += std::to_string(i);

    Slice keyslice(key);
    Slice valslice(val, len+1);

    int ttl = 30;

    ASSERT_OK(blobdb_->PutWithTTL(wo, dcfh, keyslice, valslice, ttl));
    delete [] val;
  }

  sleep(240);
}

TEST_F(BlobDBTest, MultipleWriters) {
  ASSERT_TRUE(blobdb_ != nullptr);

  std::vector<std::thread> workers;
  for (size_t ii = 0; ii < 10; ii++)
    workers.push_back(std::thread(&BlobDBTest::insert_blobs, this));

  for (std::thread &t : workers) {
    if (t.joinable()) {
      t.join();
    }
  }

  sleep(180);
  // ASSERT_OK(blobdb_->PutWithTTL(wo, dcfh, "bar", "v2", 60));
  // ASSERT_OK(blobdb_->Get(ro, dcfh, "foo", &value));
  // ASSERT_EQ("v1", value);
  // ASSERT_OK(blobdb_->Get(ro, dcfh, "bar", &value));
  // ASSERT_EQ("v2", value);
}

#if 0
TEST_F(BlobDBTest, Large) {
  ASSERT_TRUE(blobdb_ != nullptr);

  WriteOptions wo;
  ReadOptions ro;
  std::string value1, value2, value3;
  Random rnd(301);
  ColumnFamilyHandle* dcfh = blobdb_->DefaultColumnFamily();

  value1.assign(8999, '1');
  ASSERT_OK(blobdb_->PutWithTTL(wo, dcfh, "foo", value1, 3600));
  value2.assign(9001, '2');
  ASSERT_OK(blobdb_->PutWithTTL(wo, dcfh, "bar", value2, 3600));
  test::RandomString(&rnd, 13333, &value3);
  ASSERT_OK(blobdb_->PutWithTTL(wo, dcfh, "barfoo", value3, 3600));

  std::string value;
  ASSERT_OK(blobdb_->Get(ro, dcfh, "foo", &value));
  ASSERT_EQ(value1, value);
  ASSERT_OK(blobdb_->Get(ro, dcfh, "bar", &value));
  ASSERT_EQ(value2, value);
  ASSERT_OK(blobdb_->Get(ro, dcfh, "barfoo", &value));
  ASSERT_EQ(value3, value);
}
#endif

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

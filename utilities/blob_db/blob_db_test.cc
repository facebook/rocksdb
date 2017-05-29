//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
#ifndef ROCKSDB_LITE

#include "utilities/blob_db/blob_db.h"
#include <cstdlib>
#include "db/db_test_util.h"
#include "util/random.h"
#include "util/testharness.h"
#include "util/testutil.h"
#include "utilities/blob_db/blob_db_options_impl.h"

namespace rocksdb {

namespace blob_db {
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
  BlobDBTest() : blobdb_(nullptr) {
    dbname_ = test::TmpDir() + "/blob_db_test";
    // Reopen1(BlobDBOptionsImpl());
  }

  ~BlobDBTest() {
    if (blobdb_) {
      delete blobdb_;
      blobdb_ = nullptr;
    }
  }

  void Reopen1(const BlobDBOptionsImpl &bdboptions,
               const Options &options = Options()) {
    if (blobdb_) {
      delete blobdb_;
      blobdb_ = nullptr;
    }

    BlobDBOptionsImpl bblobdb_options = bdboptions;
    Options myoptions = options;
    BlobDB::DestroyBlobDB(dbname_, myoptions, bblobdb_options);

    DestroyDB(dbname_, myoptions);

    myoptions.create_if_missing = true;
    EXPECT_TRUE(
        BlobDB::Open(myoptions, bblobdb_options, dbname_, &blobdb_).ok());
  }

  void insert_blobs() {
    WriteOptions wo;
    ReadOptions ro;
    std::string value;

    ColumnFamilyHandle *dcfh = blobdb_->DefaultColumnFamily();

    Random rnd(301);
    for (size_t i = 0; i < 100000; i++) {
      int len = rnd.Next() % 16384;
      if (!len) continue;

      char *val = new char[len + 1];
      gen_random(val, len);

      std::string key("key");
      key += std::to_string(i % 500);

      Slice keyslice(key);
      Slice valslice(val, len + 1);

      int ttl = rnd.Next() % 86400;

      ASSERT_OK(blobdb_->PutWithTTL(wo, dcfh, keyslice, valslice, ttl));
      delete[] val;
    }

    for (size_t i = 0; i < 10; i++) {
      std::string key("key");
      key += std::to_string(i % 500);
      Slice keyslice(key);
      blobdb_->Delete(wo, dcfh, keyslice);
    }
  }

  BlobDB *blobdb_;
  std::string dbname_;
};  // class BlobDBTest

TEST_F(BlobDBTest, DeleteComplex) {
  BlobDBOptionsImpl bdboptions;
  bdboptions.partial_expiration_pct = 75;
  bdboptions.gc_check_period_millisecs = 20 * 1000;
  bdboptions.blob_file_size = 219 * 1024;

  Reopen1(bdboptions);

  WriteOptions wo;
  ReadOptions ro;
  std::string value;

  ColumnFamilyHandle *dcfh = blobdb_->DefaultColumnFamily();

  Random rnd(301);
  for (size_t i = 0; i < 100; i++) {
    int len = rnd.Next() % 16384;
    if (!len) continue;

    char *val = new char[len + 1];
    gen_random(val, len);

    std::string key("key");
    key += std::to_string(i);

    Slice keyslice(key);
    Slice valslice(val, len + 1);

    ASSERT_OK(blobdb_->Put(wo, dcfh, keyslice, valslice));
    delete[] val;
  }

  for (size_t i = 0; i < 99; i++) {
    std::string key("key");
    key += std::to_string(i);

    Slice keyslice(key);
    blobdb_->Delete(wo, dcfh, keyslice);
  }

  Env::Default()->SleepForMicroseconds(60 * 1000 * 1000);
}

TEST_F(BlobDBTest, OverrideTest) {
  BlobDBOptionsImpl bdboptions;
  bdboptions.ttl_range_secs = 30;
  bdboptions.gc_file_pct = 100;
  bdboptions.gc_check_period_millisecs = 20 * 1000;
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
  ColumnFamilyHandle *dcfh = blobdb_->DefaultColumnFamily();

  for (int i = 0; i < 10000; i++) {
    int len = rnd.Next() % 16384;
    if (!len) continue;

    char *val = new char[len + 1];
    gen_random(val, len);

    std::string key("key");
    char x[10];
    std::sprintf(x, "%04d", i);
    key += std::string(x);

    Slice keyslice(key);
    Slice valslice(val, len + 1);

    ASSERT_OK(blobdb_->Put(wo, dcfh, keyslice, valslice));
    delete[] val;
  }

  // override all the keys
  for (int i = 0; i < 10000; i++) {
    int len = rnd.Next() % 16384;
    if (!len) continue;

    char *val = new char[len + 1];
    gen_random(val, len);

    std::string key("key");
    char x[10];
    std::sprintf(x, "%04d", i);
    key += std::string(x);

    Slice keyslice(key);
    Slice valslice(val, len + 1);

    ASSERT_OK(blobdb_->Put(wo, dcfh, keyslice, valslice));
    delete[] val;
  }

  blobdb_->Flush(FlushOptions());

#if 1
  blobdb_->GetBaseDB()->CompactRange(CompactRangeOptions(), nullptr, nullptr);
  reinterpret_cast<DBImpl *>(blobdb_->GetBaseDB())->TEST_WaitForFlushMemTable();
  reinterpret_cast<DBImpl *>(blobdb_->GetBaseDB())->TEST_WaitForCompact();
#endif

  Env::Default()->SleepForMicroseconds(120 * 1000 * 1000);
}

TEST_F(BlobDBTest, DeleteTest) {
  BlobDBOptionsImpl bdboptions;
  bdboptions.ttl_range_secs = 30;
  bdboptions.gc_file_pct = 100;
  bdboptions.partial_expiration_pct = 18;
  bdboptions.gc_check_period_millisecs = 20 * 1000;
  bdboptions.num_concurrent_simple_blobs = 1;
  bdboptions.blob_file_size = 876 * 1024;

  Reopen1(bdboptions);

  WriteOptions wo;
  ReadOptions ro;
  std::string value;

  Random rnd(301);
  ColumnFamilyHandle *dcfh = blobdb_->DefaultColumnFamily();

  for (size_t i = 0; i < 100; i++) {
    int len = rnd.Next() % 16384;
    if (!len) continue;

    char *val = new char[len + 1];
    gen_random(val, len);

    std::string key("key");
    key += std::to_string(i);

    Slice keyslice(key);
    Slice valslice(val, len + 1);

    ASSERT_OK(blobdb_->Put(wo, dcfh, keyslice, valslice));
    delete[] val;
  }

  for (size_t i = 0; i < 100; i += 5) {
    std::string key("key");
    key += std::to_string(i);

    Slice keyslice(key);
    blobdb_->Delete(wo, dcfh, keyslice);
  }

  Env::Default()->SleepForMicroseconds(60 * 1000 * 1000);
}

TEST_F(BlobDBTest, GCTestWithWrite) {
  BlobDBOptionsImpl bdboptions;
  bdboptions.ttl_range_secs = 30;
  bdboptions.gc_file_pct = 100;
  bdboptions.gc_check_period_millisecs = 20 * 1000;
  bdboptions.default_ttl_extractor = true;

  Reopen1(bdboptions);

  WriteOptions wo;
  ReadOptions ro;
  std::string value;

  ColumnFamilyHandle *dcfh = blobdb_->DefaultColumnFamily();

  WriteBatch WB;

  Random rnd(301);
  for (size_t i = 0; i < 100; i++) {
    int len = rnd.Next() % 16384;
    if (!len) continue;

    int ttl = 30;

    char *val = new char[len + BlobDB::kTTLSuffixLength];
    gen_random(val, len);
    strncpy(val + len, "ttl:", 4);
    EncodeFixed32(val + len + 4, ttl);

    std::string key("key");
    key += std::to_string(i);

    Slice keyslice(key);
    Slice valslice(val, len + BlobDB::kTTLSuffixLength);

    WB.Put(dcfh, keyslice, valslice);
    delete[] val;
  }

  ASSERT_OK(blobdb_->Write(wo, &WB));

  Env::Default()->SleepForMicroseconds(120 * 1000 * 1000);
}

void cb_evict(const ColumnFamilyHandle *cfh, const Slice &key,
              const Slice &val) {
  fprintf(stderr, "key evicted: %s\n", key.ToString().c_str());
}

static const char *LONG_STRING =
    "AJFJFJFFFFFFFFFFFFFFFFFFFJFHFHFHFHFHFHFHHFHHFHHFH "
    "AJFJFJFFFFFFFFFFFFFFFFFFFJFHFHFHFHFHFHFHHFHHFHHFH "
    "AJFJFJFFFFFFFFFFFFFFFFFFFJFHFHFHFHFHFHFHHFHHFHHFH "
    "AJFJFJFFFFFFFFFFFFFFFFFFFJFHFHFHFHFHFHFHHFHHFHHFH "
    "AJFJFJFJFJFJTWFNLLFKFFMFMFMFMFMFMFMFMFMFMFMFMFMMF "
    "AJFJFJFFFFFFFFFFFFFFFFFFFJFHFHFHFHFHFHFHHFHHFHHFH "
    "AJFJFJFFFFFFFFFFFFFFFFFFFJFHFHFHFHFHFHFHHFHHFHHFH "
    "AJFJFJFFFFFFFFFFFFFFFFFFFJFHFHFHFHFHFHFHHFHHFHHFH "
    "AJFJFJFFFFFFFFFFFFFFFFFFFJFHFHFHFHFHFHFHHFHHFHHFH "
    "AJFJFJFFFFFFFFFFFFFFFFFFFJFHFHFHFHFHFHFHHFHHFHHFH "
    "AJFJFJFFFFFFFFFFFFFFFFFFFJFHFHFHFHFHFHFHHFHHFHHFH "
    "AJFJFJFFFFFFFFFFFFFFFFFFFJFHFHFHFHFHFHFHHFHHFHHFH "
    "AJFJFJFFFFFFFFFFFFFFFFFFFJFHFHFHFHFHFHFHHFHHFHHFH "
    "AJFJFJFFFFFFFFFFFFFFFFFFFJFHFHFHFHFHFHFHHFHHFHHFH "
    "AJFJFJFFFFFFFFFFFFFFFFFFFJFHFHFHFHFHFHFHHFHHFHHFH "
    "AJFJFJFFFFFFFFFFFFFFFFFFFJFHFHFHFHFHFHFHHFHHFHHFH "
    "AJFJFJFFFFFFFFFFFFFFFFFFFJFHFHFHFHFHFHFHHFHHFHHFH "
    "AJFJFJFFFFFFFFFFFFFFFFFFFJFHFHFHFHFHFHFHHFHHFHHFH "
    "AJFJFJAJFJFJFJFJTWBFNMFLLWMFMFMFMWKWMFMFMFMFMFMFM "
    "AJFJFJFFFFFFFFFFFFFFFFFFFJFHFHFHFHFHFHFHHFHHFHHFH "
    "AJFJFJFFFFFFFFFFFFFFFFFFFJFHFHFHFHFHFHFHHFHHFHHFH "
    "AJFJFJFFFFFFFFFFFFFFFFFFFJFHFHFHFHFHFHFHHFHHFHHFH "
    "AJFJFJFFFFFFFFFFFFFFFFFFFJFHFHFHFHFHFHFHHFHHFHHFH "
    "AJFJFJFFFFFFFFFFFFFFFFFFFJFHFHFHFHFHFHFHHFHHFHHFH "
    "AJFJFJFFFFFFFFFFFFFFFFFFFJFHFHFHFHFHFHFHHFHHFHHFH "
    "AJFJFJFFFFFFFFFFFFFFFFFFFJFHFHFHFHFHFHFHHFHHFHHFH "
    "AJFJFJFFFFFFFFFFFFFFFFFFFJFHFHFHFHFHFHFHHFHHFHHFH ";

TEST_F(BlobDBTest, GetWithCompression) {
  BlobDBOptionsImpl bdboptions;
  bdboptions.gc_file_pct = 100;
  bdboptions.gc_check_period_millisecs = 20 * 1000;
  bdboptions.default_ttl_extractor = true;
  bdboptions.gc_evict_cb_fn = &cb_evict;
  bdboptions.compression = CompressionType::kLZ4Compression;

  Reopen1(bdboptions);

  WriteOptions wo;
  ReadOptions ro;
  std::string value;
  Random rnd(301);

  ColumnFamilyHandle *dcfh = blobdb_->DefaultColumnFamily();

  std::string orig(LONG_STRING);

  for (size_t i = 0; i < 10000; i++) {
    int len = orig.length();
    int ttl = 3000 * (rnd.Next() % 10);

    char *val = new char[len + BlobDB::kTTLSuffixLength];
    strncpy(val, LONG_STRING, len);
    strncpy(val + len, "ttl:", 4);
    EncodeFixed32(val + len + 4, ttl);

    std::string key("key");
    key += std::to_string(i);

    Slice keyslice(key);
    Slice valslice(val, len + BlobDB::kTTLSuffixLength);

    ASSERT_OK(blobdb_->Put(wo, dcfh, keyslice, valslice));
    delete[] val;
  }

  for (size_t i = 0; i < 10000; i++) {
    std::string key("key");
    key += std::to_string(i);

    Slice keyslice(key);
    std::string val;
    Status s = blobdb_->Get(ro, dcfh, keyslice, &val);
    ASSERT_TRUE(orig == val);
  }

  Env::Default()->SleepForMicroseconds(120 * 1000 * 1000);
}

TEST_F(BlobDBTest, GCTestWithPutAndCompression) {
  BlobDBOptionsImpl bdboptions;
  bdboptions.ttl_range_secs = 30;
  bdboptions.gc_file_pct = 100;
  bdboptions.gc_check_period_millisecs = 20 * 1000;
  bdboptions.default_ttl_extractor = true;
  bdboptions.gc_evict_cb_fn = &cb_evict;
  bdboptions.compression = CompressionType::kLZ4Compression;

  Reopen1(bdboptions);

  WriteOptions wo;
  ReadOptions ro;
  std::string value;
  Random rnd(301);

  ColumnFamilyHandle *dcfh = blobdb_->DefaultColumnFamily();

  for (size_t i = 0; i < 100; i++) {
    int len = rnd.Next() % 16384;
    if (!len) continue;

    int ttl = 30;

    char *val = new char[len + BlobDB::kTTLSuffixLength];
    gen_random(val, len);
    strncpy(val + len, "ttl:", 4);
    EncodeFixed32(val + len + 4, ttl);

    std::string key("key");
    key += std::to_string(i);

    Slice keyslice(key);
    Slice valslice(val, len + BlobDB::kTTLSuffixLength);

    ASSERT_OK(blobdb_->Put(wo, dcfh, keyslice, valslice));
    delete[] val;
  }

  Env::Default()->SleepForMicroseconds(120 * 1000 * 1000);
}

TEST_F(BlobDBTest, GCTestWithPut) {
  BlobDBOptionsImpl bdboptions;
  bdboptions.ttl_range_secs = 30;
  bdboptions.gc_file_pct = 100;
  bdboptions.gc_check_period_millisecs = 20 * 1000;
  bdboptions.default_ttl_extractor = true;
  bdboptions.gc_evict_cb_fn = &cb_evict;

  Reopen1(bdboptions);

  WriteOptions wo;
  ReadOptions ro;
  std::string value;
  Random rnd(301);

  ColumnFamilyHandle *dcfh = blobdb_->DefaultColumnFamily();

  for (size_t i = 0; i < 100; i++) {
    int len = rnd.Next() % 16384;
    if (!len) continue;

    int ttl = 30;

    char *val = new char[len + BlobDB::kTTLSuffixLength];
    gen_random(val, len);
    strncpy(val + len, "ttl:", 4);
    EncodeFixed32(val + len + 4, ttl);

    std::string key("key");
    key += std::to_string(i);

    Slice keyslice(key);
    Slice valslice(val, len + BlobDB::kTTLSuffixLength);

    ASSERT_OK(blobdb_->Put(wo, dcfh, keyslice, valslice));
    delete[] val;
  }

  Env::Default()->SleepForMicroseconds(120 * 1000 * 1000);
}

TEST_F(BlobDBTest, GCTest) {
  BlobDBOptionsImpl bdboptions;
  bdboptions.ttl_range_secs = 30;
  bdboptions.gc_file_pct = 100;

  Reopen1(bdboptions);

  WriteOptions wo;
  ReadOptions ro;
  std::string value;
  Random rnd(301);

  ColumnFamilyHandle *dcfh = blobdb_->DefaultColumnFamily();

  for (size_t i = 0; i < 100; i++) {
    int len = rnd.Next() % 16384;
    if (!len) continue;

    char *val = new char[len + 1];
    gen_random(val, len);

    std::string key("key");
    key += std::to_string(i);

    Slice keyslice(key);
    Slice valslice(val, len + 1);

    int ttl = 30;

    ASSERT_OK(blobdb_->PutWithTTL(wo, dcfh, keyslice, valslice, ttl));
    delete[] val;
  }

  Env::Default()->SleepForMicroseconds(240 * 1000 * 1000);
}

TEST_F(BlobDBTest, DISABLED_MultipleWriters) {
  BlobDBOptionsImpl bdboptions;
  Reopen1(bdboptions);

  ASSERT_TRUE(blobdb_ != nullptr);

  std::vector<std::thread> workers;
  for (size_t ii = 0; ii < 10; ii++)
    workers.push_back(std::thread(&BlobDBTest::insert_blobs, this));

  for (std::thread &t : workers) {
    if (t.joinable()) {
      t.join();
    }
  }

  Env::Default()->SleepForMicroseconds(180 * 1000 * 1000);
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

}  //  namespace blob_db
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

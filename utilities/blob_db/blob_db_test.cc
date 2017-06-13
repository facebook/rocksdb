//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
#ifndef ROCKSDB_LITE

#include "utilities/blob_db/blob_db.h"
#include <cstdlib>
#include <map>
#include <memory>
#include <string>
#include "db/db_test_util.h"
#include "port/port.h"
#include "util/random.h"
#include "util/string_util.h"
#include "util/testharness.h"
#include "utilities/blob_db/blob_db_impl.h"
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
  const int kMaxBlobSize = 1 << 14;

  BlobDBTest() : blobdb_(nullptr) {
    dbname_ = test::TmpDir() + "/blob_db_test";
    // Reopen(BlobDBOptionsImpl());
  }

  ~BlobDBTest() {
    if (blobdb_) {
      delete blobdb_;
      blobdb_ = nullptr;
    }
  }

  void Reopen(const BlobDBOptionsImpl &bdboptions,
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
    ASSERT_NE(nullptr, blobdb_);
  }

  void PutRandomWithTTL(const std::string &key, int32_t ttl, Random *rnd,
                        std::map<std::string, std::string> *data = nullptr) {
    int len = rnd->Next() % kMaxBlobSize + 1;
    std::string value = test::RandomHumanReadableString(rnd, len);
    ColumnFamilyHandle *cfh = blobdb_->DefaultColumnFamily();
    ASSERT_OK(blobdb_->PutWithTTL(WriteOptions(), cfh, Slice(key), Slice(value),
                                  ttl));
    if (data != nullptr) {
      (*data)[key] = value;
    }
  }

  void PutRandom(const std::string &key, Random *rnd,
                 std::map<std::string, std::string> *data = nullptr) {
    PutRandomWithTTL(key, -1, rnd, data);
  }

  void Delete(const std::string &key) {
    ColumnFamilyHandle *cfh = blobdb_->DefaultColumnFamily();
    ASSERT_OK(blobdb_->Delete(WriteOptions(), cfh, key));
  }

  // Verify blob db contain expected data and nothing more.
  // TODO(yiwu): Verify blob files are consistent with data in LSM.
  void VerifyDB(const std::map<std::string, std::string> &data) {
    Iterator *iter = blobdb_->NewIterator(ReadOptions());
    iter->SeekToFirst();
    for (auto &p : data) {
      ASSERT_TRUE(iter->Valid());
      ASSERT_EQ(p.first, iter->key().ToString());
      ASSERT_EQ(p.second, iter->value().ToString());
      iter->Next();
    }
    ASSERT_FALSE(iter->Valid());
    ASSERT_OK(iter->status());
    delete iter;
  }

  void InsertBlobs() {
    WriteOptions wo;
    std::string value;

    Random rnd(301);
    for (size_t i = 0; i < 100000; i++) {
      int32_t ttl = rnd.Next() % 86400;
      PutRandomWithTTL("key" + ToString(i % 500), ttl, &rnd, nullptr);
    }

    for (size_t i = 0; i < 10; i++) {
      Delete("key" + ToString(i % 500));
    }
  }

  BlobDB *blobdb_;
  std::string dbname_;
};  // class BlobDBTest

TEST_F(BlobDBTest, DeleteComplex) {
  BlobDBOptionsImpl bdboptions;
  Reopen(bdboptions);

  Random rnd(301);
  for (size_t i = 0; i < 100; i++) {
    PutRandom("key" + ToString(i), &rnd, nullptr);
  }
  for (size_t i = 0; i < 100; i++) {
    Delete("key" + ToString(i));
  }
  // DB should be empty.
  VerifyDB({});
}

TEST_F(BlobDBTest, OverrideTest) {
  BlobDBOptionsImpl bdboptions;
  bdboptions.num_concurrent_simple_blobs = 2;
  bdboptions.blob_file_size = 876 * 1024 * 10;

  Options options;
  options.write_buffer_size = 256 * 1024;

  Reopen(bdboptions, options);

  Random rnd(301);
  std::map<std::string, std::string> data;

  for (int i = 0; i < 10000; i++) {
    PutRandom("key" + ToString(i), &rnd, nullptr);
  }
  // override all the keys
  for (int i = 0; i < 10000; i++) {
    PutRandom("key" + ToString(i), &rnd, &data);
  }
  VerifyDB(data);
}

TEST_F(BlobDBTest, DeleteTest) {
  BlobDBOptionsImpl bdboptions;
  bdboptions.num_concurrent_simple_blobs = 1;
  bdboptions.blob_file_size = 876 * 1024;

  Reopen(bdboptions);

  Random rnd(301);
  std::map<std::string, std::string> data;

  for (size_t i = 0; i < 100; i++) {
    PutRandom("key" + ToString(i), &rnd, &data);
  }
  for (size_t i = 0; i < 100; i += 5) {
    Delete("key" + ToString(i));
    data.erase("key" + ToString(i));
  }
  VerifyDB(data);
}

TEST_F(BlobDBTest, GCTestWithWrite) {
  BlobDBOptionsImpl bdboptions;
  bdboptions.ttl_range_secs = 30;
  bdboptions.gc_file_pct = 100;
  bdboptions.gc_check_period_millisecs = 20 * 1000;
  bdboptions.default_ttl_extractor = true;

  Reopen(bdboptions);

  WriteOptions wo;
  ReadOptions ro;
  std::string value;

  ColumnFamilyHandle *dcfh = blobdb_->DefaultColumnFamily();

  WriteBatch batch;

  Random rnd(301);
  for (size_t i = 0; i < 100; i++) {
    int len = rnd.Next() % kMaxBlobSize;
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

    batch.Put(dcfh, keyslice, valslice);
    delete[] val;
  }

  ASSERT_OK(blobdb_->Write(wo, &batch));

  // TODO(yiwu): Use sync point to properly trigger GC and check result.
  // Env::Default()->SleepForMicroseconds(120 * 1000 * 1000);
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

  Reopen(bdboptions);

  WriteOptions wo;
  ReadOptions ro;
  std::string value;
  Random rnd(301);

  ColumnFamilyHandle *dcfh = blobdb_->DefaultColumnFamily();

  std::string orig(LONG_STRING);

  for (size_t i = 0; i < 10000; i++) {
    size_t len = orig.length();
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

  // TODO(yiwu): Use sync point to properly trigger GC and check result.
  // Env::Default()->SleepForMicroseconds(120 * 1000 * 1000);
}

TEST_F(BlobDBTest, GCTestWithPutAndCompression) {
  BlobDBOptionsImpl bdboptions;
  bdboptions.ttl_range_secs = 30;
  bdboptions.gc_file_pct = 100;
  bdboptions.gc_check_period_millisecs = 20 * 1000;
  bdboptions.default_ttl_extractor = true;
  bdboptions.gc_evict_cb_fn = &cb_evict;
  bdboptions.compression = CompressionType::kLZ4Compression;

  Reopen(bdboptions);

  WriteOptions wo;
  ReadOptions ro;
  std::string value;
  Random rnd(301);

  ColumnFamilyHandle *dcfh = blobdb_->DefaultColumnFamily();

  for (size_t i = 0; i < 100; i++) {
    int len = rnd.Next() % kMaxBlobSize;
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

  // TODO(yiwu): Use sync point to properly trigger GC and check result.
  // Env::Default()->SleepForMicroseconds(120 * 1000 * 1000);
}

TEST_F(BlobDBTest, GCTestWithPut) {
  BlobDBOptionsImpl bdboptions;
  bdboptions.ttl_range_secs = 30;
  bdboptions.gc_file_pct = 100;
  bdboptions.gc_check_period_millisecs = 20 * 1000;
  bdboptions.default_ttl_extractor = true;
  bdboptions.gc_evict_cb_fn = &cb_evict;

  Reopen(bdboptions);

  WriteOptions wo;
  ReadOptions ro;
  std::string value;
  Random rnd(301);

  ColumnFamilyHandle *dcfh = blobdb_->DefaultColumnFamily();

  for (size_t i = 0; i < 100; i++) {
    int len = rnd.Next() % kMaxBlobSize;
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

  // TODO(yiwu): Use sync point to properly trigger GC and check result.
  // Env::Default()->SleepForMicroseconds(120 * 1000 * 1000);
}

TEST_F(BlobDBTest, GCTest) {
  BlobDBOptionsImpl bdboptions;
  bdboptions.ttl_range_secs = 30;
  bdboptions.gc_file_pct = 100;

  Reopen(bdboptions);

  WriteOptions wo;
  ReadOptions ro;
  std::string value;
  Random rnd(301);

  ColumnFamilyHandle *dcfh = blobdb_->DefaultColumnFamily();

  for (size_t i = 0; i < 100; i++) {
    int len = rnd.Next() % kMaxBlobSize;
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

  // TODO(yiwu): Use sync point to properly trigger GC and check result.
  // Env::Default()->SleepForMicroseconds(240 * 1000 * 1000);
}

TEST_F(BlobDBTest, DISABLED_MultipleWriters) {
  BlobDBOptionsImpl bdboptions;
  Reopen(bdboptions);

  ASSERT_TRUE(blobdb_ != nullptr);

  std::vector<std::thread> workers;
  for (size_t ii = 0; ii < 10; ii++)
    workers.push_back(std::thread(&BlobDBTest::InsertBlobs, this));

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

TEST_F(BlobDBTest, Large) {
  BlobDBOptionsImpl bdboptions;
  Options options;
  Reopen(bdboptions, options);

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

// Test sequence number store in blob file is correct.
TEST_F(BlobDBTest, SequenceNumber) {
  Random rnd(223);
  Reopen(BlobDBOptionsImpl(), Options());
  SequenceNumber sequence = blobdb_->GetLatestSequenceNumber();
  BlobDBImpl *blobdb_impl = reinterpret_cast<BlobDBImpl *>(blobdb_);
  for (int i = 0; i < 100; i++) {
    std::string key = "key" + ToString(i);
    PutRandom(key, &rnd);
    sequence += 1;
    ASSERT_EQ(sequence, blobdb_->GetLatestSequenceNumber());
    SequenceNumber actual_sequence = 0;
    ASSERT_OK(blobdb_impl->TEST_GetSequenceNumber(key, &actual_sequence));
    ASSERT_EQ(sequence, actual_sequence);
  }
  for (int i = 0; i < 100; i++) {
    WriteBatch batch;
    size_t batch_size = rnd.Next() % 10 + 1;
    for (size_t k = 0; k < batch_size; k++) {
      std::string value = test::RandomHumanReadableString(&rnd, 1000);
      ASSERT_OK(batch.Put("key" + ToString(i) + "-" + ToString(k), value));
    }
    ASSERT_OK(blobdb_->Write(WriteOptions(), &batch));
    for (size_t k = 0; k < batch_size; k++) {
      std::string key = "key" + ToString(i) + "-" + ToString(k);
      sequence++;
      SequenceNumber actual_sequence;
      ASSERT_OK(blobdb_impl->TEST_GetSequenceNumber(key, &actual_sequence));
      ASSERT_EQ(sequence, actual_sequence);
    }
    ASSERT_EQ(sequence, blobdb_->GetLatestSequenceNumber());
  }
}

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

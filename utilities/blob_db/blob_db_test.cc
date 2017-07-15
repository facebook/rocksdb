//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

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

class BlobDBTest : public testing::Test {
 public:
  const int kMaxBlobSize = 1 << 14;

  BlobDBTest() : dbname_(test::TmpDir() + "/blob_db_test"), blob_db_(nullptr) {
    Status s = DestroyBlobDB(dbname_, Options(), BlobDBOptions());
    assert(s.ok());
  }

  ~BlobDBTest() { Destroy(); }

  void Open(BlobDBOptionsImpl bdb_options = BlobDBOptionsImpl(),
            Options options = Options()) {
    options.create_if_missing = true;
    Reopen(bdb_options, options);
  }

  void Reopen(BlobDBOptionsImpl bdb_options = BlobDBOptionsImpl(),
              Options options = Options()) {
    if (blob_db_) {
      delete blob_db_;
      blob_db_ = nullptr;
    }
    ASSERT_OK(BlobDB::Open(options, bdb_options, dbname_, &blob_db_));
    ASSERT_TRUE(blob_db_);
  }

  void Destroy() {
    if (blob_db_) {
      Options options = blob_db_->GetOptions();
      BlobDBOptions bdb_options = blob_db_->GetBlobDBOptions();
      delete blob_db_;
      ASSERT_OK(DestroyBlobDB(dbname_, options, bdb_options));
      blob_db_ = nullptr;
    }
  }

  void PutRandomWithTTL(const std::string &key, int32_t ttl, Random *rnd,
                        std::map<std::string, std::string> *data = nullptr) {
    int len = rnd->Next() % kMaxBlobSize + 1;
    std::string value = test::RandomHumanReadableString(rnd, len);
    ASSERT_OK(
        blob_db_->PutWithTTL(WriteOptions(), Slice(key), Slice(value), ttl));
    if (data != nullptr) {
      (*data)[key] = value;
    }
  }

  void PutRandom(const std::string &key, Random *rnd,
                 std::map<std::string, std::string> *data = nullptr) {
    PutRandomWithTTL(key, -1, rnd, data);
  }

  void PutRandomToWriteBatch(
      const std::string &key, Random *rnd, WriteBatch *batch,
      std::map<std::string, std::string> *data = nullptr) {
    int len = rnd->Next() % kMaxBlobSize + 1;
    std::string value = test::RandomHumanReadableString(rnd, len);
    ASSERT_OK(batch->Put(key, value));
    if (data != nullptr) {
      (*data)[key] = value;
    }
  }

  void Delete(const std::string &key,
              std::map<std::string, std::string> *data = nullptr) {
    ASSERT_OK(blob_db_->Delete(WriteOptions(), key));
    if (data != nullptr) {
      data->erase(key);
    }
  }

  // Verify blob db contain expected data and nothing more.
  // TODO(yiwu): Verify blob files are consistent with data in LSM.
  void VerifyDB(const std::map<std::string, std::string> &data) {
    Iterator *iter = blob_db_->NewIterator(ReadOptions());
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

  const std::string dbname_;
  BlobDB *blob_db_;
};  // class BlobDBTest

TEST_F(BlobDBTest, Put) {
  Random rnd(301);
  BlobDBOptionsImpl bdb_options;
  bdb_options.disable_background_tasks = true;
  Open(bdb_options);
  std::map<std::string, std::string> data;
  for (size_t i = 0; i < 100; i++) {
    PutRandom("key" + ToString(i), &rnd, &data);
  }
  VerifyDB(data);
}

TEST_F(BlobDBTest, WriteBatch) {
  Random rnd(301);
  BlobDBOptionsImpl bdb_options;
  bdb_options.disable_background_tasks = true;
  Open(bdb_options);
  std::map<std::string, std::string> data;
  for (size_t i = 0; i < 100; i++) {
    WriteBatch batch;
    for (size_t j = 0; j < 10; j++) {
      PutRandomToWriteBatch("key" + ToString(j * 100 + i), &rnd, &batch, &data);
    }
    blob_db_->Write(WriteOptions(), &batch);
  }
  VerifyDB(data);
}

TEST_F(BlobDBTest, Delete) {
  Random rnd(301);
  BlobDBOptionsImpl bdb_options;
  bdb_options.disable_background_tasks = true;
  Open(bdb_options);
  std::map<std::string, std::string> data;
  for (size_t i = 0; i < 100; i++) {
    PutRandom("key" + ToString(i), &rnd, &data);
  }
  for (size_t i = 0; i < 100; i += 5) {
    Delete("key" + ToString(i), &data);
  }
  VerifyDB(data);
}

TEST_F(BlobDBTest, DeleteBatch) {
  Random rnd(301);
  BlobDBOptionsImpl bdb_options;
  bdb_options.disable_background_tasks = true;
  Open(bdb_options);
  for (size_t i = 0; i < 100; i++) {
    PutRandom("key" + ToString(i), &rnd);
  }
  WriteBatch batch;
  for (size_t i = 0; i < 100; i++) {
    batch.Delete("key" + ToString(i));
  }
  ASSERT_OK(blob_db_->Write(WriteOptions(), &batch));
  // DB should be empty.
  VerifyDB({});
}

TEST_F(BlobDBTest, Override) {
  Random rnd(301);
  BlobDBOptionsImpl bdb_options;
  bdb_options.disable_background_tasks = true;
  Open(bdb_options);
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

#ifdef SNAPPY
TEST_F(BlobDBTest, Compression) {
  Random rnd(301);
  BlobDBOptionsImpl bdb_options;
  bdb_options.disable_background_tasks = true;
  bdb_options.compression = CompressionType::kSnappyCompression;
  Open(bdb_options);
  std::map<std::string, std::string> data;
  for (size_t i = 0; i < 100; i++) {
    PutRandom("put-key" + ToString(i), &rnd, &data);
  }
  for (int i = 0; i < 100; i++) {
    WriteBatch batch;
    for (size_t j = 0; j < 10; j++) {
      PutRandomToWriteBatch("write-batch-key" + ToString(j * 100 + i), &rnd,
                            &batch, &data);
    }
    blob_db_->Write(WriteOptions(), &batch);
  }
  VerifyDB(data);
}
#endif

TEST_F(BlobDBTest, DISABLED_MultipleWriters) {
  Open();

  std::vector<port::Thread> workers;
  for (size_t ii = 0; ii < 10; ii++)
    workers.push_back(port::Thread(&BlobDBTest::InsertBlobs, this));

  for (auto& t : workers) {
    if (t.joinable()) {
      t.join();
    }
  }
}

// Test sequence number store in blob file is correct.
TEST_F(BlobDBTest, SequenceNumber) {
  Random rnd(301);
  BlobDBOptionsImpl bdb_options;
  bdb_options.disable_background_tasks = true;
  Open(bdb_options);
  SequenceNumber sequence = blob_db_->GetLatestSequenceNumber();
  BlobDBImpl *blob_db_impl = reinterpret_cast<BlobDBImpl *>(blob_db_);
  for (int i = 0; i < 100; i++) {
    std::string key = "key" + ToString(i);
    PutRandom(key, &rnd);
    sequence += 1;
    ASSERT_EQ(sequence, blob_db_->GetLatestSequenceNumber());
    SequenceNumber actual_sequence = 0;
    ASSERT_OK(blob_db_impl->TEST_GetSequenceNumber(key, &actual_sequence));
    ASSERT_EQ(sequence, actual_sequence);
  }
  for (int i = 0; i < 100; i++) {
    WriteBatch batch;
    size_t batch_size = rnd.Next() % 10 + 1;
    for (size_t k = 0; k < batch_size; k++) {
      std::string value = test::RandomHumanReadableString(&rnd, 1000);
      ASSERT_OK(batch.Put("key" + ToString(i) + "-" + ToString(k), value));
    }
    ASSERT_OK(blob_db_->Write(WriteOptions(), &batch));
    for (size_t k = 0; k < batch_size; k++) {
      std::string key = "key" + ToString(i) + "-" + ToString(k);
      sequence++;
      SequenceNumber actual_sequence;
      ASSERT_OK(blob_db_impl->TEST_GetSequenceNumber(key, &actual_sequence));
      ASSERT_EQ(sequence, actual_sequence);
    }
    ASSERT_EQ(sequence, blob_db_->GetLatestSequenceNumber());
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

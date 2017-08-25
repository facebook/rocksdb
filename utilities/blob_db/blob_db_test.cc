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
#include "util/sync_point.h"
#include "util/testharness.h"
#include "utilities/blob_db/blob_db_impl.h"

namespace rocksdb {
namespace blob_db {

class BlobDBTest : public testing::Test {
 public:
  const int kMaxBlobSize = 1 << 14;

  class MockEnv : public EnvWrapper {
   public:
    MockEnv() : EnvWrapper(Env::Default()) {}

    void set_now_micros(uint64_t now_micros) { now_micros_ = now_micros; }

    uint64_t NowMicros() override { return now_micros_; }

   private:
    uint64_t now_micros_ = 0;
  };

  BlobDBTest()
      : dbname_(test::TmpDir() + "/blob_db_test"),
        mock_env_(new MockEnv()),
        blob_db_(nullptr) {
    Status s = DestroyBlobDB(dbname_, Options(), BlobDBOptions());
    assert(s.ok());
  }

  ~BlobDBTest() { Destroy(); }

  void Open(BlobDBOptions bdb_options = BlobDBOptions(),
            Options options = Options()) {
    options.create_if_missing = true;
    ASSERT_OK(BlobDB::Open(options, bdb_options, dbname_, &blob_db_));
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

  void PutRandomWithTTL(const std::string &key, uint64_t ttl, Random *rnd,
                        std::map<std::string, std::string> *data = nullptr) {
    int len = rnd->Next() % kMaxBlobSize + 1;
    std::string value = test::RandomHumanReadableString(rnd, len);
    ASSERT_OK(
        blob_db_->PutWithTTL(WriteOptions(), Slice(key), Slice(value), ttl));
    if (data != nullptr) {
      (*data)[key] = value;
    }
  }

  void PutRandomUntil(const std::string &key, uint64_t expiration, Random *rnd,
                      std::map<std::string, std::string> *data = nullptr) {
    int len = rnd->Next() % kMaxBlobSize + 1;
    std::string value = test::RandomHumanReadableString(rnd, len);
    ASSERT_OK(blob_db_->PutUntil(WriteOptions(), Slice(key), Slice(value),
                                 expiration));
    if (data != nullptr) {
      (*data)[key] = value;
    }
  }

  void PutRandom(const std::string &key, Random *rnd,
                 std::map<std::string, std::string> *data = nullptr) {
    int len = rnd->Next() % kMaxBlobSize + 1;
    std::string value = test::RandomHumanReadableString(rnd, len);
    ASSERT_OK(blob_db_->Put(WriteOptions(), Slice(key), Slice(value)));
    if (data != nullptr) {
      (*data)[key] = value;
    }
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
      uint64_t ttl = rnd.Next() % 86400;
      PutRandomWithTTL("key" + ToString(i % 500), ttl, &rnd, nullptr);
    }

    for (size_t i = 0; i < 10; i++) {
      Delete("key" + ToString(i % 500));
    }
  }

  const std::string dbname_;
  std::unique_ptr<MockEnv> mock_env_;
  std::shared_ptr<TTLExtractor> ttl_extractor_;
  BlobDB *blob_db_;
};  // class BlobDBTest

TEST_F(BlobDBTest, Put) {
  Random rnd(301);
  BlobDBOptions bdb_options;
  bdb_options.disable_background_tasks = true;
  Open(bdb_options);
  std::map<std::string, std::string> data;
  for (size_t i = 0; i < 100; i++) {
    PutRandom("key" + ToString(i), &rnd, &data);
  }
  VerifyDB(data);
}

TEST_F(BlobDBTest, PutWithTTL) {
  Random rnd(301);
  Options options;
  options.env = mock_env_.get();
  BlobDBOptions bdb_options;
  bdb_options.ttl_range_secs = 1000;
  bdb_options.blob_file_size = 256 * 1000 * 1000;
  bdb_options.disable_background_tasks = true;
  Open(bdb_options, options);
  std::map<std::string, std::string> data;
  mock_env_->set_now_micros(50 * 1000000);
  for (size_t i = 0; i < 100; i++) {
    uint64_t ttl = rnd.Next() % 100;
    PutRandomWithTTL("key" + ToString(i), ttl, &rnd,
                     (ttl <= 50 ? nullptr : &data));
  }
  mock_env_->set_now_micros(100 * 1000000);
  auto *bdb_impl = static_cast<BlobDBImpl *>(blob_db_);
  auto blob_files = bdb_impl->TEST_GetBlobFiles();
  ASSERT_EQ(1, blob_files.size());
  ASSERT_TRUE(blob_files[0]->HasTTL());
  ASSERT_OK(bdb_impl->TEST_CloseBlobFile(blob_files[0]));
  GCStats gc_stats;
  ASSERT_OK(bdb_impl->TEST_GCFileAndUpdateLSM(blob_files[0], &gc_stats));
  ASSERT_EQ(100 - data.size(), gc_stats.num_deletes);
  ASSERT_EQ(data.size(), gc_stats.num_relocate);
  VerifyDB(data);
}

TEST_F(BlobDBTest, PutUntil) {
  Random rnd(301);
  Options options;
  options.env = mock_env_.get();
  BlobDBOptions bdb_options;
  bdb_options.ttl_range_secs = 1000;
  bdb_options.blob_file_size = 256 * 1000 * 1000;
  bdb_options.disable_background_tasks = true;
  Open(bdb_options, options);
  std::map<std::string, std::string> data;
  mock_env_->set_now_micros(50 * 1000000);
  for (size_t i = 0; i < 100; i++) {
    uint64_t expiration = rnd.Next() % 100 + 50;
    PutRandomUntil("key" + ToString(i), expiration, &rnd,
                   (expiration <= 100 ? nullptr : &data));
  }
  mock_env_->set_now_micros(100 * 1000000);
  auto *bdb_impl = static_cast<BlobDBImpl *>(blob_db_);
  auto blob_files = bdb_impl->TEST_GetBlobFiles();
  ASSERT_EQ(1, blob_files.size());
  ASSERT_TRUE(blob_files[0]->HasTTL());
  ASSERT_OK(bdb_impl->TEST_CloseBlobFile(blob_files[0]));
  GCStats gc_stats;
  ASSERT_OK(bdb_impl->TEST_GCFileAndUpdateLSM(blob_files[0], &gc_stats));
  ASSERT_EQ(100 - data.size(), gc_stats.num_deletes);
  ASSERT_EQ(data.size(), gc_stats.num_relocate);
  VerifyDB(data);
}

TEST_F(BlobDBTest, TTLExtrator_NoTTL) {
  // The default ttl extractor return no ttl for every key.
  ttl_extractor_.reset(new TTLExtractor());
  Random rnd(301);
  Options options;
  options.env = mock_env_.get();
  BlobDBOptions bdb_options;
  bdb_options.ttl_range_secs = 1000;
  bdb_options.blob_file_size = 256 * 1000 * 1000;
  bdb_options.num_concurrent_simple_blobs = 1;
  bdb_options.ttl_extractor = ttl_extractor_;
  bdb_options.disable_background_tasks = true;
  Open(bdb_options, options);
  std::map<std::string, std::string> data;
  mock_env_->set_now_micros(0);
  for (size_t i = 0; i < 100; i++) {
    PutRandom("key" + ToString(i), &rnd, &data);
  }
  // very far in the future..
  mock_env_->set_now_micros(std::numeric_limits<uint64_t>::max() - 10);
  auto *bdb_impl = static_cast<BlobDBImpl *>(blob_db_);
  auto blob_files = bdb_impl->TEST_GetBlobFiles();
  ASSERT_EQ(1, blob_files.size());
  ASSERT_FALSE(blob_files[0]->HasTTL());
  ASSERT_OK(bdb_impl->TEST_CloseBlobFile(blob_files[0]));
  GCStats gc_stats;
  ASSERT_OK(bdb_impl->TEST_GCFileAndUpdateLSM(blob_files[0], &gc_stats));
  ASSERT_EQ(0, gc_stats.num_deletes);
  ASSERT_EQ(100, gc_stats.num_relocate);
  VerifyDB(data);
}

TEST_F(BlobDBTest, TTLExtractor_ExtractTTL) {
  Random rnd(301);
  class TestTTLExtractor : public TTLExtractor {
   public:
    explicit TestTTLExtractor(Random *r) : rnd(r) {}

    virtual bool ExtractTTL(const Slice &key, const Slice &value, uint64_t *ttl,
                            std::string * /*new_value*/,
                            bool * /*value_changed*/) override {
      *ttl = rnd->Next() % 100;
      if (*ttl > 50) {
        data[key.ToString()] = value.ToString();
      }
      return true;
    }

    Random *rnd;
    std::map<std::string, std::string> data;
  };
  ttl_extractor_.reset(new TestTTLExtractor(&rnd));
  Options options;
  options.env = mock_env_.get();
  BlobDBOptions bdb_options;
  bdb_options.ttl_range_secs = 1000;
  bdb_options.blob_file_size = 256 * 1000 * 1000;
  bdb_options.ttl_extractor = ttl_extractor_;
  bdb_options.disable_background_tasks = true;
  Open(bdb_options, options);
  mock_env_->set_now_micros(50 * 1000000);
  for (size_t i = 0; i < 100; i++) {
    PutRandom("key" + ToString(i), &rnd);
  }
  mock_env_->set_now_micros(100 * 1000000);
  auto *bdb_impl = static_cast<BlobDBImpl *>(blob_db_);
  auto blob_files = bdb_impl->TEST_GetBlobFiles();
  ASSERT_EQ(1, blob_files.size());
  ASSERT_TRUE(blob_files[0]->HasTTL());
  ASSERT_OK(bdb_impl->TEST_CloseBlobFile(blob_files[0]));
  GCStats gc_stats;
  ASSERT_OK(bdb_impl->TEST_GCFileAndUpdateLSM(blob_files[0], &gc_stats));
  auto &data = static_cast<TestTTLExtractor *>(ttl_extractor_.get())->data;
  ASSERT_EQ(100 - data.size(), gc_stats.num_deletes);
  ASSERT_EQ(data.size(), gc_stats.num_relocate);
  VerifyDB(data);
}

TEST_F(BlobDBTest, TTLExtractor_ExtractExpiration) {
  Random rnd(301);
  class TestTTLExtractor : public TTLExtractor {
   public:
    explicit TestTTLExtractor(Random *r) : rnd(r) {}

    virtual bool ExtractExpiration(const Slice &key, const Slice &value,
                                   uint64_t /*now*/, uint64_t *expiration,
                                   std::string * /*new_value*/,
                                   bool * /*value_changed*/) override {
      *expiration = rnd->Next() % 100 + 50;
      if (*expiration > 100) {
        data[key.ToString()] = value.ToString();
      }
      return true;
    }

    Random *rnd;
    std::map<std::string, std::string> data;
  };
  ttl_extractor_.reset(new TestTTLExtractor(&rnd));
  Options options;
  options.env = mock_env_.get();
  BlobDBOptions bdb_options;
  bdb_options.ttl_range_secs = 1000;
  bdb_options.blob_file_size = 256 * 1000 * 1000;
  bdb_options.ttl_extractor = ttl_extractor_;
  bdb_options.disable_background_tasks = true;
  Open(bdb_options, options);
  mock_env_->set_now_micros(50 * 1000000);
  for (size_t i = 0; i < 100; i++) {
    PutRandom("key" + ToString(i), &rnd);
  }
  mock_env_->set_now_micros(100 * 1000000);
  auto *bdb_impl = static_cast<BlobDBImpl *>(blob_db_);
  auto blob_files = bdb_impl->TEST_GetBlobFiles();
  ASSERT_EQ(1, blob_files.size());
  ASSERT_TRUE(blob_files[0]->HasTTL());
  ASSERT_OK(bdb_impl->TEST_CloseBlobFile(blob_files[0]));
  GCStats gc_stats;
  ASSERT_OK(bdb_impl->TEST_GCFileAndUpdateLSM(blob_files[0], &gc_stats));
  auto &data = static_cast<TestTTLExtractor *>(ttl_extractor_.get())->data;
  ASSERT_EQ(100 - data.size(), gc_stats.num_deletes);
  ASSERT_EQ(data.size(), gc_stats.num_relocate);
  VerifyDB(data);
}

TEST_F(BlobDBTest, TTLExtractor_ChangeValue) {
  class TestTTLExtractor : public TTLExtractor {
   public:
    const Slice kTTLSuffix = Slice("ttl:");

    bool ExtractTTL(const Slice & /*key*/, const Slice &value, uint64_t *ttl,
                    std::string *new_value, bool *value_changed) override {
      if (value.size() < 12) {
        return false;
      }
      const char *p = value.data() + value.size() - 12;
      if (kTTLSuffix != Slice(p, 4)) {
        return false;
      }
      *ttl = DecodeFixed64(p + 4);
      *new_value = Slice(value.data(), value.size() - 12).ToString();
      *value_changed = true;
      return true;
    }
  };
  Random rnd(301);
  Options options;
  options.env = mock_env_.get();
  BlobDBOptions bdb_options;
  bdb_options.ttl_range_secs = 1000;
  bdb_options.blob_file_size = 256 * 1000 * 1000;
  bdb_options.ttl_extractor = std::make_shared<TestTTLExtractor>();
  bdb_options.disable_background_tasks = true;
  Open(bdb_options, options);
  std::map<std::string, std::string> data;
  mock_env_->set_now_micros(50 * 1000000);
  for (size_t i = 0; i < 100; i++) {
    int len = rnd.Next() % kMaxBlobSize + 1;
    std::string key = "key" + ToString(i);
    std::string value = test::RandomHumanReadableString(&rnd, len);
    uint64_t ttl = rnd.Next() % 100;
    std::string value_ttl = value + "ttl:";
    PutFixed64(&value_ttl, ttl);
    ASSERT_OK(blob_db_->Put(WriteOptions(), Slice(key), Slice(value_ttl)));
    if (ttl > 50) {
      data[key] = value;
    }
  }
  mock_env_->set_now_micros(100 * 1000000);
  auto *bdb_impl = static_cast<BlobDBImpl *>(blob_db_);
  auto blob_files = bdb_impl->TEST_GetBlobFiles();
  ASSERT_EQ(1, blob_files.size());
  ASSERT_TRUE(blob_files[0]->HasTTL());
  ASSERT_OK(bdb_impl->TEST_CloseBlobFile(blob_files[0]));
  GCStats gc_stats;
  ASSERT_OK(bdb_impl->TEST_GCFileAndUpdateLSM(blob_files[0], &gc_stats));
  ASSERT_EQ(100 - data.size(), gc_stats.num_deletes);
  ASSERT_EQ(data.size(), gc_stats.num_relocate);
  VerifyDB(data);
}

TEST_F(BlobDBTest, StackableDBGet) {
  Random rnd(301);
  BlobDBOptions bdb_options;
  bdb_options.disable_background_tasks = true;
  Open(bdb_options);
  std::map<std::string, std::string> data;
  for (size_t i = 0; i < 100; i++) {
    PutRandom("key" + ToString(i), &rnd, &data);
  }
  for (size_t i = 0; i < 100; i++) {
    StackableDB *db = blob_db_;
    ColumnFamilyHandle *column_family = db->DefaultColumnFamily();
    std::string key = "key" + ToString(i);
    PinnableSlice pinnable_value;
    ASSERT_OK(db->Get(ReadOptions(), column_family, key, &pinnable_value));
    std::string string_value;
    ASSERT_OK(db->Get(ReadOptions(), column_family, key, &string_value));
    ASSERT_EQ(string_value, pinnable_value.ToString());
    ASSERT_EQ(string_value, data[key]);
  }
}

TEST_F(BlobDBTest, WriteBatch) {
  Random rnd(301);
  BlobDBOptions bdb_options;
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
  BlobDBOptions bdb_options;
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
  BlobDBOptions bdb_options;
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
  BlobDBOptions bdb_options;
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
  BlobDBOptions bdb_options;
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

TEST_F(BlobDBTest, MultipleWriters) {
  Open(BlobDBOptions());

  std::vector<port::Thread> workers;
  std::vector<std::map<std::string, std::string>> data_set(10);
  for (uint32_t i = 0; i < 10; i++)
    workers.push_back(port::Thread(
        [&](uint32_t id) {
          Random rnd(301 + id);
          for (int j = 0; j < 100; j++) {
            std::string key = "key" + ToString(id) + "_" + ToString(j);
            if (id < 5) {
              PutRandom(key, &rnd, &data_set[id]);
            } else {
              WriteBatch batch;
              PutRandomToWriteBatch(key, &rnd, &batch, &data_set[id]);
              blob_db_->Write(WriteOptions(), &batch);
            }
          }
        },
        i));
  std::map<std::string, std::string> data;
  for (size_t i = 0; i < 10; i++) {
    workers[i].join();
    data.insert(data_set[i].begin(), data_set[i].end());
  }
  VerifyDB(data);
}

// Test sequence number store in blob file is correct.
TEST_F(BlobDBTest, SequenceNumber) {
  Random rnd(301);
  BlobDBOptions bdb_options;
  bdb_options.disable_background_tasks = true;
  Open(bdb_options);
  SequenceNumber sequence = blob_db_->GetLatestSequenceNumber();
  BlobDBImpl *blob_db_impl = dynamic_cast<BlobDBImpl*>(blob_db_);
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

TEST_F(BlobDBTest, GCAfterOverwriteKeys) {
  Random rnd(301);
  BlobDBOptions bdb_options;
  bdb_options.disable_background_tasks = true;
  Open(bdb_options);
  BlobDBImpl *blob_db_impl = dynamic_cast<BlobDBImpl*>(blob_db_);
  DBImpl *db_impl = dynamic_cast<DBImpl*>(blob_db_->GetBaseDB());
  std::map<std::string, std::string> data;
  for (int i = 0; i < 200; i++) {
    PutRandom("key" + ToString(i), &rnd, &data);
  }
  auto blob_files = blob_db_impl->TEST_GetBlobFiles();
  ASSERT_EQ(1, blob_files.size());
  ASSERT_OK(blob_db_impl->TEST_CloseBlobFile(blob_files[0]));
  // Test for data in SST
  size_t new_keys = 0;
  for (int i = 0; i < 100; i++) {
    if (rnd.Next() % 2 == 1) {
      new_keys++;
      PutRandom("key" + ToString(i), &rnd, &data);
    }
  }
  db_impl->TEST_FlushMemTable(true /*wait*/);
  // Test for data in memtable
  for (int i = 100; i < 200; i++) {
    if (rnd.Next() % 2 == 1) {
      new_keys++;
      PutRandom("key" + ToString(i), &rnd, &data);
    }
  }
  GCStats gc_stats;
  ASSERT_OK(blob_db_impl->TEST_GCFileAndUpdateLSM(blob_files[0], &gc_stats));
  ASSERT_EQ(200, gc_stats.blob_count);
  ASSERT_EQ(0, gc_stats.num_deletes);
  ASSERT_EQ(200 - new_keys, gc_stats.num_relocate);
  VerifyDB(data);
}

TEST_F(BlobDBTest, GCRelocateKeyWhileOverwritting) {
  Random rnd(301);
  BlobDBOptions bdb_options;
  bdb_options.disable_background_tasks = true;
  Open(bdb_options);
  ASSERT_OK(blob_db_->Put(WriteOptions(), "foo", "v1"));
  BlobDBImpl *blob_db_impl = dynamic_cast<BlobDBImpl*>(blob_db_);
  auto blob_files = blob_db_impl->TEST_GetBlobFiles();
  ASSERT_EQ(1, blob_files.size());
  ASSERT_OK(blob_db_impl->TEST_CloseBlobFile(blob_files[0]));

  SyncPoint::GetInstance()->LoadDependency(
      {{"BlobDBImpl::GCFileAndUpdateLSM:AfterGetForUpdate",
        "BlobDBImpl::PutUntil:Start"},
       {"BlobDBImpl::PutUntil:Finish",
        "BlobDBImpl::GCFileAndUpdateLSM:BeforeRelocate"}});
  SyncPoint::GetInstance()->EnableProcessing();

  auto writer = port::Thread(
      [this]() { ASSERT_OK(blob_db_->Put(WriteOptions(), "foo", "v2")); });

  GCStats gc_stats;
  ASSERT_OK(blob_db_impl->TEST_GCFileAndUpdateLSM(blob_files[0], &gc_stats));
  ASSERT_EQ(1, gc_stats.blob_count);
  ASSERT_EQ(0, gc_stats.num_deletes);
  ASSERT_EQ(1, gc_stats.num_relocate);
  ASSERT_EQ(0, gc_stats.relocate_succeeded);
  ASSERT_EQ(1, gc_stats.overwritten_while_relocate);
  writer.join();
  VerifyDB({{"foo", "v2"}});
}

TEST_F(BlobDBTest, GCExpiredKeyWhileOverwritting) {
  Random rnd(301);
  Options options;
  options.env = mock_env_.get();
  BlobDBOptions bdb_options;
  bdb_options.disable_background_tasks = true;
  Open(bdb_options, options);
  mock_env_->set_now_micros(100 * 1000000);
  ASSERT_OK(blob_db_->PutUntil(WriteOptions(), "foo", "v1", 200));
  BlobDBImpl *blob_db_impl = dynamic_cast<BlobDBImpl*>(blob_db_);
  auto blob_files = blob_db_impl->TEST_GetBlobFiles();
  ASSERT_EQ(1, blob_files.size());
  ASSERT_OK(blob_db_impl->TEST_CloseBlobFile(blob_files[0]));
  mock_env_->set_now_micros(300 * 1000000);

  SyncPoint::GetInstance()->LoadDependency(
      {{"BlobDBImpl::GCFileAndUpdateLSM:AfterGetForUpdate",
        "BlobDBImpl::PutUntil:Start"},
       {"BlobDBImpl::PutUntil:Finish",
        "BlobDBImpl::GCFileAndUpdateLSM:BeforeDelete"}});
  SyncPoint::GetInstance()->EnableProcessing();

  auto writer = port::Thread([this]() {
    ASSERT_OK(blob_db_->PutUntil(WriteOptions(), "foo", "v2", 400));
  });

  GCStats gc_stats;
  ASSERT_OK(blob_db_impl->TEST_GCFileAndUpdateLSM(blob_files[0], &gc_stats));
  ASSERT_EQ(1, gc_stats.blob_count);
  ASSERT_EQ(1, gc_stats.num_deletes);
  ASSERT_EQ(0, gc_stats.delete_succeeded);
  ASSERT_EQ(1, gc_stats.overwritten_while_delete);
  ASSERT_EQ(0, gc_stats.num_relocate);
  writer.join();
  VerifyDB({{"foo", "v2"}});
}

TEST_F(BlobDBTest, GCOldestSimpleBlobFileWhenOutOfSpace) {
  // Use mock env to stop wall clock.
  Options options;
  options.env = mock_env_.get();
  BlobDBOptions bdb_options;
  bdb_options.blob_dir_size = 100;
  bdb_options.blob_file_size = 100;
  bdb_options.disable_background_tasks = true;
  Open(bdb_options);
  std::string value(100, 'v');
  ASSERT_OK(blob_db_->PutWithTTL(WriteOptions(), "key_with_ttl", value, 60));
  for (int i = 0; i < 10; i++) {
    ASSERT_OK(blob_db_->Put(WriteOptions(), "key" + ToString(i), value));
  }
  BlobDBImpl *blob_db_impl = dynamic_cast<BlobDBImpl*>(blob_db_);
  auto blob_files = blob_db_impl->TEST_GetBlobFiles();
  ASSERT_EQ(11, blob_files.size());
  ASSERT_TRUE(blob_files[0]->HasTTL());
  ASSERT_TRUE(blob_files[0]->Immutable());
  for (int i = 1; i <= 10; i++) {
    ASSERT_FALSE(blob_files[i]->HasTTL());
    if (i < 10) {
      ASSERT_TRUE(blob_files[i]->Immutable());
    }
  }
  blob_db_impl->TEST_RunGC();
  // The oldest simple blob file (i.e. blob_files[1]) has been selected for GC.
  auto obsolete_files = blob_db_impl->TEST_GetObsoleteFiles();
  ASSERT_EQ(1, obsolete_files.size());
  ASSERT_EQ(blob_files[1]->BlobFileNumber(),
            obsolete_files[0]->BlobFileNumber());
}

TEST_F(BlobDBTest, ReadWhileGC) {
  // run the same test for Get(), MultiGet() and Iterator each.
  for (int i = 0; i < 3; i++) {
    BlobDBOptions bdb_options;
    bdb_options.disable_background_tasks = true;
    Open(bdb_options);
    blob_db_->Put(WriteOptions(), "foo", "bar");
    BlobDBImpl *blob_db_impl = dynamic_cast<BlobDBImpl*>(blob_db_);
    auto blob_files = blob_db_impl->TEST_GetBlobFiles();
    ASSERT_EQ(1, blob_files.size());
    std::shared_ptr<BlobFile> bfile = blob_files[0];
    uint64_t bfile_number = bfile->BlobFileNumber();
    ASSERT_OK(blob_db_impl->TEST_CloseBlobFile(bfile));

    switch (i) {
      case 0:
        SyncPoint::GetInstance()->LoadDependency(
            {{"BlobDBImpl::Get:AfterIndexEntryGet:1",
              "BlobDBTest::ReadWhileGC:1"},
             {"BlobDBTest::ReadWhileGC:2",
              "BlobDBImpl::Get:AfterIndexEntryGet:2"}});
        break;
      case 1:
        SyncPoint::GetInstance()->LoadDependency(
            {{"BlobDBImpl::MultiGet:AfterIndexEntryGet:1",
              "BlobDBTest::ReadWhileGC:1"},
             {"BlobDBTest::ReadWhileGC:2",
              "BlobDBImpl::MultiGet:AfterIndexEntryGet:2"}});
        break;
      case 2:
        SyncPoint::GetInstance()->LoadDependency(
            {{"BlobDBIterator::value:BeforeGetBlob:1",
              "BlobDBTest::ReadWhileGC:1"},
             {"BlobDBTest::ReadWhileGC:2",
              "BlobDBIterator::value:BeforeGetBlob:2"}});
        break;
    }
    SyncPoint::GetInstance()->EnableProcessing();

    auto reader = port::Thread([this, i]() {
      std::string value;
      std::vector<std::string> values;
      std::vector<Status> statuses;
      switch (i) {
        case 0:
          ASSERT_OK(blob_db_->Get(ReadOptions(), "foo", &value));
          ASSERT_EQ("bar", value);
          break;
        case 1:
          statuses = blob_db_->MultiGet(ReadOptions(), {"foo"}, &values);
          ASSERT_EQ(1, statuses.size());
          ASSERT_EQ(1, values.size());
          ASSERT_EQ("bar", values[0]);
          break;
        case 2:
          // VerifyDB use iterator to scan the DB.
          VerifyDB({{"foo", "bar"}});
          break;
      }
    });

    TEST_SYNC_POINT("BlobDBTest::ReadWhileGC:1");
    GCStats gc_stats;
    ASSERT_OK(blob_db_impl->TEST_GCFileAndUpdateLSM(bfile, &gc_stats));
    ASSERT_EQ(1, gc_stats.blob_count);
    ASSERT_EQ(1, gc_stats.num_relocate);
    ASSERT_EQ(1, gc_stats.relocate_succeeded);
    blob_db_impl->TEST_ObsoleteFile(blob_files[0]);
    blob_db_impl->TEST_DeleteObsoleteFiles();
    // The file shouln't be deleted
    blob_files = blob_db_impl->TEST_GetBlobFiles();
    ASSERT_EQ(2, blob_files.size());
    ASSERT_EQ(bfile_number, blob_files[0]->BlobFileNumber());
    auto obsolete_files = blob_db_impl->TEST_GetObsoleteFiles();
    ASSERT_EQ(1, obsolete_files.size());
    ASSERT_EQ(bfile_number, obsolete_files[0]->BlobFileNumber());
    TEST_SYNC_POINT("BlobDBTest::ReadWhileGC:2");
    reader.join();
    SyncPoint::GetInstance()->DisableProcessing();

    // The file is deleted this time
    blob_db_impl->TEST_DeleteObsoleteFiles();
    blob_files = blob_db_impl->TEST_GetBlobFiles();
    ASSERT_EQ(1, blob_files.size());
    ASSERT_NE(bfile_number, blob_files[0]->BlobFileNumber());
    ASSERT_EQ(0, blob_db_impl->TEST_GetObsoleteFiles().size());
    VerifyDB({{"foo", "bar"}});
    Destroy();
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

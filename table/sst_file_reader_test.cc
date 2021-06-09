// Copyright (c) 2011-present, Facebook, Inc. All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#ifndef ROCKSDB_LITE

#include "rocksdb/sst_file_reader.h"

#include <cinttypes>

#include "port/stack_trace.h"
#include "rocksdb/convenience.h"
#include "rocksdb/db.h"
#include "rocksdb/sst_file_writer.h"
#include "table/sst_file_writer_collectors.h"
#include "test_util/testharness.h"
#include "test_util/testutil.h"
#include "utilities/merge_operators.h"

namespace ROCKSDB_NAMESPACE {

std::string EncodeAsString(uint64_t v) {
  char buf[16];
  snprintf(buf, sizeof(buf), "%08" PRIu64, v);
  return std::string(buf);
}

std::string EncodeAsUint64(uint64_t v) {
  std::string dst;
  PutFixed64(&dst, v);
  return dst;
}

class SstFileReaderTest : public testing::Test {
 public:
  SstFileReaderTest() {
    options_.merge_operator = MergeOperators::CreateUInt64AddOperator();
    sst_name_ = test::PerThreadDBPath("sst_file");

    Env* base_env = Env::Default();
    EXPECT_OK(
        test::CreateEnvFromSystem(ConfigOptions(), &base_env, &env_guard_));
    EXPECT_NE(nullptr, base_env);
    env_ = base_env;
    options_.env = env_;
  }

  ~SstFileReaderTest() {
    Status s = env_->DeleteFile(sst_name_);
    EXPECT_OK(s);
  }

  void CreateFile(const std::string& file_name,
                  const std::vector<std::string>& keys) {
    SstFileWriter writer(soptions_, options_);
    ASSERT_OK(writer.Open(file_name));
    for (size_t i = 0; i + 2 < keys.size(); i += 3) {
      ASSERT_OK(writer.Put(keys[i], keys[i]));
      ASSERT_OK(writer.Merge(keys[i + 1], EncodeAsUint64(i + 1)));
      ASSERT_OK(writer.Delete(keys[i + 2]));
    }
    ASSERT_OK(writer.Finish());
  }

  void CheckFile(const std::string& file_name,
                 const std::vector<std::string>& keys,
                 bool check_global_seqno = false) {
    ReadOptions ropts;
    SstFileReader reader(options_);
    ASSERT_OK(reader.Open(file_name));
    ASSERT_OK(reader.VerifyChecksum());
    std::unique_ptr<Iterator> iter(reader.NewIterator(ropts));
    iter->SeekToFirst();
    for (size_t i = 0; i + 2 < keys.size(); i += 3) {
      ASSERT_TRUE(iter->Valid());
      ASSERT_EQ(iter->key().compare(keys[i]), 0);
      ASSERT_EQ(iter->value().compare(keys[i]), 0);
      iter->Next();
      ASSERT_TRUE(iter->Valid());
      ASSERT_EQ(iter->key().compare(keys[i + 1]), 0);
      ASSERT_EQ(iter->value().compare(EncodeAsUint64(i + 1)), 0);
      iter->Next();
    }
    ASSERT_FALSE(iter->Valid());
    if (check_global_seqno) {
      auto properties = reader.GetTableProperties();
      ASSERT_TRUE(properties);
      std::string hostname;
      ASSERT_OK(env_->GetHostNameString(&hostname));
      ASSERT_EQ(properties->db_host_id, hostname);
      auto& user_properties = properties->user_collected_properties;
      ASSERT_TRUE(
          user_properties.count(ExternalSstFilePropertyNames::kGlobalSeqno));
    }
  }

  void CreateFileAndCheck(const std::vector<std::string>& keys) {
    CreateFile(sst_name_, keys);
    CheckFile(sst_name_, keys);
  }

 protected:
  Options options_;
  EnvOptions soptions_;
  std::string sst_name_;
  std::shared_ptr<Env> env_guard_;
  Env* env_;
};

const uint64_t kNumKeys = 100;

TEST_F(SstFileReaderTest, Basic) {
  std::vector<std::string> keys;
  for (uint64_t i = 0; i < kNumKeys; i++) {
    keys.emplace_back(EncodeAsString(i));
  }
  CreateFileAndCheck(keys);
}

TEST_F(SstFileReaderTest, Uint64Comparator) {
  options_.comparator = test::Uint64Comparator();
  std::vector<std::string> keys;
  for (uint64_t i = 0; i < kNumKeys; i++) {
    keys.emplace_back(EncodeAsUint64(i));
  }
  CreateFileAndCheck(keys);
}

TEST_F(SstFileReaderTest, ReadOptionsOutOfScope) {
  // Repro a bug where the SstFileReader depended on its configured ReadOptions
  // outliving it.
  options_.comparator = test::Uint64Comparator();
  std::vector<std::string> keys;
  for (uint64_t i = 0; i < kNumKeys; i++) {
    keys.emplace_back(EncodeAsUint64(i));
  }
  CreateFile(sst_name_, keys);

  SstFileReader reader(options_);
  ASSERT_OK(reader.Open(sst_name_));
  std::unique_ptr<Iterator> iter;
  {
    // Make sure ReadOptions go out of scope ASAP so we know the iterator
    // operations do not depend on it.
    ReadOptions ropts;
    iter.reset(reader.NewIterator(ropts));
  }
  iter->SeekToFirst();
  while (iter->Valid()) {
    iter->Next();
  }
}

TEST_F(SstFileReaderTest, ReadFileWithGlobalSeqno) {
  std::vector<std::string> keys;
  for (uint64_t i = 0; i < kNumKeys; i++) {
    keys.emplace_back(EncodeAsString(i));
  }
  // Generate a SST file.
  CreateFile(sst_name_, keys);

  // Ingest the file into a db, to assign it a global sequence number.
  Options options;
  options.create_if_missing = true;
  std::string db_name = test::PerThreadDBPath("test_db");
  DB* db;
  ASSERT_OK(DB::Open(options, db_name, &db));
  // Bump sequence number.
  ASSERT_OK(db->Put(WriteOptions(), keys[0], "foo"));
  ASSERT_OK(db->Flush(FlushOptions()));
  // Ingest the file.
  IngestExternalFileOptions ingest_options;
  ingest_options.write_global_seqno = true;
  ASSERT_OK(db->IngestExternalFile({sst_name_}, ingest_options));
  std::vector<std::string> live_files;
  uint64_t manifest_file_size = 0;
  ASSERT_OK(db->GetLiveFiles(live_files, &manifest_file_size));
  // Get the ingested file.
  std::string ingested_file;
  for (auto& live_file : live_files) {
    if (live_file.substr(live_file.size() - 4, std::string::npos) == ".sst") {
      if (ingested_file.empty() || ingested_file < live_file) {
        ingested_file = live_file;
      }
    }
  }
  ASSERT_FALSE(ingested_file.empty());
  delete db;

  // Verify the file can be open and read by SstFileReader.
  CheckFile(db_name + ingested_file, keys, true /* check_global_seqno */);

  // Cleanup.
  ASSERT_OK(DestroyDB(db_name, options));
}

}  // namespace ROCKSDB_NAMESPACE

#ifdef ROCKSDB_UNITTESTS_WITH_CUSTOM_OBJECTS_FROM_STATIC_LIBS
extern "C" {
  void RegisterCustomObjects(int argc, char** argv);
}
#else
void RegisterCustomObjects(int /*argc*/, char** /*argv*/) {}
#endif  // !ROCKSDB_UNITTESTS_WITH_CUSTOM_OBJECTS_FROM_STATIC_LIBS

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  RegisterCustomObjects(argc, argv);
  return RUN_ALL_TESTS();
}

#else
#include <stdio.h>

int main(int /*argc*/, char** /*argv*/) {
  fprintf(stderr,
          "SKIPPED as SstFileReader is not supported in ROCKSDB_LITE\n");
  return 0;
}

#endif  // ROCKSDB_LITE

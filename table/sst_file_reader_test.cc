// Copyright (c) 2011-present, Facebook, Inc. All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#ifndef ROCKSDB_LITE

#include "rocksdb/sst_file_reader.h"
#include "rocksdb/sst_file_writer.h"
#include "util/testharness.h"
#include "util/testutil.h"

namespace rocksdb {

class SstFileReaderTest : public testing::Test {
 public:
  SstFileReaderTest() {
    options_.comparator = test::Uint64Comparator();
    sst_name_ = test::PerThreadDBPath("sst_file");
  }

  std::string EncodeUint64(uint64_t v) {
    return std::string((char*) &v, sizeof(v));
  }

  void CreateFileAndCheck() {
    std::vector<std::string> keys;
    const uint64_t kNumPuts = 100;
    for (uint64_t i = 0; i < kNumPuts; i++) {
      keys.emplace_back(EncodeUint64(i));
    }

    SstFileWriter writer(soptions_, options_);
    ASSERT_OK(writer.Open(sst_name_));
    for (auto& key : keys) {
      ASSERT_OK(writer.Put(key, key));
    }
    ASSERT_OK(writer.Finish());

    ReadOptions ropts;
    SstFileReader reader(options_);
    ASSERT_OK(reader.Open(sst_name_));
    ASSERT_OK(reader.VerifyChecksum());
    std::unique_ptr<Iterator> iter(reader.NewIterator(ropts));
    iter->SeekToFirst();
    for (auto& key : keys) {
      PinnableSlice value;
      ASSERT_OK(reader.Get(ropts, key, &value));
      ASSERT_EQ(value.compare(key), 0);
      ASSERT_TRUE(iter->Valid());
      ASSERT_EQ(iter->key().compare(key), 0);
      ASSERT_EQ(iter->value().compare(key), 0);
      iter->Next();
    }
    ASSERT_FALSE(iter->Valid());
  }

 private:
  Options options_;
  EnvOptions soptions_;
  std::string sst_name_;
};

TEST_F(SstFileReaderTest, Basic) {
  CreateFileAndCheck();
}

}  // namespace rocksdb

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

#endif  // ROCKSDB_LITE

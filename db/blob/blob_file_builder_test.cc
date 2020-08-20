//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/blob/blob_file_builder.h"

#include <cinttypes>
#include <string>
#include <vector>

#include "db/blob/blob_file_addition.h"
#include "db/blob/blob_log_format.h"
#include "db/blob/blob_log_reader.h"
#include "env/composite_env_wrapper.h"
#include "env/mock_env.h"
#include "file/filename.h"
#include "file/random_access_file_reader.h"
#include "options/cf_options.h"
#include "rocksdb/env.h"
#include "rocksdb/options.h"
#include "test_util/sync_point.h"
#include "test_util/testharness.h"

namespace ROCKSDB_NAMESPACE {

class BlobFileBuilderTest : public testing::Test {
 protected:
  class FileNumberGenerator {
   public:
    uint64_t operator()() { return ++next_file_number_; }

   private:
    uint64_t next_file_number_ = 1;
  };

  BlobFileBuilderTest() : env_(Env::Default()), fs_(&env_) {}

  MockEnv env_;
  LegacyFileSystemWrapper fs_;
  FileOptions file_options_;
};

TEST_F(BlobFileBuilderTest, Build) {
  // Build a single blob file
  Options options;
  options.cf_paths.emplace_back(
      test::PerThreadDBPath(&env_, "BlobFileBuilderTest_Build"), 0);
  options.enable_blob_files = true;

  ImmutableCFOptions immutable_cf_options(options);
  MutableCFOptions mutable_cf_options(options);

  constexpr uint32_t column_family_id = 123;
  constexpr Env::IOPriority io_priority = Env::IO_HIGH;
  constexpr Env::WriteLifeTimeHint write_hint = Env::WLTH_MEDIUM;

  std::vector<BlobFileAddition> blob_file_additions;

  BlobFileBuilder builder(FileNumberGenerator(), &env_, &fs_,
                          &immutable_cf_options, &mutable_cf_options,
                          &file_options_, column_family_id, io_priority,
                          write_hint, &blob_file_additions);

  constexpr int number_of_blobs = 10;
  constexpr uint64_t key_size = 1;
  constexpr uint64_t value_size = 4;
  constexpr int value_offset = 1234;

  for (int i = 0; i < number_of_blobs; ++i) {
    const std::string key = std::to_string(i);
    const std::string value = std::to_string(i + value_offset);

    std::string blob_index;
    ASSERT_OK(builder.Add(key, value, &blob_index));
  }

  ASSERT_OK(builder.Finish());

  // Check the metadata generated
  constexpr uint64_t blob_file_number = 2;

  ASSERT_EQ(blob_file_additions.size(), 1);
  ASSERT_EQ(blob_file_additions[0].GetBlobFileNumber(), blob_file_number);
  ASSERT_EQ(blob_file_additions[0].GetTotalBlobCount(), number_of_blobs);
  ASSERT_EQ(
      blob_file_additions[0].GetTotalBlobBytes(),
      number_of_blobs * (BlobLogRecord::kHeaderSize + key_size + value_size));

  // Check the contents of the new blob file
  const std::string blob_file_path = BlobFileName(
      immutable_cf_options.cf_paths.front().path, blob_file_number);

  std::unique_ptr<FSRandomAccessFile> file;
  constexpr IODebugContext* dbg = nullptr;
  ASSERT_OK(fs_.NewRandomAccessFile(blob_file_path, file_options_, &file, dbg));

  std::unique_ptr<RandomAccessFileReader> file_reader(
      new RandomAccessFileReader(std::move(file), blob_file_path, &env_));

  constexpr Statistics* statistics = nullptr;
  BlobLogReader blob_log_reader(std::move(file_reader), &env_, statistics);

  BlobLogHeader header;
  ASSERT_OK(blob_log_reader.ReadHeader(&header));
  ASSERT_EQ(header.version, kVersion1);
  ASSERT_EQ(header.column_family_id, column_family_id);
  ASSERT_EQ(header.compression, kNoCompression);
  ASSERT_FALSE(header.has_ttl);
  ASSERT_EQ(header.expiration_range, ExpirationRange());

  for (int i = 0; i < number_of_blobs; ++i) {
    BlobLogRecord record;
    uint64_t blob_offset = 0;

    ASSERT_OK(blob_log_reader.ReadRecord(
        &record, BlobLogReader::kReadHeaderKeyBlob, &blob_offset));
    ASSERT_EQ(record.expiration, 0);
    ASSERT_EQ(record.key, std::to_string(i));
    ASSERT_EQ(record.value, std::to_string(i + value_offset));
  }
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

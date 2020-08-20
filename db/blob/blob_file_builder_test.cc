//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/blob/blob_file_builder.h"

#include <cassert>
#include <cinttypes>
#include <string>
#include <utility>
#include <vector>

#include "db/blob/blob_file_addition.h"
#include "db/blob/blob_index.h"
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

  void VerifyBlobFile(const ImmutableCFOptions& immutable_cf_options,
                      uint64_t blob_file_number, uint32_t column_family_id,
                      const std::vector<std::pair<std::string, std::string>>&
                          expected_key_value_pairs,
                      const std::vector<std::string>& blob_indexes) {
    assert(expected_key_value_pairs.size() == blob_indexes.size());

    const std::string blob_file_path = BlobFileName(
        immutable_cf_options.cf_paths.front().path, blob_file_number);

    std::unique_ptr<FSRandomAccessFile> file;
    constexpr IODebugContext* dbg = nullptr;
    ASSERT_OK(
        fs_.NewRandomAccessFile(blob_file_path, file_options_, &file, dbg));

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

    for (size_t i = 0; i < expected_key_value_pairs.size(); ++i) {
      BlobLogRecord record;
      uint64_t blob_offset = 0;

      ASSERT_OK(blob_log_reader.ReadRecord(
          &record, BlobLogReader::kReadHeaderKeyBlob, &blob_offset));

      // Check the contents of the blob file
      const auto& expected_key_value = expected_key_value_pairs[i];
      const auto& key = expected_key_value.first;
      const auto& value = expected_key_value.second;

      ASSERT_EQ(record.key_size, key.size());
      ASSERT_EQ(record.value_size, value.size());
      ASSERT_EQ(record.expiration, 0);
      ASSERT_EQ(record.key, key);
      ASSERT_EQ(record.value, value);

      // Make sure the blob reference returned by the builder points to the
      // right place
      BlobIndex blob_index;
      ASSERT_OK(blob_index.DecodeFrom(blob_indexes[i]));
      ASSERT_FALSE(blob_index.IsInlined());
      ASSERT_FALSE(blob_index.HasTTL());
      ASSERT_EQ(blob_index.file_number(), blob_file_number);
      ASSERT_EQ(blob_index.offset(), blob_offset);
      ASSERT_EQ(blob_index.size(), value.size());
    }
  }

  MockEnv env_;
  LegacyFileSystemWrapper fs_;
  FileOptions file_options_;
};

TEST_F(BlobFileBuilderTest, BuildAndCheckOneFile) {
  // Build a single blob file
  constexpr size_t number_of_blobs = 10;
  constexpr size_t key_size = 1;
  constexpr size_t value_size = 4;
  constexpr size_t value_offset = 1234;

  Options options;
  options.cf_paths.emplace_back(
      test::PerThreadDBPath(&env_, "BlobFileBuilderTest_BuildAndCheckOneFile"),
      0);
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

  std::vector<std::pair<std::string, std::string>> expected_key_value_pairs(
      number_of_blobs);
  std::vector<std::string> blob_indexes(number_of_blobs);

  for (size_t i = 0; i < number_of_blobs; ++i) {
    auto& expected_key_value = expected_key_value_pairs[i];

    auto& key = expected_key_value.first;
    key = std::to_string(i);
    assert(key.size() == key_size);

    auto& value = expected_key_value.second;
    value = std::to_string(i + value_offset);
    assert(value.size() == value_size);

    ASSERT_OK(builder.Add(key, value, &blob_indexes[i]));
    ASSERT_FALSE(blob_indexes[i].empty());
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

  // Verify the contents of the new blob file as well as the blob references
  VerifyBlobFile(immutable_cf_options, blob_file_number, column_family_id,
                 expected_key_value_pairs, blob_indexes);
}

TEST_F(BlobFileBuilderTest, BuildAndCheckMultipleFiles) {
  // Build multiple blob files: file size limit is set to the size of a single
  // value, so each blob ends up in a file of its own
  constexpr size_t number_of_blobs = 10;
  constexpr size_t key_size = 1;
  constexpr size_t value_size = 10;
  constexpr size_t value_offset = 1234567890;

  Options options;
  options.cf_paths.emplace_back(
      test::PerThreadDBPath(&env_,
                            "BlobFileBuilderTest_BuildAndCheckMultipleFiles"),
      0);
  options.enable_blob_files = true;
  options.blob_file_size = value_size;

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

  std::vector<std::pair<std::string, std::string>> expected_key_value_pairs(
      number_of_blobs);
  std::vector<std::string> blob_indexes(number_of_blobs);

  for (size_t i = 0; i < number_of_blobs; ++i) {
    auto& expected_key_value = expected_key_value_pairs[i];

    auto& key = expected_key_value.first;
    key = std::to_string(i);
    assert(key.size() == key_size);

    auto& value = expected_key_value.second;
    value = std::to_string(i + value_offset);
    assert(value.size() == value_size);

    ASSERT_OK(builder.Add(key, value, &blob_indexes[i]));
    ASSERT_FALSE(blob_indexes[i].empty());
  }

  ASSERT_OK(builder.Finish());

  // Check the metadata generated
  ASSERT_EQ(blob_file_additions.size(), number_of_blobs);

  for (size_t i = 0; i < number_of_blobs; ++i) {
    ASSERT_EQ(blob_file_additions[i].GetBlobFileNumber(), i + 2);
    ASSERT_EQ(blob_file_additions[i].GetTotalBlobCount(), 1);
    ASSERT_EQ(blob_file_additions[i].GetTotalBlobBytes(),
              BlobLogRecord::kHeaderSize + key_size + value_size);
  }

  // Verify the contents of the new blob files as well as the blob references
  for (size_t i = 0; i < number_of_blobs; ++i) {
    std::vector<std::pair<std::string, std::string>> expected_key_value_pair{
        expected_key_value_pairs[i]};
    std::vector<std::string> blob_index{blob_indexes[i]};

    VerifyBlobFile(immutable_cf_options, i + 2, column_family_id,
                   expected_key_value_pair, blob_index);
  }
}

TEST_F(BlobFileBuilderTest, InlinedValues) {
  // All values are below the min_blob_size threshold; no blob files get written
  constexpr size_t number_of_blobs = 10;
  constexpr size_t key_size = 1;
  constexpr size_t value_size = 10;
  constexpr size_t value_offset = 1234567890;

  Options options;
  options.cf_paths.emplace_back(
      test::PerThreadDBPath(&env_, "BlobFileBuilderTest_InlinedValues"), 0);
  options.enable_blob_files = true;
  options.min_blob_size = 1024;

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

  for (size_t i = 0; i < number_of_blobs; ++i) {
    const std::string key = std::to_string(i);
    assert(key.size() == key_size);

    const std::string value = std::to_string(i + value_offset);
    assert(value.size() == value_size);

    std::string blob_index;
    ASSERT_OK(builder.Add(key, value, &blob_index));
    ASSERT_TRUE(blob_index.empty());
  }

  ASSERT_OK(builder.Finish());

  // Check the metadata generated
  ASSERT_TRUE(blob_file_additions.empty());
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

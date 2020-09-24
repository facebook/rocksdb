//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/blob/blob_file_reader.h"

#include <cassert>
#include <cinttypes>
#include <string>

#include "db/blob/blob_log_format.h"
#include "db/blob/blob_log_writer.h"
#include "env/mock_env.h"
#include "file/filename.h"
#include "file/read_write_util.h"
#include "file/writable_file_writer.h"
#include "options/cf_options.h"
#include "rocksdb/env.h"
#include "rocksdb/file_system.h"
#include "rocksdb/options.h"
#include "test_util/testharness.h"

namespace ROCKSDB_NAMESPACE {

namespace {

void WriteBlobFile(const ImmutableCFOptions& immutable_cf_options,
                   uint32_t column_family_id, uint64_t blob_file_number,
                   const Slice& key, const Slice& blob, uint64_t* blob_offset) {
  assert(!immutable_cf_options.cf_paths.empty());

  const std::string blob_file_path = BlobFileName(
      immutable_cf_options.cf_paths.front().path, blob_file_number);

  std::unique_ptr<FSWritableFile> file;
  ASSERT_OK(NewWritableFile(immutable_cf_options.fs, blob_file_path, &file,
                            FileOptions()));

  std::unique_ptr<WritableFileWriter> file_writer(
      new WritableFileWriter(std::move(file), blob_file_path, FileOptions(),
                             immutable_cf_options.env));

  constexpr Statistics* statistics = nullptr;
  constexpr bool use_fsync = false;

  BlobLogWriter blob_log_writer(std::move(file_writer),
                                immutable_cf_options.env, statistics,
                                blob_file_number, use_fsync);

  constexpr bool has_ttl = false;
  constexpr ExpirationRange expiration_range;

  BlobLogHeader header(column_family_id, kNoCompression, has_ttl,
                       expiration_range);

  ASSERT_OK(blob_log_writer.WriteHeader(header));

  uint64_t key_offset = 0;

  ASSERT_OK(blob_log_writer.AddRecord(key, blob, &key_offset, blob_offset));

  BlobLogFooter footer;
  footer.blob_count = 1;

  std::string checksum_method;
  std::string checksum_value;

  ASSERT_OK(
      blob_log_writer.AppendFooter(footer, &checksum_method, &checksum_value));
}

}  // anonymous namespace

class BlobFileReaderTest : public testing::Test {
 protected:
  BlobFileReaderTest() : mock_env_(Env::Default()) {}

  MockEnv mock_env_;
};

TEST_F(BlobFileReaderTest, CreateReaderAndGetBlob) {
  Options options;
  options.env = &mock_env_;
  options.cf_paths.emplace_back(
      test::PerThreadDBPath(&mock_env_,
                            "BlobFileReaderTest_CreateReaderAndGetBlob"),
      0);
  options.enable_blob_files = true;

  ImmutableCFOptions immutable_cf_options(options);

  constexpr uint32_t column_family_id = 1;
  constexpr uint64_t blob_file_number = 1;
  constexpr char key[] = "key";
  constexpr char blob[] = "blob";

  uint64_t blob_offset = 0;

  WriteBlobFile(immutable_cf_options, column_family_id, blob_file_number, key,
                blob, &blob_offset);

  constexpr HistogramImpl* blob_file_read_hist = nullptr;

  std::unique_ptr<BlobFileReader> reader;

  ASSERT_OK(BlobFileReader::Create(immutable_cf_options, FileOptions(),
                                   column_family_id, blob_file_read_hist,
                                   blob_file_number, &reader));

  constexpr const MergeOperator* merge_operator = nullptr;
  constexpr Logger* logger = nullptr;
  constexpr Statistics* statistics = nullptr;
  constexpr bool* value_found = nullptr;
  constexpr MergeContext* merge_context = nullptr;
  constexpr bool do_merge = true;
  constexpr SequenceNumber* max_covering_tombstone_seq = nullptr;
  constexpr Env* env = nullptr;

  PinnableSlice value;

  GetContext get_context(options.comparator, merge_operator, logger, statistics,
                         GetContext::kFound, key, &value, value_found,
                         merge_context, do_merge, max_covering_tombstone_seq,
                         env);
  ASSERT_OK(reader->GetBlob(ReadOptions(), key, blob_offset, sizeof(blob) - 1,
                            kNoCompression, &get_context));
  ASSERT_EQ(value, blob);
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

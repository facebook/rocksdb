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
#include "utilities/fault_injection_env.h"

namespace ROCKSDB_NAMESPACE {

namespace {

void WriteBlobFile(const ImmutableCFOptions& immutable_cf_options,
                   uint32_t column_family_id, bool has_ttl,
                   const ExpirationRange& expiration_range_header,
                   const ExpirationRange& expiration_range_footer,
                   uint64_t blob_file_number, const Slice& key,
                   const Slice& blob, uint64_t* blob_offset) {
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

  BlobLogHeader header(column_family_id, kNoCompression, has_ttl,
                       expiration_range_header);

  ASSERT_OK(blob_log_writer.WriteHeader(header));

  uint64_t key_offset = 0;

  ASSERT_OK(blob_log_writer.AddRecord(key, blob, &key_offset, blob_offset));

  BlobLogFooter footer;
  footer.blob_count = 1;
  footer.expiration_range = expiration_range_footer;

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

  constexpr bool has_ttl = false;
  constexpr ExpirationRange expiration_range;

  WriteBlobFile(immutable_cf_options, column_family_id, has_ttl,
                expiration_range, expiration_range, blob_file_number, key, blob,
                &blob_offset);

  constexpr HistogramImpl* blob_file_read_hist = nullptr;

  std::unique_ptr<BlobFileReader> reader;

  ASSERT_OK(BlobFileReader::Create(immutable_cf_options, FileOptions(),
                                   column_family_id, blob_file_read_hist,
                                   blob_file_number, &reader));

  // Make sure the blob can be retrieved with and without checksum verification
  ReadOptions read_options;
  read_options.verify_checksums = false;

  {
    PinnableSlice value;


    ASSERT_OK(reader->GetBlob(read_options, key, blob_offset, sizeof(blob) - 1,
                              kNoCompression, &value));
    ASSERT_EQ(value, blob);
  }

  read_options.verify_checksums = true;

  {
    PinnableSlice value;

    ASSERT_OK(reader->GetBlob(read_options, key, blob_offset, sizeof(blob) - 1,
                              kNoCompression, &value));
    ASSERT_EQ(value, blob);
  }

  // Invalid offset
  {
    PinnableSlice value;

    ASSERT_TRUE(reader
                    ->GetBlob(read_options, key, blob_offset - 1,
                              sizeof(blob) - 1, kNoCompression, &value)
                    .IsCorruption());
  }

  // Incorrect compression type
  {
    PinnableSlice value;

    ASSERT_TRUE(reader
                    ->GetBlob(read_options, key, blob_offset, sizeof(blob) - 1,
                              kZSTD, &value)
                    .IsCorruption());
  }

  // Incorrect key
  {
    constexpr char incorrect_key[] = "foo";
    PinnableSlice value;

    ASSERT_TRUE(reader
                    ->GetBlob(read_options, incorrect_key, blob_offset,
                              sizeof(blob) - 1, kNoCompression, &value)
                    .IsCorruption());
  }

  // Incorrect value size
  {
    PinnableSlice value;

    ASSERT_TRUE(reader
                    ->GetBlob(read_options, key, blob_offset, sizeof(blob),
                              kNoCompression, &value)
                    .IsCorruption());
  }
}

class BlobFileReaderIOErrorTest
    : public testing::Test,
      public testing::WithParamInterface<std::string> {
 protected:
  BlobFileReaderIOErrorTest()
      : mock_env_(Env::Default()),
        fault_injection_env_(&mock_env_),
        sync_point_(GetParam()) {}

  MockEnv mock_env_;
  FaultInjectionTestEnv fault_injection_env_;
  std::string sync_point_;
};

TEST_F(BlobFileReaderTest, TTL) {
  Options options;
  options.env = &mock_env_;
  options.cf_paths.emplace_back(
      test::PerThreadDBPath(&mock_env_, "BlobFileReaderTest_TTL"), 0);
  options.enable_blob_files = true;

  ImmutableCFOptions immutable_cf_options(options);

  constexpr uint32_t column_family_id = 1;
  constexpr uint64_t blob_file_number = 1;
  constexpr char key[] = "key";
  constexpr char blob[] = "blob";

  uint64_t blob_offset = 0;

  constexpr bool has_ttl = true;
  constexpr ExpirationRange expiration_range;

  WriteBlobFile(immutable_cf_options, column_family_id, has_ttl,
                expiration_range, expiration_range, blob_file_number, key, blob,
                &blob_offset);

  constexpr HistogramImpl* blob_file_read_hist = nullptr;

  std::unique_ptr<BlobFileReader> reader;

  ASSERT_TRUE(BlobFileReader::Create(immutable_cf_options, FileOptions(),
                                     column_family_id, blob_file_read_hist,
                                     blob_file_number, &reader)
                  .IsCorruption());
}

TEST_F(BlobFileReaderTest, ExpirationRangeInHeader) {
  Options options;
  options.env = &mock_env_;
  options.cf_paths.emplace_back(
      test::PerThreadDBPath(&mock_env_,
                            "BlobFileReaderTest_ExpirationRangeInHeader"),
      0);
  options.enable_blob_files = true;

  ImmutableCFOptions immutable_cf_options(options);

  constexpr uint32_t column_family_id = 1;
  constexpr uint64_t blob_file_number = 1;
  constexpr char key[] = "key";
  constexpr char blob[] = "blob";

  uint64_t blob_offset = 0;

  constexpr bool has_ttl = false;
  constexpr ExpirationRange expiration_range_header(1, 2);
  constexpr ExpirationRange expiration_range_footer;

  WriteBlobFile(immutable_cf_options, column_family_id, has_ttl,
                expiration_range_header, expiration_range_footer,
                blob_file_number, key, blob, &blob_offset);

  constexpr HistogramImpl* blob_file_read_hist = nullptr;

  std::unique_ptr<BlobFileReader> reader;

  ASSERT_TRUE(BlobFileReader::Create(immutable_cf_options, FileOptions(),
                                     column_family_id, blob_file_read_hist,
                                     blob_file_number, &reader)
                  .IsCorruption());
}

TEST_F(BlobFileReaderTest, ExpirationRangeInFooter) {
  Options options;
  options.env = &mock_env_;
  options.cf_paths.emplace_back(
      test::PerThreadDBPath(&mock_env_,
                            "BlobFileReaderTest_ExpirationRangeInFooter"),
      0);
  options.enable_blob_files = true;

  ImmutableCFOptions immutable_cf_options(options);

  constexpr uint32_t column_family_id = 1;
  constexpr uint64_t blob_file_number = 1;
  constexpr char key[] = "key";
  constexpr char blob[] = "blob";

  uint64_t blob_offset = 0;

  constexpr bool has_ttl = false;
  constexpr ExpirationRange expiration_range_header;
  constexpr ExpirationRange expiration_range_footer(1, 2);

  WriteBlobFile(immutable_cf_options, column_family_id, has_ttl,
                expiration_range_header, expiration_range_footer,
                blob_file_number, key, blob, &blob_offset);

  constexpr HistogramImpl* blob_file_read_hist = nullptr;

  std::unique_ptr<BlobFileReader> reader;

  ASSERT_TRUE(BlobFileReader::Create(immutable_cf_options, FileOptions(),
                                     column_family_id, blob_file_read_hist,
                                     blob_file_number, &reader)
                  .IsCorruption());
}

TEST_F(BlobFileReaderTest, IncorrectColumnFamily) {
  Options options;
  options.env = &mock_env_;
  options.cf_paths.emplace_back(
      test::PerThreadDBPath(&mock_env_,
                            "BlobFileReaderTest_IncorrectColumnFamily"),
      0);
  options.enable_blob_files = true;

  ImmutableCFOptions immutable_cf_options(options);

  constexpr uint32_t column_family_id = 1;
  constexpr uint64_t blob_file_number = 1;
  constexpr char key[] = "key";
  constexpr char blob[] = "blob";

  uint64_t blob_offset = 0;

  constexpr bool has_ttl = false;
  constexpr ExpirationRange expiration_range;

  WriteBlobFile(immutable_cf_options, column_family_id, has_ttl,
                expiration_range, expiration_range, blob_file_number, key, blob,
                &blob_offset);

  constexpr HistogramImpl* blob_file_read_hist = nullptr;

  std::unique_ptr<BlobFileReader> reader;

  constexpr uint32_t incorrect_column_family_id = 2;

  ASSERT_TRUE(BlobFileReader::Create(immutable_cf_options, FileOptions(),
                                     incorrect_column_family_id,
                                     blob_file_read_hist, blob_file_number,
                                     &reader)
                  .IsCorruption());
}

INSTANTIATE_TEST_CASE_P(BlobFileReaderTest, BlobFileReaderIOErrorTest,
                        ::testing::ValuesIn(std::vector<std::string>{
                            "BlobFileReader::OpenFile:GetFileSize",
                            "BlobFileReader::OpenFile:NewRandomAccessFile",
                            "BlobFileReader::ReadHeader:ReadFromFile",
                            "BlobFileReader::ReadFooter:ReadFromFile",
                            "BlobFileReader::GetBlob:ReadFromFile"}));

TEST_P(BlobFileReaderIOErrorTest, IOError) {
  // Simulates an I/O error during the specified step

  Options options;
  options.env = &fault_injection_env_;
  options.cf_paths.emplace_back(
      test::PerThreadDBPath(&fault_injection_env_,
                            "BlobFileReaderIOErrorTest_IOError"),
      0);
  options.enable_blob_files = true;

  ImmutableCFOptions immutable_cf_options(options);

  constexpr uint32_t column_family_id = 1;
  constexpr uint64_t blob_file_number = 1;
  constexpr char key[] = "key";
  constexpr char blob[] = "blob";

  uint64_t blob_offset = 0;

  constexpr bool has_ttl = false;
  constexpr ExpirationRange expiration_range;

  WriteBlobFile(immutable_cf_options, column_family_id, has_ttl,
                expiration_range, expiration_range, blob_file_number, key, blob,
                &blob_offset);

  SyncPoint::GetInstance()->SetCallBack(sync_point_, [this](void* /* arg */) {
    fault_injection_env_.SetFilesystemActive(false,
                                             Status::IOError(sync_point_));
  });
  SyncPoint::GetInstance()->EnableProcessing();

  constexpr HistogramImpl* blob_file_read_hist = nullptr;

  std::unique_ptr<BlobFileReader> reader;

  const Status s = BlobFileReader::Create(immutable_cf_options, FileOptions(),
                                          column_family_id, blob_file_read_hist,
                                          blob_file_number, &reader);

  const bool fail_during_create =
      (sync_point_ != "BlobFileReader::GetBlob:ReadFromFile");

  if (fail_during_create) {
    ASSERT_TRUE(s.IsIOError());
  } else {
    ASSERT_OK(s);

    PinnableSlice value;

    ASSERT_TRUE(reader
                    ->GetBlob(ReadOptions(), key, blob_offset, sizeof(blob) - 1,
                              kNoCompression, &value)
                    .IsIOError());
  }

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

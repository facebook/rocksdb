//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/blob/blob_file_reader.h"

#include <cassert>
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
#include "test_util/sync_point.h"
#include "test_util/testharness.h"
#include "util/compression.h"
#include "utilities/fault_injection_env.h"

namespace ROCKSDB_NAMESPACE {

namespace {

// Creates a test blob file with a single blob in it. Note: this method
// makes it possible to test various corner cases by allowing the caller
// to specify the contents of various blob file header/footer fields.
void WriteBlobFile(const ImmutableOptions& immutable_options,
                   uint32_t column_family_id, bool has_ttl,
                   const ExpirationRange& expiration_range_header,
                   const ExpirationRange& expiration_range_footer,
                   uint64_t blob_file_number, const Slice& key,
                   const Slice& blob, CompressionType compression_type,
                   uint64_t* blob_offset, uint64_t* blob_size) {
  assert(!immutable_options.cf_paths.empty());
  assert(blob_offset);
  assert(blob_size);

  const std::string blob_file_path =
      BlobFileName(immutable_options.cf_paths.front().path, blob_file_number);

  std::unique_ptr<FSWritableFile> file;
  ASSERT_OK(NewWritableFile(immutable_options.fs.get(), blob_file_path, &file,
                            FileOptions()));

  std::unique_ptr<WritableFileWriter> file_writer(new WritableFileWriter(
      std::move(file), blob_file_path, FileOptions(), immutable_options.clock));

  constexpr Statistics* statistics = nullptr;
  constexpr bool use_fsync = false;
  constexpr bool do_flush = false;

  BlobLogWriter blob_log_writer(std::move(file_writer), immutable_options.clock,
                                statistics, blob_file_number, use_fsync,
                                do_flush);

  BlobLogHeader header(column_family_id, compression_type, has_ttl,
                       expiration_range_header);

  ASSERT_OK(blob_log_writer.WriteHeader(header));

  std::string compressed_blob;
  Slice blob_to_write;

  if (compression_type == kNoCompression) {
    blob_to_write = blob;
    *blob_size = blob.size();
  } else {
    CompressionOptions opts;
    CompressionContext context(compression_type);
    constexpr uint64_t sample_for_compression = 0;

    CompressionInfo info(opts, context, CompressionDict::GetEmptyDict(),
                         compression_type, sample_for_compression);

    constexpr uint32_t compression_format_version = 2;

    ASSERT_TRUE(
        CompressData(blob, info, compression_format_version, &compressed_blob));

    blob_to_write = compressed_blob;
    *blob_size = compressed_blob.size();
  }

  uint64_t key_offset = 0;

  ASSERT_OK(
      blob_log_writer.AddRecord(key, blob_to_write, &key_offset, blob_offset));

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

  ImmutableOptions immutable_options(options);

  constexpr uint32_t column_family_id = 1;
  constexpr bool has_ttl = false;
  constexpr ExpirationRange expiration_range;
  constexpr uint64_t blob_file_number = 1;
  constexpr char key[] = "key";
  constexpr char blob[] = "blob";

  uint64_t blob_offset = 0;
  uint64_t blob_size = 0;

  WriteBlobFile(immutable_options, column_family_id, has_ttl, expiration_range,
                expiration_range, blob_file_number, key, blob, kNoCompression,
                &blob_offset, &blob_size);

  constexpr HistogramImpl* blob_file_read_hist = nullptr;

  std::unique_ptr<BlobFileReader> reader;

  ASSERT_OK(BlobFileReader::Create(
      immutable_options, FileOptions(), column_family_id, blob_file_read_hist,
      blob_file_number, nullptr /*IOTracer*/, &reader));

  // Make sure the blob can be retrieved with and without checksum verification
  ReadOptions read_options;
  read_options.verify_checksums = false;

  {
    PinnableSlice value;
    uint64_t bytes_read = 0;

    ASSERT_OK(reader->GetBlob(read_options, key, blob_offset, blob_size,
                              kNoCompression, &value, &bytes_read));
    ASSERT_EQ(value, blob);
    ASSERT_EQ(bytes_read, blob_size);
  }

  read_options.verify_checksums = true;

  {
    PinnableSlice value;
    uint64_t bytes_read = 0;

    ASSERT_OK(reader->GetBlob(read_options, key, blob_offset, blob_size,
                              kNoCompression, &value, &bytes_read));
    ASSERT_EQ(value, blob);

    constexpr uint64_t key_size = sizeof(key) - 1;
    ASSERT_EQ(bytes_read,
              BlobLogRecord::CalculateAdjustmentForRecordHeader(key_size) +
                  blob_size);
  }

  // Invalid offset (too close to start of file)
  {
    PinnableSlice value;
    uint64_t bytes_read = 0;

    ASSERT_TRUE(reader
                    ->GetBlob(read_options, key, blob_offset - 1, blob_size,
                              kNoCompression, &value, &bytes_read)
                    .IsCorruption());
    ASSERT_EQ(bytes_read, 0);
  }

  // Invalid offset (too close to end of file)
  {
    PinnableSlice value;
    uint64_t bytes_read = 0;

    ASSERT_TRUE(reader
                    ->GetBlob(read_options, key, blob_offset + 1, blob_size,
                              kNoCompression, &value, &bytes_read)
                    .IsCorruption());
    ASSERT_EQ(bytes_read, 0);
  }

  // Incorrect compression type
  {
    PinnableSlice value;
    uint64_t bytes_read = 0;

    ASSERT_TRUE(reader
                    ->GetBlob(read_options, key, blob_offset, blob_size, kZSTD,
                              &value, &bytes_read)
                    .IsCorruption());
    ASSERT_EQ(bytes_read, 0);
  }

  // Incorrect key size
  {
    constexpr char shorter_key[] = "k";
    PinnableSlice value;
    uint64_t bytes_read = 0;

    ASSERT_TRUE(reader
                    ->GetBlob(read_options, shorter_key,
                              blob_offset - (sizeof(key) - sizeof(shorter_key)),
                              blob_size, kNoCompression, &value, &bytes_read)
                    .IsCorruption());
    ASSERT_EQ(bytes_read, 0);
  }

  // Incorrect key
  {
    constexpr char incorrect_key[] = "foo";
    PinnableSlice value;
    uint64_t bytes_read = 0;

    ASSERT_TRUE(reader
                    ->GetBlob(read_options, incorrect_key, blob_offset,
                              blob_size, kNoCompression, &value, &bytes_read)
                    .IsCorruption());
    ASSERT_EQ(bytes_read, 0);
  }

  // Incorrect value size
  {
    PinnableSlice value;
    uint64_t bytes_read = 0;

    ASSERT_TRUE(reader
                    ->GetBlob(read_options, key, blob_offset, blob_size + 1,
                              kNoCompression, &value, &bytes_read)
                    .IsCorruption());
    ASSERT_EQ(bytes_read, 0);
  }
}

TEST_F(BlobFileReaderTest, Malformed) {
  // Write a blob file consisting of nothing but a header, and make sure we
  // detect the error when we open it for reading

  Options options;
  options.env = &mock_env_;
  options.cf_paths.emplace_back(
      test::PerThreadDBPath(&mock_env_, "BlobFileReaderTest_Malformed"), 0);
  options.enable_blob_files = true;

  ImmutableOptions immutable_options(options);

  constexpr uint32_t column_family_id = 1;
  constexpr uint64_t blob_file_number = 1;

  {
    constexpr bool has_ttl = false;
    constexpr ExpirationRange expiration_range;

    const std::string blob_file_path =
        BlobFileName(immutable_options.cf_paths.front().path, blob_file_number);

    std::unique_ptr<FSWritableFile> file;
    ASSERT_OK(NewWritableFile(immutable_options.fs.get(), blob_file_path, &file,
                              FileOptions()));

    std::unique_ptr<WritableFileWriter> file_writer(
        new WritableFileWriter(std::move(file), blob_file_path, FileOptions(),
                               immutable_options.clock));

    constexpr Statistics* statistics = nullptr;
    constexpr bool use_fsync = false;
    constexpr bool do_flush = false;

    BlobLogWriter blob_log_writer(std::move(file_writer),
                                  immutable_options.clock, statistics,
                                  blob_file_number, use_fsync, do_flush);

    BlobLogHeader header(column_family_id, kNoCompression, has_ttl,
                         expiration_range);

    ASSERT_OK(blob_log_writer.WriteHeader(header));
  }

  constexpr HistogramImpl* blob_file_read_hist = nullptr;

  std::unique_ptr<BlobFileReader> reader;

  ASSERT_TRUE(BlobFileReader::Create(immutable_options, FileOptions(),
                                     column_family_id, blob_file_read_hist,
                                     blob_file_number, nullptr /*IOTracer*/,
                                     &reader)
                  .IsCorruption());
}

TEST_F(BlobFileReaderTest, TTL) {
  Options options;
  options.env = &mock_env_;
  options.cf_paths.emplace_back(
      test::PerThreadDBPath(&mock_env_, "BlobFileReaderTest_TTL"), 0);
  options.enable_blob_files = true;

  ImmutableOptions immutable_options(options);

  constexpr uint32_t column_family_id = 1;
  constexpr bool has_ttl = true;
  constexpr ExpirationRange expiration_range;
  constexpr uint64_t blob_file_number = 1;
  constexpr char key[] = "key";
  constexpr char blob[] = "blob";

  uint64_t blob_offset = 0;
  uint64_t blob_size = 0;

  WriteBlobFile(immutable_options, column_family_id, has_ttl, expiration_range,
                expiration_range, blob_file_number, key, blob, kNoCompression,
                &blob_offset, &blob_size);

  constexpr HistogramImpl* blob_file_read_hist = nullptr;

  std::unique_ptr<BlobFileReader> reader;

  ASSERT_TRUE(BlobFileReader::Create(immutable_options, FileOptions(),
                                     column_family_id, blob_file_read_hist,
                                     blob_file_number, nullptr /*IOTracer*/,
                                     &reader)
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

  ImmutableOptions immutable_options(options);

  constexpr uint32_t column_family_id = 1;
  constexpr bool has_ttl = false;
  const ExpirationRange expiration_range_header(
      1, 2);  // can be made constexpr when we adopt C++14
  constexpr ExpirationRange expiration_range_footer;
  constexpr uint64_t blob_file_number = 1;
  constexpr char key[] = "key";
  constexpr char blob[] = "blob";

  uint64_t blob_offset = 0;
  uint64_t blob_size = 0;

  WriteBlobFile(immutable_options, column_family_id, has_ttl,
                expiration_range_header, expiration_range_footer,
                blob_file_number, key, blob, kNoCompression, &blob_offset,
                &blob_size);

  constexpr HistogramImpl* blob_file_read_hist = nullptr;

  std::unique_ptr<BlobFileReader> reader;

  ASSERT_TRUE(BlobFileReader::Create(immutable_options, FileOptions(),
                                     column_family_id, blob_file_read_hist,
                                     blob_file_number, nullptr /*IOTracer*/,
                                     &reader)
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

  ImmutableOptions immutable_options(options);

  constexpr uint32_t column_family_id = 1;
  constexpr bool has_ttl = false;
  constexpr ExpirationRange expiration_range_header;
  const ExpirationRange expiration_range_footer(
      1, 2);  // can be made constexpr when we adopt C++14
  constexpr uint64_t blob_file_number = 1;
  constexpr char key[] = "key";
  constexpr char blob[] = "blob";

  uint64_t blob_offset = 0;
  uint64_t blob_size = 0;

  WriteBlobFile(immutable_options, column_family_id, has_ttl,
                expiration_range_header, expiration_range_footer,
                blob_file_number, key, blob, kNoCompression, &blob_offset,
                &blob_size);

  constexpr HistogramImpl* blob_file_read_hist = nullptr;

  std::unique_ptr<BlobFileReader> reader;

  ASSERT_TRUE(BlobFileReader::Create(immutable_options, FileOptions(),
                                     column_family_id, blob_file_read_hist,
                                     blob_file_number, nullptr /*IOTracer*/,
                                     &reader)
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

  ImmutableOptions immutable_options(options);

  constexpr uint32_t column_family_id = 1;
  constexpr bool has_ttl = false;
  constexpr ExpirationRange expiration_range;
  constexpr uint64_t blob_file_number = 1;
  constexpr char key[] = "key";
  constexpr char blob[] = "blob";

  uint64_t blob_offset = 0;
  uint64_t blob_size = 0;

  WriteBlobFile(immutable_options, column_family_id, has_ttl, expiration_range,
                expiration_range, blob_file_number, key, blob, kNoCompression,
                &blob_offset, &blob_size);

  constexpr HistogramImpl* blob_file_read_hist = nullptr;

  std::unique_ptr<BlobFileReader> reader;

  constexpr uint32_t incorrect_column_family_id = 2;

  ASSERT_TRUE(BlobFileReader::Create(immutable_options, FileOptions(),
                                     incorrect_column_family_id,
                                     blob_file_read_hist, blob_file_number,
                                     nullptr /*IOTracer*/, &reader)
                  .IsCorruption());
}

TEST_F(BlobFileReaderTest, BlobCRCError) {
  Options options;
  options.env = &mock_env_;
  options.cf_paths.emplace_back(
      test::PerThreadDBPath(&mock_env_, "BlobFileReaderTest_BlobCRCError"), 0);
  options.enable_blob_files = true;

  ImmutableOptions immutable_options(options);

  constexpr uint32_t column_family_id = 1;
  constexpr bool has_ttl = false;
  constexpr ExpirationRange expiration_range;
  constexpr uint64_t blob_file_number = 1;
  constexpr char key[] = "key";
  constexpr char blob[] = "blob";

  uint64_t blob_offset = 0;
  uint64_t blob_size = 0;

  WriteBlobFile(immutable_options, column_family_id, has_ttl, expiration_range,
                expiration_range, blob_file_number, key, blob, kNoCompression,
                &blob_offset, &blob_size);

  constexpr HistogramImpl* blob_file_read_hist = nullptr;

  std::unique_ptr<BlobFileReader> reader;

  ASSERT_OK(BlobFileReader::Create(
      immutable_options, FileOptions(), column_family_id, blob_file_read_hist,
      blob_file_number, nullptr /*IOTracer*/, &reader));

  SyncPoint::GetInstance()->SetCallBack(
      "BlobFileReader::VerifyBlob:CheckBlobCRC", [](void* arg) {
        BlobLogRecord* const record = static_cast<BlobLogRecord*>(arg);
        assert(record);

        record->blob_crc = 0xfaceb00c;
      });

  SyncPoint::GetInstance()->EnableProcessing();

  PinnableSlice value;
  uint64_t bytes_read = 0;

  ASSERT_TRUE(reader
                  ->GetBlob(ReadOptions(), key, blob_offset, blob_size,
                            kNoCompression, &value, &bytes_read)
                  .IsCorruption());
  ASSERT_EQ(bytes_read, 0);

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
}

TEST_F(BlobFileReaderTest, Compression) {
  if (!Snappy_Supported()) {
    return;
  }

  Options options;
  options.env = &mock_env_;
  options.cf_paths.emplace_back(
      test::PerThreadDBPath(&mock_env_, "BlobFileReaderTest_Compression"), 0);
  options.enable_blob_files = true;

  ImmutableOptions immutable_options(options);

  constexpr uint32_t column_family_id = 1;
  constexpr bool has_ttl = false;
  constexpr ExpirationRange expiration_range;
  constexpr uint64_t blob_file_number = 1;
  constexpr char key[] = "key";
  constexpr char blob[] = "blob";

  uint64_t blob_offset = 0;
  uint64_t blob_size = 0;

  WriteBlobFile(immutable_options, column_family_id, has_ttl, expiration_range,
                expiration_range, blob_file_number, key, blob,
                kSnappyCompression, &blob_offset, &blob_size);

  constexpr HistogramImpl* blob_file_read_hist = nullptr;

  std::unique_ptr<BlobFileReader> reader;

  ASSERT_OK(BlobFileReader::Create(
      immutable_options, FileOptions(), column_family_id, blob_file_read_hist,
      blob_file_number, nullptr /*IOTracer*/, &reader));

  // Make sure the blob can be retrieved with and without checksum verification
  ReadOptions read_options;
  read_options.verify_checksums = false;

  {
    PinnableSlice value;
    uint64_t bytes_read = 0;

    ASSERT_OK(reader->GetBlob(read_options, key, blob_offset, blob_size,
                              kSnappyCompression, &value, &bytes_read));
    ASSERT_EQ(value, blob);
    ASSERT_EQ(bytes_read, blob_size);
  }

  read_options.verify_checksums = true;

  {
    PinnableSlice value;
    uint64_t bytes_read = 0;

    ASSERT_OK(reader->GetBlob(read_options, key, blob_offset, blob_size,
                              kSnappyCompression, &value, &bytes_read));
    ASSERT_EQ(value, blob);

    constexpr uint64_t key_size = sizeof(key) - 1;
    ASSERT_EQ(bytes_read,
              BlobLogRecord::CalculateAdjustmentForRecordHeader(key_size) +
                  blob_size);
  }
}

TEST_F(BlobFileReaderTest, UncompressionError) {
  if (!Snappy_Supported()) {
    return;
  }

  Options options;
  options.env = &mock_env_;
  options.cf_paths.emplace_back(
      test::PerThreadDBPath(&mock_env_,
                            "BlobFileReaderTest_UncompressionError"),
      0);
  options.enable_blob_files = true;

  ImmutableOptions immutable_options(options);

  constexpr uint32_t column_family_id = 1;
  constexpr bool has_ttl = false;
  constexpr ExpirationRange expiration_range;
  constexpr uint64_t blob_file_number = 1;
  constexpr char key[] = "key";
  constexpr char blob[] = "blob";

  uint64_t blob_offset = 0;
  uint64_t blob_size = 0;

  WriteBlobFile(immutable_options, column_family_id, has_ttl, expiration_range,
                expiration_range, blob_file_number, key, blob,
                kSnappyCompression, &blob_offset, &blob_size);

  constexpr HistogramImpl* blob_file_read_hist = nullptr;

  std::unique_ptr<BlobFileReader> reader;

  ASSERT_OK(BlobFileReader::Create(
      immutable_options, FileOptions(), column_family_id, blob_file_read_hist,
      blob_file_number, nullptr /*IOTracer*/, &reader));

  SyncPoint::GetInstance()->SetCallBack(
      "BlobFileReader::UncompressBlobIfNeeded:TamperWithResult", [](void* arg) {
        CacheAllocationPtr* const output =
            static_cast<CacheAllocationPtr*>(arg);
        assert(output);

        output->reset();
      });

  SyncPoint::GetInstance()->EnableProcessing();

  PinnableSlice value;
  uint64_t bytes_read = 0;

  ASSERT_TRUE(reader
                  ->GetBlob(ReadOptions(), key, blob_offset, blob_size,
                            kSnappyCompression, &value, &bytes_read)
                  .IsCorruption());
  ASSERT_EQ(bytes_read, 0);

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
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

  ImmutableOptions immutable_options(options);

  constexpr uint32_t column_family_id = 1;
  constexpr bool has_ttl = false;
  constexpr ExpirationRange expiration_range;
  constexpr uint64_t blob_file_number = 1;
  constexpr char key[] = "key";
  constexpr char blob[] = "blob";

  uint64_t blob_offset = 0;
  uint64_t blob_size = 0;

  WriteBlobFile(immutable_options, column_family_id, has_ttl, expiration_range,
                expiration_range, blob_file_number, key, blob, kNoCompression,
                &blob_offset, &blob_size);

  SyncPoint::GetInstance()->SetCallBack(sync_point_, [this](void* /* arg */) {
    fault_injection_env_.SetFilesystemActive(false,
                                             Status::IOError(sync_point_));
  });
  SyncPoint::GetInstance()->EnableProcessing();

  constexpr HistogramImpl* blob_file_read_hist = nullptr;

  std::unique_ptr<BlobFileReader> reader;

  const Status s = BlobFileReader::Create(
      immutable_options, FileOptions(), column_family_id, blob_file_read_hist,
      blob_file_number, nullptr /*IOTracer*/, &reader);

  const bool fail_during_create =
      (sync_point_ != "BlobFileReader::GetBlob:ReadFromFile");

  if (fail_during_create) {
    ASSERT_TRUE(s.IsIOError());
  } else {
    ASSERT_OK(s);

    PinnableSlice value;
    uint64_t bytes_read = 0;

    ASSERT_TRUE(reader
                    ->GetBlob(ReadOptions(), key, blob_offset, blob_size,
                              kNoCompression, &value, &bytes_read)
                    .IsIOError());
    ASSERT_EQ(bytes_read, 0);
  }

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
}

class BlobFileReaderDecodingErrorTest
    : public testing::Test,
      public testing::WithParamInterface<std::string> {
 protected:
  BlobFileReaderDecodingErrorTest()
      : mock_env_(Env::Default()), sync_point_(GetParam()) {}

  MockEnv mock_env_;
  std::string sync_point_;
};

INSTANTIATE_TEST_CASE_P(BlobFileReaderTest, BlobFileReaderDecodingErrorTest,
                        ::testing::ValuesIn(std::vector<std::string>{
                            "BlobFileReader::ReadHeader:TamperWithResult",
                            "BlobFileReader::ReadFooter:TamperWithResult",
                            "BlobFileReader::GetBlob:TamperWithResult"}));

TEST_P(BlobFileReaderDecodingErrorTest, DecodingError) {
  Options options;
  options.env = &mock_env_;
  options.cf_paths.emplace_back(
      test::PerThreadDBPath(&mock_env_,
                            "BlobFileReaderDecodingErrorTest_DecodingError"),
      0);
  options.enable_blob_files = true;

  ImmutableOptions immutable_options(options);

  constexpr uint32_t column_family_id = 1;
  constexpr bool has_ttl = false;
  constexpr ExpirationRange expiration_range;
  constexpr uint64_t blob_file_number = 1;
  constexpr char key[] = "key";
  constexpr char blob[] = "blob";

  uint64_t blob_offset = 0;
  uint64_t blob_size = 0;

  WriteBlobFile(immutable_options, column_family_id, has_ttl, expiration_range,
                expiration_range, blob_file_number, key, blob, kNoCompression,
                &blob_offset, &blob_size);

  SyncPoint::GetInstance()->SetCallBack(sync_point_, [](void* arg) {
    Slice* const slice = static_cast<Slice*>(arg);
    assert(slice);
    assert(!slice->empty());

    slice->remove_prefix(1);
  });

  SyncPoint::GetInstance()->EnableProcessing();

  constexpr HistogramImpl* blob_file_read_hist = nullptr;

  std::unique_ptr<BlobFileReader> reader;

  const Status s = BlobFileReader::Create(
      immutable_options, FileOptions(), column_family_id, blob_file_read_hist,
      blob_file_number, nullptr /*IOTracer*/, &reader);

  const bool fail_during_create =
      sync_point_ != "BlobFileReader::GetBlob:TamperWithResult";

  if (fail_during_create) {
    ASSERT_TRUE(s.IsCorruption());
  } else {
    ASSERT_OK(s);

    PinnableSlice value;
    uint64_t bytes_read = 0;

    ASSERT_TRUE(reader
                    ->GetBlob(ReadOptions(), key, blob_offset, blob_size,
                              kNoCompression, &value, &bytes_read)
                    .IsCorruption());
    ASSERT_EQ(bytes_read, 0);
  }

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/blob/blob_file_reader.h"

#include <cassert>
#include <optional>
#include <string>

#include "db/blob/blob_contents.h"
#include "db/blob/blob_log_format.h"
#include "db/blob/blob_log_writer.h"
#include "db/db_test_util.h"
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

// Common test constants
constexpr uint32_t kDefaultColumnFamilyId = 1;
constexpr bool kDefaultHasTtl = false;
constexpr uint64_t kDefaultBlobFileNumber = 1;
constexpr HistogramImpl* kNullBlobFileReadHist = nullptr;
constexpr FilePrefetchBuffer* kNullPrefetchBuffer = nullptr;
constexpr MemoryAllocator* kNullAllocator = nullptr;

// Test helper: Create BlobFileReader without compression manager
Status CreateBlobFileReader(const ImmutableOptions& immutable_options,
                            const ReadOptions& read_options,
                            const FileOptions& file_options,
                            uint32_t column_family_id,
                            HistogramImpl* blob_file_read_hist,
                            uint64_t blob_file_number,
                            const std::shared_ptr<IOTracer>& io_tracer,
                            std::unique_ptr<BlobFileReader>* reader) {
  return BlobFileReader::Create(immutable_options, read_options, file_options,
                                column_family_id, blob_file_read_hist,
                                blob_file_number, io_tracer, nullptr, reader);
}

// Creates a test blob file with `num` blobs in it.
void WriteBlobFile(const ImmutableOptions& immutable_options,
                   uint32_t column_family_id, bool has_ttl,
                   const ExpirationRange& expiration_range_header,
                   const ExpirationRange& expiration_range_footer,
                   uint64_t blob_file_number, const std::vector<Slice>& keys,
                   const std::vector<Slice>& blobs, CompressionType compression,
                   std::vector<uint64_t>& blob_offsets,
                   std::vector<uint64_t>& blob_sizes) {
  assert(!immutable_options.cf_paths.empty());
  size_t num = keys.size();
  assert(num == blobs.size());
  assert(num == blob_offsets.size());
  assert(num == blob_sizes.size());

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

  BlobLogHeader header(column_family_id, compression, has_ttl,
                       expiration_range_header);

  ASSERT_OK(blob_log_writer.WriteHeader(WriteOptions(), header));

  std::vector<GrowableBuffer> compressed_blobs(num);
  std::vector<Slice> blobs_to_write(num);
  if (kNoCompression == compression) {
    for (size_t i = 0; i < num; ++i) {
      blobs_to_write[i] = blobs[i];
      blob_sizes[i] = blobs[i].size();
    }
  } else {
    auto compressor =
        GetBuiltinV2CompressionManager()->GetCompressor({}, compression);

    for (size_t i = 0; i < num; ++i) {
      ASSERT_OK(LegacyForceBuiltinCompression(*compressor,
                                              /*working_area=*/nullptr,
                                              blobs[i], &compressed_blobs[i]));
      blobs_to_write[i] = compressed_blobs[i];
      blob_sizes[i] = compressed_blobs[i].size();
    }
  }

  for (size_t i = 0; i < num; ++i) {
    uint64_t key_offset = 0;
    ASSERT_OK(blob_log_writer.AddRecord(WriteOptions(), keys[i],
                                        blobs_to_write[i], &key_offset,
                                        &blob_offsets[i]));
  }

  BlobLogFooter footer;
  footer.blob_count = num;
  footer.expiration_range = expiration_range_footer;

  std::string checksum_method;
  std::string checksum_value;
  ASSERT_OK(blob_log_writer.AppendFooter(WriteOptions(), footer,
                                         &checksum_method, &checksum_value));
}

// Creates a test blob file with a single blob in it. Note: this method
// makes it possible to test various corner cases by allowing the caller
// to specify the contents of various blob file header/footer fields.
void WriteBlobFile(const ImmutableOptions& immutable_options,
                   uint32_t column_family_id, bool has_ttl,
                   const ExpirationRange& expiration_range_header,
                   const ExpirationRange& expiration_range_footer,
                   uint64_t blob_file_number, const Slice& key,
                   const Slice& blob, CompressionType compression,
                   uint64_t* blob_offset, uint64_t* blob_size) {
  std::vector<Slice> keys{key};
  std::vector<Slice> blobs{blob};
  std::vector<uint64_t> blob_offsets{0};
  std::vector<uint64_t> blob_sizes{0};
  WriteBlobFile(immutable_options, column_family_id, has_ttl,
                expiration_range_header, expiration_range_footer,
                blob_file_number, keys, blobs, compression, blob_offsets,
                blob_sizes);
  if (blob_offset) {
    *blob_offset = blob_offsets[0];
  }
  if (blob_size) {
    *blob_size = blob_sizes[0];
  }
}

}  // anonymous namespace

// Helper class for setting up blob file reader tests with common functionality
class BlobFileReaderTestBase : public testing::Test {
 protected:
  BlobFileReaderTestBase() { mock_env_.reset(MockEnv::Create(Env::Default())); }

  // Initialize options with the given test name for the path
  void InitOptions(const std::string& test_name) {
    options_.env = mock_env_.get();
    options_.cf_paths.emplace_back(
        test::PerThreadDBPath(mock_env_.get(), test_name), 0);
    options_.enable_blob_files = true;
    immutable_options_.emplace(options_);
  }

  // Write a single blob file and create a reader
  void WriteSingleBlobAndCreateReader(const Slice& key, const Slice& blob,
                                      CompressionType compression) {
    ASSERT_TRUE(immutable_options_.has_value());
    WriteBlobFile(*immutable_options_, kDefaultColumnFamilyId, kDefaultHasTtl,
                  ExpirationRange(), ExpirationRange(), kDefaultBlobFileNumber,
                  key, blob, compression, &blob_offset_, &blob_size_);

    ReadOptions read_options;
    ASSERT_OK(CreateBlobFileReader(
        *immutable_options_, read_options, FileOptions(),
        kDefaultColumnFamilyId, kNullBlobFileReadHist, kDefaultBlobFileNumber,
        nullptr /*IOTracer*/, &reader_));
  }

  // Write multiple blobs and create a reader
  void WriteMultipleBlobsAndCreateReader(const std::vector<Slice>& keys,
                                         const std::vector<Slice>& blobs,
                                         CompressionType compression) {
    ASSERT_TRUE(immutable_options_.has_value());
    blob_offsets_.resize(keys.size());
    blob_sizes_.resize(keys.size());
    WriteBlobFile(*immutable_options_, kDefaultColumnFamilyId, kDefaultHasTtl,
                  ExpirationRange(), ExpirationRange(), kDefaultBlobFileNumber,
                  keys, blobs, compression, blob_offsets_, blob_sizes_);

    ReadOptions read_options;
    ASSERT_OK(CreateBlobFileReader(
        *immutable_options_, read_options, FileOptions(),
        kDefaultColumnFamilyId, kNullBlobFileReadHist, kDefaultBlobFileNumber,
        nullptr /*IOTracer*/, &reader_));
  }

  std::unique_ptr<Env> mock_env_;
  Options options_;
  std::optional<ImmutableOptions> immutable_options_;
  std::unique_ptr<BlobFileReader> reader_;

  // For single blob tests
  uint64_t blob_offset_ = 0;
  uint64_t blob_size_ = 0;

  // For multiple blob tests
  std::vector<uint64_t> blob_offsets_;
  std::vector<uint64_t> blob_sizes_;
};

class BlobFileReaderTest : public BlobFileReaderTestBase {};

TEST_F(BlobFileReaderTest, CreateReaderAndGetBlob) {
  Options options;
  options.env = mock_env_.get();
  options.cf_paths.emplace_back(
      test::PerThreadDBPath(mock_env_.get(),
                            "BlobFileReaderTest_CreateReaderAndGetBlob"),
      0);
  options.enable_blob_files = true;

  ImmutableOptions immutable_options(options);

  constexpr uint32_t column_family_id = 1;
  constexpr bool has_ttl = false;
  constexpr ExpirationRange expiration_range;
  constexpr uint64_t blob_file_number = 1;
  constexpr size_t num_blobs = 3;
  const std::vector<std::string> key_strs = {"key1", "key2", "key3"};
  const std::vector<std::string> blob_strs = {"blob1", "blob2", "blob3"};

  const std::vector<Slice> keys = {key_strs[0], key_strs[1], key_strs[2]};
  const std::vector<Slice> blobs = {blob_strs[0], blob_strs[1], blob_strs[2]};

  std::vector<uint64_t> blob_offsets(keys.size());
  std::vector<uint64_t> blob_sizes(keys.size());

  WriteBlobFile(immutable_options, column_family_id, has_ttl, expiration_range,
                expiration_range, blob_file_number, keys, blobs, kNoCompression,
                blob_offsets, blob_sizes);

  constexpr HistogramImpl* blob_file_read_hist = nullptr;

  std::unique_ptr<BlobFileReader> reader;

  ReadOptions read_options;
  ASSERT_OK(CreateBlobFileReader(
      immutable_options, read_options, FileOptions(), column_family_id,
      blob_file_read_hist, blob_file_number, nullptr /*IOTracer*/, &reader));

  // Make sure the blob can be retrieved with and without checksum verification
  read_options.verify_checksums = false;

  constexpr FilePrefetchBuffer* prefetch_buffer = nullptr;
  constexpr MemoryAllocator* allocator = nullptr;

  {
    std::unique_ptr<BlobContents> value;
    uint64_t bytes_read = 0;

    ASSERT_OK(reader->GetBlob(read_options, keys[0], blob_offsets[0],
                              blob_sizes[0], kNoCompression, prefetch_buffer,
                              allocator, &value, &bytes_read));
    ASSERT_NE(value, nullptr);
    ASSERT_EQ(value->data(), blobs[0]);
    ASSERT_EQ(bytes_read, blob_sizes[0]);

    // MultiGetBlob
    bytes_read = 0;
    size_t total_size = 0;

    std::array<Status, num_blobs> statuses_buf;
    std::array<BlobReadRequest, num_blobs> requests_buf;
    autovector<std::pair<BlobReadRequest*, std::unique_ptr<BlobContents>>>
        blob_reqs;

    for (size_t i = 0; i < num_blobs; ++i) {
      requests_buf[i] =
          BlobReadRequest(keys[i], blob_offsets[i], blob_sizes[i],
                          kNoCompression, nullptr, &statuses_buf[i]);
      blob_reqs.emplace_back(&requests_buf[i], std::unique_ptr<BlobContents>());
    }

    reader->MultiGetBlob(read_options, allocator, blob_reqs, &bytes_read);

    for (size_t i = 0; i < num_blobs; ++i) {
      const auto& result = blob_reqs[i].second;

      ASSERT_OK(statuses_buf[i]);
      ASSERT_NE(result, nullptr);
      ASSERT_EQ(result->data(), blobs[i]);
      total_size += blob_sizes[i];
    }
    ASSERT_EQ(bytes_read, total_size);
  }

  read_options.verify_checksums = true;

  {
    std::unique_ptr<BlobContents> value;
    uint64_t bytes_read = 0;

    ASSERT_OK(reader->GetBlob(read_options, keys[1], blob_offsets[1],
                              blob_sizes[1], kNoCompression, prefetch_buffer,
                              allocator, &value, &bytes_read));
    ASSERT_NE(value, nullptr);
    ASSERT_EQ(value->data(), blobs[1]);

    const uint64_t key_size = keys[1].size();
    ASSERT_EQ(bytes_read,
              BlobLogRecord::CalculateAdjustmentForRecordHeader(key_size) +
                  blob_sizes[1]);
  }

  // Invalid offset (too close to start of file)
  {
    std::unique_ptr<BlobContents> value;
    uint64_t bytes_read = 0;

    ASSERT_TRUE(reader
                    ->GetBlob(read_options, keys[0], blob_offsets[0] - 1,
                              blob_sizes[0], kNoCompression, prefetch_buffer,
                              allocator, &value, &bytes_read)
                    .IsCorruption());
    ASSERT_EQ(value, nullptr);
    ASSERT_EQ(bytes_read, 0);
  }

  // Invalid offset (too close to end of file)
  {
    std::unique_ptr<BlobContents> value;
    uint64_t bytes_read = 0;

    ASSERT_TRUE(reader
                    ->GetBlob(read_options, keys[2], blob_offsets[2] + 1,
                              blob_sizes[2], kNoCompression, prefetch_buffer,
                              allocator, &value, &bytes_read)
                    .IsCorruption());
    ASSERT_EQ(value, nullptr);
    ASSERT_EQ(bytes_read, 0);
  }

  // Incorrect compression type
  {
    std::unique_ptr<BlobContents> value;
    uint64_t bytes_read = 0;

    ASSERT_TRUE(reader
                    ->GetBlob(read_options, keys[0], blob_offsets[0],
                              blob_sizes[0], kZSTD, prefetch_buffer, allocator,
                              &value, &bytes_read)
                    .IsCorruption());
    ASSERT_EQ(value, nullptr);
    ASSERT_EQ(bytes_read, 0);
  }

  // Incorrect key size
  {
    constexpr char shorter_key[] = "k";
    std::unique_ptr<BlobContents> value;
    uint64_t bytes_read = 0;

    ASSERT_TRUE(reader
                    ->GetBlob(read_options, shorter_key,
                              blob_offsets[0] -
                                  (keys[0].size() - sizeof(shorter_key) + 1),
                              blob_sizes[0], kNoCompression, prefetch_buffer,
                              allocator, &value, &bytes_read)
                    .IsCorruption());
    ASSERT_EQ(value, nullptr);
    ASSERT_EQ(bytes_read, 0);

    // MultiGetBlob
    autovector<std::reference_wrapper<const Slice>> key_refs;
    for (const auto& key_ref : keys) {
      key_refs.emplace_back(std::cref(key_ref));
    }
    Slice shorter_key_slice(shorter_key, sizeof(shorter_key) - 1);
    key_refs[1] = std::cref(shorter_key_slice);

    autovector<uint64_t> offsets{
        blob_offsets[0],
        blob_offsets[1] - (keys[1].size() - key_refs[1].get().size()),
        blob_offsets[2]};

    std::array<Status, num_blobs> statuses_buf;
    std::array<BlobReadRequest, num_blobs> requests_buf;
    autovector<std::pair<BlobReadRequest*, std::unique_ptr<BlobContents>>>
        blob_reqs;

    for (size_t i = 0; i < num_blobs; ++i) {
      requests_buf[i] =
          BlobReadRequest(key_refs[i], offsets[i], blob_sizes[i],
                          kNoCompression, nullptr, &statuses_buf[i]);
      blob_reqs.emplace_back(&requests_buf[i], std::unique_ptr<BlobContents>());
    }

    reader->MultiGetBlob(read_options, allocator, blob_reqs, &bytes_read);

    for (size_t i = 0; i < num_blobs; ++i) {
      if (i == 1) {
        ASSERT_TRUE(statuses_buf[i].IsCorruption());
      } else {
        ASSERT_OK(statuses_buf[i]);
      }
    }
  }

  // Incorrect key
  {
    constexpr char incorrect_key[] = "foo1";
    std::unique_ptr<BlobContents> value;
    uint64_t bytes_read = 0;

    ASSERT_TRUE(reader
                    ->GetBlob(read_options, incorrect_key, blob_offsets[0],
                              blob_sizes[0], kNoCompression, prefetch_buffer,
                              allocator, &value, &bytes_read)
                    .IsCorruption());
    ASSERT_EQ(value, nullptr);
    ASSERT_EQ(bytes_read, 0);

    // MultiGetBlob
    autovector<std::reference_wrapper<const Slice>> key_refs;
    for (const auto& key_ref : keys) {
      key_refs.emplace_back(std::cref(key_ref));
    }
    Slice wrong_key_slice(incorrect_key, sizeof(incorrect_key) - 1);
    key_refs[2] = std::cref(wrong_key_slice);

    std::array<Status, num_blobs> statuses_buf;
    std::array<BlobReadRequest, num_blobs> requests_buf;
    autovector<std::pair<BlobReadRequest*, std::unique_ptr<BlobContents>>>
        blob_reqs;

    for (size_t i = 0; i < num_blobs; ++i) {
      requests_buf[i] =
          BlobReadRequest(key_refs[i], blob_offsets[i], blob_sizes[i],
                          kNoCompression, nullptr, &statuses_buf[i]);
      blob_reqs.emplace_back(&requests_buf[i], std::unique_ptr<BlobContents>());
    }

    reader->MultiGetBlob(read_options, allocator, blob_reqs, &bytes_read);

    for (size_t i = 0; i < num_blobs; ++i) {
      if (i == num_blobs - 1) {
        ASSERT_TRUE(statuses_buf[i].IsCorruption());
      } else {
        ASSERT_OK(statuses_buf[i]);
      }
    }
  }

  // Incorrect value size
  {
    std::unique_ptr<BlobContents> value;
    uint64_t bytes_read = 0;

    ASSERT_TRUE(reader
                    ->GetBlob(read_options, keys[1], blob_offsets[1],
                              blob_sizes[1] + 1, kNoCompression,
                              prefetch_buffer, allocator, &value, &bytes_read)
                    .IsCorruption());
    ASSERT_EQ(value, nullptr);
    ASSERT_EQ(bytes_read, 0);

    // MultiGetBlob
    autovector<std::reference_wrapper<const Slice>> key_refs;
    for (const auto& key_ref : keys) {
      key_refs.emplace_back(std::cref(key_ref));
    }

    std::array<Status, num_blobs> statuses_buf;
    std::array<BlobReadRequest, num_blobs> requests_buf;

    requests_buf[0] =
        BlobReadRequest(key_refs[0], blob_offsets[0], blob_sizes[0],
                        kNoCompression, nullptr, statuses_buf.data());
    requests_buf[1] =
        BlobReadRequest(key_refs[1], blob_offsets[1], blob_sizes[1] + 1,
                        kNoCompression, nullptr, &statuses_buf[1]);
    requests_buf[2] =
        BlobReadRequest(key_refs[2], blob_offsets[2], blob_sizes[2],
                        kNoCompression, nullptr, &statuses_buf[2]);

    autovector<std::pair<BlobReadRequest*, std::unique_ptr<BlobContents>>>
        blob_reqs;

    for (size_t i = 0; i < num_blobs; ++i) {
      blob_reqs.emplace_back(&requests_buf[i], std::unique_ptr<BlobContents>());
    }

    reader->MultiGetBlob(read_options, allocator, blob_reqs, &bytes_read);

    for (size_t i = 0; i < num_blobs; ++i) {
      if (i != 1) {
        ASSERT_OK(statuses_buf[i]);
      } else {
        ASSERT_TRUE(statuses_buf[i].IsCorruption());
      }
    }
  }
}

TEST_F(BlobFileReaderTest, Malformed) {
  // Write a blob file consisting of nothing but a header, and make sure we
  // detect the error when we open it for reading

  Options options;
  options.env = mock_env_.get();
  options.cf_paths.emplace_back(
      test::PerThreadDBPath(mock_env_.get(), "BlobFileReaderTest_Malformed"),
      0);
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

    ASSERT_OK(blob_log_writer.WriteHeader(WriteOptions(), header));
  }

  constexpr HistogramImpl* blob_file_read_hist = nullptr;

  std::unique_ptr<BlobFileReader> reader;
  const ReadOptions read_options;
  ASSERT_TRUE(CreateBlobFileReader(immutable_options, read_options,
                                   FileOptions(), column_family_id,
                                   blob_file_read_hist, blob_file_number,
                                   nullptr /*IOTracer*/, &reader)
                  .IsCorruption());
}

TEST_F(BlobFileReaderTest, TTL) {
  Options options;
  options.env = mock_env_.get();
  options.cf_paths.emplace_back(
      test::PerThreadDBPath(mock_env_.get(), "BlobFileReaderTest_TTL"), 0);
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
  const ReadOptions read_options;
  ASSERT_TRUE(CreateBlobFileReader(immutable_options, read_options,
                                   FileOptions(), column_family_id,
                                   blob_file_read_hist, blob_file_number,
                                   nullptr /*IOTracer*/, &reader)
                  .IsCorruption());
}

TEST_F(BlobFileReaderTest, ExpirationRangeInHeader) {
  Options options;
  options.env = mock_env_.get();
  options.cf_paths.emplace_back(
      test::PerThreadDBPath(mock_env_.get(),
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
  const ReadOptions read_options;
  ASSERT_TRUE(CreateBlobFileReader(immutable_options, read_options,
                                   FileOptions(), column_family_id,
                                   blob_file_read_hist, blob_file_number,
                                   nullptr /*IOTracer*/, &reader)
                  .IsCorruption());
}

TEST_F(BlobFileReaderTest, ExpirationRangeInFooter) {
  Options options;
  options.env = mock_env_.get();
  options.cf_paths.emplace_back(
      test::PerThreadDBPath(mock_env_.get(),
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
  const ReadOptions read_options;
  ASSERT_TRUE(CreateBlobFileReader(immutable_options, read_options,
                                   FileOptions(), column_family_id,
                                   blob_file_read_hist, blob_file_number,
                                   nullptr /*IOTracer*/, &reader)
                  .IsCorruption());
}

TEST_F(BlobFileReaderTest, IncorrectColumnFamily) {
  Options options;
  options.env = mock_env_.get();
  options.cf_paths.emplace_back(
      test::PerThreadDBPath(mock_env_.get(),
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
  const ReadOptions read_options;
  ASSERT_TRUE(CreateBlobFileReader(immutable_options, read_options,
                                   FileOptions(), incorrect_column_family_id,
                                   blob_file_read_hist, blob_file_number,
                                   nullptr /*IOTracer*/, &reader)
                  .IsCorruption());
}

TEST_F(BlobFileReaderTest, BlobCRCError) {
  Options options;
  options.env = mock_env_.get();
  options.cf_paths.emplace_back(
      test::PerThreadDBPath(mock_env_.get(), "BlobFileReaderTest_BlobCRCError"),
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
  const ReadOptions read_options;
  ASSERT_OK(CreateBlobFileReader(
      immutable_options, read_options, FileOptions(), column_family_id,
      blob_file_read_hist, blob_file_number, nullptr /*IOTracer*/, &reader));

  SyncPoint::GetInstance()->SetCallBack(
      "BlobFileReader::VerifyBlob:CheckBlobCRC", [](void* arg) {
        BlobLogRecord* const record = static_cast<BlobLogRecord*>(arg);
        assert(record);

        record->blob_crc = 0xfaceb00c;
      });

  SyncPoint::GetInstance()->EnableProcessing();

  constexpr FilePrefetchBuffer* prefetch_buffer = nullptr;
  constexpr MemoryAllocator* allocator = nullptr;

  std::unique_ptr<BlobContents> value;
  uint64_t bytes_read = 0;

  ASSERT_TRUE(reader
                  ->GetBlob(ReadOptions(), key, blob_offset, blob_size,
                            kNoCompression, prefetch_buffer, allocator, &value,
                            &bytes_read)
                  .IsCorruption());
  ASSERT_EQ(value, nullptr);
  ASSERT_EQ(bytes_read, 0);

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
}

TEST_F(BlobFileReaderTest, Compression) {
  if (!Snappy_Supported()) {
    return;
  }

  Options options;
  options.env = mock_env_.get();
  options.cf_paths.emplace_back(
      test::PerThreadDBPath(mock_env_.get(), "BlobFileReaderTest_Compression"),
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
  ReadOptions read_options;
  ASSERT_OK(CreateBlobFileReader(
      immutable_options, read_options, FileOptions(), column_family_id,
      blob_file_read_hist, blob_file_number, nullptr /*IOTracer*/, &reader));

  // Make sure the blob can be retrieved with and without checksum verification
  read_options.verify_checksums = false;

  constexpr FilePrefetchBuffer* prefetch_buffer = nullptr;
  constexpr MemoryAllocator* allocator = nullptr;

  {
    std::unique_ptr<BlobContents> value;
    uint64_t bytes_read = 0;

    ASSERT_OK(reader->GetBlob(read_options, key, blob_offset, blob_size,
                              kSnappyCompression, prefetch_buffer, allocator,
                              &value, &bytes_read));
    ASSERT_NE(value, nullptr);
    ASSERT_EQ(value->data(), blob);
    ASSERT_EQ(bytes_read, blob_size);
  }

  read_options.verify_checksums = true;

  {
    std::unique_ptr<BlobContents> value;
    uint64_t bytes_read = 0;

    ASSERT_OK(reader->GetBlob(read_options, key, blob_offset, blob_size,
                              kSnappyCompression, prefetch_buffer, allocator,
                              &value, &bytes_read));
    ASSERT_NE(value, nullptr);
    ASSERT_EQ(value->data(), blob);

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
  options.env = mock_env_.get();
  options.cf_paths.emplace_back(
      test::PerThreadDBPath(mock_env_.get(),
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
  const ReadOptions read_options;
  ASSERT_OK(CreateBlobFileReader(
      immutable_options, read_options, FileOptions(), column_family_id,
      blob_file_read_hist, blob_file_number, nullptr /*IOTracer*/, &reader));

  SyncPoint::GetInstance()->SetCallBack(
      "BlobFileReader::UncompressBlobIfNeeded:TamperWithResult", [](void* arg) {
        auto* result = static_cast<Status*>(arg);
        assert(result);

        *result = Status::Corruption("Injected result");
      });

  SyncPoint::GetInstance()->EnableProcessing();

  constexpr FilePrefetchBuffer* prefetch_buffer = nullptr;
  constexpr MemoryAllocator* allocator = nullptr;

  std::unique_ptr<BlobContents> value;
  uint64_t bytes_read = 0;

  ASSERT_EQ(reader
                ->GetBlob(ReadOptions(), key, blob_offset, blob_size,
                          kSnappyCompression, prefetch_buffer, allocator,
                          &value, &bytes_read)
                .code(),
            Status::Code::kCorruption);
  ASSERT_EQ(value, nullptr);
  ASSERT_EQ(bytes_read, 0);

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
}

TEST_F(BlobFileReaderTest, GetBlob_CompressedRead_Snappy) {
  // Test reading compressed blob with read_blob_compressed=true
  // The returned data should be the raw compressed bytes, not decompressed
  if (!Snappy_Supported()) {
    return;
  }

  InitOptions("BlobFileReaderTest_GetBlob_CompressedRead_Snappy");

  constexpr char key[] = "key";
  // Use a larger blob that will actually compress well
  const std::string blob(1024, 'x');  // 1KB of repeated 'x' characters

  WriteSingleBlobAndCreateReader(key, blob, kSnappyCompression);

  // Read with read_blob_compressed=true
  ReadOptions read_options;
  read_options.read_blob_compressed = true;
  read_options.verify_checksums = false;
  std::optional<CompressionType> compression_type_out;

  std::unique_ptr<BlobContents> value;
  uint64_t bytes_read = 0;

  ASSERT_OK(reader_->GetBlob(read_options, key, blob_offset_, blob_size_,
                             kSnappyCompression, kNullPrefetchBuffer,
                             kNullAllocator, &value, &bytes_read,
                             &compression_type_out));
  ASSERT_NE(value, nullptr);
  // Verify compression type is reported correctly
  ASSERT_TRUE(compression_type_out.has_value());
  ASSERT_EQ(compression_type_out.value(), kSnappyCompression);
  // Without checksum verification, bytes_read equals blob_size
  ASSERT_EQ(bytes_read, blob_size_);

  VerifySnappyCompressedData(value->data(), blob);
}

TEST_F(BlobFileReaderTest, GetBlob_CompressedRead_NoCompression) {
  // Test reading uncompressed blob with read_blob_compressed=true
  // Should return data as-is with compression_type = kNoCompression
  InitOptions("BlobFileReaderTest_GetBlob_CompressedRead_NoCompression");

  constexpr char key[] = "key";
  constexpr char blob[] = "blob";

  WriteSingleBlobAndCreateReader(key, blob, kNoCompression);

  // Read with read_blob_compressed=true
  ReadOptions read_options;
  read_options.read_blob_compressed = true;
  read_options.verify_checksums = false;
  std::optional<CompressionType> compression_type_out;

  std::unique_ptr<BlobContents> value;
  uint64_t bytes_read = 0;

  ASSERT_OK(reader_->GetBlob(read_options, key, blob_offset_, blob_size_,
                             kNoCompression, kNullPrefetchBuffer,
                             kNullAllocator, &value, &bytes_read,
                             &compression_type_out));
  ASSERT_NE(value, nullptr);
  // Verify compression type is reported as kNoCompression
  ASSERT_TRUE(compression_type_out.has_value());
  ASSERT_EQ(compression_type_out.value(), kNoCompression);
  // Verify the data is exactly the original blob
  ASSERT_EQ(value->data(), blob);
  // Without checksum verification, bytes_read equals blob_size
  ASSERT_EQ(bytes_read, blob_size_);
}

TEST_F(BlobFileReaderTest, GetBlob_CompressedRead_ChecksumVerification) {
  // Test that checksum verification still works when read_blob_compressed=true
  if (!Snappy_Supported()) {
    return;
  }

  InitOptions("BlobFileReaderTest_GetBlob_CompressedRead_ChecksumVerification");

  constexpr char key[] = "key";
  const std::string blob(1024, 'y');

  WriteSingleBlobAndCreateReader(key, blob, kSnappyCompression);

  // Read with both read_blob_compressed=true and verify_checksums=true
  ReadOptions read_options;
  read_options.read_blob_compressed = true;
  read_options.verify_checksums = true;
  std::optional<CompressionType> compression_type_out;

  std::unique_ptr<BlobContents> value;
  uint64_t bytes_read = 0;

  ASSERT_OK(reader_->GetBlob(read_options, key, blob_offset_, blob_size_,
                             kSnappyCompression, kNullPrefetchBuffer,
                             kNullAllocator, &value, &bytes_read,
                             &compression_type_out));
  ASSERT_NE(value, nullptr);
  ASSERT_TRUE(compression_type_out.has_value());
  ASSERT_EQ(compression_type_out.value(), kSnappyCompression);

  // Verify we read the full record (with header for checksum verification)
  const uint64_t key_size = sizeof(key) - 1;
  ASSERT_EQ(
      bytes_read,
      BlobLogRecord::CalculateAdjustmentForRecordHeader(key_size) + blob_size_);
}

TEST_F(BlobFileReaderTest, MultiGetBlob_CompressedRead) {
  // Test MultiGetBlob with read_blob_compressed=true
  if (!Snappy_Supported()) {
    return;
  }

  InitOptions("BlobFileReaderTest_MultiGetBlob_CompressedRead");

  constexpr size_t num_blobs = 3;
  const std::vector<std::string> key_strs = {"key1", "key2", "key3"};
  // Use larger blobs that compress well
  const std::vector<std::string> blob_strs = {
      std::string(1024, 'a'), std::string(2048, 'b'), std::string(512, 'c')};

  const std::vector<Slice> keys = {key_strs[0], key_strs[1], key_strs[2]};
  const std::vector<Slice> blobs = {blob_strs[0], blob_strs[1], blob_strs[2]};

  WriteMultipleBlobsAndCreateReader(keys, blobs, kSnappyCompression);

  // Read with read_blob_compressed=true
  ReadOptions read_options;
  read_options.read_blob_compressed = true;
  uint64_t bytes_read = 0;

  std::array<Status, num_blobs> statuses_buf;
  std::array<std::optional<CompressionType>, num_blobs> compression_types_out;
  std::array<BlobReadRequest, num_blobs> requests_buf;
  autovector<std::pair<BlobReadRequest*, std::unique_ptr<BlobContents>>>
      blob_reqs;

  for (size_t i = 0; i < num_blobs; ++i) {
    compression_types_out[i] = std::nullopt;
    requests_buf[i] = BlobReadRequest(
        keys[i], blob_offsets_[i], blob_sizes_[i], kSnappyCompression, nullptr,
        &statuses_buf[i], &compression_types_out[i]);
    blob_reqs.emplace_back(&requests_buf[i], std::unique_ptr<BlobContents>());
  }

  reader_->MultiGetBlob(read_options, kNullAllocator, blob_reqs, &bytes_read);

  for (size_t i = 0; i < num_blobs; ++i) {
    ASSERT_OK(statuses_buf[i]);
    const auto& result = blob_reqs[i].second;
    ASSERT_NE(result, nullptr);
    // Verify compression type is reported correctly
    ASSERT_TRUE(compression_types_out[i].has_value());
    ASSERT_EQ(compression_types_out[i].value(), kSnappyCompression);

    VerifySnappyCompressedData(result->data(), blob_strs[i]);
  }
}

class BlobFileReaderIOErrorTest
    : public testing::Test,
      public testing::WithParamInterface<std::string> {
 protected:
  BlobFileReaderIOErrorTest() : sync_point_(GetParam()) {
    mock_env_.reset(MockEnv::Create(Env::Default()));
    fault_injection_env_.reset(new FaultInjectionTestEnv(mock_env_.get()));
  }

  std::unique_ptr<Env> mock_env_;
  std::unique_ptr<FaultInjectionTestEnv> fault_injection_env_;
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
  options.env = fault_injection_env_.get();
  options.cf_paths.emplace_back(
      test::PerThreadDBPath(fault_injection_env_.get(),
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
    fault_injection_env_->SetFilesystemActive(false,
                                              Status::IOError(sync_point_));
  });
  SyncPoint::GetInstance()->EnableProcessing();

  constexpr HistogramImpl* blob_file_read_hist = nullptr;

  std::unique_ptr<BlobFileReader> reader;
  const ReadOptions read_options;
  const Status s = CreateBlobFileReader(
      immutable_options, read_options, FileOptions(), column_family_id,
      blob_file_read_hist, blob_file_number, nullptr /*IOTracer*/, &reader);

  const bool fail_during_create =
      (sync_point_ != "BlobFileReader::GetBlob:ReadFromFile");

  if (fail_during_create) {
    ASSERT_TRUE(s.IsIOError());
  } else {
    ASSERT_OK(s);

    constexpr FilePrefetchBuffer* prefetch_buffer = nullptr;
    constexpr MemoryAllocator* allocator = nullptr;

    std::unique_ptr<BlobContents> value;
    uint64_t bytes_read = 0;

    ASSERT_TRUE(reader
                    ->GetBlob(ReadOptions(), key, blob_offset, blob_size,
                              kNoCompression, prefetch_buffer, allocator,
                              &value, &bytes_read)
                    .IsIOError());
    ASSERT_EQ(value, nullptr);
    ASSERT_EQ(bytes_read, 0);
  }

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
}

class BlobFileReaderDecodingErrorTest
    : public testing::Test,
      public testing::WithParamInterface<std::string> {
 protected:
  BlobFileReaderDecodingErrorTest() : sync_point_(GetParam()) {
    mock_env_.reset(MockEnv::Create(Env::Default()));
  }

  std::unique_ptr<Env> mock_env_;
  std::string sync_point_;
};

INSTANTIATE_TEST_CASE_P(BlobFileReaderTest, BlobFileReaderDecodingErrorTest,
                        ::testing::ValuesIn(std::vector<std::string>{
                            "BlobFileReader::ReadHeader:TamperWithResult",
                            "BlobFileReader::ReadFooter:TamperWithResult",
                            "BlobFileReader::GetBlob:TamperWithResult"}));

TEST_P(BlobFileReaderDecodingErrorTest, DecodingError) {
  Options options;
  options.env = mock_env_.get();
  options.cf_paths.emplace_back(
      test::PerThreadDBPath(mock_env_.get(),
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
  const ReadOptions read_options;
  const Status s = CreateBlobFileReader(
      immutable_options, read_options, FileOptions(), column_family_id,
      blob_file_read_hist, blob_file_number, nullptr /*IOTracer*/, &reader);

  const bool fail_during_create =
      sync_point_ != "BlobFileReader::GetBlob:TamperWithResult";

  if (fail_during_create) {
    ASSERT_TRUE(s.IsCorruption());
  } else {
    ASSERT_OK(s);

    constexpr FilePrefetchBuffer* prefetch_buffer = nullptr;
    constexpr MemoryAllocator* allocator = nullptr;

    std::unique_ptr<BlobContents> value;
    uint64_t bytes_read = 0;

    ASSERT_TRUE(reader
                    ->GetBlob(ReadOptions(), key, blob_offset, blob_size,
                              kNoCompression, prefetch_buffer, allocator,
                              &value, &bytes_read)
                    .IsCorruption());
    ASSERT_EQ(value, nullptr);
    ASSERT_EQ(bytes_read, 0);
  }

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Tests for PartitionedLogWriter and PartitionedLogReader.

#include <atomic>
#include <thread>
#include <vector>

#include "db/partitioned_log_reader.h"
#include "db/partitioned_log_writer.h"
#include "file/random_access_file_reader.h"
#include "file/sequence_file_reader.h"
#include "file/writable_file_writer.h"
#include "rocksdb/env.h"
#include "test_util/testharness.h"
#include "test_util/testutil.h"
#include "util/coding.h"
#include "util/crc32c.h"

namespace ROCKSDB_NAMESPACE::log {

class PartitionedLogTest : public ::testing::Test {
 protected:
  std::string contents_;
  Slice reader_contents_;
  test::StringSink* sink_;

  void SetUp() override {
    contents_.clear();
    sink_ = new test::StringSink(&reader_contents_);
  }

  std::unique_ptr<PartitionedLogWriter> CreateWriter(
      uint64_t log_number = 1, uint32_t partition_id = 0) {
    std::unique_ptr<FSWritableFile> sink_holder(sink_);
    std::unique_ptr<WritableFileWriter> file_writer(new WritableFileWriter(
        std::move(sink_holder), "" /* file name */, FileOptions()));
    return std::make_unique<PartitionedLogWriter>(
        std::move(file_writer), log_number, partition_id,
        false /* recycle_log_files */);
  }

  // Parse a record from the given data starting at offset.
  // Returns true if successful, fills in type, body, and body_crc.
  bool ParseRecord(const std::string& data, size_t offset, uint8_t* type,
                   Slice* body, uint32_t* stored_crc) {
    if (offset + PartitionedLogWriter::kPartitionedHeaderSize > data.size()) {
      return false;
    }

    // Read header
    const char* header = data.data() + offset;
    *stored_crc = DecodeFixed32(header);
    uint32_t length = DecodeFixed32(header + 4);
    *type = static_cast<uint8_t>(header[8]);

    if (offset + PartitionedLogWriter::kPartitionedHeaderSize + length >
        data.size()) {
      return false;
    }

    *body = Slice(
        data.data() + offset + PartitionedLogWriter::kPartitionedHeaderSize,
        length);
    return true;
  }

  // Verify the CRC of a record
  bool VerifyRecordCRC(const std::string& data, size_t offset) {
    uint8_t type;
    Slice body;
    uint32_t stored_crc;
    if (!ParseRecord(data, offset, &type, &body, &stored_crc)) {
      return false;
    }

    // Compute expected CRC (type + payload)
    char t = static_cast<char>(type);
    uint32_t type_crc = crc32c::Value(&t, 1);
    uint32_t payload_crc = crc32c::Value(body.data(), body.size());
    uint32_t expected_crc =
        crc32c::Crc32cCombine(type_crc, payload_crc, body.size());
    expected_crc = crc32c::Mask(expected_crc);

    return stored_crc == expected_crc;
  }
};

TEST_F(PartitionedLogTest, BasicWriteAndRead) {
  auto writer = CreateWriter(123);

  // Write a simple body
  std::string body_data = "test write batch body data";
  PartitionedLogWriter::WriteResult result;
  IOStatus s = writer->WriteBody(WriteOptions(), Slice(body_data), 5, &result);
  ASSERT_OK(s);

  // Verify result fields
  EXPECT_EQ(result.wal_number, 123);
  EXPECT_EQ(result.offset, 0);  // First record starts at offset 0
  EXPECT_EQ(result.size,
            PartitionedLogWriter::kPartitionedHeaderSize + body_data.size());
  EXPECT_EQ(result.record_count, 5);

  // Verify CRC is computed correctly
  uint32_t expected_crc = crc32c::Value(body_data.data(), body_data.size());
  EXPECT_EQ(result.crc, expected_crc);

  // Verify file size
  EXPECT_EQ(writer->GetFileSize(),
            PartitionedLogWriter::kPartitionedHeaderSize + body_data.size());

  // Parse the written data
  const std::string& written = sink_->contents_;
  ASSERT_GE(written.size(),
            PartitionedLogWriter::kPartitionedHeaderSize + body_data.size());

  uint8_t type;
  Slice parsed_body;
  uint32_t stored_crc;
  ASSERT_TRUE(ParseRecord(written, 0, &type, &parsed_body, &stored_crc));

  EXPECT_EQ(type, PartitionedLogWriter::kPartitionedRecordTypeFull);
  EXPECT_EQ(parsed_body.ToString(), body_data);

  // Close the writer
  ASSERT_OK(writer->Close(WriteOptions()));
}

TEST_F(PartitionedLogTest, CRCComputation) {
  auto writer = CreateWriter(1);

  // Test with various data patterns
  std::vector<std::string> test_bodies = {
      "",                      // Empty body
      "a",                     // Single byte
      std::string(1000, 'x'),  // Larger data
      std::string(100, '\0'),  // Null bytes
      "\xff\xfe\xfd\xfc",      // High byte values
  };

  size_t expected_offset = 0;
  for (const auto& body : test_bodies) {
    PartitionedLogWriter::WriteResult result;
    IOStatus s = writer->WriteBody(WriteOptions(), Slice(body), 1, &result);
    ASSERT_OK(s);

    // Verify the CRC matches what we expect
    uint32_t expected_crc = crc32c::Value(body.data(), body.size());
    EXPECT_EQ(result.crc, expected_crc)
        << "CRC mismatch for body of size " << body.size();

    // Verify offset
    EXPECT_EQ(result.offset, expected_offset);

    // Verify the stored CRC in the file is correct
    ASSERT_TRUE(VerifyRecordCRC(sink_->contents_, expected_offset))
        << "Record CRC verification failed for body of size " << body.size();

    expected_offset +=
        PartitionedLogWriter::kPartitionedHeaderSize + body.size();
  }
}

TEST_F(PartitionedLogTest, ConcurrentWrites) {
  auto writer = CreateWriter(456);

  const int kNumThreads = 8;
  const int kWritesPerThread = 100;
  std::atomic<int> success_count{0};
  std::atomic<int> failure_count{0};

  std::vector<std::thread> threads;
  for (int t = 0; t < kNumThreads; t++) {
    threads.emplace_back([&, t]() {
      for (int i = 0; i < kWritesPerThread; i++) {
        std::string body = "thread_" + std::to_string(t) + "_write_" +
                           std::to_string(i) + "_body_data";
        PartitionedLogWriter::WriteResult result;
        IOStatus s = writer->WriteBody(WriteOptions(), Slice(body), 1, &result);
        if (s.ok()) {
          success_count++;
          // Verify basic result fields
          EXPECT_EQ(result.wal_number, 456);
          EXPECT_EQ(result.size,
                    PartitionedLogWriter::kPartitionedHeaderSize + body.size());
          EXPECT_EQ(result.record_count, 1);
          // Verify CRC
          uint32_t expected_crc = crc32c::Value(body.data(), body.size());
          EXPECT_EQ(result.crc, expected_crc);
        } else {
          failure_count++;
        }
      }
    });
  }

  for (auto& t : threads) {
    t.join();
  }

  // All writes should succeed
  EXPECT_EQ(success_count.load(), kNumThreads * kWritesPerThread);
  EXPECT_EQ(failure_count.load(), 0);

  // Verify we can parse all records from the file
  const std::string& written = sink_->contents_;
  size_t offset = 0;
  int record_count = 0;
  while (offset < written.size()) {
    uint8_t type;
    Slice body;
    uint32_t stored_crc;
    ASSERT_TRUE(ParseRecord(written, offset, &type, &body, &stored_crc))
        << "Failed to parse record at offset " << offset;
    EXPECT_EQ(type, PartitionedLogWriter::kPartitionedRecordTypeFull);
    ASSERT_TRUE(VerifyRecordCRC(written, offset))
        << "CRC verification failed at offset " << offset;

    offset += PartitionedLogWriter::kPartitionedHeaderSize + body.size();
    record_count++;
  }

  EXPECT_EQ(record_count, kNumThreads * kWritesPerThread);
}

TEST_F(PartitionedLogTest, MultipleWrites) {
  auto writer = CreateWriter(789);

  // Write multiple records
  std::vector<std::string> bodies = {
      "first body",
      "second body with more data",
      "third",
  };

  size_t expected_offset = 0;
  for (size_t i = 0; i < bodies.size(); i++) {
    PartitionedLogWriter::WriteResult result;
    IOStatus s = writer->WriteBody(WriteOptions(), Slice(bodies[i]),
                                   static_cast<uint32_t>(i + 1), &result);
    ASSERT_OK(s);

    EXPECT_EQ(result.offset, expected_offset);
    EXPECT_EQ(result.record_count, i + 1);

    expected_offset +=
        PartitionedLogWriter::kPartitionedHeaderSize + bodies[i].size();
  }

  // Verify file size matches expected
  EXPECT_EQ(writer->GetFileSize(), expected_offset);

  // Parse all records
  const std::string& written = sink_->contents_;
  size_t offset = 0;
  for (size_t i = 0; i < bodies.size(); i++) {
    uint8_t type;
    Slice body;
    uint32_t stored_crc;
    ASSERT_TRUE(ParseRecord(written, offset, &type, &body, &stored_crc));
    EXPECT_EQ(body.ToString(), bodies[i]);
    offset += PartitionedLogWriter::kPartitionedHeaderSize + body.size();
  }
}

TEST_F(PartitionedLogTest, SyncAndClose) {
  auto writer = CreateWriter(1);

  std::string body = "test data";
  PartitionedLogWriter::WriteResult result;
  ASSERT_OK(writer->WriteBody(WriteOptions(), Slice(body), 1, &result));

  // Sync should succeed
  ASSERT_OK(writer->Sync(WriteOptions()));

  // Close should succeed
  ASSERT_OK(writer->Close(WriteOptions()));

  // After close, file size should still be accessible (returns 0)
  EXPECT_EQ(writer->GetFileSize(), 0);

  // Double close should be safe
  ASSERT_OK(writer->Close(WriteOptions()));
}

TEST_F(PartitionedLogTest, EmptyBody) {
  auto writer = CreateWriter(1);

  // Writing an empty body should work
  std::string empty_body = "";
  PartitionedLogWriter::WriteResult result;
  ASSERT_OK(writer->WriteBody(WriteOptions(), Slice(empty_body), 0, &result));

  EXPECT_EQ(result.offset, 0);
  EXPECT_EQ(result.size, PartitionedLogWriter::kPartitionedHeaderSize);
  EXPECT_EQ(result.crc, crc32c::Value("", 0));
  EXPECT_EQ(result.record_count, 0);

  // Verify file contains just the header
  EXPECT_EQ(
      writer->GetFileSize(),
      static_cast<uint64_t>(PartitionedLogWriter::kPartitionedHeaderSize));
}

TEST_F(PartitionedLogTest, LargeBody) {
  auto writer = CreateWriter(1);

  // Write a large body (1MB)
  std::string large_body(1024 * 1024, 'L');
  PartitionedLogWriter::WriteResult result;
  ASSERT_OK(
      writer->WriteBody(WriteOptions(), Slice(large_body), 1000, &result));

  EXPECT_EQ(result.offset, 0);
  EXPECT_EQ(result.size,
            PartitionedLogWriter::kPartitionedHeaderSize + large_body.size());
  EXPECT_EQ(result.record_count, 1000);

  // Verify CRC
  uint32_t expected_crc = crc32c::Value(large_body.data(), large_body.size());
  EXPECT_EQ(result.crc, expected_crc);

  // Verify record is parseable
  ASSERT_TRUE(VerifyRecordCRC(sink_->contents_, 0));
}

TEST_F(PartitionedLogTest, GetLogNumber) {
  auto writer1 = CreateWriter(100);
  EXPECT_EQ(writer1->GetLogNumber(), 100);

  // Need to create a new sink for the second writer
  SetUp();
  auto writer2 = CreateWriter(999);
  EXPECT_EQ(writer2->GetLogNumber(), 999);
}

// Helper class for testing the reader with in-memory files
class StringSequentialFile : public FSSequentialFile {
 public:
  explicit StringSequentialFile(const std::string& data)
      : data_(data), offset_(0) {}

  IOStatus Read(size_t n, const IOOptions& /*opts*/, Slice* result,
                char* scratch, IODebugContext* /*dbg*/) override {
    if (offset_ >= data_.size()) {
      *result = Slice();
      return IOStatus::OK();
    }
    n = std::min(n, data_.size() - offset_);
    memcpy(scratch, data_.data() + offset_, n);
    offset_ += n;
    *result = Slice(scratch, n);
    return IOStatus::OK();
  }

  IOStatus Skip(uint64_t n) override {
    offset_ += static_cast<size_t>(n);
    return IOStatus::OK();
  }

 private:
  std::string data_;
  size_t offset_;
};

// Test: Write bodies then read them back sequentially
TEST_F(PartitionedLogTest, WriteAndReadRoundTrip) {
  // Write multiple records
  auto writer = CreateWriter(42);
  std::vector<std::string> bodies = {
      "first body data",
      "second body with more content here",
      "third body",
      "",                      // empty body
      std::string(1000, 'x'),  // larger body
  };

  std::vector<PartitionedLogWriter::WriteResult> results;
  for (const auto& body : bodies) {
    PartitionedLogWriter::WriteResult result;
    ASSERT_OK(writer->WriteBody(WriteOptions(), Slice(body), 1, &result));
    results.push_back(result);
  }
  ASSERT_OK(writer->Close(WriteOptions()));

  // Create a reader with the written data
  const std::string& data = sink_->contents_;

  // Create sequential file reader
  std::unique_ptr<FSSequentialFile> seq_file(new StringSequentialFile(data));
  std::unique_ptr<SequentialFileReader> seq_reader(
      new SequentialFileReader(std::move(seq_file), "test"));

  // Create random access file reader
  std::unique_ptr<FSRandomAccessFile> rand_file(
      new test::StringSource(data, 0, false));
  std::unique_ptr<RandomAccessFileReader> rand_reader(
      new RandomAccessFileReader(std::move(rand_file), "test"));

  PartitionedLogReader reader(std::move(seq_reader), std::move(rand_reader),
                              42);

  EXPECT_EQ(reader.GetLogNumber(), 42);

  // Read all records sequentially
  for (size_t i = 0; i < bodies.size(); i++) {
    Slice body;
    std::string scratch;
    uint64_t offset;
    uint32_t crc;

    ASSERT_TRUE(reader.ReadNextBody(&body, &scratch, &offset, &crc))
        << "Failed to read record " << i;
    EXPECT_EQ(body.ToString(), bodies[i]) << "Body mismatch at record " << i;
    EXPECT_EQ(offset, results[i].offset) << "Offset mismatch at record " << i;
    EXPECT_EQ(crc, results[i].crc) << "CRC mismatch at record " << i;
  }

  // No more records
  Slice body;
  std::string scratch;
  uint64_t offset;
  uint32_t crc;
  EXPECT_FALSE(reader.ReadNextBody(&body, &scratch, &offset, &crc));
  EXPECT_TRUE(reader.IsEOF());
  EXPECT_OK(reader.GetLastStatus());
}

// Test: ReadBodyAt with CRC verification
TEST_F(PartitionedLogTest, ReadWithCRCVerification) {
  // Write records
  auto writer = CreateWriter(100);
  std::vector<std::string> bodies = {
      "body one",
      "body two with more data",
      "body three",
  };

  std::vector<PartitionedLogWriter::WriteResult> results;
  for (const auto& body : bodies) {
    PartitionedLogWriter::WriteResult result;
    ASSERT_OK(writer->WriteBody(WriteOptions(), Slice(body), 1, &result));
    results.push_back(result);
  }
  ASSERT_OK(writer->Close(WriteOptions()));

  const std::string& data = sink_->contents_;

  // Create only random access file reader for ReadBodyAt
  std::unique_ptr<FSRandomAccessFile> rand_file(
      new test::StringSource(data, 0, false));
  std::unique_ptr<RandomAccessFileReader> rand_reader(
      new RandomAccessFileReader(std::move(rand_file), "test"));

  PartitionedLogReader reader(nullptr, std::move(rand_reader), 100);

  // Read each record using ReadBodyAt
  for (size_t i = 0; i < bodies.size(); i++) {
    std::string read_body;
    Status s = reader.ReadBodyAt(results[i].offset, results[i].size,
                                 results[i].crc, &read_body);
    ASSERT_OK(s) << "Failed to read record " << i << ": " << s.ToString();
    EXPECT_EQ(read_body, bodies[i]) << "Body mismatch at record " << i;
  }

  // Test with wrong CRC - should fail
  std::string read_body;
  Status s = reader.ReadBodyAt(results[0].offset, results[0].size,
                               results[0].crc + 1,  // Wrong CRC
                               &read_body);
  EXPECT_TRUE(s.IsCorruption()) << "Expected corruption for wrong CRC";
  EXPECT_TRUE(s.ToString().find("CRC mismatch") != std::string::npos);
}

// Test: Corrupt data and verify detection
TEST_F(PartitionedLogTest, ReadCorruptedData) {
  // Write a record
  auto writer = CreateWriter(1);
  std::string body = "test body data for corruption test";
  PartitionedLogWriter::WriteResult result;
  ASSERT_OK(writer->WriteBody(WriteOptions(), Slice(body), 1, &result));
  ASSERT_OK(writer->Close(WriteOptions()));

  std::string data = sink_->contents_;

  // Test 1: Corrupt the body data
  {
    std::string corrupted_data = data;
    // Corrupt a byte in the body (after the header)
    size_t corrupt_pos = PartitionedLogWriter::kPartitionedHeaderSize + 5;
    corrupted_data[corrupt_pos] ^= 0xFF;

    std::unique_ptr<FSRandomAccessFile> rand_file(
        new test::StringSource(corrupted_data, 0, false));
    std::unique_ptr<RandomAccessFileReader> rand_reader(
        new RandomAccessFileReader(std::move(rand_file), "test"));

    PartitionedLogReader reader(nullptr, std::move(rand_reader), 1);

    std::string read_body;
    Status s =
        reader.ReadBodyAt(result.offset, result.size, result.crc, &read_body);
    EXPECT_TRUE(s.IsCorruption())
        << "Expected corruption for corrupted body data: " << s.ToString();
  }

  // Test 2: Corrupt the CRC field in the header
  {
    std::string corrupted_data = data;
    // Corrupt the CRC field (first 4 bytes)
    corrupted_data[0] ^= 0xFF;

    std::unique_ptr<FSRandomAccessFile> rand_file(
        new test::StringSource(corrupted_data, 0, false));
    std::unique_ptr<RandomAccessFileReader> rand_reader(
        new RandomAccessFileReader(std::move(rand_file), "test"));

    PartitionedLogReader reader(nullptr, std::move(rand_reader), 1);

    std::string read_body;
    Status s =
        reader.ReadBodyAt(result.offset, result.size, result.crc, &read_body);
    EXPECT_TRUE(s.IsCorruption())
        << "Expected corruption for corrupted CRC: " << s.ToString();
  }

  // Test 3: Corrupt the length field in the header
  {
    std::string corrupted_data = data;
    // Corrupt the length field (bytes 4-7)
    corrupted_data[4] ^= 0xFF;

    std::unique_ptr<FSRandomAccessFile> rand_file(
        new test::StringSource(corrupted_data, 0, false));
    std::unique_ptr<RandomAccessFileReader> rand_reader(
        new RandomAccessFileReader(std::move(rand_file), "test"));

    PartitionedLogReader reader(nullptr, std::move(rand_reader), 1);

    std::string read_body;
    Status s =
        reader.ReadBodyAt(result.offset, result.size, result.crc, &read_body);
    EXPECT_TRUE(s.IsCorruption())
        << "Expected corruption for corrupted length: " << s.ToString();
  }

  // Test 4: Sequential reading with corrupted data
  {
    std::string corrupted_data = data;
    // Corrupt a byte in the body
    size_t corrupt_pos = PartitionedLogWriter::kPartitionedHeaderSize + 5;
    corrupted_data[corrupt_pos] ^= 0xFF;

    std::unique_ptr<FSSequentialFile> seq_file(
        new StringSequentialFile(corrupted_data));
    std::unique_ptr<SequentialFileReader> seq_reader(
        new SequentialFileReader(std::move(seq_file), "test"));

    PartitionedLogReader reader(std::move(seq_reader), nullptr, 1);

    Slice read_body;
    std::string scratch;
    uint64_t offset;
    uint32_t crc;
    EXPECT_FALSE(reader.ReadNextBody(&read_body, &scratch, &offset, &crc));
    EXPECT_TRUE(reader.GetLastStatus().IsCorruption())
        << "Expected corruption: " << reader.GetLastStatus().ToString();
  }
}

// Test: Read with size too small
TEST_F(PartitionedLogTest, ReadBodyAtWithInvalidSize) {
  auto writer = CreateWriter(1);
  std::string body = "test body";
  PartitionedLogWriter::WriteResult result;
  ASSERT_OK(writer->WriteBody(WriteOptions(), Slice(body), 1, &result));
  ASSERT_OK(writer->Close(WriteOptions()));

  const std::string& data = sink_->contents_;

  std::unique_ptr<FSRandomAccessFile> rand_file(
      new test::StringSource(data, 0, false));
  std::unique_ptr<RandomAccessFileReader> rand_reader(
      new RandomAccessFileReader(std::move(rand_file), "test"));

  PartitionedLogReader reader(nullptr, std::move(rand_reader), 1);

  // Try to read with size smaller than header
  std::string read_body;
  Status s = reader.ReadBodyAt(
      result.offset,
      PartitionedLogWriter::kPartitionedHeaderSize - 1,  // Too small
      result.crc, &read_body);
  EXPECT_TRUE(s.IsCorruption())
      << "Expected corruption for size too small: " << s.ToString();
}

// Test: Multiple sequential reads then random access
TEST_F(PartitionedLogTest, MixedSequentialAndRandomAccess) {
  auto writer = CreateWriter(123);
  std::vector<std::string> bodies = {
      "body A",
      "body B with more data",
      "body C",
  };

  std::vector<PartitionedLogWriter::WriteResult> results;
  for (const auto& body : bodies) {
    PartitionedLogWriter::WriteResult result;
    ASSERT_OK(writer->WriteBody(WriteOptions(), Slice(body), 1, &result));
    results.push_back(result);
  }
  ASSERT_OK(writer->Close(WriteOptions()));

  const std::string& data = sink_->contents_;

  // Create reader with both file types
  std::unique_ptr<FSSequentialFile> seq_file(new StringSequentialFile(data));
  std::unique_ptr<SequentialFileReader> seq_reader(
      new SequentialFileReader(std::move(seq_file), "test"));

  std::unique_ptr<FSRandomAccessFile> rand_file(
      new test::StringSource(data, 0, false));
  std::unique_ptr<RandomAccessFileReader> rand_reader(
      new RandomAccessFileReader(std::move(rand_file), "test"));

  PartitionedLogReader reader(std::move(seq_reader), std::move(rand_reader),
                              123);

  // Read first record sequentially
  {
    Slice body;
    std::string scratch;
    uint64_t offset;
    uint32_t crc;
    ASSERT_TRUE(reader.ReadNextBody(&body, &scratch, &offset, &crc));
    EXPECT_EQ(body.ToString(), bodies[0]);
  }

  // Read third record using random access (skipping second)
  {
    std::string read_body;
    Status s = reader.ReadBodyAt(results[2].offset, results[2].size,
                                 results[2].crc, &read_body);
    ASSERT_OK(s);
    EXPECT_EQ(read_body, bodies[2]);
  }

  // Continue sequential reading (should read second record)
  {
    Slice body;
    std::string scratch;
    uint64_t offset;
    uint32_t crc;
    ASSERT_TRUE(reader.ReadNextBody(&body, &scratch, &offset, &crc));
    EXPECT_EQ(body.ToString(), bodies[1]);
  }
}

}  // namespace ROCKSDB_NAMESPACE::log

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  RegisterCustomObjects(argc, argv);
  return RUN_ALL_TESTS();
}

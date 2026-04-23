//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/blog/blog_writer.h"

#include <string>
#include <vector>

#include "db/blog/blog_reader.h"
#include "file/sequence_file_reader.h"
#include "file/writable_file_writer.h"
#include "rocksdb/write_buffer_manager.h"
#include "test_util/testharness.h"
#include "test_util/testutil.h"

namespace ROCKSDB_NAMESPACE {

class BlogWriterTest : public testing::Test {
 protected:
  // Shared contents buffer: StringSink writes here, StringSource reads from it.
  Slice reader_contents_;
  test::StringSink* sink_;

  BlogWriterTest() : sink_(new test::StringSink(&reader_contents_)) {}

  // Create a writer with the given header.
  std::unique_ptr<BlogFileWriter> MakeWriter(const BlogFileHeader& header) {
    std::unique_ptr<FSWritableFile> sink_holder(sink_);
    std::unique_ptr<WritableFileWriter> file_writer(new WritableFileWriter(
        std::move(sink_holder), "" /* file name */, FileOptions()));
    return std::make_unique<BlogFileWriter>(std::move(file_writer), header);
  }

  std::unique_ptr<BlogFileReader> MakeReaderWithReporter(
      BlogFileReader::Reporter* r) {
    return MakeReaderImpl(r);
  }

  // Create a reader over the data written so far.
  std::unique_ptr<BlogFileReader> MakeReader() {
    return MakeReaderImpl(nullptr);
  }

  std::unique_ptr<BlogFileReader> MakeReaderImpl(
      BlogFileReader::Reporter* reporter) {
    // Create an FSSequentialFile that reads from reader_contents_
    class MemSequentialFile : public FSSequentialFile {
     public:
      Slice& contents_;
      size_t pos_ = 0;
      explicit MemSequentialFile(Slice& contents) : contents_(contents) {}
      IOStatus Read(size_t n, const IOOptions& /*opts*/, Slice* result,
                    char* scratch, IODebugContext* /*dbg*/) override {
        size_t avail = contents_.size() - pos_;
        if (n > avail) {
          n = avail;
        }
        if (n > 0) {
          memcpy(scratch, contents_.data() + pos_, n);
          pos_ += n;
        }
        *result = Slice(scratch, n);
        return IOStatus::OK();
      }
      IOStatus Skip(uint64_t n) override {
        pos_ += static_cast<size_t>(n);
        return IOStatus::OK();
      }
    };

    auto* source = new MemSequentialFile(reader_contents_);
    std::unique_ptr<FSSequentialFile> source_holder(source);
    std::unique_ptr<SequentialFileReader> file_reader(
        new SequentialFileReader(std::move(source_holder), "" /* file name */));
    return std::make_unique<BlogFileReader>(std::move(file_reader), reporter,
                                            true /* verify_checksums */);
  }

  // Helper to create a default header for blob files.
  BlogFileHeader MakeBlobHeader() {
    BlogFileHeader header;
    header.checksum_type = kXXH3;
    header.compact_record_type = kBlogBlobRecord;
    header.GenerateRandomFields();
    return header;
  }

  // Helper to create a default header for WAL files.
  BlogFileHeader MakeWalHeader() {
    BlogFileHeader header;
    header.checksum_type = kXXH3;
    header.compact_record_type = kBlogWriteBatchRecord;
    header.GenerateRandomFields();
    header.SetProperty("role", "wal");
    return header;
  }
};

TEST_F(BlogWriterTest, WriteAndReadHeader) {
  auto header = MakeBlobHeader();
  auto writer = MakeWriter(header);

  WriteOptions wo;
  ASSERT_OK(writer->WriteHeader(wo));

  auto reader = MakeReader();
  BlogFileHeader read_header;
  ASSERT_OK(reader->ReadHeader(&read_header));

  ASSERT_EQ(read_header.schema_version, header.schema_version);
  ASSERT_EQ(read_header.checksum_type, header.checksum_type);
  ASSERT_EQ(read_header.compact_record_type, header.compact_record_type);
  ASSERT_EQ(memcmp(read_header.escape_sequence, header.escape_sequence,
                   kBlogEscapeSequenceSize),
            0);
}

TEST_F(BlogWriterTest, WriteSingleBlobRecord) {
  auto header = MakeBlobHeader();
  auto writer = MakeWriter(header);

  WriteOptions wo;
  ASSERT_OK(writer->WriteHeader(wo));

  std::string blob_data(100, 'A');
  uint64_t blob_offset = 0;
  ASSERT_OK(writer->AddBlobRecord(wo, blob_data, kNoCompression, &blob_offset));
  ASSERT_GT(blob_offset, 0u);

  // Verify alignment: offset after record should be 4-byte aligned
  ASSERT_EQ(writer->current_offset() % 4, 0u);

  // Read it back
  auto reader = MakeReader();
  BlogFileHeader read_header;
  ASSERT_OK(reader->ReadHeader(&read_header));

  BlogRecordType type;
  Slice payload;
  std::string scratch;
  uint64_t rec_offset;
  ASSERT_OK(reader->ReadRecord(&type, &payload, &scratch, &rec_offset));
  ASSERT_EQ(type, kBlogBlobRecord);
  ASSERT_EQ(payload.size(), 100u);
  ASSERT_EQ(payload.ToString(), blob_data);
}

TEST_F(BlogWriterTest, WriteMultipleBlobRecords) {
  auto header = MakeBlobHeader();
  auto writer = MakeWriter(header);

  WriteOptions wo;
  ASSERT_OK(writer->WriteHeader(wo));

  std::vector<std::string> blobs = {"hello", std::string(1000, 'X'), "short",
                                    std::string(500, 'Y')};
  for (const auto& blob : blobs) {
    uint64_t offset;
    ASSERT_OK(writer->AddBlobRecord(wo, blob, kNoCompression, &offset));
  }

  auto reader = MakeReader();
  BlogFileHeader rh;
  ASSERT_OK(reader->ReadHeader(&rh));

  for (const auto& expected : blobs) {
    BlogRecordType type;
    Slice payload;
    std::string scratch;
    ASSERT_OK(reader->ReadRecord(&type, &payload, &scratch));
    ASSERT_EQ(type, kBlogBlobRecord);
    ASSERT_EQ(payload.ToString(), expected);
  }

  // No more records
  BlogRecordType type;
  Slice payload;
  std::string scratch;
  ASSERT_TRUE(reader->ReadRecord(&type, &payload, &scratch).IsNotFound());
}

TEST_F(BlogWriterTest, WriteWriteBatchRecord) {
  auto header = MakeWalHeader();
  auto writer = MakeWriter(header);

  WriteOptions wo;
  ASSERT_OK(writer->WriteHeader(wo));

  std::string wb_data(200, 'W');
  ASSERT_OK(writer->AddWriteBatchRecord(wo, wb_data));

  auto reader = MakeReader();
  BlogFileHeader rh;
  ASSERT_OK(reader->ReadHeader(&rh));

  BlogRecordType type;
  Slice payload;
  std::string scratch;
  ASSERT_OK(reader->ReadRecord(&type, &payload, &scratch));
  ASSERT_EQ(type, kBlogWriteBatchRecord);
  ASSERT_EQ(payload.ToString(), wb_data);
}

TEST_F(BlogWriterTest, CompactVsFullFormat) {
  auto header = MakeBlobHeader();
  auto writer = MakeWriter(header);

  WriteOptions wo;
  ASSERT_OK(writer->WriteHeader(wo));

  // Small blob (< 2 MiB) should use compact format (kBlogBlobRecord matches
  // compact_record_type)
  std::string small_blob(100, 'S');
  uint64_t offset1;
  ASSERT_OK(writer->AddBlobRecord(wo, small_blob, kNoCompression, &offset1));

  // Large blob (> 2 MiB) should use full format
  std::string large_blob(3 * 1024 * 1024, 'L');
  uint64_t offset2;
  ASSERT_OK(writer->AddBlobRecord(wo, large_blob, kNoCompression, &offset2));

  // Read both back
  auto reader = MakeReader();
  BlogFileHeader rh;
  ASSERT_OK(reader->ReadHeader(&rh));

  BlogRecordType type;
  Slice payload;
  std::string scratch;

  ASSERT_OK(reader->ReadRecord(&type, &payload, &scratch));
  ASSERT_EQ(type, kBlogBlobRecord);
  ASSERT_EQ(payload.size(), small_blob.size());

  ASSERT_OK(reader->ReadRecord(&type, &payload, &scratch));
  ASSERT_EQ(type, kBlogBlobRecord);
  ASSERT_EQ(payload.size(), large_blob.size());
  ASSERT_EQ(payload, Slice(large_blob));
}

TEST_F(BlogWriterTest, PreambleStartRecord) {
  auto header = MakeWalHeader();
  auto writer = MakeWriter(header);

  WriteOptions wo;
  ASSERT_OK(writer->WriteHeader(wo));
  uint64_t pre_offset = writer->current_offset();
  ASSERT_EQ(pre_offset % 4, 0u) << "Pre-record offset not aligned";
  ASSERT_OK(writer->AddPreambleStartRecord(wo));
  uint64_t post_offset = writer->current_offset();
  ASSERT_EQ(post_offset % 4, 0u) << "Post-record offset not aligned";

  // Use a reporter to capture errors
  class TestReporter : public BlogFileReader::Reporter {
   public:
    std::string last_msg;
    void Corruption(size_t /*bytes*/, const Status& s) override {
      last_msg = s.ToString();
    }
  };
  TestReporter reporter;

  auto reader = MakeReaderWithReporter(&reporter);
  BlogFileHeader rh;
  ASSERT_OK(reader->ReadHeader(&rh));

  BlogRecordType type;
  Slice payload;
  std::string scratch;
  Status rs = reader->ReadRecord(&type, &payload, &scratch);
  if (!rs.ok()) {
    fprintf(stderr, "ReadRecord failed: %s, Reporter: %s, EOF=%d\n",
            rs.ToString().c_str(), reporter.last_msg.c_str(), reader->IsEOF());
  }
  ASSERT_OK(rs);
  ASSERT_EQ(type, kBlogPreambleStartRecord);
  ASSERT_TRUE(payload.empty());
}

TEST_F(BlogWriterTest, FooterRecords) {
  auto header = MakeBlobHeader();
  auto writer = MakeWriter(header);

  WriteOptions wo;
  ASSERT_OK(writer->WriteHeader(wo));

  // Write a blob record first
  std::string blob_data(50, 'B');
  uint64_t blob_offset;
  ASSERT_OK(writer->AddBlobRecord(wo, blob_data, kNoCompression, &blob_offset));

  // Write footer properties
  uint64_t props_offset = writer->current_offset();
  BlogFileFooterProperties props;
  props.SetBlobCount(1);
  props.SetTotalBlobBytes(50);
  ASSERT_OK(writer->AddFooterPropertiesRecord(wo, props));

  // Write footer locator
  BlogFileFooterLocator locator;
  uint64_t locator_offset = writer->current_offset();
  locator.entries.push_back(
      {kBlogFooterPropertiesRecord,
       static_cast<uint32_t>((locator_offset - props_offset) / 4)});
  ASSERT_OK(writer->AddFooterLocatorRecord(wo, locator));

  // Read all records
  auto reader = MakeReader();
  BlogFileHeader rh;
  ASSERT_OK(reader->ReadHeader(&rh));

  BlogRecordType type;
  Slice payload;
  std::string scratch;

  // Blob record
  ASSERT_OK(reader->ReadRecord(&type, &payload, &scratch));
  ASSERT_EQ(type, kBlogBlobRecord);

  // Footer properties record
  ASSERT_OK(reader->ReadRecord(&type, &payload, &scratch));
  ASSERT_EQ(type, kBlogFooterPropertiesRecord);
  BlogFileFooterProperties read_props;
  ASSERT_OK(read_props.DecodeFrom(payload));
  ASSERT_EQ(read_props.properties.size(), 2u);

  // Footer locator record
  ASSERT_OK(reader->ReadRecord(&type, &payload, &scratch));
  ASSERT_EQ(type, kBlogFooterLocatorRecord);
  BlogFileFooterLocator read_locator;
  ASSERT_OK(read_locator.DecodeFrom(payload));
  ASSERT_EQ(read_locator.entries.size(), 1u);
  ASSERT_EQ(read_locator.entries[0].record_type, kBlogFooterPropertiesRecord);
}

TEST_F(BlogWriterTest, ChecksumCorruptionDetected) {
  auto header = MakeBlobHeader();
  auto writer = MakeWriter(header);

  WriteOptions wo;
  ASSERT_OK(writer->WriteHeader(wo));

  std::string blob_data(100, 'C');
  uint64_t blob_offset;
  ASSERT_OK(writer->AddBlobRecord(wo, blob_data, kNoCompression, &blob_offset));

  // Corrupt one byte in the blob payload
  std::string& contents = sink_->contents_;
  ASSERT_GT(contents.size(), blob_offset + 10);
  contents[blob_offset + 10] ^= 0xFF;

  // Reading should detect the corruption
  auto reader = MakeReader();
  BlogFileHeader rh;
  ASSERT_OK(reader->ReadHeader(&rh));

  BlogRecordType type;
  Slice payload;
  std::string scratch;
  ASSERT_TRUE(reader->ReadRecord(&type, &payload, &scratch).IsCorruption());
}

TEST_F(BlogWriterTest, MixedRecordTypes) {
  // Use blob compact type but also write WriteBatch records (full format)
  auto header = MakeBlobHeader();
  auto writer = MakeWriter(header);

  WriteOptions wo;
  ASSERT_OK(writer->WriteHeader(wo));

  // Blob (compact format)
  std::string blob(50, 'B');
  uint64_t blob_offset;
  ASSERT_OK(writer->AddBlobRecord(wo, blob, kNoCompression, &blob_offset));

  // WriteBatch (full format since it doesn't match compact_record_type)
  std::string wb(60, 'W');
  ASSERT_OK(writer->AddWriteBatchRecord(wo, wb));

  // Another blob (compact)
  std::string blob2(70, 'D');
  uint64_t blob2_offset;
  ASSERT_OK(writer->AddBlobRecord(wo, blob2, kNoCompression, &blob2_offset));

  // Read all back
  auto reader = MakeReader();
  BlogFileHeader rh;
  ASSERT_OK(reader->ReadHeader(&rh));

  BlogRecordType type;
  Slice payload;
  std::string scratch;

  ASSERT_OK(reader->ReadRecord(&type, &payload, &scratch));
  ASSERT_EQ(type, kBlogBlobRecord);
  ASSERT_EQ(payload.size(), 50u);

  ASSERT_OK(reader->ReadRecord(&type, &payload, &scratch));
  ASSERT_EQ(type, kBlogWriteBatchRecord);
  ASSERT_EQ(payload.size(), 60u);

  ASSERT_OK(reader->ReadRecord(&type, &payload, &scratch));
  ASSERT_EQ(type, kBlogBlobRecord);
  ASSERT_EQ(payload.size(), 70u);

  ASSERT_TRUE(reader->ReadRecord(&type, &payload, &scratch).IsNotFound());
}

TEST_F(BlogWriterTest, AlignmentInvariant) {
  auto header = MakeBlobHeader();
  auto writer = MakeWriter(header);

  WriteOptions wo;
  ASSERT_OK(writer->WriteHeader(wo));

  // Write records of various sizes and verify alignment is maintained
  for (int size = 1; size <= 200; ++size) {
    std::string data(size, static_cast<char>(size & 0xFF));
    uint64_t offset;
    ASSERT_OK(writer->AddBlobRecord(wo, data, kNoCompression, &offset));
    ASSERT_EQ(writer->current_offset() % 4, 0u)
        << "Alignment violated after record of size " << size;
  }
}

TEST_F(BlogWriterTest, HeaderWithProperties) {
  auto header = MakeBlobHeader();
  header.SetProperty("CompressionCompatibilityName", "zstd");
  header.SetProperty("role", "blob");
  header.SetProperty("compressionSettings", "level=3");
  auto writer = MakeWriter(header);

  WriteOptions wo;
  ASSERT_OK(writer->WriteHeader(wo));

  auto reader = MakeReader();
  BlogFileHeader rh;
  ASSERT_OK(reader->ReadHeader(&rh));
  ASSERT_EQ(rh.GetProperty("CompressionCompatibilityName"), "zstd");
  ASSERT_EQ(rh.GetProperty("role"), "blob");
  ASSERT_EQ(rh.GetProperty("compressionSettings"), "level=3");
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/log_reader.h"
#include "db/log_writer.h"
#include "rocksdb/env.h"
#include "util/coding.h"
#include "util/crc32c.h"
#include "util/random.h"
#include "util/testharness.h"

namespace rocksdb {
namespace log {

// Construct a string of the specified length made out of the supplied
// partial string.
static std::string BigString(const std::string& partial_string, size_t n) {
  std::string result;
  while (result.size() < n) {
    result.append(partial_string);
  }
  result.resize(n);
  return result;
}

// Construct a string from a number
static std::string NumberString(int n) {
  char buf[50];
  snprintf(buf, sizeof(buf), "%d.", n);
  return std::string(buf);
}

// Return a skewed potentially long string
static std::string RandomSkewedString(int i, Random* rnd) {
  return BigString(NumberString(i), rnd->Skewed(17));
}

class LogTest {
 private:
  class StringDest : public WritableFile {
   public:
    std::string contents_;

    virtual Status Close() { return Status::OK(); }
    virtual Status Flush() { return Status::OK(); }
    virtual Status Sync() { return Status::OK(); }
    virtual Status Append(const Slice& slice) {
      contents_.append(slice.data(), slice.size());
      return Status::OK();
    }
  };

  class StringSource : public SequentialFile {
   public:
    Slice contents_;
    bool force_error_;
    bool returned_partial_;
    StringSource() : force_error_(false), returned_partial_(false) { }

    virtual Status Read(size_t n, Slice* result, char* scratch) {
      ASSERT_TRUE(!returned_partial_) << "must not Read() after eof/error";

      if (force_error_) {
        force_error_ = false;
        returned_partial_ = true;
        return Status::Corruption("read error");
      }

      if (contents_.size() < n) {
        n = contents_.size();
        returned_partial_ = true;
      }
      *result = Slice(contents_.data(), n);
      contents_.remove_prefix(n);
      return Status::OK();
    }

    virtual Status Skip(uint64_t n) {
      if (n > contents_.size()) {
        contents_.clear();
        return Status::NotFound("in-memory file skipepd past end");
      }

      contents_.remove_prefix(n);

      return Status::OK();
    }
  };

  class ReportCollector : public Reader::Reporter {
   public:
    size_t dropped_bytes_;
    std::string message_;

    ReportCollector() : dropped_bytes_(0) { }
    virtual void Corruption(size_t bytes, const Status& status) {
      dropped_bytes_ += bytes;
      message_.append(status.ToString());
    }
  };

  std::string& dest_contents() {
    auto dest = dynamic_cast<StringDest*>(writer_.file());
    assert(dest);
    return dest->contents_;
  }

  const std::string& dest_contents() const {
    auto dest = dynamic_cast<const StringDest*>(writer_.file());
    assert(dest);
    return dest->contents_;
  }

  void reset_source_contents() {
    auto src = dynamic_cast<StringSource*>(reader_.file());
    assert(src);
    src->contents_ = dest_contents();
  }

  unique_ptr<StringDest> dest_holder_;
  unique_ptr<StringSource> source_holder_;
  ReportCollector report_;
  bool reading_;
  Writer writer_;
  Reader reader_;

  // Record metadata for testing initial offset functionality
  static size_t initial_offset_record_sizes_[];
  static uint64_t initial_offset_last_record_offsets_[];

 public:
  LogTest() : dest_holder_(new StringDest),
              source_holder_(new StringSource),
              reading_(false),
              writer_(std::move(dest_holder_)),
              reader_(std::move(source_holder_), &report_, true/*checksum*/,
                      0/*initial_offset*/) {
  }

  void Write(const std::string& msg) {
    ASSERT_TRUE(!reading_) << "Write() after starting to read";
    writer_.AddRecord(Slice(msg));
  }

  size_t WrittenBytes() const {
    return dest_contents().size();
  }

  std::string Read() {
    if (!reading_) {
      reading_ = true;
      reset_source_contents();
    }
    std::string scratch;
    Slice record;
    if (reader_.ReadRecord(&record, &scratch)) {
      return record.ToString();
    } else {
      return "EOF";
    }
  }

  void IncrementByte(int offset, int delta) {
    dest_contents()[offset] += delta;
  }

  void SetByte(int offset, char new_byte) {
    dest_contents()[offset] = new_byte;
  }

  void ShrinkSize(int bytes) {
    dest_contents().resize(dest_contents().size() - bytes);
  }

  void FixChecksum(int header_offset, int len) {
    // Compute crc of type/len/data
    uint32_t crc = crc32c::Value(&dest_contents()[header_offset+6], 1 + len);
    crc = crc32c::Mask(crc);
    EncodeFixed32(&dest_contents()[header_offset], crc);
  }

  void ForceError() {
    auto src = dynamic_cast<StringSource*>(reader_.file());
    src->force_error_ = true;
  }

  size_t DroppedBytes() const {
    return report_.dropped_bytes_;
  }

  std::string ReportMessage() const {
    return report_.message_;
  }

  // Returns OK iff recorded error message contains "msg"
  std::string MatchError(const std::string& msg) const {
    if (report_.message_.find(msg) == std::string::npos) {
      return report_.message_;
    } else {
      return "OK";
    }
  }

  void WriteInitialOffsetLog() {
    for (int i = 0; i < 4; i++) {
      std::string record(initial_offset_record_sizes_[i],
                         static_cast<char>('a' + i));
      Write(record);
    }
  }

  void CheckOffsetPastEndReturnsNoRecords(uint64_t offset_past_end) {
    WriteInitialOffsetLog();
    reading_ = true;
    unique_ptr<StringSource> source(new StringSource);
    source->contents_ = dest_contents();
    unique_ptr<Reader> offset_reader(
      new Reader(std::move(source), &report_, true/*checksum*/,
                 WrittenBytes() + offset_past_end));
    Slice record;
    std::string scratch;
    ASSERT_TRUE(!offset_reader->ReadRecord(&record, &scratch));
  }

  void CheckInitialOffsetRecord(uint64_t initial_offset,
                                int expected_record_offset) {
    WriteInitialOffsetLog();
    reading_ = true;
    unique_ptr<StringSource> source(new StringSource);
    source->contents_ = dest_contents();
    unique_ptr<Reader> offset_reader(
      new Reader(std::move(source), &report_, true/*checksum*/,
                 initial_offset));
    Slice record;
    std::string scratch;
    ASSERT_TRUE(offset_reader->ReadRecord(&record, &scratch));
    ASSERT_EQ(initial_offset_record_sizes_[expected_record_offset],
              record.size());
    ASSERT_EQ(initial_offset_last_record_offsets_[expected_record_offset],
              offset_reader->LastRecordOffset());
    ASSERT_EQ((char)('a' + expected_record_offset), record.data()[0]);
  }

};

size_t LogTest::initial_offset_record_sizes_[] =
    {10000,  // Two sizable records in first block
     10000,
     2 * log::kBlockSize - 1000,  // Span three blocks
     1};

uint64_t LogTest::initial_offset_last_record_offsets_[] =
    {0,
     kHeaderSize + 10000,
     2 * (kHeaderSize + 10000),
     2 * (kHeaderSize + 10000) +
         (2 * log::kBlockSize - 1000) + 3 * kHeaderSize};


TEST(LogTest, Empty) {
  ASSERT_EQ("EOF", Read());
}

TEST(LogTest, ReadWrite) {
  Write("foo");
  Write("bar");
  Write("");
  Write("xxxx");
  ASSERT_EQ("foo", Read());
  ASSERT_EQ("bar", Read());
  ASSERT_EQ("", Read());
  ASSERT_EQ("xxxx", Read());
  ASSERT_EQ("EOF", Read());
  ASSERT_EQ("EOF", Read());  // Make sure reads at eof work
}

TEST(LogTest, ManyBlocks) {
  for (int i = 0; i < 100000; i++) {
    Write(NumberString(i));
  }
  for (int i = 0; i < 100000; i++) {
    ASSERT_EQ(NumberString(i), Read());
  }
  ASSERT_EQ("EOF", Read());
}

TEST(LogTest, Fragmentation) {
  Write("small");
  Write(BigString("medium", 50000));
  Write(BigString("large", 100000));
  ASSERT_EQ("small", Read());
  ASSERT_EQ(BigString("medium", 50000), Read());
  ASSERT_EQ(BigString("large", 100000), Read());
  ASSERT_EQ("EOF", Read());
}

TEST(LogTest, MarginalTrailer) {
  // Make a trailer that is exactly the same length as an empty record.
  const int n = kBlockSize - 2*kHeaderSize;
  Write(BigString("foo", n));
  ASSERT_EQ((unsigned int)(kBlockSize - kHeaderSize), WrittenBytes());
  Write("");
  Write("bar");
  ASSERT_EQ(BigString("foo", n), Read());
  ASSERT_EQ("", Read());
  ASSERT_EQ("bar", Read());
  ASSERT_EQ("EOF", Read());
}

TEST(LogTest, MarginalTrailer2) {
  // Make a trailer that is exactly the same length as an empty record.
  const int n = kBlockSize - 2*kHeaderSize;
  Write(BigString("foo", n));
  ASSERT_EQ((unsigned int)(kBlockSize - kHeaderSize), WrittenBytes());
  Write("bar");
  ASSERT_EQ(BigString("foo", n), Read());
  ASSERT_EQ("bar", Read());
  ASSERT_EQ("EOF", Read());
  ASSERT_EQ(0U, DroppedBytes());
  ASSERT_EQ("", ReportMessage());
}

TEST(LogTest, ShortTrailer) {
  const int n = kBlockSize - 2*kHeaderSize + 4;
  Write(BigString("foo", n));
  ASSERT_EQ((unsigned int)(kBlockSize - kHeaderSize + 4), WrittenBytes());
  Write("");
  Write("bar");
  ASSERT_EQ(BigString("foo", n), Read());
  ASSERT_EQ("", Read());
  ASSERT_EQ("bar", Read());
  ASSERT_EQ("EOF", Read());
}

TEST(LogTest, AlignedEof) {
  const int n = kBlockSize - 2*kHeaderSize + 4;
  Write(BigString("foo", n));
  ASSERT_EQ((unsigned int)(kBlockSize - kHeaderSize + 4), WrittenBytes());
  ASSERT_EQ(BigString("foo", n), Read());
  ASSERT_EQ("EOF", Read());
}

TEST(LogTest, RandomRead) {
  const int N = 500;
  Random write_rnd(301);
  for (int i = 0; i < N; i++) {
    Write(RandomSkewedString(i, &write_rnd));
  }
  Random read_rnd(301);
  for (int i = 0; i < N; i++) {
    ASSERT_EQ(RandomSkewedString(i, &read_rnd), Read());
  }
  ASSERT_EQ("EOF", Read());
}

// Tests of all the error paths in log_reader.cc follow:

TEST(LogTest, ReadError) {
  Write("foo");
  ForceError();
  ASSERT_EQ("EOF", Read());
  ASSERT_EQ((unsigned int)kBlockSize, DroppedBytes());
  ASSERT_EQ("OK", MatchError("read error"));
}

TEST(LogTest, BadRecordType) {
  Write("foo");
  // Type is stored in header[6]
  IncrementByte(6, 100);
  FixChecksum(0, 3);
  ASSERT_EQ("EOF", Read());
  ASSERT_EQ(3U, DroppedBytes());
  ASSERT_EQ("OK", MatchError("unknown record type"));
}

TEST(LogTest, TruncatedTrailingRecord) {
  Write("foo");
  ShrinkSize(4);   // Drop all payload as well as a header byte
  ASSERT_EQ("EOF", Read());
  ASSERT_EQ((unsigned int)(kHeaderSize - 1), DroppedBytes());
  ASSERT_EQ("OK", MatchError("truncated record at end of file"));
}

TEST(LogTest, BadLength) {
  Write("foo");
  ShrinkSize(1);
  ASSERT_EQ("EOF", Read());
  ASSERT_EQ((unsigned int)(kHeaderSize + 2), DroppedBytes());
  ASSERT_EQ("OK", MatchError("bad record length"));
}

TEST(LogTest, ChecksumMismatch) {
  Write("foo");
  IncrementByte(0, 10);
  ASSERT_EQ("EOF", Read());
  ASSERT_EQ(10U, DroppedBytes());
  ASSERT_EQ("OK", MatchError("checksum mismatch"));
}

TEST(LogTest, UnexpectedMiddleType) {
  Write("foo");
  SetByte(6, kMiddleType);
  FixChecksum(0, 3);
  ASSERT_EQ("EOF", Read());
  ASSERT_EQ(3U, DroppedBytes());
  ASSERT_EQ("OK", MatchError("missing start"));
}

TEST(LogTest, UnexpectedLastType) {
  Write("foo");
  SetByte(6, kLastType);
  FixChecksum(0, 3);
  ASSERT_EQ("EOF", Read());
  ASSERT_EQ(3U, DroppedBytes());
  ASSERT_EQ("OK", MatchError("missing start"));
}

TEST(LogTest, UnexpectedFullType) {
  Write("foo");
  Write("bar");
  SetByte(6, kFirstType);
  FixChecksum(0, 3);
  ASSERT_EQ("bar", Read());
  ASSERT_EQ("EOF", Read());
  ASSERT_EQ(3U, DroppedBytes());
  ASSERT_EQ("OK", MatchError("partial record without end"));
}

TEST(LogTest, UnexpectedFirstType) {
  Write("foo");
  Write(BigString("bar", 100000));
  SetByte(6, kFirstType);
  FixChecksum(0, 3);
  ASSERT_EQ(BigString("bar", 100000), Read());
  ASSERT_EQ("EOF", Read());
  ASSERT_EQ(3U, DroppedBytes());
  ASSERT_EQ("OK", MatchError("partial record without end"));
}

TEST(LogTest, ErrorJoinsRecords) {
  // Consider two fragmented records:
  //    first(R1) last(R1) first(R2) last(R2)
  // where the middle two fragments disappear.  We do not want
  // first(R1),last(R2) to get joined and returned as a valid record.

  // Write records that span two blocks
  Write(BigString("foo", kBlockSize));
  Write(BigString("bar", kBlockSize));
  Write("correct");

  // Wipe the middle block
  for (unsigned int offset = kBlockSize; offset < 2*kBlockSize; offset++) {
    SetByte(offset, 'x');
  }

  ASSERT_EQ("correct", Read());
  ASSERT_EQ("EOF", Read());
  const unsigned int dropped = DroppedBytes();
  ASSERT_LE(dropped, 2*kBlockSize + 100);
  ASSERT_GE(dropped, 2*kBlockSize);
}

TEST(LogTest, ReadStart) {
  CheckInitialOffsetRecord(0, 0);
}

TEST(LogTest, ReadSecondOneOff) {
  CheckInitialOffsetRecord(1, 1);
}

TEST(LogTest, ReadSecondTenThousand) {
  CheckInitialOffsetRecord(10000, 1);
}

TEST(LogTest, ReadSecondStart) {
  CheckInitialOffsetRecord(10007, 1);
}

TEST(LogTest, ReadThirdOneOff) {
  CheckInitialOffsetRecord(10008, 2);
}

TEST(LogTest, ReadThirdStart) {
  CheckInitialOffsetRecord(20014, 2);
}

TEST(LogTest, ReadFourthOneOff) {
  CheckInitialOffsetRecord(20015, 3);
}

TEST(LogTest, ReadFourthFirstBlockTrailer) {
  CheckInitialOffsetRecord(log::kBlockSize - 4, 3);
}

TEST(LogTest, ReadFourthMiddleBlock) {
  CheckInitialOffsetRecord(log::kBlockSize + 1, 3);
}

TEST(LogTest, ReadFourthLastBlock) {
  CheckInitialOffsetRecord(2 * log::kBlockSize + 1, 3);
}

TEST(LogTest, ReadFourthStart) {
  CheckInitialOffsetRecord(
      2 * (kHeaderSize + 1000) + (2 * log::kBlockSize - 1000) + 3 * kHeaderSize,
      3);
}

TEST(LogTest, ReadEnd) {
  CheckOffsetPastEndReturnsNoRecords(0);
}

TEST(LogTest, ReadPastEnd) {
  CheckOffsetPastEndReturnsNoRecords(5);
}

}  // namespace log
}  // namespace rocksdb

int main(int argc, char** argv) {
  return rocksdb::test::RunAllTests();
}

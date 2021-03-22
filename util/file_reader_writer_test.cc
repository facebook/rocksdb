//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
#include <algorithm>
#include <vector>

#include "env/mock_env.h"
#include "file/line_file_reader.h"
#include "file/random_access_file_reader.h"
#include "file/readahead_raf.h"
#include "file/sequence_file_reader.h"
#include "file/writable_file_writer.h"
#include "rocksdb/file_system.h"
#include "test_util/testharness.h"
#include "test_util/testutil.h"
#include "util/random.h"

namespace ROCKSDB_NAMESPACE {

class WritableFileWriterTest : public testing::Test {};

const uint32_t kMb = 1 << 20;

TEST_F(WritableFileWriterTest, RangeSync) {
  class FakeWF : public FSWritableFile {
   public:
    explicit FakeWF() : size_(0), last_synced_(0) {}
    ~FakeWF() override {}

    using FSWritableFile::Append;
    IOStatus Append(const Slice& data, const IOOptions& /*options*/,
                    IODebugContext* /*dbg*/) override {
      size_ += data.size();
      return IOStatus::OK();
    }
    IOStatus Truncate(uint64_t /*size*/, const IOOptions& /*options*/,
                      IODebugContext* /*dbg*/) override {
      return IOStatus::OK();
    }
    IOStatus Close(const IOOptions& /*options*/,
                   IODebugContext* /*dbg*/) override {
      EXPECT_GE(size_, last_synced_ + kMb);
      EXPECT_LT(size_, last_synced_ + 2 * kMb);
      // Make sure random writes generated enough writes.
      EXPECT_GT(size_, 10 * kMb);
      return IOStatus::OK();
    }
    IOStatus Flush(const IOOptions& /*options*/,
                   IODebugContext* /*dbg*/) override {
      return IOStatus::OK();
    }
    IOStatus Sync(const IOOptions& /*options*/,
                  IODebugContext* /*dbg*/) override {
      return IOStatus::OK();
    }
    IOStatus Fsync(const IOOptions& /*options*/,
                   IODebugContext* /*dbg*/) override {
      return IOStatus::OK();
    }
    void SetIOPriority(Env::IOPriority /*pri*/) override {}
    uint64_t GetFileSize(const IOOptions& /*options*/,
                         IODebugContext* /*dbg*/) override {
      return size_;
    }
    void GetPreallocationStatus(size_t* /*block_size*/,
                                size_t* /*last_allocated_block*/) override {}
    size_t GetUniqueId(char* /*id*/, size_t /*max_size*/) const override {
      return 0;
    }
    IOStatus InvalidateCache(size_t /*offset*/, size_t /*length*/) override {
      return IOStatus::OK();
    }

   protected:
    IOStatus Allocate(uint64_t /*offset*/, uint64_t /*len*/,
                      const IOOptions& /*options*/,
                      IODebugContext* /*dbg*/) override {
      return IOStatus::OK();
    }
    IOStatus RangeSync(uint64_t offset, uint64_t nbytes,
                       const IOOptions& /*options*/,
                       IODebugContext* /*dbg*/) override {
      EXPECT_EQ(offset % 4096, 0u);
      EXPECT_EQ(nbytes % 4096, 0u);

      EXPECT_EQ(offset, last_synced_);
      last_synced_ = offset + nbytes;
      EXPECT_GE(size_, last_synced_ + kMb);
      if (size_ > 2 * kMb) {
        EXPECT_LT(size_, last_synced_ + 2 * kMb);
      }
      return IOStatus::OK();
    }

    uint64_t size_;
    uint64_t last_synced_;
  };

  EnvOptions env_options;
  env_options.bytes_per_sync = kMb;
  std::unique_ptr<FakeWF> wf(new FakeWF);
  std::unique_ptr<WritableFileWriter> writer(
      new WritableFileWriter(std::move(wf), "" /* don't care */, env_options));
  Random r(301);
  Status s;
  std::unique_ptr<char[]> large_buf(new char[10 * kMb]);
  for (int i = 0; i < 1000; i++) {
    int skew_limit = (i < 700) ? 10 : 15;
    uint32_t num = r.Skewed(skew_limit) * 100 + r.Uniform(100);
    s = writer->Append(Slice(large_buf.get(), num));
    ASSERT_OK(s);

    // Flush in a chance of 1/10.
    if (r.Uniform(10) == 0) {
      s = writer->Flush();
      ASSERT_OK(s);
    }
  }
  s = writer->Close();
  ASSERT_OK(s);
}

TEST_F(WritableFileWriterTest, IncrementalBuffer) {
  class FakeWF : public FSWritableFile {
   public:
    explicit FakeWF(std::string* _file_data, bool _use_direct_io,
                    bool _no_flush)
        : file_data_(_file_data),
          use_direct_io_(_use_direct_io),
          no_flush_(_no_flush) {}
    ~FakeWF() override {}

    using FSWritableFile::Append;
    IOStatus Append(const Slice& data, const IOOptions& /*options*/,
                    IODebugContext* /*dbg*/) override {
      file_data_->append(data.data(), data.size());
      size_ += data.size();
      return IOStatus::OK();
    }
    using FSWritableFile::PositionedAppend;
    IOStatus PositionedAppend(const Slice& data, uint64_t pos,
                              const IOOptions& /*options*/,
                              IODebugContext* /*dbg*/) override {
      EXPECT_TRUE(pos % 512 == 0);
      EXPECT_TRUE(data.size() % 512 == 0);
      file_data_->resize(pos);
      file_data_->append(data.data(), data.size());
      size_ += data.size();
      return IOStatus::OK();
    }

    IOStatus Truncate(uint64_t size, const IOOptions& /*options*/,
                      IODebugContext* /*dbg*/) override {
      file_data_->resize(size);
      return IOStatus::OK();
    }
    IOStatus Close(const IOOptions& /*options*/,
                   IODebugContext* /*dbg*/) override {
      return IOStatus::OK();
    }
    IOStatus Flush(const IOOptions& /*options*/,
                   IODebugContext* /*dbg*/) override {
      return IOStatus::OK();
    }
    IOStatus Sync(const IOOptions& /*options*/,
                  IODebugContext* /*dbg*/) override {
      return IOStatus::OK();
    }
    IOStatus Fsync(const IOOptions& /*options*/,
                   IODebugContext* /*dbg*/) override {
      return IOStatus::OK();
    }
    void SetIOPriority(Env::IOPriority /*pri*/) override {}
    uint64_t GetFileSize(const IOOptions& /*options*/,
                         IODebugContext* /*dbg*/) override {
      return size_;
    }
    void GetPreallocationStatus(size_t* /*block_size*/,
                                size_t* /*last_allocated_block*/) override {}
    size_t GetUniqueId(char* /*id*/, size_t /*max_size*/) const override {
      return 0;
    }
    IOStatus InvalidateCache(size_t /*offset*/, size_t /*length*/) override {
      return IOStatus::OK();
    }
    bool use_direct_io() const override { return use_direct_io_; }

    std::string* file_data_;
    bool use_direct_io_;
    bool no_flush_;
    size_t size_ = 0;
  };

  Random r(301);
  const int kNumAttempts = 50;
  for (int attempt = 0; attempt < kNumAttempts; attempt++) {
    bool no_flush = (attempt % 3 == 0);
    EnvOptions env_options;
    env_options.writable_file_max_buffer_size =
        (attempt < kNumAttempts / 2) ? 512 * 1024 : 700 * 1024;
    std::string actual;
    std::unique_ptr<FakeWF> wf(new FakeWF(&actual,
#ifndef ROCKSDB_LITE
                                          attempt % 2 == 1,
#else
                                          false,
#endif
                                          no_flush));
    std::unique_ptr<WritableFileWriter> writer(new WritableFileWriter(
        std::move(wf), "" /* don't care */, env_options));

    std::string target;
    for (int i = 0; i < 20; i++) {
      uint32_t num = r.Skewed(16) * 100 + r.Uniform(100);
      std::string random_string = r.RandomString(num);
      ASSERT_OK(writer->Append(Slice(random_string.c_str(), num)));
      target.append(random_string.c_str(), num);

      // In some attempts, flush in a chance of 1/10.
      if (!no_flush && r.Uniform(10) == 0) {
        ASSERT_OK(writer->Flush());
      }
    }
    ASSERT_OK(writer->Flush());
    ASSERT_OK(writer->Close());
    ASSERT_EQ(target.size(), actual.size());
    ASSERT_EQ(target, actual);
  }
}

#ifndef ROCKSDB_LITE
TEST_F(WritableFileWriterTest, AppendStatusReturn) {
  class FakeWF : public FSWritableFile {
   public:
    explicit FakeWF() : use_direct_io_(false), io_error_(false) {}

    bool use_direct_io() const override { return use_direct_io_; }

    using FSWritableFile::Append;
    IOStatus Append(const Slice& /*data*/, const IOOptions& /*options*/,
                    IODebugContext* /*dbg*/) override {
      if (io_error_) {
        return IOStatus::IOError("Fake IO error");
      }
      return IOStatus::OK();
    }
    using FSWritableFile::PositionedAppend;
    IOStatus PositionedAppend(const Slice& /*data*/, uint64_t,
                              const IOOptions& /*options*/,
                              IODebugContext* /*dbg*/) override {
      if (io_error_) {
        return IOStatus::IOError("Fake IO error");
      }
      return IOStatus::OK();
    }
    IOStatus Close(const IOOptions& /*options*/,
                   IODebugContext* /*dbg*/) override {
      return IOStatus::OK();
    }
    IOStatus Flush(const IOOptions& /*options*/,
                   IODebugContext* /*dbg*/) override {
      return IOStatus::OK();
    }
    IOStatus Sync(const IOOptions& /*options*/,
                  IODebugContext* /*dbg*/) override {
      return IOStatus::OK();
    }
    void Setuse_direct_io(bool val) { use_direct_io_ = val; }
    void SetIOError(bool val) { io_error_ = val; }

   protected:
    bool use_direct_io_;
    bool io_error_;
  };
  std::unique_ptr<FakeWF> wf(new FakeWF());
  wf->Setuse_direct_io(true);
  std::unique_ptr<WritableFileWriter> writer(
      new WritableFileWriter(std::move(wf), "" /* don't care */, EnvOptions()));

  ASSERT_OK(writer->Append(std::string(2 * kMb, 'a')));

  // Next call to WritableFile::Append() should fail
  FakeWF* fwf = static_cast<FakeWF*>(writer->writable_file());
  fwf->SetIOError(true);
  ASSERT_NOK(writer->Append(std::string(2 * kMb, 'b')));
}
#endif

class ReadaheadRandomAccessFileTest
    : public testing::Test,
      public testing::WithParamInterface<size_t> {
 public:
  static std::vector<size_t> GetReadaheadSizeList() {
    return {1lu << 12, 1lu << 16};
  }
  void SetUp() override {
    readahead_size_ = GetParam();
    scratch_.reset(new char[2 * readahead_size_]);
    ResetSourceStr();
  }
  ReadaheadRandomAccessFileTest() : control_contents_() {}
  std::string Read(uint64_t offset, size_t n) {
    Slice result;
    Status s = test_read_holder_->Read(offset, n, IOOptions(), &result,
                                       scratch_.get(), nullptr);
    EXPECT_TRUE(s.ok() || s.IsInvalidArgument());
    return std::string(result.data(), result.size());
  }
  void ResetSourceStr(const std::string& str = "") {
    std::unique_ptr<FSWritableFile> sink(
        new test::StringSink(&control_contents_));
    std::unique_ptr<WritableFileWriter> write_holder(new WritableFileWriter(
        std::move(sink), "" /* don't care */, FileOptions()));
    Status s = write_holder->Append(Slice(str));
    EXPECT_OK(s);
    s = write_holder->Flush();
    EXPECT_OK(s);
    std::unique_ptr<FSRandomAccessFile> read_holder(
        new test::StringSource(control_contents_));
    test_read_holder_ =
        NewReadaheadRandomAccessFile(std::move(read_holder), readahead_size_);
  }
  size_t GetReadaheadSize() const { return readahead_size_; }

 private:
  size_t readahead_size_;
  Slice control_contents_;
  std::unique_ptr<FSRandomAccessFile> test_read_holder_;
  std::unique_ptr<char[]> scratch_;
};

TEST_P(ReadaheadRandomAccessFileTest, EmptySourceStr) {
  ASSERT_EQ("", Read(0, 1));
  ASSERT_EQ("", Read(0, 0));
  ASSERT_EQ("", Read(13, 13));
}

TEST_P(ReadaheadRandomAccessFileTest, SourceStrLenLessThanReadaheadSize) {
  std::string str = "abcdefghijklmnopqrs";
  ResetSourceStr(str);
  ASSERT_EQ(str.substr(3, 4), Read(3, 4));
  ASSERT_EQ(str.substr(0, 3), Read(0, 3));
  ASSERT_EQ(str, Read(0, str.size()));
  ASSERT_EQ(str.substr(7, std::min(static_cast<int>(str.size()) - 7, 30)),
            Read(7, 30));
  ASSERT_EQ("", Read(100, 100));
}

TEST_P(ReadaheadRandomAccessFileTest, SourceStrLenGreaterThanReadaheadSize) {
  Random rng(42);
  for (int k = 0; k < 100; ++k) {
    size_t strLen = k * GetReadaheadSize() +
                    rng.Uniform(static_cast<int>(GetReadaheadSize()));
    std::string str = rng.HumanReadableString(static_cast<int>(strLen));
    ResetSourceStr(str);
    for (int test = 1; test <= 100; ++test) {
      size_t offset = rng.Uniform(static_cast<int>(strLen));
      size_t n = rng.Uniform(static_cast<int>(GetReadaheadSize()));
      ASSERT_EQ(str.substr(offset, std::min(n, strLen - offset)),
                Read(offset, n));
    }
  }
}

TEST_P(ReadaheadRandomAccessFileTest, ReadExceedsReadaheadSize) {
  Random rng(7);
  size_t strLen = 4 * GetReadaheadSize() +
                  rng.Uniform(static_cast<int>(GetReadaheadSize()));
  std::string str = rng.HumanReadableString(static_cast<int>(strLen));
  ResetSourceStr(str);
  for (int test = 1; test <= 100; ++test) {
    size_t offset = rng.Uniform(static_cast<int>(strLen));
    size_t n =
        GetReadaheadSize() + rng.Uniform(static_cast<int>(GetReadaheadSize()));
    ASSERT_EQ(str.substr(offset, std::min(n, strLen - offset)),
              Read(offset, n));
  }
}

INSTANTIATE_TEST_CASE_P(
    EmptySourceStr, ReadaheadRandomAccessFileTest,
    ::testing::ValuesIn(ReadaheadRandomAccessFileTest::GetReadaheadSizeList()));
INSTANTIATE_TEST_CASE_P(
    SourceStrLenLessThanReadaheadSize, ReadaheadRandomAccessFileTest,
    ::testing::ValuesIn(ReadaheadRandomAccessFileTest::GetReadaheadSizeList()));
INSTANTIATE_TEST_CASE_P(
    SourceStrLenGreaterThanReadaheadSize, ReadaheadRandomAccessFileTest,
    ::testing::ValuesIn(ReadaheadRandomAccessFileTest::GetReadaheadSizeList()));
INSTANTIATE_TEST_CASE_P(
    ReadExceedsReadaheadSize, ReadaheadRandomAccessFileTest,
    ::testing::ValuesIn(ReadaheadRandomAccessFileTest::GetReadaheadSizeList()));

class ReadaheadSequentialFileTest : public testing::Test,
                                    public testing::WithParamInterface<size_t> {
 public:
  static std::vector<size_t> GetReadaheadSizeList() {
    return {1lu << 8, 1lu << 12, 1lu << 16, 1lu << 18};
  }
  void SetUp() override {
    readahead_size_ = GetParam();
    scratch_.reset(new char[2 * readahead_size_]);
    ResetSourceStr();
  }
  ReadaheadSequentialFileTest() {}
  std::string Read(size_t n) {
    Slice result;
    Status s = test_read_holder_->Read(n, &result, scratch_.get());
    EXPECT_TRUE(s.ok() || s.IsInvalidArgument());
    return std::string(result.data(), result.size());
  }
  void Skip(size_t n) { test_read_holder_->Skip(n); }
  void ResetSourceStr(const std::string& str = "") {
    auto read_holder = std::unique_ptr<FSSequentialFile>(
        new test::SeqStringSource(str, &seq_read_count_));
    test_read_holder_.reset(new SequentialFileReader(std::move(read_holder),
                                                     "test", readahead_size_));
  }
  size_t GetReadaheadSize() const { return readahead_size_; }

 private:
  size_t readahead_size_;
  std::unique_ptr<SequentialFileReader> test_read_holder_;
  std::unique_ptr<char[]> scratch_;
  std::atomic<int> seq_read_count_;
};

TEST_P(ReadaheadSequentialFileTest, EmptySourceStr) {
  ASSERT_EQ("", Read(0));
  ASSERT_EQ("", Read(1));
  ASSERT_EQ("", Read(13));
}

TEST_P(ReadaheadSequentialFileTest, SourceStrLenLessThanReadaheadSize) {
  std::string str = "abcdefghijklmnopqrs";
  ResetSourceStr(str);
  ASSERT_EQ(str.substr(0, 3), Read(3));
  ASSERT_EQ(str.substr(3, 1), Read(1));
  ASSERT_EQ(str.substr(4), Read(str.size()));
  ASSERT_EQ("", Read(100));
}

TEST_P(ReadaheadSequentialFileTest, SourceStrLenGreaterThanReadaheadSize) {
  Random rng(42);
  for (int s = 0; s < 1; ++s) {
    for (int k = 0; k < 100; ++k) {
      size_t strLen = k * GetReadaheadSize() +
                      rng.Uniform(static_cast<int>(GetReadaheadSize()));
      std::string str = rng.HumanReadableString(static_cast<int>(strLen));
      ResetSourceStr(str);
      size_t offset = 0;
      for (int test = 1; test <= 100; ++test) {
        size_t n = rng.Uniform(static_cast<int>(GetReadaheadSize()));
        if (s && test % 2) {
          Skip(n);
        } else {
          ASSERT_EQ(str.substr(offset, std::min(n, strLen - offset)), Read(n));
        }
        offset = std::min(offset + n, strLen);
      }
    }
  }
}

TEST_P(ReadaheadSequentialFileTest, ReadExceedsReadaheadSize) {
  Random rng(42);
  for (int s = 0; s < 1; ++s) {
    for (int k = 0; k < 100; ++k) {
      size_t strLen = k * GetReadaheadSize() +
                      rng.Uniform(static_cast<int>(GetReadaheadSize()));
      std::string str = rng.HumanReadableString(static_cast<int>(strLen));
      ResetSourceStr(str);
      size_t offset = 0;
      for (int test = 1; test <= 100; ++test) {
        size_t n = GetReadaheadSize() +
                   rng.Uniform(static_cast<int>(GetReadaheadSize()));
        if (s && test % 2) {
          Skip(n);
        } else {
          ASSERT_EQ(str.substr(offset, std::min(n, strLen - offset)), Read(n));
        }
        offset = std::min(offset + n, strLen);
      }
    }
  }
}

INSTANTIATE_TEST_CASE_P(
    EmptySourceStr, ReadaheadSequentialFileTest,
    ::testing::ValuesIn(ReadaheadSequentialFileTest::GetReadaheadSizeList()));
INSTANTIATE_TEST_CASE_P(
    SourceStrLenLessThanReadaheadSize, ReadaheadSequentialFileTest,
    ::testing::ValuesIn(ReadaheadSequentialFileTest::GetReadaheadSizeList()));
INSTANTIATE_TEST_CASE_P(
    SourceStrLenGreaterThanReadaheadSize, ReadaheadSequentialFileTest,
    ::testing::ValuesIn(ReadaheadSequentialFileTest::GetReadaheadSizeList()));
INSTANTIATE_TEST_CASE_P(
    ReadExceedsReadaheadSize, ReadaheadSequentialFileTest,
    ::testing::ValuesIn(ReadaheadSequentialFileTest::GetReadaheadSizeList()));

namespace {
std::string GenerateLine(int n) {
  std::string rv;
  // Multiples of 17 characters per line, for likely bad buffer alignment
  for (int i = 0; i < n; ++i) {
    rv.push_back(static_cast<char>('0' + (i % 10)));
    rv.append("xxxxxxxxxxxxxxxx");
  }
  return rv;
}
}  // namespace

TEST(LineFileReaderTest, LineFileReaderTest) {
  const int nlines = 1000;

  std::unique_ptr<MockEnv> mem_env(new MockEnv(Env::Default()));
  std::shared_ptr<FileSystem> fs = mem_env->GetFileSystem();
  // Create an input file
  {
    std::unique_ptr<FSWritableFile> file;
    ASSERT_OK(
        fs->NewWritableFile("testfile", FileOptions(), &file, /*dbg*/ nullptr));

    for (int i = 0; i < nlines; ++i) {
      std::string line = GenerateLine(i);
      line.push_back('\n');
      ASSERT_OK(file->Append(line, IOOptions(), /*dbg*/ nullptr));
    }
  }

  // Verify with no I/O errors
  {
    std::unique_ptr<LineFileReader> reader;
    ASSERT_OK(LineFileReader::Create(fs, "testfile", FileOptions(), &reader,
                                     nullptr));
    std::string line;
    int count = 0;
    while (reader->ReadLine(&line)) {
      ASSERT_EQ(line, GenerateLine(count));
      ++count;
      ASSERT_EQ(static_cast<int>(reader->GetLineNumber()), count);
    }
    ASSERT_OK(reader->GetStatus());
    ASSERT_EQ(count, nlines);
    ASSERT_EQ(static_cast<int>(reader->GetLineNumber()), count);
    // And still
    ASSERT_FALSE(reader->ReadLine(&line));
    ASSERT_OK(reader->GetStatus());
    ASSERT_EQ(static_cast<int>(reader->GetLineNumber()), count);
  }

  // Verify with injected I/O error
  {
    std::unique_ptr<LineFileReader> reader;
    ASSERT_OK(LineFileReader::Create(fs, "testfile", FileOptions(), &reader,
                                     nullptr));
    std::string line;
    int count = 0;
    // Read part way through the file
    while (count < nlines / 4) {
      ASSERT_TRUE(reader->ReadLine(&line));
      ASSERT_EQ(line, GenerateLine(count));
      ++count;
      ASSERT_EQ(static_cast<int>(reader->GetLineNumber()), count);
    }
    ASSERT_OK(reader->GetStatus());

    // Inject error
    int callback_count = 0;
    SyncPoint::GetInstance()->SetCallBack(
        "MemFile::Read:IOStatus", [&](void* arg) {
          IOStatus* status = static_cast<IOStatus*>(arg);
          *status = IOStatus::Corruption("test");
          ++callback_count;
        });
    SyncPoint::GetInstance()->EnableProcessing();

    while (reader->ReadLine(&line)) {
      ASSERT_EQ(line, GenerateLine(count));
      ++count;
      ASSERT_EQ(static_cast<int>(reader->GetLineNumber()), count);
    }
    ASSERT_TRUE(reader->GetStatus().IsCorruption());
    ASSERT_LT(count, nlines / 2);
    ASSERT_EQ(callback_count, 1);

    // Still get error & no retry
    ASSERT_FALSE(reader->ReadLine(&line));
    ASSERT_TRUE(reader->GetStatus().IsCorruption());
    ASSERT_EQ(callback_count, 1);

    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
  }
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
#include <algorithm>

#include "file/writable_file_writer.h"
#include "rocksdb/file_system.h"
#include "test_util/testharness.h"
#include "test_util/testutil.h"

namespace ROCKSDB_NAMESPACE {

class WritableFileWriterIOPriorityTest : public testing::Test {
 protected:
  // This test is to check whether the rate limiter priority can be passed
  // correctly from WritableFileWriter functions to FSWritableFile functions.

  void SetUp() override {
    // When op_rate_limiter_priority parameter in WritableFileWriter functions
    // is the default (Env::IO_TOTAL).
    std::unique_ptr<FakeWF> wf{new FakeWF(Env::IO_HIGH)};
    FileOptions file_options;
    writer_.reset(new WritableFileWriter(std::move(wf), "" /* don't care */,
                                         file_options));
  }

  class FakeWF : public FSWritableFile {
   public:
    explicit FakeWF(Env::IOPriority io_priority) { SetIOPriority(io_priority); }
    ~FakeWF() override {}

    IOStatus Append(const Slice& /*data*/, const IOOptions& options,
                    IODebugContext* /*dbg*/) override {
      EXPECT_EQ(options.rate_limiter_priority, io_priority_);
      return IOStatus::OK();
    }
    IOStatus Append(const Slice& data, const IOOptions& options,
                    const DataVerificationInfo& /* verification_info */,
                    IODebugContext* dbg) override {
      return Append(data, options, dbg);
    }
    IOStatus PositionedAppend(const Slice& /*data*/, uint64_t /*offset*/,
                              const IOOptions& options,
                              IODebugContext* /*dbg*/) override {
      EXPECT_EQ(options.rate_limiter_priority, io_priority_);
      return IOStatus::OK();
    }
    IOStatus PositionedAppend(
        const Slice& /* data */, uint64_t /* offset */,
        const IOOptions& options,
        const DataVerificationInfo& /* verification_info */,
        IODebugContext* /*dbg*/) override {
      EXPECT_EQ(options.rate_limiter_priority, io_priority_);
      return IOStatus::OK();
    }
    IOStatus Truncate(uint64_t /*size*/, const IOOptions& options,
                      IODebugContext* /*dbg*/) override {
      EXPECT_EQ(options.rate_limiter_priority, io_priority_);
      return IOStatus::OK();
    }
    IOStatus Close(const IOOptions& options, IODebugContext* /*dbg*/) override {
      EXPECT_EQ(options.rate_limiter_priority, io_priority_);
      return IOStatus::OK();
    }
    IOStatus Flush(const IOOptions& options, IODebugContext* /*dbg*/) override {
      EXPECT_EQ(options.rate_limiter_priority, io_priority_);
      return IOStatus::OK();
    }
    IOStatus Sync(const IOOptions& options, IODebugContext* /*dbg*/) override {
      EXPECT_EQ(options.rate_limiter_priority, io_priority_);
      return IOStatus::OK();
    }
    IOStatus Fsync(const IOOptions& options, IODebugContext* /*dbg*/) override {
      EXPECT_EQ(options.rate_limiter_priority, io_priority_);
      return IOStatus::OK();
    }
    uint64_t GetFileSize(const IOOptions& options,
                         IODebugContext* /*dbg*/) override {
      EXPECT_EQ(options.rate_limiter_priority, io_priority_);
      return 0;
    }
    void GetPreallocationStatus(size_t* /*block_size*/,
                                size_t* /*last_allocated_block*/) override {}
    size_t GetUniqueId(char* /*id*/, size_t /*max_size*/) const override {
      return 0;
    }
    IOStatus InvalidateCache(size_t /*offset*/, size_t /*length*/) override {
      return IOStatus::OK();
    }

    IOStatus Allocate(uint64_t /*offset*/, uint64_t /*len*/,
                      const IOOptions& options,
                      IODebugContext* /*dbg*/) override {
      EXPECT_EQ(options.rate_limiter_priority, io_priority_);
      return IOStatus::OK();
    }
    IOStatus RangeSync(uint64_t /*offset*/, uint64_t /*nbytes*/,
                       const IOOptions& options,
                       IODebugContext* /*dbg*/) override {
      EXPECT_EQ(options.rate_limiter_priority, io_priority_);
      return IOStatus::OK();
    }

    void PrepareWrite(size_t /*offset*/, size_t /*len*/,
                      const IOOptions& options,
                      IODebugContext* /*dbg*/) override {
      EXPECT_EQ(options.rate_limiter_priority, io_priority_);
    }

    bool IsSyncThreadSafe() const override { return true; }
  };

  std::unique_ptr<WritableFileWriter> writer_;
};

TEST_F(WritableFileWriterIOPriorityTest, Append) {
  writer_->Append(Slice("abc"));
}

TEST_F(WritableFileWriterIOPriorityTest, Pad) { writer_->Pad(500); }

TEST_F(WritableFileWriterIOPriorityTest, Flush) { writer_->Flush(); }

TEST_F(WritableFileWriterIOPriorityTest, Close) { writer_->Close(); }

TEST_F(WritableFileWriterIOPriorityTest, Sync) {
  writer_->Sync(false);
  // writer_->Sync(true);
}

TEST_F(WritableFileWriterIOPriorityTest, SyncWithoutFlush) {
  // writer_->SyncWithoutFlush(false);
  writer_->SyncWithoutFlush(true);
}

TEST_F(WritableFileWriterIOPriorityTest, SyncW) {
  uint32_t kMb = static_cast<uint32_t>(1) << 20;
  EnvOptions env_options;
  env_options.bytes_per_sync = kMb;
  std::unique_ptr<FakeWF> wf(new FakeWF(Env::IO_HIGH));
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

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

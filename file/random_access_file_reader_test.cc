//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "file/random_access_file_reader.h"

#include <algorithm>

#include "file/file_util.h"
#include "port/port.h"
#include "port/stack_trace.h"
#include "rocksdb/file_system.h"
#include "test_util/sync_point.h"
#include "test_util/testharness.h"
#include "test_util/testutil.h"
#include "util/random.h"

namespace ROCKSDB_NAMESPACE {

class RandomAccessFileReaderTest : public testing::Test {
 public:
  void SetUp() override {
    SetupSyncPointsToMockDirectIO();
    env_ = Env::Default();
    fs_ = FileSystem::Default();
    test_dir_ = test::PerThreadDBPath("random_access_file_reader_test");
    ASSERT_OK(fs_->CreateDir(test_dir_, IOOptions(), nullptr));
  }

  void TearDown() override { EXPECT_OK(DestroyDir(env_, test_dir_)); }

  void Write(const std::string& fname, const std::string& content) {
    std::unique_ptr<FSWritableFile> f;
    ASSERT_OK(fs_->NewWritableFile(Path(fname), FileOptions(), &f, nullptr));
    ASSERT_OK(f->Append(content, IOOptions(), nullptr));
    ASSERT_OK(f->Close(IOOptions(), nullptr));
  }

  void Read(const std::string& fname, const FileOptions& opts,
            std::unique_ptr<RandomAccessFileReader>* reader) {
    std::string fpath = Path(fname);
    std::unique_ptr<FSRandomAccessFile> f;
    ASSERT_OK(fs_->NewRandomAccessFile(fpath, opts, &f, nullptr));
    reader->reset(new RandomAccessFileReader(std::move(f), fpath,
                                             env_->GetSystemClock().get()));
  }

  void AssertResult(const std::string& content,
                    const std::vector<FSReadRequest>& reqs) {
    for (const auto& r : reqs) {
      ASSERT_OK(r.status);
      ASSERT_EQ(r.len, r.result.size());
      ASSERT_EQ(content.substr(r.offset, r.len), r.result.ToString());
    }
  }

  const std::shared_ptr<FileSystem>& file_system() const { return fs_; }
  std::string TestPath(const std::string& fname) { return Path(fname); }

 private:
  Env* env_;
  std::shared_ptr<FileSystem> fs_;
  std::string test_dir_;

  std::string Path(const std::string& fname) { return test_dir_ + "/" + fname; }
};

namespace {

struct TestRetainedBufferAllocatorState {
  size_t allocations = 0;
};

void DeleteRetainedBufferForTest(void* arg1, void* /*arg2*/) {
  delete[] static_cast<char*>(arg1);
}

Status AllocateRetainedBufferForTest(void* state, size_t size, size_t alignment,
                                     RetainedBufferAllocation* out) {
  assert(out != nullptr);
  auto* allocator_state = static_cast<TestRetainedBufferAllocatorState*>(state);
  allocator_state->allocations++;

  const size_t extra = alignment > 1 ? alignment - 1 : 0;
  char* raw = new char[size + extra];
  uintptr_t aligned = reinterpret_cast<uintptr_t>(raw);
  if (alignment > 1) {
    aligned =
        (aligned + alignment - 1) & ~static_cast<uintptr_t>(alignment - 1);
  }

  out->Reset();
  out->data = reinterpret_cast<char*>(aligned);
  out->size = size;
  out->cleanup.Allocate();
  out->cleanup->RegisterCleanup(&DeleteRetainedBufferForTest, raw, nullptr);
  out->dedupe_token = out->data;
  return Status::OK();
}

class SyncReadAsyncFile : public FSRandomAccessFileOwnerWrapper {
 public:
  explicit SyncReadAsyncFile(std::unique_ptr<FSRandomAccessFile>&& target)
      : FSRandomAccessFileOwnerWrapper(std::move(target)) {}

  IOStatus ReadAsync(FSReadRequest& req, const IOOptions& opts,
                     std::function<void(FSReadRequest&, void*)> cb,
                     void* cb_arg, void** io_handle, IOHandleDeleter* del_fn,
                     IODebugContext* dbg) override {
    req.status = Read(req.offset, req.len, opts, &req.result, req.scratch, dbg);
    if (io_handle != nullptr) {
      *io_handle = nullptr;
    }
    if (del_fn != nullptr) {
      *del_fn = nullptr;
    }
    cb(req, cb_arg);
    return IOStatus::OK();
  }
};

}  // namespace

// Skip the following tests in lite mode since direct I/O is unsupported.

TEST_F(RandomAccessFileReaderTest, ReadDirectIO) {
  std::string fname = "read-direct-io";
  Random rand(0);
  std::string content = rand.RandomString(kDefaultPageSize);
  Write(fname, content);

  FileOptions opts;
  opts.use_direct_reads = true;
  std::unique_ptr<RandomAccessFileReader> r;
  Read(fname, opts, &r);
  ASSERT_TRUE(r->use_direct_io());

  const size_t page_size = r->file()->GetRequiredBufferAlignment();
  size_t offset = page_size / 2;
  size_t len = page_size / 3;
  Slice result;
  AlignedBuf buf;
  for (Env::IOPriority rate_limiter_priority : {Env::IO_LOW, Env::IO_TOTAL}) {
    IOOptions io_opts;
    io_opts.rate_limiter_priority = rate_limiter_priority;
    ASSERT_OK(r->Read(io_opts, offset, len, &result, nullptr, &buf));
    ASSERT_EQ(result.ToString(), content.substr(offset, len));
  }
}

TEST_F(RandomAccessFileReaderTest, MultiReadDirectIO) {
  std::vector<FSReadRequest> aligned_reqs;
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "RandomAccessFileReader::MultiRead:AlignedReqs", [&](void* reqs) {
        // Copy reqs, since it's allocated on stack inside MultiRead, which will
        // be deallocated after MultiRead returns.
        size_t i = 0;
        aligned_reqs.resize(
            (*reinterpret_cast<std::vector<FSReadRequest>*>(reqs)).size());
        for (auto& req :
             (*reinterpret_cast<std::vector<FSReadRequest>*>(reqs))) {
          aligned_reqs[i].offset = req.offset;
          aligned_reqs[i].len = req.len;
          aligned_reqs[i].result = req.result;
          aligned_reqs[i].status = req.status;
          aligned_reqs[i].scratch = req.scratch;
          i++;
        }
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  // Creates a file with 3 pages.
  std::string fname = "multi-read-direct-io";
  Random rand(0);
  std::string content = rand.RandomString(3 * kDefaultPageSize);
  Write(fname, content);

  FileOptions opts;
  opts.use_direct_reads = true;
  std::unique_ptr<RandomAccessFileReader> r;
  Read(fname, opts, &r);
  ASSERT_TRUE(r->use_direct_io());

  const size_t page_size = r->file()->GetRequiredBufferAlignment();

  {
    // Reads 2 blocks in the 1st page.
    // The results should be SharedSlices of the same underlying buffer.
    //
    // Illustration (each x is a 1/4 page)
    // First page: xxxx
    // 1st block:  x
    // 2nd block:    xx
    FSReadRequest r0;
    r0.offset = 0;
    r0.len = page_size / 4;
    r0.scratch = nullptr;

    FSReadRequest r1;
    r1.offset = page_size / 2;
    r1.len = page_size / 2;
    r1.scratch = nullptr;

    std::vector<FSReadRequest> reqs;
    reqs.push_back(std::move(r0));
    reqs.push_back(std::move(r1));
    AlignedBuf aligned_buf;
    IODebugContext dbg;
    ASSERT_OK(r->MultiRead(IOOptions(), reqs.data(), reqs.size(), &aligned_buf,
                           &dbg));

    AssertResult(content, reqs);

    // Reads the first page internally.
    ASSERT_EQ(aligned_reqs.size(), 1);
    const FSReadRequest& aligned_r = aligned_reqs[0];
    ASSERT_OK(aligned_r.status);
    ASSERT_EQ(aligned_r.offset, 0);
    ASSERT_EQ(aligned_r.len, page_size);
  }

  {
    // Reads 3 blocks:
    // 1st block in the 1st page;
    // 2nd block from the middle of the 1st page to the middle of the 2nd page;
    // 3rd block in the 2nd page.
    // The results should be SharedSlices of the same underlying buffer.
    //
    // Illustration (each x is a 1/4 page)
    // 2 pages:   xxxxxxxx
    // 1st block: x
    // 2nd block:   xxxx
    // 3rd block:        x
    FSReadRequest r0;
    r0.offset = 0;
    r0.len = page_size / 4;
    r0.scratch = nullptr;

    FSReadRequest r1;
    r1.offset = page_size / 2;
    r1.len = page_size;
    r1.scratch = nullptr;

    FSReadRequest r2;
    r2.offset = 2 * page_size - page_size / 4;
    r2.len = page_size / 4;
    r2.scratch = nullptr;

    std::vector<FSReadRequest> reqs;
    reqs.push_back(std::move(r0));
    reqs.push_back(std::move(r1));
    reqs.push_back(std::move(r2));
    AlignedBuf aligned_buf;
    IODebugContext dbg;
    ASSERT_OK(r->MultiRead(IOOptions(), reqs.data(), reqs.size(), &aligned_buf,
                           &dbg));

    AssertResult(content, reqs);

    // Reads the first two pages in one request internally.
    ASSERT_EQ(aligned_reqs.size(), 1);
    const FSReadRequest& aligned_r = aligned_reqs[0];
    ASSERT_OK(aligned_r.status);
    ASSERT_EQ(aligned_r.offset, 0);
    ASSERT_EQ(aligned_r.len, 2 * page_size);
  }

  {
    // Reads 3 blocks:
    // 1st block in the middle of the 1st page;
    // 2nd block in the middle of the 2nd page;
    // 3rd block in the middle of the 3rd page.
    // The results should be SharedSlices of the same underlying buffer.
    //
    // Illustration (each x is a 1/4 page)
    // 3 pages:   xxxxxxxxxxxx
    // 1st block:  xx
    // 2nd block:      xx
    // 3rd block:          xx
    FSReadRequest r0;
    r0.offset = page_size / 4;
    r0.len = page_size / 2;
    r0.scratch = nullptr;

    FSReadRequest r1;
    r1.offset = page_size + page_size / 4;
    r1.len = page_size / 2;
    r1.scratch = nullptr;

    FSReadRequest r2;
    r2.offset = 2 * page_size + page_size / 4;
    r2.len = page_size / 2;
    r2.scratch = nullptr;

    std::vector<FSReadRequest> reqs;
    reqs.push_back(std::move(r0));
    reqs.push_back(std::move(r1));
    reqs.push_back(std::move(r2));
    AlignedBuf aligned_buf;
    IODebugContext dbg;
    ASSERT_OK(r->MultiRead(IOOptions(), reqs.data(), reqs.size(), &aligned_buf,
                           &dbg));

    AssertResult(content, reqs);

    // Reads the first 3 pages in one request internally.
    ASSERT_EQ(aligned_reqs.size(), 1);
    const FSReadRequest& aligned_r = aligned_reqs[0];
    ASSERT_OK(aligned_r.status);
    ASSERT_EQ(aligned_r.offset, 0);
    ASSERT_EQ(aligned_r.len, 3 * page_size);
  }

  {
    // Reads 2 blocks:
    // 1st block in the middle of the 1st page;
    // 2nd block in the middle of the 3rd page.
    // The results are two different buffers.
    //
    // Illustration (each x is a 1/4 page)
    // 3 pages:   xxxxxxxxxxxx
    // 1st block:  xx
    // 2nd block:          xx
    FSReadRequest r0;
    r0.offset = page_size / 4;
    r0.len = page_size / 2;
    r0.scratch = nullptr;

    FSReadRequest r1;
    r1.offset = 2 * page_size + page_size / 4;
    r1.len = page_size / 2;
    r1.scratch = nullptr;

    std::vector<FSReadRequest> reqs;
    reqs.push_back(std::move(r0));
    reqs.push_back(std::move(r1));
    AlignedBuf aligned_buf;
    IODebugContext dbg;
    ASSERT_OK(r->MultiRead(IOOptions(), reqs.data(), reqs.size(), &aligned_buf,
                           &dbg));

    AssertResult(content, reqs);

    // Reads the 1st and 3rd pages in two requests internally.
    ASSERT_EQ(aligned_reqs.size(), 2);
    const FSReadRequest& aligned_r0 = aligned_reqs[0];
    const FSReadRequest& aligned_r1 = aligned_reqs[1];
    ASSERT_OK(aligned_r0.status);
    ASSERT_EQ(aligned_r0.offset, 0);
    ASSERT_EQ(aligned_r0.len, page_size);
    ASSERT_OK(aligned_r1.status);
    ASSERT_EQ(aligned_r1.offset, 2 * page_size);
    ASSERT_EQ(aligned_r1.len, page_size);
  }

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearAllCallBacks();
}

TEST_F(RandomAccessFileReaderTest, ReadAsyncDirectIOExternalScratch) {
  std::string fname = "read-async-direct-io-external-scratch";
  Random rand(0);
  std::string content = rand.RandomString(2 * kDefaultPageSize);
  Write(fname, content);

  FileOptions opts;
  opts.use_direct_reads = true;
  std::string fpath = TestPath(fname);
  std::unique_ptr<FSRandomAccessFile> file;
  ASSERT_OK(file_system()->NewRandomAccessFile(fpath, opts, &file, nullptr));
  std::unique_ptr<FSRandomAccessFile> sync_async_file(
      new SyncReadAsyncFile(std::move(file)));
  std::unique_ptr<RandomAccessFileReader> r(new RandomAccessFileReader(
      std::move(sync_async_file), fpath, SystemClock::Default().get()));
  ASSERT_TRUE(r->use_direct_io());

  const size_t page_size = r->file()->GetRequiredBufferAlignment();

  FSReadRequest req;
  req.offset = page_size / 2;
  req.len = page_size / 3;
  req.scratch = nullptr;
  req.status.PermitUncheckedError();

  const FSReadRequest aligned_req = Align(req, page_size);
  ASSERT_OK(aligned_req.status);
  std::unique_ptr<char[]> raw_scratch(
      new char[aligned_req.len + page_size - 1]);
  const uintptr_t raw = reinterpret_cast<uintptr_t>(raw_scratch.get());
  const uintptr_t aligned = ((raw + page_size - 1) / page_size) * page_size;
  char* aligned_scratch = reinterpret_cast<char*>(aligned);

  bool callback_called = false;
  Slice callback_result;
  IOStatus callback_status;
  void* io_handle = nullptr;
  IOHandleDeleter del_fn;
  auto cb = [&](FSReadRequest& cb_req, void* /*cb_arg*/) {
    callback_called = true;
    callback_result = cb_req.result;
    callback_status = cb_req.status;
  };

  ASSERT_OK(r->ReadAsync(req, IOOptions(), cb, nullptr, &io_handle, &del_fn,
                         /*aligned_buf=*/nullptr, /*dbg=*/nullptr,
                         aligned_scratch));
  ASSERT_EQ(nullptr, io_handle);

  ASSERT_TRUE(callback_called);
  ASSERT_OK(callback_status);
  ASSERT_EQ(content.substr(req.offset, req.len), callback_result.ToString());
  ASSERT_GE(callback_result.data(), aligned_scratch);
  ASSERT_LE(callback_result.data() + callback_result.size(),
            aligned_scratch + aligned_req.len);
}

TEST_F(RandomAccessFileReaderTest, ReadAsyncDirectIORetainedBuffer) {
  std::string fname = "read-async-direct-io-retained-buffer";
  Random rand(0);
  std::string content = rand.RandomString(2 * kDefaultPageSize);
  Write(fname, content);

  FileOptions opts;
  opts.use_direct_reads = true;
  std::string fpath = TestPath(fname);
  std::unique_ptr<FSRandomAccessFile> file;
  ASSERT_OK(file_system()->NewRandomAccessFile(fpath, opts, &file, nullptr));
  std::unique_ptr<FSRandomAccessFile> sync_async_file(
      new SyncReadAsyncFile(std::move(file)));
  std::unique_ptr<RandomAccessFileReader> r(new RandomAccessFileReader(
      std::move(sync_async_file), fpath, SystemClock::Default().get()));
  ASSERT_TRUE(r->use_direct_io());

  const size_t page_size = r->file()->GetRequiredBufferAlignment();

  FSReadRequest req;
  req.offset = page_size / 2;
  req.len = page_size / 3;
  req.scratch = nullptr;
  req.status.PermitUncheckedError();

  TestRetainedBufferAllocatorState allocator_state;
  RetainedBufferAllocator allocator(&allocator_state,
                                    &AllocateRetainedBufferForTest);
  RetainedBufferAllocation retained_buffer;

  bool callback_called = false;
  Slice callback_result;
  IOStatus callback_status;
  void* io_handle = nullptr;
  IOHandleDeleter del_fn;
  auto cb = [&](FSReadRequest& cb_req, void* /*cb_arg*/) {
    callback_called = true;
    callback_result = cb_req.result;
    callback_status = cb_req.status;
  };

  ASSERT_OK(r->ReadAsync(req, IOOptions(), cb, nullptr, &io_handle, &del_fn,
                         /*aligned_buf=*/nullptr, /*dbg=*/nullptr,
                         /*direct_io_scratch=*/nullptr, &allocator,
                         &retained_buffer));
  ASSERT_EQ(nullptr, io_handle);

  ASSERT_TRUE(callback_called);
  ASSERT_OK(callback_status);
  ASSERT_EQ(1U, allocator_state.allocations);
  ASSERT_NE(nullptr, retained_buffer.cleanup.get());
  ASSERT_EQ(content.substr(req.offset, req.len), callback_result.ToString());
  ASSERT_GE(reinterpret_cast<uintptr_t>(callback_result.data()),
            reinterpret_cast<uintptr_t>(retained_buffer.data));
  ASSERT_LE(
      reinterpret_cast<uintptr_t>(callback_result.data() +
                                  callback_result.size()),
      reinterpret_cast<uintptr_t>(retained_buffer.data + retained_buffer.size));
}

TEST_F(RandomAccessFileReaderTest, MultiReadDirectIORetainedBuffer) {
  std::string fname = "multi-read-direct-io-retained-buffer";
  Random rand(0);
  std::string content = rand.RandomString(2 * kDefaultPageSize);
  Write(fname, content);

  FileOptions opts;
  opts.use_direct_reads = true;
  std::unique_ptr<RandomAccessFileReader> r;
  Read(fname, opts, &r);
  ASSERT_TRUE(r->use_direct_io());

  const size_t page_size = r->file()->GetRequiredBufferAlignment();

  FSReadRequest r0;
  r0.offset = page_size / 4;
  r0.len = page_size / 4;
  r0.scratch = nullptr;

  FSReadRequest r1;
  r1.offset = page_size + page_size / 4;
  r1.len = page_size / 2;
  r1.scratch = nullptr;

  std::vector<FSReadRequest> reqs;
  reqs.push_back(std::move(r0));
  reqs.push_back(std::move(r1));

  TestRetainedBufferAllocatorState allocator_state;
  RetainedBufferAllocator allocator(&allocator_state,
                                    &AllocateRetainedBufferForTest);
  RetainedBufferAllocation retained_buffer;
  AlignedBuf aligned_buf;
  IODebugContext dbg;
  ASSERT_OK(r->MultiRead(IOOptions(), reqs.data(), reqs.size(), &aligned_buf,
                         &dbg, &allocator, &retained_buffer));

  AssertResult(content, reqs);
  ASSERT_EQ(1U, allocator_state.allocations);
  ASSERT_NE(nullptr, retained_buffer.cleanup.get());
  ASSERT_EQ(retained_buffer.data, aligned_buf.get());
  for (const auto& req : reqs) {
    ASSERT_GE(reinterpret_cast<uintptr_t>(req.result.data()),
              reinterpret_cast<uintptr_t>(retained_buffer.data));
    ASSERT_LE(
        reinterpret_cast<uintptr_t>(req.result.data() + req.result.size()),
        reinterpret_cast<uintptr_t>(retained_buffer.data +
                                    retained_buffer.size));
  }
}

TEST(FSReadRequest, Align) {
  FSReadRequest r;
  r.offset = 2000;
  r.len = 2000;
  r.scratch = nullptr;
  ASSERT_OK(r.status);

  FSReadRequest aligned_r = Align(r, 1024);
  ASSERT_OK(r.status);
  ASSERT_OK(aligned_r.status);
  ASSERT_EQ(aligned_r.offset, 1024);
  ASSERT_EQ(aligned_r.len, 3072);
}

TEST(FSReadRequest, TryMerge) {
  // reverse means merging dest into src.
  for (bool reverse : {true, false}) {
    {
      // dest: [ ]
      //  src:      [ ]
      FSReadRequest dest;
      dest.offset = 0;
      dest.len = 10;
      dest.scratch = nullptr;
      ASSERT_OK(dest.status);

      FSReadRequest src;
      src.offset = 15;
      src.len = 10;
      src.scratch = nullptr;
      ASSERT_OK(src.status);

      if (reverse) {
        std::swap(dest, src);
      }
      ASSERT_FALSE(TryMerge(&dest, src));
      ASSERT_OK(dest.status);
      ASSERT_OK(src.status);
    }

    {
      // dest: [ ]
      //  src:   [ ]
      FSReadRequest dest;
      dest.offset = 0;
      dest.len = 10;
      dest.scratch = nullptr;
      ASSERT_OK(dest.status);

      FSReadRequest src;
      src.offset = 10;
      src.len = 10;
      src.scratch = nullptr;
      ASSERT_OK(src.status);

      if (reverse) {
        std::swap(dest, src);
      }
      ASSERT_TRUE(TryMerge(&dest, src));
      ASSERT_EQ(dest.offset, 0);
      ASSERT_EQ(dest.len, 20);
      ASSERT_OK(dest.status);
      ASSERT_OK(src.status);
    }

    {
      // dest: [    ]
      //  src:   [    ]
      FSReadRequest dest;
      dest.offset = 0;
      dest.len = 10;
      dest.scratch = nullptr;
      ASSERT_OK(dest.status);

      FSReadRequest src;
      src.offset = 5;
      src.len = 10;
      src.scratch = nullptr;
      ASSERT_OK(src.status);

      if (reverse) {
        std::swap(dest, src);
      }
      ASSERT_TRUE(TryMerge(&dest, src));
      ASSERT_EQ(dest.offset, 0);
      ASSERT_EQ(dest.len, 15);
      ASSERT_OK(dest.status);
      ASSERT_OK(src.status);
    }

    {
      // dest: [    ]
      //  src:   [  ]
      FSReadRequest dest;
      dest.offset = 0;
      dest.len = 10;
      dest.scratch = nullptr;
      ASSERT_OK(dest.status);

      FSReadRequest src;
      src.offset = 5;
      src.len = 5;
      src.scratch = nullptr;
      ASSERT_OK(src.status);

      if (reverse) {
        std::swap(dest, src);
      }
      ASSERT_TRUE(TryMerge(&dest, src));
      ASSERT_EQ(dest.offset, 0);
      ASSERT_EQ(dest.len, 10);
      ASSERT_OK(dest.status);
      ASSERT_OK(src.status);
    }

    {
      // dest: [     ]
      //  src:   [ ]
      FSReadRequest dest;
      dest.offset = 0;
      dest.len = 10;
      dest.scratch = nullptr;
      ASSERT_OK(dest.status);

      FSReadRequest src;
      src.offset = 5;
      src.len = 1;
      src.scratch = nullptr;
      ASSERT_OK(src.status);

      if (reverse) {
        std::swap(dest, src);
      }
      ASSERT_TRUE(TryMerge(&dest, src));
      ASSERT_EQ(dest.offset, 0);
      ASSERT_EQ(dest.len, 10);
      ASSERT_OK(dest.status);
      ASSERT_OK(src.status);
    }

    {
      // dest: [ ]
      //  src: [ ]
      FSReadRequest dest;
      dest.offset = 0;
      dest.len = 10;
      dest.scratch = nullptr;
      ASSERT_OK(dest.status);

      FSReadRequest src;
      src.offset = 0;
      src.len = 10;
      src.scratch = nullptr;
      ASSERT_OK(src.status);

      if (reverse) {
        std::swap(dest, src);
      }
      ASSERT_TRUE(TryMerge(&dest, src));
      ASSERT_EQ(dest.offset, 0);
      ASSERT_EQ(dest.len, 10);
      ASSERT_OK(dest.status);
      ASSERT_OK(src.status);
    }

    {
      // dest: [   ]
      //  src: [ ]
      FSReadRequest dest;
      dest.offset = 0;
      dest.len = 10;
      dest.scratch = nullptr;
      ASSERT_OK(dest.status);

      FSReadRequest src;
      src.offset = 0;
      src.len = 5;
      src.scratch = nullptr;
      ASSERT_OK(src.status);

      if (reverse) {
        std::swap(dest, src);
      }
      ASSERT_TRUE(TryMerge(&dest, src));
      ASSERT_EQ(dest.offset, 0);
      ASSERT_EQ(dest.len, 10);
      ASSERT_OK(dest.status);
      ASSERT_OK(src.status);
    }
  }
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

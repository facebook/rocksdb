// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//

#if !(defined ROCKSDB_COROUTINES)
#error "Only for Coroutines support"
#endif

#include "rocksdb/status.h"

#include "db/db_test_util.h"
#include "rocksdb/env.h"
#include "port/port.h"
#include <algorithm>
#include <fstream>
#include <vector>
#include <future>

#include "util/aligned_buffer.h"
#include "util/random.h"
#include "util/testharness.h"
#include "util/testutil.h"

#include "port/win/io_win.h"

namespace rocksdb {

// Helper for quickly generating random data.
class RandomGenerator {
private:
  std::string data_;
  size_t      pos_;

public:
  RandomGenerator(size_t valsize, double compression_ratio) :
    pos_(0) {
    // We use a limited amount of data over and over again and ensure
    // that it is larger than the compression window (32KB), and also
    // large enough to serve all typical value sizes we want to write.
    Random rnd(301);
    std::string piece;
    while (data_.size() < std::max<size_t>(1048576U, valsize)) {
      // Add a short fragment that is as compressible as specified
      // by FLAGS_compression_ratio.
      test::CompressibleString(&rnd, compression_ratio, 100, &piece);
      data_.append(piece);
    }
  }

  Slice Generate(size_t len) {
    assert(len <= data_.size());
    if (pos_ + len > data_.size()) {
      pos_ = 0;
    }
    pos_ += len;
    return Slice(data_.data() + pos_ - len, len);
  }
};

class RandomAccessCoroutineTest :
  public testing::Test,
  public testing::WithParamInterface<bool> {

public:

  PTP_POOL    tp_pool_;
  bool        direct_io_;
  std::string fileName_;

  RandomAccessCoroutineTest() : tp_pool_(nullptr) {

    tp_pool_ = CreateThreadpool(NULL);
    BOOL ret = SetThreadpoolThreadMinimum(tp_pool_, 5);
    assert(ret);
    SetThreadpoolThreadMaximum(tp_pool_, 10);

    direct_io_ = GetParam();

    // Generate a file for reading
    fileName_ = test::TmpDir();
    fileName_ += "/io_completion.bin";

    {
      std::ofstream os(fileName_, std::ios::binary | std::ios::out);

      // Writing 64 K so we can issue 8 reads 8K each
      //
      RandomGenerator gen(10 * 1024U, 0.5);
      uint64_t sizeWritten = 0;
      while (sizeWritten < 1024 * 64U) {
        Slice data = gen.Generate(4096);
        os.write(data.data(), data.size());
        sizeWritten += data.size();
      }
      os.flush();
      os.close();
    }
  }

  ~RandomAccessCoroutineTest() {
    unlink(fileName_.c_str());
    if (tp_pool_ != nullptr) {
      CloseThreadpool(tp_pool_);
    }
  }
};

// We use future here because we can wait on it
// in the test but generally we do not have to wait
// for anything since the code completes on another thread
std::future<void> InitiateAsyncRead(Env* env, RandomAccessFileReader* reader,
  uint64_t offset, size_t n) {

  Slice result;
  AlignedBuffer  buffer;
  buffer.Alignment(reader->file()->GetRequiredBufferAlignment());
  buffer.AllocateNewBuffer(n);

  auto tid = env->GetThreadID();
  Status s = co_await reader->RequestRead(offset, n, &result, buffer.BufferStart());

  // The thread that calls back has no reference to the caller since the
  // caller is NOT a coroutine so the thread goes away
  std::cout << "Thread ID before co_await: " << tid << " after co_await: " << env->GetThreadID() << "\n";
  std::cout << "Read:" << result.size() << " bytes, status: " << s.ToString() << "\n";
  // This is just to signal the future
  co_return;
}

TEST_P(RandomAccessCoroutineTest, TestAsyncRead) {

  Env* env = Env::Default();
  // This is where the io completions will run
  auto io_tp(env->CreateAsyncThreadPool(tp_pool_));

  EnvOptions options;
  options.use_direct_reads = direct_io_;
  options.use_async_reads = true;
  options.async_threadpool = io_tp.get();

  std::unique_ptr<RandomAccessFile> file;
  Status s = env->NewRandomAccessFile(fileName_, &file, options);
  ASSERT_OK(s);

  std::unique_ptr<RandomAccessFileReader> f_reader(new RandomAccessFileReader(std::move(file), 
    fileName_, env));

  {
    // Single wait
    // Make sure that offsets and sizes are not factors of
    // page size
    std::future<void> f = InitiateAsyncRead(env, f_reader.get(), 815, 8 * 1024 - 256);
    // Wait here for the callback to return
    f.get();
    std::cout << "Wait completed" << std::endl;
  }

  // Now several threads at once
  {
    const size_t tnum = 12;
    std::vector<port::Thread> threads;
    threads.reserve(tnum);

    auto t = [env, &f_reader](size_t cust, size_t size)
    {
      uint64_t offset = 1024 * cust + 37;
      std::future<void> f = InitiateAsyncRead(env, f_reader.get(), offset, size);
      // Wait here for the callback to return
      f.get();
    };

    for (size_t n = 0; n < tnum; ++n) {
      threads.emplace_back(t, n, 8 * 1024 - 257);
    }

    for (auto& th : threads) {
      th.join();
    }
  }

  io_tp->CloseThreadPool();
}

INSTANTIATE_TEST_CASE_P(
  TestAsyncRead, RandomAccessCoroutineTest,
  ::testing::ValuesIn({ false, true }));

} // rocksdb

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

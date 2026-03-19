//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "utilities/fault_injection_fs.h"

#include <atomic>
#include <thread>
#include <vector>

#include "test_util/testharness.h"

namespace ROCKSDB_NAMESPACE {

class InjectedErrorLogTest : public testing::Test {};

// Test basic Record and PrintAll functionality.
TEST_F(InjectedErrorLogTest, BasicRecordAndPrint) {
  InjectedErrorLog log;
  log.SetLogFilePath("/dev/null");

  // Record some entries.
  log.Record("op=Get key=0x%08x status=%s", 0x12345678, "OK");
  log.Record("op=Put key=0x%08x value_size=%d", 0xABCDEF00, 100);
  log.Record("op=Delete key=0x%08x", 0x00000001);

  // PrintAll should not crash.
  log.PrintAll();
}

// Test that the circular buffer wraps correctly.
TEST_F(InjectedErrorLogTest, CircularBufferWrap) {
  InjectedErrorLog log;
  log.SetLogFilePath("/dev/null");

  // Fill beyond kMaxEntries to trigger wraparound.
  for (size_t i = 0; i < InjectedErrorLog::kMaxEntries + 100; i++) {
    log.Record("entry=%zu", i);
  }

  // PrintAll should handle the wrapped buffer without crashing.
  log.PrintAll();
}

// Test concurrent Record() from multiple threads.
// Keep total records (kNumThreads * kRecordsPerThread) under kMaxEntries
// to avoid write-write races from buffer wraparound, which are benign but
// would trigger TSAN warnings.
TEST_F(InjectedErrorLogTest, ConcurrentRecord) {
  InjectedErrorLog log;
  constexpr int kNumThreads = 4;
  constexpr int kRecordsPerThread = 200;
  static_assert(kNumThreads * kRecordsPerThread <
                    static_cast<int>(InjectedErrorLog::kMaxEntries),
                "total records must stay within buffer to avoid TSAN-visible "
                "write-write races on overlapping slots");

  std::vector<std::thread> threads;
  threads.reserve(kNumThreads);
  for (int t = 0; t < kNumThreads; t++) {
    threads.emplace_back([&log, t]() {
      for (int i = 0; i < kRecordsPerThread; i++) {
        log.Record("thread=%d iter=%d op=Get key=0x%08x", t, i, i * 17);
      }
    });
  }

  for (auto& t : threads) {
    t.join();
  }

  // PrintAll after all threads are done -- no race.
  log.SetLogFilePath("/dev/null");
  log.PrintAll();
}

// Test HexHead utility.
TEST_F(InjectedErrorLogTest, HexHead) {
  const char data[] = "\x01\x02\xAB\xCD";
  std::string result = InjectedErrorLog::HexHead(data, 4);
  ASSERT_EQ(result, "01 02 ab cd");

  result = InjectedErrorLog::HexHead(data, 4, 2);
  ASSERT_EQ(result, "01 02 ...");
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

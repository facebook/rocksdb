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
class FaultInjectionTestFSTest : public testing::Test {};

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

TEST_F(FaultInjectionTestFSTest, MetadataReadFaultExcludesInfoLogFiles) {
  Env* env = Env::Default();
  const std::string dbname =
      test::PerThreadDBPath("fault_injection_fs_test_metadata_read");
  const std::string log_dir = dbname + "_logs";
  ASSERT_OK(env->CreateDirIfMissing(dbname));
  ASSERT_OK(env->CreateDirIfMissing(log_dir));

  const std::string old_info_log = OldInfoLogFileName(dbname, 123, dbname,
                                                      log_dir);
  const std::string manifest = DescriptorFileName(dbname, 1);
  ASSERT_OK(
      WriteStringToFile(env, "old log", old_info_log, false /* should_sync */));
  ASSERT_OK(
      WriteStringToFile(env, "manifest", manifest, false /* should_sync */));

  auto fault_fs = std::make_shared<FaultInjectionTestFS>(env->GetFileSystem());
  fault_fs->SetFileTypesExcludedFromMetadataReadFaultInjection(
      {FileType::kInfoLogFile});
  fault_fs->SetThreadLocalErrorContext(
      FaultInjectionIOType::kMetadataRead, /*seed=*/0, /*one_in=*/1,
      /*retryable=*/false, /*has_data_loss=*/false);
  fault_fs->EnableThreadLocalErrorInjection(
      FaultInjectionIOType::kMetadataRead);

  ASSERT_OK(fault_fs->FileExists(old_info_log, IOOptions(), nullptr));
  ASSERT_EQ(0, fault_fs->GetAndResetInjectedThreadLocalErrorCount(
                   FaultInjectionIOType::kMetadataRead));

  IOStatus s = fault_fs->FileExists(manifest, IOOptions(), nullptr);
  ASSERT_NOK(s);
  ASSERT_TRUE(s.IsIOError()) << s.ToString();
  ASSERT_EQ(1, fault_fs->GetAndResetInjectedThreadLocalErrorCount(
                   FaultInjectionIOType::kMetadataRead));
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

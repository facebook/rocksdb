//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "utilities/fault_injection_fs.h"

#include <atomic>
#include <cstring>
#include <thread>
#include <vector>

#include "test_util/testharness.h"

namespace ROCKSDB_NAMESPACE {

class InjectedErrorLogTest : public testing::Test {
 protected:
  static std::string DecodeCString(const char* data, size_t len) {
    size_t actual_len = 0;
    while (actual_len < len && data[actual_len] != '\0') {
      ++actual_len;
    }
    return std::string(data, actual_len);
  }

  std::string ReadRawLog(const std::string& path) {
    std::string raw;
    Status s = ReadFileToString(Env::Default(), path, &raw);
    EXPECT_OK(s);
    return raw;
  }

  InjectedErrorLog::RawFileHeader DecodeHeader(const std::string& raw) {
    InjectedErrorLog::RawFileHeader header{};
    EXPECT_GE(raw.size(), sizeof(header));
    if (raw.size() >= sizeof(header)) {
      std::memcpy(&header, raw.data(), sizeof(header));
    }
    return header;
  }

  InjectedErrorLog::Entry DecodeEntry(const std::string& raw, size_t index) {
    InjectedErrorLog::Entry entry{};
    size_t offset = sizeof(InjectedErrorLog::RawFileHeader) +
                    index * sizeof(InjectedErrorLog::Entry);
    EXPECT_GE(raw.size(), offset + sizeof(entry));
    if (raw.size() >= offset + sizeof(entry)) {
      std::memcpy(&entry, raw.data() + offset, sizeof(entry));
    }
    return entry;
  }
};

TEST_F(InjectedErrorLogTest, BasicRecordAndDumpRaw) {
  std::string path = test::PerThreadDBPath("injected_error_log_basic.bin");
  InjectedErrorLog log;
  log.SetLogFilePath(path);
  log.Record("Append", "/tmp/000001.log",
             fault_injection_detail::OffsetSizeAndHead(7, Slice("abcd", 4)),
             "injected write error", false, true);
  log.DumpRaw();

  std::string raw = ReadRawLog(path);
  auto header = DecodeHeader(raw);
  ASSERT_EQ(std::string(header.magic, sizeof(header.magic)),
            std::string(InjectedErrorLog::kFileMagic.data(),
                        InjectedErrorLog::kFileMagic.size()));
  ASSERT_EQ(header.version, InjectedErrorLog::kFileVersion);
  ASSERT_EQ(header.entry_size, sizeof(InjectedErrorLog::Entry));
  ASSERT_EQ(header.dumped_entries, 1U);
  ASSERT_EQ(header.total_entries, 1U);

  auto entry = DecodeEntry(raw, 0);
  EXPECT_NE(entry.timestamp_us, 0U);
  EXPECT_EQ(entry.offset, 7U);
  EXPECT_EQ(entry.size, 4U);
  EXPECT_EQ(
      entry.detail_kind,
      static_cast<uint8_t>(InjectedErrorLog::DetailKind::kOffsetSizeAndHead));
  EXPECT_EQ(entry.detail_payload_size, 4U);
  EXPECT_EQ(entry.retryable, 0U);
  EXPECT_EQ(entry.data_loss, 1U);
  EXPECT_EQ(DecodeCString(entry.op_name, sizeof(entry.op_name)), "Append");
  EXPECT_EQ(DecodeCString(entry.file_name, sizeof(entry.file_name)),
            "/tmp/000001.log");
  EXPECT_EQ(DecodeCString(entry.status_message, sizeof(entry.status_message)),
            "injected write error");
  EXPECT_EQ(std::string(entry.detail_payload, entry.detail_payload + 4),
            "abcd");
}

TEST_F(InjectedErrorLogTest, CircularBufferWrap) {
  std::string path = test::PerThreadDBPath("injected_error_log_wrap.bin");
  InjectedErrorLog log;
  log.SetLogFilePath(path);

  for (size_t i = 0; i < InjectedErrorLog::kMaxEntries + 100; i++) {
    std::string file_name = "file" + std::to_string(i);
    log.Record("Append", file_name, fault_injection_detail::NoDetail(),
               "injected write error", false, false);
  }
  log.DumpRaw();

  std::string raw = ReadRawLog(path);
  auto header = DecodeHeader(raw);
  ASSERT_EQ(header.total_entries,
            static_cast<uint64_t>(InjectedErrorLog::kMaxEntries + 100));
  ASSERT_EQ(header.dumped_entries,
            static_cast<uint32_t>(InjectedErrorLog::kMaxEntries));

  auto first = DecodeEntry(raw, 0);
  auto last = DecodeEntry(raw, InjectedErrorLog::kMaxEntries - 1);
  EXPECT_EQ(DecodeCString(first.file_name, sizeof(first.file_name)), "file100");
  EXPECT_EQ(DecodeCString(last.file_name, sizeof(last.file_name)), "file1099");
}

TEST_F(InjectedErrorLogTest, ConcurrentRecord) {
  std::string path = test::PerThreadDBPath("injected_error_log_concurrent.bin");
  InjectedErrorLog log;
  log.SetLogFilePath(path);
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
        std::string file_name =
            "thread" + std::to_string(t) + "_" + std::to_string(i);
        log.Record("Read", file_name, fault_injection_detail::NoDetail(),
                   "injected read error", false, false);
      }
    });
  }

  for (auto& t : threads) {
    t.join();
  }

  log.DumpRaw();

  std::string raw = ReadRawLog(path);
  auto header = DecodeHeader(raw);
  ASSERT_EQ(header.total_entries,
            static_cast<uint64_t>(kNumThreads * kRecordsPerThread));
  ASSERT_EQ(header.dumped_entries,
            static_cast<uint32_t>(kNumThreads * kRecordsPerThread));
}

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

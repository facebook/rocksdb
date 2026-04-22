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

class FaultInjectionTestFSTest : public testing::Test {};

namespace {

std::shared_ptr<FaultInjectionTestFS> NewFaultFsExcludingInfoLogs(
    Env* env, FaultInjectionIOType type) {
  auto fault_fs = std::make_shared<FaultInjectionTestFS>(env->GetFileSystem());
  fault_fs->SetFileTypesExcludedFromFaultInjection({FileType::kInfoLogFile});
  fault_fs->SetThreadLocalErrorContext(type, /*seed=*/0, /*one_in=*/1,
                                       /*retryable=*/false,
                                       /*has_data_loss=*/false);
  fault_fs->EnableThreadLocalErrorInjection(type);
  return fault_fs;
}

}  // namespace

// Goal: verify a single structured record is persisted to the binary log with
// the expected fixed-width fields. The test writes one entry, finalizes the
// file, and decodes the raw bytes back into the header and entry structs.
TEST_F(InjectedErrorLogTest, BasicRecordAndFinalize) {
  std::string path = test::PerThreadDBPath("injected_error_log_basic.bin");
  InjectedErrorLog log;
  log.SetLogFilePath(path);
  log.Record("Append", "/tmp/000001.log",
             fault_injection_detail::OffsetSizeAndHead(7, Slice("abcd", 4)),
             "injected write error", false, true);
  log.Finalize();

  std::string raw = ReadRawLog(path);
  auto header = DecodeHeader(raw);
  ASSERT_EQ(std::string(header.magic, sizeof(header.magic)),
            std::string(InjectedErrorLog::kFileMagic.data(),
                        InjectedErrorLog::kFileMagic.size()));
  ASSERT_EQ(header.version, InjectedErrorLog::kFileVersion);
  ASSERT_EQ(header.entry_size, sizeof(InjectedErrorLog::Entry));
  ASSERT_EQ(header.total_entries, 1U);
  ASSERT_EQ(header.dumped_entries, 1U);

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

// Goal: verify the file-backed logger keeps all records instead of truncating
// to the old in-memory ring size. The test writes more than 1,000 entries and
// checks that both the first and last ones are still present in the file.
TEST_F(InjectedErrorLogTest, DirectLogKeepsAllEntries) {
  std::string path =
      test::PerThreadDBPath("injected_error_log_all_entries.bin");
  InjectedErrorLog log;
  log.SetLogFilePath(path);

  constexpr size_t kNumEntries = 1100;
  for (size_t i = 0; i < kNumEntries; ++i) {
    std::string file_name = "file" + std::to_string(i);
    log.Record("Append", file_name, fault_injection_detail::NoDetail(),
               "injected write error", false, false);
  }
  log.Finalize();

  std::string raw = ReadRawLog(path);
  auto header = DecodeHeader(raw);
  ASSERT_EQ(header.total_entries, static_cast<uint64_t>(kNumEntries));
  ASSERT_EQ(header.dumped_entries, static_cast<uint32_t>(kNumEntries));

  auto first = DecodeEntry(raw, 0);
  auto last = DecodeEntry(raw, kNumEntries - 1);
  EXPECT_EQ(DecodeCString(first.file_name, sizeof(first.file_name)), "file0");
  EXPECT_EQ(DecodeCString(last.file_name, sizeof(last.file_name)),
            "file" + std::to_string(kNumEntries - 1));
}

// Goal: verify concurrent Record() calls append independent entries to the
// shared file. The test has several threads emit records concurrently and then
// checks the finalized header count matches the total number of writes.
TEST_F(InjectedErrorLogTest, ConcurrentRecord) {
  std::string path = test::PerThreadDBPath("injected_error_log_concurrent.bin");
  InjectedErrorLog log;
  log.SetLogFilePath(path);
  constexpr int kNumThreads = 4;
  constexpr int kRecordsPerThread = 200;

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

  log.Finalize();

  std::string raw = ReadRawLog(path);
  auto header = DecodeHeader(raw);
  ASSERT_EQ(header.total_entries,
            static_cast<uint64_t>(kNumThreads * kRecordsPerThread));
  ASSERT_EQ(header.dumped_entries,
            static_cast<uint32_t>(kNumThreads * kRecordsPerThread));
}

// Goal: verify the human-readable hex helper still formats the payload samples
// exactly as expected, since the Python decoder depends on the same output.
TEST_F(InjectedErrorLogTest, HexHead) {
  const char data[] = "\x01\x02\xAB\xCD";
  std::string result = InjectedErrorLog::HexHead(data, 4);
  ASSERT_EQ(result, "01 02 ab cd");

  result = InjectedErrorLog::HexHead(data, 4, 2);
  ASSERT_EQ(result, "01 02 ...");
}

TEST_F(FaultInjectionTestFSTest, FaultInjectionExcludesInfoLogFiles) {
  Env* env = Env::Default();
  const std::string dbname =
      test::PerThreadDBPath("fault_injection_fs_test_metadata_read");
  const std::string log_dir = dbname + "_logs";
  ASSERT_OK(env->CreateDirIfMissing(dbname));
  ASSERT_OK(env->CreateDirIfMissing(log_dir));

  const std::string current_info_log = InfoLogFileName(dbname, dbname, log_dir);
  const std::string old_info_log =
      OldInfoLogFileName(dbname, 123, dbname, log_dir);
  const std::string manifest = DescriptorFileName(dbname, 1);
  const std::string manifest_for_write = DescriptorFileName(dbname, 2);
  const std::string manifest_for_delete = DescriptorFileName(dbname, 3);
  ASSERT_OK(
      WriteStringToFile(env, "old log", old_info_log, false /* should_sync */));
  ASSERT_OK(
      WriteStringToFile(env, "manifest", manifest, false /* should_sync */));
  ASSERT_OK(WriteStringToFile(env, "manifest delete", manifest_for_delete,
                              false /* should_sync */));

  {
    auto fault_fs =
        NewFaultFsExcludingInfoLogs(env, FaultInjectionIOType::kMetadataRead);

    ASSERT_OK(fault_fs->FileExists(old_info_log, IOOptions(), nullptr));
    ASSERT_EQ(0, fault_fs->GetAndResetInjectedThreadLocalErrorCount(
                     FaultInjectionIOType::kMetadataRead));

    IOStatus s = fault_fs->FileExists(manifest, IOOptions(), nullptr);
    ASSERT_NOK(s);
    ASSERT_TRUE(s.IsIOError()) << s.ToString();
    ASSERT_EQ(1, fault_fs->GetAndResetInjectedThreadLocalErrorCount(
                     FaultInjectionIOType::kMetadataRead));
  }

  {
    auto fault_fs =
        NewFaultFsExcludingInfoLogs(env, FaultInjectionIOType::kRead);
    std::unique_ptr<FSSequentialFile> seq_file;
    ASSERT_OK(fault_fs->NewSequentialFile(old_info_log, FileOptions(),
                                          &seq_file, nullptr /* dbg */));
    char scratch[16];
    Slice result;
    ASSERT_OK(seq_file->Read(sizeof(scratch), IOOptions(), &result, scratch,
                             nullptr /* dbg */));
    ASSERT_EQ("old log", result.ToString());
    ASSERT_EQ(0, fault_fs->GetAndResetInjectedThreadLocalErrorCount(
                     FaultInjectionIOType::kRead));

    std::unique_ptr<FSSequentialFile> manifest_seq_file;
    IOStatus s = fault_fs->NewSequentialFile(
        manifest, FileOptions(), &manifest_seq_file, nullptr /* dbg */);
    ASSERT_NOK(s);
    ASSERT_TRUE(s.IsIOError()) << s.ToString();
    ASSERT_EQ(1, fault_fs->GetAndResetInjectedThreadLocalErrorCount(
                     FaultInjectionIOType::kRead));
  }

  {
    auto fault_fs =
        NewFaultFsExcludingInfoLogs(env, FaultInjectionIOType::kWrite);
    std::unique_ptr<FSWritableFile> info_log_writer;
    ASSERT_OK(fault_fs->NewWritableFile(current_info_log, FileOptions(),
                                        &info_log_writer, nullptr /* dbg */));
    ASSERT_OK(
        info_log_writer->Append("current log", IOOptions(), nullptr /* dbg */));
    ASSERT_EQ(0, fault_fs->GetAndResetInjectedThreadLocalErrorCount(
                     FaultInjectionIOType::kWrite));

    std::unique_ptr<FSWritableFile> manifest_writer;
    IOStatus s = fault_fs->NewWritableFile(manifest_for_write, FileOptions(),
                                           &manifest_writer, nullptr /* dbg */);
    ASSERT_NOK(s);
    ASSERT_TRUE(s.IsIOError()) << s.ToString();
    ASSERT_EQ(1, fault_fs->GetAndResetInjectedThreadLocalErrorCount(
                     FaultInjectionIOType::kWrite));
  }

  {
    auto fault_fs =
        NewFaultFsExcludingInfoLogs(env, FaultInjectionIOType::kMetadataWrite);
    ASSERT_OK(fault_fs->DeleteFile(old_info_log, IOOptions(), nullptr));
    ASSERT_EQ(0, fault_fs->GetAndResetInjectedThreadLocalErrorCount(
                     FaultInjectionIOType::kMetadataWrite));

    IOStatus s =
        fault_fs->DeleteFile(manifest_for_delete, IOOptions(), nullptr);
    ASSERT_NOK(s);
    ASSERT_TRUE(s.IsIOError()) << s.ToString();
    ASSERT_EQ(1, fault_fs->GetAndResetInjectedThreadLocalErrorCount(
                     FaultInjectionIOType::kMetadataWrite));
  }
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

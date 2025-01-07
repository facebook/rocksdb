//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/wal_manager.h"

#include <map>
#include <string>

#include "db/column_family.h"
#include "db/db_impl/db_impl.h"
#include "db/log_writer.h"
#include "db/version_set.h"
#include "env/mock_env.h"
#include "file/writable_file_writer.h"
#include "rocksdb/cache.h"
#include "rocksdb/file_system.h"
#include "rocksdb/write_batch.h"
#include "rocksdb/write_buffer_manager.h"
#include "table/mock_table.h"
#include "test_util/testharness.h"
#include "test_util/testutil.h"
#include "util/string_util.h"

namespace ROCKSDB_NAMESPACE {

// TODO(icanadi) mock out VersionSet
// TODO(icanadi) move other WalManager-specific tests from db_test here
class WalManagerTest : public testing::Test {
 public:
  WalManagerTest()
      : dbname_(test::PerThreadDBPath("wal_manager_test")),
        db_options_(),
        table_cache_(NewLRUCache(50000, 16)),
        write_buffer_manager_(db_options_.db_write_buffer_size),
        current_log_number_(0) {
    env_.reset(MockEnv::Create(Env::Default()));
    EXPECT_OK(DestroyDB(dbname_, Options()));
  }

  void Init() {
    ASSERT_OK(env_->CreateDirIfMissing(dbname_));
    ASSERT_OK(env_->CreateDirIfMissing(ArchivalDirectory(dbname_)));
    db_options_.db_paths.emplace_back(dbname_,
                                      std::numeric_limits<uint64_t>::max());
    db_options_.wal_dir = dbname_;
    db_options_.env = env_.get();
    db_options_.fs = env_->GetFileSystem();
    db_options_.clock = env_->GetSystemClock().get();

    versions_.reset(new VersionSet(
        dbname_, &db_options_, env_options_, table_cache_.get(),
        &write_buffer_manager_, &write_controller_,
        /*block_cache_tracer=*/nullptr, /*io_tracer=*/nullptr,
        /*db_id=*/"", /*db_session_id=*/"", /*daily_offpeak_time_utc=*/"",
        /*error_handler=*/nullptr, /*read_only=*/false));

    wal_manager_.reset(
        new WalManager(db_options_, env_options_, nullptr /*IOTracer*/));
  }

  void Reopen() {
    wal_manager_.reset(
        new WalManager(db_options_, env_options_, nullptr /*IOTracer*/));
  }

  // NOT thread safe
  void Put(const std::string& key, const std::string& value) {
    assert(current_log_writer_.get() != nullptr);
    uint64_t seq = versions_->LastSequence() + 1;
    WriteBatch batch;
    ASSERT_OK(batch.Put(key, value));
    WriteBatchInternal::SetSequence(&batch, seq);
    ASSERT_OK(current_log_writer_->AddRecord(
        WriteOptions(), WriteBatchInternal::Contents(&batch)));
    versions_->SetLastAllocatedSequence(seq);
    versions_->SetLastPublishedSequence(seq);
    versions_->SetLastSequence(seq);
  }

  // NOT thread safe
  void RollTheLog(bool /*archived*/) {
    current_log_number_++;
    std::string fname = ArchivedLogFileName(dbname_, current_log_number_);
    const auto& fs = env_->GetFileSystem();
    std::unique_ptr<WritableFileWriter> file_writer;
    ASSERT_OK(WritableFileWriter::Create(fs, fname, env_options_, &file_writer,
                                         nullptr));
    current_log_writer_.reset(
        new log::Writer(std::move(file_writer), 0, false));
  }

  void CreateArchiveLogs(int num_logs, int entries_per_log) {
    for (int i = 1; i <= num_logs; ++i) {
      RollTheLog(true);
      for (int k = 0; k < entries_per_log; ++k) {
        Put(std::to_string(k), std::string(1024, 'a'));
      }
    }
  }

  std::unique_ptr<TransactionLogIterator> OpenTransactionLogIter(
      const SequenceNumber seq) {
    std::unique_ptr<TransactionLogIterator> iter;
    Status status = wal_manager_->GetUpdatesSince(
        seq, &iter, TransactionLogIterator::ReadOptions(), versions_.get());
    EXPECT_OK(status);
    return iter;
  }

  std::unique_ptr<MockEnv> env_;
  std::string dbname_;
  ImmutableDBOptions db_options_;
  WriteController write_controller_;
  EnvOptions env_options_;
  std::shared_ptr<Cache> table_cache_;
  WriteBufferManager write_buffer_manager_;
  std::unique_ptr<VersionSet> versions_;
  std::unique_ptr<WalManager> wal_manager_;

  std::unique_ptr<log::Writer> current_log_writer_;
  uint64_t current_log_number_;
};

TEST_F(WalManagerTest, ReadFirstRecordCache) {
  Init();
  std::string path = dbname_ + "/000001.log";
  std::unique_ptr<FSWritableFile> file;
  ASSERT_OK(env_->GetFileSystem()->NewWritableFile(path, FileOptions(), &file,
                                                   nullptr));

  SequenceNumber s;
  ASSERT_OK(wal_manager_->TEST_ReadFirstLine(path, 1 /* number */, &s));
  ASSERT_EQ(s, 0U);

  ASSERT_OK(
      wal_manager_->TEST_ReadFirstRecord(kAliveLogFile, 1 /* number */, &s));
  ASSERT_EQ(s, 0U);

  std::unique_ptr<WritableFileWriter> file_writer(
      new WritableFileWriter(std::move(file), path, FileOptions()));
  log::Writer writer(std::move(file_writer), 1,
                     db_options_.recycle_log_file_num > 0);
  WriteBatch batch;
  ASSERT_OK(batch.Put("foo", "bar"));
  WriteBatchInternal::SetSequence(&batch, 10);
  ASSERT_OK(
      writer.AddRecord(WriteOptions(), WriteBatchInternal::Contents(&batch)));

  // TODO(icanadi) move SpecialEnv outside of db_test, so we can reuse it here.
  // Waiting for lei to finish with db_test
  // env_->count_sequential_reads_ = true;
  // sequential_read_counter_ sanity test
  // ASSERT_EQ(env_->sequential_read_counter_.Read(), 0);

  ASSERT_OK(wal_manager_->TEST_ReadFirstRecord(kAliveLogFile, 1, &s));
  ASSERT_EQ(s, 10U);
  // did a read
  // TODO(icanadi) move SpecialEnv outside of db_test, so we can reuse it here
  // ASSERT_EQ(env_->sequential_read_counter_.Read(), 1);

  ASSERT_OK(wal_manager_->TEST_ReadFirstRecord(kAliveLogFile, 1, &s));
  ASSERT_EQ(s, 10U);
  // no new reads since the value is cached
  // TODO(icanadi) move SpecialEnv outside of db_test, so we can reuse it here
  // ASSERT_EQ(env_->sequential_read_counter_.Read(), 1);
}

namespace {
uint64_t GetLogDirSize(std::string dir_path, Env* env) {
  uint64_t dir_size = 0;
  std::vector<std::string> files;
  EXPECT_OK(env->GetChildren(dir_path, &files));
  for (auto& f : files) {
    uint64_t number;
    FileType type;
    if (ParseFileName(f, &number, &type) && type == kWalFile) {
      std::string const file_path = dir_path + "/" + f;
      uint64_t file_size;
      EXPECT_OK(env->GetFileSize(file_path, &file_size));
      dir_size += file_size;
    }
  }
  return dir_size;
}
std::vector<std::uint64_t> ListSpecificFiles(
    Env* env, const std::string& path, const FileType expected_file_type) {
  std::vector<std::string> files;
  std::vector<uint64_t> file_numbers;
  uint64_t number;
  FileType type;
  EXPECT_OK(env->GetChildren(path, &files));
  for (size_t i = 0; i < files.size(); ++i) {
    if (ParseFileName(files[i], &number, &type)) {
      if (type == expected_file_type) {
        file_numbers.push_back(number);
      }
    }
  }
  return file_numbers;
}

int CountRecords(TransactionLogIterator* iter) {
  int count = 0;
  SequenceNumber lastSequence = 0;
  BatchResult res;
  while (iter->Valid()) {
    res = iter->GetBatch();
    EXPECT_TRUE(res.sequence > lastSequence);
    ++count;
    lastSequence = res.sequence;
    EXPECT_OK(iter->status());
    iter->Next();
  }
  EXPECT_OK(iter->status());
  return count;
}
}  // anonymous namespace

TEST_F(WalManagerTest, WALArchivalSizeLimit) {
  db_options_.WAL_ttl_seconds = 0;
  db_options_.WAL_size_limit_MB = 1000;
  Init();

  // TEST : Create WalManager with huge size limit and no ttl.
  // Create some archived files and call PurgeObsoleteWALFiles().
  // Count the archived log files that survived.
  // Assert that all of them did.
  // Change size limit. Re-open WalManager.
  // Assert that archive is not greater than WAL_size_limit_MB after
  // PurgeObsoleteWALFiles()
  // Set ttl and time_to_check_ to small values. Re-open db.
  // Assert that there are no archived logs left.

  std::string archive_dir = ArchivalDirectory(dbname_);
  CreateArchiveLogs(20, 5000);

  std::vector<std::uint64_t> log_files =
      ListSpecificFiles(env_.get(), archive_dir, kWalFile);
  ASSERT_EQ(log_files.size(), 20U);

  db_options_.WAL_size_limit_MB = 8;
  Reopen();
  wal_manager_->PurgeObsoleteWALFiles();

  uint64_t archive_size = GetLogDirSize(archive_dir, env_.get());
  ASSERT_TRUE(archive_size <= db_options_.WAL_size_limit_MB * 1024 * 1024);

  db_options_.WAL_ttl_seconds = 1;
  env_->SleepForMicroseconds(2 * 1000 * 1000);
  Reopen();
  wal_manager_->PurgeObsoleteWALFiles();

  log_files = ListSpecificFiles(env_.get(), archive_dir, kWalFile);
  ASSERT_TRUE(log_files.empty());
}

TEST_F(WalManagerTest, WALArchivalTtl) {
  db_options_.WAL_ttl_seconds = 1000;
  Init();

  // TEST : Create WalManager with a ttl and no size limit.
  // Create some archived log files and call PurgeObsoleteWALFiles().
  // Assert that files are not deleted
  // Reopen db with small ttl.
  // Assert that all archived logs was removed.

  std::string archive_dir = ArchivalDirectory(dbname_);
  CreateArchiveLogs(20, 5000);

  std::vector<uint64_t> log_files =
      ListSpecificFiles(env_.get(), archive_dir, kWalFile);
  ASSERT_GT(log_files.size(), 0U);

  db_options_.WAL_ttl_seconds = 1;
  env_->SleepForMicroseconds(3 * 1000 * 1000);
  Reopen();
  wal_manager_->PurgeObsoleteWALFiles();

  log_files = ListSpecificFiles(env_.get(), archive_dir, kWalFile);
  ASSERT_TRUE(log_files.empty());
}

TEST_F(WalManagerTest, TransactionLogIteratorMoveOverZeroFiles) {
  Init();
  RollTheLog(false);
  Put("key1", std::string(1024, 'a'));
  // Create a zero record WAL file.
  RollTheLog(false);
  RollTheLog(false);

  Put("key2", std::string(1024, 'a'));

  auto iter = OpenTransactionLogIter(0);
  ASSERT_EQ(2, CountRecords(iter.get()));
}

TEST_F(WalManagerTest, TransactionLogIteratorJustEmptyFile) {
  Init();
  RollTheLog(false);
  auto iter = OpenTransactionLogIter(0);
  // Check that an empty iterator is returned
  ASSERT_TRUE(!iter->Valid());
}

TEST_F(WalManagerTest, TransactionLogIteratorNewFileWhileScanning) {
  Init();
  CreateArchiveLogs(2, 100);
  auto iter = OpenTransactionLogIter(0);
  CreateArchiveLogs(1, 100);
  int i = 0;
  for (; iter->Valid(); iter->Next()) {
    i++;
  }
  ASSERT_EQ(i, 200);
  // A new log file was added after the iterator was created.
  // TryAgain indicates a new iterator is needed to fetch the new data
  ASSERT_TRUE(iter->status().IsTryAgain());

  iter = OpenTransactionLogIter(0);
  i = 0;
  for (; iter->Valid(); iter->Next()) {
    i++;
  }
  ASSERT_EQ(i, 300);
  ASSERT_TRUE(iter->status().ok());
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

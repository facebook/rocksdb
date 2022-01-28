//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#if !defined(ROCKSDB_LITE) && !defined(OS_WIN)

#include <algorithm>
#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <random>
#include <string>
#include <utility>

#include "db/db_impl/db_impl.h"
#include "db/db_test_util.h"
#include "env/env_chroot.h"
#include "file/filename.h"
#include "port/port.h"
#include "port/stack_trace.h"
#include "rocksdb/env.h"
#include "rocksdb/file_checksum.h"
#include "rocksdb/rate_limiter.h"
#include "rocksdb/statistics.h"
#include "rocksdb/transaction_log.h"
#include "rocksdb/types.h"
#include "rocksdb/utilities/backup_engine.h"
#include "rocksdb/utilities/options_util.h"
#include "rocksdb/utilities/stackable_db.h"
#include "test_util/sync_point.h"
#include "test_util/testharness.h"
#include "test_util/testutil.h"
#include "util/cast_util.h"
#include "util/mutexlock.h"
#include "util/random.h"
#include "util/rate_limiter.h"
#include "util/stderr_logger.h"
#include "util/string_util.h"
#include "utilities/backupable/backupable_db_impl.h"

namespace ROCKSDB_NAMESPACE {

namespace {
using ShareFilesNaming = BackupEngineOptions::ShareFilesNaming;
const auto kLegacyCrc32cAndFileSize =
    BackupEngineOptions::kLegacyCrc32cAndFileSize;
const auto kUseDbSessionId = BackupEngineOptions::kUseDbSessionId;
const auto kFlagIncludeFileSize = BackupEngineOptions::kFlagIncludeFileSize;
const auto kNamingDefault = kUseDbSessionId | kFlagIncludeFileSize;

class DummyDB : public StackableDB {
 public:
  /* implicit */
  DummyDB(const Options& options, const std::string& dbname)
     : StackableDB(nullptr), options_(options), dbname_(dbname),
       deletions_enabled_(true), sequence_number_(0) {}

  SequenceNumber GetLatestSequenceNumber() const override {
    return ++sequence_number_;
  }

  const std::string& GetName() const override { return dbname_; }

  Env* GetEnv() const override { return options_.env; }

  using DB::GetOptions;
  Options GetOptions(ColumnFamilyHandle* /*column_family*/) const override {
    return options_;
  }

  DBOptions GetDBOptions() const override { return DBOptions(options_); }

  Status EnableFileDeletions(bool /*force*/) override {
    EXPECT_TRUE(!deletions_enabled_);
    deletions_enabled_ = true;
    return Status::OK();
  }

  Status DisableFileDeletions() override {
    EXPECT_TRUE(deletions_enabled_);
    deletions_enabled_ = false;
    return Status::OK();
  }

  ColumnFamilyHandle* DefaultColumnFamily() const override { return nullptr; }

  class DummyLogFile : public LogFile {
   public:
    /* implicit */
     DummyLogFile(const std::string& path, bool alive = true)
         : path_(path), alive_(alive) {}

     std::string PathName() const override { return path_; }

     uint64_t LogNumber() const override {
       // what business do you have calling this method?
       ADD_FAILURE();
       return 0;
     }

     WalFileType Type() const override {
       return alive_ ? kAliveLogFile : kArchivedLogFile;
     }

     SequenceNumber StartSequence() const override {
       // this seqnum guarantees the dummy file will be included in the backup
       // as long as it is alive.
       return kMaxSequenceNumber;
     }

     uint64_t SizeFileBytes() const override { return 0; }

    private:
     std::string path_;
     bool alive_;
  }; // DummyLogFile

  Status GetLiveFilesStorageInfo(
      const LiveFilesStorageInfoOptions& opts,
      std::vector<LiveFileStorageInfo>* files) override {
    uint64_t number;
    FileType type;
    files->clear();
    for (auto& f : live_files_) {
      bool success = ParseFileName(f, &number, &type);
      if (!success) {
        return Status::InvalidArgument("Bad file name: " + f);
      }
      files->emplace_back();
      LiveFileStorageInfo& info = files->back();
      info.relative_filename = f;
      info.directory = dbname_;
      info.file_number = number;
      info.file_type = type;
      if (type == kDescriptorFile) {
        info.size = 100;  // See TestEnv::GetChildrenFileAttributes below
        info.trim_to_size = true;
      } else if (type == kCurrentFile) {
        info.size = 0;
        info.trim_to_size = true;
      } else {
        info.size = 200;  // See TestEnv::GetChildrenFileAttributes below
      }
      if (opts.include_checksum_info) {
        info.file_checksum = kUnknownFileChecksum;
        info.file_checksum_func_name = kUnknownFileChecksumFuncName;
      }
    }
    return Status::OK();
  }

  // To avoid FlushWAL called on stacked db which is nullptr
  Status FlushWAL(bool /*sync*/) override { return Status::OK(); }

  std::vector<std::string> live_files_;

 private:
  Options options_;
  std::string dbname_;
  bool deletions_enabled_;
  mutable SequenceNumber sequence_number_;
}; // DummyDB

class TestEnv : public EnvWrapper {
 public:
  explicit TestEnv(Env* t) : EnvWrapper(t) {}
  const char* Name() const override { return "TestEnv"; }

  class DummySequentialFile : public SequentialFile {
   public:
    explicit DummySequentialFile(bool fail_reads)
        : SequentialFile(), rnd_(5), fail_reads_(fail_reads) {}
    Status Read(size_t n, Slice* result, char* scratch) override {
      if (fail_reads_) {
        return Status::IOError();
      }
      size_t read_size = (n > size_left) ? size_left : n;
      for (size_t i = 0; i < read_size; ++i) {
        scratch[i] = rnd_.Next() & 255;
      }
      *result = Slice(scratch, read_size);
      size_left -= read_size;
      return Status::OK();
    }

    Status Skip(uint64_t n) override {
      size_left = (n > size_left) ? size_left - n : 0;
      return Status::OK();
    }

   private:
    size_t size_left = 200;
    Random rnd_;
    bool fail_reads_;
  };

  Status NewSequentialFile(const std::string& f,
                           std::unique_ptr<SequentialFile>* r,
                           const EnvOptions& options) override {
    MutexLock l(&mutex_);
    if (dummy_sequential_file_) {
      r->reset(
          new TestEnv::DummySequentialFile(dummy_sequential_file_fail_reads_));
      return Status::OK();
    } else {
      Status s = EnvWrapper::NewSequentialFile(f, r, options);
      if (s.ok()) {
        if ((*r)->use_direct_io()) {
          ++num_direct_seq_readers_;
        }
        ++num_seq_readers_;
      }
      return s;
    }
  }

  Status NewWritableFile(const std::string& f, std::unique_ptr<WritableFile>* r,
                         const EnvOptions& options) override {
    MutexLock l(&mutex_);
    written_files_.push_back(f);
    if (limit_written_files_ <= 0) {
      return Status::NotSupported("Sorry, can't do this");
    }
    limit_written_files_--;
    Status s = EnvWrapper::NewWritableFile(f, r, options);
    if (s.ok()) {
      if ((*r)->use_direct_io()) {
        ++num_direct_writers_;
      }
      ++num_writers_;
    }
    return s;
  }

  Status NewRandomAccessFile(const std::string& fname,
                             std::unique_ptr<RandomAccessFile>* result,
                             const EnvOptions& options) override {
    MutexLock l(&mutex_);
    Status s = EnvWrapper::NewRandomAccessFile(fname, result, options);
    if (s.ok()) {
      if ((*result)->use_direct_io()) {
        ++num_direct_rand_readers_;
      }
      ++num_rand_readers_;
    }
    return s;
  }

  Status DeleteFile(const std::string& fname) override {
    MutexLock l(&mutex_);
    if (fail_delete_files_) {
      return Status::IOError();
    }
    EXPECT_GT(limit_delete_files_, 0U);
    limit_delete_files_--;
    return EnvWrapper::DeleteFile(fname);
  }

  Status DeleteDir(const std::string& dirname) override {
    MutexLock l(&mutex_);
    if (fail_delete_files_) {
      return Status::IOError();
    }
    return EnvWrapper::DeleteDir(dirname);
  }

  void AssertWrittenFiles(std::vector<std::string>& should_have_written) {
    MutexLock l(&mutex_);
    std::sort(should_have_written.begin(), should_have_written.end());
    std::sort(written_files_.begin(), written_files_.end());

    ASSERT_EQ(should_have_written, written_files_);
  }

  void ClearWrittenFiles() {
    MutexLock l(&mutex_);
    written_files_.clear();
  }

  void SetLimitWrittenFiles(uint64_t limit) {
    MutexLock l(&mutex_);
    limit_written_files_ = limit;
  }

  void SetLimitDeleteFiles(uint64_t limit) {
    MutexLock l(&mutex_);
    limit_delete_files_ = limit;
  }

  void SetDeleteFileFailure(bool fail) {
    MutexLock l(&mutex_);
    fail_delete_files_ = fail;
  }

  void SetDummySequentialFile(bool dummy_sequential_file) {
    MutexLock l(&mutex_);
    dummy_sequential_file_ = dummy_sequential_file;
  }
  void SetDummySequentialFileFailReads(bool dummy_sequential_file_fail_reads) {
    MutexLock l(&mutex_);
    dummy_sequential_file_fail_reads_ = dummy_sequential_file_fail_reads;
  }

  void SetGetChildrenFailure(bool fail) { get_children_failure_ = fail; }
  Status GetChildren(const std::string& dir,
                     std::vector<std::string>* r) override {
    if (get_children_failure_) {
      return Status::IOError("SimulatedFailure");
    }
    return EnvWrapper::GetChildren(dir, r);
  }

  // Some test cases do not actually create the test files (e.g., see
  // DummyDB::live_files_) - for those cases, we mock those files' attributes
  // so CreateNewBackup() can get their attributes.
  void SetFilenamesForMockedAttrs(const std::vector<std::string>& filenames) {
    filenames_for_mocked_attrs_ = filenames;
  }
  Status GetChildrenFileAttributes(
      const std::string& dir, std::vector<Env::FileAttributes>* r) override {
    if (filenames_for_mocked_attrs_.size() > 0) {
      for (const auto& filename : filenames_for_mocked_attrs_) {
        uint64_t size_bytes = 200;  // Match TestEnv
        if (filename.find("MANIFEST") == 0) {
          size_bytes = 100;  // Match DummyDB::GetLiveFiles
        }
        r->push_back({dir + "/" + filename, size_bytes});
      }
      return Status::OK();
    }
    return EnvWrapper::GetChildrenFileAttributes(dir, r);
  }
  Status GetFileSize(const std::string& path, uint64_t* size_bytes) override {
    if (filenames_for_mocked_attrs_.size() > 0) {
      auto fname = path.substr(path.find_last_of('/') + 1);
      auto filename_iter = std::find(filenames_for_mocked_attrs_.begin(),
                                     filenames_for_mocked_attrs_.end(), fname);
      if (filename_iter != filenames_for_mocked_attrs_.end()) {
        *size_bytes = 200;  // Match TestEnv
        if (fname.find("MANIFEST") == 0) {
          *size_bytes = 100;  // Match DummyDB::GetLiveFiles
        }
        return Status::OK();
      }
      return Status::NotFound(fname);
    }
    return EnvWrapper::GetFileSize(path, size_bytes);
  }

  void SetCreateDirIfMissingFailure(bool fail) {
    create_dir_if_missing_failure_ = fail;
  }
  Status CreateDirIfMissing(const std::string& d) override {
    if (create_dir_if_missing_failure_) {
      return Status::IOError("SimulatedFailure");
    }
    return EnvWrapper::CreateDirIfMissing(d);
  }

  void SetNewDirectoryFailure(bool fail) { new_directory_failure_ = fail; }
  Status NewDirectory(const std::string& name,
                      std::unique_ptr<Directory>* result) override {
    if (new_directory_failure_) {
      return Status::IOError("SimulatedFailure");
    }
    return EnvWrapper::NewDirectory(name, result);
  }

  void ClearFileOpenCounters() {
    MutexLock l(&mutex_);
    num_rand_readers_ = 0;
    num_direct_rand_readers_ = 0;
    num_seq_readers_ = 0;
    num_direct_seq_readers_ = 0;
    num_writers_ = 0;
    num_direct_writers_ = 0;
  }

  int num_rand_readers() { return num_rand_readers_; }
  int num_direct_rand_readers() { return num_direct_rand_readers_; }
  int num_seq_readers() { return num_seq_readers_; }
  int num_direct_seq_readers() { return num_direct_seq_readers_; }
  int num_writers() { return num_writers_; }
  int num_direct_writers() { return num_direct_writers_; }

 private:
  port::Mutex mutex_;
  bool dummy_sequential_file_ = false;
  bool dummy_sequential_file_fail_reads_ = false;
  std::vector<std::string> written_files_;
  std::vector<std::string> filenames_for_mocked_attrs_;
  uint64_t limit_written_files_ = 1000000;
  uint64_t limit_delete_files_ = 1000000;
  bool fail_delete_files_ = false;

  bool get_children_failure_ = false;
  bool create_dir_if_missing_failure_ = false;
  bool new_directory_failure_ = false;

  // Keeps track of how many files of each type were successfully opened, and
  // out of those, how many were opened with direct I/O.
  std::atomic<int> num_rand_readers_;
  std::atomic<int> num_direct_rand_readers_;
  std::atomic<int> num_seq_readers_;
  std::atomic<int> num_direct_seq_readers_;
  std::atomic<int> num_writers_;
  std::atomic<int> num_direct_writers_;
};  // TestEnv

class FileManager : public EnvWrapper {
 public:
  explicit FileManager(Env* t) : EnvWrapper(t), rnd_(5) {}
  const char* Name() const override { return "FileManager"; }

  Status GetRandomFileInDir(const std::string& dir, std::string* fname,
                            uint64_t* fsize) {
    std::vector<FileAttributes> children;
    auto s = GetChildrenFileAttributes(dir, &children);
    if (!s.ok()) {
      return s;
    } else if (children.size() <= 2) {  // . and ..
      return Status::NotFound("Empty directory: " + dir);
    }
    assert(fname != nullptr);
    while (true) {
      int i = rnd_.Next() % children.size();
      fname->assign(dir + "/" + children[i].name);
      *fsize = children[i].size_bytes;
      return Status::OK();
    }
    // should never get here
    assert(false);
    return Status::NotFound("");
  }

  Status DeleteRandomFileInDir(const std::string& dir) {
    std::vector<std::string> children;
    Status s = GetChildren(dir, &children);
    if (!s.ok()) {
      return s;
    }
    while (true) {
      int i = rnd_.Next() % children.size();
      return DeleteFile(dir + "/" + children[i]);
    }
    // should never get here
    assert(false);
    return Status::NotFound("");
  }

  Status AppendToRandomFileInDir(const std::string& dir,
                                 const std::string& data) {
    std::vector<std::string> children;
    Status s = GetChildren(dir, &children);
    if (!s.ok()) {
      return s;
    }
    while (true) {
      int i = rnd_.Next() % children.size();
      return WriteToFile(dir + "/" + children[i], data);
    }
    // should never get here
    assert(false);
    return Status::NotFound("");
  }

  Status CorruptFile(const std::string& fname, uint64_t bytes_to_corrupt) {
    std::string file_contents;
    Status s = ReadFileToString(this, fname, &file_contents);
    if (!s.ok()) {
      return s;
    }
    s = DeleteFile(fname);
    if (!s.ok()) {
      return s;
    }

    for (uint64_t i = 0; i < bytes_to_corrupt; ++i) {
      std::string tmp = rnd_.RandomString(1);
      file_contents[rnd_.Next() % file_contents.size()] = tmp[0];
    }
    return WriteToFile(fname, file_contents);
  }

  Status CorruptFileStart(const std::string& fname) {
    std::string to_xor = "blah";
    std::string file_contents;
    Status s = ReadFileToString(this, fname, &file_contents);
    if (!s.ok()) {
      return s;
    }
    s = DeleteFile(fname);
    if (!s.ok()) {
      return s;
    }
    for (size_t i = 0; i < to_xor.size(); ++i) {
      file_contents[i] ^= to_xor[i];
    }
    return WriteToFile(fname, file_contents);
  }

  Status CorruptChecksum(const std::string& fname, bool appear_valid) {
    std::string metadata;
    Status s = ReadFileToString(this, fname, &metadata);
    if (!s.ok()) {
      return s;
    }
    s = DeleteFile(fname);
    if (!s.ok()) {
      return s;
    }

    auto pos = metadata.find("private");
    if (pos == std::string::npos) {
      return Status::Corruption("private file is expected");
    }
    pos = metadata.find(" crc32 ", pos + 6);
    if (pos == std::string::npos) {
      return Status::Corruption("checksum not found");
    }

    if (metadata.size() < pos + 7) {
      return Status::Corruption("bad CRC32 checksum value");
    }

    if (appear_valid) {
      if (metadata[pos + 8] == '\n') {
        // single digit value, safe to insert one more digit
        metadata.insert(pos + 8, 1, '0');
      } else {
        metadata.erase(pos + 8, 1);
      }
    } else {
      metadata[pos + 7] = 'a';
    }

    return WriteToFile(fname, metadata);
  }

  Status WriteToFile(const std::string& fname, const std::string& data) {
    std::unique_ptr<WritableFile> file;
    EnvOptions env_options;
    env_options.use_mmap_writes = false;
    Status s = EnvWrapper::NewWritableFile(fname, &file, env_options);
    if (!s.ok()) {
      return s;
    }
    return file->Append(Slice(data));
  }

 private:
  Random rnd_;
}; // FileManager

// utility functions
namespace {

enum FillDBFlushAction {
  kFlushMost,
  kFlushAll,
  kAutoFlushOnly,
};

// Many tests in this file expect FillDB to write at least one sst file,
// so the default behavior (if not kAutoFlushOnly) of FillDB is to force
// a flush. But to ensure coverage of the WAL file case, we also (by default)
// do one Put after the Flush (kFlushMost).
size_t FillDB(DB* db, int from, int to,
              FillDBFlushAction flush_action = kFlushMost) {
  size_t bytes_written = 0;
  for (int i = from; i < to; ++i) {
    std::string key = "testkey" + ToString(i);
    std::string value = "testvalue" + ToString(i);
    bytes_written += key.size() + value.size();

    EXPECT_OK(db->Put(WriteOptions(), Slice(key), Slice(value)));

    if (flush_action == kFlushMost && i == to - 2) {
      EXPECT_OK(db->Flush(FlushOptions()));
    }
  }
  if (flush_action == kFlushAll) {
    EXPECT_OK(db->Flush(FlushOptions()));
  }
  return bytes_written;
}

void AssertExists(DB* db, int from, int to) {
  for (int i = from; i < to; ++i) {
    std::string key = "testkey" + ToString(i);
    std::string value;
    Status s = db->Get(ReadOptions(), Slice(key), &value);
    ASSERT_EQ(value, "testvalue" + ToString(i));
  }
}

void AssertEmpty(DB* db, int from, int to) {
  for (int i = from; i < to; ++i) {
    std::string key = "testkey" + ToString(i);
    std::string value = "testvalue" + ToString(i);

    Status s = db->Get(ReadOptions(), Slice(key), &value);
    ASSERT_TRUE(s.IsNotFound());
  }
}
}  // namespace

class BackupEngineTest : public testing::Test {
 public:
  enum ShareOption {
    kNoShare,
    kShareNoChecksum,
    kShareWithChecksum,
  };

  const std::vector<ShareOption> kAllShareOptions = {
      kNoShare, kShareNoChecksum, kShareWithChecksum};

  BackupEngineTest() {
    // set up files
    std::string db_chroot = test::PerThreadDBPath("db_for_backup");
    std::string backup_chroot = test::PerThreadDBPath("db_backups");
    EXPECT_OK(Env::Default()->CreateDirIfMissing(db_chroot));
    EXPECT_OK(Env::Default()->CreateDirIfMissing(backup_chroot));
    dbname_ = "/tempdb";
    backupdir_ = "/tempbk";
    latest_backup_ = backupdir_ + "/LATEST_BACKUP";

    // set up envs
    db_chroot_env_.reset(NewChrootEnv(Env::Default(), db_chroot));
    backup_chroot_env_.reset(NewChrootEnv(Env::Default(), backup_chroot));
    test_db_env_.reset(new TestEnv(db_chroot_env_.get()));
    test_backup_env_.reset(new TestEnv(backup_chroot_env_.get()));
    file_manager_.reset(new FileManager(backup_chroot_env_.get()));
    db_file_manager_.reset(new FileManager(db_chroot_env_.get()));

    // set up db options
    options_.create_if_missing = true;
    options_.paranoid_checks = true;
    options_.write_buffer_size = 1 << 17; // 128KB
    options_.env = test_db_env_.get();
    options_.wal_dir = dbname_;
    options_.enable_blob_files = true;

    // Create logger
    DBOptions logger_options;
    logger_options.env = db_chroot_env_.get();
    // TODO: This should really be an EXPECT_OK, but this CreateLogger fails
    // regularly in some environments with "no such directory"
    CreateLoggerFromOptions(dbname_, logger_options, &logger_)
        .PermitUncheckedError();

    // The sync option is not easily testable in unit tests, but should be
    // smoke tested across all the other backup tests. However, it is
    // certainly not worth doubling the runtime of backup tests for it.
    // Thus, we can enable sync for one of our alternate testing
    // configurations.
    constexpr bool kUseSync =
#ifdef ROCKSDB_MODIFY_NPHASH
        true;
#else
        false;
#endif  // ROCKSDB_MODIFY_NPHASH

    // set up backup db options
    backupable_options_.reset(new BackupEngineOptions(
        backupdir_, test_backup_env_.get(), /*share_table_files*/ true,
        logger_.get(), kUseSync));

    // most tests will use multi-threaded backups
    backupable_options_->max_background_operations = 7;

    // delete old files in db
    DestroyDB(dbname_, options_);

    // delete old LATEST_BACKUP file, which some tests create for compatibility
    // testing.
    backup_chroot_env_->DeleteFile(latest_backup_).PermitUncheckedError();
  }

  DB* OpenDB() {
    DB* db;
    EXPECT_OK(DB::Open(options_, dbname_, &db));
    return db;
  }

  void CloseAndReopenDB(bool read_only = false) {
    // Close DB
    db_.reset();

    // Open DB
    test_db_env_->SetLimitWrittenFiles(1000000);
    DB* db;
    if (read_only) {
      ASSERT_OK(DB::OpenForReadOnly(options_, dbname_, &db));
    } else {
      ASSERT_OK(DB::Open(options_, dbname_, &db));
    }
    db_.reset(db);
  }

  void InitializeDBAndBackupEngine(bool dummy = false) {
    // reset all the db env defaults
    test_db_env_->SetLimitWrittenFiles(1000000);
    test_db_env_->SetDummySequentialFile(dummy);

    DB* db;
    if (dummy) {
      dummy_db_ = new DummyDB(options_, dbname_);
      db = dummy_db_;
    } else {
      ASSERT_OK(DB::Open(options_, dbname_, &db));
    }
    db_.reset(db);
  }

  virtual void OpenDBAndBackupEngine(
      bool destroy_old_data = false, bool dummy = false,
      ShareOption shared_option = kShareNoChecksum) {
    InitializeDBAndBackupEngine(dummy);
    // reset backup env defaults
    test_backup_env_->SetLimitWrittenFiles(1000000);
    backupable_options_->destroy_old_data = destroy_old_data;
    backupable_options_->share_table_files = shared_option != kNoShare;
    backupable_options_->share_files_with_checksum =
        shared_option == kShareWithChecksum;
    OpenBackupEngine(destroy_old_data);
  }

  void CloseDBAndBackupEngine() {
    db_.reset();
    backup_engine_.reset();
  }

  void OpenBackupEngine(bool destroy_old_data = false) {
    backupable_options_->destroy_old_data = destroy_old_data;
    BackupEngine* backup_engine;
    ASSERT_OK(BackupEngine::Open(test_db_env_.get(), *backupable_options_,
                                 &backup_engine));
    backup_engine_.reset(backup_engine);
  }

  void CloseBackupEngine() { backup_engine_.reset(nullptr); }

  // cross-cutting test of GetBackupInfo
  void AssertBackupInfoConsistency() {
    std::vector<BackupInfo> backup_info;
    backup_engine_->GetBackupInfo(&backup_info, /*with file details*/ true);
    std::map<std::string, uint64_t> file_sizes;

    // Find the files that are supposed to be there
    for (auto& backup : backup_info) {
      uint64_t sum_for_backup = 0;
      for (auto& file : backup.file_details) {
        auto e = file_sizes.find(file.relative_filename);
        if (e == file_sizes.end()) {
          // fprintf(stderr, "Adding %s -> %u\n",
          // file.relative_filename.c_str(), (unsigned)file.size);
          file_sizes[file.relative_filename] = file.size;
        } else {
          ASSERT_EQ(file_sizes[file.relative_filename], file.size);
        }
        sum_for_backup += file.size;
      }
      ASSERT_EQ(backup.size, sum_for_backup);
    }

    std::vector<BackupID> corrupt_backup_ids;
    backup_engine_->GetCorruptedBackups(&corrupt_backup_ids);
    bool has_corrupt = corrupt_backup_ids.size() > 0;

    // Compare with what's in backup dir
    std::vector<std::string> child_dirs;
    ASSERT_OK(
        test_backup_env_->GetChildren(backupdir_ + "/private", &child_dirs));
    for (auto& dir : child_dirs) {
      dir = "private/" + dir;
    }
    child_dirs.push_back("shared");           // might not exist
    child_dirs.push_back("shared_checksum");  // might not exist
    for (auto& dir : child_dirs) {
      std::vector<std::string> children;
      test_backup_env_->GetChildren(backupdir_ + "/" + dir, &children)
          .PermitUncheckedError();
      // fprintf(stderr, "ls %s\n", (backupdir_ + "/" + dir).c_str());
      for (auto& file : children) {
        uint64_t size;
        size = UINT64_MAX;  // appease clang-analyze
        std::string rel_file = dir + "/" + file;
        // fprintf(stderr, "stat %s\n", (backupdir_ + "/" + rel_file).c_str());
        ASSERT_OK(
            test_backup_env_->GetFileSize(backupdir_ + "/" + rel_file, &size));
        auto e = file_sizes.find(rel_file);
        if (e == file_sizes.end()) {
          // The only case in which we should find files not reported
          ASSERT_TRUE(has_corrupt);
        } else {
          ASSERT_EQ(e->second, size);
          file_sizes.erase(e);
        }
      }
    }

    // Everything should have been matched
    ASSERT_EQ(file_sizes.size(), 0);
  }

  // restores backup backup_id and asserts the existence of
  // [start_exist, end_exist> and not-existence of
  // [end_exist, end>
  //
  // if backup_id == 0, it means restore from latest
  // if end == 0, don't check AssertEmpty
  void AssertBackupConsistency(BackupID backup_id, uint32_t start_exist,
                               uint32_t end_exist, uint32_t end = 0,
                               bool keep_log_files = false) {
    RestoreOptions restore_options(keep_log_files);
    bool opened_backup_engine = false;
    if (backup_engine_.get() == nullptr) {
      opened_backup_engine = true;
      OpenBackupEngine();
    }
    AssertBackupInfoConsistency();

    // Now perform restore
    if (backup_id > 0) {
      ASSERT_OK(backup_engine_->RestoreDBFromBackup(backup_id, dbname_, dbname_,
                                                    restore_options));
    } else {
      ASSERT_OK(backup_engine_->RestoreDBFromLatestBackup(dbname_, dbname_,
                                                          restore_options));
    }
    DB* db = OpenDB();
    // Check DB contents
    AssertExists(db, start_exist, end_exist);
    if (end != 0) {
      AssertEmpty(db, end_exist, end);
    }
    delete db;
    if (opened_backup_engine) {
      CloseBackupEngine();
    }
  }

  void DeleteLogFiles() {
    std::vector<std::string> delete_logs;
    ASSERT_OK(db_chroot_env_->GetChildren(dbname_, &delete_logs));
    for (auto f : delete_logs) {
      uint64_t number;
      FileType type;
      bool ok = ParseFileName(f, &number, &type);
      if (ok && type == kWalFile) {
        ASSERT_OK(db_chroot_env_->DeleteFile(dbname_ + "/" + f));
      }
    }
  }

  Status GetDataFilesInDB(const FileType& file_type,
                          std::vector<FileAttributes>* files) {
    std::vector<std::string> live;
    uint64_t ignore_manifest_size;
    Status s = db_->GetLiveFiles(live, &ignore_manifest_size, /*flush*/ false);
    if (!s.ok()) {
      return s;
    }
    std::vector<FileAttributes> children;
    s = test_db_env_->GetChildrenFileAttributes(dbname_, &children);
    for (const auto& child : children) {
      FileType type;
      uint64_t number = 0;
      if (ParseFileName(child.name, &number, &type) && type == file_type &&
          std::find(live.begin(), live.end(), "/" + child.name) != live.end()) {
        files->push_back(child);
      }
    }
    return s;
  }

  Status GetRandomDataFileInDB(const FileType& file_type,
                               std::string* fname_out,
                               uint64_t* fsize_out = nullptr) {
    Random rnd(6);  // NB: hardly "random"
    std::vector<FileAttributes> files;
    Status s = GetDataFilesInDB(file_type, &files);
    if (!s.ok()) {
      return s;
    }
    if (files.empty()) {
      return Status::NotFound("");
    }
    size_t i = rnd.Uniform(static_cast<int>(files.size()));
    *fname_out = dbname_ + "/" + files[i].name;
    if (fsize_out) {
      *fsize_out = files[i].size_bytes;
    }
    return Status::OK();
  }

  Status CorruptRandomDataFileInDB(const FileType& file_type) {
    std::string fname;
    uint64_t fsize = 0;
    Status s = GetRandomDataFileInDB(file_type, &fname, &fsize);
    if (!s.ok()) {
      return s;
    }

    std::string file_contents;
    s = ReadFileToString(test_db_env_.get(), fname, &file_contents);
    if (!s.ok()) {
      return s;
    }
    s = test_db_env_->DeleteFile(fname);
    if (!s.ok()) {
      return s;
    }

    file_contents[0] = (file_contents[0] + 257) % 256;
    return WriteStringToFile(test_db_env_.get(), file_contents, fname);
  }

  void AssertDirectoryFilesMatchRegex(const std::string& dir,
                                      const TestRegex& pattern,
                                      const std::string& file_type,
                                      int minimum_count) {
    std::vector<FileAttributes> children;
    ASSERT_OK(file_manager_->GetChildrenFileAttributes(dir, &children));
    int found_count = 0;
    for (const auto& child : children) {
      if (EndsWith(child.name, file_type)) {
        ASSERT_MATCHES_REGEX(child.name, pattern);
        ++found_count;
      }
    }
    ASSERT_GE(found_count, minimum_count);
  }

  void AssertDirectoryFilesSizeIndicators(const std::string& dir,
                                          int minimum_count) {
    std::vector<FileAttributes> children;
    ASSERT_OK(file_manager_->GetChildrenFileAttributes(dir, &children));
    int found_count = 0;
    for (const auto& child : children) {
      auto last_underscore = child.name.find_last_of('_');
      auto last_dot = child.name.find_last_of('.');
      ASSERT_NE(child.name, child.name.substr(0, last_underscore));
      ASSERT_NE(child.name, child.name.substr(0, last_dot));
      ASSERT_LT(last_underscore, last_dot);
      std::string s = child.name.substr(last_underscore + 1,
                                        last_dot - (last_underscore + 1));
      ASSERT_EQ(s, ToString(child.size_bytes));
      ++found_count;
    }
    ASSERT_GE(found_count, minimum_count);
  }

  // files
  std::string dbname_;
  std::string backupdir_;
  std::string latest_backup_;

  // logger_ must be above backup_engine_ such that the engine's destructor,
  // which uses a raw pointer to the logger, executes first.
  std::shared_ptr<Logger> logger_;

  // envs
  std::unique_ptr<Env> db_chroot_env_;
  std::unique_ptr<Env> backup_chroot_env_;
  std::unique_ptr<TestEnv> test_db_env_;
  std::unique_ptr<TestEnv> test_backup_env_;
  std::unique_ptr<FileManager> file_manager_;
  std::unique_ptr<FileManager> db_file_manager_;

  // all the dbs!
  DummyDB* dummy_db_;  // owned as db_ when present
  std::unique_ptr<DB> db_;
  std::unique_ptr<BackupEngine> backup_engine_;

  // options
  Options options_;

 protected:
  std::unique_ptr<BackupEngineOptions> backupable_options_;
};  // BackupEngineTest

void AppendPath(const std::string& path, std::vector<std::string>& v) {
  for (auto& f : v) {
    f = path + f;
  }
}

class BackupEngineTestWithParam : public BackupEngineTest,
                                  public testing::WithParamInterface<bool> {
 public:
  BackupEngineTestWithParam() {
    backupable_options_->share_files_with_checksum = GetParam();
  }
  void OpenDBAndBackupEngine(
      bool destroy_old_data = false, bool dummy = false,
      ShareOption shared_option = kShareNoChecksum) override {
    BackupEngineTest::InitializeDBAndBackupEngine(dummy);
    // reset backup env defaults
    test_backup_env_->SetLimitWrittenFiles(1000000);
    backupable_options_->destroy_old_data = destroy_old_data;
    backupable_options_->share_table_files = shared_option != kNoShare;
    // NOTE: keep share_files_with_checksum setting from constructor
    OpenBackupEngine(destroy_old_data);
  }
};

TEST_F(BackupEngineTest, FileCollision) {
  const int keys_iteration = 100;
  for (const auto& sopt : kAllShareOptions) {
    OpenDBAndBackupEngine(true /* destroy_old_data */, false /* dummy */, sopt);
    FillDB(db_.get(), 0, keys_iteration);
    ASSERT_OK(backup_engine_->CreateNewBackup(db_.get()));
    FillDB(db_.get(), keys_iteration, keys_iteration * 2);
    ASSERT_OK(backup_engine_->CreateNewBackup(db_.get()));
    CloseDBAndBackupEngine();

    // If the db directory has been cleaned up, it is sensitive to file
    // collision.
    ASSERT_OK(DestroyDB(dbname_, options_));

    // open fresh DB, but old backups present
    OpenDBAndBackupEngine(false /* destroy_old_data */, false /* dummy */,
                          sopt);
    FillDB(db_.get(), 0, keys_iteration);
    ASSERT_OK(db_->Flush(FlushOptions()));  // like backup would do
    FillDB(db_.get(), keys_iteration, keys_iteration * 2);
    if (sopt != kShareNoChecksum) {
      ASSERT_OK(backup_engine_->CreateNewBackup(db_.get()));
    } else {
      // The new table files created in FillDB() will clash with the old
      // backup and sharing tables with no checksum will have the file
      // collision problem.
      ASSERT_NOK(backup_engine_->CreateNewBackup(db_.get()));
      ASSERT_OK(backup_engine_->PurgeOldBackups(0));
      ASSERT_OK(backup_engine_->CreateNewBackup(db_.get()));
    }
    CloseDBAndBackupEngine();

    // delete old data
    ASSERT_OK(DestroyDB(dbname_, options_));
  }
}

// This test verifies that the verifyBackup method correctly identifies
// invalid backups
TEST_P(BackupEngineTestWithParam, VerifyBackup) {
  const int keys_iteration = 5000;
  OpenDBAndBackupEngine(true);
  // create five backups
  for (int i = 0; i < 5; ++i) {
    FillDB(db_.get(), keys_iteration * i, keys_iteration * (i + 1));
    ASSERT_OK(backup_engine_->CreateNewBackup(db_.get(), true));
  }
  CloseDBAndBackupEngine();

  OpenDBAndBackupEngine();
  // ---------- case 1. - valid backup -----------
  ASSERT_TRUE(backup_engine_->VerifyBackup(1).ok());

  // ---------- case 2. - delete a file -----------i
  ASSERT_OK(file_manager_->DeleteRandomFileInDir(backupdir_ + "/private/1"));
  ASSERT_TRUE(backup_engine_->VerifyBackup(1).IsNotFound());

  // ---------- case 3. - corrupt a file -----------
  std::string append_data = "Corrupting a random file";
  ASSERT_OK(file_manager_->AppendToRandomFileInDir(backupdir_ + "/private/2",
                                                   append_data));
  ASSERT_TRUE(backup_engine_->VerifyBackup(2).IsCorruption());

  // ---------- case 4. - invalid backup -----------
  ASSERT_TRUE(backup_engine_->VerifyBackup(6).IsNotFound());
  CloseDBAndBackupEngine();
}

#if !defined(ROCKSDB_VALGRIND_RUN) || defined(ROCKSDB_FULL_VALGRIND_RUN)
// open DB, write, close DB, backup, restore, repeat
TEST_P(BackupEngineTestWithParam, OfflineIntegrationTest) {
  // has to be a big number, so that it triggers the memtable flush
  const int keys_iteration = 5000;
  const int max_key = keys_iteration * 4 + 10;
  // first iter -- flush before backup
  // second iter -- don't flush before backup
  for (int iter = 0; iter < 2; ++iter) {
    // delete old data
    DestroyDB(dbname_, options_);
    bool destroy_data = true;

    // every iteration --
    // 1. insert new data in the DB
    // 2. backup the DB
    // 3. destroy the db
    // 4. restore the db, check everything is still there
    for (int i = 0; i < 5; ++i) {
      // in last iteration, put smaller amount of data,
      int fill_up_to = std::min(keys_iteration * (i + 1), max_key);
      // ---- insert new data and back up ----
      OpenDBAndBackupEngine(destroy_data);
      destroy_data = false;
      // kAutoFlushOnly to preserve legacy test behavior (consider updating)
      FillDB(db_.get(), keys_iteration * i, fill_up_to, kAutoFlushOnly);
      ASSERT_OK(backup_engine_->CreateNewBackup(db_.get(), iter == 0));
      CloseDBAndBackupEngine();
      DestroyDB(dbname_, options_);

      // ---- make sure it's empty ----
      DB* db = OpenDB();
      AssertEmpty(db, 0, fill_up_to);
      delete db;

      // ---- restore the DB ----
      OpenBackupEngine();
      if (i >= 3) {  // test purge old backups
        // when i == 4, purge to only 1 backup
        // when i == 3, purge to 2 backups
        ASSERT_OK(backup_engine_->PurgeOldBackups(5 - i));
      }
      // ---- make sure the data is there ---
      AssertBackupConsistency(0, 0, fill_up_to, max_key);
      CloseBackupEngine();
    }
  }
}

// open DB, write, backup, write, backup, close, restore
TEST_P(BackupEngineTestWithParam, OnlineIntegrationTest) {
  // has to be a big number, so that it triggers the memtable flush
  const int keys_iteration = 5000;
  const int max_key = keys_iteration * 4 + 10;
  Random rnd(7);
  // delete old data
  DestroyDB(dbname_, options_);

  // TODO: Implement & test db_paths support in backup (not supported in
  // restore)
  // options_.db_paths.emplace_back(dbname_, 500 * 1024);
  // options_.db_paths.emplace_back(dbname_ + "_2", 1024 * 1024 * 1024);

  OpenDBAndBackupEngine(true);
  // write some data, backup, repeat
  for (int i = 0; i < 5; ++i) {
    if (i == 4) {
      // delete backup number 2, online delete!
      ASSERT_OK(backup_engine_->DeleteBackup(2));
    }
    // in last iteration, put smaller amount of data,
    // so that backups can share sst files
    int fill_up_to = std::min(keys_iteration * (i + 1), max_key);
    // kAutoFlushOnly to preserve legacy test behavior (consider updating)
    FillDB(db_.get(), keys_iteration * i, fill_up_to, kAutoFlushOnly);
    // we should get consistent results with flush_before_backup
    // set to both true and false
    ASSERT_OK(backup_engine_->CreateNewBackup(db_.get(), !!(rnd.Next() % 2)));
  }
  // close and destroy
  CloseDBAndBackupEngine();
  DestroyDB(dbname_, options_);

  // ---- make sure it's empty ----
  DB* db = OpenDB();
  AssertEmpty(db, 0, max_key);
  delete db;

  // ---- restore every backup and verify all the data is there ----
  OpenBackupEngine();
  for (int i = 1; i <= 5; ++i) {
    if (i == 2) {
      // we deleted backup 2
      Status s = backup_engine_->RestoreDBFromBackup(2, dbname_, dbname_);
      ASSERT_TRUE(!s.ok());
    } else {
      int fill_up_to = std::min(keys_iteration * i, max_key);
      AssertBackupConsistency(i, 0, fill_up_to, max_key);
    }
  }

  // delete some backups -- this should leave only backups 3 and 5 alive
  ASSERT_OK(backup_engine_->DeleteBackup(4));
  ASSERT_OK(backup_engine_->PurgeOldBackups(2));

  std::vector<BackupInfo> backup_info;
  backup_engine_->GetBackupInfo(&backup_info);
  ASSERT_EQ(2UL, backup_info.size());

  // check backup 3
  AssertBackupConsistency(3, 0, 3 * keys_iteration, max_key);
  // check backup 5
  AssertBackupConsistency(5, 0, max_key);

  CloseBackupEngine();
}
#endif  // !defined(ROCKSDB_VALGRIND_RUN) || defined(ROCKSDB_FULL_VALGRIND_RUN)

INSTANTIATE_TEST_CASE_P(BackupEngineTestWithParam, BackupEngineTestWithParam,
                        ::testing::Bool());

// this will make sure that backup does not copy the same file twice
TEST_F(BackupEngineTest, NoDoubleCopy_And_AutoGC) {
  OpenDBAndBackupEngine(true, true);

  // should write 5 DB files + one meta file
  test_backup_env_->SetLimitWrittenFiles(7);
  test_backup_env_->ClearWrittenFiles();
  test_db_env_->SetLimitWrittenFiles(0);
  dummy_db_->live_files_ = {"00010.sst", "00011.sst", "CURRENT", "MANIFEST-01",
                            "00011.log"};
  test_db_env_->SetFilenamesForMockedAttrs(dummy_db_->live_files_);
  ASSERT_OK(backup_engine_->CreateNewBackup(db_.get(), false));
  std::vector<std::string> should_have_written = {
      "/shared/.00010.sst.tmp", "/shared/.00011.sst.tmp", "/private/1/CURRENT",
      "/private/1/MANIFEST-01", "/private/1/00011.log",   "/meta/.1.tmp"};
  AppendPath(backupdir_, should_have_written);
  test_backup_env_->AssertWrittenFiles(should_have_written);

  char db_number = '1';

  for (std::string other_sst : {"00015.sst", "00017.sst", "00019.sst"}) {
    // should write 4 new DB files + one meta file
    // should not write/copy 00010.sst, since it's already there!
    test_backup_env_->SetLimitWrittenFiles(6);
    test_backup_env_->ClearWrittenFiles();

    dummy_db_->live_files_ = {"00010.sst", other_sst, "CURRENT", "MANIFEST-01",
                              "00011.log"};
    test_db_env_->SetFilenamesForMockedAttrs(dummy_db_->live_files_);
    ASSERT_OK(backup_engine_->CreateNewBackup(db_.get(), false));
    // should not open 00010.sst - it's already there

    ++db_number;
    std::string private_dir = std::string("/private/") + db_number;
    should_have_written = {
        "/shared/." + other_sst + ".tmp", private_dir + "/CURRENT",
        private_dir + "/MANIFEST-01", private_dir + "/00011.log",
        std::string("/meta/.") + db_number + ".tmp"};
    AppendPath(backupdir_, should_have_written);
    test_backup_env_->AssertWrittenFiles(should_have_written);
  }

  ASSERT_OK(backup_engine_->DeleteBackup(1));
  ASSERT_OK(test_backup_env_->FileExists(backupdir_ + "/shared/00010.sst"));

  // 00011.sst was only in backup 1, should be deleted
  ASSERT_EQ(Status::NotFound(),
            test_backup_env_->FileExists(backupdir_ + "/shared/00011.sst"));
  ASSERT_OK(test_backup_env_->FileExists(backupdir_ + "/shared/00015.sst"));

  // MANIFEST file size should be only 100
  uint64_t size = 0;
  ASSERT_OK(test_backup_env_->GetFileSize(backupdir_ + "/private/2/MANIFEST-01",
                                          &size));
  ASSERT_EQ(100UL, size);
  ASSERT_OK(
      test_backup_env_->GetFileSize(backupdir_ + "/shared/00015.sst", &size));
  ASSERT_EQ(200UL, size);

  CloseBackupEngine();

  //
  // Now simulate incomplete delete by removing just meta
  //
  ASSERT_OK(test_backup_env_->DeleteFile(backupdir_ + "/meta/2"));

  OpenBackupEngine();

  // 1 appears to be removed, so
  // 2 non-corrupt and 0 corrupt seen
  std::vector<BackupInfo> backup_info;
  std::vector<BackupID> corrupt_backup_ids;
  backup_engine_->GetBackupInfo(&backup_info);
  backup_engine_->GetCorruptedBackups(&corrupt_backup_ids);
  ASSERT_EQ(2UL, backup_info.size());
  ASSERT_EQ(0UL, corrupt_backup_ids.size());

  // Keep the two we see, but this should suffice to purge unreferenced
  // shared files from incomplete delete.
  ASSERT_OK(backup_engine_->PurgeOldBackups(2));

  // Make sure dangling sst file has been removed (somewhere along this
  // process). GarbageCollect should not be needed.
  ASSERT_EQ(Status::NotFound(),
            test_backup_env_->FileExists(backupdir_ + "/shared/00015.sst"));
  ASSERT_OK(test_backup_env_->FileExists(backupdir_ + "/shared/00017.sst"));
  ASSERT_OK(test_backup_env_->FileExists(backupdir_ + "/shared/00019.sst"));

  // Now actually purge a good one
  ASSERT_OK(backup_engine_->PurgeOldBackups(1));

  ASSERT_EQ(Status::NotFound(),
            test_backup_env_->FileExists(backupdir_ + "/shared/00017.sst"));
  ASSERT_OK(test_backup_env_->FileExists(backupdir_ + "/shared/00019.sst"));

  CloseDBAndBackupEngine();
}

// test various kind of corruptions that may happen:
// 1. Not able to write a file for backup - that backup should fail,
//      everything else should work
// 2. Corrupted backup meta file or missing backuped file - we should
//      not be able to open that backup, but all other backups should be
//      fine
// 3. Corrupted checksum value - if the checksum is not a valid uint32_t,
//      db open should fail, otherwise, it aborts during the restore process.
TEST_F(BackupEngineTest, CorruptionsTest) {
  const int keys_iteration = 5000;
  Random rnd(6);
  Status s;

  OpenDBAndBackupEngine(true);
  // create five backups
  for (int i = 0; i < 5; ++i) {
    FillDB(db_.get(), keys_iteration * i, keys_iteration * (i + 1));
    ASSERT_OK(backup_engine_->CreateNewBackup(db_.get(), !!(rnd.Next() % 2)));
  }

  // ---------- case 1. - fail a write -----------
  // try creating backup 6, but fail a write
  FillDB(db_.get(), keys_iteration * 5, keys_iteration * 6);
  test_backup_env_->SetLimitWrittenFiles(2);
  // should fail
  s = backup_engine_->CreateNewBackup(db_.get(), !!(rnd.Next() % 2));
  ASSERT_NOK(s);
  test_backup_env_->SetLimitWrittenFiles(1000000);
  // latest backup should have all the keys
  CloseDBAndBackupEngine();
  AssertBackupConsistency(0, 0, keys_iteration * 5, keys_iteration * 6);

  // --------- case 2. corrupted backup meta or missing backuped file ----
  ASSERT_OK(file_manager_->CorruptFile(backupdir_ + "/meta/5", 3));
  // since 5 meta is now corrupted, latest backup should be 4
  AssertBackupConsistency(0, 0, keys_iteration * 4, keys_iteration * 5);
  OpenBackupEngine();
  s = backup_engine_->RestoreDBFromBackup(5, dbname_, dbname_);
  ASSERT_NOK(s);
  CloseBackupEngine();
  ASSERT_OK(file_manager_->DeleteRandomFileInDir(backupdir_ + "/private/4"));
  // 4 is corrupted, 3 is the latest backup now
  AssertBackupConsistency(0, 0, keys_iteration * 3, keys_iteration * 5);
  OpenBackupEngine();
  s = backup_engine_->RestoreDBFromBackup(4, dbname_, dbname_);
  CloseBackupEngine();
  ASSERT_NOK(s);

  // --------- case 3. corrupted checksum value ----
  ASSERT_OK(file_manager_->CorruptChecksum(backupdir_ + "/meta/3", false));
  // checksum of backup 3 is an invalid value, this can be detected at
  // db open time, and it reverts to the previous backup automatically
  AssertBackupConsistency(0, 0, keys_iteration * 2, keys_iteration * 5);
  // checksum of the backup 2 appears to be valid, this can cause checksum
  // mismatch and abort restore process
  ASSERT_OK(file_manager_->CorruptChecksum(backupdir_ + "/meta/2", true));
  ASSERT_OK(file_manager_->FileExists(backupdir_ + "/meta/2"));
  OpenBackupEngine();
  ASSERT_OK(file_manager_->FileExists(backupdir_ + "/meta/2"));
  s = backup_engine_->RestoreDBFromBackup(2, dbname_, dbname_);
  ASSERT_NOK(s);

  // make sure that no corrupt backups have actually been deleted!
  ASSERT_OK(file_manager_->FileExists(backupdir_ + "/meta/1"));
  ASSERT_OK(file_manager_->FileExists(backupdir_ + "/meta/2"));
  ASSERT_OK(file_manager_->FileExists(backupdir_ + "/meta/3"));
  ASSERT_OK(file_manager_->FileExists(backupdir_ + "/meta/4"));
  ASSERT_OK(file_manager_->FileExists(backupdir_ + "/meta/5"));
  ASSERT_OK(file_manager_->FileExists(backupdir_ + "/private/1"));
  ASSERT_OK(file_manager_->FileExists(backupdir_ + "/private/2"));
  ASSERT_OK(file_manager_->FileExists(backupdir_ + "/private/3"));
  ASSERT_OK(file_manager_->FileExists(backupdir_ + "/private/4"));
  ASSERT_OK(file_manager_->FileExists(backupdir_ + "/private/5"));

  // delete the corrupt backups and then make sure they're actually deleted
  ASSERT_OK(backup_engine_->DeleteBackup(5));
  ASSERT_OK(backup_engine_->DeleteBackup(4));
  ASSERT_OK(backup_engine_->DeleteBackup(3));
  ASSERT_OK(backup_engine_->DeleteBackup(2));
  // Should not be needed anymore with auto-GC on DeleteBackup
  //(void)backup_engine_->GarbageCollect();
  ASSERT_EQ(Status::NotFound(),
            file_manager_->FileExists(backupdir_ + "/meta/5"));
  ASSERT_EQ(Status::NotFound(),
            file_manager_->FileExists(backupdir_ + "/private/5"));
  ASSERT_EQ(Status::NotFound(),
            file_manager_->FileExists(backupdir_ + "/meta/4"));
  ASSERT_EQ(Status::NotFound(),
            file_manager_->FileExists(backupdir_ + "/private/4"));
  ASSERT_EQ(Status::NotFound(),
            file_manager_->FileExists(backupdir_ + "/meta/3"));
  ASSERT_EQ(Status::NotFound(),
            file_manager_->FileExists(backupdir_ + "/private/3"));
  ASSERT_EQ(Status::NotFound(),
            file_manager_->FileExists(backupdir_ + "/meta/2"));
  ASSERT_EQ(Status::NotFound(),
            file_manager_->FileExists(backupdir_ + "/private/2"));
  CloseBackupEngine();
  AssertBackupConsistency(0, 0, keys_iteration * 1, keys_iteration * 5);

  // new backup should be 2!
  OpenDBAndBackupEngine();
  FillDB(db_.get(), keys_iteration * 1, keys_iteration * 2);
  ASSERT_OK(backup_engine_->CreateNewBackup(db_.get(), !!(rnd.Next() % 2)));
  CloseDBAndBackupEngine();
  AssertBackupConsistency(2, 0, keys_iteration * 2, keys_iteration * 5);
}

// Corrupt a file but maintain its size
TEST_F(BackupEngineTest, CorruptFileMaintainSize) {
  const int keys_iteration = 5000;
  OpenDBAndBackupEngine(true);
  // create a backup
  FillDB(db_.get(), 0, keys_iteration);
  ASSERT_OK(backup_engine_->CreateNewBackup(db_.get(), true));
  CloseDBAndBackupEngine();

  OpenDBAndBackupEngine();
  // verify with file size
  ASSERT_OK(backup_engine_->VerifyBackup(1, false));
  // verify with file checksum
  ASSERT_OK(backup_engine_->VerifyBackup(1, true));

  std::string file_to_corrupt;
  uint64_t file_size = 0;
  // under normal circumstance, there should be at least one nonempty file
  while (file_size == 0) {
    // get a random file in /private/1
    assert(file_manager_
               ->GetRandomFileInDir(backupdir_ + "/private/1", &file_to_corrupt,
                                    &file_size)
               .ok());
    // corrupt the file by replacing its content by file_size random bytes
    ASSERT_OK(file_manager_->CorruptFile(file_to_corrupt, file_size));
  }
  // file sizes match
  ASSERT_OK(backup_engine_->VerifyBackup(1, false));
  // file checksums mismatch
  ASSERT_NOK(backup_engine_->VerifyBackup(1, true));
  // sanity check, use default second argument
  ASSERT_OK(backup_engine_->VerifyBackup(1));
  CloseDBAndBackupEngine();

  // an extra challenge
  // set share_files_with_checksum to true and do two more backups
  // corrupt all the table files in shared_checksum but maintain their sizes
  OpenDBAndBackupEngine(true /* destroy_old_data */, false /* dummy */,
                        kShareWithChecksum);
  // creat two backups
  for (int i = 1; i < 3; ++i) {
    FillDB(db_.get(), keys_iteration * i, keys_iteration * (i + 1));
    ASSERT_OK(backup_engine_->CreateNewBackup(db_.get(), true));
  }
  CloseDBAndBackupEngine();

  OpenDBAndBackupEngine();
  std::vector<FileAttributes> children;
  const std::string dir = backupdir_ + "/shared_checksum";
  ASSERT_OK(file_manager_->GetChildrenFileAttributes(dir, &children));
  for (const auto& child : children) {
    if (child.size_bytes == 0) {
      continue;
    }
    // corrupt the file by replacing its content by file_size random bytes
    ASSERT_OK(
        file_manager_->CorruptFile(dir + "/" + child.name, child.size_bytes));
  }
  // file sizes match
  ASSERT_OK(backup_engine_->VerifyBackup(1, false));
  ASSERT_OK(backup_engine_->VerifyBackup(2, false));
  // file checksums mismatch
  ASSERT_NOK(backup_engine_->VerifyBackup(1, true));
  ASSERT_NOK(backup_engine_->VerifyBackup(2, true));
  CloseDBAndBackupEngine();
}

// Corrupt a blob file but maintain its size
TEST_P(BackupEngineTestWithParam, CorruptBlobFileMaintainSize) {
  const int keys_iteration = 5000;
  OpenDBAndBackupEngine(true);
  // create a backup
  FillDB(db_.get(), 0, keys_iteration);
  ASSERT_OK(backup_engine_->CreateNewBackup(db_.get(), true));
  CloseDBAndBackupEngine();

  OpenDBAndBackupEngine();
  // verify with file size
  ASSERT_OK(backup_engine_->VerifyBackup(1, false));
  // verify with file checksum
  ASSERT_OK(backup_engine_->VerifyBackup(1, true));

  std::string file_to_corrupt;
  std::vector<FileAttributes> children;

  std::string dir = backupdir_;
  if (backupable_options_->share_files_with_checksum) {
    dir += "/shared_checksum";
  } else {
    dir += "/shared";
  }

  ASSERT_OK(file_manager_->GetChildrenFileAttributes(dir, &children));

  for (const auto& child : children) {
    if (EndsWith(child.name, ".blob") && child.size_bytes != 0) {
      // corrupt the blob files by replacing its content by file_size random
      // bytes
      ASSERT_OK(
          file_manager_->CorruptFile(dir + "/" + child.name, child.size_bytes));
    }
  }

  // file sizes match
  ASSERT_OK(backup_engine_->VerifyBackup(1, false));
  // file checksums mismatch
  ASSERT_NOK(backup_engine_->VerifyBackup(1, true));
  // sanity check, use default second argument
  ASSERT_OK(backup_engine_->VerifyBackup(1));
  CloseDBAndBackupEngine();
}

// Test if BackupEngine will fail to create new backup if some table has been
// corrupted and the table file checksum is stored in the DB manifest
TEST_F(BackupEngineTest, TableFileCorruptedBeforeBackup) {
  const int keys_iteration = 50000;

  OpenDBAndBackupEngine(true /* destroy_old_data */, false /* dummy */,
                        kNoShare);
  FillDB(db_.get(), 0, keys_iteration);
  CloseAndReopenDB(/*read_only*/ true);
  // corrupt a random table file in the DB directory
  ASSERT_OK(CorruptRandomDataFileInDB(kTableFile));
  // file_checksum_gen_factory is null, and thus table checksum is not
  // verified for creating a new backup; no correction is detected
  ASSERT_OK(backup_engine_->CreateNewBackup(db_.get()));
  CloseDBAndBackupEngine();

  // delete old files in db
  ASSERT_OK(DestroyDB(dbname_, options_));

  // Enable table file checksum in DB manifest
  options_.file_checksum_gen_factory = GetFileChecksumGenCrc32cFactory();
  OpenDBAndBackupEngine(true /* destroy_old_data */, false /* dummy */,
                        kNoShare);
  FillDB(db_.get(), 0, keys_iteration);
  CloseAndReopenDB(/*read_only*/ true);
  // corrupt a random table file in the DB directory
  ASSERT_OK(CorruptRandomDataFileInDB(kTableFile));
  // table file checksum is enabled so we should be able to detect any
  // corruption
  ASSERT_NOK(backup_engine_->CreateNewBackup(db_.get()));
  CloseDBAndBackupEngine();
}

// Test if BackupEngine will fail to create new backup if some blob files has
// been corrupted and the blob file checksum is stored in the DB manifest
TEST_F(BackupEngineTest, BlobFileCorruptedBeforeBackup) {
  const int keys_iteration = 50000;

  OpenDBAndBackupEngine(true /* destroy_old_data */, false /* dummy */,
                        kNoShare);
  FillDB(db_.get(), 0, keys_iteration);
  CloseAndReopenDB(/*read_only*/ true);
  // corrupt a random blob file in the DB directory
  ASSERT_OK(CorruptRandomDataFileInDB(kBlobFile));
  // file_checksum_gen_factory is null, and thus blob checksum is not
  // verified for creating a new backup; no correction is detected
  ASSERT_OK(backup_engine_->CreateNewBackup(db_.get()));
  CloseDBAndBackupEngine();

  // delete old files in db
  ASSERT_OK(DestroyDB(dbname_, options_));

  // Enable file checksum in DB manifest
  options_.file_checksum_gen_factory = GetFileChecksumGenCrc32cFactory();
  OpenDBAndBackupEngine(true /* destroy_old_data */, false /* dummy */,
                        kNoShare);
  FillDB(db_.get(), 0, keys_iteration);
  CloseAndReopenDB(/*read_only*/ true);
  // corrupt a random blob file in the DB directory
  ASSERT_OK(CorruptRandomDataFileInDB(kBlobFile));

  // file checksum is enabled so we should be able to detect any
  // corruption
  ASSERT_NOK(backup_engine_->CreateNewBackup(db_.get()));
  CloseDBAndBackupEngine();
}

#if !defined(ROCKSDB_VALGRIND_RUN) || defined(ROCKSDB_FULL_VALGRIND_RUN)
// Test if BackupEngine will fail to create new backup if some table has been
// corrupted and the table file checksum is stored in the DB manifest for the
// case when backup table files will be stored in a shared directory
TEST_P(BackupEngineTestWithParam, TableFileCorruptedBeforeBackup) {
  const int keys_iteration = 50000;

  OpenDBAndBackupEngine(true /* destroy_old_data */);
  FillDB(db_.get(), 0, keys_iteration);
  CloseAndReopenDB(/*read_only*/ true);
  // corrupt a random table file in the DB directory
  ASSERT_OK(CorruptRandomDataFileInDB(kTableFile));
  // cannot detect corruption since DB manifest has no table checksums
  ASSERT_OK(backup_engine_->CreateNewBackup(db_.get()));
  CloseDBAndBackupEngine();

  // delete old files in db
  ASSERT_OK(DestroyDB(dbname_, options_));

  // Enable table checksums in DB manifest
  options_.file_checksum_gen_factory = GetFileChecksumGenCrc32cFactory();
  OpenDBAndBackupEngine(true /* destroy_old_data */);
  FillDB(db_.get(), 0, keys_iteration);
  CloseAndReopenDB(/*read_only*/ true);
  // corrupt a random table file in the DB directory
  ASSERT_OK(CorruptRandomDataFileInDB(kTableFile));
  // corruption is detected
  ASSERT_NOK(backup_engine_->CreateNewBackup(db_.get()));
  CloseDBAndBackupEngine();
}

// Test if BackupEngine will fail to create new backup if some blob files have
// been corrupted and the blob file checksum is stored in the DB manifest for
// the case when backup blob files will be stored in a shared directory
TEST_P(BackupEngineTestWithParam, BlobFileCorruptedBeforeBackup) {
  const int keys_iteration = 50000;
  OpenDBAndBackupEngine(true /* destroy_old_data */);
  FillDB(db_.get(), 0, keys_iteration);
  CloseAndReopenDB(/*read_only*/ true);
  // corrupt a random blob file in the DB directory
  ASSERT_OK(CorruptRandomDataFileInDB(kBlobFile));
  // cannot detect corruption since DB manifest has no blob file checksums
  ASSERT_OK(backup_engine_->CreateNewBackup(db_.get()));
  CloseDBAndBackupEngine();

  // delete old files in db
  ASSERT_OK(DestroyDB(dbname_, options_));

  // Enable blob file checksums in DB manifest
  options_.file_checksum_gen_factory = GetFileChecksumGenCrc32cFactory();
  OpenDBAndBackupEngine(true /* destroy_old_data */);
  FillDB(db_.get(), 0, keys_iteration);
  CloseAndReopenDB(/*read_only*/ true);
  // corrupt a random blob file in the DB directory
  ASSERT_OK(CorruptRandomDataFileInDB(kBlobFile));
  // corruption is detected
  ASSERT_NOK(backup_engine_->CreateNewBackup(db_.get()));
  CloseDBAndBackupEngine();
}
#endif  // !defined(ROCKSDB_VALGRIND_RUN) || defined(ROCKSDB_FULL_VALGRIND_RUN)

TEST_F(BackupEngineTest, TableFileWithoutDbChecksumCorruptedDuringBackup) {
  const int keys_iteration = 50000;
  backupable_options_->share_files_with_checksum_naming =
      kLegacyCrc32cAndFileSize;
  // When share_files_with_checksum is on, we calculate checksums of table
  // files before and after copying. So we can test whether a corruption has
  // happened during the file is copied to backup directory.
  OpenDBAndBackupEngine(true /* destroy_old_data */, false /* dummy */,
                        kShareWithChecksum);

  FillDB(db_.get(), 0, keys_iteration);
  std::atomic<bool> corrupted{false};
  // corrupt files when copying to the backup directory
  SyncPoint::GetInstance()->SetCallBack(
      "BackupEngineImpl::CopyOrCreateFile:CorruptionDuringBackup",
      [&](void* data) {
        if (data != nullptr) {
          Slice* d = reinterpret_cast<Slice*>(data);
          if (!d->empty()) {
            d->remove_suffix(1);
            corrupted = true;
          }
        }
      });
  SyncPoint::GetInstance()->EnableProcessing();
  Status s = backup_engine_->CreateNewBackup(db_.get());
  if (corrupted) {
    ASSERT_NOK(s);
  } else {
    // should not in this path in normal cases
    ASSERT_OK(s);
  }

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();

  CloseDBAndBackupEngine();
  // delete old files in db
  ASSERT_OK(DestroyDB(dbname_, options_));
}

TEST_F(BackupEngineTest, TableFileWithDbChecksumCorruptedDuringBackup) {
  const int keys_iteration = 50000;
  options_.file_checksum_gen_factory = GetFileChecksumGenCrc32cFactory();
  for (auto& sopt : kAllShareOptions) {
    // Since the default DB table file checksum is on, we obtain checksums of
    // table files from the DB manifest before copying and verify it with the
    // one calculated during copying.
    // Therefore, we can test whether a corruption has happened during the file
    // being copied to backup directory.
    OpenDBAndBackupEngine(true /* destroy_old_data */, false /* dummy */, sopt);

    FillDB(db_.get(), 0, keys_iteration);

    // corrupt files when copying to the backup directory
    SyncPoint::GetInstance()->SetCallBack(
        "BackupEngineImpl::CopyOrCreateFile:CorruptionDuringBackup",
        [&](void* data) {
          if (data != nullptr) {
            Slice* d = reinterpret_cast<Slice*>(data);
            if (!d->empty()) {
              d->remove_suffix(1);
            }
          }
        });
    SyncPoint::GetInstance()->EnableProcessing();
    // The only case that we can't detect a corruption is when the file
    // being backed up is empty. But as keys_iteration is large, such
    // a case shouldn't have happened and we should be able to detect
    // the corruption.
    ASSERT_NOK(backup_engine_->CreateNewBackup(db_.get()));

    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();

    CloseDBAndBackupEngine();
    // delete old files in db
    ASSERT_OK(DestroyDB(dbname_, options_));
  }
}

TEST_F(BackupEngineTest, InterruptCreationTest) {
  // Interrupt backup creation by failing new writes and failing cleanup of the
  // partial state. Then verify a subsequent backup can still succeed.
  const int keys_iteration = 5000;
  Random rnd(6);

  OpenDBAndBackupEngine(true /* destroy_old_data */);
  FillDB(db_.get(), 0, keys_iteration);
  test_backup_env_->SetLimitWrittenFiles(2);
  test_backup_env_->SetDeleteFileFailure(true);
  // should fail creation
  ASSERT_NOK(backup_engine_->CreateNewBackup(db_.get(), !!(rnd.Next() % 2)));
  CloseDBAndBackupEngine();
  // should also fail cleanup so the tmp directory stays behind
  ASSERT_OK(backup_chroot_env_->FileExists(backupdir_ + "/private/1/"));

  OpenDBAndBackupEngine(false /* destroy_old_data */);
  test_backup_env_->SetLimitWrittenFiles(1000000);
  test_backup_env_->SetDeleteFileFailure(false);
  ASSERT_OK(backup_engine_->CreateNewBackup(db_.get(), !!(rnd.Next() % 2)));
  // latest backup should have all the keys
  CloseDBAndBackupEngine();
  AssertBackupConsistency(0, 0, keys_iteration);
}

TEST_F(BackupEngineTest, FlushCompactDuringBackupCheckpoint) {
  const int keys_iteration = 5000;
  options_.file_checksum_gen_factory = GetFileChecksumGenCrc32cFactory();
  for (const auto& sopt : kAllShareOptions) {
    OpenDBAndBackupEngine(true /* destroy_old_data */, false /* dummy */, sopt);
    FillDB(db_.get(), 0, keys_iteration);
    // That FillDB leaves a mix of flushed and unflushed data
    SyncPoint::GetInstance()->LoadDependency(
        {{"CheckpointImpl::CreateCustomCheckpoint:AfterGetLive1",
          "BackupEngineTest::FlushCompactDuringBackupCheckpoint:Before"},
         {"BackupEngineTest::FlushCompactDuringBackupCheckpoint:After",
          "CheckpointImpl::CreateCustomCheckpoint:AfterGetLive2"}});
    SyncPoint::GetInstance()->EnableProcessing();
    ROCKSDB_NAMESPACE::port::Thread flush_thread{[this]() {
      TEST_SYNC_POINT(
          "BackupEngineTest::FlushCompactDuringBackupCheckpoint:Before");
      FillDB(db_.get(), keys_iteration, 2 * keys_iteration);
      ASSERT_OK(db_->Flush(FlushOptions()));
      DBImpl* dbi = static_cast<DBImpl*>(db_.get());
      ASSERT_OK(dbi->TEST_WaitForFlushMemTable());
      ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
      ASSERT_OK(dbi->TEST_WaitForCompact());
      TEST_SYNC_POINT(
          "BackupEngineTest::FlushCompactDuringBackupCheckpoint:After");
    }};
    ASSERT_OK(backup_engine_->CreateNewBackup(db_.get()));
    flush_thread.join();
    CloseDBAndBackupEngine();
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
    /* FIXME(peterd): reinstate with option for checksum in file names
    if (sopt == kShareWithChecksum) {
      // Ensure we actually got DB manifest checksums by inspecting
      // shared_checksum file names for hex checksum component
      TestRegex expected("[^_]+_[0-9A-F]{8}_[^_]+.sst");
      std::vector<FileAttributes> children;
      const std::string dir = backupdir_ + "/shared_checksum";
      ASSERT_OK(file_manager_->GetChildrenFileAttributes(dir, &children));
      for (const auto& child : children) {
        if (child.size_bytes == 0) {
          continue;
        }
        EXPECT_MATCHES_REGEX(child.name, expected);
      }
    }
    */
    AssertBackupConsistency(0, 0, keys_iteration);
  }
}

inline std::string OptionsPath(std::string ret, int backupID) {
  ret += "/private/";
  ret += std::to_string(backupID);
  ret += "/";
  return ret;
}

// Backup the LATEST options file to
// "<backup_dir>/private/<backup_id>/OPTIONS<number>"

TEST_F(BackupEngineTest, BackupOptions) {
  OpenDBAndBackupEngine(true);
  for (int i = 1; i < 5; i++) {
    std::string name;
    std::vector<std::string> filenames;
    // Must reset() before reset(OpenDB()) again.
    // Calling OpenDB() while *db_ is existing will cause LOCK issue
    db_.reset();
    db_.reset(OpenDB());
    ASSERT_OK(backup_engine_->CreateNewBackup(db_.get(), true));
    ASSERT_OK(ROCKSDB_NAMESPACE::GetLatestOptionsFileName(db_->GetName(),
                                                          options_.env, &name));
    ASSERT_OK(file_manager_->FileExists(OptionsPath(backupdir_, i) + name));
    ASSERT_OK(backup_chroot_env_->GetChildren(OptionsPath(backupdir_, i),
                                              &filenames));
    for (auto fn : filenames) {
      if (fn.compare(0, 7, "OPTIONS") == 0) {
        ASSERT_EQ(name, fn);
      }
    }
  }

  CloseDBAndBackupEngine();
}

TEST_F(BackupEngineTest, SetOptionsBackupRaceCondition) {
  OpenDBAndBackupEngine(true);
  SyncPoint::GetInstance()->LoadDependency(
      {{"CheckpointImpl::CreateCheckpoint:SavedLiveFiles1",
        "BackupEngineTest::SetOptionsBackupRaceCondition:BeforeSetOptions"},
       {"BackupEngineTest::SetOptionsBackupRaceCondition:AfterSetOptions",
        "CheckpointImpl::CreateCheckpoint:SavedLiveFiles2"}});
  SyncPoint::GetInstance()->EnableProcessing();
  ROCKSDB_NAMESPACE::port::Thread setoptions_thread{[this]() {
    TEST_SYNC_POINT(
        "BackupEngineTest::SetOptionsBackupRaceCondition:BeforeSetOptions");
    DBImpl* dbi = static_cast<DBImpl*>(db_.get());
    // Change arbitrary option to trigger OPTIONS file deletion
    ASSERT_OK(dbi->SetOptions(dbi->DefaultColumnFamily(),
                              {{"paranoid_file_checks", "false"}}));
    ASSERT_OK(dbi->SetOptions(dbi->DefaultColumnFamily(),
                              {{"paranoid_file_checks", "true"}}));
    ASSERT_OK(dbi->SetOptions(dbi->DefaultColumnFamily(),
                              {{"paranoid_file_checks", "false"}}));
    TEST_SYNC_POINT(
        "BackupEngineTest::SetOptionsBackupRaceCondition:AfterSetOptions");
  }};
  ASSERT_OK(backup_engine_->CreateNewBackup(db_.get()));
  setoptions_thread.join();
  CloseDBAndBackupEngine();
}

// This test verifies we don't delete the latest backup when read-only option is
// set
TEST_F(BackupEngineTest, NoDeleteWithReadOnly) {
  const int keys_iteration = 5000;
  Random rnd(6);

  OpenDBAndBackupEngine(true);
  // create five backups
  for (int i = 0; i < 5; ++i) {
    FillDB(db_.get(), keys_iteration * i, keys_iteration * (i + 1));
    ASSERT_OK(backup_engine_->CreateNewBackup(db_.get(), !!(rnd.Next() % 2)));
  }
  CloseDBAndBackupEngine();
  ASSERT_OK(file_manager_->WriteToFile(latest_backup_, "4"));

  backupable_options_->destroy_old_data = false;
  BackupEngineReadOnly* read_only_backup_engine;
  ASSERT_OK(BackupEngineReadOnly::Open(backup_chroot_env_.get(),
                                       *backupable_options_,
                                       &read_only_backup_engine));

  // assert that data from backup 5 is still here (even though LATEST_BACKUP
  // says 4 is latest)
  ASSERT_OK(file_manager_->FileExists(backupdir_ + "/meta/5"));
  ASSERT_OK(file_manager_->FileExists(backupdir_ + "/private/5"));

  // Behavior change: We now ignore LATEST_BACKUP contents. This means that
  // we should have 5 backups, even if LATEST_BACKUP says 4.
  std::vector<BackupInfo> backup_info;
  read_only_backup_engine->GetBackupInfo(&backup_info);
  ASSERT_EQ(5UL, backup_info.size());
  delete read_only_backup_engine;
}

TEST_F(BackupEngineTest, FailOverwritingBackups) {
  options_.write_buffer_size = 1024 * 1024 * 1024;  // 1GB
  options_.disable_auto_compactions = true;

  // create backups 1, 2, 3, 4, 5
  OpenDBAndBackupEngine(true);
  for (int i = 0; i < 5; ++i) {
    CloseDBAndBackupEngine();
    DeleteLogFiles();
    OpenDBAndBackupEngine(false);
    FillDB(db_.get(), 100 * i, 100 * (i + 1), kFlushAll);
    ASSERT_OK(backup_engine_->CreateNewBackup(db_.get()));
  }
  CloseDBAndBackupEngine();

  // restore 3
  OpenBackupEngine();
  ASSERT_OK(backup_engine_->RestoreDBFromBackup(3, dbname_, dbname_));
  CloseBackupEngine();

  OpenDBAndBackupEngine(false);
  // More data, bigger SST
  FillDB(db_.get(), 1000, 1300, kFlushAll);
  Status s = backup_engine_->CreateNewBackup(db_.get());
  // the new backup fails because new table files
  // clash with old table files from backups 4 and 5
  // (since write_buffer_size is huge, we can be sure that
  // each backup will generate only one sst file and that
  // a file generated here would have the same name as an
  // sst file generated by backup 4, and will be bigger)
  ASSERT_TRUE(s.IsCorruption());
  ASSERT_OK(backup_engine_->DeleteBackup(4));
  ASSERT_OK(backup_engine_->DeleteBackup(5));
  // now, the backup can succeed
  ASSERT_OK(backup_engine_->CreateNewBackup(db_.get()));
  CloseDBAndBackupEngine();
}

TEST_F(BackupEngineTest, NoShareTableFiles) {
  const int keys_iteration = 5000;
  OpenDBAndBackupEngine(true, false, kNoShare);
  for (int i = 0; i < 5; ++i) {
    FillDB(db_.get(), keys_iteration * i, keys_iteration * (i + 1));
    ASSERT_OK(backup_engine_->CreateNewBackup(db_.get(), !!(i % 2)));
  }
  CloseDBAndBackupEngine();

  for (int i = 0; i < 5; ++i) {
    AssertBackupConsistency(i + 1, 0, keys_iteration * (i + 1),
                            keys_iteration * 6);
  }
}

// Verify that you can backup and restore with share_files_with_checksum on
TEST_F(BackupEngineTest, ShareTableFilesWithChecksums) {
  const int keys_iteration = 5000;
  OpenDBAndBackupEngine(true, false, kShareWithChecksum);
  for (int i = 0; i < 5; ++i) {
    FillDB(db_.get(), keys_iteration * i, keys_iteration * (i + 1));
    ASSERT_OK(backup_engine_->CreateNewBackup(db_.get(), !!(i % 2)));
  }
  CloseDBAndBackupEngine();

  for (int i = 0; i < 5; ++i) {
    AssertBackupConsistency(i + 1, 0, keys_iteration * (i + 1),
                            keys_iteration * 6);
  }
}

// Verify that you can backup and restore using share_files_with_checksum set to
// false and then transition this option to true
TEST_F(BackupEngineTest, ShareTableFilesWithChecksumsTransition) {
  const int keys_iteration = 5000;
  // set share_files_with_checksum to false
  OpenDBAndBackupEngine(true, false, kShareNoChecksum);
  for (int i = 0; i < 5; ++i) {
    FillDB(db_.get(), keys_iteration * i, keys_iteration * (i + 1));
    ASSERT_OK(backup_engine_->CreateNewBackup(db_.get(), true));
  }
  CloseDBAndBackupEngine();

  for (int i = 0; i < 5; ++i) {
    AssertBackupConsistency(i + 1, 0, keys_iteration * (i + 1),
                            keys_iteration * 6);
  }

  // set share_files_with_checksum to true and do some more backups
  OpenDBAndBackupEngine(false /* destroy_old_data */, false,
                        kShareWithChecksum);
  for (int i = 5; i < 10; ++i) {
    FillDB(db_.get(), keys_iteration * i, keys_iteration * (i + 1));
    ASSERT_OK(backup_engine_->CreateNewBackup(db_.get(), true));
  }
  CloseDBAndBackupEngine();

  // Verify first (about to delete)
  AssertBackupConsistency(1, 0, keys_iteration, keys_iteration * 11);

  // For an extra challenge, make sure that GarbageCollect / DeleteBackup
  // is OK even if we open without share_table_files
  OpenDBAndBackupEngine(false /* destroy_old_data */, false, kNoShare);
  ASSERT_OK(backup_engine_->DeleteBackup(1));
  ASSERT_OK(backup_engine_->GarbageCollect());
  CloseDBAndBackupEngine();

  // Verify rest (not deleted)
  for (int i = 1; i < 10; ++i) {
    AssertBackupConsistency(i + 1, 0, keys_iteration * (i + 1),
                            keys_iteration * 11);
  }
}

// Verify backup and restore with various naming options, check names
TEST_F(BackupEngineTest, ShareTableFilesWithChecksumsNewNaming) {
  ASSERT_TRUE(backupable_options_->share_files_with_checksum_naming ==
              kNamingDefault);

  const int keys_iteration = 5000;

  OpenDBAndBackupEngine(true, false, kShareWithChecksum);
  FillDB(db_.get(), 0, keys_iteration);
  CloseDBAndBackupEngine();

  static const std::map<ShareFilesNaming, TestRegex> option_to_expected = {
      {kLegacyCrc32cAndFileSize, "[0-9]+_[0-9]+_[0-9]+[.]sst"},
      // kFlagIncludeFileSize redundant here
      {kLegacyCrc32cAndFileSize | kFlagIncludeFileSize,
       "[0-9]+_[0-9]+_[0-9]+[.]sst"},
      {kUseDbSessionId, "[0-9]+_s[0-9A-Z]{20}[.]sst"},
      {kUseDbSessionId | kFlagIncludeFileSize,
       "[0-9]+_s[0-9A-Z]{20}_[0-9]+[.]sst"},
  };

  const TestRegex blobfile_pattern = "[0-9]+_[0-9]+_[0-9]+[.]blob";

  for (const auto& pair : option_to_expected) {
    CloseAndReopenDB();
    backupable_options_->share_files_with_checksum_naming = pair.first;
    OpenBackupEngine(true /*destroy_old_data*/);
    ASSERT_OK(backup_engine_->CreateNewBackup(db_.get()));
    CloseDBAndBackupEngine();
    AssertBackupConsistency(1, 0, keys_iteration, keys_iteration * 2);
    AssertDirectoryFilesMatchRegex(backupdir_ + "/shared_checksum", pair.second,
                                   ".sst", 1 /* minimum_count */);
    if (std::string::npos != pair.second.GetPattern().find("_[0-9]+[.]sst")) {
      AssertDirectoryFilesSizeIndicators(backupdir_ + "/shared_checksum",
                                         1 /* minimum_count */);
    }

    AssertDirectoryFilesMatchRegex(backupdir_ + "/shared_checksum",
                                   blobfile_pattern, ".blob",
                                   1 /* minimum_count */);
  }
}

// Mimic SST file generated by pre-6.12 releases and verify that
// old names are always used regardless of naming option.
TEST_F(BackupEngineTest, ShareTableFilesWithChecksumsOldFileNaming) {
  const int keys_iteration = 5000;

  // Pre-6.12 release did not include db id and db session id properties.
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "PropertyBlockBuilder::AddTableProperty:Start", [&](void* props_vs) {
        auto props = static_cast<TableProperties*>(props_vs);
        props->db_id = "";
        props->db_session_id = "";
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  OpenDBAndBackupEngine(true, false, kShareWithChecksum);
  FillDB(db_.get(), 0, keys_iteration);
  CloseDBAndBackupEngine();

  // Old names should always be used on old files
  const TestRegex sstfile_pattern("[0-9]+_[0-9]+_[0-9]+[.]sst");

  const TestRegex blobfile_pattern = "[0-9]+_[0-9]+_[0-9]+[.]blob";

  for (ShareFilesNaming option : {kNamingDefault, kUseDbSessionId}) {
    CloseAndReopenDB();
    backupable_options_->share_files_with_checksum_naming = option;
    OpenBackupEngine(true /*destroy_old_data*/);
    ASSERT_OK(backup_engine_->CreateNewBackup(db_.get()));
    CloseDBAndBackupEngine();
    AssertBackupConsistency(1, 0, keys_iteration, keys_iteration * 2);
    AssertDirectoryFilesMatchRegex(backupdir_ + "/shared_checksum",
                                   sstfile_pattern, ".sst",
                                   1 /* minimum_count */);
    AssertDirectoryFilesMatchRegex(backupdir_ + "/shared_checksum",
                                   blobfile_pattern, ".blob",
                                   1 /* minimum_count */);
  }

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearAllCallBacks();
}

// Test how naming options interact with detecting DB corruption
// between incremental backups
TEST_F(BackupEngineTest, TableFileCorruptionBeforeIncremental) {
  const auto share_no_checksum = static_cast<ShareFilesNaming>(0);

  for (bool corrupt_before_first_backup : {false, true}) {
    for (ShareFilesNaming option :
         {share_no_checksum, kLegacyCrc32cAndFileSize, kNamingDefault}) {
      auto share =
          option == share_no_checksum ? kShareNoChecksum : kShareWithChecksum;
      if (option != share_no_checksum) {
        backupable_options_->share_files_with_checksum_naming = option;
      }
      OpenDBAndBackupEngine(true, false, share);
      DBImpl* dbi = static_cast<DBImpl*>(db_.get());
      // A small SST file
      ASSERT_OK(dbi->Put(WriteOptions(), "x", "y"));
      ASSERT_OK(dbi->Flush(FlushOptions()));
      // And a bigger one
      ASSERT_OK(dbi->Put(WriteOptions(), "y", Random(42).RandomString(500)));
      ASSERT_OK(dbi->Flush(FlushOptions()));
      ASSERT_OK(dbi->TEST_WaitForFlushMemTable());
      CloseAndReopenDB(/*read_only*/ true);

      std::vector<FileAttributes> table_files;
      ASSERT_OK(GetDataFilesInDB(kTableFile, &table_files));
      ASSERT_EQ(table_files.size(), 2);
      std::string tf0 = dbname_ + "/" + table_files[0].name;
      std::string tf1 = dbname_ + "/" + table_files[1].name;

      CloseDBAndBackupEngine();

      if (corrupt_before_first_backup) {
        // This corrupts a data block, which does not cause DB open
        // failure, only failure on accessing the block.
        ASSERT_OK(db_file_manager_->CorruptFileStart(tf0));
      }

      OpenDBAndBackupEngine(false, false, share);
      ASSERT_OK(backup_engine_->CreateNewBackup(db_.get()));
      CloseDBAndBackupEngine();

      // if corrupt_before_first_backup, this undoes the initial corruption
      ASSERT_OK(db_file_manager_->CorruptFileStart(tf0));

      OpenDBAndBackupEngine(false, false, share);
      Status s = backup_engine_->CreateNewBackup(db_.get());

      // Even though none of the naming options catch the inconsistency
      // between the first and second time backing up fname, in the case
      // of kUseDbSessionId (kNamingDefault), this is an intentional
      // trade-off to avoid full scan of files from the DB that are
      // already backed up. If we did the scan, kUseDbSessionId could catch
      // the corruption. kLegacyCrc32cAndFileSize does the scan (to
      // compute checksum for name) without catching the corruption,
      // because the corruption means the names don't merge.
      EXPECT_OK(s);

      // VerifyBackup doesn't check DB integrity or table file internal
      // checksums
      EXPECT_OK(backup_engine_->VerifyBackup(1, true));
      EXPECT_OK(backup_engine_->VerifyBackup(2, true));

      db_.reset();
      ASSERT_OK(backup_engine_->RestoreDBFromBackup(2, dbname_, dbname_));
      {
        DB* db = OpenDB();
        s = db->VerifyChecksum();
        delete db;
      }
      if (option != kLegacyCrc32cAndFileSize && !corrupt_before_first_backup) {
        // Second backup is OK because it used (uncorrupt) file from first
        // backup instead of (corrupt) file from DB.
        // This is arguably a good trade-off vs. treating the file as distinct
        // from the old version, because a file should be more likely to be
        // corrupt as it ages. Although the backed-up file might also corrupt
        // with age, the alternative approach (checksum in file name computed
        // from current DB file contents) wouldn't detect that case at backup
        // time either. Although you would have both copies of the file with
        // the alternative approach, that would only last until the older
        // backup is deleted.
        ASSERT_OK(s);
      } else if (option == kLegacyCrc32cAndFileSize &&
                 corrupt_before_first_backup) {
        // Second backup is OK because it saved the updated (uncorrupt)
        // file from DB, instead of the sharing with first backup.
        // Recall: if corrupt_before_first_backup, [second CorruptFileStart]
        // undoes the initial corruption.
        // This is arguably a bad trade-off vs. sharing the old version of the
        // file because a file should be more likely to corrupt as it ages.
        // (Not likely that the previously backed-up version was already
        // corrupt and the new version is non-corrupt. This approach doesn't
        // help if backed-up version is corrupted after taking the backup.)
        ASSERT_OK(s);
      } else {
        // Something is legitimately corrupted, but we can't be sure what
        // with information available (TODO? unless one passes block checksum
        // test and other doesn't. Probably better to use end-to-end full file
        // checksum anyway.)
        ASSERT_TRUE(s.IsCorruption());
      }

      CloseDBAndBackupEngine();
      ASSERT_OK(DestroyDB(dbname_, options_));
    }
  }
}

// Test how naming options interact with detecting file size corruption
// between incremental backups
TEST_F(BackupEngineTest, FileSizeForIncremental) {
  const auto share_no_checksum = static_cast<ShareFilesNaming>(0);
  // TODO: enable blob files once Integrated BlobDB supports DB session id.
  options_.enable_blob_files = false;

  for (ShareFilesNaming option : {share_no_checksum, kLegacyCrc32cAndFileSize,
                                  kNamingDefault, kUseDbSessionId}) {
    auto share =
        option == share_no_checksum ? kShareNoChecksum : kShareWithChecksum;
    if (option != share_no_checksum) {
      backupable_options_->share_files_with_checksum_naming = option;
    }
    OpenDBAndBackupEngine(true, false, share);

    std::vector<FileAttributes> children;
    const std::string shared_dir =
        backupdir_ +
        (option == share_no_checksum ? "/shared" : "/shared_checksum");

    // A single small SST file
    ASSERT_OK(db_->Put(WriteOptions(), "x", "y"));

    // First, test that we always detect file size corruption on the shared
    // backup side on incremental. (Since sizes aren't really part of backup
    // meta file, this works by querying the filesystem for the sizes.)
    ASSERT_OK(backup_engine_->CreateNewBackup(db_.get(), true /*flush*/));
    CloseDBAndBackupEngine();

    // Corrupt backup SST file
    ASSERT_OK(file_manager_->GetChildrenFileAttributes(shared_dir, &children));
    ASSERT_EQ(children.size(), 1U);  // one sst
    for (const auto& child : children) {
      if (child.name.size() > 4 && child.size_bytes > 0) {
        ASSERT_OK(
            file_manager_->WriteToFile(shared_dir + "/" + child.name, "asdf"));
        break;
      }
    }

    OpenDBAndBackupEngine(false, false, share);
    Status s = backup_engine_->CreateNewBackup(db_.get());
    EXPECT_TRUE(s.IsCorruption());

    ASSERT_OK(backup_engine_->PurgeOldBackups(0));
    CloseDBAndBackupEngine();

    // Second, test that a hypothetical db session id collision would likely
    // not suffice to corrupt a backup, because there's a good chance of
    // file size difference (in this test, guaranteed) so either no name
    // collision or detected collision.

    // Create backup 1
    OpenDBAndBackupEngine(false, false, share);
    ASSERT_OK(backup_engine_->CreateNewBackup(db_.get()));

    // Even though we have "the same" DB state as backup 1, we need
    // to restore to recreate the same conditions as later restore.
    db_.reset();
    ASSERT_OK(DestroyDB(dbname_, options_));
    ASSERT_OK(backup_engine_->RestoreDBFromBackup(1, dbname_, dbname_));
    CloseDBAndBackupEngine();

    // Forge session id
    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
        "DBImpl::SetDbSessionId", [](void* sid_void_star) {
          std::string* sid = static_cast<std::string*>(sid_void_star);
          *sid = "01234567890123456789";
        });
    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

    // Create another SST file
    OpenDBAndBackupEngine(false, false, share);
    ASSERT_OK(db_->Put(WriteOptions(), "y", "x"));

    // Create backup 2
    ASSERT_OK(backup_engine_->CreateNewBackup(db_.get(), true /*flush*/));

    // Restore backup 1 (again)
    db_.reset();
    ASSERT_OK(DestroyDB(dbname_, options_));
    ASSERT_OK(backup_engine_->RestoreDBFromBackup(1, dbname_, dbname_));
    CloseDBAndBackupEngine();

    // Create another SST file with same number and db session id, only bigger
    OpenDBAndBackupEngine(false, false, share);
    ASSERT_OK(db_->Put(WriteOptions(), "y", Random(42).RandomString(500)));

    // Count backup SSTs files.
    children.clear();
    ASSERT_OK(file_manager_->GetChildrenFileAttributes(shared_dir, &children));
    ASSERT_EQ(children.size(), 2U);  // two sst files

    // Try create backup 3
    s = backup_engine_->CreateNewBackup(db_.get(), true /*flush*/);

    // Re-count backup SSTs
    children.clear();
    ASSERT_OK(file_manager_->GetChildrenFileAttributes(shared_dir, &children));

    if (option == kUseDbSessionId) {
      // Acceptable to call it corruption if size is not in name and
      // db session id collision is practically impossible.
      EXPECT_TRUE(s.IsCorruption());
      EXPECT_EQ(children.size(), 2U);  // no SST file added
    } else if (option == share_no_checksum) {
      // Good to call it corruption if both backups cannot be
      // accommodated.
      EXPECT_TRUE(s.IsCorruption());
      EXPECT_EQ(children.size(), 2U);  // no SST file added
    } else {
      // Since opening a DB seems sufficient for detecting size corruption
      // on the DB side, this should be a good thing, ...
      EXPECT_OK(s);
      // ... as long as we did actually treat it as a distinct SST file.
      EXPECT_EQ(children.size(), 3U);  // Another SST added
    }
    CloseDBAndBackupEngine();
    ASSERT_OK(DestroyDB(dbname_, options_));
    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearAllCallBacks();
  }
}

// Verify backup and restore with share_files_with_checksum off and then
// transition this option to on and share_files_with_checksum_naming to be
// based on kUseDbSessionId
TEST_F(BackupEngineTest, ShareTableFilesWithChecksumsNewNamingTransition) {
  const int keys_iteration = 5000;
  // We may set share_files_with_checksum_naming to kLegacyCrc32cAndFileSize
  // here but even if we don't, it should have no effect when
  // share_files_with_checksum is false
  ASSERT_TRUE(backupable_options_->share_files_with_checksum_naming ==
              kNamingDefault);
  // set share_files_with_checksum to false
  OpenDBAndBackupEngine(true, false, kShareNoChecksum);
  int j = 3;
  for (int i = 0; i < j; ++i) {
    FillDB(db_.get(), keys_iteration * i, keys_iteration * (i + 1));
    ASSERT_OK(backup_engine_->CreateNewBackup(db_.get(), true));
  }
  CloseDBAndBackupEngine();

  for (int i = 0; i < j; ++i) {
    AssertBackupConsistency(i + 1, 0, keys_iteration * (i + 1),
                            keys_iteration * (j + 1));
  }

  // set share_files_with_checksum to true and do some more backups
  // and use session id in the name of SST file backup
  ASSERT_TRUE(backupable_options_->share_files_with_checksum_naming ==
              kNamingDefault);
  OpenDBAndBackupEngine(false /* destroy_old_data */, false,
                        kShareWithChecksum);
  FillDB(db_.get(), keys_iteration * j, keys_iteration * (j + 1));
  ASSERT_OK(backup_engine_->CreateNewBackup(db_.get(), true));
  CloseDBAndBackupEngine();
  // Use checksum in the name as well
  ++j;
  options_.file_checksum_gen_factory = GetFileChecksumGenCrc32cFactory();
  OpenDBAndBackupEngine(false /* destroy_old_data */, false,
                        kShareWithChecksum);
  FillDB(db_.get(), keys_iteration * j, keys_iteration * (j + 1));
  ASSERT_OK(backup_engine_->CreateNewBackup(db_.get(), true));
  CloseDBAndBackupEngine();

  // Verify first (about to delete)
  AssertBackupConsistency(1, 0, keys_iteration, keys_iteration * (j + 1));

  // For an extra challenge, make sure that GarbageCollect / DeleteBackup
  // is OK even if we open without share_table_files but with
  // share_files_with_checksum_naming based on kUseDbSessionId
  ASSERT_TRUE(backupable_options_->share_files_with_checksum_naming ==
              kNamingDefault);
  OpenDBAndBackupEngine(false /* destroy_old_data */, false, kNoShare);
  ASSERT_OK(backup_engine_->DeleteBackup(1));
  ASSERT_OK(backup_engine_->GarbageCollect());
  CloseDBAndBackupEngine();

  // Verify second (about to delete)
  AssertBackupConsistency(2, 0, keys_iteration * 2, keys_iteration * (j + 1));

  // Use checksum and file size for backup table file names and open without
  // share_table_files
  // Again, make sure that GarbageCollect / DeleteBackup is OK
  backupable_options_->share_files_with_checksum_naming =
      kLegacyCrc32cAndFileSize;
  OpenDBAndBackupEngine(false /* destroy_old_data */, false, kNoShare);
  ASSERT_OK(backup_engine_->DeleteBackup(2));
  ASSERT_OK(backup_engine_->GarbageCollect());
  CloseDBAndBackupEngine();

  // Verify rest (not deleted)
  for (int i = 2; i < j; ++i) {
    AssertBackupConsistency(i + 1, 0, keys_iteration * (i + 1),
                            keys_iteration * (j + 1));
  }
}

// Verify backup and restore with share_files_with_checksum on and transition
// from kLegacyCrc32cAndFileSize to kUseDbSessionId
TEST_F(BackupEngineTest, ShareTableFilesWithChecksumsNewNamingUpgrade) {
  backupable_options_->share_files_with_checksum_naming =
      kLegacyCrc32cAndFileSize;
  const int keys_iteration = 5000;
  // set share_files_with_checksum to true
  OpenDBAndBackupEngine(true, false, kShareWithChecksum);
  int j = 3;
  for (int i = 0; i < j; ++i) {
    FillDB(db_.get(), keys_iteration * i, keys_iteration * (i + 1));
    ASSERT_OK(backup_engine_->CreateNewBackup(db_.get(), true));
  }
  CloseDBAndBackupEngine();

  for (int i = 0; i < j; ++i) {
    AssertBackupConsistency(i + 1, 0, keys_iteration * (i + 1),
                            keys_iteration * (j + 1));
  }

  backupable_options_->share_files_with_checksum_naming = kUseDbSessionId;
  OpenDBAndBackupEngine(false /* destroy_old_data */, false,
                        kShareWithChecksum);
  FillDB(db_.get(), keys_iteration * j, keys_iteration * (j + 1));
  ASSERT_OK(backup_engine_->CreateNewBackup(db_.get(), true));
  CloseDBAndBackupEngine();

  ++j;
  options_.file_checksum_gen_factory = GetFileChecksumGenCrc32cFactory();
  OpenDBAndBackupEngine(false /* destroy_old_data */, false,
                        kShareWithChecksum);
  FillDB(db_.get(), keys_iteration * j, keys_iteration * (j + 1));
  ASSERT_OK(backup_engine_->CreateNewBackup(db_.get(), true));
  CloseDBAndBackupEngine();

  // Verify first (about to delete)
  AssertBackupConsistency(1, 0, keys_iteration, keys_iteration * (j + 1));

  // For an extra challenge, make sure that GarbageCollect / DeleteBackup
  // is OK even if we open without share_table_files
  OpenDBAndBackupEngine(false /* destroy_old_data */, false, kNoShare);
  ASSERT_OK(backup_engine_->DeleteBackup(1));
  ASSERT_OK(backup_engine_->GarbageCollect());
  CloseDBAndBackupEngine();

  // Verify second (about to delete)
  AssertBackupConsistency(2, 0, keys_iteration * 2, keys_iteration * (j + 1));

  // Use checksum and file size for backup table file names and open without
  // share_table_files
  // Again, make sure that GarbageCollect / DeleteBackup is OK
  backupable_options_->share_files_with_checksum_naming =
      kLegacyCrc32cAndFileSize;
  OpenDBAndBackupEngine(false /* destroy_old_data */, false, kNoShare);
  ASSERT_OK(backup_engine_->DeleteBackup(2));
  ASSERT_OK(backup_engine_->GarbageCollect());
  CloseDBAndBackupEngine();

  // Verify rest (not deleted)
  for (int i = 2; i < j; ++i) {
    AssertBackupConsistency(i + 1, 0, keys_iteration * (i + 1),
                            keys_iteration * (j + 1));
  }
}

// This test simulates cleaning up after aborted or incomplete creation
// of a new backup.
TEST_F(BackupEngineTest, DeleteTmpFiles) {
  for (int cleanup_fn : {1, 2, 3, 4}) {
    for (ShareOption shared_option : kAllShareOptions) {
      OpenDBAndBackupEngine(false /* destroy_old_data */, false /* dummy */,
                            shared_option);
      ASSERT_OK(backup_engine_->CreateNewBackup(db_.get()));
      BackupID next_id = 1;
      BackupID oldest_id = std::numeric_limits<BackupID>::max();
      {
        std::vector<BackupInfo> backup_info;
        backup_engine_->GetBackupInfo(&backup_info);
        for (const auto& bi : backup_info) {
          next_id = std::max(next_id, bi.backup_id + 1);
          oldest_id = std::min(oldest_id, bi.backup_id);
        }
      }
      CloseDBAndBackupEngine();

      // An aborted or incomplete new backup will always be in the next
      // id (maybe more)
      std::string next_private = "private/" + std::to_string(next_id);

      // NOTE: both shared and shared_checksum should be cleaned up
      // regardless of how the backup engine is opened.
      std::vector<std::string> tmp_files_and_dirs;
      for (const auto& dir_and_file : {
               std::make_pair(std::string("shared"),
                              std::string(".00006.sst.tmp")),
               std::make_pair(std::string("shared_checksum"),
                              std::string(".00007.sst.tmp")),
               std::make_pair(next_private, std::string("00003.sst")),
           }) {
        std::string dir = backupdir_ + "/" + dir_and_file.first;
        ASSERT_OK(file_manager_->CreateDirIfMissing(dir));
        ASSERT_OK(file_manager_->FileExists(dir));

        std::string file = dir + "/" + dir_and_file.second;
        ASSERT_OK(file_manager_->WriteToFile(file, "tmp"));
        ASSERT_OK(file_manager_->FileExists(file));

        tmp_files_and_dirs.push_back(file);
      }
      if (cleanup_fn != /*CreateNewBackup*/ 4) {
        // This exists after CreateNewBackup because it's deleted then
        // re-created.
        tmp_files_and_dirs.push_back(backupdir_ + "/" + next_private);
      }

      OpenDBAndBackupEngine(false /* destroy_old_data */, false /* dummy */,
                            shared_option);
      // Need to call one of these explicitly to delete tmp files
      switch (cleanup_fn) {
        case 1:
          ASSERT_OK(backup_engine_->GarbageCollect());
          break;
        case 2:
          ASSERT_OK(backup_engine_->DeleteBackup(oldest_id));
          break;
        case 3:
          ASSERT_OK(backup_engine_->PurgeOldBackups(1));
          break;
        case 4:
          // Does a garbage collect if it sees that next private dir exists
          ASSERT_OK(backup_engine_->CreateNewBackup(db_.get()));
          break;
        default:
          assert(false);
      }
      CloseDBAndBackupEngine();
      for (std::string file_or_dir : tmp_files_and_dirs) {
        if (file_manager_->FileExists(file_or_dir) != Status::NotFound()) {
          FAIL() << file_or_dir << " was expected to be deleted." << cleanup_fn;
        }
      }
    }
  }
}

TEST_F(BackupEngineTest, KeepLogFiles) {
  backupable_options_->backup_log_files = false;
  // basically infinite
  options_.WAL_ttl_seconds = 24 * 60 * 60;
  OpenDBAndBackupEngine(true);
  FillDB(db_.get(), 0, 100, kFlushAll);
  FillDB(db_.get(), 100, 200, kFlushAll);
  ASSERT_OK(backup_engine_->CreateNewBackup(db_.get(), false));
  FillDB(db_.get(), 200, 300, kFlushAll);
  FillDB(db_.get(), 300, 400, kFlushAll);
  FillDB(db_.get(), 400, 500, kFlushAll);
  CloseDBAndBackupEngine();

  // all data should be there if we call with keep_log_files = true
  AssertBackupConsistency(0, 0, 500, 600, true);
}

#if !defined(ROCKSDB_VALGRIND_RUN) || defined(ROCKSDB_FULL_VALGRIND_RUN)
class BackupEngineRateLimitingTestWithParam
    : public BackupEngineTest,
      public testing::WithParamInterface<
          std::tuple<bool /* make throttle */,
                     int /* 0 = single threaded, 1 = multi threaded*/,
                     std::pair<uint64_t, uint64_t> /* limits */>> {
 public:
  BackupEngineRateLimitingTestWithParam() {}
};

uint64_t const MB = 1024 * 1024;

INSTANTIATE_TEST_CASE_P(
    RateLimiting, BackupEngineRateLimitingTestWithParam,
    ::testing::Values(std::make_tuple(false, 0, std::make_pair(1 * MB, 5 * MB)),
                      std::make_tuple(false, 0, std::make_pair(2 * MB, 3 * MB)),
                      std::make_tuple(false, 1, std::make_pair(1 * MB, 5 * MB)),
                      std::make_tuple(false, 1, std::make_pair(2 * MB, 3 * MB)),
                      std::make_tuple(true, 0, std::make_pair(1 * MB, 5 * MB)),
                      std::make_tuple(true, 0, std::make_pair(2 * MB, 3 * MB)),
                      std::make_tuple(true, 1, std::make_pair(1 * MB, 5 * MB)),
                      std::make_tuple(true, 1,
                                      std::make_pair(2 * MB, 3 * MB))));

TEST_P(BackupEngineRateLimitingTestWithParam, RateLimiting) {
  size_t const kMicrosPerSec = 1000 * 1000LL;

  std::shared_ptr<RateLimiter> backupThrottler(NewGenericRateLimiter(1));
  std::shared_ptr<RateLimiter> restoreThrottler(NewGenericRateLimiter(1));

  bool makeThrottler = std::get<0>(GetParam());
  if (makeThrottler) {
    backupable_options_->backup_rate_limiter = backupThrottler;
    backupable_options_->restore_rate_limiter = restoreThrottler;
  }

  // iter 0 -- single threaded
  // iter 1 -- multi threaded
  int iter = std::get<1>(GetParam());
  const std::pair<uint64_t, uint64_t> limit = std::get<2>(GetParam());

  // destroy old data
  DestroyDB(dbname_, Options());
  if (makeThrottler) {
    backupThrottler->SetBytesPerSecond(limit.first);
    restoreThrottler->SetBytesPerSecond(limit.second);
  } else {
    backupable_options_->backup_rate_limit = limit.first;
    backupable_options_->restore_rate_limit = limit.second;
  }
  backupable_options_->max_background_operations = (iter == 0) ? 1 : 10;
  options_.compression = kNoCompression;
  OpenDBAndBackupEngine(true);
  size_t bytes_written = FillDB(db_.get(), 0, 100000);

  auto start_backup = db_chroot_env_->NowMicros();
  ASSERT_OK(backup_engine_->CreateNewBackup(db_.get(), false));
  auto backup_time = db_chroot_env_->NowMicros() - start_backup;
  auto rate_limited_backup_time = (bytes_written * kMicrosPerSec) / limit.first;
  ASSERT_GT(backup_time, 0.8 * rate_limited_backup_time);

  CloseDBAndBackupEngine();

  OpenBackupEngine();
  auto start_restore = db_chroot_env_->NowMicros();
  ASSERT_OK(backup_engine_->RestoreDBFromLatestBackup(dbname_, dbname_));
  auto restore_time = db_chroot_env_->NowMicros() - start_restore;
  CloseBackupEngine();
  auto rate_limited_restore_time =
      (bytes_written * kMicrosPerSec) / limit.second;
  ASSERT_GT(restore_time, 0.8 * rate_limited_restore_time);

  AssertBackupConsistency(0, 0, 100000, 100010);
}

TEST_P(BackupEngineRateLimitingTestWithParam, RateLimitingVerifyBackup) {
  const std::size_t kMicrosPerSec = 1000 * 1000LL;
  std::shared_ptr<RateLimiter> backupThrottler(NewGenericRateLimiter(
      1, 100 * 1000 /* refill_period_us */, 10 /* fairness */,
      RateLimiter::Mode::kAllIo /* mode */));

  bool makeThrottler = std::get<0>(GetParam());
  if (makeThrottler) {
    backupable_options_->backup_rate_limiter = backupThrottler;
  }

  bool is_single_threaded = std::get<1>(GetParam()) == 0 ? true : false;
  backupable_options_->max_background_operations = is_single_threaded ? 1 : 10;

  const std::uint64_t backup_rate_limiter_limit = std::get<2>(GetParam()).first;
  if (makeThrottler) {
    backupable_options_->backup_rate_limiter->SetBytesPerSecond(
        backup_rate_limiter_limit);
  } else {
    backupable_options_->backup_rate_limit = backup_rate_limiter_limit;
  }

  DestroyDB(dbname_, Options());
  OpenDBAndBackupEngine(true /* destroy_old_data */);
  FillDB(db_.get(), 0, 100000);
  ASSERT_OK(backup_engine_->CreateNewBackup(db_.get(),
                                            false /* flush_before_backup */));

  std::vector<BackupInfo> backup_infos;
  BackupInfo backup_info;
  backup_engine_->GetBackupInfo(&backup_infos);
  ASSERT_EQ(1, backup_infos.size());
  const int backup_id = 1;
  ASSERT_EQ(backup_id, backup_infos[0].backup_id);
  ASSERT_OK(backup_engine_->GetBackupInfo(backup_id, &backup_info,
                                          true /* include_file_details */));

  std::uint64_t bytes_read_during_verify_backup = 0;
  for (BackupFileInfo backup_file_info : backup_info.file_details) {
    bytes_read_during_verify_backup += backup_file_info.size;
  }

  auto start_verify_backup = db_chroot_env_->NowMicros();
  ASSERT_OK(
      backup_engine_->VerifyBackup(backup_id, true /* verify_with_checksum */));
  auto verify_backup_time = db_chroot_env_->NowMicros() - start_verify_backup;
  auto rate_limited_verify_backup_time =
      (bytes_read_during_verify_backup * kMicrosPerSec) /
      backup_rate_limiter_limit;

  if (makeThrottler) {
    EXPECT_GE(verify_backup_time, 0.8 * rate_limited_verify_backup_time);
  }
  CloseDBAndBackupEngine();
  AssertBackupConsistency(backup_id, 0, 100000, 100010);
  DestroyDB(dbname_, Options());
}

TEST_P(BackupEngineRateLimitingTestWithParam, RateLimitingChargeReadInBackup) {
  bool is_single_threaded = std::get<1>(GetParam()) == 0 ? true : false;
  backupable_options_->max_background_operations = is_single_threaded ? 1 : 10;

  const std::uint64_t backup_rate_limiter_limit = std::get<2>(GetParam()).first;
  std::shared_ptr<RateLimiter> backup_rate_limiter(NewGenericRateLimiter(
      backup_rate_limiter_limit, 100 * 1000 /* refill_period_us */,
      10 /* fairness */, RateLimiter::Mode::kWritesOnly /* mode */));
  backupable_options_->backup_rate_limiter = backup_rate_limiter;

  DestroyDB(dbname_, Options());
  OpenDBAndBackupEngine(true /* destroy_old_data */, false /* dummy */,
                        kShareWithChecksum /* shared_option */);
  FillDB(db_.get(), 0, 10);
  ASSERT_OK(backup_engine_->CreateNewBackup(db_.get(),
                                            false /* flush_before_backup */));
  std::int64_t total_bytes_through_with_no_read_charged =
      backup_rate_limiter->GetTotalBytesThrough();
  CloseBackupEngine();

  backup_rate_limiter.reset(NewGenericRateLimiter(
      backup_rate_limiter_limit, 100 * 1000 /* refill_period_us */,
      10 /* fairness */, RateLimiter::Mode::kAllIo /* mode */));
  backupable_options_->backup_rate_limiter = backup_rate_limiter;

  OpenBackupEngine(true);
  ASSERT_OK(backup_engine_->CreateNewBackup(db_.get(),
                                            false /* flush_before_backup */));
  std::int64_t total_bytes_through_with_read_charged =
      backup_rate_limiter->GetTotalBytesThrough();
  EXPECT_GT(total_bytes_through_with_read_charged,
            total_bytes_through_with_no_read_charged);
  CloseDBAndBackupEngine();
  AssertBackupConsistency(1, 0, 10, 20);
  DestroyDB(dbname_, Options());
}

TEST_P(BackupEngineRateLimitingTestWithParam, RateLimitingChargeReadInRestore) {
  bool is_single_threaded = std::get<1>(GetParam()) == 0 ? true : false;
  backupable_options_->max_background_operations = is_single_threaded ? 1 : 10;

  const std::uint64_t restore_rate_limiter_limit =
      std::get<2>(GetParam()).second;
  std::shared_ptr<RateLimiter> restore_rate_limiter(NewGenericRateLimiter(
      restore_rate_limiter_limit, 100 * 1000 /* refill_period_us */,
      10 /* fairness */, RateLimiter::Mode::kWritesOnly /* mode */));
  backupable_options_->restore_rate_limiter = restore_rate_limiter;

  DestroyDB(dbname_, Options());
  OpenDBAndBackupEngine(true /* destroy_old_data */);
  FillDB(db_.get(), 0, 10);
  ASSERT_OK(backup_engine_->CreateNewBackup(db_.get(),
                                            false /* flush_before_backup */));
  CloseDBAndBackupEngine();
  DestroyDB(dbname_, Options());

  OpenBackupEngine(false /* destroy_old_data */);
  ASSERT_OK(backup_engine_->RestoreDBFromLatestBackup(dbname_, dbname_));
  std::int64_t total_bytes_through_with_no_read_charged =
      restore_rate_limiter->GetTotalBytesThrough();
  CloseBackupEngine();
  DestroyDB(dbname_, Options());

  restore_rate_limiter.reset(NewGenericRateLimiter(
      restore_rate_limiter_limit, 100 * 1000 /* refill_period_us */,
      10 /* fairness */, RateLimiter::Mode::kAllIo /* mode */));
  backupable_options_->restore_rate_limiter = restore_rate_limiter;

  OpenBackupEngine(false /* destroy_old_data */);
  ASSERT_OK(backup_engine_->RestoreDBFromLatestBackup(dbname_, dbname_));
  std::int64_t total_bytes_through_with_read_charged =
      restore_rate_limiter->GetTotalBytesThrough();
  EXPECT_EQ(total_bytes_through_with_read_charged,
            total_bytes_through_with_no_read_charged * 2);
  CloseBackupEngine();
  AssertBackupConsistency(1, 0, 10, 20);
  DestroyDB(dbname_, Options());
}

TEST_P(BackupEngineRateLimitingTestWithParam,
       RateLimitingChargeReadInInitialize) {
  bool is_single_threaded = std::get<1>(GetParam()) == 0 ? true : false;
  backupable_options_->max_background_operations = is_single_threaded ? 1 : 10;

  const std::uint64_t backup_rate_limiter_limit = std::get<2>(GetParam()).first;
  std::shared_ptr<RateLimiter> backup_rate_limiter(NewGenericRateLimiter(
      backup_rate_limiter_limit, 100 * 1000 /* refill_period_us */,
      10 /* fairness */, RateLimiter::Mode::kAllIo /* mode */));
  backupable_options_->backup_rate_limiter = backup_rate_limiter;

  DestroyDB(dbname_, Options());
  OpenDBAndBackupEngine(true /* destroy_old_data */);
  FillDB(db_.get(), 0, 10);
  ASSERT_OK(backup_engine_->CreateNewBackup(db_.get(),
                                            false /* flush_before_backup */));
  CloseDBAndBackupEngine();
  AssertBackupConsistency(1, 0, 10, 20);

  std::int64_t total_bytes_through_before_initialize =
      backupable_options_->backup_rate_limiter->GetTotalBytesThrough();
  OpenDBAndBackupEngine(false /* destroy_old_data */);
  // We charge read in BackupEngineImpl::BackupMeta::LoadFromFile,
  // which is called in BackupEngineImpl::Initialize() during
  // OpenBackupEngine(false)
  EXPECT_GT(backupable_options_->backup_rate_limiter->GetTotalBytesThrough(),
            total_bytes_through_before_initialize);
  CloseDBAndBackupEngine();
  DestroyDB(dbname_, Options());
}

class BackupEngineRateLimitingTestWithParam2
    : public BackupEngineTest,
      public testing::WithParamInterface<
          std::tuple<std::pair<uint64_t, uint64_t> /* limits */>> {
 public:
  BackupEngineRateLimitingTestWithParam2() {}
};

INSTANTIATE_TEST_CASE_P(
    LowRefillBytesPerPeriod, BackupEngineRateLimitingTestWithParam2,
    ::testing::Values(std::make_tuple(std::make_pair(1, 1))));
// To verify we don't request over-sized bytes relative to
// refill_bytes_per_period_ in each RateLimiter::Request() called in
// BackupEngine through verifying we don't trigger assertion
// failure on over-sized request in GenericRateLimiter in debug builds
TEST_P(BackupEngineRateLimitingTestWithParam2,
       RateLimitingWithLowRefillBytesPerPeriod) {
  SpecialEnv special_env(Env::Default(), /*time_elapse_only_sleep*/ true);

  backupable_options_->max_background_operations = 1;
  const uint64_t backup_rate_limiter_limit = std::get<0>(GetParam()).first;
  std::shared_ptr<RateLimiter> backup_rate_limiter(
      std::make_shared<GenericRateLimiter>(
          backup_rate_limiter_limit, 1000 * 1000 /* refill_period_us */,
          10 /* fairness */, RateLimiter::Mode::kAllIo /* mode */,
          special_env.GetSystemClock(), false /* auto_tuned */));

  backupable_options_->backup_rate_limiter = backup_rate_limiter;

  const uint64_t restore_rate_limiter_limit = std::get<0>(GetParam()).second;
  std::shared_ptr<RateLimiter> restore_rate_limiter(
      std::make_shared<GenericRateLimiter>(
          restore_rate_limiter_limit, 1000 * 1000 /* refill_period_us */,
          10 /* fairness */, RateLimiter::Mode::kAllIo /* mode */,
          special_env.GetSystemClock(), false /* auto_tuned */));

  backupable_options_->restore_rate_limiter = restore_rate_limiter;

  // Rate limiter uses `CondVar::TimedWait()`, which does not have access to the
  // `Env` to advance its time according to the fake wait duration. The
  // workaround is to install a callback that advance the `Env`'s mock time.
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "GenericRateLimiter::Request:PostTimedWait", [&](void* arg) {
        int64_t time_waited_us = *static_cast<int64_t*>(arg);
        special_env.SleepForMicroseconds(static_cast<int>(time_waited_us));
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  DestroyDB(dbname_, Options());
  OpenDBAndBackupEngine(true /* destroy_old_data */, false /* dummy */,
                        kShareWithChecksum /* shared_option */);

  FillDB(db_.get(), 0, 100);
  int64_t total_bytes_through_before_backup =
      backupable_options_->backup_rate_limiter->GetTotalBytesThrough();
  EXPECT_OK(backup_engine_->CreateNewBackup(db_.get(),
                                            false /* flush_before_backup */));
  int64_t total_bytes_through_after_backup =
      backupable_options_->backup_rate_limiter->GetTotalBytesThrough();
  ASSERT_GT(total_bytes_through_after_backup,
            total_bytes_through_before_backup);

  std::vector<BackupInfo> backup_infos;
  BackupInfo backup_info;
  backup_engine_->GetBackupInfo(&backup_infos);
  ASSERT_EQ(1, backup_infos.size());
  const int backup_id = 1;
  ASSERT_EQ(backup_id, backup_infos[0].backup_id);
  ASSERT_OK(backup_engine_->GetBackupInfo(backup_id, &backup_info,
                                          true /* include_file_details */));
  int64_t total_bytes_through_before_verify_backup =
      backupable_options_->backup_rate_limiter->GetTotalBytesThrough();
  EXPECT_OK(
      backup_engine_->VerifyBackup(backup_id, true /* verify_with_checksum */));
  int64_t total_bytes_through_after_verify_backup =
      backupable_options_->backup_rate_limiter->GetTotalBytesThrough();
  ASSERT_GT(total_bytes_through_after_verify_backup,
            total_bytes_through_before_verify_backup);

  CloseDBAndBackupEngine();
  AssertBackupConsistency(backup_id, 0, 100, 101);

  int64_t total_bytes_through_before_initialize =
      backupable_options_->backup_rate_limiter->GetTotalBytesThrough();
  OpenDBAndBackupEngine(false /* destroy_old_data */);
  // We charge read in BackupEngineImpl::BackupMeta::LoadFromFile,
  // which is called in BackupEngineImpl::Initialize() during
  // OpenBackupEngine(false)
  int64_t total_bytes_through_after_initialize =
      backupable_options_->backup_rate_limiter->GetTotalBytesThrough();
  ASSERT_GT(total_bytes_through_after_initialize,
            total_bytes_through_before_initialize);
  CloseDBAndBackupEngine();

  DestroyDB(dbname_, Options());
  OpenBackupEngine(false /* destroy_old_data */);
  int64_t total_bytes_through_before_restore =
      backupable_options_->restore_rate_limiter->GetTotalBytesThrough();
  EXPECT_OK(backup_engine_->RestoreDBFromLatestBackup(dbname_, dbname_));
  int64_t total_bytes_through_after_restore =
      backupable_options_->restore_rate_limiter->GetTotalBytesThrough();
  ASSERT_GT(total_bytes_through_after_restore,
            total_bytes_through_before_restore);
  CloseBackupEngine();

  DestroyDB(dbname_, Options());

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearCallBack(
      "GenericRateLimiter::Request:PostTimedWait");
}

#endif  // !defined(ROCKSDB_VALGRIND_RUN) || defined(ROCKSDB_FULL_VALGRIND_RUN)

TEST_F(BackupEngineTest, ReadOnlyBackupEngine) {
  DestroyDB(dbname_, options_);
  OpenDBAndBackupEngine(true);
  FillDB(db_.get(), 0, 100);
  ASSERT_OK(backup_engine_->CreateNewBackup(db_.get(), true));
  FillDB(db_.get(), 100, 200);
  ASSERT_OK(backup_engine_->CreateNewBackup(db_.get(), true));
  CloseDBAndBackupEngine();
  DestroyDB(dbname_, options_);

  backupable_options_->destroy_old_data = false;
  test_backup_env_->ClearWrittenFiles();
  test_backup_env_->SetLimitDeleteFiles(0);
  BackupEngineReadOnly* read_only_backup_engine;
  ASSERT_OK(BackupEngineReadOnly::Open(
      db_chroot_env_.get(), *backupable_options_, &read_only_backup_engine));
  std::vector<BackupInfo> backup_info;
  read_only_backup_engine->GetBackupInfo(&backup_info);
  ASSERT_EQ(backup_info.size(), 2U);

  RestoreOptions restore_options(false);
  ASSERT_OK(read_only_backup_engine->RestoreDBFromLatestBackup(
      dbname_, dbname_, restore_options));
  delete read_only_backup_engine;
  std::vector<std::string> should_have_written;
  test_backup_env_->AssertWrittenFiles(should_have_written);

  DB* db = OpenDB();
  AssertExists(db, 0, 200);
  delete db;
}

TEST_F(BackupEngineTest, OpenBackupAsReadOnlyDB) {
  DestroyDB(dbname_, options_);
  options_.write_dbid_to_manifest = false;

  OpenDBAndBackupEngine(true);
  FillDB(db_.get(), 0, 100);
  ASSERT_OK(backup_engine_->CreateNewBackup(db_.get(), /*flush*/ false));

  options_.write_dbid_to_manifest = true;  // exercises some read-only DB code
  CloseAndReopenDB();

  FillDB(db_.get(), 100, 200);
  ASSERT_OK(backup_engine_->CreateNewBackup(db_.get(), /*flush*/ false));
  db_.reset();  // CloseDB
  DestroyDB(dbname_, options_);
  BackupInfo backup_info;
  // First, check that we get empty fields without include_file_details
  ASSERT_OK(backup_engine_->GetBackupInfo(/*id*/ 1U, &backup_info,
                                          /*with file details*/ false));
  ASSERT_EQ(backup_info.name_for_open, "");
  ASSERT_FALSE(backup_info.env_for_open);

  // Now for the real test
  backup_info = BackupInfo();
  ASSERT_OK(backup_engine_->GetBackupInfo(/*id*/ 1U, &backup_info,
                                          /*with file details*/ true));

  // Caution: DBOptions only holds a raw pointer to Env, so something else
  // must keep it alive.
  // Case 1: Keeping BackupEngine open suffices to keep Env alive
  DB* db = nullptr;
  Options opts = options_;
  // Ensure some key defaults are set
  opts.wal_dir = "";
  opts.create_if_missing = false;
  opts.info_log.reset();

  opts.env = backup_info.env_for_open.get();
  std::string name = backup_info.name_for_open;
  backup_info = BackupInfo();
  ASSERT_OK(DB::OpenForReadOnly(opts, name, &db));

  AssertExists(db, 0, 100);
  AssertEmpty(db, 100, 200);

  delete db;
  db = nullptr;

  // Case 2: Keeping BackupInfo alive rather than BackupEngine also suffices
  ASSERT_OK(backup_engine_->GetBackupInfo(/*id*/ 2U, &backup_info,
                                          /*with file details*/ true));
  CloseBackupEngine();
  opts.create_if_missing = true;  // check also OK (though pointless)
  opts.env = backup_info.env_for_open.get();
  name = backup_info.name_for_open;
  // Note: keeping backup_info alive
  ASSERT_OK(DB::OpenForReadOnly(opts, name, &db));

  AssertExists(db, 0, 200);
  delete db;
  db = nullptr;

  // Now try opening read-write and make sure it fails, for safety.
  ASSERT_TRUE(DB::Open(opts, name, &db).IsIOError());
}

TEST_F(BackupEngineTest, ProgressCallbackDuringBackup) {
  DestroyDB(dbname_, options_);
  // Too big for this small DB
  backupable_options_->callback_trigger_interval_size = 100000;
  OpenDBAndBackupEngine(true);
  FillDB(db_.get(), 0, 100);
  bool is_callback_invoked = false;
  ASSERT_OK(backup_engine_->CreateNewBackup(
      db_.get(), true,
      [&is_callback_invoked]() { is_callback_invoked = true; }));
  ASSERT_FALSE(is_callback_invoked);
  CloseBackupEngine();

  // Easily small enough for this small DB
  backupable_options_->callback_trigger_interval_size = 1000;
  OpenBackupEngine();
  ASSERT_OK(backup_engine_->CreateNewBackup(
      db_.get(), true,
      [&is_callback_invoked]() { is_callback_invoked = true; }));
  ASSERT_TRUE(is_callback_invoked);
  CloseDBAndBackupEngine();
  DestroyDB(dbname_, options_);
}

TEST_F(BackupEngineTest, GarbageCollectionBeforeBackup) {
  DestroyDB(dbname_, options_);
  OpenDBAndBackupEngine(true);

  ASSERT_OK(backup_chroot_env_->CreateDirIfMissing(backupdir_ + "/shared"));
  std::string file_five = backupdir_ + "/shared/000009.sst";
  std::string file_five_contents = "I'm not really a sst file";
  // this depends on the fact that 00009.sst is the first file created by the DB
  ASSERT_OK(file_manager_->WriteToFile(file_five, file_five_contents));

  FillDB(db_.get(), 0, 100);
  // backup overwrites file 000009.sst
  ASSERT_OK(backup_engine_->CreateNewBackup(db_.get(), true));

  std::string new_file_five_contents;
  ASSERT_OK(ReadFileToString(backup_chroot_env_.get(), file_five,
                             &new_file_five_contents));
  // file 000009.sst was overwritten
  ASSERT_TRUE(new_file_five_contents != file_five_contents);

  CloseDBAndBackupEngine();

  AssertBackupConsistency(0, 0, 100);
}

// Test that we properly propagate Env failures
TEST_F(BackupEngineTest, EnvFailures) {
  BackupEngine* backup_engine;

  // get children failure
  {
    test_backup_env_->SetGetChildrenFailure(true);
    ASSERT_NOK(BackupEngine::Open(test_db_env_.get(), *backupable_options_,
                                  &backup_engine));
    test_backup_env_->SetGetChildrenFailure(false);
  }

  // created dir failure
  {
    test_backup_env_->SetCreateDirIfMissingFailure(true);
    ASSERT_NOK(BackupEngine::Open(test_db_env_.get(), *backupable_options_,
                                  &backup_engine));
    test_backup_env_->SetCreateDirIfMissingFailure(false);
  }

  // new directory failure
  {
    test_backup_env_->SetNewDirectoryFailure(true);
    ASSERT_NOK(BackupEngine::Open(test_db_env_.get(), *backupable_options_,
                                  &backup_engine));
    test_backup_env_->SetNewDirectoryFailure(false);
  }

  // Read from meta-file failure
  {
    DestroyDB(dbname_, options_);
    OpenDBAndBackupEngine(true);
    FillDB(db_.get(), 0, 100);
    ASSERT_OK(backup_engine_->CreateNewBackup(db_.get(), true));
    CloseDBAndBackupEngine();
    test_backup_env_->SetDummySequentialFile(true);
    test_backup_env_->SetDummySequentialFileFailReads(true);
    backupable_options_->destroy_old_data = false;
    ASSERT_NOK(BackupEngine::Open(test_db_env_.get(), *backupable_options_,
                                  &backup_engine));
    test_backup_env_->SetDummySequentialFile(false);
    test_backup_env_->SetDummySequentialFileFailReads(false);
  }

  // no failure
  {
    ASSERT_OK(BackupEngine::Open(test_db_env_.get(), *backupable_options_,
                                 &backup_engine));
    delete backup_engine;
  }
}

// Verify manifest can roll while a backup is being created with the old
// manifest.
TEST_F(BackupEngineTest, ChangeManifestDuringBackupCreation) {
  DestroyDB(dbname_, options_);
  options_.max_manifest_file_size = 0;  // always rollover manifest for file add
  OpenDBAndBackupEngine(true);
  FillDB(db_.get(), 0, 100, kAutoFlushOnly);

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency({
      {"CheckpointImpl::CreateCheckpoint:SavedLiveFiles1",
       "VersionSet::LogAndApply:WriteManifest"},
      {"VersionSet::LogAndApply:WriteManifestDone",
       "CheckpointImpl::CreateCheckpoint:SavedLiveFiles2"},
  });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  ROCKSDB_NAMESPACE::port::Thread flush_thread{
      [this]() { ASSERT_OK(db_->Flush(FlushOptions())); }};

  ASSERT_OK(backup_engine_->CreateNewBackup(db_.get(), false));

  flush_thread.join();
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();

  // The last manifest roll would've already been cleaned up by the full scan
  // that happens when CreateNewBackup invokes EnableFileDeletions. We need to
  // trigger another roll to verify non-full scan purges stale manifests.
  DBImpl* db_impl = static_cast_with_check<DBImpl>(db_.get());
  std::string prev_manifest_path =
      DescriptorFileName(dbname_, db_impl->TEST_Current_Manifest_FileNo());
  FillDB(db_.get(), 0, 100, kAutoFlushOnly);
  ASSERT_OK(db_chroot_env_->FileExists(prev_manifest_path));
  ASSERT_OK(db_->Flush(FlushOptions()));
  // Even though manual flush completed above, the background thread may not
  // have finished its cleanup work. `TEST_WaitForBackgroundWork()` will wait
  // until all the background thread's work has completed, including cleanup.
  ASSERT_OK(db_impl->TEST_WaitForBackgroundWork());
  ASSERT_TRUE(db_chroot_env_->FileExists(prev_manifest_path).IsNotFound());

  CloseDBAndBackupEngine();
  DestroyDB(dbname_, options_);
  AssertBackupConsistency(0, 0, 100);
}

// see https://github.com/facebook/rocksdb/issues/921
TEST_F(BackupEngineTest, Issue921Test) {
  BackupEngine* backup_engine;
  backupable_options_->share_table_files = false;
  ASSERT_OK(
      backup_chroot_env_->CreateDirIfMissing(backupable_options_->backup_dir));
  backupable_options_->backup_dir += "/new_dir";
  ASSERT_OK(BackupEngine::Open(backup_chroot_env_.get(), *backupable_options_,
                               &backup_engine));

  delete backup_engine;
}

TEST_F(BackupEngineTest, BackupWithMetadata) {
  const int keys_iteration = 5000;
  OpenDBAndBackupEngine(true);
  // create five backups
  for (int i = 0; i < 5; ++i) {
    const std::string metadata = std::to_string(i);
    FillDB(db_.get(), keys_iteration * i, keys_iteration * (i + 1));
    // Here also test CreateNewBackupWithMetadata with CreateBackupOptions
    // and outputting saved BackupID.
    CreateBackupOptions opts;
    opts.flush_before_backup = true;
    BackupID new_id = 0;
    ASSERT_OK(backup_engine_->CreateNewBackupWithMetadata(opts, db_.get(),
                                                          metadata, &new_id));
    ASSERT_EQ(new_id, static_cast<BackupID>(i + 1));
  }
  CloseDBAndBackupEngine();

  OpenDBAndBackupEngine();
  {  // Verify in bulk BackupInfo
    std::vector<BackupInfo> backup_infos;
    backup_engine_->GetBackupInfo(&backup_infos);
    ASSERT_EQ(5, backup_infos.size());
    for (int i = 0; i < 5; i++) {
      ASSERT_EQ(std::to_string(i), backup_infos[i].app_metadata);
    }
  }
  // Also verify in individual BackupInfo
  for (int i = 0; i < 5; i++) {
    BackupInfo backup_info;
    ASSERT_OK(backup_engine_->GetBackupInfo(static_cast<BackupID>(i + 1),
                                            &backup_info));
    ASSERT_EQ(std::to_string(i), backup_info.app_metadata);
  }
  CloseDBAndBackupEngine();
  DestroyDB(dbname_, options_);
}

TEST_F(BackupEngineTest, BinaryMetadata) {
  OpenDBAndBackupEngine(true);
  std::string binaryMetadata = "abc\ndef";
  binaryMetadata.push_back('\0');
  binaryMetadata.append("ghi");
  ASSERT_OK(
      backup_engine_->CreateNewBackupWithMetadata(db_.get(), binaryMetadata));
  CloseDBAndBackupEngine();

  OpenDBAndBackupEngine();
  std::vector<BackupInfo> backup_infos;
  backup_engine_->GetBackupInfo(&backup_infos);
  ASSERT_EQ(1, backup_infos.size());
  ASSERT_EQ(binaryMetadata, backup_infos[0].app_metadata);
  CloseDBAndBackupEngine();
  DestroyDB(dbname_, options_);
}

TEST_F(BackupEngineTest, MetadataTooLarge) {
  OpenDBAndBackupEngine(true);
  std::string largeMetadata(1024 * 1024 + 1, 0);
  ASSERT_NOK(
      backup_engine_->CreateNewBackupWithMetadata(db_.get(), largeMetadata));
  CloseDBAndBackupEngine();
  DestroyDB(dbname_, options_);
}

TEST_F(BackupEngineTest, FutureMetaSchemaVersion2_SizeCorruption) {
  OpenDBAndBackupEngine(true);

  // Backup 1: no future schema, no sizes, with checksums
  ASSERT_OK(backup_engine_->CreateNewBackup(db_.get()));

  // Backup 2: no checksums, no sizes
  TEST_FutureSchemaVersion2Options test_opts;
  test_opts.crc32c_checksums = false;
  test_opts.file_sizes = false;
  TEST_EnableWriteFutureSchemaVersion2(backup_engine_.get(), test_opts);
  ASSERT_OK(backup_engine_->CreateNewBackup(db_.get()));

  // Backup 3: no checksums, with sizes
  test_opts.file_sizes = true;
  TEST_EnableWriteFutureSchemaVersion2(backup_engine_.get(), test_opts);
  ASSERT_OK(backup_engine_->CreateNewBackup(db_.get()));

  // Backup 4: with checksums and sizes
  test_opts.crc32c_checksums = true;
  TEST_EnableWriteFutureSchemaVersion2(backup_engine_.get(), test_opts);
  ASSERT_OK(backup_engine_->CreateNewBackup(db_.get()));

  CloseDBAndBackupEngine();

  // Corrupt all the CURRENT files with the wrong size
  const std::string private_dir = backupdir_ + "/private";

  for (int id = 1; id <= 3; ++id) {
    ASSERT_OK(file_manager_->WriteToFile(
        private_dir + "/" + ToString(id) + "/CURRENT", "x"));
  }
  // Except corrupt Backup 4 with same size CURRENT file
  {
    uint64_t size = 0;
    ASSERT_OK(test_backup_env_->GetFileSize(private_dir + "/4/CURRENT", &size));
    ASSERT_OK(file_manager_->WriteToFile(private_dir + "/4/CURRENT",
                                         std::string(size, 'x')));
  }

  OpenBackupEngine();

  // Only the one with sizes in metadata will be immediately detected
  // as corrupt
  std::vector<BackupID> corrupted;
  backup_engine_->GetCorruptedBackups(&corrupted);
  ASSERT_EQ(corrupted.size(), 1);
  ASSERT_EQ(corrupted[0], 3);

  // Size corruption detected on Restore with checksum
  ASSERT_TRUE(backup_engine_->RestoreDBFromBackup(1 /*id*/, dbname_, dbname_)
                  .IsCorruption());

  // Size corruption not detected without checksums nor sizes
  ASSERT_OK(backup_engine_->RestoreDBFromBackup(2 /*id*/, dbname_, dbname_));

  // Non-size corruption detected on Restore with checksum
  ASSERT_TRUE(backup_engine_->RestoreDBFromBackup(4 /*id*/, dbname_, dbname_)
                  .IsCorruption());

  CloseBackupEngine();
}

TEST_F(BackupEngineTest, FutureMetaSchemaVersion2_NotSupported) {
  TEST_FutureSchemaVersion2Options test_opts;
  std::string app_metadata = "abc\ndef";

  OpenDBAndBackupEngine(true);
  // Start with supported
  TEST_EnableWriteFutureSchemaVersion2(backup_engine_.get(), test_opts);
  ASSERT_OK(
      backup_engine_->CreateNewBackupWithMetadata(db_.get(), app_metadata));

  // Because we are injecting badness with a TEST API, the badness is only
  // detected on attempt to restore.
  // Not supported versions
  test_opts.version = "3";
  TEST_EnableWriteFutureSchemaVersion2(backup_engine_.get(), test_opts);
  ASSERT_OK(
      backup_engine_->CreateNewBackupWithMetadata(db_.get(), app_metadata));
  test_opts.version = "23.45.67";
  TEST_EnableWriteFutureSchemaVersion2(backup_engine_.get(), test_opts);
  ASSERT_OK(
      backup_engine_->CreateNewBackupWithMetadata(db_.get(), app_metadata));
  test_opts.version = "2";

  // Non-ignorable fields
  test_opts.meta_fields["ni::blah"] = "123";
  TEST_EnableWriteFutureSchemaVersion2(backup_engine_.get(), test_opts);
  ASSERT_OK(
      backup_engine_->CreateNewBackupWithMetadata(db_.get(), app_metadata));
  test_opts.meta_fields.clear();

  test_opts.file_fields["ni::123"] = "xyz";
  TEST_EnableWriteFutureSchemaVersion2(backup_engine_.get(), test_opts);
  ASSERT_OK(
      backup_engine_->CreateNewBackupWithMetadata(db_.get(), app_metadata));
  test_opts.file_fields.clear();

  test_opts.footer_fields["ni::123"] = "xyz";
  TEST_EnableWriteFutureSchemaVersion2(backup_engine_.get(), test_opts);
  ASSERT_OK(
      backup_engine_->CreateNewBackupWithMetadata(db_.get(), app_metadata));
  test_opts.footer_fields.clear();
  CloseDBAndBackupEngine();

  OpenBackupEngine();
  std::vector<BackupID> corrupted;
  backup_engine_->GetCorruptedBackups(&corrupted);
  ASSERT_EQ(corrupted.size(), 5);

  ASSERT_OK(backup_engine_->RestoreDBFromLatestBackup(dbname_, dbname_));
  CloseBackupEngine();
}

TEST_F(BackupEngineTest, FutureMetaSchemaVersion2_Restore) {
  TEST_FutureSchemaVersion2Options test_opts;
  const int keys_iteration = 5000;

  OpenDBAndBackupEngine(true, false, kShareWithChecksum);
  FillDB(db_.get(), 0, keys_iteration);
  // Start with minimum metadata to ensure it works without it being filled
  // based on shared files also in other backups with the metadata.
  test_opts.crc32c_checksums = false;
  test_opts.file_sizes = false;
  TEST_EnableWriteFutureSchemaVersion2(backup_engine_.get(), test_opts);
  ASSERT_OK(backup_engine_->CreateNewBackup(db_.get(), true));
  CloseDBAndBackupEngine();

  AssertBackupConsistency(1 /* id */, 0, keys_iteration, keys_iteration * 2);

  OpenDBAndBackupEngine(false /* destroy_old_data */, false,
                        kShareWithChecksum);
  test_opts.file_sizes = true;
  TEST_EnableWriteFutureSchemaVersion2(backup_engine_.get(), test_opts);
  ASSERT_OK(backup_engine_->CreateNewBackup(db_.get(), true));
  CloseDBAndBackupEngine();

  for (int id = 1; id <= 2; ++id) {
    AssertBackupConsistency(id, 0, keys_iteration, keys_iteration * 2);
  }

  OpenDBAndBackupEngine(false /* destroy_old_data */, false,
                        kShareWithChecksum);
  test_opts.crc32c_checksums = true;
  TEST_EnableWriteFutureSchemaVersion2(backup_engine_.get(), test_opts);
  ASSERT_OK(backup_engine_->CreateNewBackup(db_.get(), true));
  CloseDBAndBackupEngine();

  for (int id = 1; id <= 3; ++id) {
    AssertBackupConsistency(id, 0, keys_iteration, keys_iteration * 2);
  }

  OpenDBAndBackupEngine(false /* destroy_old_data */, false,
                        kShareWithChecksum);
  // No TEST_EnableWriteFutureSchemaVersion2
  ASSERT_OK(backup_engine_->CreateNewBackup(db_.get(), true));
  CloseDBAndBackupEngine();

  for (int id = 1; id <= 4; ++id) {
    AssertBackupConsistency(id, 0, keys_iteration, keys_iteration * 2);
  }

  OpenDBAndBackupEngine(false /* destroy_old_data */, false,
                        kShareWithChecksum);
  // Minor version updates should be forward-compatible
  test_opts.version = "2.5.70";
  test_opts.meta_fields["asdf.3456"] = "-42";
  test_opts.meta_fields["__QRST"] = " 1 $ %%& ";
  test_opts.file_fields["z94._"] = "^\\";
  test_opts.file_fields["_7yyyyyyyyy"] = "111111111111";
  test_opts.footer_fields["Qwzn.tz89"] = "ASDF!!@# ##=\t ";
  test_opts.footer_fields["yes"] = "no!";
  TEST_EnableWriteFutureSchemaVersion2(backup_engine_.get(), test_opts);
  ASSERT_OK(backup_engine_->CreateNewBackup(db_.get(), true));
  CloseDBAndBackupEngine();

  for (int id = 1; id <= 5; ++id) {
    AssertBackupConsistency(id, 0, keys_iteration, keys_iteration * 2);
  }
}

TEST_F(BackupEngineTest, Concurrency) {
  // Check that we can simultaneously:
  // * Run several read operations in different threads on a single
  // BackupEngine object, and
  // * With another BackupEngine object on the same
  // backup_dir, run the same read operations in another thread, and
  // * With yet another BackupEngine object on the same
  // backup_dir, create two new backups in parallel threads.
  //
  // Because of the challenges of integrating this into db_stress,
  // this is a non-deterministic mini-stress test here instead.

  // To check for a race condition in handling buffer size based on byte
  // burst limit, we need a (generous) rate limiter
  std::shared_ptr<RateLimiter> limiter{NewGenericRateLimiter(1000000000)};
  backupable_options_->backup_rate_limiter = limiter;
  backupable_options_->restore_rate_limiter = limiter;

  OpenDBAndBackupEngine(true, false, kShareWithChecksum);

  static constexpr int keys_iteration = 5000;
  FillDB(db_.get(), 0, keys_iteration);
  ASSERT_OK(backup_engine_->CreateNewBackup(db_.get()));

  FillDB(db_.get(), keys_iteration, 2 * keys_iteration);
  ASSERT_OK(backup_engine_->CreateNewBackup(db_.get()));

  static constexpr int max_factor = 3;
  FillDB(db_.get(), 2 * keys_iteration, max_factor * keys_iteration);
  // will create another backup soon...

  Options db_opts = options_;
  db_opts.wal_dir = "";
  db_opts.create_if_missing = false;
  BackupEngineOptions be_opts = *backupable_options_;
  be_opts.destroy_old_data = false;

  std::mt19937 rng{std::random_device()()};

  std::array<std::thread, 4> read_threads;
  std::array<std::thread, 4> restore_verify_threads;
  for (uint32_t i = 0; i < read_threads.size(); ++i) {
    uint32_t sleep_micros = rng() % 100000;
    read_threads[i] =
        std::thread([this, i, sleep_micros, &db_opts, &be_opts,
                     &restore_verify_threads, &limiter] {
          test_db_env_->SleepForMicroseconds(sleep_micros);

          // Whether to also re-open the BackupEngine, potentially seeing
          // additional backups
          bool reopen = i == 3;
          // Whether we are going to restore "latest"
          bool latest = i > 1;

          BackupEngine* my_be;
          if (reopen) {
            ASSERT_OK(BackupEngine::Open(test_db_env_.get(), be_opts, &my_be));
          } else {
            my_be = backup_engine_.get();
          }

          // Verify metadata (we don't receive updates from concurrently
          // creating a new backup)
          std::vector<BackupInfo> infos;
          my_be->GetBackupInfo(&infos);
          const uint32_t count = static_cast<uint32_t>(infos.size());
          infos.clear();
          if (reopen) {
            ASSERT_GE(count, 2U);
            ASSERT_LE(count, 4U);
            fprintf(stderr, "Reopen saw %u backups\n", count);
          } else {
            ASSERT_EQ(count, 2U);
          }
          std::vector<BackupID> ids;
          my_be->GetCorruptedBackups(&ids);
          ASSERT_EQ(ids.size(), 0U);

          // (Eventually, see below) Restore one of the backups, or "latest"
          std::string restore_db_dir = dbname_ + "/restore" + ToString(i);
          DestroyDir(test_db_env_.get(), restore_db_dir).PermitUncheckedError();
          BackupID to_restore;
          if (latest) {
            to_restore = count;
          } else {
            to_restore = i + 1;
          }

          // Open restored DB to verify its contents, but test atomic restore
          // by doing it async and ensuring we either get OK or InvalidArgument
          restore_verify_threads[i] =
              std::thread([this, &db_opts, restore_db_dir, to_restore] {
                DB* restored;
                Status s;
                for (;;) {
                  s = DB::Open(db_opts, restore_db_dir, &restored);
                  if (s.IsInvalidArgument()) {
                    // Restore hasn't finished
                    test_db_env_->SleepForMicroseconds(1000);
                    continue;
                  } else {
                    // We should only get InvalidArgument if restore is
                    // incomplete, or OK if complete
                    ASSERT_OK(s);
                    break;
                  }
                }
                int factor = std::min(static_cast<int>(to_restore), max_factor);
                AssertExists(restored, 0, factor * keys_iteration);
                AssertEmpty(restored, factor * keys_iteration,
                            (factor + 1) * keys_iteration);
                delete restored;
              });

          // (Ok now) Restore one of the backups, or "latest"
          if (latest) {
            ASSERT_OK(my_be->RestoreDBFromLatestBackup(restore_db_dir,
                                                       restore_db_dir));
          } else {
            ASSERT_OK(my_be->VerifyBackup(to_restore, true));
            ASSERT_OK(my_be->RestoreDBFromBackup(to_restore, restore_db_dir,
                                                 restore_db_dir));
          }

          // Test for race condition in reconfiguring limiter
          // FIXME: this could set to a different value in all threads, except
          // GenericRateLimiter::SetBytesPerSecond has a write-write race
          // reported by TSAN
          if (i == 0) {
            limiter->SetBytesPerSecond(2000000000);
          }

          // Re-verify metadata (we don't receive updates from concurrently
          // creating a new backup)
          my_be->GetBackupInfo(&infos);
          ASSERT_EQ(infos.size(), count);
          my_be->GetCorruptedBackups(&ids);
          ASSERT_EQ(ids.size(), 0);
          // fprintf(stderr, "Finished read thread\n");

          if (reopen) {
            delete my_be;
          }
        });
  }

  BackupEngine* alt_be;
  ASSERT_OK(BackupEngine::Open(test_db_env_.get(), be_opts, &alt_be));

  std::array<std::thread, 2> append_threads;
  for (unsigned i = 0; i < append_threads.size(); ++i) {
    uint32_t sleep_micros = rng() % 100000;
    append_threads[i] = std::thread([this, sleep_micros, alt_be] {
      test_db_env_->SleepForMicroseconds(sleep_micros);
      // WART: CreateNewBackup doesn't tell you the BackupID it just created,
      // which is ugly for multithreaded setting.
      // TODO: add delete backup also when that is added
      ASSERT_OK(alt_be->CreateNewBackup(db_.get()));
      // fprintf(stderr, "Finished append thread\n");
    });
  }

  for (auto& t : append_threads) {
    t.join();
  }
  // Verify metadata
  std::vector<BackupInfo> infos;
  alt_be->GetBackupInfo(&infos);
  ASSERT_EQ(infos.size(), 2 + append_threads.size());

  for (auto& t : read_threads) {
    t.join();
  }

  delete alt_be;

  for (auto& t : restore_verify_threads) {
    t.join();
  }

  CloseDBAndBackupEngine();
}

TEST_F(BackupEngineTest, LimitBackupsOpened) {
  // Verify the specified max backups are opened, including skipping over
  // corrupted backups.
  //
  // Setup:
  // - backups 1, 2, and 4 are valid
  // - backup 3 is corrupt
  // - max_valid_backups_to_open == 2
  //
  // Expectation: the engine opens backups 4 and 2 since those are latest two
  // non-corrupt backups.
  const int kNumKeys = 5000;
  OpenDBAndBackupEngine(true);
  for (int i = 1; i <= 4; ++i) {
    FillDB(db_.get(), kNumKeys * i, kNumKeys * (i + 1));
    ASSERT_OK(backup_engine_->CreateNewBackup(db_.get(), true));
    if (i == 3) {
      ASSERT_OK(file_manager_->CorruptFile(backupdir_ + "/meta/3", 3));
    }
  }
  CloseDBAndBackupEngine();

  backupable_options_->max_valid_backups_to_open = 2;
  backupable_options_->destroy_old_data = false;
  BackupEngineReadOnly* read_only_backup_engine;
  ASSERT_OK(BackupEngineReadOnly::Open(backup_chroot_env_.get(),
                                       *backupable_options_,
                                       &read_only_backup_engine));

  std::vector<BackupInfo> backup_infos;
  read_only_backup_engine->GetBackupInfo(&backup_infos);
  ASSERT_EQ(2, backup_infos.size());
  ASSERT_EQ(2, backup_infos[0].backup_id);
  ASSERT_EQ(4, backup_infos[1].backup_id);
  delete read_only_backup_engine;
}

TEST_F(BackupEngineTest, IgnoreLimitBackupsOpenedWhenNotReadOnly) {
  // Verify the specified max_valid_backups_to_open is ignored if the engine
  // is not read-only.
  //
  // Setup:
  // - backups 1, 2, and 4 are valid
  // - backup 3 is corrupt
  // - max_valid_backups_to_open == 2
  //
  // Expectation: the engine opens backups 4, 2, and 1 since those are latest
  // non-corrupt backups, by ignoring max_valid_backups_to_open == 2.
  const int kNumKeys = 5000;
  OpenDBAndBackupEngine(true);
  for (int i = 1; i <= 4; ++i) {
    FillDB(db_.get(), kNumKeys * i, kNumKeys * (i + 1));
    ASSERT_OK(backup_engine_->CreateNewBackup(db_.get(), true));
    if (i == 3) {
      ASSERT_OK(file_manager_->CorruptFile(backupdir_ + "/meta/3", 3));
    }
  }
  CloseDBAndBackupEngine();

  backupable_options_->max_valid_backups_to_open = 2;
  OpenDBAndBackupEngine();
  std::vector<BackupInfo> backup_infos;
  backup_engine_->GetBackupInfo(&backup_infos);
  ASSERT_EQ(3, backup_infos.size());
  ASSERT_EQ(1, backup_infos[0].backup_id);
  ASSERT_EQ(2, backup_infos[1].backup_id);
  ASSERT_EQ(4, backup_infos[2].backup_id);
  CloseDBAndBackupEngine();
  DestroyDB(dbname_, options_);
}

TEST_F(BackupEngineTest, CreateWhenLatestBackupCorrupted) {
  // we should pick an ID greater than corrupted backups' IDs so creation can
  // succeed even when latest backup is corrupted.
  const int kNumKeys = 5000;
  OpenDBAndBackupEngine(true /* destroy_old_data */);
  BackupInfo backup_info;
  ASSERT_TRUE(backup_engine_->GetLatestBackupInfo(&backup_info).IsNotFound());
  FillDB(db_.get(), 0 /* from */, kNumKeys);
  ASSERT_OK(backup_engine_->CreateNewBackup(db_.get(),
                                            true /* flush_before_backup */));
  ASSERT_OK(file_manager_->CorruptFile(backupdir_ + "/meta/1",
                                       3 /* bytes_to_corrupt */));
  CloseDBAndBackupEngine();

  OpenDBAndBackupEngine();
  ASSERT_TRUE(backup_engine_->GetLatestBackupInfo(&backup_info).IsNotFound());

  ASSERT_OK(backup_engine_->CreateNewBackup(db_.get(),
                                            true /* flush_before_backup */));

  ASSERT_TRUE(backup_engine_->GetLatestBackupInfo(&backup_info).ok());
  ASSERT_EQ(2, backup_info.backup_id);

  std::vector<BackupInfo> backup_infos;
  backup_engine_->GetBackupInfo(&backup_infos);
  ASSERT_EQ(1, backup_infos.size());
  ASSERT_EQ(2, backup_infos[0].backup_id);

  // Verify individual GetBackupInfo by ID
  ASSERT_TRUE(backup_engine_->GetBackupInfo(0U, &backup_info).IsNotFound());
  ASSERT_TRUE(backup_engine_->GetBackupInfo(1U, &backup_info).IsCorruption());
  ASSERT_TRUE(backup_engine_->GetBackupInfo(2U, &backup_info).ok());
  ASSERT_TRUE(backup_engine_->GetBackupInfo(3U, &backup_info).IsNotFound());
  ASSERT_TRUE(
      backup_engine_->GetBackupInfo(999999U, &backup_info).IsNotFound());
}

TEST_F(BackupEngineTest, WriteOnlyEngineNoSharedFileDeletion) {
  // Verifies a write-only BackupEngine does not delete files belonging to valid
  // backups when GarbageCollect, PurgeOldBackups, or DeleteBackup are called.
  const int kNumKeys = 5000;
  for (int i = 0; i < 3; ++i) {
    OpenDBAndBackupEngine(i == 0 /* destroy_old_data */);
    FillDB(db_.get(), i * kNumKeys, (i + 1) * kNumKeys);
    ASSERT_OK(backup_engine_->CreateNewBackup(db_.get(), true));
    CloseDBAndBackupEngine();

    backupable_options_->max_valid_backups_to_open = 0;
    OpenDBAndBackupEngine();
    switch (i) {
      case 0:
        ASSERT_OK(backup_engine_->GarbageCollect());
        break;
      case 1:
        ASSERT_OK(backup_engine_->PurgeOldBackups(1 /* num_backups_to_keep */));
        break;
      case 2:
        ASSERT_OK(backup_engine_->DeleteBackup(2 /* backup_id */));
        break;
      default:
        assert(false);
    }
    CloseDBAndBackupEngine();

    backupable_options_->max_valid_backups_to_open = port::kMaxInt32;
    AssertBackupConsistency(i + 1, 0, (i + 1) * kNumKeys);
  }
}

TEST_P(BackupEngineTestWithParam, BackupUsingDirectIO) {
  // Tests direct I/O on the backup engine's reads and writes on the DB env and
  // backup env
  // We use ChrootEnv underneath so the below line checks for direct I/O support
  // in the chroot directory, not the true filesystem root.
  if (!test::IsDirectIOSupported(test_db_env_.get(), "/")) {
    ROCKSDB_GTEST_SKIP("Test requires Direct I/O Support");
    return;
  }
  const int kNumKeysPerBackup = 100;
  const int kNumBackups = 3;
  options_.use_direct_reads = true;
  OpenDBAndBackupEngine(true /* destroy_old_data */);
  for (int i = 0; i < kNumBackups; ++i) {
    FillDB(db_.get(), i * kNumKeysPerBackup /* from */,
           (i + 1) * kNumKeysPerBackup /* to */, kFlushAll);

    // Clear the file open counters and then do a bunch of backup engine ops.
    // For all ops, files should be opened in direct mode.
    test_backup_env_->ClearFileOpenCounters();
    test_db_env_->ClearFileOpenCounters();
    CloseBackupEngine();
    OpenBackupEngine();
    ASSERT_OK(backup_engine_->CreateNewBackup(db_.get(),
                                              false /* flush_before_backup */));
    ASSERT_OK(backup_engine_->VerifyBackup(i + 1));
    CloseBackupEngine();
    OpenBackupEngine();
    std::vector<BackupInfo> backup_infos;
    backup_engine_->GetBackupInfo(&backup_infos);
    ASSERT_EQ(static_cast<size_t>(i + 1), backup_infos.size());

    // Verify backup engine always opened files with direct I/O
    ASSERT_EQ(0, test_db_env_->num_writers());
    ASSERT_GE(test_db_env_->num_direct_rand_readers(), 0);
    ASSERT_GT(test_db_env_->num_direct_seq_readers(), 0);
    // Currently the DB doesn't support reading WALs or manifest with direct
    // I/O, so subtract two.
    ASSERT_EQ(test_db_env_->num_seq_readers() - 2,
              test_db_env_->num_direct_seq_readers());
    ASSERT_EQ(test_db_env_->num_rand_readers(),
              test_db_env_->num_direct_rand_readers());
  }
  CloseDBAndBackupEngine();

  for (int i = 0; i < kNumBackups; ++i) {
    AssertBackupConsistency(i + 1 /* backup_id */,
                            i * kNumKeysPerBackup /* start_exist */,
                            (i + 1) * kNumKeysPerBackup /* end_exist */,
                            (i + 2) * kNumKeysPerBackup /* end */);
  }
}

TEST_F(BackupEngineTest, BackgroundThreadCpuPriority) {
  std::atomic<CpuPriority> priority(CpuPriority::kNormal);
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "BackupEngineImpl::Initialize:SetCpuPriority", [&](void* new_priority) {
        priority.store(*reinterpret_cast<CpuPriority*>(new_priority));
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  // 1 thread is easier to test, otherwise, we may not be sure which thread
  // actually does the work during CreateNewBackup.
  backupable_options_->max_background_operations = 1;
  OpenDBAndBackupEngine(true);

  {
    FillDB(db_.get(), 0, 100);

    // by default, cpu priority is not changed.
    CreateBackupOptions options;
    ASSERT_OK(backup_engine_->CreateNewBackup(options, db_.get()));

    ASSERT_EQ(priority, CpuPriority::kNormal);
  }

  {
    FillDB(db_.get(), 101, 200);

    // decrease cpu priority from normal to low.
    CreateBackupOptions options;
    options.decrease_background_thread_cpu_priority = true;
    options.background_thread_cpu_priority = CpuPriority::kLow;
    ASSERT_OK(backup_engine_->CreateNewBackup(options, db_.get()));

    ASSERT_EQ(priority, CpuPriority::kLow);
  }

  {
    FillDB(db_.get(), 201, 300);

    // try to upgrade cpu priority back to normal,
    // the priority should still low.
    CreateBackupOptions options;
    options.decrease_background_thread_cpu_priority = true;
    options.background_thread_cpu_priority = CpuPriority::kNormal;
    ASSERT_OK(backup_engine_->CreateNewBackup(options, db_.get()));

    ASSERT_EQ(priority, CpuPriority::kLow);
  }

  {
    FillDB(db_.get(), 301, 400);

    // decrease cpu priority from low to idle.
    CreateBackupOptions options;
    options.decrease_background_thread_cpu_priority = true;
    options.background_thread_cpu_priority = CpuPriority::kIdle;
    ASSERT_OK(backup_engine_->CreateNewBackup(options, db_.get()));

    ASSERT_EQ(priority, CpuPriority::kIdle);
  }

  {
    FillDB(db_.get(), 301, 400);

    // reset priority to later verify that it's not updated by SetCpuPriority.
    priority = CpuPriority::kNormal;

    // setting the same cpu priority won't call SetCpuPriority.
    CreateBackupOptions options;
    options.decrease_background_thread_cpu_priority = true;
    options.background_thread_cpu_priority = CpuPriority::kIdle;

    // Also check output backup_id with CreateNewBackup
    BackupID new_id = 0;
    ASSERT_OK(backup_engine_->CreateNewBackup(options, db_.get(), &new_id));
    ASSERT_EQ(new_id, 5U);

    ASSERT_EQ(priority, CpuPriority::kNormal);
  }

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearAllCallBacks();
  CloseDBAndBackupEngine();
  DestroyDB(dbname_, options_);
}

// Populates `*total_size` with the size of all files under `backup_dir`.
// We don't go through `BackupEngine` currently because it's hard to figure out
// the metadata file size.
Status GetSizeOfBackupFiles(FileSystem* backup_fs,
                            const std::string& backup_dir, size_t* total_size) {
  *total_size = 0;
  std::vector<std::string> dir_stack = {backup_dir};
  Status s;
  while (s.ok() && !dir_stack.empty()) {
    std::string dir = std::move(dir_stack.back());
    dir_stack.pop_back();
    std::vector<std::string> children;
    s = backup_fs->GetChildren(dir, IOOptions(), &children, nullptr /* dbg */);
    for (size_t i = 0; s.ok() && i < children.size(); ++i) {
      std::string path = dir + "/" + children[i];
      bool is_dir;
      s = backup_fs->IsDirectory(path, IOOptions(), &is_dir, nullptr /* dbg */);
      uint64_t file_size = 0;
      if (s.ok()) {
        if (is_dir) {
          dir_stack.emplace_back(std::move(path));
        } else {
          s = backup_fs->GetFileSize(path, IOOptions(), &file_size,
                                     nullptr /* dbg */);
        }
      }
      if (s.ok()) {
        *total_size += file_size;
      }
    }
  }
  return s;
}

TEST_F(BackupEngineTest, IOStats) {
  // Tests the `BACKUP_READ_BYTES` and `BACKUP_WRITE_BYTES` ticker stats have
  // the expected values according to the files in the backups.

  // These ticker stats are expected to be populated regardless of `PerfLevel`
  // in user thread
  SetPerfLevel(kDisable);

  options_.statistics = CreateDBStatistics();
  OpenDBAndBackupEngine(true /* destroy_old_data */, false /* dummy */,
                        kShareWithChecksum);

  FillDB(db_.get(), 0 /* from */, 100 /* to */, kFlushMost);

  ASSERT_EQ(0, options_.statistics->getTickerCount(BACKUP_READ_BYTES));
  ASSERT_EQ(0, options_.statistics->getTickerCount(BACKUP_WRITE_BYTES));
  ASSERT_OK(backup_engine_->CreateNewBackup(db_.get(),
                                            false /* flush_before_backup */));

  size_t orig_backup_files_size;
  ASSERT_OK(GetSizeOfBackupFiles(test_backup_env_->GetFileSystem().get(),
                                 backupdir_, &orig_backup_files_size));
  size_t expected_bytes_written = orig_backup_files_size;
  ASSERT_EQ(expected_bytes_written,
            options_.statistics->getTickerCount(BACKUP_WRITE_BYTES));
  // Bytes read is more difficult to pin down since there are reads for many
  // purposes other than creating file, like `GetSortedWalFiles()` to find first
  // sequence number, or `CreateNewBackup()` thread to find SST file session ID.
  // So we loosely require there are at least as many reads as needed for
  // copying, but not as many as twice that.
  ASSERT_GE(options_.statistics->getTickerCount(BACKUP_READ_BYTES),
            expected_bytes_written);
  ASSERT_LT(expected_bytes_written,
            2 * options_.statistics->getTickerCount(BACKUP_READ_BYTES));

  FillDB(db_.get(), 100 /* from */, 200 /* to */, kFlushMost);

  ASSERT_OK(options_.statistics->Reset());
  ASSERT_OK(backup_engine_->CreateNewBackup(db_.get(),
                                            false /* flush_before_backup */));
  size_t final_backup_files_size;
  ASSERT_OK(GetSizeOfBackupFiles(test_backup_env_->GetFileSystem().get(),
                                 backupdir_, &final_backup_files_size));
  expected_bytes_written = final_backup_files_size - orig_backup_files_size;
  ASSERT_EQ(expected_bytes_written,
            options_.statistics->getTickerCount(BACKUP_WRITE_BYTES));
  // See above for why these bounds were chosen.
  ASSERT_GE(options_.statistics->getTickerCount(BACKUP_READ_BYTES),
            expected_bytes_written);
  ASSERT_LT(expected_bytes_written,
            2 * options_.statistics->getTickerCount(BACKUP_READ_BYTES));
}

}  // anon namespace

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

#else
#include <stdio.h>

int main(int /*argc*/, char** /*argv*/) {
  fprintf(stderr, "SKIPPED as BackupEngine is not supported in ROCKSDB_LITE\n");
  return 0;
}

#endif  // !defined(ROCKSDB_LITE) && !defined(OS_WIN)

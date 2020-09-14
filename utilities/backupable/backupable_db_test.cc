//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#if !defined(ROCKSDB_LITE) && !defined(OS_WIN)

#include "rocksdb/utilities/backupable_db.h"

#include <algorithm>
#include <limits>
#include <regex>
#include <string>
#include <utility>

#include "db/db_impl/db_impl.h"
#include "env/env_chroot.h"
#include "file/filename.h"
#include "port/port.h"
#include "port/stack_trace.h"
#include "rocksdb/rate_limiter.h"
#include "rocksdb/transaction_log.h"
#include "rocksdb/types.h"
#include "rocksdb/utilities/options_util.h"
#include "test_util/sync_point.h"
#include "test_util/testharness.h"
#include "test_util/testutil.h"
#include "util/cast_util.h"
#include "util/mutexlock.h"
#include "util/random.h"
#include "util/stderr_logger.h"
#include "util/string_util.h"

namespace ROCKSDB_NAMESPACE {

namespace {

class DummyFileChecksumGen : public FileChecksumGenerator {
 public:
  explicit DummyFileChecksumGen(const FileChecksumGenContext& /* context */,
                                bool state) {
    if (state) {
      checksum_ = 0;
    } else {
      checksum_ = 1;
    }
  }

  void Update(const char* /* data */, size_t /* n */) override {}

  void Finalize() override {
    assert(checksum_str_.empty());
    // Store as big endian raw bytes
    PutFixed32(&checksum_str_, EndianSwapValue(checksum_));
  }

  std::string GetChecksum() const override {
    assert(!checksum_str_.empty());
    return checksum_str_;
  }

  const char* Name() const override { return "DummyFileChecksum"; }

 private:
  uint32_t checksum_;
  std::string checksum_str_;
};

class DummyFileChecksumGenFactory : public FileChecksumGenFactory {
 public:
  explicit DummyFileChecksumGenFactory(bool state = false) : state_(state) {}

  std::unique_ptr<FileChecksumGenerator> CreateFileChecksumGenerator(
      const FileChecksumGenContext& context) override {
    if (context.requested_checksum_func_name.empty() ||
        context.requested_checksum_func_name == "DummyFileChecksum") {
      return std::unique_ptr<FileChecksumGenerator>(
          new DummyFileChecksumGen(context, state_));
    } else {
      return nullptr;
    }
  }

  const char* Name() const override { return "DummyFileChecksumGenFactory"; }

 private:
  bool state_;
};

class FileHash32Gen : public FileChecksumGenerator {
 public:
  explicit FileHash32Gen(const FileChecksumGenContext& /*context*/) {
    checksum_ = 0;
  }

  void Update(const char* data, size_t n) override { content_.append(data, n); }

  void Finalize() override {
    assert(checksum_str_.empty());
    const char* str = content_.c_str();
    checksum_ = Hash(str, strlen(str), 1);
    // Store as big endian raw bytes
    PutFixed32(&checksum_str_, EndianSwapValue(checksum_));
  }

  std::string GetChecksum() const override {
    assert(!checksum_str_.empty());
    return checksum_str_;
  }

  const char* Name() const override { return "FileHash32"; }

 private:
  std::string content_;
  uint32_t checksum_;
  std::string checksum_str_;
};

class FileHash64Gen : public FileChecksumGenerator {
 public:
  explicit FileHash64Gen(const FileChecksumGenContext& /*context*/) {
    checksum_ = 0;
  }

  void Update(const char* data, size_t n) override { content_.append(data, n); }

  void Finalize() override {
    assert(checksum_str_.empty());
    const char* str = content_.c_str();
    checksum_ = Hash64(str, strlen(str), 1);
    // Store as big endian raw bytes
    PutFixed64(&checksum_str_, EndianSwapValue(checksum_));
  }

  std::string GetChecksum() const override {
    assert(!checksum_str_.empty());
    return checksum_str_;
  }

  const char* Name() const override { return "FileHash64"; }

 private:
  std::string content_;
  uint64_t checksum_;
  std::string checksum_str_;
};

class FileHash32GenFactory : public FileChecksumGenFactory {
 public:
  std::unique_ptr<FileChecksumGenerator> CreateFileChecksumGenerator(
      const FileChecksumGenContext& context) override {
    if (context.requested_checksum_func_name.empty() ||
        context.requested_checksum_func_name == "FileHash32") {
      return std::unique_ptr<FileChecksumGenerator>(new FileHash32Gen(context));
    } else {
      return nullptr;
    }
  }

  const char* Name() const override { return "FileHash32GenFactory"; }
};

class FileHashGenFactory : public FileChecksumGenFactory {
 public:
  std::unique_ptr<FileChecksumGenerator> CreateFileChecksumGenerator(
      const FileChecksumGenContext& context) override {
    if (context.requested_checksum_func_name.empty() ||
        context.requested_checksum_func_name == "FileHash64") {
      return std::unique_ptr<FileChecksumGenerator>(new FileHash64Gen(context));
    } else if (context.requested_checksum_func_name == "FileHash32") {
      return std::unique_ptr<FileChecksumGenerator>(new FileHash32Gen(context));
    } else {
      return nullptr;
    }
  }

  const char* Name() const override { return "FileHashGenFactory"; }
};

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

  Status GetLiveFiles(std::vector<std::string>& vec, uint64_t* mfs,
                      bool /*flush_memtable*/ = true) override {
    EXPECT_TRUE(!deletions_enabled_);
    vec = live_files_;
    *mfs = 100;
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

  Status GetSortedWalFiles(VectorLogPtr& files) override {
    EXPECT_TRUE(!deletions_enabled_);
    files.resize(wal_files_.size());
    for (size_t i = 0; i < files.size(); ++i) {
      files[i].reset(
          new DummyLogFile(wal_files_[i].first, wal_files_[i].second));
    }
    return Status::OK();
  }

  // To avoid FlushWAL called on stacked db which is nullptr
  Status FlushWAL(bool /*sync*/) override { return Status::OK(); }

  std::vector<std::string> live_files_;
  // pair<filename, alive?>
  std::vector<std::pair<std::string, bool>> wal_files_;
 private:
  Options options_;
  std::string dbname_;
  bool deletions_enabled_;
  mutable SequenceNumber sequence_number_;
}; // DummyDB

class TestEnv : public EnvWrapper {
 public:
  explicit TestEnv(Env* t) : EnvWrapper(t) {}

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
        r->push_back({dir + filename, 10 /* size_bytes */});
      }
      return Status::OK();
    }
    return EnvWrapper::GetChildrenFileAttributes(dir, r);
  }
  Status GetFileSize(const std::string& path, uint64_t* size_bytes) override {
    if (filenames_for_mocked_attrs_.size() > 0) {
      auto fname = path.substr(path.find_last_of('/'));
      auto filename_iter = std::find(filenames_for_mocked_attrs_.begin(),
                                     filenames_for_mocked_attrs_.end(), fname);
      if (filename_iter != filenames_for_mocked_attrs_.end()) {
        *size_bytes = 10;
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

  Status GetRandomFileInDir(const std::string& dir, std::string* fname,
                            uint64_t* fsize) {
    std::vector<FileAttributes> children;
    GetChildrenFileAttributes(dir, &children);
    if (children.size() <= 2) {  // . and ..
      return Status::NotFound("Empty directory: " + dir);
    }
    assert(fname != nullptr);
    while (true) {
      int i = rnd_.Next() % children.size();
      if (children[i].name != "." && children[i].name != "..") {
        fname->assign(dir + "/" + children[i].name);
        *fsize = children[i].size_bytes;
        return Status::OK();
      }
    }
    // should never get here
    assert(false);
    return Status::NotFound("");
  }

  Status DeleteRandomFileInDir(const std::string& dir) {
    std::vector<std::string> children;
    GetChildren(dir, &children);
    if (children.size() <= 2) { // . and ..
      return Status::NotFound("");
    }
    while (true) {
      int i = rnd_.Next() % children.size();
      if (children[i] != "." && children[i] != "..") {
        return DeleteFile(dir + "/" + children[i]);
      }
    }
    // should never get here
    assert(false);
    return Status::NotFound("");
  }

  Status AppendToRandomFileInDir(const std::string& dir,
                                 const std::string& data) {
    std::vector<std::string> children;
    GetChildren(dir, &children);
    if (children.size() <= 2) {
      return Status::NotFound("");
    }
    while (true) {
      int i = rnd_.Next() % children.size();
      if (children[i] != "." && children[i] != "..") {
        return WriteToFile(dir + "/" + children[i], data);
      }
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

class BackupableDBTest : public testing::Test {
 public:
  enum ShareOption {
    kNoShare,
    kShareNoChecksum,
    kShareWithChecksum,
  };

  const std::vector<ShareOption> kAllShareOptions = {
      kNoShare, kShareNoChecksum, kShareWithChecksum};

  BackupableDBTest() {
    // set up files
    std::string db_chroot = test::PerThreadDBPath("backupable_db");
    std::string backup_chroot = test::PerThreadDBPath("backupable_db_backup");
    Env::Default()->CreateDir(db_chroot);
    Env::Default()->CreateDir(backup_chroot);
    dbname_ = "/tempdb";
    backupdir_ = "/tempbk";

    // set up envs
    db_chroot_env_.reset(NewChrootEnv(Env::Default(), db_chroot));
    backup_chroot_env_.reset(NewChrootEnv(Env::Default(), backup_chroot));
    test_db_env_.reset(new TestEnv(db_chroot_env_.get()));
    test_backup_env_.reset(new TestEnv(backup_chroot_env_.get()));
    file_manager_.reset(new FileManager(backup_chroot_env_.get()));

    // set up db options
    options_.create_if_missing = true;
    options_.paranoid_checks = true;
    options_.write_buffer_size = 1 << 17; // 128KB
    options_.env = test_db_env_.get();
    options_.wal_dir = dbname_;

    // Create logger
    DBOptions logger_options;
    logger_options.env = db_chroot_env_.get();
    CreateLoggerFromOptions(dbname_, logger_options, &logger_);

    // set up backup db options
    backupable_options_.reset(new BackupableDBOptions(
        backupdir_, test_backup_env_.get(), true, logger_.get(), true));

    // most tests will use multi-threaded backups
    backupable_options_->max_background_operations = 7;

    // delete old files in db
    DestroyDB(dbname_, options_);
  }

  DB* OpenDB() {
    DB* db;
    EXPECT_OK(DB::Open(options_, dbname_, &db));
    return db;
  }

  void CloseAndReopenDB() {
    // Close DB
    db_.reset();

    // Open DB
    test_db_env_->SetLimitWrittenFiles(1000000);
    DB* db;
    ASSERT_OK(DB::Open(options_, dbname_, &db));
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
    if (backup_id > 0) {
      ASSERT_OK(backup_engine_->RestoreDBFromBackup(backup_id, dbname_, dbname_,
                                                    restore_options));
    } else {
      ASSERT_OK(backup_engine_->RestoreDBFromLatestBackup(dbname_, dbname_,
                                                          restore_options));
    }
    DB* db = OpenDB();
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
    db_chroot_env_->GetChildren(dbname_, &delete_logs);
    for (auto f : delete_logs) {
      uint64_t number;
      FileType type;
      bool ok = ParseFileName(f, &number, &type);
      if (ok && type == kLogFile) {
        db_chroot_env_->DeleteFile(dbname_ + "/" + f);
      }
    }
  }

  Status CorruptRandomTableFileInDB() {
    Random rnd(6);
    std::vector<FileAttributes> children;
    test_db_env_->GetChildrenFileAttributes(dbname_, &children);
    if (children.size() <= 2) {  // . and ..
      return Status::NotFound("");
    }
    std::string fname;
    uint64_t fsize = 0;
    while (true) {
      int i = rnd.Next() % children.size();
      fname = children[i].name;
      fsize = children[i].size_bytes;
      // find an sst file
      if (fsize > 0 && fname.length() > 4 &&
          fname.rfind(".sst") == fname.length() - 4) {
        fname = dbname_ + "/" + fname;
        break;
      }
    }

    std::string file_contents;
    Status s = ReadFileToString(test_db_env_.get(), fname, &file_contents);
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

  // files
  std::string dbname_;
  std::string backupdir_;

  // logger_ must be above backup_engine_ such that the engine's destructor,
  // which uses a raw pointer to the logger, executes first.
  std::shared_ptr<Logger> logger_;

  // envs
  std::unique_ptr<Env> db_chroot_env_;
  std::unique_ptr<Env> backup_chroot_env_;
  std::unique_ptr<TestEnv> test_db_env_;
  std::unique_ptr<TestEnv> test_backup_env_;
  std::unique_ptr<FileManager> file_manager_;

  // all the dbs!
  DummyDB* dummy_db_; // BackupableDB owns dummy_db_
  std::unique_ptr<DB> db_;
  std::unique_ptr<BackupEngine> backup_engine_;

  // options
  Options options_;

 protected:
  std::unique_ptr<BackupableDBOptions> backupable_options_;
}; // BackupableDBTest

void AppendPath(const std::string& path, std::vector<std::string>& v) {
  for (auto& f : v) {
    f = path + f;
  }
}

class BackupableDBTestWithParam : public BackupableDBTest,
                                  public testing::WithParamInterface<bool> {
 public:
  BackupableDBTestWithParam() {
    backupable_options_->share_files_with_checksum = GetParam();
  }
  void OpenDBAndBackupEngine(
      bool destroy_old_data = false, bool dummy = false,
      ShareOption shared_option = kShareNoChecksum) override {
    BackupableDBTest::InitializeDBAndBackupEngine(dummy);
    // reset backup env defaults
    test_backup_env_->SetLimitWrittenFiles(1000000);
    backupable_options_->destroy_old_data = destroy_old_data;
    backupable_options_->share_table_files = shared_option != kNoShare;
    // NOTE: keep share_files_with_checksum setting from constructor
    OpenBackupEngine(destroy_old_data);
  }
};

TEST_F(BackupableDBTest, DbAndBackupSameCustomChecksum) {
  const int keys_iteration = 5000;
  options_.file_checksum_gen_factory = GetFileChecksumGenCrc32cFactory();
  // backup uses it default crc32c
  for (const auto& sopt : kAllShareOptions) {
    OpenDBAndBackupEngine(true /* destroy_old_data */, false /* dummy */, sopt);
    FillDB(db_.get(), 0, keys_iteration);
    ASSERT_OK(backup_engine_->CreateNewBackup(db_.get()));
    ASSERT_OK(backup_engine_->VerifyBackup(1, false));
    ASSERT_OK(backup_engine_->VerifyBackup(1, true));
    CloseDBAndBackupEngine();
    AssertBackupConsistency(1, 0, keys_iteration, keys_iteration + 1);
    // delete old data
    DestroyDB(dbname_, options_);
  }

  // backup uses db crc32c
  backupable_options_->file_checksum_gen_factory =
      GetFileChecksumGenCrc32cFactory();
  for (const auto& sopt : kAllShareOptions) {
    OpenDBAndBackupEngine(true /* destroy_old_data */, false /* dummy */, sopt);
    FillDB(db_.get(), 0, keys_iteration);
    ASSERT_OK(backup_engine_->CreateNewBackup(db_.get()));
    ASSERT_OK(backup_engine_->VerifyBackup(1, false));
    ASSERT_OK(backup_engine_->VerifyBackup(1, true));
    CloseDBAndBackupEngine();
    AssertBackupConsistency(1, 0, keys_iteration, keys_iteration + 1);
    // delete old data
    DestroyDB(dbname_, options_);
  }

  std::shared_ptr<FileChecksumGenFactory> hash_factory =
      std::make_shared<FileHashGenFactory>();
  options_.file_checksum_gen_factory = hash_factory;
  backupable_options_->file_checksum_gen_factory = hash_factory;
  for (const auto& sopt : kAllShareOptions) {
    OpenDBAndBackupEngine(true /* destroy_old_data */, false /* dummy */, sopt);
    FillDB(db_.get(), 0, keys_iteration);
    ASSERT_OK(backup_engine_->CreateNewBackup(db_.get()));
    ASSERT_OK(backup_engine_->VerifyBackup(1, false));
    ASSERT_OK(backup_engine_->VerifyBackup(1, true));
    CloseDBAndBackupEngine();
    AssertBackupConsistency(1, 0, keys_iteration, keys_iteration + 1);
    // delete old data
    DestroyDB(dbname_, options_);
  }

  // Mimic a checksum mismatch for custom checksum function by using a dummy
  // checksum function with a state
  std::shared_ptr<FileChecksumGenFactory> dummy_factory_0 =
      std::make_shared<DummyFileChecksumGenFactory>(false);
  std::shared_ptr<FileChecksumGenFactory> dummy_factory_1 =
      std::make_shared<DummyFileChecksumGenFactory>(true);
  FileChecksumGenContext context;
  // Both factories have the same generator name
  std::string dummy_checksum_function_name =
      dummy_factory_0->CreateFileChecksumGenerator(context)->Name();
  options_.file_checksum_gen_factory = dummy_factory_0;
  for (const auto& sopt : kAllShareOptions) {
    backupable_options_->file_checksum_gen_factory = dummy_factory_1;
    OpenDBAndBackupEngine(true /* destroy_old_data */, false /* dummy */, sopt);
    FillDB(db_.get(), 0, keys_iteration);
    // DB and backup engine do not have the same custom checksum function
    // "state"
    Status s = backup_engine_->CreateNewBackup(db_.get());
    ASSERT_NOK(s);
    ASSERT_TRUE(
        s.ToString().find("Corruption: " + dummy_checksum_function_name +
                          " mismatch") != std::string::npos);
    CloseBackupEngine();

    // Change custom checksum function and try again
    backupable_options_->file_checksum_gen_factory = dummy_factory_0;
    OpenBackupEngine(true /* destroy_old_data */);
    ASSERT_OK(backup_engine_->CreateNewBackup(db_.get()));
    ASSERT_OK(backup_engine_->VerifyBackup(1, true));
    ASSERT_OK(backup_engine_->RestoreDBFromBackup(1, dbname_, dbname_));
    CloseBackupEngine();

    // Try verifying or restoring a backup using a different custom checksum
    // function "state"
    backupable_options_->file_checksum_gen_factory = dummy_factory_1;
    OpenBackupEngine(false /* destroy_old_data */);
    ASSERT_NOK(backup_engine_->VerifyBackup(1, true));
    ASSERT_NOK(backup_engine_->RestoreDBFromBackup(1, dbname_, dbname_));
    CloseDBAndBackupEngine();

    // delete old data
    DestroyDB(dbname_, options_);
  }
}

TEST_F(BackupableDBTest, CustomChecksumTransition) {
  const int keys_iteration = 5000;
  std::shared_ptr<FileChecksumGenFactory> hash32_factory =
      std::make_shared<FileHash32GenFactory>();
  std::shared_ptr<FileChecksumGenFactory> hash_factory =
      std::make_shared<FileHashGenFactory>();
  for (const auto& sopt : kAllShareOptions) {
    // 1) with one custom checksum function (FileHash32GenFactory) for both
    //    db and backup
    int i = 0;
    options_.file_checksum_gen_factory = hash32_factory;
    backupable_options_->file_checksum_gen_factory = hash32_factory;
    // open with old backup
    OpenDBAndBackupEngine(true /* destroy_old_data */, false /* dummy */, sopt);
    FillDB(db_.get(), 0, keys_iteration * (i + 1));
    ASSERT_OK(backup_engine_->CreateNewBackup(db_.get()));
    // verify the backup with checksum
    ASSERT_OK(backup_engine_->VerifyBackup(i + 1, true));
    CloseDBAndBackupEngine();
    AssertBackupConsistency(i + 1, 0, keys_iteration * (i + 1),
                            keys_iteration * (i + 2));

    // 2) with two custom checksum functions (FileHashGenFactory) for db
    //    but one custom checksum function (FileHash32GenFactory) for backup
    ++i;
    options_.file_checksum_gen_factory = hash_factory;
    backupable_options_->file_checksum_gen_factory = hash32_factory;
    // open with old backup
    OpenDBAndBackupEngine(false /* destroy_old_data */, false /* dummy */,
                          sopt);
    FillDB(db_.get(), 0, keys_iteration * (i + 1));
    // note that the checksum factory for backup does not know the custom
    // checksum function used in the db
    ASSERT_NOK(backup_engine_->CreateNewBackup(db_.get()));
    // but it knows the custom checksum function for the older backup
    ASSERT_OK(backup_engine_->VerifyBackup(i, true));
    // reset the factory to nullptr and try again
    CloseBackupEngine();
    backupable_options_->file_checksum_gen_factory = nullptr;
    OpenBackupEngine();
    ASSERT_NOK(backup_engine_->DeleteBackup(i + 1));
    ASSERT_OK(backup_engine_->CreateNewBackup(db_.get()));
    ASSERT_OK(backup_engine_->VerifyBackup(i + 1, true));
    CloseDBAndBackupEngine();
    AssertBackupConsistency(i, 0, keys_iteration * i, keys_iteration * (i + 1));
    AssertBackupConsistency(i + 1, 0, keys_iteration * (i + 1),
                            keys_iteration * (i + 2));
    // Now set the factory to the same as the one used in the db
    backupable_options_->file_checksum_gen_factory = hash_factory;
    OpenDBAndBackupEngine(false /* destroy_old_data */, false /* dummy */,
                          sopt);
    ASSERT_OK(backup_engine_->CreateNewBackup(db_.get()));
    CloseBackupEngine();

    ++i;
    // Say, we accidentally change the factory
    backupable_options_->file_checksum_gen_factory = hash32_factory;
    OpenBackupEngine();
    // Unable to verify the latest backup.
    ASSERT_NOK(backup_engine_->VerifyBackup(i + 1, true));
    // Unable to restore the latest backup.
    ASSERT_NOK(backup_engine_->RestoreDBFromBackup(i + 1, dbname_, dbname_));
    CloseBackupEngine();
    // Reset the factory to the same as the one used in the db.
    backupable_options_->file_checksum_gen_factory = hash_factory;
    OpenBackupEngine();
    ASSERT_OK(backup_engine_->VerifyBackup(i + 1, true));
    ASSERT_OK(backup_engine_->RestoreDBFromBackup(i + 1, dbname_, dbname_));
    ASSERT_OK(backup_engine_->DeleteBackup(i + 1));
    --i;
    CloseDBAndBackupEngine();

    // 3) with one custom checksum function (FileHash32GenFactory) for db
    //    but two custom checksum functions (FileHashGenFactory) for backup
    // note that the checksum factory for backup does know the checksum
    // function in the db
    ++i;
    options_.file_checksum_gen_factory = hash32_factory;
    backupable_options_->file_checksum_gen_factory = hash_factory;
    // open with old backup
    OpenDBAndBackupEngine(false /* destroy_old_data */, false /* dummy */,
                          sopt);
    FillDB(db_.get(), 0, keys_iteration * (i + 1));
    ASSERT_OK(backup_engine_->CreateNewBackup(db_.get()));

    ASSERT_OK(backup_engine_->VerifyBackup(i - 1, true));
    ASSERT_OK(backup_engine_->VerifyBackup(i, true));
    ASSERT_OK(backup_engine_->VerifyBackup(i + 1, true));
    CloseDBAndBackupEngine();
    AssertBackupConsistency(i - 1, 0, keys_iteration * (i - 1),
                            keys_iteration * i);
    AssertBackupConsistency(i, 0, keys_iteration * i, keys_iteration * (i + 1));
    AssertBackupConsistency(i + 1, 0, keys_iteration * (i + 1),
                            keys_iteration * (i + 2));

    // 4) no custom checksums
    ++i;
    options_.file_checksum_gen_factory = nullptr;
    backupable_options_->file_checksum_gen_factory = nullptr;
    OpenDBAndBackupEngine(false /* destroy_old_data */, false /* dummy */,
                          sopt);
    FillDB(db_.get(), 0, keys_iteration * (i + 1));
    ASSERT_OK(backup_engine_->CreateNewBackup(db_.get()));
    for (int j = 0; j <= i; ++j) {
      ASSERT_OK(backup_engine_->VerifyBackup(j + 1, true));
    }
    CloseDBAndBackupEngine();
    for (int j = 0; j <= i; ++j) {
      AssertBackupConsistency(j + 1, 0, keys_iteration * (j + 1),
                              keys_iteration * (j + 2));
    }

    // delete old data
    DestroyDB(dbname_, options_);
  }
}

TEST_F(BackupableDBTest, CustomChecksumNoNewDbTables) {
  const int keys_iteration = 5000;
  std::vector<std::shared_ptr<FileChecksumGenFactory>> checksum_factories{
      nullptr, GetFileChecksumGenCrc32cFactory(),
      std::make_shared<FileHash32GenFactory>(),
      std::make_shared<FileHashGenFactory>()};

  for (const auto& sopt : kAllShareOptions) {
    for (const auto& f : checksum_factories) {
      options_.file_checksum_gen_factory = f;
      backupable_options_->file_checksum_gen_factory = f;
      OpenDBAndBackupEngine(true /* destroy_old_data */, false /* dummy */,
                            sopt);
      FillDB(db_.get(), 0, keys_iteration);
      ASSERT_OK(backup_engine_->CreateNewBackup(db_.get()));
      ASSERT_OK(backup_engine_->VerifyBackup(1, true));
      // No new table files have been created since the last backup.
      ASSERT_OK(backup_engine_->CreateNewBackup(db_.get()));
      ASSERT_OK(backup_engine_->VerifyBackup(2, true));
      CloseDBAndBackupEngine();
      AssertBackupConsistency(1, 0, keys_iteration, keys_iteration * 2);
      AssertBackupConsistency(2, 0, keys_iteration, keys_iteration * 2);

      OpenDBAndBackupEngine(false /* destroy_old_data */, false /* dummy */,
                            sopt);
      // No new table files have been created since the last backup and backup
      // engine opening
      ASSERT_OK(backup_engine_->CreateNewBackup(db_.get()));
      ASSERT_OK(backup_engine_->VerifyBackup(3, true));
      CloseDBAndBackupEngine();
      AssertBackupConsistency(3, 0, keys_iteration, keys_iteration * 2);

      // delete old data
      DestroyDB(dbname_, options_);
    }
  }
}

TEST_F(BackupableDBTest, FileCollision) {
  const int keys_iteration = 5000;
  for (const auto& sopt : kAllShareOptions) {
    OpenDBAndBackupEngine(true /* destroy_old_data */, false /* dummy */, sopt);
    FillDB(db_.get(), 0, keys_iteration);
    ASSERT_OK(backup_engine_->CreateNewBackup(db_.get()));
    FillDB(db_.get(), 0, keys_iteration);
    ASSERT_OK(backup_engine_->CreateNewBackup(db_.get()));
    CloseDBAndBackupEngine();

    // If the db directory has been cleaned up, it is sensitive to file
    // collision.
    DestroyDB(dbname_, options_);

    // open with old backup
    OpenDBAndBackupEngine(false /* destroy_old_data */, false /* dummy */,
                          sopt);
    FillDB(db_.get(), 0, keys_iteration * 2);
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
    DestroyDB(dbname_, options_);
  }
}

// This test verifies that the verifyBackup method correctly identifies
// invalid backups
TEST_P(BackupableDBTestWithParam, VerifyBackup) {
  const int keys_iteration = 5000;
  Status s;
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
  file_manager_->DeleteRandomFileInDir(backupdir_ + "/private/1");
  ASSERT_TRUE(backup_engine_->VerifyBackup(1).IsNotFound());

  // ---------- case 3. - corrupt a file -----------
  std::string append_data = "Corrupting a random file";
  file_manager_->AppendToRandomFileInDir(backupdir_ + "/private/2",
                                         append_data);
  ASSERT_TRUE(backup_engine_->VerifyBackup(2).IsCorruption());

  // ---------- case 4. - invalid backup -----------
  ASSERT_TRUE(backup_engine_->VerifyBackup(6).IsNotFound());
  CloseDBAndBackupEngine();
}

// open DB, write, close DB, backup, restore, repeat
TEST_P(BackupableDBTestWithParam, OfflineIntegrationTest) {
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
TEST_P(BackupableDBTestWithParam, OnlineIntegrationTest) {
  // has to be a big number, so that it triggers the memtable flush
  const int keys_iteration = 5000;
  const int max_key = keys_iteration * 4 + 10;
  Random rnd(7);
  // delete old data
  DestroyDB(dbname_, options_);

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

INSTANTIATE_TEST_CASE_P(BackupableDBTestWithParam, BackupableDBTestWithParam,
                        ::testing::Bool());

// this will make sure that backup does not copy the same file twice
TEST_F(BackupableDBTest, NoDoubleCopy_And_AutoGC) {
  OpenDBAndBackupEngine(true, true);

  // should write 5 DB files + one meta file
  test_backup_env_->SetLimitWrittenFiles(7);
  test_backup_env_->ClearWrittenFiles();
  test_db_env_->SetLimitWrittenFiles(0);
  dummy_db_->live_files_ = {"/00010.sst", "/00011.sst", "/CURRENT",
                            "/MANIFEST-01"};
  dummy_db_->wal_files_ = {{"/00011.log", true}, {"/00012.log", false}};
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

    dummy_db_->live_files_ = {"/00010.sst", "/" + other_sst, "/CURRENT",
                              "/MANIFEST-01"};
    dummy_db_->wal_files_ = {{"/00011.log", true}, {"/00012.log", false}};
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
  test_backup_env_->GetFileSize(backupdir_ + "/private/2/MANIFEST-01", &size);
  ASSERT_EQ(100UL, size);
  test_backup_env_->GetFileSize(backupdir_ + "/shared/00015.sst", &size);
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
TEST_F(BackupableDBTest, CorruptionsTest) {
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
  ASSERT_TRUE(!s.ok());
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
  ASSERT_TRUE(!s.ok());
  CloseBackupEngine();
  ASSERT_OK(file_manager_->DeleteRandomFileInDir(backupdir_ + "/private/4"));
  // 4 is corrupted, 3 is the latest backup now
  AssertBackupConsistency(0, 0, keys_iteration * 3, keys_iteration * 5);
  OpenBackupEngine();
  s = backup_engine_->RestoreDBFromBackup(4, dbname_, dbname_);
  CloseBackupEngine();
  ASSERT_TRUE(!s.ok());

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
  ASSERT_TRUE(!s.ok());

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
TEST_F(BackupableDBTest, CorruptFileMaintainSize) {
  const int keys_iteration = 5000;
  Status s;
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
    ASSERT_OK(file_manager_->GetRandomFileInDir(backupdir_ + "/private/1",
                                                &file_to_corrupt, &file_size));
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
    if (child.name == "." || child.name == ".." || child.size_bytes == 0) {
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

// Test if BackupEngine will fail to create new backup if some table has been
// corrupted and the table file checksum is stored in the DB manifest
TEST_F(BackupableDBTest, TableFileCorruptedBeforeBackup) {
  const int keys_iteration = 50000;

  OpenDBAndBackupEngine(true /* destroy_old_data */, false /* dummy */,
                        kNoShare);
  FillDB(db_.get(), 0, keys_iteration);
  CloseAndReopenDB();
  // corrupt a random table file in the DB directory
  ASSERT_OK(CorruptRandomTableFileInDB());
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
  CloseAndReopenDB();
  // corrupt a random table file in the DB directory
  ASSERT_OK(CorruptRandomTableFileInDB());
  // table file checksum is enabled so we should be able to detect any
  // corruption
  ASSERT_NOK(backup_engine_->CreateNewBackup(db_.get()));
  CloseDBAndBackupEngine();
}

// Test if BackupEngine will fail to create new backup if some table has been
// corrupted and the table file checksum is stored in the DB manifest for the
// case when backup table files will be stored in a shared directory
TEST_P(BackupableDBTestWithParam, TableFileCorruptedBeforeBackup) {
  const int keys_iteration = 50000;

  OpenDBAndBackupEngine(true /* destroy_old_data */);
  FillDB(db_.get(), 0, keys_iteration);
  CloseAndReopenDB();
  // corrupt a random table file in the DB directory
  ASSERT_OK(CorruptRandomTableFileInDB());
  // cannot detect corruption since DB manifest has no table checksums
  ASSERT_OK(backup_engine_->CreateNewBackup(db_.get()));
  CloseDBAndBackupEngine();

  // delete old files in db
  ASSERT_OK(DestroyDB(dbname_, options_));

  // Enable table checksums in DB manifest
  options_.file_checksum_gen_factory = GetFileChecksumGenCrc32cFactory();
  OpenDBAndBackupEngine(true /* destroy_old_data */);
  FillDB(db_.get(), 0, keys_iteration);
  CloseAndReopenDB();
  // corrupt a random table file in the DB directory
  ASSERT_OK(CorruptRandomTableFileInDB());
  // corruption is detected
  ASSERT_NOK(backup_engine_->CreateNewBackup(db_.get()));
  CloseDBAndBackupEngine();
}

TEST_F(BackupableDBTest, TableFileWithoutDbChecksumCorruptedDuringBackup) {
  const int keys_iteration = 50000;
  backupable_options_->share_files_with_checksum_naming = kChecksumAndFileSize;
  // When share_files_with_checksum is on, we calculate checksums of table
  // files before and after copying. So we can test whether a corruption has
  // happened during the file is copied to backup directory.
  OpenDBAndBackupEngine(true /* destroy_old_data */, false /* dummy */,
                        kShareWithChecksum);

  FillDB(db_.get(), 0, keys_iteration);
  bool corrupted = false;
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

TEST_F(BackupableDBTest, TableFileWithDbChecksumCorruptedDuringBackup) {
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

TEST_F(BackupableDBTest, InterruptCreationTest) {
  // Interrupt backup creation by failing new writes and failing cleanup of the
  // partial state. Then verify a subsequent backup can still succeed.
  const int keys_iteration = 5000;
  Random rnd(6);

  OpenDBAndBackupEngine(true /* destroy_old_data */);
  FillDB(db_.get(), 0, keys_iteration);
  test_backup_env_->SetLimitWrittenFiles(2);
  test_backup_env_->SetDeleteFileFailure(true);
  // should fail creation
  ASSERT_FALSE(
      backup_engine_->CreateNewBackup(db_.get(), !!(rnd.Next() % 2)).ok());
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

TEST_F(BackupableDBTest, FlushCompactDuringBackupCheckpoint) {
  const int keys_iteration = 5000;
  options_.file_checksum_gen_factory = GetFileChecksumGenCrc32cFactory();
  for (const auto& sopt : kAllShareOptions) {
    OpenDBAndBackupEngine(true /* destroy_old_data */, false /* dummy */, sopt);
    FillDB(db_.get(), 0, keys_iteration);
    // That FillDB leaves a mix of flushed and unflushed data
    SyncPoint::GetInstance()->LoadDependency(
        {{"CheckpointImpl::CreateCustomCheckpoint:AfterGetLive1",
          "BackupableDBTest::FlushCompactDuringBackupCheckpoint:Before"},
         {"BackupableDBTest::FlushCompactDuringBackupCheckpoint:After",
          "CheckpointImpl::CreateCustomCheckpoint:AfterGetLive2"}});
    SyncPoint::GetInstance()->EnableProcessing();
    ROCKSDB_NAMESPACE::port::Thread flush_thread{[this]() {
      TEST_SYNC_POINT(
          "BackupableDBTest::FlushCompactDuringBackupCheckpoint:Before");
      FillDB(db_.get(), keys_iteration, 2 * keys_iteration);
      ASSERT_OK(db_->Flush(FlushOptions()));
      DBImpl* dbi = static_cast<DBImpl*>(db_.get());
      dbi->TEST_WaitForFlushMemTable();
      ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
      dbi->TEST_WaitForCompact();
      TEST_SYNC_POINT(
          "BackupableDBTest::FlushCompactDuringBackupCheckpoint:After");
    }};
    ASSERT_OK(backup_engine_->CreateNewBackup(db_.get()));
    flush_thread.join();
    CloseDBAndBackupEngine();
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
    if (sopt == kShareWithChecksum) {
      // Ensure we actually got DB manifest checksums by inspecting
      // shared_checksum file names for hex checksum component
      std::regex expected("[^_]+_[0-9A-F]{8}_[^_]+.sst");
      std::vector<FileAttributes> children;
      const std::string dir = backupdir_ + "/shared_checksum";
      ASSERT_OK(file_manager_->GetChildrenFileAttributes(dir, &children));
      for (const auto& child : children) {
        if (child.name == "." || child.name == ".." || child.size_bytes == 0) {
          continue;
        }
        const std::string match("match");
        EXPECT_EQ(match, std::regex_replace(child.name, expected, match));
      }
    }
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

TEST_F(BackupableDBTest, BackupOptions) {
  OpenDBAndBackupEngine(true);
  for (int i = 1; i < 5; i++) {
    std::string name;
    std::vector<std::string> filenames;
    // Must reset() before reset(OpenDB()) again.
    // Calling OpenDB() while *db_ is existing will cause LOCK issue
    db_.reset();
    db_.reset(OpenDB());
    ASSERT_OK(backup_engine_->CreateNewBackup(db_.get(), true));
    ROCKSDB_NAMESPACE::GetLatestOptionsFileName(db_->GetName(), options_.env,
                                                &name);
    ASSERT_OK(file_manager_->FileExists(OptionsPath(backupdir_, i) + name));
    backup_chroot_env_->GetChildren(OptionsPath(backupdir_, i), &filenames);
    for (auto fn : filenames) {
      if (fn.compare(0, 7, "OPTIONS") == 0) {
        ASSERT_EQ(name, fn);
      }
    }
  }

  CloseDBAndBackupEngine();
}

TEST_F(BackupableDBTest, SetOptionsBackupRaceCondition) {
  OpenDBAndBackupEngine(true);
  SyncPoint::GetInstance()->LoadDependency(
      {{"CheckpointImpl::CreateCheckpoint:SavedLiveFiles1",
        "BackupableDBTest::SetOptionsBackupRaceCondition:BeforeSetOptions"},
       {"BackupableDBTest::SetOptionsBackupRaceCondition:AfterSetOptions",
        "CheckpointImpl::CreateCheckpoint:SavedLiveFiles2"}});
  SyncPoint::GetInstance()->EnableProcessing();
  ROCKSDB_NAMESPACE::port::Thread setoptions_thread{[this]() {
    TEST_SYNC_POINT(
        "BackupableDBTest::SetOptionsBackupRaceCondition:BeforeSetOptions");
    DBImpl* dbi = static_cast<DBImpl*>(db_.get());
    // Change arbitrary option to trigger OPTIONS file deletion
    ASSERT_OK(dbi->SetOptions(dbi->DefaultColumnFamily(),
                              {{"paranoid_file_checks", "false"}}));
    ASSERT_OK(dbi->SetOptions(dbi->DefaultColumnFamily(),
                              {{"paranoid_file_checks", "true"}}));
    ASSERT_OK(dbi->SetOptions(dbi->DefaultColumnFamily(),
                              {{"paranoid_file_checks", "false"}}));
    TEST_SYNC_POINT(
        "BackupableDBTest::SetOptionsBackupRaceCondition:AfterSetOptions");
  }};
  ASSERT_OK(backup_engine_->CreateNewBackup(db_.get()));
  setoptions_thread.join();
  CloseDBAndBackupEngine();
}

// This test verifies we don't delete the latest backup when read-only option is
// set
TEST_F(BackupableDBTest, NoDeleteWithReadOnly) {
  const int keys_iteration = 5000;
  Random rnd(6);
  Status s;

  OpenDBAndBackupEngine(true);
  // create five backups
  for (int i = 0; i < 5; ++i) {
    FillDB(db_.get(), keys_iteration * i, keys_iteration * (i + 1));
    ASSERT_OK(backup_engine_->CreateNewBackup(db_.get(), !!(rnd.Next() % 2)));
  }
  CloseDBAndBackupEngine();
  ASSERT_OK(file_manager_->WriteToFile(backupdir_ + "/LATEST_BACKUP", "4"));

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

TEST_F(BackupableDBTest, FailOverwritingBackups) {
  options_.write_buffer_size = 1024 * 1024 * 1024;  // 1GB
  options_.disable_auto_compactions = true;

  // create backups 1, 2, 3, 4, 5
  OpenDBAndBackupEngine(true);
  for (int i = 0; i < 5; ++i) {
    CloseDBAndBackupEngine();
    DeleteLogFiles();
    OpenDBAndBackupEngine(false);
    FillDB(db_.get(), 100 * i, 100 * (i + 1));
    ASSERT_OK(backup_engine_->CreateNewBackup(db_.get(), true));
  }
  CloseDBAndBackupEngine();

  // restore 3
  OpenBackupEngine();
  ASSERT_OK(backup_engine_->RestoreDBFromBackup(3, dbname_, dbname_));
  CloseBackupEngine();

  OpenDBAndBackupEngine(false);
  FillDB(db_.get(), 0, 300);
  Status s = backup_engine_->CreateNewBackup(db_.get(), true);
  // the new backup fails because new table files
  // clash with old table files from backups 4 and 5
  // (since write_buffer_size is huge, we can be sure that
  // each backup will generate only one sst file and that
  // a file generated by a new backup is the same as
  // sst file generated by backup 4)
  ASSERT_TRUE(s.IsCorruption());
  ASSERT_OK(backup_engine_->DeleteBackup(4));
  ASSERT_OK(backup_engine_->DeleteBackup(5));
  // now, the backup can succeed
  ASSERT_OK(backup_engine_->CreateNewBackup(db_.get(), true));
  CloseDBAndBackupEngine();
}

TEST_F(BackupableDBTest, NoShareTableFiles) {
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
TEST_F(BackupableDBTest, ShareTableFilesWithChecksums) {
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
TEST_F(BackupableDBTest, ShareTableFilesWithChecksumsTransition) {
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
  backup_engine_->DeleteBackup(1);
  backup_engine_->GarbageCollect();
  CloseDBAndBackupEngine();

  // Verify rest (not deleted)
  for (int i = 1; i < 10; ++i) {
    AssertBackupConsistency(i + 1, 0, keys_iteration * (i + 1),
                            keys_iteration * 11);
  }
}

//  Verify backup and restore with share_files_with_checksum on and
//  share_files_with_checksum_naming = kOptionalChecksumAndDbSessionId
TEST_F(BackupableDBTest, ShareTableFilesWithChecksumsNewNaming) {
  // Use session id in the name of SST files
  ASSERT_TRUE(backupable_options_->share_files_with_checksum_naming ==
              kOptionalChecksumAndDbSessionId);
  const int keys_iteration = 5000;
  int i = 0;

  OpenDBAndBackupEngine(true, false, kShareWithChecksum);
  FillDB(db_.get(), keys_iteration * i, keys_iteration * (i + 1));
  ASSERT_OK(backup_engine_->CreateNewBackup(db_.get(), !!(i % 2)));
  CloseDBAndBackupEngine();
  AssertBackupConsistency(i + 1, 0, keys_iteration * (i + 1),
                          keys_iteration * (i + 2));

  // Both checksum and session id in the name of SST files
  options_.file_checksum_gen_factory = GetFileChecksumGenCrc32cFactory();
  OpenDBAndBackupEngine(false, false, kShareWithChecksum);
  FillDB(db_.get(), keys_iteration * i, keys_iteration * (i + 1));
  ASSERT_OK(backup_engine_->CreateNewBackup(db_.get(), !!(i % 2)));
  CloseDBAndBackupEngine();
  AssertBackupConsistency(i + 1, 0, keys_iteration * (i + 1),
                          keys_iteration * (i + 2));
}

// Verify backup and restore with share_files_with_checksum off and then
// transition this option to on and share_files_with_checksum_naming to be
// kOptionalChecksumAndDbSessionId
TEST_F(BackupableDBTest, ShareTableFilesWithChecksumsNewNamingTransition) {
  const int keys_iteration = 5000;
  // We may set share_files_with_checksum_naming to kChecksumAndFileSize
  // here but even if we don't, it should have no effect when
  // share_files_with_checksum is false
  ASSERT_TRUE(backupable_options_->share_files_with_checksum_naming ==
              kOptionalChecksumAndDbSessionId);
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
              kOptionalChecksumAndDbSessionId);
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
  // share_files_with_checksum_naming being kOptionalChecksumAndDbSessionId
  ASSERT_TRUE(backupable_options_->share_files_with_checksum_naming ==
              kOptionalChecksumAndDbSessionId);
  OpenDBAndBackupEngine(false /* destroy_old_data */, false, kNoShare);
  backup_engine_->DeleteBackup(1);
  backup_engine_->GarbageCollect();
  CloseDBAndBackupEngine();

  // Verify second (about to delete)
  AssertBackupConsistency(2, 0, keys_iteration * 2, keys_iteration * (j + 1));

  // Use checksum and file size for backup table file names and open without
  // share_table_files
  // Again, make sure that GarbageCollect / DeleteBackup is OK
  backupable_options_->share_files_with_checksum_naming = kChecksumAndFileSize;
  OpenDBAndBackupEngine(false /* destroy_old_data */, false, kNoShare);
  backup_engine_->DeleteBackup(2);
  backup_engine_->GarbageCollect();
  CloseDBAndBackupEngine();

  // Verify rest (not deleted)
  for (int i = 2; i < j; ++i) {
    AssertBackupConsistency(i + 1, 0, keys_iteration * (i + 1),
                            keys_iteration * (j + 1));
  }
}

// Verify backup and restore with share_files_with_checksum on and transition
// from kChecksumAndFileSize to kOptionalChecksumAndDbSessionId
TEST_F(BackupableDBTest, ShareTableFilesWithChecksumsNewNamingUpgrade) {
  backupable_options_->share_files_with_checksum_naming = kChecksumAndFileSize;
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

  backupable_options_->share_files_with_checksum_naming =
      kOptionalChecksumAndDbSessionId;
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
  backup_engine_->DeleteBackup(1);
  backup_engine_->GarbageCollect();
  CloseDBAndBackupEngine();

  // Verify second (about to delete)
  AssertBackupConsistency(2, 0, keys_iteration * 2, keys_iteration * (j + 1));

  // Use checksum and file size for backup table file names and open without
  // share_table_files
  // Again, make sure that GarbageCollect / DeleteBackup is OK
  backupable_options_->share_files_with_checksum_naming = kChecksumAndFileSize;
  OpenDBAndBackupEngine(false /* destroy_old_data */, false, kNoShare);
  backup_engine_->DeleteBackup(2);
  backup_engine_->GarbageCollect();
  CloseDBAndBackupEngine();

  // Verify rest (not deleted)
  for (int i = 2; i < j; ++i) {
    AssertBackupConsistency(i + 1, 0, keys_iteration * (i + 1),
                            keys_iteration * (j + 1));
  }
}

// This test simulates cleaning up after aborted or incomplete creation
// of a new backup.
TEST_F(BackupableDBTest, DeleteTmpFiles) {
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
        file_manager_->CreateDir(dir);
        ASSERT_OK(file_manager_->FileExists(dir));

        std::string file = dir + "/" + dir_and_file.second;
        file_manager_->WriteToFile(file, "tmp");
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

TEST_F(BackupableDBTest, KeepLogFiles) {
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

class BackupableDBRateLimitingTestWithParam
    : public BackupableDBTest,
      public testing::WithParamInterface<
          std::tuple<bool /* make throttle */,
                     int /* 0 = single threaded, 1 = multi threaded*/,
                     std::pair<uint64_t, uint64_t> /* limits */>> {
 public:
  BackupableDBRateLimitingTestWithParam() {}
};

uint64_t const MB = 1024 * 1024;

INSTANTIATE_TEST_CASE_P(
    RateLimiting, BackupableDBRateLimitingTestWithParam,
    ::testing::Values(std::make_tuple(false, 0, std::make_pair(1 * MB, 5 * MB)),
                      std::make_tuple(false, 0, std::make_pair(2 * MB, 3 * MB)),
                      std::make_tuple(false, 1, std::make_pair(1 * MB, 5 * MB)),
                      std::make_tuple(false, 1, std::make_pair(2 * MB, 3 * MB)),
                      std::make_tuple(true, 0, std::make_pair(1 * MB, 5 * MB)),
                      std::make_tuple(true, 0, std::make_pair(2 * MB, 3 * MB)),
                      std::make_tuple(true, 1, std::make_pair(1 * MB, 5 * MB)),
                      std::make_tuple(true, 1,
                                      std::make_pair(2 * MB, 3 * MB))));

TEST_P(BackupableDBRateLimitingTestWithParam, RateLimiting) {
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

TEST_F(BackupableDBTest, ReadOnlyBackupEngine) {
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

TEST_F(BackupableDBTest, ProgressCallbackDuringBackup) {
  DestroyDB(dbname_, options_);
  OpenDBAndBackupEngine(true);
  FillDB(db_.get(), 0, 100);
  bool is_callback_invoked = false;
  ASSERT_OK(backup_engine_->CreateNewBackup(
      db_.get(), true,
      [&is_callback_invoked]() { is_callback_invoked = true; }));

  ASSERT_TRUE(is_callback_invoked);
  CloseDBAndBackupEngine();
  DestroyDB(dbname_, options_);
}

TEST_F(BackupableDBTest, GarbageCollectionBeforeBackup) {
  DestroyDB(dbname_, options_);
  OpenDBAndBackupEngine(true);

  backup_chroot_env_->CreateDirIfMissing(backupdir_ + "/shared");
  std::string file_five = backupdir_ + "/shared/000007.sst";
  std::string file_five_contents = "I'm not really a sst file";
  // this depends on the fact that 00007.sst is the first file created by the DB
  ASSERT_OK(file_manager_->WriteToFile(file_five, file_five_contents));

  FillDB(db_.get(), 0, 100);
  // backup overwrites file 000007.sst
  ASSERT_TRUE(backup_engine_->CreateNewBackup(db_.get(), true).ok());

  std::string new_file_five_contents;
  ASSERT_OK(ReadFileToString(backup_chroot_env_.get(), file_five,
                             &new_file_five_contents));
  // file 000007.sst was overwritten
  ASSERT_TRUE(new_file_five_contents != file_five_contents);

  CloseDBAndBackupEngine();

  AssertBackupConsistency(0, 0, 100);
}

// Test that we properly propagate Env failures
TEST_F(BackupableDBTest, EnvFailures) {
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
    ASSERT_TRUE(backup_engine_->CreateNewBackup(db_.get(), true).ok());
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
TEST_F(BackupableDBTest, ChangeManifestDuringBackupCreation) {
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
  ASSERT_TRUE(db_chroot_env_->FileExists(prev_manifest_path).IsNotFound());

  CloseDBAndBackupEngine();
  DestroyDB(dbname_, options_);
  AssertBackupConsistency(0, 0, 100);
}

// see https://github.com/facebook/rocksdb/issues/921
TEST_F(BackupableDBTest, Issue921Test) {
  BackupEngine* backup_engine;
  backupable_options_->share_table_files = false;
  backup_chroot_env_->CreateDirIfMissing(backupable_options_->backup_dir);
  backupable_options_->backup_dir += "/new_dir";
  ASSERT_OK(BackupEngine::Open(backup_chroot_env_.get(), *backupable_options_,
                               &backup_engine));

  delete backup_engine;
}

TEST_F(BackupableDBTest, BackupWithMetadata) {
  const int keys_iteration = 5000;
  OpenDBAndBackupEngine(true);
  // create five backups
  for (int i = 0; i < 5; ++i) {
    const std::string metadata = std::to_string(i);
    FillDB(db_.get(), keys_iteration * i, keys_iteration * (i + 1));
    ASSERT_OK(
        backup_engine_->CreateNewBackupWithMetadata(db_.get(), metadata, true));
  }
  CloseDBAndBackupEngine();

  OpenDBAndBackupEngine();
  std::vector<BackupInfo> backup_infos;
  backup_engine_->GetBackupInfo(&backup_infos);
  ASSERT_EQ(5, backup_infos.size());
  for (int i = 0; i < 5; i++) {
    ASSERT_EQ(std::to_string(i), backup_infos[i].app_metadata);
  }
  CloseDBAndBackupEngine();
  DestroyDB(dbname_, options_);
}

TEST_F(BackupableDBTest, BinaryMetadata) {
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

TEST_F(BackupableDBTest, MetadataTooLarge) {
  OpenDBAndBackupEngine(true);
  std::string largeMetadata(1024 * 1024 + 1, 0);
  ASSERT_NOK(
      backup_engine_->CreateNewBackupWithMetadata(db_.get(), largeMetadata));
  CloseDBAndBackupEngine();
  DestroyDB(dbname_, options_);
}

TEST_F(BackupableDBTest, LimitBackupsOpened) {
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

TEST_F(BackupableDBTest, IgnoreLimitBackupsOpenedWhenNotReadOnly) {
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

TEST_F(BackupableDBTest, CreateWhenLatestBackupCorrupted) {
  // we should pick an ID greater than corrupted backups' IDs so creation can
  // succeed even when latest backup is corrupted.
  const int kNumKeys = 5000;
  OpenDBAndBackupEngine(true /* destroy_old_data */);
  FillDB(db_.get(), 0 /* from */, kNumKeys);
  ASSERT_OK(backup_engine_->CreateNewBackup(db_.get(),
                                            true /* flush_before_backup */));
  ASSERT_OK(file_manager_->CorruptFile(backupdir_ + "/meta/1",
                                       3 /* bytes_to_corrupt */));
  CloseDBAndBackupEngine();

  OpenDBAndBackupEngine();
  ASSERT_OK(backup_engine_->CreateNewBackup(db_.get(),
                                            true /* flush_before_backup */));
  std::vector<BackupInfo> backup_infos;
  backup_engine_->GetBackupInfo(&backup_infos);
  ASSERT_EQ(1, backup_infos.size());
  ASSERT_EQ(2, backup_infos[0].backup_id);
}

TEST_F(BackupableDBTest, WriteOnlyEngineNoSharedFileDeletion) {
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

TEST_P(BackupableDBTestWithParam, BackupUsingDirectIO) {
  // Tests direct I/O on the backup engine's reads and writes on the DB env and
  // backup env
  // We use ChrootEnv underneath so the below line checks for direct I/O support
  // in the chroot directory, not the true filesystem root.
  if (!test::IsDirectIOSupported(test_db_env_.get(), "/")) {
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

TEST_F(BackupableDBTest, BackgroundThreadCpuPriority) {
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
    ASSERT_OK(backup_engine_->CreateNewBackup(options, db_.get()));

    ASSERT_EQ(priority, CpuPriority::kNormal);
  }

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearAllCallBacks();
  CloseDBAndBackupEngine();
  DestroyDB(dbname_, options_);
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
  fprintf(stderr, "SKIPPED as BackupableDB is not supported in ROCKSDB_LITE\n");
  return 0;
}

#endif  // !defined(ROCKSDB_LITE) && !defined(OS_WIN)

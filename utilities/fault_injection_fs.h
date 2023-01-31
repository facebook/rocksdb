//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright 2014 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

// This test uses a custom FileSystem to keep track of the state of a file
// system the last "Sync". The data being written is cached in a "buffer".
// Only when "Sync" is called, the data will be persistent. It can similate
// file data loss (or entire files) not protected by a "Sync". For any of the
// FileSystem related operations, by specify the "IOStatus Error", a specific
// error can be returned when file system is not activated.

#pragma once

#include <algorithm>
#include <map>
#include <set>
#include <string>

#include "file/filename.h"
#include "rocksdb/file_system.h"
#include "util/mutexlock.h"
#include "util/random.h"
#include "util/thread_local.h"

namespace ROCKSDB_NAMESPACE {

class TestFSWritableFile;
class FaultInjectionTestFS;

struct FSFileState {
  std::string filename_;
  ssize_t pos_;
  ssize_t pos_at_last_sync_;
  ssize_t pos_at_last_flush_;
  std::string buffer_;

  explicit FSFileState(const std::string& filename)
      : filename_(filename),
        pos_(-1),
        pos_at_last_sync_(-1),
        pos_at_last_flush_(-1) {}

  FSFileState() : pos_(-1), pos_at_last_sync_(-1), pos_at_last_flush_(-1) {}

  bool IsFullySynced() const { return pos_ <= 0 || pos_ == pos_at_last_sync_; }

  IOStatus DropUnsyncedData();

  IOStatus DropRandomUnsyncedData(Random* rand);
};

// A wrapper around WritableFileWriter* file
// is written to or sync'ed.
class TestFSWritableFile : public FSWritableFile {
 public:
  explicit TestFSWritableFile(const std::string& fname,
                              const FileOptions& file_opts,
                              std::unique_ptr<FSWritableFile>&& f,
                              FaultInjectionTestFS* fs);
  virtual ~TestFSWritableFile();
  virtual IOStatus Append(const Slice& data, const IOOptions&,
                          IODebugContext*) override;
  virtual IOStatus Append(const Slice& data, const IOOptions& options,
                          const DataVerificationInfo& verification_info,
                          IODebugContext* dbg) override;
  virtual IOStatus Truncate(uint64_t size, const IOOptions& options,
                            IODebugContext* dbg) override {
    return target_->Truncate(size, options, dbg);
  }
  virtual IOStatus Close(const IOOptions& options,
                         IODebugContext* dbg) override;
  virtual IOStatus Flush(const IOOptions&, IODebugContext*) override;
  virtual IOStatus Sync(const IOOptions& options, IODebugContext* dbg) override;
  virtual IOStatus RangeSync(uint64_t /*offset*/, uint64_t /*nbytes*/,
                             const IOOptions& options,
                             IODebugContext* dbg) override;
  virtual bool IsSyncThreadSafe() const override { return true; }
  virtual IOStatus PositionedAppend(const Slice& data, uint64_t offset,
                                    const IOOptions& options,
                                    IODebugContext* dbg) override {
    return target_->PositionedAppend(data, offset, options, dbg);
  }
  IOStatus PositionedAppend(const Slice& data, uint64_t offset,
                            const IOOptions& options,
                            const DataVerificationInfo& verification_info,
                            IODebugContext* dbg) override;
  virtual size_t GetRequiredBufferAlignment() const override {
    return target_->GetRequiredBufferAlignment();
  }
  virtual bool use_direct_io() const override {
    return target_->use_direct_io();
  };

 private:
  FSFileState state_;  // Need protection by mutex_
  FileOptions file_opts_;
  std::unique_ptr<FSWritableFile> target_;
  bool writable_file_opened_;
  FaultInjectionTestFS* fs_;
  port::Mutex mutex_;
};

// A wrapper around WritableFileWriter* file
// is written to or sync'ed.
class TestFSRandomRWFile : public FSRandomRWFile {
 public:
  explicit TestFSRandomRWFile(const std::string& fname,
                              std::unique_ptr<FSRandomRWFile>&& f,
                              FaultInjectionTestFS* fs);
  virtual ~TestFSRandomRWFile();
  IOStatus Write(uint64_t offset, const Slice& data, const IOOptions& options,
                 IODebugContext* dbg) override;
  IOStatus Read(uint64_t offset, size_t n, const IOOptions& options,
                Slice* result, char* scratch,
                IODebugContext* dbg) const override;
  IOStatus Close(const IOOptions& options, IODebugContext* dbg) override;
  IOStatus Flush(const IOOptions& options, IODebugContext* dbg) override;
  IOStatus Sync(const IOOptions& options, IODebugContext* dbg) override;
  size_t GetRequiredBufferAlignment() const override {
    return target_->GetRequiredBufferAlignment();
  }
  bool use_direct_io() const override { return target_->use_direct_io(); };

 private:
  std::unique_ptr<FSRandomRWFile> target_;
  bool file_opened_;
  FaultInjectionTestFS* fs_;
};

class TestFSRandomAccessFile : public FSRandomAccessFile {
 public:
  explicit TestFSRandomAccessFile(const std::string& fname,
                                  std::unique_ptr<FSRandomAccessFile>&& f,
                                  FaultInjectionTestFS* fs);
  ~TestFSRandomAccessFile() override {}
  IOStatus Read(uint64_t offset, size_t n, const IOOptions& options,
                Slice* result, char* scratch,
                IODebugContext* dbg) const override;
  IOStatus ReadAsync(FSReadRequest& req, const IOOptions& opts,
                     std::function<void(const FSReadRequest&, void*)> cb,
                     void* cb_arg, void** io_handle, IOHandleDeleter* del_fn,
                     IODebugContext* dbg) override;
  IOStatus MultiRead(FSReadRequest* reqs, size_t num_reqs,
                     const IOOptions& options, IODebugContext* dbg) override;
  size_t GetRequiredBufferAlignment() const override {
    return target_->GetRequiredBufferAlignment();
  }
  bool use_direct_io() const override { return target_->use_direct_io(); }

  size_t GetUniqueId(char* id, size_t max_size) const override;

 private:
  std::unique_ptr<FSRandomAccessFile> target_;
  FaultInjectionTestFS* fs_;
};

class TestFSSequentialFile : public FSSequentialFileOwnerWrapper {
 public:
  explicit TestFSSequentialFile(std::unique_ptr<FSSequentialFile>&& f,
                                FaultInjectionTestFS* fs)
      : FSSequentialFileOwnerWrapper(std::move(f)), fs_(fs) {}
  IOStatus Read(size_t n, const IOOptions& options, Slice* result,
                char* scratch, IODebugContext* dbg) override;
  IOStatus PositionedRead(uint64_t offset, size_t n, const IOOptions& options,
                          Slice* result, char* scratch,
                          IODebugContext* dbg) override;

 private:
  FaultInjectionTestFS* fs_;
};

class TestFSDirectory : public FSDirectory {
 public:
  explicit TestFSDirectory(FaultInjectionTestFS* fs, std::string dirname,
                           FSDirectory* dir)
      : fs_(fs), dirname_(dirname), dir_(dir) {}
  ~TestFSDirectory() {}

  virtual IOStatus Fsync(const IOOptions& options,
                         IODebugContext* dbg) override;

  virtual IOStatus Close(const IOOptions& options,
                         IODebugContext* dbg) override;

  virtual IOStatus FsyncWithDirOptions(
      const IOOptions& options, IODebugContext* dbg,
      const DirFsyncOptions& dir_fsync_options) override;

 private:
  FaultInjectionTestFS* fs_;
  std::string dirname_;
  std::unique_ptr<FSDirectory> dir_;
};

class FaultInjectionTestFS : public FileSystemWrapper {
 public:
  explicit FaultInjectionTestFS(const std::shared_ptr<FileSystem>& base)
      : FileSystemWrapper(base),
        filesystem_active_(true),
        filesystem_writable_(false),
        thread_local_error_(new ThreadLocalPtr(DeleteThreadLocalErrorContext)),
        enable_write_error_injection_(false),
        enable_metadata_write_error_injection_(false),
        write_error_rand_(0),
        write_error_one_in_(0),
        metadata_write_error_one_in_(0),
        read_error_one_in_(0),
        ingest_data_corruption_before_write_(false),
        fail_get_file_unique_id_(false) {}
  virtual ~FaultInjectionTestFS() { error_.PermitUncheckedError(); }

  static const char* kClassName() { return "FaultInjectionTestFS"; }
  const char* Name() const override { return kClassName(); }

  IOStatus NewDirectory(const std::string& name, const IOOptions& options,
                        std::unique_ptr<FSDirectory>* result,
                        IODebugContext* dbg) override;

  IOStatus NewWritableFile(const std::string& fname,
                           const FileOptions& file_opts,
                           std::unique_ptr<FSWritableFile>* result,
                           IODebugContext* dbg) override;

  IOStatus ReopenWritableFile(const std::string& fname,
                              const FileOptions& file_opts,
                              std::unique_ptr<FSWritableFile>* result,
                              IODebugContext* dbg) override;

  IOStatus NewRandomRWFile(const std::string& fname,
                           const FileOptions& file_opts,
                           std::unique_ptr<FSRandomRWFile>* result,
                           IODebugContext* dbg) override;

  IOStatus NewRandomAccessFile(const std::string& fname,
                               const FileOptions& file_opts,
                               std::unique_ptr<FSRandomAccessFile>* result,
                               IODebugContext* dbg) override;
  IOStatus NewSequentialFile(const std::string& f, const FileOptions& file_opts,
                             std::unique_ptr<FSSequentialFile>* r,
                             IODebugContext* dbg) override;

  virtual IOStatus DeleteFile(const std::string& f, const IOOptions& options,
                              IODebugContext* dbg) override;

  virtual IOStatus RenameFile(const std::string& s, const std::string& t,
                              const IOOptions& options,
                              IODebugContext* dbg) override;

  virtual IOStatus LinkFile(const std::string& src, const std::string& target,
                            const IOOptions& options,
                            IODebugContext* dbg) override;

// Undef to eliminate clash on Windows
#undef GetFreeSpace
  virtual IOStatus GetFreeSpace(const std::string& path,
                                const IOOptions& options, uint64_t* disk_free,
                                IODebugContext* dbg) override {
    IOStatus io_s;
    if (!IsFilesystemActive() &&
        error_.subcode() == IOStatus::SubCode::kNoSpace) {
      *disk_free = 0;
    } else {
      io_s = target()->GetFreeSpace(path, options, disk_free, dbg);
    }
    return io_s;
  }

  virtual IOStatus Poll(std::vector<void*>& io_handles,
                        size_t min_completions) override;

  virtual IOStatus AbortIO(std::vector<void*>& io_handles) override;

  void WritableFileClosed(const FSFileState& state);

  void WritableFileSynced(const FSFileState& state);

  void WritableFileAppended(const FSFileState& state);

  IOStatus DropUnsyncedFileData();

  IOStatus DropRandomUnsyncedFileData(Random* rnd);

  IOStatus DeleteFilesCreatedAfterLastDirSync(const IOOptions& options,
                                              IODebugContext* dbg);

  void ResetState();

  void UntrackFile(const std::string& f);

  void SyncDir(const std::string& dirname) {
    MutexLock l(&mutex_);
    dir_to_new_files_since_last_sync_.erase(dirname);
  }

  // Setting the filesystem to inactive is the test equivalent to simulating a
  // system reset. Setting to inactive will freeze our saved filesystem state so
  // that it will stop being recorded. It can then be reset back to the state at
  // the time of the reset.
  bool IsFilesystemActive() {
    MutexLock l(&mutex_);
    return filesystem_active_;
  }

  // Setting filesystem_writable_ makes NewWritableFile. ReopenWritableFile,
  // and NewRandomRWFile bypass FaultInjectionTestFS and go directly to the
  // target FS
  bool IsFilesystemDirectWritable() {
    MutexLock l(&mutex_);
    return filesystem_writable_;
  }
  bool ShouldUseDiretWritable(const std::string& file_name) {
    MutexLock l(&mutex_);
    if (filesystem_writable_) {
      return true;
    }
    FileType file_type = kTempFile;
    uint64_t file_number = 0;
    if (!TryParseFileName(file_name, &file_number, &file_type)) {
      return false;
    }
    return skip_direct_writable_types_.find(file_type) !=
           skip_direct_writable_types_.end();
  }
  void SetFilesystemActiveNoLock(
      bool active, IOStatus error = IOStatus::Corruption("Not active")) {
    error.PermitUncheckedError();
    filesystem_active_ = active;
    if (!active) {
      error_ = error;
    }
  }
  void SetFilesystemActive(
      bool active, IOStatus error = IOStatus::Corruption("Not active")) {
    MutexLock l(&mutex_);
    error.PermitUncheckedError();
    SetFilesystemActiveNoLock(active, error);
  }
  void SetFilesystemDirectWritable(bool writable) {
    MutexLock l(&mutex_);
    filesystem_writable_ = writable;
  }
  void AssertNoOpenFile() { assert(open_managed_files_.empty()); }

  IOStatus GetError() { return error_; }

  void SetFileSystemIOError(IOStatus io_error) {
    MutexLock l(&mutex_);
    io_error.PermitUncheckedError();
    error_ = io_error;
  }

  // To simulate the data corruption before data is written in FS
  void IngestDataCorruptionBeforeWrite() {
    MutexLock l(&mutex_);
    ingest_data_corruption_before_write_ = true;
  }

  void NoDataCorruptionBeforeWrite() {
    MutexLock l(&mutex_);
    ingest_data_corruption_before_write_ = false;
  }

  bool ShouldDataCorruptionBeforeWrite() {
    MutexLock l(&mutex_);
    return ingest_data_corruption_before_write_;
  }

  void SetChecksumHandoffFuncType(const ChecksumType& func_type) {
    MutexLock l(&mutex_);
    checksum_handoff_func_tpye_ = func_type;
  }

  const ChecksumType& GetChecksumHandoffFuncType() {
    MutexLock l(&mutex_);
    return checksum_handoff_func_tpye_;
  }

  void SetFailGetUniqueId(bool flag) {
    MutexLock l(&mutex_);
    fail_get_file_unique_id_ = flag;
  }

  bool ShouldFailGetUniqueId() {
    MutexLock l(&mutex_);
    return fail_get_file_unique_id_;
  }

  // Specify what the operation, so we can inject the right type of error
  enum ErrorOperation : char {
    kRead = 0,
    kMultiReadSingleReq = 1,
    kMultiRead = 2,
    kOpen,
  };

  // Set thread-local parameters for error injection. The first argument,
  // seed is the seed for the random number generator, and one_in determines
  // the probability of injecting error (i.e an error is injected with
  // 1/one_in probability)
  void SetThreadLocalReadErrorContext(uint32_t seed, int one_in) {
    struct ErrorContext* ctx =
        static_cast<struct ErrorContext*>(thread_local_error_->Get());
    if (ctx == nullptr) {
      ctx = new ErrorContext(seed);
      thread_local_error_->Reset(ctx);
    }
    ctx->one_in = one_in;
    ctx->count = 0;
  }

  static void DeleteThreadLocalErrorContext(void* p) {
    ErrorContext* ctx = static_cast<ErrorContext*>(p);
    delete ctx;
  }

  // This is to set the parameters for the write error injection.
  // seed is the seed for the random number generator, and one_in determines
  // the probability of injecting error (i.e an error is injected with
  // 1/one_in probability). For write error, we can specify the error we
  // want to inject. Types decides the file types we want to inject the
  // error (e.g., Wal files, SST files), which is empty by default.
  void SetRandomWriteError(uint32_t seed, int one_in, IOStatus error,
                           bool inject_for_all_file_types,
                           const std::vector<FileType>& types) {
    MutexLock l(&mutex_);
    Random tmp_rand(seed);
    error.PermitUncheckedError();
    error_ = error;
    write_error_rand_ = tmp_rand;
    write_error_one_in_ = one_in;
    inject_for_all_file_types_ = inject_for_all_file_types;
    write_error_allowed_types_ = types;
  }

  void SetSkipDirectWritableTypes(const std::set<FileType>& types) {
    MutexLock l(&mutex_);
    skip_direct_writable_types_ = types;
  }

  void SetRandomMetadataWriteError(int one_in) {
    MutexLock l(&mutex_);
    metadata_write_error_one_in_ = one_in;
  }
  // If the value is not 0, it is enabled. Otherwise, it is disabled.
  void SetRandomReadError(int one_in) { read_error_one_in_ = one_in; }

  bool ShouldInjectRandomReadError() {
    return read_error_one_in() &&
           Random::GetTLSInstance()->OneIn(read_error_one_in());
  }

  // Inject an write error with randomlized parameter and the predefined
  // error type. Only the allowed file types will inject the write error
  IOStatus InjectWriteError(const std::string& file_name);

  // Ingest error to metadata operations.
  IOStatus InjectMetadataWriteError();

  // Inject an error. For a READ operation, a status of IOError(), a
  // corruption in the contents of scratch, or truncation of slice
  // are the types of error with equal probability. For OPEN,
  // its always an IOError.
  // fault_injected returns whether a fault is injected. It is needed
  // because some fault is inected with IOStatus to be OK.
  IOStatus InjectThreadSpecificReadError(ErrorOperation op, Slice* slice,
                                         bool direct_io, char* scratch,
                                         bool need_count_increase,
                                         bool* fault_injected);

  // Get the count of how many times we injected since the previous call
  int GetAndResetErrorCount() {
    ErrorContext* ctx = static_cast<ErrorContext*>(thread_local_error_->Get());
    int count = 0;
    if (ctx != nullptr) {
      count = ctx->count;
      ctx->count = 0;
    }
    return count;
  }

  void EnableErrorInjection() {
    ErrorContext* ctx = static_cast<ErrorContext*>(thread_local_error_->Get());
    if (ctx) {
      ctx->enable_error_injection = true;
    }
  }

  void EnableWriteErrorInjection() {
    MutexLock l(&mutex_);
    enable_write_error_injection_ = true;
  }
  void EnableMetadataWriteErrorInjection() {
    MutexLock l(&mutex_);
    enable_metadata_write_error_injection_ = true;
  }

  void DisableWriteErrorInjection() {
    MutexLock l(&mutex_);
    enable_write_error_injection_ = false;
  }

  void DisableErrorInjection() {
    ErrorContext* ctx = static_cast<ErrorContext*>(thread_local_error_->Get());
    if (ctx) {
      ctx->enable_error_injection = false;
    }
  }

  void DisableMetadataWriteErrorInjection() {
    MutexLock l(&mutex_);
    enable_metadata_write_error_injection_ = false;
  }

  int read_error_one_in() const { return read_error_one_in_.load(); }

  int write_error_one_in() const { return write_error_one_in_; }

  // We capture a backtrace every time a fault is injected, for debugging
  // purposes. This call prints the backtrace to stderr and frees the
  // saved callstack
  void PrintFaultBacktrace();

 private:
  port::Mutex mutex_;
  std::map<std::string, FSFileState> db_file_state_;
  std::set<std::string> open_managed_files_;
  // directory -> (file name -> file contents to recover)
  // When data is recovered from unsyned parent directory, the files with
  // empty file contents to recover is deleted. Those with non-empty ones
  // will be recovered to content accordingly.
  std::unordered_map<std::string, std::map<std::string, std::string>>
      dir_to_new_files_since_last_sync_;
  bool filesystem_active_;    // Record flushes, syncs, writes
  bool filesystem_writable_;  // Bypass FaultInjectionTestFS and go directly
                              // to underlying FS for writable files
  IOStatus error_;

  enum ErrorType : int {
    kErrorTypeStatus = 0,
    kErrorTypeCorruption,
    kErrorTypeTruncated,
    kErrorTypeMax
  };

  struct ErrorContext {
    Random rand;
    int one_in;
    int count;
    bool enable_error_injection;
    void* callstack;
    std::string message;
    int frames;
    ErrorType type;

    explicit ErrorContext(uint32_t seed)
        : rand(seed),
          enable_error_injection(false),
          callstack(nullptr),
          frames(0) {}
    ~ErrorContext() {
      if (callstack) {
        free(callstack);
      }
    }
  };

  std::unique_ptr<ThreadLocalPtr> thread_local_error_;
  bool enable_write_error_injection_;
  bool enable_metadata_write_error_injection_;
  Random write_error_rand_;
  int write_error_one_in_;
  int metadata_write_error_one_in_;
  std::atomic<int> read_error_one_in_;
  bool inject_for_all_file_types_;
  std::vector<FileType> write_error_allowed_types_;
  // File types where direct writable is skipped.
  std::set<FileType> skip_direct_writable_types_;
  bool ingest_data_corruption_before_write_;
  ChecksumType checksum_handoff_func_tpye_;
  bool fail_get_file_unique_id_;

  // Extract number of type from file name. Return false if failing to fine
  // them.
  bool TryParseFileName(const std::string& file_name, uint64_t* number,
                        FileType* type);
};

}  // namespace ROCKSDB_NAMESPACE

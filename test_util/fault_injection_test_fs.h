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

#include "db/db_test_util.h"
#include "db/version_set.h"
#include "env/mock_env.h"
#include "file/filename.h"
#include "include/rocksdb/file_system.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "util/mutexlock.h"
#include "util/random.h"

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
                              std::unique_ptr<FSWritableFile>&& f,
                              FaultInjectionTestFS* fs);
  virtual ~TestFSWritableFile();
  virtual IOStatus Append(const Slice& data, const IOOptions&,
                          IODebugContext*) override;
  virtual IOStatus Truncate(uint64_t size, const IOOptions& options,
                            IODebugContext* dbg) override {
    return target_->Truncate(size, options, dbg);
  }
  virtual IOStatus Close(const IOOptions& options,
                         IODebugContext* dbg) override;
  virtual IOStatus Flush(const IOOptions&, IODebugContext*) override;
  virtual IOStatus Sync(const IOOptions& options, IODebugContext* dbg) override;
  virtual bool IsSyncThreadSafe() const override { return true; }
  virtual IOStatus PositionedAppend(const Slice& data, uint64_t offset,
                                    const IOOptions& options,
                                    IODebugContext* dbg) override {
    return target_->PositionedAppend(data, offset, options, dbg);
  }
  virtual size_t GetRequiredBufferAlignment() const override {
    return target_->GetRequiredBufferAlignment();
  }
  virtual bool use_direct_io() const override {
    return target_->use_direct_io();
  };

 private:
  FSFileState state_;
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
  size_t GetRequiredBufferAlignment() const override {
    return target_->GetRequiredBufferAlignment();
  }
  bool use_direct_io() const override { return target_->use_direct_io(); }

 private:
  std::unique_ptr<FSRandomAccessFile> target_;
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

 private:
  FaultInjectionTestFS* fs_;
  std::string dirname_;
  std::unique_ptr<FSDirectory> dir_;
};

class FaultInjectionTestFS : public FileSystemWrapper {
 public:
  explicit FaultInjectionTestFS(std::shared_ptr<FileSystem> base)
      : FileSystemWrapper(base),
        filesystem_active_(true),
        filesystem_writable_(false),
        thread_local_error_(
            new ThreadLocalPtr(DeleteThreadLocalErrorContext)) {}
  virtual ~FaultInjectionTestFS() {}

  const char* Name() const override { return "FaultInjectionTestFS"; }

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

  virtual IOStatus DeleteFile(const std::string& f, const IOOptions& options,
                              IODebugContext* dbg) override;

  virtual IOStatus RenameFile(const std::string& s, const std::string& t,
                              const IOOptions& options,
                              IODebugContext* dbg) override;

// Undef to eliminate clash on Windows
#undef GetFreeSpace
  virtual IOStatus GetFreeSpace(const std::string& path,
                                const IOOptions& options, uint64_t* disk_free,
                                IODebugContext* dbg) override {
    if (!IsFilesystemActive() && error_ == IOStatus::NoSpace()) {
      *disk_free = 0;
      return IOStatus::OK();
    } else {
      return target()->GetFreeSpace(path, options, disk_free, dbg);
    }
  }

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
  void SetFilesystemActiveNoLock(
      bool active, IOStatus error = IOStatus::Corruption("Not active")) {
    filesystem_active_ = active;
    if (!active) {
      error_ = error;
    }
  }
  void SetFilesystemActive(
      bool active, IOStatus error = IOStatus::Corruption("Not active")) {
    MutexLock l(&mutex_);
    SetFilesystemActiveNoLock(active, error);
  }
  void SetFilesystemDirectWritable(
      bool writable) {
    MutexLock l(&mutex_);
    filesystem_writable_ = writable;
  }
  void AssertNoOpenFile() { assert(open_files_.empty()); }

  IOStatus GetError() { return error_; }

  void SetFileSystemIOError(IOStatus io_error) {
    MutexLock l(&mutex_);
    error_ = io_error;
  }

  // Specify what the operation, so we can inject the right type of error
  enum ErrorOperation : char {
    kRead = 0,
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

  static void DeleteThreadLocalErrorContext(void *p) {
    ErrorContext* ctx = static_cast<ErrorContext*>(p);
    delete ctx;
  }

  // Inject an error. For a READ operation, a status of IOError(), a
  // corruption in the contents of scratch, or truncation of slice
  // are the types of error with equal probability. For OPEN,
  // its always an IOError.
  IOStatus InjectError(ErrorOperation op, Slice* slice,
                       bool direct_io, char* scratch);

  // Get the count of how many times we injected since the previous call
  int GetAndResetErrorCount() {
    ErrorContext* ctx =
          static_cast<ErrorContext*>(thread_local_error_->Get());
    int count = 0;
    if (ctx != nullptr) {
      count = ctx->count;
      ctx->count = 0;
    }
    return count;
  }

  void EnableErrorInjection() {
    ErrorContext* ctx =
          static_cast<ErrorContext*>(thread_local_error_->Get());
    if (ctx) {
      ctx->enable_error_injection = true;
    }
  }

  void DisableErrorInjection() {
    ErrorContext* ctx =
          static_cast<ErrorContext*>(thread_local_error_->Get());
    if (ctx) {
      ctx->enable_error_injection = false;
    }
  }

  // We capture a backtrace every time a fault is injected, for debugging
  // purposes. This call prints the backtrace to stderr and frees the
  // saved callstack
  void PrintFaultBacktrace();

 private:
  port::Mutex mutex_;
  std::map<std::string, FSFileState> db_file_state_;
  std::set<std::string> open_files_;
  std::unordered_map<std::string, std::set<std::string>>
      dir_to_new_files_since_last_sync_;
  bool filesystem_active_;  // Record flushes, syncs, writes
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
};

}  // namespace ROCKSDB_NAMESPACE

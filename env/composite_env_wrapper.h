// Copyright (c) 2019-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include "rocksdb/env.h"
#include "rocksdb/file_system.h"

namespace ROCKSDB_NAMESPACE {

// The CompositeEnvWrapper class provides an interface that is compatible
// with the old monolithic Env API, and an implementation that wraps around
// the new Env that provides threading and other OS related functionality, and
// the new FileSystem API that provides storage functionality. By
// providing the old Env interface, it allows the rest of RocksDB code to
// be agnostic of whether the underlying Env implementation is a monolithic
// Env or an Env + FileSystem. In the former case, the user will specify
// Options::env only, whereas in the latter case, the user will specify
// Options::env and Options::file_system.

class CompositeSequentialFileWrapper : public SequentialFile {
 public:
  explicit CompositeSequentialFileWrapper(
      std::unique_ptr<FSSequentialFile>& target)
      : target_(std::move(target)) {}

  Status Read(size_t n, Slice* result, char* scratch) override {
    IOOptions io_opts;
    IODebugContext dbg;
    return target_->Read(n, io_opts, result, scratch, &dbg);
  }
  Status Skip(uint64_t n) override { return target_->Skip(n); }
  bool use_direct_io() const override { return target_->use_direct_io(); }
  size_t GetRequiredBufferAlignment() const override {
    return target_->GetRequiredBufferAlignment();
  }
  Status InvalidateCache(size_t offset, size_t length) override {
    return target_->InvalidateCache(offset, length);
  }
  Status PositionedRead(uint64_t offset, size_t n, Slice* result,
                        char* scratch) override {
    IOOptions io_opts;
    IODebugContext dbg;
    return target_->PositionedRead(offset, n, io_opts, result, scratch, &dbg);
  }

 private:
  std::unique_ptr<FSSequentialFile> target_;
};

class CompositeRandomAccessFileWrapper : public RandomAccessFile {
 public:
  explicit CompositeRandomAccessFileWrapper(
      std::unique_ptr<FSRandomAccessFile>& target)
      : target_(std::move(target)) {}

  Status Read(uint64_t offset, size_t n, Slice* result,
              char* scratch) const override {
    IOOptions io_opts;
    IODebugContext dbg;
    return target_->Read(offset, n, io_opts, result, scratch, &dbg);
  }
  Status MultiRead(ReadRequest* reqs, size_t num_reqs) override {
    IOOptions io_opts;
    IODebugContext dbg;
    std::vector<FSReadRequest> fs_reqs;
    Status status;

    fs_reqs.resize(num_reqs);
    for (size_t i = 0; i < num_reqs; ++i) {
      fs_reqs[i].offset = reqs[i].offset;
      fs_reqs[i].len = reqs[i].len;
      fs_reqs[i].scratch = reqs[i].scratch;
      fs_reqs[i].status = IOStatus::OK();
    }
    status = target_->MultiRead(fs_reqs.data(), num_reqs, io_opts, &dbg);
    for (size_t i = 0; i < num_reqs; ++i) {
      reqs[i].result = fs_reqs[i].result;
      reqs[i].status = fs_reqs[i].status;
    }
    return status;
  }
  Status Prefetch(uint64_t offset, size_t n) override {
    IOOptions io_opts;
    IODebugContext dbg;
    return target_->Prefetch(offset, n, io_opts, &dbg);
  }
  size_t GetUniqueId(char* id, size_t max_size) const override {
    return target_->GetUniqueId(id, max_size);
  };
  void Hint(AccessPattern pattern) override {
    target_->Hint((FSRandomAccessFile::AccessPattern)pattern);
  }
  bool use_direct_io() const override { return target_->use_direct_io(); }
  size_t GetRequiredBufferAlignment() const override {
    return target_->GetRequiredBufferAlignment();
  }
  Status InvalidateCache(size_t offset, size_t length) override {
    return target_->InvalidateCache(offset, length);
  }

 private:
  std::unique_ptr<FSRandomAccessFile> target_;
};

class CompositeWritableFileWrapper : public WritableFile {
 public:
  explicit CompositeWritableFileWrapper(std::unique_ptr<FSWritableFile>& t)
      : target_(std::move(t)) {}

  Status Append(const Slice& data) override {
    IOOptions io_opts;
    IODebugContext dbg;
    return target_->Append(data, io_opts, &dbg);
  }
  Status PositionedAppend(const Slice& data, uint64_t offset) override {
    IOOptions io_opts;
    IODebugContext dbg;
    return target_->PositionedAppend(data, offset, io_opts, &dbg);
  }
  Status Truncate(uint64_t size) override {
    IOOptions io_opts;
    IODebugContext dbg;
    return target_->Truncate(size, io_opts, &dbg);
  }
  Status Close() override {
    IOOptions io_opts;
    IODebugContext dbg;
    return target_->Close(io_opts, &dbg);
  }
  Status Flush() override {
    IOOptions io_opts;
    IODebugContext dbg;
    return target_->Flush(io_opts, &dbg);
  }
  Status Sync() override {
    IOOptions io_opts;
    IODebugContext dbg;
    return target_->Sync(io_opts, &dbg);
  }
  Status Fsync() override {
    IOOptions io_opts;
    IODebugContext dbg;
    return target_->Fsync(io_opts, &dbg);
  }
  bool IsSyncThreadSafe() const override { return target_->IsSyncThreadSafe(); }

  bool use_direct_io() const override { return target_->use_direct_io(); }

  size_t GetRequiredBufferAlignment() const override {
    return target_->GetRequiredBufferAlignment();
  }

  void SetWriteLifeTimeHint(Env::WriteLifeTimeHint hint) override {
    target_->SetWriteLifeTimeHint(hint);
  }

  Env::WriteLifeTimeHint GetWriteLifeTimeHint() override {
    return target_->GetWriteLifeTimeHint();
  }

  uint64_t GetFileSize() override {
    IOOptions io_opts;
    IODebugContext dbg;
    return target_->GetFileSize(io_opts, &dbg);
  }

  void SetPreallocationBlockSize(size_t size) override {
    target_->SetPreallocationBlockSize(size);
  }

  void GetPreallocationStatus(size_t* block_size,
                              size_t* last_allocated_block) override {
    target_->GetPreallocationStatus(block_size, last_allocated_block);
  }

  size_t GetUniqueId(char* id, size_t max_size) const override {
    return target_->GetUniqueId(id, max_size);
  }

  Status InvalidateCache(size_t offset, size_t length) override {
    return target_->InvalidateCache(offset, length);
  }

  Status RangeSync(uint64_t offset, uint64_t nbytes) override {
    IOOptions io_opts;
    IODebugContext dbg;
    return target_->RangeSync(offset, nbytes, io_opts, &dbg);
  }

  void PrepareWrite(size_t offset, size_t len) override {
    IOOptions io_opts;
    IODebugContext dbg;
    target_->PrepareWrite(offset, len, io_opts, &dbg);
  }

  Status Allocate(uint64_t offset, uint64_t len) override {
    IOOptions io_opts;
    IODebugContext dbg;
    return target_->Allocate(offset, len, io_opts, &dbg);
  }

  std::unique_ptr<FSWritableFile>* target() { return &target_; }

 private:
  std::unique_ptr<FSWritableFile> target_;
};

class CompositeRandomRWFileWrapper : public RandomRWFile {
 public:
  explicit CompositeRandomRWFileWrapper(std::unique_ptr<FSRandomRWFile>& target)
      : target_(std::move(target)) {}

  bool use_direct_io() const override { return target_->use_direct_io(); }
  size_t GetRequiredBufferAlignment() const override {
    return target_->GetRequiredBufferAlignment();
  }
  Status Write(uint64_t offset, const Slice& data) override {
    IOOptions io_opts;
    IODebugContext dbg;
    return target_->Write(offset, data, io_opts, &dbg);
  }
  Status Read(uint64_t offset, size_t n, Slice* result,
              char* scratch) const override {
    IOOptions io_opts;
    IODebugContext dbg;
    return target_->Read(offset, n, io_opts, result, scratch, &dbg);
  }
  Status Flush() override {
    IOOptions io_opts;
    IODebugContext dbg;
    return target_->Flush(io_opts, &dbg);
  }
  Status Sync() override {
    IOOptions io_opts;
    IODebugContext dbg;
    return target_->Sync(io_opts, &dbg);
  }
  Status Fsync() override {
    IOOptions io_opts;
    IODebugContext dbg;
    return target_->Fsync(io_opts, &dbg);
  }
  Status Close() override {
    IOOptions io_opts;
    IODebugContext dbg;
    return target_->Close(io_opts, &dbg);
  }

 private:
  std::unique_ptr<FSRandomRWFile> target_;
};

class CompositeDirectoryWrapper : public Directory {
 public:
  explicit CompositeDirectoryWrapper(std::unique_ptr<FSDirectory>& target)
      : target_(std::move(target)) {}

  Status Fsync() override {
    IOOptions io_opts;
    IODebugContext dbg;
    return target_->Fsync(io_opts, &dbg);
  }
  size_t GetUniqueId(char* id, size_t max_size) const override {
    return target_->GetUniqueId(id, max_size);
  }

 private:
  std::unique_ptr<FSDirectory> target_;
};

class CompositeEnvWrapper : public Env {
 public:
  // Initialize a CompositeEnvWrapper that delegates all thread/time related
  // calls to env, and all file operations to fs
  explicit CompositeEnvWrapper(Env* env, std::shared_ptr<FileSystem> fs)
      : Env(fs), env_target_(env) {}
  ~CompositeEnvWrapper() {}

  // Return the target to which this Env forwards all calls
  Env* env_target() const { return env_target_; }

  Status RegisterDbPaths(const std::vector<std::string>& paths) override {
    return file_system_->RegisterDbPaths(paths);
  }
  Status UnregisterDbPaths(const std::vector<std::string>& paths) override {
    return file_system_->UnregisterDbPaths(paths);
  }

  // The following text is boilerplate that forwards all methods to target()
  Status NewSequentialFile(const std::string& f,
                           std::unique_ptr<SequentialFile>* r,
                           const EnvOptions& options) override {
    IODebugContext dbg;
    std::unique_ptr<FSSequentialFile> file;
    Status status;
    status =
        file_system_->NewSequentialFile(f, FileOptions(options), &file, &dbg);
    if (status.ok()) {
      r->reset(new CompositeSequentialFileWrapper(file));
    }
    return status;
  }
  Status NewRandomAccessFile(const std::string& f,
                             std::unique_ptr<RandomAccessFile>* r,
                             const EnvOptions& options) override {
    IODebugContext dbg;
    std::unique_ptr<FSRandomAccessFile> file;
    Status status;
    status =
        file_system_->NewRandomAccessFile(f, FileOptions(options), &file, &dbg);
    if (status.ok()) {
      r->reset(new CompositeRandomAccessFileWrapper(file));
    }
    return status;
  }
  Status NewWritableFile(const std::string& f, std::unique_ptr<WritableFile>* r,
                         const EnvOptions& options) override {
    IODebugContext dbg;
    std::unique_ptr<FSWritableFile> file;
    Status status;
    status =
        file_system_->NewWritableFile(f, FileOptions(options), &file, &dbg);
    if (status.ok()) {
      r->reset(new CompositeWritableFileWrapper(file));
    }
    return status;
  }
  Status ReopenWritableFile(const std::string& fname,
                            std::unique_ptr<WritableFile>* result,
                            const EnvOptions& options) override {
    IODebugContext dbg;
    Status status;
    std::unique_ptr<FSWritableFile> file;
    status = file_system_->ReopenWritableFile(fname, FileOptions(options),
                                              &file, &dbg);
    if (status.ok()) {
      result->reset(new CompositeWritableFileWrapper(file));
    }
    return status;
  }
  Status ReuseWritableFile(const std::string& fname,
                           const std::string& old_fname,
                           std::unique_ptr<WritableFile>* r,
                           const EnvOptions& options) override {
    IODebugContext dbg;
    Status status;
    std::unique_ptr<FSWritableFile> file;
    status = file_system_->ReuseWritableFile(fname, old_fname,
                                             FileOptions(options), &file, &dbg);
    if (status.ok()) {
      r->reset(new CompositeWritableFileWrapper(file));
    }
    return status;
  }
  Status NewRandomRWFile(const std::string& fname,
                         std::unique_ptr<RandomRWFile>* result,
                         const EnvOptions& options) override {
    IODebugContext dbg;
    std::unique_ptr<FSRandomRWFile> file;
    Status status;
    status =
        file_system_->NewRandomRWFile(fname, FileOptions(options), &file, &dbg);
    if (status.ok()) {
      result->reset(new CompositeRandomRWFileWrapper(file));
    }
    return status;
  }
  Status NewMemoryMappedFileBuffer(
      const std::string& fname,
      std::unique_ptr<MemoryMappedFileBuffer>* result) override {
    return file_system_->NewMemoryMappedFileBuffer(fname, result);
  }
  Status NewDirectory(const std::string& name,
                      std::unique_ptr<Directory>* result) override {
    IOOptions io_opts;
    IODebugContext dbg;
    std::unique_ptr<FSDirectory> dir;
    Status status;
    status = file_system_->NewDirectory(name, io_opts, &dir, &dbg);
    if (status.ok()) {
      result->reset(new CompositeDirectoryWrapper(dir));
    }
    return status;
  }
  Status FileExists(const std::string& f) override {
    IOOptions io_opts;
    IODebugContext dbg;
    return file_system_->FileExists(f, io_opts, &dbg);
  }
  Status GetChildren(const std::string& dir,
                     std::vector<std::string>* r) override {
    IOOptions io_opts;
    IODebugContext dbg;
    return file_system_->GetChildren(dir, io_opts, r, &dbg);
  }
  Status GetChildrenFileAttributes(
      const std::string& dir, std::vector<FileAttributes>* result) override {
    IOOptions io_opts;
    IODebugContext dbg;
    return file_system_->GetChildrenFileAttributes(dir, io_opts, result, &dbg);
  }
  Status DeleteFile(const std::string& f) override {
    IOOptions io_opts;
    IODebugContext dbg;
    return file_system_->DeleteFile(f, io_opts, &dbg);
  }
  Status Truncate(const std::string& fname, size_t size) override {
    IOOptions io_opts;
    IODebugContext dbg;
    return file_system_->Truncate(fname, size, io_opts, &dbg);
  }
  Status CreateDir(const std::string& d) override {
    IOOptions io_opts;
    IODebugContext dbg;
    return file_system_->CreateDir(d, io_opts, &dbg);
  }
  Status CreateDirIfMissing(const std::string& d) override {
    IOOptions io_opts;
    IODebugContext dbg;
    return file_system_->CreateDirIfMissing(d, io_opts, &dbg);
  }
  Status DeleteDir(const std::string& d) override {
    IOOptions io_opts;
    IODebugContext dbg;
    return file_system_->DeleteDir(d, io_opts, &dbg);
  }
  Status GetFileSize(const std::string& f, uint64_t* s) override {
    IOOptions io_opts;
    IODebugContext dbg;
    return file_system_->GetFileSize(f, io_opts, s, &dbg);
  }

  Status GetFileModificationTime(const std::string& fname,
                                 uint64_t* file_mtime) override {
    IOOptions io_opts;
    IODebugContext dbg;
    return file_system_->GetFileModificationTime(fname, io_opts, file_mtime,
                                                 &dbg);
  }

  Status RenameFile(const std::string& s, const std::string& t) override {
    IOOptions io_opts;
    IODebugContext dbg;
    return file_system_->RenameFile(s, t, io_opts, &dbg);
  }

  Status LinkFile(const std::string& s, const std::string& t) override {
    IOOptions io_opts;
    IODebugContext dbg;
    return file_system_->LinkFile(s, t, io_opts, &dbg);
  }

  Status NumFileLinks(const std::string& fname, uint64_t* count) override {
    IOOptions io_opts;
    IODebugContext dbg;
    return file_system_->NumFileLinks(fname, io_opts, count, &dbg);
  }

  Status AreFilesSame(const std::string& first, const std::string& second,
                      bool* res) override {
    IOOptions io_opts;
    IODebugContext dbg;
    return file_system_->AreFilesSame(first, second, io_opts, res, &dbg);
  }

  Status LockFile(const std::string& f, FileLock** l) override {
    IOOptions io_opts;
    IODebugContext dbg;
    return file_system_->LockFile(f, io_opts, l, &dbg);
  }

  Status UnlockFile(FileLock* l) override {
    IOOptions io_opts;
    IODebugContext dbg;
    return file_system_->UnlockFile(l, io_opts, &dbg);
  }

  Status GetAbsolutePath(const std::string& db_path,
                         std::string* output_path) override {
    IOOptions io_opts;
    IODebugContext dbg;
    return file_system_->GetAbsolutePath(db_path, io_opts, output_path, &dbg);
  }

  Status NewLogger(const std::string& fname,
                   std::shared_ptr<Logger>* result) override {
    IOOptions io_opts;
    IODebugContext dbg;
    return file_system_->NewLogger(fname, io_opts, result, &dbg);
  }

  Status IsDirectory(const std::string& path, bool* is_dir) override {
    IOOptions io_opts;
    IODebugContext dbg;
    return file_system_->IsDirectory(path, io_opts, is_dir, &dbg);
  }

#if !defined(OS_WIN) && !defined(ROCKSDB_NO_DYNAMIC_EXTENSION)
  Status LoadLibrary(const std::string& lib_name,
                     const std::string& search_path,
                     std::shared_ptr<DynamicLibrary>* result) override {
    return env_target_->LoadLibrary(lib_name, search_path, result);
  }
#endif

  void Schedule(void (*f)(void* arg), void* a, Priority pri,
                void* tag = nullptr, void (*u)(void* arg) = nullptr) override {
    return env_target_->Schedule(f, a, pri, tag, u);
  }

  int UnSchedule(void* tag, Priority pri) override {
    return env_target_->UnSchedule(tag, pri);
  }

  void StartThread(void (*f)(void*), void* a) override {
    return env_target_->StartThread(f, a);
  }
  void WaitForJoin() override { return env_target_->WaitForJoin(); }
  unsigned int GetThreadPoolQueueLen(Priority pri = LOW) const override {
    return env_target_->GetThreadPoolQueueLen(pri);
  }
  Status GetTestDirectory(std::string* path) override {
    IOOptions io_opts;
    IODebugContext dbg;
    return file_system_->GetTestDirectory(io_opts, path, &dbg);
  }
  uint64_t NowMicros() override { return env_target_->NowMicros(); }
  uint64_t NowNanos() override { return env_target_->NowNanos(); }
  uint64_t NowCPUNanos() override { return env_target_->NowCPUNanos(); }

  void SleepForMicroseconds(int micros) override {
    env_target_->SleepForMicroseconds(micros);
  }
  Status GetHostName(char* name, uint64_t len) override {
    return env_target_->GetHostName(name, len);
  }
  Status GetCurrentTime(int64_t* unix_time) override {
    return env_target_->GetCurrentTime(unix_time);
  }
  void SetBackgroundThreads(int num, Priority pri) override {
    return env_target_->SetBackgroundThreads(num, pri);
  }
  int GetBackgroundThreads(Priority pri) override {
    return env_target_->GetBackgroundThreads(pri);
  }

  Status SetAllowNonOwnerAccess(bool allow_non_owner_access) override {
    return env_target_->SetAllowNonOwnerAccess(allow_non_owner_access);
  }

  void IncBackgroundThreadsIfNeeded(int num, Priority pri) override {
    return env_target_->IncBackgroundThreadsIfNeeded(num, pri);
  }

  void LowerThreadPoolIOPriority(Priority pool) override {
    env_target_->LowerThreadPoolIOPriority(pool);
  }

  void LowerThreadPoolCPUPriority(Priority pool) override {
    env_target_->LowerThreadPoolCPUPriority(pool);
  }

  Status LowerThreadPoolCPUPriority(Priority pool, CpuPriority pri) override {
    return env_target_->LowerThreadPoolCPUPriority(pool, pri);
  }

  std::string TimeToString(uint64_t time) override {
    return env_target_->TimeToString(time);
  }

  Status GetThreadList(std::vector<ThreadStatus>* thread_list) override {
    return env_target_->GetThreadList(thread_list);
  }

  ThreadStatusUpdater* GetThreadStatusUpdater() const override {
    return env_target_->GetThreadStatusUpdater();
  }

  uint64_t GetThreadID() const override { return env_target_->GetThreadID(); }

  std::string GenerateUniqueId() override {
    return env_target_->GenerateUniqueId();
  }

  EnvOptions OptimizeForLogRead(const EnvOptions& env_options) const override {
    return file_system_->OptimizeForLogRead(FileOptions(env_options));
  }
  EnvOptions OptimizeForManifestRead(
      const EnvOptions& env_options) const override {
    return file_system_->OptimizeForManifestRead(FileOptions(env_options));
  }
  EnvOptions OptimizeForLogWrite(const EnvOptions& env_options,
                                 const DBOptions& db_options) const override {
    return file_system_->OptimizeForLogWrite(FileOptions(env_options),
                                             db_options);
  }
  EnvOptions OptimizeForManifestWrite(
      const EnvOptions& env_options) const override {
    return file_system_->OptimizeForManifestWrite(FileOptions(env_options));
  }
  EnvOptions OptimizeForCompactionTableWrite(
      const EnvOptions& env_options,
      const ImmutableDBOptions& immutable_ops) const override {
    return file_system_->OptimizeForCompactionTableWrite(
        FileOptions(env_options), immutable_ops);
  }
  EnvOptions OptimizeForCompactionTableRead(
      const EnvOptions& env_options,
      const ImmutableDBOptions& db_options) const override {
    return file_system_->OptimizeForCompactionTableRead(
        FileOptions(env_options), db_options);
  }

  // This seems to clash with a macro on Windows, so #undef it here
#ifdef GetFreeSpace
#undef GetFreeSpace
#endif
  Status GetFreeSpace(const std::string& path, uint64_t* diskfree) override {
    IOOptions io_opts;
    IODebugContext dbg;
    return file_system_->GetFreeSpace(path, io_opts, diskfree, &dbg);
  }

 private:
  Env* env_target_;
};

class LegacySequentialFileWrapper : public FSSequentialFile {
 public:
  explicit LegacySequentialFileWrapper(
      std::unique_ptr<SequentialFile>&& _target)
      : target_(std::move(_target)) {}

  IOStatus Read(size_t n, const IOOptions& /*options*/, Slice* result,
                char* scratch, IODebugContext* /*dbg*/) override {
    return status_to_io_status(target_->Read(n, result, scratch));
  }
  IOStatus Skip(uint64_t n) override {
    return status_to_io_status(target_->Skip(n));
  }
  bool use_direct_io() const override { return target_->use_direct_io(); }
  size_t GetRequiredBufferAlignment() const override {
    return target_->GetRequiredBufferAlignment();
  }
  IOStatus InvalidateCache(size_t offset, size_t length) override {
    return status_to_io_status(target_->InvalidateCache(offset, length));
  }
  IOStatus PositionedRead(uint64_t offset, size_t n,
                          const IOOptions& /*options*/, Slice* result,
                          char* scratch, IODebugContext* /*dbg*/) override {
    return status_to_io_status(
        target_->PositionedRead(offset, n, result, scratch));
  }
  SequentialFile* target() { return target_.get(); }

 private:
  std::unique_ptr<SequentialFile> target_;
};

class LegacyRandomAccessFileWrapper : public FSRandomAccessFile {
 public:
  explicit LegacyRandomAccessFileWrapper(
      std::unique_ptr<RandomAccessFile>&& target)
      : target_(std::move(target)) {}

  IOStatus Read(uint64_t offset, size_t n, const IOOptions& /*options*/,
                Slice* result, char* scratch,
                IODebugContext* /*dbg*/) const override {
    return status_to_io_status(target_->Read(offset, n, result, scratch));
  }
  IOStatus MultiRead(FSReadRequest* fs_reqs, size_t num_reqs,
                     const IOOptions& /*options*/,
                     IODebugContext* /*dbg*/) override {
    std::vector<ReadRequest> reqs;
    Status status;

    reqs.reserve(num_reqs);
    for (size_t i = 0; i < num_reqs; ++i) {
      ReadRequest req;

      req.offset = fs_reqs[i].offset;
      req.len = fs_reqs[i].len;
      req.scratch = fs_reqs[i].scratch;
      req.status = Status::OK();

      reqs.emplace_back(req);
    }
    status = target_->MultiRead(reqs.data(), num_reqs);
    for (size_t i = 0; i < num_reqs; ++i) {
      fs_reqs[i].result = reqs[i].result;
      fs_reqs[i].status = status_to_io_status(std::move(reqs[i].status));
    }
    return status_to_io_status(std::move(status));
    ;
  }
  IOStatus Prefetch(uint64_t offset, size_t n, const IOOptions& /*options*/,
                    IODebugContext* /*dbg*/) override {
    return status_to_io_status(target_->Prefetch(offset, n));
  }
  size_t GetUniqueId(char* id, size_t max_size) const override {
    return target_->GetUniqueId(id, max_size);
  };
  void Hint(AccessPattern pattern) override {
    target_->Hint((RandomAccessFile::AccessPattern)pattern);
  }
  bool use_direct_io() const override { return target_->use_direct_io(); }
  size_t GetRequiredBufferAlignment() const override {
    return target_->GetRequiredBufferAlignment();
  }
  IOStatus InvalidateCache(size_t offset, size_t length) override {
    return status_to_io_status(target_->InvalidateCache(offset, length));
  }

 private:
  std::unique_ptr<RandomAccessFile> target_;
};

class LegacyWritableFileWrapper : public FSWritableFile {
 public:
  explicit LegacyWritableFileWrapper(std::unique_ptr<WritableFile>&& _target)
      : target_(std::move(_target)) {}

  IOStatus Append(const Slice& data, const IOOptions& /*options*/,
                  IODebugContext* /*dbg*/) override {
    return status_to_io_status(target_->Append(data));
  }
  IOStatus PositionedAppend(const Slice& data, uint64_t offset,
                            const IOOptions& /*options*/,
                            IODebugContext* /*dbg*/) override {
    return status_to_io_status(target_->PositionedAppend(data, offset));
  }
  IOStatus Truncate(uint64_t size, const IOOptions& /*options*/,
                    IODebugContext* /*dbg*/) override {
    return status_to_io_status(target_->Truncate(size));
  }
  IOStatus Close(const IOOptions& /*options*/,
                 IODebugContext* /*dbg*/) override {
    return status_to_io_status(target_->Close());
  }
  IOStatus Flush(const IOOptions& /*options*/,
                 IODebugContext* /*dbg*/) override {
    return status_to_io_status(target_->Flush());
  }
  IOStatus Sync(const IOOptions& /*options*/,
                IODebugContext* /*dbg*/) override {
    return status_to_io_status(target_->Sync());
  }
  IOStatus Fsync(const IOOptions& /*options*/,
                 IODebugContext* /*dbg*/) override {
    return status_to_io_status(target_->Fsync());
  }
  bool IsSyncThreadSafe() const override { return target_->IsSyncThreadSafe(); }

  bool use_direct_io() const override { return target_->use_direct_io(); }

  size_t GetRequiredBufferAlignment() const override {
    return target_->GetRequiredBufferAlignment();
  }

  void SetWriteLifeTimeHint(Env::WriteLifeTimeHint hint) override {
    target_->SetWriteLifeTimeHint(hint);
  }

  Env::WriteLifeTimeHint GetWriteLifeTimeHint() override {
    return target_->GetWriteLifeTimeHint();
  }

  uint64_t GetFileSize(const IOOptions& /*options*/,
                       IODebugContext* /*dbg*/) override {
    return target_->GetFileSize();
  }

  void SetPreallocationBlockSize(size_t size) override {
    target_->SetPreallocationBlockSize(size);
  }

  void GetPreallocationStatus(size_t* block_size,
                              size_t* last_allocated_block) override {
    target_->GetPreallocationStatus(block_size, last_allocated_block);
  }

  size_t GetUniqueId(char* id, size_t max_size) const override {
    return target_->GetUniqueId(id, max_size);
  }

  IOStatus InvalidateCache(size_t offset, size_t length) override {
    return status_to_io_status(target_->InvalidateCache(offset, length));
  }

  IOStatus RangeSync(uint64_t offset, uint64_t nbytes,
                     const IOOptions& /*options*/,
                     IODebugContext* /*dbg*/) override {
    return status_to_io_status(target_->RangeSync(offset, nbytes));
  }

  void PrepareWrite(size_t offset, size_t len, const IOOptions& /*options*/,
                    IODebugContext* /*dbg*/) override {
    target_->PrepareWrite(offset, len);
  }

  IOStatus Allocate(uint64_t offset, uint64_t len, const IOOptions& /*options*/,
                    IODebugContext* /*dbg*/) override {
    return status_to_io_status(target_->Allocate(offset, len));
  }

  WritableFile* target() { return target_.get(); }

 private:
  std::unique_ptr<WritableFile> target_;
};

class LegacyRandomRWFileWrapper : public FSRandomRWFile {
 public:
  explicit LegacyRandomRWFileWrapper(std::unique_ptr<RandomRWFile>&& target)
      : target_(std::move(target)) {}

  bool use_direct_io() const override { return target_->use_direct_io(); }
  size_t GetRequiredBufferAlignment() const override {
    return target_->GetRequiredBufferAlignment();
  }
  IOStatus Write(uint64_t offset, const Slice& data,
                 const IOOptions& /*options*/,
                 IODebugContext* /*dbg*/) override {
    return status_to_io_status(target_->Write(offset, data));
  }
  IOStatus Read(uint64_t offset, size_t n, const IOOptions& /*options*/,
                Slice* result, char* scratch,
                IODebugContext* /*dbg*/) const override {
    return status_to_io_status(target_->Read(offset, n, result, scratch));
  }
  IOStatus Flush(const IOOptions& /*options*/,
                 IODebugContext* /*dbg*/) override {
    return status_to_io_status(target_->Flush());
  }
  IOStatus Sync(const IOOptions& /*options*/,
                IODebugContext* /*dbg*/) override {
    return status_to_io_status(target_->Sync());
  }
  IOStatus Fsync(const IOOptions& /*options*/,
                 IODebugContext* /*dbg*/) override {
    return status_to_io_status(target_->Fsync());
  }
  IOStatus Close(const IOOptions& /*options*/,
                 IODebugContext* /*dbg*/) override {
    return status_to_io_status(target_->Close());
  }

 private:
  std::unique_ptr<RandomRWFile> target_;
};

class LegacyDirectoryWrapper : public FSDirectory {
 public:
  explicit LegacyDirectoryWrapper(std::unique_ptr<Directory>&& target)
      : target_(std::move(target)) {}

  IOStatus Fsync(const IOOptions& /*options*/,
                 IODebugContext* /*dbg*/) override {
    return status_to_io_status(target_->Fsync());
  }
  size_t GetUniqueId(char* id, size_t max_size) const override {
    return target_->GetUniqueId(id, max_size);
  }

 private:
  std::unique_ptr<Directory> target_;
};

class LegacyFileSystemWrapper : public FileSystem {
 public:
  // Initialize an EnvWrapper that delegates all calls to *t
  explicit LegacyFileSystemWrapper(Env* t) : target_(t) {}
  ~LegacyFileSystemWrapper() override {}

  const char* Name() const override { return "Legacy File System"; }

  // Return the target to which this Env forwards all calls
  Env* target() const { return target_; }

  // The following text is boilerplate that forwards all methods to target()
  IOStatus NewSequentialFile(const std::string& f,
                             const FileOptions& file_opts,
                             std::unique_ptr<FSSequentialFile>* r,
                             IODebugContext* /*dbg*/) override {
    std::unique_ptr<SequentialFile> file;
    Status s = target_->NewSequentialFile(f, &file, file_opts);
    if (s.ok()) {
      r->reset(new LegacySequentialFileWrapper(std::move(file)));
    }
    return status_to_io_status(std::move(s));
  }
  IOStatus NewRandomAccessFile(const std::string& f,
      const FileOptions& file_opts,
                               std::unique_ptr<FSRandomAccessFile>* r,
                               IODebugContext* /*dbg*/) override {
    std::unique_ptr<RandomAccessFile> file;
    Status s = target_->NewRandomAccessFile(f, &file, file_opts);
    if (s.ok()) {
      r->reset(new LegacyRandomAccessFileWrapper(std::move(file)));
    }
    return status_to_io_status(std::move(s));
  }
  IOStatus NewWritableFile(const std::string& f, const FileOptions& file_opts,
                           std::unique_ptr<FSWritableFile>* r,
                           IODebugContext* /*dbg*/) override {
    std::unique_ptr<WritableFile> file;
    Status s = target_->NewWritableFile(f, &file, file_opts);
    if (s.ok()) {
      r->reset(new LegacyWritableFileWrapper(std::move(file)));
    }
    return status_to_io_status(std::move(s));
  }
  IOStatus ReopenWritableFile(const std::string& fname,
                              const FileOptions& file_opts,
                              std::unique_ptr<FSWritableFile>* result,
                              IODebugContext* /*dbg*/) override {
    std::unique_ptr<WritableFile> file;
    Status s = target_->ReopenWritableFile(fname, &file, file_opts);
    if (s.ok()) {
      result->reset(new LegacyWritableFileWrapper(std::move(file)));
    }
    return status_to_io_status(std::move(s));
  }
  IOStatus ReuseWritableFile(const std::string& fname,
                             const std::string& old_fname,
                             const FileOptions& file_opts,
                             std::unique_ptr<FSWritableFile>* r,
                             IODebugContext* /*dbg*/) override {
    std::unique_ptr<WritableFile> file;
    Status s = target_->ReuseWritableFile(fname, old_fname, &file, file_opts);
    if (s.ok()) {
      r->reset(new LegacyWritableFileWrapper(std::move(file)));
    }
    return status_to_io_status(std::move(s));
  }
  IOStatus NewRandomRWFile(const std::string& fname,
      const FileOptions& file_opts,
                           std::unique_ptr<FSRandomRWFile>* result,
                           IODebugContext* /*dbg*/) override {
    std::unique_ptr<RandomRWFile> file;
    Status s = target_->NewRandomRWFile(fname, &file, file_opts);
    if (s.ok()) {
      result->reset(new LegacyRandomRWFileWrapper(std::move(file)));
    }
    return status_to_io_status(std::move(s));
  }
  IOStatus NewMemoryMappedFileBuffer(
      const std::string& fname,
      std::unique_ptr<MemoryMappedFileBuffer>* result) override {
    return status_to_io_status(
        target_->NewMemoryMappedFileBuffer(fname, result));
  }
  IOStatus NewDirectory(const std::string& name, const IOOptions& /*io_opts*/,
                        std::unique_ptr<FSDirectory>* result,
                        IODebugContext* /*dbg*/) override {
    std::unique_ptr<Directory> dir;
    Status s = target_->NewDirectory(name, &dir);
    if (s.ok()) {
      result->reset(new LegacyDirectoryWrapper(std::move(dir)));
    }
    return status_to_io_status(std::move(s));
  }
  IOStatus FileExists(const std::string& f, const IOOptions& /*io_opts*/,
                      IODebugContext* /*dbg*/) override {
    return status_to_io_status(target_->FileExists(f));
  }
  IOStatus GetChildren(const std::string& dir, const IOOptions& /*io_opts*/,
                       std::vector<std::string>* r,
                       IODebugContext* /*dbg*/) override {
    return status_to_io_status(target_->GetChildren(dir, r));
  }
  IOStatus GetChildrenFileAttributes(const std::string& dir,
                                     const IOOptions& /*options*/,
                                     std::vector<FileAttributes>* result,
                                     IODebugContext* /*dbg*/) override {
    return status_to_io_status(target_->GetChildrenFileAttributes(dir, result));
  }
  IOStatus DeleteFile(const std::string& f, const IOOptions& /*options*/,
                      IODebugContext* /*dbg*/) override {
    return status_to_io_status(target_->DeleteFile(f));
  }
  IOStatus Truncate(const std::string& fname, size_t size,
                    const IOOptions& /*options*/,
                    IODebugContext* /*dbg*/) override {
    return status_to_io_status(target_->Truncate(fname, size));
  }
  IOStatus CreateDir(const std::string& d, const IOOptions& /*options*/,
                     IODebugContext* /*dbg*/) override {
    return status_to_io_status(target_->CreateDir(d));
  }
  IOStatus CreateDirIfMissing(const std::string& d,
                              const IOOptions& /*options*/,
                              IODebugContext* /*dbg*/) override {
    return status_to_io_status(target_->CreateDirIfMissing(d));
  }
  IOStatus DeleteDir(const std::string& d, const IOOptions& /*options*/,
                     IODebugContext* /*dbg*/) override {
    return status_to_io_status(target_->DeleteDir(d));
  }
  IOStatus GetFileSize(const std::string& f, const IOOptions& /*options*/,
                       uint64_t* s, IODebugContext* /*dbg*/) override {
    return status_to_io_status(target_->GetFileSize(f, s));
  }

  IOStatus GetFileModificationTime(const std::string& fname,
                                   const IOOptions& /*options*/,
                                   uint64_t* file_mtime,
                                   IODebugContext* /*dbg*/) override {
    return status_to_io_status(
        target_->GetFileModificationTime(fname, file_mtime));
  }

  IOStatus GetAbsolutePath(const std::string& db_path,
                           const IOOptions& /*options*/,
                           std::string* output_path,
                           IODebugContext* /*dbg*/) override {
    return status_to_io_status(target_->GetAbsolutePath(db_path, output_path));
  }

  IOStatus RenameFile(const std::string& s, const std::string& t,
                      const IOOptions& /*options*/,
                      IODebugContext* /*dbg*/) override {
    return status_to_io_status(target_->RenameFile(s, t));
  }

  IOStatus LinkFile(const std::string& s, const std::string& t,
                    const IOOptions& /*options*/,
                    IODebugContext* /*dbg*/) override {
    return status_to_io_status(target_->LinkFile(s, t));
  }

  IOStatus NumFileLinks(const std::string& fname, const IOOptions& /*options*/,
                        uint64_t* count, IODebugContext* /*dbg*/) override {
    return status_to_io_status(target_->NumFileLinks(fname, count));
  }

  IOStatus AreFilesSame(const std::string& first, const std::string& second,
                        const IOOptions& /*options*/, bool* res,
                        IODebugContext* /*dbg*/) override {
    return status_to_io_status(target_->AreFilesSame(first, second, res));
  }

  IOStatus LockFile(const std::string& f, const IOOptions& /*options*/,
                    FileLock** l, IODebugContext* /*dbg*/) override {
    return status_to_io_status(target_->LockFile(f, l));
  }

  IOStatus UnlockFile(FileLock* l, const IOOptions& /*options*/,
                      IODebugContext* /*dbg*/) override {
    return status_to_io_status(target_->UnlockFile(l));
  }

  IOStatus GetTestDirectory(const IOOptions& /*options*/, std::string* path,
                            IODebugContext* /*dbg*/) override {
    return status_to_io_status(target_->GetTestDirectory(path));
  }
  IOStatus NewLogger(const std::string& fname, const IOOptions& /*options*/,
                     std::shared_ptr<Logger>* result,
                     IODebugContext* /*dbg*/) override {
    return status_to_io_status(target_->NewLogger(fname, result));
  }

  void SanitizeFileOptions(FileOptions* opts) const override {
    target_->SanitizeEnvOptions(opts);
  }

  FileOptions OptimizeForLogRead(
                  const FileOptions& file_options) const override {
    return target_->OptimizeForLogRead(file_options);
  }
  FileOptions OptimizeForManifestRead(
      const FileOptions& file_options) const override {
    return target_->OptimizeForManifestRead(file_options);
  }
  FileOptions OptimizeForLogWrite(const FileOptions& file_options,
                                 const DBOptions& db_options) const override {
    return target_->OptimizeForLogWrite(file_options, db_options);
  }
  FileOptions OptimizeForManifestWrite(
      const FileOptions& file_options) const override {
    return target_->OptimizeForManifestWrite(file_options);
  }
  FileOptions OptimizeForCompactionTableWrite(
      const FileOptions& file_options,
      const ImmutableDBOptions& immutable_ops) const override {
    return target_->OptimizeForCompactionTableWrite(file_options,
                                                     immutable_ops);
  }
  FileOptions OptimizeForCompactionTableRead(
      const FileOptions& file_options,
      const ImmutableDBOptions& db_options) const override {
    return target_->OptimizeForCompactionTableRead(file_options, db_options);
  }

// This seems to clash with a macro on Windows, so #undef it here
#ifdef GetFreeSpace
#undef GetFreeSpace
#endif
  IOStatus GetFreeSpace(const std::string& path, const IOOptions& /*options*/,
                        uint64_t* diskfree, IODebugContext* /*dbg*/) override {
    return status_to_io_status(target_->GetFreeSpace(path, diskfree));
  }
  IOStatus IsDirectory(const std::string& path, const IOOptions& /*options*/,
                       bool* is_dir, IODebugContext* /*dbg*/) override {
    return status_to_io_status(target_->IsDirectory(path, is_dir));
  }

 private:
  Env* target_;
};

inline std::unique_ptr<FSSequentialFile> NewLegacySequentialFileWrapper(
    std::unique_ptr<SequentialFile>& file) {
  return std::unique_ptr<FSSequentialFile>(
      new LegacySequentialFileWrapper(std::move(file)));
}

inline std::unique_ptr<FSRandomAccessFile> NewLegacyRandomAccessFileWrapper(
    std::unique_ptr<RandomAccessFile>& file) {
  return std::unique_ptr<FSRandomAccessFile>(
      new LegacyRandomAccessFileWrapper(std::move(file)));
}

inline std::unique_ptr<FSWritableFile> NewLegacyWritableFileWrapper(
    std::unique_ptr<WritableFile>&& file) {
  return std::unique_ptr<FSWritableFile>(
      new LegacyWritableFileWrapper(std::move(file)));
}

}  // namespace ROCKSDB_NAMESPACE

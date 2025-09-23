// Copyright (c) 2019-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include "rocksdb/env.h"
#include "rocksdb/file_system.h"
#include "rocksdb/system_clock.h"

#ifdef _WIN32
// Windows API macro interference
#undef DeleteFile
#undef GetCurrentTime
#undef LoadLibrary
#endif

namespace ROCKSDB_NAMESPACE {

// This class supports abstracting different types of an `Env`'s functionality
// into separate interfaces. It is constructed with a `FileSystem` and a
// `SystemClock` and delegates:
// * File system operations to member `file_system_`.
// * Time related misc operations to member `clock_`.
// A subclass needs to inherit `CompositeEnv` and provide implementations for
// the thread management related APIs.
class CompositeEnv : public Env {
 public:
  // Initialize a CompositeEnvWrapper that delegates all thread/time related
  // calls to env, and all file operations to fs
  explicit CompositeEnv(const std::shared_ptr<FileSystem>& fs,
                        const std::shared_ptr<SystemClock>& clock)
      : Env(fs, clock) {}

  Status RegisterDbPaths(const std::vector<std::string>& paths) override {
    return file_system_->RegisterDbPaths(paths);
  }
  Status UnregisterDbPaths(const std::vector<std::string>& paths) override {
    return file_system_->UnregisterDbPaths(paths);
  }

  // The following text is boilerplate that forwards all methods to target()
  Status NewSequentialFile(const std::string& f,
                           std::unique_ptr<SequentialFile>* r,
                           const EnvOptions& options) override;

  Status NewRandomAccessFile(const std::string& f,
                             std::unique_ptr<RandomAccessFile>* r,
                             const EnvOptions& options) override;

  Status NewWritableFile(const std::string& f, std::unique_ptr<WritableFile>* r,
                         const EnvOptions& options) override;

  Status ReopenWritableFile(const std::string& fname,
                            std::unique_ptr<WritableFile>* result,
                            const EnvOptions& options) override;

  Status ReuseWritableFile(const std::string& fname,
                           const std::string& old_fname,
                           std::unique_ptr<WritableFile>* r,
                           const EnvOptions& options) override;

  Status NewRandomRWFile(const std::string& fname,
                         std::unique_ptr<RandomRWFile>* result,
                         const EnvOptions& options) override;

  Status NewMemoryMappedFileBuffer(
      const std::string& fname,
      std::unique_ptr<MemoryMappedFileBuffer>* result) override {
    return file_system_->NewMemoryMappedFileBuffer(fname, result);
  }

  Status NewDirectory(const std::string& name,
                      std::unique_ptr<Directory>* result) override;

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

  Status GetTestDirectory(std::string* path) override {
    IOOptions io_opts;
    IODebugContext dbg;
    return file_system_->GetTestDirectory(io_opts, path, &dbg);
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
  EnvOptions OptimizeForBlobFileRead(
      const EnvOptions& env_options,
      const ImmutableDBOptions& db_options) const override {
    return file_system_->OptimizeForBlobFileRead(FileOptions(env_options),
                                                 db_options);
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
  uint64_t NowMicros() override { return system_clock_->NowMicros(); }
  uint64_t NowNanos() override { return system_clock_->NowNanos(); }

  uint64_t NowCPUNanos() override { return system_clock_->CPUNanos(); }

  void SleepForMicroseconds(int micros) override {
    system_clock_->SleepForMicroseconds(micros);
  }

  Status GetCurrentTime(int64_t* unix_time) override {
    return system_clock_->GetCurrentTime(unix_time);
  }
  std::string TimeToString(uint64_t time) override {
    return system_clock_->TimeToString(time);
  }
};

// A `CompositeEnvWrapper` is constructed with a target `Env` object, an
// optional `FileSystem` object and an optional `SystemClock` object.
// `Env::GetFileSystem()` is a fallback file system if no such object is
// explicitly provided. Similarly, `Env::GetSystemClock()` is a fallback system
// clock.
// Besides delegating corresponding functionality to `file_system_` and `clock_`
// which is inherited from `CompositeEnv`, it also implements the thread
// management APIs by delegating them to the target `Env` object.
//
// Effectively, this class helps to support using customized file system
// implementations such as a remote file system instead of the default file
// system provided by the operating system.
//
// Also see public API `NewCompositeEnv` in rocksdb/include/env.h
class CompositeEnvWrapper : public CompositeEnv {
 public:
  // Initialize a CompositeEnvWrapper that delegates all thread/time related
  // calls to env, and all file operations to fs
  explicit CompositeEnvWrapper(Env* env)
      : CompositeEnvWrapper(env, env->GetFileSystem(), env->GetSystemClock()) {}
  explicit CompositeEnvWrapper(Env* env, const std::shared_ptr<FileSystem>& fs)
      : CompositeEnvWrapper(env, fs, env->GetSystemClock()) {}

  explicit CompositeEnvWrapper(Env* env, const std::shared_ptr<SystemClock>& sc)
      : CompositeEnvWrapper(env, env->GetFileSystem(), sc) {}

  explicit CompositeEnvWrapper(Env* env, const std::shared_ptr<FileSystem>& fs,
                               const std::shared_ptr<SystemClock>& sc);

  explicit CompositeEnvWrapper(const std::shared_ptr<Env>& env,
                               const std::shared_ptr<FileSystem>& fs)
      : CompositeEnvWrapper(env, fs, env->GetSystemClock()) {}

  explicit CompositeEnvWrapper(const std::shared_ptr<Env>& env,
                               const std::shared_ptr<SystemClock>& sc)
      : CompositeEnvWrapper(env, env->GetFileSystem(), sc) {}

  explicit CompositeEnvWrapper(const std::shared_ptr<Env>& env,
                               const std::shared_ptr<FileSystem>& fs,
                               const std::shared_ptr<SystemClock>& sc);

  static const char* kClassName() { return "CompositeEnv"; }
  const char* Name() const override { return kClassName(); }
  bool IsInstanceOf(const std::string& name) const override {
    if (name == kClassName()) {
      return true;
    } else {
      return CompositeEnv::IsInstanceOf(name);
    }
  }
  const Customizable* Inner() const override { return target_.env; }

  Status PrepareOptions(const ConfigOptions& options) override;
  std::string SerializeOptions(const ConfigOptions& config_options,
                               const std::string& header) const override;

  // Return the target to which this Env forwards all calls
  Env* env_target() const { return target_.env; }

#if !defined(OS_WIN) && !defined(ROCKSDB_NO_DYNAMIC_EXTENSION)
  Status LoadLibrary(const std::string& lib_name,
                     const std::string& search_path,
                     std::shared_ptr<DynamicLibrary>* result) override {
    return target_.env->LoadLibrary(lib_name, search_path, result);
  }
#endif

  void Schedule(void (*f)(void* arg), void* a, Priority pri,
                void* tag = nullptr, void (*u)(void* arg) = nullptr) override {
    return target_.env->Schedule(f, a, pri, tag, u);
  }

  int UnSchedule(void* tag, Priority pri) override {
    return target_.env->UnSchedule(tag, pri);
  }

  void StartThread(void (*f)(void*), void* a) override {
    return target_.env->StartThread(f, a);
  }
  void WaitForJoin() override { return target_.env->WaitForJoin(); }
  unsigned int GetThreadPoolQueueLen(Priority pri = LOW) const override {
    return target_.env->GetThreadPoolQueueLen(pri);
  }

  int ReserveThreads(int threads_to_be_reserved, Priority pri) override {
    return target_.env->ReserveThreads(threads_to_be_reserved, pri);
  }

  int ReleaseThreads(int threads_to_be_released, Priority pri) override {
    return target_.env->ReleaseThreads(threads_to_be_released, pri);
  }

  Status GetHostName(char* name, uint64_t len) override {
    return target_.env->GetHostName(name, len);
  }
  void SetBackgroundThreads(int num, Priority pri) override {
    return target_.env->SetBackgroundThreads(num, pri);
  }
  int GetBackgroundThreads(Priority pri) override {
    return target_.env->GetBackgroundThreads(pri);
  }

  Status SetAllowNonOwnerAccess(bool allow_non_owner_access) override {
    return target_.env->SetAllowNonOwnerAccess(allow_non_owner_access);
  }

  void IncBackgroundThreadsIfNeeded(int num, Priority pri) override {
    return target_.env->IncBackgroundThreadsIfNeeded(num, pri);
  }

  void LowerThreadPoolIOPriority(Priority pool) override {
    target_.env->LowerThreadPoolIOPriority(pool);
  }

  void LowerThreadPoolCPUPriority(Priority pool) override {
    target_.env->LowerThreadPoolCPUPriority(pool);
  }

  Status LowerThreadPoolCPUPriority(Priority pool, CpuPriority pri) override {
    return target_.env->LowerThreadPoolCPUPriority(pool, pri);
  }

  Status GetThreadList(std::vector<ThreadStatus>* thread_list) override {
    return target_.env->GetThreadList(thread_list);
  }

  ThreadStatusUpdater* GetThreadStatusUpdater() const override {
    return target_.env->GetThreadStatusUpdater();
  }

  uint64_t GetThreadID() const override { return target_.env->GetThreadID(); }

  std::string GenerateUniqueId() override {
    return target_.env->GenerateUniqueId();
  }

 private:
  EnvWrapper::Target target_;
};
}  // namespace ROCKSDB_NAMESPACE

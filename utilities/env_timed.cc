// Copyright (c) 2017-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
#include "utilities/env_timed.h"

#include "env/composite_env_wrapper.h"
#include "monitoring/perf_context_imp.h"
#include "rocksdb/env.h"
#include "rocksdb/file_system.h"
#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {

TimedFileSystem::TimedFileSystem(const std::shared_ptr<FileSystem>& base)
    : FileSystemWrapper(base) {}
IOStatus TimedFileSystem::NewSequentialFile(
    const std::string& fname, const FileOptions& options,
    std::unique_ptr<FSSequentialFile>* result, IODebugContext* dbg) {
  PERF_TIMER_GUARD(env_new_sequential_file_nanos);
  return FileSystemWrapper::NewSequentialFile(fname, options, result, dbg);
}

IOStatus TimedFileSystem::NewRandomAccessFile(
    const std::string& fname, const FileOptions& options,
    std::unique_ptr<FSRandomAccessFile>* result, IODebugContext* dbg) {
  PERF_TIMER_GUARD(env_new_random_access_file_nanos);
  return FileSystemWrapper::NewRandomAccessFile(fname, options, result, dbg);
}

IOStatus TimedFileSystem::NewWritableFile(
    const std::string& fname, const FileOptions& options,
    std::unique_ptr<FSWritableFile>* result, IODebugContext* dbg) {
  PERF_TIMER_GUARD(env_new_writable_file_nanos);
  return FileSystemWrapper::NewWritableFile(fname, options, result, dbg);
}

IOStatus TimedFileSystem::ReuseWritableFile(
    const std::string& fname, const std::string& old_fname,
    const FileOptions& options, std::unique_ptr<FSWritableFile>* result,
    IODebugContext* dbg) {
  PERF_TIMER_GUARD(env_reuse_writable_file_nanos);
  return FileSystemWrapper::ReuseWritableFile(fname, old_fname, options, result,
                                              dbg);
}

IOStatus TimedFileSystem::NewRandomRWFile(
    const std::string& fname, const FileOptions& options,
    std::unique_ptr<FSRandomRWFile>* result, IODebugContext* dbg) {
  PERF_TIMER_GUARD(env_new_random_rw_file_nanos);
  return FileSystemWrapper::NewRandomRWFile(fname, options, result, dbg);
}

IOStatus TimedFileSystem::NewDirectory(const std::string& name,
                                       const IOOptions& options,
                                       std::unique_ptr<FSDirectory>* result,
                                       IODebugContext* dbg) {
  PERF_TIMER_GUARD(env_new_directory_nanos);
  return FileSystemWrapper::NewDirectory(name, options, result, dbg);
}

IOStatus TimedFileSystem::FileExists(const std::string& fname,
                                     const IOOptions& options,
                                     IODebugContext* dbg) {
  PERF_TIMER_GUARD(env_file_exists_nanos);
  return FileSystemWrapper::FileExists(fname, options, dbg);
}

IOStatus TimedFileSystem::GetChildren(const std::string& dir,
                                      const IOOptions& options,
                                      std::vector<std::string>* result,
                                      IODebugContext* dbg) {
  PERF_TIMER_GUARD(env_get_children_nanos);
  return FileSystemWrapper::GetChildren(dir, options, result, dbg);
}

IOStatus TimedFileSystem::GetChildrenFileAttributes(
    const std::string& dir, const IOOptions& options,
    std::vector<FileAttributes>* result, IODebugContext* dbg) {
  PERF_TIMER_GUARD(env_get_children_file_attributes_nanos);
  return FileSystemWrapper::GetChildrenFileAttributes(dir, options, result,
                                                      dbg);
}

IOStatus TimedFileSystem::DeleteFile(const std::string& fname,
                                     const IOOptions& options,
                                     IODebugContext* dbg) {
  PERF_TIMER_GUARD(env_delete_file_nanos);
  return FileSystemWrapper::DeleteFile(fname, options, dbg);
}

IOStatus TimedFileSystem::CreateDir(const std::string& dirname,
                                    const IOOptions& options,
                                    IODebugContext* dbg) {
  PERF_TIMER_GUARD(env_create_dir_nanos);
  return FileSystemWrapper::CreateDir(dirname, options, dbg);
}

IOStatus TimedFileSystem::CreateDirIfMissing(const std::string& dirname,
                                             const IOOptions& options,
                                             IODebugContext* dbg) {
  PERF_TIMER_GUARD(env_create_dir_if_missing_nanos);
  return FileSystemWrapper::CreateDirIfMissing(dirname, options, dbg);
}

IOStatus TimedFileSystem::DeleteDir(const std::string& dirname,
                                    const IOOptions& options,
                                    IODebugContext* dbg) {
  PERF_TIMER_GUARD(env_delete_dir_nanos);
  return FileSystemWrapper::DeleteDir(dirname, options, dbg);
}

IOStatus TimedFileSystem::GetFileSize(const std::string& fname,
                                      const IOOptions& options,
                                      uint64_t* file_size,
                                      IODebugContext* dbg) {
  PERF_TIMER_GUARD(env_get_file_size_nanos);
  return FileSystemWrapper::GetFileSize(fname, options, file_size, dbg);
}

IOStatus TimedFileSystem::GetFileModificationTime(const std::string& fname,
                                                  const IOOptions& options,
                                                  uint64_t* file_mtime,
                                                  IODebugContext* dbg) {
  PERF_TIMER_GUARD(env_get_file_modification_time_nanos);
  return FileSystemWrapper::GetFileModificationTime(fname, options, file_mtime,
                                                    dbg);
}

IOStatus TimedFileSystem::RenameFile(const std::string& src,
                                     const std::string& dst,
                                     const IOOptions& options,
                                     IODebugContext* dbg) {
  PERF_TIMER_GUARD(env_rename_file_nanos);
  return FileSystemWrapper::RenameFile(src, dst, options, dbg);
}

IOStatus TimedFileSystem::LinkFile(const std::string& src,
                                   const std::string& dst,
                                   const IOOptions& options,
                                   IODebugContext* dbg) {
  PERF_TIMER_GUARD(env_link_file_nanos);
  return FileSystemWrapper::LinkFile(src, dst, options, dbg);
}

IOStatus TimedFileSystem::LockFile(const std::string& fname,
                                   const IOOptions& options, FileLock** lock,
                                   IODebugContext* dbg) {
  PERF_TIMER_GUARD(env_lock_file_nanos);
  return FileSystemWrapper::LockFile(fname, options, lock, dbg);
}

IOStatus TimedFileSystem::UnlockFile(FileLock* lock, const IOOptions& options,
                                     IODebugContext* dbg) {
  PERF_TIMER_GUARD(env_unlock_file_nanos);
  return FileSystemWrapper::UnlockFile(lock, options, dbg);
}

IOStatus TimedFileSystem::NewLogger(const std::string& fname,
                                    const IOOptions& options,
                                    std::shared_ptr<Logger>* result,
                                    IODebugContext* dbg) {
  PERF_TIMER_GUARD(env_new_logger_nanos);
  return FileSystemWrapper::NewLogger(fname, options, result, dbg);
}

std::shared_ptr<FileSystem> NewTimedFileSystem(
    const std::shared_ptr<FileSystem>& base) {
  return std::make_shared<TimedFileSystem>(base);
}

// An environment that measures function call times for filesystem
// operations, reporting results to variables in PerfContext.
Env* NewTimedEnv(Env* base_env) {
  std::shared_ptr<FileSystem> timed_fs =
      NewTimedFileSystem(base_env->GetFileSystem());
  return new CompositeEnvWrapper(base_env, timed_fs);
}

}  // namespace ROCKSDB_NAMESPACE

// Copyright (c) 2019-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//

#pragma once
#include "rocksdb/file_system.h"
namespace ROCKSDB_NAMESPACE {
#ifndef ROCKSDB_LITE
class TimedFileSystem : public FileSystemWrapper {
 public:
  explicit TimedFileSystem(const std::shared_ptr<FileSystem>& base);

  static const char* kClassName() { return "TimedFS"; }
  const char* Name() const override { return kClassName(); }

  IOStatus NewSequentialFile(const std::string& fname,
                             const FileOptions& options,
                             std::unique_ptr<FSSequentialFile>* result,
                             IODebugContext* dbg) override;

  IOStatus NewRandomAccessFile(const std::string& fname,
                               const FileOptions& options,
                               std::unique_ptr<FSRandomAccessFile>* result,
                               IODebugContext* dbg) override;

  IOStatus NewWritableFile(const std::string& fname, const FileOptions& options,
                           std::unique_ptr<FSWritableFile>* result,
                           IODebugContext* dbg) override;

  IOStatus ReuseWritableFile(const std::string& fname,
                             const std::string& old_fname,
                             const FileOptions& options,
                             std::unique_ptr<FSWritableFile>* result,
                             IODebugContext* dbg) override;

  IOStatus NewRandomRWFile(const std::string& fname, const FileOptions& options,
                           std::unique_ptr<FSRandomRWFile>* result,
                           IODebugContext* dbg) override;

  IOStatus NewDirectory(const std::string& name, const IOOptions& options,
                        std::unique_ptr<FSDirectory>* result,
                        IODebugContext* dbg) override;

  IOStatus FileExists(const std::string& fname, const IOOptions& options,
                      IODebugContext* dbg) override;

  IOStatus GetChildren(const std::string& dir, const IOOptions& options,
                       std::vector<std::string>* result,
                       IODebugContext* dbg) override;

  IOStatus GetChildrenFileAttributes(const std::string& dir,
                                     const IOOptions& options,
                                     std::vector<FileAttributes>* result,
                                     IODebugContext* dbg) override;

  IOStatus DeleteFile(const std::string& fname, const IOOptions& options,
                      IODebugContext* dbg) override;

  IOStatus CreateDir(const std::string& dirname, const IOOptions& options,
                     IODebugContext* dbg) override;

  IOStatus CreateDirIfMissing(const std::string& dirname,
                              const IOOptions& options,
                              IODebugContext* dbg) override;

  IOStatus DeleteDir(const std::string& dirname, const IOOptions& options,
                     IODebugContext* dbg) override;

  IOStatus GetFileSize(const std::string& fname, const IOOptions& options,
                       uint64_t* file_size, IODebugContext* dbg) override;

  IOStatus GetFileModificationTime(const std::string& fname,
                                   const IOOptions& options,
                                   uint64_t* file_mtime,
                                   IODebugContext* dbg) override;

  IOStatus RenameFile(const std::string& src, const std::string& dst,
                      const IOOptions& options, IODebugContext* dbg) override;

  IOStatus LinkFile(const std::string& src, const std::string& dst,
                    const IOOptions& options, IODebugContext* dbg) override;

  IOStatus LockFile(const std::string& fname, const IOOptions& options,
                    FileLock** lock, IODebugContext* dbg) override;

  IOStatus UnlockFile(FileLock* lock, const IOOptions& options,
                      IODebugContext* dbg) override;

  IOStatus NewLogger(const std::string& fname, const IOOptions& options,
                     std::shared_ptr<Logger>* result,
                     IODebugContext* dbg) override;
};

#endif  // ROCKSDB_LITE
}  // namespace ROCKSDB_NAMESPACE

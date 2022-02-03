//  Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#ifndef ROCKSDB_LITE

#include "env/fs_remap.h"

namespace ROCKSDB_NAMESPACE {

RemapFileSystem::RemapFileSystem(const std::shared_ptr<FileSystem>& base)
    : FileSystemWrapper(base) {}

std::pair<IOStatus, std::string> RemapFileSystem::EncodePathWithNewBasename(
    const std::string& path) {
  // No difference by default
  return EncodePath(path);
}

Status RemapFileSystem::RegisterDbPaths(const std::vector<std::string>& paths) {
  std::vector<std::string> encoded_paths;
  encoded_paths.reserve(paths.size());
  for (auto& path : paths) {
    auto status_and_enc_path = EncodePathWithNewBasename(path);
    if (!status_and_enc_path.first.ok()) {
      return status_and_enc_path.first;
    }
    encoded_paths.emplace_back(status_and_enc_path.second);
  }
  return FileSystemWrapper::RegisterDbPaths(encoded_paths);
}

Status RemapFileSystem::UnregisterDbPaths(
    const std::vector<std::string>& paths) {
  std::vector<std::string> encoded_paths;
  encoded_paths.reserve(paths.size());
  for (auto& path : paths) {
    auto status_and_enc_path = EncodePathWithNewBasename(path);
    if (!status_and_enc_path.first.ok()) {
      return status_and_enc_path.first;
    }
    encoded_paths.emplace_back(status_and_enc_path.second);
  }
  return FileSystemWrapper::UnregisterDbPaths(encoded_paths);
}

IOStatus RemapFileSystem::NewSequentialFile(
    const std::string& fname, const FileOptions& options,
    std::unique_ptr<FSSequentialFile>* result, IODebugContext* dbg) {
  auto status_and_enc_path = EncodePathWithNewBasename(fname);
  if (!status_and_enc_path.first.ok()) {
    return status_and_enc_path.first;
  }
  return FileSystemWrapper::NewSequentialFile(status_and_enc_path.second,
                                              options, result, dbg);
}

IOStatus RemapFileSystem::NewRandomAccessFile(
    const std::string& fname, const FileOptions& options,
    std::unique_ptr<FSRandomAccessFile>* result, IODebugContext* dbg) {
  auto status_and_enc_path = EncodePathWithNewBasename(fname);
  if (!status_and_enc_path.first.ok()) {
    return status_and_enc_path.first;
  }
  return FileSystemWrapper::NewRandomAccessFile(status_and_enc_path.second,
                                                options, result, dbg);
}

IOStatus RemapFileSystem::NewWritableFile(
    const std::string& fname, const FileOptions& options,
    std::unique_ptr<FSWritableFile>* result, IODebugContext* dbg) {
  auto status_and_enc_path = EncodePathWithNewBasename(fname);
  if (!status_and_enc_path.first.ok()) {
    return status_and_enc_path.first;
  }
  return FileSystemWrapper::NewWritableFile(status_and_enc_path.second, options,
                                            result, dbg);
}

IOStatus RemapFileSystem::ReuseWritableFile(
    const std::string& fname, const std::string& old_fname,
    const FileOptions& options, std::unique_ptr<FSWritableFile>* result,
    IODebugContext* dbg) {
  auto status_and_enc_path = EncodePathWithNewBasename(fname);
  if (!status_and_enc_path.first.ok()) {
    return status_and_enc_path.first;
  }
  auto status_and_old_enc_path = EncodePath(old_fname);
  if (!status_and_old_enc_path.first.ok()) {
    return status_and_old_enc_path.first;
  }
  return FileSystemWrapper::ReuseWritableFile(status_and_old_enc_path.second,
                                              status_and_old_enc_path.second,
                                              options, result, dbg);
}

IOStatus RemapFileSystem::NewRandomRWFile(
    const std::string& fname, const FileOptions& options,
    std::unique_ptr<FSRandomRWFile>* result, IODebugContext* dbg) {
  auto status_and_enc_path = EncodePathWithNewBasename(fname);
  if (!status_and_enc_path.first.ok()) {
    return status_and_enc_path.first;
  }
  return FileSystemWrapper::NewRandomRWFile(status_and_enc_path.second, options,
                                            result, dbg);
}

IOStatus RemapFileSystem::NewDirectory(const std::string& dir,
                                       const IOOptions& options,
                                       std::unique_ptr<FSDirectory>* result,
                                       IODebugContext* dbg) {
  auto status_and_enc_path = EncodePathWithNewBasename(dir);
  if (!status_and_enc_path.first.ok()) {
    return status_and_enc_path.first;
  }
  return FileSystemWrapper::NewDirectory(status_and_enc_path.second, options,
                                         result, dbg);
}

IOStatus RemapFileSystem::FileExists(const std::string& fname,
                                     const IOOptions& options,
                                     IODebugContext* dbg) {
  auto status_and_enc_path = EncodePathWithNewBasename(fname);
  if (!status_and_enc_path.first.ok()) {
    return status_and_enc_path.first;
  }
  return FileSystemWrapper::FileExists(status_and_enc_path.second, options,
                                       dbg);
}

IOStatus RemapFileSystem::GetChildren(const std::string& dir,
                                      const IOOptions& options,
                                      std::vector<std::string>* result,
                                      IODebugContext* dbg) {
  auto status_and_enc_path = EncodePath(dir);
  if (!status_and_enc_path.first.ok()) {
    return status_and_enc_path.first;
  }
  return FileSystemWrapper::GetChildren(status_and_enc_path.second, options,
                                        result, dbg);
}

IOStatus RemapFileSystem::GetChildrenFileAttributes(
    const std::string& dir, const IOOptions& options,
    std::vector<FileAttributes>* result, IODebugContext* dbg) {
  auto status_and_enc_path = EncodePath(dir);
  if (!status_and_enc_path.first.ok()) {
    return status_and_enc_path.first;
  }
  return FileSystemWrapper::GetChildrenFileAttributes(
      status_and_enc_path.second, options, result, dbg);
}

IOStatus RemapFileSystem::DeleteFile(const std::string& fname,
                                     const IOOptions& options,
                                     IODebugContext* dbg) {
  auto status_and_enc_path = EncodePath(fname);
  if (!status_and_enc_path.first.ok()) {
    return status_and_enc_path.first;
  }
  return FileSystemWrapper::DeleteFile(status_and_enc_path.second, options,
                                       dbg);
}

IOStatus RemapFileSystem::CreateDir(const std::string& dirname,
                                    const IOOptions& options,
                                    IODebugContext* dbg) {
  auto status_and_enc_path = EncodePathWithNewBasename(dirname);
  if (!status_and_enc_path.first.ok()) {
    return status_and_enc_path.first;
  }
  return FileSystemWrapper::CreateDir(status_and_enc_path.second, options, dbg);
}

IOStatus RemapFileSystem::CreateDirIfMissing(const std::string& dirname,
                                             const IOOptions& options,
                                             IODebugContext* dbg) {
  auto status_and_enc_path = EncodePathWithNewBasename(dirname);
  if (!status_and_enc_path.first.ok()) {
    return status_and_enc_path.first;
  }
  return FileSystemWrapper::CreateDirIfMissing(status_and_enc_path.second,
                                               options, dbg);
}

IOStatus RemapFileSystem::DeleteDir(const std::string& dirname,
                                    const IOOptions& options,
                                    IODebugContext* dbg) {
  auto status_and_enc_path = EncodePath(dirname);
  if (!status_and_enc_path.first.ok()) {
    return status_and_enc_path.first;
  }
  return FileSystemWrapper::DeleteDir(status_and_enc_path.second, options, dbg);
}

IOStatus RemapFileSystem::GetFileSize(const std::string& fname,
                                      const IOOptions& options,
                                      uint64_t* file_size,
                                      IODebugContext* dbg) {
  auto status_and_enc_path = EncodePath(fname);
  if (!status_and_enc_path.first.ok()) {
    return status_and_enc_path.first;
  }
  return FileSystemWrapper::GetFileSize(status_and_enc_path.second, options,
                                        file_size, dbg);
}

IOStatus RemapFileSystem::GetFileModificationTime(const std::string& fname,
                                                  const IOOptions& options,
                                                  uint64_t* file_mtime,
                                                  IODebugContext* dbg) {
  auto status_and_enc_path = EncodePath(fname);
  if (!status_and_enc_path.first.ok()) {
    return status_and_enc_path.first;
  }
  return FileSystemWrapper::GetFileModificationTime(status_and_enc_path.second,
                                                    options, file_mtime, dbg);
}

IOStatus RemapFileSystem::IsDirectory(const std::string& path,
                                      const IOOptions& options, bool* is_dir,
                                      IODebugContext* dbg) {
  auto status_and_enc_path = EncodePath(path);
  if (!status_and_enc_path.first.ok()) {
    return status_and_enc_path.first;
  }
  return FileSystemWrapper::IsDirectory(status_and_enc_path.second, options,
                                        is_dir, dbg);
}

IOStatus RemapFileSystem::RenameFile(const std::string& src,
                                     const std::string& dest,
                                     const IOOptions& options,
                                     IODebugContext* dbg) {
  auto status_and_src_enc_path = EncodePath(src);
  if (!status_and_src_enc_path.first.ok()) {
    return status_and_src_enc_path.first;
  }
  auto status_and_dest_enc_path = EncodePathWithNewBasename(dest);
  if (!status_and_dest_enc_path.first.ok()) {
    return status_and_dest_enc_path.first;
  }
  return FileSystemWrapper::RenameFile(status_and_src_enc_path.second,
                                       status_and_dest_enc_path.second, options,
                                       dbg);
}

IOStatus RemapFileSystem::LinkFile(const std::string& src,
                                   const std::string& dest,
                                   const IOOptions& options,
                                   IODebugContext* dbg) {
  auto status_and_src_enc_path = EncodePath(src);
  if (!status_and_src_enc_path.first.ok()) {
    return status_and_src_enc_path.first;
  }
  auto status_and_dest_enc_path = EncodePathWithNewBasename(dest);
  if (!status_and_dest_enc_path.first.ok()) {
    return status_and_dest_enc_path.first;
  }
  return FileSystemWrapper::LinkFile(status_and_src_enc_path.second,
                                     status_and_dest_enc_path.second, options,
                                     dbg);
}

IOStatus RemapFileSystem::LockFile(const std::string& fname,
                                   const IOOptions& options, FileLock** lock,
                                   IODebugContext* dbg) {
  auto status_and_enc_path = EncodePathWithNewBasename(fname);
  if (!status_and_enc_path.first.ok()) {
    return status_and_enc_path.first;
  }
  // FileLock subclasses may store path (e.g., PosixFileLock stores it). We
  // can skip stripping the chroot directory from this path because callers
  // shouldn't use it.
  return FileSystemWrapper::LockFile(status_and_enc_path.second, options, lock,
                                     dbg);
}

IOStatus RemapFileSystem::NewLogger(const std::string& fname,
                                    const IOOptions& options,
                                    std::shared_ptr<Logger>* result,
                                    IODebugContext* dbg) {
  auto status_and_enc_path = EncodePathWithNewBasename(fname);
  if (!status_and_enc_path.first.ok()) {
    return status_and_enc_path.first;
  }
  return FileSystemWrapper::NewLogger(status_and_enc_path.second, options,
                                      result, dbg);
}

IOStatus RemapFileSystem::GetAbsolutePath(const std::string& db_path,
                                          const IOOptions& options,
                                          std::string* output_path,
                                          IODebugContext* dbg) {
  auto status_and_enc_path = EncodePath(db_path);
  if (!status_and_enc_path.first.ok()) {
    return status_and_enc_path.first;
  }
  return FileSystemWrapper::GetAbsolutePath(status_and_enc_path.second, options,
                                            output_path, dbg);
}

}  // namespace ROCKSDB_NAMESPACE

#endif  // ROCKSDB_LITE

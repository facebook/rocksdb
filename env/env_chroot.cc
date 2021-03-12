//  Copyright (c) 2016-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#if !defined(ROCKSDB_LITE) && !defined(OS_WIN)

#include "env/env_chroot.h"

#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include <string>
#include <utility>
#include <vector>

#include "env/composite_env_wrapper.h"
#include "rocksdb/file_system.h"
#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {
namespace {
class ChrootFileSystem : public FileSystemWrapper {
 public:
  ChrootFileSystem(const std::shared_ptr<FileSystem>& base,
                   const std::string& chroot_dir)
      : FileSystemWrapper(base) {
#if defined(OS_AIX)
    char resolvedName[PATH_MAX];
    char* real_chroot_dir = realpath(chroot_dir.c_str(), resolvedName);
#else
    char* real_chroot_dir = realpath(chroot_dir.c_str(), nullptr);
#endif
    // chroot_dir must exist so realpath() returns non-nullptr.
    assert(real_chroot_dir != nullptr);
    chroot_dir_ = real_chroot_dir;
#if !defined(OS_AIX)
    free(real_chroot_dir);
#endif
  }

  const char* Name() const override { return "ChrootFS"; }
  Status RegisterDbPaths(const std::vector<std::string>& paths) override {
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

  Status UnregisterDbPaths(const std::vector<std::string>& paths) override {
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

  IOStatus NewSequentialFile(const std::string& fname,
                             const FileOptions& options,
                             std::unique_ptr<FSSequentialFile>* result,
                             IODebugContext* dbg) override {
    auto status_and_enc_path = EncodePathWithNewBasename(fname);
    if (!status_and_enc_path.first.ok()) {
      return status_and_enc_path.first;
    }
    return FileSystemWrapper::NewSequentialFile(status_and_enc_path.second,
                                                options, result, dbg);
  }

  IOStatus NewRandomAccessFile(const std::string& fname,
                               const FileOptions& options,
                               std::unique_ptr<FSRandomAccessFile>* result,
                               IODebugContext* dbg) override {
    auto status_and_enc_path = EncodePathWithNewBasename(fname);
    if (!status_and_enc_path.first.ok()) {
      return status_and_enc_path.first;
    }
    return FileSystemWrapper::NewRandomAccessFile(status_and_enc_path.second,
                                                  options, result, dbg);
  }

  IOStatus NewWritableFile(const std::string& fname, const FileOptions& options,
                           std::unique_ptr<FSWritableFile>* result,
                           IODebugContext* dbg) override {
    auto status_and_enc_path = EncodePathWithNewBasename(fname);
    if (!status_and_enc_path.first.ok()) {
      return status_and_enc_path.first;
    }
    return FileSystemWrapper::NewWritableFile(status_and_enc_path.second,
                                              options, result, dbg);
  }

  IOStatus ReuseWritableFile(const std::string& fname,
                             const std::string& old_fname,
                             const FileOptions& options,
                             std::unique_ptr<FSWritableFile>* result,
                             IODebugContext* dbg) override {
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

  IOStatus NewRandomRWFile(const std::string& fname, const FileOptions& options,
                           std::unique_ptr<FSRandomRWFile>* result,
                           IODebugContext* dbg) override {
    auto status_and_enc_path = EncodePathWithNewBasename(fname);
    if (!status_and_enc_path.first.ok()) {
      return status_and_enc_path.first;
    }
    return FileSystemWrapper::NewRandomRWFile(status_and_enc_path.second,
                                              options, result, dbg);
  }

  IOStatus NewDirectory(const std::string& dir, const IOOptions& options,
                        std::unique_ptr<FSDirectory>* result,
                        IODebugContext* dbg) override {
    auto status_and_enc_path = EncodePathWithNewBasename(dir);
    if (!status_and_enc_path.first.ok()) {
      return status_and_enc_path.first;
    }
    return FileSystemWrapper::NewDirectory(status_and_enc_path.second, options,
                                           result, dbg);
  }

  IOStatus FileExists(const std::string& fname, const IOOptions& options,
                      IODebugContext* dbg) override {
    auto status_and_enc_path = EncodePathWithNewBasename(fname);
    if (!status_and_enc_path.first.ok()) {
      return status_and_enc_path.first;
    }
    return FileSystemWrapper::FileExists(status_and_enc_path.second, options,
                                         dbg);
  }

  IOStatus GetChildren(const std::string& dir, const IOOptions& options,
                       std::vector<std::string>* result,
                       IODebugContext* dbg) override {
    auto status_and_enc_path = EncodePath(dir);
    if (!status_and_enc_path.first.ok()) {
      return status_and_enc_path.first;
    }
    return FileSystemWrapper::GetChildren(status_and_enc_path.second, options,
                                          result, dbg);
  }

  IOStatus GetChildrenFileAttributes(const std::string& dir,
                                     const IOOptions& options,
                                     std::vector<FileAttributes>* result,
                                     IODebugContext* dbg) override {
    auto status_and_enc_path = EncodePath(dir);
    if (!status_and_enc_path.first.ok()) {
      return status_and_enc_path.first;
    }
    return FileSystemWrapper::GetChildrenFileAttributes(
        status_and_enc_path.second, options, result, dbg);
  }

  IOStatus DeleteFile(const std::string& fname, const IOOptions& options,
                      IODebugContext* dbg) override {
    auto status_and_enc_path = EncodePath(fname);
    if (!status_and_enc_path.first.ok()) {
      return status_and_enc_path.first;
    }
    return FileSystemWrapper::DeleteFile(status_and_enc_path.second, options,
                                         dbg);
  }

  IOStatus CreateDir(const std::string& dirname, const IOOptions& options,
                     IODebugContext* dbg) override {
    auto status_and_enc_path = EncodePathWithNewBasename(dirname);
    if (!status_and_enc_path.first.ok()) {
      return status_and_enc_path.first;
    }
    return FileSystemWrapper::CreateDir(status_and_enc_path.second, options,
                                        dbg);
  }

  IOStatus CreateDirIfMissing(const std::string& dirname,
                              const IOOptions& options,
                              IODebugContext* dbg) override {
    auto status_and_enc_path = EncodePathWithNewBasename(dirname);
    if (!status_and_enc_path.first.ok()) {
      return status_and_enc_path.first;
    }
    return FileSystemWrapper::CreateDirIfMissing(status_and_enc_path.second,
                                                 options, dbg);
  }

  IOStatus DeleteDir(const std::string& dirname, const IOOptions& options,
                     IODebugContext* dbg) override {
    auto status_and_enc_path = EncodePath(dirname);
    if (!status_and_enc_path.first.ok()) {
      return status_and_enc_path.first;
    }
    return FileSystemWrapper::DeleteDir(status_and_enc_path.second, options,
                                        dbg);
  }

  IOStatus GetFileSize(const std::string& fname, const IOOptions& options,
                       uint64_t* file_size, IODebugContext* dbg) override {
    auto status_and_enc_path = EncodePath(fname);
    if (!status_and_enc_path.first.ok()) {
      return status_and_enc_path.first;
    }
    return FileSystemWrapper::GetFileSize(status_and_enc_path.second, options,
                                          file_size, dbg);
  }

  IOStatus GetFileModificationTime(const std::string& fname,
                                   const IOOptions& options,
                                   uint64_t* file_mtime,
                                   IODebugContext* dbg) override {
    auto status_and_enc_path = EncodePath(fname);
    if (!status_and_enc_path.first.ok()) {
      return status_and_enc_path.first;
    }
    return FileSystemWrapper::GetFileModificationTime(
        status_and_enc_path.second, options, file_mtime, dbg);
  }

  IOStatus RenameFile(const std::string& src, const std::string& dest,
                      const IOOptions& options, IODebugContext* dbg) override {
    auto status_and_src_enc_path = EncodePath(src);
    if (!status_and_src_enc_path.first.ok()) {
      return status_and_src_enc_path.first;
    }
    auto status_and_dest_enc_path = EncodePathWithNewBasename(dest);
    if (!status_and_dest_enc_path.first.ok()) {
      return status_and_dest_enc_path.first;
    }
    return FileSystemWrapper::RenameFile(status_and_src_enc_path.second,
                                         status_and_dest_enc_path.second,
                                         options, dbg);
  }

  IOStatus LinkFile(const std::string& src, const std::string& dest,
                    const IOOptions& options, IODebugContext* dbg) override {
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

  IOStatus LockFile(const std::string& fname, const IOOptions& options,
                    FileLock** lock, IODebugContext* dbg) override {
    auto status_and_enc_path = EncodePathWithNewBasename(fname);
    if (!status_and_enc_path.first.ok()) {
      return status_and_enc_path.first;
    }
    // FileLock subclasses may store path (e.g., PosixFileLock stores it). We
    // can skip stripping the chroot directory from this path because callers
    // shouldn't use it.
    return FileSystemWrapper::LockFile(status_and_enc_path.second, options,
                                       lock, dbg);
  }

  IOStatus GetTestDirectory(const IOOptions& options, std::string* path,
                            IODebugContext* dbg) override {
    // Adapted from PosixEnv's implementation since it doesn't provide a way to
    // create directory in the chroot.
    char buf[256];
    snprintf(buf, sizeof(buf), "/rocksdbtest-%d", static_cast<int>(geteuid()));
    *path = buf;

    // Directory may already exist, so ignore return
    return CreateDirIfMissing(*path, options, dbg);
  }

  IOStatus NewLogger(const std::string& fname, const IOOptions& options,
                     std::shared_ptr<Logger>* result,
                     IODebugContext* dbg) override {
    auto status_and_enc_path = EncodePathWithNewBasename(fname);
    if (!status_and_enc_path.first.ok()) {
      return status_and_enc_path.first;
    }
    return FileSystemWrapper::NewLogger(status_and_enc_path.second, options,
                                        result, dbg);
  }

  IOStatus GetAbsolutePath(const std::string& db_path, const IOOptions& options,
                           std::string* output_path,
                           IODebugContext* dbg) override {
    auto status_and_enc_path = EncodePath(db_path);
    if (!status_and_enc_path.first.ok()) {
      return status_and_enc_path.first;
    }
    return FileSystemWrapper::GetAbsolutePath(status_and_enc_path.second,
                                              options, output_path, dbg);
  }

 private:
  // Returns status and expanded absolute path including the chroot directory.
  // Checks whether the provided path breaks out of the chroot. If it returns
  // non-OK status, the returned path should not be used.
  std::pair<IOStatus, std::string> EncodePath(const std::string& path) {
    if (path.empty() || path[0] != '/') {
      return {IOStatus::InvalidArgument(path, "Not an absolute path"), ""};
    }
    std::pair<IOStatus, std::string> res;
    res.second = chroot_dir_ + path;
#if defined(OS_AIX)
    char resolvedName[PATH_MAX];
    char* normalized_path = realpath(res.second.c_str(), resolvedName);
#else
    char* normalized_path = realpath(res.second.c_str(), nullptr);
#endif
    if (normalized_path == nullptr) {
      res.first = IOStatus::NotFound(res.second, strerror(errno));
    } else if (strlen(normalized_path) < chroot_dir_.size() ||
               strncmp(normalized_path, chroot_dir_.c_str(),
                       chroot_dir_.size()) != 0) {
      res.first = IOStatus::IOError(res.second,
                                    "Attempted to access path outside chroot");
    } else {
      res.first = IOStatus::OK();
    }
#if !defined(OS_AIX)
    free(normalized_path);
#endif
    return res;
  }

  // Similar to EncodePath() except assumes the basename in the path hasn't been
  // created yet.
  std::pair<IOStatus, std::string> EncodePathWithNewBasename(
      const std::string& path) {
    if (path.empty() || path[0] != '/') {
      return {IOStatus::InvalidArgument(path, "Not an absolute path"), ""};
    }
    // Basename may be followed by trailing slashes
    size_t final_idx = path.find_last_not_of('/');
    if (final_idx == std::string::npos) {
      // It's only slashes so no basename to extract
      return EncodePath(path);
    }

    // Pull off the basename temporarily since realname(3) (used by
    // EncodePath()) requires a path that exists
    size_t base_sep = path.rfind('/', final_idx);
    auto status_and_enc_path = EncodePath(path.substr(0, base_sep + 1));
    status_and_enc_path.second.append(path.substr(base_sep + 1));
    return status_and_enc_path;
  }

  std::string chroot_dir_;
};
}  // namespace

std::shared_ptr<FileSystem> NewChrootFileSystem(
    const std::shared_ptr<FileSystem>& base, const std::string& chroot_dir) {
  return std::make_shared<ChrootFileSystem>(base, chroot_dir);
}

Env* NewChrootEnv(Env* base_env, const std::string& chroot_dir) {
  if (!base_env->FileExists(chroot_dir).ok()) {
    return nullptr;
  }
  std::shared_ptr<FileSystem> chroot_fs =
      NewChrootFileSystem(base_env->GetFileSystem(), chroot_dir);
  return new CompositeEnvWrapper(base_env, chroot_fs);
}

}  // namespace ROCKSDB_NAMESPACE

#endif  // !defined(ROCKSDB_LITE) && !defined(OS_WIN)

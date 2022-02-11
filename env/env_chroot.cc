//  Copyright (c) 2016-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#if !defined(ROCKSDB_LITE) && !defined(OS_WIN)

#include "env/env_chroot.h"

#include <errno.h>   // errno
#include <stdlib.h>  // realpath, free
#include <unistd.h>  // geteuid

#include "env/composite_env_wrapper.h"
#include "env/fs_remap.h"
#include "rocksdb/utilities/options_type.h"
#include "util/string_util.h"  // errnoStr

namespace ROCKSDB_NAMESPACE {
namespace {
static std::unordered_map<std::string, OptionTypeInfo> chroot_fs_type_info = {
    {"chroot_dir", {0, OptionType::kString}}};
}  // namespace
ChrootFileSystem::ChrootFileSystem(const std::shared_ptr<FileSystem>& base,
                                   const std::string& chroot_dir)
    : RemapFileSystem(base), chroot_dir_(chroot_dir) {
  RegisterOptions("chroot_dir", &chroot_dir_, &chroot_fs_type_info);
}

Status ChrootFileSystem::PrepareOptions(const ConfigOptions& options) {
  Status s = FileSystemWrapper::PrepareOptions(options);
  if (!s.ok()) {
    return s;
  } else if (chroot_dir_.empty()) {
    s = Status::InvalidArgument("ChRootFileSystem requires a chroot dir");
  } else {
    s = target_->FileExists(chroot_dir_, IOOptions(), nullptr);
  }
  if (s.ok()) {
#if defined(OS_AIX)
    char resolvedName[PATH_MAX];
    char* real_chroot_dir = realpath(chroot_dir_.c_str(), resolvedName);
#else
    char* real_chroot_dir = realpath(chroot_dir_.c_str(), nullptr);
#endif
    // chroot_dir must exist so realpath() returns non-nullptr.
    assert(real_chroot_dir != nullptr);
    chroot_dir_ = real_chroot_dir;
#if !defined(OS_AIX)
    free(real_chroot_dir);
#endif
  }
  return s;
}

IOStatus ChrootFileSystem::GetTestDirectory(const IOOptions& options,
                                            std::string* path,
                                            IODebugContext* dbg) {
  // Adapted from PosixEnv's implementation since it doesn't provide a way to
  // create directory in the chroot.
  char buf[256];
  snprintf(buf, sizeof(buf), "/rocksdbtest-%d", static_cast<int>(geteuid()));
  *path = buf;

  // Directory may already exist, so ignore return
  return CreateDirIfMissing(*path, options, dbg);
}

  // Returns status and expanded absolute path including the chroot directory.
  // Checks whether the provided path breaks out of the chroot. If it returns
  // non-OK status, the returned path should not be used.
std::pair<IOStatus, std::string> ChrootFileSystem::EncodePath(
    const std::string& path) {
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
      res.first = IOStatus::NotFound(res.second, errnoStr(errno).c_str());
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
std::pair<IOStatus, std::string> ChrootFileSystem::EncodePathWithNewBasename(
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

std::shared_ptr<FileSystem> NewChrootFileSystem(
    const std::shared_ptr<FileSystem>& base, const std::string& chroot_dir) {
  auto chroot_fs = std::make_shared<ChrootFileSystem>(base, chroot_dir);
  Status s = chroot_fs->PrepareOptions(ConfigOptions());
  if (s.ok()) {
    return chroot_fs;
  } else {
    return nullptr;
  }
}

Env* NewChrootEnv(Env* base_env, const std::string& chroot_dir) {
  if (!base_env->FileExists(chroot_dir).ok()) {
    return nullptr;
  }
  auto chroot_fs = NewChrootFileSystem(base_env->GetFileSystem(), chroot_dir);
  if (chroot_fs != nullptr) {
    return new CompositeEnvWrapper(base_env, chroot_fs);
  } else {
    return nullptr;
  }
}

}  // namespace ROCKSDB_NAMESPACE

#endif  // !defined(ROCKSDB_LITE) && !defined(OS_WIN)

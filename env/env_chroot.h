//  Copyright (c) 2016-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#if !defined(ROCKSDB_LITE) && !defined(OS_WIN)

#include <string>

#include "env/fs_remap.h"
#include "rocksdb/file_system.h"

namespace ROCKSDB_NAMESPACE {
class ChrootFileSystem : public RemapFileSystem {
 public:
  ChrootFileSystem(const std::shared_ptr<FileSystem>& base,
                   const std::string& chroot_dir);

  static const char* kClassName() { return "ChrootFS"; }
  const char* Name() const override { return kClassName(); }

  IOStatus GetTestDirectory(const IOOptions& options, std::string* path,
                            IODebugContext* dbg) override;

  Status PrepareOptions(const ConfigOptions& options) override;

 protected:
  // Returns status and expanded absolute path including the chroot directory.
  // Checks whether the provided path breaks out of the chroot. If it returns
  // non-OK status, the returned path should not be used.
  std::pair<IOStatus, std::string> EncodePath(const std::string& path) override;

  // Similar to EncodePath() except assumes the basename in the path hasn't been
  // created yet.
  std::pair<IOStatus, std::string> EncodePathWithNewBasename(
      const std::string& path) override;

 private:
  std::string chroot_dir_;
};

// Returns an Env that translates paths such that the root directory appears to
// be chroot_dir. chroot_dir should refer to an existing directory.
//
// This class has not been fully analyzed for providing strong security
// guarantees.
Env* NewChrootEnv(Env* base_env, const std::string& chroot_dir);
std::shared_ptr<FileSystem> NewChrootFileSystem(
    const std::shared_ptr<FileSystem>& base, const std::string& chroot_dir);

}  // namespace ROCKSDB_NAMESPACE

#endif  // !defined(ROCKSDB_LITE) && !defined(OS_WIN)

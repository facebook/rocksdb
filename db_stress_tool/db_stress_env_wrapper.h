//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifdef GFLAGS
#pragma once
#include "db_stress_tool/db_stress_common.h"

namespace ROCKSDB_NAMESPACE {
class DbStressFSWrapper : public FileSystemWrapper {
 public:
  explicit DbStressFSWrapper(const std::shared_ptr<FileSystem>& t)
      : FileSystemWrapper(t) {}
  static const char* kClassName() { return "DbStressFS"; }
  const char* Name() const override { return kClassName(); }

  IOStatus DeleteFile(const std::string& f, const IOOptions& opts,
                      IODebugContext* dbg) override {
    // We determine whether it is a manifest file by searching a strong,
    // so that there will be false positive if the directory path contains the
    // keyword but it is unlikely.
    // Checkpoint, backup, and restore directories needs to be exempted.
    if (!if_preserve_all_manifests ||
        f.find("MANIFEST-") == std::string::npos ||
        f.find("checkpoint") != std::string::npos ||
        f.find(".backup") != std::string::npos ||
        f.find(".restore") != std::string::npos) {
      return target()->DeleteFile(f, opts, dbg);
    }
    // Rename the file instead of deletion to keep the history, and
    // at the same time it is not visible to RocksDB.
    return target()->RenameFile(f, f + "_renamed_", opts, dbg);
  }

  // If true, all manifest files will not be delted in DeleteFile().
  bool if_preserve_all_manifests = true;
};
}  // namespace ROCKSDB_NAMESPACE
#endif  // GFLAGS

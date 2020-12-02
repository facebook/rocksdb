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
class DbStressEnvWrapper : public EnvWrapper {
 public:
  explicit DbStressEnvWrapper(Env* t) : EnvWrapper(t) {}

  Status DeleteFile(const std::string& f) override {
    // We determine whether it is a manifest file by searching a strong,
    // so that there will be false positive if the directory path contains the
    // keyword but it is unlikely.
    // Checkpoint, backup, and restore directories needs to be exempted.
    if (!if_preserve_all_manifests ||
        f.find("MANIFEST-") == std::string::npos ||
        f.find("checkpoint") != std::string::npos ||
        f.find(".backup") != std::string::npos ||
        f.find(".restore") != std::string::npos) {
      return target()->DeleteFile(f);
    }
    return Status::OK();
  }

  // If true, all manifest files will not be delted in DeleteFile().
  bool if_preserve_all_manifests = true;
};
}  // namespace ROCKSDB_NAMESPACE
#endif  // GFLAGS

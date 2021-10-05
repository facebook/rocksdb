//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#pragma once

#include <atomic>
#include <map>
#include <string>
#include <vector>

#include "env/composite_env_wrapper.h"
#include "rocksdb/env.h"
#include "rocksdb/status.h"
#include "rocksdb/system_clock.h"

namespace ROCKSDB_NAMESPACE {
class MockEnv : public CompositeEnvWrapper {
 public:
  static MockEnv* Create(Env* base);
  static MockEnv* Create(Env* base, const std::shared_ptr<SystemClock>& clock);

  Status CorruptBuffer(const std::string& fname);
 private:
  MockEnv(Env* env, const std::shared_ptr<FileSystem>& fs,
          const std::shared_ptr<SystemClock>& clock);
};

}  // namespace ROCKSDB_NAMESPACE

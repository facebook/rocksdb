//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright 2014 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

// This custom env injects bit flips into file reads.

#ifndef UTIL_BIT_CORRUPTION_INJECTION_TEST_ENV_H_
#define UTIL_BIT_CORRUPTION_INJECTION_TEST_ENV_H_

#include <map>
#include <set>
#include <string>
#include <vector>

#include "db/version_set.h"
#include "env/mock_env.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "util/filename.h"
#include "util/mutexlock.h"
#include "util/random.h"

namespace rocksdb {

class CorruptedSequentialFile;
class CorruptedRandomAccessFile;
class BitCorruptionInjectionTestEnv;

class BitCorruptionInjectionTestEnv : public EnvWrapper {
 public:
  explicit BitCorruptionInjectionTestEnv(Env* base, uint64_t uber)
      : EnvWrapper(base), UBER_(uber) {}
  virtual ~BitCorruptionInjectionTestEnv() {}

  virtual Status NewSequentialFile(const std::string& f, unique_ptr<SequentialFile>* r,
                           const EnvOptions& options) override;
  virtual Status NewRandomAccessFile(const std::string& f,
                             unique_ptr<RandomAccessFile>* r,
                             const EnvOptions& options) override;

 private:
   uint64_t UBER_;
   static std::vector<std::string> excludedFiles_;
};

}  // namespace rocksdb

#endif  // UTIL_BIT_CORRUPTION_INJECTION_TEST_ENV_H_

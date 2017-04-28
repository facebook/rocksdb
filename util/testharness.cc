//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "util/testharness.h"
#include <string>

namespace rocksdb {
namespace test {

::testing::AssertionResult AssertStatus(const char* s_expr, const Status& s) {
  if (s.ok()) {
    return ::testing::AssertionSuccess();
  } else {
    return ::testing::AssertionFailure() << s_expr << std::endl
                                         << s.ToString();
  }
}

std::string TmpDir(Env* env) {
  std::string dir;
  Status s = env->GetTestDirectory(&dir);
  EXPECT_TRUE(s.ok()) << s.ToString();
  return dir;
}

int RandomSeed() {
  const char* env = getenv("TEST_RANDOM_SEED");
  int result = (env != nullptr ? atoi(env) : 301);
  if (result <= 0) {
    result = 301;
  }
  return result;
}

}  // namespace test
}  // namespace rocksdb

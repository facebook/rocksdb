// Copyright (c) 2017-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef ROCKSDB_LITE

#include "rocksdb/env.h"
#include "rocksdb/perf_context.h"
#include "util/testharness.h"

namespace rocksdb {

class TimedEnvTest : public testing::Test {
};

TEST_F(TimedEnvTest, BasicTest) {
  SetPerfLevel(PerfLevel::kEnableTime);
  ASSERT_EQ(0, get_perf_context()->env_new_writable_file_nanos);

  std::unique_ptr<Env> mem_env(NewMemEnv(Env::Default()));
  std::unique_ptr<Env> timed_env(NewTimedEnv(mem_env.get()));
  std::unique_ptr<WritableFile> writable_file;
  timed_env->NewWritableFile("f", &writable_file, EnvOptions());

  ASSERT_GT(get_perf_context()->env_new_writable_file_nanos, 0);
}

}  // namespace rocksdb

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

#else  // ROCKSDB_LITE
#include <stdio.h>

int main(int argc, char** argv) {
  fprintf(stderr, "SKIPPED as TimedEnv is not supported in ROCKSDB_LITE\n");
  return 0;
}

#endif  // ROCKSDB_LITE

//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "test_util/testutil.h"

#include "file/file_util.h"
#include "port/port.h"
#include "port/stack_trace.h"
#include "test_util/testharness.h"

namespace ROCKSDB_NAMESPACE {

void CreateFile(Env* env, const std::string& path) {
  std::unique_ptr<WritableFile> f;
  ASSERT_OK(env->NewWritableFile(path, &f, EnvOptions()));
  f->Close();
}

TEST(TestUtil, DestroyDirRecursively) {
  auto env = Env::Default();
  // test_util/file
  //          /dir
  //          /dir/file
  std::string test_dir = test::PerThreadDBPath("test_util");
  ASSERT_OK(env->CreateDir(test_dir));
  CreateFile(env, test_dir + "/file");
  ASSERT_OK(env->CreateDir(test_dir + "/dir"));
  CreateFile(env, test_dir + "/dir/file");

  ASSERT_OK(DestroyDir(env, test_dir));
  auto s = env->FileExists(test_dir);
  ASSERT_TRUE(s.IsNotFound());
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

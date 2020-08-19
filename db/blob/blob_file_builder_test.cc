//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/blob/blob_file_builder.h"

#include <cinttypes>
#include <string>
#include <vector>

#include "db/blob/blob_file_addition.h"
#include "env/composite_env_wrapper.h"
#include "env/mock_env.h"
#include "options/cf_options.h"
#include "rocksdb/env.h"
#include "rocksdb/options.h"
#include "test_util/sync_point.h"
#include "test_util/testharness.h"

namespace ROCKSDB_NAMESPACE {

class BlobFileBuilderTest : public testing::Test {
 protected:
  class FileNumberGenerator {
   public:
    uint64_t operator()() { return ++next_file_number_; }

   private:
    uint64_t next_file_number_ = 1;
  };

  BlobFileBuilderTest() : env_(Env::Default()), fs_(&env_) {}

  MockEnv env_;
  LegacyFileSystemWrapper fs_;
  FileOptions file_options_;
};

TEST_F(BlobFileBuilderTest, Build) {
  Options options;
  options.cf_paths.emplace_back(
      test::PerThreadDBPath(&env_, "BlobFileBuilderTest_Build"), 0);
  options.enable_blob_files = true;

  ImmutableCFOptions immutable_cf_options(options);
  MutableCFOptions mutable_cf_options(options);

  constexpr uint32_t column_family_id = 123;
  constexpr Env::IOPriority io_priority = Env::IO_HIGH;
  constexpr Env::WriteLifeTimeHint write_hint = Env::WLTH_MEDIUM;

  std::vector<BlobFileAddition> blob_file_additions;

  BlobFileBuilder builder(FileNumberGenerator(), &env_, &fs_,
                          &immutable_cf_options, &mutable_cf_options,
                          &file_options_, column_family_id, io_priority,
                          write_hint, &blob_file_additions);

  for (int i = 0; i < 10; ++i) {
    const std::string key = std::to_string(i);
    const std::string value = std::to_string(i + 1234);

    std::string blob_index;
    ASSERT_OK(builder.Add(key, value, &blob_index));
  }

  ASSERT_OK(builder.Finish());
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

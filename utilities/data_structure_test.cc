// Copyright (c) 2017-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "rocksdb/utilities/data_structure.h"

#include "db/db_test_util.h"
#include "rocksdb/types.h"

namespace ROCKSDB_NAMESPACE {

// Unit test for SmallEnumSet
class SmallEnumSetTest : public testing::Test {
 public:
  SmallEnumSetTest() {}
  ~SmallEnumSetTest() {}
};

TEST_F(SmallEnumSetTest, SmallSetTest) {
  FileTypeSet fs;
  ASSERT_TRUE(fs.Add(FileType::kIdentityFile));
  ASSERT_TRUE(fs.Add(FileType::kInfoLogFile));
  ASSERT_TRUE(fs.Contains(FileType::kIdentityFile));
  ASSERT_FALSE(fs.Contains(FileType::kDBLockFile));
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

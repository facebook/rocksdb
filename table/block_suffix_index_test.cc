// Copyright (c) 2011-present, Facebook, Inc. All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "rocksdb/slice.h"
#include "table/block_suffix_index.h"
#include "util/testharness.h"
#include "util/testutil.h"

namespace rocksdb {


TEST(BlockTest, BlockSuffixTest) {
  //TODO(fwu)
  BlockSuffixIndexBuilder builder(128);

  std::string s = "key";
  Slice k(s);
  uint16_t off = 7;
  builder.Add(k, off);
  ASSERT_TRUE(true);
}

}  // namespace rocksdb

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

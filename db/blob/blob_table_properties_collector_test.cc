//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/blob/blob_table_properties_collector.h"

#include "db/blob/blob_stats_collection.h"
#include "test_util/testharness.h"

namespace ROCKSDB_NAMESPACE {

class BlobTablePropertiesCollectorTest : public testing::Test {
 public:
};

TEST_F(BlobTablePropertiesCollectorTest, InternalAddAndFinish) {}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

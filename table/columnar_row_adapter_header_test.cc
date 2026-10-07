//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <gtest/gtest.h>

#include "rocksdb/columnar_row_adapter.h"
#include "rocksdb/status.h"

namespace facebook::rocks {
namespace {

TEST(ColumnarRowAdapterHeaderTest, CanIncludeWithoutVelox) {
#ifdef ROCKSDB_USE_VELOX
  FAIL() << "This test target must compile without ROCKSDB_USE_VELOX";
#else
  EXPECT_TRUE(rocksdb::Status::OK().ok());
#endif
}

}  // namespace
}  // namespace facebook::rocks

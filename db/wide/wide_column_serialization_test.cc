//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/wide/wide_column_serialization.h"

#include "test_util/testharness.h"

namespace ROCKSDB_NAMESPACE {

TEST(WideColumnSerializationTest, Serialize) {
  WideColumnDescs column_descs{{"foo", "bar"}, {"hello", "world"}};
  std::string output;

  ASSERT_OK(WideColumnSerialization::Serialize(column_descs, &output));

  {
    Slice input(output);
    WideColumnDescs deserialized_descs;

    ASSERT_OK(
        WideColumnSerialization::DeserializeAll(&input, &deserialized_descs));
    ASSERT_EQ(column_descs, deserialized_descs);
  }

  {
    Slice input(output);
    WideColumnDesc deserialized_desc;

    ASSERT_OK(WideColumnSerialization::DeserializeOne(&input, "foo",
                                                      &deserialized_desc));

    WideColumnDesc expected_desc{"foo", "bar"};
    ASSERT_EQ(deserialized_desc, expected_desc);
  }

  {
    Slice input(output);
    WideColumnDesc deserialized_desc;

    ASSERT_OK(WideColumnSerialization::DeserializeOne(&input, "hello",
                                                      &deserialized_desc));

    WideColumnDesc expected_desc{"hello", "world"};
    ASSERT_EQ(deserialized_desc, expected_desc);
  }

  {
    Slice input(output);
    WideColumnDesc deserialized_desc;

    ASSERT_NOK(WideColumnSerialization::DeserializeOne(&input, "snafu",
                                                       &deserialized_desc));
  }
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

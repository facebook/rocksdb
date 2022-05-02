//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/wide/wide_column_serialization.h"

#include "test_util/testharness.h"
#include "util/coding.h"

namespace ROCKSDB_NAMESPACE {

TEST(WideColumnSerializationTest, SerializeDeserialize) {
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

TEST(WideColumnSerializationTest, DeserializeVersionError) {
  // Can't decode version

  std::string buf;

  Slice input(buf);
  WideColumnDescs descs;

  const Status s = WideColumnSerialization::DeserializeAll(&input, &descs);
  ASSERT_TRUE(s.IsCorruption());
  ASSERT_TRUE(std::strstr(s.getState(), "version"));
}

TEST(WideColumnSerializationTest, DeserializeUnsupportedVersion) {
  // Unsupported version
  constexpr uint32_t future_version = 1000;

  std::string buf;
  PutVarint32(&buf, future_version);

  Slice input(buf);
  WideColumnDescs descs;

  const Status s = WideColumnSerialization::DeserializeAll(&input, &descs);
  ASSERT_TRUE(s.IsNotSupported());
  ASSERT_TRUE(std::strstr(s.getState(), "version"));
}

TEST(WideColumnSerializationTest, DeserializeNumberOfColumnsError) {
  // Can't decode number of columns

  std::string buf;
  PutVarint32(&buf, WideColumnSerialization::kCurrentVersion);

  Slice input(buf);
  WideColumnDescs descs;

  const Status s = WideColumnSerialization::DeserializeAll(&input, &descs);
  ASSERT_TRUE(s.IsCorruption());
  ASSERT_TRUE(std::strstr(s.getState(), "number"));
}

TEST(WideColumnSerializationTest, DeserializeColumnsError) {
  std::string buf;

  PutVarint32(&buf, WideColumnSerialization::kCurrentVersion);

  constexpr uint32_t num_columns = 2;
  PutVarint32(&buf, num_columns);

  // Can't decode the first column name
  {
    Slice input(buf);
    WideColumnDescs descs;

    const Status s = WideColumnSerialization::DeserializeAll(&input, &descs);
    ASSERT_TRUE(s.IsCorruption());
    ASSERT_TRUE(std::strstr(s.getState(), "name"));
  }

  constexpr char first_column_name[] = "foo";
  PutLengthPrefixedSlice(&buf, first_column_name);

  // Can't decode the size of the first column value
  {
    Slice input(buf);
    WideColumnDescs descs;

    const Status s = WideColumnSerialization::DeserializeAll(&input, &descs);
    ASSERT_TRUE(s.IsCorruption());
    ASSERT_TRUE(std::strstr(s.getState(), "value size"));
  }

  constexpr uint32_t first_value_size = 16;
  PutVarint32(&buf, first_value_size);

  // Can't decode the second column name
  {
    Slice input(buf);
    WideColumnDescs descs;

    const Status s = WideColumnSerialization::DeserializeAll(&input, &descs);
    ASSERT_TRUE(s.IsCorruption());
    ASSERT_TRUE(std::strstr(s.getState(), "name"));
  }

  constexpr char second_column_name[] = "hello";
  PutLengthPrefixedSlice(&buf, second_column_name);

  // Can't decode the size of the second column value
  {
    Slice input(buf);
    WideColumnDescs descs;

    const Status s = WideColumnSerialization::DeserializeAll(&input, &descs);
    ASSERT_TRUE(s.IsCorruption());
    ASSERT_TRUE(std::strstr(s.getState(), "value size"));
  }

  constexpr uint32_t second_value_size = 64;
  PutVarint32(&buf, second_value_size);

  // Can't decode the payload of the first column
  {
    Slice input(buf);
    WideColumnDescs descs;

    const Status s = WideColumnSerialization::DeserializeAll(&input, &descs);
    ASSERT_TRUE(s.IsCorruption());
    ASSERT_TRUE(std::strstr(s.getState(), "payload"));
  }

  buf.append(first_value_size, '0');

  // Can't decode the payload of the second column
  {
    Slice input(buf);
    WideColumnDescs descs;

    const Status s = WideColumnSerialization::DeserializeAll(&input, &descs);
    ASSERT_TRUE(s.IsCorruption());
    ASSERT_TRUE(std::strstr(s.getState(), "payload"));
  }

  buf.append(second_value_size, 'x');

  // Success
  {
    Slice input(buf);
    WideColumnDescs descs;

    ASSERT_OK(WideColumnSerialization::DeserializeAll(&input, &descs));
  }
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

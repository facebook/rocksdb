//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/wide/wide_column_serialization.h"

#include "db/blob/blob_index.h"
#include "db/wide/wide_columns_helper.h"
#include "test_util/testharness.h"
#include "util/coding.h"

namespace ROCKSDB_NAMESPACE {

TEST(WideColumnSerializationTest, Construct) {
  constexpr char foo[] = "foo";
  constexpr char bar[] = "bar";

  const std::string foo_str(foo);
  const std::string bar_str(bar);

  const Slice foo_slice(foo_str);
  const Slice bar_slice(bar_str);

  {
    WideColumn column(foo, bar);
    ASSERT_EQ(column.name(), foo);
    ASSERT_EQ(column.value(), bar);
  }

  {
    WideColumn column(foo_str, bar);
    ASSERT_EQ(column.name(), foo_str);
    ASSERT_EQ(column.value(), bar);
  }

  {
    WideColumn column(foo_slice, bar);
    ASSERT_EQ(column.name(), foo_slice);
    ASSERT_EQ(column.value(), bar);
  }

  {
    WideColumn column(foo, bar_str);
    ASSERT_EQ(column.name(), foo);
    ASSERT_EQ(column.value(), bar_str);
  }

  {
    WideColumn column(foo_str, bar_str);
    ASSERT_EQ(column.name(), foo_str);
    ASSERT_EQ(column.value(), bar_str);
  }

  {
    WideColumn column(foo_slice, bar_str);
    ASSERT_EQ(column.name(), foo_slice);
    ASSERT_EQ(column.value(), bar_str);
  }

  {
    WideColumn column(foo, bar_slice);
    ASSERT_EQ(column.name(), foo);
    ASSERT_EQ(column.value(), bar_slice);
  }

  {
    WideColumn column(foo_str, bar_slice);
    ASSERT_EQ(column.name(), foo_str);
    ASSERT_EQ(column.value(), bar_slice);
  }

  {
    WideColumn column(foo_slice, bar_slice);
    ASSERT_EQ(column.name(), foo_slice);
    ASSERT_EQ(column.value(), bar_slice);
  }

  {
    constexpr char foo_name[] = "foo_name";
    constexpr char bar_value[] = "bar_value";

    WideColumn column(std::piecewise_construct,
                      std::forward_as_tuple(foo_name, sizeof(foo) - 1),
                      std::forward_as_tuple(bar_value, sizeof(bar) - 1));
    ASSERT_EQ(column.name(), foo);
    ASSERT_EQ(column.value(), bar);
  }
}

TEST(WideColumnSerializationTest, SerializeDeserialize) {
  WideColumns columns{{"foo", "bar"}, {"hello", "world"}};
  std::string output;

  ASSERT_OK(WideColumnSerialization::Serialize(columns, output));

  Slice input(output);
  WideColumns deserialized_columns;

  ASSERT_OK(WideColumnSerialization::Deserialize(input, deserialized_columns));
  ASSERT_EQ(columns, deserialized_columns);

  {
    const auto it = WideColumnsHelper::Find(deserialized_columns.cbegin(),
                                            deserialized_columns.cend(), "foo");
    ASSERT_NE(it, deserialized_columns.cend());
    ASSERT_EQ(*it, deserialized_columns.front());
  }

  {
    const auto it = WideColumnsHelper::Find(
        deserialized_columns.cbegin(), deserialized_columns.cend(), "hello");
    ASSERT_NE(it, deserialized_columns.cend());
    ASSERT_EQ(*it, deserialized_columns.back());
  }

  {
    const auto it = WideColumnsHelper::Find(
        deserialized_columns.cbegin(), deserialized_columns.cend(), "fubar");
    ASSERT_EQ(it, deserialized_columns.cend());
  }

  {
    const auto it = WideColumnsHelper::Find(
        deserialized_columns.cbegin(), deserialized_columns.cend(), "snafu");
    ASSERT_EQ(it, deserialized_columns.cend());
  }
}

TEST(WideColumnSerializationTest, SerializeDuplicateError) {
  WideColumns columns{{"foo", "bar"}, {"foo", "baz"}};
  std::string output;

  ASSERT_TRUE(
      WideColumnSerialization::Serialize(columns, output).IsCorruption());
}

TEST(WideColumnSerializationTest, SerializeOutOfOrderError) {
  WideColumns columns{{"hello", "world"}, {"foo", "bar"}};
  std::string output;

  ASSERT_TRUE(
      WideColumnSerialization::Serialize(columns, output).IsCorruption());
}

TEST(WideColumnSerializationTest, DeserializeVersionError) {
  // Can't decode version

  std::string buf;

  Slice input(buf);
  WideColumns columns;

  const Status s = WideColumnSerialization::Deserialize(input, columns);
  ASSERT_TRUE(s.IsCorruption());
  ASSERT_TRUE(std::strstr(s.getState(), "version"));
}

TEST(WideColumnSerializationTest, DeserializeUnsupportedVersion) {
  // Unsupported version
  constexpr uint32_t future_version = 1000;

  std::string buf;
  PutVarint32(&buf, future_version);

  Slice input(buf);
  WideColumns columns;

  const Status s = WideColumnSerialization::Deserialize(input, columns);
  ASSERT_TRUE(s.IsNotSupported());
  ASSERT_TRUE(std::strstr(s.getState(), "version"));
}

TEST(WideColumnSerializationTest, DeserializeNumberOfColumnsError) {
  // Can't decode number of columns

  std::string buf;
  PutVarint32(&buf, WideColumnSerialization::kVersion1);

  Slice input(buf);
  WideColumns columns;

  const Status s = WideColumnSerialization::Deserialize(input, columns);
  ASSERT_TRUE(s.IsCorruption());
  ASSERT_TRUE(std::strstr(s.getState(), "number"));
}

TEST(WideColumnSerializationTest, DeserializeColumnsError) {
  std::string buf;

  PutVarint32(&buf, WideColumnSerialization::kVersion1);

  constexpr uint32_t num_columns = 2;
  PutVarint32(&buf, num_columns);

  // Can't decode the first column name
  {
    Slice input(buf);
    WideColumns columns;

    const Status s = WideColumnSerialization::Deserialize(input, columns);
    ASSERT_TRUE(s.IsCorruption());
    ASSERT_TRUE(std::strstr(s.getState(), "name"));
  }

  constexpr char first_column_name[] = "foo";
  PutLengthPrefixedSlice(&buf, first_column_name);

  // Can't decode the size of the first column value
  {
    Slice input(buf);
    WideColumns columns;

    const Status s = WideColumnSerialization::Deserialize(input, columns);
    ASSERT_TRUE(s.IsCorruption());
    ASSERT_TRUE(std::strstr(s.getState(), "value size"));
  }

  constexpr uint32_t first_value_size = 16;
  PutVarint32(&buf, first_value_size);

  // Can't decode the second column name
  {
    Slice input(buf);
    WideColumns columns;

    const Status s = WideColumnSerialization::Deserialize(input, columns);
    ASSERT_TRUE(s.IsCorruption());
    ASSERT_TRUE(std::strstr(s.getState(), "name"));
  }

  constexpr char second_column_name[] = "hello";
  PutLengthPrefixedSlice(&buf, second_column_name);

  // Can't decode the size of the second column value
  {
    Slice input(buf);
    WideColumns columns;

    const Status s = WideColumnSerialization::Deserialize(input, columns);
    ASSERT_TRUE(s.IsCorruption());
    ASSERT_TRUE(std::strstr(s.getState(), "value size"));
  }

  constexpr uint32_t second_value_size = 64;
  PutVarint32(&buf, second_value_size);

  // Can't decode the payload of the first column
  {
    Slice input(buf);
    WideColumns columns;

    const Status s = WideColumnSerialization::Deserialize(input, columns);
    ASSERT_TRUE(s.IsCorruption());
    ASSERT_TRUE(std::strstr(s.getState(), "payload"));
  }

  buf.append(first_value_size, '0');

  // Can't decode the payload of the second column
  {
    Slice input(buf);
    WideColumns columns;

    const Status s = WideColumnSerialization::Deserialize(input, columns);
    ASSERT_TRUE(s.IsCorruption());
    ASSERT_TRUE(std::strstr(s.getState(), "payload"));
  }

  buf.append(second_value_size, 'x');

  // Success
  {
    Slice input(buf);
    WideColumns columns;

    ASSERT_OK(WideColumnSerialization::Deserialize(input, columns));
  }
}

TEST(WideColumnSerializationTest, DeserializeColumnsOutOfOrder) {
  std::string buf;

  PutVarint32(&buf, WideColumnSerialization::kVersion1);

  constexpr uint32_t num_columns = 2;
  PutVarint32(&buf, num_columns);

  constexpr char first_column_name[] = "b";
  PutLengthPrefixedSlice(&buf, first_column_name);

  constexpr uint32_t first_value_size = 16;
  PutVarint32(&buf, first_value_size);

  constexpr char second_column_name[] = "a";
  PutLengthPrefixedSlice(&buf, second_column_name);

  Slice input(buf);
  WideColumns columns;

  const Status s = WideColumnSerialization::Deserialize(input, columns);
  ASSERT_TRUE(s.IsCorruption());
  ASSERT_TRUE(std::strstr(s.getState(), "order"));
}

TEST(WideColumnSerializationTest, SerializeV2WithBlobIndex) {
  // Create columns as (name, value) pairs
  std::vector<std::pair<std::string, std::string>> columns = {
      {"col1", "value1"},
      {"col2", "value2"},  // This will be replaced by a blob index
      {"col3", "value3"}};

  // Create a blob index for col2 (index 1)
  std::string blob_index_str;
  BlobIndex::EncodeBlob(&blob_index_str, /* file_number */ 100,
                        /* offset */ 2000, /* size */ 500, kNoCompression);

  // Decode the blob index to create a BlobIndex object
  BlobIndex blob_idx;
  Slice blob_slice(blob_index_str);
  ASSERT_OK(blob_idx.DecodeFrom(blob_slice));

  std::vector<std::pair<size_t, BlobIndex>> blob_columns = {{1, blob_idx}};

  std::string output;
  ASSERT_OK(WideColumnSerialization::SerializeWithBlobIndices(
      columns, blob_columns, &output));

  // Verify version is 2
  ASSERT_EQ(WideColumnSerialization::GetVersion(Slice(output)),
            WideColumnSerialization::kVersion2);

  // Verify HasBlobColumns returns true
  ASSERT_TRUE(WideColumnSerialization::HasBlobColumns(Slice(output)));
}

TEST(WideColumnSerializationTest, DeserializeV2WithBlobIndex) {
  // Create columns with a blob index
  std::vector<std::pair<std::string, std::string>> columns = {
      {"alpha", "inline_value_alpha"},
      {"beta", "placeholder"},  // Will be blob
      {"gamma", "inline_value_gamma"}};

  // Create a blob index for beta (index 1)
  std::string blob_index_str;
  BlobIndex::EncodeBlob(&blob_index_str, /* file_number */ 42,
                        /* offset */ 1024, /* size */ 2048, kNoCompression);

  BlobIndex blob_idx;
  Slice blob_slice(blob_index_str);
  ASSERT_OK(blob_idx.DecodeFrom(blob_slice));

  std::vector<std::pair<size_t, BlobIndex>> blob_columns_in = {{1, blob_idx}};

  std::string serialized;
  ASSERT_OK(WideColumnSerialization::SerializeWithBlobIndices(
      columns, blob_columns_in, &serialized));

  // Now deserialize using DeserializeColumns
  Slice input(serialized);
  std::vector<WideColumn> deserialized_columns;
  std::vector<std::pair<size_t, BlobIndex>> blob_columns_out;

  ASSERT_OK(WideColumnSerialization::DeserializeColumns(
      input, &deserialized_columns, &blob_columns_out));

  // Should have 3 columns
  ASSERT_EQ(deserialized_columns.size(), 3u);

  // Verify column names
  ASSERT_EQ(deserialized_columns[0].name(), "alpha");
  ASSERT_EQ(deserialized_columns[1].name(), "beta");
  ASSERT_EQ(deserialized_columns[2].name(), "gamma");

  // Verify inline column values
  ASSERT_EQ(deserialized_columns[0].value(), "inline_value_alpha");
  ASSERT_EQ(deserialized_columns[2].value(), "inline_value_gamma");

  // Verify blob columns
  ASSERT_EQ(blob_columns_out.size(), 1u);
  ASSERT_EQ(blob_columns_out[0].first, 1u);  // column index

  const BlobIndex& decoded_blob = blob_columns_out[0].second;
  ASSERT_FALSE(decoded_blob.IsInlined());
  ASSERT_FALSE(decoded_blob.HasTTL());
  ASSERT_EQ(decoded_blob.file_number(), 42u);
  ASSERT_EQ(decoded_blob.offset(), 1024u);
  ASSERT_EQ(decoded_blob.size(), 2048u);
  ASSERT_EQ(decoded_blob.compression(), kNoCompression);
}

TEST(WideColumnSerializationTest, V2BackwardCompatibleWithV1) {
  // Serialize V2 format with NO blob references - all inline columns
  std::vector<std::pair<std::string, std::string>> columns = {
      {"col_a", "value_a"}, {"col_b", "value_b"}};

  std::vector<std::pair<size_t, BlobIndex>> empty_blob_columns;

  std::string serialized;
  ASSERT_OK(WideColumnSerialization::SerializeWithBlobIndices(
      columns, empty_blob_columns, &serialized));

  // Verify version is 2
  ASSERT_EQ(WideColumnSerialization::GetVersion(Slice(serialized)),
            WideColumnSerialization::kVersion2);

  // HasBlobColumns should return false since all columns are inline
  ASSERT_FALSE(WideColumnSerialization::HasBlobColumns(Slice(serialized)));

  // The standard Deserialize() should work since there are no blob columns
  Slice input(serialized);
  WideColumns deserialized;

  ASSERT_OK(WideColumnSerialization::Deserialize(input, deserialized));

  ASSERT_EQ(deserialized.size(), 2u);
  ASSERT_EQ(deserialized[0].name(), "col_a");
  ASSERT_EQ(deserialized[0].value(), "value_a");
  ASSERT_EQ(deserialized[1].name(), "col_b");
  ASSERT_EQ(deserialized[1].value(), "value_b");
}

TEST(WideColumnSerializationTest, V1ForwardCompatibleWithV2NoBlobRefs) {
  // Serialize using V1 format (the regular Serialize method)
  WideColumns columns{{"name1", "val1"}, {"name2", "val2"}, {"name3", "val3"}};
  std::string serialized;

  ASSERT_OK(WideColumnSerialization::Serialize(columns, serialized));

  // Verify version is 1
  ASSERT_EQ(WideColumnSerialization::GetVersion(Slice(serialized)),
            WideColumnSerialization::kVersion1);

  // HasBlobColumns should return false for V1 format
  ASSERT_FALSE(WideColumnSerialization::HasBlobColumns(Slice(serialized)));

  // DeserializeColumns should be able to read V1 format
  Slice input(serialized);
  std::vector<WideColumn> deserialized_columns;
  std::vector<std::pair<size_t, BlobIndex>> blob_columns;

  ASSERT_OK(WideColumnSerialization::DeserializeColumns(
      input, &deserialized_columns, &blob_columns));

  // Should have 3 columns, no blob columns
  ASSERT_EQ(deserialized_columns.size(), 3u);
  ASSERT_TRUE(blob_columns.empty());

  // Verify values
  ASSERT_EQ(deserialized_columns[0].name(), "name1");
  ASSERT_EQ(deserialized_columns[0].value(), "val1");
  ASSERT_EQ(deserialized_columns[1].name(), "name2");
  ASSERT_EQ(deserialized_columns[1].value(), "val2");
  ASSERT_EQ(deserialized_columns[2].name(), "name3");
  ASSERT_EQ(deserialized_columns[2].value(), "val3");
}

TEST(WideColumnSerializationTest, MixedInlineAndBlobColumns) {
  // Test with multiple blob columns mixed with inline columns
  std::vector<std::pair<std::string, std::string>> columns = {
      {"a_inline", "value_a"},
      {"b_blob", "placeholder"},
      {"c_inline", "value_c"},
      {"d_blob", "placeholder"},
      {"e_inline", "value_e"}};

  // Create blob indices for columns 1 and 3
  std::string blob_index_str1;
  BlobIndex::EncodeBlob(&blob_index_str1, /* file_number */ 10,
                        /* offset */ 100, /* size */ 1000, kNoCompression);
  BlobIndex blob_idx1;
  Slice blob_slice1(blob_index_str1);
  ASSERT_OK(blob_idx1.DecodeFrom(blob_slice1));

  // Second blob with TTL
  std::string blob_index_str2;
  BlobIndex::EncodeBlobTTL(&blob_index_str2, /* expiration */ 9999999,
                           /* file_number */ 20, /* offset */ 200,
                           /* size */ 2000, kSnappyCompression);
  BlobIndex blob_idx2;
  Slice blob_slice2(blob_index_str2);
  ASSERT_OK(blob_idx2.DecodeFrom(blob_slice2));

  std::vector<std::pair<size_t, BlobIndex>> blob_columns_in = {{1, blob_idx1},
                                                               {3, blob_idx2}};

  std::string serialized;
  ASSERT_OK(WideColumnSerialization::SerializeWithBlobIndices(
      columns, blob_columns_in, &serialized));

  // Verify version and HasBlobColumns
  ASSERT_EQ(WideColumnSerialization::GetVersion(Slice(serialized)),
            WideColumnSerialization::kVersion2);
  ASSERT_TRUE(WideColumnSerialization::HasBlobColumns(Slice(serialized)));

  // Deserialize
  Slice input(serialized);
  std::vector<WideColumn> deserialized_columns;
  std::vector<std::pair<size_t, BlobIndex>> blob_columns_out;

  ASSERT_OK(WideColumnSerialization::DeserializeColumns(
      input, &deserialized_columns, &blob_columns_out));

  // Should have 5 columns total
  ASSERT_EQ(deserialized_columns.size(), 5u);

  // Verify column names
  ASSERT_EQ(deserialized_columns[0].name(), "a_inline");
  ASSERT_EQ(deserialized_columns[1].name(), "b_blob");
  ASSERT_EQ(deserialized_columns[2].name(), "c_inline");
  ASSERT_EQ(deserialized_columns[3].name(), "d_blob");
  ASSERT_EQ(deserialized_columns[4].name(), "e_inline");

  // Verify inline column values
  ASSERT_EQ(deserialized_columns[0].value(), "value_a");
  ASSERT_EQ(deserialized_columns[2].value(), "value_c");
  ASSERT_EQ(deserialized_columns[4].value(), "value_e");

  // Verify blob columns - should have 2 blob columns
  ASSERT_EQ(blob_columns_out.size(), 2u);

  // First blob (index 1)
  ASSERT_EQ(blob_columns_out[0].first, 1u);
  const BlobIndex& blob1 = blob_columns_out[0].second;
  ASSERT_FALSE(blob1.IsInlined());
  ASSERT_FALSE(blob1.HasTTL());
  ASSERT_EQ(blob1.file_number(), 10u);
  ASSERT_EQ(blob1.offset(), 100u);
  ASSERT_EQ(blob1.size(), 1000u);
  ASSERT_EQ(blob1.compression(), kNoCompression);

  // Second blob (index 3) - with TTL
  ASSERT_EQ(blob_columns_out[1].first, 3u);
  const BlobIndex& blob2 = blob_columns_out[1].second;
  ASSERT_FALSE(blob2.IsInlined());
  ASSERT_TRUE(blob2.HasTTL());
  ASSERT_EQ(blob2.expiration(), 9999999u);
  ASSERT_EQ(blob2.file_number(), 20u);
  ASSERT_EQ(blob2.offset(), 200u);
  ASSERT_EQ(blob2.size(), 2000u);
  ASSERT_EQ(blob2.compression(), kSnappyCompression);
}

TEST(WideColumnSerializationTest, SerializeResolvedEntityMixedColumns) {
  // Create a V2 entity with mixed inline and blob columns, then resolve
  // blob values and verify the result is a valid V1 entity.
  std::vector<std::pair<std::string, std::string>> columns = {
      {"col_a", "inline_a"},
      {"col_b", "placeholder"},  // blob column
      {"col_c", "inline_c"}};

  // Create a blob index for col_b (index 1)
  std::string blob_index_str;
  BlobIndex::EncodeBlob(&blob_index_str, /* file_number */ 50,
                        /* offset */ 500, /* size */ 100, kNoCompression);
  BlobIndex blob_idx;
  Slice blob_slice(blob_index_str);
  ASSERT_OK(blob_idx.DecodeFrom(blob_slice));

  std::vector<std::pair<size_t, BlobIndex>> blob_columns_in = {{1, blob_idx}};

  std::string serialized;
  ASSERT_OK(WideColumnSerialization::SerializeWithBlobIndices(
      columns, blob_columns_in, &serialized));

  // Deserialize to get columns and blob column info
  Slice input(serialized);
  std::vector<WideColumn> deserialized_columns;
  std::vector<std::pair<size_t, BlobIndex>> blob_columns_out;
  ASSERT_OK(WideColumnSerialization::DeserializeColumns(
      input, &deserialized_columns, &blob_columns_out));
  ASSERT_EQ(blob_columns_out.size(), 1u);

  // Simulate resolved blob values
  std::vector<std::string> resolved_blob_values = {"resolved_blob_b"};

  // Serialize resolved entity
  std::string resolved_output;
  ASSERT_OK(WideColumnSerialization::SerializeResolvedEntity(
      deserialized_columns, blob_columns_out, resolved_blob_values,
      &resolved_output));

  // The result should be a V1 entity with all values inline
  ASSERT_EQ(WideColumnSerialization::GetVersion(Slice(resolved_output)),
            WideColumnSerialization::kVersion1);
  ASSERT_FALSE(WideColumnSerialization::HasBlobColumns(Slice(resolved_output)));

  // Deserialize the V1 result and verify
  Slice resolved_input(resolved_output);
  WideColumns result_columns;
  ASSERT_OK(
      WideColumnSerialization::Deserialize(resolved_input, result_columns));

  ASSERT_EQ(result_columns.size(), 3u);
  ASSERT_EQ(result_columns[0].name(), "col_a");
  ASSERT_EQ(result_columns[0].value(), "inline_a");
  ASSERT_EQ(result_columns[1].name(), "col_b");
  ASSERT_EQ(result_columns[1].value(), "resolved_blob_b");
  ASSERT_EQ(result_columns[2].name(), "col_c");
  ASSERT_EQ(result_columns[2].value(), "inline_c");
}

TEST(WideColumnSerializationTest, SerializeResolvedEntityAllBlobColumns) {
  // Test entity where every column is a blob reference
  std::vector<std::pair<std::string, std::string>> columns = {
      {"x", "placeholder1"}, {"y", "placeholder2"}, {"z", "placeholder3"}};

  // Create blob indices for all columns
  std::string bi_str1, bi_str2, bi_str3;
  BlobIndex::EncodeBlob(&bi_str1, 10, 100, 50, kNoCompression);
  BlobIndex::EncodeBlob(&bi_str2, 20, 200, 60, kNoCompression);
  BlobIndex::EncodeBlob(&bi_str3, 30, 300, 70, kNoCompression);

  BlobIndex bi1, bi2, bi3;
  Slice s1(bi_str1), s2(bi_str2), s3(bi_str3);
  ASSERT_OK(bi1.DecodeFrom(s1));
  ASSERT_OK(bi2.DecodeFrom(s2));
  ASSERT_OK(bi3.DecodeFrom(s3));

  std::vector<std::pair<size_t, BlobIndex>> blob_columns_in = {
      {0, bi1}, {1, bi2}, {2, bi3}};

  std::string serialized;
  ASSERT_OK(WideColumnSerialization::SerializeWithBlobIndices(
      columns, blob_columns_in, &serialized));
  ASSERT_TRUE(WideColumnSerialization::HasBlobColumns(Slice(serialized)));

  // Deserialize
  Slice input(serialized);
  std::vector<WideColumn> deserialized_columns;
  std::vector<std::pair<size_t, BlobIndex>> blob_columns_out;
  ASSERT_OK(WideColumnSerialization::DeserializeColumns(
      input, &deserialized_columns, &blob_columns_out));
  ASSERT_EQ(blob_columns_out.size(), 3u);

  // Resolve all blobs
  std::vector<std::string> resolved = {"val_x", "val_y", "val_z"};

  std::string resolved_output;
  ASSERT_OK(WideColumnSerialization::SerializeResolvedEntity(
      deserialized_columns, blob_columns_out, resolved, &resolved_output));

  // Verify V1 format result
  ASSERT_EQ(WideColumnSerialization::GetVersion(Slice(resolved_output)),
            WideColumnSerialization::kVersion1);
  ASSERT_FALSE(WideColumnSerialization::HasBlobColumns(Slice(resolved_output)));

  Slice resolved_input(resolved_output);
  WideColumns result_columns;
  ASSERT_OK(
      WideColumnSerialization::Deserialize(resolved_input, result_columns));

  ASSERT_EQ(result_columns.size(), 3u);
  ASSERT_EQ(result_columns[0].name(), "x");
  ASSERT_EQ(result_columns[0].value(), "val_x");
  ASSERT_EQ(result_columns[1].name(), "y");
  ASSERT_EQ(result_columns[1].value(), "val_y");
  ASSERT_EQ(result_columns[2].name(), "z");
  ASSERT_EQ(result_columns[2].value(), "val_z");
}

TEST(WideColumnSerializationTest, SerializeResolvedEntityNoBlobColumns) {
  // Test SerializeResolvedEntity with empty blob columns vector
  // (should behave like regular Serialize)
  WideColumns columns{{"alpha", "val_alpha"}, {"beta", "val_beta"}};

  std::vector<std::pair<size_t, BlobIndex>> empty_blob_columns;
  std::vector<std::string> empty_resolved;

  std::string resolved_output;
  ASSERT_OK(WideColumnSerialization::SerializeResolvedEntity(
      columns, empty_blob_columns, empty_resolved, &resolved_output));

  // Should produce valid V1 output
  ASSERT_EQ(WideColumnSerialization::GetVersion(Slice(resolved_output)),
            WideColumnSerialization::kVersion1);

  Slice resolved_input(resolved_output);
  WideColumns result_columns;
  ASSERT_OK(
      WideColumnSerialization::Deserialize(resolved_input, result_columns));

  ASSERT_EQ(result_columns.size(), 2u);
  ASSERT_EQ(result_columns[0].name(), "alpha");
  ASSERT_EQ(result_columns[0].value(), "val_alpha");
  ASSERT_EQ(result_columns[1].name(), "beta");
  ASSERT_EQ(result_columns[1].value(), "val_beta");
}

TEST(WideColumnSerializationTest, V2EmptyColumns) {
  // Serialize V2 with zero columns
  std::vector<std::pair<std::string, std::string>> columns;
  std::vector<std::pair<size_t, BlobIndex>> empty_blob_columns;

  std::string serialized;
  ASSERT_OK(WideColumnSerialization::SerializeWithBlobIndices(
      columns, empty_blob_columns, &serialized));

  ASSERT_EQ(WideColumnSerialization::GetVersion(Slice(serialized)),
            WideColumnSerialization::kVersion2);
  ASSERT_FALSE(WideColumnSerialization::HasBlobColumns(Slice(serialized)));

  // Deserialize via Deserialize
  {
    Slice input(serialized);
    WideColumns deserialized;
    ASSERT_OK(WideColumnSerialization::Deserialize(input, deserialized));
    ASSERT_TRUE(deserialized.empty());
  }

  // Deserialize via DeserializeColumns
  {
    Slice input(serialized);
    std::vector<WideColumn> deserialized;
    std::vector<std::pair<size_t, BlobIndex>> blob_cols;
    ASSERT_OK(WideColumnSerialization::DeserializeColumns(input, &deserialized,
                                                          &blob_cols));
    ASSERT_TRUE(deserialized.empty());
    ASSERT_TRUE(blob_cols.empty());
  }
}

TEST(WideColumnSerializationTest, V2SingleColumn) {
  // Serialize V2 with a single inline column
  std::vector<std::pair<std::string, std::string>> columns = {
      {"only_col", "only_val"}};
  std::vector<std::pair<size_t, BlobIndex>> empty_blob_columns;

  std::string serialized;
  ASSERT_OK(WideColumnSerialization::SerializeWithBlobIndices(
      columns, empty_blob_columns, &serialized));

  Slice input(serialized);
  WideColumns deserialized;
  ASSERT_OK(WideColumnSerialization::Deserialize(input, deserialized));

  ASSERT_EQ(deserialized.size(), 1u);
  ASSERT_EQ(deserialized[0].name(), "only_col");
  ASSERT_EQ(deserialized[0].value(), "only_val");
}

TEST(WideColumnSerializationTest, V2GetValueOfDefaultColumnFastPath) {
  // Test GetValueOfDefaultColumn with V2 format containing default column
  std::vector<std::pair<std::string, std::string>> columns = {
      {"", "default_value"}, {"col1", "value1"}, {"col2", "value2"}};
  std::vector<std::pair<size_t, BlobIndex>> empty_blob_columns;

  std::string serialized;
  ASSERT_OK(WideColumnSerialization::SerializeWithBlobIndices(
      columns, empty_blob_columns, &serialized));

  Slice input(serialized);
  Slice value;
  ASSERT_OK(WideColumnSerialization::GetValueOfDefaultColumn(input, value));
  ASSERT_EQ(value, "default_value");
}

TEST(WideColumnSerializationTest, V2GetValueOfDefaultColumnNoDefault) {
  // Test GetValueOfDefaultColumn with V2 format where first column is not
  // default (non-empty name)
  std::vector<std::pair<std::string, std::string>> columns = {
      {"col1", "value1"}, {"col2", "value2"}};
  std::vector<std::pair<size_t, BlobIndex>> empty_blob_columns;

  std::string serialized;
  ASSERT_OK(WideColumnSerialization::SerializeWithBlobIndices(
      columns, empty_blob_columns, &serialized));

  Slice input(serialized);
  Slice value;
  ASSERT_OK(WideColumnSerialization::GetValueOfDefaultColumn(input, value));
  ASSERT_TRUE(value.empty());
}

TEST(WideColumnSerializationTest, V2GetValueOfDefaultColumnV1Fallback) {
  // Test GetValueOfDefaultColumn with V1 format (fallback path)
  WideColumns columns{{"", "v1_default"}, {"col1", "v1"}};
  std::string serialized;
  ASSERT_OK(WideColumnSerialization::Serialize(columns, serialized));

  Slice input(serialized);
  Slice value;
  ASSERT_OK(WideColumnSerialization::GetValueOfDefaultColumn(input, value));
  ASSERT_EQ(value, "v1_default");
}

TEST(WideColumnSerializationTest, V2DeserializeBlobColumnRejectsDeserialize) {
  // Serialize V2 with a blob column, then try to use Deserialize()
  // which should return NotSupported
  std::vector<std::pair<std::string, std::string>> columns = {
      {"a", "inline"}, {"b", "placeholder"}};

  std::string blob_index_str;
  BlobIndex::EncodeBlob(&blob_index_str, 1, 2, 3, kNoCompression);
  BlobIndex blob_idx;
  Slice blob_slice(blob_index_str);
  ASSERT_OK(blob_idx.DecodeFrom(blob_slice));

  std::vector<std::pair<size_t, BlobIndex>> blob_columns = {{1, blob_idx}};

  std::string serialized;
  ASSERT_OK(WideColumnSerialization::SerializeWithBlobIndices(
      columns, blob_columns, &serialized));

  Slice input(serialized);
  WideColumns deserialized;
  Status s = WideColumnSerialization::Deserialize(input, deserialized);
  ASSERT_TRUE(s.IsNotSupported());
}

TEST(WideColumnSerializationTest, V2LayoutStructureVerification) {
  // Verify the V2 binary layout structure by manually parsing sections
  std::vector<std::pair<std::string, std::string>> columns = {
      {"aa", "val_aa"}, {"bbb", "val_bbb"}};
  std::vector<std::pair<size_t, BlobIndex>> empty_blob_columns;

  std::string serialized;
  ASSERT_OK(WideColumnSerialization::SerializeWithBlobIndices(
      columns, empty_blob_columns, &serialized));

  Slice data(serialized);

  // Section 1: HEADER
  uint32_t version = 0;
  ASSERT_TRUE(GetVarint32(&data, &version));
  ASSERT_EQ(version, WideColumnSerialization::kVersion2);

  uint32_t num_columns = 0;
  ASSERT_TRUE(GetVarint32(&data, &num_columns));
  ASSERT_EQ(num_columns, 2u);

  // Section 2: COLUMN TYPES (2 bytes, both inline)
  ASSERT_GE(data.size(), 2u);
  ASSERT_EQ(static_cast<uint8_t>(data[0]),
            WideColumnSerialization::kColumnTypeInline);
  ASSERT_EQ(static_cast<uint8_t>(data[1]),
            WideColumnSerialization::kColumnTypeInline);
  data.remove_prefix(2);

  // Section 3: SKIP INFO
  uint32_t name_sizes_bytes = 0;
  uint32_t names_bytes = 0;
  ASSERT_TRUE(GetVarint32(&data, &name_sizes_bytes));
  ASSERT_TRUE(GetVarint32(&data, &names_bytes));
  // name sizes: varint(2) + varint(3) = 1 + 1 = 2 bytes
  ASSERT_EQ(name_sizes_bytes, 2u);
  // names: "aa" + "bbb" = 2 + 3 = 5 bytes
  ASSERT_EQ(names_bytes, 5u);

  // Section 4: NAME SIZES
  uint32_t ns0 = 0, ns1 = 0;
  ASSERT_TRUE(GetVarint32(&data, &ns0));
  ASSERT_TRUE(GetVarint32(&data, &ns1));
  ASSERT_EQ(ns0, 2u);
  ASSERT_EQ(ns1, 3u);

  // Section 5: VALUE SIZES
  uint32_t vs0 = 0, vs1 = 0;
  ASSERT_TRUE(GetVarint32(&data, &vs0));
  ASSERT_TRUE(GetVarint32(&data, &vs1));
  ASSERT_EQ(vs0, 6u);  // "val_aa" = 6
  ASSERT_EQ(vs1, 7u);  // "val_bbb" = 7

  // Section 6: COLUMN NAMES
  ASSERT_GE(data.size(), 5u);
  ASSERT_EQ(Slice(data.data(), 2), "aa");
  ASSERT_EQ(Slice(data.data() + 2, 3), "bbb");
  data.remove_prefix(5);

  // Section 7: COLUMN VALUES
  ASSERT_GE(data.size(), 13u);
  ASSERT_EQ(Slice(data.data(), 6), "val_aa");
  ASSERT_EQ(Slice(data.data() + 6, 7), "val_bbb");
}

TEST(WideColumnSerializationTest, V2GetValueOfDefaultColumnEmpty) {
  // Test GetValueOfDefaultColumn with V2 format with zero columns
  std::vector<std::pair<std::string, std::string>> columns;
  std::vector<std::pair<size_t, BlobIndex>> empty_blob_columns;

  std::string serialized;
  ASSERT_OK(WideColumnSerialization::SerializeWithBlobIndices(
      columns, empty_blob_columns, &serialized));

  Slice input(serialized);
  Slice value;
  ASSERT_OK(WideColumnSerialization::GetValueOfDefaultColumn(input, value));
  ASSERT_TRUE(value.empty());
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

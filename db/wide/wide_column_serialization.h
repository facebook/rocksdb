//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <cstdint>
#include <string>
#include <utility>
#include <vector>

#include "rocksdb/rocksdb_namespace.h"
#include "rocksdb/status.h"
#include "rocksdb/wide_columns.h"

namespace ROCKSDB_NAMESPACE {

class BlobIndex;
class Slice;

// Wide-column serialization/deserialization primitives.
//
// Version 1 Layout:
// The two main parts of the layout are 1) a sorted index containing the column
// names and column value sizes and 2) the column values themselves. Keeping the
// index and the values separate will enable selectively reading column values
// down the line. Note that currently the index has to be fully parsed in order
// to find out the offset of each column value.
//
// Legend: cn = column name, cv = column value, cns = column name size, cvs =
// column value size.
//
//      +----------+--------------+----------+-------+----------+---...
//      | version  | # of columns |  cns 1   | cn 1  |  cvs 1   |
//      +----------+--------------+------------------+--------- +---...
//      | varint32 |   varint32   | varint32 | bytes | varint32 |
//      +----------+--------------+----------+-------+----------+---...
//
//      ... continued ...
//
//          ...---+----------+-------+----------+-------+---...---+-------+
//                |  cns N   | cn N  |  cvs N   | cv 1  |         | cv N  |
//          ...---+----------+-------+----------+-------+---...---+-------+
//                | varint32 | bytes | varint32 | bytes |         | bytes |
//          ...---+----------+-------+----------+-------+---...---+-------+
//
// Version 2 Layout (with blob index support):
// Extends version 1 by adding a column type field after each column name.
// This allows columns to store either inline values or references to blobs.
//
// Legend: cn = column name, cv = column value, cns = column name size,
//         cvs = column value size, ct = column type.
//
//      +----------+--------------+----------+-------+--------+----------+---...
//      | version  | # of columns |  cns 1   | cn 1  |  ct 1  |  cvs 1   |
//      +----------+--------------+----------+-------+--------+----------+---...
//      | varint32 |   varint32   | varint32 | bytes | 1 byte | varint32 |
//      +----------+--------------+----------+-------+--------+----------+---...
//
//      ... continued ...
//
//        ...---+----------+-------+--------+----------+-------+---...---+-------+
//              |  cns N   | cn N  |  ct N  |  cvs N   | cv 1  |         | cv N
//              |
//        ...---+----------+-------+--------+----------+-------+---...---+-------+
//              | varint32 | bytes | 1 byte | varint32 | bytes |         | bytes
//              |
//        ...---+----------+-------+--------+----------+-------+---...---+-------+
//
// Column type (ct): 0 = inline value, 1 = blob index
// When ct = 1, the cv contains a serialized BlobIndex.

class WideColumnSerialization {
 public:
  // Version constants for wide column serialization format.
  // - kVersion1: Original format with inline column values only.
  // - kVersion2: Extended format that supports blob index references in
  // columns.
  //              Used when large column values are stored in blob files.
  static constexpr uint32_t kVersion1 = 1;
  static constexpr uint32_t kVersion2 = 2;

  // Column type constants for version 2 format
  static constexpr uint8_t kColumnTypeInline = 0;
  static constexpr uint8_t kColumnTypeBlobIndex = 1;

  // Serialize columns using version 1 format (no blob support)
  static Status Serialize(const WideColumns& columns, std::string& output);

  // Serialize columns with some columns replaced by blob indices (version 2)
  // columns: vector of (column_name, column_value) pairs
  // blob_columns: vector of (column_index, blob_index) pairs indicating which
  //               columns should be stored as blob references
  static Status SerializeWithBlobIndices(
      const std::vector<std::pair<std::string, std::string>>& columns,
      const std::vector<std::pair<size_t, BlobIndex>>& blob_columns,
      std::string* output);

  // Deserialize columns (version 1 format only)
  static Status Deserialize(Slice& input, WideColumns& columns);

  // Deserialize columns and separate inline columns from blob columns
  // columns: receives inline column values
  // blob_columns: receives (column_index, blob_index) pairs for blob references
  static Status DeserializeColumns(
      Slice& input, std::vector<WideColumn>* columns,
      std::vector<std::pair<size_t, BlobIndex>>* blob_columns);

  // Check if the serialized entity has any blob column references
  // Returns true if version >= 2 and at least one column has blob type
  static bool HasBlobColumns(const Slice& input);

  // Get the serialization version from the input
  // Returns the version number, or 0 if the input is invalid
  static uint32_t GetVersion(const Slice& input);

  static Status GetValueOfDefaultColumn(Slice& input, Slice& value);

  // Helper function to merge deserialized columns with resolved blob values
  // and serialize the result using version 1 format (all values inline).
  //
  // This is useful when an entity with blob columns needs to be re-serialized
  // with all blob values resolved/inlined (e.g., for merge operations, reads).
  //
  // Parameters:
  //   columns: The columns from DeserializeColumns()
  //   blob_columns: The blob column info from DeserializeColumns()
  //   resolved_blob_values: Vector of resolved blob values, one per blob column
  //                         in the same order as blob_columns
  //   output: Receives the serialized entity in V1 format
  //
  // Returns OK on success, or an error status on failure.
  static Status SerializeResolvedEntity(
      const std::vector<WideColumn>& columns,
      const std::vector<std::pair<size_t, BlobIndex>>& blob_columns,
      const std::vector<std::string>& resolved_blob_values,
      std::string* output);
};

}  // namespace ROCKSDB_NAMESPACE

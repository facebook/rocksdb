//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/wide/wide_column_serialization.h"

#include <algorithm>
#include <cassert>
#include <limits>
#include <set>
#include <unordered_map>

#include "db/blob/blob_index.h"
#include "db/wide/wide_columns_helper.h"
#include "rocksdb/slice.h"
#include "util/autovector.h"
#include "util/coding.h"

namespace ROCKSDB_NAMESPACE {

Status WideColumnSerialization::Serialize(const WideColumns& columns,
                                          std::string& output) {
  const size_t num_columns = columns.size();

  if (num_columns > static_cast<size_t>(std::numeric_limits<uint32_t>::max())) {
    return Status::InvalidArgument("Too many wide columns");
  }

  PutVarint32(&output, kVersion1);

  PutVarint32(&output, static_cast<uint32_t>(num_columns));

  const Slice* prev_name = nullptr;

  for (size_t i = 0; i < columns.size(); ++i) {
    const WideColumn& column = columns[i];

    const Slice& name = column.name();
    if (name.size() >
        static_cast<size_t>(std::numeric_limits<uint32_t>::max())) {
      return Status::InvalidArgument("Wide column name too long");
    }

    if (prev_name && prev_name->compare(name) >= 0) {
      return Status::Corruption("Wide columns out of order");
    }

    const Slice& value = column.value();
    if (value.size() >
        static_cast<size_t>(std::numeric_limits<uint32_t>::max())) {
      return Status::InvalidArgument("Wide column value too long");
    }

    PutLengthPrefixedSlice(&output, name);
    PutVarint32(&output, static_cast<uint32_t>(value.size()));

    prev_name = &name;
  }

  for (const auto& column : columns) {
    const Slice& value = column.value();

    output.append(value.data(), value.size());
  }

  return Status::OK();
}

Status WideColumnSerialization::SerializeWithBlobIndices(
    const std::vector<std::pair<std::string, std::string>>& columns,
    const std::vector<std::pair<size_t, BlobIndex>>& blob_columns,
    std::string* output) {
  assert(output != nullptr);

  const size_t num_columns = columns.size();

  if (num_columns > static_cast<size_t>(std::numeric_limits<uint32_t>::max())) {
    return Status::InvalidArgument("Too many wide columns");
  }

  // Build a set of column indices that are blob references for O(1) lookup
  std::set<size_t> blob_column_indices;
  for (const auto& blob_col : blob_columns) {
    if (blob_col.first >= num_columns) {
      return Status::InvalidArgument("Blob column index out of range");
    }
    blob_column_indices.insert(blob_col.first);
  }

  // Create a map from column index to blob index
  std::vector<const BlobIndex*> blob_index_map(num_columns, nullptr);
  for (const auto& blob_col : blob_columns) {
    blob_index_map[blob_col.first] = &blob_col.second;
  }

  // Write version 2 header
  PutVarint32(output, kVersion2);
  PutVarint32(output, static_cast<uint32_t>(num_columns));

  const std::string* prev_name = nullptr;

  // Prepare serialized blob indices and compute value sizes
  std::vector<std::string> serialized_blob_indices(num_columns);
  std::vector<uint32_t> value_sizes(num_columns);

  for (size_t i = 0; i < num_columns; ++i) {
    const std::string& name = columns[i].first;
    const std::string& value = columns[i].second;

    if (name.size() >
        static_cast<size_t>(std::numeric_limits<uint32_t>::max())) {
      return Status::InvalidArgument("Wide column name too long");
    }

    if (prev_name && *prev_name >= name) {
      return Status::Corruption("Wide columns out of order");
    }

    if (blob_index_map[i] != nullptr) {
      // This column is a blob reference - serialize the blob index
      const BlobIndex* blob_idx = blob_index_map[i];
      std::string& serialized = serialized_blob_indices[i];

      if (blob_idx->IsInlined()) {
        BlobIndex::EncodeInlinedTTL(&serialized, blob_idx->expiration(),
                                    blob_idx->value());
      } else if (blob_idx->HasTTL()) {
        BlobIndex::EncodeBlobTTL(&serialized, blob_idx->expiration(),
                                 blob_idx->file_number(), blob_idx->offset(),
                                 blob_idx->size(), blob_idx->compression());
      } else {
        BlobIndex::EncodeBlob(&serialized, blob_idx->file_number(),
                              blob_idx->offset(), blob_idx->size(),
                              blob_idx->compression());
      }

      value_sizes[i] = static_cast<uint32_t>(serialized.size());
    } else {
      // This column is an inline value
      if (value.size() >
          static_cast<size_t>(std::numeric_limits<uint32_t>::max())) {
        return Status::InvalidArgument("Wide column value too long");
      }
      value_sizes[i] = static_cast<uint32_t>(value.size());
    }

    prev_name = &name;
  }

  // Write the index (column names, types, and value sizes)
  for (size_t i = 0; i < num_columns; ++i) {
    const std::string& name = columns[i].first;

    PutLengthPrefixedSlice(output, Slice(name));

    // Write column type
    if (blob_column_indices.count(i) > 0) {
      output->push_back(static_cast<char>(kColumnTypeBlobIndex));
    } else {
      output->push_back(static_cast<char>(kColumnTypeInline));
    }

    PutVarint32(output, value_sizes[i]);
  }

  // Write the values
  for (size_t i = 0; i < num_columns; ++i) {
    if (blob_index_map[i] != nullptr) {
      // Write serialized blob index
      output->append(serialized_blob_indices[i]);
    } else {
      // Write inline value
      output->append(columns[i].second);
    }
  }

  return Status::OK();
}

Status WideColumnSerialization::Deserialize(Slice& input,
                                            WideColumns& columns) {
  assert(columns.empty());

  uint32_t version = 0;
  if (!GetVarint32(&input, &version)) {
    return Status::Corruption("Error decoding wide column version");
  }

  if (version > kVersion2) {
    return Status::NotSupported("Unsupported wide column version");
  }

  uint32_t num_columns = 0;
  if (!GetVarint32(&input, &num_columns)) {
    return Status::Corruption("Error decoding number of wide columns");
  }

  if (!num_columns) {
    return Status::OK();
  }

  columns.reserve(num_columns);

  autovector<uint32_t, 16> column_value_sizes;
  column_value_sizes.reserve(num_columns);

  // For version 2, we also need to track column types
  autovector<uint8_t, 16> column_types;
  if (version >= kVersion2) {
    column_types.reserve(num_columns);
  }

  for (uint32_t i = 0; i < num_columns; ++i) {
    Slice name;
    if (!GetLengthPrefixedSlice(&input, &name)) {
      return Status::Corruption("Error decoding wide column name");
    }

    if (!columns.empty() && columns.back().name().compare(name) >= 0) {
      return Status::Corruption("Wide columns out of order");
    }

    columns.emplace_back(name, Slice());

    // For version 2, read the column type
    if (version >= kVersion2) {
      if (input.size() < 1) {
        return Status::Corruption("Error decoding wide column type");
      }
      uint8_t col_type = static_cast<uint8_t>(input[0]);
      input.remove_prefix(1);

      // For the basic Deserialize, we don't support blob columns
      // Blob columns should be read via DeserializeColumns
      if (col_type == kColumnTypeBlobIndex) {
        return Status::NotSupported(
            "Wide column contains blob references. Use DeserializeColumns.");
      }
      column_types.emplace_back(col_type);
    }

    uint32_t value_size = 0;
    if (!GetVarint32(&input, &value_size)) {
      return Status::Corruption("Error decoding wide column value size");
    }

    column_value_sizes.emplace_back(value_size);
  }

  const Slice data(input);
  size_t pos = 0;

  for (uint32_t i = 0; i < num_columns; ++i) {
    const uint32_t value_size = column_value_sizes[i];

    if (pos + value_size > data.size()) {
      return Status::Corruption("Error decoding wide column value payload");
    }

    columns[i].value() = Slice(data.data() + pos, value_size);

    pos += value_size;
  }

  return Status::OK();
}

Status WideColumnSerialization::DeserializeColumns(
    Slice& input, std::vector<WideColumn>* columns,
    std::vector<std::pair<size_t, BlobIndex>>* blob_columns) {
  assert(columns != nullptr);
  assert(columns->empty());
  assert(blob_columns != nullptr);
  assert(blob_columns->empty());

  uint32_t version = 0;
  if (!GetVarint32(&input, &version)) {
    return Status::Corruption("Error decoding wide column version");
  }

  if (version > kVersion2) {
    return Status::NotSupported("Unsupported wide column version");
  }

  uint32_t num_columns = 0;
  if (!GetVarint32(&input, &num_columns)) {
    return Status::Corruption("Error decoding number of wide columns");
  }

  if (!num_columns) {
    return Status::OK();
  }

  columns->reserve(num_columns);

  autovector<uint32_t, 16> column_value_sizes;
  column_value_sizes.reserve(num_columns);

  autovector<uint8_t, 16> column_types;
  column_types.reserve(num_columns);

  for (uint32_t i = 0; i < num_columns; ++i) {
    Slice name;
    if (!GetLengthPrefixedSlice(&input, &name)) {
      return Status::Corruption("Error decoding wide column name");
    }

    if (!columns->empty() && columns->back().name().compare(name) >= 0) {
      return Status::Corruption("Wide columns out of order");
    }

    columns->emplace_back(name, Slice());

    // Read column type (version 2) or assume inline (version 1)
    uint8_t col_type = kColumnTypeInline;
    if (version >= kVersion2) {
      if (input.size() < 1) {
        return Status::Corruption("Error decoding wide column type");
      }
      col_type = static_cast<uint8_t>(input[0]);
      input.remove_prefix(1);
    }
    column_types.emplace_back(col_type);

    uint32_t value_size = 0;
    if (!GetVarint32(&input, &value_size)) {
      return Status::Corruption("Error decoding wide column value size");
    }

    column_value_sizes.emplace_back(value_size);
  }

  const Slice data(input);
  size_t pos = 0;

  for (uint32_t i = 0; i < num_columns; ++i) {
    const uint32_t value_size = column_value_sizes[i];

    if (pos + value_size > data.size()) {
      return Status::Corruption("Error decoding wide column value payload");
    }

    if (column_types[i] == kColumnTypeBlobIndex) {
      // This is a blob reference - decode the blob index
      BlobIndex blob_idx;
      Slice blob_slice(data.data() + pos, value_size);
      Status s = blob_idx.DecodeFrom(blob_slice);
      if (!s.ok()) {
        return Status::Corruption("Error decoding blob index in wide column");
      }
      blob_columns->emplace_back(i, blob_idx);
      // Set the column value to the raw serialized blob index for reference
      (*columns)[i].value() = Slice(data.data() + pos, value_size);
    } else {
      // This is an inline value
      (*columns)[i].value() = Slice(data.data() + pos, value_size);
    }

    pos += value_size;
  }

  return Status::OK();
}

bool WideColumnSerialization::HasBlobColumns(const Slice& input) {
  Slice copy = input;

  uint32_t version = 0;
  if (!GetVarint32(&copy, &version)) {
    return false;
  }

  // Version 1 never has blob columns
  if (version < kVersion2) {
    return false;
  }

  uint32_t num_columns = 0;
  if (!GetVarint32(&copy, &num_columns)) {
    return false;
  }

  // Check each column's type
  for (uint32_t i = 0; i < num_columns; ++i) {
    Slice name;
    if (!GetLengthPrefixedSlice(&copy, &name)) {
      return false;
    }

    if (copy.size() < 1) {
      return false;
    }
    uint8_t col_type = static_cast<uint8_t>(copy[0]);
    copy.remove_prefix(1);

    if (col_type == kColumnTypeBlobIndex) {
      return true;
    }

    uint32_t value_size = 0;
    if (!GetVarint32(&copy, &value_size)) {
      return false;
    }
  }

  return false;
}

uint32_t WideColumnSerialization::GetVersion(const Slice& input) {
  Slice copy = input;

  uint32_t version = 0;
  if (!GetVarint32(&copy, &version)) {
    return 0;
  }

  return version;
}

Status WideColumnSerialization::GetValueOfDefaultColumn(Slice& input,
                                                        Slice& value) {
  WideColumns columns;

  const Status s = Deserialize(input, columns);
  if (!s.ok()) {
    return s;
  }

  if (!WideColumnsHelper::HasDefaultColumn(columns)) {
    value.clear();
    return Status::OK();
  }

  value = WideColumnsHelper::GetDefaultColumn(columns);

  return Status::OK();
}

Status WideColumnSerialization::SerializeResolvedEntity(
    const std::vector<WideColumn>& columns,
    const std::vector<std::pair<size_t, BlobIndex>>& blob_columns,
    const std::vector<std::string>& resolved_blob_values, std::string* output) {
  assert(output != nullptr);
  assert(blob_columns.size() == resolved_blob_values.size());

  // Build a map from column index to resolved blob value index
  std::unordered_map<size_t, size_t> blob_col_to_resolved_idx;
  for (size_t i = 0; i < blob_columns.size(); ++i) {
    blob_col_to_resolved_idx[blob_columns[i].first] = i;
  }

  // Build result columns with resolved blob values
  WideColumns result_columns;
  result_columns.reserve(columns.size());

  for (size_t i = 0; i < columns.size(); ++i) {
    auto it = blob_col_to_resolved_idx.find(i);
    if (it != blob_col_to_resolved_idx.end()) {
      // This is a blob column - use the resolved value
      result_columns.emplace_back(columns[i].name(),
                                  Slice(resolved_blob_values[it->second]));
    } else {
      // This is an inline column - use the original value
      result_columns.emplace_back(columns[i].name(), columns[i].value());
    }
  }

  // Serialize using V1 format (all values inline)
  return Serialize(result_columns, *output);
}

}  // namespace ROCKSDB_NAMESPACE

//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/wide/wide_column_serialization.h"

#include <cassert>
#include <limits>
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

  // Create a map from column index to blob index
  std::vector<const BlobIndex*> blob_index_map(num_columns, nullptr);
  for (const auto& blob_col : blob_columns) {
    if (blob_col.first >= num_columns) {
      return Status::InvalidArgument("Blob column index out of range");
    }
    blob_index_map[blob_col.first] = &blob_col.second;
  }

  // Validate column ordering and prepare serialized blob indices
  const std::string* prev_name = nullptr;
  std::vector<std::string> serialized_blob_indices(num_columns);
  std::vector<uint32_t> name_sizes(num_columns);
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

    name_sizes[i] = static_cast<uint32_t>(name.size());

    if (blob_index_map[i] != nullptr) {
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
      if (value.size() >
          static_cast<size_t>(std::numeric_limits<uint32_t>::max())) {
        return Status::InvalidArgument("Wide column value too long");
      }
      value_sizes[i] = static_cast<uint32_t>(value.size());
    }

    prev_name = &name;
  }

  // Compute skip info: byte sizes of NAME SIZES section and NAMES section
  uint32_t name_sizes_bytes = 0;
  uint32_t names_bytes = 0;
  for (size_t i = 0; i < num_columns; ++i) {
    name_sizes_bytes += VarintLength(name_sizes[i]);
    names_bytes += name_sizes[i];
  }

  // Section 1: HEADER
  PutVarint32(output, kVersion2);
  PutVarint32(output, static_cast<uint32_t>(num_columns));

  // Section 2: COLUMN TYPES (N bytes)
  for (size_t i = 0; i < num_columns; ++i) {
    if (blob_index_map[i] != nullptr) {
      output->push_back(static_cast<char>(kColumnTypeBlobIndex));
    } else {
      output->push_back(static_cast<char>(kColumnTypeInline));
    }
  }

  // Section 3: SKIP INFO (2 varints)
  PutVarint32(output, name_sizes_bytes);
  PutVarint32(output, names_bytes);

  // Section 4: NAME SIZES (N varints)
  for (size_t i = 0; i < num_columns; ++i) {
    PutVarint32(output, name_sizes[i]);
  }

  // Section 5: VALUE SIZES (N varints)
  for (size_t i = 0; i < num_columns; ++i) {
    PutVarint32(output, value_sizes[i]);
  }

  // Section 6: COLUMN NAMES (concatenated)
  for (size_t i = 0; i < num_columns; ++i) {
    output->append(columns[i].first);
  }

  // Section 7: COLUMN VALUES (concatenated)
  for (size_t i = 0; i < num_columns; ++i) {
    if (blob_index_map[i] != nullptr) {
      output->append(serialized_blob_indices[i]);
    } else {
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

  if (version >= kVersion2) {
    // V2 layout: TYPES → SKIP_INFO → NAME_SIZES → VALUE_SIZES → NAMES →
    // VALUES

    // Section 2: COLUMN TYPES (N bytes)
    if (input.size() < num_columns) {
      return Status::Corruption("Error decoding wide column types");
    }
    for (uint32_t i = 0; i < num_columns; ++i) {
      if (static_cast<uint8_t>(input[i]) == kColumnTypeBlobIndex) {
        return Status::NotSupported(
            "Wide column contains blob references. Use DeserializeColumns.");
      }
    }
    input.remove_prefix(num_columns);

    // Section 3: SKIP INFO (2 varints)
    uint32_t name_sizes_bytes = 0;
    uint32_t names_bytes = 0;
    if (!GetVarint32(&input, &name_sizes_bytes)) {
      return Status::Corruption("Error decoding wide column name sizes bytes");
    }
    if (!GetVarint32(&input, &names_bytes)) {
      return Status::Corruption("Error decoding wide column names bytes");
    }

    // Section 4: NAME SIZES (N varints)
    if (input.size() < name_sizes_bytes) {
      return Status::Corruption("Error decoding wide column name sizes");
    }
    Slice name_sizes_section(input.data(), name_sizes_bytes);
    input.remove_prefix(name_sizes_bytes);

    autovector<uint32_t, 16> column_name_sizes;
    column_name_sizes.reserve(num_columns);
    for (uint32_t i = 0; i < num_columns; ++i) {
      uint32_t name_size = 0;
      if (!GetVarint32(&name_sizes_section, &name_size)) {
        return Status::Corruption("Error decoding wide column name size");
      }
      column_name_sizes.emplace_back(name_size);
    }

    // Section 5: VALUE SIZES (N varints)
    autovector<uint32_t, 16> column_value_sizes;
    column_value_sizes.reserve(num_columns);
    for (uint32_t i = 0; i < num_columns; ++i) {
      uint32_t value_size = 0;
      if (!GetVarint32(&input, &value_size)) {
        return Status::Corruption("Error decoding wide column value size");
      }
      column_value_sizes.emplace_back(value_size);
    }

    // Section 6: COLUMN NAMES (concatenated)
    if (input.size() < names_bytes) {
      return Status::Corruption("Error decoding wide column names");
    }
    const char* names_data = input.data();
    columns.reserve(num_columns);
    size_t name_pos = 0;
    for (uint32_t i = 0; i < num_columns; ++i) {
      const uint32_t ns = column_name_sizes[i];
      if (name_pos + ns > names_bytes) {
        return Status::Corruption("Error decoding wide column name");
      }
      Slice name(names_data + name_pos, ns);

      if (!columns.empty() && columns.back().name().compare(name) >= 0) {
        return Status::Corruption("Wide columns out of order");
      }

      columns.emplace_back(name, Slice());
      name_pos += ns;
    }
    input.remove_prefix(names_bytes);

    // Section 7: COLUMN VALUES (concatenated)
    const char* values_data = input.data();
    size_t value_pos = 0;
    for (uint32_t i = 0; i < num_columns; ++i) {
      const uint32_t vs = column_value_sizes[i];
      if (value_pos + vs > input.size()) {
        return Status::Corruption("Error decoding wide column value payload");
      }
      columns[i].value() = Slice(values_data + value_pos, vs);
      value_pos += vs;
    }
  } else {
    // V1 layout: interleaved name/value_size pairs followed by values
    columns.reserve(num_columns);

    autovector<uint32_t, 16> column_value_sizes;
    column_value_sizes.reserve(num_columns);

    for (uint32_t i = 0; i < num_columns; ++i) {
      Slice name;
      if (!GetLengthPrefixedSlice(&input, &name)) {
        return Status::Corruption("Error decoding wide column name");
      }

      if (!columns.empty() && columns.back().name().compare(name) >= 0) {
        return Status::Corruption("Wide columns out of order");
      }

      columns.emplace_back(name, Slice());

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

  if (version >= kVersion2) {
    // V2 layout: TYPES → SKIP_INFO → NAME_SIZES → VALUE_SIZES → NAMES →
    // VALUES

    // Section 2: COLUMN TYPES (N bytes)
    if (input.size() < num_columns) {
      return Status::Corruption("Error decoding wide column types");
    }
    autovector<uint8_t, 16> column_types;
    column_types.reserve(num_columns);
    for (uint32_t i = 0; i < num_columns; ++i) {
      column_types.emplace_back(static_cast<uint8_t>(input[i]));
    }
    input.remove_prefix(num_columns);

    // Section 3: SKIP INFO (2 varints)
    uint32_t name_sizes_bytes = 0;
    uint32_t names_bytes = 0;
    if (!GetVarint32(&input, &name_sizes_bytes)) {
      return Status::Corruption("Error decoding wide column name sizes bytes");
    }
    if (!GetVarint32(&input, &names_bytes)) {
      return Status::Corruption("Error decoding wide column names bytes");
    }

    // Section 4: NAME SIZES (N varints)
    if (input.size() < name_sizes_bytes) {
      return Status::Corruption("Error decoding wide column name sizes");
    }
    Slice name_sizes_section(input.data(), name_sizes_bytes);
    input.remove_prefix(name_sizes_bytes);

    autovector<uint32_t, 16> column_name_sizes;
    column_name_sizes.reserve(num_columns);
    for (uint32_t i = 0; i < num_columns; ++i) {
      uint32_t name_size = 0;
      if (!GetVarint32(&name_sizes_section, &name_size)) {
        return Status::Corruption("Error decoding wide column name size");
      }
      column_name_sizes.emplace_back(name_size);
    }

    // Section 5: VALUE SIZES (N varints)
    autovector<uint32_t, 16> column_value_sizes;
    column_value_sizes.reserve(num_columns);
    for (uint32_t i = 0; i < num_columns; ++i) {
      uint32_t value_size = 0;
      if (!GetVarint32(&input, &value_size)) {
        return Status::Corruption("Error decoding wide column value size");
      }
      column_value_sizes.emplace_back(value_size);
    }

    // Section 6: COLUMN NAMES (concatenated)
    if (input.size() < names_bytes) {
      return Status::Corruption("Error decoding wide column names");
    }
    const char* names_data = input.data();
    columns->reserve(num_columns);
    size_t name_pos = 0;
    for (uint32_t i = 0; i < num_columns; ++i) {
      const uint32_t ns = column_name_sizes[i];
      if (name_pos + ns > names_bytes) {
        return Status::Corruption("Error decoding wide column name");
      }
      Slice name(names_data + name_pos, ns);

      if (!columns->empty() && columns->back().name().compare(name) >= 0) {
        return Status::Corruption("Wide columns out of order");
      }

      columns->emplace_back(name, Slice());
      name_pos += ns;
    }
    input.remove_prefix(names_bytes);

    // Section 7: COLUMN VALUES (concatenated)
    const char* values_data = input.data();
    size_t value_pos = 0;
    for (uint32_t i = 0; i < num_columns; ++i) {
      const uint32_t vs = column_value_sizes[i];
      if (value_pos + vs > input.size()) {
        return Status::Corruption("Error decoding wide column value payload");
      }

      if (column_types[i] == kColumnTypeBlobIndex) {
        BlobIndex blob_idx;
        Slice blob_slice(values_data + value_pos, vs);
        Status s = blob_idx.DecodeFrom(blob_slice);
        if (!s.ok()) {
          return Status::Corruption("Error decoding blob index in wide column");
        }
        blob_columns->emplace_back(i, blob_idx);
        (*columns)[i].value() = Slice(values_data + value_pos, vs);
      } else {
        (*columns)[i].value() = Slice(values_data + value_pos, vs);
      }

      value_pos += vs;
    }
  } else {
    // V1 layout: interleaved name/value_size pairs followed by values
    columns->reserve(num_columns);

    autovector<uint32_t, 16> column_value_sizes;
    column_value_sizes.reserve(num_columns);

    for (uint32_t i = 0; i < num_columns; ++i) {
      Slice name;
      if (!GetLengthPrefixedSlice(&input, &name)) {
        return Status::Corruption("Error decoding wide column name");
      }

      if (!columns->empty() && columns->back().name().compare(name) >= 0) {
        return Status::Corruption("Wide columns out of order");
      }

      columns->emplace_back(name, Slice());

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

      (*columns)[i].value() = Slice(data.data() + pos, value_size);

      pos += value_size;
    }
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

  if (!num_columns) {
    return false;
  }

  // V2: COLUMN TYPES section is N contiguous bytes right after the header.
  // Scan the contiguous type bytes for any blob column.
  if (copy.size() < num_columns) {
    return false;
  }
  for (uint32_t i = 0; i < num_columns; ++i) {
    if (static_cast<uint8_t>(copy[i]) == kColumnTypeBlobIndex) {
      return true;
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
  Slice copy = input;

  uint32_t version = 0;
  if (!GetVarint32(&copy, &version)) {
    return Status::Corruption("Error decoding wide column version");
  }

  if (version > kVersion2) {
    return Status::NotSupported("Unsupported wide column version");
  }

  uint32_t num_columns = 0;
  if (!GetVarint32(&copy, &num_columns)) {
    return Status::Corruption("Error decoding number of wide columns");
  }

  if (!num_columns) {
    value.clear();
    return Status::OK();
  }

  if (version >= kVersion2) {
    // V2 fast path: skip types, use skip info to jump to value sizes,
    // then skip names to reach values.

    // Skip COLUMN TYPES (N bytes)
    if (copy.size() < num_columns) {
      return Status::Corruption("Error decoding wide column types");
    }
    // Check if default column (index 0) is a blob reference
    if (static_cast<uint8_t>(copy[0]) == kColumnTypeBlobIndex) {
      return Status::NotSupported(
          "Wide column contains blob references. Use DeserializeColumns.");
    }
    copy.remove_prefix(num_columns);

    // Read SKIP INFO
    uint32_t name_sizes_bytes = 0;
    uint32_t names_bytes = 0;
    if (!GetVarint32(&copy, &name_sizes_bytes)) {
      return Status::Corruption("Error decoding wide column name sizes bytes");
    }
    if (!GetVarint32(&copy, &names_bytes)) {
      return Status::Corruption("Error decoding wide column names bytes");
    }

    // Skip NAME SIZES section
    if (copy.size() < name_sizes_bytes) {
      return Status::Corruption("Error decoding wide column name sizes");
    }
    // Read the first name size to check if it's the default column (empty name)
    Slice name_sizes_section(copy.data(), name_sizes_bytes);
    uint32_t first_name_size = 0;
    if (!GetVarint32(&name_sizes_section, &first_name_size)) {
      return Status::Corruption("Error decoding wide column name size");
    }
    copy.remove_prefix(name_sizes_bytes);

    // Read first value size from VALUE SIZES section
    uint32_t first_value_size = 0;
    if (!GetVarint32(&copy, &first_value_size)) {
      return Status::Corruption("Error decoding wide column value size");
    }

    // Skip remaining value sizes (we only need the first one)
    for (uint32_t i = 1; i < num_columns; ++i) {
      uint32_t vs = 0;
      if (!GetVarint32(&copy, &vs)) {
        return Status::Corruption("Error decoding wide column value size");
      }
    }

    // Skip NAMES section
    if (copy.size() < names_bytes) {
      return Status::Corruption("Error decoding wide column names");
    }

    // Check if the first column is the default column (empty name)
    if (first_name_size != 0) {
      value.clear();
      return Status::OK();
    }

    copy.remove_prefix(names_bytes);

    // Read the first value from VALUES section
    if (copy.size() < first_value_size) {
      return Status::Corruption("Error decoding wide column value payload");
    }
    value = Slice(copy.data(), first_value_size);
    return Status::OK();
  }

  // V1 fallback: full deserialization
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

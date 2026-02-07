//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/wide/wide_column_serialization.h"

#include <cassert>
#include <cstring>
#include <limits>

#include "db/blob/blob_fetcher.h"
#include "db/blob/blob_index.h"
#include "db/blob/prefetch_buffer_collection.h"
#include "db/wide/wide_columns_helper.h"
#include "rocksdb/slice.h"
#include "util/autovector.h"
#include "util/coding.h"

namespace ROCKSDB_NAMESPACE {

Status WideColumnSerialization::BuildBlobIndexMap(
    size_t num_columns,
    const std::vector<std::pair<size_t, BlobIndex>>& blob_columns,
    std::vector<const BlobIndex*>* blob_index_map) {
  assert(blob_index_map != nullptr);

  if (num_columns > static_cast<size_t>(std::numeric_limits<uint32_t>::max())) {
    return Status::InvalidArgument("Too many wide columns");
  }

  blob_index_map->assign(num_columns, nullptr);
  for (const auto& blob_col : blob_columns) {
    if (blob_col.first >= num_columns) {
      return Status::InvalidArgument("Blob column index out of range");
    }
    (*blob_index_map)[blob_col.first] = &blob_col.second;
  }

  return Status::OK();
}

bool WideColumnSerialization::ContainsBlobType(const char* type_bytes,
                                               uint32_t num_columns) {
  for (uint32_t i = 0; i < num_columns; ++i) {
    if (static_cast<uint8_t>(type_bytes[i]) == kColumnTypeBlobIndex) {
      return true;
    }
  }
  return false;
}

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

template <typename GetNameData, typename GetValueData>
void WideColumnSerialization::WriteV2Sections(
    std::string* output, size_t num_columns, const std::string& column_types,
    const std::vector<uint32_t>& name_sizes,
    const std::vector<uint32_t>& value_sizes, uint32_t name_sizes_bytes,
    uint32_t names_bytes, uint32_t total_value_sizes_bytes,
    uint32_t total_values_bytes, GetNameData get_name_data,
    GetValueData get_value_data) {
  // Pre-allocate output string
  const size_t total_size =
      VarintLength(kVersion2) +
      VarintLength(static_cast<uint32_t>(num_columns)) +
      num_columns +  // column types
      VarintLength(name_sizes_bytes) + VarintLength(total_value_sizes_bytes) +
      VarintLength(names_bytes) + name_sizes_bytes + total_value_sizes_bytes +
      names_bytes + total_values_bytes;

  const size_t base_offset = output->size();
  output->reserve(base_offset + total_size);

  // Sections 1-3: header, column types, skip info
  PutVarint32(output, kVersion2);
  PutVarint32(output, static_cast<uint32_t>(num_columns));
  output->append(column_types);
  PutVarint32(output, name_sizes_bytes);
  PutVarint32(output, total_value_sizes_bytes);
  PutVarint32(output, names_bytes);

  // Sections 4-7: resize to final size, then write all 4 sections in a
  // single loop using independent pointers. Each section's start offset is
  // known from the sizes computed in the first pass.
  const size_t sec4_offset = output->size();
  output->resize(base_offset + total_size);

  char* s4 = &(*output)[sec4_offset];       // section 4: name sizes
  char* s5 = s4 + name_sizes_bytes;         // section 5: value sizes
  char* s6 = s5 + total_value_sizes_bytes;  // section 6: names
  char* s7 = s6 + names_bytes;              // section 7: values

  for (size_t i = 0; i < num_columns; ++i) {
    s4 = EncodeVarint32(s4, name_sizes[i]);
    s5 = EncodeVarint32(s5, value_sizes[i]);

    memcpy(s6, get_name_data(i), name_sizes[i]);
    s6 += name_sizes[i];

    memcpy(s7, get_value_data(i), value_sizes[i]);
    s7 += value_sizes[i];
  }
}

Status WideColumnSerialization::SerializeWithBlobIndices(
    const std::vector<std::pair<std::string, std::string>>& columns,
    const std::vector<std::pair<size_t, BlobIndex>>& blob_columns,
    std::string* output) {
  assert(output != nullptr);

  const size_t num_columns = columns.size();

  std::vector<const BlobIndex*> blob_index_map;
  const Status s =
      BuildBlobIndexMap(num_columns, blob_columns, &blob_index_map);
  if (!s.ok()) {
    return s;
  }

  // Single pass: validate column ordering, compute sizes, serialize blob
  // indices, and emit column types.
  std::vector<std::string> serialized_blob_indices(num_columns);
  std::vector<uint32_t> name_sizes(num_columns);
  std::vector<uint32_t> value_sizes(num_columns);
  std::string column_types;
  column_types.reserve(num_columns);

  const std::string* prev_name = nullptr;
  uint32_t name_sizes_bytes = 0;
  uint32_t names_bytes = 0;
  uint32_t total_value_sizes_bytes = 0;
  uint32_t total_values_bytes = 0;

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
    name_sizes_bytes += VarintLength(name_sizes[i]);
    names_bytes += name_sizes[i];

    if (blob_index_map[i] != nullptr) {
      const BlobIndex* blob_idx = blob_index_map[i];
      blob_idx->EncodeTo(&serialized_blob_indices[i]);
      value_sizes[i] = static_cast<uint32_t>(serialized_blob_indices[i].size());
      column_types.push_back(static_cast<char>(kColumnTypeBlobIndex));
    } else {
      if (value.size() >
          static_cast<size_t>(std::numeric_limits<uint32_t>::max())) {
        return Status::InvalidArgument("Wide column value too long");
      }
      value_sizes[i] = static_cast<uint32_t>(value.size());
      column_types.push_back(static_cast<char>(kColumnTypeInline));
    }

    total_value_sizes_bytes += VarintLength(value_sizes[i]);
    total_values_bytes += value_sizes[i];

    prev_name = &name;
  }

  WriteV2Sections(
      output, num_columns, column_types, name_sizes, value_sizes,
      name_sizes_bytes, names_bytes, total_value_sizes_bytes,
      total_values_bytes, [&](size_t i) { return columns[i].first.data(); },
      [&](size_t i) {
        return blob_index_map[i] != nullptr ? serialized_blob_indices[i].data()
                                            : columns[i].second.data();
      });

  return Status::OK();
}

Status WideColumnSerialization::SerializeWithBlobIndices(
    const WideColumns& columns,
    const std::vector<std::pair<size_t, BlobIndex>>& blob_columns,
    std::string* output) {
  assert(output != nullptr);

  const size_t num_columns = columns.size();

  std::vector<const BlobIndex*> blob_index_map;
  const Status s =
      BuildBlobIndexMap(num_columns, blob_columns, &blob_index_map);
  if (!s.ok()) {
    return s;
  }

  // Single pass: validate column ordering, compute sizes, serialize blob
  // indices, and emit column types.
  const Slice* prev_name = nullptr;
  std::vector<std::string> serialized_blob_indices(num_columns);
  std::vector<uint32_t> name_sizes(num_columns);
  std::vector<uint32_t> value_sizes(num_columns);
  std::string column_types;
  column_types.reserve(num_columns);

  uint32_t name_sizes_bytes = 0;
  uint32_t names_bytes = 0;
  uint32_t total_value_sizes_bytes = 0;
  uint32_t total_values_bytes = 0;

  for (size_t i = 0; i < num_columns; ++i) {
    const Slice& name = columns[i].name();
    const Slice& value = columns[i].value();

    if (name.size() >
        static_cast<size_t>(std::numeric_limits<uint32_t>::max())) {
      return Status::InvalidArgument("Wide column name too long");
    }

    if (prev_name && prev_name->compare(name) >= 0) {
      return Status::Corruption("Wide columns out of order");
    }

    name_sizes[i] = static_cast<uint32_t>(name.size());
    name_sizes_bytes += VarintLength(name_sizes[i]);
    names_bytes += name_sizes[i];

    if (blob_index_map[i] != nullptr) {
      const BlobIndex* blob_idx = blob_index_map[i];
      blob_idx->EncodeTo(&serialized_blob_indices[i]);
      value_sizes[i] = static_cast<uint32_t>(serialized_blob_indices[i].size());
      column_types.push_back(static_cast<char>(kColumnTypeBlobIndex));
    } else {
      if (value.size() >
          static_cast<size_t>(std::numeric_limits<uint32_t>::max())) {
        return Status::InvalidArgument("Wide column value too long");
      }
      value_sizes[i] = static_cast<uint32_t>(value.size());
      column_types.push_back(static_cast<char>(kColumnTypeInline));
    }

    total_value_sizes_bytes += VarintLength(value_sizes[i]);
    total_values_bytes += value_sizes[i];

    prev_name = &name;
  }

  WriteV2Sections(
      output, num_columns, column_types, name_sizes, value_sizes,
      name_sizes_bytes, names_bytes, total_value_sizes_bytes,
      total_values_bytes, [&](size_t i) { return columns[i].name().data(); },
      [&](size_t i) {
        return blob_index_map[i] != nullptr ? serialized_blob_indices[i].data()
                                            : columns[i].value().data();
      });

  return Status::OK();
}

Status WideColumnSerialization::DeserializeV1(
    Slice& input, uint32_t num_columns, std::vector<WideColumn>* columns) {
  assert(columns != nullptr);

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

  return Status::OK();
}

Status WideColumnSerialization::DeserializeV2(
    Slice& input, uint32_t num_columns, std::vector<WideColumn>* columns,
    std::vector<uint8_t>* column_types) {
  assert(columns != nullptr);
  assert(column_types != nullptr);

  // Section 2: COLUMN TYPES (N bytes)
  if (input.size() < num_columns) {
    return Status::Corruption("Error decoding wide column types");
  }
  column_types->assign(input.data(), input.data() + num_columns);
  input.remove_prefix(num_columns);

  // Section 3: SKIP INFO (3 varints)
  uint32_t name_sizes_bytes = 0;
  uint32_t value_sizes_bytes = 0;
  uint32_t names_bytes = 0;
  if (!GetVarint32(&input, &name_sizes_bytes)) {
    return Status::Corruption("Error decoding wide column name sizes bytes");
  }
  if (!GetVarint32(&input, &value_sizes_bytes)) {
    return Status::Corruption("Error decoding wide column value sizes bytes");
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
  if (input.size() < value_sizes_bytes) {
    return Status::Corruption("Error decoding wide column value sizes");
  }
  Slice value_sizes_section(input.data(), value_sizes_bytes);
  input.remove_prefix(value_sizes_bytes);

  autovector<uint32_t, 16> column_value_sizes;
  column_value_sizes.reserve(num_columns);
  for (uint32_t i = 0; i < num_columns; ++i) {
    uint32_t value_size = 0;
    if (!GetVarint32(&value_sizes_section, &value_size)) {
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
    (*columns)[i].value() = Slice(values_data + value_pos, vs);
    value_pos += vs;
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

  if (version < kVersion2) {
    return DeserializeV1(input, num_columns, &columns);
  } else {
    // V2 layout: use shared helper, then reject blob columns
    std::vector<uint8_t> column_types;
    std::vector<WideColumn> temp_columns;

    const Status s =
        DeserializeV2(input, num_columns, &temp_columns, &column_types);
    if (!s.ok()) {
      return s;
    }

    if (ContainsBlobType(reinterpret_cast<const char*>(column_types.data()),
                         num_columns)) {
      return Status::NotSupported(
          "Wide column contains blob references. Use DeserializeColumns.");
    }

    columns.reserve(temp_columns.size());
    for (auto& col : temp_columns) {
      columns.emplace_back(std::move(col));
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
    // V2 layout: use shared helper, then extract blob columns
    std::vector<uint8_t> column_types;

    const Status s = DeserializeV2(input, num_columns, columns, &column_types);
    if (!s.ok()) {
      return s;
    }

    // Decode blob indices from value data
    for (uint32_t i = 0; i < num_columns; ++i) {
      if (column_types[i] == kColumnTypeBlobIndex) {
        BlobIndex blob_idx;
        Slice blob_slice = (*columns)[i].value();
        Status bs = blob_idx.DecodeFrom(blob_slice);
        if (!bs.ok()) {
          return Status::Corruption("Error decoding blob index in wide column");
        }
        blob_columns->emplace_back(i, blob_idx);
      }
    }
  } else {
    return DeserializeV1(input, num_columns, columns);
  }

  return Status::OK();
}

Status WideColumnSerialization::HasBlobColumns(const Slice& input,
                                                bool* has_blob_columns) {
  assert(has_blob_columns != nullptr);
  *has_blob_columns = false;

  Slice input_ref = input;

  uint32_t version = 0;
  if (!GetVarint32(&input_ref, &version)) {
    return Status::Corruption("Error decoding wide column version");
  }

  // Version 1 never has blob columns
  if (version < kVersion2) {
    return Status::OK();
  }

  uint32_t num_columns = 0;
  if (!GetVarint32(&input_ref, &num_columns)) {
    return Status::Corruption("Error decoding number of wide columns");
  }

  if (!num_columns) {
    return Status::OK();
  }

  // V2: COLUMN TYPES section is N contiguous bytes right after the header.
  if (input_ref.size() < num_columns) {
    return Status::Corruption("Error decoding wide column types");
  }
  *has_blob_columns = ContainsBlobType(input_ref.data(), num_columns);

  return Status::OK();
}

Status WideColumnSerialization::GetVersion(const Slice& input,
                                            uint32_t* version) {
  assert(version != nullptr);

  Slice input_ref = input;

  *version = 0;
  if (!GetVarint32(&input_ref, version)) {
    return Status::Corruption("Error decoding wide column version");
  }

  return Status::OK();
}

Status WideColumnSerialization::GetValueOfDefaultColumn(Slice& input,
                                                        Slice& value) {
  Slice input_ref = input;

  uint32_t version = 0;
  if (!GetVarint32(&input_ref, &version)) {
    return Status::Corruption("Error decoding wide column version");
  }

  if (version > kVersion2) {
    return Status::NotSupported("Unsupported wide column version");
  }

  uint32_t num_columns = 0;
  if (!GetVarint32(&input_ref, &num_columns)) {
    return Status::Corruption("Error decoding number of wide columns");
  }

  if (!num_columns) {
    value.clear();
    return Status::OK();
  }

  if (version >= kVersion2) {
    // V2 fast path: use skip info to jump directly to values without
    // scanning through variable-length sections.

    // Skip COLUMN TYPES (N bytes)
    if (input_ref.size() < num_columns) {
      return Status::Corruption("Error decoding wide column types");
    }
    // Check if default column (index 0) is a blob reference
    if (static_cast<uint8_t>(input_ref[0]) == kColumnTypeBlobIndex) {
      return Status::NotSupported(
          "Wide column contains blob references. Use DeserializeColumns.");
    }
    input_ref.remove_prefix(num_columns);

    // Read SKIP INFO (3 varints)
    uint32_t name_sizes_bytes = 0;
    uint32_t value_sizes_bytes = 0;
    uint32_t names_bytes = 0;
    if (!GetVarint32(&input_ref, &name_sizes_bytes)) {
      return Status::Corruption("Error decoding wide column name sizes bytes");
    }
    if (!GetVarint32(&input_ref, &value_sizes_bytes)) {
      return Status::Corruption("Error decoding wide column value sizes bytes");
    }
    if (!GetVarint32(&input_ref, &names_bytes)) {
      return Status::Corruption("Error decoding wide column names bytes");
    }

    // Peek first name size from NAME SIZES section
    if (input_ref.size() < name_sizes_bytes) {
      return Status::Corruption("Error decoding wide column name sizes");
    }
    Slice name_sizes_section(input_ref.data(), name_sizes_bytes);
    uint32_t first_name_size = 0;
    if (!GetVarint32(&name_sizes_section, &first_name_size)) {
      return Status::Corruption("Error decoding wide column name size");
    }
    input_ref.remove_prefix(name_sizes_bytes);

    // Peek first value size from VALUE SIZES section
    if (input_ref.size() < value_sizes_bytes) {
      return Status::Corruption("Error decoding wide column value sizes");
    }
    Slice value_sizes_section(input_ref.data(), value_sizes_bytes);
    uint32_t first_value_size = 0;
    if (!GetVarint32(&value_sizes_section, &first_value_size)) {
      return Status::Corruption("Error decoding wide column value size");
    }
    // Skip entire VALUE SIZES section using value_sizes_bytes
    input_ref.remove_prefix(value_sizes_bytes);

    // Check if the first column is the default column (empty name)
    if (first_name_size != 0) {
      value.clear();
      return Status::OK();
    }

    // Skip NAMES section
    if (input_ref.size() < names_bytes) {
      return Status::Corruption("Error decoding wide column names");
    }
    input_ref.remove_prefix(names_bytes);

    // Read the first value from VALUES section
    if (input_ref.size() < first_value_size) {
      return Status::Corruption("Error decoding wide column value payload");
    }
    value = Slice(input_ref.data(), first_value_size);
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

Status WideColumnSerialization::ResolveEntityBlobColumns(
    const Slice& entity_value, const Slice& user_key,
    const BlobFetcher* blob_fetcher, PrefetchBufferCollection* prefetch_buffers,
    std::string* resolved_entity, bool* resolved, uint64_t* total_bytes_read,
    uint64_t* num_blobs_resolved) {
  assert(blob_fetcher);
  assert(resolved_entity);
  assert(resolved);

  *resolved = false;

  std::vector<WideColumn> columns;
  std::vector<std::pair<size_t, BlobIndex>> blob_columns;

  Slice input_copy = entity_value;
  const Status s = DeserializeColumns(input_copy, &columns, &blob_columns);
  if (!s.ok()) {
    return s;
  }

  if (blob_columns.empty()) {
    return Status::OK();
  }

  *resolved = true;

  // Fetch each blob value
  std::vector<std::string> resolved_blob_values;
  resolved_blob_values.reserve(blob_columns.size());

  for (const auto& blob_col : blob_columns) {
    const BlobIndex& blob_idx = blob_col.second;

    if (blob_idx.IsInlined()) {
      resolved_blob_values.emplace_back(blob_idx.value().data(),
                                        blob_idx.value().size());
      continue;
    }

    FilePrefetchBuffer* prefetch_buffer =
        prefetch_buffers ? prefetch_buffers->GetOrCreatePrefetchBuffer(
                               blob_idx.file_number())
                         : nullptr;

    uint64_t bytes_read = 0;

    PinnableSlice blob_value;
    const Status fetch_s = blob_fetcher->FetchBlob(
        user_key, blob_idx, prefetch_buffer, &blob_value, &bytes_read);
    if (!fetch_s.ok()) {
      return fetch_s;
    }

    resolved_blob_values.emplace_back(blob_value.data(), blob_value.size());

    if (total_bytes_read) {
      *total_bytes_read += bytes_read;
    }
  }

  if (num_blobs_resolved) {
    *num_blobs_resolved += blob_columns.size();
  }

  return SerializeResolvedEntity(columns, blob_columns, resolved_blob_values,
                                 resolved_entity);
}

Status WideColumnSerialization::GetDefaultColumnFromEntityResolvingBlobs(
    const Slice& entity_value, const Slice& user_key,
    const BlobFetcher* blob_fetcher, PinnableSlice* result, bool* resolved) {
  assert(blob_fetcher);
  assert(result);
  assert(resolved);

  *resolved = false;

  std::vector<WideColumn> columns;
  std::vector<std::pair<size_t, BlobIndex>> blob_columns;

  Slice input_copy = entity_value;
  const Status s = DeserializeColumns(input_copy, &columns, &blob_columns);
  if (!s.ok()) {
    return s;
  }

  // The default column (empty name) is always at index 0 when present
  // (columns are sorted by name).
  if (columns.empty() || columns[0].name() != kDefaultWideColumnName) {
    result->PinSelf(Slice());
    return Status::OK();
  }

  // Check if the default column (index 0) is a blob reference
  for (const auto& blob_col : blob_columns) {
    if (blob_col.first == 0) {
      const BlobIndex& blob_idx = blob_col.second;

      *resolved = true;

      if (blob_idx.IsInlined()) {
        result->PinSelf(blob_idx.value());
        return Status::OK();
      }

      return blob_fetcher->FetchBlob(user_key, blob_idx,
                                     nullptr /* prefetch_buffer */, result,
                                     nullptr /* bytes_read */);
    }
  }

  // Default column is inline
  result->PinSelf(columns[0].value());
  return Status::OK();
}

Status WideColumnSerialization::SerializeResolvedEntity(
    const std::vector<WideColumn>& columns,
    const std::vector<std::pair<size_t, BlobIndex>>& blob_columns,
    const std::vector<std::string>& resolved_blob_values, std::string* output) {
  assert(output != nullptr);
  assert(blob_columns.size() == resolved_blob_values.size());

  // blob_columns is sorted by column index and typically small, so use a
  // linear scan with a cursor instead of an unordered_map.
  size_t blob_cursor = 0;

  // Build result columns with resolved blob values
  WideColumns result_columns;
  result_columns.reserve(columns.size());

  for (size_t i = 0; i < columns.size(); ++i) {
    if (blob_cursor < blob_columns.size() &&
        blob_columns[blob_cursor].first == i) {
      // This is a blob column - use the resolved value
      result_columns.emplace_back(columns[i].name(),
                                  Slice(resolved_blob_values[blob_cursor]));
      ++blob_cursor;
    } else {
      // This is an inline column - use the original value
      result_columns.emplace_back(columns[i].name(), columns[i].value());
    }
  }

  // Serialize using V1 format (all values inline)
  return Serialize(result_columns, *output);
}

}  // namespace ROCKSDB_NAMESPACE

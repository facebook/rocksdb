//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/wide/wide_column_serialization.h"

#include <cassert>
#include <cstring>

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
    std::vector<const BlobIndex*>& blob_index_map) {
  if (Status s = ValidateWideColumnLimit(num_columns, "Too many wide columns");
      !s.ok()) {
    return s;
  }

  blob_index_map.assign(num_columns, nullptr);
  for (const auto& blob_col : blob_columns) {
    if (blob_col.first >= blob_index_map.size()) {
      return Status::InvalidArgument("Blob column index out of range");
    }
    blob_index_map[blob_col.first] = &blob_col.second;
  }

  return Status::OK();
}

bool WideColumnSerialization::ContainsBlobType(const char* type_bytes,
                                               uint32_t num_columns) {
  for (uint32_t i = 0; i < num_columns; ++i) {
    if (static_cast<uint8_t>(type_bytes[i]) == kTypeBlobIndex) {
      return true;
    }
  }
  return false;
}

Status WideColumnSerialization::Serialize(const WideColumns& columns,
                                          std::string& output) {
  const size_t num_columns = columns.size();

  if (Status sv = ValidateWideColumnLimit(num_columns, "Too many wide columns");
      !sv.ok()) {
    return sv;
  }

  PutVarint32(&output, kVersion1);

  PutVarint32(&output, static_cast<uint32_t>(num_columns));

  const Slice* prev_name = nullptr;

  for (size_t i = 0; i < columns.size(); ++i) {
    const WideColumn& column = columns[i];

    const Slice& name = column.name();
    if (Status s_name =
            ValidateWideColumnLimit(name.size(), "Wide column name too long");
        !s_name.ok()) {
      return s_name;
    }

    if (prev_name) {
      if (Status so = ValidateColumnOrder(*prev_name, name); !so.ok()) {
        return so;
      }
    }

    const Slice& value = column.value();
    if (Status s_val =
            ValidateWideColumnLimit(value.size(), "Wide column value too long");
        !s_val.ok()) {
      return s_val;
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

template <typename GetName, typename GetValue>
Status WideColumnSerialization::SerializeV2Impl(
    size_t num_columns,
    const std::vector<std::pair<size_t, BlobIndex>>& blob_columns,
    std::string& output, GetName get_name, GetValue get_value) {
  std::vector<const BlobIndex*> blob_index_map;
  if (Status s = BuildBlobIndexMap(num_columns, blob_columns, blob_index_map);
      !s.ok()) {
    return s;
  }
  assert(blob_index_map.size() == num_columns);

  // First pass: validate column ordering, compute sizes, serialize blob
  // indices, and build column types.
  std::vector<std::string> serialized_blob_indices(num_columns);
  std::vector<uint32_t> name_sizes(num_columns);
  std::vector<uint32_t> value_sizes(num_columns);
  std::string column_types;
  column_types.reserve(num_columns);

  Slice prev_name_storage;
  bool has_prev = false;
  uint32_t name_sizes_bytes = 0;
  uint32_t names_bytes = 0;
  uint32_t total_value_sizes_bytes = 0;
  uint32_t total_values_bytes = 0;

  for (size_t i = 0; i < num_columns; ++i) {
    const Slice name = get_name(i);
    const Slice value = get_value(i);

    if (Status sn =
            ValidateWideColumnLimit(name.size(), "Wide column name too long");
        !sn.ok()) {
      return sn;
    }

    if (has_prev) {
      if (Status so = ValidateColumnOrder(prev_name_storage, name); !so.ok()) {
        return so;
      }
    }

    name_sizes[i] = static_cast<uint32_t>(name.size());
    name_sizes_bytes += VarintLength(name_sizes[i]);
    names_bytes += name_sizes[i];

    if (blob_index_map[i] != nullptr) {
      const BlobIndex* blob_idx = blob_index_map[i];
      blob_idx->EncodeTo(&serialized_blob_indices[i]);
      value_sizes[i] = static_cast<uint32_t>(serialized_blob_indices[i].size());
      column_types.push_back(static_cast<char>(kTypeBlobIndex));
    } else {
      if (Status svl = ValidateWideColumnLimit(value.size(),
                                               "Wide column value too long");
          !svl.ok()) {
        return svl;
      }
      value_sizes[i] = static_cast<uint32_t>(value.size());
      column_types.push_back(static_cast<char>(kTypeValue));
    }

    total_value_sizes_bytes += VarintLength(value_sizes[i]);
    total_values_bytes += value_sizes[i];

    prev_name_storage = name;
    has_prev = true;
  }

  // Second pass: write all V2 sections to output.
  // Pre-allocate output string.
  const size_t total_size =
      VarintLength(kVersion2) +
      VarintLength(static_cast<uint32_t>(num_columns)) +
      num_columns +  // column types
      VarintLength(name_sizes_bytes) + VarintLength(total_value_sizes_bytes) +
      VarintLength(names_bytes) + name_sizes_bytes + total_value_sizes_bytes +
      names_bytes + total_values_bytes;

  const size_t base_offset = output.size();
  output.reserve(base_offset + total_size);

  // Sections 1-3: header, skip info, column types
  PutVarint32(&output, kVersion2);
  PutVarint32(&output, static_cast<uint32_t>(num_columns));
  PutVarint32(&output, name_sizes_bytes);
  PutVarint32(&output, total_value_sizes_bytes);
  PutVarint32(&output, names_bytes);
  output.append(column_types);

  // Sections 4-7: resize to final size, then write all 4 sections in a
  // single loop using independent pointers. Each section's start offset is
  // known from the sizes computed in the first pass.
  if (num_columns == 0) {
    return Status::OK();
  }

  const size_t sec4_offset = output.size();
  output.resize(base_offset + total_size);

  char* s4 = &output[sec4_offset];          // section 4: name sizes
  char* s5 = s4 + name_sizes_bytes;         // section 5: value sizes
  char* s6 = s5 + total_value_sizes_bytes;  // section 6: names
  char* s7 = s6 + names_bytes;              // section 7: values

  for (size_t i = 0; i < num_columns; ++i) {
    s4 = EncodeVarint32(s4, name_sizes[i]);
    s5 = EncodeVarint32(s5, value_sizes[i]);

    memcpy(s6, get_name(i).data(), name_sizes[i]);
    s6 += name_sizes[i];

    if (blob_index_map[i] != nullptr) {
      memcpy(s7, serialized_blob_indices[i].data(), value_sizes[i]);
    } else {
      memcpy(s7, get_value(i).data(), value_sizes[i]);
    }
    s7 += value_sizes[i];
  }

  return Status::OK();
}

Status WideColumnSerialization::SerializeV2(
    const std::vector<std::pair<std::string, std::string>>& columns,
    const std::vector<std::pair<size_t, BlobIndex>>& blob_columns,
    std::string& output) {
  return SerializeV2Impl(
      columns.size(), blob_columns, output,
      [&](size_t i) { return Slice(columns[i].first); },
      [&](size_t i) { return Slice(columns[i].second); });
}

Status WideColumnSerialization::SerializeV2(
    const WideColumns& columns,
    const std::vector<std::pair<size_t, BlobIndex>>& blob_columns,
    std::string& output) {
  return SerializeV2Impl(
      columns.size(), blob_columns, output,
      [&](size_t i) { return columns[i].name(); },
      [&](size_t i) { return columns[i].value(); });
}

Status WideColumnSerialization::DeserializeV1(
    Slice& input, uint32_t num_columns, std::vector<WideColumn>& columns) {
  columns.reserve(num_columns);

  autovector<uint32_t, 16> column_value_sizes;
  column_value_sizes.reserve(num_columns);

  for (uint32_t i = 0; i < num_columns; ++i) {
    Slice name;
    if (!GetLengthPrefixedSlice(&input, &name)) {
      return Status::Corruption("Error decoding wide column name");
    }

    if (!columns.empty()) {
      if (Status so = ValidateColumnOrder(columns.back().name(), name);
          !so.ok()) {
        return so;
      }
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

  return Status::OK();
}

Status WideColumnSerialization::DeserializeV2Impl(
    Slice& input, uint32_t num_columns, std::vector<WideColumn>& columns,
    std::vector<ValueType>& column_types) {
  // Section 2: SKIP INFO (3 varints)
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

  // Section 3: COLUMN TYPES (N bytes, each is a ValueType)
  if (input.size() < num_columns) {
    return Status::Corruption("Error decoding wide column types");
  }
  column_types.resize(num_columns);
  for (uint32_t i = 0; i < num_columns; ++i) {
    column_types[i] = static_cast<ValueType>(input[i]);
    if (!IsValidColumnValueType(column_types[i])) {
      return Status::Corruption("Unsupported wide column ValueType");
    }
  }
  input.remove_prefix(num_columns);

  // Validate that sections 4-6 fit in the remaining input
  const size_t metadata_size =
      name_sizes_bytes + value_sizes_bytes + names_bytes;
  if (input.size() < metadata_size) {
    return Status::Corruption("Error decoding wide column sections");
  }

  // Set up 4 pointers into sections 4-7 for single-loop parsing.
  // Skip info gives us exact boundaries for each section.
  const char* s4 = input.data();  // section 4: name sizes
  const char* s4_limit = s4 + name_sizes_bytes;
  const char* s5 = s4_limit;  // section 5: value sizes
  const char* s5_limit = s5 + value_sizes_bytes;
  const char* s6 = s5_limit;          // section 6: names
  const char* s7 = s6 + names_bytes;  // section 7: values
  const char* input_end = input.data() + input.size();

  columns.reserve(num_columns);
  size_t name_pos = 0;
  size_t value_pos = 0;

  for (uint32_t i = 0; i < num_columns; ++i) {
    // Decode name size from section 4
    uint32_t ns = 0;
    const char* s4_next = GetVarint32Ptr(s4, s4_limit, &ns);
    if (s4_next == nullptr) {
      return Status::Corruption("Error decoding wide column name size");
    }
    s4 = s4_next;

    // Decode value size from section 5
    uint32_t vs = 0;
    const char* s5_next = GetVarint32Ptr(s5, s5_limit, &vs);
    if (s5_next == nullptr) {
      return Status::Corruption("Error decoding wide column value size");
    }
    s5 = s5_next;

    // Read name from section 6
    if (name_pos + ns > names_bytes) {
      return Status::Corruption("Error decoding wide column name");
    }
    Slice name(s6 + name_pos, ns);

    if (!columns.empty()) {
      if (Status so = ValidateColumnOrder(columns.back().name(), name);
          !so.ok()) {
        return so;
      }
    }

    // Read value from section 7
    if (s7 + value_pos + vs > input_end) {
      return Status::Corruption("Error decoding wide column value payload");
    }

    columns.emplace_back(name, Slice(s7 + value_pos, vs));
    name_pos += ns;
    value_pos += vs;
  }

  return Status::OK();
}

Status WideColumnSerialization::Deserialize(Slice& input,
                                            WideColumns& columns) {
  assert(columns.empty());

  // Reuse DeserializeV2, then reject any blob references.
  std::vector<std::pair<size_t, BlobIndex>> blob_columns;
  if (Status s = DeserializeV2(input, columns, blob_columns); !s.ok()) {
    return s;
  }

  if (!blob_columns.empty()) {
    return Status::NotSupported(
        "Wide column contains blob references. Use DeserializeV2.");
  }

  return Status::OK();
}

Status WideColumnSerialization::DeserializeV2(
    Slice& input, std::vector<WideColumn>& columns,
    std::vector<std::pair<size_t, BlobIndex>>& blob_columns) {
  assert(columns.empty());
  assert(blob_columns.empty());

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
    // V2 layout: parse columns and extract blob column info
    std::vector<ValueType> column_types;

    if (Status s = DeserializeV2Impl(input, num_columns, columns, column_types);
        !s.ok()) {
      return s;
    }
    assert(column_types.size() == num_columns);
    assert(columns.size() == num_columns);

    // Decode blob indices from value data
    for (uint32_t i = 0; i < num_columns; ++i) {
      if (column_types[i] == kTypeBlobIndex) {
        BlobIndex blob_idx;
        Slice blob_slice = columns[i].value();
        if (Status bs = blob_idx.DecodeFrom(blob_slice); !bs.ok()) {
          return Status::Corruption("Error decoding blob index in wide column");
        }
        blob_columns.emplace_back(i, blob_idx);
      }
    }
  } else {
    return DeserializeV1(input, num_columns, columns);
  }

  return Status::OK();
}

Status WideColumnSerialization::HasBlobColumns(const Slice& input,
                                               bool& has_blob_columns) {
  has_blob_columns = false;

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

  // V2: Skip over SKIP INFO (3 varints) to reach COLUMN TYPES section.
  uint32_t unused_name_sizes_bytes = 0;
  uint32_t unused_value_sizes_bytes = 0;
  uint32_t unused_names_bytes = 0;
  if (!GetVarint32(&input_ref, &unused_name_sizes_bytes) ||
      !GetVarint32(&input_ref, &unused_value_sizes_bytes) ||
      !GetVarint32(&input_ref, &unused_names_bytes)) {
    return Status::Corruption("Error decoding wide column skip info");
  }
  if (input_ref.size() < num_columns) {
    return Status::Corruption("Error decoding wide column types");
  }
  has_blob_columns = ContainsBlobType(input_ref.data(), num_columns);

  return Status::OK();
}

Status WideColumnSerialization::GetVersion(const Slice& input,
                                           uint32_t& version) {
  Slice input_ref = input;

  version = 0;
  if (!GetVarint32(&input_ref, &version)) {
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

    // Read SKIP INFO (3 varints, immediately after header)
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

    // Read COLUMN TYPES (N bytes)
    if (input_ref.size() < num_columns) {
      return Status::Corruption("Error decoding wide column types");
    }
    // Check if default column (index 0) is a blob reference
    if (static_cast<uint8_t>(input_ref[0]) == kTypeBlobIndex) {
      return Status::NotSupported(
          "Wide column contains blob references. Use DeserializeV2.");
    }
    input_ref.remove_prefix(num_columns);

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

  if (Status s = Deserialize(input, columns); !s.ok()) {
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
    std::string& resolved_entity, bool& resolved, uint64_t* total_bytes_read,
    uint64_t* num_blobs_resolved) {
  assert(blob_fetcher);

  resolved = false;

  std::vector<WideColumn> columns;
  std::vector<std::pair<size_t, BlobIndex>> blob_columns;

  Slice input_copy = entity_value;
  if (Status s = DeserializeV2(input_copy, columns, blob_columns); !s.ok()) {
    return s;
  }

  if (blob_columns.empty()) {
    return Status::OK();
  }

  resolved = true;

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

Status WideColumnSerialization::GetValueOfDefaultColumnResolvingBlobs(
    const Slice& entity_value, const Slice& user_key,
    const BlobFetcher* blob_fetcher, PinnableSlice& result, bool& resolved) {
  assert(blob_fetcher);

  resolved = false;

  std::vector<WideColumn> columns;
  std::vector<std::pair<size_t, BlobIndex>> blob_columns;

  Slice input_copy = entity_value;
  if (Status s = DeserializeV2(input_copy, columns, blob_columns); !s.ok()) {
    return s;
  }

  // The default column (empty name) is always at index 0 when present
  // (columns are sorted by name).
  if (columns.empty() || columns[0].name() != kDefaultWideColumnName) {
    result.PinSelf(Slice());
    return Status::OK();
  }

  // Check if the default column (index 0) is a blob reference
  for (const auto& blob_col : blob_columns) {
    if (blob_col.first == 0) {
      const BlobIndex& blob_idx = blob_col.second;

      resolved = true;

      if (blob_idx.IsInlined()) {
        result.PinSelf(blob_idx.value());
        return Status::OK();
      }

      return blob_fetcher->FetchBlob(user_key, blob_idx,
                                     nullptr /* prefetch_buffer */, &result,
                                     nullptr /* bytes_read */);
    }
  }

  // Default column is inline
  result.PinSelf(columns[0].value());
  return Status::OK();
}

Status WideColumnSerialization::SerializeResolvedEntity(
    const std::vector<WideColumn>& columns,
    const std::vector<std::pair<size_t, BlobIndex>>& blob_columns,
    const std::vector<std::string>& resolved_blob_values, std::string& output) {
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
  return Serialize(result_columns, output);
}

}  // namespace ROCKSDB_NAMESPACE

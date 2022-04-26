//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/wide/wide_column_serialization.h"

#include <algorithm>
#include <cassert>

#include "util/coding.h"

namespace ROCKSDB_NAMESPACE {

Status WideColumnSerialization::Serialize(const WideColumnDescs& column_descs,
                                          std::string* output) {
  assert(
      std::is_sorted(column_descs.cbegin(), column_descs.cend(),
                     [](const WideColumnDesc& lhs, const WideColumnDesc& rhs) {
                       return lhs.first.compare(rhs.first) < 0;
                     }));
  assert(output);

  PutVarint32(output, static_cast<uint32_t>(column_descs.size()));

  size_t total_column_size = 0;

  for (const auto& [column_name, column_value] : column_descs) {
    PutVarint32(output, static_cast<uint32_t>(column_name.size()));
    PutVarint32(output, static_cast<uint32_t>(column_value.size()));

    total_column_size += column_name.size() + column_value.size();
  }

  output->reserve(output->size() + total_column_size);

  for (const auto& [column_name, column_value] : column_descs) {
    output->append(column_name.data(), column_name.size());
    output->append(column_value.data(), column_value.size());
  }

  return Status::OK();
}

Status WideColumnSerialization::DeserializeOne(Slice* input,
                                               const Slice& column_name,
                                               WideColumnDesc* column_desc) {
  WideColumnDescs all_column_descs;

  const Status s = DeserializeIndex(input, &all_column_descs);
  if (!s.ok()) {
    return s;
  }

  auto it = std::lower_bound(all_column_descs.cbegin(), all_column_descs.cend(),
                             column_name,
                             [](const WideColumnDesc& lhs, const Slice& rhs) {
                               return lhs.first.compare(rhs) < 0;
                             });
  if (it == all_column_descs.end() || it->first != column_name) {
    return Status::NotFound("Column not found");
  }

  *column_desc = *it;

  return Status::OK();
}

Status WideColumnSerialization::DeserializeAll(Slice* input,
                                               WideColumnDescs* column_descs) {
  return DeserializeIndex(input, column_descs);
}

Status WideColumnSerialization::DeserializeIndex(
    Slice* input, WideColumnDescs* column_descs) {
  assert(input);
  assert(column_descs);

  uint32_t num_columns = 0;
  if (!GetVarint32(input, &num_columns)) {
    return Status::Corruption("Error decoding number of columns");
  }

  if (!num_columns) {
    return Status::OK();
  }

  std::vector<std::pair<uint32_t, uint32_t>> column_sizes;
  column_sizes.reserve(num_columns);

  for (uint32_t i = 0; i < num_columns; ++i) {
    uint32_t name_size = 0;
    if (!GetVarint32(input, &name_size)) {
      return Status::Corruption("Error decoding column name size");
    }

    uint32_t value_size = 0;
    if (!GetVarint32(input, &value_size)) {
      return Status::Corruption("Error decoding column value size");
    }

    column_sizes.emplace_back(name_size, value_size);
  }

  const Slice data(*input);
  size_t pos = 0;

  for (const auto& [name_size, value_size] : column_sizes) {
    Slice column_name(data.data() + pos, name_size);
    Slice column_value(data.data() + pos + name_size, value_size);

    column_descs->emplace_back(column_name, column_value);

    pos += name_size + value_size;
  }

  return Status::OK();
}

}  // namespace ROCKSDB_NAMESPACE

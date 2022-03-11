//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/wide/wide_column_serialization.h"

#include <bits/stdint-uintn.h>

#include <algorithm>
#include <cassert>
#include <string>

#include "util/coding.h"

namespace ROCKSDB_NAMESPACE {

Status WideColumnSerialization::Serialize(const ColumnDescs& column_descs,
                                          std::string* output) {
  assert(output);

  PutFixed16(output, column_descs.size());

  uint32_t pos = sizeof(uint16_t) + column_descs.size() * 3 * sizeof(uint32_t);

  for (const auto& column_desc : column_descs) {
    PutFixed32(output, pos);
    PutFixed32(output, column_desc.first.size());
    PutFixed32(output, column_desc.second.size());

    pos += column_desc.first.size() + column_desc.second.size();
  }

  for (const auto& column_desc : column_descs) {
    const auto& column_name = column_desc.first;
    output->append(column_name.data(), column_name.size());

    const auto& column_value = column_desc.second;
    output->append(column_value.data(), column_value.size());
  }

  return Status::OK();
}

Status WideColumnSerialization::DeserializeAll(Slice* input,
                                            ColumnDescs* column_descs) {
  assert(input);
  assert(column_descs);

  const Slice orig_input(*input);

  uint16_t num_columns = 0;
  if (!GetFixed16(input, &num_columns)) {
    return Status::Corruption("Error decoding number of columns");
  }

  if (!num_columns) {
    return Status::OK();
  }

  for (uint16_t i = 0; i < num_columns; ++i) {
    uint32_t pos = 0;
    if (!GetFixed32(input, &pos)) {
      return Status::Corruption("Error decoding column position");
    }

    uint32_t name_size = 0;
    if (!GetFixed32(input, &name_size)) {
      return Status::Corruption("Error decoding column name size");
    }

    uint32_t value_size = 0;
    if (!GetFixed32(input, &value_size)) {
      return Status::Corruption("Error decoding column value size");
    }

    Slice column_name(orig_input.data() + pos, name_size);
    Slice column_value(orig_input.data() + pos + name_size, value_size);

    column_descs->emplace_back(column_name, column_value);
  }

  return Status::OK();
}

}  // namespace ROCKSDB_NAMESPACE

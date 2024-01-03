//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/wide/wide_column_serialization.h"

#include <algorithm>
#include <cassert>
#include <limits>

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

  PutVarint32(&output, kCurrentVersion);

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

Status WideColumnSerialization::Deserialize(Slice& input,
                                            WideColumns& columns) {
  assert(columns.empty());

  uint32_t version = 0;
  if (!GetVarint32(&input, &version)) {
    return Status::Corruption("Error decoding wide column version");
  }

  if (version > kCurrentVersion) {
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

  return Status::OK();
}

WideColumns::const_iterator WideColumnSerialization::Find(
    const WideColumns& columns, const Slice& column_name) {
  const auto it =
      std::lower_bound(columns.cbegin(), columns.cend(), column_name,
                       [](const WideColumn& lhs, const Slice& rhs) {
                         return lhs.name().compare(rhs) < 0;
                       });

  if (it == columns.cend() || it->name() != column_name) {
    return columns.cend();
  }

  return it;
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

}  // namespace ROCKSDB_NAMESPACE

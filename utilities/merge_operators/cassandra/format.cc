// Copyright (c) 2017-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
// This source code is also licensed under the GPLv2 license found in the
// COPYING file in the root directory of this source tree.

#include "format.h"

#include <algorithm>
#include <map>
#include <memory>

#include "utilities/merge_operators/cassandra/serialize.h"

namespace rocksdb {
namespace cassandra {
namespace {
const int32_t kDefaultLocalDeletionTime =
  std::numeric_limits<int32_t>::max();
const int64_t kDefaultMarkedForDeleteAt =
  std::numeric_limits<int64_t>::min();
}

ColumnBase::ColumnBase(int8_t mask, int8_t index)
  : mask_(mask), index_(index) {}

std::size_t ColumnBase::Size() const {
  return sizeof(mask_) + sizeof(index_);
}

int8_t ColumnBase::Mask() const {
  return mask_;
}

int8_t ColumnBase::Index() const {
  return index_;
}

void ColumnBase::Serialize(std::string* dest) const {
  rocksdb::cassandra::Serialize<int8_t>(mask_, dest);
  rocksdb::cassandra::Serialize<int8_t>(index_, dest);
}

std::unique_ptr<ColumnBase> ColumnBase::Deserialize(const char* src,
                                                    std::size_t offset) {
  int8_t mask = rocksdb::cassandra::Deserialize<int8_t>(src, offset);
  if ((mask & ColumnTypeMask::DELETION_MASK) != 0) {
    return Tombstone::Deserialize(src, offset);
  } else if ((mask & ColumnTypeMask::EXPIRATION_MASK) != 0) {
    return ExpiringColumn::Deserialize(src, offset);
  } else {
    return Column::Deserialize(src, offset);
  }
}

Column::Column(
  int8_t mask,
  int8_t index,
  int64_t timestamp,
  int32_t value_size,
  const char* value
) : ColumnBase(mask, index), timestamp_(timestamp),
  value_size_(value_size), value_(value) {}

int64_t Column::Timestamp() const {
  return timestamp_;
}

std::size_t Column::Size() const {
  return ColumnBase::Size() + sizeof(timestamp_) + sizeof(value_size_)
    + value_size_;
}

void Column::Serialize(std::string* dest) const {
  ColumnBase::Serialize(dest);
  rocksdb::cassandra::Serialize<int64_t>(timestamp_, dest);
  rocksdb::cassandra::Serialize<int32_t>(value_size_, dest);
  dest->append(value_, value_size_);
}

std::unique_ptr<Column> Column::Deserialize(const char *src,
                                            std::size_t offset) {
  int8_t mask = rocksdb::cassandra::Deserialize<int8_t>(src, offset);
  offset += sizeof(mask);
  int8_t index = rocksdb::cassandra::Deserialize<int8_t>(src, offset);
  offset += sizeof(index);
  int64_t timestamp = rocksdb::cassandra::Deserialize<int64_t>(src, offset);
  offset += sizeof(timestamp);
  int32_t value_size = rocksdb::cassandra::Deserialize<int32_t>(src, offset);
  offset += sizeof(value_size);
  return std::unique_ptr<Column>(
    new Column(mask, index, timestamp, value_size, src + offset));
}

ExpiringColumn::ExpiringColumn(
  int8_t mask,
  int8_t index,
  int64_t timestamp,
  int32_t value_size,
  const char* value,
  int32_t ttl
) : Column(mask, index, timestamp, value_size, value),
  ttl_(ttl) {}

std::size_t ExpiringColumn::Size() const {
  return Column::Size() + sizeof(ttl_);
}

void ExpiringColumn::Serialize(std::string* dest) const {
  Column::Serialize(dest);
  rocksdb::cassandra::Serialize<int32_t>(ttl_, dest);
}

std::unique_ptr<ExpiringColumn> ExpiringColumn::Deserialize(
    const char *src,
    std::size_t offset) {
  int8_t mask = rocksdb::cassandra::Deserialize<int8_t>(src, offset);
  offset += sizeof(mask);
  int8_t index = rocksdb::cassandra::Deserialize<int8_t>(src, offset);
  offset += sizeof(index);
  int64_t timestamp = rocksdb::cassandra::Deserialize<int64_t>(src, offset);
  offset += sizeof(timestamp);
  int32_t value_size = rocksdb::cassandra::Deserialize<int32_t>(src, offset);
  offset += sizeof(value_size);
  const char* value = src + offset;
  offset += value_size;
  int32_t ttl =  rocksdb::cassandra::Deserialize<int32_t>(src, offset);
  return std::unique_ptr<ExpiringColumn>(
    new ExpiringColumn(mask, index, timestamp, value_size, value, ttl));
}

Tombstone::Tombstone(
  int8_t mask,
  int8_t index,
  int32_t local_deletion_time,
  int64_t marked_for_delete_at
) : ColumnBase(mask, index), local_deletion_time_(local_deletion_time),
  marked_for_delete_at_(marked_for_delete_at) {}

int64_t Tombstone::Timestamp() const {
  return marked_for_delete_at_;
}

std::size_t Tombstone::Size() const {
  return ColumnBase::Size() + sizeof(local_deletion_time_)
    + sizeof(marked_for_delete_at_);
}

void Tombstone::Serialize(std::string* dest) const {
  ColumnBase::Serialize(dest);
  rocksdb::cassandra::Serialize<int32_t>(local_deletion_time_, dest);
  rocksdb::cassandra::Serialize<int64_t>(marked_for_delete_at_, dest);
}

std::unique_ptr<Tombstone> Tombstone::Deserialize(const char *src,
                                                  std::size_t offset) {
  int8_t mask = rocksdb::cassandra::Deserialize<int8_t>(src, offset);
  offset += sizeof(mask);
  int8_t index = rocksdb::cassandra::Deserialize<int8_t>(src, offset);
  offset += sizeof(index);
  int32_t local_deletion_time =
    rocksdb::cassandra::Deserialize<int32_t>(src, offset);
  offset += sizeof(int32_t);
  int64_t marked_for_delete_at =
    rocksdb::cassandra::Deserialize<int64_t>(src, offset);
  return std::unique_ptr<Tombstone>(
    new Tombstone(mask, index, local_deletion_time, marked_for_delete_at));
}

RowValue::RowValue(int32_t local_deletion_time, int64_t marked_for_delete_at)
  : local_deletion_time_(local_deletion_time),
  marked_for_delete_at_(marked_for_delete_at), columns_(),
  last_modified_time_(0) {}

RowValue::RowValue(std::vector<std::unique_ptr<ColumnBase>> columns,
                  int64_t last_modified_time)
  : local_deletion_time_(kDefaultLocalDeletionTime),
  marked_for_delete_at_(kDefaultMarkedForDeleteAt),
  columns_(std::move(columns)), last_modified_time_(last_modified_time) {}

std::size_t RowValue::Size() const {
  std::size_t size = sizeof(local_deletion_time_)
    + sizeof(marked_for_delete_at_);
  for (const auto& column : columns_) {
    size += column -> Size();
  }
  return size;
}

int64_t RowValue::LastModifiedTime() const {
  if (IsTombstone()) {
    return marked_for_delete_at_;
  } else {
    return last_modified_time_;
  }
}

bool RowValue::IsTombstone() const {
  return marked_for_delete_at_ > kDefaultMarkedForDeleteAt;
}

void RowValue::Serialize(std::string* dest) const {
  rocksdb::cassandra::Serialize<int32_t>(local_deletion_time_, dest);
  rocksdb::cassandra::Serialize<int64_t>(marked_for_delete_at_, dest);
  for (const auto& column : columns_) {
    column -> Serialize(dest);
  }
}

RowValue RowValue::Deserialize(const char *src, std::size_t size) {
  std::size_t offset = 0;
  assert(size >= sizeof(local_deletion_time_) + sizeof(marked_for_delete_at_));
  int32_t local_deletion_time =
    rocksdb::cassandra::Deserialize<int32_t>(src, offset);
  offset += sizeof(int32_t);
  int64_t marked_for_delete_at =
    rocksdb::cassandra::Deserialize<int64_t>(src, offset);
  offset += sizeof(int64_t);
  if (offset == size) {
    return RowValue(local_deletion_time, marked_for_delete_at);
  }

  assert(local_deletion_time == kDefaultLocalDeletionTime);
  assert(marked_for_delete_at == kDefaultMarkedForDeleteAt);
  std::vector<std::unique_ptr<ColumnBase>> columns;
  int64_t last_modified_time = 0;
  while (offset < size) {
    auto c = ColumnBase::Deserialize(src, offset);
    offset += c -> Size();
    assert(offset <= size);
    last_modified_time = std::max(last_modified_time, c -> Timestamp());
    columns.push_back(std::move(c));
  }

  return RowValue(std::move(columns), last_modified_time);
}

// Merge multiple row values into one.
// For each column in rows with same index, we pick the one with latest
// timestamp. And we also take row tombstone into consideration, by iterating
// each row from reverse timestamp order, and stop once we hit the first
// row tombstone.
RowValue RowValue::Merge(std::vector<RowValue>&& values) {
  assert(values.size() > 0);
  if (values.size() == 1) {
    return std::move(values[0]);
  }

  // Merge columns by their last modified time, and skip once we hit
  // a row tombstone.
  std::sort(values.begin(), values.end(),
    [](const RowValue& r1, const RowValue& r2) {
      return r1.LastModifiedTime() > r2.LastModifiedTime();
    });

  std::map<int8_t, std::unique_ptr<ColumnBase>> merged_columns;
  int64_t tombstone_timestamp = 0;

  for (auto& value : values) {
    if (value.IsTombstone()) {
      if (merged_columns.size() == 0) {
        return std::move(value);
      }
      tombstone_timestamp = value.LastModifiedTime();
      break;
    }
    for (auto& column : value.columns_) {
      int8_t index = column->Index();
      if (merged_columns.find(index) == merged_columns.end()) {
        merged_columns[index] = std::move(column);
      } else {
        if (column->Timestamp() > merged_columns[index]->Timestamp()) {
          merged_columns[index] = std::move(column);
        }
      }
    }
  }

  int64_t last_modified_time = 0;
  std::vector<std::unique_ptr<ColumnBase>> columns;
  for (auto& pair: merged_columns) {
    // For some row, its last_modified_time > row tombstone_timestamp, but
    // it might have rows whose timestamp is ealier than tombstone, so we
    // ned to filter these rows.
    if (pair.second->Timestamp() <= tombstone_timestamp) {
      continue;
    }
    last_modified_time = std::max(last_modified_time, pair.second->Timestamp());
    columns.push_back(std::move(pair.second));
  }
  return RowValue(std::move(columns), last_modified_time);
}

} // namepsace cassandrda
} // namespace rocksdb

// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "utilities/cassandra/cassandra_compaction_filter.h"

namespace rocksdb {
namespace cassandra {

const char* CassandraCompactionFilter::Name() const {
  return "CassandraCompactionFilter";
}

void CassandraCompactionFilter::SetMetaCfHandle(
    DB* meta_db, ColumnFamilyHandle* meta_cf_handle) {
  meta_db_ = meta_db;
  meta_cf_handle_ = meta_cf_handle;
}

PartitionDeletion CassandraCompactionFilter::GetPartitionDelete(
    const Slice& key) const {
  if (!meta_db_) {
    // skip triming when parition meta db is not ready yet
    return PartitionDeletion::kDefault;
  }

  DB* meta_db = meta_db_.load();
  if (!meta_cf_handle_) {
    // skip triming when parition meta cf handle is not ready yet
    return PartitionDeletion::kDefault;
  }
  ColumnFamilyHandle* meta_cf_handle = meta_cf_handle_.load();

  auto it = unique_ptr<Iterator>(
    meta_db->NewIterator(rocksdb::ReadOptions(), meta_cf_handle));
  // partition meta key is encoded token+paritionkey
  it->SeekForPrev(key);
  if (!it->Valid()) {
    // skip trimming when
    return PartitionDeletion::kDefault;
  }

  if (!key.starts_with(it->key())) {
    // skip trimming when there is no parition meta data
    return PartitionDeletion::kDefault;
  }

  Slice value = it->value();
  return PartitionDeletion::Deserialize(value.data(), value.size());
}

bool CassandraCompactionFilter::ShouldDropByParitionDelete(
    const Slice& key,
    std::chrono::time_point<std::chrono::system_clock> row_timestamp) const {
  std::chrono::seconds gc_grace_period =
      ignore_range_delete_on_read_ ? std::chrono::seconds(0) : gc_grace_period_;
  return GetPartitionDelete(key).MarkForDeleteAt() >
         row_timestamp + gc_grace_period;
}

CompactionFilter::Decision CassandraCompactionFilter::FilterV2(
    int /*level*/, const Slice& key, ValueType value_type,
    const Slice& existing_value, std::string* new_value,
    std::string* /*skip_until*/) const {
  bool value_changed = false;
  RowValue row_value = RowValue::Deserialize(
    existing_value.data(), existing_value.size());

  if (ShouldDropByParitionDelete(key, row_value.LastModifiedTimePoint())) {
    return Decision::kRemove;
  }

  RowValue compacted =
      purge_ttl_on_expiration_
          ? row_value.RemoveExpiredColumns(&value_changed)
          : row_value.ConvertExpiredColumnsToTombstones(&value_changed);

  if (value_type == ValueType::kValue) {
    compacted = compacted.RemoveTombstones(gc_grace_period_);
  }

  if(compacted.Empty()) {
    return Decision::kRemove;
  }

  if (value_changed) {
    compacted.Serialize(new_value);
    return Decision::kChangeValue;
  }

  return Decision::kKeep;
}

}  // namespace cassandra
}  // namespace rocksdb

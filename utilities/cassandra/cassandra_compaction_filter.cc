// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "utilities/cassandra/cassandra_compaction_filter.h"

#include <string>

#include "rocksdb/slice.h"
#include "rocksdb/utilities/object_registry.h"
#include "rocksdb/utilities/options_type.h"
#include "utilities/cassandra/format.h"

namespace ROCKSDB_NAMESPACE {
namespace cassandra {
static std::unordered_map<std::string, OptionTypeInfo>
    cassandra_filter_type_info = {
#ifndef ROCKSDB_LITE
        {"purge_ttl_on_expiration",
         {offsetof(struct CassandraOptions, purge_ttl_on_expiration),
          OptionType::kBoolean, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone, 0}},
        {"gc_grace_period_in_seconds",
         {offsetof(struct CassandraOptions, gc_grace_period_in_seconds),
          OptionType::kUInt32T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone, 0}},
#endif  // ROCKSDB_LITE
};

CassandraCompactionFilter::CassandraCompactionFilter(
    bool purge_ttl_on_expiration, int32_t gc_grace_period_in_seconds) {
  options_.purge_ttl_on_expiration = purge_ttl_on_expiration;
  options_.gc_grace_period_in_seconds = gc_grace_period_in_seconds;
  RegisterOptions("CassandraFilterOptions", &options_,
                  &cassandra_filter_type_info);
}

const char* CassandraCompactionFilter::Name() const {
  return CassandraOptionsHelper::kCompactionFilterName.c_str();
}

CompactionFilter::Decision CassandraCompactionFilter::FilterV2(
    int /*level*/, const Slice& /*key*/, ValueType value_type,
    const Slice& existing_value, std::string* new_value,
    std::string* /*skip_until*/) const {
  bool value_changed = false;
  RowValue row_value = RowValue::Deserialize(
    existing_value.data(), existing_value.size());
  RowValue compacted =
      options_.purge_ttl_on_expiration
          ? row_value.RemoveExpiredColumns(&value_changed)
          : row_value.ConvertExpiredColumnsToTombstones(&value_changed);

  if (value_type == ValueType::kValue) {
    compacted = compacted.RemoveTombstones(options_.gc_grace_period_in_seconds);
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

CassandraCompactionFilterFactory::CassandraCompactionFilterFactory(
    bool purge_ttl_on_expiration, int32_t gc_grace_period_in_seconds) {
  options_.purge_ttl_on_expiration = purge_ttl_on_expiration;
  options_.gc_grace_period_in_seconds = gc_grace_period_in_seconds;
  RegisterOptions("CassandraFilterOptions", &options_,
                  &cassandra_filter_type_info);
}

std::unique_ptr<CompactionFilter>
CassandraCompactionFilterFactory::CreateCompactionFilter(
    const CompactionFilter::Context&) {
  std::unique_ptr<CompactionFilter> result(new CassandraCompactionFilter(
      options_.purge_ttl_on_expiration, options_.gc_grace_period_in_seconds));
  return result;
}

const char* CassandraCompactionFilterFactory::Name() const {
  return CassandraOptionsHelper::kCompactionFilterName.c_str();
}

}  // namespace cassandra
}  // namespace ROCKSDB_NAMESPACE

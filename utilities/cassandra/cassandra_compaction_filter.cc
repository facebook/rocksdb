// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "utilities/cassandra/cassandra_compaction_filter.h"
#include <string>
#include "rocksdb/extension_loader.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "utilities/cassandra/format.h"


namespace rocksdb {
namespace cassandra {

#ifndef ROCKSDB_LITE
static rocksdb::Extension *NewCassandraCompactionFilter(const std::string & ,
							       const rocksdb::DBOptions &,
							       const rocksdb::ColumnFamilyOptions *,
							       std::unique_ptr<rocksdb::Extension>* guard) {
  guard->reset(new rocksdb::cassandra::CassandraCompactionFilter(false, 0));
  return guard->get();
}

void CassandraCompactionFilter::RegisterFactory(ExtensionLoader & loader) {
  loader.RegisterFactory(CompactionFilter::Type(), "cassandra", NewCassandraCompactionFilter);
}
  
void CassandraCompactionFilter::RegisterFactory(DBOptions & dbOpts) {
  RegisterFactory(*(dbOpts.extensions));
}
  
#endif
const char* CassandraCompactionFilter::Name() const {
  return "CassandraCompactionFilter";
}

static OptionTypeMap CassandraCompactionFilterOptionsMap =
{
 {"purge_ttl_on_expiration", {offsetof(struct CassandraFilterOptions, purge_ttl_on_expiration),
	       OptionType::kBoolean, OptionVerificationType::kNormal,
	       true, 0}},
 {"gc_grace_seconds", {offsetof(struct CassandraFilterOptions, gc_grace_period_in_seconds),
	       OptionType::kUInt32T, OptionVerificationType::kNormal,
	       true, 0}}
};
  
const OptionTypeMap *CassandraCompactionFilter::GetOptionsMap() const {
  return &CassandraCompactionFilterOptionsMap;
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

#ifndef ROCKSDB_LITE
static rocksdb::Extension *NewCassandraCompactionFilterFactory(const std::string & ,
							       const rocksdb::DBOptions &,
							       const rocksdb::ColumnFamilyOptions *,
							       std::unique_ptr<rocksdb::Extension>* guard) {
  guard->reset(new rocksdb::cassandra::CassandraCompactionFilterFactory(false, 0));
  return guard->get();
}

void CassandraCompactionFilterFactory::RegisterFactory(ExtensionLoader & loader) {
  loader.RegisterFactory(CompactionFilterFactory::Type(), "cassandra", NewCassandraCompactionFilterFactory);
}
void CassandraCompactionFilterFactory::RegisterFactory(DBOptions & dbOpts) {
  RegisterFactory(*(dbOpts.extensions));
}
#endif

const char* CassandraCompactionFilterFactory::Name() const {
  return "CassandraCompactionFilterFactory";
}

const OptionTypeMap *CassandraCompactionFilterFactory::GetOptionsMap() const {
  return &CassandraCompactionFilterOptionsMap;
}
}  // namespace cassandra
}  // namespace rocksdb

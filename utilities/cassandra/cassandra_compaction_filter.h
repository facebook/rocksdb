// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once
#include <string>
#include "rocksdb/compaction_filter.h"
#include "rocksdb/slice.h"

namespace rocksdb {
struct DBOptions;
class ExtensionLoader;
namespace cassandra {

/**
 * Compaction filter for removing expired Cassandra data with ttl.
 * If option `purge_ttl_on_expiration` is set to true, expired data
 * will be directly purged. Otherwise expired data will be converted
 * tombstones first, then be eventally removed after gc grace period.
 * `purge_ttl_on_expiration` should only be on in the case all the
 * writes have same ttl setting, otherwise it could bring old data back.
 *
 * Compaction filter is also in charge of removing tombstone that has been
 * promoted to kValue type after serials of merging in compaction.
 */
struct CassandraFilterOptions {
  bool purge_ttl_on_expiration;
  uint32_t gc_grace_period_in_seconds;
};

class CassandraCompactionFilter : public CompactionFilter {
public:
#ifndef ROCKSDB_LITE
  static void RegisterFactory(DBOptions & dbOpts);
  static void RegisterFactory(ExtensionLoader & loader);
#endif
public:
  explicit CassandraCompactionFilter(bool purge_ttl_on_expiration,
                                    uint32_t gc_grace_period_in_seconds) {
   options_.purge_ttl_on_expiration = purge_ttl_on_expiration;
   options_.gc_grace_period_in_seconds = gc_grace_period_in_seconds;
 }

 virtual void *GetOptionsPtr() override { return &options_; }
 virtual const char *GetOptionsPrefix() const override { return "cassandra.rocksdb."; }
 virtual const OptionTypeMap *GetOptionsMap() const override;
 virtual const char* Name() const override;
 virtual Decision FilterV2(int level, const Slice& key, ValueType value_type,
                           const Slice& existing_value, std::string* new_value,
                           std::string* skip_until) const override;

private:
  CassandraFilterOptions options_;
};

class CassandraCompactionFilterFactory : public CompactionFilterFactory {
public:
#ifndef ROCKSDB_LITE
  static void RegisterFactory(DBOptions & dbOpts);
  static void RegisterFactory(ExtensionLoader & loader);
#endif
public:
 explicit CassandraCompactionFilterFactory(bool purge_ttl_on_expiration,
					    uint32_t gc_grace_period_in_seconds) {
   options_.purge_ttl_on_expiration = purge_ttl_on_expiration;
   options_.gc_grace_period_in_seconds = gc_grace_period_in_seconds;
 }
 virtual void *GetOptionsPtr() override { return &options_; }
 virtual const char *GetOptionsPrefix() const override { return "cassandra.rocksdb."; }
 virtual const OptionTypeMap *GetOptionsMap() const override;
 virtual const char* Name() const override;
  virtual std::unique_ptr<CompactionFilter> CreateCompactionFilter(const CompactionFilter::Context& /*context*/) override {
   return std::unique_ptr<CompactionFilter>(new CassandraCompactionFilter(
       options_.purge_ttl_on_expiration, options_.gc_grace_period_in_seconds));
  }
private:
  CassandraFilterOptions options_;
};
}  // namespace cassandra
}  // namespace rocksdb

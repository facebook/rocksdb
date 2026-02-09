//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include "rocksdb/table.h"
#include "util/gflags_compat.h"
#include "util/random.h"

DECLARE_int32(mark_for_compaction_one_file_in);

namespace ROCKSDB_NAMESPACE {

// A `DbStressTablePropertiesCollector` ignores what keys/values were added to
// the table, adds no properties to the table, and decides at random whether the
// table will be marked for compaction according to
// `FLAGS_mark_for_compaction_one_file_in`.
class DbStressTablePropertiesCollector : public TablePropertiesCollector {
 public:
  DbStressTablePropertiesCollector()
      : need_compact_(Random::GetTLSInstance()->OneInOpt(
            FLAGS_mark_for_compaction_one_file_in)) {}

  Status AddUserKey(const Slice& /* key */, const Slice& /* value */,
                    EntryType /*type*/, SequenceNumber /*seq*/,
                    uint64_t /*file_size*/) override {
    ++keys_added;
    ++all_calls;
    return Status::OK();
  }

  void BlockAdd(uint64_t /* block_uncomp_bytes */,
                uint64_t /* block_compressed_bytes_fast */,
                uint64_t /* block_compressed_bytes_slow */) override {
    ++blocks_added;
    ++all_calls;
  }

  Status Finish(UserCollectedProperties* properties) override {
    ++all_calls;
    (*properties)["db_stress_collector_property"] =
        std::to_string(keys_added) + ";" + std::to_string(blocks_added) + ";" +
        std::to_string(all_calls);
    return Status::OK();
  }

  UserCollectedProperties GetReadableProperties() const override {
    UserCollectedProperties props;
    const_cast<DbStressTablePropertiesCollector*>(this)->Finish(&props);
    return props;
  }

  const char* Name() const override {
    return "DbStressTablePropertiesCollector";
  }

  bool NeedCompact() const override {
    ++all_calls;
    return need_compact_;
  }

 private:
  const bool need_compact_;
  // These are tracked to detect race conditions that would arise from RocksDB
  // invoking TablePropertiesCollector functions in an unsynchronized way, as
  // TablePropertiesCollectors are allowed (encouraged) not to be thread safe.
  size_t keys_added = 0;
  size_t blocks_added = 0;
  // Including race between BlockAdd and AddUserKey (etc.)
  mutable size_t all_calls = 0;
};

// A `DbStressTablePropertiesCollectorFactory` creates
// `DbStressTablePropertiesCollectorFactory`s.
class DbStressTablePropertiesCollectorFactory
    : public TablePropertiesCollectorFactory {
 public:
  TablePropertiesCollector* CreateTablePropertiesCollector(
      TablePropertiesCollectorFactory::Context /* context */) override {
    return new DbStressTablePropertiesCollector();
  }

  const char* Name() const override {
    return "DbStressTablePropertiesCollectorFactory";
  }
};

}  // namespace ROCKSDB_NAMESPACE

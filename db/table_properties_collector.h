//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// This file defines a collection of statistics collectors.
#pragma once

#include <memory>
#include <string>
#include <vector>

#include "db/dbformat.h"
#include "rocksdb/comparator.h"
#include "rocksdb/table_properties.h"

namespace ROCKSDB_NAMESPACE {

// Base class for internal table properties collector.
class IntTblPropCollector {
 public:
  virtual ~IntTblPropCollector() {}
  virtual Status Finish(UserCollectedProperties* properties) = 0;

  virtual const char* Name() const = 0;

  // @params key    the user key that is inserted into the table.
  // @params value  the value that is inserted into the table.
  virtual Status InternalAdd(const Slice& key, const Slice& value,
                             uint64_t file_size) = 0;

  virtual void BlockAdd(uint64_t block_raw_bytes,
                        uint64_t block_compressed_bytes_fast,
                        uint64_t block_compressed_bytes_slow) = 0;

  virtual UserCollectedProperties GetReadableProperties() const = 0;

  virtual bool NeedCompact() const { return false; }
};

// Factory for internal table properties collector.
class IntTblPropCollectorFactory {
 public:
  virtual ~IntTblPropCollectorFactory() {}
  // has to be thread-safe
  virtual IntTblPropCollector* CreateIntTblPropCollector(
      uint32_t column_family_id, int level_at_creation) = 0;

  // The name of the properties collector can be used for debugging purpose.
  virtual const char* Name() const = 0;
};

using IntTblPropCollectorFactories =
    std::vector<std::unique_ptr<IntTblPropCollectorFactory>>;

// When rocksdb creates a new table, it will encode all "user keys" into
// "internal keys", which contains meta information of a given entry.
//
// This class extracts user key from the encoded internal key when Add() is
// invoked.
class UserKeyTablePropertiesCollector : public IntTblPropCollector {
 public:
  // transfer of ownership
  explicit UserKeyTablePropertiesCollector(TablePropertiesCollector* collector)
      : collector_(collector) {}

  virtual ~UserKeyTablePropertiesCollector() {}

  virtual Status InternalAdd(const Slice& key, const Slice& value,
                             uint64_t file_size) override;

  virtual void BlockAdd(uint64_t block_raw_bytes,
                        uint64_t block_compressed_bytes_fast,
                        uint64_t block_compressed_bytes_slow) override;

  virtual Status Finish(UserCollectedProperties* properties) override;

  virtual const char* Name() const override { return collector_->Name(); }

  UserCollectedProperties GetReadableProperties() const override;

  virtual bool NeedCompact() const override {
    return collector_->NeedCompact();
  }

 protected:
  std::unique_ptr<TablePropertiesCollector> collector_;
};

class UserKeyTablePropertiesCollectorFactory
    : public IntTblPropCollectorFactory {
 public:
  explicit UserKeyTablePropertiesCollectorFactory(
      std::shared_ptr<TablePropertiesCollectorFactory> user_collector_factory)
      : user_collector_factory_(user_collector_factory) {}
  virtual IntTblPropCollector* CreateIntTblPropCollector(
      uint32_t column_family_id, int level_at_creation) override {
    TablePropertiesCollectorFactory::Context context;
    context.column_family_id = column_family_id;
    context.level_at_creation = level_at_creation;
    return new UserKeyTablePropertiesCollector(
        user_collector_factory_->CreateTablePropertiesCollector(context));
  }

  virtual const char* Name() const override {
    return user_collector_factory_->Name();
  }

 private:
  std::shared_ptr<TablePropertiesCollectorFactory> user_collector_factory_;
};

// When rocksdb creates a newtable, it will encode all "user keys" into
// "internal keys". This class collects min/max timestamp from the encoded
// internal key when Add() is invoked.
//
// @param cmp the Comparator to compare timestamp of key by the
//     Comparator::CompareTimestamp function.
class TimestampTablePropertiesCollector : public IntTblPropCollector {
 public:
  explicit TimestampTablePropertiesCollector(const Comparator* cmp)
      : cmp_(cmp) {}

  Status InternalAdd(const Slice& key, const Slice& /* value */,
                     uint64_t /* file_size */) override {
    auto user_key = ExtractUserKey(key);
    assert(cmp_ && cmp_->timestamp_size() > 0);
    if (user_key.size() < cmp_->timestamp_size()) {
      return Status::Corruption(
          "User key size mismatch when comparing to timestamp size.");
    }
    auto timestamp_in_key =
        ExtractTimestampFromUserKey(user_key, cmp_->timestamp_size());
    if (max_timestamp_ == kDisableUserTimestamp ||
        cmp_->CompareTimestamp(timestamp_in_key, max_timestamp_) > 0) {
      max_timestamp_ = timestamp_in_key.ToString();
    }
    if (min_timestamp_ == kDisableUserTimestamp ||
        cmp_->CompareTimestamp(min_timestamp_, timestamp_in_key) > 0) {
      min_timestamp_ = timestamp_in_key.ToString();
    }
    return Status::OK();
  }

  void BlockAdd(uint64_t /* block_raw_bytes */,
                uint64_t /* block_compressed_bytes_fast */,
                uint64_t /* block_compressed_bytes_slow */) override {
    return;
  }

  Status Finish(UserCollectedProperties* properties) override {
    assert(min_timestamp_.size() == max_timestamp_.size() &&
           max_timestamp_.size() == cmp_->timestamp_size());
    properties->insert({"rocksdb.min_timestamp", min_timestamp_});
    properties->insert({"rocksdb.max_timestamp", max_timestamp_});
    return Status::OK();
  }

  const char* Name() const override {
    return "TimestampTablePropertiesCollector";
  }

  UserCollectedProperties GetReadableProperties() const override {
    return {{"rocksdb.min_timestamp", Slice(min_timestamp_).ToString(true)},
            {"rocksdb.max_timestamp", Slice(max_timestamp_).ToString(true)}};
  }

 protected:
  const Comparator* const cmp_;
  std::string max_timestamp_;
  std::string min_timestamp_;
};

}  // namespace ROCKSDB_NAMESPACE

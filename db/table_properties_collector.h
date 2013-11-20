//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// This file defines a collection of statistics collectors.
#pragma once

#include "rocksdb/table_properties.h"

#include <memory>
#include <string>
#include <vector>

namespace rocksdb {

struct InternalKeyTablePropertiesNames {
  static const std::string kDeletedKeys;
};

// Collecting the statistics for internal keys. Visible only by internal
// rocksdb modules.
class InternalKeyPropertiesCollector : public TablePropertiesCollector {
 public:
  virtual Status Add(const Slice& key, const Slice& value) override;

  virtual Status Finish(
      TableProperties::UserCollectedProperties* properties) override;

  virtual const char* Name() const override {
    return "InternalKeyPropertiesCollector";
  }

  TableProperties::UserCollectedProperties
    GetReadableProperties() const override;

 private:
  uint64_t deleted_keys_ = 0;
};

// When rocksdb creates a new table, it will encode all "user keys" into
// "internal keys", which contains meta information of a given entry.
//
// This class extracts user key from the encoded internal key when Add() is
// invoked.
class UserKeyTablePropertiesCollector : public TablePropertiesCollector {
 public:
  explicit UserKeyTablePropertiesCollector(
      TablePropertiesCollector* collector) :
      UserKeyTablePropertiesCollector(
        std::shared_ptr<TablePropertiesCollector>(collector)
    ) {
  }

  explicit UserKeyTablePropertiesCollector(
      std::shared_ptr<TablePropertiesCollector> collector) :
      collector_(collector) {
  }

  virtual ~UserKeyTablePropertiesCollector() { }

  virtual Status Add(const Slice& key, const Slice& value) override;

  virtual Status Finish(
      TableProperties::UserCollectedProperties* properties) override;

  virtual const char* Name() const override { return collector_->Name(); }

  TableProperties::UserCollectedProperties
    GetReadableProperties() const override;

 protected:
  std::shared_ptr<TablePropertiesCollector> collector_;
};

}  // namespace rocksdb

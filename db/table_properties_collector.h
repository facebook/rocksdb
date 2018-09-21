//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
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
  static const std::string kMergeOperands;
};

struct SSTVarietiesTablePropertiesNames {
  static const std::string kSstPurpose;
  static const std::string kSstDepend;
  static const std::string kSstReadAmp;
};

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

  virtual UserCollectedProperties GetReadableProperties() const = 0;

  virtual bool NeedCompact() const { return false; }
};

// Factory for internal table properties collector.
class IntTblPropCollectorFactory {
 public:
  virtual ~IntTblPropCollectorFactory() {}
  // has to be thread-safe
  virtual IntTblPropCollector* CreateIntTblPropCollector(
      uint32_t column_family_id) = 0;

  // The name of the properties collector can be used for debugging purpose.
  virtual const char* Name() const = 0;
};

// Collecting the statistics for internal keys. Visible only by internal
// rocksdb modules.
class InternalKeyPropertiesCollector : public IntTblPropCollector {
 public:
  virtual Status InternalAdd(const Slice& key, const Slice& value,
                             uint64_t file_size) override;

  virtual Status Finish(UserCollectedProperties* properties) override;

  virtual const char* Name() const override {
    return "InternalKeyPropertiesCollector";
  }

  UserCollectedProperties GetReadableProperties() const override;

 private:
  uint64_t deleted_keys_ = 0;
  uint64_t merge_operands_ = 0;
};

class InternalKeyPropertiesCollectorFactory
    : public IntTblPropCollectorFactory {
 public:
  virtual IntTblPropCollector* CreateIntTblPropCollector(
      uint32_t /*column_family_id*/) override {
    return new InternalKeyPropertiesCollector();
  }

  virtual const char* Name() const override {
    return "InternalKeyPropertiesCollectorFactory";
  }
};

// Write link or map info
// Used for repair. E.g missing manifest
class SstVarietyPropertiesCollector final : public IntTblPropCollector {
 public:
  SstVarietyPropertiesCollector(uint8_t _sst_variety,
                                std::vector<uint64_t>* _sst_depend,
                                size_t* _sst_read_amp)
      : sst_variety_(_sst_variety),
        sst_depend_(_sst_depend),
        sst_read_amp_(_sst_read_amp) {}

  virtual Status InternalAdd(const Slice& /*key*/, const Slice& /*value*/,
                             uint64_t /*file_size*/) override {
    return Status::OK();
  }

  virtual Status Finish(UserCollectedProperties* properties) override;

  virtual const char* Name() const override {
    return "SSTVarietyPropertiesCollector";
  }

  UserCollectedProperties GetReadableProperties() const override;

 private:
  uint8_t sst_variety_;
  std::vector<uint64_t>* sst_depend_;
  size_t* sst_read_amp_;
};

class SstPurposePropertiesCollectorFactory final
    : public IntTblPropCollectorFactory {
 public:
  SstPurposePropertiesCollectorFactory(uint8_t _sst_variety,
                                       std::vector<uint64_t>* _sst_depend,
                                       size_t* _sst_read_amp)
      : sst_variety_(_sst_variety),
        sst_depend_(_sst_depend),
        sst_read_amp_(_sst_read_amp) {}

  virtual IntTblPropCollector* CreateIntTblPropCollector(
      uint32_t /*column_family_id*/) override {
    return new SstVarietyPropertiesCollector(sst_variety_, sst_depend_,
                                             sst_read_amp_);
  }

  virtual const char* Name() const override {
    return "SstPurposePropertiesCollectorFactory";
  }

 private:
  uint8_t sst_variety_;
  std::vector<uint64_t>* sst_depend_;
  size_t* sst_read_amp_;
};

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
      uint32_t column_family_id) override {
    TablePropertiesCollectorFactory::Context context;
    context.column_family_id = column_family_id;
    return new UserKeyTablePropertiesCollector(
        user_collector_factory_->CreateTablePropertiesCollector(context));
  }

  virtual const char* Name() const override {
    return user_collector_factory_->Name();
  }

 private:
  std::shared_ptr<TablePropertiesCollectorFactory> user_collector_factory_;
};

}  // namespace rocksdb

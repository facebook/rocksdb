//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// This file defines a collection of statistics collectors.
#pragma once

#include <unordered_map>

#include "db/table_properties_collector.h"

namespace ROCKSDB_NAMESPACE {

class BlobTablePropertiesCollector : public IntTblPropCollector {
 public:
  Status InternalAdd(const Slice& key, const Slice& value,
                     uint64_t /* file_size */) override;

  void BlockAdd(uint64_t /* block_raw_bytes */,
                uint64_t /* block_compressed_bytes_fast */,
                uint64_t /* block_compressed_bytes_slow */) override {}

  Status Finish(UserCollectedProperties* properties) override;

  UserCollectedProperties GetReadableProperties() const override {
    return UserCollectedProperties();
  }

  const char* Name() const override { return "BlobTablePropertiesCollector"; }

 private:
  class BlobStats {
   public:
    void AddBlob(uint64_t bytes) {
      ++count_;
      bytes_ += bytes;
    }

    uint64_t GetCount() const { return count_; }
    uint64_t GetBytes() const { return bytes_; }

   private:
    uint64_t count_ = 0;
    uint64_t bytes_ = 0;
  };

  std::unordered_map<uint64_t, BlobStats> blob_stats_;
};

class BlobTablePropertiesCollectorFactory : public IntTblPropCollectorFactory {
 public:
  IntTblPropCollector* CreateIntTblPropCollector(
      uint32_t /* column_family_id */) override {
    return new BlobTablePropertiesCollector;
  }

  const char* Name() const override {
    return "BlobTablePropertiesCollectorFactory";
  }
};

}  // namespace ROCKSDB_NAMESPACE

//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
#pragma once

#include <unordered_map>

#include "db/blob/blob_stats.h"
#include "db/blob/blob_stats_collection.h"
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
  BlobStatsCollection<BlobStats> blob_stats_;
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

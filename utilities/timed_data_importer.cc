//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "rocksdb/utilities/timed_data_importer.h"

namespace ROCKSDB_NAMESPACE {

class TimedDataImporterImpl : public TimedDataImporter {
  Status TimedPut(const Slice& user_key, const Slice& value,
                  uint64_t write_unix_time) override {
    (void)user_key;
    (void)value;
    (void)write_unix_time;
    return Status::NotSupported();
  }
  Status TimedPut(const Slice& user_key, const Slice& timestamp,
                  const Slice& value, uint64_t write_unix_time) override {
    (void)user_key;
    (void)timestamp;
    (void)value;
    (void)write_unix_time;
    return Status::NotSupported();
  }

  Status Commit() override { return Status::NotSupported(); }

  Status Reset() override { return Status::NotSupported(); }
};

std::unique_ptr<TimedDataImporter> MakeTimedDataImporter(
    DB* target_db, ColumnFamilyHandle* target_cf) {
  (void)target_db;
  (void)target_cf;
  return std::make_unique<TimedDataImporterImpl>();
}

}  // namespace ROCKSDB_NAMESPACE

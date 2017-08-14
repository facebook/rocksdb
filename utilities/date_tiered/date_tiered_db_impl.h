//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once
#ifndef ROCKSDB_LITE

#include <map>
#include <string>
#include <vector>

#include "monitoring/instrumented_mutex.h"
#include "options/cf_options.h"
#include "rocksdb/db.h"
#include "rocksdb/utilities/date_tiered_db.h"

namespace rocksdb {

// Implementation of DateTieredDB.
class DateTieredDBImpl : public DateTieredDB {
 public:
  DateTieredDBImpl(DB* db, Options options,
                   const std::vector<ColumnFamilyDescriptor>& descriptors,
                   const std::vector<ColumnFamilyHandle*>& handles, int64_t ttl,
                   int64_t column_family_interval);

  virtual ~DateTieredDBImpl();

  Status Put(const WriteOptions& options, const Slice& key,
             const Slice& val) override;

  Status Get(const ReadOptions& options, const Slice& key,
             std::string* value) override;

  Status Delete(const WriteOptions& options, const Slice& key) override;

  bool KeyMayExist(const ReadOptions& options, const Slice& key,
                   std::string* value, bool* value_found = nullptr) override;

  Status Merge(const WriteOptions& options, const Slice& key,
               const Slice& value) override;

  Iterator* NewIterator(const ReadOptions& opts) override;

  Status DropObsoleteColumnFamilies() override;

  // Extract timestamp from key.
  static Status GetTimestamp(const Slice& key, int64_t* result);

 private:
  // Base database object
  DB* db_;

  const ColumnFamilyOptions cf_options_;

  const ImmutableCFOptions ioptions_;

  // Storing all column family handles for time series data.
  std::vector<ColumnFamilyHandle*> handles_;

  // Manages a mapping from a column family's maximum timestamp to its handle.
  std::map<int64_t, ColumnFamilyHandle*> handle_map_;

  // A time-to-live value to indicate when the data should be removed.
  int64_t ttl_;

  // An variable to indicate the time range of a column family.
  int64_t column_family_interval_;

  // Indicate largest maximum timestamp of a column family.
  int64_t latest_timebound_;

  // Mutex to protect handle_map_ operations.
  InstrumentedMutex mutex_;

  // Internal method to execute Put and Merge in batch.
  Status Write(const WriteOptions& opts, WriteBatch* updates);

  Status CreateColumnFamily(ColumnFamilyHandle** column_family);

  Status FindColumnFamily(int64_t keytime, ColumnFamilyHandle** column_family,
                          bool create_if_missing);

  static bool IsStale(int64_t keytime, int64_t ttl, Env* env);
};

}  // namespace rocksdb
#endif  // ROCKSDB_LITE

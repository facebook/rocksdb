//  Copyright (c) 2017-present, Rockset

#pragma once
#ifndef ROCKSDB_LITE

#include <string>
#include <vector>

#include "rocksdb/utilities/stackable_db.h"
#include "rocksdb/db.h"
#include "rocksdb/cloud/cloud_env_options.h"

namespace rocksdb {

//
// Database with Cloud support.
//
class DBCloud : public StackableDB {
 public:

  // This API is to open a DB when key-values are to be made durable by
  // backing up database state into a cloud-storage system like S3.
  // All kv updates are persisted in cloud-storage.
  // options.env is an object of type rocksdb::CloudEnv and the cloud
  // buckets are specified there.
  static Status Open(const Options& options,
		     const std::string& dbname,
                     const std::vector<ColumnFamilyDescriptor>& column_families,
                     std::vector<ColumnFamilyHandle*>* handles,
                     DBCloud** dbptr,
                     bool read_only = false);

  virtual ~DBCloud() {}

 protected:
  explicit DBCloud(DB* db) : StackableDB(db) {}
};

}  // namespace rocksdb
#endif  // ROCKSDB_LITE

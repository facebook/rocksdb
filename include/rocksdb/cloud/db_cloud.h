//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

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
  static Status Open(const Options& options,
		     const std::string& dbname,
                     const std::vector<ColumnFamilyDescriptor>& column_families,
                     std::vector<ColumnFamilyHandle*>* handles,
                     DBCloud** dbptr,
                     bool read_only = false);

  // This API is used to clone a DB from cloud storage.
  // SST files are downloaded from the cloud storage as and when needed.
  // All new kv updates are written to local files only, no newly created
  // sst files are uploaded to cloud storage.
  static Status OpenClone(
		     const Options& options, const std::string& dbname,
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

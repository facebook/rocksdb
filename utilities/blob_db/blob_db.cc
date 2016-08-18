// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "utilities/blob_db/blob_db.h"
#include "utilities/blob_db/blob_db_impl.h"

namespace rocksdb {

Status BlobDB::Open(const Options& options, const BlobDBOptions& bdb_options,
    const std::string& dbname, BlobDB** blob_db) {

  DB* db;
  Status s = DB::Open(options, dbname, &db);
  if (!s.ok()) {
    return s;
  }
  BlobDBImpl* bdb = new BlobDBImpl(db, bdb_options);
  s = bdb->Open();
  if (!s.ok()) {
    delete bdb;
  }
  *blob_db = bdb;
  return s;
}

BlobDB::BlobDB(DB* db)
    : StackableDB(db)
{
}

}  // namespace rocksdb

// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef ROCKSDB_LITE
#include "db/filename.h"
#include "db/write_batch_internal.h"
#include "rocksdb/convenience.h"
#include "rocksdb/env.h"
#include "rocksdb/iterator.h"
#include "rocksdb/utilities/stackable_db.h"
#include "table/block.h"
#include "table/block_based_table_builder.h"
#include "table/block_builder.h"
#include "util/cf_options.h"
#include "util/crc32c.h"
#include "util/file_reader_writer.h"
#include "util/instrumented_mutex.h"
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
    bdb = nullptr;
  }
  *blob_db = bdb;
  return s;
}

BlobDB::BlobDB(DB* db)
    : StackableDB(db)
{
}


BlobDBOptions::BlobDBOptions()
  : path_relative(true), has_ttl(false),
    is_fifo(false), blob_dir_size(0), ttl_range(3600),
    min_blob_size(512), bytes_per_sync(0),
    blob_file_size(256 * 1024 * 1024)
{
}

}  // namespace rocksdb
#endif

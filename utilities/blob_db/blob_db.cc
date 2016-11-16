// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef ROCKSDB_LITE
#include "utilities/blob_db/blob_db.h"
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
#include "utilities/blob_db/blob_db_impl.h"

namespace rocksdb {

Status BlobDB::Open(const Options& options, const BlobDBOptions& bdb_options,
    const std::string& dbname, BlobDB** blob_db) {
  Options myoptions(options);
  std::shared_ptr<BlobDBFlushBeginListener> fblistener =
    std::make_shared<BlobDBFlushBeginListener>();
  myoptions.listeners.emplace_back(fblistener);
  myoptions.flush_begin_listeners = true;

  DBOptions db_options(myoptions);
  EnvOptions env_options(db_options);

  // we need to open blob db first so that recovery can happen
  BlobDBImpl* bdb = new BlobDBImpl(dbname, bdb_options, myoptions,
    db_options, env_options);
  fblistener->setImplPtr(bdb);

  Status s = bdb->OpenP1();
  if (!s.ok()) {
    return s;
  }

  DB* db;
  s = DB::Open(myoptions, dbname, &db);
  if (!s.ok()) {
    return s;
  }

  // set the implementation pointer
  bdb->setDBImplPtr(db);

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
    : path_relative(true),
      is_fifo(false),
      blob_dir_size(0),
      ttl_range(3600),
      min_blob_size(512),
      bytes_per_sync(0),
      blob_file_size(256 * 1024 * 1024),
      num_concurrent_simple_blobs(4),
      deletion_check_period(2 * 1000),
      gc_file_pct(20),
      gc_check_period(60 * 1000),
      sanity_check_period(20 * 60 * 1000),
      open_files_trigger(100),
      wa_num_stats_periods(24),
      wa_stats_period(3600 * 1000),
      partial_expiration_gc_range(4 * 3600),
      partial_expiration_pct(75),
      fsync_files_period(10*1000) {}

}  // namespace rocksdb
#endif

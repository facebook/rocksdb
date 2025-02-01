// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <cstdio>
#include <string>
#include <iostream>

#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"

using ROCKSDB_NAMESPACE::DB;
using ROCKSDB_NAMESPACE::Options;
using ROCKSDB_NAMESPACE::PinnableSlice;
using ROCKSDB_NAMESPACE::ReadOptions;
using ROCKSDB_NAMESPACE::Status;
using ROCKSDB_NAMESPACE::WriteBatch;
using ROCKSDB_NAMESPACE::WriteOptions;

#if defined(OS_WIN)
std::string kDBPath = "C:\\Windows\\TEMP\\rocksdb_simple_example";
#else
std::string kDBPath = "/tmp/rocksdb_simple_example";
#endif


int main() {
  DB* db;
  Options options;
  // Set RocksDB option
  options.OptimizeLevelStyleCompaction();
  options.level_compaction_dynamic_level_bytes = false;
  // create the DB if it's not already present
  options.create_if_missing = true;

  // open DB
  Status s = DB::Open(options, kDBPath, &db);
  assert(s.ok());

  // Put key-value
  s = db->Put(rocksdb::WriteOptions(), "Key 0", "value 0000");
  s = db->Put(rocksdb::WriteOptions(), "Key 1", "value 0001");

  // Get key-value
  std::string opt;

  s = db->Get(rocksdb::ReadOptions(), "Key 0", &opt);
  std::cout << "Key 0: " << opt << std::endl;
  s = db->Get(rocksdb::ReadOptions(), "Key 1", &opt);
  std::cout << "Key 1: " << opt << std::endl;

  // close DB
  db->Close();
  delete db;

  return 0;
}

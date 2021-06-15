// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <cstdio>
#include <string>

#include "rocksdb/db.h"
#include "rocksdb/slice.h"
#include "rocksdb/options.h"

using namespace ROCKSDB_NAMESPACE;

#if defined(OS_WIN)
std::string kDBPath = "C:\\Windows\\TEMP\\rocksdb_simple_example";
#else
std::string kDBPath = "/tmp/rocksdb_simple_example";
#endif

int main() {
  DB* db;
  Options options;
  options.statistics = rocksdb::CreateDBStatistics();
  // Record all statistics.
  options.statistics->set_stats_level(StatsLevel::kAll);
  // Optimize RocksDB. This is the easiest way to get RocksDB to perform well
  // options.IncreaseParallelism();
  // options.OptimizeLevelStyleCompaction();
  // create the DB if it's not already present
  options.compression = rocksdb::kNoCompression;
  options.create_if_missing = true;

  // open DB
  Status s = DB::Open(options, kDBPath, &db);
  assert(s.ok());

  // Put key-value
  for(size_t i=0; i<2560000; i++){
    s = db->Put(WriteOptions(), "key1", "value1");
  }
  // s = db->Put(WriteOptions(), "key1", "value2");
  // s = db->Put(WriteOptions(), "key1", "value3");
  // s = db->Put(WriteOptions(), "key1", "value4");
  // s = db->Put(WriteOptions(), "key1", "value5");
  // s = db->Put(WriteOptions(), "key1", "value6");
  // s = db->Put(WriteOptions(), "key1", "value7");
  // s = db->Put(WriteOptions(), "key1", "value8");

  assert(s.ok());
  std::string value;
  // get value
  s = db->Get(ReadOptions(), "key1", &value);
  assert(s.ok());
  // assert(value == "value8");
  uint64_t bytes_written = options.statistics->getTickerCount(BYTES_WRITTEN);
  uint64_t flush_write_bytes = options.statistics->getTickerCount(FLUSH_WRITE_BYTES);
  uint64_t mgb = options.statistics->getTickerCount(MEMTABLE_GARBAGE_BYTES);
  if (db != nullptr) {
    FlushOptions flush_opt;
    flush_opt.wait = true; // function returns once the flush is over.
    s = db->Flush(flush_opt);
  }
  std::string flushed, unflushed;
  if (!db->GetProperty(ROCKSDB_NAMESPACE::DB::Properties::kCurSizeActiveMemTable, &unflushed)) {
    unflushed = "(failed)";
  }
  if (!db->GetProperty(ROCKSDB_NAMESPACE::DB::Properties::kNumImmutableMemTableFlushed, &flushed)) {
    flushed = "(failed)";
  }

  printf("Memtables yet to be flushed: %s, already flushed: %s\n", unflushed.c_str(), flushed.c_str());
  printf("Bytes written: %zu\nFLush write bytes: %zu\nMemtable garbage bytes: %zu.\n", bytes_written, flush_write_bytes, mgb);
  std::string allstats;
  if (!db->GetProperty(ROCKSDB_NAMESPACE::DB::Properties::kStats, &allstats)) {
    allstats = "(failed)";
  }
  // printf("%s\n",allstats.c_str());

  // // atomically apply a set of updates
  // {
  //   WriteBatch batch;
  //   batch.Delete("key1");
  //   batch.Put("key2", value);
  //   s = db->Write(WriteOptions(), &batch);
  // }

  // s = db->Get(ReadOptions(), "key1", &value);
  // assert(s.IsNotFound());

  // db->Get(ReadOptions(), "key2", &value);
  // assert(value == "value");

  // {
  //   PinnableSlice pinnable_val;
  //   db->Get(ReadOptions(), db->DefaultColumnFamily(), "key2", &pinnable_val);
  //   assert(pinnable_val == "value");
  // }

  // {
  //   std::string string_val;
  //   // If it cannot pin the value, it copies the value to its internal buffer.
  //   // The intenral buffer could be set during construction.
  //   PinnableSlice pinnable_val(&string_val);
  //   db->Get(ReadOptions(), db->DefaultColumnFamily(), "key2", &pinnable_val);
  //   assert(pinnable_val == "value");
  //   // If the value is not pinned, the internal buffer must have the value.
  //   assert(pinnable_val.IsPinned() || string_val == "value");
  // }

  // PinnableSlice pinnable_val;
  // s = db->Get(ReadOptions(), db->DefaultColumnFamily(), "key1", &pinnable_val);
  // assert(s.IsNotFound());
  // // Reset PinnableSlice after each use and before each reuse
  // pinnable_val.Reset();
  // db->Get(ReadOptions(), db->DefaultColumnFamily(), "key2", &pinnable_val);
  // assert(pinnable_val == "value");
  // pinnable_val.Reset();
  // The Slice pointed by pinnable_val is not valid after this point

  delete db;

  return 0;
}

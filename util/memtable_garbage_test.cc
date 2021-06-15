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
// std::string kDBPath = "/tmp/rocksdb_memtable_garbage_test";
std::string kDBPath = "./rocksdb_memtable_garbage_test";
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
  options.create_if_missing = true;

  // Useful for now as we are trying to compare uncompressed data savings on flush().
  options.compression = rocksdb::kNoCompression;


  // Prevent memtable in place updates. Should already be disabled
  // (from Wiki:
  //  In place updates can be enabled by toggling on the bool inplace_update_support
  //  flag. However, this flag is by default set to false because this thread-safe
  //  in-place update support is not compatible with concurrent memtable writes.
  //  Note that the bool allow_concurrent_memtable_write is set to true by default )
  options.inplace_update_support=false;
  options.allow_concurrent_memtable_write = true;

  // Enforce size of a single MemTable to 64MB. (64<<6)
  options.write_buffer_size = 67108864;

  // ======= General Information ======= (from GitHub Wiki).
  // There are three scenarios where memtable flush can be triggered:
  //
  // 1 - Memtable size exceeds ColumnFamilyOptions::write_buffer_size
  //     after a write.
  // 2 - Total memtable size across all column families exceeds DBOptions::db_write_buffer_size,
  //     or DBOptions::write_buffer_manager signals a flush. In this scenario the largest
  //     memtable will be flushed.
  // 3 - Total WAL file size exceeds DBOptions::max_total_wal_size.
  //     In this scenario the memtable with the oldest data will be flushed,
  //     in order to allow the WAL file with data from this memtable to be purged.
  //
  // As a result, a memtable can be flushed before it is full. This is one reason
  // the generated SST file can be smaller than the corresponding memtable.
  // Compression is another factor to make SST file smaller than corresponding memtable,
  // since data in memtable is uncompressed.

  // open DB
  Status s = DB::Open(options, kDBPath, &db);
  assert(s.ok());

  // Tests
  // Put key-value
  // for(size_t i=0; i<2; i++){
  //   std::string key = "key" + std::to_string(i);
  //   s = db->Put(WriteOptions(), key.c_str(), "value1");
  // }

  // Put multiple times the same key-values.
  // The raw data size in the memtable is defined
  // in db/memtable.cc (MemTable::Add) as the variable:
  // encoded_len=  VarintLength(internal_key_size)  --> = log_256(internal_key). Min # of bytes
  //                                                       necessary to store internal_key_size.
  //             + internal_key_size                --> = actual key string, (size key_size: w/o term null char)
  //                                                      + 8 bytes for fixed uint64 "seq number + insertion type"
  //             + VarintLength(val_size)           --> = min # of bytes to store val_size
  //             + val_size                         --> = actual value string
  // For us, "key1" = size 4, "value1" = size 6
  // And therefore encoded_len = 1 + 12 + 1 + 6 = 20 bytes per entry.
  // ===> 2,560,000 * 20 = 51,200,000 raw bytes. (compare this is memtable_garbage_bytes)
  // Additional info leads us to 25 bytes per entry.
  size_t NUM_ENTRIES = 2560000;
  for(size_t i=0; i<NUM_ENTRIES; i++){
    s = db->Put(WriteOptions(), "key1", "value1");
    assert(s.ok());
  }

  std::string value;
  s = db->Get(ReadOptions(), "key1", &value);
  assert(s.ok());
  assert(value == "value1");

  // Force any remaining flush.
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
  uint64_t bytes_written = options.statistics->getTickerCount(BYTES_WRITTEN);
  uint64_t flush_write_bytes = options.statistics->getTickerCount(FLUSH_WRITE_BYTES);
  uint64_t mdb = options.statistics->getTickerCount(MEMTABLE_DATA_BYTES);
  uint64_t mgb = options.statistics->getTickerCount(MEMTABLE_GARBAGE_BYTES);

  printf("Memtables yet to be flushed: %s, already flushed: %s\n", unflushed.c_str(), flushed.c_str());
  printf("Bytes written: %zu\n",bytes_written);
  printf("Flush write bytes: %zu\n",flush_write_bytes);
  printf("Memtable data bytes: %zu.\n",mdb);
  printf("Memtable garbage bytes: %zu.\n",mgb);

  // // atomically apply a set of updates
  // {
  //   WriteBatch batch;
  //   batch.Delete("key1");
  //   batch.Put("key2", value);
  //   s = db->Write(WriteOptions(), &batch);
  // }

  // s = db->Get(ReadOptions(), "key1", &value);
  // assert(s.IsNotFound());
  delete db;

  return 0;
}

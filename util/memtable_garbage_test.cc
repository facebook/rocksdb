// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <cstdio>
#include <string>

#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "test_util/testharness.h"

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
  options.IncreaseParallelism();
  options.OptimizeLevelStyleCompaction();

  // create the DB if it's not already present
  options.create_if_missing = true;

  // Useful for now as we are trying to compare uncompressed data savings on
  // flush().
  options.compression = rocksdb::kNoCompression;

  // Prevent memtable in place updates. Should already be disabled
  // (from Wiki:
  //  In place updates can be enabled by toggling on the bool
  //  inplace_update_support flag. However, this flag is by default set to false
  //  because this thread-safe in-place update support is not compatible with
  //  concurrent memtable writes. Note that the bool
  //  allow_concurrent_memtable_write is set to true by default )
  options.inplace_update_support = false;
  options.allow_concurrent_memtable_write = true;

  // Enforce size of a single MemTable to 64MB. (64<<6)
  options.write_buffer_size = 67108864;

  // ======= General Information ======= (from GitHub Wiki).
  // There are three scenarios where memtable flush can be triggered:
  //
  // 1 - Memtable size exceeds ColumnFamilyOptions::write_buffer_size
  //     after a write.
  // 2 - Total memtable size across all column families exceeds
  // DBOptions::db_write_buffer_size,
  //     or DBOptions::write_buffer_manager signals a flush. In this scenario
  //     the largest memtable will be flushed.
  // 3 - Total WAL file size exceeds DBOptions::max_total_wal_size.
  //     In this scenario the memtable with the oldest data will be flushed,
  //     in order to allow the WAL file with data from this memtable to be
  //     purged.
  //
  // As a result, a memtable can be flushed before it is full. This is one
  // reason the generated SST file can be smaller than the corresponding
  // memtable. Compression is another factor to make SST file smaller than
  // corresponding memtable, since data in memtable is uncompressed.

  // open DB
  Status s = DB::Open(options, kDBPath, &db);
  assert(s.ok());

  // Put multiple times the same key-values.
  // The encoded length of a db entry in the memtable is
  // defined in db/memtable.cc (MemTable::Add) as the variable:
  // encoded_len=  VarintLength(internal_key_size)  --> = log_256(internal_key).
  // Min # of bytes
  //                                                       necessary to store
  //                                                       internal_key_size.
  //             + internal_key_size                --> = actual key string,
  //             (size key_size: w/o term null char)
  //                                                      + 8 bytes for fixed
  //                                                      uint64 "seq number +
  //                                                      insertion type"
  //             + VarintLength(val_size)           --> = min # of bytes to
  //             store val_size
  //             + val_size                         --> = actual value string
  // For example, in our situation, "key1" : size 4, "value1" : size 6
  // (the terminating null characters are not copied over to the memtable).
  // And therefore encoded_len = 1 + (4+8) + 1 + 6 = 20 bytes per entry.
  // However in terms of raw data contained in the memtable, and written
  // over to the SSTable, we only count internal_key_size and val_size,
  // because this is the only raw chunk of bytes that contains everything
  // necessary to reconstruct a user entry: sequence number, insertion type,
  // key, and value.

  // To test the relevance of our Memtable garbage statistics,
  // namely MEMTABLE_DATA_BYTES and MEMTABLE_GARBAGE_BYTES,
  // we insert 3 distinct K-V pairs NUM_REPEAT times.
  // I chose NUM_REPEAT=20,000 such that no automatic flush is
  // triggered (the number of bytes in the memtable is therefore
  // well below any meaningful heuristic for a memtable of size 64MB).
  // As a result, since each K-V pair is inserted as a payload
  // of 18 meaningful bytes (sequence number, insertion type,
  // key, and value = (8) + 4 + 6 = 18), MEMTABLE_DATA_BYTES
  // should be equal to 20,000 * 3 * 18 = 1,080,000 bytes
  // and MEMTABLE_GARBAGE_BYTES = MEMTABLE_DATA_BYTES - (3*18)
  //                            = 1,079,946 bytes.

  const size_t NUM_REPEAT = 20000;
  const char KEY1[] = "key1";
  const char KEY2[] = "key2";
  const char KEY3[] = "key3";
  const char VALUE1[] = "value1";
  const char VALUE2[] = "value2";
  const char VALUE3[] = "value3";
  const uint64_t USEFUL_PAYLOAD_BYTES =
      strlen(KEY1) + strlen(VALUE1) + strlen(KEY2) + strlen(VALUE2) +
      strlen(KEY3) + strlen(VALUE3) + 3 * sizeof(uint64_t);
  const uint64_t EXPECTED_MEMTABLE_DATA_BYTES =
      NUM_REPEAT * USEFUL_PAYLOAD_BYTES;
  const uint64_t EXPECTED_MEMTABLE_GARBAGE_BYTES =
      (NUM_REPEAT - 1) * USEFUL_PAYLOAD_BYTES;

  // Insertion of of K-V pairs, multiple times.
  for (size_t i = 0; i < NUM_REPEAT; i++) {
    s = db->Put(WriteOptions(), KEY1, VALUE1);
    assert(s.ok());
    s = db->Put(WriteOptions(), KEY2, VALUE2);
    assert(s.ok());
    s = db->Put(WriteOptions(), KEY3, VALUE3);
    assert(s.ok());
  }

  // We assert that the K-V pairs have been successfully inserted.
  std::string value;
  s = db->Get(ReadOptions(), KEY1, &value);
  assert(s.ok());
  assert(value == VALUE1);
  s = db->Get(ReadOptions(), KEY2, &value);
  assert(s.ok());
  assert(value == VALUE2);
  s = db->Get(ReadOptions(), KEY3, &value);
  assert(s.ok());
  assert(value == VALUE3);

  // Force flush to SST. Increments the statistics counter.
  if (db != nullptr) {
    FlushOptions flush_opt;
    flush_opt.wait = true;  // function returns once the flush is over.
    s = db->Flush(flush_opt);
    assert(s.ok());
  }

  // Collect statistics.
  uint64_t mem_data_bytes =
      options.statistics->getTickerCount(MEMTABLE_DATA_BYTES);
  uint64_t mem_garbage_bytes =
      options.statistics->getTickerCount(MEMTABLE_GARBAGE_BYTES);

  EXPECT_EQ(mem_data_bytes, EXPECTED_MEMTABLE_DATA_BYTES);
  EXPECT_EQ(mem_garbage_bytes, EXPECTED_MEMTABLE_GARBAGE_BYTES);

  delete db;

  return 0;
}

//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <thread>
#include <vector>

#include "db/db_test_util.h"
#include "port/stack_trace.h"
#include "rocksdb/statistics.h"
#include "test_util/testharness.h"

namespace ROCKSDB_NAMESPACE {

class DBBlobDirectWriteTest : public DBTestBase {
 public:
  explicit DBBlobDirectWriteTest()
      : DBTestBase("db_blob_direct_write_test", /*env_do_fsync=*/true) {}

 protected:
  // Common helper to create blob direct write options with sensible defaults.
  Options GetBlobDirectWriteOptions() {
    Options options = CurrentOptions();
    options.enable_blob_files = true;
    options.min_blob_size = 10;
    options.enable_blob_direct_write = true;
    options.blob_direct_write_partitions = 2;
    options.blob_file_size = 1024 * 1024;  // 1MB
    return options;
  }

  // Write num_keys key-value pairs where values exceed min_blob_size.
  void WriteLargeValues(int num_keys, int value_size = 100,
                        const std::string& key_prefix = "key") {
    for (int i = 0; i < num_keys; i++) {
      std::string key = key_prefix + std::to_string(i);
      std::string value(value_size + i, 'a' + (i % 26));
      ASSERT_OK(Put(key, value));
    }
  }

  // Verify num_keys key-value pairs written by WriteLargeValues.
  void VerifyLargeValues(int num_keys, int value_size = 100,
                         const std::string& key_prefix = "key") {
    for (int i = 0; i < num_keys; i++) {
      std::string key = key_prefix + std::to_string(i);
      std::string expected(value_size + i, 'a' + (i % 26));
      ASSERT_EQ(Get(key), expected);
    }
  }
};

TEST_F(DBBlobDirectWriteTest, BasicPutGet) {
  Options options = GetBlobDirectWriteOptions();
  DestroyAndReopen(options);

  // Write a value that should go to blob file (>= min_blob_size)
  std::string large_value(100, 'x');
  ASSERT_OK(Put("key1", large_value));

  // Write a value that should stay inline (< min_blob_size)
  std::string small_value("tiny");
  ASSERT_OK(Put("key2", small_value));

  // Read back both values
  ASSERT_EQ(Get("key1"), large_value);
  ASSERT_EQ(Get("key2"), small_value);
}

TEST_F(DBBlobDirectWriteTest, MultipleWrites) {
  Options options = GetBlobDirectWriteOptions();
  options.blob_direct_write_partitions = 4;
  DestroyAndReopen(options);

  const int num_keys = 100;
  WriteLargeValues(num_keys);
  VerifyLargeValues(num_keys);
}

TEST_F(DBBlobDirectWriteTest, FlushAndRead) {
  Options options = GetBlobDirectWriteOptions();
  DestroyAndReopen(options);

  std::string large_value(200, 'v');
  ASSERT_OK(Put("key1", large_value));
  ASSERT_OK(Put("key2", large_value));

  ASSERT_OK(Flush());

  ASSERT_EQ(Get("key1"), large_value);
  ASSERT_EQ(Get("key2"), large_value);
}

TEST_F(DBBlobDirectWriteTest, DeleteAndRead) {
  Options options = GetBlobDirectWriteOptions();
  options.blob_direct_write_partitions = 1;
  DestroyAndReopen(options);

  std::string large_value(100, 'z');
  ASSERT_OK(Put("key1", large_value));
  ASSERT_EQ(Get("key1"), large_value);

  ASSERT_OK(Delete("key1"));
  ASSERT_EQ(Get("key1"), "NOT_FOUND");
}

TEST_F(DBBlobDirectWriteTest, MixedBlobAndInlineValues) {
  Options options = GetBlobDirectWriteOptions();
  options.min_blob_size = 50;
  DestroyAndReopen(options);

  std::string small(10, 's');
  std::string large(100, 'l');
  ASSERT_OK(Put("small1", small));
  ASSERT_OK(Put("large1", large));
  ASSERT_OK(Put("small2", small));
  ASSERT_OK(Put("large2", large));

  ASSERT_EQ(Get("small1"), small);
  ASSERT_EQ(Get("large1"), large);
  ASSERT_EQ(Get("small2"), small);
  ASSERT_EQ(Get("large2"), large);

  ASSERT_OK(Flush());
  ASSERT_EQ(Get("small1"), small);
  ASSERT_EQ(Get("large1"), large);
  ASSERT_EQ(Get("small2"), small);
  ASSERT_EQ(Get("large2"), large);
}

TEST_F(DBBlobDirectWriteTest, WALRecovery) {
  Options options = GetBlobDirectWriteOptions();
  DestroyAndReopen(options);

  std::string large_value(100, 'r');
  ASSERT_OK(Put("recovery_key1", large_value));
  ASSERT_OK(Put("recovery_key2", large_value));

  // Flush before reopen to seal blob files, then verify data survives reopen
  ASSERT_OK(Flush());
  Reopen(options);

  ASSERT_EQ(Get("recovery_key1"), large_value);
  ASSERT_EQ(Get("recovery_key2"), large_value);
}

TEST_F(DBBlobDirectWriteTest, IteratorForwardScan) {
  Options options = GetBlobDirectWriteOptions();
  options.min_blob_size = 20;
  DestroyAndReopen(options);

  // Write interleaved small and large values in sorted key order
  ASSERT_OK(Put("a_small", "tiny"));
  ASSERT_OK(Put("b_large", std::string(50, 'B')));
  ASSERT_OK(Put("c_small", "mini"));
  ASSERT_OK(Put("d_large", std::string(50, 'D')));

  // Verify forward scan before flush (memtable iteration)
  auto verify_forward_scan = [&]() {
    std::unique_ptr<Iterator> iter(db_->NewIterator(ReadOptions()));
    iter->SeekToFirst();
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key(), "a_small");
    ASSERT_EQ(iter->value(), "tiny");
    iter->Next();
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key(), "b_large");
    ASSERT_EQ(iter->value(), std::string(50, 'B'));
    iter->Next();
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key(), "c_small");
    ASSERT_EQ(iter->value(), "mini");
    iter->Next();
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key(), "d_large");
    ASSERT_EQ(iter->value(), std::string(50, 'D'));
    iter->Next();
    ASSERT_FALSE(iter->Valid());
    ASSERT_OK(iter->status());
  };

  verify_forward_scan();

  // Verify forward scan after flush (SST + blob file iteration)
  ASSERT_OK(Flush());
  verify_forward_scan();
}

TEST_F(DBBlobDirectWriteTest, IteratorReverseScan) {
  Options options = GetBlobDirectWriteOptions();
  options.min_blob_size = 20;
  DestroyAndReopen(options);

  ASSERT_OK(Put("a_small", "tiny"));
  ASSERT_OK(Put("b_large", std::string(50, 'B')));
  ASSERT_OK(Put("c_small", "mini"));
  ASSERT_OK(Put("d_large", std::string(50, 'D')));

  auto verify_reverse_scan = [&]() {
    std::unique_ptr<Iterator> iter(db_->NewIterator(ReadOptions()));
    iter->SeekToLast();
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key(), "d_large");
    ASSERT_EQ(iter->value(), std::string(50, 'D'));
    iter->Prev();
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key(), "c_small");
    ASSERT_EQ(iter->value(), "mini");
    iter->Prev();
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key(), "b_large");
    ASSERT_EQ(iter->value(), std::string(50, 'B'));
    iter->Prev();
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key(), "a_small");
    ASSERT_EQ(iter->value(), "tiny");
    iter->Prev();
    ASSERT_FALSE(iter->Valid());
    ASSERT_OK(iter->status());
  };

  verify_reverse_scan();

  ASSERT_OK(Flush());
  verify_reverse_scan();
}

TEST_F(DBBlobDirectWriteTest, MultiGetWithBlobDirectWrite) {
  Options options = GetBlobDirectWriteOptions();
  DestroyAndReopen(options);

  std::string large1(100, 'A'), large2(100, 'B'), large3(100, 'C');
  ASSERT_OK(Put("key1", large1));
  ASSERT_OK(Put("key2", large2));
  ASSERT_OK(Put("key3", large3));

  // Flush first so MultiGet reads from SST + blob files
  ASSERT_OK(Flush());

  std::vector<Slice> keys = {Slice("key1"), Slice("key2"), Slice("key3"),
                             Slice("missing")};
  std::vector<std::string> values(4);
  std::vector<Status> statuses =
      dbfull()->MultiGet(ReadOptions(), keys, &values);
  ASSERT_OK(statuses[0]);
  ASSERT_EQ(values[0], large1);
  ASSERT_OK(statuses[1]);
  ASSERT_EQ(values[1], large2);
  ASSERT_OK(statuses[2]);
  ASSERT_EQ(values[2], large3);
  ASSERT_TRUE(statuses[3].IsNotFound());
}

TEST_F(DBBlobDirectWriteTest, MultiGetFromMemtable) {
  Options options = GetBlobDirectWriteOptions();
  DestroyAndReopen(options);

  std::string large1(100, 'X'), large2(100, 'Y'), large3(100, 'Z');
  ASSERT_OK(Put("mkey1", large1));
  ASSERT_OK(Put("mkey2", large2));
  ASSERT_OK(Put("mkey3", large3));

  // Read from memtable without flushing.
  std::vector<Slice> keys = {Slice("mkey1"), Slice("mkey2"), Slice("mkey3"),
                             Slice("missing")};
  std::vector<std::string> values(4);
  std::vector<Status> statuses =
      dbfull()->MultiGet(ReadOptions(), keys, &values);
  ASSERT_OK(statuses[0]);
  ASSERT_EQ(values[0], large1);
  ASSERT_OK(statuses[1]);
  ASSERT_EQ(values[1], large2);
  ASSERT_OK(statuses[2]);
  ASSERT_EQ(values[2], large3);
  ASSERT_TRUE(statuses[3].IsNotFound());
}

TEST_F(DBBlobDirectWriteTest, FlushAndCompaction) {
  Options options = GetBlobDirectWriteOptions();
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);

  // Write and flush multiple times to create multiple SST files
  for (int batch = 0; batch < 3; batch++) {
    WriteLargeValues(10, 100, "batch" + std::to_string(batch) + "_key");
    ASSERT_OK(Flush());
  }

  // Compact all data
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));

  // Verify all data survives compaction
  for (int batch = 0; batch < 3; batch++) {
    VerifyLargeValues(10, 100, "batch" + std::to_string(batch) + "_key");
  }
}

TEST_F(DBBlobDirectWriteTest, DBReopen) {
  Options options = GetBlobDirectWriteOptions();
  DestroyAndReopen(options);

  std::string large_value(200, 'R');
  ASSERT_OK(Put("reopen_key1", large_value));
  ASSERT_OK(Put("reopen_key2", large_value));

  // Flush to create sealed blob files, then close and reopen
  ASSERT_OK(Flush());
  Reopen(options);

  ASSERT_EQ(Get("reopen_key1"), large_value);
  ASSERT_EQ(Get("reopen_key2"), large_value);
}

TEST_F(DBBlobDirectWriteTest, SnapshotIsolation) {
  Options options = GetBlobDirectWriteOptions();
  DestroyAndReopen(options);

  std::string value_v1(100, '1');
  ASSERT_OK(Put("snap_key", value_v1));

  // Take a snapshot
  const Snapshot* snap = db_->GetSnapshot();

  // Write a new value after the snapshot
  std::string value_v2(100, '2');
  ASSERT_OK(Put("snap_key", value_v2));
  ASSERT_OK(Put("snap_new_key", value_v2));

  // Current read should see v2
  ASSERT_EQ(Get("snap_key"), value_v2);
  ASSERT_EQ(Get("snap_new_key"), value_v2);

  // Snapshot read should see v1 and not see snap_new_key
  ReadOptions read_opts;
  read_opts.snapshot = snap;
  std::string result;
  ASSERT_OK(
      db_->Get(read_opts, db_->DefaultColumnFamily(), "snap_key", &result));
  ASSERT_EQ(result, value_v1);
  Status s =
      db_->Get(read_opts, db_->DefaultColumnFamily(), "snap_new_key", &result);
  ASSERT_TRUE(s.IsNotFound());

  db_->ReleaseSnapshot(snap);
}

TEST_F(DBBlobDirectWriteTest, BlobFileRotation) {
  Options options = GetBlobDirectWriteOptions();
  // Small blob file size to force rotation
  options.blob_file_size = 512;
  options.blob_direct_write_partitions = 1;
  DestroyAndReopen(options);

  // Write enough data to exceed blob_file_size and trigger rotation
  const int num_keys = 20;
  for (int i = 0; i < num_keys; i++) {
    std::string key = "rot_key" + std::to_string(i);
    std::string value(100, 'a' + (i % 26));
    ASSERT_OK(Put(key, value));
  }

  // Verify all data is readable after rotations
  for (int i = 0; i < num_keys; i++) {
    std::string key = "rot_key" + std::to_string(i);
    std::string expected(100, 'a' + (i % 26));
    ASSERT_EQ(Get(key), expected);
  }

  // Also verify after flush
  ASSERT_OK(Flush());
  for (int i = 0; i < num_keys; i++) {
    std::string key = "rot_key" + std::to_string(i);
    std::string expected(100, 'a' + (i % 26));
    ASSERT_EQ(Get(key), expected);
  }
}

TEST_F(DBBlobDirectWriteTest, BoundaryValues) {
  Options options = GetBlobDirectWriteOptions();
  options.min_blob_size = 20;
  DestroyAndReopen(options);

  // One byte below threshold - should stay inline
  std::string below(19, 'b');
  // Exactly at threshold - should go to blob
  std::string exact(20, 'e');
  // One byte above threshold - should go to blob
  std::string above(21, 'a');

  ASSERT_OK(Put("below", below));
  ASSERT_OK(Put("exact", exact));
  ASSERT_OK(Put("above", above));

  // Verify before flush
  ASSERT_EQ(Get("below"), below);
  ASSERT_EQ(Get("exact"), exact);
  ASSERT_EQ(Get("above"), above);

  // Verify after flush
  ASSERT_OK(Flush());
  ASSERT_EQ(Get("below"), below);
  ASSERT_EQ(Get("exact"), exact);
  ASSERT_EQ(Get("above"), above);
}

TEST_F(DBBlobDirectWriteTest, OverwriteWithBlobValue) {
  Options options = GetBlobDirectWriteOptions();
  DestroyAndReopen(options);

  std::string value_v1(100, '1');
  std::string value_v2(150, '2');

  ASSERT_OK(Put("overwrite_key", value_v1));
  ASSERT_EQ(Get("overwrite_key"), value_v1);

  // Overwrite with a different large value
  ASSERT_OK(Put("overwrite_key", value_v2));
  ASSERT_EQ(Get("overwrite_key"), value_v2);

  // Verify after flush
  ASSERT_OK(Flush());
  ASSERT_EQ(Get("overwrite_key"), value_v2);

  // Overwrite again after flush
  std::string value_v3(200, '3');
  ASSERT_OK(Put("overwrite_key", value_v3));
  ASSERT_EQ(Get("overwrite_key"), value_v3);
}

TEST_F(DBBlobDirectWriteTest, Statistics) {
  Options options = GetBlobDirectWriteOptions();
  options.statistics = CreateDBStatistics();
  DestroyAndReopen(options);

  uint64_t count_before =
      options.statistics->getTickerCount(BLOB_DB_DIRECT_WRITE_COUNT);
  uint64_t bytes_before =
      options.statistics->getTickerCount(BLOB_DB_DIRECT_WRITE_BYTES);

  // Write values that exceed min_blob_size
  std::string large_value(100, 'S');
  const int num_writes = 5;
  for (int i = 0; i < num_writes; i++) {
    ASSERT_OK(Put("stat_key" + std::to_string(i), large_value));
  }

  uint64_t count_after =
      options.statistics->getTickerCount(BLOB_DB_DIRECT_WRITE_COUNT);
  uint64_t bytes_after =
      options.statistics->getTickerCount(BLOB_DB_DIRECT_WRITE_BYTES);

  // Each large write should increment the count
  ASSERT_EQ(count_after - count_before, num_writes);
  // Total bytes should account for all blob values written
  ASSERT_EQ(bytes_after - bytes_before, num_writes * large_value.size());

  // Small values should NOT increment blob direct write stats
  uint64_t count_mid = count_after;
  ASSERT_OK(Put("small_stat_key", "tiny"));
  uint64_t count_final =
      options.statistics->getTickerCount(BLOB_DB_DIRECT_WRITE_COUNT);
  ASSERT_EQ(count_final, count_mid);
}

TEST_F(DBBlobDirectWriteTest, ConcurrentWriters) {
  Options options = GetBlobDirectWriteOptions();
  options.blob_direct_write_partitions = 4;
  DestroyAndReopen(options);

  const int num_threads = 4;
  const int keys_per_thread = 50;
  std::vector<std::thread> threads;

  for (int t = 0; t < num_threads; t++) {
    threads.emplace_back([&, t]() {
      for (int i = 0; i < keys_per_thread; i++) {
        std::string key =
            "thread" + std::to_string(t) + "_key" + std::to_string(i);
        std::string value(100, 'a' + (t % 26));
        ASSERT_OK(Put(key, value));
      }
    });
  }

  for (auto& t : threads) {
    t.join();
  }

  // Verify all data from all threads
  for (int t = 0; t < num_threads; t++) {
    for (int i = 0; i < keys_per_thread; i++) {
      std::string key =
          "thread" + std::to_string(t) + "_key" + std::to_string(i);
      std::string expected(100, 'a' + (t % 26));
      ASSERT_EQ(Get(key), expected);
    }
  }
}

// High-concurrency test that exercises the backpressure path.
// Uses many threads with a small buffer to force pending_bytes to exceed
// buffer_size frequently, triggering SubmitFlush + timed wait.
// This catches deadlocks where:
// - SubmitFlush is only called once before the wait loop
// - Signals are sent without holding the mutex (missed wakeups)
TEST_F(DBBlobDirectWriteTest, BackpressureHighConcurrency) {
  Options options = GetBlobDirectWriteOptions();
  options.blob_direct_write_partitions = 4;
  // Small buffer (64KB) with 4KB values = ~16 records before backpressure.
  // This forces frequent backpressure stalls with many concurrent writers.
  options.blob_direct_write_buffer_size = 65536;
  options.blob_file_size = 1024 * 1024;
  DestroyAndReopen(options);

  const int num_threads = 16;
  const int keys_per_thread = 500;
  const int value_size = 4096;
  std::vector<std::thread> threads;

  for (int t = 0; t < num_threads; t++) {
    threads.emplace_back([&, t]() {
      for (int i = 0; i < keys_per_thread; i++) {
        std::string key = "bp_t" + std::to_string(t) + "_k" + std::to_string(i);
        std::string value(value_size, 'a' + (t % 26));
        ASSERT_OK(Put(key, value));
      }
    });
  }

  for (auto& t : threads) {
    t.join();
  }

  // Verify a sample of keys from each thread
  for (int t = 0; t < num_threads; t++) {
    for (int i = 0; i < keys_per_thread; i += 50) {
      std::string key = "bp_t" + std::to_string(t) + "_k" + std::to_string(i);
      std::string expected(value_size, 'a' + (t % 26));
      ASSERT_EQ(Get(key), expected);
    }
  }

  // Verify after flush
  ASSERT_OK(Flush());
  for (int t = 0; t < num_threads; t++) {
    std::string key = "bp_t" + std::to_string(t) + "_k0";
    std::string expected(value_size, 'a' + (t % 26));
    ASSERT_EQ(Get(key), expected);
  }
}

TEST_F(DBBlobDirectWriteTest, OptionsValidation) {
  // enable_blob_direct_write=true with enable_blob_files=false should
  // be silently corrected by option sanitization
  Options options = CurrentOptions();
  options.enable_blob_files = false;
  options.enable_blob_direct_write = true;
  DestroyAndReopen(options);

  // Write should succeed (direct write is disabled, values stay inline)
  std::string large_value(100, 'V');
  ASSERT_OK(Put("key1", large_value));
  ASSERT_EQ(Get("key1"), large_value);
}

// Test that data survives close+reopen after explicit flush.
// Blob files should be sealed during flush and registered in MANIFEST.
TEST_F(DBBlobDirectWriteTest, RecoveryAfterFlush) {
  Options options = GetBlobDirectWriteOptions();
  options.blob_direct_write_partitions = 2;
  DestroyAndReopen(options);

  // Write data
  const int num_keys = 50;
  for (int i = 0; i < num_keys; i++) {
    std::string key = "rec_key" + std::to_string(i);
    std::string value(100, 'a' + (i % 26));
    ASSERT_OK(Put(key, value));
  }

  // Flush to persist everything
  ASSERT_OK(Flush());

  // Reopen and verify all data
  Reopen(options);
  for (int i = 0; i < num_keys; i++) {
    std::string key = "rec_key" + std::to_string(i);
    std::string expected(100, 'a' + (i % 26));
    ASSERT_EQ(Get(key), expected);
  }
}

// Test that data survives close+reopen WITHOUT explicit flush.
// Blob files should be discovered as orphans during DB open and
// registered in MANIFEST before DeleteObsoleteFiles runs.
// WAL replay recreates the BlobIndex entries.
TEST_F(DBBlobDirectWriteTest, RecoveryWithoutFlush) {
  Options options = GetBlobDirectWriteOptions();
  options.blob_direct_write_partitions = 2;
  DestroyAndReopen(options);

  // Write data but do NOT flush
  const int num_keys = 50;
  for (int i = 0; i < num_keys; i++) {
    std::string key = "nf_key" + std::to_string(i);
    std::string value(100, 'A' + (i % 26));
    ASSERT_OK(Put(key, value));
  }

  // Close and reopen — WAL replay + orphan blob file recovery
  Reopen(options);

  // Verify all data is readable
  for (int i = 0; i < num_keys; i++) {
    std::string key = "nf_key" + std::to_string(i);
    std::string expected(100, 'A' + (i % 26));
    ASSERT_EQ(Get(key), expected);
  }
}

// Test recovery after blob file rotation (small blob_file_size).
// Multiple blob files may be sealed/unsealed at close time.
TEST_F(DBBlobDirectWriteTest, RecoveryWithRotation) {
  Options options = GetBlobDirectWriteOptions();
  options.blob_file_size = 512;  // Very small to force rotation
  options.blob_direct_write_partitions = 1;
  DestroyAndReopen(options);

  // Write enough data to trigger multiple rotations
  const int num_keys = 30;
  for (int i = 0; i < num_keys; i++) {
    std::string key = "rot_rec_key" + std::to_string(i);
    std::string value(100, 'a' + (i % 26));
    ASSERT_OK(Put(key, value));
  }

  // Flush and reopen
  ASSERT_OK(Flush());
  Reopen(options);

  // Verify all data
  for (int i = 0; i < num_keys; i++) {
    std::string key = "rot_rec_key" + std::to_string(i);
    std::string expected(100, 'a' + (i % 26));
    ASSERT_EQ(Get(key), expected);
  }
}

// Test recovery with rotation and WITHOUT flush.
TEST_F(DBBlobDirectWriteTest, RecoveryWithRotationNoFlush) {
  Options options = GetBlobDirectWriteOptions();
  options.blob_file_size = 512;
  options.blob_direct_write_partitions = 1;
  DestroyAndReopen(options);

  const int num_keys = 30;
  for (int i = 0; i < num_keys; i++) {
    std::string key = "rot_nf_key" + std::to_string(i);
    std::string value(100, 'A' + (i % 26));
    ASSERT_OK(Put(key, value));
  }

  // Close and reopen without flush
  Reopen(options);

  for (int i = 0; i < num_keys; i++) {
    std::string key = "rot_nf_key" + std::to_string(i);
    std::string expected(100, 'A' + (i % 26));
    ASSERT_EQ(Get(key), expected);
  }
}

TEST_F(DBBlobDirectWriteTest, CompressionBasic) {
  Options options = GetBlobDirectWriteOptions();
  options.blob_compression_type = kSnappyCompression;
  options.blob_direct_write_partitions = 1;
  DestroyAndReopen(options);

  // Write compressible data (repeated chars compress well with snappy)
  const int num_keys = 20;
  for (int i = 0; i < num_keys; i++) {
    std::string key = "comp_key" + std::to_string(i);
    std::string value(200, 'a' + (i % 3));  // Highly compressible
    ASSERT_OK(Put(key, value));
  }

  // Verify reads before flush (reads from pending records, decompresses)
  for (int i = 0; i < num_keys; i++) {
    std::string key = "comp_key" + std::to_string(i);
    std::string expected(200, 'a' + (i % 3));
    ASSERT_EQ(Get(key), expected);
  }

  // Flush and verify reads from disk (BlobFileReader handles decompression)
  ASSERT_OK(Flush());
  for (int i = 0; i < num_keys; i++) {
    std::string key = "comp_key" + std::to_string(i);
    std::string expected(200, 'a' + (i % 3));
    ASSERT_EQ(Get(key), expected);
  }
}

TEST_F(DBBlobDirectWriteTest, CompressionWithReopen) {
  Options options = GetBlobDirectWriteOptions();
  options.blob_compression_type = kSnappyCompression;
  DestroyAndReopen(options);

  const int num_keys = 30;
  for (int i = 0; i < num_keys; i++) {
    std::string key = "creopen_key" + std::to_string(i);
    std::string value(150, 'x' + (i % 3));
    ASSERT_OK(Put(key, value));
  }

  ASSERT_OK(Flush());
  Reopen(options);

  for (int i = 0; i < num_keys; i++) {
    std::string key = "creopen_key" + std::to_string(i);
    std::string expected(150, 'x' + (i % 3));
    ASSERT_EQ(Get(key), expected);
  }
}

TEST_F(DBBlobDirectWriteTest, CompressionReducesFileSize) {
  // Write same data with and without compression, compare blob file sizes.
  const int num_keys = 50;
  const int value_size = 500;

  auto get_blob_file_total_size = [&]() -> uint64_t {
    uint64_t total = 0;
    std::vector<std::string> files;
    EXPECT_OK(env_->GetChildren(dbname_, &files));
    for (const auto& f : files) {
      if (f.find(".blob") != std::string::npos) {
        uint64_t fsize = 0;
        EXPECT_OK(env_->GetFileSize(dbname_ + "/" + f, &fsize));
        total += fsize;
      }
    }
    return total;
  };

  // First: no compression
  Options options = GetBlobDirectWriteOptions();
  options.blob_compression_type = kNoCompression;
  options.blob_direct_write_partitions = 1;
  DestroyAndReopen(options);

  for (int i = 0; i < num_keys; i++) {
    std::string key = "size_key" + std::to_string(i);
    // Highly compressible: all same character
    std::string value(value_size, 'A');
    ASSERT_OK(Put(key, value));
  }
  ASSERT_OK(Flush());

  uint64_t uncompressed_size = get_blob_file_total_size();

  // Second: with snappy compression
  options.blob_compression_type = kSnappyCompression;
  DestroyAndReopen(options);

  for (int i = 0; i < num_keys; i++) {
    std::string key = "size_key" + std::to_string(i);
    std::string value(value_size, 'A');
    ASSERT_OK(Put(key, value));
  }
  ASSERT_OK(Flush());

  uint64_t compressed_size = get_blob_file_total_size();

  // Compressed size should be significantly smaller for repeated-char data
  ASSERT_GT(uncompressed_size, 0);
  ASSERT_GT(compressed_size, 0);
  ASSERT_LT(compressed_size, uncompressed_size);
}

TEST_F(DBBlobDirectWriteTest, PipelinedWriteBasic) {
  Options options = GetBlobDirectWriteOptions();
  options.enable_pipelined_write = true;
  DestroyAndReopen(options);

  const int num_keys = 20;
  WriteLargeValues(num_keys);

  // Verify reads before flush
  VerifyLargeValues(num_keys);

  // Verify reads after flush
  ASSERT_OK(Flush());
  VerifyLargeValues(num_keys);

  // Verify after reopen
  Reopen(options);
  VerifyLargeValues(num_keys);
}

TEST_F(DBBlobDirectWriteTest, PipelinedWriteWithBatchWrite) {
  Options options = GetBlobDirectWriteOptions();
  options.enable_pipelined_write = true;
  DestroyAndReopen(options);

  // Use WriteBatch (not DBImpl::Put fast path) to exercise TransformBatch
  // in the pipelined write path.
  WriteBatch batch;
  for (int i = 0; i < 10; i++) {
    std::string key = "pw_batch_key" + std::to_string(i);
    std::string value(100, 'a' + (i % 26));
    ASSERT_OK(batch.Put(key, value));
  }
  ASSERT_OK(db_->Write(WriteOptions(), &batch));

  // Verify all values
  for (int i = 0; i < 10; i++) {
    std::string key = "pw_batch_key" + std::to_string(i);
    std::string expected(100, 'a' + (i % 26));
    ASSERT_EQ(Get(key), expected);
  }

  ASSERT_OK(Flush());
  for (int i = 0; i < 10; i++) {
    std::string key = "pw_batch_key" + std::to_string(i);
    std::string expected(100, 'a' + (i % 26));
    ASSERT_EQ(Get(key), expected);
  }
}

TEST_F(DBBlobDirectWriteTest, UnorderedWriteBasic) {
  Options options = GetBlobDirectWriteOptions();
  options.unordered_write = true;
  options.allow_concurrent_memtable_write = true;
  DestroyAndReopen(options);

  const int num_keys = 20;
  WriteLargeValues(num_keys);

  // Verify reads before flush
  VerifyLargeValues(num_keys);

  // Verify reads after flush
  ASSERT_OK(Flush());
  VerifyLargeValues(num_keys);

  // Verify after reopen
  Reopen(options);
  VerifyLargeValues(num_keys);
}

TEST_F(DBBlobDirectWriteTest, PrepopulateBlobCache) {
  Options options = GetBlobDirectWriteOptions();
  options.statistics = CreateDBStatistics();
  auto cache = NewLRUCache(1 << 20);  // 1MB cache
  options.blob_cache = cache;
  options.prepopulate_blob_cache = PrepopulateBlobCache::kFlushOnly;
  DestroyAndReopen(options);

  uint64_t cache_add_before =
      options.statistics->getTickerCount(BLOB_DB_CACHE_ADD);

  // Write values that exceed min_blob_size
  const int num_keys = 10;
  for (int i = 0; i < num_keys; i++) {
    std::string key = "cache_key" + std::to_string(i);
    std::string value(100, 'a' + (i % 26));
    ASSERT_OK(Put(key, value));
  }

  uint64_t cache_add_after =
      options.statistics->getTickerCount(BLOB_DB_CACHE_ADD);
  // Each direct write Put should have added to cache
  ASSERT_EQ(cache_add_after - cache_add_before,
            static_cast<uint64_t>(num_keys));

  // Verify values are readable (should hit cache for unflushed data)
  for (int i = 0; i < num_keys; i++) {
    std::string key = "cache_key" + std::to_string(i);
    std::string expected(100, 'a' + (i % 26));
    ASSERT_EQ(Get(key), expected);
  }

  // Verify after flush too
  ASSERT_OK(Flush());
  for (int i = 0; i < num_keys; i++) {
    std::string key = "cache_key" + std::to_string(i);
    std::string expected(100, 'a' + (i % 26));
    ASSERT_EQ(Get(key), expected);
  }
}

TEST_F(DBBlobDirectWriteTest, CompressionTimingMetric) {
  Options options = GetBlobDirectWriteOptions();
  options.blob_compression_type = kSnappyCompression;
  options.statistics = CreateDBStatistics();
  DestroyAndReopen(options);

  HistogramData before_data;
  options.statistics->histogramData(BLOB_DB_COMPRESSION_MICROS, &before_data);

  // Write compressible data
  for (int i = 0; i < 10; i++) {
    std::string key = "comp_time_key" + std::to_string(i);
    std::string value(200, 'a' + (i % 3));
    ASSERT_OK(Put(key, value));
  }

  HistogramData after_data;
  options.statistics->histogramData(BLOB_DB_COMPRESSION_MICROS, &after_data);
  ASSERT_GT(after_data.count, before_data.count);
}

TEST_F(DBBlobDirectWriteTest, EventListenerNotifications) {
  // Verify that EventListener receives blob file creation/completion events.
  class BlobFileListener : public EventListener {
   public:
    std::atomic<int> creation_started{0};
    std::atomic<int> creation_completed{0};

    void OnBlobFileCreationStarted(
        const BlobFileCreationBriefInfo& /*info*/) override {
      creation_started.fetch_add(1, std::memory_order_relaxed);
    }

    void OnBlobFileCreated(const BlobFileCreationInfo& /*info*/) override {
      creation_completed.fetch_add(1, std::memory_order_relaxed);
    }
  };

  auto listener = std::make_shared<BlobFileListener>();
  Options options = GetBlobDirectWriteOptions();
  options.listeners.push_back(listener);
  options.blob_file_size = 512;  // Small to force rotation
  options.blob_direct_write_partitions = 1;
  DestroyAndReopen(options);

  // Write enough to trigger at least one rotation
  for (int i = 0; i < 20; i++) {
    std::string key = "evt_key" + std::to_string(i);
    std::string value(100, 'a' + (i % 26));
    ASSERT_OK(Put(key, value));
  }

  // Flush to seal remaining files
  ASSERT_OK(Flush());

  ASSERT_GT(listener->creation_started.load(), 0);
  ASSERT_GT(listener->creation_completed.load(), 0);
}

TEST_F(DBBlobDirectWriteTest, CompressionWithRotation) {
  Options options = GetBlobDirectWriteOptions();
  options.blob_compression_type = kSnappyCompression;
  options.blob_file_size = 512;  // Small to force rotation
  options.blob_direct_write_partitions = 1;
  DestroyAndReopen(options);

  const int num_keys = 30;
  for (int i = 0; i < num_keys; i++) {
    std::string key = "crot_key" + std::to_string(i);
    std::string value(100, 'a' + (i % 26));
    ASSERT_OK(Put(key, value));
  }

  // Verify before flush
  for (int i = 0; i < num_keys; i++) {
    std::string key = "crot_key" + std::to_string(i);
    std::string expected(100, 'a' + (i % 26));
    ASSERT_EQ(Get(key), expected);
  }

  // Verify after flush
  ASSERT_OK(Flush());
  for (int i = 0; i < num_keys; i++) {
    std::string key = "crot_key" + std::to_string(i);
    std::string expected(100, 'a' + (i % 26));
    ASSERT_EQ(Get(key), expected);
  }
}

TEST_F(DBBlobDirectWriteTest, PeriodicFlush) {
  // Verify that periodic flush drains pending records even without
  // hitting the high-water mark.
  Options options = GetBlobDirectWriteOptions();
  options.blob_direct_write_partitions = 1;
  options.blob_direct_write_buffer_size = 1 * 1024 * 1024;  // 1MB
  options.blob_direct_write_flush_interval_ms = 50;         // 50ms
  DestroyAndReopen(options);

  // Write a single key (well below high-water mark)
  std::string large_value(200, 'v');
  ASSERT_OK(Put("periodic_key", large_value));

  // Wait for 2x the flush interval to ensure periodic flush fires
  Env::Default()->SleepForMicroseconds(150 * 1000);  // 150ms

  // Verify the value is readable (flushed to disk by periodic timer)
  ASSERT_EQ(Get("periodic_key"), large_value);

  // Write more data and verify it also gets flushed
  for (int i = 0; i < 5; i++) {
    std::string key = "periodic_key_" + std::to_string(i);
    std::string value(200 + i, 'a' + (i % 26));
    ASSERT_OK(Put(key, value));
  }

  Env::Default()->SleepForMicroseconds(150 * 1000);  // 150ms

  for (int i = 0; i < 5; i++) {
    std::string key = "periodic_key_" + std::to_string(i);
    std::string expected(200 + i, 'a' + (i % 26));
    ASSERT_EQ(Get(key), expected);
  }
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

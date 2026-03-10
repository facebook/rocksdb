//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <atomic>
#include <functional>
#include <thread>
#include <vector>

#include "db/db_test_util.h"
#include "port/stack_trace.h"
#include "rocksdb/statistics.h"
#include "rocksdb/utilities/transaction.h"
#include "rocksdb/utilities/transaction_db.h"
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
  // value_fn allows custom value construction for specialized tests.
  using ValueFn = std::function<std::string(int i, int value_size)>;

  static std::string DefaultValueFn(int i, int value_size) {
    return std::string(value_size + i, 'a' + (i % 26));
  }

  void WriteLargeValues(int num_keys, int value_size = 100,
                        const std::string& key_prefix = "key",
                        const ValueFn& value_fn = DefaultValueFn) {
    for (int i = 0; i < num_keys; i++) {
      std::string key = key_prefix + std::to_string(i);
      ASSERT_OK(Put(key, value_fn(i, value_size)));
    }
  }

  // Verify num_keys key-value pairs written by WriteLargeValues.
  void VerifyLargeValues(int num_keys, int value_size = 100,
                         const std::string& key_prefix = "key",
                         const ValueFn& value_fn = DefaultValueFn) {
    for (int i = 0; i < num_keys; i++) {
      std::string key = key_prefix + std::to_string(i);
      ASSERT_EQ(Get(key), value_fn(i, value_size));
    }
  }

  // Common pattern: write → verify → flush → verify → reopen → verify.
  void WriteVerifyFlushReopenVerify(const Options& options, int num_keys = 20,
                                    int value_size = 100,
                                    const std::string& key_prefix = "key",
                                    const ValueFn& value_fn = DefaultValueFn) {
    WriteLargeValues(num_keys, value_size, key_prefix, value_fn);
    VerifyLargeValues(num_keys, value_size, key_prefix, value_fn);
    ASSERT_OK(Flush());
    VerifyLargeValues(num_keys, value_size, key_prefix, value_fn);
    Reopen(options);
    VerifyLargeValues(num_keys, value_size, key_prefix, value_fn);
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
  options.blob_direct_write_buffer_size = 65536;
  options.blob_file_size = 1024 * 1024;
  DestroyAndReopen(options);

  // Track that backpressure actually fires via sync point.
  std::atomic<int> backpressure_count{0};
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "BlobFilePartitionManager::WriteBlob:BackpressureStall",
      [&](void* /*arg*/) {
        backpressure_count.fetch_add(1, std::memory_order_relaxed);
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

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

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearAllCallBacks();

  // With 16 threads x 500 keys x 4KB values = 32MB through a 64KB buffer,
  // backpressure must have triggered.
  ASSERT_GT(backpressure_count.load(), 0);

  for (int t = 0; t < num_threads; t++) {
    for (int i = 0; i < keys_per_thread; i += 50) {
      std::string key = "bp_t" + std::to_string(t) + "_k" + std::to_string(i);
      std::string expected(value_size, 'a' + (t % 26));
      ASSERT_EQ(Get(key), expected);
    }
  }

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

  const int num_keys = 50;
  auto value_fn = [](int i, int) -> std::string {
    return std::string(100, 'a' + (i % 26));
  };
  WriteLargeValues(num_keys, 100, "rec_key", value_fn);
  ASSERT_OK(Flush());
  Reopen(options);
  VerifyLargeValues(num_keys, 100, "rec_key", value_fn);
}

// Test that data survives close+reopen WITHOUT explicit flush.
// Blob files should be discovered as orphans during DB open and
// registered in MANIFEST before DeleteObsoleteFiles runs.
// WAL replay recreates the BlobIndex entries.
TEST_F(DBBlobDirectWriteTest, RecoveryWithoutFlush) {
  Options options = GetBlobDirectWriteOptions();
  options.blob_direct_write_partitions = 2;
  DestroyAndReopen(options);

  const int num_keys = 50;
  auto value_fn = [](int i, int) -> std::string {
    return std::string(100, 'A' + (i % 26));
  };
  WriteLargeValues(num_keys, 100, "nf_key", value_fn);
  Reopen(options);
  VerifyLargeValues(num_keys, 100, "nf_key", value_fn);
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

  WriteVerifyFlushReopenVerify(options, 20, 100, "key");
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

  WriteVerifyFlushReopenVerify(options, 20, 100, "key");
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
  Options options = GetBlobDirectWriteOptions();
  options.blob_direct_write_partitions = 1;
  options.blob_direct_write_buffer_size = 1 * 1024 * 1024;  // 1MB
  options.blob_direct_write_flush_interval_ms = 50;         // 50ms
  DestroyAndReopen(options);

  port::Mutex flush_mu;
  port::CondVar flush_cv(&flush_mu);
  std::atomic<int> periodic_flush_count{0};
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "BlobFilePartitionManager::BackgroundIOLoop:PeriodicFlush",
      [&](void* /*arg*/) {
        periodic_flush_count.fetch_add(1, std::memory_order_relaxed);
        MutexLock lock(&flush_mu);
        flush_cv.SignalAll();
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  // Write data well below the high-water mark so only the periodic timer
  // triggers a flush (not backpressure).
  std::string large_value(200, 'v');
  ASSERT_OK(Put("periodic_key", large_value));

  ASSERT_EQ(Get("periodic_key"), large_value);

  for (int i = 0; i < 5; i++) {
    std::string key = "periodic_key_" + std::to_string(i);
    std::string value(200 + i, 'a' + (i % 26));
    ASSERT_OK(Put(key, value));
  }

  // Wait for the periodic flush via condvar signaled by SyncPoint callback.
  {
    MutexLock lock(&flush_mu);
    if (periodic_flush_count.load(std::memory_order_relaxed) == 0) {
      flush_cv.TimedWait(Env::Default()->NowMicros() + 5 * 1000 * 1000);
    }
  }

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearAllCallBacks();

  ASSERT_GT(periodic_flush_count.load(), 0);

  for (int i = 0; i < 5; i++) {
    std::string key = "periodic_key_" + std::to_string(i);
    std::string expected(200 + i, 'a' + (i % 26));
    ASSERT_EQ(Get(key), expected);
  }
}

// Test concurrent readers and writers exercising the multi-tier read fallback.
TEST_F(DBBlobDirectWriteTest, ConcurrentReadersAndWriters) {
  Options options = GetBlobDirectWriteOptions();
  options.blob_direct_write_partitions = 4;
  options.blob_direct_write_buffer_size = 65536;
  DestroyAndReopen(options);

  // Pre-populate some data so readers have something to read.
  const int initial_keys = 50;
  WriteLargeValues(initial_keys, 100, "init_");

  const int target_writes = 200;
  std::atomic<bool> stop{false};
  std::atomic<int> write_errors{0};
  std::atomic<int> read_errors{0};
  std::atomic<int> total_writes{0};

  const int num_writers = 4;
  std::vector<std::thread> writers;
  for (int t = 0; t < num_writers; t++) {
    writers.emplace_back([&, t]() {
      int i = 0;
      while (!stop.load(std::memory_order_relaxed)) {
        std::string key = "w" + std::to_string(t) + "_" + std::to_string(i);
        std::string value(100, 'a' + (i % 26));
        Status s = Put(key, value);
        if (!s.ok()) {
          write_errors.fetch_add(1, std::memory_order_relaxed);
        } else {
          total_writes.fetch_add(1, std::memory_order_relaxed);
        }
        i++;
      }
    });
  }

  const int num_readers = 4;
  std::vector<std::thread> readers;
  for (int t = 0; t < num_readers; t++) {
    readers.emplace_back([&, t]() {
      while (!stop.load(std::memory_order_relaxed)) {
        int idx = t % initial_keys;
        std::string key = "init_" + std::to_string(idx);
        std::string expected(100 + idx, 'a' + (idx % 26));
        std::string result = Get(key);
        if (result != expected) {
          read_errors.fetch_add(1, std::memory_order_relaxed);
        }
      }
    });
  }

  // Wait for writers to reach target (no sleep polling — spin on atomics).
  while (total_writes.load(std::memory_order_relaxed) <
             num_writers * target_writes &&
         write_errors.load(std::memory_order_relaxed) == 0 &&
         read_errors.load(std::memory_order_relaxed) == 0) {
    std::this_thread::yield();
  }
  stop.store(true, std::memory_order_relaxed);

  for (auto& t : writers) {
    t.join();
  }
  for (auto& t : readers) {
    t.join();
  }

  ASSERT_EQ(write_errors.load(), 0);
  ASSERT_EQ(read_errors.load(), 0);
}

// Test WriteBatch with mixed operation types.
TEST_F(DBBlobDirectWriteTest, MixedWriteBatchOperations) {
  Options options = GetBlobDirectWriteOptions();
  options.min_blob_size = 50;
  DestroyAndReopen(options);

  WriteBatch batch;
  std::string large1(100, 'L');
  std::string large2(100, 'M');
  std::string small1("tiny");
  ASSERT_OK(batch.Put("large_key1", large1));
  ASSERT_OK(batch.Delete("nonexistent_key"));
  ASSERT_OK(batch.Put("large_key2", large2));
  ASSERT_OK(batch.Put("small_key1", small1));
  ASSERT_OK(batch.SingleDelete("another_nonexistent"));
  ASSERT_OK(db_->Write(WriteOptions(), &batch));

  ASSERT_EQ(Get("large_key1"), large1);
  ASSERT_EQ(Get("large_key2"), large2);
  ASSERT_EQ(Get("small_key1"), small1);
  ASSERT_EQ(Get("nonexistent_key"), "NOT_FOUND");

  ASSERT_OK(Flush());
  ASSERT_EQ(Get("large_key1"), large1);
  ASSERT_EQ(Get("large_key2"), large2);
  ASSERT_EQ(Get("small_key1"), small1);
}

// Test WriteBatch with only non-blob operations (no values qualify).
TEST_F(DBBlobDirectWriteTest, WriteBatchNoQualifyingValues) {
  Options options = GetBlobDirectWriteOptions();
  options.min_blob_size = 1000;
  DestroyAndReopen(options);

  WriteBatch batch;
  ASSERT_OK(batch.Put("k1", "small_v1"));
  ASSERT_OK(batch.Put("k2", "small_v2"));
  ASSERT_OK(batch.Delete("k3"));
  ASSERT_OK(db_->Write(WriteOptions(), &batch));

  ASSERT_EQ(Get("k1"), "small_v1");
  ASSERT_EQ(Get("k2"), "small_v2");
}

// Test with sync=true to exercise WAL sync + blob file sync interaction.
TEST_F(DBBlobDirectWriteTest, SyncWrite) {
  Options options = GetBlobDirectWriteOptions();
  DestroyAndReopen(options);

  WriteOptions wo;
  wo.sync = true;

  std::string large_value(200, 'S');
  ASSERT_OK(db_->Put(wo, "sync_key1", large_value));
  ASSERT_OK(db_->Put(wo, "sync_key2", large_value));

  ASSERT_EQ(Get("sync_key1"), large_value);
  ASSERT_EQ(Get("sync_key2"), large_value);

  Reopen(options);
  ASSERT_EQ(Get("sync_key1"), large_value);
  ASSERT_EQ(Get("sync_key2"), large_value);
}

// Test that disableWAL is rejected only when blob values are actually
// extracted (not for inline values or non-blob CFs).
TEST_F(DBBlobDirectWriteTest, DisableWALSkipsTransformation) {
  Options options = GetBlobDirectWriteOptions();
  DestroyAndReopen(options);

  WriteOptions wo;
  wo.disableWAL = true;

  // Put with disableWAL: the fast path skips blob direct write entirely,
  // so the value stays inline in the memtable.
  std::string large_value(200, 'W');
  ASSERT_OK(db_->Put(wo, "wal_key_inline", large_value));
  ASSERT_EQ(Get("wal_key_inline"), large_value);

  // WriteBatch with disableWAL: transformation is skipped entirely,
  // so blob-qualifying values stay inline. No orphaned blob data.
  WriteBatch batch;
  ASSERT_OK(batch.Put("wal_batch_key", large_value));
  ASSERT_OK(db_->Write(wo, &batch));
  ASSERT_EQ(Get("wal_batch_key"), large_value);

  // Small values (below min_blob_size) should succeed with disableWAL.
  std::string small_value("tiny");
  ASSERT_OK(db_->Put(wo, "wal_small_key", small_value));
  ASSERT_EQ(Get("wal_small_key"), small_value);
}

// enable_blob_direct_write is immutable and cannot be changed via SetOptions.
TEST_F(DBBlobDirectWriteTest, DynamicSetOptions) {
  Options options = GetBlobDirectWriteOptions();
  DestroyAndReopen(options);

  std::string large_v1(200, '1');
  ASSERT_OK(Put("dyn_key1", large_v1));
  ASSERT_EQ(Get("dyn_key1"), large_v1);

  // SetOptions should reject changes to enable_blob_direct_write.
  ASSERT_NOK(dbfull()->SetOptions({{"enable_blob_direct_write", "false"}}));
  ASSERT_NOK(dbfull()->SetOptions({{"enable_blob_direct_write", "true"}}));

  // Writes still work after the rejected SetOptions.
  std::string large_v2(200, '2');
  ASSERT_OK(Put("dyn_key2", large_v2));
  ASSERT_EQ(Get("dyn_key1"), large_v1);
  ASSERT_EQ(Get("dyn_key2"), large_v2);

  ASSERT_OK(Flush());
  Reopen(options);
  ASSERT_EQ(Get("dyn_key1"), large_v1);
  ASSERT_EQ(Get("dyn_key2"), large_v2);
}

// Test Delete followed by re-Put with the same key (tombstone interaction).
TEST_F(DBBlobDirectWriteTest, DeleteAndReput) {
  Options options = GetBlobDirectWriteOptions();
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);

  std::string blob_v1(100, '1');
  std::string blob_v2(150, '2');

  // Put → Delete → Put (same key, new blob value).
  ASSERT_OK(Put("reput_key", blob_v1));
  ASSERT_EQ(Get("reput_key"), blob_v1);

  ASSERT_OK(Delete("reput_key"));
  ASSERT_EQ(Get("reput_key"), "NOT_FOUND");

  ASSERT_OK(Put("reput_key", blob_v2));
  ASSERT_EQ(Get("reput_key"), blob_v2);

  // After flush, the latest Put should win over the tombstone.
  ASSERT_OK(Flush());
  ASSERT_EQ(Get("reput_key"), blob_v2);

  // After compaction, the tombstone and old blob_v1 should be cleaned up.
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  ASSERT_EQ(Get("reput_key"), blob_v2);
}

// Transaction/2PC interaction tests (H6 coverage).
TEST_F(DBBlobDirectWriteTest, TransactionDBBasicPutGet) {
  Options options = GetBlobDirectWriteOptions();
  options.disable_auto_compactions = true;
  TransactionDBOptions txn_db_options;

  Close();
  DestroyDB(dbname_, options);

  TransactionDB* txn_db = nullptr;
  ASSERT_OK(TransactionDB::Open(options, txn_db_options, dbname_, &txn_db));
  ASSERT_NE(txn_db, nullptr);

  WriteOptions wo;
  std::string blob_v1(100, 'x');
  std::string blob_v2(200, 'y');

  ASSERT_OK(txn_db->Put(wo, "txn_key1", blob_v1));
  std::string value;
  ASSERT_OK(txn_db->Get(ReadOptions(), "txn_key1", &value));
  ASSERT_EQ(value, blob_v1);

  Transaction* txn = txn_db->BeginTransaction(wo);
  ASSERT_NE(txn, nullptr);
  ASSERT_OK(txn->Put("txn_key2", blob_v2));
  ASSERT_OK(txn->Commit());
  delete txn;

  ASSERT_OK(txn_db->Get(ReadOptions(), "txn_key2", &value));
  ASSERT_EQ(value, blob_v2);

  ASSERT_OK(txn_db->Flush(FlushOptions()));
  ASSERT_OK(txn_db->Get(ReadOptions(), "txn_key1", &value));
  ASSERT_EQ(value, blob_v1);
  ASSERT_OK(txn_db->Get(ReadOptions(), "txn_key2", &value));
  ASSERT_EQ(value, blob_v2);

  delete txn_db;
}

TEST_F(DBBlobDirectWriteTest, TransactionConflictDetection) {
  Options options = GetBlobDirectWriteOptions();
  TransactionDBOptions txn_db_options;

  Close();
  DestroyDB(dbname_, options);

  TransactionDB* txn_db = nullptr;
  ASSERT_OK(TransactionDB::Open(options, txn_db_options, dbname_, &txn_db));

  WriteOptions wo;
  std::string blob_v(100, 'a');
  ASSERT_OK(txn_db->Put(wo, "conflict_key", blob_v));

  Transaction* txn1 = txn_db->BeginTransaction(wo);
  ASSERT_OK(txn1->GetForUpdate(ReadOptions(), "conflict_key", &blob_v));

  TransactionOptions txn_opts;
  txn_opts.lock_timeout = 0;
  Transaction* txn2 = txn_db->BeginTransaction(wo, txn_opts);
  std::string v2;
  Status lock_s = txn2->GetForUpdate(ReadOptions(), "conflict_key", &v2);
  ASSERT_TRUE(lock_s.IsTimedOut());

  ASSERT_OK(txn1->Put("conflict_key", std::string(100, 'b')));
  ASSERT_OK(txn1->Commit());

  std::string value;
  ASSERT_OK(txn_db->Get(ReadOptions(), "conflict_key", &value));
  ASSERT_EQ(value, std::string(100, 'b'));

  delete txn1;
  delete txn2;
  delete txn_db;
}

TEST_F(DBBlobDirectWriteTest, TwoPhaseCommit) {
  Options options = GetBlobDirectWriteOptions();
  options.disable_auto_compactions = true;
  TransactionDBOptions txn_db_options;
  txn_db_options.write_policy = TxnDBWritePolicy::WRITE_COMMITTED;

  Close();
  DestroyDB(dbname_, options);

  TransactionDB* txn_db = nullptr;
  ASSERT_OK(TransactionDB::Open(options, txn_db_options, dbname_, &txn_db));

  WriteOptions wo;
  Transaction* txn = txn_db->BeginTransaction(wo);
  ASSERT_NE(txn, nullptr);
  txn->SetName("blob_txn_1");

  std::string blob_v1(100, 'p');
  std::string blob_v2(150, 'q');
  ASSERT_OK(txn->Put("2pc_key1", blob_v1));
  ASSERT_OK(txn->Put("2pc_key2", blob_v2));

  ASSERT_OK(txn->Prepare());
  ASSERT_OK(txn->Commit());
  delete txn;

  std::string value;
  ASSERT_OK(txn_db->Get(ReadOptions(), "2pc_key1", &value));
  ASSERT_EQ(value, blob_v1);
  ASSERT_OK(txn_db->Get(ReadOptions(), "2pc_key2", &value));
  ASSERT_EQ(value, blob_v2);

  ASSERT_OK(txn_db->Flush(FlushOptions()));
  delete txn_db;
  txn_db = nullptr;

  ASSERT_OK(TransactionDB::Open(options, txn_db_options, dbname_, &txn_db));
  ASSERT_OK(txn_db->Get(ReadOptions(), "2pc_key1", &value));
  ASSERT_EQ(value, blob_v1);
  ASSERT_OK(txn_db->Get(ReadOptions(), "2pc_key2", &value));
  ASSERT_EQ(value, blob_v2);

  delete txn_db;
}

// Multi-CF test: different blob settings per CF, cross-CF WriteBatch.
TEST_F(DBBlobDirectWriteTest, MultiColumnFamilyBasic) {
  Options options = GetBlobDirectWriteOptions();
  DestroyAndReopen(options);

  // Create a second CF with a larger min_blob_size so small values stay inline.
  ColumnFamilyOptions cf_opts(options);
  cf_opts.enable_blob_files = true;
  cf_opts.enable_blob_direct_write = true;
  cf_opts.min_blob_size = 500;
  ColumnFamilyHandle* cf_handle = nullptr;
  ASSERT_OK(db_->CreateColumnFamily(cf_opts, "data_cf", &cf_handle));

  // Write to default CF (min_blob_size=10): goes to blob file.
  std::string blob_value(100, 'B');
  ASSERT_OK(db_->Put(WriteOptions(), "default_key", blob_value));
  ASSERT_EQ(Get("default_key"), blob_value);

  // Write to data_cf with value below its min_blob_size: stays inline.
  std::string inline_value(200, 'I');
  ASSERT_OK(db_->Put(WriteOptions(), cf_handle, "data_key1", inline_value));
  std::string result;
  ASSERT_OK(db_->Get(ReadOptions(), cf_handle, "data_key1", &result));
  ASSERT_EQ(result, inline_value);

  // Write to data_cf with value above its min_blob_size: goes to blob file.
  std::string large_value(600, 'L');
  ASSERT_OK(db_->Put(WriteOptions(), cf_handle, "data_key2", large_value));
  ASSERT_OK(db_->Get(ReadOptions(), cf_handle, "data_key2", &result));
  ASSERT_EQ(result, large_value);

  // Cross-CF WriteBatch.
  WriteBatch batch;
  std::string batch_val1(50, 'X');
  std::string batch_val2(700, 'Y');
  ASSERT_OK(batch.Put("batch_default", batch_val1));
  ASSERT_OK(batch.Put(cf_handle, "batch_data", batch_val2));
  ASSERT_OK(db_->Write(WriteOptions(), &batch));

  ASSERT_EQ(Get("batch_default"), batch_val1);
  ASSERT_OK(db_->Get(ReadOptions(), cf_handle, "batch_data", &result));
  ASSERT_EQ(result, batch_val2);

  // Flush both CFs and verify data survives.
  ASSERT_OK(db_->Flush(FlushOptions()));
  ASSERT_OK(db_->Flush(FlushOptions(), cf_handle));

  ASSERT_EQ(Get("default_key"), blob_value);
  ASSERT_OK(db_->Get(ReadOptions(), cf_handle, "data_key2", &result));
  ASSERT_EQ(result, large_value);

  db_->DestroyColumnFamilyHandle(cf_handle);
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

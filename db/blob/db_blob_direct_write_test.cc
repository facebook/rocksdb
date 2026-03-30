//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <functional>
#include <mutex>
#include <set>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "db/blob/blob_file_cache.h"
#include "db/blob/blob_file_meta.h"
#include "db/blob/blob_file_partition_manager.h"
#include "db/blob/blob_log_format.h"
#include "db/column_family.h"
#include "db/db_test_util.h"
#include "db/db_with_timestamp_test_util.h"
#include "db/version_set.h"
#include "env/composite_env_wrapper.h"
#include "file/filename.h"
#include "port/stack_trace.h"
#include "rocksdb/file_checksum.h"
#include "rocksdb/statistics.h"
#include "rocksdb/utilities/backup_engine.h"
#include "rocksdb/utilities/transaction.h"
#include "rocksdb/utilities/transaction_db.h"
#include "test_util/testharness.h"
#include "util/compression.h"
#include "utilities/fault_injection_env.h"
#include "utilities/fault_injection_fs.h"
#include "utilities/merge_operators.h"

namespace ROCKSDB_NAMESPACE {

class DBBlobDirectWriteTest : public DBTestBase {
 public:
  explicit DBBlobDirectWriteTest()
      : DBTestBase("db_blob_direct_write_test", /*env_do_fsync=*/true) {}

 protected:
  // Helper: get blob file metadata from current version.
  // Returns map of blob_file_number -> (linked_ssts_count, total_blob_count).
  struct BlobFileInfo {
    uint64_t file_number;
    uint64_t file_size;
    size_t linked_ssts_count;
    uint64_t total_blob_count;
    uint64_t total_blob_bytes;
    uint64_t garbage_blob_count;
  };

  std::vector<BlobFileInfo> GetBlobFileInfoFromVersion() {
    std::vector<BlobFileInfo> result;
    VersionSet* versions = dbfull()->GetVersionSet();
    assert(versions);
    ColumnFamilyData* cfd = versions->GetColumnFamilySet()->GetDefault();
    assert(cfd);
    Version* current = cfd->current();
    assert(current);
    const VersionStorageInfo* vstorage = current->storage_info();
    assert(vstorage);
    for (const auto& blob_file : vstorage->GetBlobFiles()) {
      BlobFileInfo info;
      info.file_number = blob_file->GetBlobFileNumber();
      info.file_size = blob_file->GetBlobFileSize();
      info.linked_ssts_count = blob_file->GetLinkedSsts().size();
      info.total_blob_count = blob_file->GetTotalBlobCount();
      info.total_blob_bytes = blob_file->GetTotalBlobBytes();
      info.garbage_blob_count = blob_file->GetGarbageBlobCount();
      result.push_back(info);
    }
    return result;
  }

  bool VersionContainsBlobFile(uint64_t file_number) {
    const auto blob_files = GetBlobFileInfoFromVersion();
    return std::any_of(blob_files.begin(), blob_files.end(),
                       [&](const BlobFileInfo& info) {
                         return info.file_number == file_number;
                       });
  }

  static size_t CountLinkedBlobFiles(
      const std::vector<BlobFileInfo>& blob_files) {
    return static_cast<size_t>(std::count_if(
        blob_files.begin(), blob_files.end(),
        [](const BlobFileInfo& bf) { return bf.linked_ssts_count > 0; }));
  }

  static void AssertBlobFilesHaveBlobs(
      const std::vector<BlobFileInfo>& blob_files) {
    for (const auto& bf : blob_files) {
      ASSERT_GT(bf.total_blob_count, 0u)
          << "Blob file " << bf.file_number << " has 0 blobs";
    }
  }

  static void AssertSurvivingBlobFilesHaveLiveBlobs(
      const std::vector<BlobFileInfo>& blob_files) {
    for (const auto& bf : blob_files) {
      ASSERT_GT(bf.total_blob_count, bf.garbage_blob_count)
          << "Blob file " << bf.file_number
          << " is fully garbage but still present";
    }
  }

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
    return std::string(value_size + i, static_cast<char>('a' + (i % 26)));
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

  // Common pattern: write -> verify -> flush -> verify -> reopen -> verify.
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

  // Helper: write a raw blob file to the DB directory. Returns the file path.
  // If cf_id is non-zero, the header encodes that CF ID.
  std::string WriteSyntheticBlobFile(uint64_t file_number, uint32_t cf_id,
                                     int num_records, bool write_footer = false,
                                     bool truncate_last_record = false) {
    std::string path = BlobFileName(dbname_, file_number);
    std::string data;

    // Header.
    BlobLogHeader header(cf_id, kNoCompression, /*has_ttl=*/false, {0, 0});
    header.EncodeTo(&data);

    // Records.
    for (int i = 0; i < num_records; i++) {
      std::string key = "synth_key" + std::to_string(i);
      std::string value(100, static_cast<char>('A' + (i % 26)));

      BlobLogRecord record;
      record.key = Slice(key);
      record.value = Slice(value);
      record.expiration = 0;
      std::string record_buf;
      record.EncodeHeaderTo(&record_buf);
      record_buf.append(key);
      record_buf.append(value);

      if (truncate_last_record && i == num_records - 1) {
        // Truncate the last record: keep header + partial body.
        data.append(record_buf.substr(0, BlobLogRecord::kHeaderSize + 5));
      } else {
        data.append(record_buf);
      }
    }

    if (write_footer) {
      BlobLogFooter footer;
      footer.blob_count = num_records;
      footer.expiration_range = {0, 0};
      std::string footer_buf;
      footer.EncodeTo(&footer_buf);
      data.append(footer_buf);
    }

    EXPECT_OK(WriteStringToFile(Env::Default(), data, path));
    return path;
  }

  std::vector<std::string> GetBlobFilePaths() const {
    std::vector<std::string> blob_paths;
    std::vector<std::string> filenames;
    EXPECT_OK(env_->GetChildren(dbname_, &filenames));
    for (const auto& fname : filenames) {
      uint64_t file_number = 0;
      FileType file_type;
      if (ParseFileName(fname, &file_number, &file_type) &&
          file_type == kBlobFile) {
        blob_paths.push_back(BlobFileName(dbname_, file_number));
      }
    }
    std::sort(blob_paths.begin(), blob_paths.end());
    return blob_paths;
  }

  std::string GetOnlyBlobFilePath() const {
    auto blob_paths = GetBlobFilePaths();
    EXPECT_EQ(blob_paths.size(), 1u);
    return blob_paths.empty() ? std::string() : blob_paths.front();
  }

  uint64_t GetUnderlyingFileSize(const std::string& path) const {
    uint64_t file_size = 0;
    EXPECT_OK(env_->GetFileSystem()->GetFileSize(path, IOOptions(), &file_size,
                                                 nullptr));
    return file_size;
  }

  void VerifyActiveBlobReadAfterBgFlushWithFaultInjectionFS(
      const Options& options, FaultInjectionTestFS* fault_fs) {
    ASSERT_NE(fault_fs, nullptr);
    DestroyAndReopen(options);

    const std::string value(200, 'U');
    ASSERT_OK(Put("unsynced_key", value));

    auto* cfd = dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
    ASSERT_NE(cfd, nullptr);
    auto* mgr = cfd->blob_partition_manager();
    ASSERT_NE(mgr, nullptr);

    // Force deferred writes out of pending_records and into the fault-injection
    // wrapper's unsynced buffer without sealing/syncing the file.
    ASSERT_OK(mgr->FlushAllOpenFiles(WriteOptions()));

    const std::string blob_path = GetOnlyBlobFilePath();
    ASSERT_FALSE(blob_path.empty());

    uint64_t logical_size = 0;
    ASSERT_OK(
        fault_fs->GetFileSize(blob_path, IOOptions(), &logical_size, nullptr));
    ASSERT_GT(logical_size, 0);
    ASSERT_EQ(GetUnderlyingFileSize(blob_path), 0);

    {
      std::unique_ptr<Iterator> it(db_->NewIterator(ReadOptions()));
      it->Seek("unsynced_key");
      ASSERT_OK(it->status());
      ASSERT_TRUE(it->Valid());
      ASSERT_EQ(it->key().ToString(), "unsynced_key");
      ASSERT_EQ(it->value().ToString(), value);
    }
    ASSERT_EQ(Get("unsynced_key"), value);

    // Sealing the file Sync()s it, after which the same value remains
    // readable.
    ASSERT_OK(Flush());
    ASSERT_EQ(Get("unsynced_key"), value);

    Close();
    last_options_.env = env_;
  }

  void ReadBlobRecordSizes(uint64_t file_number,
                           std::vector<uint64_t>* record_sizes) {
    ASSERT_NE(record_sizes, nullptr);
    const std::string blob_path = BlobFileName(dbname_, file_number);
    std::string content;
    ASSERT_OK(ReadFileToString(env_, blob_path, &content));
    ASSERT_GE(content.size(), BlobLogHeader::kSize + BlobLogFooter::kSize);

    record_sizes->clear();
    size_t offset = BlobLogHeader::kSize;
    const size_t data_limit = content.size() - BlobLogFooter::kSize;
    while (offset < data_limit) {
      ASSERT_GE(data_limit - offset, BlobLogRecord::kHeaderSize);
      BlobLogRecord record;
      ASSERT_OK(record.DecodeHeaderFrom(
          Slice(content.data() + offset, BlobLogRecord::kHeaderSize)));
      const uint64_t record_size = record.record_size();
      ASSERT_LE(offset + record_size, data_limit);
      record_sizes->push_back(record_size);
      offset += static_cast<size_t>(record_size);
    }

    ASSERT_EQ(offset, data_limit);
  }
};

class DBBlobDirectWriteWithTimestampTest : public DBBasicTestWithTimestampBase {
 public:
  DBBlobDirectWriteWithTimestampTest()
      : DBBasicTestWithTimestampBase(
            "db_blob_direct_write_with_timestamp_test") {}

 protected:
  static std::string EncodeTimestamp(uint64_t ts) {
    std::string encoded;
    EncodeU64Ts(ts, &encoded);
    return encoded;
  }

  Options GetBlobDirectWriteOptions(const Comparator* comparator) {
    Options options = GetDefaultOptions();
    options.create_if_missing = true;
    options.enable_blob_files = true;
    options.min_blob_size = 0;
    options.enable_blob_direct_write = true;
    options.blob_direct_write_partitions = 1;
    options.blob_direct_write_buffer_size = 0;
    options.persist_user_defined_timestamps = true;
    options.comparator = comparator;
    return options;
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

TEST_F(DBBlobDirectWriteWithTimestampTest,
       GetFromMemtableUsesFoundTimestampedKey) {
  const Comparator* comparator = test::BytewiseComparatorWithU64TsWrapper();
  Options options = GetBlobDirectWriteOptions(comparator);
  DestroyAndReopen(options);

  const std::string write_ts = EncodeTimestamp(1);
  const std::string read_ts = EncodeTimestamp(2);
  const std::string blob_value(64, 'v');

  ASSERT_OK(db_->Put(WriteOptions(), "key", write_ts, blob_value));

  Slice read_ts_slice(read_ts);
  ReadOptions read_options;
  read_options.timestamp = &read_ts_slice;
  read_options.verify_checksums = true;

  std::string value;
  ASSERT_OK(db_->Get(read_options, "key", &value));
  ASSERT_EQ(value, blob_value);
}

TEST_F(DBBlobDirectWriteWithTimestampTest,
       MultiGetFromMemtableUsesFoundTimestampedKey) {
  const Comparator* comparator = test::BytewiseComparatorWithU64TsWrapper();
  Options options = GetBlobDirectWriteOptions(comparator);
  DestroyAndReopen(options);

  const std::string write_ts = EncodeTimestamp(5);
  const std::string read_ts = EncodeTimestamp(8);
  const std::string first_value(64, 'x');
  const std::string second_value(80, 'y');

  ASSERT_OK(db_->Put(WriteOptions(), "key0", write_ts, first_value));
  ASSERT_OK(db_->Put(WriteOptions(), "key1", write_ts, second_value));

  Slice read_ts_slice(read_ts);
  ReadOptions read_options;
  read_options.timestamp = &read_ts_slice;
  read_options.verify_checksums = true;

  std::array<Slice, 2> keys{{Slice("key0"), Slice("key1")}};
  std::array<PinnableSlice, 2> values;
  std::array<Status, 2> statuses;

  db_->MultiGet(read_options, db_->DefaultColumnFamily(), keys.size(),
                keys.data(), values.data(), statuses.data());

  ASSERT_OK(statuses[0]);
  ASSERT_EQ(values[0], first_value);

  ASSERT_OK(statuses[1]);
  ASSERT_EQ(values[1], second_value);
}

TEST_F(DBBlobDirectWriteWithTimestampTest,
       MultiGetEntityFromMemtableUsesFoundTimestampedKey) {
  const Comparator* comparator = test::BytewiseComparatorWithU64TsWrapper();
  Options options = GetBlobDirectWriteOptions(comparator);
  DestroyAndReopen(options);

  const std::string write_ts = EncodeTimestamp(7);
  const std::string read_ts = EncodeTimestamp(9);
  const std::string first_value(64, 'a');
  const std::string second_value(96, 'b');

  ASSERT_OK(db_->Put(WriteOptions(), "key0", write_ts, first_value));
  ASSERT_OK(db_->Put(WriteOptions(), "key1", write_ts, second_value));

  Slice read_ts_slice(read_ts);
  ReadOptions read_options;
  read_options.timestamp = &read_ts_slice;
  read_options.verify_checksums = true;

  std::array<Slice, 2> keys{{Slice("key0"), Slice("key1")}};
  std::array<PinnableWideColumns, 2> results;
  std::array<Status, 2> statuses;
  const WideColumns expected_first{{kDefaultWideColumnName, first_value}};
  const WideColumns expected_second{{kDefaultWideColumnName, second_value}};

  db_->MultiGetEntity(read_options, db_->DefaultColumnFamily(), keys.size(),
                      keys.data(), results.data(), statuses.data());

  ASSERT_OK(statuses[0]);
  ASSERT_EQ(results[0].columns(), expected_first);

  ASSERT_OK(statuses[1]);
  ASSERT_EQ(results[1].columns(), expected_second);
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

  std::string large1(100, 'A');
  std::string large2(100, 'B');
  std::string large3(100, 'C');
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

  std::string large1(100, 'X');
  std::string large2(100, 'Y');
  std::string large3(100, 'Z');
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
    std::string value(100, static_cast<char>('a' + (i % 26)));
    ASSERT_OK(Put(key, value));
  }

  // Verify all data is readable after rotations
  for (int i = 0; i < num_keys; i++) {
    std::string key = "rot_key" + std::to_string(i);
    std::string expected(100, static_cast<char>('a' + (i % 26)));
    ASSERT_EQ(Get(key), expected);
  }

  // Also verify after flush
  ASSERT_OK(Flush());
  for (int i = 0; i < num_keys; i++) {
    std::string key = "rot_key" + std::to_string(i);
    std::string expected(100, static_cast<char>('a' + (i % 26)));
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
  threads.reserve(num_threads);

  for (int t = 0; t < num_threads; t++) {
    threads.emplace_back([&, t]() {
      for (int i = 0; i < keys_per_thread; i++) {
        std::string key =
            "thread" + std::to_string(t) + "_key" + std::to_string(i);
        std::string value(100, static_cast<char>('a' + (t % 26)));
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
      std::string expected(100, static_cast<char>('a' + (t % 26)));
      ASSERT_EQ(Get(key), expected);
    }
  }
}

// High-concurrency test that exercises the backpressure path.
// Stalls BG flush via SyncPoint so pending_bytes accumulates and
// backpressure triggers deterministically, even on 2-core CI machines.
TEST_F(DBBlobDirectWriteTest, BackpressureHighConcurrency) {
  Options options = GetBlobDirectWriteOptions();
  options.blob_direct_write_partitions = 4;
  // buffer_size=1 means any pending bytes trigger backpressure.
  // This deterministically exercises the backpressure path without
  // fragile SyncPoint stalling. The test verifies no deadlocks,
  // data corruption, or dropped writes under heavy concurrency.
  options.blob_direct_write_buffer_size = 1;
  options.blob_file_size = 1024 * 1024;
  DestroyAndReopen(options);

  const int num_threads = 16;
  const int keys_per_thread = 500;
  const int value_size = 4096;
  std::vector<std::thread> threads;
  threads.reserve(num_threads);

  for (int t = 0; t < num_threads; t++) {
    threads.emplace_back([&, t]() {
      for (int i = 0; i < keys_per_thread; i++) {
        std::string key = "bp_t" + std::to_string(t) + "_k" + std::to_string(i);
        std::string value(value_size, static_cast<char>('a' + (t % 26)));
        ASSERT_OK(Put(key, value));
      }
    });
  }

  for (auto& t : threads) {
    t.join();
  }

  // Verify data integrity: all writes completed without deadlock or loss.
  for (int t = 0; t < num_threads; t++) {
    for (int i = 0; i < keys_per_thread; i += 50) {
      std::string key = "bp_t" + std::to_string(t) + "_k" + std::to_string(i);
      std::string expected(value_size, static_cast<char>('a' + (t % 26)));
      ASSERT_EQ(Get(key), expected);
    }
  }

  ASSERT_OK(Flush());
  for (int t = 0; t < num_threads; t++) {
    std::string key = "bp_t" + std::to_string(t) + "_k0";
    std::string expected(value_size, static_cast<char>('a' + (t % 26)));
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
    return std::string(100, static_cast<char>('a' + (i % 26)));
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
    return std::string(100, static_cast<char>('A' + (i % 26)));
  };
  WriteLargeValues(num_keys, 100, "nf_key", value_fn);
  Reopen(options);
  VerifyLargeValues(num_keys, 100, "nf_key", value_fn);
}

// Recovered orphan blob files must stay on disk while the original WALs are
// still live. Otherwise a later crash can replay the same WAL again and fail
// because the orphan blob file was prematurely purged.
TEST_F(DBBlobDirectWriteTest,
       RecoveryWithoutFlushKeepsResolvedOrphanFilesForFutureReopen) {
  Options options = GetBlobDirectWriteOptions();
  options.blob_direct_write_partitions = 1;
  options.blob_direct_write_buffer_size = 0;
  options.avoid_flush_during_recovery = true;
  options.avoid_flush_during_shutdown = true;
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);

  const std::string value(100, 'R');
  ASSERT_OK(Put("repeat_recovery_key", value));

  const auto blob_paths = GetBlobFilePaths();
  ASSERT_EQ(blob_paths.size(), 1u);
  const std::string orphan_blob_path = blob_paths.front();

  Close();

  Reopen(options);
  ASSERT_EQ(Get("repeat_recovery_key"), value);
  ASSERT_OK(env_->FileExists(orphan_blob_path));

  dbfull()->TEST_DeleteObsoleteFiles();
  ASSERT_OK(env_->FileExists(orphan_blob_path));

  Close();

  Reopen(options);
  ASSERT_EQ(Get("repeat_recovery_key"), value);
}

// A blob file can be MANIFEST-tracked at first, then become fully garbage and
// get dropped from MANIFEST by compaction while a live WAL still contains the
// original BlobIndex batch. PurgeObsoleteFiles must keep the file on disk until
// that WAL ages out so recovery can replay the batch again after a crash.
TEST_F(DBBlobDirectWriteTest,
       LiveWalKeepsObsoleteManifestBlobFileForFutureRecovery) {
  Options options = GetBlobDirectWriteOptions();
  options.blob_direct_write_partitions = 1;
  options.blob_direct_write_buffer_size = 0;
  options.disable_auto_compactions = true;
  options.avoid_flush_during_shutdown = true;
  CreateAndReopenWithCF({"hold"}, options);

  WriteBatch batch;
  const int num_victim_keys = 4;
  const std::string overwritten_value(100, 'Z');
  for (int i = 0; i < num_victim_keys; ++i) {
    ASSERT_OK(batch.Put(handles_[0], "victim" + std::to_string(i),
                        std::string(100, static_cast<char>('A' + i))));
  }
  ASSERT_OK(batch.Put(handles_[1], "hold_key", "h"));
  ASSERT_OK(db_->Write(WriteOptions(), &batch));

  ASSERT_OK(Flush(0));

  const auto blob_infos_initial = GetBlobFileInfoFromVersion();
  ASSERT_EQ(blob_infos_initial.size(), 1u);
  const uint64_t victim_blob_number = blob_infos_initial.front().file_number;
  const std::string victim_blob_path =
      BlobFileName(dbname_, victim_blob_number);
  ASSERT_OK(env_->FileExists(victim_blob_path));

  for (int i = 0; i < num_victim_keys; ++i) {
    ASSERT_OK(Put("victim" + std::to_string(i), overwritten_value));
  }
  ASSERT_OK(Flush(0));
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));

  ASSERT_FALSE(VersionContainsBlobFile(victim_blob_number))
      << "Victim blob file should have been dropped from MANIFEST first";

  dbfull()->TEST_DeleteObsoleteFiles();
  ASSERT_OK(env_->FileExists(victim_blob_path));

  Close();

  ReopenWithColumnFamilies({"default", "hold"}, options);
  for (int i = 0; i < num_victim_keys; ++i) {
    ASSERT_EQ(Get("victim" + std::to_string(i)), overwritten_value);
  }
  ASSERT_EQ(Get(1, "hold_key"), "h");
}

// Recovery must rebuild the same WAL-based protection for manifest-tracked
// blob files. Otherwise a blob file can survive reopen, become obsolete in the
// new process, and then get deleted while an older live WAL still references
// it.
TEST_F(DBBlobDirectWriteTest,
       RecoveryRebuildsWalProtectionForManifestBlobFileNeededByLiveWal) {
  Options options = GetBlobDirectWriteOptions();
  options.blob_direct_write_partitions = 1;
  options.blob_direct_write_buffer_size = 0;
  options.disable_auto_compactions = true;
  options.avoid_flush_during_recovery = true;
  options.avoid_flush_during_shutdown = true;
  CreateAndReopenWithCF({"hold"}, options);

  WriteBatch batch;
  const int num_victim_keys = 4;
  const std::string overwritten_value(100, 'Y');
  for (int i = 0; i < num_victim_keys; ++i) {
    ASSERT_OK(batch.Put(handles_[0], "victim" + std::to_string(i),
                        std::string(100, static_cast<char>('K' + i))));
  }
  ASSERT_OK(batch.Put(handles_[1], "hold_key", "h"));
  ASSERT_OK(db_->Write(WriteOptions(), &batch));

  ASSERT_OK(Flush(0));

  const auto blob_infos_initial = GetBlobFileInfoFromVersion();
  ASSERT_EQ(blob_infos_initial.size(), 1u);
  const uint64_t victim_blob_number = blob_infos_initial.front().file_number;
  const std::string victim_blob_path =
      BlobFileName(dbname_, victim_blob_number);
  ASSERT_OK(env_->FileExists(victim_blob_path));

  Close();

  ReopenWithColumnFamilies({"default", "hold"}, options);
  ASSERT_TRUE(VersionContainsBlobFile(victim_blob_number));
  ASSERT_EQ(Get(1, "hold_key"), "h");

  for (int i = 0; i < num_victim_keys; ++i) {
    ASSERT_OK(Put("victim" + std::to_string(i), overwritten_value));
  }
  ASSERT_OK(Flush(0));
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));

  ASSERT_FALSE(VersionContainsBlobFile(victim_blob_number))
      << "Victim blob file should have been dropped from MANIFEST after "
         "reopen";

  dbfull()->TEST_DeleteObsoleteFiles();
  ASSERT_OK(env_->FileExists(victim_blob_path));

  Close();

  ReopenWithColumnFamilies({"default", "hold"}, options);
  for (int i = 0; i < num_victim_keys; ++i) {
    ASSERT_EQ(Get("victim" + std::to_string(i)), overwritten_value);
  }
  ASSERT_EQ(Get(1, "hold_key"), "h");
}

// If a column family has already flushed past an old WAL, recovery must skip
// that WAL's BlobIndex entries for the CF even when the once-tracked blob file
// was later garbage-collected and removed from disk.
TEST_F(DBBlobDirectWriteTest,
       PointInTimeRecoverySkipsStaleBlobIndexWhenTrackedBlobMissing) {
  Options options = GetBlobDirectWriteOptions();
  options.blob_direct_write_partitions = 1;
  options.blob_direct_write_buffer_size = 0;
  options.disable_auto_compactions = true;
  options.avoid_flush_during_shutdown = true;
  options.max_write_buffer_number = 8;
  options.wal_recovery_mode = WALRecoveryMode::kPointInTimeRecovery;
  CreateAndReopenWithCF({"hold"}, options);

  WriteBatch batch;
  const int num_victim_keys = 4;
  const std::string final_value = "i";
  for (int i = 0; i < num_victim_keys; ++i) {
    ASSERT_OK(batch.Put(handles_[0], "victim" + std::to_string(i),
                        std::string(100, static_cast<char>('L' + i))));
  }
  ASSERT_OK(batch.Put(handles_[1], "hold_key", "h"));
  ASSERT_OK(db_->Write(WriteOptions(), &batch));
  const uint64_t stale_wal_number = dbfull()->TEST_LogfileNumber();

  auto* default_cfd = static_cast<ColumnFamilyHandleImpl*>(handles_[0])->cfd();
  auto* hold_cfd = static_cast<ColumnFamilyHandleImpl*>(handles_[1])->cfd();
  ASSERT_NE(default_cfd, nullptr);
  ASSERT_NE(hold_cfd, nullptr);

  ASSERT_OK(dbfull()->TEST_SwitchMemtable(default_cfd));
  ASSERT_NE(dbfull()->TEST_LogfileNumber(), stale_wal_number);

  ASSERT_OK(Flush(0));
  ASSERT_GT(default_cfd->GetLogNumber(), stale_wal_number);
  ASSERT_LE(hold_cfd->GetLogNumber(), stale_wal_number);

  const auto blob_infos_initial = GetBlobFileInfoFromVersion();
  ASSERT_EQ(blob_infos_initial.size(), 1u);
  const uint64_t victim_blob_number = blob_infos_initial.front().file_number;
  const std::string victim_blob_path =
      BlobFileName(dbname_, victim_blob_number);
  ASSERT_OK(env_->FileExists(victim_blob_path));

  for (int i = 0; i < num_victim_keys; ++i) {
    ASSERT_OK(Put("victim" + std::to_string(i), final_value));
  }
  ASSERT_OK(Flush(0));
  ASSERT_OK(
      db_->CompactRange(CompactRangeOptions(), handles_[0], nullptr, nullptr));

  ASSERT_FALSE(VersionContainsBlobFile(victim_blob_number))
      << "Victim blob file should have been dropped from MANIFEST first";

  // Reproduce the post-GC state from stress logs: another CF still keeps the
  // WAL alive, but this once-tracked blob file is gone.
  Status delete_s = env_->DeleteFile(victim_blob_path);
  ASSERT_TRUE(delete_s.ok() || delete_s.IsNotFound()) << delete_s.ToString();

  Close();

  ReopenWithColumnFamilies({"default", "hold"}, options);
  for (int i = 0; i < num_victim_keys; ++i) {
    ASSERT_EQ(Get("victim" + std::to_string(i)), final_value);
  }
  ASSERT_EQ(Get(1, "hold_key"), "h");
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
    std::string value(100, static_cast<char>('a' + (i % 26)));
    ASSERT_OK(Put(key, value));
  }

  // Flush and reopen
  ASSERT_OK(Flush());
  Reopen(options);

  // Verify all data
  for (int i = 0; i < num_keys; i++) {
    std::string key = "rot_rec_key" + std::to_string(i);
    std::string expected(100, static_cast<char>('a' + (i % 26)));
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
    std::string value(100, static_cast<char>('A' + (i % 26)));
    ASSERT_OK(Put(key, value));
  }

  // Close and reopen without flush
  Reopen(options);

  for (int i = 0; i < num_keys; i++) {
    std::string key = "rot_nf_key" + std::to_string(i);
    std::string expected(100, static_cast<char>('A' + (i % 26)));
    ASSERT_EQ(Get(key), expected);
  }
}

TEST_F(DBBlobDirectWriteTest, CompressionBasic) {
  if (!Snappy_Supported()) {
    ROCKSDB_GTEST_SKIP("Snappy compression not available");
    return;
  }
  Options options = GetBlobDirectWriteOptions();
  options.blob_compression_type = kSnappyCompression;
  options.blob_direct_write_partitions = 1;
  DestroyAndReopen(options);

  // Write compressible data (repeated chars compress well with snappy)
  const int num_keys = 20;
  for (int i = 0; i < num_keys; i++) {
    std::string key = "comp_key" + std::to_string(i);
    std::string value(200,
                      static_cast<char>('a' + (i % 3)));  // Highly compressible
    ASSERT_OK(Put(key, value));
  }

  // Verify reads before flush (reads from pending records, decompresses)
  for (int i = 0; i < num_keys; i++) {
    std::string key = "comp_key" + std::to_string(i);
    std::string expected(200, static_cast<char>('a' + (i % 3)));
    ASSERT_EQ(Get(key), expected);
  }

  // Flush and verify reads from disk (BlobFileReader handles decompression)
  ASSERT_OK(Flush());
  for (int i = 0; i < num_keys; i++) {
    std::string key = "comp_key" + std::to_string(i);
    std::string expected(200, static_cast<char>('a' + (i % 3)));
    ASSERT_EQ(Get(key), expected);
  }
}

TEST_F(DBBlobDirectWriteTest, CompressionWithReopen) {
  if (!Snappy_Supported()) {
    ROCKSDB_GTEST_SKIP("Snappy compression not available");
    return;
  }
  Options options = GetBlobDirectWriteOptions();
  options.blob_compression_type = kSnappyCompression;
  DestroyAndReopen(options);

  const int num_keys = 30;
  for (int i = 0; i < num_keys; i++) {
    std::string key = "creopen_key" + std::to_string(i);
    std::string value(150, static_cast<char>('x' + (i % 3)));
    ASSERT_OK(Put(key, value));
  }

  ASSERT_OK(Flush());
  Reopen(options);

  for (int i = 0; i < num_keys; i++) {
    std::string key = "creopen_key" + std::to_string(i);
    std::string expected(150, static_cast<char>('x' + (i % 3)));
    ASSERT_EQ(Get(key), expected);
  }
}

TEST_F(DBBlobDirectWriteTest, CompressionReducesFileSize) {
  if (!Snappy_Supported()) {
    ROCKSDB_GTEST_SKIP("Snappy compression not available");
    return;
  }
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
    std::string value(100, static_cast<char>('a' + (i % 26)));
    ASSERT_OK(batch.Put(key, value));
  }
  ASSERT_OK(db_->Write(WriteOptions(), &batch));

  // Verify all values
  for (int i = 0; i < 10; i++) {
    std::string key = "pw_batch_key" + std::to_string(i);
    std::string expected(100, static_cast<char>('a' + (i % 26)));
    ASSERT_EQ(Get(key), expected);
  }

  ASSERT_OK(Flush());
  for (int i = 0; i < 10; i++) {
    std::string key = "pw_batch_key" + std::to_string(i);
    std::string expected(100, static_cast<char>('a' + (i % 26)));
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
    std::string value(100, static_cast<char>('a' + (i % 26)));
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
    std::string expected(100, static_cast<char>('a' + (i % 26)));
    ASSERT_EQ(Get(key), expected);
  }

  // Verify after flush too
  ASSERT_OK(Flush());
  for (int i = 0; i < num_keys; i++) {
    std::string key = "cache_key" + std::to_string(i);
    std::string expected(100, static_cast<char>('a' + (i % 26)));
    ASSERT_EQ(Get(key), expected);
  }
}

TEST_F(DBBlobDirectWriteTest, CompressionTimingMetric) {
  if (!Snappy_Supported()) {
    ROCKSDB_GTEST_SKIP("Snappy compression not available");
    return;
  }
  Options options = GetBlobDirectWriteOptions();
  options.blob_compression_type = kSnappyCompression;
  options.statistics = CreateDBStatistics();
  DestroyAndReopen(options);

  HistogramData before_data;
  options.statistics->histogramData(BLOB_DB_COMPRESSION_MICROS, &before_data);

  // Write compressible data
  for (int i = 0; i < 10; i++) {
    std::string key = "comp_time_key" + std::to_string(i);
    std::string value(200, static_cast<char>('a' + (i % 3)));
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
    std::string value(100, static_cast<char>('a' + (i % 26)));
    ASSERT_OK(Put(key, value));
  }

  // Flush to seal remaining files
  ASSERT_OK(Flush());

  ASSERT_GT(listener->creation_started.load(), 0);
  ASSERT_GT(listener->creation_completed.load(), 0);
}

TEST_F(DBBlobDirectWriteTest, CompressionWithRotation) {
  if (!Snappy_Supported()) {
    ROCKSDB_GTEST_SKIP("Snappy compression not available");
    return;
  }
  Options options = GetBlobDirectWriteOptions();
  options.blob_compression_type = kSnappyCompression;
  options.blob_file_size = 512;  // Small to force rotation
  options.blob_direct_write_partitions = 1;
  DestroyAndReopen(options);

  const int num_keys = 30;
  for (int i = 0; i < num_keys; i++) {
    std::string key = "crot_key" + std::to_string(i);
    std::string value(100, static_cast<char>('a' + (i % 26)));
    ASSERT_OK(Put(key, value));
  }

  // Verify before flush
  for (int i = 0; i < num_keys; i++) {
    std::string key = "crot_key" + std::to_string(i);
    std::string expected(100, static_cast<char>('a' + (i % 26)));
    ASSERT_EQ(Get(key), expected);
  }

  // Verify after flush
  ASSERT_OK(Flush());
  for (int i = 0; i < num_keys; i++) {
    std::string key = "crot_key" + std::to_string(i);
    std::string expected(100, static_cast<char>('a' + (i % 26)));
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
      "BlobFilePartitionManager::BGPeriodicFlush:SubmitFlush",
      [&](void* /*arg*/) {
        periodic_flush_count.fetch_add(1, std::memory_order_relaxed);
        MutexLock lock(&flush_mu);
        flush_cv.SignalAll();
      });
  // Delay FlushAllOpenFiles (called from Put fast path) so the periodic
  // timer has a chance to fire while pending records are still queued.
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency({
      {"BlobFilePartitionManager::BGPeriodicFlush:SubmitFlush",
       "BlobFilePartitionManager::FlushAllOpenFiles:Begin"},
  });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  // Write data well below the high-water mark so only the periodic timer
  // triggers a flush (not backpressure).
  std::string large_value(200, 'v');
  ASSERT_OK(Put("periodic_key", large_value));

  ASSERT_EQ(Get("periodic_key"), large_value);

  for (int i = 0; i < 5; i++) {
    std::string key = "periodic_key_" + std::to_string(i);
    std::string value(200 + i, static_cast<char>('a' + (i % 26)));
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
    std::string expected(200 + i, static_cast<char>('a' + (i % 26)));
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
  writers.reserve(num_writers);
  for (int t = 0; t < num_writers; t++) {
    writers.emplace_back([&, t]() {
      int i = 0;
      while (!stop.load(std::memory_order_relaxed)) {
        std::string key = "w" + std::to_string(t) + "_" + std::to_string(i);
        std::string value(100, static_cast<char>('a' + (i % 26)));
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
  readers.reserve(num_readers);
  for (int t = 0; t < num_readers; t++) {
    readers.emplace_back([&, t]() {
      while (!stop.load(std::memory_order_relaxed)) {
        int idx = t % initial_keys;
        std::string key = "init_" + std::to_string(idx);
        std::string expected(100 + idx, static_cast<char>('a' + (idx % 26)));
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
// Verifies that blob files are synced before the WAL entry when sync=true,
// and that data survives reopen. Tests both sync mode (buffer_size=0) and
// deferred flush mode (buffer_size>0).
TEST_F(DBBlobDirectWriteTest, SyncWrite) {
  for (uint64_t buffer_size : {uint64_t{0}, uint64_t{4096}}) {
    SCOPED_TRACE("buffer_size=" + std::to_string(buffer_size));

    Options options = GetBlobDirectWriteOptions();
    options.blob_direct_write_buffer_size = buffer_size;
    DestroyAndReopen(options);

    // Count blob file syncs via SyncPoint callback.
    std::atomic<int> blob_sync_count{0};
    SyncPoint::GetInstance()->SetCallBack(
        "BlobFilePartitionManager::SyncAllOpenFiles:BeforeSync",
        [&](void* /*arg*/) { blob_sync_count.fetch_add(1); });
    SyncPoint::GetInstance()->EnableProcessing();

    WriteOptions wo;
    wo.sync = true;

    std::string large_value(200, 'S');
    ASSERT_OK(db_->Put(wo, "sync_key1", large_value));
    ASSERT_OK(db_->Put(wo, "sync_key2", large_value));

    // Blob sync should have been called at least once per Put.
    ASSERT_GE(blob_sync_count.load(), 2);

    ASSERT_EQ(Get("sync_key1"), large_value);
    ASSERT_EQ(Get("sync_key2"), large_value);

    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();

    Reopen(options);
    ASSERT_EQ(Get("sync_key1"), large_value);
    ASSERT_EQ(Get("sync_key2"), large_value);
  }
}

// Regression test for the pre-WAL flush visibility race. While
// FlushAllOpenFiles() owns a partition's active writer state, a same-partition
// write must not be able to append behind that drain.
TEST_F(DBBlobDirectWriteTest,
       FlushAllOpenFilesBlocksSamePartitionWriteUntilFlushCompletes) {
  Options options = GetBlobDirectWriteOptions();
  options.blob_direct_write_partitions = 1;
  options.blob_direct_write_buffer_size = 4096;
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);

  auto* cfh = static_cast<ColumnFamilyHandleImpl*>(db_->DefaultColumnFamily());
  ASSERT_NE(cfh, nullptr);
  auto* cfd = cfh->cfd();
  ASSERT_NE(cfd, nullptr);
  auto* mgr = cfd->blob_partition_manager();
  ASSERT_NE(mgr, nullptr);

  const std::string seed_value(200, 'F');
  uint64_t seed_file_number = 0;
  uint64_t seed_offset = 0;
  uint64_t seed_size = 0;
  ASSERT_OK(mgr->WriteBlob(WriteOptions(), cfd->GetID(), kNoCompression,
                           Slice("seed"), Slice(seed_value), &seed_file_number,
                           &seed_offset, &seed_size));
  ASSERT_EQ(seed_size, seed_value.size());

  std::mutex mu;
  std::condition_variable cv;
  bool flush_paused = false;
  bool release_flush = false;
  bool writer_waiting = false;
  bool writer_done = false;
  int flush_pause_calls = 0;
  Status flush_status;
  Status write_status;
  uint64_t blocked_file_number = 0;
  uint64_t blocked_offset = 0;
  uint64_t blocked_size = 0;

  auto wait_for = [&](const char* what, const std::function<bool()>& pred) {
    std::unique_lock<std::mutex> lock(mu);
    ASSERT_TRUE(cv.wait_for(lock, std::chrono::seconds(10), pred))
        << "Timed out waiting for " << what;
  };

  SyncPoint::GetInstance()->SetCallBack(
      "BlobFilePartitionManager::FlushPendingRecords:Begin", [&](void*) {
        std::unique_lock<std::mutex> lock(mu);
        if (flush_pause_calls++ == 0) {
          flush_paused = true;
          cv.notify_all();
          cv.wait(lock, [&] { return release_flush; });
        }
      });
  SyncPoint::GetInstance()->SetCallBack(
      "BlobFilePartitionManager::WriteBlob:WaitOnSyncBarrier", [&](void*) {
        std::lock_guard<std::mutex> lock(mu);
        writer_waiting = true;
        cv.notify_all();
      });
  SyncPoint::GetInstance()->EnableProcessing();

  std::thread flush_thread(
      [&] { flush_status = mgr->FlushAllOpenFiles(WriteOptions()); });
  wait_for("flush to pause before draining pending records",
           [&] { return flush_paused; });

  const std::string blocked_value(200, 'G');
  std::thread writer_thread([&] {
    write_status =
        mgr->WriteBlob(WriteOptions(), cfd->GetID(), kNoCompression,
                       Slice("blocked"), Slice(blocked_value),
                       &blocked_file_number, &blocked_offset, &blocked_size);
    {
      std::lock_guard<std::mutex> lock(mu);
      writer_done = true;
    }
    cv.notify_all();
  });
  wait_for("writer to block on the flush barrier",
           [&] { return writer_waiting; });

  {
    std::lock_guard<std::mutex> lock(mu);
    ASSERT_FALSE(writer_done);
    release_flush = true;
  }
  cv.notify_all();

  flush_thread.join();
  writer_thread.join();

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();

  ASSERT_OK(flush_status);
  ASSERT_OK(write_status);
  ASSERT_EQ(blocked_file_number, seed_file_number);
  ASSERT_GT(blocked_offset, seed_offset);
  ASSERT_EQ(blocked_size, blocked_value.size());

  ASSERT_OK(mgr->FlushAllOpenFiles(WriteOptions()));
  ASSERT_GE(GetUnderlyingFileSize(BlobFileName(dbname_, blocked_file_number)),
            blocked_offset + blocked_size);
}

// Regression test for the active-writer Sync()/Flush() race. While
// SyncAllOpenFiles() owns the partition's active writer, a same-partition
// write must not be able to append to that writer until the sync finishes.
TEST_F(DBBlobDirectWriteTest,
       SyncAllOpenFilesBlocksSamePartitionWriteUntilSyncCompletes) {
  Options options = GetBlobDirectWriteOptions();
  options.blob_direct_write_partitions = 1;
  options.blob_direct_write_buffer_size = 4096;
  DestroyAndReopen(options);

  const std::string seed_value(200, 'S');
  const std::string blocked_value(200, 'B');
  ASSERT_OK(Put("seed", seed_value));

  auto* cfh = static_cast<ColumnFamilyHandleImpl*>(db_->DefaultColumnFamily());
  auto* mgr = cfh->cfd()->blob_partition_manager();
  ASSERT_NE(mgr, nullptr);

  std::mutex mu;
  std::condition_variable cv;
  bool sync_paused = false;
  bool release_sync = false;
  bool writer_waiting = false;
  bool writer_done = false;
  int sync_pause_calls = 0;
  Status sync_status;
  Status write_status;

  auto wait_for = [&](const char* what, const std::function<bool()>& pred) {
    std::unique_lock<std::mutex> lock(mu);
    ASSERT_TRUE(cv.wait_for(lock, std::chrono::seconds(10), pred))
        << "Timed out waiting for " << what;
  };

  SyncPoint::GetInstance()->SetCallBack(
      "BlobFilePartitionManager::SyncAllOpenFiles:BeforeSync", [&](void*) {
        std::unique_lock<std::mutex> lock(mu);
        if (sync_pause_calls++ == 0) {
          sync_paused = true;
          cv.notify_all();
          cv.wait(lock, [&] { return release_sync; });
        }
      });
  SyncPoint::GetInstance()->SetCallBack(
      "BlobFilePartitionManager::WriteBlob:WaitOnSyncBarrier", [&](void*) {
        std::lock_guard<std::mutex> lock(mu);
        writer_waiting = true;
        cv.notify_all();
      });
  SyncPoint::GetInstance()->EnableProcessing();

  std::thread sync_thread([&] {
    WriteOptions wo;
    wo.sync = true;
    sync_status = mgr->SyncAllOpenFiles(wo);
  });
  wait_for("sync to pause before syncing the active blob file",
           [&] { return sync_paused; });

  std::thread writer_thread([&] {
    write_status = Put("blocked", blocked_value);
    {
      std::lock_guard<std::mutex> lock(mu);
      writer_done = true;
    }
    cv.notify_all();
  });
  wait_for("writer to block on the sync barrier",
           [&] { return writer_waiting; });

  {
    std::lock_guard<std::mutex> lock(mu);
    ASSERT_FALSE(writer_done);
    release_sync = true;
  }
  cv.notify_all();

  sync_thread.join();
  writer_thread.join();

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();

  ASSERT_OK(sync_status);
  ASSERT_OK(write_status);
  ASSERT_EQ(Get("seed"), seed_value);
  ASSERT_EQ(Get("blocked"), blocked_value);
}

// Test that non-sync writes do NOT trigger blob file sync (for performance).
TEST_F(DBBlobDirectWriteTest, NonSyncWriteSkipsBlobSync) {
  Options options = GetBlobDirectWriteOptions();
  options.blob_direct_write_buffer_size = 4096;
  DestroyAndReopen(options);

  std::atomic<int> blob_sync_count{0};
  SyncPoint::GetInstance()->SetCallBack(
      "BlobFilePartitionManager::SyncAllOpenFiles:BeforeSync",
      [&](void* /*arg*/) { blob_sync_count.fetch_add(1); });
  SyncPoint::GetInstance()->EnableProcessing();

  WriteOptions wo;
  wo.sync = false;

  std::string large_value(200, 'N');
  ASSERT_OK(db_->Put(wo, "nosync_key1", large_value));
  ASSERT_OK(db_->Put(wo, "nosync_key2", large_value));

  // Non-sync writes should NOT trigger blob file sync.
  ASSERT_EQ(blob_sync_count.load(), 0);

  ASSERT_EQ(Get("nosync_key1"), large_value);
  ASSERT_EQ(Get("nosync_key2"), large_value);

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
}

// Test sync=true with WriteBatch (batch path, not DBImpl::Put fast path).
TEST_F(DBBlobDirectWriteTest, SyncWriteBatch) {
  Options options = GetBlobDirectWriteOptions();
  DestroyAndReopen(options);

  std::atomic<int> blob_sync_count{0};
  SyncPoint::GetInstance()->SetCallBack(
      "BlobFilePartitionManager::SyncAllOpenFiles:BeforeSync",
      [&](void* /*arg*/) { blob_sync_count.fetch_add(1); });
  SyncPoint::GetInstance()->EnableProcessing();

  WriteOptions wo;
  wo.sync = true;

  std::string large_value(200, 'B');
  WriteBatch batch;
  ASSERT_OK(batch.Put("batch_key1", large_value));
  ASSERT_OK(batch.Put("batch_key2", large_value));
  ASSERT_OK(db_->Write(wo, &batch));

  // Blob sync should have been called for the batch write.
  ASSERT_GE(blob_sync_count.load(), 1);

  ASSERT_EQ(Get("batch_key1"), large_value);
  ASSERT_EQ(Get("batch_key2"), large_value);

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();

  Reopen(options);
  ASSERT_EQ(Get("batch_key1"), large_value);
  ASSERT_EQ(Get("batch_key2"), large_value);
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
  ASSERT_OK(DestroyDB(dbname_, options));

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
  ASSERT_OK(DestroyDB(dbname_, options));

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
  ASSERT_OK(DestroyDB(dbname_, options));

  TransactionDB* txn_db = nullptr;
  ASSERT_OK(TransactionDB::Open(options, txn_db_options, dbname_, &txn_db));

  WriteOptions wo;
  Transaction* txn = txn_db->BeginTransaction(wo);
  ASSERT_NE(txn, nullptr);
  ASSERT_OK(txn->SetName("blob_txn_1"));

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

  ASSERT_OK(db_->DestroyColumnFamilyHandle(cf_handle));
}

// Regression test: PurgeObsoleteFiles must not delete blob files created
// after FindObsoleteFiles snapshots the active blob file set. Blob direct
// write opens new files without db_mutex_ (the Put fast path calls WriteBlob
// before WriteImpl), so a race exists between the snapshot and the directory
// scan if PurgeObsoleteFiles doesn't account for newly allocated file numbers.
TEST_F(DBBlobDirectWriteTest, PurgeDoesNotDeleteNewlyCreatedBlobFiles) {
  Options options = GetBlobDirectWriteOptions();
  options.blob_direct_write_partitions = 1;
  options.blob_direct_write_buffer_size = 0;  // sync mode
  options.delete_obsolete_files_period_micros = 0;
  options.disable_auto_compactions = true;
  Reopen(options);

  // Write + flush initial data.
  ASSERT_OK(Put("key0", std::string(100, 'a')));
  ASSERT_OK(Flush());

  // Orchestrate the race:
  // 1. Write thread creates blob file via Put fast path (no db_mutex)
  // 2. Write thread pauses after file is on disk but BEFORE WriteImpl
  // 3. Flush thread runs FindObsoleteFiles — snapshots active blobs
  //    (includes the new file since AddFilePartitionMapping is before
  //    NewWritableFile). BUT we need to test the case where the snapshot
  //    does NOT include the file.
  //
  // The actual race is: FindObsoleteFiles snapshots active blobs, THEN
  // a writer allocates a file number + creates a file. The file appears
  // in the directory scan but not in the snapshot.
  //
  // To reproduce: we pause FindObsoleteFiles AFTER the snapshot, inject
  // a new blob file directly into the directory (simulating a concurrent
  // writer), and verify PurgeObsoleteFiles doesn't delete it.

  // Find the current next file number — any blob file with this number
  // or higher should be protected by min_blob_file_number_to_keep.
  uint64_t next_file_before =
      dbfull()->GetVersionSet()->current_next_file_number();

  // Create a "phantom" blob file that simulates a file created by a
  // concurrent writer after FindObsoleteFiles snapshots the active set.
  // This file is on disk but NOT in file_to_partition_ or blob_live_set.
  uint64_t phantom_number = next_file_before + 100;
  std::string phantom_path = BlobFileName(dbname_, phantom_number);
  {
    std::unique_ptr<WritableFile> f;
    ASSERT_OK(env_->NewWritableFile(phantom_path, &f, EnvOptions()));
    ASSERT_OK(f->Append("phantom blob data"));
    ASSERT_OK(f->Close());
  }
  ASSERT_OK(env_->FileExists(phantom_path));

  // Trigger FindObsoleteFiles + PurgeObsoleteFiles via Flush.
  ASSERT_OK(Put("key1", std::string(100, 'b')));
  ASSERT_OK(Flush());

  // Without min_blob_file_number_to_keep: the phantom file is on disk,
  // not in blob_live_set, not in active_blob -> gets deleted.
  // With the fix: phantom_number >= min_blob_file_number_to_keep -> kept.
  Status exists = env_->FileExists(phantom_path);
  ASSERT_OK(exists) << "Phantom blob file " << phantom_number
                    << " was deleted by PurgeObsoleteFiles. "
                    << "min_blob_file_number_to_keep should have protected it.";

  // Clean up.
  ASSERT_OK(env_->DeleteFile(phantom_path));
}

// Regression test: a direct-write read can cache a BlobFileReader for an
// unsealed blob file (opened via footer-skip retry). When shutdown sealing
// finalizes that file, the cached reader must be evicted so the next lookup
// sees the footer and final file size rather than the stale pre-seal view.
TEST_F(DBBlobDirectWriteTest, ShutdownSealEvictsCachedBlobReader) {
  Options options = GetBlobDirectWriteOptions();
  options.blob_direct_write_partitions = 1;
  options.blob_direct_write_buffer_size = 0;  // Force direct disk writes.
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);

  auto* cfh = static_cast<ColumnFamilyHandleImpl*>(db_->DefaultColumnFamily());
  ASSERT_NE(cfh, nullptr);
  auto* cfd = cfh->cfd();
  ASSERT_NE(cfd, nullptr);
  auto* mgr = cfd->blob_partition_manager();
  ASSERT_NE(mgr, nullptr);
  auto* blob_file_cache = cfd->blob_file_cache();
  ASSERT_NE(blob_file_cache, nullptr);

  ASSERT_OK(Put("k", std::string(100, 'x')));

  std::unordered_set<uint64_t> active_files;
  mgr->GetActiveBlobFileNumbers(&active_files);
  ASSERT_EQ(active_files.size(), 1u);
  const uint64_t blob_file_number = *active_files.begin();

  CacheHandleGuard<BlobFileReader> unsealed_reader;
  ASSERT_OK(blob_file_cache->GetBlobFileReader(
      ReadOptions(), blob_file_number, &unsealed_reader,
      /*allow_footer_skip_retry=*/true));
  ASSERT_FALSE(unsealed_reader.GetValue()->HasFooter());
  const uint64_t pre_seal_size = unsealed_reader.GetValue()->GetFileSize();
  unsealed_reader.Reset();

  std::vector<BlobFileAddition> additions;
  ASSERT_OK(mgr->SealAllPartitions(WriteOptions(), &additions,
                                   /*seal_all=*/true));
  ASSERT_EQ(additions.size(), 1u);
  ASSERT_EQ(additions[0].GetBlobFileNumber(), blob_file_number);

  const std::string blob_path = BlobFileName(dbname_, blob_file_number);
  uint64_t sealed_file_size = 0;
  ASSERT_OK(env_->GetFileSize(blob_path, &sealed_file_size));
  ASSERT_GT(sealed_file_size, pre_seal_size);

  CacheHandleGuard<BlobFileReader> sealed_reader;
  ASSERT_OK(blob_file_cache->GetBlobFileReader(
      ReadOptions(), blob_file_number, &sealed_reader,
      /*allow_footer_skip_retry=*/true));
  EXPECT_TRUE(sealed_reader.GetValue()->HasFooter());
  EXPECT_EQ(sealed_reader.GetValue()->GetFileSize(), sealed_file_size);

  // Release the cache handle and evict so TEST_VerifyNoObsoleteFilesCached
  // (called at DB close) does not find a stale cache entry for a file that
  // is no longer tracked as active (it has been sealed but not yet committed
  // to MANIFEST in this test scenario).
  sealed_reader.Reset();
  blob_file_cache->Evict(blob_file_number);
}

// Regression test: if an active-file read hits a cached BlobFileReader with a
// stale file_size_, the corruption retry must reopen uncached, refresh the
// cache with that reader, and avoid another reopen on the next lookup.
TEST_F(DBBlobDirectWriteTest, ActiveReadRetryUsesUncachedBlobReader) {
  Options options = GetBlobDirectWriteOptions();
  options.statistics = CreateDBStatistics();
  options.blob_direct_write_partitions = 1;
  options.blob_direct_write_buffer_size = 0;  // Force direct disk writes.
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);

  auto* cfh = static_cast<ColumnFamilyHandleImpl*>(db_->DefaultColumnFamily());
  ASSERT_NE(cfh, nullptr);
  auto* cfd = cfh->cfd();
  ASSERT_NE(cfd, nullptr);
  auto* mgr = cfd->blob_partition_manager();
  ASSERT_NE(mgr, nullptr);
  auto* blob_file_cache = cfd->blob_file_cache();
  ASSERT_NE(blob_file_cache, nullptr);

  ASSERT_OK(Put("k1", std::string(100, 'a')));

  std::unordered_set<uint64_t> active_files;
  mgr->GetActiveBlobFileNumbers(&active_files);
  ASSERT_EQ(active_files.size(), 1u);
  const uint64_t blob_file_number = *active_files.begin();

  CacheHandleGuard<BlobFileReader> stale_reader;
  ASSERT_OK(blob_file_cache->GetBlobFileReader(
      ReadOptions(), blob_file_number, &stale_reader,
      /*allow_footer_skip_retry=*/true));
  ASSERT_FALSE(stale_reader.GetValue()->HasFooter());
  const uint64_t stale_file_size = stale_reader.GetValue()->GetFileSize();
  const uint64_t opens_before_retry =
      options.statistics->getTickerCount(NO_FILE_OPENS);
  stale_reader.Reset();

  ASSERT_OK(Put("k2", std::string(100, 'b')));
  mgr->GetActiveBlobFileNumbers(&active_files);
  ASSERT_EQ(active_files.size(), 1u);
  ASSERT_EQ(*active_files.begin(), blob_file_number);

  const std::string blob_path = BlobFileName(dbname_, blob_file_number);
  uint64_t current_file_size = 0;
  ASSERT_OK(env_->GetFileSize(blob_path, &current_file_size));
  ASSERT_GT(current_file_size, stale_file_size);

  ASSERT_EQ(Get("k2"), std::string(100, 'b'));
  ASSERT_EQ(options.statistics->getTickerCount(NO_FILE_OPENS),
            opens_before_retry + 1);

  CacheHandleGuard<BlobFileReader> post_retry_reader;
  ASSERT_OK(blob_file_cache->GetBlobFileReader(
      ReadOptions(), blob_file_number, &post_retry_reader,
      /*allow_footer_skip_retry=*/true));
  ASSERT_EQ(options.statistics->getTickerCount(NO_FILE_OPENS),
            opens_before_retry + 1);
  ASSERT_NE(post_retry_reader.GetValue(), nullptr);
  ASSERT_FALSE(post_retry_reader.GetValue()->HasFooter());
  ASSERT_EQ(post_retry_reader.GetValue()->GetFileSize(), current_file_size);
}

// H2: Reopen without enable_blob_direct_write must not lose data.
// Blob files sealed during shutdown are not registered in the MANIFEST.
// Orphan recovery must run unconditionally to register them before
// DeleteObsoleteFiles can purge them.
TEST_F(DBBlobDirectWriteTest, ReopenWithoutDirectWrite) {
  Options options = GetBlobDirectWriteOptions();
  options.blob_direct_write_partitions = 2;
  DestroyAndReopen(options);

  const int num_keys = 30;
  auto value_fn = [](int i, int) -> std::string {
    return std::string(100 + i, static_cast<char>('a' + (i % 26)));
  };
  WriteLargeValues(num_keys, 100, "reopen_key", value_fn);

  // Also write some data that gets flushed (registered in MANIFEST).
  ASSERT_OK(Flush());

  // Write more data WITHOUT flush — these blobs are sealed during Close
  // but not registered in the MANIFEST.
  WriteLargeValues(num_keys, 100, "unflushed_key", value_fn);

  // Reopen with blob direct write DISABLED.
  Options options_no_direct_write = CurrentOptions();
  options_no_direct_write.enable_blob_files = true;
  options_no_direct_write.min_blob_size = 10;
  options_no_direct_write.enable_blob_direct_write = false;
  Reopen(options_no_direct_write);

  // All data must survive — both flushed and unflushed.
  VerifyLargeValues(num_keys, 100, "reopen_key", value_fn);
  VerifyLargeValues(num_keys, 100, "unflushed_key", value_fn);

  // Reopen again (still without direct write) to verify MANIFEST is stable.
  Reopen(options_no_direct_write);
  VerifyLargeValues(num_keys, 100, "reopen_key", value_fn);
  VerifyLargeValues(num_keys, 100, "unflushed_key", value_fn);
}

// H2 variant: reopen with blob files completely disabled.
TEST_F(DBBlobDirectWriteTest, ReopenWithBlobFilesDisabled) {
  Options options = GetBlobDirectWriteOptions();
  DestroyAndReopen(options);

  const int num_keys = 20;
  auto value_fn = [](int i, int) -> std::string {
    return std::string(100, static_cast<char>('Z' - (i % 26)));
  };

  // Write data and flush (registers blob files in MANIFEST).
  WriteLargeValues(num_keys, 100, "bfdis_key", value_fn);
  ASSERT_OK(Flush());

  // Write more data WITHOUT flush.
  WriteLargeValues(num_keys, 100, "bfdis_unfl_key", value_fn);

  // Reopen with blob files completely disabled.
  Options options_no_blobs = CurrentOptions();
  options_no_blobs.enable_blob_files = false;
  options_no_blobs.enable_blob_direct_write = false;
  Reopen(options_no_blobs);

  // All data must survive.
  VerifyLargeValues(num_keys, 100, "bfdis_key", value_fn);
  VerifyLargeValues(num_keys, 100, "bfdis_unfl_key", value_fn);
}

// H6: Multi-CF orphan recovery.
// Blob files sealed during shutdown must be recovered under the correct CF.
TEST_F(DBBlobDirectWriteTest, MultiCFOrphanRecovery) {
  Options options = GetBlobDirectWriteOptions();
  options.blob_direct_write_partitions = 1;
  DestroyAndReopen(options);

  // Create a second column family with blob direct write.
  ColumnFamilyOptions cf_opts;
  cf_opts.enable_blob_files = true;
  cf_opts.enable_blob_direct_write = true;
  cf_opts.min_blob_size = 10;
  cf_opts.blob_direct_write_partitions = 1;
  ColumnFamilyHandle* cf_handle = nullptr;
  ASSERT_OK(db_->CreateColumnFamily(cf_opts, "data_cf", &cf_handle));

  // Write blob data to both CFs.
  const int num_keys = 20;
  for (int i = 0; i < num_keys; i++) {
    std::string key = "cf0_key" + std::to_string(i);
    std::string value(100, static_cast<char>('A' + (i % 26)));
    ASSERT_OK(db_->Put(WriteOptions(), key, value));
  }
  for (int i = 0; i < num_keys; i++) {
    std::string key = "cf1_key" + std::to_string(i);
    std::string value(100, static_cast<char>('a' + (i % 26)));
    ASSERT_OK(db_->Put(WriteOptions(), cf_handle, key, value));
  }

  // Flush both CFs to register some blob files.
  ASSERT_OK(db_->Flush(FlushOptions()));
  ASSERT_OK(db_->Flush(FlushOptions(), cf_handle));

  // Write more data to both CFs WITHOUT flush — orphan scenario.
  for (int i = 0; i < num_keys; i++) {
    std::string key = "cf0_unfl_key" + std::to_string(i);
    std::string value(100, static_cast<char>('X' - (i % 10)));
    ASSERT_OK(db_->Put(WriteOptions(), key, value));
  }
  for (int i = 0; i < num_keys; i++) {
    std::string key = "cf1_unfl_key" + std::to_string(i);
    std::string value(100, static_cast<char>('x' - (i % 10)));
    ASSERT_OK(db_->Put(WriteOptions(), cf_handle, key, value));
  }

  ASSERT_OK(db_->DestroyColumnFamilyHandle(cf_handle));
  cf_handle = nullptr;

  // Close and reopen with both CFs.
  std::vector<ColumnFamilyDescriptor> cf_descs;
  cf_descs.emplace_back(kDefaultColumnFamilyName, options);
  ColumnFamilyOptions reopen_cf_opts = options;
  cf_descs.emplace_back("data_cf", reopen_cf_opts);

  std::vector<ColumnFamilyHandle*> handles;
  Close();
  ASSERT_OK(DB::Open(options, dbname_, cf_descs, &handles, &db_));

  // Verify all data across both CFs.
  for (int i = 0; i < num_keys; i++) {
    std::string key = "cf0_key" + std::to_string(i);
    std::string expected(100, static_cast<char>('A' + (i % 26)));
    ASSERT_EQ(Get(key), expected);
  }
  for (int i = 0; i < num_keys; i++) {
    std::string key = "cf1_key" + std::to_string(i);
    std::string expected(100, static_cast<char>('a' + (i % 26)));
    std::string result;
    ASSERT_OK(db_->Get(ReadOptions(), handles[1], key, &result));
    ASSERT_EQ(result, expected);
  }
  for (int i = 0; i < num_keys; i++) {
    std::string key = "cf0_unfl_key" + std::to_string(i);
    std::string expected(100, static_cast<char>('X' - (i % 10)));
    ASSERT_EQ(Get(key), expected);
  }
  for (int i = 0; i < num_keys; i++) {
    std::string key = "cf1_unfl_key" + std::to_string(i);
    std::string expected(100, static_cast<char>('x' - (i % 10)));
    std::string result;
    ASSERT_OK(db_->Get(ReadOptions(), handles[1], key, &result));
    ASSERT_EQ(result, expected);
  }

  for (auto* h : handles) {
    ASSERT_OK(db_->DestroyColumnFamilyHandle(h));
  }
}

// H4: Test both sync (buffer_size=0) and deferred (buffer_size>0) modes
// side by side via parameterized write-read-flush-reopen cycle.
TEST_F(DBBlobDirectWriteTest, SyncFlushMode) {
  Options options = GetBlobDirectWriteOptions();
  options.blob_direct_write_buffer_size = 0;
  DestroyAndReopen(options);
  WriteVerifyFlushReopenVerify(options, 20, 200);
}

TEST_F(DBBlobDirectWriteTest, DeferredFlushMode) {
  Options options = GetBlobDirectWriteOptions();
  options.blob_direct_write_buffer_size = 65536;
  DestroyAndReopen(options);
  WriteVerifyFlushReopenVerify(options, 20, 200);
}

// H5: Test O_DIRECT mode with blob direct write via
// use_direct_io_for_flush_and_compaction DB option.
TEST_F(DBBlobDirectWriteTest, DirectIOMode) {
  if (!IsDirectIOSupported()) {
    ROCKSDB_GTEST_SKIP("Direct I/O not supported on this platform");
    return;
  }
  Options options = GetBlobDirectWriteOptions();
  options.use_direct_io_for_flush_and_compaction = true;
  Status s = TryReopen(options);
  if (!s.ok()) {
    ROCKSDB_GTEST_SKIP("Cannot open DB with direct I/O");
    return;
  }
  Close();
}

// H6: Test file checksums with blob direct write.
TEST_F(DBBlobDirectWriteTest, FileChecksums) {
  Options options = GetBlobDirectWriteOptions();
  options.file_checksum_gen_factory = GetFileChecksumGenCrc32cFactory();
  DestroyAndReopen(options);

  const int num_keys = 20;
  WriteLargeValues(num_keys, 200);
  ASSERT_OK(Flush());

  FileChecksumList* raw_list = NewFileChecksumList();
  std::unique_ptr<FileChecksumList> checksum_list(raw_list);
  ASSERT_OK(db_->GetLiveFilesChecksumInfo(raw_list));

  std::vector<uint64_t> file_numbers;
  std::vector<std::string> checksums;
  std::vector<std::string> func_names;
  ASSERT_OK(
      raw_list->GetAllFileChecksums(&file_numbers, &checksums, &func_names));
  ASSERT_GT(file_numbers.size(), 0u);

  bool found_blob_checksum = false;
  for (size_t i = 0; i < func_names.size(); i++) {
    if (!func_names[i].empty() && !checksums[i].empty()) {
      found_blob_checksum = true;
    }
  }
  ASSERT_TRUE(found_blob_checksum);

  VerifyLargeValues(num_keys, 200);
}

// H7: Partial WriteBatch failure during TransformBatch.
// Injects an I/O error during BlobLogWriter::EmitPhysicalRecord to verify
// that a mid-batch blob write failure fails the entire batch. After the
// error, a reopen is needed because the sync-mode blob writer's internal
// offset becomes desynchronized on write failure.
TEST_F(DBBlobDirectWriteTest, TransformBatchPartialFailure) {
  Options options = GetBlobDirectWriteOptions();
  options.blob_direct_write_partitions = 1;
  options.blob_direct_write_buffer_size = 0;
  DestroyAndReopen(options);

  ASSERT_OK(Put("pre_key", std::string(100, 'P')));
  ASSERT_EQ(Get("pre_key"), std::string(100, 'P'));

  ASSERT_OK(Flush());

  std::atomic<int> append_count{0};
  SyncPoint::GetInstance()->SetCallBack(
      "BlobLogWriter::EmitPhysicalRecord:BeforeAppend", [&](void* arg) {
        auto* s = static_cast<Status*>(arg);
        if (append_count.fetch_add(1, std::memory_order_relaxed) == 2) {
          *s = Status::IOError("Injected blob write failure");
        }
      });
  SyncPoint::GetInstance()->EnableProcessing();

  WriteBatch batch;
  for (int i = 0; i < 5; i++) {
    std::string key = "batch_key" + std::to_string(i);
    std::string value(100, static_cast<char>('B' + i));
    ASSERT_OK(batch.Put(key, value));
  }
  Status s = db_->Write(WriteOptions(), &batch);
  ASSERT_TRUE(s.IsIOError());

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();

  Reopen(options);

  ASSERT_EQ(Get("pre_key"), std::string(100, 'P'));

  ASSERT_OK(Put("post_key", std::string(100, 'Q')));
  ASSERT_EQ(Get("post_key"), std::string(100, 'Q'));

  ASSERT_OK(Flush());
  ASSERT_EQ(Get("pre_key"), std::string(100, 'P'));
  ASSERT_EQ(Get("post_key"), std::string(100, 'Q'));
}

// H8: Background I/O error propagation in deferred flush mode.
// Verifies that when a background flush fails, the error is surfaced to
// subsequent writers via bg_has_error_ / bg_status_.
TEST_F(DBBlobDirectWriteTest, BackgroundIOErrorPropagation) {
  Options options = GetBlobDirectWriteOptions();
  options.blob_direct_write_partitions = 1;
  options.blob_direct_write_buffer_size = 65536;
  DestroyAndReopen(options);

  ASSERT_OK(Put("pre_key", std::string(100, 'P')));
  ASSERT_EQ(Get("pre_key"), std::string(100, 'P'));

  std::atomic<bool> inject_error{false};
  SyncPoint::GetInstance()->SetCallBack(
      "BlobLogWriter::EmitPhysicalRecord:BeforeAppend", [&](void* arg) {
        if (inject_error.load(std::memory_order_relaxed)) {
          auto* s = static_cast<Status*>(arg);
          *s = Status::IOError("Injected background flush I/O error");
        }
      });
  SyncPoint::GetInstance()->EnableProcessing();

  inject_error.store(true, std::memory_order_relaxed);

  bool error_seen = false;
  for (int i = 0; i < 200; i++) {
    std::string key = "bg_err_key" + std::to_string(i);
    std::string value(500, 'E');
    Status s = Put(key, value);
    if (!s.ok()) {
      error_seen = true;
      break;
    }
  }

  ASSERT_TRUE(error_seen);

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
}

// Merge operation with blob direct write: Put+Flush+Merge works after
// the blob value is flushed to SST (BlobIndex resolved during Get).
// Note: Merge on an unflushed BlobIndex in memtable is not supported
// (returns NotSupported), which is a pre-existing BlobDB limitation.
TEST_F(DBBlobDirectWriteTest, MergeWithBlobDirectWrite) {
  Options options = GetBlobDirectWriteOptions();
  options.merge_operator = MergeOperators::CreateStringAppendOperator();
  DestroyAndReopen(options);

  std::string blob_v1(100, 'A');
  ASSERT_OK(Put("key", blob_v1));
  ASSERT_OK(Flush());
  ASSERT_EQ(Get("key"), blob_v1);

  ASSERT_OK(Merge("key", "suffix"));
  ASSERT_OK(Flush());
  ASSERT_EQ(Get("key"), blob_v1 + ",suffix");

  Reopen(options);
  ASSERT_EQ(Get("key"), blob_v1 + ",suffix");
}

// Zero-length value with min_blob_size = 0: every Put goes through blob
// direct write, including empty values.
TEST_F(DBBlobDirectWriteTest, ZeroLengthValue) {
  Options options = GetBlobDirectWriteOptions();
  options.min_blob_size = 0;
  DestroyAndReopen(options);

  ASSERT_OK(Put("empty", ""));
  ASSERT_EQ(Get("empty"), "");

  ASSERT_OK(Put("nonempty", std::string(100, 'X')));
  ASSERT_EQ(Get("nonempty"), std::string(100, 'X'));

  ASSERT_OK(Flush());
  ASSERT_EQ(Get("empty"), "");
  ASSERT_EQ(Get("nonempty"), std::string(100, 'X'));

  Reopen(options);
  ASSERT_EQ(Get("empty"), "");
  ASSERT_EQ(Get("nonempty"), std::string(100, 'X'));
}

// Iterator Seek and SeekForPrev with blob direct write values.
TEST_F(DBBlobDirectWriteTest, IteratorSeek) {
  Options options = GetBlobDirectWriteOptions();
  DestroyAndReopen(options);

  for (int i = 0; i < 10; i++) {
    std::string key = "key" + std::to_string(i);
    std::string value(100 + i, static_cast<char>('a' + (i % 26)));
    ASSERT_OK(Put(key, value));
  }

  {
    auto* iter = db_->NewIterator(ReadOptions());
    iter->Seek("key5");
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key().ToString(), "key5");
    ASSERT_EQ(iter->value().ToString(),
              std::string(105, static_cast<char>('a' + 5)));

    iter->Next();
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key().ToString(), "key6");

    iter->SeekForPrev("key5");
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key().ToString(), "key5");

    iter->Prev();
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key().ToString(), "key4");
    ASSERT_EQ(iter->value().ToString(),
              std::string(104, static_cast<char>('a' + 4)));
    delete iter;
  }

  ASSERT_OK(Flush());

  {
    auto* iter = db_->NewIterator(ReadOptions());
    iter->Seek("key5");
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key().ToString(), "key5");
    ASSERT_EQ(iter->value().ToString(),
              std::string(105, static_cast<char>('a' + 5)));
    delete iter;
  }
}

// Seal failure during shutdown: inject I/O error during SealAllPartitions,
// verify data is recovered via orphan recovery on next open.
TEST_F(DBBlobDirectWriteTest, SealFailureRecovery) {
  Options options = GetBlobDirectWriteOptions();
  options.blob_direct_write_partitions = 1;
  options.blob_direct_write_buffer_size = 0;
  DestroyAndReopen(options);

  for (int i = 0; i < 10; i++) {
    std::string key = "seal_key" + std::to_string(i);
    ASSERT_OK(Put(key, std::string(100, static_cast<char>('S' + (i % 3)))));
  }

  ASSERT_OK(Flush());

  for (int i = 0; i < 10; i++) {
    std::string key = "seal_key" + std::to_string(i);
    ASSERT_EQ(Get(key), std::string(100, static_cast<char>('S' + (i % 3))));
  }

  for (int i = 10; i < 20; i++) {
    std::string key = "seal_key" + std::to_string(i);
    ASSERT_OK(Put(key, std::string(100, static_cast<char>('T' + (i % 3)))));
  }

  SyncPoint::GetInstance()->SetCallBack(
      "BlobLogWriter::EmitPhysicalRecord:BeforeAppend", [&](void* arg) {
        auto* s = static_cast<Status*>(arg);
        *s = Status::IOError("Injected seal failure");
      });
  SyncPoint::GetInstance()->EnableProcessing();

  Status close_s = TryReopen(options);
  close_s.PermitUncheckedError();

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();

  Reopen(options);

  for (int i = 0; i < 10; i++) {
    std::string key = "seal_key" + std::to_string(i);
    ASSERT_EQ(Get(key), std::string(100, static_cast<char>('S' + (i % 3))));
  }
}

// BLOB_DB_DIRECT_WRITE_STALL_COUNT statistic is incremented during
// backpressure.
TEST_F(DBBlobDirectWriteTest, StallCountStatistic) {
  Options options = GetBlobDirectWriteOptions();
  options.statistics = CreateDBStatistics();
  options.blob_direct_write_partitions = 1;
  options.blob_direct_write_buffer_size = 1024;
  DestroyAndReopen(options);

  std::atomic<bool> stall_seen{false};
  SyncPoint::GetInstance()->SetCallBack(
      "BlobFilePartitionManager::WriteBlob:BackpressureStall",
      [&](void*) { stall_seen.store(true, std::memory_order_relaxed); });
  SyncPoint::GetInstance()->EnableProcessing();

  std::vector<std::thread> writers;
  writers.reserve(4);
  for (int t = 0; t < 4; t++) {
    writers.emplace_back([&, t]() {
      for (int i = 0; i < 200; i++) {
        std::string key =
            "stall_t" + std::to_string(t) + "_k" + std::to_string(i);
        std::string value(500, 'V');
        Status s = Put(key, value);
        if (!s.ok()) {
          break;
        }
      }
    });
  }
  for (auto& w : writers) {
    w.join();
  }

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();

  if (stall_seen.load()) {
    ASSERT_GT(
        options.statistics->getTickerCount(BLOB_DB_DIRECT_WRITE_STALL_COUNT),
        0);
  }
}

// BlobFileCreationReason::kDirectWrite is reported to event listeners.
TEST_F(DBBlobDirectWriteTest, EventListenerDirectWriteReason) {
  class TestListener : public EventListener {
   public:
    std::atomic<int> direct_write_count{0};

    void OnBlobFileCreationStarted(
        const BlobFileCreationBriefInfo& info) override {
      if (info.reason == BlobFileCreationReason::kDirectWrite) {
        direct_write_count.fetch_add(1, std::memory_order_relaxed);
      }
    }
  };

  auto listener = std::make_shared<TestListener>();
  Options options = GetBlobDirectWriteOptions();
  options.listeners.push_back(listener);
  DestroyAndReopen(options);

  ASSERT_OK(Put("key1", std::string(100, 'x')));
  ASSERT_OK(Flush());

  ASSERT_GT(listener->direct_write_count.load(), 0);
}

// GC tests: verify garbage collection works with direct-write blob files.

TEST_F(DBBlobDirectWriteTest, ActiveGarbageCollection) {
  Options options = GetBlobDirectWriteOptions();
  options.disable_auto_compactions = true;
  options.enable_blob_garbage_collection = true;
  options.blob_garbage_collection_age_cutoff = 1.0;
  options.blob_garbage_collection_force_threshold = 0.5;
  options.blob_direct_write_partitions = 1;
  options.statistics = CreateDBStatistics();
  DestroyAndReopen(options);

  // Write initial data — each key gets a blob.
  const int num_keys = 20;
  for (int i = 0; i < num_keys; i++) {
    std::string key = "gc_key" + std::to_string(i);
    std::string value(200, static_cast<char>('A' + (i % 26)));
    ASSERT_OK(Put(key, value));
  }
  ASSERT_OK(Flush());

  // Verify data is readable after flush.
  for (int i = 0; i < num_keys; i++) {
    std::string key = "gc_key" + std::to_string(i);
    std::string expected(200, static_cast<char>('A' + (i % 26)));
    ASSERT_EQ(Get(key), expected);
  }

  // Overwrite all keys with new values — old blobs become garbage.
  for (int i = 0; i < num_keys; i++) {
    std::string key = "gc_key" + std::to_string(i);
    std::string value(200, static_cast<char>('Z' - (i % 26)));
    ASSERT_OK(Put(key, value));
  }
  ASSERT_OK(Flush());

  // Compact to trigger GC — old blob files should be cleaned up.
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));

  // Verify data is correct after GC.
  for (int i = 0; i < num_keys; i++) {
    std::string key = "gc_key" + std::to_string(i);
    std::string expected(200, static_cast<char>('Z' - (i % 26)));
    ASSERT_EQ(Get(key), expected);
  }

  // Verify GC ran: relocated bytes counter should be positive when GC
  // relocates live blobs from old files to new files.
  uint64_t gc_bytes_relocated =
      options.statistics->getTickerCount(BLOB_DB_GC_BYTES_RELOCATED);
  ASSERT_GT(gc_bytes_relocated, 0);
}

TEST_F(DBBlobDirectWriteTest, PassiveGarbageCollection) {
  Options options = GetBlobDirectWriteOptions();
  options.disable_auto_compactions = true;
  options.enable_blob_garbage_collection = true;
  options.blob_garbage_collection_age_cutoff = 1.0;
  options.blob_direct_write_partitions = 1;
  DestroyAndReopen(options);

  // Write initial data.
  const int num_keys = 20;
  for (int i = 0; i < num_keys; i++) {
    std::string key = "pgc_key" + std::to_string(i);
    std::string value(200, static_cast<char>('P' + (i % 6)));
    ASSERT_OK(Put(key, value));
  }
  ASSERT_OK(Flush());

  // Delete all keys — blobs become unreferenced.
  for (int i = 0; i < num_keys; i++) {
    std::string key = "pgc_key" + std::to_string(i);
    ASSERT_OK(Delete(key));
  }
  ASSERT_OK(Flush());

  // Compact — tombstones should remove all entries, and GC should
  // eventually clean up the blob files.
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));

  // Verify all keys are deleted.
  for (int i = 0; i < num_keys; i++) {
    std::string key = "pgc_key" + std::to_string(i);
    ASSERT_EQ(Get(key), "NOT_FOUND");
  }
}

// Version builder bypass test: orphan blob files without linked SSTs
// should survive SaveTo.
TEST_F(DBBlobDirectWriteTest, OrphanBlobFileSurvivesSaveTo) {
  Options options = GetBlobDirectWriteOptions();
  options.blob_direct_write_partitions = 1;
  DestroyAndReopen(options);

  // Write blob data — creates blob files via direct write.
  const int num_keys = 10;
  for (int i = 0; i < num_keys; i++) {
    std::string key = "saveto_key" + std::to_string(i);
    std::string value(200, static_cast<char>('S' + (i % 10)));
    ASSERT_OK(Put(key, value));
  }

  // Close without flush — blob files are sealed during shutdown but not
  // registered in the MANIFEST via flush. On reopen, orphan recovery
  // registers them via VersionBuilder. The key test is that SaveTo
  // (called during subsequent flushes/compactions) preserves these
  // newly added blob files even though no SSTs reference them yet.
  Close();

  // Reopen — orphan recovery adds blob files to VersionBuilder.
  Reopen(options);

  // Verify all data is readable (orphan recovery worked).
  for (int i = 0; i < num_keys; i++) {
    std::string key = "saveto_key" + std::to_string(i);
    std::string expected(200, static_cast<char>('S' + (i % 10)));
    ASSERT_EQ(Get(key), expected);
  }

  // Write more data and flush — this triggers SaveTo on the version
  // that includes the orphan-recovered blob files. If the bypass is
  // wrong, the blob files would be dropped and reads would fail.
  for (int i = 0; i < num_keys; i++) {
    std::string key = "saveto_new_key" + std::to_string(i);
    std::string value(200, static_cast<char>('T' + (i % 10)));
    ASSERT_OK(Put(key, value));
  }
  ASSERT_OK(Flush());

  // Verify both old (orphan-recovered) and new data survive SaveTo.
  for (int i = 0; i < num_keys; i++) {
    std::string key = "saveto_key" + std::to_string(i);
    std::string expected(200, static_cast<char>('S' + (i % 10)));
    ASSERT_EQ(Get(key), expected);
  }
  for (int i = 0; i < num_keys; i++) {
    std::string key = "saveto_new_key" + std::to_string(i);
    std::string expected(200, static_cast<char>('T' + (i % 10)));
    ASSERT_EQ(Get(key), expected);
  }

  // Reopen once more to confirm MANIFEST is consistent.
  Reopen(options);
  for (int i = 0; i < num_keys; i++) {
    std::string key = "saveto_key" + std::to_string(i);
    std::string expected(200, static_cast<char>('S' + (i % 10)));
    ASSERT_EQ(Get(key), expected);
  }
}

// ========================================================================
// Orphan recovery branch coverage tests
// ========================================================================

// Corrupt/unreadable header: file skipped during orphan recovery.
TEST_F(DBBlobDirectWriteTest, OrphanRecoveryCorruptHeader) {
  if (encrypted_env_) {
    ROCKSDB_GTEST_SKIP("Test creates intentionally malformed files");
    return;
  }
  Options options = GetBlobDirectWriteOptions();
  options.blob_direct_write_partitions = 1;
  DestroyAndReopen(options);

  // Write data so the DB has some real blob files and a next file number.
  WriteLargeValues(5, 100, "real_");
  ASSERT_OK(Flush());
  Close();

  // Plant a blob file with garbage bytes (corrupt header).
  uint64_t fake_number = 999990;
  std::string path = BlobFileName(dbname_, fake_number);
  std::string corrupt_data(BlobLogHeader::kSize, '\xFF');
  ASSERT_OK(WriteStringToFile(Env::Default(), corrupt_data, path));

  // Reopen: orphan recovery should skip the corrupt file.
  Reopen(options);

  // Original data should be intact.
  VerifyLargeValues(5, 100, "real_");

  // Verify the corrupt file was cleaned up by DeleteObsoleteFiles
  // (it was skipped by orphan recovery, so not in the live set).
  Status file_status = env_->FileExists(path);
  ASSERT_TRUE(file_status.IsNotFound());
}

// Zero-size file: file skipped during orphan recovery.
TEST_F(DBBlobDirectWriteTest, OrphanRecoveryZeroSizeFile) {
  if (encrypted_env_) {
    ROCKSDB_GTEST_SKIP("Test creates intentionally malformed files");
    return;
  }
  Options options = GetBlobDirectWriteOptions();
  options.blob_direct_write_partitions = 1;
  DestroyAndReopen(options);

  WriteLargeValues(5, 100, "real_");
  ASSERT_OK(Flush());
  Close();

  // Plant an empty blob file.
  uint64_t fake_number = 999991;
  std::string path = BlobFileName(dbname_, fake_number);
  ASSERT_OK(WriteStringToFile(Env::Default(), "", path));

  Reopen(options);
  VerifyLargeValues(5, 100, "real_");

  // Empty file should be cleaned up.
  ASSERT_TRUE(env_->FileExists(path).IsNotFound());
}

// Valid header but zero complete records: file skipped.
TEST_F(DBBlobDirectWriteTest, OrphanRecoveryHeaderOnlyNoRecords) {
  if (encrypted_env_) {
    ROCKSDB_GTEST_SKIP("Test creates intentionally malformed files");
    return;
  }
  Options options = GetBlobDirectWriteOptions();
  options.blob_direct_write_partitions = 1;
  DestroyAndReopen(options);

  WriteLargeValues(5, 100, "real_");
  ASSERT_OK(Flush());
  Close();

  // Plant a blob file with only a valid header (no records).
  uint64_t fake_number = 999992;
  WriteSyntheticBlobFile(fake_number, /*cf_id=*/0, /*num_records=*/0);

  Reopen(options);
  VerifyLargeValues(5, 100, "real_");

  // Header-only file should be cleaned up (zero valid records).
  std::string path = BlobFileName(dbname_, fake_number);
  ASSERT_TRUE(env_->FileExists(path).IsNotFound());
}

// File already registered in MANIFEST: file skipped (no double-registration).
TEST_F(DBBlobDirectWriteTest, OrphanRecoveryAlreadyRegistered) {
  Options options = GetBlobDirectWriteOptions();
  options.blob_direct_write_partitions = 1;
  DestroyAndReopen(options);

  // Write and flush so blob files are registered in the MANIFEST.
  WriteLargeValues(10, 100, "reg_");
  ASSERT_OK(Flush());

  // Reopen: the flushed blob files are already in MANIFEST.
  // Orphan recovery should skip them without error.
  Reopen(options);
  VerifyLargeValues(10, 100, "reg_");

  // Reopen once more to confirm consistency.
  Reopen(options);
  VerifyLargeValues(10, 100, "reg_");
}

// File with valid header + partial last record (truncated):
// With WAL-replay-based recovery, unreferenced synthetic files are
// cleaned up by DeleteObsoleteFiles regardless of record count.
TEST_F(DBBlobDirectWriteTest, OrphanRecoveryTruncatedLastRecord) {
  if (encrypted_env_) {
    ROCKSDB_GTEST_SKIP("Test creates intentionally malformed files");
    return;
  }
  Options options = GetBlobDirectWriteOptions();
  options.blob_direct_write_partitions = 1;
  DestroyAndReopen(options);

  WriteLargeValues(5, 100, "real_");
  ASSERT_OK(Flush());
  Close();

  // Plant a blob file with 3 valid records + a truncated 4th record.
  // No WAL entries reference this file. Orphan recovery resolves WAL
  // entries to raw values, so unreferenced orphan files are deleted
  // by PurgeObsoleteFiles after recovery.
  uint64_t fake_number = 999993;
  WriteSyntheticBlobFile(fake_number, /*cf_id=*/0, /*num_records=*/4,
                         /*write_footer=*/false,
                         /*truncate_last_record=*/true);

  Reopen(options);
  VerifyLargeValues(5, 100, "real_");

  // The orphan file is not registered in MANIFEST (no WAL entries
  // reference it). PurgeObsoleteFiles deletes it after recovery.
  std::string path = BlobFileName(dbname_, fake_number);
  ASSERT_TRUE(env_->FileExists(path).IsNotFound());

  // Reopen again to verify MANIFEST consistency.
  Reopen(options);
  VerifyLargeValues(5, 100, "real_");
}

// Multi-CF orphan recovery: files from different CFs recovered to correct CFs.
TEST_F(DBBlobDirectWriteTest, OrphanRecoveryMultiCF) {
  Options options = GetBlobDirectWriteOptions();
  options.blob_direct_write_partitions = 1;
  options.blob_direct_write_buffer_size = 0;

  // CreateAndReopenWithCF creates the CF, then reopens with
  // handles_[0]=default, handles_[1]=cf1.
  CreateAndReopenWithCF({"cf1"}, options);

  // Write data to default CF (handles_[0]).
  for (int i = 0; i < 5; i++) {
    ASSERT_OK(Put(0, "cf0_key" + std::to_string(i),
                  std::string(100, static_cast<char>('A' + i))));
  }
  // Write data to cf1 (handles_[1]).
  for (int i = 0; i < 5; i++) {
    ASSERT_OK(Put(1, "cf1_key" + std::to_string(i),
                  std::string(100, static_cast<char>('X' + (i % 3)))));
  }

  // Flush both CFs to create MANIFEST-registered blob files,
  // then write more data that will be orphaned after close.
  ASSERT_OK(Flush(0));
  ASSERT_OK(Flush(1));

  for (int i = 5; i < 10; i++) {
    ASSERT_OK(Put(0, "cf0_key" + std::to_string(i),
                  std::string(100, static_cast<char>('A' + i))));
  }
  for (int i = 5; i < 10; i++) {
    ASSERT_OK(Put(1, "cf1_key" + std::to_string(i),
                  std::string(100, static_cast<char>('X' + (i % 3)))));
  }

  // Close without flush for the second batch: creates orphan blob files.
  Close();

  // Reopen with both CFs — orphan recovery should register each file
  // under the correct CF based on the blob file header's column_family_id.
  ReopenWithColumnFamilies({"default", "cf1"}, options);

  // Verify data in both CFs (first batch from flush + second from recovery).
  for (int i = 0; i < 10; i++) {
    ASSERT_EQ(Get(0, "cf0_key" + std::to_string(i)),
              std::string(100, static_cast<char>('A' + i)));
  }
  for (int i = 0; i < 10; i++) {
    ASSERT_EQ(Get(1, "cf1_key" + std::to_string(i)),
              std::string(100, static_cast<char>('X' + (i % 3))));
  }
}

// ========================================================================
// Get/MultiGet test gaps
// ========================================================================

// Immutable memtable read: verify blob is readable from immutable memtable
// after memtable switch but before flush completes.
TEST_F(DBBlobDirectWriteTest, ImmutableMemtableRead) {
  Options options = GetBlobDirectWriteOptions();
  options.blob_direct_write_partitions = 1;
  DestroyAndReopen(options);

  // Write data to memtable.
  const int num_keys = 10;
  for (int i = 0; i < num_keys; i++) {
    std::string key = "imm_key" + std::to_string(i);
    ASSERT_OK(Put(key, std::string(100 + i, static_cast<char>('I' + (i % 5)))));
  }

  // Switch memtable without waiting for flush to complete.
  // TEST_SwitchMemtable moves the current memtable to the immutable list.
  ASSERT_OK(dbfull()->TEST_SwitchMemtable());

  // Read from immutable memtable: blob values should be resolvable.
  for (int i = 0; i < num_keys; i++) {
    std::string key = "imm_key" + std::to_string(i);
    ASSERT_EQ(Get(key), std::string(100 + i, static_cast<char>('I' + (i % 5))));
  }

  // Now flush and verify again.
  ASSERT_OK(dbfull()->TEST_FlushMemTable(true));
  for (int i = 0; i < num_keys; i++) {
    std::string key = "imm_key" + std::to_string(i);
    ASSERT_EQ(Get(key), std::string(100 + i, static_cast<char>('I' + (i % 5))));
  }
}

// MultiGet with a mix of blob (direct write) and small inline values.
TEST_F(DBBlobDirectWriteTest, MultiGetMixedBlobAndInline) {
  Options options = GetBlobDirectWriteOptions();
  options.blob_direct_write_partitions = 1;
  DestroyAndReopen(options);

  // Write a mix of large (blob) and small (inline) values.
  std::vector<std::string> keys;
  std::vector<std::string> expected_values;
  for (int i = 0; i < 10; i++) {
    std::string key = "mg_key" + std::to_string(i);
    keys.push_back(key);
    if (i % 2 == 0) {
      // Large value -> blob direct write.
      std::string value(200, static_cast<char>('B' + (i % 10)));
      ASSERT_OK(Put(key, value));
      expected_values.push_back(value);
    } else {
      // Small value -> inline in memtable.
      std::string value = "s" + std::to_string(i);
      ASSERT_OK(Put(key, value));
      expected_values.push_back(value);
    }
  }

  // MultiGet from memtable.
  auto results = MultiGet(keys);
  for (size_t i = 0; i < keys.size(); i++) {
    ASSERT_EQ(results[i], expected_values[i]) << "key=" << keys[i];
  }

  // Flush and MultiGet from SST + blob files.
  ASSERT_OK(Flush());
  results = MultiGet(keys);
  for (size_t i = 0; i < keys.size(); i++) {
    ASSERT_EQ(results[i], expected_values[i]) << "key=" << keys[i];
  }
}

// IO error on blob file read during Get: error propagates correctly.
TEST_F(DBBlobDirectWriteTest, GetBlobIOError) {
  std::unique_ptr<FaultInjectionTestEnv> fault_env(
      new FaultInjectionTestEnv(env_));

  Options options = GetBlobDirectWriteOptions();
  options.blob_direct_write_partitions = 1;
  options.env = fault_env.get();
  DestroyAndReopen(options);

  // Write data and flush so blobs are in sealed blob files on disk.
  ASSERT_OK(Put("err_key", std::string(200, 'E')));
  ASSERT_OK(Flush());

  // Verify normal read works.
  ASSERT_EQ(Get("err_key"), std::string(200, 'E'));

  // Inject IO error on blob file read.
  SyncPoint::GetInstance()->SetCallBack(
      "BlobFileReader::GetBlob:ReadFromFile", [&](void* /*arg*/) {
        fault_env->SetFilesystemActive(false,
                                       Status::IOError("Injected blob read"));
      });
  SyncPoint::GetInstance()->EnableProcessing();

  PinnableSlice result;
  Status s =
      db_->Get(ReadOptions(), db_->DefaultColumnFamily(), "err_key", &result);
  ASSERT_TRUE(s.IsIOError()) << s.ToString();

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();

  // Re-enable filesystem and verify read works again.
  fault_env->SetFilesystemActive(true);
  ASSERT_EQ(Get("err_key"), std::string(200, 'E'));

  Close();
}

// Regression test for the stress failure behind active-file blob reads under
// FaultInjectionTestFS unsynced-data mode. After FlushAllOpenFiles(), BDW has
// removed the in-memory pending entry, so reads must come through the active
// blob file path. The wrapper still reports a logical size > 0 while the real
// file remains 0 bytes until Sync(), so random-access reads must honor the
// unsynced tracked state instead of relying on the underlying file size alone.
TEST_F(DBBlobDirectWriteTest,
       IteratorReadOnActiveBlobSucceedsAfterBgFlushUnderFaultInjectionFS) {
  if (encrypted_env_) {
    ROCKSDB_GTEST_SKIP("Test inspects underlying file sizes directly");
    return;
  }

  auto fault_fs = std::make_shared<FaultInjectionTestFS>(env_->GetFileSystem());
  fault_fs->SetFilesystemDirectWritable(false);
  fault_fs->SetInjectUnsyncedDataLoss(true);
  auto fault_env = std::make_unique<CompositeEnvWrapper>(env_, fault_fs);

  Options options = GetBlobDirectWriteOptions();
  options.env = fault_env.get();
  options.allow_mmap_reads = true;
  options.blob_direct_write_partitions = 1;
  options.blob_direct_write_buffer_size = 256;
  VerifyActiveBlobReadAfterBgFlushWithFaultInjectionFS(options, fault_fs.get());
}

TEST_F(DBBlobDirectWriteTest,
       IteratorReadOnActiveBlobSucceedsWithDirectReadsAfterBgFlush) {
  if (encrypted_env_) {
    ROCKSDB_GTEST_SKIP("Test inspects underlying file sizes directly");
    return;
  }
  if (!IsDirectIOSupported()) {
    ROCKSDB_GTEST_SKIP("Direct I/O not supported on this platform");
    return;
  }

  auto fault_fs = std::make_shared<FaultInjectionTestFS>(env_->GetFileSystem());
  fault_fs->SetFilesystemDirectWritable(false);
  fault_fs->SetInjectUnsyncedDataLoss(true);
  auto fault_env = std::make_unique<CompositeEnvWrapper>(env_, fault_fs);

  Options options = GetBlobDirectWriteOptions();
  options.env = fault_env.get();
  options.use_direct_reads = true;
  options.blob_direct_write_partitions = 1;
  options.blob_direct_write_buffer_size = 256;
  VerifyActiveBlobReadAfterBgFlushWithFaultInjectionFS(options, fault_fs.get());
}

// ========================================================================
// Half-written blob file from normal BlobDB (no direct write)
// ========================================================================

// Verify that orphan recovery skips blob files with no complete records
// (half-written from a normal BlobDB flush crash).
TEST_F(DBBlobDirectWriteTest, HalfWrittenBlobFromNormalBlobDB) {
  if (encrypted_env_) {
    ROCKSDB_GTEST_SKIP("Test creates intentionally malformed files");
    return;
  }
  // Open with standard blob support but NOT direct write.
  Options options = CurrentOptions();
  options.enable_blob_files = true;
  options.min_blob_size = 10;
  options.enable_blob_direct_write = false;
  DestroyAndReopen(options);

  // Write data and flush to create normal blob files.
  for (int i = 0; i < 10; i++) {
    ASSERT_OK(Put("norm_key" + std::to_string(i), std::string(100, 'N')));
  }
  ASSERT_OK(Flush());
  for (int i = 0; i < 10; i++) {
    ASSERT_EQ(Get("norm_key" + std::to_string(i)), std::string(100, 'N'));
  }

  Close();

  // Simulate a half-written blob file from a crashed flush:
  // valid header but no complete records (just the header).
  uint64_t fake_number = 999995;
  WriteSyntheticBlobFile(fake_number, /*cf_id=*/0, /*num_records=*/0);

  // Reopen: orphan recovery should skip the header-only file (zero records).
  // Normal data should be intact.
  Reopen(options);
  for (int i = 0; i < 10; i++) {
    ASSERT_EQ(Get("norm_key" + std::to_string(i)), std::string(100, 'N'));
  }

  // The half-written file should be cleaned up by DeleteObsoleteFiles.
  std::string path = BlobFileName(dbname_, fake_number);
  ASSERT_TRUE(env_->FileExists(path).IsNotFound());
}

// ========================================================================
// WAL-replay-based orphan recovery tests
// ========================================================================

// Verify that orphan blob records are rewritten into new properly-tracked
// blob files during recovery, and old orphan files are cleaned up.
TEST_F(DBBlobDirectWriteTest, RecoveryRewritesOrphanBlobs) {
  Options options = GetBlobDirectWriteOptions();
  options.blob_direct_write_partitions = 1;
  options.blob_direct_write_buffer_size = 0;
  options.statistics = CreateDBStatistics();
  DestroyAndReopen(options);

  const int num_keys = 20;
  WriteLargeValues(num_keys, 100);

  // Collect orphan blob file numbers before close.
  std::vector<std::string> filenames;
  ASSERT_OK(env_->GetChildren(dbname_, &filenames));
  std::set<uint64_t> pre_close_blob_files;
  for (const auto& fname : filenames) {
    uint64_t file_number;
    FileType file_type;
    if (ParseFileName(fname, &file_number, &file_type) &&
        file_type == kBlobFile) {
      pre_close_blob_files.insert(file_number);
    }
  }
  ASSERT_FALSE(pre_close_blob_files.empty());

  // Close without flush: blob files are sealed but not in MANIFEST.
  Close();

  // Reopen: WAL replay resolves orphan BlobIndex entries.
  Reopen(options);

  // Verify all data is readable.
  VerifyLargeValues(num_keys, 100);

  // After recovery flush, old orphan blob files should be gone and
  // new blob files should exist.
  ASSERT_OK(env_->GetChildren(dbname_, &filenames));
  std::set<uint64_t> post_recovery_blob_files;
  for (const auto& fname : filenames) {
    uint64_t file_number;
    FileType file_type;
    if (ParseFileName(fname, &file_number, &file_type) &&
        file_type == kBlobFile) {
      post_recovery_blob_files.insert(file_number);
    }
  }
  // Old orphan files should be cleaned up.
  for (uint64_t old_fn : pre_close_blob_files) {
    ASSERT_EQ(post_recovery_blob_files.count(old_fn), 0)
        << "Old orphan blob file " << old_fn << " should be gone";
  }
  // New blob files should exist (created by recovery flush).
  ASSERT_FALSE(post_recovery_blob_files.empty());

  // Verify recovery metrics.
  ASSERT_GT(
      options.statistics->getTickerCount(BLOB_DB_ORPHAN_RECOVERY_RESOLVED), 0);

  // Second reopen to confirm MANIFEST consistency.
  Reopen(options);
  VerifyLargeValues(num_keys, 100);
}

// WAL has BlobIndex entries but the blob file was deleted from disk.
// The resolver won't find the file (not in orphan set), so the BlobIndex
// is inserted as-is. Reads should fail with Corruption.
TEST_F(DBBlobDirectWriteTest, RecoveryMissingBlobFile) {
  Options options = GetBlobDirectWriteOptions();
  options.blob_direct_write_partitions = 1;
  options.blob_direct_write_buffer_size = 0;
  DestroyAndReopen(options);

  WriteLargeValues(5, 100);
  Close();

  auto delete_blob_files = [&]() {
    std::vector<std::string> filenames;
    ASSERT_OK(env_->GetChildren(dbname_, &filenames));
    for (const auto& fname : filenames) {
      uint64_t file_number;
      FileType file_type;
      if (ParseFileName(fname, &file_number, &file_type) &&
          file_type == kBlobFile) {
        ASSERT_OK(env_->DeleteFile(BlobFileName(dbname_, file_number)));
      }
    }
  };

  delete_blob_files();

  // With paranoid_checks=true (default): recovery aborts because the WAL
  // contains PutBlobIndex entries whose blob files are missing.
  Status s = TryReopen(options);
  ASSERT_TRUE(s.IsAborted()) << s.ToString();

  // With paranoid_checks=false: batch is skipped, DB opens, keys are gone.
  options.paranoid_checks = false;
  delete_blob_files();
  Reopen(options);
  for (int i = 0; i < 5; i++) {
    std::string key = "key" + std::to_string(i);
    ASSERT_EQ(Get(key), "NOT_FOUND");
  }
}

// Write a single WriteBatch with entries routed to multiple partitions.
// Delete one partition's blob file. Verify that recovery aborts the entire
// batch (not just the entries in the missing file), maintaining write batch
// atomicity.
TEST_F(DBBlobDirectWriteTest, RecoveryBatchAtomicityWithMultiPartition) {
  Options options = GetBlobDirectWriteOptions();
  options.blob_direct_write_partitions = 2;
  options.blob_direct_write_buffer_size = 0;
  DestroyAndReopen(options);

  // Write a single batch with enough entries to span both partitions
  // (round-robin assignment).
  WriteBatch batch;
  const int num_keys = 6;
  for (int i = 0; i < num_keys; i++) {
    std::string key = "batchkey" + std::to_string(i);
    std::string value(100, static_cast<char>('A' + i));
    ASSERT_OK(batch.Put(key, value));
  }
  ASSERT_OK(db_->Write(WriteOptions(), &batch));
  Close();

  // Identify all blob files and delete only one (simulate partial data loss
  // across partitions).
  std::vector<std::string> blob_files;
  {
    std::vector<std::string> filenames;
    ASSERT_OK(env_->GetChildren(dbname_, &filenames));
    for (const auto& fname : filenames) {
      uint64_t file_number;
      FileType file_type;
      if (ParseFileName(fname, &file_number, &file_type) &&
          file_type == kBlobFile) {
        blob_files.push_back(BlobFileName(dbname_, file_number));
      }
    }
  }
  ASSERT_GE(blob_files.size(), 2u)
      << "Expected at least 2 blob files from 2 partitions";

  ASSERT_OK(env_->DeleteFile(blob_files[0]));

  // paranoid_checks=true: recovery aborts because the batch has entries
  // referencing the deleted blob file.
  Status s = TryReopen(options);
  ASSERT_TRUE(s.IsAborted()) << s.ToString();

  // paranoid_checks=false: the entire batch is skipped (not partially
  // applied), so ALL keys from the batch should be missing.
  // The blob file is already deleted from the first attempt above; the
  // on-disk state is unchanged after TryReopen fails.
  options.paranoid_checks = false;
  Reopen(options);
  for (int i = 0; i < num_keys; i++) {
    std::string key = "batchkey" + std::to_string(i);
    ASSERT_EQ(Get(key), "NOT_FOUND")
        << "key=" << key << " should be missing (entire batch skipped)";
  }
}

// Reproduce the crash scenario from stress test tsan-atomic-flush-blackbox:
// BDW with deferred flush (buffer_size > 0) creates blob files on disk via
// RotateAllPartitions, but the BG flush thread never writes header+data before
// the crash. The blob files remain 0 bytes on disk while the WAL already has
// PutBlobIndex entries referencing them. On recovery, OrphanBlobFileResolver
// must treat these 0-byte files as empty orphans so the batch validator can
// atomically discard the affected batches.
TEST_F(DBBlobDirectWriteTest, RecoveryCrashBeforeBlobHeaderFlush) {
  if (encrypted_env_) {
    ROCKSDB_GTEST_SKIP("Test creates intentionally malformed files");
    return;
  }
  Options options = GetBlobDirectWriteOptions();
  options.blob_direct_write_partitions = 4;
  options.blob_direct_write_buffer_size = 0;
  DestroyAndReopen(options);

  const int num_keys = 10;
  WriteLargeValues(num_keys, 100);
  // Close without Flush: WAL has PutBlobIndex entries, memtable is not
  // flushed to SST, so blob files are not registered in MANIFEST.
  Close();

  std::vector<std::string> blob_paths;
  {
    std::vector<std::string> filenames;
    ASSERT_OK(env_->GetChildren(dbname_, &filenames));
    for (const auto& fname : filenames) {
      uint64_t file_number;
      FileType file_type;
      if (ParseFileName(fname, &file_number, &file_type) &&
          file_type == kBlobFile) {
        blob_paths.push_back(BlobFileName(dbname_, file_number));
      }
    }
  }
  ASSERT_GE(blob_paths.size(), 1u);

  // Truncate all blob files to 0 bytes: simulates crash in deferred flush
  // mode where RotateAllPartitions created new files on disk but the
  // buffered header+data was never flushed before the process was killed.
  auto truncate_blob_files = [&]() {
    for (const auto& path : blob_paths) {
      env_->DeleteFile(path);
      ASSERT_OK(WriteStringToFile(Env::Default(), "", path));
    }
  };

  truncate_blob_files();

  // paranoid_checks=true: recovery aborts because empty orphan blob files
  // can't be resolved by TryResolveBlob (file_size=0 → invalid offset).
  Status s = TryReopen(options);
  ASSERT_TRUE(s.IsAborted()) << s.ToString();

  // paranoid_checks=false: each WAL batch referencing an empty orphan is
  // skipped via MaybeIgnoreError. DB opens but the affected keys are gone.
  truncate_blob_files();
  options.paranoid_checks = false;
  Reopen(options);
  for (int i = 0; i < num_keys; i++) {
    ASSERT_EQ(Get("key" + std::to_string(i)), "NOT_FOUND");
  }

  // Empty orphan files should be cleaned up by PurgeObsoleteFiles.
  for (const auto& path : blob_paths) {
    ASSERT_TRUE(env_->FileExists(path).IsNotFound())
        << "Empty orphan should be cleaned up: " << path;
  }
}

// Same scenario as RecoveryCrashBeforeBlobHeaderFlush but with a single
// WriteBatch spanning multiple partitions, verifying batch atomicity: if ONE
// partition's blob file is 0 bytes (crash before header flush), the ENTIRE
// batch is rejected, not just the entries referencing that partition.
TEST_F(DBBlobDirectWriteTest, RecoveryBatchAtomicityWithEmptyOrphanPartition) {
  if (encrypted_env_) {
    ROCKSDB_GTEST_SKIP("Test creates intentionally malformed files");
    return;
  }
  Options options = GetBlobDirectWriteOptions();
  options.blob_direct_write_partitions = 2;
  options.blob_direct_write_buffer_size = 0;
  DestroyAndReopen(options);

  // Single WriteBatch with enough entries to span both partitions.
  WriteBatch batch;
  const int num_keys = 6;
  for (int i = 0; i < num_keys; i++) {
    std::string key = "atomickey" + std::to_string(i);
    std::string value(100, static_cast<char>('A' + i));
    ASSERT_OK(batch.Put(key, value));
  }
  ASSERT_OK(db_->Write(WriteOptions(), &batch));
  Close();

  // Collect blob files.
  std::vector<std::string> blob_paths;
  {
    std::vector<std::string> filenames;
    ASSERT_OK(env_->GetChildren(dbname_, &filenames));
    for (const auto& fname : filenames) {
      uint64_t file_number;
      FileType file_type;
      if (ParseFileName(fname, &file_number, &file_type) &&
          file_type == kBlobFile) {
        blob_paths.push_back(BlobFileName(dbname_, file_number));
      }
    }
  }
  ASSERT_GE(blob_paths.size(), 2u)
      << "Expected at least 2 blob files from 2 partitions";

  // Truncate only ONE partition's blob file to 0 bytes: the other partition's
  // file retains valid data. This tests that the batch is rejected as a whole.
  auto truncate_first = [&]() {
    env_->DeleteFile(blob_paths[0]);
    ASSERT_OK(WriteStringToFile(Env::Default(), "", blob_paths[0]));
  };

  truncate_first();

  // paranoid_checks=true: batch rejected → recovery aborts.
  Status s = TryReopen(options);
  ASSERT_TRUE(s.IsAborted()) << s.ToString();

  // paranoid_checks=false: entire batch skipped (atomicity), ALL keys missing.
  truncate_first();
  options.paranoid_checks = false;
  Reopen(options);
  for (int i = 0; i < num_keys; i++) {
    std::string key = "atomickey" + std::to_string(i);
    ASSERT_EQ(Get(key), "NOT_FOUND")
        << "key=" << key << " should be missing (entire batch skipped)";
  }
}

// Regression test for the stress durability gap: when a later CF flush syncs
// an older closed WAL via SyncClosedWals(), the rotated blob file referenced
// by that WAL must become durable as well under FaultInjectionTestFS's
// unsynced-data-loss model.
TEST_F(DBBlobDirectWriteTest,
       LaterCFFlushSyncsClosedWalAndReferencedDeferredBlobFile) {
  if (encrypted_env_) {
    ROCKSDB_GTEST_SKIP("Test inspects raw file sizes under fault injection");
    return;
  }

  auto fault_fs = std::make_shared<FaultInjectionTestFS>(env_->GetFileSystem());
  fault_fs->SetFilesystemDirectWritable(false);
  fault_fs->SetInjectUnsyncedDataLoss(true);
  auto fault_env = std::make_unique<CompositeEnvWrapper>(env_, fault_fs);

  Options options = GetBlobDirectWriteOptions();
  options.env = fault_env.get();
  options.blob_direct_write_partitions = 1;
  options.blob_direct_write_buffer_size = 0;
  options.disable_auto_compactions = true;
  options.max_write_buffer_number = 8;
  options.wal_recovery_mode = WALRecoveryMode::kPointInTimeRecovery;
  DestroyAndReopen(options);
  CreateAndReopenWithCF({"cf1"}, options);

  const uint64_t bad_wal_number = dbfull()->TEST_LogfileNumber();
  ASSERT_OK(Put("bad_key", std::string(100, 'b')));

  const std::string bad_blob_path = GetOnlyBlobFilePath();
  ASSERT_FALSE(bad_blob_path.empty());
  const std::string bad_wal_path = LogFileName(dbname_, bad_wal_number);

  uint64_t logical_blob_size = 0;
  ASSERT_OK(fault_fs->GetFileSize(bad_blob_path, IOOptions(),
                                  &logical_blob_size, nullptr));
  ASSERT_GT(logical_blob_size, 0);
  ASSERT_EQ(GetUnderlyingFileSize(bad_blob_path), 0);

  uint64_t logical_wal_size = 0;
  ASSERT_OK(fault_fs->GetFileSize(bad_wal_path, IOOptions(), &logical_wal_size,
                                  nullptr));
  ASSERT_GT(logical_wal_size, 0);
  ASSERT_EQ(GetUnderlyingFileSize(bad_wal_path), 0);

  ASSERT_OK(dbfull()->TEST_SwitchMemtable());
  ASSERT_NE(dbfull()->TEST_LogfileNumber(), bad_wal_number);

  ASSERT_OK(Put(1, "cf1_key", "small"));
  ASSERT_OK(Flush(1));

  ASSERT_GT(GetUnderlyingFileSize(bad_wal_path), 0);
  ASSERT_GT(GetUnderlyingFileSize(bad_blob_path), 0);

  // Simulate crash-style loss of any remaining unsynced tails. The deferred
  // blob file referenced by the now-synced closed WAL must remain durable.
  ASSERT_OK(fault_fs->DropUnsyncedFileData());
  ASSERT_GT(GetUnderlyingFileSize(bad_wal_path), 0);
  ASSERT_GT(GetUnderlyingFileSize(bad_blob_path), 0);
  Close();
}

// Regression test for the active-file variant of the same durability gap:
// another CF can switch the WAL and later flush it while this CF's blob file
// remains open across that WAL boundary. SyncClosedWals() must make the active
// blob file durable before the closed WAL is allowed to advance.
TEST_F(DBBlobDirectWriteTest,
       LaterCFFlushSyncsClosedWalAndReferencedActiveBlobFile) {
  if (encrypted_env_) {
    ROCKSDB_GTEST_SKIP("Test inspects raw file sizes under fault injection");
    return;
  }

  auto fault_fs = std::make_shared<FaultInjectionTestFS>(env_->GetFileSystem());
  fault_fs->SetFilesystemDirectWritable(false);
  fault_fs->SetInjectUnsyncedDataLoss(true);
  auto fault_env = std::make_unique<CompositeEnvWrapper>(env_, fault_fs);

  Options options = GetBlobDirectWriteOptions();
  options.env = fault_env.get();
  options.blob_direct_write_partitions = 1;
  options.blob_direct_write_buffer_size = 64 * 1024;
  options.disable_auto_compactions = true;
  options.max_write_buffer_number = 8;
  options.wal_recovery_mode = WALRecoveryMode::kPointInTimeRecovery;
  DestroyAndReopen(options);
  CreateAndReopenWithCF({"cf1"}, options);

  ASSERT_OK(Put("bad_key", std::string(100, 'b')));
  ASSERT_OK(Put(1, "cf1_key", "small"));

  const uint64_t bad_wal_number = dbfull()->TEST_LogfileNumber();
  const std::string bad_blob_path = GetOnlyBlobFilePath();
  ASSERT_FALSE(bad_blob_path.empty());
  const std::string bad_wal_path = LogFileName(dbname_, bad_wal_number);

  uint64_t logical_blob_size = 0;
  ASSERT_OK(fault_fs->GetFileSize(bad_blob_path, IOOptions(),
                                  &logical_blob_size, nullptr));
  ASSERT_GT(logical_blob_size, 0);
  ASSERT_EQ(GetUnderlyingFileSize(bad_blob_path), 0);

  uint64_t logical_wal_size = 0;
  ASSERT_OK(fault_fs->GetFileSize(bad_wal_path, IOOptions(), &logical_wal_size,
                                  nullptr));
  ASSERT_GT(logical_wal_size, 0);
  ASSERT_EQ(GetUnderlyingFileSize(bad_wal_path), 0);

  auto* cf1_cfd = static_cast<ColumnFamilyHandleImpl*>(handles_[1])->cfd();
  ASSERT_NE(cf1_cfd, nullptr);
  ASSERT_OK(dbfull()->TEST_SwitchMemtable(cf1_cfd));
  ASSERT_NE(dbfull()->TEST_LogfileNumber(), bad_wal_number);

  ASSERT_OK(Flush(1));

  ASSERT_GT(GetUnderlyingFileSize(bad_wal_path), 0);
  ASSERT_GT(GetUnderlyingFileSize(bad_blob_path), 0);

  ASSERT_OK(fault_fs->DropUnsyncedFileData());
  ASSERT_GT(GetUnderlyingFileSize(bad_wal_path), 0);
  ASSERT_GT(GetUnderlyingFileSize(bad_blob_path), 0);
  Close();
}

// Regression test for the current-WAL variant of the same durability issue:
// an explicit SyncWAL/FlushWAL(true) must also sync blob files referenced by
// the current WAL before that WAL is marked durable.
TEST_F(DBBlobDirectWriteTest, SyncWALSyncsCurrentWalReferencedActiveBlobFile) {
  if (encrypted_env_) {
    ROCKSDB_GTEST_SKIP("Test inspects raw file sizes under fault injection");
    return;
  }

  auto fault_fs = std::make_shared<FaultInjectionTestFS>(env_->GetFileSystem());
  fault_fs->SetFilesystemDirectWritable(false);
  fault_fs->SetInjectUnsyncedDataLoss(true);
  auto fault_env = std::make_unique<CompositeEnvWrapper>(env_, fault_fs);

  Options options = GetBlobDirectWriteOptions();
  options.env = fault_env.get();
  options.blob_direct_write_partitions = 1;
  options.blob_direct_write_buffer_size = 0;
  options.disable_auto_compactions = true;
  options.max_write_buffer_number = 8;
  options.wal_recovery_mode = WALRecoveryMode::kPointInTimeRecovery;
  DestroyAndReopen(options);

  ASSERT_OK(Put("bad_key", std::string(100, 'b')));

  const uint64_t wal_number = dbfull()->TEST_LogfileNumber();
  const std::string blob_path = GetOnlyBlobFilePath();
  ASSERT_FALSE(blob_path.empty());
  const std::string wal_path = LogFileName(dbname_, wal_number);

  uint64_t logical_blob_size = 0;
  ASSERT_OK(fault_fs->GetFileSize(blob_path, IOOptions(), &logical_blob_size,
                                  nullptr));
  ASSERT_GT(logical_blob_size, 0);
  ASSERT_EQ(GetUnderlyingFileSize(blob_path), 0);

  uint64_t logical_wal_size = 0;
  ASSERT_OK(
      fault_fs->GetFileSize(wal_path, IOOptions(), &logical_wal_size, nullptr));
  ASSERT_GT(logical_wal_size, 0);
  ASSERT_EQ(GetUnderlyingFileSize(wal_path), 0);

  ASSERT_OK(db_->FlushWAL(true));

  ASSERT_GT(GetUnderlyingFileSize(wal_path), 0);
  ASSERT_GT(GetUnderlyingFileSize(blob_path), 0);

  ASSERT_OK(fault_fs->DropUnsyncedFileData());
  ASSERT_GT(GetUnderlyingFileSize(wal_path), 0);
  ASSERT_GT(GetUnderlyingFileSize(blob_path), 0);
  Close();
}

// A later sync=true write can make earlier async blob-index entries in the
// same current WAL durable even when the later write itself does not use blob
// direct write. The referenced blob file must be synced before WAL sync.
TEST_F(DBBlobDirectWriteTest, SyncWriteSyncsEarlierCurrentWalBlobFile) {
  if (encrypted_env_) {
    ROCKSDB_GTEST_SKIP("Test inspects raw file sizes under fault injection");
    return;
  }

  auto fault_fs = std::make_shared<FaultInjectionTestFS>(env_->GetFileSystem());
  fault_fs->SetFilesystemDirectWritable(false);
  fault_fs->SetInjectUnsyncedDataLoss(true);
  auto fault_env = std::make_unique<CompositeEnvWrapper>(env_, fault_fs);

  Options options = GetBlobDirectWriteOptions();
  options.env = fault_env.get();
  options.blob_direct_write_partitions = 1;
  options.blob_direct_write_buffer_size = 64 * 1024;
  options.disable_auto_compactions = true;
  options.max_write_buffer_number = 8;
  options.wal_recovery_mode = WALRecoveryMode::kPointInTimeRecovery;
  DestroyAndReopen(options);

  ASSERT_OK(Put("bad_key", std::string(100, 'b')));

  const uint64_t wal_number = dbfull()->TEST_LogfileNumber();
  const std::string blob_path = GetOnlyBlobFilePath();
  ASSERT_FALSE(blob_path.empty());
  const std::string wal_path = LogFileName(dbname_, wal_number);

  uint64_t logical_blob_size = 0;
  ASSERT_OK(fault_fs->GetFileSize(blob_path, IOOptions(), &logical_blob_size,
                                  nullptr));
  ASSERT_GT(logical_blob_size, 0);
  ASSERT_EQ(GetUnderlyingFileSize(blob_path), 0);

  uint64_t logical_wal_size = 0;
  ASSERT_OK(
      fault_fs->GetFileSize(wal_path, IOOptions(), &logical_wal_size, nullptr));
  ASSERT_GT(logical_wal_size, 0);
  ASSERT_EQ(GetUnderlyingFileSize(wal_path), 0);

  WriteOptions sync_write_options;
  sync_write_options.sync = true;
  ASSERT_OK(db_->Put(sync_write_options, "sync_key", "small"));

  ASSERT_GT(GetUnderlyingFileSize(wal_path), 0);
  ASSERT_GT(GetUnderlyingFileSize(blob_path), 0);

  ASSERT_OK(fault_fs->DropUnsyncedFileData());
  ASSERT_GT(GetUnderlyingFileSize(wal_path), 0);
  ASSERT_GT(GetUnderlyingFileSize(blob_path), 0);
  Close();
}

// Reproduce the stress failure mode where point-in-time recovery stops at a
// BlobIndex batch referencing an empty orphan blob file, and another CF has
// already flushed newer data to SST. Recovery must fail with the multi-CF
// consistency check rather than a plain batch-validation abort.
TEST_F(DBBlobDirectWriteTest,
       PointInTimeRecoveryFailsWhenLaterCFAheadOfEmptyOrphanBatch) {
  if (encrypted_env_) {
    ROCKSDB_GTEST_SKIP("Test creates intentionally malformed files");
    return;
  }

  Options options = GetBlobDirectWriteOptions();
  options.blob_direct_write_partitions = 1;
  options.blob_direct_write_buffer_size = 0;
  options.disable_auto_compactions = true;
  options.max_write_buffer_number = 8;
  options.wal_recovery_mode = WALRecoveryMode::kPointInTimeRecovery;
  DestroyAndReopen(options);
  CreateAndReopenWithCF({"cf1"}, options);

  // Write a blob-index batch into the current WAL and remember its blob file.
  ASSERT_OK(Put("bad_key", std::string(100, 'b')));
  const std::string bad_blob_path = GetOnlyBlobFilePath();
  ASSERT_FALSE(bad_blob_path.empty());

  // Advance to a later WAL while keeping the default CF data unflushed, then
  // flush a different CF so its log number moves past the bad batch's WAL.
  ASSERT_OK(dbfull()->TEST_SwitchMemtable());
  ASSERT_OK(Put(1, "cf1_key", "small"));
  ASSERT_OK(Flush(1));
  Close();

  // Simulate crash before the orphan blob file's contents are durable.
  ASSERT_OK(env_->DeleteFile(bad_blob_path));
  ASSERT_OK(WriteStringToFile(env_, "", bad_blob_path));

  Status s = TryReopenWithColumnFamilies({"default", "cf1"}, options);
  ASSERT_TRUE(s.IsCorruption()) << s.ToString();
  ASSERT_NE(s.ToString().find("Column family inconsistency"), std::string::npos)
      << s.ToString();
  ASSERT_NE(s.ToString().find("beyond the point of corruption"),
            std::string::npos)
      << s.ToString();
}

// Truncate an orphan blob file mid-record. With paranoid_checks=true,
// recovery aborts when the first batch referencing truncated data is
// encountered (write batch atomicity). With paranoid_checks=false, batches
// with unresolvable blob indices are skipped.
TEST_F(DBBlobDirectWriteTest, RecoveryPartialFile) {
  Options options = GetBlobDirectWriteOptions();
  options.blob_direct_write_partitions = 1;
  options.blob_direct_write_buffer_size = 0;
  options.blob_file_size = 1024 * 1024;  // 1MB, single file
  options.statistics = CreateDBStatistics();
  DestroyAndReopen(options);

  const int num_keys = 10;
  WriteLargeValues(num_keys, 100);
  Close();

  auto truncate_blob_file = [&]() {
    std::vector<std::string> filenames;
    ASSERT_OK(env_->GetChildren(dbname_, &filenames));
    std::string blob_path;
    for (const auto& fname : filenames) {
      uint64_t file_number;
      FileType file_type;
      if (ParseFileName(fname, &file_number, &file_type) &&
          file_type == kBlobFile) {
        blob_path = BlobFileName(dbname_, file_number);
        break;
      }
    }
    ASSERT_FALSE(blob_path.empty());
    uint64_t orig_size;
    ASSERT_OK(env_->GetFileSize(blob_path, &orig_size));
    std::string content;
    ASSERT_OK(ReadFileToString(env_, blob_path, &content));
    content.resize(static_cast<size_t>(orig_size / 2));
    ASSERT_OK(WriteStringToFile(env_, content, blob_path));
  };

  truncate_blob_file();

  // paranoid_checks=true (default): recovery aborts at the first batch whose
  // blob data is in the truncated region.
  Status s = TryReopen(options);
  ASSERT_TRUE(s.IsAborted()) << s.ToString();

  // paranoid_checks=false: batches with unresolvable blobs are skipped,
  // batches with resolvable blobs are applied.
  options.paranoid_checks = false;
  options.statistics = CreateDBStatistics();
  truncate_blob_file();
  Reopen(options);

  int readable = 0;
  for (int i = 0; i < num_keys; i++) {
    std::string key = "key" + std::to_string(i);
    PinnableSlice result;
    Status s2 =
        db_->Get(ReadOptions(), db_->DefaultColumnFamily(), key, &result);
    if (s2.ok()) {
      readable++;
    }
  }
  ASSERT_GT(readable, 0) << "At least some records before truncation";
  ASSERT_LT(readable, num_keys)
      << "Some records after truncation should be lost";
}

// Mix of registered (flushed) and orphan (unflushed) blob files.
TEST_F(DBBlobDirectWriteTest, RecoveryMixedRegisteredAndOrphan) {
  Options options = GetBlobDirectWriteOptions();
  options.blob_direct_write_partitions = 1;
  options.blob_direct_write_buffer_size = 0;
  options.statistics = CreateDBStatistics();
  DestroyAndReopen(options);

  // Write first batch and flush (registered in MANIFEST).
  WriteLargeValues(10, 100, "flushed_");
  ASSERT_OK(Flush());

  // Write second batch without flush (will be orphan).
  WriteLargeValues(10, 100, "orphan_");

  // Close: second batch creates orphan blob files.
  Close();
  Reopen(options);

  // Both batches should be readable.
  VerifyLargeValues(10, 100, "flushed_");
  VerifyLargeValues(10, 100, "orphan_");

  // Orphan recovery should have resolved some records.
  ASSERT_GT(
      options.statistics->getTickerCount(BLOB_DB_ORPHAN_RECOVERY_RESOLVED), 0);

  // Second reopen to verify consistency.
  Reopen(options);
  VerifyLargeValues(10, 100, "flushed_");
  VerifyLargeValues(10, 100, "orphan_");
}

// Verify that recovery metrics (tickers) are correctly updated.
TEST_F(DBBlobDirectWriteTest, RecoveryOrphanMetrics) {
  Options options = GetBlobDirectWriteOptions();
  options.blob_direct_write_partitions = 1;
  options.blob_direct_write_buffer_size = 0;
  options.statistics = CreateDBStatistics();
  DestroyAndReopen(options);

  // Write data without flush.
  const int num_keys = 5;
  WriteLargeValues(num_keys, 100);
  Close();

  // Reopen with fresh statistics to capture only recovery metrics.
  options.statistics = CreateDBStatistics();
  Reopen(options);

  // All keys should be recovered.
  VerifyLargeValues(num_keys, 100);

  // Verify resolved count: each orphan blob is resolved twice -- once during
  // pre-validation (batch atomicity check) and once during InsertInto.
  uint64_t resolved =
      options.statistics->getTickerCount(BLOB_DB_ORPHAN_RECOVERY_RESOLVED);
  ASSERT_EQ(resolved, static_cast<uint64_t>(num_keys) * 2);

  // No records should be discarded (all blob data was intact).
  uint64_t discarded =
      options.statistics->getTickerCount(BLOB_DB_ORPHAN_RECOVERY_DISCARDED);
  ASSERT_EQ(discarded, 0);
}

// Verify that orphan recovery truncates partial last records and the file
// is sealed at valid_data_end. This simulates SIGKILL during a blob write
// where the record header was flushed but the key/value data is incomplete.
TEST_F(DBBlobDirectWriteTest, RecoveryTruncatesPartialRecord) {
  if (encrypted_env_) {
    ROCKSDB_GTEST_SKIP("Test creates intentionally malformed files");
    return;
  }
  Options options = GetBlobDirectWriteOptions();
  options.blob_direct_write_partitions = 1;
  options.blob_direct_write_buffer_size = 0;
  options.blob_file_size = 1024 * 1024;  // 1MB, single file
  options.statistics = CreateDBStatistics();
  DestroyAndReopen(options);

  // Write 10 keys — all go to the same blob file.
  const int num_keys = 10;
  WriteLargeValues(num_keys, 100);
  Close();

  // Find the orphan blob file (sealed during close, not in MANIFEST).
  std::vector<std::string> filenames;
  ASSERT_OK(env_->GetChildren(dbname_, &filenames));
  std::string blob_path;
  uint64_t blob_file_number = 0;
  for (const auto& fname : filenames) {
    uint64_t file_number;
    FileType file_type;
    if (ParseFileName(fname, &file_number, &file_type) &&
        file_type == kBlobFile) {
      blob_path = BlobFileName(dbname_, file_number);
      blob_file_number = file_number;
      break;
    }
  }
  ASSERT_NE(blob_file_number, 0);

  // Read the original content. The file has: header + 10 records + footer.
  std::string content;
  ASSERT_OK(ReadFileToString(env_, blob_path, &content));
  uint64_t orig_size = content.size();
  ASSERT_GE(orig_size, BlobLogHeader::kSize + BlobLogFooter::kSize);

  // Remove the footer and append a partial record: valid header but
  // truncated key/value data. This simulates SIGKILL during a write.
  uint64_t valid_data_end = orig_size - BlobLogFooter::kSize;
  content.resize(static_cast<size_t>(valid_data_end));

  // Create a fake record header for a large record (larger than remaining
  // file space if the file were read naively).
  BlobLogRecord fake_record;
  fake_record.key = Slice("fake_partial_key");
  std::string fake_record_value(500, 'X');
  fake_record.value = Slice(fake_record_value);
  fake_record.expiration = 0;
  std::string fake_header;
  fake_record.EncodeHeaderTo(&fake_header);
  // Append just the header + a few bytes of key (partial record).
  content.append(fake_header);
  content.append("fak");  // 3 bytes of partial key data
  ASSERT_OK(WriteStringToFile(env_, content, blob_path));

  uint64_t corrupted_size = content.size();
  ASSERT_GT(corrupted_size, valid_data_end);

  // Reopen: orphan recovery should detect the partial record, truncate
  // the file to valid_data_end, then seal with a footer.
  Reopen(options);

  // All 10 keys should be readable (their records were before the partial).
  VerifyLargeValues(num_keys, 100);

  // All records should have been resolved (none discarded — the partial
  // record at the end was not referenced by any WAL entry). Each orphan blob
  // is resolved twice (pre-validation + InsertInto).
  ASSERT_EQ(
      options.statistics->getTickerCount(BLOB_DB_ORPHAN_RECOVERY_RESOLVED),
      static_cast<uint64_t>(num_keys) * 2);
  ASSERT_EQ(
      options.statistics->getTickerCount(BLOB_DB_ORPHAN_RECOVERY_DISCARDED), 0);

  // Reopen again to verify MANIFEST consistency after truncation.
  Reopen(options);
  VerifyLargeValues(num_keys, 100);
}

// Verify that WAL entries referencing records in the truncated (partial)
// region are correctly discarded during recovery. This tests the full
// crash scenario: blob data partially written, WAL committed.
TEST_F(DBBlobDirectWriteTest, RecoveryDiscardsEntriesInTruncatedRegion) {
  if (encrypted_env_) {
    ROCKSDB_GTEST_SKIP("Test creates intentionally malformed files");
    return;
  }
  Options options = GetBlobDirectWriteOptions();
  options.blob_direct_write_partitions = 1;
  options.blob_direct_write_buffer_size = 0;
  options.blob_file_size = 1024 * 1024;
  options.statistics = CreateDBStatistics();
  DestroyAndReopen(options);

  const int num_keys = 10;
  WriteLargeValues(num_keys, 100);
  Close();

  auto corrupt_blob_file = [&]() {
    std::vector<std::string> filenames;
    ASSERT_OK(env_->GetChildren(dbname_, &filenames));
    std::string blob_path;
    for (const auto& fname : filenames) {
      uint64_t file_number;
      FileType file_type;
      if (ParseFileName(fname, &file_number, &file_type) &&
          file_type == kBlobFile) {
        blob_path = BlobFileName(dbname_, file_number);
        break;
      }
    }
    ASSERT_FALSE(blob_path.empty());
    std::string content;
    ASSERT_OK(ReadFileToString(env_, blob_path, &content));
    uint64_t orig_size = content.size();
    uint64_t trunc_size = (orig_size * 6) / 10;
    content.resize(static_cast<size_t>(trunc_size));
    BlobLogRecord fake;
    fake.key = Slice("x");
    std::string fake_value(200, 'Z');
    fake.value = Slice(fake_value);
    fake.expiration = 0;
    std::string fake_hdr;
    fake.EncodeHeaderTo(&fake_hdr);
    content.append(fake_hdr);
    content.append("x");
    ASSERT_OK(WriteStringToFile(env_, content, blob_path));
  };

  corrupt_blob_file();

  // paranoid_checks=true: recovery aborts when a batch references a blob
  // record in the truncated region.
  Status s = TryReopen(options);
  ASSERT_TRUE(s.IsAborted()) << s.ToString();

  // paranoid_checks=false: unresolvable batches skipped, rest applied.
  options.paranoid_checks = false;
  options.statistics = CreateDBStatistics();
  corrupt_blob_file();
  Reopen(options);

  int readable = 0;
  for (int i = 0; i < num_keys; i++) {
    std::string key = "key" + std::to_string(i);
    PinnableSlice result;
    Status s2 =
        db_->Get(ReadOptions(), db_->DefaultColumnFamily(), key, &result);
    if (s2.ok()) {
      readable++;
    }
  }
  ASSERT_GT(readable, 0);
  ASSERT_LT(readable, num_keys);

  // Reopen again to verify consistency (now all data is registered, no
  // orphan resolution needed).
  Reopen(options);
  int readable2 = 0;
  for (int i = 0; i < num_keys; i++) {
    std::string key = "key" + std::to_string(i);
    PinnableSlice result;
    Status s2 =
        db_->Get(ReadOptions(), db_->DefaultColumnFamily(), key, &result);
    if (s2.ok()) {
      readable2++;
    }
  }
  ASSERT_EQ(readable, readable2) << "Readable count must be stable";
}

// Test: verify linked_ssts are properly set after orphan recovery.
// Writes data without flush (creating orphan blob files), then closes and
// reopens. After recovery, checks blob files in the version and their
// linked_ssts.
TEST_F(DBBlobDirectWriteTest, OrphanRecoveryLinkedSsts) {
  Options options = GetBlobDirectWriteOptions();
  options.blob_direct_write_partitions = 1;
  options.blob_direct_write_buffer_size = 0;
  options.blob_file_size = 1024 * 1024;
  options.disable_auto_compactions = true;
  options.statistics = CreateDBStatistics();
  DestroyAndReopen(options);

  // Write values without flush → blob files on disk but not in MANIFEST.
  const int num_keys = 20;
  WriteLargeValues(num_keys, 100);

  // Verify readable before crash.
  VerifyLargeValues(num_keys, 100);

  // Close simulates crash: blob files exist but not in MANIFEST.
  Close();

  // Reopen triggers WAL replay + orphan blob file recovery.
  Reopen(options);

  // Check blob files in the version after recovery.
  auto blob_infos = GetBlobFileInfoFromVersion();

  // Blob files should be present in the version.
  ASSERT_FALSE(blob_infos.empty())
      << "Blob files missing from version after recovery";

  // Verify data is still readable.
  VerifyLargeValues(num_keys, 100);

  // Flush to create SSTs that reference the blob files.
  ASSERT_OK(Flush());

  // After flush, check linked_ssts.
  auto blob_infos_flushed = GetBlobFileInfoFromVersion();
  ASSERT_FALSE(blob_infos_flushed.empty());

  // Verify data still readable.
  VerifyLargeValues(num_keys, 100);
}

// Test: verify blob files survive compaction after orphan recovery.
// This is the actual bug scenario: orphan blob files may lose their
// linked_ssts relationship after compaction, causing PurgeObsoleteFiles
// to delete them.
TEST_F(DBBlobDirectWriteTest, OrphanRecoveryBlobSurvivesCompaction) {
  Options options = GetBlobDirectWriteOptions();
  options.blob_direct_write_partitions = 1;
  options.blob_direct_write_buffer_size = 0;
  options.blob_file_size = 1024 * 1024;
  options.disable_auto_compactions = true;
  options.statistics = CreateDBStatistics();
  DestroyAndReopen(options);

  // Write values without flush (orphan blob files).
  const int num_keys = 20;
  WriteLargeValues(num_keys, 100);
  VerifyLargeValues(num_keys, 100);

  // Close + reopen → orphan recovery.
  Close();
  Reopen(options);
  VerifyLargeValues(num_keys, 100);

  // Flush to create SSTs referencing blob files.
  ASSERT_OK(Flush());

  // Log pre-compaction state.
  auto blob_infos_pre = GetBlobFileInfoFromVersion();
  ASSERT_FALSE(blob_infos_pre.empty());

  // Write more data to create L0 files for compaction to work with.
  WriteLargeValues(20, 100, "batch2_");
  ASSERT_OK(Flush());
  WriteLargeValues(20, 100, "batch3_");
  ASSERT_OK(Flush());

  // Trigger full compaction that rewrites SSTs.
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));

  // Check blob files after compaction.
  auto blob_infos_post = GetBlobFileInfoFromVersion();

  // THE KEY ASSERTION: blob files from batch1 should still exist.
  ASSERT_FALSE(blob_infos_post.empty())
      << "Bug reproduced: blob files dropped from version after compaction";

  // Verify blob files still on disk.
  std::vector<std::string> filenames;
  ASSERT_OK(env_->GetChildren(dbname_, &filenames));
  int blob_file_count = 0;
  for (const auto& fname : filenames) {
    uint64_t file_number;
    FileType file_type;
    if (ParseFileName(fname, &file_number, &file_type) &&
        file_type == kBlobFile) {
      blob_file_count++;
    }
  }
  ASSERT_GT(blob_file_count, 0)
      << "Bug reproduced: blob files deleted from disk after compaction";

  // All values should be readable.
  VerifyLargeValues(num_keys, 100);
  VerifyLargeValues(20, 100, "batch2_");
  VerifyLargeValues(20, 100, "batch3_");
}

// Test that with multiple partitions, only the oldest blob file per SST gets
// linked_ssts. Non-oldest blob files survive via garbage_count < total_count,
// including after a compaction rewrites the SSTs.
TEST_F(DBBlobDirectWriteTest, MultiPartitionLinkedSstsAfterCompaction) {
  Options options = GetBlobDirectWriteOptions();
  options.blob_direct_write_partitions = 4;
  options.blob_direct_write_buffer_size = 0;
  options.blob_file_size = 1024 * 1024;
  options.disable_auto_compactions = true;
  options.statistics = CreateDBStatistics();
  DestroyAndReopen(options);

  // Step 1: Write enough keys to populate all 4 partitions.
  const int num_keys = 40;
  WriteLargeValues(num_keys, 200);
  ASSERT_OK(Flush());

  // Step 2: Inspect blob file linked_ssts state.
  auto blob_infos = GetBlobFileInfoFromVersion();

  // With 4 partitions, we expect multiple blob files.
  ASSERT_GE(blob_infos.size(), 2u)
      << "Expected multiple blob files from 4 partitions";

  // Count how many blob files have linked_ssts > 0.
  int linked_count = 0;
  int unlinked_count = 0;
  for (const auto& bi : blob_infos) {
    if (bi.linked_ssts_count > 0) {
      linked_count++;
    } else {
      unlinked_count++;
    }
    // All blob files should have zero garbage initially.
    ASSERT_EQ(bi.garbage_blob_count, 0u);
  }

  // With multiple partitions, only the oldest blob file gets linked.
  // This documents the current design limitation.
  ASSERT_EQ(linked_count, 1)
      << "Expected exactly 1 blob file with linked_ssts "
         "(the one matching oldest_blob_file_number on the SST)";
  ASSERT_GE(unlinked_count, 1)
      << "Expected at least 1 unlinked blob file from non-oldest partitions";

  // Step 3: Verify all data is readable.
  VerifyLargeValues(num_keys, 200);

  // Step 4: Write more data to create additional L0 files for compaction.
  WriteLargeValues(40, 200, "batch2_");
  ASSERT_OK(Flush());

  // Step 5: Compact (without blob GC) — blobs just pass through.
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));

  auto blob_infos_post = GetBlobFileInfoFromVersion();

  // All original blob files should survive compaction (no garbage was added).
  int post_compaction_unlinked_count = 0;
  for (const auto& bi : blob_infos) {
    bool found = false;
    for (const auto& bi_post : blob_infos_post) {
      if (bi_post.file_number == bi.file_number) {
        found = true;
        if (bi_post.linked_ssts_count == 0) {
          post_compaction_unlinked_count++;
        }
        // Garbage should still be 0 since we didn't delete/overwrite anything.
        ASSERT_EQ(bi_post.garbage_blob_count, 0u)
            << "Unexpected garbage on blob file " << bi.file_number;
        break;
      }
    }
    ASSERT_TRUE(found) << "Blob file " << bi.file_number
                       << " disappeared after compaction (no GC)";
  }
  ASSERT_GE(post_compaction_unlinked_count, 1)
      << "Expected at least one live blob file to remain unlinked after "
         "compaction";

  // All data should still be readable.
  VerifyLargeValues(num_keys, 200);
  VerifyLargeValues(40, 200, "batch2_");
}

// Test that blob GC with multiple partitions correctly handles
// unlinked blob files. When blob GC relocates blobs from a file,
// the old file should only be dropped if ALL its blobs are relocated.
TEST_F(DBBlobDirectWriteTest, MultiPartitionBlobGCDoesNotDropLiveFiles) {
  Options options = GetBlobDirectWriteOptions();
  options.blob_direct_write_partitions = 4;
  options.blob_direct_write_buffer_size = 0;
  options.blob_file_size = 1024 * 1024;
  options.disable_auto_compactions = true;
  options.enable_blob_garbage_collection = true;
  options.blob_garbage_collection_age_cutoff = 1.0;
  options.blob_garbage_collection_force_threshold = 0.0;
  options.statistics = CreateDBStatistics();
  DestroyAndReopen(options);

  // Write initial data across all 4 partitions.
  const int num_keys = 40;
  WriteLargeValues(num_keys, 200);
  ASSERT_OK(Flush());

  auto blob_infos_initial = GetBlobFileInfoFromVersion();
  ASSERT_GE(blob_infos_initial.size(), 2u);

  // Overwrite HALF the keys — this creates garbage for some blob files.
  for (int i = 0; i < num_keys / 2; i++) {
    std::string key = "key" + std::to_string(i);
    ASSERT_OK(Put(key, std::string(200, 'X')));
  }
  ASSERT_OK(Flush());

  // Compact with blob GC — this should relocate old blobs and add garbage.
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));

  auto blob_infos_post_gc = GetBlobFileInfoFromVersion();

  // THE KEY CHECK: all data must be readable.
  // If any blob file was prematurely dropped, reads will fail.
  for (int i = 0; i < num_keys / 2; i++) {
    std::string key = "key" + std::to_string(i);
    ASSERT_EQ(Get(key), std::string(200, 'X'))
        << "Overwritten key " << key << " not readable after blob GC";
  }
  for (int i = num_keys / 2; i < num_keys; i++) {
    std::string key = "key" + std::to_string(i);
    std::string expected = DefaultValueFn(i, 200);
    ASSERT_EQ(Get(key), expected)
        << "Original key " << key << " not readable after blob GC";
  }
}

// Test the full crash recovery + compaction scenario with multiple partitions.
// After recovery, orphan resolver converts kTypeBlobIndex → kTypeValue, so
// subsequent flush creates NEW blob files via BlobFileBuilder. The orphan
// files are registered in MANIFEST but have no SST references — they are
// correctly dropped by SaveBlobFilesTo since their data was copied.
TEST_F(DBBlobDirectWriteTest, MultiPartitionRecoveryThenCompaction) {
  Options options = GetBlobDirectWriteOptions();
  options.blob_direct_write_partitions = 4;
  options.blob_direct_write_buffer_size = 0;
  options.blob_file_size = 1024 * 1024;
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);

  // Write data — creates blob files via direct write (unflushed = orphans).
  const int num_keys = 40;
  WriteLargeValues(num_keys, 200);

  // Close without flush → orphan blob files.
  Close();

  // Reopen → orphan recovery converts kTypeBlobIndex → kTypeValue.
  Reopen(options);
  VerifyLargeValues(num_keys, 200);

  // Flush creates NEW blob files (from BlobFileBuilder), not orphans.
  ASSERT_OK(Flush());

  auto blob_infos = GetBlobFileInfoFromVersion();
  // After recovery, orphan data is re-encoded into new blob files via
  // BlobFileBuilder. The orphan files 8-11 are dropped from the version
  // because they have no linked SSTs and their numbers are below
  // oldest_blob_file_with_linked_ssts. This is correct — their data lives
  // in the new file.
  ASSERT_GE(blob_infos.size(), 1u);

  // Write more data and flush to create multiple L0 files.
  WriteLargeValues(40, 200, "post_recovery_");
  ASSERT_OK(Flush());

  // Compact.
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));

  // Verify all data survives.
  VerifyLargeValues(num_keys, 200);
  VerifyLargeValues(40, 200, "post_recovery_");

  // Reopen again (simulating whitebox reopen=20).
  Reopen(options);
  VerifyLargeValues(num_keys, 200);
  VerifyLargeValues(40, 200, "post_recovery_");

  // Compact again after reopen.
  WriteLargeValues(20, 200, "reopen_batch_");
  ASSERT_OK(Flush());
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));

  // Final verification — all data should survive multiple compaction rounds.
  VerifyLargeValues(num_keys, 200);
  VerifyLargeValues(40, 200, "post_recovery_");
  VerifyLargeValues(20, 200, "reopen_batch_");
}

// Test the scenario that most closely matches the crash test failure:
// recovery + blob GC compaction with multiple partitions.
// This combines orphan recovery with blob GC that can add garbage
// to unlinked blob files.
TEST_F(DBBlobDirectWriteTest, MultiPartitionRecoveryWithBlobGC) {
  Options options = GetBlobDirectWriteOptions();
  options.blob_direct_write_partitions = 4;
  options.blob_direct_write_buffer_size = 0;
  options.blob_file_size = 1024 * 1024;
  options.disable_auto_compactions = true;
  options.enable_blob_garbage_collection = true;
  options.blob_garbage_collection_age_cutoff = 1.0;
  options.blob_garbage_collection_force_threshold = 0.0;
  DestroyAndReopen(options);

  // Write initial data (will become orphans after crash).
  const int num_keys = 40;
  WriteLargeValues(num_keys, 200);

  // Crash (close without flush).
  Close();

  // Recover.
  Reopen(options);
  VerifyLargeValues(num_keys, 200);
  ASSERT_OK(Flush());

  // Overwrite half the keys to create garbage.
  for (int i = 0; i < num_keys / 2; i++) {
    std::string key = "key" + std::to_string(i);
    ASSERT_OK(Put(key, std::string(200, 'Y')));
  }
  ASSERT_OK(Flush());

  // Compact with blob GC.
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));

  // Verify all data.
  for (int i = 0; i < num_keys / 2; i++) {
    std::string key = "key" + std::to_string(i);
    ASSERT_EQ(Get(key), std::string(200, 'Y'))
        << "Key " << key << " lost after recovery + blob GC";
  }
  for (int i = num_keys / 2; i < num_keys; i++) {
    std::string key = "key" + std::to_string(i);
    std::string expected = DefaultValueFn(i, 200);
    ASSERT_EQ(Get(key), expected)
        << "Key " << key << " lost after recovery + blob GC";
  }

  // Reopen and verify again.
  Reopen(options);
  for (int i = 0; i < num_keys / 2; i++) {
    std::string key = "key" + std::to_string(i);
    ASSERT_EQ(Get(key), std::string(200, 'Y'))
        << "Key " << key << " lost after reopen following blob GC";
  }
  for (int i = num_keys / 2; i < num_keys; i++) {
    std::string key = "key" + std::to_string(i);
    std::string expected = DefaultValueFn(i, 200);
    ASSERT_EQ(Get(key), expected)
        << "Key " << key << " lost after reopen following blob GC";
  }
}

// Test the scenario where blob GC progressively relocates the "oldest linked"
// blob file across multiple compactions. Each compaction shifts which blob
// file gets linked_ssts, and unlinked files must continue to survive.
TEST_F(DBBlobDirectWriteTest, MultiPartitionProgressiveBlobGC) {
  Options options = GetBlobDirectWriteOptions();
  options.blob_direct_write_partitions = 4;
  options.blob_direct_write_buffer_size = 0;
  options.blob_file_size = 1024 * 1024;
  options.disable_auto_compactions = true;
  options.enable_blob_garbage_collection = true;
  options.blob_garbage_collection_age_cutoff = 0.25;  // GC oldest 25%
  options.blob_garbage_collection_force_threshold = 0.0;
  options.num_levels = 4;
  DestroyAndReopen(options);

  // Write batch 1: creates blob files in 4 partitions.
  WriteLargeValues(40, 200, "batch1_");
  ASSERT_OK(Flush());

  auto infos1 = GetBlobFileInfoFromVersion();
  ASSERT_EQ(infos1.size(), 4u);

  // Write batch 2: creates 4 more blob files.
  WriteLargeValues(40, 200, "batch2_");
  ASSERT_OK(Flush());

  // Write batch 3: creates 4 more blob files.
  WriteLargeValues(40, 200, "batch3_");
  ASSERT_OK(Flush());

  // Now compact — blob GC may relocate oldest files.
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));

  auto infos_post = GetBlobFileInfoFromVersion();

  // All data must be readable.
  VerifyLargeValues(40, 200, "batch1_");
  VerifyLargeValues(40, 200, "batch2_");
  VerifyLargeValues(40, 200, "batch3_");

  // Overwrite batch1 keys to create garbage in the oldest blob files.
  for (int i = 0; i < 40; i++) {
    ASSERT_OK(Put("batch1_key" + std::to_string(i), std::string(200, 'Q')));
  }
  ASSERT_OK(Flush());

  // Second compaction — should GC the old batch1 blob files.
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));

  auto infos_post2 = GetBlobFileInfoFromVersion();

  // All data readable — overwritten batch1 and original batch2/3.
  for (int i = 0; i < 40; i++) {
    ASSERT_EQ(Get("batch1_key" + std::to_string(i)), std::string(200, 'Q'));
  }
  VerifyLargeValues(40, 200, "batch2_");
  VerifyLargeValues(40, 200, "batch3_");

  // Reopen and verify.
  Reopen(options);
  for (int i = 0; i < 40; i++) {
    ASSERT_EQ(Get("batch1_key" + std::to_string(i)), std::string(200, 'Q'));
  }
  VerifyLargeValues(40, 200, "batch2_");
  VerifyLargeValues(40, 200, "batch3_");
}

// Test that GetLiveFilesStorageInfo works correctly with unlinked
// blob files from multi-partition direct write. This is the specific
// operation that fails in the crash test.
TEST_F(DBBlobDirectWriteTest, MultiPartitionGetLiveFilesStorageInfo) {
  Options options = GetBlobDirectWriteOptions();
  options.blob_direct_write_partitions = 4;
  options.blob_direct_write_buffer_size = 0;
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);

  // Write and flush.
  WriteLargeValues(40, 200);
  ASSERT_OK(Flush());

  // Get live files — this should include ALL blob files, not just linked ones.
  std::vector<LiveFileStorageInfo> live_files;
  ASSERT_OK(
      db_->GetLiveFilesStorageInfo(LiveFilesStorageInfoOptions(), &live_files));

  int blob_count_in_live = 0;
  for (const auto& f : live_files) {
    if (f.file_type == kBlobFile) {
      blob_count_in_live++;
    }
  }

  auto blob_infos = GetBlobFileInfoFromVersion();

  ASSERT_EQ(static_cast<size_t>(blob_count_in_live), blob_infos.size())
      << "GetLiveFilesStorageInfo should report ALL blob files in version";

  // Compact and check again.
  WriteLargeValues(40, 200, "extra_");
  ASSERT_OK(Flush());
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));

  live_files.clear();
  ASSERT_OK(
      db_->GetLiveFilesStorageInfo(LiveFilesStorageInfoOptions(), &live_files));

  blob_count_in_live = 0;
  for (const auto& f : live_files) {
    if (f.file_type == kBlobFile) {
      blob_count_in_live++;
    }
  }

  blob_infos = GetBlobFileInfoFromVersion();

  ASSERT_EQ(static_cast<size_t>(blob_count_in_live), blob_infos.size())
      << "GetLiveFilesStorageInfo mismatch after compaction";

  // All data readable.
  VerifyLargeValues(40, 200);
  VerifyLargeValues(40, 200, "extra_");
}

// Test that GetLiveFilesStorageInfo EXCLUDES active (unsealed) blob direct
// write files. Active files have unstable on-disk sizes, so they must not
// appear in the backup file list. They are safe to exclude because their
// data is covered by the WAL + memtable and will be replayed on recovery.
TEST_F(DBBlobDirectWriteTest, GetLiveFilesStorageInfoSizeMismatch) {
  Options options = GetBlobDirectWriteOptions();
  options.blob_direct_write_partitions = 2;
  options.blob_direct_write_buffer_size = 0;
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);

  // Write some data and flush so blob files are sealed and in the MANIFEST.
  WriteLargeValues(20, 200);
  ASSERT_OK(Flush());

  // Write more data WITHOUT flushing — blob files are active (unsealed).
  WriteLargeValues(20, 200, "batch2_");

  // Collect the set of active blob file numbers from partition managers.
  std::unordered_set<uint64_t> active_files;
  {
    InstrumentedMutexLock l(dbfull()->mutex());
    VersionSet* versions = dbfull()->GetVersionSet();
    for (auto cfd : *versions->GetColumnFamilySet()) {
      if (cfd->IsDropped()) continue;
      auto* mgr = cfd->blob_partition_manager();
      if (mgr) {
        mgr->GetActiveBlobFileNumbers(&active_files);
      }
    }
  }
  ASSERT_GT(active_files.size(), 0u) << "Expected active blob files";

  // Get live files WITH flush (default). Active files should be excluded.
  {
    std::vector<LiveFileStorageInfo> live_files;
    ASSERT_OK(db_->GetLiveFilesStorageInfo(LiveFilesStorageInfoOptions(),
                                           &live_files));

    for (const auto& f : live_files) {
      if (f.file_type == kBlobFile) {
        // After flush, all active files should have been sealed, so none
        // of the originally-active files should be excluded (they got sealed
        // by the flush). Verify size matches on-disk.
        std::string full_path = f.directory + "/" + f.relative_filename;
        uint64_t actual_size = 0;
        ASSERT_OK(env_->GetFileSize(full_path, &actual_size));
        ASSERT_EQ(f.size, actual_size)
            << "Size mismatch for blob file " << f.relative_filename
            << ": reported=" << f.size << " actual=" << actual_size;
      }
    }
  }

  // Now test the no-flush path: write data and request live files WITHOUT
  // flushing (wal_size_for_flush = max). Active blob files must be EXCLUDED.
  WriteLargeValues(10, 200, "batch3_");

  // Re-collect active files (new ones from batch3).
  std::unordered_set<uint64_t> active_files_nf;
  {
    InstrumentedMutexLock l(dbfull()->mutex());
    VersionSet* versions = dbfull()->GetVersionSet();
    for (auto cfd : *versions->GetColumnFamilySet()) {
      if (cfd->IsDropped()) continue;
      auto* mgr = cfd->blob_partition_manager();
      if (mgr) {
        mgr->GetActiveBlobFileNumbers(&active_files_nf);
      }
    }
  }

  {
    LiveFilesStorageInfoOptions opts;
    opts.wal_size_for_flush = std::numeric_limits<uint64_t>::max();
    std::vector<LiveFileStorageInfo> live_files;
    ASSERT_OK(db_->GetLiveFilesStorageInfo(opts, &live_files));

    int blob_count = 0;
    for (const auto& f : live_files) {
      if (f.file_type == kBlobFile) {
        blob_count++;
        // Active files must NOT appear in the list.
        ASSERT_EQ(active_files_nf.count(f.file_number), 0u)
            << "Active blob file " << f.file_number
            << " should be excluded from GetLiveFilesStorageInfo";
        // Sealed files: verify size matches on-disk.
        std::string full_path = f.directory + "/" + f.relative_filename;
        uint64_t actual_size = 0;
        ASSERT_OK(env_->GetFileSize(full_path, &actual_size));
        ASSERT_EQ(f.size, actual_size)
            << "Size mismatch (no-flush) for blob file " << f.relative_filename
            << ": reported=" << f.size << " actual=" << actual_size;
      }
    }
    // We should have blob files from the flushed batches.
    ASSERT_GT(blob_count, 0) << "No blob files in GetLiveFilesStorageInfo";
  }

  // Verify all data is still readable (active files served from memtable).
  VerifyLargeValues(20, 200);
  VerifyLargeValues(20, 200, "batch2_");
  VerifyLargeValues(10, 200, "batch3_");
}

// Test that repeated GetLiveFilesStorageInfo calls don't cause size mismatches.
// Active blob files are excluded, so only sealed (immutable) files appear.
// Between snapshots, sizes of sealed files must not change.
TEST_F(DBBlobDirectWriteTest, GetLiveFilesStorageInfoRepeatedCalls) {
  Options options = GetBlobDirectWriteOptions();
  options.blob_direct_write_partitions = 1;
  options.blob_direct_write_buffer_size = 0;
  options.disable_auto_compactions = true;
  // Use a small blob file size so files rotate.
  options.blob_file_size = 512;
  DestroyAndReopen(options);

  // First snapshot: write data and get live files (flush seals active files).
  WriteLargeValues(10, 100);
  std::vector<LiveFileStorageInfo> first_snapshot;
  ASSERT_OK(db_->GetLiveFilesStorageInfo(LiveFilesStorageInfoOptions(),
                                         &first_snapshot));

  // Collect blob file sizes from first snapshot.
  std::unordered_map<uint64_t, uint64_t> first_sizes;
  for (const auto& f : first_snapshot) {
    if (f.file_type == kBlobFile) {
      first_sizes[f.file_number] = f.size;
    }
  }
  ASSERT_GT(first_sizes.size(), 0u);

  // Write more data between snapshots. The new active files will be excluded.
  WriteLargeValues(10, 100, "more_");

  // Second snapshot (with flush — seals the new active files too).
  std::vector<LiveFileStorageInfo> second_snapshot;
  ASSERT_OK(db_->GetLiveFilesStorageInfo(LiveFilesStorageInfoOptions(),
                                         &second_snapshot));

  // For files present in both snapshots, sizes must match (sealed files
  // are immutable). New files may appear in the second snapshot.
  for (const auto& f : second_snapshot) {
    if (f.file_type == kBlobFile) {
      auto it = first_sizes.find(f.file_number);
      if (it != first_sizes.end()) {
        ASSERT_EQ(it->second, f.size)
            << "Blob file " << f.file_number << " changed size between "
            << "GetLiveFilesStorageInfo calls: first=" << it->second
            << " second=" << f.size;
      }
      // Verify against on-disk size.
      std::string full_path = f.directory + "/" + f.relative_filename;
      uint64_t actual_size = 0;
      ASSERT_OK(env_->GetFileSize(full_path, &actual_size));
      ASSERT_EQ(f.size, actual_size)
          << "Size mismatch for blob file " << f.file_number;
    }
  }

  // Test no-flush path: active files excluded, no size mismatch possible.
  WriteLargeValues(5, 100, "extra_");

  LiveFilesStorageInfoOptions opts_nf;
  opts_nf.wal_size_for_flush = std::numeric_limits<uint64_t>::max();
  std::vector<LiveFileStorageInfo> third_snapshot;
  ASSERT_OK(db_->GetLiveFilesStorageInfo(opts_nf, &third_snapshot));

  // Collect active blob file numbers.
  std::unordered_set<uint64_t> active_files;
  {
    InstrumentedMutexLock l(dbfull()->mutex());
    VersionSet* versions = dbfull()->GetVersionSet();
    for (auto cfd : *versions->GetColumnFamilySet()) {
      if (cfd->IsDropped()) continue;
      auto* mgr = cfd->blob_partition_manager();
      if (mgr) {
        mgr->GetActiveBlobFileNumbers(&active_files);
      }
    }
  }

  for (const auto& f : third_snapshot) {
    if (f.file_type == kBlobFile) {
      // No active files in the snapshot.
      ASSERT_EQ(active_files.count(f.file_number), 0u)
          << "Active blob file " << f.file_number << " should be excluded";
      // Size must match on-disk.
      std::string full_path = f.directory + "/" + f.relative_filename;
      uint64_t actual_size = 0;
      ASSERT_OK(env_->GetFileSize(full_path, &actual_size));
      ASSERT_EQ(f.size, actual_size)
          << "Size mismatch for blob file " << f.file_number;
    }
  }

  // All data readable.
  VerifyLargeValues(10, 100);
  VerifyLargeValues(10, 100, "more_");
  VerifyLargeValues(5, 100, "extra_");
}

// Reproduces the bug where sealed blob files are removed from
// file_to_partition_ protection even when FlushJob::Run returns OK with
// empty mems_. The blob files are never committed to MANIFEST and get
// deleted by PurgeObsoleteFiles.
//
// The bug happens when concurrent writers and multiple flush requests
// cause some flushes to see empty mems_ while having sealed blob files.
// The test spawns a writer thread that continuously writes while multiple
// flushes are triggered. If the bug exists, some blob files will be
// orphaned and deleted, causing read failures.
TEST_F(DBBlobDirectWriteTest, SealedBlobFilesNotLostOnEmptyFlush) {
  Options options = GetBlobDirectWriteOptions();
  options.atomic_flush = true;
  options.blob_direct_write_partitions = 2;
  options.write_buffer_size = 4 * 1024;  // 4KB - very small to trigger flushes
  options.max_write_buffer_number = 6;
  options.max_background_flushes = 2;
  options.blob_direct_write_buffer_size = 0;  // Synchronous seals
  Reopen(options);

  // Track the empty mems_ path.
  std::atomic<int> empty_mems_count{0};
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "FlushJob::Run:EmptyMems",
      [&](void* /* arg */) { empty_mems_count.fetch_add(1); });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  // Spawn a writer thread that continuously writes while we trigger flushes.
  std::atomic<bool> stop_writing{false};
  std::atomic<int> total_keys_written{0};
  std::thread writer_thread([&]() {
    int i = 0;
    while (!stop_writing.load(std::memory_order_relaxed)) {
      std::string key = "wkey_" + std::to_string(i);
      std::string value(100 + (i % 50), static_cast<char>('a' + (i % 26)));
      auto s = db_->Put(WriteOptions(), key, value);
      if (!s.ok()) {
        // Write stall or error — just retry.
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
        continue;
      }
      total_keys_written.fetch_add(1);
      i++;
    }
  });

  // Rapidly trigger flushes while the writer is active.
  // Multiple concurrent flush requests create the race condition.
  for (int round = 0; round < 20; round++) {
    FlushOptions flush_opts;
    flush_opts.wait = false;
    flush_opts.allow_write_stall = true;
    auto s = db_->Flush(flush_opts);
    // Flush may fail if write stall is in effect.
    s.PermitUncheckedError();
    std::this_thread::sleep_for(std::chrono::milliseconds(2));
  }

  // Stop writer and wait.
  stop_writing.store(true, std::memory_order_relaxed);
  writer_thread.join();

  // Wait for all pending flushes.
  ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());

  // Do a final flush to commit any remaining data.
  ASSERT_OK(Flush());

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearAllCallBacks();

  int num_keys = total_keys_written.load();

  // Force PurgeObsoleteFiles via compaction.
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));

  // Verify ALL written data is still readable. If sealed blob files were
  // orphaned and deleted, reads will fail with "No such file or directory".
  for (int i = 0; i < num_keys; i++) {
    std::string key = "wkey_" + std::to_string(i);
    std::string expected(100 + (i % 50), static_cast<char>('a' + (i % 26)));
    ASSERT_EQ(Get(key), expected);
  }
}

// ========================================================================
// KeyMayExist must not return false for blob direct write keys
// when blob resolution fails (e.g., read fault injection).
// Bug: KeyMayExist calls GetImpl which triggers blob resolution.
// If blob read fails (IOError), GetImpl returns IOError, and
// KeyMayExist returns false ("key definitely doesn't exist") even
// though the key IS in the memtable.
// ========================================================================
TEST_F(DBBlobDirectWriteTest, KeyMayExistWithBlobIOError) {
  Options options = GetBlobDirectWriteOptions();
  options.blob_direct_write_partitions = 1;
  DestroyAndReopen(options);

  // Write a key via blob direct write (value > min_blob_size=10).
  ASSERT_OK(Put("test_key", std::string(200, 'V')));

  // Verify normal read works (data in pending_records, resolved from memory).
  ASSERT_EQ(Get("test_key"), std::string(200, 'V'));

  // Inject IOError in MaybeResolveBlobForWritePath AFTER the blob resolution
  // attempt. This simulates what happens when:
  //   - BG thread flushed pending_records to disk
  //   - Read fault injection causes the blob file read to fail
  // The sync point fires after ResolveBlobIndexForWritePath, overriding the
  // status to IOError.
  std::atomic<int> resolve_count{0};
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::MaybeResolveBlobForWritePath:AfterResolve",
      [&](void* status_arg) {
        resolve_count.fetch_add(1);
        auto* s = static_cast<Status*>(status_arg);
        *s = Status::IOError("Injected blob read fault for KeyMayExist test");
      });
  SyncPoint::GetInstance()->EnableProcessing();

  // KeyMayExist should return true: the key IS in the memtable.
  // Bug: blob resolution fails with IOError, GetImpl returns IOError,
  // and KeyMayExist returns false ("key definitely doesn't exist").
  // The key DOES exist in the memtable -- only the blob VALUE can't be read.
  std::string value;
  bool key_may_exist = db_->KeyMayExist(
      ReadOptions(), db_->DefaultColumnFamily(), "test_key", &value);

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();

  // Verify the sync point was hit (blob resolution was attempted).
  // With the fix, blob resolution is skipped entirely (is_blob_index
  // pointer is set in KeyMayExist, preventing MaybeResolveBlobForWritePath).
  ASSERT_EQ(resolve_count.load(), 0)
      << "MaybeResolveBlobForWritePath should NOT be called after fix";

  // After fix: KeyMayExist skips blob resolution and correctly returns true.
  // The is_blob_index pointer prevents GetImpl from calling
  // MaybeResolveBlobForWritePath, so IOError cannot occur.
  ASSERT_TRUE(key_may_exist)
      << "KeyMayExist should return true for existing key even when blob "
         "resolution fails with IOError";

  Close();
}

// Same bug but for unflushed data (blob data still in pending_records
// or in-flight). When pending_records lookup succeeds, there's no bug.
// The bug manifests when data has been flushed from pending to disk by
// the BG thread but the disk read fails.
TEST_F(DBBlobDirectWriteTest, KeyMayExistUnflushedBlobIOError) {
  std::unique_ptr<FaultInjectionTestEnv> fault_env(
      new FaultInjectionTestEnv(env_));

  Options options = GetBlobDirectWriteOptions();
  options.blob_direct_write_partitions = 1;
  options.env = fault_env.get();
  DestroyAndReopen(options);

  // Write a key. Data is in pending_records (in-memory buffer).
  ASSERT_OK(Put("mem_key", std::string(200, 'M')));

  // Without flushing to SST, data is in memtable with BlobIndex.
  // KeyMayExist should find it in the memtable and return true,
  // even if blob resolution fails (because the key itself IS there).

  // For this case, pending_records lookup (Tier 2) should succeed,
  // so KeyMayExist returns true. This is the non-buggy case.
  std::string value;
  bool key_may_exist = db_->KeyMayExist(
      ReadOptions(), db_->DefaultColumnFamily(), "mem_key", &value);
  ASSERT_TRUE(key_may_exist);

  Close();
}

// ========================================================================
// Epoch-based rotation tests
// ========================================================================

// Multi-threaded stress test for blob file rotation at SwitchMemtable.
// Verifies that concurrent writers + frequent memtable switches produce
// correct results with no lost keys and no corruption.
TEST_F(DBBlobDirectWriteTest, RotationEpochStressTest) {
  Options options = GetBlobDirectWriteOptions();
  options.blob_direct_write_partitions = 4;
  options.write_buffer_size = 16 * 1024;  // 16KB - frequent SwitchMemtable
  options.max_write_buffer_number = 8;
  options.max_background_flushes = 4;
  options.blob_direct_write_buffer_size = 0;  // Synchronous mode
  Reopen(options);

  const int num_threads = 4;
  const int ops_per_thread = 200;
  std::atomic<int> total_keys{0};
  std::atomic<bool> write_error{false};
  std::vector<std::thread> threads;

  for (int t = 0; t < num_threads; t++) {
    threads.emplace_back([&, t]() {
      for (int i = 0; i < ops_per_thread; i++) {
        int key_id = t * ops_per_thread + i;
        std::string key = "rkey_" + std::to_string(key_id);
        std::string value(100 + (key_id % 50),
                          static_cast<char>('a' + (key_id % 26)));
        auto s = db_->Put(WriteOptions(), key, value);
        if (!s.ok()) {
          write_error.store(true, std::memory_order_relaxed);
          return;
        }
        total_keys.fetch_add(1, std::memory_order_relaxed);
      }
    });
  }

  for (auto& th : threads) {
    th.join();
  }
  ASSERT_FALSE(write_error.load()) << "Some Put() calls failed";

  // Flush and wait.
  ASSERT_OK(Flush());
  ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());

  int num_keys = total_keys.load();
  ASSERT_EQ(num_keys, num_threads * ops_per_thread);

  // Verify all keys.
  for (int i = 0; i < num_keys; i++) {
    std::string key = "rkey_" + std::to_string(i);
    std::string expected(100 + (i % 50), static_cast<char>('a' + (i % 26)));
    ASSERT_EQ(Get(key), expected) << "Failed to read key: " << key;
  }

  // Verify after compaction (tests that blob files survive PurgeObsolete).
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  for (int i = 0; i < num_keys; i++) {
    std::string key = "rkey_" + std::to_string(i);
    std::string expected(100 + (i % 50), static_cast<char>('a' + (i % 26)));
    ASSERT_EQ(Get(key), expected) << "After compaction: " << key;
  }

  // Verify after reopen (tests crash recovery with rotated files).
  Reopen(options);
  for (int i = 0; i < num_keys; i++) {
    std::string key = "rkey_" + std::to_string(i);
    std::string expected(100 + (i % 50), static_cast<char>('a' + (i % 26)));
    ASSERT_EQ(Get(key), expected) << "After reopen: " << key;
  }
}

// Test that rotation works correctly with crash recovery. Write data,
// trigger rotation via flush, close, reopen, and verify all data.
TEST_F(DBBlobDirectWriteTest, RotationCrashRecoveryTest) {
  Options options = GetBlobDirectWriteOptions();
  options.blob_direct_write_partitions = 2;
  options.write_buffer_size = 8 * 1024;  // 8KB
  options.blob_direct_write_buffer_size = 0;
  Reopen(options);

  // Write enough to trigger multiple memtable switches.
  const int num_keys = 500;
  WriteLargeValues(num_keys, 100, "crkey_");

  // Flush to commit everything.
  ASSERT_OK(Flush());
  ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());

  // Verify before close.
  VerifyLargeValues(num_keys, 100, "crkey_");

  // Close and reopen (simulates clean restart).
  Reopen(options);

  // Verify after reopen.
  VerifyLargeValues(num_keys, 100, "crkey_");

  // Write more data after reopen to verify rotation works across restarts.
  WriteLargeValues(num_keys, 100, "crkey2_");
  ASSERT_OK(Flush());
  ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());

  // Verify both batches.
  VerifyLargeValues(num_keys, 100, "crkey_");
  VerifyLargeValues(num_keys, 100, "crkey2_");
}

// Use SyncPoints to force the epoch mismatch race: a writer completes
// WriteBlob, then SwitchMemtable fires before the writer enters the
// write group. Verify the writer retries and succeeds.
TEST_F(DBBlobDirectWriteTest, RotationInvariantTest) {
  Options options = GetBlobDirectWriteOptions();
  options.blob_direct_write_partitions = 2;
  options.write_buffer_size = 64 * 1024;  // 64KB
  options.blob_direct_write_buffer_size = 0;
  Reopen(options);

  // Write enough data to fill the memtable, triggering rotation.
  // With 64KB memtable and ~100 byte values, ~640 keys per memtable.
  const int num_keys = 2000;  // ~3 memtable switches
  WriteLargeValues(num_keys, 100, "invkey_");

  // Flush and verify.
  ASSERT_OK(Flush());
  ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());
  VerifyLargeValues(num_keys, 100, "invkey_");

  // Compact and verify (exercises PurgeObsoleteFiles).
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  VerifyLargeValues(num_keys, 100, "invkey_");

  // Verify blob files are properly registered.
  auto blob_files = GetBlobFileInfoFromVersion();
  ASSERT_GT(blob_files.size(), 0u) << "Should have blob files after write";
  AssertBlobFilesHaveBlobs(blob_files);
  ASSERT_GT(CountLinkedBlobFiles(blob_files), 0u)
      << "Expected at least one blob file to be linked from an SST";
}

TEST_F(DBBlobDirectWriteTest, StaleLeaderRetryDoesNotReuseFollowerSequence) {
  Options options = GetBlobDirectWriteOptions();
  options.blob_direct_write_partitions = 1;
  options.blob_direct_write_buffer_size = 0;  // Synchronous blob writes
  options.write_buffer_size = 1024 * 1024;
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);

  std::mutex mu;
  std::condition_variable cv;
  bool first_blob_written = false;
  bool release_first_writer = false;
  bool leader_waiting = false;
  bool release_leader = false;
  bool follower_joined = false;
  int after_blob_write_calls = 0;
  int before_leader_calls = 0;

  auto wait_for = [&](const char* what, const std::function<bool()>& pred) {
    std::unique_lock<std::mutex> lock(mu);
    ASSERT_TRUE(cv.wait_for(lock, std::chrono::seconds(10), pred))
        << "Timed out waiting for " << what;
  };

  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::Put:AfterBlobWriteBeforeWriteImpl", [&](void*) {
        std::unique_lock<std::mutex> lock(mu);
        if (after_blob_write_calls++ == 0) {
          first_blob_written = true;
          cv.notify_all();
          cv.wait(lock, [&] { return release_first_writer; });
        }
      });
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::WriteImpl:BeforeLeaderEnters", [&](void*) {
        std::unique_lock<std::mutex> lock(mu);
        if (before_leader_calls++ == 0) {
          leader_waiting = true;
          cv.notify_all();
          cv.wait(lock, [&] { return release_leader; });
        }
      });
  SyncPoint::GetInstance()->SetCallBack("WriteThread::JoinBatchGroup:Wait",
                                        [&](void*) {
                                          std::lock_guard<std::mutex> lock(mu);
                                          follower_joined = true;
                                          cv.notify_all();
                                        });
  SyncPoint::GetInstance()->EnableProcessing();

  const std::string stale_key = "stale-leader";
  const std::string stale_value(256, 'a');
  const std::string follower_key = "fresh-follower";
  const std::string follower_value(256, 'b');
  Status stale_status;
  Status follower_status;

  std::thread stale_writer([&] { stale_status = Put(stale_key, stale_value); });
  wait_for("first blob write", [&] { return first_blob_written; });

  ASSERT_OK(dbfull()->TEST_SwitchMemtable());
  const SequenceNumber seq_before = db_->GetLatestSequenceNumber();

  {
    std::lock_guard<std::mutex> lock(mu);
    release_first_writer = true;
    cv.notify_all();
  }
  wait_for("leader before group entry", [&] { return leader_waiting; });

  std::thread follower_writer(
      [&] { follower_status = Put(follower_key, follower_value); });
  wait_for("follower to join batch group", [&] { return follower_joined; });

  {
    std::lock_guard<std::mutex> lock(mu);
    release_leader = true;
    cv.notify_all();
  }

  stale_writer.join();
  follower_writer.join();
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();

  ASSERT_OK(stale_status);
  ASSERT_OK(follower_status);
  ASSERT_EQ(db_->GetLatestSequenceNumber(), seq_before + 2);
  ASSERT_EQ(Get(stale_key), stale_value);
  ASSERT_EQ(Get(follower_key), follower_value);

  Reopen(options);
  ASSERT_EQ(Get(stale_key), stale_value);
  ASSERT_EQ(Get(follower_key), follower_value);
}

// TSAN regression: SealAllPartitions() used to log file_to_partition_.size()
// without taking file_partition_mutex_. A background flush thread can hit that
// log site while another thread rotates partitions and inserts new file-number
// mappings. This test recreates that schedule. It passes functionally both
// before and after the fix, but on the buggy code TSAN reports the data race.
TEST_F(DBBlobDirectWriteTest, SealAllPartitionsEntryLogTsanRegression) {
  Options options = GetBlobDirectWriteOptions();
  options.blob_direct_write_partitions = 4;
  options.blob_direct_write_buffer_size = 0;
  options.write_buffer_size = 8 * 1024;
  options.max_write_buffer_number = 4;
  options.max_background_flushes = 2;
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);

  WriteLargeValues(8, 200);

  std::atomic<bool> seal_paused{false};
  std::atomic<bool> allow_seal{false};
  std::atomic<int> open_after_create_calls{0};
  Status switch_status;

  auto spin_until = [&](const std::function<bool()>& pred) {
    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(10);
    while (!pred() && std::chrono::steady_clock::now() < deadline) {
      std::this_thread::yield();
    }
    return pred();
  };

  SyncPoint::GetInstance()->SetCallBack(
      "BlobFilePartitionManager::SealAllPartitions:BeforeEntryLog", [&](void*) {
        seal_paused.store(true, std::memory_order_relaxed);
        while (!allow_seal.load(std::memory_order_relaxed)) {
          std::this_thread::yield();
        }
      });
  SyncPoint::GetInstance()->SetCallBack(
      "BlobFilePartitionManager::OpenNewBlobFile:AfterCreate", [&](void*) {
        open_after_create_calls.fetch_add(1, std::memory_order_relaxed);
      });
  SyncPoint::GetInstance()->EnableProcessing();

  FlushOptions flush_opts;
  flush_opts.wait = false;
  ASSERT_OK(db_->Flush(flush_opts));

  ASSERT_TRUE(spin_until([&] {
    return seal_paused.load(std::memory_order_relaxed);
  })) << "Timed out waiting for background seal to pause";
  const int baseline_open_count =
      open_after_create_calls.load(std::memory_order_relaxed);

  std::thread switch_thread(
      [&] { switch_status = dbfull()->TEST_SwitchMemtable(); });

  ASSERT_TRUE(spin_until([&] {
    return open_after_create_calls.load(std::memory_order_relaxed) >
           baseline_open_count;
  })) << "Timed out waiting for rotation to open replacement blob files";

  allow_seal.store(true, std::memory_order_relaxed);
  switch_thread.join();

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();

  ASSERT_OK(switch_status);
  ASSERT_OK(dbfull()->TEST_FlushMemTable(true));
  VerifyLargeValues(8, 200);
}

TEST_F(DBBlobDirectWriteTest,
       TransformedWriteBatchRetryNeedsPerFileRollbackAccounting) {
  Options options = GetBlobDirectWriteOptions();
  options.blob_direct_write_partitions = 4;
  options.blob_direct_write_buffer_size = 0;  // Synchronous blob writes
  options.write_buffer_size = 1024 * 1024;
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);

  auto* cfh = static_cast<ColumnFamilyHandleImpl*>(db_->DefaultColumnFamily());
  auto* mgr = cfh->cfd()->blob_partition_manager();
  ASSERT_NE(mgr, nullptr);

  const std::vector<int> seed_value_sizes = {33, 40, 47, 54};
  for (int i = 0; i < 4; ++i) {
    ASSERT_OK(
        Put("seed" + std::to_string(i),
            std::string(seed_value_sizes[i], static_cast<char>('a' + i))));
  }
  const uint64_t old_epoch = mgr->GetRotationEpoch();

  std::unordered_set<uint64_t> old_files;
  mgr->GetActiveBlobFileNumbers(&old_files);
  ASSERT_EQ(old_files.size(), 4u);

  WriteBatch batch;
  const std::vector<int> retry_value_sizes = {35, 42, 49, 70};
  for (int i = 0; i < 4; ++i) {
    ASSERT_OK(batch.Put(
        "retry" + std::to_string(i),
        std::string(retry_value_sizes[i], static_cast<char>('k' + i))));
  }

  std::mutex mu;
  std::condition_variable cv;
  bool transform_done = false;
  bool release_writer = false;
  int after_transform_calls = 0;
  Status write_status;

  auto wait_for = [&](const char* what, const std::function<bool()>& pred) {
    std::unique_lock<std::mutex> lock(mu);
    ASSERT_TRUE(cv.wait_for(lock, std::chrono::seconds(10), pred))
        << "Timed out waiting for " << what;
  };

  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::WriteImpl:AfterTransformBatch", [&](void*) {
        std::unique_lock<std::mutex> lock(mu);
        if (after_transform_calls++ == 0) {
          transform_done = true;
          cv.notify_all();
          cv.wait(lock, [&] { return release_writer; });
        }
      });
  SyncPoint::GetInstance()->EnableProcessing();

  std::thread writer([&] {
    WriteOptions write_options;
    write_status = db_->Write(write_options, &batch);
  });

  wait_for("transform batch to finish", [&] { return transform_done; });
  ASSERT_OK(dbfull()->TEST_SwitchMemtable());
  {
    std::lock_guard<std::mutex> lock(mu);
    release_writer = true;
    cv.notify_all();
  }

  writer.join();
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();

  ASSERT_OK(write_status);
  std::vector<BlobFileAddition> additions;
  ASSERT_OK(mgr->SealAllPartitions(WriteOptions(), &additions,
                                   /*seal_all=*/false, {old_epoch}));

  std::unordered_map<uint64_t, uint64_t> total_blob_bytes_by_file;
  for (const auto& addition : additions) {
    total_blob_bytes_by_file.emplace(addition.GetBlobFileNumber(),
                                     addition.GetTotalBlobBytes());
  }

  for (uint64_t file_number : old_files) {
    auto it = total_blob_bytes_by_file.find(file_number);
    ASSERT_NE(it, total_blob_bytes_by_file.end())
        << "Missing sealed metadata for blob file " << file_number;

    std::vector<uint64_t> record_sizes;
    ReadBlobRecordSizes(file_number, &record_sizes);
    ASSERT_EQ(record_sizes.size(), 2u)
        << "Expected one committed record and one stale retry record in blob "
        << "file " << file_number;

    EXPECT_TRUE(it->second == record_sizes[0] || it->second == record_sizes[1])
        << "Blob file " << file_number << " has total_blob_bytes=" << it->second
        << " but on-disk records are sized " << record_sizes[0] << " and "
        << record_sizes[1];
  }
}

// Test that orphaned blob bytes from epoch mismatch retries are correctly
// subtracted, allowing GC to collect the sealed blob file. Without
// SubtractUncommittedBytes, the file's total_blob_bytes is inflated and
// GC never collects it because it thinks the file has more live data.
TEST_F(DBBlobDirectWriteTest, OrphanedBlobBytesSubtractedOnEpochRetry) {
  Options options = GetBlobDirectWriteOptions();
  options.blob_direct_write_partitions = 1;
  options.blob_direct_write_buffer_size = 0;  // Synchronous mode
  options.blob_file_size = 1024 * 1024;       // Large, no normal rollover
  options.write_buffer_size = 4 * 1024;       // 4KB - triggers SwitchMemtable
  options.max_write_buffer_number = 8;
  options.max_background_flushes = 4;
  options.disable_auto_compactions = true;
  options.enable_blob_garbage_collection = true;
  options.blob_garbage_collection_age_cutoff = 1.0;
  options.blob_garbage_collection_force_threshold = 0.0;
  options.statistics = CreateDBStatistics();
  DestroyAndReopen(options);

  // Step 1: Write enough data to fill the memtable and trigger flush/rotation.
  // The small write_buffer_size (4KB) means SwitchMemtable will fire after
  // a few Put calls, which calls RotateAllPartitions and bumps the epoch.
  // Some writer will naturally hit the epoch mismatch and retry.
  const int num_keys = 50;
  const int value_size = 200;
  WriteLargeValues(num_keys, value_size);

  // Flush to seal all active blob files.
  ASSERT_OK(Flush());
  ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());

  // Step 2: Verify all keys are readable.
  VerifyLargeValues(num_keys, value_size);

  // Step 3: Overwrite ALL keys so all original blob data becomes garbage.
  for (int i = 0; i < num_keys; i++) {
    std::string key = "key" + std::to_string(i);
    ASSERT_OK(Put(key, std::string(value_size, 'Z')));
  }
  ASSERT_OK(Flush());
  ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());

  // Record blob files before GC.
  auto blob_files_before_gc = GetBlobFileInfoFromVersion();
  ASSERT_GT(blob_files_before_gc.size(), 0u);

  // Step 4: Compact with GC enabled. Old blob files whose data is fully
  // garbage should be collected. If SubtractUncommittedBytes was not called
  // on epoch retry, total_blob_bytes would be inflated and GC would think
  // the file has live data, leaving it uncollected.
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));

  // Step 5: Verify that old blob files were garbage collected.
  auto blob_files_after_gc = GetBlobFileInfoFromVersion();
  // After GC, files from the first round of writes should be gone because
  // all their data was overwritten. Only files from the second round of
  // writes (the overwrite values) should remain.
  AssertSurvivingBlobFilesHaveLiveBlobs(blob_files_after_gc);

  // Step 6: Verify all keys still readable (pointing to new blob files).
  for (int i = 0; i < num_keys; i++) {
    std::string key = "key" + std::to_string(i);
    ASSERT_EQ(Get(key), std::string(value_size, 'Z'))
        << "Key " << key << " not readable after GC";
  }
}

// Directly test that SubtractUncommittedBytes correctly adjusts
// total_blob_bytes in the sealed BlobFileAddition. Writes blobs, subtracts
// some bytes (simulating epoch mismatch), seals, and verifies the addition
// has the correct total_blob_bytes.
TEST_F(DBBlobDirectWriteTest, SubtractUncommittedBytesOnEpochMismatch) {
  Options options = GetBlobDirectWriteOptions();
  options.blob_direct_write_partitions = 1;
  options.blob_direct_write_buffer_size = 0;  // Synchronous mode
  options.blob_file_size = 1024 * 1024;       // Large, no rollover
  options.disable_auto_compactions = true;
  options.file_checksum_gen_factory = GetFileChecksumGenCrc32cFactory();
  DestroyAndReopen(options);

  // Write 11 keys to establish blob data in the partition.
  // One of them (the 11th) simulates the orphaned blob — its data IS
  // physically in the blob file, but we will subtract its bytes to
  // simulate an epoch mismatch retry where the BlobIndex was discarded.
  const int num_real_keys = 10;
  const int num_total_keys = 11;  // 10 real + 1 simulated orphan
  const int value_size = 100;

  // Write all 11 keys (blob data goes to the file for all of them).
  for (int i = 0; i < num_total_keys; i++) {
    std::string key = "key" + std::to_string(i);
    ASSERT_OK(Put(key, std::string(value_size, 'X')));
  }

  // Now simulate that key10's blob write was orphaned (epoch mismatch):
  // subtract its record size from uncommitted bytes. In production, this
  // happens when the writer detects epoch mismatch and retries — the
  // BlobIndex for the first attempt is discarded, but the blob data
  // remains in the file.
  auto* cfh = static_cast<ColumnFamilyHandleImpl*>(db_->DefaultColumnFamily());
  auto* mgr = cfh->cfd()->blob_partition_manager();
  ASSERT_NE(mgr, nullptr);

  // However, we can't truly discard key10's BlobIndex (it's already in the
  // memtable). Instead, we'll delete key10 so GC treats it as garbage,
  // and subtract its record size to make the accounting match production.
  // In production: orphan has data in file but NO BlobIndex → not counted
  // as garbage by GC. Here: orphan has data in file AND a BlobIndex that
  // we delete → counted as garbage. So we need the subtraction to keep
  // total_blob_bytes >= garbage when GC processes the deletion.
  ASSERT_OK(Delete("key10"));

  const std::string orphan_key = "key10";
  const uint64_t orphan_record_size =
      BlobLogRecord::kHeaderSize + orphan_key.size() + value_size;
  mgr->SubtractUncommittedBytes(orphan_record_size, 0);  // wildcard

  // Flush to trigger SealAllPartitions. The seal should subtract the
  // uncommitted bytes from the BlobFileAddition's total_blob_bytes.
  ASSERT_OK(Flush());
  ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());

  auto blob_files_after_flush = GetBlobFileInfoFromVersion();
  ASSERT_EQ(blob_files_after_flush.size(), 1u);
  const auto& blob_file = blob_files_after_flush.front();
  const uint64_t expected_file_size =
      blob_file.total_blob_bytes + orphan_record_size + BlobLogHeader::kSize +
      BlobLogFooter::kSize;
  ASSERT_EQ(blob_file.file_size, expected_file_size);

  uint64_t actual_file_size = 0;
  ASSERT_OK(env_->GetFileSize(BlobFileName(dbname_, blob_file.file_number),
                              &actual_file_size));
  ASSERT_EQ(actual_file_size, expected_file_size);

  // Regression: checksum-based backup must copy the full sealed blob file,
  // not a truncated size derived only from live blob bytes.
  const std::string backup_dir = dbname_ + "_backup_epoch_mismatch";
  BackupEngineOptions backup_options(backup_dir, env_);
  backup_options.destroy_old_data = true;
  backup_options.max_background_operations = 4;
  std::unique_ptr<BackupEngine> backup_engine;
  BackupEngine* backup_engine_ptr = nullptr;
  IOStatus io_s = BackupEngine::Open(backup_options, env_, &backup_engine_ptr);
  ASSERT_TRUE(io_s.ok()) << io_s.ToString();
  backup_engine.reset(backup_engine_ptr);
  io_s =
      backup_engine->CreateNewBackup(db_.get(), /*flush_before_backup=*/true);
  ASSERT_TRUE(io_s.ok()) << io_s.ToString();

  // All real keys should still be readable.
  for (int i = 0; i < num_real_keys; i++) {
    std::string key = "key" + std::to_string(i);
    ASSERT_EQ(Get(key), std::string(value_size, 'X'));
  }

  // Overwrite the 10 real keys with new values (makes old blob data garbage).
  for (int i = 0; i < num_real_keys; i++) {
    std::string key = "key" + std::to_string(i);
    ASSERT_OK(Put(key, std::string(value_size, 'Y')));
  }
  ASSERT_OK(Flush());
  ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());

  // Enable GC and compact. If SubtractUncommittedBytes worked correctly,
  // total_blob_bytes (11 records - 1 orphan = 10 records) matches the
  // garbage (10 real keys overwritten + key10 deleted = ~10-11 records).
  // The file should be fully collected.
  ASSERT_OK(db_->SetOptions({
      {"enable_blob_garbage_collection", "true"},
      {"blob_garbage_collection_age_cutoff", "1.0"},
      {"blob_garbage_collection_force_threshold", "0.0"},
  }));
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));

  // Verify all real keys still readable (from new blob file).
  for (int i = 0; i < num_real_keys; i++) {
    std::string key = "key" + std::to_string(i);
    ASSERT_EQ(Get(key), std::string(value_size, 'Y'));
  }
}

// Regression test: verify the 1-blob-file-to-1-SST invariant prevents GC
// leaks from orphan bytes. Without rotation, a blob file could span two
// memtables. After overwriting the first memtable's keys, the second
// memtable's data in the same blob file would permanently block GC.
TEST_F(DBBlobDirectWriteTest, OrphanBytesBlockGC) {
  Options options = GetBlobDirectWriteOptions();
  options.blob_direct_write_partitions = 1;   // 1 partition for simplicity
  options.blob_direct_write_buffer_size = 0;  // Synchronous mode
  options.blob_file_size = 1024 * 1024;       // Large, no normal rollover
  options.write_buffer_size = 4 * 1024;       // 4KB triggers SwitchMemtable
  options.max_write_buffer_number = 8;
  options.max_background_flushes = 4;
  options.disable_auto_compactions = true;
  options.enable_blob_garbage_collection = true;
  options.blob_garbage_collection_age_cutoff = 1.0;
  options.blob_garbage_collection_force_threshold = 0.0;
  DestroyAndReopen(options);

  const int value_size = 200;

  // Write 4 keys to M0 -> all go to blob file B0.
  for (int i = 0; i < 4; i++) {
    ASSERT_OK(Put("m0key" + std::to_string(i),
                  std::string(value_size, static_cast<char>('A' + i))));
  }

  // Trigger SwitchMemtable by writing enough to fill M0.
  // Rotation: B0 -> deferred, B1 opened.
  // Continue writing to fill memtable with small values that don't go to blob.
  ASSERT_OK(Flush());
  ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());

  // Write 1 key to M1 -> goes to B1 (NOT B0, because rotation happened).
  ASSERT_OK(Put("m1key0", std::string(value_size, 'X')));

  // Flush M1 -> seals B1.
  ASSERT_OK(Flush());
  ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());

  // Verify all keys readable.
  for (int i = 0; i < 4; i++) {
    ASSERT_EQ(Get("m0key" + std::to_string(i)),
              std::string(value_size, static_cast<char>('A' + i)));
  }
  ASSERT_EQ(Get("m1key0"), std::string(value_size, 'X'));

  // Overwrite all M0's keys. After compaction, B0's data is fully garbage.
  for (int i = 0; i < 4; i++) {
    ASSERT_OK(Put("m0key" + std::to_string(i), std::string(value_size, 'Z')));
  }
  ASSERT_OK(Flush());
  ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());

  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));

  // B0 should be collected (garbage = total because all 4 keys overwritten).
  // If rotation didn't work, B0 would have 5 entries and only 4 overwritten,
  // leaving 1 entry's worth of bytes preventing collection.

  // Now overwrite M1's key and compact again.
  ASSERT_OK(Put("m1key0", std::string(value_size, 'Y')));
  ASSERT_OK(Flush());
  ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));

  // Verify no old blob files remain. Only new blob files from overwrites
  // should survive.
  auto blob_files = GetBlobFileInfoFromVersion();
  AssertSurvivingBlobFilesHaveLiveBlobs(blob_files);

  // Verify all keys still readable.
  for (int i = 0; i < 4; i++) {
    ASSERT_EQ(Get("m0key" + std::to_string(i)), std::string(value_size, 'Z'));
  }
  ASSERT_EQ(Get("m1key0"), std::string(value_size, 'Y'));
}

// Regression test: verify crash recovery works without orphan bytes.
// If a memtable is lost (crash without WAL), only that memtable's blob
// files contain unreachable data. Those files should be cleaned up.
TEST_F(DBBlobDirectWriteTest, CrashRecoveryNoOrphanBytes) {
  Options options = GetBlobDirectWriteOptions();
  options.blob_direct_write_partitions = 1;
  options.blob_direct_write_buffer_size = 0;
  options.blob_file_size = 1024 * 1024;
  options.write_buffer_size = 4 * 1024;
  options.max_write_buffer_number = 8;
  options.disable_auto_compactions = true;
  options.enable_blob_garbage_collection = true;
  options.blob_garbage_collection_age_cutoff = 1.0;
  options.blob_garbage_collection_force_threshold = 0.0;

  // Use FaultInjectionEnv to simulate crash (drop unflushed data).
  auto* fault_env = new FaultInjectionTestEnv(env_);
  options.env = fault_env;
  DestroyAndReopen(options);

  const int value_size = 200;

  // Write 4 keys to M0 -> all go to blob file B0.
  WriteOptions wo;
  wo.disableWAL = true;
  for (int i = 0; i < 4; i++) {
    ASSERT_OK(db_->Put(wo, "crkey" + std::to_string(i),
                       std::string(value_size, static_cast<char>('A' + i))));
  }

  // Flush M0 -> seals B0, SST S0 committed.
  ASSERT_OK(Flush());
  ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());

  // Write 1 key to M1 (with WAL disabled) -> goes to B1.
  ASSERT_OK(db_->Put(wo, "crkey_m1", std::string(value_size, 'X')));

  // Simulate crash: drop unflushed data, then close.
  fault_env->SetFilesystemActive(false);
  Close();
  fault_env->SetFilesystemActive(true);

  // Reopen DB. M1 is lost (no WAL). B1 is orphan (not in MANIFEST).
  options.env = fault_env;
  Reopen(options);

  // B0 in MANIFEST: total matches committed SST's references.
  // M1's key is lost.
  for (int i = 0; i < 4; i++) {
    ASSERT_EQ(Get("crkey" + std::to_string(i)),
              std::string(value_size, static_cast<char>('A' + i)));
  }

  // Overwrite all M0's keys so B0's data becomes fully garbage.
  for (int i = 0; i < 4; i++) {
    ASSERT_OK(Put("crkey" + std::to_string(i), std::string(value_size, 'Z')));
  }
  ASSERT_OK(Flush());
  ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());

  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));

  // B0: garbage = total -> collected. B1 was orphan, cleaned up.
  auto blob_files = GetBlobFileInfoFromVersion();
  AssertSurvivingBlobFilesHaveLiveBlobs(blob_files);

  // Verify keys.
  for (int i = 0; i < 4; i++) {
    ASSERT_EQ(Get("crkey" + std::to_string(i)), std::string(value_size, 'Z'));
  }

  Close();
  delete fault_env;
}

// Regression test: verify epoch-tagged deferred batches handle out-of-order
// flushes correctly. Rapid SwitchMemtable creates M0, M1, M2 before any
// flush. Then M1 is flushed before M0 (out of order). Each flush should
// seal its own epoch's blob files, not the wrong batch.
TEST_F(DBBlobDirectWriteTest, EpochMatchFlushOutOfOrder) {
  Options options = GetBlobDirectWriteOptions();
  options.blob_direct_write_partitions = 1;
  options.blob_direct_write_buffer_size = 0;
  options.blob_file_size = 1024 * 1024;
  // Small memtable to trigger frequent SwitchMemtable.
  options.write_buffer_size = 2 * 1024;
  options.max_write_buffer_number = 10;
  options.max_background_flushes = 4;
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);

  const int value_size = 200;
  const int keys_per_batch = 30;

  // Write enough keys to cause multiple SwitchMemtable events.
  // With 2KB write buffer and 200-byte values, ~10 keys per memtable.
  for (int i = 0; i < keys_per_batch; i++) {
    ASSERT_OK(Put("oookey" + std::to_string(i),
                  std::string(value_size, 'A' + (i % 26))));
  }

  // Flush all pending memtables.
  ASSERT_OK(Flush());
  ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());

  // Verify all keys readable and blob files properly registered.
  for (int i = 0; i < keys_per_batch; i++) {
    ASSERT_EQ(Get("oookey" + std::to_string(i)),
              std::string(value_size, 'A' + (i % 26)));
  }

  auto blob_files = GetBlobFileInfoFromVersion();
  ASSERT_GT(blob_files.size(), 0u);
  AssertBlobFilesHaveBlobs(blob_files);
  ASSERT_GT(CountLinkedBlobFiles(blob_files), 0u)
      << "Expected at least one blob file to be linked from an SST";

  // Reopen to verify persistence.
  Reopen(options);
  for (int i = 0; i < keys_per_batch; i++) {
    ASSERT_EQ(Get("oookey" + std::to_string(i)),
              std::string(value_size, 'A' + (i % 26)));
  }
}

// Test that atomic flush with multiple CFs correctly handles epoch-tagged
// deferred batches. Each CF's SealAllPartitions should find its own
// epoch-matched batch without cross-CF confusion.
TEST_F(DBBlobDirectWriteTest, AtomicFlushEpochMatch) {
  Options options = GetBlobDirectWriteOptions();
  options.blob_direct_write_partitions = 2;
  options.blob_direct_write_buffer_size = 0;
  options.blob_file_size = 1024 * 1024;
  options.write_buffer_size = 4 * 1024;
  options.max_write_buffer_number = 8;
  options.atomic_flush = true;
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);

  // Create 2 additional CFs (3 total including default).
  CreateColumnFamilies({"cf1", "cf2"}, options);
  ReopenWithColumnFamilies({"default", "cf1", "cf2"}, options);

  const int value_size = 200;

  // Write data to all CFs. The small write_buffer_size will trigger
  // SwitchMemtable and rotation during writes.
  for (int i = 0; i < 20; i++) {
    for (int cf = 0; cf < 3; cf++) {
      ASSERT_OK(Put(cf, "afkey" + std::to_string(i),
                    std::string(value_size, static_cast<char>('A' + cf))));
    }
  }

  // Flush (atomic flush touches all CFs).
  std::vector<ColumnFamilyHandle*> cf_handles;
  for (int cf = 0; cf < 3; cf++) {
    cf_handles.push_back(handles_[cf]);
  }
  ASSERT_OK(dbfull()->Flush(FlushOptions(), cf_handles));
  ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());

  // Verify all keys readable from all CFs.
  for (int i = 0; i < 20; i++) {
    for (int cf = 0; cf < 3; cf++) {
      ASSERT_EQ(Get(cf, "afkey" + std::to_string(i)),
                std::string(value_size, static_cast<char>('A' + cf)));
    }
  }

  // Reopen and verify persistence.
  ReopenWithColumnFamilies({"default", "cf1", "cf2"}, options);
  for (int i = 0; i < 20; i++) {
    for (int cf = 0; cf < 3; cf++) {
      ASSERT_EQ(Get(cf, "afkey" + std::to_string(i)),
                std::string(value_size, static_cast<char>('A' + cf)));
    }
  }
}

// Regression test: when the initial memtable (blob_write_epoch=0) is flushed
// together with a later memtable (blob_write_epoch=N), the epoch-0 memtable's
// deferred seal batch (epoch=1) was skipped because epoch 0 was filtered out
// by `if (ep != 0)` in the flush path. This left epoch 1's blob file
// additions unregistered in the MANIFEST, causing "Invalid blob file number"
// corruption during compaction/read.
TEST_F(DBBlobDirectWriteTest, MultiMemtableFlushEpochZeroBlobFiles) {
  Options options = GetBlobDirectWriteOptions();
  options.blob_direct_write_partitions = 2;
  options.max_write_buffer_number = 4;
  options.write_buffer_size = 1024 * 1024;
  options.min_blob_size = 10;
  DestroyAndReopen(options);

  // Phase 1: Write blob values into the initial memtable (epoch 0).
  // The partition manager's rotation_epoch_ starts at 1, so writers use
  // epoch 1 internally, but the memtable has blob_write_epoch_=0 because
  // SetBlobWriteEpoch is only called during SwitchMemtable.
  const int keys_phase1 = 20;
  for (int i = 0; i < keys_phase1; i++) {
    std::string key = "epoch0_key" + std::to_string(i);
    std::string value(100, static_cast<char>('A' + (i % 26)));
    ASSERT_OK(Put(key, value));
  }

  // Phase 2: SwitchMemtable triggers RotateAllPartitions, which captures
  // epoch 1's blob files into DeferredSeals(epoch=1) and bumps epoch to 2.
  // The new memtable is tagged with epoch 2.
  ASSERT_OK(dbfull()->TEST_SwitchMemtable());

  // Phase 3: Write blob values into the new memtable (epoch 2).
  const int keys_phase2 = 20;
  for (int i = 0; i < keys_phase2; i++) {
    std::string key = "epoch2_key" + std::to_string(i);
    std::string value(100, static_cast<char>('a' + (i % 26)));
    ASSERT_OK(Put(key, value));
  }

  // Phase 4: Flush ALL memtables together. This triggers the bug: the flush
  // sees memtable epochs [0, 2], filters out 0, passes only [2] to
  // SealAllPartitions. Epoch 1's deferred seals are left behind.
  ASSERT_OK(dbfull()->TEST_FlushMemTable(true));

  // Phase 5: Verify all values are readable. If epoch 1's blob files were
  // not committed, reads for epoch0 keys would fail with "Invalid blob file
  // number" or return incorrect data.
  for (int i = 0; i < keys_phase1; i++) {
    std::string key = "epoch0_key" + std::to_string(i);
    std::string expected(100, static_cast<char>('A' + (i % 26)));
    ASSERT_EQ(Get(key), expected) << "Failed to read key from epoch-0 memtable";
  }
  for (int i = 0; i < keys_phase2; i++) {
    std::string key = "epoch2_key" + std::to_string(i);
    std::string expected(100, static_cast<char>('a' + (i % 26)));
    ASSERT_EQ(Get(key), expected) << "Failed to read key from epoch-2 memtable";
  }

  // Phase 6: Verify blob file metadata is present in the version for ALL
  // blob files. If epoch 1's files were missed, the version would have SSTs
  // referencing blob files without metadata.
  auto blob_infos = GetBlobFileInfoFromVersion();
  ASSERT_GT(blob_infos.size(), 0u);
  size_t linked_count = CountLinkedBlobFiles(blob_infos);
  ASSERT_GT(linked_count, 0u)
      << "Expected blob files linked to SSTs after flush";

  // Phase 7: Trigger compaction that reads all L0 files. If any SST
  // references a blob file missing from the version, the compaction fails
  // with "Corruption: Invalid blob file number".
  ASSERT_OK(dbfull()->TEST_CompactRange(0, nullptr, nullptr));

  // Phase 8: Verify values survive compaction.
  for (int i = 0; i < keys_phase1; i++) {
    std::string key = "epoch0_key" + std::to_string(i);
    std::string expected(100, static_cast<char>('A' + (i % 26)));
    ASSERT_EQ(Get(key), expected)
        << "Failed to read epoch-0 key after compaction";
  }
  for (int i = 0; i < keys_phase2; i++) {
    std::string key = "epoch2_key" + std::to_string(i);
    std::string expected(100, static_cast<char>('a' + (i % 26)));
    ASSERT_EQ(Get(key), expected)
        << "Failed to read epoch-2 key after compaction";
  }

  // Phase 9: Reopen and verify persistence.
  Reopen(options);
  for (int i = 0; i < keys_phase1; i++) {
    std::string key = "epoch0_key" + std::to_string(i);
    std::string expected(100, static_cast<char>('A' + (i % 26)));
    ASSERT_EQ(Get(key), expected) << "Failed to read epoch-0 key after reopen";
  }
  for (int i = 0; i < keys_phase2; i++) {
    std::string key = "epoch2_key" + std::to_string(i);
    std::string expected(100, static_cast<char>('a' + (i % 26)));
    ASSERT_EQ(Get(key), expected) << "Failed to read epoch-2 key after reopen";
  }
}

// Same bug pattern but with 3 epochs: verifies that multiple accumulated
// epoch-0 rotation batches are all consumed when flushed together.
TEST_F(DBBlobDirectWriteTest, TripleMemtableFlushEpochZeroBlobFiles) {
  Options options = GetBlobDirectWriteOptions();
  options.blob_direct_write_partitions = 2;
  options.max_write_buffer_number = 6;
  options.write_buffer_size = 1024 * 1024;
  options.min_blob_size = 10;
  DestroyAndReopen(options);

  auto write_keys = [&](const std::string& prefix, int count, char base_char) {
    for (int i = 0; i < count; i++) {
      std::string key = prefix + std::to_string(i);
      std::string value(100, static_cast<char>(base_char + (i % 26)));
      ASSERT_OK(Put(key, value));
    }
  };

  auto verify_keys = [&](const std::string& prefix, int count, char base_char) {
    for (int i = 0; i < count; i++) {
      std::string key = prefix + std::to_string(i);
      std::string expected(100, static_cast<char>(base_char + (i % 26)));
      ASSERT_EQ(Get(key), expected) << "Failed for key=" << key;
    }
  };

  const int nkeys = 15;

  // Memtable 1: epoch 0 (initial, untagged)
  write_keys("m0_", nkeys, 'A');
  ASSERT_OK(dbfull()->TEST_SwitchMemtable());

  // Memtable 2: epoch 2
  write_keys("m1_", nkeys, 'a');
  ASSERT_OK(dbfull()->TEST_SwitchMemtable());

  // Memtable 3: epoch 3
  write_keys("m2_", nkeys, '0');

  // Flush all 3 memtables together.
  ASSERT_OK(dbfull()->TEST_FlushMemTable(true));

  // Verify all data is readable.
  verify_keys("m0_", nkeys, 'A');
  verify_keys("m1_", nkeys, 'a');
  verify_keys("m2_", nkeys, '0');

  // Compaction should succeed without corruption.
  ASSERT_OK(dbfull()->TEST_CompactRange(0, nullptr, nullptr));

  verify_keys("m0_", nkeys, 'A');
  verify_keys("m1_", nkeys, 'a');
  verify_keys("m2_", nkeys, '0');
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

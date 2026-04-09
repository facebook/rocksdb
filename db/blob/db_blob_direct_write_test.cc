//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <algorithm>
#include <array>
#include <limits>
#include <mutex>
#include <set>
#include <sstream>
#include <string>
#include <vector>

#include "cache/compressed_secondary_cache.h"
#include "db/blob/blob_file_partition_manager.h"
#include "db/blob/blob_index.h"
#include "db/blob/blob_log_format.h"
#include "db/blob/blob_log_sequential_reader.h"
#include "db/column_family.h"
#include "db/db_test_util.h"
#include "db/db_with_timestamp_test_util.h"
#include "db/wide/wide_column_test_util.h"
#include "file/filename.h"
#include "file/random_access_file_reader.h"
#include "port/stack_trace.h"
#include "rocksdb/convenience.h"
#include "rocksdb/trace_reader_writer.h"
#include "rocksdb/trace_record.h"
#include "rocksdb/utilities/replayer.h"
#include "test_util/sync_point.h"
#include "util/compression.h"
#include "utilities/fault_injection_env.h"

namespace ROCKSDB_NAMESPACE {

class FixedBlobDirectWritePartitionStrategy : public BlobFilePartitionStrategy {
 public:
  using BlobFilePartitionStrategy::SelectPartition;

  explicit FixedBlobDirectWritePartitionStrategy(uint32_t partition)
      : partition_(partition) {}

  const char* Name() const override {
    return "FixedBlobDirectWritePartitionStrategy";
  }

  uint32_t SelectPartition(uint32_t /*num_partitions*/,
                           uint32_t /*column_family_id*/, const Slice& /*key*/,
                           const Slice& /*value*/) override {
    return partition_;
  }

 private:
  const uint32_t partition_;
};

class RecordingBlobDirectWritePartitionStrategy
    : public BlobFilePartitionStrategy {
 public:
  using BlobFilePartitionStrategy::SelectPartition;

  struct Call {
    uint32_t num_partitions = 0;
    uint32_t column_family_id = 0;
    std::string key;
    std::string value;
    uint32_t selected_partition = 0;
    uint64_t call_count = 0;
  };

  explicit RecordingBlobDirectWritePartitionStrategy(uint32_t partition)
      : partition_(partition) {}

  const char* Name() const override {
    return "RecordingBlobDirectWritePartitionStrategy";
  }

  uint32_t SelectPartition(uint32_t num_partitions, uint32_t column_family_id,
                           const Slice& key, const Slice& value) override {
    std::lock_guard<std::mutex> lock(mutex_);
    calls_.push_back({num_partitions, column_family_id, key.ToString(),
                      value.ToString(), partition_,
                      static_cast<uint64_t>(calls_.size()) + 1});
    return partition_;
  }

  std::vector<Call> GetCalls() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return calls_;
  }

 private:
  const uint32_t partition_;
  mutable std::mutex mutex_;
  std::vector<Call> calls_;
};

class DBBlobDirectWriteTest : public DBTestBase {
 protected:
  DBBlobDirectWriteTest()
      : DBTestBase("db_blob_direct_write_test", /* env_do_fsync */ false) {}

  Options GetBlobDirectWriteCompatibleOptions() {
    return wide_column_test_util::GetBlobDirectWriteCompatibleOptions(
        GetDefaultOptions());
  }

  Options GetDirectWriteOptions() {
    return wide_column_test_util::GetDirectWriteOptions(GetDefaultOptions());
  }

  size_t CountBlobFiles() {
    std::vector<std::string> files;
    EXPECT_OK(env_->GetChildren(dbname_, &files));

    size_t blob_files = 0;
    for (const auto& file : files) {
      if (file.size() > 5 && file.substr(file.size() - 5) == ".blob") {
        ++blob_files;
      }
    }
    return blob_files;
  }

  Status ReadBlobFileHeader(uint64_t blob_file_number, BlobLogHeader* header) {
    assert(header != nullptr);

    std::unique_ptr<FSRandomAccessFile> file;
    FileOptions file_options;
    constexpr IODebugContext* dbg = nullptr;
    const std::string blob_file_path = BlobFileName(dbname_, blob_file_number);
    FileSystem* fs = env_->GetFileSystem().get();
    SystemClock* clock = env_->GetSystemClock().get();

    Status s =
        fs->NewRandomAccessFile(blob_file_path, file_options, &file, dbg);
    if (!s.ok()) {
      return s;
    }

    std::unique_ptr<RandomAccessFileReader> file_reader(
        new RandomAccessFileReader(std::move(file), blob_file_path, clock));
    BlobLogSequentialReader reader(std::move(file_reader), clock,
                                   /*statistics=*/nullptr);
    return reader.ReadHeader(header);
  }

  CompressionType GetSupportedCompressedBlobCompression() {
    static constexpr std::array<CompressionType, 6> kCandidates{
        kSnappyCompression, kLZ4Compression,   kZSTD,
        kZlibCompression,   kBZip2Compression, kLZ4HCCompression};
    for (CompressionType compression : kCandidates) {
      if (CompressionTypeSupported(compression)) {
        return compression;
      }
    }
    return kNoCompression;
  }

  void AssertOrderedTraceStoresLogicalPut(const Options& options,
                                          bool expect_blob_files) {
    const std::string trace_file = dbname_ + "/rocksdb.trace";
    Reopen(options);

    TraceOptions trace_options;
    trace_options.preserve_write_order = true;

    std::unique_ptr<TraceWriter> trace_writer;
    ASSERT_OK(
        NewFileTraceWriter(env_, EnvOptions(), trace_file, &trace_writer));
    ASSERT_OK(db_->StartTrace(trace_options, std::move(trace_writer)));

    const std::string key = "bdw-key";
    const std::string value(64, 'x');
    ASSERT_OK(Put(key, value));
    ASSERT_OK(db_->EndTrace());

    ASSERT_EQ(Get(key), value);
    if (expect_blob_files) {
      ASSERT_GT(CountBlobFiles(), 0U);
    } else {
      ASSERT_EQ(CountBlobFiles(), 0U);
    }

    std::unique_ptr<TraceReader> trace_reader;
    ASSERT_OK(
        NewFileTraceReader(env_, EnvOptions(), trace_file, &trace_reader));

    std::vector<ColumnFamilyHandle*> handles{db_->DefaultColumnFamily()};
    std::unique_ptr<Replayer> replayer;
    ASSERT_OK(
        db_->NewDefaultReplayer(handles, std::move(trace_reader), &replayer));
    ASSERT_OK(replayer->Prepare());

    std::unique_ptr<TraceRecord> record;
    ASSERT_OK(replayer->Next(&record));
    ASSERT_NE(record, nullptr);
    ASSERT_EQ(record->GetTraceType(), kTraceWrite);

    auto* write_record = dynamic_cast<WriteQueryTraceRecord*>(record.get());
    ASSERT_NE(write_record, nullptr);

    class SingleWriteInspector : public WriteBatch::Handler {
     public:
      Status PutCF(uint32_t, const Slice& key, const Slice& value) override {
        saw_put = true;
        key_ = key.ToString();
        value_ = value.ToString();
        return Status::OK();
      }

      Status PutBlobIndexCF(uint32_t, const Slice&, const Slice&) override {
        saw_put_blob_index = true;
        return Status::OK();
      }

      bool saw_put = false;
      bool saw_put_blob_index = false;
      std::string key_;
      std::string value_;
    };

    WriteBatch traced_batch(write_record->GetWriteBatchRep().ToString());
    SingleWriteInspector inspector;
    ASSERT_OK(traced_batch.Iterate(&inspector));
    ASSERT_TRUE(inspector.saw_put);
    ASSERT_FALSE(inspector.saw_put_blob_index);
    ASSERT_EQ(inspector.key_, key);
    ASSERT_EQ(inspector.value_, value);
  }
};

TEST_F(
    DBBlobDirectWriteTest,
    DirectWriteDefaultRoundRobinSpreadsWriteBatchAcrossPartitionsBeforeAndAfterFlush) {
  Options options = GetDefaultOptions();
  options.enable_blob_files = true;
  options.enable_blob_direct_write = true;
  options.allow_concurrent_memtable_write = false;
  options.blob_direct_write_partitions = 4;
  options.min_blob_size = 32;
  options.use_direct_reads = true;
  options.use_direct_io_for_flush_and_compaction = true;
  ASSERT_EQ(options.blob_direct_write_partition_strategy, nullptr);

  Status s = TryReopen(options);
  if (s.IsInvalidArgument()) {
    ROCKSDB_GTEST_SKIP("This test requires direct IO support");
    return;
  }
  ASSERT_OK(s);

  const std::array<std::string, 5> keys{
      "blob_key_0", "blob_key_1", "blob_key_2", "blob_key_3", "inline_key"};
  const std::array<std::string, 5> values{
      std::string(256, 'a'), std::string(256, 'b'), std::string(256, 'c'),
      std::string(256, 'd'), "tiny"};

  WriteBatch batch;
  for (size_t i = 0; i < keys.size(); ++i) {
    ASSERT_OK(batch.Put(keys[i], values[i]));
  }
  ASSERT_OK(batch.Delete("deleted_key"));
  ASSERT_OK(db_->Write(WriteOptions(), &batch));

  for (size_t i = 0; i < keys.size(); ++i) {
    ASSERT_EQ(Get(keys[i]), values[i]);
  }

  const std::array<Slice, 5> key_slices{Slice(keys[0]), Slice(keys[1]),
                                        Slice(keys[2]), Slice(keys[3]),
                                        Slice(keys[4])};
  auto assert_multiget = [&]() {
    std::array<PinnableSlice, 5> results;
    std::array<Status, 5> statuses;
    db_->MultiGet(ReadOptions(), db_->DefaultColumnFamily(), key_slices.size(),
                  key_slices.data(), results.data(), statuses.data());
    for (size_t i = 0; i < key_slices.size(); ++i) {
      ASSERT_OK(statuses[i]);
      ASSERT_EQ(results[i], values[i]);
    }
  };
  assert_multiget();

  for (size_t i = 0; i < 4; ++i) {
    std::string value;
    bool value_found = true;
    ASSERT_TRUE(db_->KeyMayExist(ReadOptions(), db_->DefaultColumnFamily(),
                                 keys[i], &value, &value_found));
    ASSERT_FALSE(value_found);
  }

  std::string inline_value;
  bool inline_value_found = false;
  ASSERT_TRUE(db_->KeyMayExist(ReadOptions(), db_->DefaultColumnFamily(),
                               keys[4], &inline_value, &inline_value_found));
  ASSERT_TRUE(inline_value_found);
  ASSERT_EQ(inline_value, values[4]);

  ASSERT_OK(Flush());
  assert_multiget();

  std::vector<std::string> files;
  ASSERT_OK(env_->GetChildren(dbname_, &files));
  size_t blob_files = 0;
  for (const auto& f : files) {
    if (f.size() > 5 && f.substr(f.size() - 5) == ".blob") {
      ++blob_files;
    }
  }
  ASSERT_EQ(blob_files, 4);

  Close();
  Reopen(options);

  for (size_t i = 0; i < keys.size(); ++i) {
    ASSERT_EQ(Get(keys[i]), values[i]);
  }
}

TEST_F(DBBlobDirectWriteTest,
       DirectWriteCustomPartitionStrategyRoutesWritesToOneBlobFile) {
  Options options = GetDirectWriteOptions();
  options.blob_direct_write_partitions = 4;
  options.blob_file_size = 1 << 20;
  options.blob_direct_write_partition_strategy =
      std::make_shared<FixedBlobDirectWritePartitionStrategy>(3);

  Reopen(options);

  const std::array<std::string, 4> keys{
      "strategy_blob_key_0", "strategy_blob_key_1", "strategy_blob_key_2",
      "strategy_blob_key_3"};
  const std::array<std::string, 4> values{
      std::string(128, 'a'), std::string(128, 'b'), std::string(128, 'c'),
      std::string(128, 'd')};

  for (size_t i = 0; i < keys.size(); ++i) {
    ASSERT_OK(Put(keys[i], values[i]));
  }

  ASSERT_EQ(CountBlobFiles(), 1U);
  ASSERT_OK(Flush());
  ASSERT_EQ(CountBlobFiles(), 1U);

  ColumnFamilyMetaData cf_meta;
  db_->GetColumnFamilyMetaData(&cf_meta);
  ASSERT_EQ(cf_meta.blob_file_count, 1U);
  ASSERT_EQ(cf_meta.blob_files.size(), 1U);

  for (size_t i = 0; i < keys.size(); ++i) {
    ASSERT_EQ(Get(keys[i]), values[i]);
  }
}

TEST_F(DBBlobDirectWriteTest,
       DirectWriteCustomPartitionStrategyReceivesOriginalWriteParameters) {
  const CompressionType compression = GetSupportedCompressedBlobCompression();
  if (compression == kNoCompression) {
    ROCKSDB_GTEST_SKIP("This test requires a supported blob compression");
    return;
  }

  auto strategy =
      std::make_shared<RecordingBlobDirectWritePartitionStrategy>(7);
  Options options = GetDirectWriteOptions();
  options.blob_direct_write_partitions = 4;
  options.blob_file_size = 1 << 20;
  options.blob_compression_type = compression;
  options.blob_direct_write_partition_strategy = strategy;

  Reopen(options);

  const std::string key = "strategy_params_key";
  const std::string value(256, 'z');

  ASSERT_OK(Put(key, value));
  ASSERT_OK(Flush());
  ASSERT_EQ(Get(key), value);

  const std::vector<RecordingBlobDirectWritePartitionStrategy::Call> calls =
      strategy->GetCalls();
  ASSERT_EQ(calls.size(), 1U);
  const auto& call = calls.front();
  ASSERT_EQ(call.num_partitions, 4U);
  ASSERT_EQ(call.column_family_id, db_->DefaultColumnFamily()->GetID());
  ASSERT_EQ(call.key, key);
  ASSERT_EQ(call.value, value);
  ASSERT_EQ(call.selected_partition, 7U);
  ASSERT_EQ(call.call_count, 1U);

  const std::vector<uint64_t> blob_file_numbers = GetBlobFileNumbers();
  ASSERT_EQ(blob_file_numbers.size(), 1U);

  BlobLogHeader header;
  ASSERT_OK(ReadBlobFileHeader(blob_file_numbers[0], &header));
  ASSERT_EQ(header.column_family_id, db_->DefaultColumnFamily()->GetID());
  ASSERT_EQ(header.compression, compression);
}

TEST_F(DBBlobDirectWriteTest,
       DirectWriteCustomPartitionStrategyAppliesModuloToOutOfRangeReturns) {
  struct TestCase {
    uint32_t raw_partition;
    const char* suffix;
  };

  const std::array<TestCase, 3> cases{{
      {4, "equal"},
      {9, "greater"},
      {std::numeric_limits<uint32_t>::max(), "max"},
  }};

  for (const auto& test_case : cases) {
    SCOPED_TRACE(test_case.suffix);

    Options options = GetDirectWriteOptions();
    options.create_if_missing = true;
    options.blob_direct_write_partitions = 4;
    options.blob_file_size = 1 << 20;
    options.blob_direct_write_partition_strategy =
        std::make_shared<FixedBlobDirectWritePartitionStrategy>(
            test_case.raw_partition);

    DestroyAndReopen(options);

    const std::string key =
        std::string("partition_mod_key_") + test_case.suffix;
    const std::string value(128, test_case.suffix[0]);

    ASSERT_OK(Put(key, value));
    ASSERT_EQ(CountBlobFiles(), 1U);
    ASSERT_OK(Flush());
    ASSERT_EQ(CountBlobFiles(), 1U);
    ASSERT_EQ(Get(key), value);
  }
}

TEST_F(DBBlobDirectWriteTest,
       DirectWriteCustomPartitionStrategyCalledForEachBlobInWriteBatch) {
  auto strategy =
      std::make_shared<RecordingBlobDirectWritePartitionStrategy>(2);
  Options options = GetDirectWriteOptions();
  options.blob_direct_write_partitions = 4;
  options.blob_file_size = 1 << 20;
  options.blob_direct_write_partition_strategy = strategy;

  Reopen(options);

  const std::array<std::string, 3> blob_keys{
      "batch_blob_key_0", "batch_blob_key_1", "batch_blob_key_2"};
  const std::array<std::string, 3> blob_values{
      std::string(128, 'a'), std::string(128, 'b'), std::string(128, 'c')};
  const std::string inline_key = "batch_inline_key";
  const std::string inline_value = "tiny";

  WriteBatch batch;
  for (size_t i = 0; i < blob_keys.size(); ++i) {
    ASSERT_OK(
        batch.Put(db_->DefaultColumnFamily(), blob_keys[i], blob_values[i]));
  }
  ASSERT_OK(batch.Put(db_->DefaultColumnFamily(), inline_key, inline_value));
  ASSERT_OK(batch.Delete(db_->DefaultColumnFamily(), "batch_deleted_key"));
  ASSERT_OK(db_->Write(WriteOptions(), &batch));

  const auto calls = strategy->GetCalls();
  ASSERT_EQ(calls.size(), blob_keys.size());
  for (size_t i = 0; i < blob_keys.size(); ++i) {
    ASSERT_EQ(calls[i].num_partitions, 4U);
    ASSERT_EQ(calls[i].column_family_id, db_->DefaultColumnFamily()->GetID());
    ASSERT_EQ(calls[i].key, blob_keys[i]);
    ASSERT_EQ(calls[i].value, blob_values[i]);
    ASSERT_EQ(calls[i].selected_partition, 2U);
    ASSERT_EQ(calls[i].call_count, i + 1);
    ASSERT_EQ(Get(blob_keys[i]), blob_values[i]);
  }

  ASSERT_EQ(Get(inline_key), inline_value);
  ASSERT_EQ(CountBlobFiles(), 1U);
}

TEST_F(DBBlobDirectWriteTest,
       DirectWriteSharedCustomPartitionStrategyAcrossColumnFamilies) {
  Reopen(GetBlobDirectWriteCompatibleOptions());

  auto strategy =
      std::make_shared<RecordingBlobDirectWritePartitionStrategy>(1);
  Options options = GetDirectWriteOptions();
  options.blob_direct_write_partitions = 4;
  options.blob_file_size = 1 << 20;
  options.blob_direct_write_partition_strategy = strategy;

  ColumnFamilyHandle* first_cfh = nullptr;
  ColumnFamilyHandle* second_cfh = nullptr;
  ASSERT_OK(db_->CreateColumnFamily(options, "bdw_one", &first_cfh));
  ASSERT_OK(db_->CreateColumnFamily(options, "bdw_two", &second_cfh));

  const std::string first_key = "shared_cf_key_0";
  const std::string second_key = "shared_cf_key_1";
  const std::string first_value(128, 'x');
  const std::string second_value(128, 'y');

  ASSERT_OK(db_->Put(WriteOptions(), first_cfh, first_key, first_value));
  ASSERT_OK(db_->Put(WriteOptions(), second_cfh, second_key, second_value));

  std::string value;
  ASSERT_OK(db_->Get(ReadOptions(), first_cfh, first_key, &value));
  ASSERT_EQ(value, first_value);
  ASSERT_OK(db_->Get(ReadOptions(), second_cfh, second_key, &value));
  ASSERT_EQ(value, second_value);

  const auto calls = strategy->GetCalls();
  ASSERT_EQ(calls.size(), 2U);
  std::set<uint32_t> seen_cf_ids;
  std::set<std::string> seen_keys;
  for (const auto& call : calls) {
    ASSERT_EQ(call.num_partitions, 4U);
    ASSERT_EQ(call.selected_partition, 1U);
    seen_cf_ids.insert(call.column_family_id);
    seen_keys.insert(call.key);
  }
  ASSERT_EQ(seen_cf_ids.size(), 2U);
  ASSERT_TRUE(seen_cf_ids.count(first_cfh->GetID()) > 0);
  ASSERT_TRUE(seen_cf_ids.count(second_cfh->GetID()) > 0);
  ASSERT_TRUE(seen_keys.count(first_key) > 0);
  ASSERT_TRUE(seen_keys.count(second_key) > 0);

  ASSERT_OK(db_->DestroyColumnFamilyHandle(first_cfh));
  ASSERT_OK(db_->DestroyColumnFamilyHandle(second_cfh));
}
TEST_F(DBBlobDirectWriteTest,
       DirectWriteCloseFlushesWhenShutdownFlushIsDisabled) {
  Options options = GetDefaultOptions();
  options.enable_blob_files = true;
  options.enable_blob_direct_write = true;
  options.allow_concurrent_memtable_write = false;
  options.blob_direct_write_partitions = 4;
  options.min_blob_size = 32;
  options.avoid_flush_during_shutdown = true;

  Reopen(options);

  const std::array<std::string, 4> keys{"close_blob_key_0", "close_blob_key_1",
                                        "close_blob_key_2", "close_blob_key_3"};
  const std::array<std::string, 4> values{
      std::string(128, 'a'), std::string(128, 'b'), std::string(128, 'c'),
      std::string(128, 'd')};

  for (size_t i = 0; i < keys.size(); ++i) {
    ASSERT_OK(Put(keys[i], values[i]));
  }

  Close();
  Reopen(options);

  for (size_t i = 0; i < keys.size(); ++i) {
    ASSERT_EQ(Get(keys[i]), values[i]);
  }
}

TEST_F(DBBlobDirectWriteTest,
       DirectWriteAutoFlushPreservesBlobGenerationOrder) {
  Options options = GetDirectWriteOptions();
  options.allow_concurrent_memtable_write = false;
  options.disable_auto_compactions = true;
  options.arena_block_size = 4096;
  options.write_buffer_size = 500000;
  options.max_write_buffer_number = 4;
  options.write_buffer_manager =
      std::make_shared<WriteBufferManager>(100, nullptr, false);

  Reopen(options);

  WriteOptions write_options;
  write_options.disableWAL = true;

  const std::string first_value(128, 'a');
  const std::string second_value(128, 'b');

  ASSERT_OK(db_->Put(write_options, "first_key", first_value));
  ASSERT_OK(db_->Put(write_options, "second_key", second_value));

  // The tiny shared write buffer forces the first write to queue a flush. The
  // second write then goes through PreprocessWrite(), which can switch
  // memtables before the transformed batch is inserted. By the time the test
  // inspects DB state, that auto flush may already have finished, so wait for
  // any in-flight auto flush instead of asserting on an instantaneous imm
  // count.
  ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());
  ASSERT_OK(Flush());

  ASSERT_EQ(Get("first_key"), first_value);
  ASSERT_EQ(Get("second_key"), second_value);

  ColumnFamilyMetaData cf_meta;
  db_->GetColumnFamilyMetaData(&cf_meta);
  ASSERT_EQ(cf_meta.blob_files.size(), 2U);

  std::vector<uint64_t> blob_file_numbers;
  blob_file_numbers.reserve(cf_meta.blob_files.size());
  for (const auto& blob_meta : cf_meta.blob_files) {
    blob_file_numbers.push_back(blob_meta.blob_file_number);
  }
  std::sort(blob_file_numbers.begin(), blob_file_numbers.end());

  std::vector<LiveFileMetaData> live_files;
  db_->GetLiveFilesMetaData(&live_files);
  ASSERT_EQ(live_files.size(), 2U);
  std::sort(live_files.begin(), live_files.end(),
            [](const LiveFileMetaData& lhs, const LiveFileMetaData& rhs) {
              return lhs.smallest_seqno < rhs.smallest_seqno;
            });

  ASSERT_EQ(live_files[0].oldest_blob_file_number, blob_file_numbers[0]);
  ASSERT_EQ(live_files[1].oldest_blob_file_number, blob_file_numbers[1]);
}

TEST_F(DBBlobDirectWriteTest, DirectWriteCreateMissingColumnFamilyOnOpen) {
  Reopen(GetBlobDirectWriteCompatibleOptions());

  Options default_options = GetBlobDirectWriteCompatibleOptions();
  default_options.create_if_missing = false;
  default_options.create_missing_column_families = true;

  Options direct_write_options = GetDirectWriteOptions();
  direct_write_options.create_if_missing = false;
  direct_write_options.create_missing_column_families = true;

  const std::vector<std::string> cfs{kDefaultColumnFamilyName, "bdw"};
  const std::vector<Options> options{default_options, direct_write_options};
  ASSERT_OK(TryReopenWithColumnFamilies(cfs, options));

  ASSERT_EQ(handles_.size(), 2U);
  ASSERT_TRUE(dbfull()->HasAnyBlobDirectWriteColumnFamily());

  const std::string key = "cf_key";
  const std::string value(128, 'd');
  ASSERT_OK(db_->Put(WriteOptions(), handles_[1], key, value));
  ASSERT_EQ(Get(1, key), value);
}

TEST_F(DBBlobDirectWriteTest, DirectWriteRejectsMemPurge) {
  Options options = GetDirectWriteOptions();
  options.experimental_mempurge_threshold = 1.0;
  ASSERT_TRUE(TryReopen(options).IsNotSupported());

  options.experimental_mempurge_threshold = 0.0;
  ASSERT_OK(TryReopen(options));
  ASSERT_TRUE(db_->SetOptions(db_->DefaultColumnFamily(),
                              {{"experimental_mempurge_threshold", "1.0"}})
                  .IsNotSupported());
}

TEST_F(DBBlobDirectWriteTest, DirectWriteRejectsUnsupportedWriteModes) {
  auto assert_rejected = [&](const Options& options,
                             const std::string& expected_substr) {
    Status s = TryReopen(options);
    ASSERT_TRUE(s.IsNotSupported()) << s.ToString();
    ASSERT_NE(s.ToString().find(expected_substr), std::string::npos)
        << s.ToString();
  };

  Options options = GetDirectWriteOptions();
  options.enable_pipelined_write = true;
  assert_rejected(options, "pipelined writes");

  options = GetDirectWriteOptions();
  options.allow_concurrent_memtable_write = true;
  assert_rejected(options, "concurrent memtable writes");

  options = GetDirectWriteOptions();
  options.unordered_write = true;
  assert_rejected(options, "unordered writes");

  options = GetDirectWriteOptions();
  options.two_write_queues = true;
  assert_rejected(options, "two write queues");
}

TEST_F(DBBlobDirectWriteTest,
       DirectWriteIteratorMixedValuesBeforeAndAfterFlush) {
  Options options = GetDirectWriteOptions();
  options.min_blob_size = 20;

  Reopen(options);

  const std::array<std::string, 4> keys{"a_small", "b_large", "c_small",
                                        "d_large"};
  const std::array<std::string, 4> values{"tiny", std::string(80, 'B'), "mini",
                                          std::string(80, 'D')};

  for (size_t i = 0; i < keys.size(); ++i) {
    ASSERT_OK(Put(keys[i], values[i]));
  }

  auto verify_iteration = [&]() {
    ReadOptions read_options;
    std::unique_ptr<Iterator> iter(db_->NewIterator(read_options));

    iter->SeekToFirst();
    for (size_t i = 0; i < keys.size(); ++i) {
      ASSERT_TRUE(iter->Valid());
      ASSERT_EQ(iter->key(), keys[i]);
      ASSERT_EQ(iter->value(), values[i]);
      iter->Next();
    }
    ASSERT_FALSE(iter->Valid());
    ASSERT_OK(iter->status());

    iter->SeekToLast();
    for (size_t i = keys.size(); i-- > 0;) {
      ASSERT_TRUE(iter->Valid());
      ASSERT_EQ(iter->key(), keys[i]);
      ASSERT_EQ(iter->value(), values[i]);
      iter->Prev();
    }
    ASSERT_FALSE(iter->Valid());
    ASSERT_OK(iter->status());

    iter->Seek("b_large");
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key(), keys[1]);
    ASSERT_EQ(iter->value(), values[1]);
  };

  verify_iteration();
  ASSERT_OK(Flush());
  verify_iteration();
}

TEST_F(DBBlobDirectWriteTest, DirectWriteImmutableMemtableRead) {
  Options options = GetDirectWriteOptions();

  Reopen(options);

  constexpr size_t kNumKeys = 6;
  std::array<std::string, kNumKeys> keys;
  std::array<std::string, kNumKeys> values;
  for (size_t i = 0; i < kNumKeys; ++i) {
    keys[i] = "imm_key_" + std::to_string(i);
    values[i] = std::string(96 + i, static_cast<char>('A' + i));
    ASSERT_OK(Put(keys[i], values[i]));
  }

  ASSERT_OK(dbfull()->TEST_SwitchMemtable());

  auto verify_reads = [&]() {
    std::array<Slice, kNumKeys> key_slices{Slice(keys[0]), Slice(keys[1]),
                                           Slice(keys[2]), Slice(keys[3]),
                                           Slice(keys[4]), Slice(keys[5])};
    std::array<PinnableSlice, kNumKeys> results;
    std::array<Status, kNumKeys> statuses;

    for (size_t i = 0; i < kNumKeys; ++i) {
      ASSERT_EQ(Get(keys[i]), values[i]);
    }

    db_->MultiGet(ReadOptions(), db_->DefaultColumnFamily(), key_slices.size(),
                  key_slices.data(), results.data(), statuses.data());
    for (size_t i = 0; i < kNumKeys; ++i) {
      ASSERT_OK(statuses[i]);
      ASSERT_EQ(results[i], values[i]);
    }
  };

  verify_reads();
  ASSERT_OK(dbfull()->TEST_FlushMemTable(true));
  verify_reads();
}

TEST_F(DBBlobDirectWriteTest, DirectWriteRefreshesReaderAfterFlush) {
  Options options = GetDirectWriteOptions();
  options.blob_direct_write_partitions = 1;

  Reopen(options);

  const std::string first_key = "first_key";
  const std::string second_key = "second_key";
  const std::string third_key = "third_key";
  const std::string first_value(128, 'a');
  const std::string second_value(128, 'b');
  const std::string third_value(128, 'c');

  ASSERT_OK(Put(first_key, first_value));
  // This read opens and caches an in-flight direct-write reader before the
  // blob file is sealed by flush.
  ASSERT_EQ(Get(first_key), first_value);

  ASSERT_OK(Put(second_key, second_value));
  ASSERT_OK(Put(third_key, third_value));
  ASSERT_OK(Flush());

  ASSERT_EQ(Get(second_key), second_value);
  ASSERT_EQ(Get(third_key), third_value);

  const std::array<std::string, 2> keys{second_key, third_key};
  const std::array<std::string, 2> values{second_value, third_value};
  const std::array<Slice, 2> key_slices{Slice(keys[0]), Slice(keys[1])};
  std::array<PinnableSlice, 2> results;
  std::array<Status, 2> statuses;

  db_->MultiGet(ReadOptions(), db_->DefaultColumnFamily(), key_slices.size(),
                key_slices.data(), results.data(), statuses.data());
  for (size_t i = 0; i < key_slices.size(); ++i) {
    ASSERT_OK(statuses[i]);
    ASSERT_EQ(results[i], values[i]);
  }
}

TEST_F(DBBlobDirectWriteTest, DirectWriteRefreshesReaderWhileFileIsGrowing) {
  Options options = GetDirectWriteOptions();
  options.blob_direct_write_partitions = 1;

  Reopen(options);

  const std::string first_key = "active_first_key";
  const std::string second_key = "active_second_key";
  const std::string third_key = "active_third_key";
  const std::string first_value(128, 'x');
  const std::string second_value(128, 'y');
  const std::string third_value(128, 'z');

  ASSERT_OK(Put(first_key, first_value));
  // This read caches a reader for the current active direct-write file before
  // more blob records extend the file.
  ASSERT_EQ(Get(first_key), first_value);

  ASSERT_OK(Put(second_key, second_value));
  ASSERT_OK(Put(third_key, third_value));

  ASSERT_EQ(Get(second_key), second_value);
  ASSERT_EQ(Get(third_key), third_value);

  const std::array<std::string, 2> keys{second_key, third_key};
  const std::array<std::string, 2> values{second_value, third_value};
  const std::array<Slice, 2> key_slices{Slice(keys[0]), Slice(keys[1])};
  std::array<PinnableSlice, 2> results;
  std::array<Status, 2> statuses;

  db_->MultiGet(ReadOptions(), db_->DefaultColumnFamily(), key_slices.size(),
                key_slices.data(), results.data(), statuses.data());
  for (size_t i = 0; i < key_slices.size(); ++i) {
    ASSERT_OK(statuses[i]);
    ASSERT_EQ(results[i], values[i]);
  }
}

TEST_F(DBBlobDirectWriteTest, DirectWriteBlobFileRotationSinglePartition) {
  Options options = GetDirectWriteOptions();
  options.blob_file_size = 512;

  Reopen(options);

  constexpr size_t kNumKeys = 12;
  for (size_t i = 0; i < kNumKeys; ++i) {
    ASSERT_OK(Put("rot_key_" + std::to_string(i),
                  std::string(120, static_cast<char>('a' + i))));
  }

  for (size_t i = 0; i < kNumKeys; ++i) {
    ASSERT_EQ(Get("rot_key_" + std::to_string(i)),
              std::string(120, static_cast<char>('a' + i)));
  }

  ASSERT_OK(Flush());
  ASSERT_GT(CountBlobFiles(), 1U);

  Close();
  Reopen(options);

  for (size_t i = 0; i < kNumKeys; ++i) {
    ASSERT_EQ(Get("rot_key_" + std::to_string(i)),
              std::string(120, static_cast<char>('a' + i)));
  }
}

TEST_F(DBBlobDirectWriteTest,
       DirectWriteCompressionUpdateReusesCachedCompressorAndRotatesFiles) {
  const CompressionType compressed = GetSupportedCompressedBlobCompression();
  if (compressed == kNoCompression) {
    return;
  }

  std::string compressed_option_value;
  ASSERT_OK(GetStringFromCompressionType(&compressed_option_value, compressed));

  Options options = GetDirectWriteOptions();
  options.blob_file_size = 1 << 20;
  options.blob_compression_type = compressed;

  Reopen(options);

  const std::string first_key = "compressed_before_update";
  const std::string second_key = "uncompressed_between_updates";
  const std::string third_key = "compressed_after_update";
  const std::string first_value(256, 'a');
  const std::string second_value(256, 'b');
  const std::string third_value(256, 'c');

  ASSERT_OK(Put(first_key, first_value));
  ASSERT_EQ(Get(first_key), first_value);

  ASSERT_OK(db_->SetOptions(db_->DefaultColumnFamily(),
                            {{"blob_compression_type", "kNoCompression"}}));
  ASSERT_OK(Put(second_key, second_value));
  ASSERT_EQ(Get(first_key), first_value);
  ASSERT_EQ(Get(second_key), second_value);

  ASSERT_OK(
      db_->SetOptions(db_->DefaultColumnFamily(),
                      {{"blob_compression_type", compressed_option_value}}));
  ASSERT_OK(Put(third_key, third_value));
  ASSERT_EQ(Get(first_key), first_value);
  ASSERT_EQ(Get(second_key), second_value);
  ASSERT_EQ(Get(third_key), third_value);

  ASSERT_OK(Flush());
  ASSERT_EQ(CountBlobFiles(), 3U);

  const std::vector<uint64_t> blob_file_numbers = GetBlobFileNumbers();
  ASSERT_EQ(blob_file_numbers.size(), 3U);

  BlobLogHeader header;
  ASSERT_OK(ReadBlobFileHeader(blob_file_numbers[0], &header));
  ASSERT_EQ(header.compression, compressed);

  ASSERT_OK(ReadBlobFileHeader(blob_file_numbers[1], &header));
  ASSERT_EQ(header.compression, kNoCompression);

  ASSERT_OK(ReadBlobFileHeader(blob_file_numbers[2], &header));
  ASSERT_EQ(header.compression, compressed);

  Close();
  Reopen(options);

  ASSERT_EQ(Get(first_key), first_value);
  ASSERT_EQ(Get(second_key), second_value);
  ASSERT_EQ(Get(third_key), third_value);
}

TEST_F(DBBlobDirectWriteTest, DirectWriteFailedBatchTrackedAsInitialGarbage) {
  Options options = GetDirectWriteOptions();
  options.blob_file_size = 1 << 20;
  options.blob_compression_type = kNoCompression;

  Reopen(options);

  const std::string failed_key = "failed_key";
  const std::string failed_value(128, 'f');
  const uint64_t failed_record_bytes =
      BlobLogRecord::kHeaderSize + failed_key.size() + failed_value.size();

  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::WriteImpl:AfterBlobDirectWrite", [](void* arg) {
        auto* status = static_cast<Status*>(arg);
        assert(status != nullptr);
        *status = Status::IOError("Injected post-BDW failure");
      });
  SyncPoint::GetInstance()->EnableProcessing();

  ASSERT_TRUE(Put(failed_key, failed_value).IsIOError());

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();

  const std::string good_key = "good_key";
  const std::string good_value(96, 'g');
  const uint64_t good_record_bytes =
      BlobLogRecord::kHeaderSize + good_key.size() + good_value.size();

  ASSERT_OK(Put(good_key, good_value));
  ASSERT_EQ(Get(failed_key), "NOT_FOUND");
  ASSERT_EQ(Get(good_key), good_value);

  ASSERT_OK(Flush());

  ColumnFamilyMetaData cf_meta;
  db_->GetColumnFamilyMetaData(&cf_meta);
  ASSERT_EQ(cf_meta.blob_file_count, 1U);
  ASSERT_EQ(cf_meta.blob_files.size(), 1U);

  const BlobMetaData& blob_meta = cf_meta.blob_files[0];
  ASSERT_EQ(blob_meta.total_blob_count, 2U);
  ASSERT_EQ(blob_meta.total_blob_bytes,
            failed_record_bytes + good_record_bytes);
  ASSERT_EQ(blob_meta.garbage_blob_count, 1U);
  ASSERT_EQ(blob_meta.garbage_blob_bytes, failed_record_bytes);

  Close();
  Reopen(options);

  ASSERT_EQ(Get(failed_key), "NOT_FOUND");
  ASSERT_EQ(Get(good_key), good_value);

  db_->GetColumnFamilyMetaData(&cf_meta);
  ASSERT_EQ(cf_meta.blob_file_count, 1U);
  ASSERT_EQ(cf_meta.blob_files.size(), 1U);
  ASSERT_EQ(cf_meta.blob_files[0].total_blob_count, 2U);
  ASSERT_EQ(cf_meta.blob_files[0].garbage_blob_count, 1U);
  ASSERT_EQ(cf_meta.blob_files[0].garbage_blob_bytes, failed_record_bytes);
}

TEST_F(DBBlobDirectWriteTest, DirectWriteRollbackMismatchReturnsCorruption) {
  Reopen(GetDirectWriteOptions());

  ASSERT_TRUE(dbfull()->HasAnyBlobDirectWriteColumnFamily());

  auto* cfh = static_cast_with_check<ColumnFamilyHandleImpl>(
      db_->DefaultColumnFamily());
  auto* mgr = cfh->cfd()->blob_partition_manager();
  ASSERT_NE(mgr, nullptr);

  Status s =
      mgr->MarkBlobWriteAsGarbage(/*file_number=*/123456789,
                                  /*blob_count=*/1,
                                  /*blob_bytes=*/BlobLogRecord::kHeaderSize);
  ASSERT_TRUE(s.IsCorruption());
}

TEST_F(DBBlobDirectWriteTest,
       DirectWriteCountTracksCreatedAndDroppedColumnFamily) {
  Reopen(GetBlobDirectWriteCompatibleOptions());

  ASSERT_FALSE(dbfull()->HasAnyBlobDirectWriteColumnFamily());

  ColumnFamilyHandle* bdw_cfh = nullptr;
  ASSERT_OK(db_->CreateColumnFamily(GetDirectWriteOptions(), "bdw", &bdw_cfh));
  ASSERT_NE(bdw_cfh, nullptr);
  ASSERT_TRUE(dbfull()->HasAnyBlobDirectWriteColumnFamily());

  const std::string key = "cf_key";
  const std::string value(128, 'c');
  ASSERT_OK(db_->Put(WriteOptions(), bdw_cfh, key, value));

  PinnableSlice read_value;
  ASSERT_OK(db_->Get(ReadOptions(), bdw_cfh, key, &read_value));
  ASSERT_EQ(read_value.ToString(), value);

  ASSERT_OK(db_->DropColumnFamily(bdw_cfh));
  ASSERT_FALSE(dbfull()->HasAnyBlobDirectWriteColumnFamily());
  ASSERT_OK(db_->DestroyColumnFamilyHandle(bdw_cfh));
}

TEST_F(DBBlobDirectWriteTest,
       DirectWritePreparedGenerationsStayReusableUntilCommit) {
  Options options = GetDirectWriteOptions();
  options.blob_file_size = 1 << 20;

  Reopen(options);

  ASSERT_TRUE(dbfull()->HasAnyBlobDirectWriteColumnFamily());

  auto* cfh = static_cast_with_check<ColumnFamilyHandleImpl>(
      db_->DefaultColumnFamily());
  auto* mgr = cfh->cfd()->blob_partition_manager();
  ASSERT_NE(mgr, nullptr);

  const std::string key = "retry_key";
  const std::string value(128, 'r');
  ASSERT_OK(Put(key, value));
  ASSERT_OK(dbfull()->TEST_SwitchMemtable(cfh->cfd()));

  std::vector<BlobFileAddition> additions1;
  std::vector<BlobFileGarbage> garbages1;
  // Simulate a flush retry: prepare once, skip commit, then prepare the same
  // generation again and verify it reuses the already sealed blob file.
  ASSERT_OK(mgr->PrepareFlushAdditions(WriteOptions(), /*num_generations=*/1,
                                       &additions1, &garbages1));
  ASSERT_EQ(additions1.size(), 1U);
  ASSERT_TRUE(garbages1.empty());
  ASSERT_EQ(Get(key), value);
  ASSERT_EQ(CountBlobFiles(), 1U);

  std::vector<BlobFileAddition> additions2;
  std::vector<BlobFileGarbage> garbages2;
  ASSERT_OK(mgr->PrepareFlushAdditions(WriteOptions(), /*num_generations=*/1,
                                       &additions2, &garbages2));
  ASSERT_EQ(additions2.size(), 1U);
  ASSERT_TRUE(garbages2.empty());
  ASSERT_EQ(additions2[0].GetBlobFileNumber(),
            additions1[0].GetBlobFileNumber());
  ASSERT_EQ(additions2[0].GetTotalBlobCount(),
            additions1[0].GetTotalBlobCount());
  ASSERT_EQ(additions2[0].GetTotalBlobBytes(),
            additions1[0].GetTotalBlobBytes());

  ASSERT_EQ(Get(key), value);
  ASSERT_OK(Flush());
  ASSERT_EQ(Get(key), value);
  ASSERT_EQ(CountBlobFiles(), 1U);

  ColumnFamilyMetaData cf_meta;
  db_->GetColumnFamilyMetaData(&cf_meta);
  ASSERT_EQ(cf_meta.blob_file_count, 1U);
  ASSERT_EQ(cf_meta.blob_files.size(), 1U);
  ASSERT_EQ(cf_meta.blob_files[0].total_blob_count, 1U);

  Close();
  Reopen(options);

  ASSERT_EQ(Get(key), value);
  ASSERT_EQ(CountBlobFiles(), 1U);
}

TEST_F(DBBlobDirectWriteTest, DirectWriteBoundaryValues) {
  Options options = GetDirectWriteOptions();
  options.min_blob_size = 20;

  Reopen(options);

  const std::string below(19, 'b');
  const std::string exact(20, 'e');
  const std::string above(21, 'a');

  ASSERT_OK(Put("below", below));
  ASSERT_OK(Put("exact", exact));
  ASSERT_OK(Put("above", above));

  ASSERT_EQ(Get("below"), below);
  ASSERT_EQ(Get("exact"), exact);
  ASSERT_EQ(Get("above"), above);

  ASSERT_OK(Flush());
  ASSERT_EQ(CountBlobFiles(), 1U);
  ASSERT_EQ(Get("below"), below);
  ASSERT_EQ(Get("exact"), exact);
  ASSERT_EQ(Get("above"), above);
}

TEST_F(DBBlobDirectWriteTest, DirectWriteWriteBatchNoQualifyingValues) {
  Options options = GetDirectWriteOptions();
  options.min_blob_size = 1024;

  Reopen(options);

  WriteBatch batch;
  ASSERT_OK(batch.Put("k1", "small_v1"));
  ASSERT_OK(batch.Put("k2", "small_v2"));
  ASSERT_OK(batch.Delete("k3"));
  ASSERT_OK(batch.SingleDelete("k4"));
  ASSERT_OK(db_->Write(WriteOptions(), &batch));

  ASSERT_EQ(Get("k1"), "small_v1");
  ASSERT_EQ(Get("k2"), "small_v2");
  ASSERT_EQ(Get("k3"), "NOT_FOUND");
  ASSERT_EQ(CountBlobFiles(), 0U);

  ASSERT_OK(Flush());
  ASSERT_EQ(Get("k1"), "small_v1");
  ASSERT_EQ(Get("k2"), "small_v2");
  ASSERT_EQ(CountBlobFiles(), 0U);
}

TEST_F(DBBlobDirectWriteTest, DirectWriteDeleteAndReput) {
  Options options = GetDirectWriteOptions();

  Reopen(options);

  const std::string first_value(96, '1');
  const std::string second_value(128, '2');

  ASSERT_OK(Put("reput_key", first_value));
  ASSERT_EQ(Get("reput_key"), first_value);

  ASSERT_OK(Delete("reput_key"));
  ASSERT_EQ(Get("reput_key"), "NOT_FOUND");

  ASSERT_OK(Put("reput_key", second_value));
  ASSERT_EQ(Get("reput_key"), second_value);

  ASSERT_OK(Flush());
  ASSERT_EQ(Get("reput_key"), second_value);
}

TEST_F(DBBlobDirectWriteTest, DirectWriteSnapshotIsolation) {
  Options options = GetDirectWriteOptions();

  Reopen(options);

  const std::string first_value(96, '1');
  const std::string second_value(128, '2');

  ASSERT_OK(Put("snap_key", first_value));
  const Snapshot* snapshot = db_->GetSnapshot();

  ASSERT_OK(Put("snap_key", second_value));
  ASSERT_OK(Put("snap_new_key", second_value));

  ASSERT_EQ(Get("snap_key"), second_value);
  ASSERT_EQ(Get("snap_new_key"), second_value);

  ReadOptions read_options;
  read_options.snapshot = snapshot;
  std::string value;

  ASSERT_OK(
      db_->Get(read_options, db_->DefaultColumnFamily(), "snap_key", &value));
  ASSERT_EQ(value, first_value);
  ASSERT_TRUE(
      db_->Get(read_options, db_->DefaultColumnFamily(), "snap_new_key", &value)
          .IsNotFound());

  db_->ReleaseSnapshot(snapshot);
}

TEST_F(DBBlobDirectWriteTest,
       DirectWriteLazyIteratorReadSurvivesCompactionWithMultiplePartitions) {
  Options options = GetDirectWriteOptions();
  options.blob_direct_write_partitions = 4;
  options.disable_auto_compactions = true;

  Reopen(options);

  const std::array<std::string, 4> keys{"lazy_iter_key_0", "lazy_iter_key_1",
                                        "lazy_iter_key_2", "lazy_iter_key_3"};
  const std::array<std::string, 4> values{
      std::string(96, 'a'), std::string(96, 'b'), std::string(96, 'c'),
      std::string(96, 'd')};
  const std::array<std::string, 4> new_values{
      std::string(128, 'w'), std::string(128, 'x'), std::string(128, 'y'),
      std::string(128, 'z')};

  for (size_t i = 0; i < keys.size(); ++i) {
    ASSERT_OK(Put(keys[i], values[i]));
  }
  ASSERT_OK(Flush());

  auto get_blob_file_number = [this](const std::string& key) -> uint64_t {
    PinnableSlice value;
    bool is_blob_index = false;
    DBImpl::GetImplOptions get_impl_options;
    get_impl_options.column_family = db_->DefaultColumnFamily();
    get_impl_options.value = &value;
    get_impl_options.is_blob_index = &is_blob_index;
    Status s = dbfull()->GetImpl(ReadOptions(), key, get_impl_options);
    EXPECT_OK(s);
    EXPECT_TRUE(is_blob_index);
    if (!s.ok() || !is_blob_index) {
      return kInvalidBlobFileNumber;
    }

    BlobIndex blob_index;
    s = blob_index.DecodeFrom(value);
    EXPECT_OK(s);
    if (!s.ok()) {
      return kInvalidBlobFileNumber;
    }
    return blob_index.file_number();
  };

  std::array<uint64_t, 4> original_blob_files{};
  for (size_t i = 0; i < keys.size(); ++i) {
    original_blob_files[i] = get_blob_file_number(keys[i]);
    ASSERT_NE(original_blob_files[i], kInvalidBlobFileNumber);
  }

  const uint64_t oldest_blob_file =
      *std::min_element(original_blob_files.begin(), original_blob_files.end());
  size_t target_idx = 0;
  while (target_idx < original_blob_files.size() &&
         original_blob_files[target_idx] == oldest_blob_file) {
    ++target_idx;
  }
  ASSERT_LT(target_idx, original_blob_files.size());

  ReadOptions read_options;
  read_options.allow_unprepared_value = true;
  std::unique_ptr<Iterator> iter(db_->NewIterator(read_options));
  iter->Seek(keys[target_idx]);
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(iter->key(), keys[target_idx]);
  ASSERT_TRUE(iter->value().empty());
  ASSERT_OK(iter->status());

  for (size_t i = 0; i < keys.size(); ++i) {
    ASSERT_OK(Put(keys[i], new_values[i]));
  }
  ASSERT_OK(Flush());

  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), /*begin=*/nullptr,
                              /*end=*/nullptr));
  ASSERT_OK(dbfull()->TEST_WaitForPurge());

  auto get_live_blob_file_numbers = [this]() -> std::set<uint64_t> {
    std::set<uint64_t> blob_file_numbers;

    VersionSet* const versions = dbfull()->GetVersionSet();
    EXPECT_NE(versions, nullptr);
    if (versions == nullptr || versions->GetColumnFamilySet() == nullptr) {
      return blob_file_numbers;
    }

    ColumnFamilyData* const cfd = versions->GetColumnFamilySet()->GetDefault();
    EXPECT_NE(cfd, nullptr);
    if (cfd == nullptr) {
      return blob_file_numbers;
    }

    Version* const dummy_versions = cfd->dummy_versions();
    EXPECT_NE(dummy_versions, nullptr);
    if (dummy_versions == nullptr) {
      return blob_file_numbers;
    }

    for (Version* v = dummy_versions->Next(); v != dummy_versions;
         v = v->Next()) {
      EXPECT_NE(v, nullptr);
      if (v == nullptr) {
        continue;
      }
      for (const auto& meta : v->storage_info()->GetBlobFiles()) {
        EXPECT_NE(meta, nullptr);
        if (meta == nullptr) {
          continue;
        }
        blob_file_numbers.insert(meta->GetBlobFileNumber());
      }
    }

    return blob_file_numbers;
  };

  const auto live_blob_files = get_live_blob_file_numbers();
  ASSERT_TRUE(live_blob_files.find(original_blob_files[target_idx]) !=
              live_blob_files.end());

  ASSERT_TRUE(iter->PrepareValue());
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(iter->value().ToString(), values[target_idx]);
}

TEST_F(
    DBBlobDirectWriteTest,
    DirectWriteLazyIteratorFromMemtableSurvivesFlushCompactionWithMultiplePartitions) {
  Options options = GetDirectWriteOptions();
  options.blob_direct_write_partitions = 4;
  options.disable_auto_compactions = true;

  Reopen(options);

  const std::array<std::string, 4> keys{
      "mem_lazy_iter_key_0", "mem_lazy_iter_key_1", "mem_lazy_iter_key_2",
      "mem_lazy_iter_key_3"};
  const std::array<std::string, 4> values{
      std::string(96, 'a'), std::string(96, 'b'), std::string(96, 'c'),
      std::string(96, 'd')};
  const std::array<std::string, 4> new_values{
      std::string(128, 'w'), std::string(128, 'x'), std::string(128, 'y'),
      std::string(128, 'z')};

  auto get_blob_file_number = [this](const std::string& key) -> uint64_t {
    PinnableSlice value;
    bool is_blob_index = false;
    DBImpl::GetImplOptions get_impl_options;
    get_impl_options.column_family = db_->DefaultColumnFamily();
    get_impl_options.value = &value;
    get_impl_options.is_blob_index = &is_blob_index;
    Status s = dbfull()->GetImpl(ReadOptions(), key, get_impl_options);
    EXPECT_OK(s);
    EXPECT_TRUE(is_blob_index);
    if (!s.ok() || !is_blob_index) {
      return kInvalidBlobFileNumber;
    }

    BlobIndex blob_index;
    s = blob_index.DecodeFrom(value);
    EXPECT_OK(s);
    if (!s.ok()) {
      return kInvalidBlobFileNumber;
    }
    return blob_index.file_number();
  };

  for (size_t i = 0; i < keys.size(); ++i) {
    ASSERT_OK(Put(keys[i], values[i]));
  }

  std::array<uint64_t, 4> original_blob_files{};
  for (size_t i = 0; i < keys.size(); ++i) {
    original_blob_files[i] = get_blob_file_number(keys[i]);
    ASSERT_NE(original_blob_files[i], kInvalidBlobFileNumber);
  }

  const uint64_t oldest_blob_file =
      *std::min_element(original_blob_files.begin(), original_blob_files.end());
  size_t target_idx = 0;
  while (target_idx < original_blob_files.size() &&
         original_blob_files[target_idx] == oldest_blob_file) {
    ++target_idx;
  }
  ASSERT_LT(target_idx, original_blob_files.size());

  ReadOptions read_options;
  read_options.allow_unprepared_value = true;
  std::unique_ptr<Iterator> iter(db_->NewIterator(read_options));
  iter->Seek(keys[target_idx]);
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(iter->key(), keys[target_idx]);
  ASSERT_TRUE(iter->value().empty());
  ASSERT_OK(iter->status());

  ASSERT_OK(Flush());

  for (size_t i = 0; i < keys.size(); ++i) {
    ASSERT_OK(Put(keys[i], new_values[i]));
  }
  ASSERT_OK(Flush());

  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), /*begin=*/nullptr,
                              /*end=*/nullptr));
  ASSERT_OK(dbfull()->TEST_WaitForPurge());

  ASSERT_TRUE(iter->PrepareValue());
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(iter->value().ToString(), values[target_idx]);
}
TEST_F(DBBlobDirectWriteTest, OrderedTraceUsesLogicalBatchForBlobDirectWrite) {
  AssertOrderedTraceStoresLogicalPut(GetDirectWriteOptions(),
                                     /*expect_blob_files=*/true);
}

TEST_F(DBBlobDirectWriteTest,
       OrderedTraceUsesLogicalBatchForBlobDirectWritePipelinedWrite) {
  Options options = GetDirectWriteOptions();
  options.enable_pipelined_write = true;

  Status s = TryReopen(options);
  ASSERT_TRUE(s.IsNotSupported()) << s.ToString();

  options.enable_blob_direct_write = false;
  AssertOrderedTraceStoresLogicalPut(options, /*expect_blob_files=*/false);
}

class DBBlobDirectWriteWithTimestampTest : public DBBasicTestWithTimestampBase {
 protected:
  DBBlobDirectWriteWithTimestampTest()
      : DBBasicTestWithTimestampBase(
            "db_blob_direct_write_with_timestamp_test") {}
};

TEST_F(DBBlobDirectWriteWithTimestampTest,
       DirectWriteRejectsUserDefinedTimestamps) {
  Options options = GetDefaultOptions();
  options.create_if_missing = true;
  options.enable_blob_files = true;
  options.enable_blob_direct_write = true;
  options.allow_concurrent_memtable_write = false;
  options.min_blob_size = 32;
  const size_t kTimestampSize = Timestamp(0, 0).size();
  TestComparator test_cmp(kTimestampSize);
  options.comparator = &test_cmp;

  ASSERT_TRUE(TryReopen(options).IsNotSupported());
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  RegisterCustomObjects(argc, argv);
  return RUN_ALL_TESTS();
}

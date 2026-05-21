//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <set>

#include "db/column_family.h"
#include "db/compaction/compaction.h"
#include "db/db_test_util.h"
#include "port/stack_trace.h"
#include "rocksdb/sst_file_reader.h"
#include "test_util/testutil.h"

namespace ROCKSDB_NAMESPACE {

namespace {
std::string Key1(uint64_t key) {
  std::string ret;
  PutFixed64(&ret, key);
  std::reverse(ret.begin(), ret.end());
  return ret;
}

std::string Timestamp(uint64_t ts) {
  std::string ret;
  PutFixed64(&ret, ts);
  return ret;
}
}  // anonymous namespace

class TimestampCompatibleCompactionTest : public DBTestBase {
 public:
  TimestampCompatibleCompactionTest()
      : DBTestBase("ts_compatible_compaction_test", /*env_do_fsync=*/true) {}

  std::string Get(const std::string& key, uint64_t ts) {
    ReadOptions read_opts;
    std::string ts_str = Timestamp(ts);
    Slice ts_slice = ts_str;
    read_opts.timestamp = &ts_slice;
    std::string value;
    Status s = db_->Get(read_opts, key, &value);
    if (s.IsNotFound()) {
      value.assign("NOT_FOUND");
    } else if (!s.ok()) {
      value.assign(s.ToString());
    }
    return value;
  }

  // Helper to get all files with their level and timestamps
  std::vector<std::tuple<int, std::string, std::string>>
  GetAllFileTimestamps() {
    std::vector<std::tuple<int, std::string, std::string>> results;
    ColumnFamilyHandle* cfh = db_->DefaultColumnFamily();
    auto* cfd = static_cast_with_check<ColumnFamilyHandleImpl>(cfh)->cfd();
    auto* vstorage = cfd->current()->storage_info();

    for (int level = 0; level < cfd->NumberLevels(); level++) {
      for (auto* file : vstorage->LevelFiles(level)) {
        results.emplace_back(level, file->min_timestamp, file->max_timestamp);
      }
    }
    return results;
  }

  // Helper to compute overall min/max timestamps across all files
  // Returns {min_ts, max_ts} as uint64_t values
  // Asserts that all files have non-empty timestamps
  std::pair<uint64_t, uint64_t> GetOverallTimestampRange() {
    auto files = GetAllFileTimestamps();
    EXPECT_GE(files.size(), 1U);

    uint64_t overall_min = UINT64_MAX;
    uint64_t overall_max = 0;
    for (const auto& [level, min_ts, max_ts] : files) {
      EXPECT_FALSE(min_ts.empty()) << "min_timestamp empty at level " << level;
      EXPECT_FALSE(max_ts.empty()) << "max_timestamp empty at level " << level;

      if (!min_ts.empty() && !max_ts.empty()) {
        uint64_t file_min = DecodeFixed64(min_ts.data());
        uint64_t file_max = DecodeFixed64(max_ts.data());
        overall_min = std::min(overall_min, file_min);
        overall_max = std::max(overall_max, file_max);
      }
    }
    return {overall_min, overall_max};
  }

  // Helper to verify timestamp range matches expected values, including after
  // reopen
  void VerifyTimestampRangeWithPersistence(const Options& options,
                                           uint64_t expected_min,
                                           uint64_t expected_max) {
    // Verify before reopen
    auto [min_ts, max_ts] = GetOverallTimestampRange();
    ASSERT_EQ(expected_min, min_ts);
    ASSERT_EQ(expected_max, max_ts);

    size_t file_count_before = GetAllFileTimestamps().size();

    // Verify manifest persistence by reopening
    Reopen(options);

    // Verify after reopen
    auto [reopened_min_ts, reopened_max_ts] = GetOverallTimestampRange();
    ASSERT_EQ(expected_min, reopened_min_ts);
    ASSERT_EQ(expected_max, reopened_max_ts);
    ASSERT_EQ(file_count_before, GetAllFileTimestamps().size());
  }

  // Helper to create common options for UDT tests with level compaction
  Options CreateTimestampOptions(bool disable_auto_compactions = false) {
    Options options = CurrentOptions();
    options.env = env_;
    options.compaction_style = kCompactionStyleLevel;
    options.num_levels = 4;
    options.persist_user_defined_timestamps = true;
    options.comparator = test::BytewiseComparatorWithU64TsWrapper();
    options.disable_auto_compactions = disable_auto_compactions;
    return options;
  }

  // Helper to write test data with alternating timestamps in a range
  // Writes keys [start_key, end_key) with timestamps alternating between
  // min_ts and max_ts
  void WriteDataWithTimestampRange(int start_key, int end_key, uint64_t min_ts,
                                   uint64_t max_ts) {
    std::string ts_buf;
    for (int i = start_key; i < end_key; i++) {
      ts_buf.clear();
      uint64_t ts = (i % 2 == 0) ? min_ts : max_ts;
      PutFixed64(&ts_buf, ts);
      ASSERT_OK(db_->Put(WriteOptions(), Key(i), ts_buf,
                         "value" + std::to_string(i)));
    }
  }

  // Helper to check if any file has the expected timestamp range
  bool HasFileWithTimestampRange(uint64_t expected_min, uint64_t expected_max) {
    auto file_timestamps = GetAllFileTimestamps();
    for (const auto& [level, min_ts, max_ts] : file_timestamps) {
      if (!min_ts.empty() && !max_ts.empty()) {
        uint64_t file_min = DecodeFixed64(min_ts.data());
        uint64_t file_max = DecodeFixed64(max_ts.data());
        if (file_min == expected_min && file_max == expected_max) {
          return true;
        }
      }
    }
    return false;
  }

  // Helper to verify data is readable with a given timestamp
  void VerifyDataReadable(int key, const std::string& expected_value,
                          uint64_t read_ts) {
    std::string value;
    std::string ts_buf;
    PutFixed64(&ts_buf, read_ts);
    ReadOptions read_opts;
    Slice ts_slice(ts_buf);
    read_opts.timestamp = &ts_slice;
    ASSERT_OK(db_->Get(read_opts, Key(key), &value));
    ASSERT_EQ(expected_value, value);
  }
};

TEST_F(TimestampCompatibleCompactionTest, UserKeyCrossFileBoundary) {
  Options options = CurrentOptions();
  options.env = env_;
  options.compaction_style = kCompactionStyleLevel;
  options.comparator = test::BytewiseComparatorWithU64TsWrapper();
  options.level0_file_num_compaction_trigger = 3;
  constexpr size_t kNumKeysPerFile = 101;
  options.memtable_factory.reset(
      test::NewSpecialSkipListFactory(kNumKeysPerFile));
  DestroyAndReopen(options);
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  SyncPoint::GetInstance()->SetCallBack(
      "LevelCompactionPicker::PickCompaction:Return", [&](void* arg) {
        const auto* compaction = static_cast<Compaction*>(arg);
        ASSERT_NE(nullptr, compaction);
        ASSERT_EQ(0, compaction->start_level());
        ASSERT_EQ(1, compaction->num_input_levels());
        // Check that all 3 L0 ssts are picked for level compaction.
        ASSERT_EQ(3, compaction->num_input_files(0));
      });
  SyncPoint::GetInstance()->EnableProcessing();
  // Write a L0 with keys 0, 1, ..., 99 with ts from 100 to 199.
  uint64_t ts = 100;
  uint64_t key = 0;
  WriteOptions write_opts;
  for (; key < kNumKeysPerFile - 1; ++key, ++ts) {
    std::string ts_str = Timestamp(ts);
    ASSERT_OK(
        db_->Put(write_opts, Key1(key), ts_str, "foo_" + std::to_string(key)));
  }
  // Write another L0 with keys 99 with newer ts.
  ASSERT_OK(Flush());
  uint64_t saved_read_ts1 = ts++;
  key = 99;
  for (int i = 0; i < 4; ++i, ++ts) {
    std::string ts_str = Timestamp(ts);
    ASSERT_OK(
        db_->Put(write_opts, Key1(key), ts_str, "bar_" + std::to_string(key)));
  }
  ASSERT_OK(Flush());
  uint64_t saved_read_ts2 = ts++;
  // Write another L0 with keys 99, 100, 101, ..., 150
  for (; key <= 150; ++key, ++ts) {
    std::string ts_str = Timestamp(ts);
    ASSERT_OK(
        db_->Put(write_opts, Key1(key), ts_str, "foo1_" + std::to_string(key)));
  }
  ASSERT_OK(Flush());
  // Wait for compaction to finish
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  uint64_t read_ts = ts;
  ASSERT_EQ("foo_99", Get(Key1(99), saved_read_ts1));
  ASSERT_EQ("bar_99", Get(Key1(99), saved_read_ts2));
  ASSERT_EQ("foo1_99", Get(Key1(99), read_ts));
  SyncPoint::GetInstance()->ClearAllCallBacks();
  SyncPoint::GetInstance()->DisableProcessing();
}

TEST_F(TimestampCompatibleCompactionTest, MultipleSubCompactions) {
  Options options = CurrentOptions();
  options.env = env_;
  options.compaction_style = kCompactionStyleUniversal;
  options.comparator = test::BytewiseComparatorWithU64TsWrapper();
  options.level0_file_num_compaction_trigger = 3;
  options.max_subcompactions = 3;
  options.target_file_size_base = 1024;
  options.statistics = CreateDBStatistics();
  DestroyAndReopen(options);

  uint64_t ts = 100;
  uint64_t key = 0;
  WriteOptions write_opts;

  // Write keys 0, 1, ..., 499 with ts from 100 to 599.
  {
    for (; key <= 499; ++key, ++ts) {
      std::string ts_str = Timestamp(ts);
      ASSERT_OK(db_->Put(write_opts, Key1(key), ts_str,
                         "foo_" + std::to_string(key)));
    }
  }

  // Write keys 500, ..., 999 with ts from 600 to 1099.
  {
    for (; key <= 999; ++key, ++ts) {
      std::string ts_str = Timestamp(ts);
      ASSERT_OK(db_->Put(write_opts, Key1(key), ts_str,
                         "foo_" + std::to_string(key)));
    }
    ASSERT_OK(Flush());
  }

  // Wait for compaction to finish
  {
    ASSERT_OK(dbfull()->RunManualCompaction(
        static_cast_with_check<ColumnFamilyHandleImpl>(
            db_->DefaultColumnFamily())
            ->cfd(),
        0 /* input_level */, 1 /* output_level */, CompactRangeOptions(),
        nullptr /* begin */, nullptr /* end */, true /* exclusive */,
        true /* disallow_trivial_move */,
        std::numeric_limits<uint64_t>::max() /* max_file_num_to_ignore */,
        "" /*trim_ts*/));
  }

  // Check stats to make sure multiple subcompactions were scheduled for
  // boundaries not to be nullptr.
  {
    HistogramData num_sub_compactions;
    options.statistics->histogramData(NUM_SUBCOMPACTIONS_SCHEDULED,
                                      &num_sub_compactions);
    ASSERT_GT(num_sub_compactions.sum, 1);
  }

  for (key = 0; key <= 999; ++key) {
    ASSERT_EQ("foo_" + std::to_string(key), Get(Key1(key), ts));
  }
}

class TestFilePartitioner : public SstPartitioner {
 public:
  explicit TestFilePartitioner() = default;
  ~TestFilePartitioner() override = default;

  const char* Name() const override { return "TestFilePartitioner"; }
  PartitionerResult ShouldPartition(
      const PartitionerRequest& /*request*/) override {
    return PartitionerResult::kRequired;
  }
  bool CanDoTrivialMove(const Slice& /*smallest_user_key*/,
                        const Slice& /*largest_user_key*/) override {
    return false;
  }
};

class TestFilePartitionerFactory : public SstPartitionerFactory {
 public:
  explicit TestFilePartitionerFactory() = default;
  std::unique_ptr<SstPartitioner> CreatePartitioner(
      const SstPartitioner::Context& /*context*/) const override {
    std::unique_ptr<SstPartitioner> ret =
        std::make_unique<TestFilePartitioner>();
    return ret;
  }
  const char* Name() const override { return "TestFilePartitionerFactory"; }
};

TEST_F(TimestampCompatibleCompactionTest, CompactFilesRangeCheckL0) {
  Options options = CurrentOptions();
  options.env = env_;
  options.sst_partitioner_factory =
      std::make_shared<TestFilePartitionerFactory>();
  options.comparator = test::BytewiseComparatorWithU64TsWrapper();
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);

  constexpr int kNumFiles = 10;
  constexpr int kKeysPerFile = 2;
  const std::string user_key = "foo";
  constexpr uint64_t start_ts = 10000;

  uint64_t cur_ts = start_ts;
  for (int k = 0; k < kNumFiles; ++k) {
    for (int i = 0; i < kKeysPerFile; ++i) {
      ASSERT_OK(db_->Put(WriteOptions(), user_key, Timestamp(cur_ts),
                         "v" + std::to_string(i)));
      ++cur_ts;
    }
    ASSERT_OK(db_->Flush(FlushOptions()));
  }

  std::vector<std::string> input_files{};
  {
    std::vector<std::string> files;
    ASSERT_OK(env_->GetChildren(dbname_, &files));
    for (const auto& f : files) {
      uint64_t file_num = 0;
      FileType file_type = FileType::kWalFile;
      if (!ParseFileName(f, &file_num, &file_type) ||
          file_type != FileType::kTableFile) {
        continue;
      }
      input_files.emplace_back(f);
    }
    // sorting here by name, which also happens to sort by generation date.
    std::sort(input_files.begin(), input_files.end());
    assert(kNumFiles == input_files.size());
    std::vector<std::string> tmp;
    tmp.emplace_back(input_files[input_files.size() / 2]);
    input_files.swap(tmp);
  }

  {
    std::vector<std::string> output_file_names;
    CompactionJobInfo compaction_job_info;
    ASSERT_OK(db_->CompactFiles(CompactionOptions(), input_files,
                                /*output_level=*/1, /*output_path_id=*/-1,
                                &output_file_names, &compaction_job_info));
    // We expect the L0 files older than the original provided input were all
    // included in the compaction.
    ASSERT_EQ(static_cast<size_t>(kNumFiles / 2 + 1),
              compaction_job_info.input_files.size());
  }
}

TEST_F(TimestampCompatibleCompactionTest, CompactFilesRangeCheckL1) {
  Options options = CurrentOptions();
  options.env = env_;
  options.sst_partitioner_factory =
      std::make_shared<TestFilePartitionerFactory>();
  options.comparator = test::BytewiseComparatorWithU64TsWrapper();

  constexpr int kNumFiles = 4;
  options.level0_file_num_compaction_trigger = kNumFiles;

  DestroyAndReopen(options);

  constexpr int kKeysPerFile = 2;
  const std::string user_key = "foo";
  constexpr uint64_t start_ts = 10000;

  uint64_t cur_ts = start_ts;
  // Generate some initial files in both L0 and L1.
  for (int k = 0; k < kNumFiles; ++k) {
    for (int i = 0; i < kKeysPerFile; ++i) {
      ASSERT_OK(db_->Put(WriteOptions(), user_key, Timestamp(cur_ts),
                         "v" + std::to_string(i)));
      ++cur_ts;
    }
    ASSERT_OK(db_->Flush(FlushOptions()));
  }
  ASSERT_OK(dbfull()->TEST_WaitForCompact());

  ASSERT_EQ(0, NumTableFilesAtLevel(/*level=*/0, /*cf=*/0));
  ASSERT_EQ(kNumFiles * kKeysPerFile,
            NumTableFilesAtLevel(/*level=*/1, /*cf=*/0));

  constexpr int additional_l0s = 2;
  for (int i = 0; i < additional_l0s; ++i, ++cur_ts) {
    ASSERT_OK(db_->Put(WriteOptions(), user_key, Timestamp(cur_ts), "v"));
    ASSERT_OK(db_->Flush(FlushOptions()));
  }
  ASSERT_EQ(additional_l0s, NumTableFilesAtLevel(/*level=*/0, /*cf=*/0));

  std::vector<std::string> inputs;
  {
    std::vector<LiveFileMetaData> fmetas;
    db_->GetLiveFilesMetaData(&fmetas);
    bool included_one_l1 = false;
    for (const auto& meta : fmetas) {
      if (meta.level == 0) {
        inputs.emplace_back(meta.relative_filename);
      } else if (!included_one_l1) {
        inputs.emplace_back(meta.relative_filename);
        included_one_l1 = true;
      }
    }
  }
  ASSERT_EQ(static_cast<size_t>(3), inputs.size());
  {
    std::vector<std::string> output_file_names;
    CompactionJobInfo compaction_job_info;

    ASSERT_OK(db_->CompactFiles(CompactionOptions(), inputs, /*output_level=*/1,
                                /*output_path_id=*/-1, &output_file_names,
                                &compaction_job_info));
    ASSERT_EQ(kNumFiles * kKeysPerFile + 2, output_file_names.size());
    ASSERT_EQ(kNumFiles * kKeysPerFile + 2,
              static_cast<int>(compaction_job_info.input_files.size()));
  }
}

TEST_F(TimestampCompatibleCompactionTest, EmptyCompactionOutput) {
  Options options = CurrentOptions();
  options.env = env_;
  options.comparator = test::BytewiseComparatorWithU64TsWrapper();
  DestroyAndReopen(options);

  std::string ts_str = Timestamp(1);
  WriteOptions wopts;
  ASSERT_OK(
      db_->DeleteRange(wopts, db_->DefaultColumnFamily(), "k1", "k3", ts_str));
  ASSERT_OK(Flush());

  ts_str = Timestamp(3);
  Slice ts = ts_str;
  CompactRangeOptions cro;
  // range tombstone will be dropped during compaction
  cro.full_history_ts_low = &ts;
  cro.bottommost_level_compaction = BottommostLevelCompaction::kForce;
  ASSERT_OK(db_->CompactRange(cro, nullptr, nullptr));
}

TEST_F(TimestampCompatibleCompactionTest, SeqnoZeroingWithUDT) {
  // This test validates that seqno is only zeroed when the timestamp is older
  // than full_history_ts_low_. Before the fix, seqno was incorrectly zeroed
  // even when UDT was enabled but timestamp wasn't old enough.

  Options options = CurrentOptions();
  options.env = env_;
  options.comparator = test::BytewiseComparatorWithU64TsWrapper();
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);

  // Track seqno zeroing events and which keys are zeroed
  std::set<std::string> zeroed_keys;
  SyncPoint::GetInstance()->SetCallBack(
      "CompactionIterator::PrepareOutput:ZeroingSeq", [&](void* arg) {
        auto* ikey = static_cast<ParsedInternalKey*>(arg);
        ASSERT_EQ(0, ikey->sequence);
        // Extract user key without timestamp (last 8 bytes)
        Slice user_key_with_ts = ikey->user_key;
        std::string user_key =
            user_key_with_ts.ToString().substr(0, user_key_with_ts.size() - 8);
        zeroed_keys.insert(user_key);
      });
  SyncPoint::GetInstance()->EnableProcessing();

  // Case 1: Test that seqno is NOT zeroed when full_history_ts_low is not set
  // Write a key with timestamp 100
  std::string ts_str = Timestamp(100);
  ASSERT_OK(db_->Put(WriteOptions(), "key1", ts_str, "value1"));
  ASSERT_OK(Flush());

  zeroed_keys.clear();
  {
    CompactRangeOptions cro;
    cro.bottommost_level_compaction = BottommostLevelCompaction::kForce;
    ASSERT_OK(db_->CompactRange(cro, nullptr, nullptr));
  }
  // With UDT enabled and no full_history_ts_low, seqno should NOT be zeroed
  ASSERT_TRUE(zeroed_keys.empty());

  // Case 2: Test that seqno IS zeroed when timestamp < full_history_ts_low
  // Write a new key with timestamp 200
  ts_str = Timestamp(200);
  ASSERT_OK(db_->Put(WriteOptions(), "key2", ts_str, "value2"));
  ASSERT_OK(Flush());

  zeroed_keys.clear();
  {
    // Set full_history_ts_low to 300, so ts < 300 should be zeroed
    std::string full_history_ts_low = Timestamp(300);
    Slice ts_slice = full_history_ts_low;
    CompactRangeOptions cro;
    cro.full_history_ts_low = &ts_slice;
    cro.bottommost_level_compaction = BottommostLevelCompaction::kForce;
    ASSERT_OK(db_->CompactRange(cro, nullptr, nullptr));
  }
  // key1 (ts=100) and key2 (ts=200) both have ts < 300, so both should be
  // zeroed
  ASSERT_EQ(2u, zeroed_keys.size());
  ASSERT_TRUE(zeroed_keys.count("key1") > 0);
  ASSERT_TRUE(zeroed_keys.count("key2") > 0);

  // Case 3: Write a new key with timestamp >= full_history_ts_low
  // and verify it is NOT zeroed while old keys are re-zeroed
  ts_str = Timestamp(500);
  ASSERT_OK(db_->Put(WriteOptions(), "key3", ts_str, "value3"));
  ASSERT_OK(Flush());

  zeroed_keys.clear();
  {
    // Set full_history_ts_low to 400
    // key1 (ts=100) and key2 (ts=200) have ts < 400, will be re-processed
    // key3 (ts=500) has ts >= 400, should NOT be zeroed
    std::string full_history_ts_low = Timestamp(400);
    Slice ts_slice = full_history_ts_low;
    CompactRangeOptions cro;
    cro.full_history_ts_low = &ts_slice;
    cro.bottommost_level_compaction = BottommostLevelCompaction::kForce;
    ASSERT_OK(db_->CompactRange(cro, nullptr, nullptr));
  }
  // key3 should NOT appear in zeroed_keys since ts=500 >= 400
  ASSERT_TRUE(zeroed_keys.count("key3") == 0);

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();

  // Verify data is still readable
  std::string value;
  ts_str = Timestamp(600);
  Slice read_ts = ts_str;
  ReadOptions read_opts;
  read_opts.timestamp = &read_ts;
  ASSERT_OK(db_->Get(read_opts, "key1", &value));
  ASSERT_EQ("value1", value);
  ASSERT_OK(db_->Get(read_opts, "key2", &value));
  ASSERT_EQ("value2", value);
  ASSERT_OK(db_->Get(read_opts, "key3", &value));
  ASSERT_EQ("value3", value);
}

// Test that files with max_timestamp >= full_history_ts_low are not marked
// for bottommost compaction, which prevents infinite compaction loops.
TEST_F(TimestampCompatibleCompactionTest,
       BottommostCompactionRespectsFullHistoryTsLow) {
  Options options = CreateTimestampOptions();
  options.level0_file_num_compaction_trigger = 4;

  DestroyAndReopen(options);

  // Write some data with timestamps 100-199
  std::string ts_buf;
  for (int i = 0; i < 100; i++) {
    ts_buf.clear();
    PutFixed64(&ts_buf, 100 + i);
    ASSERT_OK(
        db_->Put(WriteOptions(), Key(i), ts_buf, "value" + std::to_string(i)));
  }
  ASSERT_OK(Flush());

  // Compact to the bottommost level
  CompactRangeOptions cro;
  cro.bottommost_level_compaction = BottommostLevelCompaction::kForce;
  ASSERT_OK(db_->CompactRange(cro, nullptr, nullptr));

  // Set full_history_ts_low to 150 - files with max_ts >= 150 should NOT be
  // marked for bottommost compaction since seqno cannot be zeroed
  ts_buf.clear();
  PutFixed64(&ts_buf, 150);
  ASSERT_OK(db_->IncreaseFullHistoryTsLow(db_->DefaultColumnFamily(), ts_buf));

  // Release a snapshot to potentially trigger bottommost file marking
  // but files should NOT be marked because max_ts (199) >= full_history_ts_low
  // (150)
  const Snapshot* snap = db_->GetSnapshot();
  db_->ReleaseSnapshot(snap);

  // Wait for any scheduled compactions - should complete without infinite loop
  // Use a reasonable timeout to detect infinite loops
  WaitForCompactOptions wfc_options;
  wfc_options.timeout = std::chrono::microseconds(5000000);  // 5 seconds
  Status s = dbfull()->WaitForCompact(wfc_options);
  // Should succeed without timeout (no infinite compaction loop)
  ASSERT_TRUE(s.ok() || s.IsTimedOut());
  if (s.IsTimedOut()) {
    // If timeout, the fix is not working - this should not happen
    FAIL() << "WaitForCompact timed out - possible infinite compaction loop";
  }

  // Now set full_history_ts_low beyond max timestamp in the file (200+)
  // This should allow the file to be properly marked and compacted
  ts_buf.clear();
  PutFixed64(&ts_buf, 300);
  ASSERT_OK(db_->IncreaseFullHistoryTsLow(db_->DefaultColumnFamily(), ts_buf));

  // Trigger another snapshot release to potentially mark files
  snap = db_->GetSnapshot();
  db_->ReleaseSnapshot(snap);

  // Now compaction should clean up the file.
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
}

// Test that files are NOT marked for bottommost compaction when UDT is enabled
// and full_history_ts_low has never been set (empty).
TEST_F(TimestampCompatibleCompactionTest,
       BottommostCompactionSkipsWhenFullHistoryTsLowNotSet) {
  Options options = CreateTimestampOptions();

  DestroyAndReopen(options);

  // Write some data with timestamps 100-199
  std::string ts_buf;
  for (int i = 0; i < 100; i++) {
    ts_buf.clear();
    PutFixed64(&ts_buf, 100 + i);
    ASSERT_OK(
        db_->Put(WriteOptions(), Key(i), ts_buf, "value" + std::to_string(i)));
  }
  ASSERT_OK(Flush());

  // Compact to the bottommost level without setting full_history_ts_low
  CompactRangeOptions cro;
  cro.bottommost_level_compaction = BottommostLevelCompaction::kForce;
  ASSERT_OK(db_->CompactRange(cro, nullptr, nullptr));

  // Verify files have valid max_timestamp
  auto file_timestamps = GetAllFileTimestamps();
  ASSERT_GE(file_timestamps.size(), 1U);
  for (const auto& [level, min_ts, max_ts] : file_timestamps) {
    ASSERT_FALSE(max_ts.empty()) << "max_timestamp should not be empty";
  }

  // full_history_ts_low is NOT set (empty), so files should NOT be marked
  // for bottommost compaction even after releasing a snapshot.
  // This tests the branch: if (full_history_ts_low.empty()) { continue; }
  const Snapshot* snap = db_->GetSnapshot();
  db_->ReleaseSnapshot(snap);

  // Wait for any scheduled compactions
  ASSERT_OK(dbfull()->TEST_WaitForCompact());

  // Now set full_history_ts_low to a value > max_timestamp (199) in the file
  // This should allow the file to be properly marked and compacted
  ts_buf.clear();
  PutFixed64(&ts_buf, 300);
  ASSERT_OK(db_->IncreaseFullHistoryTsLow(db_->DefaultColumnFamily(), ts_buf));

  // Trigger another snapshot release to potentially mark files
  snap = db_->GetSnapshot();
  db_->ReleaseSnapshot(snap);

  // Now compaction should be able to proceed since full_history_ts_low is set
  ASSERT_OK(dbfull()->TEST_WaitForCompact());

  // Verify data is still readable
  VerifyDataReadable(0, "value0", 250);
}

// Test that ingested SST files created with UDT have their min/max timestamps
// properly extracted from table properties and populated in FileMetaData.
// This verifies the fix in external_sst_file_ingestion_job.cc that calls
// ExtractTimestampFromTableProperties after creating FileMetaData.
TEST_F(TimestampCompatibleCompactionTest,
       IngestedFileTimestampsExtractedFromTableProperties) {
  Options options = CreateTimestampOptions();

  DestroyAndReopen(options);

  // Create an SST file WITH timestamps using SstFileWriter
  std::string sst_file = dbname_ + "/ingested_udt_file.sst";
  const uint64_t kMinTs = 100;
  const uint64_t kMaxTs = 200;

  {
    SstFileWriter sst_file_writer(EnvOptions(), options);
    ASSERT_OK(sst_file_writer.Open(sst_file));

    std::string ts_buf;
    for (int i = 0; i < 10; i++) {
      // Alternate between min and max timestamps
      uint64_t ts = (i % 2 == 0) ? kMinTs : kMaxTs;
      ts_buf.clear();
      PutFixed64(&ts_buf, ts);
      // SstFileWriter with UDT comparator requires key with timestamp
      ASSERT_OK(
          sst_file_writer.Put(Key(i), ts_buf, "value" + std::to_string(i)));
    }
    ASSERT_OK(sst_file_writer.Finish());
  }

  // Verify the SST file has timestamp properties before ingestion
  {
    std::unique_ptr<SstFileReader> reader(new SstFileReader(options));
    ASSERT_OK(reader->Open(sst_file));
    auto props = reader->GetTableProperties();
    auto& user_collected = props->user_collected_properties;
    ASSERT_TRUE(user_collected.find("rocksdb.timestamp_min") !=
                user_collected.end())
        << "SST file should have rocksdb.timestamp_min property";
    ASSERT_TRUE(user_collected.find("rocksdb.timestamp_max") !=
                user_collected.end())
        << "SST file should have rocksdb.timestamp_max property";
  }

  // Ingest the SST file
  IngestExternalFileOptions ifo;
  ifo.move_files = false;
  ASSERT_OK(db_->IngestExternalFile({sst_file}, ifo));

  // Verify the ingested file has proper timestamps in FileMetaData
  ASSERT_TRUE(HasFileWithTimestampRange(kMinTs, kMaxTs))
      << "Ingested file should have min_timestamp=" << kMinTs
      << " and max_timestamp=" << kMaxTs << " in FileMetaData";

  // Verify timestamps persist after reopen
  Reopen(options);

  ASSERT_TRUE(HasFileWithTimestampRange(kMinTs, kMaxTs))
      << "Ingested file timestamps should persist after reopen";

  // Verify data is readable
  VerifyDataReadable(0, "value0", kMaxTs);

  // Clean up
  ASSERT_OK(env_->DeleteFile(sst_file));
}

// Test that min/max timestamps are correctly tracked in FileMetaData and
// persisted in the manifest during flush.
TEST_F(TimestampCompatibleCompactionTest, TimestampRangePersistenceFlush) {
  Options options = CreateTimestampOptions();

  DestroyAndReopen(options);

  // Expected timestamp range
  const uint64_t kMinTs = 100;
  const uint64_t kMaxTs = 200;

  // Write data with specific timestamp range
  WriteDataWithTimestampRange(0, 50, kMinTs, kMaxTs);
  ASSERT_OK(Flush());

  // First verify table properties have the timestamps
  // (this confirms TimestampTablePropertiesCollector is working)
  TablePropertiesCollection props;
  ASSERT_OK(db_->GetPropertiesOfAllTables(&props));
  ASSERT_EQ(1U, props.size());
  for (const auto& item : props) {
    auto& user_collected = item.second->user_collected_properties;
    ASSERT_TRUE(user_collected.find("rocksdb.timestamp_min") !=
                user_collected.end());
    ASSERT_TRUE(user_collected.find("rocksdb.timestamp_max") !=
                user_collected.end());
    // Verify the collected timestamps match expected values
    std::string collected_min_ts = user_collected.at("rocksdb.timestamp_min");
    std::string collected_max_ts = user_collected.at("rocksdb.timestamp_max");
    ASSERT_EQ(kMinTs, DecodeFixed64(collected_min_ts.data()));
    ASSERT_EQ(kMaxTs, DecodeFixed64(collected_max_ts.data()));
  }

  // Verify FileMetaData timestamps and persistence through reopen
  VerifyTimestampRangeWithPersistence(options, kMinTs, kMaxTs);

  // Verify we can still read the data
  VerifyDataReadable(0, "value0", kMaxTs);
}

// Test that min/max timestamps are correctly merged during compaction
// and persisted in the manifest.
TEST_F(TimestampCompatibleCompactionTest, TimestampRangePersistenceCompaction) {
  Options options = CreateTimestampOptions(true /* disable_auto_compactions */);

  DestroyAndReopen(options);

  // Create multiple L0 files with different timestamp ranges
  // File 1: timestamps 100-150
  const uint64_t kFile1MinTs = 100;
  const uint64_t kFile1MaxTs = 150;
  WriteDataWithTimestampRange(0, 10, kFile1MinTs, kFile1MaxTs);
  ASSERT_OK(Flush());

  // File 2: timestamps 50-80 (earlier range)
  const uint64_t kFile2MinTs = 50;
  const uint64_t kFile2MaxTs = 80;
  WriteDataWithTimestampRange(10, 20, kFile2MinTs, kFile2MaxTs);
  ASSERT_OK(Flush());

  // File 3: timestamps 200-300 (later range)
  const uint64_t kFile3MinTs = 200;
  const uint64_t kFile3MaxTs = 300;
  WriteDataWithTimestampRange(20, 30, kFile3MinTs, kFile3MaxTs);
  ASSERT_OK(Flush());

  // Expected combined range: min=50, max=300
  const uint64_t kExpectedMinTs = 50;
  const uint64_t kExpectedMaxTs = 300;

  // Verify we have 3 L0 files before compaction with valid timestamps
  auto files_before = GetAllFileTimestamps();
  ASSERT_EQ(3U, files_before.size());
  for (const auto& [level, min_ts, max_ts] : files_before) {
    ASSERT_EQ(0, level);  // All files should be in L0
    ASSERT_FALSE(min_ts.empty());
    ASSERT_FALSE(max_ts.empty());
  }

  // Trigger compaction
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  ASSERT_OK(dbfull()->TEST_WaitForCompact());

  // Verify timestamp range and persistence through reopen
  VerifyTimestampRangeWithPersistence(options, kExpectedMinTs, kExpectedMaxTs);

  // Verify data is still readable
  VerifyDataReadable(0, "value0", kExpectedMaxTs);
  VerifyDataReadable(15, "value15", kExpectedMaxTs);
  VerifyDataReadable(25, "value25", kExpectedMaxTs);
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

//  Copyright (c) Meta Platforms, Inc. and affiliates.
//
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/db_test_util.h"
#include "port/stack_trace.h"
#include "rocksdb/listener.h"

namespace ROCKSDB_NAMESPACE {

#if !defined(ROCKSDB_LITE)

class TieredCompactionTest : public DBTestBase,
                             public testing::WithParamInterface<bool> {
 public:
  TieredCompactionTest()
      : DBTestBase("tiered_compaction_test", /*env_do_fsync=*/true),
        kBasicCompStats(CompactionReason::kUniversalSizeAmplification, 1),
        kBasicPerKeyPlacementCompStats(
            CompactionReason::kUniversalSizeAmplification, 1),
        kBasicFlushStats(CompactionReason::kFlush, 1) {
    kBasicCompStats.micros = kHasValue;
    kBasicCompStats.cpu_micros = kHasValue;
    kBasicCompStats.bytes_read_non_output_levels = kHasValue;
    kBasicCompStats.num_input_files_in_non_output_levels = kHasValue;
    kBasicCompStats.num_input_records = kHasValue;
    kBasicCompStats.num_dropped_records = kHasValue;

    kBasicPerLevelStats.num_output_records = kHasValue;
    kBasicPerLevelStats.bytes_written = kHasValue;
    kBasicPerLevelStats.num_output_files = kHasValue;

    kBasicPerKeyPlacementCompStats.micros = kHasValue;
    kBasicPerKeyPlacementCompStats.cpu_micros = kHasValue;
    kBasicPerKeyPlacementCompStats.Add(kBasicPerLevelStats);

    kBasicFlushStats.micros = kHasValue;
    kBasicFlushStats.cpu_micros = kHasValue;
    kBasicFlushStats.bytes_written = kHasValue;
    kBasicFlushStats.num_output_files = kHasValue;
  }

 protected:
  static constexpr uint8_t kHasValue = 1;

  InternalStats::CompactionStats kBasicCompStats;
  InternalStats::CompactionStats kBasicPerKeyPlacementCompStats;
  InternalStats::CompactionOutputsStats kBasicPerLevelStats;
  InternalStats::CompactionStats kBasicFlushStats;

  std::atomic_bool enable_per_key_placement = true;

  void SetUp() override {
    SyncPoint::GetInstance()->SetCallBack(
        "Compaction::SupportsPerKeyPlacement:Enabled", [&](void* arg) {
          auto supports_per_key_placement = static_cast<bool*>(arg);
          *supports_per_key_placement = enable_per_key_placement;
        });
    SyncPoint::GetInstance()->EnableProcessing();
  }

  const std::vector<InternalStats::CompactionStats>& GetCompactionStats() {
    VersionSet* const versions = dbfull()->GetVersionSet();
    assert(versions);
    assert(versions->GetColumnFamilySet());

    ColumnFamilyData* const cfd = versions->GetColumnFamilySet()->GetDefault();
    assert(cfd);

    const InternalStats* const internal_stats = cfd->internal_stats();
    assert(internal_stats);

    return internal_stats->TEST_GetCompactionStats();
  }

  const InternalStats::CompactionStats& GetPerKeyPlacementCompactionStats() {
    VersionSet* const versions = dbfull()->GetVersionSet();
    assert(versions);
    assert(versions->GetColumnFamilySet());

    ColumnFamilyData* const cfd = versions->GetColumnFamilySet()->GetDefault();
    assert(cfd);

    const InternalStats* const internal_stats = cfd->internal_stats();
    assert(internal_stats);

    return internal_stats->TEST_GetPerKeyPlacementCompactionStats();
  }

  // Verify the compaction stats, the stats are roughly compared
  void VerifyCompactionStats(
      const std::vector<InternalStats::CompactionStats>& expect_stats,
      const InternalStats::CompactionStats& expect_pl_stats) {
    const std::vector<InternalStats::CompactionStats>& stats =
        GetCompactionStats();
    const size_t kLevels = expect_stats.size();
    ASSERT_EQ(kLevels, stats.size());

    for (auto it = stats.begin(), expect = expect_stats.begin();
         it != stats.end(); it++, expect++) {
      VerifyCompactionStats(*it, *expect);
    }

    const InternalStats::CompactionStats& pl_stats =
        GetPerKeyPlacementCompactionStats();
    VerifyCompactionStats(pl_stats, expect_pl_stats);
  }

  void ResetAllStats(std::vector<InternalStats::CompactionStats>& stats,
                     InternalStats::CompactionStats& pl_stats) {
    ASSERT_OK(dbfull()->ResetStats());
    for (auto& level_stats : stats) {
      level_stats.Clear();
    }
    pl_stats.Clear();
  }

  // bottommost_temperature is renaming to last_level_temperature, set either
  // of them should have the same effect.
  void SetColdTemperature(Options& options) {
    if (GetParam()) {
      options.bottommost_temperature = Temperature::kCold;
    } else {
      options.last_level_temperature = Temperature::kCold;
    }
  }

 private:
  void CompareStats(uint64_t val, uint64_t expect) {
    if (expect > 0) {
      ASSERT_TRUE(val > 0);
    } else {
      ASSERT_EQ(val, 0);
    }
  }

  void VerifyCompactionStats(
      const InternalStats::CompactionStats& stats,
      const InternalStats::CompactionStats& expect_stats) {
    CompareStats(stats.micros, expect_stats.micros);
    CompareStats(stats.cpu_micros, expect_stats.cpu_micros);
    CompareStats(stats.bytes_read_non_output_levels,
                 expect_stats.bytes_read_non_output_levels);
    CompareStats(stats.bytes_read_output_level,
                 expect_stats.bytes_read_output_level);
    CompareStats(stats.bytes_read_blob, expect_stats.bytes_read_blob);
    CompareStats(stats.bytes_written, expect_stats.bytes_written);
    CompareStats(stats.bytes_moved, expect_stats.bytes_moved);
    CompareStats(stats.num_input_files_in_non_output_levels,
                 expect_stats.num_input_files_in_non_output_levels);
    CompareStats(stats.num_input_files_in_output_level,
                 expect_stats.num_input_files_in_output_level);
    CompareStats(stats.num_output_files, expect_stats.num_output_files);
    CompareStats(stats.num_output_files_blob,
                 expect_stats.num_output_files_blob);
    CompareStats(stats.num_input_records, expect_stats.num_input_records);
    CompareStats(stats.num_dropped_records, expect_stats.num_dropped_records);
    CompareStats(stats.num_output_records, expect_stats.num_output_records);
    ASSERT_EQ(stats.count, expect_stats.count);
    for (int i = 0; i < static_cast<int>(CompactionReason::kNumOfReasons);
         i++) {
      ASSERT_EQ(stats.counts[i], expect_stats.counts[i]);
    }
  }
};

TEST_P(TieredCompactionTest, SequenceBasedTieredStorageUniversal) {
  const int kNumTrigger = 4;
  const int kNumLevels = 7;
  const int kNumKeys = 100;
  const int kLastLevel = kNumLevels - 1;

  auto options = CurrentOptions();
  options.compaction_style = kCompactionStyleUniversal;
  SetColdTemperature(options);
  options.level0_file_num_compaction_trigger = kNumTrigger;
  options.statistics = CreateDBStatistics();
  options.max_subcompactions = 10;
  DestroyAndReopen(options);

  std::atomic_uint64_t latest_cold_seq = 0;
  std::vector<SequenceNumber> seq_history;

  SyncPoint::GetInstance()->SetCallBack(
      "CompactionIterator::PrepareOutput.context", [&](void* arg) {
        auto context = static_cast<PerKeyPlacementContext*>(arg);
        context->output_to_penultimate_level =
            context->seq_num > latest_cold_seq;
      });
  SyncPoint::GetInstance()->EnableProcessing();

  std::vector<InternalStats::CompactionStats> expect_stats(kNumLevels);
  InternalStats::CompactionStats& last_stats = expect_stats[kLastLevel];
  InternalStats::CompactionStats expect_pl_stats;

  for (int i = 0; i < kNumTrigger; i++) {
    for (int j = 0; j < kNumKeys; j++) {
      ASSERT_OK(Put(Key(i * 10 + j), "value" + std::to_string(i)));
    }
    ASSERT_OK(Flush());
    seq_history.emplace_back(dbfull()->GetLatestSequenceNumber());
    expect_stats[0].Add(kBasicFlushStats);
  }
  ASSERT_OK(dbfull()->WaitForCompact(true));

  // the penultimate level file temperature is not cold, all data are output to
  // the penultimate level.
  ASSERT_EQ("0,0,0,0,0,1", FilesPerLevel());
  ASSERT_GT(GetSstSizeHelper(Temperature::kUnknown), 0);
  ASSERT_EQ(GetSstSizeHelper(Temperature::kCold), 0);

  // basic compaction stats are still counted to the last level
  expect_stats[kLastLevel].Add(kBasicCompStats);
  expect_pl_stats.Add(kBasicPerKeyPlacementCompStats);

  VerifyCompactionStats(expect_stats, expect_pl_stats);

  ResetAllStats(expect_stats, expect_pl_stats);

  // move forward the cold_seq to split the file into 2 levels, so should have
  // both the last level stats and the output_to_penultimate_level stats
  latest_cold_seq = seq_history[0];
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  ASSERT_EQ("0,0,0,0,0,1,1", FilesPerLevel());

  ASSERT_GT(GetSstSizeHelper(Temperature::kUnknown), 0);
  ASSERT_GT(GetSstSizeHelper(Temperature::kCold), 0);

  last_stats.Add(kBasicCompStats);
  last_stats.ResetCompactionReason(CompactionReason::kManualCompaction);
  last_stats.Add(kBasicPerLevelStats);
  last_stats.num_dropped_records = 0;
  expect_pl_stats.Add(kBasicPerKeyPlacementCompStats);
  expect_pl_stats.ResetCompactionReason(CompactionReason::kManualCompaction);
  VerifyCompactionStats(expect_stats, expect_pl_stats);

  // delete all cold data, so all data will be on penultimate level
  for (int i = 0; i < 10; i++) {
    ASSERT_OK(Delete(Key(i)));
  }
  ASSERT_OK(Flush());

  ResetAllStats(expect_stats, expect_pl_stats);

  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  ASSERT_EQ("0,0,0,0,0,1", FilesPerLevel());
  ASSERT_GT(GetSstSizeHelper(Temperature::kUnknown), 0);
  ASSERT_EQ(GetSstSizeHelper(Temperature::kCold), 0);

  last_stats.Add(kBasicCompStats);
  last_stats.ResetCompactionReason(CompactionReason::kManualCompaction);
  last_stats.bytes_read_output_level = kHasValue;
  last_stats.num_input_files_in_output_level = kHasValue;
  expect_pl_stats.Add(kBasicPerKeyPlacementCompStats);
  expect_pl_stats.ResetCompactionReason(CompactionReason::kManualCompaction);
  VerifyCompactionStats(expect_stats, expect_pl_stats);

  // move forward the cold_seq again with range delete, take a snapshot to keep
  // the range dels in both cold and hot SSTs
  auto snap = db_->GetSnapshot();
  latest_cold_seq = seq_history[2];
  std::string start = Key(25), end = Key(35);
  ASSERT_OK(
      db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), start, end));
  ASSERT_OK(Flush());

  ResetAllStats(expect_stats, expect_pl_stats);

  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  ASSERT_EQ("0,0,0,0,0,1,1", FilesPerLevel());
  ASSERT_GT(GetSstSizeHelper(Temperature::kUnknown), 0);
  ASSERT_GT(GetSstSizeHelper(Temperature::kCold), 0);

  last_stats.Add(kBasicCompStats);
  last_stats.Add(kBasicPerLevelStats);
  last_stats.ResetCompactionReason(CompactionReason::kManualCompaction);
  expect_pl_stats.Add(kBasicPerKeyPlacementCompStats);
  expect_pl_stats.ResetCompactionReason(CompactionReason::kManualCompaction);
  VerifyCompactionStats(expect_stats, expect_pl_stats);

  // verify data
  std::string value;
  for (int i = 0; i < kNumKeys; i++) {
    if (i < 10 || (i >= 25 && i < 35)) {
      ASSERT_TRUE(db_->Get(ReadOptions(), Key(i), &value).IsNotFound());
    } else {
      ASSERT_OK(db_->Get(ReadOptions(), Key(i), &value));
    }
  }

  // range delete all hot data
  start = Key(30);
  end = Key(130);
  ASSERT_OK(
      db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), start, end));
  ASSERT_OK(Flush());
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  ASSERT_EQ("0,0,0,0,0,1,1", FilesPerLevel());
  ASSERT_GT(GetSstSizeHelper(Temperature::kUnknown), 0);
  ASSERT_GT(GetSstSizeHelper(Temperature::kCold), 0);

  // no range del is dropped because of snapshot
  ASSERT_EQ(
      options.statistics->getTickerCount(COMPACTION_RANGE_DEL_DROP_OBSOLETE),
      0);

  // release the snapshot and do compaction again should remove all hot data
  db_->ReleaseSnapshot(snap);
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  ASSERT_EQ("0,0,0,0,0,0,1", FilesPerLevel());
  ASSERT_EQ(GetSstSizeHelper(Temperature::kUnknown), 0);
  ASSERT_GT(GetSstSizeHelper(Temperature::kCold), 0);

  // 2 range dels are dropped
  ASSERT_EQ(
      options.statistics->getTickerCount(COMPACTION_RANGE_DEL_DROP_OBSOLETE),
      3);

  // move backward the cold_seq, for example the user may change the setting of
  // hot/cold data, but it won't impact the existing cold data, as the sequence
  // number is zeroed out.
  latest_cold_seq = seq_history[1];
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  ASSERT_EQ("0,0,0,0,0,0,1", FilesPerLevel());
  ASSERT_EQ(GetSstSizeHelper(Temperature::kUnknown), 0);
  ASSERT_GT(GetSstSizeHelper(Temperature::kCold), 0);
}

TEST_P(TieredCompactionTest, RangeBasedTieredStorageUniversal) {
  const int kNumTrigger = 4;
  const int kNumLevels = 7;
  const int kNumKeys = 100;
  const int kLastLevel = kNumLevels - 1;

  auto options = CurrentOptions();
  options.compaction_style = kCompactionStyleUniversal;
  SetColdTemperature(options);
  options.level0_file_num_compaction_trigger = kNumTrigger;
  options.statistics = CreateDBStatistics();
  options.max_subcompactions = 10;
  DestroyAndReopen(options);
  auto cmp = options.comparator;

  port::Mutex mutex;
  std::string hot_start = Key(10);
  std::string hot_end = Key(50);

  SyncPoint::GetInstance()->SetCallBack(
      "CompactionIterator::PrepareOutput.context", [&](void* arg) {
        auto context = static_cast<PerKeyPlacementContext*>(arg);
        MutexLock l(&mutex);
        context->output_to_penultimate_level =
            cmp->Compare(context->key, hot_start) >= 0 &&
            cmp->Compare(context->key, hot_end) < 0;
      });
  SyncPoint::GetInstance()->EnableProcessing();

  std::vector<InternalStats::CompactionStats> expect_stats(kNumLevels);
  InternalStats::CompactionStats& last_stats = expect_stats[kLastLevel];
  InternalStats::CompactionStats expect_pl_stats;

  for (int i = 0; i < kNumTrigger; i++) {
    for (int j = 0; j < kNumKeys; j++) {
      ASSERT_OK(Put(Key(j), "value" + std::to_string(j)));
    }
    ASSERT_OK(Flush());
    expect_stats[0].Add(kBasicFlushStats);
  }
  ASSERT_OK(dbfull()->WaitForCompact(true));
  ASSERT_EQ("0,0,0,0,0,1,1", FilesPerLevel());
  ASSERT_GT(GetSstSizeHelper(Temperature::kUnknown), 0);
  ASSERT_GT(GetSstSizeHelper(Temperature::kCold), 0);

  last_stats.Add(kBasicCompStats);
  last_stats.Add(kBasicPerLevelStats);
  expect_pl_stats.Add(kBasicPerKeyPlacementCompStats);
  VerifyCompactionStats(expect_stats, expect_pl_stats);

  ResetAllStats(expect_stats, expect_pl_stats);

  // change to all cold, no output_to_penultimate_level output
  {
    MutexLock l(&mutex);
    hot_start = Key(100);
    hot_end = Key(200);
  }
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  ASSERT_EQ("0,0,0,0,0,0,1", FilesPerLevel());
  ASSERT_EQ(GetSstSizeHelper(Temperature::kUnknown), 0);
  ASSERT_GT(GetSstSizeHelper(Temperature::kCold), 0);

  last_stats.Add(kBasicCompStats);
  last_stats.ResetCompactionReason(CompactionReason::kManualCompaction);
  last_stats.Add(kBasicPerLevelStats);
  last_stats.num_dropped_records = 0;
  last_stats.bytes_read_output_level = kHasValue;
  last_stats.num_input_files_in_output_level = kHasValue;
  VerifyCompactionStats(expect_stats, expect_pl_stats);

  // change to all hot, universal compaction support moving data to up level if
  // it's within compaction level range.
  {
    MutexLock l(&mutex);
    hot_start = Key(0);
    hot_end = Key(100);
  }

  // No data is moved from cold tier to hot tier because no input files from L5
  // or higher, it's not safe to move data to output_to_penultimate_level level.
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  ASSERT_EQ("0,0,0,0,0,0,1", FilesPerLevel());

  // Add 2 keys in higher level, but in separated files, the keys within that
  // range should be moved up to the penultimate level
  ASSERT_OK(Put(Key(0), "value" + std::to_string(0)));
  ASSERT_OK(Flush());
  ASSERT_OK(Put(Key(50), "value" + std::to_string(0)));
  ASSERT_OK(Flush());
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  ASSERT_EQ("0,0,0,0,0,1,1", FilesPerLevel());

  // Add an SST with a key range cover all the data we want to move from the
  // last level to the penultimate level
  ASSERT_OK(Put(Key(0), "value" + std::to_string(0)));
  ASSERT_OK(Put(Key(99), "value" + std::to_string(0)));
  ASSERT_OK(Flush());

  ResetAllStats(expect_stats, expect_pl_stats);

  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  ASSERT_EQ("0,0,0,0,0,1", FilesPerLevel());
  ASSERT_GT(GetSstSizeHelper(Temperature::kUnknown), 0);
  ASSERT_EQ(GetSstSizeHelper(Temperature::kCold), 0);

  last_stats.Add(kBasicCompStats);
  last_stats.ResetCompactionReason(CompactionReason::kManualCompaction);
  last_stats.bytes_read_output_level = kHasValue;
  last_stats.num_input_files_in_output_level = kHasValue;
  expect_pl_stats.Add(kBasicPerKeyPlacementCompStats);
  expect_pl_stats.ResetCompactionReason(CompactionReason::kManualCompaction);
  VerifyCompactionStats(expect_stats, expect_pl_stats);

  // change to only 1 key cold, to test compaction could stop even it matches
  // size amp compaction threshold
  {
    MutexLock l(&mutex);
    hot_start = Key(1);
    hot_end = Key(1000);
  }

  // generate files just enough to trigger compaction
  for (int i = 0; i < kNumTrigger - 1; i++) {
    for (int j = 0; j < 1000; j++) {
      ASSERT_OK(Put(Key(j), "value" + std::to_string(j)));
    }
    ASSERT_OK(Flush());
  }
  ASSERT_OK(dbfull()->WaitForCompact(
      true));  // make sure the compaction is able to finish
  ASSERT_EQ("0,0,0,0,0,1,1", FilesPerLevel());
  ASSERT_GT(GetSstSizeHelper(Temperature::kUnknown), 0);
  ASSERT_GT(GetSstSizeHelper(Temperature::kCold), 0);
  auto opts = db_->GetOptions();
  auto max_size_amp =
      opts.compaction_options_universal.max_size_amplification_percent / 100;
  ASSERT_GT(GetSstSizeHelper(Temperature::kUnknown),
            GetSstSizeHelper(Temperature::kCold) * max_size_amp);

  // delete all cold data
  ASSERT_OK(Delete(Key(0)));
  ASSERT_OK(Flush());
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  ASSERT_EQ("0,0,0,0,0,1", FilesPerLevel());
  ASSERT_GT(GetSstSizeHelper(Temperature::kUnknown), 0);
  ASSERT_EQ(GetSstSizeHelper(Temperature::kCold), 0);

  // range delete overlap with both hot/cold data, with a snapshot to make sure
  // the range del is saved
  auto snap = db_->GetSnapshot();
  {
    MutexLock l(&mutex);
    hot_start = Key(50);
    hot_end = Key(100);
  }
  std::string start = Key(1), end = Key(70);
  ASSERT_OK(
      db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), start, end));
  ASSERT_OK(Flush());
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  ASSERT_EQ("0,0,0,0,0,1,1", FilesPerLevel());
  ASSERT_GT(GetSstSizeHelper(Temperature::kUnknown), 0);
  ASSERT_GT(GetSstSizeHelper(Temperature::kCold), 0);

  // no range del is dropped until snapshot is released
  ASSERT_EQ(
      options.statistics->getTickerCount(COMPACTION_RANGE_DEL_DROP_OBSOLETE),
      0);

  // verify data
  std::string value;
  for (int i = 0; i < kNumKeys; i++) {
    if (i < 70) {
      ASSERT_TRUE(db_->Get(ReadOptions(), Key(i), &value).IsNotFound());
    } else {
      ASSERT_OK(db_->Get(ReadOptions(), Key(i), &value));
    }
  }

  db_->ReleaseSnapshot(snap);
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  ASSERT_EQ("0,0,0,0,0,1,1", FilesPerLevel());
  ASSERT_GT(GetSstSizeHelper(Temperature::kUnknown), 0);
  ASSERT_GT(GetSstSizeHelper(Temperature::kCold), 0);

  // range del is dropped
  ASSERT_EQ(
      options.statistics->getTickerCount(COMPACTION_RANGE_DEL_DROP_OBSOLETE),
      1);
}

TEST_P(TieredCompactionTest, LevelColdRangeDelete) {
  const int kNumTrigger = 4;
  const int kNumLevels = 7;
  const int kNumKeys = 100;
  const int kLastLevel = kNumLevels - 1;

  auto options = CurrentOptions();
  SetColdTemperature(options);
  options.level0_file_num_compaction_trigger = kNumTrigger;
  options.num_levels = kNumLevels;
  options.statistics = CreateDBStatistics();
  options.max_subcompactions = 10;
  DestroyAndReopen(options);

  std::atomic_uint64_t latest_cold_seq = 0;

  SyncPoint::GetInstance()->SetCallBack(
      "CompactionIterator::PrepareOutput.context", [&](void* arg) {
        auto context = static_cast<PerKeyPlacementContext*>(arg);
        context->output_to_penultimate_level =
            context->seq_num > latest_cold_seq;
      });
  SyncPoint::GetInstance()->EnableProcessing();

  for (int i = 0; i < kNumKeys; i++) {
    ASSERT_OK(Put(Key(i), "value" + std::to_string(i)));
  }
  ASSERT_OK(Flush());

  CompactRangeOptions cro;
  cro.bottommost_level_compaction = BottommostLevelCompaction::kForce;
  ASSERT_OK(db_->CompactRange(cro, nullptr, nullptr));
  ASSERT_EQ("0,1",
            FilesPerLevel());  // bottommost but not last level file is hot
  ASSERT_GT(GetSstSizeHelper(Temperature::kUnknown), 0);
  ASSERT_EQ(GetSstSizeHelper(Temperature::kCold), 0);

  // explicitly move the data to the last level
  MoveFilesToLevel(kLastLevel);

  ASSERT_EQ("0,0,0,0,0,0,1", FilesPerLevel());

  auto snap = db_->GetSnapshot();

  std::string start = Key(10);
  std::string end = Key(50);
  ASSERT_OK(
      db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), start, end));

  // 20->30 will be marked as cold data, but it cannot be placed to cold tier
  // (bottommost) otherwise, it will be "deleted" by the range del in
  // output_to_penultimate_level level verify that these data will be able to
  // queried
  for (int i = 20; i < 30; i++) {
    ASSERT_OK(Put(Key(i), "value" + std::to_string(i)));
  }
  // make the range tombstone and data after that cold
  latest_cold_seq = dbfull()->GetLatestSequenceNumber();

  // add home hot data, just for test
  for (int i = 30; i < 40; i++) {
    ASSERT_OK(Put(Key(i), "value" + std::to_string(i)));
  }

  ASSERT_OK(db_->CompactRange(cro, nullptr, nullptr));

  std::string value;
  for (int i = 0; i < kNumKeys; i++) {
    auto s = db_->Get(ReadOptions(), Key(i), &value);
    if ((i >= 10 && i < 20) || (i >= 40 && i < 50)) {
      ASSERT_TRUE(s.IsNotFound());
    } else {
      ASSERT_OK(s);
    }
  }

  db_->ReleaseSnapshot(snap);
}

// Test SST partitioner cut after every single key
class SingleKeySstPartitioner : public SstPartitioner {
 public:
  const char* Name() const override { return "SingleKeySstPartitioner"; }

  PartitionerResult ShouldPartition(
      const PartitionerRequest& /*request*/) override {
    return kRequired;
  }

  bool CanDoTrivialMove(const Slice& /*smallest_user_key*/,
                        const Slice& /*largest_user_key*/) override {
    return false;
  }
};

class SingleKeySstPartitionerFactory : public SstPartitionerFactory {
 public:
  static const char* kClassName() { return "SingleKeySstPartitionerFactory"; }
  const char* Name() const override { return kClassName(); }

  std::unique_ptr<SstPartitioner> CreatePartitioner(
      const SstPartitioner::Context& /* context */) const override {
    return std::unique_ptr<SstPartitioner>(new SingleKeySstPartitioner());
  }
};

TEST_P(TieredCompactionTest, LevelOutofBoundaryRangeDelete) {
  const int kNumTrigger = 4;
  const int kNumLevels = 3;
  const int kNumKeys = 10;

  auto factory = std::make_shared<SingleKeySstPartitionerFactory>();
  auto options = CurrentOptions();
  SetColdTemperature(options);
  options.level0_file_num_compaction_trigger = kNumTrigger;
  options.num_levels = kNumLevels;
  options.statistics = CreateDBStatistics();
  options.sst_partitioner_factory = factory;
  options.max_subcompactions = 10;
  DestroyAndReopen(options);

  std::atomic_uint64_t latest_cold_seq = 0;

  SyncPoint::GetInstance()->SetCallBack(
      "CompactionIterator::PrepareOutput.context", [&](void* arg) {
        auto context = static_cast<PerKeyPlacementContext*>(arg);
        context->output_to_penultimate_level =
            context->seq_num > latest_cold_seq;
      });
  SyncPoint::GetInstance()->EnableProcessing();

  for (int i = 0; i < kNumKeys; i++) {
    ASSERT_OK(Put(Key(i), "value" + std::to_string(i)));
  }
  ASSERT_OK(Flush());

  MoveFilesToLevel(kNumLevels - 1);
  ASSERT_EQ(GetSstSizeHelper(Temperature::kUnknown), 0);
  ASSERT_GT(GetSstSizeHelper(Temperature::kCold), 0);
  ASSERT_EQ("0,0,10", FilesPerLevel());

  auto snap = db_->GetSnapshot();

  // only range delete
  std::string start = Key(3);
  std::string end = Key(5);
  ASSERT_OK(
      db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), start, end));
  ASSERT_OK(Flush());

  CompactRangeOptions cro;
  cro.bottommost_level_compaction = BottommostLevelCompaction::kForce;
  ASSERT_OK(db_->CompactRange(cro, nullptr, nullptr));

  ASSERT_EQ(GetSstSizeHelper(Temperature::kUnknown),
            0);  // tombstone has no size, even it's in hot tier
  ASSERT_GT(GetSstSizeHelper(Temperature::kCold), 0);
  ASSERT_EQ("0,1,10",
            FilesPerLevel());  // one file is at the penultimate level which
                               // only contains a range delete

  // Add 2 hot keys, each is a new SST, they will be placed in the same level as
  // range del, but they don't have overlap with range del, make sure the range
  // del will still be placed there
  latest_cold_seq = dbfull()->GetLatestSequenceNumber();
  ASSERT_OK(Put(Key(0), "new value" + std::to_string(0)));
  auto snap2 = db_->GetSnapshot();
  ASSERT_OK(Put(Key(6), "new value" + std::to_string(6)));
  ASSERT_OK(Flush());

  ASSERT_OK(db_->CompactRange(cro, nullptr, nullptr));
  ASSERT_EQ("0,2,10",
            FilesPerLevel());  // one file is at the penultimate level
                               // which only contains a range delete
  std::vector<LiveFileMetaData> live_file_meta;
  db_->GetLiveFilesMetaData(&live_file_meta);
  bool found_sst_with_del = false;
  uint64_t sst_with_del_num = 0;
  for (const auto& meta : live_file_meta) {
    if (meta.num_deletions > 0) {
      // found SST with del, which has 2 entries, one for data one for range del
      ASSERT_EQ(meta.level,
                kNumLevels - 2);  // output to penultimate level
      ASSERT_EQ(meta.num_entries, 2);
      ASSERT_EQ(meta.num_deletions, 1);
      found_sst_with_del = true;
      sst_with_del_num = meta.file_number;
    }
  }
  ASSERT_TRUE(found_sst_with_del);

  // release the first snapshot and compact, which should compact the range del
  // but new inserted key `0` and `6` are still hot data which will be placed on
  // the penultimate level
  db_->ReleaseSnapshot(snap);
  ASSERT_OK(db_->CompactRange(cro, nullptr, nullptr));
  ASSERT_EQ("0,2,7", FilesPerLevel());
  db_->GetLiveFilesMetaData(&live_file_meta);
  found_sst_with_del = false;
  for (const auto& meta : live_file_meta) {
    // check new SST with del (the old one may not yet be deleted after
    // compaction)
    if (meta.num_deletions > 0 && meta.file_number != sst_with_del_num) {
      found_sst_with_del = true;
    }
  }
  ASSERT_FALSE(found_sst_with_del);

  // Now make all data cold, key 0 will be moved to the last level, but key 6 is
  // still in snap2, so it will be kept at the penultimate level
  latest_cold_seq = dbfull()->GetLatestSequenceNumber();
  ASSERT_OK(db_->CompactRange(cro, nullptr, nullptr));
  ASSERT_EQ("0,1,8", FilesPerLevel());
  ASSERT_GT(GetSstSizeHelper(Temperature::kUnknown), 0);
  ASSERT_GT(GetSstSizeHelper(Temperature::kCold), 0);

  db_->ReleaseSnapshot(snap2);

  ASSERT_OK(db_->CompactRange(cro, nullptr, nullptr));
  ASSERT_EQ("0,0,8", FilesPerLevel());
  ASSERT_EQ(GetSstSizeHelper(Temperature::kUnknown), 0);
  ASSERT_GT(GetSstSizeHelper(Temperature::kCold), 0);
}

TEST_P(TieredCompactionTest, UniversalRangeDelete) {
  const int kNumTrigger = 4;
  const int kNumLevels = 7;
  const int kNumKeys = 10;

  auto factory = std::make_shared<SingleKeySstPartitionerFactory>();

  auto options = CurrentOptions();
  options.compaction_style = kCompactionStyleUniversal;
  SetColdTemperature(options);
  options.level0_file_num_compaction_trigger = kNumTrigger;
  options.statistics = CreateDBStatistics();
  options.sst_partitioner_factory = factory;
  options.max_subcompactions = 10;
  DestroyAndReopen(options);

  std::atomic_uint64_t latest_cold_seq = 0;

  SyncPoint::GetInstance()->SetCallBack(
      "CompactionIterator::PrepareOutput.context", [&](void* arg) {
        auto context = static_cast<PerKeyPlacementContext*>(arg);
        context->output_to_penultimate_level =
            context->seq_num > latest_cold_seq;
      });
  SyncPoint::GetInstance()->EnableProcessing();

  for (int i = 0; i < kNumKeys; i++) {
    ASSERT_OK(Put(Key(i), "value" + std::to_string(i)));
  }
  ASSERT_OK(Flush());

  // compact to the penultimate level with 10 files
  CompactRangeOptions cro;
  cro.bottommost_level_compaction = BottommostLevelCompaction::kForce;
  ASSERT_OK(db_->CompactRange(cro, nullptr, nullptr));

  ASSERT_EQ("0,0,0,0,0,10", FilesPerLevel());
  ASSERT_GT(GetSstSizeHelper(Temperature::kUnknown), 0);
  ASSERT_EQ(GetSstSizeHelper(Temperature::kCold), 0);

  // make all data cold
  latest_cold_seq = dbfull()->GetLatestSequenceNumber();
  ASSERT_OK(db_->CompactRange(cro, nullptr, nullptr));
  ASSERT_EQ("0,0,0,0,0,0,10", FilesPerLevel());
  ASSERT_EQ(GetSstSizeHelper(Temperature::kUnknown), 0);
  ASSERT_GT(GetSstSizeHelper(Temperature::kCold), 0);

  // range del which considered as hot data, but it will be merged and deleted
  // with the last level data
  std::string start = Key(3);
  std::string end = Key(5);
  ASSERT_OK(
      db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), start, end));
  ASSERT_OK(Flush());
  ASSERT_OK(db_->CompactRange(cro, nullptr, nullptr));

  ASSERT_EQ("0,0,0,0,0,0,8", FilesPerLevel());

  // range del with snapshot should be preserved in the penultimate level
  auto snap = db_->GetSnapshot();

  start = Key(6);
  end = Key(8);
  ASSERT_OK(
      db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), start, end));
  ASSERT_OK(Flush());
  ASSERT_OK(db_->CompactRange(cro, nullptr, nullptr));
  ASSERT_EQ("0,0,0,0,0,1,8", FilesPerLevel());

  // Add 2 hot keys, each is a new SST, they will be placed in the same level as
  // range del, but no overlap with range del.
  latest_cold_seq = dbfull()->GetLatestSequenceNumber();
  ASSERT_OK(Put(Key(4), "new value" + std::to_string(0)));
  auto snap2 = db_->GetSnapshot();
  ASSERT_OK(Put(Key(9), "new value" + std::to_string(6)));

  ASSERT_OK(Flush());

  ASSERT_OK(db_->CompactRange(cro, nullptr, nullptr));
  ASSERT_EQ("0,0,0,0,0,2,8", FilesPerLevel());
  // find the SST with range del
  std::vector<LiveFileMetaData> live_file_meta;
  db_->GetLiveFilesMetaData(&live_file_meta);
  bool found_sst_with_del = false;
  uint64_t sst_with_del_num = 0;
  for (const auto& meta : live_file_meta) {
    if (meta.num_deletions > 0) {
      // found SST with del, which has 2 entries, one for data one for range del
      ASSERT_EQ(meta.level,
                kNumLevels - 2);  // output_to_penultimate_level level
      ASSERT_EQ(meta.num_entries, 2);
      ASSERT_EQ(meta.num_deletions, 1);
      found_sst_with_del = true;
      sst_with_del_num = meta.file_number;
    }
  }
  ASSERT_TRUE(found_sst_with_del);

  // release the first snapshot which should compact the range del, but data on
  // the same level is still hot
  db_->ReleaseSnapshot(snap);

  ASSERT_OK(db_->CompactRange(cro, nullptr, nullptr));
  ASSERT_EQ("0,0,0,0,0,2,6", FilesPerLevel());
  db_->GetLiveFilesMetaData(&live_file_meta);
  // no range del should be found in SST
  found_sst_with_del = false;
  for (const auto& meta : live_file_meta) {
    // check new SST with del (the old one may not yet be deleted after
    // compaction)
    if (meta.num_deletions > 0 && meta.file_number != sst_with_del_num) {
      found_sst_with_del = true;
    }
  }
  ASSERT_FALSE(found_sst_with_del);

  // make all data to cold, but key 6 is still protected by snap2
  latest_cold_seq = dbfull()->GetLatestSequenceNumber();
  ASSERT_OK(db_->CompactRange(cro, nullptr, nullptr));
  ASSERT_EQ("0,0,0,0,0,1,7", FilesPerLevel());
  ASSERT_GT(GetSstSizeHelper(Temperature::kUnknown), 0);
  ASSERT_GT(GetSstSizeHelper(Temperature::kCold), 0);

  db_->ReleaseSnapshot(snap2);

  // release snapshot, everything go to bottommost
  ASSERT_OK(db_->CompactRange(cro, nullptr, nullptr));
  ASSERT_EQ("0,0,0,0,0,0,7", FilesPerLevel());
  ASSERT_EQ(GetSstSizeHelper(Temperature::kUnknown), 0);
  ASSERT_GT(GetSstSizeHelper(Temperature::kCold), 0);
}

TEST_P(TieredCompactionTest, SequenceBasedTieredStorageLevel) {
  const int kNumTrigger = 4;
  const int kNumLevels = 7;
  const int kNumKeys = 100;
  const int kLastLevel = kNumLevels - 1;

  auto options = CurrentOptions();
  SetColdTemperature(options);
  options.level0_file_num_compaction_trigger = kNumTrigger;
  options.num_levels = kNumLevels;
  options.statistics = CreateDBStatistics();
  options.max_subcompactions = 10;
  DestroyAndReopen(options);

  std::atomic_uint64_t latest_cold_seq = 0;
  std::vector<SequenceNumber> seq_history;

  SyncPoint::GetInstance()->SetCallBack(
      "CompactionIterator::PrepareOutput.context", [&](void* arg) {
        auto context = static_cast<PerKeyPlacementContext*>(arg);
        context->output_to_penultimate_level =
            context->seq_num > latest_cold_seq;
      });
  SyncPoint::GetInstance()->EnableProcessing();

  std::vector<InternalStats::CompactionStats> expect_stats(kNumLevels);
  InternalStats::CompactionStats& last_stats = expect_stats[kLastLevel];
  InternalStats::CompactionStats expect_pl_stats;

  for (int i = 0; i < kNumTrigger; i++) {
    for (int j = 0; j < kNumKeys; j++) {
      ASSERT_OK(Put(Key(i * 10 + j), "value" + std::to_string(i)));
    }
    ASSERT_OK(Flush());
    expect_stats[0].Add(kBasicFlushStats);
  }
  ASSERT_OK(dbfull()->WaitForCompact(true));

  // non last level is hot
  ASSERT_EQ("0,1", FilesPerLevel());
  ASSERT_GT(GetSstSizeHelper(Temperature::kUnknown), 0);
  ASSERT_EQ(GetSstSizeHelper(Temperature::kCold), 0);

  expect_stats[1].Add(kBasicCompStats);
  expect_stats[1].Add(kBasicPerLevelStats);
  expect_stats[1].ResetCompactionReason(CompactionReason::kLevelL0FilesNum);
  VerifyCompactionStats(expect_stats, expect_pl_stats);

  // move all data to the last level
  MoveFilesToLevel(kLastLevel);

  ResetAllStats(expect_stats, expect_pl_stats);

  // The compaction won't move the data up
  CompactRangeOptions cro;
  cro.bottommost_level_compaction = BottommostLevelCompaction::kForce;
  ASSERT_OK(db_->CompactRange(cro, nullptr, nullptr));
  ASSERT_EQ("0,0,0,0,0,0,1", FilesPerLevel());
  ASSERT_EQ(GetSstSizeHelper(Temperature::kUnknown), 0);
  ASSERT_GT(GetSstSizeHelper(Temperature::kCold), 0);

  last_stats.Add(kBasicCompStats);
  last_stats.Add(kBasicPerLevelStats);
  last_stats.num_dropped_records = 0;
  last_stats.bytes_read_non_output_levels = 0;
  last_stats.num_input_files_in_non_output_levels = 0;
  last_stats.bytes_read_output_level = kHasValue;
  last_stats.num_input_files_in_output_level = kHasValue;
  last_stats.ResetCompactionReason(CompactionReason::kManualCompaction);
  VerifyCompactionStats(expect_stats, expect_pl_stats);

  // Add new data, which is all hot and overriding all existing data
  for (int i = 0; i < kNumTrigger; i++) {
    for (int j = 0; j < kNumKeys; j++) {
      ASSERT_OK(Put(Key(i * 10 + j), "value" + std::to_string(i)));
    }
    ASSERT_OK(Flush());
    seq_history.emplace_back(dbfull()->GetLatestSequenceNumber());
  }
  ASSERT_OK(dbfull()->WaitForCompact(true));
  ASSERT_EQ("0,1,0,0,0,0,1", FilesPerLevel());
  ASSERT_GT(GetSstSizeHelper(Temperature::kUnknown), 0);
  ASSERT_GT(GetSstSizeHelper(Temperature::kCold), 0);

  ResetAllStats(expect_stats, expect_pl_stats);

  // after compaction, all data are hot
  ASSERT_OK(db_->CompactRange(cro, nullptr, nullptr));
  ASSERT_EQ("0,0,0,0,0,1", FilesPerLevel());
  ASSERT_GT(GetSstSizeHelper(Temperature::kUnknown), 0);
  ASSERT_EQ(GetSstSizeHelper(Temperature::kCold), 0);

  for (int level = 2; level < kNumLevels - 1; level++) {
    expect_stats[level].bytes_moved = kHasValue;
  }

  last_stats.Add(kBasicCompStats);
  last_stats.bytes_read_output_level = kHasValue;
  last_stats.num_input_files_in_output_level = kHasValue;
  last_stats.ResetCompactionReason(CompactionReason::kManualCompaction);
  expect_pl_stats.Add(kBasicPerKeyPlacementCompStats);
  expect_pl_stats.ResetCompactionReason(CompactionReason::kManualCompaction);
  VerifyCompactionStats(expect_stats, expect_pl_stats);

  // move forward the cold_seq, try to split the data into cold and hot, but in
  // this case it's unsafe to split the data
  // because it's non-last-level but bottommost file, the sequence number will
  // be zeroed out and lost the time information (with
  // `level_compaction_dynamic_level_bytes` or Universal Compaction, it should
  // be rare.)
  // TODO(zjay): ideally we should avoid zero out non-last-level bottommost file
  latest_cold_seq = seq_history[1];
  ASSERT_OK(db_->CompactRange(cro, nullptr, nullptr));
  ASSERT_EQ("0,0,0,0,0,1", FilesPerLevel());
  ASSERT_GT(GetSstSizeHelper(Temperature::kUnknown), 0);
  ASSERT_EQ(GetSstSizeHelper(Temperature::kCold), 0);

  seq_history.clear();

  // manually move all data (cold) to last level
  MoveFilesToLevel(kLastLevel);
  seq_history.clear();
  // Add new data once again
  for (int i = 0; i < kNumTrigger; i++) {
    for (int j = 0; j < kNumKeys; j++) {
      ASSERT_OK(Put(Key(i * 10 + j), "value" + std::to_string(i)));
    }
    ASSERT_OK(Flush());
    seq_history.emplace_back(dbfull()->GetLatestSequenceNumber());
  }
  ASSERT_OK(dbfull()->WaitForCompact(true));

  latest_cold_seq = seq_history[0];
  ASSERT_OK(db_->CompactRange(cro, nullptr, nullptr));
  ASSERT_EQ("0,0,0,0,0,1,1", FilesPerLevel());
  ASSERT_GT(GetSstSizeHelper(Temperature::kUnknown), 0);
  ASSERT_GT(GetSstSizeHelper(Temperature::kCold), 0);

  // delete all cold data
  for (int i = 0; i < 10; i++) {
    ASSERT_OK(Delete(Key(i)));
  }
  ASSERT_OK(Flush());
  ASSERT_OK(db_->CompactRange(cro, nullptr, nullptr));
  ASSERT_EQ("0,0,0,0,0,1", FilesPerLevel());
  ASSERT_GT(GetSstSizeHelper(Temperature::kUnknown), 0);
  ASSERT_EQ(GetSstSizeHelper(Temperature::kCold), 0);

  latest_cold_seq = seq_history[2];

  MoveFilesToLevel(kLastLevel);

  // move forward the cold_seq again with range delete, take a snapshot to keep
  // the range dels in bottommost
  auto snap = db_->GetSnapshot();

  std::string start = Key(25), end = Key(35);
  ASSERT_OK(
      db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), start, end));
  // add one small key and large key in the input level, to make sure it's able
  // to move hot data to input level within that range
  ASSERT_OK(Put(Key(0), "value" + std::to_string(0)));
  ASSERT_OK(Put(Key(100), "value" + std::to_string(0)));

  ASSERT_OK(Flush());
  ASSERT_OK(db_->CompactRange(cro, nullptr, nullptr));
  ASSERT_EQ("0,0,0,0,0,1,1", FilesPerLevel());
  ASSERT_GT(GetSstSizeHelper(Temperature::kUnknown), 0);
  ASSERT_GT(GetSstSizeHelper(Temperature::kCold), 0);

  // verify data
  std::string value;
  for (int i = 1; i < 130; i++) {
    if (i < 10 || (i >= 25 && i < 35)) {
      ASSERT_TRUE(db_->Get(ReadOptions(), Key(i), &value).IsNotFound());
    } else {
      ASSERT_OK(db_->Get(ReadOptions(), Key(i), &value));
    }
  }

  // delete all hot data
  ASSERT_OK(Delete(Key(0)));
  start = Key(30);
  end = Key(101);  // range [101, 130] is cold, because it's not in input range
                   // in previous compaction
  ASSERT_OK(
      db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), start, end));
  ASSERT_OK(Flush());
  ASSERT_OK(db_->CompactRange(cro, nullptr, nullptr));
  ASSERT_EQ("0,0,0,0,0,1,1", FilesPerLevel());
  ASSERT_GT(GetSstSizeHelper(Temperature::kUnknown), 0);
  ASSERT_GT(GetSstSizeHelper(Temperature::kCold), 0);

  // no range del is dropped because of snapshot
  ASSERT_EQ(
      options.statistics->getTickerCount(COMPACTION_RANGE_DEL_DROP_OBSOLETE),
      0);

  db_->ReleaseSnapshot(snap);

  ASSERT_OK(db_->CompactRange(cro, nullptr, nullptr));
  ASSERT_EQ("0,0,0,0,0,1,1", FilesPerLevel());
  ASSERT_GT(GetSstSizeHelper(Temperature::kUnknown), 0);
  ASSERT_GT(GetSstSizeHelper(Temperature::kCold), 0);

  // 3 range dels dropped, the first one is double counted as expected, which is
  // spread into 2 SST files
  ASSERT_EQ(
      options.statistics->getTickerCount(COMPACTION_RANGE_DEL_DROP_OBSOLETE),
      3);

  // move backward of cold_seq, which might happen when the user change the
  // setting. the hot data won't move up, just to make sure it still runs
  // fine, which is because:
  // 1. sequence number is zeroed out, so no time information
  // 2. leveled compaction only support move data up within the higher level
  // input range
  latest_cold_seq = seq_history[1];
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  ASSERT_EQ("0,0,0,0,0,1,1", FilesPerLevel());
  ASSERT_GT(GetSstSizeHelper(Temperature::kUnknown), 0);
  ASSERT_GT(GetSstSizeHelper(Temperature::kCold), 0);
}

TEST_P(TieredCompactionTest, RangeBasedTieredStorageLevel) {
  const int kNumTrigger = 4;
  const int kNumLevels = 7;
  const int kNumKeys = 100;

  auto options = CurrentOptions();
  SetColdTemperature(options);
  options.level0_file_num_compaction_trigger = kNumTrigger;
  options.level_compaction_dynamic_level_bytes = true;
  options.num_levels = kNumLevels;
  options.statistics = CreateDBStatistics();
  options.max_subcompactions = 10;
  DestroyAndReopen(options);
  auto cmp = options.comparator;

  port::Mutex mutex;
  std::string hot_start = Key(10);
  std::string hot_end = Key(50);

  SyncPoint::GetInstance()->SetCallBack(
      "CompactionIterator::PrepareOutput.context", [&](void* arg) {
        auto context = static_cast<PerKeyPlacementContext*>(arg);
        MutexLock l(&mutex);
        context->output_to_penultimate_level =
            cmp->Compare(context->key, hot_start) >= 0 &&
            cmp->Compare(context->key, hot_end) < 0;
      });
  SyncPoint::GetInstance()->EnableProcessing();

  for (int i = 0; i < kNumTrigger; i++) {
    for (int j = 0; j < kNumKeys; j++) {
      ASSERT_OK(Put(Key(j), "value" + std::to_string(j)));
    }
    ASSERT_OK(Flush());
  }
  ASSERT_OK(dbfull()->WaitForCompact(true));
  ASSERT_EQ("0,0,0,0,0,1,1", FilesPerLevel());
  ASSERT_GT(GetSstSizeHelper(Temperature::kUnknown), 0);
  ASSERT_GT(GetSstSizeHelper(Temperature::kCold), 0);

  // change to all cold
  {
    MutexLock l(&mutex);
    hot_start = Key(100);
    hot_end = Key(200);
  }
  CompactRangeOptions cro;
  cro.bottommost_level_compaction = BottommostLevelCompaction::kForce;
  ASSERT_OK(db_->CompactRange(cro, nullptr, nullptr));
  ASSERT_EQ("0,0,0,0,0,0,1", FilesPerLevel());
  ASSERT_EQ(GetSstSizeHelper(Temperature::kUnknown), 0);
  ASSERT_GT(GetSstSizeHelper(Temperature::kCold), 0);

  // change to all hot, but level compaction only support move cold to hot
  // within it's higher level input range.
  {
    MutexLock l(&mutex);
    hot_start = Key(0);
    hot_end = Key(100);
  }
  ASSERT_OK(db_->CompactRange(cro, nullptr, nullptr));
  ASSERT_EQ("0,0,0,0,0,0,1", FilesPerLevel());
  ASSERT_EQ(GetSstSizeHelper(Temperature::kUnknown), 0);
  ASSERT_GT(GetSstSizeHelper(Temperature::kCold), 0);

  // with mixed hot/cold data
  {
    MutexLock l(&mutex);
    hot_start = Key(50);
    hot_end = Key(100);
  }
  ASSERT_OK(Put(Key(0), "value" + std::to_string(0)));
  ASSERT_OK(Put(Key(100), "value" + std::to_string(100)));
  ASSERT_OK(Flush());
  ASSERT_OK(db_->CompactRange(cro, nullptr, nullptr));
  ASSERT_EQ("0,0,0,0,0,1,1", FilesPerLevel());
  ASSERT_GT(GetSstSizeHelper(Temperature::kUnknown), 0);
  ASSERT_GT(GetSstSizeHelper(Temperature::kCold), 0);

  // delete all hot data, but with snapshot to keep the range del
  auto snap = db_->GetSnapshot();
  std::string start = Key(50);
  std::string end = Key(100);
  ASSERT_OK(
      db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), start, end));
  ASSERT_OK(Flush());
  ASSERT_OK(db_->CompactRange(cro, nullptr, nullptr));
  ASSERT_EQ("0,0,0,0,0,1,1", FilesPerLevel());
  ASSERT_GT(GetSstSizeHelper(Temperature::kUnknown), 0);
  ASSERT_GT(GetSstSizeHelper(Temperature::kCold), 0);

  // no range del is dropped because of snapshot
  ASSERT_EQ(
      options.statistics->getTickerCount(COMPACTION_RANGE_DEL_DROP_OBSOLETE),
      0);

  // release the snapshot and do compaction again should remove all hot data
  db_->ReleaseSnapshot(snap);
  ASSERT_OK(db_->CompactRange(cro, nullptr, nullptr));
  ASSERT_EQ("0,0,0,0,0,0,1", FilesPerLevel());
  ASSERT_EQ(GetSstSizeHelper(Temperature::kUnknown), 0);
  ASSERT_GT(GetSstSizeHelper(Temperature::kCold), 0);

  ASSERT_EQ(
      options.statistics->getTickerCount(COMPACTION_RANGE_DEL_DROP_OBSOLETE),
      1);
}

INSTANTIATE_TEST_CASE_P(TieredCompactionTest, TieredCompactionTest,
                        testing::Bool());

#endif  // !defined(ROCKSDB_LITE)

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
#if !defined(ROCKSDB_LITE)
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
#else
  (void)argc;
  (void)argv;
  return 0;
#endif
}

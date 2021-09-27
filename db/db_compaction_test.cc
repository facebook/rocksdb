//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <tuple>

#include "db/blob/blob_index.h"
#include "db/db_test_util.h"
#include "env/mock_env.h"
#include "port/port.h"
#include "port/stack_trace.h"
#include "rocksdb/concurrent_task_limiter.h"
#include "rocksdb/experimental.h"
#include "rocksdb/sst_file_writer.h"
#include "rocksdb/utilities/convenience.h"
#include "test_util/sync_point.h"
#include "util/concurrent_task_limiter_impl.h"
#include "util/random.h"
#include "utilities/fault_injection_env.h"
#include "utilities/fault_injection_fs.h"

namespace ROCKSDB_NAMESPACE {

// SYNC_POINT is not supported in released Windows mode.
#if !defined(ROCKSDB_LITE)

class DBCompactionTest : public DBTestBase {
 public:
  DBCompactionTest()
      : DBTestBase("/db_compaction_test", /*env_do_fsync=*/true) {}
};

class DBCompactionTestWithParam
    : public DBTestBase,
      public testing::WithParamInterface<std::tuple<uint32_t, bool>> {
 public:
  DBCompactionTestWithParam()
      : DBTestBase("/db_compaction_test", /*env_do_fsync=*/true) {
    max_subcompactions_ = std::get<0>(GetParam());
    exclusive_manual_compaction_ = std::get<1>(GetParam());
  }

  // Required if inheriting from testing::WithParamInterface<>
  static void SetUpTestCase() {}
  static void TearDownTestCase() {}

  uint32_t max_subcompactions_;
  bool exclusive_manual_compaction_;
};

class DBCompactionTestWithBottommostParam
    : public DBTestBase,
      public testing::WithParamInterface<BottommostLevelCompaction> {
 public:
  DBCompactionTestWithBottommostParam()
      : DBTestBase("/db_compaction_test", /*env_do_fsync=*/true) {
    bottommost_level_compaction_ = GetParam();
  }

  BottommostLevelCompaction bottommost_level_compaction_;
};

class DBCompactionDirectIOTest : public DBCompactionTest,
                                 public ::testing::WithParamInterface<bool> {
 public:
  DBCompactionDirectIOTest() : DBCompactionTest() {}
};

// Param = true : target level is non-empty
// Param = false: level between target level and source level
//  is not empty.
class ChangeLevelConflictsWithAuto
    : public DBCompactionTest,
      public ::testing::WithParamInterface<bool> {
 public:
  ChangeLevelConflictsWithAuto() : DBCompactionTest() {}
};

namespace {

class FlushedFileCollector : public EventListener {
 public:
  FlushedFileCollector() {}
  ~FlushedFileCollector() override {}

  void OnFlushCompleted(DB* /*db*/, const FlushJobInfo& info) override {
    std::lock_guard<std::mutex> lock(mutex_);
    flushed_files_.push_back(info.file_path);
  }

  std::vector<std::string> GetFlushedFiles() {
    std::lock_guard<std::mutex> lock(mutex_);
    std::vector<std::string> result;
    for (auto fname : flushed_files_) {
      result.push_back(fname);
    }
    return result;
  }

  void ClearFlushedFiles() { flushed_files_.clear(); }

 private:
  std::vector<std::string> flushed_files_;
  std::mutex mutex_;
};

class CompactionStatsCollector : public EventListener {
public:
  CompactionStatsCollector()
      : compaction_completed_(static_cast<int>(CompactionReason::kNumOfReasons)) {
    for (auto& v : compaction_completed_) {
      v.store(0);
    }
  }

  ~CompactionStatsCollector() override {}

  void OnCompactionCompleted(DB* /* db */,
                             const CompactionJobInfo& info) override {
    int k = static_cast<int>(info.compaction_reason);
    int num_of_reasons = static_cast<int>(CompactionReason::kNumOfReasons);
    assert(k >= 0 && k < num_of_reasons);
    compaction_completed_[k]++;
  }

  void OnExternalFileIngested(
      DB* /* db */, const ExternalFileIngestionInfo& /* info */) override {
    int k = static_cast<int>(CompactionReason::kExternalSstIngestion);
    compaction_completed_[k]++;
  }

  void OnFlushCompleted(DB* /* db */, const FlushJobInfo& /* info */) override {
    int k = static_cast<int>(CompactionReason::kFlush);
    compaction_completed_[k]++;
  }

  int NumberOfCompactions(CompactionReason reason) const {
    int num_of_reasons = static_cast<int>(CompactionReason::kNumOfReasons);
    int k = static_cast<int>(reason);
    assert(k >= 0 && k < num_of_reasons);
    return compaction_completed_.at(k).load();
  }

private:
  std::vector<std::atomic<int>> compaction_completed_;
};

class SstStatsCollector : public EventListener {
 public:
  SstStatsCollector() : num_ssts_creation_started_(0) {}

  void OnTableFileCreationStarted(
      const TableFileCreationBriefInfo& /* info */) override {
    ++num_ssts_creation_started_;
  }

  int num_ssts_creation_started() { return num_ssts_creation_started_; }

 private:
  std::atomic<int> num_ssts_creation_started_;
};

static const int kCDTValueSize = 1000;
static const int kCDTKeysPerBuffer = 4;
static const int kCDTNumLevels = 8;
Options DeletionTriggerOptions(Options options) {
  options.compression = kNoCompression;
  options.write_buffer_size = kCDTKeysPerBuffer * (kCDTValueSize + 24);
  options.min_write_buffer_number_to_merge = 1;
  options.max_write_buffer_size_to_maintain = 0;
  options.num_levels = kCDTNumLevels;
  options.level0_file_num_compaction_trigger = 1;
  options.target_file_size_base = options.write_buffer_size * 2;
  options.target_file_size_multiplier = 2;
  options.max_bytes_for_level_base =
      options.target_file_size_base * options.target_file_size_multiplier;
  options.max_bytes_for_level_multiplier = 2;
  options.disable_auto_compactions = false;
  options.compaction_options_universal.max_size_amplification_percent = 100;
  return options;
}

bool HaveOverlappingKeyRanges(
    const Comparator* c,
    const SstFileMetaData& a, const SstFileMetaData& b) {
  if (c->CompareWithoutTimestamp(a.smallestkey, b.smallestkey) >= 0) {
    if (c->CompareWithoutTimestamp(a.smallestkey, b.largestkey) <= 0) {
      // b.smallestkey <= a.smallestkey <= b.largestkey
      return true;
    }
  } else if (c->CompareWithoutTimestamp(a.largestkey, b.smallestkey) >= 0) {
    // a.smallestkey < b.smallestkey <= a.largestkey
    return true;
  }
  if (c->CompareWithoutTimestamp(a.largestkey, b.largestkey) <= 0) {
    if (c->CompareWithoutTimestamp(a.largestkey, b.smallestkey) >= 0) {
      // b.smallestkey <= a.largestkey <= b.largestkey
      return true;
    }
  } else if (c->CompareWithoutTimestamp(a.smallestkey, b.largestkey) <= 0) {
    // a.smallestkey <= b.largestkey < a.largestkey
    return true;
  }
  return false;
}

// Identifies all files between level "min_level" and "max_level"
// which has overlapping key range with "input_file_meta".
void GetOverlappingFileNumbersForLevelCompaction(
    const ColumnFamilyMetaData& cf_meta,
    const Comparator* comparator,
    int min_level, int max_level,
    const SstFileMetaData* input_file_meta,
    std::set<std::string>* overlapping_file_names) {
  std::set<const SstFileMetaData*> overlapping_files;
  overlapping_files.insert(input_file_meta);
  for (int m = min_level; m <= max_level; ++m) {
    for (auto& file : cf_meta.levels[m].files) {
      for (auto* included_file : overlapping_files) {
        if (HaveOverlappingKeyRanges(
                comparator, *included_file, file)) {
          overlapping_files.insert(&file);
          overlapping_file_names->insert(file.name);
          break;
        }
      }
    }
  }
}

void VerifyCompactionResult(
    const ColumnFamilyMetaData& cf_meta,
    const std::set<std::string>& overlapping_file_numbers) {
#ifndef NDEBUG
  for (auto& level : cf_meta.levels) {
    for (auto& file : level.files) {
      assert(overlapping_file_numbers.find(file.name) ==
             overlapping_file_numbers.end());
    }
  }
#endif
}

/*
 * Verifies compaction stats of cfd are valid.
 *
 * For each level of cfd, its compaction stats are valid if
 * 1) sum(stat.counts) == stat.count, and
 * 2) stat.counts[i] == collector.NumberOfCompactions(i)
 */
void VerifyCompactionStats(ColumnFamilyData& cfd,
    const CompactionStatsCollector& collector) {
#ifndef NDEBUG
  InternalStats* internal_stats_ptr = cfd.internal_stats();
  ASSERT_NE(internal_stats_ptr, nullptr);
  const std::vector<InternalStats::CompactionStats>& comp_stats =
      internal_stats_ptr->TEST_GetCompactionStats();
  const int num_of_reasons = static_cast<int>(CompactionReason::kNumOfReasons);
  std::vector<int> counts(num_of_reasons, 0);
  // Count the number of compactions caused by each CompactionReason across
  // all levels.
  for (const auto& stat : comp_stats) {
    int sum = 0;
    for (int i = 0; i < num_of_reasons; i++) {
      counts[i] += stat.counts[i];
      sum += stat.counts[i];
    }
    ASSERT_EQ(sum, stat.count);
  }
  // Verify InternalStats bookkeeping matches that of CompactionStatsCollector,
  // assuming that all compactions complete.
  for (int i = 0; i < num_of_reasons; i++) {
    ASSERT_EQ(collector.NumberOfCompactions(static_cast<CompactionReason>(i)), counts[i]);
  }
#endif /* NDEBUG */
}

const SstFileMetaData* PickFileRandomly(
    const ColumnFamilyMetaData& cf_meta,
    Random* rand,
    int* level = nullptr) {
  auto file_id = rand->Uniform(static_cast<int>(
      cf_meta.file_count)) + 1;
  for (auto& level_meta : cf_meta.levels) {
    if (file_id <= level_meta.files.size()) {
      if (level != nullptr) {
        *level = level_meta.level;
      }
      auto result = rand->Uniform(file_id);
      return &(level_meta.files[result]);
    }
    file_id -= static_cast<uint32_t>(level_meta.files.size());
  }
  assert(false);
  return nullptr;
}
}  // anonymous namespace

#ifndef ROCKSDB_VALGRIND_RUN
// All the TEST_P tests run once with sub_compactions disabled (i.e.
// options.max_subcompactions = 1) and once with it enabled
TEST_P(DBCompactionTestWithParam, CompactionDeletionTrigger) {
  for (int tid = 0; tid < 3; ++tid) {
    uint64_t db_size[2];
    Options options = DeletionTriggerOptions(CurrentOptions());
    options.max_subcompactions = max_subcompactions_;

    if (tid == 1) {
      // the following only disable stats update in DB::Open()
      // and should not affect the result of this test.
      options.skip_stats_update_on_db_open = true;
    } else if (tid == 2) {
      // third pass with universal compaction
      options.compaction_style = kCompactionStyleUniversal;
      options.num_levels = 1;
    }

    DestroyAndReopen(options);
    Random rnd(301);

    const int kTestSize = kCDTKeysPerBuffer * 1024;
    std::vector<std::string> values;
    for (int k = 0; k < kTestSize; ++k) {
      values.push_back(rnd.RandomString(kCDTValueSize));
      ASSERT_OK(Put(Key(k), values[k]));
    }
    ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());
    ASSERT_OK(dbfull()->TEST_WaitForCompact());
    ASSERT_OK(Size(Key(0), Key(kTestSize - 1), &db_size[0]));

    for (int k = 0; k < kTestSize; ++k) {
      ASSERT_OK(Delete(Key(k)));
    }
    ASSERT_OK(Flush());
    ASSERT_OK(dbfull()->TEST_WaitForCompact());
    ASSERT_OK(Size(Key(0), Key(kTestSize - 1), &db_size[1]));

    if (options.compaction_style == kCompactionStyleUniversal) {
      // Claim: in universal compaction none of the original data will remain
      // once compactions settle.
      //
      // Proof: The compensated size of the file containing the most tombstones
      // is enough on its own to trigger size amp compaction. Size amp
      // compaction is a full compaction, so all tombstones meet the obsolete
      // keys they cover.
      ASSERT_EQ(0, db_size[1]);
    } else {
      // Claim: in level compaction at most `db_size[0] / 2` of the original
      // data will remain once compactions settle.
      //
      // Proof: Assume the original data is all in the bottom level. If it were
      // not, it would meet its tombstone sooner. The original data size is
      // large enough to require fanout to bottom level to be greater than
      // `max_bytes_for_level_multiplier == 2`. In the level just above,
      // tombstones must cover less than `db_size[0] / 4` bytes since fanout >=
      // 2 and file size is compensated by doubling the size of values we expect
      // are covered (`kDeletionWeightOnCompaction == 2`). The tombstones in
      // levels above must cover less than `db_size[0] / 8` bytes of original
      // data, `db_size[0] / 16`, and so on.
      ASSERT_GT(db_size[0] / 2, db_size[1]);
    }
  }
}
#endif  // ROCKSDB_VALGRIND_RUN

TEST_P(DBCompactionTestWithParam, CompactionsPreserveDeletes) {
  //  For each options type we test following
  //  - Enable preserve_deletes
  //  - write bunch of keys and deletes
  //  - Set start_seqnum to the beginning; compact; check that keys are present
  //  - rewind start_seqnum way forward; compact; check that keys are gone

  for (int tid = 0; tid < 3; ++tid) {
    Options options = DeletionTriggerOptions(CurrentOptions());
    options.max_subcompactions = max_subcompactions_;
    options.preserve_deletes=true;
    options.num_levels = 2;

    if (tid == 1) {
      options.skip_stats_update_on_db_open = true;
    } else if (tid == 2) {
      // third pass with universal compaction
      options.compaction_style = kCompactionStyleUniversal;
    }

    DestroyAndReopen(options);
    Random rnd(301);
    // highlight the default; all deletes should be preserved
    SetPreserveDeletesSequenceNumber(0);

    const int kTestSize = kCDTKeysPerBuffer;
    std::vector<std::string> values;
    for (int k = 0; k < kTestSize; ++k) {
      values.push_back(rnd.RandomString(kCDTValueSize));
      ASSERT_OK(Put(Key(k), values[k]));
    }

    for (int k = 0; k < kTestSize; ++k) {
      ASSERT_OK(Delete(Key(k)));
    }
    // to ensure we tackle all tombstones
    CompactRangeOptions cro;
    cro.change_level = true;
    cro.target_level = 2;
    cro.bottommost_level_compaction =
        BottommostLevelCompaction::kForceOptimized;

    ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());
    ASSERT_TRUE(
        dbfull()->CompactRange(cro, nullptr, nullptr).IsInvalidArgument());

    // check that normal user iterator doesn't see anything
    Iterator* db_iter = dbfull()->NewIterator(ReadOptions());
    int i = 0;
    for (db_iter->SeekToFirst(); db_iter->Valid(); db_iter->Next()) {
      i++;
    }
    ASSERT_OK(db_iter->status());
    ASSERT_EQ(i, 0);
    delete db_iter;

    // check that iterator that sees internal keys sees tombstones
    ReadOptions ro;
    ro.iter_start_seqnum=1;
    db_iter = dbfull()->NewIterator(ro);
    ASSERT_OK(db_iter->status());
    i = 0;
    for (db_iter->SeekToFirst(); db_iter->Valid(); db_iter->Next()) {
      i++;
    }
    ASSERT_EQ(i, 4);
    delete db_iter;

    // now all deletes should be gone
    SetPreserveDeletesSequenceNumber(100000000);
    ASSERT_NOK(dbfull()->CompactRange(cro, nullptr, nullptr));

    db_iter = dbfull()->NewIterator(ro);
    ASSERT_TRUE(db_iter->status().IsInvalidArgument());
    i = 0;
    for (db_iter->SeekToFirst(); db_iter->Valid(); db_iter->Next()) {
      i++;
    }
    ASSERT_EQ(i, 0);
    delete db_iter;
  }
}

TEST_F(DBCompactionTest, SkipStatsUpdateTest) {
  // This test verify UpdateAccumulatedStats is not on
  // if options.skip_stats_update_on_db_open = true
  // The test will need to be updated if the internal behavior changes.

  Options options = DeletionTriggerOptions(CurrentOptions());
  options.disable_auto_compactions = true;
  options.env = env_;
  DestroyAndReopen(options);
  Random rnd(301);

  const int kTestSize = kCDTKeysPerBuffer * 512;
  std::vector<std::string> values;
  for (int k = 0; k < kTestSize; ++k) {
    values.push_back(rnd.RandomString(kCDTValueSize));
    ASSERT_OK(Put(Key(k), values[k]));
  }

  ASSERT_OK(Flush());

  Close();

  int update_acc_stats_called = 0;
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "VersionStorageInfo::UpdateAccumulatedStats",
      [&](void* /* arg */) { ++update_acc_stats_called; });
  SyncPoint::GetInstance()->EnableProcessing();

  // Reopen the DB with stats-update disabled
  options.skip_stats_update_on_db_open = true;
  options.max_open_files = 20;
  Reopen(options);

  ASSERT_EQ(update_acc_stats_called, 0);

  // Repeat the reopen process, but this time we enable
  // stats-update.
  options.skip_stats_update_on_db_open = false;
  Reopen(options);

  ASSERT_GT(update_acc_stats_called, 0);

  SyncPoint::GetInstance()->ClearAllCallBacks();
  SyncPoint::GetInstance()->DisableProcessing();
}

TEST_F(DBCompactionTest, TestTableReaderForCompaction) {
  Options options = CurrentOptions();
  options.env = env_;
  options.new_table_reader_for_compaction_inputs = true;
  options.max_open_files = 20;
  options.level0_file_num_compaction_trigger = 3;
  // Avoid many shards with small max_open_files, where as little as
  // two table insertions could lead to an LRU eviction, depending on
  // hash values.
  options.table_cache_numshardbits = 2;
  DestroyAndReopen(options);
  Random rnd(301);

  int num_table_cache_lookup = 0;
  int num_new_table_reader = 0;
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "TableCache::FindTable:0", [&](void* arg) {
        assert(arg != nullptr);
        bool no_io = *(reinterpret_cast<bool*>(arg));
        if (!no_io) {
          // filter out cases for table properties queries.
          num_table_cache_lookup++;
        }
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "TableCache::GetTableReader:0",
      [&](void* /*arg*/) { num_new_table_reader++; });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  for (int k = 0; k < options.level0_file_num_compaction_trigger; ++k) {
    ASSERT_OK(Put(Key(k), Key(k)));
    ASSERT_OK(Put(Key(10 - k), "bar"));
    if (k < options.level0_file_num_compaction_trigger - 1) {
      num_table_cache_lookup = 0;
      ASSERT_OK(Flush());
      ASSERT_OK(dbfull()->TEST_WaitForCompact());
      // preloading iterator issues one table cache lookup and create
      // a new table reader, if not preloaded.
      int old_num_table_cache_lookup = num_table_cache_lookup;
      ASSERT_GE(num_table_cache_lookup, 1);
      ASSERT_EQ(num_new_table_reader, 1);

      num_table_cache_lookup = 0;
      num_new_table_reader = 0;
      ASSERT_EQ(Key(k), Get(Key(k)));
      // lookup iterator from table cache and no need to create a new one.
      ASSERT_EQ(old_num_table_cache_lookup + num_table_cache_lookup, 2);
      ASSERT_EQ(num_new_table_reader, 0);
    }
  }

  num_table_cache_lookup = 0;
  num_new_table_reader = 0;
  ASSERT_OK(Flush());
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  // Preloading iterator issues one table cache lookup and creates
  // a new table reader. One file is created for flush and one for compaction.
  // Compaction inputs make no table cache look-up for data/range deletion
  // iterators
  // May preload table cache too.
  ASSERT_GE(num_table_cache_lookup, 2);
  int old_num_table_cache_lookup2 = num_table_cache_lookup;

  // Create new iterator for:
  // (1) 1 for verifying flush results
  // (2) 1 for verifying compaction results.
  // (3) New TableReaders will not be created for compaction inputs
  ASSERT_EQ(num_new_table_reader, 2);

  num_table_cache_lookup = 0;
  num_new_table_reader = 0;
  ASSERT_EQ(Key(1), Get(Key(1)));
  ASSERT_EQ(num_table_cache_lookup + old_num_table_cache_lookup2, 5);
  ASSERT_EQ(num_new_table_reader, 0);

  num_table_cache_lookup = 0;
  num_new_table_reader = 0;
  CompactRangeOptions cro;
  cro.change_level = true;
  cro.target_level = 2;
  cro.bottommost_level_compaction = BottommostLevelCompaction::kForceOptimized;
  ASSERT_OK(db_->CompactRange(cro, nullptr, nullptr));
  // Only verifying compaction outputs issues one table cache lookup
  // for both data block and range deletion block).
  // May preload table cache too.
  ASSERT_GE(num_table_cache_lookup, 1);
  old_num_table_cache_lookup2 = num_table_cache_lookup;
  // One for verifying compaction results.
  // No new iterator created for compaction.
  ASSERT_EQ(num_new_table_reader, 1);

  num_table_cache_lookup = 0;
  num_new_table_reader = 0;
  ASSERT_EQ(Key(1), Get(Key(1)));
  ASSERT_EQ(num_table_cache_lookup + old_num_table_cache_lookup2, 3);
  ASSERT_EQ(num_new_table_reader, 0);

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearAllCallBacks();
}

TEST_P(DBCompactionTestWithParam, CompactionDeletionTriggerReopen) {
  for (int tid = 0; tid < 2; ++tid) {
    uint64_t db_size[3];
    Options options = DeletionTriggerOptions(CurrentOptions());
    options.max_subcompactions = max_subcompactions_;

    if (tid == 1) {
      // second pass with universal compaction
      options.compaction_style = kCompactionStyleUniversal;
      options.num_levels = 1;
    }

    DestroyAndReopen(options);
    Random rnd(301);

    // round 1 --- insert key/value pairs.
    const int kTestSize = kCDTKeysPerBuffer * 512;
    std::vector<std::string> values;
    for (int k = 0; k < kTestSize; ++k) {
      values.push_back(rnd.RandomString(kCDTValueSize));
      ASSERT_OK(Put(Key(k), values[k]));
    }
    ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());
    ASSERT_OK(dbfull()->TEST_WaitForCompact());
    ASSERT_OK(Size(Key(0), Key(kTestSize - 1), &db_size[0]));
    Close();

    // round 2 --- disable auto-compactions and issue deletions.
    options.create_if_missing = false;
    options.disable_auto_compactions = true;
    Reopen(options);

    for (int k = 0; k < kTestSize; ++k) {
      ASSERT_OK(Delete(Key(k)));
    }
    ASSERT_OK(Size(Key(0), Key(kTestSize - 1), &db_size[1]));
    Close();
    // as auto_compaction is off, we shouldn't see any reduction in db size.
    ASSERT_LE(db_size[0], db_size[1]);

    // round 3 --- reopen db with auto_compaction on and see if
    // deletion compensation still work.
    options.disable_auto_compactions = false;
    Reopen(options);
    // insert relatively small amount of data to trigger auto compaction.
    for (int k = 0; k < kTestSize / 10; ++k) {
      ASSERT_OK(Put(Key(k), values[k]));
    }
    ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());
    ASSERT_OK(dbfull()->TEST_WaitForCompact());
    ASSERT_OK(Size(Key(0), Key(kTestSize - 1), &db_size[2]));
    // this time we're expecting significant drop in size.
    //
    // See "CompactionDeletionTrigger" test for proof that at most
    // `db_size[0] / 2` of the original data remains. In addition to that, this
    // test inserts `db_size[0] / 10` to push the tombstones into SST files and
    // then through automatic compactions. So in total `3 * db_size[0] / 5` of
    // the original data may remain.
    ASSERT_GT(3 * db_size[0] / 5, db_size[2]);
  }
}

TEST_F(DBCompactionTest, CompactRangeBottomPri) {
  ASSERT_OK(Put(Key(50), ""));
  ASSERT_OK(Flush());
  ASSERT_OK(Put(Key(100), ""));
  ASSERT_OK(Flush());
  ASSERT_OK(Put(Key(200), ""));
  ASSERT_OK(Flush());

  {
    CompactRangeOptions cro;
    cro.change_level = true;
    cro.target_level = 2;
    ASSERT_OK(dbfull()->CompactRange(cro, nullptr, nullptr));
  }
  ASSERT_EQ("0,0,3", FilesPerLevel(0));

  ASSERT_OK(Put(Key(1), ""));
  ASSERT_OK(Put(Key(199), ""));
  ASSERT_OK(Flush());
  ASSERT_OK(Put(Key(2), ""));
  ASSERT_OK(Put(Key(199), ""));
  ASSERT_OK(Flush());
  ASSERT_EQ("2,0,3", FilesPerLevel(0));

  // Now we have 2 L0 files, and 3 L2 files, and a manual compaction will
  // be triggered.
  // Two compaction jobs will run. One compacts 2 L0 files in Low Pri Pool
  // and one compact to L2 in bottom pri pool.
  int low_pri_count = 0;
  int bottom_pri_count = 0;
  SyncPoint::GetInstance()->SetCallBack(
      "ThreadPoolImpl::Impl::BGThread:BeforeRun", [&](void* arg) {
        Env::Priority* pri = reinterpret_cast<Env::Priority*>(arg);
        // First time is low pri pool in the test case.
        if (low_pri_count == 0 && bottom_pri_count == 0) {
          ASSERT_EQ(Env::Priority::LOW, *pri);
        }
        if (*pri == Env::Priority::LOW) {
          low_pri_count++;
        } else {
          bottom_pri_count++;
        }
      });
  SyncPoint::GetInstance()->EnableProcessing();
  env_->SetBackgroundThreads(1, Env::Priority::BOTTOM);
  ASSERT_OK(dbfull()->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  ASSERT_EQ(1, low_pri_count);
  ASSERT_EQ(1, bottom_pri_count);
  ASSERT_EQ("0,0,2", FilesPerLevel(0));

  // Recompact bottom most level uses bottom pool
  CompactRangeOptions cro;
  cro.bottommost_level_compaction = BottommostLevelCompaction::kForce;
  ASSERT_OK(dbfull()->CompactRange(cro, nullptr, nullptr));
  ASSERT_EQ(1, low_pri_count);
  ASSERT_EQ(2, bottom_pri_count);

  env_->SetBackgroundThreads(0, Env::Priority::BOTTOM);
  ASSERT_OK(dbfull()->CompactRange(cro, nullptr, nullptr));
  // Low pri pool is used if bottom pool has size 0.
  ASSERT_EQ(2, low_pri_count);
  ASSERT_EQ(2, bottom_pri_count);

  SyncPoint::GetInstance()->DisableProcessing();
}

TEST_F(DBCompactionTest, DisableStatsUpdateReopen) {
  uint64_t db_size[3];
  for (int test = 0; test < 2; ++test) {
    Options options = DeletionTriggerOptions(CurrentOptions());
    options.skip_stats_update_on_db_open = (test == 0);

    env_->random_read_counter_.Reset();
    DestroyAndReopen(options);
    Random rnd(301);

    // round 1 --- insert key/value pairs.
    const int kTestSize = kCDTKeysPerBuffer * 512;
    std::vector<std::string> values;
    for (int k = 0; k < kTestSize; ++k) {
      values.push_back(rnd.RandomString(kCDTValueSize));
      ASSERT_OK(Put(Key(k), values[k]));
    }
    ASSERT_OK(Flush());
    ASSERT_OK(dbfull()->TEST_WaitForCompact());
    // L1 and L2 can fit deletions iff size compensation does not take effect,
    // i.e., when `skip_stats_update_on_db_open == true`. Move any remaining
    // files at or above L2 down to L3 to ensure obsolete data does not
    // accidentally meet its tombstone above L3. This makes the final size more
    // deterministic and easy to see whether size compensation for deletions
    // took effect.
    MoveFilesToLevel(3 /* level */);
    ASSERT_OK(Size(Key(0), Key(kTestSize - 1), &db_size[0]));
    Close();

    // round 2 --- disable auto-compactions and issue deletions.
    options.create_if_missing = false;
    options.disable_auto_compactions = true;

    env_->random_read_counter_.Reset();
    Reopen(options);

    for (int k = 0; k < kTestSize; ++k) {
      ASSERT_OK(Delete(Key(k)));
    }
    ASSERT_OK(Size(Key(0), Key(kTestSize - 1), &db_size[1]));
    Close();
    // as auto_compaction is off, we shouldn't see any reduction in db size.
    ASSERT_LE(db_size[0], db_size[1]);

    // round 3 --- reopen db with auto_compaction on and see if
    // deletion compensation still work.
    options.disable_auto_compactions = false;
    Reopen(options);
    ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());
    ASSERT_OK(dbfull()->TEST_WaitForCompact());
    ASSERT_OK(Size(Key(0), Key(kTestSize - 1), &db_size[2]));

    if (options.skip_stats_update_on_db_open) {
      // If update stats on DB::Open is disable, we don't expect
      // deletion entries taking effect.
      //
      // The deletions are small enough to fit in L1 and L2, and obsolete keys
      // were moved to L3+, so none of the original data should have been
      // dropped.
      ASSERT_LE(db_size[0], db_size[2]);
    } else {
      // Otherwise, we should see a significant drop in db size.
      //
      // See "CompactionDeletionTrigger" test for proof that at most
      // `db_size[0] / 2` of the original data remains.
      ASSERT_GT(db_size[0] / 2, db_size[2]);
    }
  }
}


TEST_P(DBCompactionTestWithParam, CompactionTrigger) {
  const int kNumKeysPerFile = 100;

  Options options = CurrentOptions();
  options.write_buffer_size = 110 << 10;  // 110KB
  options.arena_block_size = 4 << 10;
  options.num_levels = 3;
  options.level0_file_num_compaction_trigger = 3;
  options.max_subcompactions = max_subcompactions_;
  options.memtable_factory.reset(new SpecialSkipListFactory(kNumKeysPerFile));
  CreateAndReopenWithCF({"pikachu"}, options);

  Random rnd(301);

  for (int num = 0; num < options.level0_file_num_compaction_trigger - 1;
       num++) {
    std::vector<std::string> values;
    // Write 100KB (100 values, each 1K)
    for (int i = 0; i < kNumKeysPerFile; i++) {
      values.push_back(rnd.RandomString(990));
      ASSERT_OK(Put(1, Key(i), values[i]));
    }
    // put extra key to trigger flush
    ASSERT_OK(Put(1, "", ""));
    ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable(handles_[1]));
    ASSERT_EQ(NumTableFilesAtLevel(0, 1), num + 1);
  }

  // generate one more file in level-0, and should trigger level-0 compaction
  std::vector<std::string> values;
  for (int i = 0; i < kNumKeysPerFile; i++) {
    values.push_back(rnd.RandomString(990));
    ASSERT_OK(Put(1, Key(i), values[i]));
  }
  // put extra key to trigger flush
  ASSERT_OK(Put(1, "", ""));
  ASSERT_OK(dbfull()->TEST_WaitForCompact());

  ASSERT_EQ(NumTableFilesAtLevel(0, 1), 0);
  ASSERT_EQ(NumTableFilesAtLevel(1, 1), 1);
}

TEST_F(DBCompactionTest, BGCompactionsAllowed) {
  // Create several column families. Make compaction triggers in all of them
  // and see number of compactions scheduled to be less than allowed.
  const int kNumKeysPerFile = 100;

  Options options = CurrentOptions();
  options.write_buffer_size = 110 << 10;  // 110KB
  options.arena_block_size = 4 << 10;
  options.num_levels = 3;
  // Should speed up compaction when there are 4 files.
  options.level0_file_num_compaction_trigger = 2;
  options.level0_slowdown_writes_trigger = 20;
  options.soft_pending_compaction_bytes_limit = 1 << 30;  // Infinitely large
  options.max_background_compactions = 3;
  options.memtable_factory.reset(new SpecialSkipListFactory(kNumKeysPerFile));

  // Block all threads in thread pool.
  const size_t kTotalTasks = 4;
  env_->SetBackgroundThreads(4, Env::LOW);
  test::SleepingBackgroundTask sleeping_tasks[kTotalTasks];
  for (size_t i = 0; i < kTotalTasks; i++) {
    env_->Schedule(&test::SleepingBackgroundTask::DoSleepTask,
                   &sleeping_tasks[i], Env::Priority::LOW);
    sleeping_tasks[i].WaitUntilSleeping();
  }

  CreateAndReopenWithCF({"one", "two", "three"}, options);

  Random rnd(301);
  for (int cf = 0; cf < 4; cf++) {
    for (int num = 0; num < options.level0_file_num_compaction_trigger; num++) {
      for (int i = 0; i < kNumKeysPerFile; i++) {
        ASSERT_OK(Put(cf, Key(i), ""));
      }
      // put extra key to trigger flush
      ASSERT_OK(Put(cf, "", ""));
      ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable(handles_[cf]));
      ASSERT_EQ(NumTableFilesAtLevel(0, cf), num + 1);
    }
  }

  // Now all column families qualify compaction but only one should be
  // scheduled, because no column family hits speed up condition.
  ASSERT_EQ(1u, env_->GetThreadPoolQueueLen(Env::Priority::LOW));

  // Create two more files for one column family, which triggers speed up
  // condition, three compactions will be scheduled.
  for (int num = 0; num < options.level0_file_num_compaction_trigger; num++) {
    for (int i = 0; i < kNumKeysPerFile; i++) {
      ASSERT_OK(Put(2, Key(i), ""));
    }
    // put extra key to trigger flush
    ASSERT_OK(Put(2, "", ""));
    ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable(handles_[2]));
    ASSERT_EQ(options.level0_file_num_compaction_trigger + num + 1,
              NumTableFilesAtLevel(0, 2));
  }
  ASSERT_EQ(3U, env_->GetThreadPoolQueueLen(Env::Priority::LOW));

  // Unblock all threads to unblock all compactions.
  for (size_t i = 0; i < kTotalTasks; i++) {
    sleeping_tasks[i].WakeUp();
    sleeping_tasks[i].WaitUntilDone();
  }
  ASSERT_OK(dbfull()->TEST_WaitForCompact());

  // Verify number of compactions allowed will come back to 1.

  for (size_t i = 0; i < kTotalTasks; i++) {
    sleeping_tasks[i].Reset();
    env_->Schedule(&test::SleepingBackgroundTask::DoSleepTask,
                   &sleeping_tasks[i], Env::Priority::LOW);
    sleeping_tasks[i].WaitUntilSleeping();
  }
  for (int cf = 0; cf < 4; cf++) {
    for (int num = 0; num < options.level0_file_num_compaction_trigger; num++) {
      for (int i = 0; i < kNumKeysPerFile; i++) {
        ASSERT_OK(Put(cf, Key(i), ""));
      }
      // put extra key to trigger flush
      ASSERT_OK(Put(cf, "", ""));
      ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable(handles_[cf]));
      ASSERT_EQ(NumTableFilesAtLevel(0, cf), num + 1);
    }
  }

  // Now all column families qualify compaction but only one should be
  // scheduled, because no column family hits speed up condition.
  ASSERT_EQ(1U, env_->GetThreadPoolQueueLen(Env::Priority::LOW));

  for (size_t i = 0; i < kTotalTasks; i++) {
    sleeping_tasks[i].WakeUp();
    sleeping_tasks[i].WaitUntilDone();
  }
}

TEST_P(DBCompactionTestWithParam, CompactionsGenerateMultipleFiles) {
  Options options = CurrentOptions();
  options.write_buffer_size = 100000000;        // Large write buffer
  options.max_subcompactions = max_subcompactions_;
  CreateAndReopenWithCF({"pikachu"}, options);

  Random rnd(301);

  // Write 8MB (80 values, each 100K)
  ASSERT_EQ(NumTableFilesAtLevel(0, 1), 0);
  std::vector<std::string> values;
  for (int i = 0; i < 80; i++) {
    values.push_back(rnd.RandomString(100000));
    ASSERT_OK(Put(1, Key(i), values[i]));
  }

  // Reopening moves updates to level-0
  ReopenWithColumnFamilies({"default", "pikachu"}, options);
  ASSERT_OK(dbfull()->TEST_CompactRange(0, nullptr, nullptr, handles_[1],
                                        true /* disallow trivial move */));

  ASSERT_EQ(NumTableFilesAtLevel(0, 1), 0);
  ASSERT_GT(NumTableFilesAtLevel(1, 1), 1);
  for (int i = 0; i < 80; i++) {
    ASSERT_EQ(Get(1, Key(i)), values[i]);
  }
}

TEST_F(DBCompactionTest, MinorCompactionsHappen) {
  do {
    Options options = CurrentOptions();
    options.write_buffer_size = 10000;
    CreateAndReopenWithCF({"pikachu"}, options);

    const int N = 500;

    int starting_num_tables = TotalTableFiles(1);
    for (int i = 0; i < N; i++) {
      ASSERT_OK(Put(1, Key(i), Key(i) + std::string(1000, 'v')));
    }
    int ending_num_tables = TotalTableFiles(1);
    ASSERT_GT(ending_num_tables, starting_num_tables);

    for (int i = 0; i < N; i++) {
      ASSERT_EQ(Key(i) + std::string(1000, 'v'), Get(1, Key(i)));
    }

    ReopenWithColumnFamilies({"default", "pikachu"}, options);

    for (int i = 0; i < N; i++) {
      ASSERT_EQ(Key(i) + std::string(1000, 'v'), Get(1, Key(i)));
    }
  } while (ChangeCompactOptions());
}

TEST_F(DBCompactionTest, UserKeyCrossFile1) {
  Options options = CurrentOptions();
  options.compaction_style = kCompactionStyleLevel;
  options.level0_file_num_compaction_trigger = 3;

  DestroyAndReopen(options);

  // create first file and flush to l0
  ASSERT_OK(Put("4", "A"));
  ASSERT_OK(Put("3", "A"));
  ASSERT_OK(Flush());
  ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());

  ASSERT_OK(Put("2", "A"));
  ASSERT_OK(Delete("3"));
  ASSERT_OK(Flush());
  ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());
  ASSERT_EQ("NOT_FOUND", Get("3"));

  // move both files down to l1
  ASSERT_OK(dbfull()->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  ASSERT_EQ("NOT_FOUND", Get("3"));

  for (int i = 0; i < 3; i++) {
    ASSERT_OK(Put("2", "B"));
    ASSERT_OK(Flush());
    ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());
  }
  ASSERT_OK(dbfull()->TEST_WaitForCompact());

  ASSERT_EQ("NOT_FOUND", Get("3"));
}

TEST_F(DBCompactionTest, UserKeyCrossFile2) {
  Options options = CurrentOptions();
  options.compaction_style = kCompactionStyleLevel;
  options.level0_file_num_compaction_trigger = 3;

  DestroyAndReopen(options);

  // create first file and flush to l0
  ASSERT_OK(Put("4", "A"));
  ASSERT_OK(Put("3", "A"));
  ASSERT_OK(Flush());
  ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());

  ASSERT_OK(Put("2", "A"));
  ASSERT_OK(SingleDelete("3"));
  ASSERT_OK(Flush());
  ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());
  ASSERT_EQ("NOT_FOUND", Get("3"));

  // move both files down to l1
  ASSERT_OK(dbfull()->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  ASSERT_EQ("NOT_FOUND", Get("3"));

  for (int i = 0; i < 3; i++) {
    ASSERT_OK(Put("2", "B"));
    ASSERT_OK(Flush());
    ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());
  }
  ASSERT_OK(dbfull()->TEST_WaitForCompact());

  ASSERT_EQ("NOT_FOUND", Get("3"));
}

TEST_F(DBCompactionTest, CompactionSstPartitioner) {
  Options options = CurrentOptions();
  options.compaction_style = kCompactionStyleLevel;
  options.level0_file_num_compaction_trigger = 3;
  std::shared_ptr<SstPartitionerFactory> factory(
      NewSstPartitionerFixedPrefixFactory(4));
  options.sst_partitioner_factory = factory;

  DestroyAndReopen(options);

  // create first file and flush to l0
  ASSERT_OK(Put("aaaa1", "A"));
  ASSERT_OK(Put("bbbb1", "B"));
  ASSERT_OK(Flush());
  ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());

  ASSERT_OK(Put("aaaa1", "A2"));
  ASSERT_OK(Flush());
  ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());

  // move both files down to l1
  ASSERT_OK(dbfull()->CompactRange(CompactRangeOptions(), nullptr, nullptr));

  std::vector<LiveFileMetaData> files;
  dbfull()->GetLiveFilesMetaData(&files);
  ASSERT_EQ(2, files.size());
  ASSERT_EQ("A2", Get("aaaa1"));
  ASSERT_EQ("B", Get("bbbb1"));
}

TEST_F(DBCompactionTest, CompactionSstPartitionerNonTrivial) {
  Options options = CurrentOptions();
  options.compaction_style = kCompactionStyleLevel;
  options.level0_file_num_compaction_trigger = 1;
  std::shared_ptr<SstPartitionerFactory> factory(
      NewSstPartitionerFixedPrefixFactory(4));
  options.sst_partitioner_factory = factory;

  DestroyAndReopen(options);

  // create first file and flush to l0
  ASSERT_OK(Put("aaaa1", "A"));
  ASSERT_OK(Put("bbbb1", "B"));
  ASSERT_OK(Flush());
  ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());
  ASSERT_OK(dbfull()->TEST_WaitForCompact(true));

  std::vector<LiveFileMetaData> files;
  dbfull()->GetLiveFilesMetaData(&files);
  ASSERT_EQ(2, files.size());
  ASSERT_EQ("A", Get("aaaa1"));
  ASSERT_EQ("B", Get("bbbb1"));
}

TEST_F(DBCompactionTest, ZeroSeqIdCompaction) {
  Options options = CurrentOptions();
  options.compaction_style = kCompactionStyleLevel;
  options.level0_file_num_compaction_trigger = 3;

  FlushedFileCollector* collector = new FlushedFileCollector();
  options.listeners.emplace_back(collector);

  // compaction options
  CompactionOptions compact_opt;
  compact_opt.compression = kNoCompression;
  compact_opt.output_file_size_limit = 4096;
  const size_t key_len =
    static_cast<size_t>(compact_opt.output_file_size_limit) / 5;

  DestroyAndReopen(options);

  std::vector<const Snapshot*> snaps;

  // create first file and flush to l0
  for (auto& key : {"1", "2", "3", "3", "3", "3"}) {
    ASSERT_OK(Put(key, std::string(key_len, 'A')));
    snaps.push_back(dbfull()->GetSnapshot());
  }
  ASSERT_OK(Flush());
  ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());

  // create second file and flush to l0
  for (auto& key : {"3", "4", "5", "6", "7", "8"}) {
    ASSERT_OK(Put(key, std::string(key_len, 'A')));
    snaps.push_back(dbfull()->GetSnapshot());
  }
  ASSERT_OK(Flush());
  ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());

  // move both files down to l1
  ASSERT_OK(
      dbfull()->CompactFiles(compact_opt, collector->GetFlushedFiles(), 1));

  // release snap so that first instance of key(3) can have seqId=0
  for (auto snap : snaps) {
    dbfull()->ReleaseSnapshot(snap);
  }

  // create 3 files in l0 so to trigger compaction
  for (int i = 0; i < options.level0_file_num_compaction_trigger; i++) {
    ASSERT_OK(Put("2", std::string(1, 'A')));
    ASSERT_OK(Flush());
    ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());
  }

  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  ASSERT_OK(Put("", ""));
}

TEST_F(DBCompactionTest, ManualCompactionUnknownOutputSize) {
  // github issue #2249
  Options options = CurrentOptions();
  options.compaction_style = kCompactionStyleLevel;
  options.level0_file_num_compaction_trigger = 3;
  DestroyAndReopen(options);

  // create two files in l1 that we can compact
  for (int i = 0; i < 2; ++i) {
    for (int j = 0; j < options.level0_file_num_compaction_trigger; j++) {
      // make l0 files' ranges overlap to avoid trivial move
      ASSERT_OK(Put(std::to_string(2 * i), std::string(1, 'A')));
      ASSERT_OK(Put(std::to_string(2 * i + 1), std::string(1, 'A')));
      ASSERT_OK(Flush());
      ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());
    }
    ASSERT_OK(dbfull()->TEST_WaitForCompact());
    ASSERT_EQ(NumTableFilesAtLevel(0, 0), 0);
    ASSERT_EQ(NumTableFilesAtLevel(1, 0), i + 1);
  }

  ColumnFamilyMetaData cf_meta;
  dbfull()->GetColumnFamilyMetaData(dbfull()->DefaultColumnFamily(), &cf_meta);
  ASSERT_EQ(2, cf_meta.levels[1].files.size());
  std::vector<std::string> input_filenames;
  for (const auto& sst_file : cf_meta.levels[1].files) {
    input_filenames.push_back(sst_file.name);
  }

  // note CompactionOptions::output_file_size_limit is unset.
  CompactionOptions compact_opt;
  compact_opt.compression = kNoCompression;
  ASSERT_OK(dbfull()->CompactFiles(compact_opt, input_filenames, 1));
}

// Check that writes done during a memtable compaction are recovered
// if the database is shutdown during the memtable compaction.
TEST_F(DBCompactionTest, RecoverDuringMemtableCompaction) {
  do {
    Options options = CurrentOptions();
    options.env = env_;
    CreateAndReopenWithCF({"pikachu"}, options);

    // Trigger a long memtable compaction and reopen the database during it
    ASSERT_OK(Put(1, "foo", "v1"));  // Goes to 1st log file
    ASSERT_OK(Put(1, "big1", std::string(10000000, 'x')));  // Fills memtable
    ASSERT_OK(Put(1, "big2", std::string(1000, 'y')));  // Triggers compaction
    ASSERT_OK(Put(1, "bar", "v2"));                     // Goes to new log file

    ReopenWithColumnFamilies({"default", "pikachu"}, options);
    ASSERT_EQ("v1", Get(1, "foo"));
    ASSERT_EQ("v2", Get(1, "bar"));
    ASSERT_EQ(std::string(10000000, 'x'), Get(1, "big1"));
    ASSERT_EQ(std::string(1000, 'y'), Get(1, "big2"));
  } while (ChangeOptions());
}

TEST_P(DBCompactionTestWithParam, TrivialMoveOneFile) {
  int32_t trivial_move = 0;
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::BackgroundCompaction:TrivialMove",
      [&](void* /*arg*/) { trivial_move++; });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  Options options = CurrentOptions();
  options.write_buffer_size = 100000000;
  options.max_subcompactions = max_subcompactions_;
  DestroyAndReopen(options);

  int32_t num_keys = 80;
  int32_t value_size = 100 * 1024;  // 100 KB

  Random rnd(301);
  std::vector<std::string> values;
  for (int i = 0; i < num_keys; i++) {
    values.push_back(rnd.RandomString(value_size));
    ASSERT_OK(Put(Key(i), values[i]));
  }

  // Reopening moves updates to L0
  Reopen(options);
  ASSERT_EQ(NumTableFilesAtLevel(0, 0), 1);  // 1 file in L0
  ASSERT_EQ(NumTableFilesAtLevel(1, 0), 0);  // 0 files in L1

  std::vector<LiveFileMetaData> metadata;
  db_->GetLiveFilesMetaData(&metadata);
  ASSERT_EQ(metadata.size(), 1U);
  LiveFileMetaData level0_file = metadata[0];  // L0 file meta

  CompactRangeOptions cro;
  cro.exclusive_manual_compaction = exclusive_manual_compaction_;

  // Compaction will initiate a trivial move from L0 to L1
  ASSERT_OK(dbfull()->CompactRange(cro, nullptr, nullptr));

  // File moved From L0 to L1
  ASSERT_EQ(NumTableFilesAtLevel(0, 0), 0);  // 0 files in L0
  ASSERT_EQ(NumTableFilesAtLevel(1, 0), 1);  // 1 file in L1

  metadata.clear();
  db_->GetLiveFilesMetaData(&metadata);
  ASSERT_EQ(metadata.size(), 1U);
  ASSERT_EQ(metadata[0].name /* level1_file.name */, level0_file.name);
  ASSERT_EQ(metadata[0].size /* level1_file.size */, level0_file.size);

  for (int i = 0; i < num_keys; i++) {
    ASSERT_EQ(Get(Key(i)), values[i]);
  }

  ASSERT_EQ(trivial_move, 1);
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
}

TEST_P(DBCompactionTestWithParam, TrivialMoveNonOverlappingFiles) {
  int32_t trivial_move = 0;
  int32_t non_trivial_move = 0;
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::BackgroundCompaction:TrivialMove",
      [&](void* /*arg*/) { trivial_move++; });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::BackgroundCompaction:NonTrivial",
      [&](void* /*arg*/) { non_trivial_move++; });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  options.write_buffer_size = 10 * 1024 * 1024;
  options.max_subcompactions = max_subcompactions_;

  DestroyAndReopen(options);
  // non overlapping ranges
  std::vector<std::pair<int32_t, int32_t>> ranges = {
    {100, 199},
    {300, 399},
    {0, 99},
    {200, 299},
    {600, 699},
    {400, 499},
    {500, 550},
    {551, 599},
  };
  int32_t value_size = 10 * 1024;  // 10 KB

  Random rnd(301);
  std::map<int32_t, std::string> values;
  for (size_t i = 0; i < ranges.size(); i++) {
    for (int32_t j = ranges[i].first; j <= ranges[i].second; j++) {
      values[j] = rnd.RandomString(value_size);
      ASSERT_OK(Put(Key(j), values[j]));
    }
    ASSERT_OK(Flush());
  }

  int32_t level0_files = NumTableFilesAtLevel(0, 0);
  ASSERT_EQ(level0_files, ranges.size());    // Multiple files in L0
  ASSERT_EQ(NumTableFilesAtLevel(1, 0), 0);  // No files in L1

  CompactRangeOptions cro;
  cro.exclusive_manual_compaction = exclusive_manual_compaction_;

  // Since data is non-overlapping we expect compaction to initiate
  // a trivial move
  ASSERT_OK(db_->CompactRange(cro, nullptr, nullptr));
  // We expect that all the files were trivially moved from L0 to L1
  ASSERT_EQ(NumTableFilesAtLevel(0, 0), 0);
  ASSERT_EQ(NumTableFilesAtLevel(1, 0) /* level1_files */, level0_files);

  for (size_t i = 0; i < ranges.size(); i++) {
    for (int32_t j = ranges[i].first; j <= ranges[i].second; j++) {
      ASSERT_EQ(Get(Key(j)), values[j]);
    }
  }

  ASSERT_EQ(trivial_move, 1);
  ASSERT_EQ(non_trivial_move, 0);

  trivial_move = 0;
  non_trivial_move = 0;
  values.clear();
  DestroyAndReopen(options);
  // Same ranges as above but overlapping
  ranges = {
    {100, 199},
    {300, 399},
    {0, 99},
    {200, 299},
    {600, 699},
    {400, 499},
    {500, 560},  // this range overlap with the next one
    {551, 599},
  };
  for (size_t i = 0; i < ranges.size(); i++) {
    for (int32_t j = ranges[i].first; j <= ranges[i].second; j++) {
      values[j] = rnd.RandomString(value_size);
      ASSERT_OK(Put(Key(j), values[j]));
    }
    ASSERT_OK(Flush());
  }

  ASSERT_OK(db_->CompactRange(cro, nullptr, nullptr));

  for (size_t i = 0; i < ranges.size(); i++) {
    for (int32_t j = ranges[i].first; j <= ranges[i].second; j++) {
      ASSERT_EQ(Get(Key(j)), values[j]);
    }
  }
  ASSERT_EQ(trivial_move, 0);
  ASSERT_EQ(non_trivial_move, 1);

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
}

TEST_P(DBCompactionTestWithParam, TrivialMoveTargetLevel) {
  int32_t trivial_move = 0;
  int32_t non_trivial_move = 0;
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::BackgroundCompaction:TrivialMove",
      [&](void* /*arg*/) { trivial_move++; });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::BackgroundCompaction:NonTrivial",
      [&](void* /*arg*/) { non_trivial_move++; });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  options.write_buffer_size = 10 * 1024 * 1024;
  options.num_levels = 7;
  options.max_subcompactions = max_subcompactions_;

  DestroyAndReopen(options);
  int32_t value_size = 10 * 1024;  // 10 KB

  // Add 2 non-overlapping files
  Random rnd(301);
  std::map<int32_t, std::string> values;

  // file 1 [0 => 300]
  for (int32_t i = 0; i <= 300; i++) {
    values[i] = rnd.RandomString(value_size);
    ASSERT_OK(Put(Key(i), values[i]));
  }
  ASSERT_OK(Flush());

  // file 2 [600 => 700]
  for (int32_t i = 600; i <= 700; i++) {
    values[i] = rnd.RandomString(value_size);
    ASSERT_OK(Put(Key(i), values[i]));
  }
  ASSERT_OK(Flush());

  // 2 files in L0
  ASSERT_EQ("2", FilesPerLevel(0));
  CompactRangeOptions compact_options;
  compact_options.change_level = true;
  compact_options.target_level = 6;
  compact_options.exclusive_manual_compaction = exclusive_manual_compaction_;
  ASSERT_OK(db_->CompactRange(compact_options, nullptr, nullptr));
  // 2 files in L6
  ASSERT_EQ("0,0,0,0,0,0,2", FilesPerLevel(0));

  ASSERT_EQ(trivial_move, 1);
  ASSERT_EQ(non_trivial_move, 0);

  for (int32_t i = 0; i <= 300; i++) {
    ASSERT_EQ(Get(Key(i)), values[i]);
  }
  for (int32_t i = 600; i <= 700; i++) {
    ASSERT_EQ(Get(Key(i)), values[i]);
  }
}

TEST_P(DBCompactionTestWithParam, ManualCompactionPartial) {
  int32_t trivial_move = 0;
  int32_t non_trivial_move = 0;
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::BackgroundCompaction:TrivialMove",
      [&](void* /*arg*/) { trivial_move++; });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::BackgroundCompaction:NonTrivial",
      [&](void* /*arg*/) { non_trivial_move++; });
  bool first = true;
  // Purpose of dependencies:
  // 4 -> 1: ensure the order of two non-trivial compactions
  // 5 -> 2 and 5 -> 3: ensure we do a check before two non-trivial compactions
  // are installed
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency(
      {{"DBCompaction::ManualPartial:4", "DBCompaction::ManualPartial:1"},
       {"DBCompaction::ManualPartial:5", "DBCompaction::ManualPartial:2"},
       {"DBCompaction::ManualPartial:5", "DBCompaction::ManualPartial:3"}});
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::BackgroundCompaction:NonTrivial:AfterRun", [&](void* /*arg*/) {
        if (first) {
          first = false;
          TEST_SYNC_POINT("DBCompaction::ManualPartial:4");
          TEST_SYNC_POINT("DBCompaction::ManualPartial:3");
        } else {  // second non-trivial compaction
          TEST_SYNC_POINT("DBCompaction::ManualPartial:2");
        }
      });

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  Options options = CurrentOptions();
  options.write_buffer_size = 10 * 1024 * 1024;
  options.num_levels = 7;
  options.max_subcompactions = max_subcompactions_;
  options.level0_file_num_compaction_trigger = 3;
  options.max_background_compactions = 3;
  options.target_file_size_base = 1 << 23;  // 8 MB

  DestroyAndReopen(options);
  int32_t value_size = 10 * 1024;  // 10 KB

  // Add 2 non-overlapping files
  Random rnd(301);
  std::map<int32_t, std::string> values;

  // file 1 [0 => 100]
  for (int32_t i = 0; i < 100; i++) {
    values[i] = rnd.RandomString(value_size);
    ASSERT_OK(Put(Key(i), values[i]));
  }
  ASSERT_OK(Flush());

  // file 2 [100 => 300]
  for (int32_t i = 100; i < 300; i++) {
    values[i] = rnd.RandomString(value_size);
    ASSERT_OK(Put(Key(i), values[i]));
  }
  ASSERT_OK(Flush());

  // 2 files in L0
  ASSERT_EQ("2", FilesPerLevel(0));
  CompactRangeOptions compact_options;
  compact_options.change_level = true;
  compact_options.target_level = 6;
  compact_options.exclusive_manual_compaction = exclusive_manual_compaction_;
  // Trivial move the two non-overlapping files to level 6
  ASSERT_OK(db_->CompactRange(compact_options, nullptr, nullptr));
  // 2 files in L6
  ASSERT_EQ("0,0,0,0,0,0,2", FilesPerLevel(0));

  ASSERT_EQ(trivial_move, 1);
  ASSERT_EQ(non_trivial_move, 0);

  // file 3 [ 0 => 200]
  for (int32_t i = 0; i < 200; i++) {
    values[i] = rnd.RandomString(value_size);
    ASSERT_OK(Put(Key(i), values[i]));
  }
  ASSERT_OK(Flush());

  // 1 files in L0
  ASSERT_EQ("1,0,0,0,0,0,2", FilesPerLevel(0));
  ASSERT_OK(dbfull()->TEST_CompactRange(0, nullptr, nullptr, nullptr, false));
  ASSERT_OK(dbfull()->TEST_CompactRange(1, nullptr, nullptr, nullptr, false));
  ASSERT_OK(dbfull()->TEST_CompactRange(2, nullptr, nullptr, nullptr, false));
  ASSERT_OK(dbfull()->TEST_CompactRange(3, nullptr, nullptr, nullptr, false));
  ASSERT_OK(dbfull()->TEST_CompactRange(4, nullptr, nullptr, nullptr, false));
  // 2 files in L6, 1 file in L5
  ASSERT_EQ("0,0,0,0,0,1,2", FilesPerLevel(0));

  ASSERT_EQ(trivial_move, 6);
  ASSERT_EQ(non_trivial_move, 0);

  ROCKSDB_NAMESPACE::port::Thread threads([&] {
    compact_options.change_level = false;
    compact_options.exclusive_manual_compaction = false;
    std::string begin_string = Key(0);
    std::string end_string = Key(199);
    Slice begin(begin_string);
    Slice end(end_string);
    // First non-trivial compaction is triggered
    ASSERT_OK(db_->CompactRange(compact_options, &begin, &end));
  });

  TEST_SYNC_POINT("DBCompaction::ManualPartial:1");
  // file 4 [300 => 400)
  for (int32_t i = 300; i <= 400; i++) {
    values[i] = rnd.RandomString(value_size);
    ASSERT_OK(Put(Key(i), values[i]));
  }
  ASSERT_OK(Flush());

  // file 5 [400 => 500)
  for (int32_t i = 400; i <= 500; i++) {
    values[i] = rnd.RandomString(value_size);
    ASSERT_OK(Put(Key(i), values[i]));
  }
  ASSERT_OK(Flush());

  // file 6 [500 => 600)
  for (int32_t i = 500; i <= 600; i++) {
    values[i] = rnd.RandomString(value_size);
    ASSERT_OK(Put(Key(i), values[i]));
  }
  // Second non-trivial compaction is triggered
  ASSERT_OK(Flush());

  // Before two non-trivial compactions are installed, there are 3 files in L0
  ASSERT_EQ("3,0,0,0,0,1,2", FilesPerLevel(0));
  TEST_SYNC_POINT("DBCompaction::ManualPartial:5");

  ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  // After two non-trivial compactions are installed, there is 1 file in L6, and
  // 1 file in L1
  ASSERT_EQ("0,1,0,0,0,0,1", FilesPerLevel(0));
  threads.join();

  for (int32_t i = 0; i < 600; i++) {
    ASSERT_EQ(Get(Key(i)), values[i]);
  }
}

// Disable as the test is flaky.
TEST_F(DBCompactionTest, DISABLED_ManualPartialFill) {
  int32_t trivial_move = 0;
  int32_t non_trivial_move = 0;
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::BackgroundCompaction:TrivialMove",
      [&](void* /*arg*/) { trivial_move++; });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::BackgroundCompaction:NonTrivial",
      [&](void* /*arg*/) { non_trivial_move++; });
  bool first = true;
  bool second = true;
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency(
      {{"DBCompaction::PartialFill:4", "DBCompaction::PartialFill:1"},
       {"DBCompaction::PartialFill:2", "DBCompaction::PartialFill:3"}});
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::BackgroundCompaction:NonTrivial:AfterRun", [&](void* /*arg*/) {
        if (first) {
          TEST_SYNC_POINT("DBCompaction::PartialFill:4");
          first = false;
          TEST_SYNC_POINT("DBCompaction::PartialFill:3");
        } else if (second) {
        }
      });

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  Options options = CurrentOptions();
  options.write_buffer_size = 10 * 1024 * 1024;
  options.max_bytes_for_level_multiplier = 2;
  options.num_levels = 4;
  options.level0_file_num_compaction_trigger = 3;
  options.max_background_compactions = 3;

  DestroyAndReopen(options);
  // make sure all background compaction jobs can be scheduled
  auto stop_token =
      dbfull()->TEST_write_controler().GetCompactionPressureToken();
  int32_t value_size = 10 * 1024;  // 10 KB

  // Add 2 non-overlapping files
  Random rnd(301);
  std::map<int32_t, std::string> values;

  // file 1 [0 => 100]
  for (int32_t i = 0; i < 100; i++) {
    values[i] = rnd.RandomString(value_size);
    ASSERT_OK(Put(Key(i), values[i]));
  }
  ASSERT_OK(Flush());

  // file 2 [100 => 300]
  for (int32_t i = 100; i < 300; i++) {
    values[i] = rnd.RandomString(value_size);
    ASSERT_OK(Put(Key(i), values[i]));
  }
  ASSERT_OK(Flush());

  // 2 files in L0
  ASSERT_EQ("2", FilesPerLevel(0));
  CompactRangeOptions compact_options;
  compact_options.change_level = true;
  compact_options.target_level = 2;
  ASSERT_OK(db_->CompactRange(compact_options, nullptr, nullptr));
  // 2 files in L2
  ASSERT_EQ("0,0,2", FilesPerLevel(0));

  ASSERT_EQ(trivial_move, 1);
  ASSERT_EQ(non_trivial_move, 0);

  // file 3 [ 0 => 200]
  for (int32_t i = 0; i < 200; i++) {
    values[i] = rnd.RandomString(value_size);
    ASSERT_OK(Put(Key(i), values[i]));
  }
  ASSERT_OK(Flush());

  // 2 files in L2, 1 in L0
  ASSERT_EQ("1,0,2", FilesPerLevel(0));
  ASSERT_OK(dbfull()->TEST_CompactRange(0, nullptr, nullptr, nullptr, false));
  // 2 files in L2, 1 in L1
  ASSERT_EQ("0,1,2", FilesPerLevel(0));

  ASSERT_EQ(trivial_move, 2);
  ASSERT_EQ(non_trivial_move, 0);

  ROCKSDB_NAMESPACE::port::Thread threads([&] {
    compact_options.change_level = false;
    compact_options.exclusive_manual_compaction = false;
    std::string begin_string = Key(0);
    std::string end_string = Key(199);
    Slice begin(begin_string);
    Slice end(end_string);
    ASSERT_OK(db_->CompactRange(compact_options, &begin, &end));
  });

  TEST_SYNC_POINT("DBCompaction::PartialFill:1");
  // Many files 4 [300 => 4300)
  for (int32_t i = 0; i <= 5; i++) {
    for (int32_t j = 300; j < 4300; j++) {
      if (j == 2300) {
        ASSERT_OK(Flush());
        ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());
      }
      values[j] = rnd.RandomString(value_size);
      ASSERT_OK(Put(Key(j), values[j]));
    }
  }

  // Verify level sizes
  uint64_t target_size = 4 * options.max_bytes_for_level_base;
  for (int32_t i = 1; i < options.num_levels; i++) {
    ASSERT_LE(SizeAtLevel(i), target_size);
    target_size = static_cast<uint64_t>(target_size *
                                        options.max_bytes_for_level_multiplier);
  }

  TEST_SYNC_POINT("DBCompaction::PartialFill:2");
  ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  threads.join();

  for (int32_t i = 0; i < 4300; i++) {
    ASSERT_EQ(Get(Key(i)), values[i]);
  }
}

TEST_F(DBCompactionTest, ManualCompactionWithUnorderedWrite) {
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency(
      {{"DBImpl::WriteImpl:UnorderedWriteAfterWriteWAL",
        "DBCompactionTest::ManualCompactionWithUnorderedWrite:WaitWriteWAL"},
       {"DBImpl::WaitForPendingWrites:BeforeBlock",
        "DBImpl::WriteImpl:BeforeUnorderedWriteMemtable"}});

  Options options = CurrentOptions();
  options.unordered_write = true;
  DestroyAndReopen(options);
  ASSERT_OK(Put("foo", "v1"));
  ASSERT_OK(Flush());

  ASSERT_OK(Put("bar", "v1"));
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();
  port::Thread writer([&]() { ASSERT_OK(Put("foo", "v2")); });

  TEST_SYNC_POINT(
      "DBCompactionTest::ManualCompactionWithUnorderedWrite:WaitWriteWAL");
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));

  writer.join();
  ASSERT_EQ(Get("foo"), "v2");

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();

  Reopen(options);
  ASSERT_EQ(Get("foo"), "v2");
}

TEST_F(DBCompactionTest, DeleteFileRange) {
  Options options = CurrentOptions();
  options.write_buffer_size = 10 * 1024 * 1024;
  options.max_bytes_for_level_multiplier = 2;
  options.num_levels = 4;
  options.level0_file_num_compaction_trigger = 3;
  options.max_background_compactions = 3;

  DestroyAndReopen(options);
  int32_t value_size = 10 * 1024;  // 10 KB

  // Add 2 non-overlapping files
  Random rnd(301);
  std::map<int32_t, std::string> values;

  // file 1 [0 => 100]
  for (int32_t i = 0; i < 100; i++) {
    values[i] = rnd.RandomString(value_size);
    ASSERT_OK(Put(Key(i), values[i]));
  }
  ASSERT_OK(Flush());

  // file 2 [100 => 300]
  for (int32_t i = 100; i < 300; i++) {
    values[i] = rnd.RandomString(value_size);
    ASSERT_OK(Put(Key(i), values[i]));
  }
  ASSERT_OK(Flush());

  // 2 files in L0
  ASSERT_EQ("2", FilesPerLevel(0));
  CompactRangeOptions compact_options;
  compact_options.change_level = true;
  compact_options.target_level = 2;
  ASSERT_OK(db_->CompactRange(compact_options, nullptr, nullptr));
  // 2 files in L2
  ASSERT_EQ("0,0,2", FilesPerLevel(0));

  // file 3 [ 0 => 200]
  for (int32_t i = 0; i < 200; i++) {
    values[i] = rnd.RandomString(value_size);
    ASSERT_OK(Put(Key(i), values[i]));
  }
  ASSERT_OK(Flush());

  // Many files 4 [300 => 4300)
  for (int32_t i = 0; i <= 5; i++) {
    for (int32_t j = 300; j < 4300; j++) {
      if (j == 2300) {
        ASSERT_OK(Flush());
        ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());
      }
      values[j] = rnd.RandomString(value_size);
      ASSERT_OK(Put(Key(j), values[j]));
    }
  }
  ASSERT_OK(Flush());
  ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());
  ASSERT_OK(dbfull()->TEST_WaitForCompact());

  // Verify level sizes
  uint64_t target_size = 4 * options.max_bytes_for_level_base;
  for (int32_t i = 1; i < options.num_levels; i++) {
    ASSERT_LE(SizeAtLevel(i), target_size);
    target_size = static_cast<uint64_t>(target_size *
                                        options.max_bytes_for_level_multiplier);
  }

  const size_t old_num_files = CountFiles();
  std::string begin_string = Key(1000);
  std::string end_string = Key(2000);
  Slice begin(begin_string);
  Slice end(end_string);
  ASSERT_OK(DeleteFilesInRange(db_, db_->DefaultColumnFamily(), &begin, &end));

  int32_t deleted_count = 0;
  for (int32_t i = 0; i < 4300; i++) {
    if (i < 1000 || i > 2000) {
      ASSERT_EQ(Get(Key(i)), values[i]);
    } else {
      ReadOptions roptions;
      std::string result;
      Status s = db_->Get(roptions, Key(i), &result);
      ASSERT_TRUE(s.IsNotFound() || s.ok());
      if (s.IsNotFound()) {
        deleted_count++;
      }
    }
  }
  ASSERT_GT(deleted_count, 0);
  begin_string = Key(5000);
  end_string = Key(6000);
  Slice begin1(begin_string);
  Slice end1(end_string);
  // Try deleting files in range which contain no keys
  ASSERT_OK(
      DeleteFilesInRange(db_, db_->DefaultColumnFamily(), &begin1, &end1));

  // Push data from level 0 to level 1 to force all data to be deleted
  // Note that we don't delete level 0 files
  compact_options.change_level = true;
  compact_options.target_level = 1;
  ASSERT_OK(db_->CompactRange(compact_options, nullptr, nullptr));
  ASSERT_OK(dbfull()->TEST_WaitForCompact());

  ASSERT_OK(
      DeleteFilesInRange(db_, db_->DefaultColumnFamily(), nullptr, nullptr));

  int32_t deleted_count2 = 0;
  for (int32_t i = 0; i < 4300; i++) {
    ReadOptions roptions;
    std::string result;
    ASSERT_TRUE(db_->Get(roptions, Key(i), &result).IsNotFound());
    deleted_count2++;
  }
  ASSERT_GT(deleted_count2, deleted_count);
  const size_t new_num_files = CountFiles();
  ASSERT_GT(old_num_files, new_num_files);
}

TEST_F(DBCompactionTest, DeleteFilesInRanges) {
  Options options = CurrentOptions();
  options.write_buffer_size = 10 * 1024 * 1024;
  options.max_bytes_for_level_multiplier = 2;
  options.num_levels = 4;
  options.max_background_compactions = 3;
  options.disable_auto_compactions = true;

  DestroyAndReopen(options);
  int32_t value_size = 10 * 1024;  // 10 KB

  Random rnd(301);
  std::map<int32_t, std::string> values;

  // file [0 => 100), [100 => 200), ... [900, 1000)
  for (auto i = 0; i < 10; i++) {
    for (auto j = 0; j < 100; j++) {
      auto k = i * 100 + j;
      values[k] = rnd.RandomString(value_size);
      ASSERT_OK(Put(Key(k), values[k]));
    }
    ASSERT_OK(Flush());
  }
  ASSERT_EQ("10", FilesPerLevel(0));
  CompactRangeOptions compact_options;
  compact_options.change_level = true;
  compact_options.target_level = 2;
  ASSERT_OK(db_->CompactRange(compact_options, nullptr, nullptr));
  ASSERT_EQ("0,0,10", FilesPerLevel(0));

  // file [0 => 100), [200 => 300), ... [800, 900)
  for (auto i = 0; i < 10; i+=2) {
    for (auto j = 0; j < 100; j++) {
      auto k = i * 100 + j;
      ASSERT_OK(Put(Key(k), values[k]));
    }
    ASSERT_OK(Flush());
  }
  ASSERT_EQ("5,0,10", FilesPerLevel(0));
  ASSERT_OK(dbfull()->TEST_CompactRange(0, nullptr, nullptr));
  ASSERT_EQ("0,5,10", FilesPerLevel(0));

  // Delete files in range [0, 299] (inclusive)
  {
    auto begin_str1 = Key(0), end_str1 = Key(100);
    auto begin_str2 = Key(100), end_str2 = Key(200);
    auto begin_str3 = Key(200), end_str3 = Key(299);
    Slice begin1(begin_str1), end1(end_str1);
    Slice begin2(begin_str2), end2(end_str2);
    Slice begin3(begin_str3), end3(end_str3);
    std::vector<RangePtr> ranges;
    ranges.push_back(RangePtr(&begin1, &end1));
    ranges.push_back(RangePtr(&begin2, &end2));
    ranges.push_back(RangePtr(&begin3, &end3));
    ASSERT_OK(DeleteFilesInRanges(db_, db_->DefaultColumnFamily(),
                                  ranges.data(), ranges.size()));
    ASSERT_EQ("0,3,7", FilesPerLevel(0));

    // Keys [0, 300) should not exist.
    for (auto i = 0; i < 300; i++) {
      ReadOptions ropts;
      std::string result;
      auto s = db_->Get(ropts, Key(i), &result);
      ASSERT_TRUE(s.IsNotFound());
    }
    for (auto i = 300; i < 1000; i++) {
      ASSERT_EQ(Get(Key(i)), values[i]);
    }
  }

  // Delete files in range [600, 999) (exclusive)
  {
    auto begin_str1 = Key(600), end_str1 = Key(800);
    auto begin_str2 = Key(700), end_str2 = Key(900);
    auto begin_str3 = Key(800), end_str3 = Key(999);
    Slice begin1(begin_str1), end1(end_str1);
    Slice begin2(begin_str2), end2(end_str2);
    Slice begin3(begin_str3), end3(end_str3);
    std::vector<RangePtr> ranges;
    ranges.push_back(RangePtr(&begin1, &end1));
    ranges.push_back(RangePtr(&begin2, &end2));
    ranges.push_back(RangePtr(&begin3, &end3));
    ASSERT_OK(DeleteFilesInRanges(db_, db_->DefaultColumnFamily(),
                                  ranges.data(), ranges.size(), false));
    ASSERT_EQ("0,1,4", FilesPerLevel(0));

    // Keys [600, 900) should not exist.
    for (auto i = 600; i < 900; i++) {
      ReadOptions ropts;
      std::string result;
      auto s = db_->Get(ropts, Key(i), &result);
      ASSERT_TRUE(s.IsNotFound());
    }
    for (auto i = 300; i < 600; i++) {
      ASSERT_EQ(Get(Key(i)), values[i]);
    }
    for (auto i = 900; i < 1000; i++) {
      ASSERT_EQ(Get(Key(i)), values[i]);
    }
  }

  // Delete all files.
  {
    RangePtr range;
    ASSERT_OK(DeleteFilesInRanges(db_, db_->DefaultColumnFamily(), &range, 1));
    ASSERT_EQ("", FilesPerLevel(0));

    for (auto i = 0; i < 1000; i++) {
      ReadOptions ropts;
      std::string result;
      auto s = db_->Get(ropts, Key(i), &result);
      ASSERT_TRUE(s.IsNotFound());
    }
  }
}

TEST_F(DBCompactionTest, DeleteFileRangeFileEndpointsOverlapBug) {
  // regression test for #2833: groups of files whose user-keys overlap at the
  // endpoints could be split by `DeleteFilesInRange`. This caused old data to
  // reappear, either because a new version of the key was removed, or a range
  // deletion was partially dropped. It could also cause non-overlapping
  // invariant to be violated if the files dropped by DeleteFilesInRange were
  // a subset of files that a range deletion spans.
  const int kNumL0Files = 2;
  const int kValSize = 8 << 10;  // 8KB
  Options options = CurrentOptions();
  options.level0_file_num_compaction_trigger = kNumL0Files;
  options.target_file_size_base = 1 << 10;  // 1KB
  DestroyAndReopen(options);

  // The snapshot prevents key 1 from having its old version dropped. The low
  // `target_file_size_base` ensures two keys will be in each output file.
  const Snapshot* snapshot = nullptr;
  Random rnd(301);
  // The value indicates which flush the key belonged to, which is enough
  // for us to determine the keys' relative ages. After L0 flushes finish,
  // files look like:
  //
  // File 0: 0 -> vals[0], 1 -> vals[0]
  // File 1:               1 -> vals[1], 2 -> vals[1]
  //
  // Then L0->L1 compaction happens, which outputs keys as follows:
  //
  // File 0: 0 -> vals[0], 1 -> vals[1]
  // File 1:               1 -> vals[0], 2 -> vals[1]
  //
  // DeleteFilesInRange shouldn't be allowed to drop just file 0, as that
  // would cause `1 -> vals[0]` (an older key) to reappear.
  std::string vals[kNumL0Files];
  for (int i = 0; i < kNumL0Files; ++i) {
    vals[i] = rnd.RandomString(kValSize);
    ASSERT_OK(Put(Key(i), vals[i]));
    ASSERT_OK(Put(Key(i + 1), vals[i]));
    ASSERT_OK(Flush());
    if (i == 0) {
      snapshot = db_->GetSnapshot();
    }
  }
  ASSERT_OK(dbfull()->TEST_WaitForCompact());

  // Verify `DeleteFilesInRange` can't drop only file 0 which would cause
  // "1 -> vals[0]" to reappear.
  std::string begin_str = Key(0), end_str = Key(1);
  Slice begin = begin_str, end = end_str;
  ASSERT_OK(DeleteFilesInRange(db_, db_->DefaultColumnFamily(), &begin, &end));
  ASSERT_EQ(vals[1], Get(Key(1)));

  db_->ReleaseSnapshot(snapshot);
}

TEST_P(DBCompactionTestWithParam, TrivialMoveToLastLevelWithFiles) {
  int32_t trivial_move = 0;
  int32_t non_trivial_move = 0;
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::BackgroundCompaction:TrivialMove",
      [&](void* /*arg*/) { trivial_move++; });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::BackgroundCompaction:NonTrivial",
      [&](void* /*arg*/) { non_trivial_move++; });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  Options options = CurrentOptions();
  options.write_buffer_size = 100000000;
  options.max_subcompactions = max_subcompactions_;
  DestroyAndReopen(options);

  int32_t value_size = 10 * 1024;  // 10 KB

  Random rnd(301);
  std::vector<std::string> values;
  // File with keys [ 0 => 99 ]
  for (int i = 0; i < 100; i++) {
    values.push_back(rnd.RandomString(value_size));
    ASSERT_OK(Put(Key(i), values[i]));
  }
  ASSERT_OK(Flush());

  ASSERT_EQ("1", FilesPerLevel(0));
  // Compaction will do L0=>L1 (trivial move) then move L1 files to L3
  CompactRangeOptions compact_options;
  compact_options.change_level = true;
  compact_options.target_level = 3;
  compact_options.exclusive_manual_compaction = exclusive_manual_compaction_;
  ASSERT_OK(db_->CompactRange(compact_options, nullptr, nullptr));
  ASSERT_EQ("0,0,0,1", FilesPerLevel(0));
  ASSERT_EQ(trivial_move, 1);
  ASSERT_EQ(non_trivial_move, 0);

  // File with keys [ 100 => 199 ]
  for (int i = 100; i < 200; i++) {
    values.push_back(rnd.RandomString(value_size));
    ASSERT_OK(Put(Key(i), values[i]));
  }
  ASSERT_OK(Flush());

  ASSERT_EQ("1,0,0,1", FilesPerLevel(0));
  CompactRangeOptions cro;
  cro.exclusive_manual_compaction = exclusive_manual_compaction_;
  // Compaction will do L0=>L1 L1=>L2 L2=>L3 (3 trivial moves)
  ASSERT_OK(db_->CompactRange(cro, nullptr, nullptr));
  ASSERT_EQ("0,0,0,2", FilesPerLevel(0));
  ASSERT_EQ(trivial_move, 4);
  ASSERT_EQ(non_trivial_move, 0);

  for (int i = 0; i < 200; i++) {
    ASSERT_EQ(Get(Key(i)), values[i]);
  }

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
}

TEST_P(DBCompactionTestWithParam, LevelCompactionThirdPath) {
  Options options = CurrentOptions();
  options.db_paths.emplace_back(dbname_, 500 * 1024);
  options.db_paths.emplace_back(dbname_ + "_2", 4 * 1024 * 1024);
  options.db_paths.emplace_back(dbname_ + "_3", 1024 * 1024 * 1024);
  options.memtable_factory.reset(
      new SpecialSkipListFactory(KNumKeysByGenerateNewFile - 1));
  options.compaction_style = kCompactionStyleLevel;
  options.write_buffer_size = 110 << 10;  // 110KB
  options.arena_block_size = 4 << 10;
  options.level0_file_num_compaction_trigger = 2;
  options.num_levels = 4;
  options.max_bytes_for_level_base = 400 * 1024;
  options.max_subcompactions = max_subcompactions_;

  DestroyAndReopen(options);

  Random rnd(301);
  int key_idx = 0;

  // First three 110KB files are not going to second path.
  // After that, (100K, 200K)
  for (int num = 0; num < 3; num++) {
    GenerateNewFile(&rnd, &key_idx);
  }

  // Another 110KB triggers a compaction to 400K file to fill up first path
  GenerateNewFile(&rnd, &key_idx);
  ASSERT_EQ(3, GetSstFileCount(options.db_paths[1].path));

  // (1, 4)
  GenerateNewFile(&rnd, &key_idx);
  ASSERT_EQ("1,4", FilesPerLevel(0));
  ASSERT_EQ(4, GetSstFileCount(options.db_paths[1].path));
  ASSERT_EQ(1, GetSstFileCount(dbname_));

  // (1, 4, 1)
  GenerateNewFile(&rnd, &key_idx);
  ASSERT_EQ("1,4,1", FilesPerLevel(0));
  ASSERT_EQ(1, GetSstFileCount(options.db_paths[2].path));
  ASSERT_EQ(4, GetSstFileCount(options.db_paths[1].path));
  ASSERT_EQ(1, GetSstFileCount(dbname_));

  // (1, 4, 2)
  GenerateNewFile(&rnd, &key_idx);
  ASSERT_EQ("1,4,2", FilesPerLevel(0));
  ASSERT_EQ(2, GetSstFileCount(options.db_paths[2].path));
  ASSERT_EQ(4, GetSstFileCount(options.db_paths[1].path));
  ASSERT_EQ(1, GetSstFileCount(dbname_));

  // (1, 4, 3)
  GenerateNewFile(&rnd, &key_idx);
  ASSERT_EQ("1,4,3", FilesPerLevel(0));
  ASSERT_EQ(3, GetSstFileCount(options.db_paths[2].path));
  ASSERT_EQ(4, GetSstFileCount(options.db_paths[1].path));
  ASSERT_EQ(1, GetSstFileCount(dbname_));

  // (1, 4, 4)
  GenerateNewFile(&rnd, &key_idx);
  ASSERT_EQ("1,4,4", FilesPerLevel(0));
  ASSERT_EQ(4, GetSstFileCount(options.db_paths[2].path));
  ASSERT_EQ(4, GetSstFileCount(options.db_paths[1].path));
  ASSERT_EQ(1, GetSstFileCount(dbname_));

  // (1, 4, 5)
  GenerateNewFile(&rnd, &key_idx);
  ASSERT_EQ("1,4,5", FilesPerLevel(0));
  ASSERT_EQ(5, GetSstFileCount(options.db_paths[2].path));
  ASSERT_EQ(4, GetSstFileCount(options.db_paths[1].path));
  ASSERT_EQ(1, GetSstFileCount(dbname_));

  // (1, 4, 6)
  GenerateNewFile(&rnd, &key_idx);
  ASSERT_EQ("1,4,6", FilesPerLevel(0));
  ASSERT_EQ(6, GetSstFileCount(options.db_paths[2].path));
  ASSERT_EQ(4, GetSstFileCount(options.db_paths[1].path));
  ASSERT_EQ(1, GetSstFileCount(dbname_));

  // (1, 4, 7)
  GenerateNewFile(&rnd, &key_idx);
  ASSERT_EQ("1,4,7", FilesPerLevel(0));
  ASSERT_EQ(7, GetSstFileCount(options.db_paths[2].path));
  ASSERT_EQ(4, GetSstFileCount(options.db_paths[1].path));
  ASSERT_EQ(1, GetSstFileCount(dbname_));

  // (1, 4, 8)
  GenerateNewFile(&rnd, &key_idx);
  ASSERT_EQ("1,4,8", FilesPerLevel(0));
  ASSERT_EQ(8, GetSstFileCount(options.db_paths[2].path));
  ASSERT_EQ(4, GetSstFileCount(options.db_paths[1].path));
  ASSERT_EQ(1, GetSstFileCount(dbname_));

  for (int i = 0; i < key_idx; i++) {
    auto v = Get(Key(i));
    ASSERT_NE(v, "NOT_FOUND");
    ASSERT_TRUE(v.size() == 1 || v.size() == 990);
  }

  Reopen(options);

  for (int i = 0; i < key_idx; i++) {
    auto v = Get(Key(i));
    ASSERT_NE(v, "NOT_FOUND");
    ASSERT_TRUE(v.size() == 1 || v.size() == 990);
  }

  Destroy(options);
}

TEST_P(DBCompactionTestWithParam, LevelCompactionPathUse) {
  Options options = CurrentOptions();
  options.db_paths.emplace_back(dbname_, 500 * 1024);
  options.db_paths.emplace_back(dbname_ + "_2", 4 * 1024 * 1024);
  options.db_paths.emplace_back(dbname_ + "_3", 1024 * 1024 * 1024);
  options.memtable_factory.reset(
      new SpecialSkipListFactory(KNumKeysByGenerateNewFile - 1));
  options.compaction_style = kCompactionStyleLevel;
  options.write_buffer_size = 110 << 10;  // 110KB
  options.arena_block_size = 4 << 10;
  options.level0_file_num_compaction_trigger = 2;
  options.num_levels = 4;
  options.max_bytes_for_level_base = 400 * 1024;
  options.max_subcompactions = max_subcompactions_;

  DestroyAndReopen(options);

  Random rnd(301);
  int key_idx = 0;

  // Always gets compacted into 1 Level1 file,
  // 0/1 Level 0 file
  for (int num = 0; num < 3; num++) {
    key_idx = 0;
    GenerateNewFile(&rnd, &key_idx);
  }

  key_idx = 0;
  GenerateNewFile(&rnd, &key_idx);
  ASSERT_EQ(1, GetSstFileCount(options.db_paths[1].path));

  key_idx = 0;
  GenerateNewFile(&rnd, &key_idx);
  ASSERT_EQ("1,1", FilesPerLevel(0));
  ASSERT_EQ(1, GetSstFileCount(options.db_paths[1].path));
  ASSERT_EQ(1, GetSstFileCount(dbname_));

  key_idx = 0;
  GenerateNewFile(&rnd, &key_idx);
  ASSERT_EQ("0,1", FilesPerLevel(0));
  ASSERT_EQ(0, GetSstFileCount(options.db_paths[2].path));
  ASSERT_EQ(1, GetSstFileCount(options.db_paths[1].path));
  ASSERT_EQ(0, GetSstFileCount(dbname_));

  key_idx = 0;
  GenerateNewFile(&rnd, &key_idx);
  ASSERT_EQ("1,1", FilesPerLevel(0));
  ASSERT_EQ(0, GetSstFileCount(options.db_paths[2].path));
  ASSERT_EQ(1, GetSstFileCount(options.db_paths[1].path));
  ASSERT_EQ(1, GetSstFileCount(dbname_));

  key_idx = 0;
  GenerateNewFile(&rnd, &key_idx);
  ASSERT_EQ("0,1", FilesPerLevel(0));
  ASSERT_EQ(0, GetSstFileCount(options.db_paths[2].path));
  ASSERT_EQ(1, GetSstFileCount(options.db_paths[1].path));
  ASSERT_EQ(0, GetSstFileCount(dbname_));

  key_idx = 0;
  GenerateNewFile(&rnd, &key_idx);
  ASSERT_EQ("1,1", FilesPerLevel(0));
  ASSERT_EQ(0, GetSstFileCount(options.db_paths[2].path));
  ASSERT_EQ(1, GetSstFileCount(options.db_paths[1].path));
  ASSERT_EQ(1, GetSstFileCount(dbname_));

  key_idx = 0;
  GenerateNewFile(&rnd, &key_idx);
  ASSERT_EQ("0,1", FilesPerLevel(0));
  ASSERT_EQ(0, GetSstFileCount(options.db_paths[2].path));
  ASSERT_EQ(1, GetSstFileCount(options.db_paths[1].path));
  ASSERT_EQ(0, GetSstFileCount(dbname_));

  key_idx = 0;
  GenerateNewFile(&rnd, &key_idx);
  ASSERT_EQ("1,1", FilesPerLevel(0));
  ASSERT_EQ(0, GetSstFileCount(options.db_paths[2].path));
  ASSERT_EQ(1, GetSstFileCount(options.db_paths[1].path));
  ASSERT_EQ(1, GetSstFileCount(dbname_));

  key_idx = 0;
  GenerateNewFile(&rnd, &key_idx);
  ASSERT_EQ("0,1", FilesPerLevel(0));
  ASSERT_EQ(0, GetSstFileCount(options.db_paths[2].path));
  ASSERT_EQ(1, GetSstFileCount(options.db_paths[1].path));
  ASSERT_EQ(0, GetSstFileCount(dbname_));

  key_idx = 0;
  GenerateNewFile(&rnd, &key_idx);
  ASSERT_EQ("1,1", FilesPerLevel(0));
  ASSERT_EQ(0, GetSstFileCount(options.db_paths[2].path));
  ASSERT_EQ(1, GetSstFileCount(options.db_paths[1].path));
  ASSERT_EQ(1, GetSstFileCount(dbname_));

  for (int i = 0; i < key_idx; i++) {
    auto v = Get(Key(i));
    ASSERT_NE(v, "NOT_FOUND");
    ASSERT_TRUE(v.size() == 1 || v.size() == 990);
  }

  Reopen(options);

  for (int i = 0; i < key_idx; i++) {
    auto v = Get(Key(i));
    ASSERT_NE(v, "NOT_FOUND");
    ASSERT_TRUE(v.size() == 1 || v.size() == 990);
  }

  Destroy(options);
}

TEST_P(DBCompactionTestWithParam, LevelCompactionCFPathUse) {
  Options options = CurrentOptions();
  options.db_paths.emplace_back(dbname_, 500 * 1024);
  options.db_paths.emplace_back(dbname_ + "_2", 4 * 1024 * 1024);
  options.db_paths.emplace_back(dbname_ + "_3", 1024 * 1024 * 1024);
  options.memtable_factory.reset(
    new SpecialSkipListFactory(KNumKeysByGenerateNewFile - 1));
  options.compaction_style = kCompactionStyleLevel;
  options.write_buffer_size = 110 << 10;  // 110KB
  options.arena_block_size = 4 << 10;
  options.level0_file_num_compaction_trigger = 2;
  options.num_levels = 4;
  options.max_bytes_for_level_base = 400 * 1024;
  options.max_subcompactions = max_subcompactions_;

  std::vector<Options> option_vector;
  option_vector.emplace_back(options);
  ColumnFamilyOptions cf_opt1(options), cf_opt2(options);
  // Configure CF1 specific paths.
  cf_opt1.cf_paths.emplace_back(dbname_ + "cf1", 500 * 1024);
  cf_opt1.cf_paths.emplace_back(dbname_ + "cf1_2", 4 * 1024 * 1024);
  cf_opt1.cf_paths.emplace_back(dbname_ + "cf1_3", 1024 * 1024 * 1024);
  option_vector.emplace_back(DBOptions(options), cf_opt1);
  CreateColumnFamilies({"one"},option_vector[1]);

  // Configura CF2 specific paths.
  cf_opt2.cf_paths.emplace_back(dbname_ + "cf2", 500 * 1024);
  cf_opt2.cf_paths.emplace_back(dbname_ + "cf2_2", 4 * 1024 * 1024);
  cf_opt2.cf_paths.emplace_back(dbname_ + "cf2_3", 1024 * 1024 * 1024);
  option_vector.emplace_back(DBOptions(options), cf_opt2);
  CreateColumnFamilies({"two"},option_vector[2]);

  ReopenWithColumnFamilies({"default", "one", "two"}, option_vector);

  Random rnd(301);
  int key_idx = 0;
  int key_idx1 = 0;
  int key_idx2 = 0;

  auto generate_file = [&]() {
    GenerateNewFile(0, &rnd, &key_idx);
    GenerateNewFile(1, &rnd, &key_idx1);
    GenerateNewFile(2, &rnd, &key_idx2);
  };

  auto check_sstfilecount = [&](int path_id, int expected) {
    ASSERT_EQ(expected, GetSstFileCount(options.db_paths[path_id].path));
    ASSERT_EQ(expected, GetSstFileCount(cf_opt1.cf_paths[path_id].path));
    ASSERT_EQ(expected, GetSstFileCount(cf_opt2.cf_paths[path_id].path));
  };

  auto check_filesperlevel = [&](const std::string& expected) {
    ASSERT_EQ(expected, FilesPerLevel(0));
    ASSERT_EQ(expected, FilesPerLevel(1));
    ASSERT_EQ(expected, FilesPerLevel(2));
  };

  auto check_getvalues = [&]() {
    for (int i = 0; i < key_idx; i++) {
      auto v = Get(0, Key(i));
      ASSERT_NE(v, "NOT_FOUND");
      ASSERT_TRUE(v.size() == 1 || v.size() == 990);
    }

    for (int i = 0; i < key_idx1; i++) {
      auto v = Get(1, Key(i));
      ASSERT_NE(v, "NOT_FOUND");
      ASSERT_TRUE(v.size() == 1 || v.size() == 990);
    }

    for (int i = 0; i < key_idx2; i++) {
      auto v = Get(2, Key(i));
      ASSERT_NE(v, "NOT_FOUND");
      ASSERT_TRUE(v.size() == 1 || v.size() == 990);
    }
  };

  // Check that default column family uses db_paths.
  // And Column family "one" uses cf_paths.

  // First three 110KB files are not going to second path.
  // After that, (100K, 200K)
  for (int num = 0; num < 3; num++) {
    generate_file();
  }

  // Another 110KB triggers a compaction to 400K file to fill up first path
  generate_file();
  check_sstfilecount(1, 3);

  // (1, 4)
  generate_file();
  check_filesperlevel("1,4");
  check_sstfilecount(1, 4);
  check_sstfilecount(0, 1);

  // (1, 4, 1)
  generate_file();
  check_filesperlevel("1,4,1");
  check_sstfilecount(2, 1);
  check_sstfilecount(1, 4);
  check_sstfilecount(0, 1);

  // (1, 4, 2)
  generate_file();
  check_filesperlevel("1,4,2");
  check_sstfilecount(2, 2);
  check_sstfilecount(1, 4);
  check_sstfilecount(0, 1);

  check_getvalues();

  ReopenWithColumnFamilies({"default", "one", "two"}, option_vector);

  check_getvalues();

  Destroy(options, true);
}

TEST_P(DBCompactionTestWithParam, ConvertCompactionStyle) {
  Random rnd(301);
  int max_key_level_insert = 200;
  int max_key_universal_insert = 600;

  // Stage 1: generate a db with level compaction
  Options options = CurrentOptions();
  options.write_buffer_size = 110 << 10;  // 110KB
  options.arena_block_size = 4 << 10;
  options.num_levels = 4;
  options.level0_file_num_compaction_trigger = 3;
  options.max_bytes_for_level_base = 500 << 10;  // 500KB
  options.max_bytes_for_level_multiplier = 1;
  options.target_file_size_base = 200 << 10;  // 200KB
  options.target_file_size_multiplier = 1;
  options.max_subcompactions = max_subcompactions_;
  CreateAndReopenWithCF({"pikachu"}, options);

  for (int i = 0; i <= max_key_level_insert; i++) {
    // each value is 10K
    ASSERT_OK(Put(1, Key(i), rnd.RandomString(10000)));
  }
  ASSERT_OK(Flush(1));
  ASSERT_OK(dbfull()->TEST_WaitForCompact());

  ASSERT_GT(TotalTableFiles(1, 4), 1);
  int non_level0_num_files = 0;
  for (int i = 1; i < options.num_levels; i++) {
    non_level0_num_files += NumTableFilesAtLevel(i, 1);
  }
  ASSERT_GT(non_level0_num_files, 0);

  // Stage 2: reopen with universal compaction - should fail
  options = CurrentOptions();
  options.compaction_style = kCompactionStyleUniversal;
  options.num_levels = 1;
  options = CurrentOptions(options);
  Status s = TryReopenWithColumnFamilies({"default", "pikachu"}, options);
  ASSERT_TRUE(s.IsInvalidArgument());

  // Stage 3: compact into a single file and move the file to level 0
  options = CurrentOptions();
  options.disable_auto_compactions = true;
  options.target_file_size_base = INT_MAX;
  options.target_file_size_multiplier = 1;
  options.max_bytes_for_level_base = INT_MAX;
  options.max_bytes_for_level_multiplier = 1;
  options.num_levels = 4;
  options = CurrentOptions(options);
  ReopenWithColumnFamilies({"default", "pikachu"}, options);

  CompactRangeOptions compact_options;
  compact_options.change_level = true;
  compact_options.target_level = 0;
  // cannot use kForceOptimized here because the compaction here is expected
  // to generate one output file
  compact_options.bottommost_level_compaction =
      BottommostLevelCompaction::kForce;
  compact_options.exclusive_manual_compaction = exclusive_manual_compaction_;
  ASSERT_OK(
      dbfull()->CompactRange(compact_options, handles_[1], nullptr, nullptr));

  // Only 1 file in L0
  ASSERT_EQ("1", FilesPerLevel(1));

  // Stage 4: re-open in universal compaction style and do some db operations
  options = CurrentOptions();
  options.compaction_style = kCompactionStyleUniversal;
  options.num_levels = 4;
  options.write_buffer_size = 110 << 10;  // 110KB
  options.arena_block_size = 4 << 10;
  options.level0_file_num_compaction_trigger = 3;
  options = CurrentOptions(options);
  ReopenWithColumnFamilies({"default", "pikachu"}, options);

  options.num_levels = 1;
  ReopenWithColumnFamilies({"default", "pikachu"}, options);

  for (int i = max_key_level_insert / 2; i <= max_key_universal_insert; i++) {
    ASSERT_OK(Put(1, Key(i), rnd.RandomString(10000)));
  }
  ASSERT_OK(dbfull()->Flush(FlushOptions()));
  ASSERT_OK(Flush(1));
  ASSERT_OK(dbfull()->TEST_WaitForCompact());

  for (int i = 1; i < options.num_levels; i++) {
    ASSERT_EQ(NumTableFilesAtLevel(i, 1), 0);
  }

  // verify keys inserted in both level compaction style and universal
  // compaction style
  std::string keys_in_db;
  Iterator* iter = dbfull()->NewIterator(ReadOptions(), handles_[1]);
  ASSERT_OK(iter->status());
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    keys_in_db.append(iter->key().ToString());
    keys_in_db.push_back(',');
  }
  delete iter;

  std::string expected_keys;
  for (int i = 0; i <= max_key_universal_insert; i++) {
    expected_keys.append(Key(i));
    expected_keys.push_back(',');
  }

  ASSERT_EQ(keys_in_db, expected_keys);
}

TEST_F(DBCompactionTest, L0_CompactionBug_Issue44_a) {
  do {
    CreateAndReopenWithCF({"pikachu"}, CurrentOptions());
    ASSERT_OK(Put(1, "b", "v"));
    ReopenWithColumnFamilies({"default", "pikachu"}, CurrentOptions());
    ASSERT_OK(Delete(1, "b"));
    ASSERT_OK(Delete(1, "a"));
    ReopenWithColumnFamilies({"default", "pikachu"}, CurrentOptions());
    ASSERT_OK(Delete(1, "a"));
    ReopenWithColumnFamilies({"default", "pikachu"}, CurrentOptions());
    ASSERT_OK(Put(1, "a", "v"));
    ReopenWithColumnFamilies({"default", "pikachu"}, CurrentOptions());
    ReopenWithColumnFamilies({"default", "pikachu"}, CurrentOptions());
    ASSERT_EQ("(a->v)", Contents(1));
    env_->SleepForMicroseconds(1000000);  // Wait for compaction to finish
    ASSERT_EQ("(a->v)", Contents(1));
  } while (ChangeCompactOptions());
}

TEST_F(DBCompactionTest, L0_CompactionBug_Issue44_b) {
  do {
    CreateAndReopenWithCF({"pikachu"}, CurrentOptions());
    ASSERT_OK(Put(1, "", ""));
    ReopenWithColumnFamilies({"default", "pikachu"}, CurrentOptions());
    ASSERT_OK(Delete(1, "e"));
    ASSERT_OK(Put(1, "", ""));
    ReopenWithColumnFamilies({"default", "pikachu"}, CurrentOptions());
    ASSERT_OK(Put(1, "c", "cv"));
    ReopenWithColumnFamilies({"default", "pikachu"}, CurrentOptions());
    ASSERT_OK(Put(1, "", ""));
    ReopenWithColumnFamilies({"default", "pikachu"}, CurrentOptions());
    ASSERT_OK(Put(1, "", ""));
    env_->SleepForMicroseconds(1000000);  // Wait for compaction to finish
    ReopenWithColumnFamilies({"default", "pikachu"}, CurrentOptions());
    ASSERT_OK(Put(1, "d", "dv"));
    ReopenWithColumnFamilies({"default", "pikachu"}, CurrentOptions());
    ASSERT_OK(Put(1, "", ""));
    ReopenWithColumnFamilies({"default", "pikachu"}, CurrentOptions());
    ASSERT_OK(Delete(1, "d"));
    ASSERT_OK(Delete(1, "b"));
    ReopenWithColumnFamilies({"default", "pikachu"}, CurrentOptions());
    ASSERT_EQ("(->)(c->cv)", Contents(1));
    env_->SleepForMicroseconds(1000000);  // Wait for compaction to finish
    ASSERT_EQ("(->)(c->cv)", Contents(1));
  } while (ChangeCompactOptions());
}

TEST_F(DBCompactionTest, ManualAutoRace) {
  CreateAndReopenWithCF({"pikachu"}, CurrentOptions());
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency(
      {{"DBImpl::BGWorkCompaction", "DBCompactionTest::ManualAutoRace:1"},
       {"DBImpl::RunManualCompaction:WaitScheduled",
        "BackgroundCallCompaction:0"}});

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  ASSERT_OK(Put(1, "foo", ""));
  ASSERT_OK(Put(1, "bar", ""));
  ASSERT_OK(Flush(1));
  ASSERT_OK(Put(1, "foo", ""));
  ASSERT_OK(Put(1, "bar", ""));
  // Generate four files in CF 0, which should trigger an auto compaction
  ASSERT_OK(Put("foo", ""));
  ASSERT_OK(Put("bar", ""));
  ASSERT_OK(Flush());
  ASSERT_OK(Put("foo", ""));
  ASSERT_OK(Put("bar", ""));
  ASSERT_OK(Flush());
  ASSERT_OK(Put("foo", ""));
  ASSERT_OK(Put("bar", ""));
  ASSERT_OK(Flush());
  ASSERT_OK(Put("foo", ""));
  ASSERT_OK(Put("bar", ""));
  ASSERT_OK(Flush());

  // The auto compaction is scheduled but waited until here
  TEST_SYNC_POINT("DBCompactionTest::ManualAutoRace:1");
  // The auto compaction will wait until the manual compaction is registerd
  // before processing so that it will be cancelled.
  ASSERT_OK(dbfull()->CompactRange(CompactRangeOptions(), handles_[1], nullptr,
                                   nullptr));
  ASSERT_EQ("0,1", FilesPerLevel(1));

  // Eventually the cancelled compaction will be rescheduled and executed.
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  ASSERT_EQ("0,1", FilesPerLevel(0));
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
}

TEST_P(DBCompactionTestWithParam, ManualCompaction) {
  Options options = CurrentOptions();
  options.max_subcompactions = max_subcompactions_;
  options.statistics = ROCKSDB_NAMESPACE::CreateDBStatistics();
  CreateAndReopenWithCF({"pikachu"}, options);

  // iter - 0 with 7 levels
  // iter - 1 with 3 levels
  for (int iter = 0; iter < 2; ++iter) {
    MakeTables(3, "p", "q", 1);
    ASSERT_EQ("1,1,1", FilesPerLevel(1));

    // Compaction range falls before files
    Compact(1, "", "c");
    ASSERT_EQ("1,1,1", FilesPerLevel(1));

    // Compaction range falls after files
    Compact(1, "r", "z");
    ASSERT_EQ("1,1,1", FilesPerLevel(1));

    // Compaction range overlaps files
    Compact(1, "p", "q");
    ASSERT_EQ("0,0,1", FilesPerLevel(1));

    // Populate a different range
    MakeTables(3, "c", "e", 1);
    ASSERT_EQ("1,1,2", FilesPerLevel(1));

    // Compact just the new range
    Compact(1, "b", "f");
    ASSERT_EQ("0,0,2", FilesPerLevel(1));

    // Compact all
    MakeTables(1, "a", "z", 1);
    ASSERT_EQ("1,0,2", FilesPerLevel(1));

    uint64_t prev_block_cache_add =
        options.statistics->getTickerCount(BLOCK_CACHE_ADD);
    CompactRangeOptions cro;
    cro.exclusive_manual_compaction = exclusive_manual_compaction_;
    ASSERT_OK(db_->CompactRange(cro, handles_[1], nullptr, nullptr));
    // Verify manual compaction doesn't fill block cache
    ASSERT_EQ(prev_block_cache_add,
              options.statistics->getTickerCount(BLOCK_CACHE_ADD));

    ASSERT_EQ("0,0,1", FilesPerLevel(1));

    if (iter == 0) {
      options = CurrentOptions();
      options.num_levels = 3;
      options.create_if_missing = true;
      options.statistics = ROCKSDB_NAMESPACE::CreateDBStatistics();
      DestroyAndReopen(options);
      CreateAndReopenWithCF({"pikachu"}, options);
    }
  }
}


TEST_P(DBCompactionTestWithParam, ManualLevelCompactionOutputPathId) {
  Options options = CurrentOptions();
  options.db_paths.emplace_back(dbname_ + "_2", 2 * 10485760);
  options.db_paths.emplace_back(dbname_ + "_3", 100 * 10485760);
  options.db_paths.emplace_back(dbname_ + "_4", 120 * 10485760);
  options.max_subcompactions = max_subcompactions_;
  CreateAndReopenWithCF({"pikachu"}, options);

  // iter - 0 with 7 levels
  // iter - 1 with 3 levels
  for (int iter = 0; iter < 2; ++iter) {
    for (int i = 0; i < 3; ++i) {
      ASSERT_OK(Put(1, "p", "begin"));
      ASSERT_OK(Put(1, "q", "end"));
      ASSERT_OK(Flush(1));
    }
    ASSERT_EQ("3", FilesPerLevel(1));
    ASSERT_EQ(3, GetSstFileCount(options.db_paths[0].path));
    ASSERT_EQ(0, GetSstFileCount(dbname_));

    // Compaction range falls before files
    Compact(1, "", "c");
    ASSERT_EQ("3", FilesPerLevel(1));

    // Compaction range falls after files
    Compact(1, "r", "z");
    ASSERT_EQ("3", FilesPerLevel(1));

    // Compaction range overlaps files
    Compact(1, "p", "q", 1);
    ASSERT_OK(dbfull()->TEST_WaitForCompact());
    ASSERT_EQ("0,1", FilesPerLevel(1));
    ASSERT_EQ(1, GetSstFileCount(options.db_paths[1].path));
    ASSERT_EQ(0, GetSstFileCount(options.db_paths[0].path));
    ASSERT_EQ(0, GetSstFileCount(dbname_));

    // Populate a different range
    for (int i = 0; i < 3; ++i) {
      ASSERT_OK(Put(1, "c", "begin"));
      ASSERT_OK(Put(1, "e", "end"));
      ASSERT_OK(Flush(1));
    }
    ASSERT_EQ("3,1", FilesPerLevel(1));

    // Compact just the new range
    Compact(1, "b", "f", 1);
    ASSERT_OK(dbfull()->TEST_WaitForCompact());
    ASSERT_EQ("0,2", FilesPerLevel(1));
    ASSERT_EQ(2, GetSstFileCount(options.db_paths[1].path));
    ASSERT_EQ(0, GetSstFileCount(options.db_paths[0].path));
    ASSERT_EQ(0, GetSstFileCount(dbname_));

    // Compact all
    ASSERT_OK(Put(1, "a", "begin"));
    ASSERT_OK(Put(1, "z", "end"));
    ASSERT_OK(Flush(1));
    ASSERT_EQ("1,2", FilesPerLevel(1));
    ASSERT_EQ(2, GetSstFileCount(options.db_paths[1].path));
    ASSERT_EQ(1, GetSstFileCount(options.db_paths[0].path));
    CompactRangeOptions compact_options;
    compact_options.target_path_id = 1;
    compact_options.exclusive_manual_compaction = exclusive_manual_compaction_;
    ASSERT_OK(
        db_->CompactRange(compact_options, handles_[1], nullptr, nullptr));
    ASSERT_OK(dbfull()->TEST_WaitForCompact());

    ASSERT_EQ("0,1", FilesPerLevel(1));
    ASSERT_EQ(1, GetSstFileCount(options.db_paths[1].path));
    ASSERT_EQ(0, GetSstFileCount(options.db_paths[0].path));
    ASSERT_EQ(0, GetSstFileCount(dbname_));

    if (iter == 0) {
      DestroyAndReopen(options);
      options = CurrentOptions();
      options.db_paths.emplace_back(dbname_ + "_2", 2 * 10485760);
      options.db_paths.emplace_back(dbname_ + "_3", 100 * 10485760);
      options.db_paths.emplace_back(dbname_ + "_4", 120 * 10485760);
      options.max_background_flushes = 1;
      options.num_levels = 3;
      options.create_if_missing = true;
      CreateAndReopenWithCF({"pikachu"}, options);
    }
  }
}

TEST_F(DBCompactionTest, FilesDeletedAfterCompaction) {
  do {
    CreateAndReopenWithCF({"pikachu"}, CurrentOptions());
    ASSERT_OK(Put(1, "foo", "v2"));
    Compact(1, "a", "z");
    const size_t num_files = CountLiveFiles();
    for (int i = 0; i < 10; i++) {
      ASSERT_OK(Put(1, "foo", "v2"));
      Compact(1, "a", "z");
    }
    ASSERT_EQ(CountLiveFiles(), num_files);
  } while (ChangeCompactOptions());
}

// Check level comapction with compact files
TEST_P(DBCompactionTestWithParam, DISABLED_CompactFilesOnLevelCompaction) {
  const int kTestKeySize = 16;
  const int kTestValueSize = 984;
  const int kEntrySize = kTestKeySize + kTestValueSize;
  const int kEntriesPerBuffer = 100;
  Options options;
  options.create_if_missing = true;
  options.write_buffer_size = kEntrySize * kEntriesPerBuffer;
  options.compaction_style = kCompactionStyleLevel;
  options.target_file_size_base = options.write_buffer_size;
  options.max_bytes_for_level_base = options.target_file_size_base * 2;
  options.level0_stop_writes_trigger = 2;
  options.max_bytes_for_level_multiplier = 2;
  options.compression = kNoCompression;
  options.max_subcompactions = max_subcompactions_;
  options = CurrentOptions(options);
  CreateAndReopenWithCF({"pikachu"}, options);

  Random rnd(301);
  for (int key = 64 * kEntriesPerBuffer; key >= 0; --key) {
    ASSERT_OK(Put(1, ToString(key), rnd.RandomString(kTestValueSize)));
  }
  ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable(handles_[1]));
  ASSERT_OK(dbfull()->TEST_WaitForCompact());

  ColumnFamilyMetaData cf_meta;
  dbfull()->GetColumnFamilyMetaData(handles_[1], &cf_meta);
  int output_level = static_cast<int>(cf_meta.levels.size()) - 1;
  for (int file_picked = 5; file_picked > 0; --file_picked) {
    std::set<std::string> overlapping_file_names;
    std::vector<std::string> compaction_input_file_names;
    for (int f = 0; f < file_picked; ++f) {
      int level = 0;
      auto file_meta = PickFileRandomly(cf_meta, &rnd, &level);
      compaction_input_file_names.push_back(file_meta->name);
      GetOverlappingFileNumbersForLevelCompaction(
          cf_meta, options.comparator, level, output_level,
          file_meta, &overlapping_file_names);
    }

    ASSERT_OK(dbfull()->CompactFiles(
        CompactionOptions(), handles_[1],
        compaction_input_file_names,
        output_level));

    // Make sure all overlapping files do not exist after compaction
    dbfull()->GetColumnFamilyMetaData(handles_[1], &cf_meta);
    VerifyCompactionResult(cf_meta, overlapping_file_names);
  }

  // make sure all key-values are still there.
  for (int key = 64 * kEntriesPerBuffer; key >= 0; --key) {
    ASSERT_NE(Get(1, ToString(key)), "NOT_FOUND");
  }
}

TEST_P(DBCompactionTestWithParam, PartialCompactionFailure) {
  Options options;
  const int kKeySize = 16;
  const int kKvSize = 1000;
  const int kKeysPerBuffer = 100;
  const int kNumL1Files = 5;
  options.create_if_missing = true;
  options.write_buffer_size = kKeysPerBuffer * kKvSize;
  options.max_write_buffer_number = 2;
  options.target_file_size_base =
      options.write_buffer_size *
      (options.max_write_buffer_number - 1);
  options.level0_file_num_compaction_trigger = kNumL1Files;
  options.max_bytes_for_level_base =
      options.level0_file_num_compaction_trigger *
      options.target_file_size_base;
  options.max_bytes_for_level_multiplier = 2;
  options.compression = kNoCompression;
  options.max_subcompactions = max_subcompactions_;

  env_->SetBackgroundThreads(1, Env::HIGH);
  env_->SetBackgroundThreads(1, Env::LOW);
  // stop the compaction thread until we simulate the file creation failure.
  test::SleepingBackgroundTask sleeping_task_low;
  env_->Schedule(&test::SleepingBackgroundTask::DoSleepTask, &sleeping_task_low,
                 Env::Priority::LOW);

  options.env = env_;

  DestroyAndReopen(options);

  const int kNumInsertedKeys =
      options.level0_file_num_compaction_trigger *
      (options.max_write_buffer_number - 1) *
      kKeysPerBuffer;

  Random rnd(301);
  std::vector<std::string> keys;
  std::vector<std::string> values;
  for (int k = 0; k < kNumInsertedKeys; ++k) {
    keys.emplace_back(rnd.RandomString(kKeySize));
    values.emplace_back(rnd.RandomString(kKvSize - kKeySize));
    ASSERT_OK(Put(Slice(keys[k]), Slice(values[k])));
    ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());
  }

  ASSERT_OK(dbfull()->TEST_FlushMemTable(true));
  // Make sure the number of L0 files can trigger compaction.
  ASSERT_GE(NumTableFilesAtLevel(0),
            options.level0_file_num_compaction_trigger);

  auto previous_num_level0_files = NumTableFilesAtLevel(0);

  // Fail the first file creation.
  env_->non_writable_count_ = 1;
  sleeping_task_low.WakeUp();
  sleeping_task_low.WaitUntilDone();

  // Expect compaction to fail here as one file will fail its
  // creation.
  ASSERT_TRUE(!dbfull()->TEST_WaitForCompact().ok());

  // Verify L0 -> L1 compaction does fail.
  ASSERT_EQ(NumTableFilesAtLevel(1), 0);

  // Verify all L0 files are still there.
  ASSERT_EQ(NumTableFilesAtLevel(0), previous_num_level0_files);

  // All key-values must exist after compaction fails.
  for (int k = 0; k < kNumInsertedKeys; ++k) {
    ASSERT_EQ(values[k], Get(keys[k]));
  }

  env_->non_writable_count_ = 0;

  // Make sure RocksDB will not get into corrupted state.
  Reopen(options);

  // Verify again after reopen.
  for (int k = 0; k < kNumInsertedKeys; ++k) {
    ASSERT_EQ(values[k], Get(keys[k]));
  }
}

TEST_P(DBCompactionTestWithParam, DeleteMovedFileAfterCompaction) {
  // iter 1 -- delete_obsolete_files_period_micros == 0
  for (int iter = 0; iter < 2; ++iter) {
    // This test triggers move compaction and verifies that the file is not
    // deleted when it's part of move compaction
    Options options = CurrentOptions();
    options.env = env_;
    if (iter == 1) {
      options.delete_obsolete_files_period_micros = 0;
    }
    options.create_if_missing = true;
    options.level0_file_num_compaction_trigger =
        2;  // trigger compaction when we have 2 files
    OnFileDeletionListener* listener = new OnFileDeletionListener();
    options.listeners.emplace_back(listener);
    options.max_subcompactions = max_subcompactions_;
    DestroyAndReopen(options);

    Random rnd(301);
    // Create two 1MB sst files
    for (int i = 0; i < 2; ++i) {
      // Create 1MB sst file
      for (int j = 0; j < 100; ++j) {
        ASSERT_OK(Put(Key(i * 50 + j), rnd.RandomString(10 * 1024)));
      }
      ASSERT_OK(Flush());
    }
    // this should execute L0->L1
    ASSERT_OK(dbfull()->TEST_WaitForCompact());
    ASSERT_EQ("0,1", FilesPerLevel(0));

    // block compactions
    test::SleepingBackgroundTask sleeping_task;
    env_->Schedule(&test::SleepingBackgroundTask::DoSleepTask, &sleeping_task,
                   Env::Priority::LOW);

    options.max_bytes_for_level_base = 1024 * 1024;  // 1 MB
    Reopen(options);
    std::unique_ptr<Iterator> iterator(db_->NewIterator(ReadOptions()));
    ASSERT_EQ("0,1", FilesPerLevel(0));
    // let compactions go
    sleeping_task.WakeUp();
    sleeping_task.WaitUntilDone();

    // this should execute L1->L2 (move)
    ASSERT_OK(dbfull()->TEST_WaitForCompact());

    ASSERT_EQ("0,0,1", FilesPerLevel(0));

    std::vector<LiveFileMetaData> metadata;
    db_->GetLiveFilesMetaData(&metadata);
    ASSERT_EQ(metadata.size(), 1U);
    auto moved_file_name = metadata[0].name;

    // Create two more 1MB sst files
    for (int i = 0; i < 2; ++i) {
      // Create 1MB sst file
      for (int j = 0; j < 100; ++j) {
        ASSERT_OK(Put(Key(i * 50 + j + 100), rnd.RandomString(10 * 1024)));
      }
      ASSERT_OK(Flush());
    }
    // this should execute both L0->L1 and L1->L2 (merge with previous file)
    ASSERT_OK(dbfull()->TEST_WaitForCompact());

    ASSERT_EQ("0,0,2", FilesPerLevel(0));

    // iterator is holding the file
    ASSERT_OK(env_->FileExists(dbname_ + moved_file_name));

    listener->SetExpectedFileName(dbname_ + moved_file_name);
    ASSERT_OK(iterator->status());
    iterator.reset();

    // this file should have been compacted away
    ASSERT_NOK(env_->FileExists(dbname_ + moved_file_name));
    listener->VerifyMatchedCount(1);
  }
}

TEST_P(DBCompactionTestWithParam, CompressLevelCompaction) {
  if (!Zlib_Supported()) {
    return;
  }
  Options options = CurrentOptions();
  options.memtable_factory.reset(
      new SpecialSkipListFactory(KNumKeysByGenerateNewFile - 1));
  options.compaction_style = kCompactionStyleLevel;
  options.write_buffer_size = 110 << 10;  // 110KB
  options.arena_block_size = 4 << 10;
  options.level0_file_num_compaction_trigger = 2;
  options.num_levels = 4;
  options.max_bytes_for_level_base = 400 * 1024;
  options.max_subcompactions = max_subcompactions_;
  // First two levels have no compression, so that a trivial move between
  // them will be allowed. Level 2 has Zlib compression so that a trivial
  // move to level 3 will not be allowed
  options.compression_per_level = {kNoCompression, kNoCompression,
                                   kZlibCompression};
  int matches = 0, didnt_match = 0, trivial_move = 0, non_trivial = 0;

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "Compaction::InputCompressionMatchesOutput:Matches",
      [&](void* /*arg*/) { matches++; });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "Compaction::InputCompressionMatchesOutput:DidntMatch",
      [&](void* /*arg*/) { didnt_match++; });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::BackgroundCompaction:NonTrivial",
      [&](void* /*arg*/) { non_trivial++; });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::BackgroundCompaction:TrivialMove",
      [&](void* /*arg*/) { trivial_move++; });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  Reopen(options);

  Random rnd(301);
  int key_idx = 0;

  // First three 110KB files are going to level 0
  // After that, (100K, 200K)
  for (int num = 0; num < 3; num++) {
    GenerateNewFile(&rnd, &key_idx);
  }

  // Another 110KB triggers a compaction to 400K file to fill up level 0
  GenerateNewFile(&rnd, &key_idx);
  ASSERT_EQ(4, GetSstFileCount(dbname_));

  // (1, 4)
  GenerateNewFile(&rnd, &key_idx);
  ASSERT_EQ("1,4", FilesPerLevel(0));

  // (1, 4, 1)
  GenerateNewFile(&rnd, &key_idx);
  ASSERT_EQ("1,4,1", FilesPerLevel(0));

  // (1, 4, 2)
  GenerateNewFile(&rnd, &key_idx);
  ASSERT_EQ("1,4,2", FilesPerLevel(0));

  // (1, 4, 3)
  GenerateNewFile(&rnd, &key_idx);
  ASSERT_EQ("1,4,3", FilesPerLevel(0));

  // (1, 4, 4)
  GenerateNewFile(&rnd, &key_idx);
  ASSERT_EQ("1,4,4", FilesPerLevel(0));

  // (1, 4, 5)
  GenerateNewFile(&rnd, &key_idx);
  ASSERT_EQ("1,4,5", FilesPerLevel(0));

  // (1, 4, 6)
  GenerateNewFile(&rnd, &key_idx);
  ASSERT_EQ("1,4,6", FilesPerLevel(0));

  // (1, 4, 7)
  GenerateNewFile(&rnd, &key_idx);
  ASSERT_EQ("1,4,7", FilesPerLevel(0));

  // (1, 4, 8)
  GenerateNewFile(&rnd, &key_idx);
  ASSERT_EQ("1,4,8", FilesPerLevel(0));

  ASSERT_EQ(matches, 12);
  // Currently, the test relies on the number of calls to
  // InputCompressionMatchesOutput() per compaction.
  const int kCallsToInputCompressionMatch = 2;
  ASSERT_EQ(didnt_match, 8 * kCallsToInputCompressionMatch);
  ASSERT_EQ(trivial_move, 12);
  ASSERT_EQ(non_trivial, 8);

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();

  for (int i = 0; i < key_idx; i++) {
    auto v = Get(Key(i));
    ASSERT_NE(v, "NOT_FOUND");
    ASSERT_TRUE(v.size() == 1 || v.size() == 990);
  }

  Reopen(options);

  for (int i = 0; i < key_idx; i++) {
    auto v = Get(Key(i));
    ASSERT_NE(v, "NOT_FOUND");
    ASSERT_TRUE(v.size() == 1 || v.size() == 990);
  }

  Destroy(options);
}

TEST_F(DBCompactionTest, SanitizeCompactionOptionsTest) {
  Options options = CurrentOptions();
  options.max_background_compactions = 5;
  options.soft_pending_compaction_bytes_limit = 0;
  options.hard_pending_compaction_bytes_limit = 100;
  options.create_if_missing = true;
  DestroyAndReopen(options);
  ASSERT_EQ(100, db_->GetOptions().soft_pending_compaction_bytes_limit);

  options.max_background_compactions = 3;
  options.soft_pending_compaction_bytes_limit = 200;
  options.hard_pending_compaction_bytes_limit = 150;
  DestroyAndReopen(options);
  ASSERT_EQ(150, db_->GetOptions().soft_pending_compaction_bytes_limit);
}

// This tests for a bug that could cause two level0 compactions running
// concurrently
// TODO(aekmekji): Make sure that the reason this fails when run with
// max_subcompactions > 1 is not a correctness issue but just inherent to
// running parallel L0-L1 compactions
TEST_F(DBCompactionTest, SuggestCompactRangeNoTwoLevel0Compactions) {
  Options options = CurrentOptions();
  options.compaction_style = kCompactionStyleLevel;
  options.write_buffer_size = 110 << 10;
  options.arena_block_size = 4 << 10;
  options.level0_file_num_compaction_trigger = 4;
  options.num_levels = 4;
  options.compression = kNoCompression;
  options.max_bytes_for_level_base = 450 << 10;
  options.target_file_size_base = 98 << 10;
  options.max_write_buffer_number = 2;
  options.max_background_compactions = 2;

  DestroyAndReopen(options);

  // fill up the DB
  Random rnd(301);
  for (int num = 0; num < 10; num++) {
    GenerateNewRandomFile(&rnd);
  }
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency(
      {{"CompactionJob::Run():Start",
        "DBCompactionTest::SuggestCompactRangeNoTwoLevel0Compactions:1"},
       {"DBCompactionTest::SuggestCompactRangeNoTwoLevel0Compactions:2",
        "CompactionJob::Run():End"}});

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  // trigger L0 compaction
  for (int num = 0; num < options.level0_file_num_compaction_trigger + 1;
       num++) {
    GenerateNewRandomFile(&rnd, /* nowait */ true);
    ASSERT_OK(Flush());
  }

  TEST_SYNC_POINT(
      "DBCompactionTest::SuggestCompactRangeNoTwoLevel0Compactions:1");

  GenerateNewRandomFile(&rnd, /* nowait */ true);
  ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());
  ASSERT_OK(experimental::SuggestCompactRange(db_, nullptr, nullptr));
  for (int num = 0; num < options.level0_file_num_compaction_trigger + 1;
       num++) {
    GenerateNewRandomFile(&rnd, /* nowait */ true);
    ASSERT_OK(Flush());
  }

  TEST_SYNC_POINT(
      "DBCompactionTest::SuggestCompactRangeNoTwoLevel0Compactions:2");
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
}

static std::string ShortKey(int i) {
  assert(i < 10000);
  char buf[100];
  snprintf(buf, sizeof(buf), "key%04d", i);
  return std::string(buf);
}

TEST_P(DBCompactionTestWithParam, ForceBottommostLevelCompaction) {
  int32_t trivial_move = 0;
  int32_t non_trivial_move = 0;
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::BackgroundCompaction:TrivialMove",
      [&](void* /*arg*/) { trivial_move++; });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::BackgroundCompaction:NonTrivial",
      [&](void* /*arg*/) { non_trivial_move++; });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  // The key size is guaranteed to be <= 8
  class ShortKeyComparator : public Comparator {
    int Compare(const ROCKSDB_NAMESPACE::Slice& a,
                const ROCKSDB_NAMESPACE::Slice& b) const override {
      assert(a.size() <= 8);
      assert(b.size() <= 8);
      return BytewiseComparator()->Compare(a, b);
    }
    const char* Name() const override { return "ShortKeyComparator"; }
    void FindShortestSeparator(
        std::string* start,
        const ROCKSDB_NAMESPACE::Slice& limit) const override {
      return BytewiseComparator()->FindShortestSeparator(start, limit);
    }
    void FindShortSuccessor(std::string* key) const override {
      return BytewiseComparator()->FindShortSuccessor(key);
    }
  } short_key_cmp;
  Options options = CurrentOptions();
  options.target_file_size_base = 100000000;
  options.write_buffer_size = 100000000;
  options.max_subcompactions = max_subcompactions_;
  options.comparator = &short_key_cmp;
  DestroyAndReopen(options);

  int32_t value_size = 10 * 1024;  // 10 KB

  Random rnd(301);
  std::vector<std::string> values;
  // File with keys [ 0 => 99 ]
  for (int i = 0; i < 100; i++) {
    values.push_back(rnd.RandomString(value_size));
    ASSERT_OK(Put(ShortKey(i), values[i]));
  }
  ASSERT_OK(Flush());

  ASSERT_EQ("1", FilesPerLevel(0));
  // Compaction will do L0=>L1 (trivial move) then move L1 files to L3
  CompactRangeOptions compact_options;
  compact_options.change_level = true;
  compact_options.target_level = 3;
  ASSERT_OK(db_->CompactRange(compact_options, nullptr, nullptr));
  ASSERT_EQ("0,0,0,1", FilesPerLevel(0));
  ASSERT_EQ(trivial_move, 1);
  ASSERT_EQ(non_trivial_move, 0);

  // File with keys [ 100 => 199 ]
  for (int i = 100; i < 200; i++) {
    values.push_back(rnd.RandomString(value_size));
    ASSERT_OK(Put(ShortKey(i), values[i]));
  }
  ASSERT_OK(Flush());

  ASSERT_EQ("1,0,0,1", FilesPerLevel(0));
  // Compaction will do L0=>L1 L1=>L2 L2=>L3 (3 trivial moves)
  // then compacte the bottommost level L3=>L3 (non trivial move)
  compact_options = CompactRangeOptions();
  compact_options.bottommost_level_compaction =
      BottommostLevelCompaction::kForceOptimized;
  ASSERT_OK(db_->CompactRange(compact_options, nullptr, nullptr));
  ASSERT_EQ("0,0,0,1", FilesPerLevel(0));
  ASSERT_EQ(trivial_move, 4);
  ASSERT_EQ(non_trivial_move, 1);

  // File with keys [ 200 => 299 ]
  for (int i = 200; i < 300; i++) {
    values.push_back(rnd.RandomString(value_size));
    ASSERT_OK(Put(ShortKey(i), values[i]));
  }
  ASSERT_OK(Flush());

  ASSERT_EQ("1,0,0,1", FilesPerLevel(0));
  trivial_move = 0;
  non_trivial_move = 0;
  compact_options = CompactRangeOptions();
  compact_options.bottommost_level_compaction =
      BottommostLevelCompaction::kSkip;
  // Compaction will do L0=>L1 L1=>L2 L2=>L3 (3 trivial moves)
  // and will skip bottommost level compaction
  ASSERT_OK(db_->CompactRange(compact_options, nullptr, nullptr));
  ASSERT_EQ("0,0,0,2", FilesPerLevel(0));
  ASSERT_EQ(trivial_move, 3);
  ASSERT_EQ(non_trivial_move, 0);

  for (int i = 0; i < 300; i++) {
    ASSERT_EQ(Get(ShortKey(i)), values[i]);
  }

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
}

TEST_P(DBCompactionTestWithParam, IntraL0Compaction) {
  Options options = CurrentOptions();
  options.compression = kNoCompression;
  options.level0_file_num_compaction_trigger = 5;
  options.max_background_compactions = 2;
  options.max_subcompactions = max_subcompactions_;
  options.statistics = ROCKSDB_NAMESPACE::CreateDBStatistics();
  options.write_buffer_size = 2 << 20;  // 2MB

  BlockBasedTableOptions table_options;
  table_options.block_cache = NewLRUCache(64 << 20);  // 64MB
  table_options.cache_index_and_filter_blocks = true;
  table_options.pin_l0_filter_and_index_blocks_in_cache = true;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));

  DestroyAndReopen(options);

  const size_t kValueSize = 1 << 20;
  Random rnd(301);
  std::string value(rnd.RandomString(kValueSize));

  // The L0->L1 must be picked before we begin flushing files to trigger
  // intra-L0 compaction, and must not finish until after an intra-L0
  // compaction has been picked.
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency(
      {{"LevelCompactionPicker::PickCompaction:Return",
        "DBCompactionTest::IntraL0Compaction:L0ToL1Ready"},
       {"LevelCompactionPicker::PickCompactionBySize:0",
        "CompactionJob::Run():Start"}});
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  // index:   0   1   2   3   4   5   6   7   8   9
  // size:  1MB 1MB 1MB 1MB 1MB 2MB 1MB 1MB 1MB 1MB
  // score:                     1.5 1.3 1.5 2.0 inf
  //
  // Files 0-4 will be included in an L0->L1 compaction.
  //
  // L0->L0 will be triggered since the sync points guarantee compaction to base
  // level is still blocked when files 5-9 trigger another compaction.
  //
  // Files 6-9 are the longest span of available files for which
  // work-per-deleted-file decreases (see "score" row above).
  for (int i = 0; i < 10; ++i) {
    ASSERT_OK(Put(Key(0), ""));  // prevents trivial move
    if (i == 5) {
      TEST_SYNC_POINT("DBCompactionTest::IntraL0Compaction:L0ToL1Ready");
      ASSERT_OK(Put(Key(i + 1), value + value));
    } else {
      ASSERT_OK(Put(Key(i + 1), value));
    }
    ASSERT_OK(Flush());
  }
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();

  std::vector<std::vector<FileMetaData>> level_to_files;
  dbfull()->TEST_GetFilesMetaData(dbfull()->DefaultColumnFamily(),
                                  &level_to_files);
  ASSERT_GE(level_to_files.size(), 2);  // at least L0 and L1
  // L0 has the 2MB file (not compacted) and 4MB file (output of L0->L0)
  ASSERT_EQ(2, level_to_files[0].size());
  ASSERT_GT(level_to_files[1].size(), 0);
  for (int i = 0; i < 2; ++i) {
    ASSERT_GE(level_to_files[0][i].fd.file_size, 1 << 21);
  }

  // The index/filter in the file produced by intra-L0 should not be pinned.
  // That means clearing unref'd entries in block cache and re-accessing the
  // file produced by intra-L0 should bump the index block miss count.
  uint64_t prev_index_misses =
      TestGetTickerCount(options, BLOCK_CACHE_INDEX_MISS);
  table_options.block_cache->EraseUnRefEntries();
  ASSERT_EQ("", Get(Key(0)));
  ASSERT_EQ(prev_index_misses + 1,
            TestGetTickerCount(options, BLOCK_CACHE_INDEX_MISS));
}

TEST_P(DBCompactionTestWithParam, IntraL0CompactionDoesNotObsoleteDeletions) {
  // regression test for issue #2722: L0->L0 compaction can resurrect deleted
  // keys from older L0 files if L1+ files' key-ranges do not include the key.
  Options options = CurrentOptions();
  options.compression = kNoCompression;
  options.level0_file_num_compaction_trigger = 5;
  options.max_background_compactions = 2;
  options.max_subcompactions = max_subcompactions_;
  DestroyAndReopen(options);

  const size_t kValueSize = 1 << 20;
  Random rnd(301);
  std::string value(rnd.RandomString(kValueSize));

  // The L0->L1 must be picked before we begin flushing files to trigger
  // intra-L0 compaction, and must not finish until after an intra-L0
  // compaction has been picked.
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency(
      {{"LevelCompactionPicker::PickCompaction:Return",
        "DBCompactionTest::IntraL0CompactionDoesNotObsoleteDeletions:"
        "L0ToL1Ready"},
       {"LevelCompactionPicker::PickCompactionBySize:0",
        "CompactionJob::Run():Start"}});
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  // index:   0   1   2   3   4    5    6   7   8   9
  // size:  1MB 1MB 1MB 1MB 1MB  1MB  1MB 1MB 1MB 1MB
  // score:                     1.25 1.33 1.5 2.0 inf
  //
  // Files 0-4 will be included in an L0->L1 compaction.
  //
  // L0->L0 will be triggered since the sync points guarantee compaction to base
  // level is still blocked when files 5-9 trigger another compaction. All files
  // 5-9 are included in the L0->L0 due to work-per-deleted file decreasing.
  //
  // Put a key-value in files 0-4. Delete that key in files 5-9. Verify the
  // L0->L0 preserves the deletion such that the key remains deleted.
  for (int i = 0; i < 10; ++i) {
    // key 0 serves both to prevent trivial move and as the key we want to
    // verify is not resurrected by L0->L0 compaction.
    if (i < 5) {
      ASSERT_OK(Put(Key(0), ""));
    } else {
      ASSERT_OK(Delete(Key(0)));
    }
    if (i == 5) {
      TEST_SYNC_POINT(
          "DBCompactionTest::IntraL0CompactionDoesNotObsoleteDeletions:"
          "L0ToL1Ready");
    }
    ASSERT_OK(Put(Key(i + 1), value));
    ASSERT_OK(Flush());
  }
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();

  std::vector<std::vector<FileMetaData>> level_to_files;
  dbfull()->TEST_GetFilesMetaData(dbfull()->DefaultColumnFamily(),
                                  &level_to_files);
  ASSERT_GE(level_to_files.size(), 2);  // at least L0 and L1
  // L0 has a single output file from L0->L0
  ASSERT_EQ(1, level_to_files[0].size());
  ASSERT_GT(level_to_files[1].size(), 0);
  ASSERT_GE(level_to_files[0][0].fd.file_size, 1 << 22);

  ReadOptions roptions;
  std::string result;
  ASSERT_TRUE(db_->Get(roptions, Key(0), &result).IsNotFound());
}

TEST_P(DBCompactionTestWithParam, FullCompactionInBottomPriThreadPool) {
  const int kNumFilesTrigger = 3;
  Env::Default()->SetBackgroundThreads(1, Env::Priority::BOTTOM);
  for (bool use_universal_compaction : {false, true}) {
    Options options = CurrentOptions();
    if (use_universal_compaction) {
      options.compaction_style = kCompactionStyleUniversal;
    } else {
      options.compaction_style = kCompactionStyleLevel;
      options.level_compaction_dynamic_level_bytes = true;
    }
    options.num_levels = 4;
    options.write_buffer_size = 100 << 10;     // 100KB
    options.target_file_size_base = 32 << 10;  // 32KB
    options.level0_file_num_compaction_trigger = kNumFilesTrigger;
    // Trigger compaction if size amplification exceeds 110%
    options.compaction_options_universal.max_size_amplification_percent = 110;
    DestroyAndReopen(options);

    int num_bottom_pri_compactions = 0;
    SyncPoint::GetInstance()->SetCallBack(
        "DBImpl::BGWorkBottomCompaction",
        [&](void* /*arg*/) { ++num_bottom_pri_compactions; });
    SyncPoint::GetInstance()->EnableProcessing();

    Random rnd(301);
    for (int num = 0; num < kNumFilesTrigger; num++) {
      ASSERT_EQ(NumSortedRuns(), num);
      int key_idx = 0;
      GenerateNewFile(&rnd, &key_idx);
    }
    ASSERT_OK(dbfull()->TEST_WaitForCompact());

    ASSERT_EQ(1, num_bottom_pri_compactions);

    // Verify that size amplification did occur
    ASSERT_EQ(NumSortedRuns(), 1);
    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
  }
  Env::Default()->SetBackgroundThreads(0, Env::Priority::BOTTOM);
}

TEST_F(DBCompactionTest, OptimizedDeletionObsoleting) {
  // Deletions can be dropped when compacted to non-last level if they fall
  // outside the lower-level files' key-ranges.
  const int kNumL0Files = 4;
  Options options = CurrentOptions();
  options.level0_file_num_compaction_trigger = kNumL0Files;
  options.statistics = ROCKSDB_NAMESPACE::CreateDBStatistics();
  DestroyAndReopen(options);

  // put key 1 and 3 in separate L1, L2 files.
  // So key 0, 2, and 4+ fall outside these levels' key-ranges.
  for (int level = 2; level >= 1; --level) {
    for (int i = 0; i < 2; ++i) {
      ASSERT_OK(Put(Key(2 * i + 1), "val"));
      ASSERT_OK(Flush());
    }
    MoveFilesToLevel(level);
    ASSERT_EQ(2, NumTableFilesAtLevel(level));
  }

  // Delete keys in range [1, 4]. These L0 files will be compacted with L1:
  // - Tombstones for keys 2 and 4 can be dropped early.
  // - Tombstones for keys 1 and 3 must be kept due to L2 files' key-ranges.
  for (int i = 0; i < kNumL0Files; ++i) {
    ASSERT_OK(Put(Key(0), "val"));  // sentinel to prevent trivial move
    ASSERT_OK(Delete(Key(i + 1)));
    ASSERT_OK(Flush());
  }
  ASSERT_OK(dbfull()->TEST_WaitForCompact());

  for (int i = 0; i < kNumL0Files; ++i) {
    std::string value;
    ASSERT_TRUE(db_->Get(ReadOptions(), Key(i + 1), &value).IsNotFound());
  }
  ASSERT_EQ(2, options.statistics->getTickerCount(
                   COMPACTION_OPTIMIZED_DEL_DROP_OBSOLETE));
  ASSERT_EQ(2,
            options.statistics->getTickerCount(COMPACTION_KEY_DROP_OBSOLETE));
}

TEST_F(DBCompactionTest, CompactFilesPendingL0Bug) {
  // https://www.facebook.com/groups/rocksdb.dev/permalink/1389452781153232/
  // CompactFiles() had a bug where it failed to pick a compaction when an L0
  // compaction existed, but marked it as scheduled anyways. It'd never be
  // unmarked as scheduled, so future compactions or DB close could hang.
  const int kNumL0Files = 5;
  Options options = CurrentOptions();
  options.level0_file_num_compaction_trigger = kNumL0Files - 1;
  options.max_background_compactions = 2;
  DestroyAndReopen(options);

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency(
      {{"LevelCompactionPicker::PickCompaction:Return",
        "DBCompactionTest::CompactFilesPendingL0Bug:Picked"},
       {"DBCompactionTest::CompactFilesPendingL0Bug:ManualCompacted",
        "DBImpl::BackgroundCompaction:NonTrivial:AfterRun"}});
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  auto schedule_multi_compaction_token =
      dbfull()->TEST_write_controler().GetCompactionPressureToken();

  // Files 0-3 will be included in an L0->L1 compaction.
  //
  // File 4 will be included in a call to CompactFiles() while the first
  // compaction is running.
  for (int i = 0; i < kNumL0Files - 1; ++i) {
    ASSERT_OK(Put(Key(0), "val"));  // sentinel to prevent trivial move
    ASSERT_OK(Put(Key(i + 1), "val"));
    ASSERT_OK(Flush());
  }
  TEST_SYNC_POINT("DBCompactionTest::CompactFilesPendingL0Bug:Picked");
  // file 4 flushed after 0-3 picked
  ASSERT_OK(Put(Key(kNumL0Files), "val"));
  ASSERT_OK(Flush());

  // previously DB close would hang forever as this situation caused scheduled
  // compactions count to never decrement to zero.
  ColumnFamilyMetaData cf_meta;
  dbfull()->GetColumnFamilyMetaData(dbfull()->DefaultColumnFamily(), &cf_meta);
  ASSERT_EQ(kNumL0Files, cf_meta.levels[0].files.size());
  std::vector<std::string> input_filenames;
  input_filenames.push_back(cf_meta.levels[0].files.front().name);
  ASSERT_OK(dbfull()
                  ->CompactFiles(CompactionOptions(), input_filenames,
                                 0 /* output_level */));
  TEST_SYNC_POINT("DBCompactionTest::CompactFilesPendingL0Bug:ManualCompacted");
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
}

TEST_F(DBCompactionTest, CompactFilesOverlapInL0Bug) {
  // Regression test for bug of not pulling in L0 files that overlap the user-
  // specified input files in time- and key-ranges.
  ASSERT_OK(Put(Key(0), "old_val"));
  ASSERT_OK(Flush());
  ASSERT_OK(Put(Key(0), "new_val"));
  ASSERT_OK(Flush());

  ColumnFamilyMetaData cf_meta;
  dbfull()->GetColumnFamilyMetaData(dbfull()->DefaultColumnFamily(), &cf_meta);
  ASSERT_GE(cf_meta.levels.size(), 2);
  ASSERT_EQ(2, cf_meta.levels[0].files.size());

  // Compacting {new L0 file, L1 file} should pull in the old L0 file since it
  // overlaps in key-range and time-range.
  std::vector<std::string> input_filenames;
  input_filenames.push_back(cf_meta.levels[0].files.front().name);
  ASSERT_OK(dbfull()->CompactFiles(CompactionOptions(), input_filenames,
                                   1 /* output_level */));
  ASSERT_EQ("new_val", Get(Key(0)));
}

TEST_F(DBCompactionTest, DeleteFilesInRangeConflictWithCompaction) {
  Options options = CurrentOptions();
  DestroyAndReopen(options);
  const Snapshot* snapshot = nullptr;
  const int kMaxKey = 10;

  for (int i = 0; i < kMaxKey; i++) {
    ASSERT_OK(Put(Key(i), Key(i)));
    ASSERT_OK(Delete(Key(i)));
    if (!snapshot) {
      snapshot = db_->GetSnapshot();
    }
  }
  ASSERT_OK(Flush());
  MoveFilesToLevel(1);
  ASSERT_OK(Put(Key(kMaxKey), Key(kMaxKey)));
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  // test DeleteFilesInRange() deletes the files already picked for compaction
  SyncPoint::GetInstance()->LoadDependency(
      {{"VersionSet::LogAndApply:WriteManifestStart",
        "BackgroundCallCompaction:0"},
       {"DBImpl::BackgroundCompaction:Finish",
        "VersionSet::LogAndApply:WriteManifestDone"}});
  SyncPoint::GetInstance()->EnableProcessing();

  // release snapshot which mark bottommost file for compaction
  db_->ReleaseSnapshot(snapshot);
  std::string begin_string = Key(0);
  std::string end_string = Key(kMaxKey + 1);
  Slice begin(begin_string);
  Slice end(end_string);
  ASSERT_OK(DeleteFilesInRange(db_, db_->DefaultColumnFamily(), &begin, &end));
  SyncPoint::GetInstance()->DisableProcessing();
}

TEST_F(DBCompactionTest, CompactBottomLevelFilesWithDeletions) {
  // bottom-level files may contain deletions due to snapshots protecting the
  // deleted keys. Once the snapshot is released, we should see files with many
  // such deletions undergo single-file compactions.
  const int kNumKeysPerFile = 1024;
  const int kNumLevelFiles = 4;
  const int kValueSize = 128;
  Options options = CurrentOptions();
  options.compression = kNoCompression;
  options.level0_file_num_compaction_trigger = kNumLevelFiles;
  // inflate it a bit to account for key/metadata overhead
  options.target_file_size_base = 120 * kNumKeysPerFile * kValueSize / 100;
  CreateAndReopenWithCF({"one"}, options);

  Random rnd(301);
  const Snapshot* snapshot = nullptr;
  for (int i = 0; i < kNumLevelFiles; ++i) {
    for (int j = 0; j < kNumKeysPerFile; ++j) {
      ASSERT_OK(
          Put(Key(i * kNumKeysPerFile + j), rnd.RandomString(kValueSize)));
    }
    if (i == kNumLevelFiles - 1) {
      snapshot = db_->GetSnapshot();
      // delete every other key after grabbing a snapshot, so these deletions
      // and the keys they cover can't be dropped until after the snapshot is
      // released.
      for (int j = 0; j < kNumLevelFiles * kNumKeysPerFile; j += 2) {
        ASSERT_OK(Delete(Key(j)));
      }
    }
    ASSERT_OK(Flush());
    if (i < kNumLevelFiles - 1) {
      ASSERT_EQ(i + 1, NumTableFilesAtLevel(0));
    }
  }
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  ASSERT_EQ(kNumLevelFiles, NumTableFilesAtLevel(1));

  std::vector<LiveFileMetaData> pre_release_metadata, post_release_metadata;
  db_->GetLiveFilesMetaData(&pre_release_metadata);
  // just need to bump seqnum so ReleaseSnapshot knows the newest key in the SST
  // files does not need to be preserved in case of a future snapshot.
  ASSERT_OK(Put(Key(0), "val"));
  ASSERT_NE(kMaxSequenceNumber, dbfull()->bottommost_files_mark_threshold_);
  // release snapshot and wait for compactions to finish. Single-file
  // compactions should be triggered, which reduce the size of each bottom-level
  // file without changing file count.
  db_->ReleaseSnapshot(snapshot);
  ASSERT_EQ(kMaxSequenceNumber, dbfull()->bottommost_files_mark_threshold_);
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "LevelCompactionPicker::PickCompaction:Return", [&](void* arg) {
        Compaction* compaction = reinterpret_cast<Compaction*>(arg);
        ASSERT_TRUE(compaction->compaction_reason() ==
                    CompactionReason::kBottommostFiles);
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  db_->GetLiveFilesMetaData(&post_release_metadata);
  ASSERT_EQ(pre_release_metadata.size(), post_release_metadata.size());

  for (size_t i = 0; i < pre_release_metadata.size(); ++i) {
    const auto& pre_file = pre_release_metadata[i];
    const auto& post_file = post_release_metadata[i];
    ASSERT_EQ(1, pre_file.level);
    ASSERT_EQ(1, post_file.level);
    // each file is smaller than it was before as it was rewritten without
    // deletion markers/deleted keys.
    ASSERT_LT(post_file.size, pre_file.size);
  }
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
}

TEST_F(DBCompactionTest, NoCompactBottomLevelFilesWithDeletions) {
  // bottom-level files may contain deletions due to snapshots protecting the
  // deleted keys. Once the snapshot is released, we should see files with many
  // such deletions undergo single-file compactions. But when disabling auto
  // compactions, it shouldn't be triggered which may causing too many
  // background jobs.
  const int kNumKeysPerFile = 1024;
  const int kNumLevelFiles = 4;
  const int kValueSize = 128;
  Options options = CurrentOptions();
  options.compression = kNoCompression;
  options.disable_auto_compactions = true;
  options.level0_file_num_compaction_trigger = kNumLevelFiles;
  // inflate it a bit to account for key/metadata overhead
  options.target_file_size_base = 120 * kNumKeysPerFile * kValueSize / 100;
  Reopen(options);

  Random rnd(301);
  const Snapshot* snapshot = nullptr;
  for (int i = 0; i < kNumLevelFiles; ++i) {
    for (int j = 0; j < kNumKeysPerFile; ++j) {
      ASSERT_OK(
          Put(Key(i * kNumKeysPerFile + j), rnd.RandomString(kValueSize)));
    }
    if (i == kNumLevelFiles - 1) {
      snapshot = db_->GetSnapshot();
      // delete every other key after grabbing a snapshot, so these deletions
      // and the keys they cover can't be dropped until after the snapshot is
      // released.
      for (int j = 0; j < kNumLevelFiles * kNumKeysPerFile; j += 2) {
        ASSERT_OK(Delete(Key(j)));
      }
    }
    ASSERT_OK(Flush());
    if (i < kNumLevelFiles - 1) {
      ASSERT_EQ(i + 1, NumTableFilesAtLevel(0));
    }
  }
  ASSERT_OK(dbfull()->TEST_CompactRange(0, nullptr, nullptr, nullptr));
  ASSERT_EQ(kNumLevelFiles, NumTableFilesAtLevel(1));

  std::vector<LiveFileMetaData> pre_release_metadata, post_release_metadata;
  db_->GetLiveFilesMetaData(&pre_release_metadata);
  // just need to bump seqnum so ReleaseSnapshot knows the newest key in the SST
  // files does not need to be preserved in case of a future snapshot.
  ASSERT_OK(Put(Key(0), "val"));

  // release snapshot and no compaction should be triggered.
  std::atomic<int> num_compactions{0};
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::BackgroundCompaction:Start",
      [&](void* /*arg*/) { num_compactions.fetch_add(1); });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();
  db_->ReleaseSnapshot(snapshot);
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  ASSERT_EQ(0, num_compactions);
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();

  db_->GetLiveFilesMetaData(&post_release_metadata);
  ASSERT_EQ(pre_release_metadata.size(), post_release_metadata.size());
  for (size_t i = 0; i < pre_release_metadata.size(); ++i) {
    const auto& pre_file = pre_release_metadata[i];
    const auto& post_file = post_release_metadata[i];
    ASSERT_EQ(1, pre_file.level);
    ASSERT_EQ(1, post_file.level);
    // each file is same as before with deletion markers/deleted keys.
    ASSERT_EQ(post_file.size, pre_file.size);
  }
}

TEST_F(DBCompactionTest, LevelCompactExpiredTtlFiles) {
  const int kNumKeysPerFile = 32;
  const int kNumLevelFiles = 2;
  const int kValueSize = 1024;

  Options options = CurrentOptions();
  options.compression = kNoCompression;
  options.ttl = 24 * 60 * 60;  // 24 hours
  options.max_open_files = -1;
  env_->SetMockSleep();
  options.env = env_;

  // NOTE: Presumed unnecessary and removed: resetting mock time in env

  DestroyAndReopen(options);

  Random rnd(301);
  for (int i = 0; i < kNumLevelFiles; ++i) {
    for (int j = 0; j < kNumKeysPerFile; ++j) {
      ASSERT_OK(
          Put(Key(i * kNumKeysPerFile + j), rnd.RandomString(kValueSize)));
    }
    ASSERT_OK(Flush());
  }
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  MoveFilesToLevel(3);
  ASSERT_EQ("0,0,0,2", FilesPerLevel());

  // Delete previously written keys.
  for (int i = 0; i < kNumLevelFiles; ++i) {
    for (int j = 0; j < kNumKeysPerFile; ++j) {
      ASSERT_OK(Delete(Key(i * kNumKeysPerFile + j)));
    }
    ASSERT_OK(Flush());
  }
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  ASSERT_EQ("2,0,0,2", FilesPerLevel());
  MoveFilesToLevel(1);
  ASSERT_EQ("0,2,0,2", FilesPerLevel());

  env_->MockSleepForSeconds(36 * 60 * 60);  // 36 hours
  ASSERT_EQ("0,2,0,2", FilesPerLevel());

  // Just do a simple write + flush so that the Ttl expired files get
  // compacted.
  ASSERT_OK(Put("a", "1"));
  ASSERT_OK(Flush());
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "LevelCompactionPicker::PickCompaction:Return", [&](void* arg) {
        Compaction* compaction = reinterpret_cast<Compaction*>(arg);
        ASSERT_TRUE(compaction->compaction_reason() == CompactionReason::kTtl);
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  // All non-L0 files are deleted, as they contained only deleted data.
  ASSERT_EQ("1", FilesPerLevel());
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();

  // Test dynamically changing ttl.

  // NOTE: Presumed unnecessary and removed: resetting mock time in env

  DestroyAndReopen(options);

  for (int i = 0; i < kNumLevelFiles; ++i) {
    for (int j = 0; j < kNumKeysPerFile; ++j) {
      ASSERT_OK(
          Put(Key(i * kNumKeysPerFile + j), rnd.RandomString(kValueSize)));
    }
    ASSERT_OK(Flush());
  }
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  MoveFilesToLevel(3);
  ASSERT_EQ("0,0,0,2", FilesPerLevel());

  // Delete previously written keys.
  for (int i = 0; i < kNumLevelFiles; ++i) {
    for (int j = 0; j < kNumKeysPerFile; ++j) {
      ASSERT_OK(Delete(Key(i * kNumKeysPerFile + j)));
    }
    ASSERT_OK(Flush());
  }
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  ASSERT_EQ("2,0,0,2", FilesPerLevel());
  MoveFilesToLevel(1);
  ASSERT_EQ("0,2,0,2", FilesPerLevel());

  // Move time forward by 12 hours, and make sure that compaction still doesn't
  // trigger as ttl is set to 24 hours.
  env_->MockSleepForSeconds(12 * 60 * 60);
  ASSERT_OK(Put("a", "1"));
  ASSERT_OK(Flush());
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  ASSERT_EQ("1,2,0,2", FilesPerLevel());

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "LevelCompactionPicker::PickCompaction:Return", [&](void* arg) {
        Compaction* compaction = reinterpret_cast<Compaction*>(arg);
        ASSERT_TRUE(compaction->compaction_reason() == CompactionReason::kTtl);
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  // Dynamically change ttl to 10 hours.
  // This should trigger a ttl compaction, as 12 hours have already passed.
  ASSERT_OK(dbfull()->SetOptions({{"ttl", "36000"}}));
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  // All non-L0 files are deleted, as they contained only deleted data.
  ASSERT_EQ("1", FilesPerLevel());
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
}

TEST_F(DBCompactionTest, LevelTtlCascadingCompactions) {
  env_->SetMockSleep();
  const int kValueSize = 100;

  for (bool if_restart : {false, true}) {
    for (bool if_open_all_files : {false, true}) {
      Options options = CurrentOptions();
      options.compression = kNoCompression;
      options.ttl = 24 * 60 * 60;  // 24 hours
      if (if_open_all_files) {
        options.max_open_files = -1;
      } else {
        options.max_open_files = 20;
      }
      // RocksDB sanitize max open files to at least 20. Modify it back.
      ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
          "SanitizeOptions::AfterChangeMaxOpenFiles", [&](void* arg) {
            int* max_open_files = static_cast<int*>(arg);
            *max_open_files = 2;
          });
      // In the case where all files are opened and doing DB restart
      // forcing the oldest ancester time in manifest file to be 0 to
      // simulate the case of reading from an old version.
      ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
          "VersionEdit::EncodeTo:VarintOldestAncesterTime", [&](void* arg) {
            if (if_restart && if_open_all_files) {
              std::string* encoded_fieled = static_cast<std::string*>(arg);
              *encoded_fieled = "";
              PutVarint64(encoded_fieled, 0);
            }
          });

      options.env = env_;

      // NOTE: Presumed unnecessary and removed: resetting mock time in env

      DestroyAndReopen(options);

      int ttl_compactions = 0;
      ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
          "LevelCompactionPicker::PickCompaction:Return", [&](void* arg) {
            Compaction* compaction = reinterpret_cast<Compaction*>(arg);
            auto compaction_reason = compaction->compaction_reason();
            if (compaction_reason == CompactionReason::kTtl) {
              ttl_compactions++;
            }
          });
      ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

      // Add two L6 files with key ranges: [1 .. 100], [101 .. 200].
      Random rnd(301);
      for (int i = 1; i <= 100; ++i) {
        ASSERT_OK(Put(Key(i), rnd.RandomString(kValueSize)));
      }
      ASSERT_OK(Flush());
      // Get the first file's creation time. This will be the oldest file in the
      // DB. Compactions inolving this file's descendents should keep getting
      // this time.
      std::vector<std::vector<FileMetaData>> level_to_files;
      dbfull()->TEST_GetFilesMetaData(dbfull()->DefaultColumnFamily(),
                                      &level_to_files);
      uint64_t oldest_time = level_to_files[0][0].oldest_ancester_time;
      // Add 1 hour and do another flush.
      env_->MockSleepForSeconds(1 * 60 * 60);
      for (int i = 101; i <= 200; ++i) {
        ASSERT_OK(Put(Key(i), rnd.RandomString(kValueSize)));
      }
      ASSERT_OK(Flush());
      MoveFilesToLevel(6);
      ASSERT_EQ("0,0,0,0,0,0,2", FilesPerLevel());

      env_->MockSleepForSeconds(1 * 60 * 60);
      // Add two L4 files with key ranges: [1 .. 50], [51 .. 150].
      for (int i = 1; i <= 50; ++i) {
        ASSERT_OK(Put(Key(i), rnd.RandomString(kValueSize)));
      }
      ASSERT_OK(Flush());
      env_->MockSleepForSeconds(1 * 60 * 60);
      for (int i = 51; i <= 150; ++i) {
        ASSERT_OK(Put(Key(i), rnd.RandomString(kValueSize)));
      }
      ASSERT_OK(Flush());
      MoveFilesToLevel(4);
      ASSERT_EQ("0,0,0,0,2,0,2", FilesPerLevel());

      env_->MockSleepForSeconds(1 * 60 * 60);
      // Add one L1 file with key range: [26, 75].
      for (int i = 26; i <= 75; ++i) {
        ASSERT_OK(Put(Key(i), rnd.RandomString(kValueSize)));
      }
      ASSERT_OK(Flush());
      ASSERT_OK(dbfull()->TEST_WaitForCompact());
      MoveFilesToLevel(1);
      ASSERT_EQ("0,1,0,0,2,0,2", FilesPerLevel());

      // LSM tree:
      // L1:         [26 .. 75]
      // L4:     [1 .. 50][51 ..... 150]
      // L6:     [1 ........ 100][101 .... 200]
      //
      // On TTL expiry, TTL compaction should be initiated on L1 file, and the
      // compactions should keep going on until the key range hits bottom level.
      // In other words: the compaction on this data range "cascasdes" until
      // reaching the bottom level.
      //
      // Order of events on TTL expiry:
      // 1. L1 file falls to L3 via 2 trivial moves which are initiated by the
      // ttl
      //    compaction.
      // 2. A TTL compaction happens between L3 and L4 files. Output file in L4.
      // 3. The new output file from L4 falls to L5 via 1 trival move initiated
      //    by the ttl compaction.
      // 4. A TTL compaction happens between L5 and L6 files. Ouptut in L6.

      // Add 25 hours and do a write
      env_->MockSleepForSeconds(25 * 60 * 60);

      ASSERT_OK(Put(Key(1), "1"));
      if (if_restart) {
        Reopen(options);
      } else {
        ASSERT_OK(Flush());
      }
      ASSERT_OK(dbfull()->TEST_WaitForCompact());
      ASSERT_EQ("1,0,0,0,0,0,1", FilesPerLevel());
      ASSERT_EQ(5, ttl_compactions);

      dbfull()->TEST_GetFilesMetaData(dbfull()->DefaultColumnFamily(),
                                      &level_to_files);
      ASSERT_EQ(oldest_time, level_to_files[6][0].oldest_ancester_time);

      env_->MockSleepForSeconds(25 * 60 * 60);
      ASSERT_OK(Put(Key(2), "1"));
      if (if_restart) {
        Reopen(options);
      } else {
        ASSERT_OK(Flush());
      }
      ASSERT_OK(dbfull()->TEST_WaitForCompact());
      ASSERT_EQ("1,0,0,0,0,0,1", FilesPerLevel());
      ASSERT_GE(ttl_compactions, 6);

      ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
    }
  }
}

TEST_F(DBCompactionTest, LevelPeriodicCompaction) {
  env_->SetMockSleep();
  const int kNumKeysPerFile = 32;
  const int kNumLevelFiles = 2;
  const int kValueSize = 100;

  for (bool if_restart : {false, true}) {
    for (bool if_open_all_files : {false, true}) {
      Options options = CurrentOptions();
      options.periodic_compaction_seconds = 48 * 60 * 60;  // 2 days
      if (if_open_all_files) {
        options.max_open_files = -1;  // needed for ttl compaction
      } else {
        options.max_open_files = 20;
      }
      // RocksDB sanitize max open files to at least 20. Modify it back.
      ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
          "SanitizeOptions::AfterChangeMaxOpenFiles", [&](void* arg) {
            int* max_open_files = static_cast<int*>(arg);
            *max_open_files = 0;
          });
      // In the case where all files are opened and doing DB restart
      // forcing the file creation time in manifest file to be 0 to
      // simulate the case of reading from an old version.
      ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
          "VersionEdit::EncodeTo:VarintFileCreationTime", [&](void* arg) {
            if (if_restart && if_open_all_files) {
              std::string* encoded_fieled = static_cast<std::string*>(arg);
              *encoded_fieled = "";
              PutVarint64(encoded_fieled, 0);
            }
          });

      options.env = env_;

      // NOTE: Presumed unnecessary and removed: resetting mock time in env

      DestroyAndReopen(options);

      int periodic_compactions = 0;
      ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
          "LevelCompactionPicker::PickCompaction:Return", [&](void* arg) {
            Compaction* compaction = reinterpret_cast<Compaction*>(arg);
            auto compaction_reason = compaction->compaction_reason();
            if (compaction_reason == CompactionReason::kPeriodicCompaction) {
              periodic_compactions++;
            }
          });
      ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

      Random rnd(301);
      for (int i = 0; i < kNumLevelFiles; ++i) {
        for (int j = 0; j < kNumKeysPerFile; ++j) {
          ASSERT_OK(
              Put(Key(i * kNumKeysPerFile + j), rnd.RandomString(kValueSize)));
        }
        ASSERT_OK(Flush());
      }
      ASSERT_OK(dbfull()->TEST_WaitForCompact());

      ASSERT_EQ("2", FilesPerLevel());
      ASSERT_EQ(0, periodic_compactions);

      // Add 50 hours and do a write
      env_->MockSleepForSeconds(50 * 60 * 60);
      ASSERT_OK(Put("a", "1"));
      ASSERT_OK(Flush());
      ASSERT_OK(dbfull()->TEST_WaitForCompact());
      // Assert that the files stay in the same level
      ASSERT_EQ("3", FilesPerLevel());
      // The two old files go through the periodic compaction process
      ASSERT_EQ(2, periodic_compactions);

      MoveFilesToLevel(1);
      ASSERT_EQ("0,3", FilesPerLevel());

      // Add another 50 hours and do another write
      env_->MockSleepForSeconds(50 * 60 * 60);
      ASSERT_OK(Put("b", "2"));
      if (if_restart) {
        Reopen(options);
      } else {
        ASSERT_OK(Flush());
      }
      ASSERT_OK(dbfull()->TEST_WaitForCompact());
      ASSERT_EQ("1,3", FilesPerLevel());
      // The three old files now go through the periodic compaction process. 2
      // + 3.
      ASSERT_EQ(5, periodic_compactions);

      // Add another 50 hours and do another write
      env_->MockSleepForSeconds(50 * 60 * 60);
      ASSERT_OK(Put("c", "3"));
      ASSERT_OK(Flush());
      ASSERT_OK(dbfull()->TEST_WaitForCompact());
      ASSERT_EQ("2,3", FilesPerLevel());
      // The four old files now go through the periodic compaction process. 5
      // + 4.
      ASSERT_EQ(9, periodic_compactions);

      ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
    }
  }
}

TEST_F(DBCompactionTest, LevelPeriodicCompactionWithOldDB) {
  // This test makes sure that periodic compactions are working with a DB
  // where file_creation_time of some files is 0.
  // After compactions the new files are created with a valid file_creation_time

  const int kNumKeysPerFile = 32;
  const int kNumFiles = 4;
  const int kValueSize = 100;

  Options options = CurrentOptions();
  env_->SetMockSleep();
  options.env = env_;

  // NOTE: Presumed unnecessary and removed: resetting mock time in env

  DestroyAndReopen(options);

  int periodic_compactions = 0;
  bool set_file_creation_time_to_zero = true;
  bool set_creation_time_to_zero = true;
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "LevelCompactionPicker::PickCompaction:Return", [&](void* arg) {
        Compaction* compaction = reinterpret_cast<Compaction*>(arg);
        auto compaction_reason = compaction->compaction_reason();
        if (compaction_reason == CompactionReason::kPeriodicCompaction) {
          periodic_compactions++;
        }
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "PropertyBlockBuilder::AddTableProperty:Start", [&](void* arg) {
        TableProperties* props = reinterpret_cast<TableProperties*>(arg);
        if (set_file_creation_time_to_zero) {
          props->file_creation_time = 0;
        }
        if (set_creation_time_to_zero) {
          props->creation_time = 0;
        }
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  Random rnd(301);
  for (int i = 0; i < kNumFiles; ++i) {
    for (int j = 0; j < kNumKeysPerFile; ++j) {
      ASSERT_OK(
          Put(Key(i * kNumKeysPerFile + j), rnd.RandomString(kValueSize)));
    }
    ASSERT_OK(Flush());
    // Move the first two files to L2.
    if (i == 1) {
      MoveFilesToLevel(2);
      set_creation_time_to_zero = false;
    }
  }
  ASSERT_OK(dbfull()->TEST_WaitForCompact());

  ASSERT_EQ("2,0,2", FilesPerLevel());
  ASSERT_EQ(0, periodic_compactions);

  Close();

  set_file_creation_time_to_zero = false;
  // Forward the clock by 2 days.
  env_->MockSleepForSeconds(2 * 24 * 60 * 60);
  options.periodic_compaction_seconds = 1 * 24 * 60 * 60;  // 1 day

  Reopen(options);
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  ASSERT_EQ("2,0,2", FilesPerLevel());
  // Make sure that all files go through periodic compaction.
  ASSERT_EQ(kNumFiles, periodic_compactions);

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
}

TEST_F(DBCompactionTest, LevelPeriodicAndTtlCompaction) {
  const int kNumKeysPerFile = 32;
  const int kNumLevelFiles = 2;
  const int kValueSize = 100;

  Options options = CurrentOptions();
  options.ttl = 10 * 60 * 60;  // 10 hours
  options.periodic_compaction_seconds = 48 * 60 * 60;  // 2 days
  options.max_open_files = -1;   // needed for both periodic and ttl compactions
  env_->SetMockSleep();
  options.env = env_;

  // NOTE: Presumed unnecessary and removed: resetting mock time in env

  DestroyAndReopen(options);

  int periodic_compactions = 0;
  int ttl_compactions = 0;
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "LevelCompactionPicker::PickCompaction:Return", [&](void* arg) {
        Compaction* compaction = reinterpret_cast<Compaction*>(arg);
        auto compaction_reason = compaction->compaction_reason();
        if (compaction_reason == CompactionReason::kPeriodicCompaction) {
          periodic_compactions++;
        } else if (compaction_reason == CompactionReason::kTtl) {
          ttl_compactions++;
        }
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  Random rnd(301);
  for (int i = 0; i < kNumLevelFiles; ++i) {
    for (int j = 0; j < kNumKeysPerFile; ++j) {
      ASSERT_OK(
          Put(Key(i * kNumKeysPerFile + j), rnd.RandomString(kValueSize)));
    }
    ASSERT_OK(Flush());
  }
  ASSERT_OK(dbfull()->TEST_WaitForCompact());

  MoveFilesToLevel(3);

  ASSERT_EQ("0,0,0,2", FilesPerLevel());
  ASSERT_EQ(0, periodic_compactions);
  ASSERT_EQ(0, ttl_compactions);

  // Add some time greater than periodic_compaction_time.
  env_->MockSleepForSeconds(50 * 60 * 60);
  ASSERT_OK(Put("a", "1"));
  ASSERT_OK(Flush());
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  // Files in the bottom level go through periodic compactions.
  ASSERT_EQ("1,0,0,2", FilesPerLevel());
  ASSERT_EQ(2, periodic_compactions);
  ASSERT_EQ(0, ttl_compactions);

  // Add a little more time than ttl
  env_->MockSleepForSeconds(11 * 60 * 60);
  ASSERT_OK(Put("b", "1"));
  ASSERT_OK(Flush());
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  // Notice that the previous file in level 1 falls down to the bottom level
  // due to ttl compactions, one level at a time.
  // And bottom level files don't get picked up for ttl compactions.
  ASSERT_EQ("1,0,0,3", FilesPerLevel());
  ASSERT_EQ(2, periodic_compactions);
  ASSERT_EQ(3, ttl_compactions);

  // Add some time greater than periodic_compaction_time.
  env_->MockSleepForSeconds(50 * 60 * 60);
  ASSERT_OK(Put("c", "1"));
  ASSERT_OK(Flush());
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  // Previous L0 file falls one level at a time to bottom level due to ttl.
  // And all 4 bottom files go through periodic compactions.
  ASSERT_EQ("1,0,0,4", FilesPerLevel());
  ASSERT_EQ(6, periodic_compactions);
  ASSERT_EQ(6, ttl_compactions);

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
}

TEST_F(DBCompactionTest, LevelPeriodicCompactionWithCompactionFilters) {
  class TestCompactionFilter : public CompactionFilter {
    const char* Name() const override { return "TestCompactionFilter"; }
  };
  class TestCompactionFilterFactory : public CompactionFilterFactory {
    const char* Name() const override { return "TestCompactionFilterFactory"; }
    std::unique_ptr<CompactionFilter> CreateCompactionFilter(
        const CompactionFilter::Context& /*context*/) override {
      return std::unique_ptr<CompactionFilter>(new TestCompactionFilter());
    }
  };

  const int kNumKeysPerFile = 32;
  const int kNumLevelFiles = 2;
  const int kValueSize = 100;

  Random rnd(301);

  Options options = CurrentOptions();
  TestCompactionFilter test_compaction_filter;
  env_->SetMockSleep();
  options.env = env_;

  // NOTE: Presumed unnecessary and removed: resetting mock time in env

  enum CompactionFilterType {
    kUseCompactionFilter,
    kUseCompactionFilterFactory
  };

  for (CompactionFilterType comp_filter_type :
       {kUseCompactionFilter, kUseCompactionFilterFactory}) {
    // Assert that periodic compactions are not enabled.
    ASSERT_EQ(port::kMaxUint64 - 1, options.periodic_compaction_seconds);

    if (comp_filter_type == kUseCompactionFilter) {
      options.compaction_filter = &test_compaction_filter;
      options.compaction_filter_factory.reset();
    } else if (comp_filter_type == kUseCompactionFilterFactory) {
      options.compaction_filter = nullptr;
      options.compaction_filter_factory.reset(
          new TestCompactionFilterFactory());
    }
    DestroyAndReopen(options);

    // periodic_compaction_seconds should be set to the sanitized value when
    // a compaction filter or a compaction filter factory is used.
    ASSERT_EQ(30 * 24 * 60 * 60,
              dbfull()->GetOptions().periodic_compaction_seconds);

    int periodic_compactions = 0;
    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
        "LevelCompactionPicker::PickCompaction:Return", [&](void* arg) {
          Compaction* compaction = reinterpret_cast<Compaction*>(arg);
          auto compaction_reason = compaction->compaction_reason();
          if (compaction_reason == CompactionReason::kPeriodicCompaction) {
            periodic_compactions++;
          }
        });
    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

    for (int i = 0; i < kNumLevelFiles; ++i) {
      for (int j = 0; j < kNumKeysPerFile; ++j) {
        ASSERT_OK(
            Put(Key(i * kNumKeysPerFile + j), rnd.RandomString(kValueSize)));
      }
      ASSERT_OK(Flush());
    }
    ASSERT_OK(dbfull()->TEST_WaitForCompact());

    ASSERT_EQ("2", FilesPerLevel());
    ASSERT_EQ(0, periodic_compactions);

    // Add 31 days and do a write
    env_->MockSleepForSeconds(31 * 24 * 60 * 60);
    ASSERT_OK(Put("a", "1"));
    ASSERT_OK(Flush());
    ASSERT_OK(dbfull()->TEST_WaitForCompact());
    // Assert that the files stay in the same level
    ASSERT_EQ("3", FilesPerLevel());
    // The two old files go through the periodic compaction process
    ASSERT_EQ(2, periodic_compactions);

    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
  }
}

TEST_F(DBCompactionTest, CompactRangeDelayedByL0FileCount) {
  // Verify that, when `CompactRangeOptions::allow_write_stall == false`, manual
  // compaction only triggers flush after it's sure stall won't be triggered for
  // L0 file count going too high.
  const int kNumL0FilesTrigger = 4;
  const int kNumL0FilesLimit = 8;
  // i == 0: verifies normal case where stall is avoided by delay
  // i == 1: verifies no delay in edge case where stall trigger is same as
  //         compaction trigger, so stall can't be avoided
  for (int i = 0; i < 2; ++i) {
    Options options = CurrentOptions();
    options.level0_slowdown_writes_trigger = kNumL0FilesLimit;
    if (i == 0) {
      options.level0_file_num_compaction_trigger = kNumL0FilesTrigger;
    } else {
      options.level0_file_num_compaction_trigger = kNumL0FilesLimit;
    }
    Reopen(options);

    if (i == 0) {
      // ensure the auto compaction doesn't finish until manual compaction has
      // had a chance to be delayed.
      ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency(
          {{"DBImpl::WaitUntilFlushWouldNotStallWrites:StallWait",
            "CompactionJob::Run():End"}});
    } else {
      // ensure the auto-compaction doesn't finish until manual compaction has
      // continued without delay.
      ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency(
          {{"DBImpl::FlushMemTable:StallWaitDone",
            "CompactionJob::Run():End"}});
    }
    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

    Random rnd(301);
    for (int j = 0; j < kNumL0FilesLimit - 1; ++j) {
      for (int k = 0; k < 2; ++k) {
        ASSERT_OK(Put(Key(k), rnd.RandomString(1024)));
      }
      ASSERT_OK(Flush());
    }
    auto manual_compaction_thread = port::Thread([this]() {
      CompactRangeOptions cro;
      cro.allow_write_stall = false;
      ASSERT_OK(db_->CompactRange(cro, nullptr, nullptr));
    });

    manual_compaction_thread.join();
    ASSERT_OK(dbfull()->TEST_WaitForCompact());
    ASSERT_EQ(0, NumTableFilesAtLevel(0));
    ASSERT_GT(NumTableFilesAtLevel(1), 0);
    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
  }
}

TEST_F(DBCompactionTest, CompactRangeDelayedByImmMemTableCount) {
  // Verify that, when `CompactRangeOptions::allow_write_stall == false`, manual
  // compaction only triggers flush after it's sure stall won't be triggered for
  // immutable memtable count going too high.
  const int kNumImmMemTableLimit = 8;
  // i == 0: verifies normal case where stall is avoided by delay
  // i == 1: verifies no delay in edge case where stall trigger is same as flush
  //         trigger, so stall can't be avoided
  for (int i = 0; i < 2; ++i) {
    Options options = CurrentOptions();
    options.disable_auto_compactions = true;
    // the delay limit is one less than the stop limit. This test focuses on
    // avoiding delay limit, but this option sets stop limit, so add one.
    options.max_write_buffer_number = kNumImmMemTableLimit + 1;
    if (i == 1) {
      options.min_write_buffer_number_to_merge = kNumImmMemTableLimit;
    }
    Reopen(options);

    if (i == 0) {
      // ensure the flush doesn't finish until manual compaction has had a
      // chance to be delayed.
      ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency(
          {{"DBImpl::WaitUntilFlushWouldNotStallWrites:StallWait",
            "FlushJob::WriteLevel0Table"}});
    } else {
      // ensure the flush doesn't finish until manual compaction has continued
      // without delay.
      ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency(
          {{"DBImpl::FlushMemTable:StallWaitDone",
            "FlushJob::WriteLevel0Table"}});
    }
    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

    Random rnd(301);
    for (int j = 0; j < kNumImmMemTableLimit - 1; ++j) {
      ASSERT_OK(Put(Key(0), rnd.RandomString(1024)));
      FlushOptions flush_opts;
      flush_opts.wait = false;
      flush_opts.allow_write_stall = true;
      ASSERT_OK(dbfull()->Flush(flush_opts));
    }

    auto manual_compaction_thread = port::Thread([this]() {
      CompactRangeOptions cro;
      cro.allow_write_stall = false;
      ASSERT_OK(db_->CompactRange(cro, nullptr, nullptr));
    });

    manual_compaction_thread.join();
    ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());
    ASSERT_EQ(0, NumTableFilesAtLevel(0));
    ASSERT_GT(NumTableFilesAtLevel(1), 0);
    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
  }
}

TEST_F(DBCompactionTest, CompactRangeShutdownWhileDelayed) {
  // Verify that, when `CompactRangeOptions::allow_write_stall == false`, delay
  // does not hang if CF is dropped or DB is closed
  const int kNumL0FilesTrigger = 4;
  const int kNumL0FilesLimit = 8;
  Options options = CurrentOptions();
  options.level0_file_num_compaction_trigger = kNumL0FilesTrigger;
  options.level0_slowdown_writes_trigger = kNumL0FilesLimit;
  // i == 0: DB::DropColumnFamily() on CompactRange's target CF unblocks it
  // i == 1: DB::CancelAllBackgroundWork() unblocks CompactRange. This is to
  //         simulate what happens during Close as we can't call Close (it
  //         blocks on the auto-compaction, making a cycle).
  for (int i = 0; i < 2; ++i) {
    CreateAndReopenWithCF({"one"}, options);
    // The calls to close CF/DB wait until the manual compaction stalls.
    // The auto-compaction waits until the manual compaction finishes to ensure
    // the signal comes from closing CF/DB, not from compaction making progress.
    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency(
        {{"DBImpl::WaitUntilFlushWouldNotStallWrites:StallWait",
          "DBCompactionTest::CompactRangeShutdownWhileDelayed:PreShutdown"},
         {"DBCompactionTest::CompactRangeShutdownWhileDelayed:PostManual",
          "CompactionJob::Run():End"}});
    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

    Random rnd(301);
    for (int j = 0; j < kNumL0FilesLimit - 1; ++j) {
      for (int k = 0; k < 2; ++k) {
        ASSERT_OK(Put(1, Key(k), rnd.RandomString(1024)));
      }
      ASSERT_OK(Flush(1));
    }
    auto manual_compaction_thread = port::Thread([this, i]() {
      CompactRangeOptions cro;
      cro.allow_write_stall = false;
      if (i == 0) {
        ASSERT_TRUE(db_->CompactRange(cro, handles_[1], nullptr, nullptr)
                        .IsColumnFamilyDropped());
      } else {
        ASSERT_TRUE(db_->CompactRange(cro, handles_[1], nullptr, nullptr)
                        .IsShutdownInProgress());
      }
    });

    TEST_SYNC_POINT(
        "DBCompactionTest::CompactRangeShutdownWhileDelayed:PreShutdown");
    if (i == 0) {
      ASSERT_OK(db_->DropColumnFamily(handles_[1]));
    } else {
      dbfull()->CancelAllBackgroundWork(false /* wait */);
    }
    manual_compaction_thread.join();
    TEST_SYNC_POINT(
        "DBCompactionTest::CompactRangeShutdownWhileDelayed:PostManual");
    ASSERT_OK(dbfull()->TEST_WaitForCompact());
    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
  }
}

TEST_F(DBCompactionTest, CompactRangeSkipFlushAfterDelay) {
  // Verify that, when `CompactRangeOptions::allow_write_stall == false`,
  // CompactRange skips its flush if the delay is long enough that the memtables
  // existing at the beginning of the call have already been flushed.
  const int kNumL0FilesTrigger = 4;
  const int kNumL0FilesLimit = 8;
  Options options = CurrentOptions();
  options.level0_slowdown_writes_trigger = kNumL0FilesLimit;
  options.level0_file_num_compaction_trigger = kNumL0FilesTrigger;
  Reopen(options);

  Random rnd(301);
  // The manual flush includes the memtable that was active when CompactRange
  // began. So it unblocks CompactRange and precludes its flush. Throughout the
  // test, stall conditions are upheld via high L0 file count.
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency(
      {{"DBImpl::WaitUntilFlushWouldNotStallWrites:StallWait",
        "DBCompactionTest::CompactRangeSkipFlushAfterDelay:PreFlush"},
       {"DBCompactionTest::CompactRangeSkipFlushAfterDelay:PostFlush",
        "DBImpl::FlushMemTable:StallWaitDone"},
       {"DBImpl::FlushMemTable:StallWaitDone", "CompactionJob::Run():End"}});
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  //used for the delayable flushes
  FlushOptions flush_opts;
  flush_opts.allow_write_stall = true;
  for (int i = 0; i < kNumL0FilesLimit - 1; ++i) {
    for (int j = 0; j < 2; ++j) {
      ASSERT_OK(Put(Key(j), rnd.RandomString(1024)));
    }
    ASSERT_OK(dbfull()->Flush(flush_opts));
  }
  auto manual_compaction_thread = port::Thread([this]() {
    CompactRangeOptions cro;
    cro.allow_write_stall = false;
    ASSERT_OK(db_->CompactRange(cro, nullptr, nullptr));
  });

  TEST_SYNC_POINT("DBCompactionTest::CompactRangeSkipFlushAfterDelay:PreFlush");
  ASSERT_OK(Put(ToString(0), rnd.RandomString(1024)));
  ASSERT_OK(dbfull()->Flush(flush_opts));
  ASSERT_OK(Put(ToString(0), rnd.RandomString(1024)));
  TEST_SYNC_POINT("DBCompactionTest::CompactRangeSkipFlushAfterDelay:PostFlush");
  manual_compaction_thread.join();

  // If CompactRange's flush was skipped, the final Put above will still be
  // in the active memtable.
  std::string num_keys_in_memtable;
  ASSERT_TRUE(db_->GetProperty(DB::Properties::kNumEntriesActiveMemTable,
                               &num_keys_in_memtable));
  ASSERT_EQ(ToString(1), num_keys_in_memtable);

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
}

TEST_F(DBCompactionTest, CompactRangeFlushOverlappingMemtable) {
  // Verify memtable only gets flushed if it contains data overlapping the range
  // provided to `CompactRange`. Tests all kinds of overlap/non-overlap.
  const int kNumEndpointKeys = 5;
  std::string keys[kNumEndpointKeys] = {"a", "b", "c", "d", "e"};
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  Reopen(options);

  // One extra iteration for nullptr, which means left side of interval is
  // unbounded.
  for (int i = 0; i <= kNumEndpointKeys; ++i) {
    Slice begin;
    Slice* begin_ptr;
    if (i == 0) {
      begin_ptr = nullptr;
    } else {
      begin = keys[i - 1];
      begin_ptr = &begin;
    }
    // Start at `i` so right endpoint comes after left endpoint. One extra
    // iteration for nullptr, which means right side of interval is unbounded.
    for (int j = std::max(0, i - 1); j <= kNumEndpointKeys; ++j) {
      Slice end;
      Slice* end_ptr;
      if (j == kNumEndpointKeys) {
        end_ptr = nullptr;
      } else {
        end = keys[j];
        end_ptr = &end;
      }
      ASSERT_OK(Put("b", "val"));
      ASSERT_OK(Put("d", "val"));
      CompactRangeOptions compact_range_opts;
      ASSERT_OK(db_->CompactRange(compact_range_opts, begin_ptr, end_ptr));

      uint64_t get_prop_tmp, num_memtable_entries = 0;
      ASSERT_TRUE(db_->GetIntProperty(DB::Properties::kNumEntriesImmMemTables,
                                      &get_prop_tmp));
      num_memtable_entries += get_prop_tmp;
      ASSERT_TRUE(db_->GetIntProperty(DB::Properties::kNumEntriesActiveMemTable,
                                      &get_prop_tmp));
      num_memtable_entries += get_prop_tmp;
      if (begin_ptr == nullptr || end_ptr == nullptr ||
          (i <= 4 && j >= 1 && (begin != "c" || end != "c"))) {
        // In this case `CompactRange`'s range overlapped in some way with the
        // memtable's range, so flush should've happened. Then "b" and "d" won't
        // be in the memtable.
        ASSERT_EQ(0, num_memtable_entries);
      } else {
        ASSERT_EQ(2, num_memtable_entries);
        // flush anyways to prepare for next iteration
        ASSERT_OK(db_->Flush(FlushOptions()));
      }
    }
  }
}

TEST_F(DBCompactionTest, CompactionStatsTest) {
  Options options = CurrentOptions();
  options.level0_file_num_compaction_trigger = 2;
  CompactionStatsCollector* collector = new CompactionStatsCollector();
  options.listeners.emplace_back(collector);
  DestroyAndReopen(options);

  for (int i = 0; i < 32; i++) {
    for (int j = 0; j < 5000; j++) {
      ASSERT_OK(Put(std::to_string(j), std::string(1, 'A')));
    }
    ASSERT_OK(Flush());
    ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());
  }
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  ColumnFamilyHandleImpl* cfh =
      static_cast<ColumnFamilyHandleImpl*>(dbfull()->DefaultColumnFamily());
  ColumnFamilyData* cfd = cfh->cfd();

  VerifyCompactionStats(*cfd, *collector);
}

TEST_F(DBCompactionTest, CompactFilesOutputRangeConflict) {
  // LSM setup:
  // L1:      [ba bz]
  // L2: [a b]       [c d]
  // L3: [a b]       [c d]
  //
  // Thread 1:                        Thread 2:
  // Begin compacting all L2->L3
  //                                  Compact [ba bz] L1->L3
  // End compacting all L2->L3
  //
  // The compaction operation in thread 2 should be disallowed because the range
  // overlaps with the compaction in thread 1, which also covers that range in
  // L3.
  Options options = CurrentOptions();
  FlushedFileCollector* collector = new FlushedFileCollector();
  options.listeners.emplace_back(collector);
  Reopen(options);

  for (int level = 3; level >= 2; --level) {
    ASSERT_OK(Put("a", "val"));
    ASSERT_OK(Put("b", "val"));
    ASSERT_OK(Flush());
    ASSERT_OK(Put("c", "val"));
    ASSERT_OK(Put("d", "val"));
    ASSERT_OK(Flush());
    MoveFilesToLevel(level);
  }
  ASSERT_OK(Put("ba", "val"));
  ASSERT_OK(Put("bz", "val"));
  ASSERT_OK(Flush());
  MoveFilesToLevel(1);

  SyncPoint::GetInstance()->LoadDependency({
      {"CompactFilesImpl:0",
       "DBCompactionTest::CompactFilesOutputRangeConflict:Thread2Begin"},
      {"DBCompactionTest::CompactFilesOutputRangeConflict:Thread2End",
       "CompactFilesImpl:1"},
  });
  SyncPoint::GetInstance()->EnableProcessing();

  auto bg_thread = port::Thread([&]() {
    // Thread 1
    std::vector<std::string> filenames = collector->GetFlushedFiles();
    filenames.pop_back();
    ASSERT_OK(db_->CompactFiles(CompactionOptions(), filenames,
                                3 /* output_level */));
  });

  // Thread 2
  TEST_SYNC_POINT(
      "DBCompactionTest::CompactFilesOutputRangeConflict:Thread2Begin");
  std::string filename = collector->GetFlushedFiles().back();
  ASSERT_FALSE(
      db_->CompactFiles(CompactionOptions(), {filename}, 3 /* output_level */)
          .ok());
  TEST_SYNC_POINT(
      "DBCompactionTest::CompactFilesOutputRangeConflict:Thread2End");

  bg_thread.join();
}

TEST_F(DBCompactionTest, CompactionHasEmptyOutput) {
  Options options = CurrentOptions();
  SstStatsCollector* collector = new SstStatsCollector();
  options.level0_file_num_compaction_trigger = 2;
  options.listeners.emplace_back(collector);
  Reopen(options);

  // Make sure the L0 files overlap to prevent trivial move.
  ASSERT_OK(Put("a", "val"));
  ASSERT_OK(Put("b", "val"));
  ASSERT_OK(Flush());
  ASSERT_OK(Delete("a"));
  ASSERT_OK(Delete("b"));
  ASSERT_OK(Flush());

  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  ASSERT_EQ(NumTableFilesAtLevel(0), 0);
  ASSERT_EQ(NumTableFilesAtLevel(1), 0);

  // Expect one file creation to start for each flush, and zero for compaction
  // since no keys are written.
  ASSERT_EQ(2, collector->num_ssts_creation_started());
}

TEST_F(DBCompactionTest, CompactionLimiter) {
  const int kNumKeysPerFile = 10;
  const int kMaxBackgroundThreads = 64;

  struct CompactionLimiter {
    std::string name;
    int limit_tasks;
    int max_tasks;
    int tasks;
    std::shared_ptr<ConcurrentTaskLimiter> limiter;
  };

  std::vector<CompactionLimiter> limiter_settings;
  limiter_settings.push_back({"limiter_1", 1, 0, 0, nullptr});
  limiter_settings.push_back({"limiter_2", 2, 0, 0, nullptr});
  limiter_settings.push_back({"limiter_3", 3, 0, 0, nullptr});

  for (auto& ls : limiter_settings) {
    ls.limiter.reset(NewConcurrentTaskLimiter(ls.name, ls.limit_tasks));
  }

  std::shared_ptr<ConcurrentTaskLimiter> unique_limiter(
    NewConcurrentTaskLimiter("unique_limiter", -1));

  const char* cf_names[] = {"default", "0", "1", "2", "3", "4", "5",
    "6", "7", "8", "9", "a", "b", "c", "d", "e", "f" };
  const unsigned int cf_count = sizeof cf_names / sizeof cf_names[0];

  std::unordered_map<std::string, CompactionLimiter*> cf_to_limiter;

  Options options = CurrentOptions();
  options.write_buffer_size = 110 * 1024;  // 110KB
  options.arena_block_size = 4096;
  options.num_levels = 3;
  options.level0_file_num_compaction_trigger = 4;
  options.level0_slowdown_writes_trigger = 64;
  options.level0_stop_writes_trigger = 64;
  options.max_background_jobs = kMaxBackgroundThreads; // Enough threads
  options.memtable_factory.reset(new SpecialSkipListFactory(kNumKeysPerFile));
  options.max_write_buffer_number = 10; // Enough memtables
  DestroyAndReopen(options);

  std::vector<Options> option_vector;
  option_vector.reserve(cf_count);

  for (unsigned int cf = 0; cf < cf_count; cf++) {
    ColumnFamilyOptions cf_opt(options);
    if (cf == 0) {
      // "Default" CF does't use compaction limiter
      cf_opt.compaction_thread_limiter = nullptr;
    } else if (cf == 1) {
      // "1" CF uses bypass compaction limiter
      unique_limiter->SetMaxOutstandingTask(-1);
      cf_opt.compaction_thread_limiter = unique_limiter;
    } else {
      // Assign limiter by mod
      auto& ls = limiter_settings[cf % 3];
      cf_opt.compaction_thread_limiter = ls.limiter;
      cf_to_limiter[cf_names[cf]] = &ls;
    }
    option_vector.emplace_back(DBOptions(options), cf_opt);
  }

  for (unsigned int cf = 1; cf < cf_count; cf++) {
    CreateColumnFamilies({cf_names[cf]}, option_vector[cf]);
  }

  ReopenWithColumnFamilies(std::vector<std::string>(cf_names,
                                                    cf_names + cf_count),
                           option_vector);

  port::Mutex mutex;

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::BackgroundCompaction:BeforeCompaction", [&](void* arg) {
        const auto& cf_name = static_cast<ColumnFamilyData*>(arg)->GetName();
        auto iter = cf_to_limiter.find(cf_name);
        if (iter != cf_to_limiter.end()) {
          MutexLock l(&mutex);
          ASSERT_GE(iter->second->limit_tasks, ++iter->second->tasks);
          iter->second->max_tasks =
              std::max(iter->second->max_tasks, iter->second->limit_tasks);
        }
      });

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::BackgroundCompaction:AfterCompaction", [&](void* arg) {
        const auto& cf_name = static_cast<ColumnFamilyData*>(arg)->GetName();
        auto iter = cf_to_limiter.find(cf_name);
        if (iter != cf_to_limiter.end()) {
          MutexLock l(&mutex);
          ASSERT_GE(--iter->second->tasks, 0);
        }
      });

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  // Block all compact threads in thread pool.
  const size_t kTotalFlushTasks = kMaxBackgroundThreads / 4;
  const size_t kTotalCompactTasks = kMaxBackgroundThreads - kTotalFlushTasks;
  env_->SetBackgroundThreads((int)kTotalFlushTasks, Env::HIGH);
  env_->SetBackgroundThreads((int)kTotalCompactTasks, Env::LOW);

  test::SleepingBackgroundTask sleeping_compact_tasks[kTotalCompactTasks];

  // Block all compaction threads in thread pool.
  for (size_t i = 0; i < kTotalCompactTasks; i++) {
    env_->Schedule(&test::SleepingBackgroundTask::DoSleepTask,
                   &sleeping_compact_tasks[i], Env::LOW);
    sleeping_compact_tasks[i].WaitUntilSleeping();
  }

  int keyIndex = 0;

  for (int n = 0; n < options.level0_file_num_compaction_trigger; n++) {
    for (unsigned int cf = 0; cf < cf_count; cf++) {
      for (int i = 0; i < kNumKeysPerFile; i++) {
        ASSERT_OK(Put(cf, Key(keyIndex++), ""));
      }
      // put extra key to trigger flush
      ASSERT_OK(Put(cf, "", ""));
    }

    for (unsigned int cf = 0; cf < cf_count; cf++) {
      ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable(handles_[cf]));
    }
  }

  // Enough L0 files to trigger compaction
  for (unsigned int cf = 0; cf < cf_count; cf++) {
    ASSERT_EQ(NumTableFilesAtLevel(0, cf),
      options.level0_file_num_compaction_trigger);
  }

  // Create more files for one column family, which triggers speed up
  // condition, all compactions will be scheduled.
  for (int num = 0; num < options.level0_file_num_compaction_trigger; num++) {
    for (int i = 0; i < kNumKeysPerFile; i++) {
      ASSERT_OK(Put(0, Key(i), ""));
    }
    // put extra key to trigger flush
    ASSERT_OK(Put(0, "", ""));
    ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable(handles_[0]));
    ASSERT_EQ(options.level0_file_num_compaction_trigger + num + 1,
              NumTableFilesAtLevel(0, 0));
  }

  // All CFs are pending compaction
  ASSERT_EQ(cf_count, env_->GetThreadPoolQueueLen(Env::LOW));

  // Unblock all compaction threads
  for (size_t i = 0; i < kTotalCompactTasks; i++) {
    sleeping_compact_tasks[i].WakeUp();
    sleeping_compact_tasks[i].WaitUntilDone();
  }

  for (unsigned int cf = 0; cf < cf_count; cf++) {
    ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable(handles_[cf]));
  }

  ASSERT_OK(dbfull()->TEST_WaitForCompact());

  // Max outstanding compact tasks reached limit
  for (auto& ls : limiter_settings) {
    ASSERT_EQ(ls.limit_tasks, ls.max_tasks);
    ASSERT_EQ(0, ls.limiter->GetOutstandingTask());
  }

  // test manual compaction under a fully throttled limiter
  int cf_test = 1;
  unique_limiter->SetMaxOutstandingTask(0);

  // flush one more file to cf 1
  for (int i = 0; i < kNumKeysPerFile; i++) {
      ASSERT_OK(Put(cf_test, Key(keyIndex++), ""));
  }
  // put extra key to trigger flush
  ASSERT_OK(Put(cf_test, "", ""));

  ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable(handles_[cf_test]));
  ASSERT_EQ(1, NumTableFilesAtLevel(0, cf_test));

  Compact(cf_test, Key(0), Key(keyIndex));
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
}

INSTANTIATE_TEST_CASE_P(DBCompactionTestWithParam, DBCompactionTestWithParam,
                        ::testing::Values(std::make_tuple(1, true),
                                          std::make_tuple(1, false),
                                          std::make_tuple(4, true),
                                          std::make_tuple(4, false)));

TEST_P(DBCompactionDirectIOTest, DirectIO) {
  Options options = CurrentOptions();
  Destroy(options);
  options.create_if_missing = true;
  options.disable_auto_compactions = true;
  options.use_direct_io_for_flush_and_compaction = GetParam();
  options.env = new MockEnv(Env::Default());
  Reopen(options);
  bool readahead = false;
  SyncPoint::GetInstance()->SetCallBack(
      "CompactionJob::OpenCompactionOutputFile", [&](void* arg) {
        bool* use_direct_writes = static_cast<bool*>(arg);
        ASSERT_EQ(*use_direct_writes,
                  options.use_direct_io_for_flush_and_compaction);
      });
  if (options.use_direct_io_for_flush_and_compaction) {
    SyncPoint::GetInstance()->SetCallBack(
        "SanitizeOptions:direct_io", [&](void* /*arg*/) {
          readahead = true;
        });
  }
  SyncPoint::GetInstance()->EnableProcessing();
  CreateAndReopenWithCF({"pikachu"}, options);
  MakeTables(3, "p", "q", 1);
  ASSERT_EQ("1,1,1", FilesPerLevel(1));
  Compact(1, "p", "q");
  ASSERT_EQ(readahead, options.use_direct_reads);
  ASSERT_EQ("0,0,1", FilesPerLevel(1));
  Destroy(options);
  delete options.env;
}

INSTANTIATE_TEST_CASE_P(DBCompactionDirectIOTest, DBCompactionDirectIOTest,
                        testing::Bool());

class CompactionPriTest : public DBTestBase,
                          public testing::WithParamInterface<uint32_t> {
 public:
  CompactionPriTest()
      : DBTestBase("/compaction_pri_test", /*env_do_fsync=*/true) {
    compaction_pri_ = GetParam();
  }

  // Required if inheriting from testing::WithParamInterface<>
  static void SetUpTestCase() {}
  static void TearDownTestCase() {}

  uint32_t compaction_pri_;
};

TEST_P(CompactionPriTest, Test) {
  Options options = CurrentOptions();
  options.write_buffer_size = 16 * 1024;
  options.compaction_pri = static_cast<CompactionPri>(compaction_pri_);
  options.hard_pending_compaction_bytes_limit = 256 * 1024;
  options.max_bytes_for_level_base = 64 * 1024;
  options.max_bytes_for_level_multiplier = 4;
  options.compression = kNoCompression;

  DestroyAndReopen(options);

  Random rnd(301);
  const int kNKeys = 5000;
  int keys[kNKeys];
  for (int i = 0; i < kNKeys; i++) {
    keys[i] = i;
  }
  RandomShuffle(std::begin(keys), std::end(keys), rnd.Next());

  for (int i = 0; i < kNKeys; i++) {
    ASSERT_OK(Put(Key(keys[i]), rnd.RandomString(102)));
  }

  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  for (int i = 0; i < kNKeys; i++) {
    ASSERT_NE("NOT_FOUND", Get(Key(i)));
  }
}

INSTANTIATE_TEST_CASE_P(
    CompactionPriTest, CompactionPriTest,
    ::testing::Values(CompactionPri::kByCompensatedSize,
                      CompactionPri::kOldestLargestSeqFirst,
                      CompactionPri::kOldestSmallestSeqFirst,
                      CompactionPri::kMinOverlappingRatio));

class NoopMergeOperator : public MergeOperator {
 public:
  NoopMergeOperator() {}

  bool FullMergeV2(const MergeOperationInput& /*merge_in*/,
                   MergeOperationOutput* merge_out) const override {
    std::string val("bar");
    merge_out->new_value = val;
    return true;
  }

  const char* Name() const override { return "Noop"; }
};

TEST_F(DBCompactionTest, PartialManualCompaction) {
  Options opts = CurrentOptions();
  opts.num_levels = 3;
  opts.level0_file_num_compaction_trigger = 10;
  opts.compression = kNoCompression;
  opts.merge_operator.reset(new NoopMergeOperator());
  opts.target_file_size_base = 10240;
  DestroyAndReopen(opts);

  Random rnd(301);
  for (auto i = 0; i < 8; ++i) {
    for (auto j = 0; j < 10; ++j) {
      ASSERT_OK(Merge("foo", rnd.RandomString(1024)));
    }
    ASSERT_OK(Flush());
  }

  MoveFilesToLevel(2);

  std::string prop;
  EXPECT_TRUE(dbfull()->GetProperty(DB::Properties::kLiveSstFilesSize, &prop));
  uint64_t max_compaction_bytes = atoi(prop.c_str()) / 2;
  ASSERT_OK(dbfull()->SetOptions(
      {{"max_compaction_bytes", std::to_string(max_compaction_bytes)}}));

  CompactRangeOptions cro;
  cro.bottommost_level_compaction = BottommostLevelCompaction::kForceOptimized;
  ASSERT_OK(dbfull()->CompactRange(cro, nullptr, nullptr));
}

TEST_F(DBCompactionTest, ManualCompactionFailsInReadOnlyMode) {
  // Regression test for bug where manual compaction hangs forever when the DB
  // is in read-only mode. Verify it now at least returns, despite failing.
  const int kNumL0Files = 4;
  std::unique_ptr<FaultInjectionTestEnv> mock_env(
      new FaultInjectionTestEnv(env_));
  Options opts = CurrentOptions();
  opts.disable_auto_compactions = true;
  opts.env = mock_env.get();
  DestroyAndReopen(opts);

  Random rnd(301);
  for (int i = 0; i < kNumL0Files; ++i) {
    // Make sure files are overlapping in key-range to prevent trivial move.
    ASSERT_OK(Put("key1", rnd.RandomString(1024)));
    ASSERT_OK(Put("key2", rnd.RandomString(1024)));
    ASSERT_OK(Flush());
  }
  ASSERT_EQ(kNumL0Files, NumTableFilesAtLevel(0));

  // Enter read-only mode by failing a write.
  mock_env->SetFilesystemActive(false);
  // Make sure this is outside `CompactRange`'s range so that it doesn't fail
  // early trying to flush memtable.
  ASSERT_NOK(Put("key3", rnd.RandomString(1024)));

  // In the bug scenario, the first manual compaction would fail and forget to
  // unregister itself, causing the second one to hang forever due to conflict
  // with a non-running compaction.
  CompactRangeOptions cro;
  cro.exclusive_manual_compaction = false;
  Slice begin_key("key1");
  Slice end_key("key2");
  ASSERT_NOK(dbfull()->CompactRange(cro, &begin_key, &end_key));
  ASSERT_NOK(dbfull()->CompactRange(cro, &begin_key, &end_key));

  // Close before mock_env destruct.
  Close();
}

// ManualCompactionBottomLevelOptimization tests the bottom level manual
// compaction optimization to skip recompacting files created by Ln-1 to Ln
// compaction
TEST_F(DBCompactionTest, ManualCompactionBottomLevelOptimized) {
  Options opts = CurrentOptions();
  opts.num_levels = 3;
  opts.level0_file_num_compaction_trigger = 5;
  opts.compression = kNoCompression;
  opts.merge_operator.reset(new NoopMergeOperator());
  opts.target_file_size_base = 1024;
  opts.max_bytes_for_level_multiplier = 2;
  opts.disable_auto_compactions = true;
  DestroyAndReopen(opts);
  ColumnFamilyHandleImpl* cfh =
      static_cast<ColumnFamilyHandleImpl*>(dbfull()->DefaultColumnFamily());
  ColumnFamilyData* cfd = cfh->cfd();
  InternalStats* internal_stats_ptr = cfd->internal_stats();
  ASSERT_NE(internal_stats_ptr, nullptr);

  Random rnd(301);
  for (auto i = 0; i < 8; ++i) {
    for (auto j = 0; j < 10; ++j) {
      ASSERT_OK(
          Put("foo" + std::to_string(i * 10 + j), rnd.RandomString(1024)));
    }
    ASSERT_OK(Flush());
  }

  MoveFilesToLevel(2);

  for (auto i = 0; i < 8; ++i) {
    for (auto j = 0; j < 10; ++j) {
      ASSERT_OK(
          Put("bar" + std::to_string(i * 10 + j), rnd.RandomString(1024)));
    }
    ASSERT_OK(Flush());
  }
  const std::vector<InternalStats::CompactionStats>& comp_stats =
      internal_stats_ptr->TEST_GetCompactionStats();
  int num = comp_stats[2].num_input_files_in_output_level;
  ASSERT_EQ(num, 0);

  CompactRangeOptions cro;
  cro.bottommost_level_compaction = BottommostLevelCompaction::kForceOptimized;
  ASSERT_OK(dbfull()->CompactRange(cro, nullptr, nullptr));

  const std::vector<InternalStats::CompactionStats>& comp_stats2 =
      internal_stats_ptr->TEST_GetCompactionStats();
  num = comp_stats2[2].num_input_files_in_output_level;
  ASSERT_EQ(num, 0);
}

TEST_F(DBCompactionTest, ManualCompactionMax) {
  uint64_t l1_avg_size = 0, l2_avg_size = 0;
  auto generate_sst_func = [&]() {
    Random rnd(301);
    for (auto i = 0; i < 100; i++) {
      for (auto j = 0; j < 10; j++) {
        ASSERT_OK(Put(Key(i * 10 + j), rnd.RandomString(1024)));
      }
      ASSERT_OK(Flush());
    }
    MoveFilesToLevel(2);

    for (auto i = 0; i < 10; i++) {
      for (auto j = 0; j < 10; j++) {
        ASSERT_OK(Put(Key(i * 100 + j * 10), rnd.RandomString(1024)));
      }
      ASSERT_OK(Flush());
    }
    MoveFilesToLevel(1);

    std::vector<std::vector<FileMetaData>> level_to_files;
    dbfull()->TEST_GetFilesMetaData(dbfull()->DefaultColumnFamily(),
                                    &level_to_files);

    uint64_t total = 0;
    for (const auto& file : level_to_files[1]) {
      total += file.compensated_file_size;
    }
    l1_avg_size = total / level_to_files[1].size();

    total = 0;
    for (const auto& file : level_to_files[2]) {
      total += file.compensated_file_size;
    }
    l2_avg_size = total / level_to_files[2].size();
  };

  std::atomic_int num_compactions(0);
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::BGWorkCompaction", [&](void* /*arg*/) { ++num_compactions; });
  SyncPoint::GetInstance()->EnableProcessing();

  Options opts = CurrentOptions();
  opts.disable_auto_compactions = true;

  // with default setting (1.6G by default), it should cover all files in 1
  // compaction
  DestroyAndReopen(opts);
  generate_sst_func();
  num_compactions.store(0);
  CompactRangeOptions cro;
  ASSERT_OK(db_->CompactRange(cro, nullptr, nullptr));
  ASSERT_TRUE(num_compactions.load() == 1);

  // split the compaction to 5
  int num_split = 5;
  DestroyAndReopen(opts);
  generate_sst_func();
  uint64_t total_size = (l1_avg_size * 10) + (l2_avg_size * 100);
  opts.max_compaction_bytes = total_size / num_split;
  Reopen(opts);
  num_compactions.store(0);
  ASSERT_OK(db_->CompactRange(cro, nullptr, nullptr));
  ASSERT_TRUE(num_compactions.load() == num_split);

  // very small max_compaction_bytes, it should still move forward
  opts.max_compaction_bytes = l1_avg_size / 2;
  DestroyAndReopen(opts);
  generate_sst_func();
  num_compactions.store(0);
  ASSERT_OK(db_->CompactRange(cro, nullptr, nullptr));
  ASSERT_TRUE(num_compactions.load() > 10);

  // dynamically set the option
  num_split = 2;
  opts.max_compaction_bytes = 0;
  DestroyAndReopen(opts);
  generate_sst_func();
  total_size = (l1_avg_size * 10) + (l2_avg_size * 100);
  Status s = db_->SetOptions(
      {{"max_compaction_bytes", std::to_string(total_size / num_split)}});
  ASSERT_OK(s);

  num_compactions.store(0);
  ASSERT_OK(db_->CompactRange(cro, nullptr, nullptr));
  ASSERT_TRUE(num_compactions.load() == num_split);
}

TEST_F(DBCompactionTest, CompactionDuringShutdown) {
  Options opts = CurrentOptions();
  opts.level0_file_num_compaction_trigger = 2;
  opts.disable_auto_compactions = true;
  DestroyAndReopen(opts);
  ColumnFamilyHandleImpl* cfh =
      static_cast<ColumnFamilyHandleImpl*>(dbfull()->DefaultColumnFamily());
  ColumnFamilyData* cfd = cfh->cfd();
  InternalStats* internal_stats_ptr = cfd->internal_stats();
  ASSERT_NE(internal_stats_ptr, nullptr);

  Random rnd(301);
  for (auto i = 0; i < 2; ++i) {
    for (auto j = 0; j < 10; ++j) {
      ASSERT_OK(
          Put("foo" + std::to_string(i * 10 + j), rnd.RandomString(1024)));
    }
    ASSERT_OK(Flush());
  }

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::BackgroundCompaction:NonTrivial:BeforeRun",
      [&](void* /*arg*/) { dbfull()->shutting_down_.store(true); });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();
  Status s = dbfull()->CompactRange(CompactRangeOptions(), nullptr, nullptr);
  ASSERT_TRUE(s.ok() || s.IsShutdownInProgress());
  ASSERT_OK(dbfull()->error_handler_.GetBGError());
}

// FixFileIngestionCompactionDeadlock tests and verifies that compaction and
// file ingestion do not cause deadlock in the event of write stall triggered
// by number of L0 files reaching level0_stop_writes_trigger.
TEST_P(DBCompactionTestWithParam, FixFileIngestionCompactionDeadlock) {
  const int kNumKeysPerFile = 100;
  // Generate SST files.
  Options options = CurrentOptions();

  // Generate an external SST file containing a single key, i.e. 99
  std::string sst_files_dir = dbname_ + "/sst_files/";
  ASSERT_OK(DestroyDir(env_, sst_files_dir));
  ASSERT_OK(env_->CreateDir(sst_files_dir));
  SstFileWriter sst_writer(EnvOptions(), options);
  const std::string sst_file_path = sst_files_dir + "test.sst";
  ASSERT_OK(sst_writer.Open(sst_file_path));
  ASSERT_OK(sst_writer.Put(Key(kNumKeysPerFile - 1), "value"));
  ASSERT_OK(sst_writer.Finish());

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  SyncPoint::GetInstance()->LoadDependency({
      {"DBImpl::IngestExternalFile:AfterIncIngestFileCounter",
       "BackgroundCallCompaction:0"},
  });
  SyncPoint::GetInstance()->EnableProcessing();

  options.write_buffer_size = 110 << 10;  // 110KB
  options.level0_file_num_compaction_trigger =
      options.level0_stop_writes_trigger;
  options.max_subcompactions = max_subcompactions_;
  options.memtable_factory.reset(new SpecialSkipListFactory(kNumKeysPerFile));
  DestroyAndReopen(options);
  Random rnd(301);

  // Generate level0_stop_writes_trigger L0 files to trigger write stop
  for (int i = 0; i != options.level0_file_num_compaction_trigger; ++i) {
    for (int j = 0; j != kNumKeysPerFile; ++j) {
      ASSERT_OK(Put(Key(j), rnd.RandomString(990)));
    }
    if (0 == i) {
      // When we reach here, the memtables have kNumKeysPerFile keys. Note that
      // flush is not yet triggered. We need to write an extra key so that the
      // write path will call PreprocessWrite and flush the previous key-value
      // pairs to e flushed. After that, there will be the newest key in the
      // memtable, and a bunch of L0 files. Since there is already one key in
      // the memtable, then for i = 1, 2, ..., we do not have to write this
      // extra key to trigger flush.
      ASSERT_OK(Put("", ""));
    }
    ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());
    ASSERT_EQ(NumTableFilesAtLevel(0 /*level*/, 0 /*cf*/), i + 1);
  }
  // When we reach this point, there will be level0_stop_writes_trigger L0
  // files and one extra key (99) in memory, which overlaps with the external
  // SST file. Write stall triggers, and can be cleared only after compaction
  // reduces the number of L0 files.

  // Compaction will also be triggered since we have reached the threshold for
  // auto compaction. Note that compaction may begin after the following file
  // ingestion thread and waits for ingestion to finish.

  // Thread to ingest file with overlapping key range with the current
  // memtable. Consequently ingestion will trigger a flush. The flush MUST
  // proceed without waiting for the write stall condition to clear, otherwise
  // deadlock can happen.
  port::Thread ingestion_thr([&]() {
    IngestExternalFileOptions ifo;
    Status s = db_->IngestExternalFile({sst_file_path}, ifo);
    ASSERT_OK(s);
  });

  // More write to trigger write stop
  ingestion_thr.join();
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  Close();
}

TEST_F(DBCompactionTest, ConsistencyFailTest) {
  Options options = CurrentOptions();
  options.force_consistency_checks = true;
  DestroyAndReopen(options);

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "VersionBuilder::CheckConsistency0", [&](void* arg) {
        auto p =
            reinterpret_cast<std::pair<FileMetaData**, FileMetaData**>*>(arg);
        // just swap the two FileMetaData so that we hit error
        // in CheckConsistency funcion
        FileMetaData* temp = *(p->first);
        *(p->first) = *(p->second);
        *(p->second) = temp;
      });

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  for (int k = 0; k < 2; ++k) {
    ASSERT_OK(Put("foo", "bar"));
    Status s = Flush();
    if (k < 1) {
      ASSERT_OK(s);
    } else {
      ASSERT_TRUE(s.IsCorruption());
    }
  }

  ASSERT_NOK(Put("foo", "bar"));
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
}

TEST_F(DBCompactionTest, ConsistencyFailTest2) {
  Options options = CurrentOptions();
  options.force_consistency_checks = true;
  options.target_file_size_base = 1000;
  options.level0_file_num_compaction_trigger = 2;
  BlockBasedTableOptions bbto;
  bbto.block_size = 400;  // small block size
  options.table_factory.reset(NewBlockBasedTableFactory(bbto));
  DestroyAndReopen(options);

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "VersionBuilder::CheckConsistency1", [&](void* arg) {
        auto p =
            reinterpret_cast<std::pair<FileMetaData**, FileMetaData**>*>(arg);
        // just swap the two FileMetaData so that we hit error
        // in CheckConsistency funcion
        FileMetaData* temp = *(p->first);
        *(p->first) = *(p->second);
        *(p->second) = temp;
      });

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  Random rnd(301);
  std::string value = rnd.RandomString(1000);

  ASSERT_OK(Put("foo1", value));
  ASSERT_OK(Put("z", ""));
  ASSERT_OK(Flush());
  ASSERT_OK(Put("foo2", value));
  ASSERT_OK(Put("z", ""));
  Status s = Flush();
  ASSERT_TRUE(s.ok() || s.IsCorruption());

  // This probably returns non-OK, but we rely on the next Put()
  // to determine the DB is frozen.
  ASSERT_NOK(dbfull()->TEST_WaitForCompact());
  ASSERT_NOK(Put("foo", "bar"));
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
}

void IngestOneKeyValue(DBImpl* db, const std::string& key,
                       const std::string& value, const Options& options) {
  ExternalSstFileInfo info;
  std::string f = test::PerThreadDBPath("sst_file" + key);
  EnvOptions env;
  ROCKSDB_NAMESPACE::SstFileWriter writer(env, options);
  auto s = writer.Open(f);
  ASSERT_OK(s);
  // ASSERT_OK(writer.Put(Key(), ""));
  ASSERT_OK(writer.Put(key, value));

  ASSERT_OK(writer.Finish(&info));
  IngestExternalFileOptions ingest_opt;

  ASSERT_OK(db->IngestExternalFile({info.file_path}, ingest_opt));
}

TEST_P(DBCompactionTestWithParam,
       FlushAfterIntraL0CompactionCheckConsistencyFail) {
  Options options = CurrentOptions();
  options.force_consistency_checks = true;
  options.compression = kNoCompression;
  options.level0_file_num_compaction_trigger = 5;
  options.max_background_compactions = 2;
  options.max_subcompactions = max_subcompactions_;
  DestroyAndReopen(options);

  const size_t kValueSize = 1 << 20;
  Random rnd(301);
  std::atomic<int> pick_intra_l0_count(0);
  std::string value(rnd.RandomString(kValueSize));

  // The L0->L1 must be picked before we begin ingesting files to trigger
  // intra-L0 compaction, and must not finish until after an intra-L0
  // compaction has been picked.
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency(
      {{"LevelCompactionPicker::PickCompaction:Return",
        "DBCompactionTestWithParam::"
        "FlushAfterIntraL0CompactionCheckConsistencyFail:L0ToL1Ready"},
       {"LevelCompactionPicker::PickCompactionBySize:0",
        "CompactionJob::Run():Start"}});
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "FindIntraL0Compaction",
      [&](void* /*arg*/) { pick_intra_l0_count.fetch_add(1); });

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  // prevents trivial move
  for (int i = 0; i < 10; ++i) {
    ASSERT_OK(Put(Key(i), ""));  // prevents trivial move
  }
  ASSERT_OK(Flush());
  Compact("", Key(99));
  ASSERT_EQ(0, NumTableFilesAtLevel(0));

  // Flush 5 L0 sst.
  for (int i = 0; i < 5; ++i) {
    ASSERT_OK(Put(Key(i + 1), value));
    ASSERT_OK(Flush());
  }
  ASSERT_EQ(5, NumTableFilesAtLevel(0));

  // Put one key, to make smallest log sequence number in this memtable is less
  // than sst which would be ingested in next step.
  ASSERT_OK(Put(Key(0), "a"));

  ASSERT_EQ(5, NumTableFilesAtLevel(0));
  TEST_SYNC_POINT(
      "DBCompactionTestWithParam::"
      "FlushAfterIntraL0CompactionCheckConsistencyFail:L0ToL1Ready");

  // Ingest 5 L0 sst. And this files would trigger PickIntraL0Compaction.
  for (int i = 5; i < 10; i++) {
    ASSERT_EQ(i, NumTableFilesAtLevel(0));
    IngestOneKeyValue(dbfull(), Key(i), value, options);
  }

  // Put one key, to make biggest log sequence number in this memtable is bigger
  // than sst which would be ingested in next step.
  ASSERT_OK(Put(Key(2), "b"));
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
  std::vector<std::vector<FileMetaData>> level_to_files;
  dbfull()->TEST_GetFilesMetaData(dbfull()->DefaultColumnFamily(),
                                  &level_to_files);
  ASSERT_GT(level_to_files[0].size(), 0);
  ASSERT_GT(pick_intra_l0_count.load(), 0);

  ASSERT_OK(Flush());
}

TEST_P(DBCompactionTestWithParam,
       IntraL0CompactionAfterFlushCheckConsistencyFail) {
  Options options = CurrentOptions();
  options.force_consistency_checks = true;
  options.compression = kNoCompression;
  options.level0_file_num_compaction_trigger = 5;
  options.max_background_compactions = 2;
  options.max_subcompactions = max_subcompactions_;
  options.write_buffer_size = 2 << 20;
  options.max_write_buffer_number = 6;
  DestroyAndReopen(options);

  const size_t kValueSize = 1 << 20;
  Random rnd(301);
  std::string value(rnd.RandomString(kValueSize));
  std::string value2(rnd.RandomString(kValueSize));
  std::string bigvalue = value + value;

  // prevents trivial move
  for (int i = 0; i < 10; ++i) {
    ASSERT_OK(Put(Key(i), ""));  // prevents trivial move
  }
  ASSERT_OK(Flush());
  Compact("", Key(99));
  ASSERT_EQ(0, NumTableFilesAtLevel(0));

  std::atomic<int> pick_intra_l0_count(0);
  // The L0->L1 must be picked before we begin ingesting files to trigger
  // intra-L0 compaction, and must not finish until after an intra-L0
  // compaction has been picked.
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency(
      {{"LevelCompactionPicker::PickCompaction:Return",
        "DBCompactionTestWithParam::"
        "IntraL0CompactionAfterFlushCheckConsistencyFail:L0ToL1Ready"},
       {"LevelCompactionPicker::PickCompactionBySize:0",
        "CompactionJob::Run():Start"}});
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "FindIntraL0Compaction",
      [&](void* /*arg*/) { pick_intra_l0_count.fetch_add(1); });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();
  // Make 6 L0 sst.
  for (int i = 0; i < 6; ++i) {
    if (i % 2 == 0) {
      IngestOneKeyValue(dbfull(), Key(i), value, options);
    } else {
      ASSERT_OK(Put(Key(i), value));
      ASSERT_OK(Flush());
    }
  }

  ASSERT_EQ(6, NumTableFilesAtLevel(0));

  // Stop run flush job
  env_->SetBackgroundThreads(1, Env::HIGH);
  test::SleepingBackgroundTask sleeping_tasks;
  env_->Schedule(&test::SleepingBackgroundTask::DoSleepTask, &sleeping_tasks,
                 Env::Priority::HIGH);
  sleeping_tasks.WaitUntilSleeping();

  // Put many keys to make memtable request to flush
  for (int i = 0; i < 6; ++i) {
    ASSERT_OK(Put(Key(i), bigvalue));
  }

  ASSERT_EQ(6, NumTableFilesAtLevel(0));
  TEST_SYNC_POINT(
      "DBCompactionTestWithParam::"
      "IntraL0CompactionAfterFlushCheckConsistencyFail:L0ToL1Ready");
  // ingest file to trigger IntraL0Compaction
  for (int i = 6; i < 10; ++i) {
    ASSERT_EQ(i, NumTableFilesAtLevel(0));
    IngestOneKeyValue(dbfull(), Key(i), value2, options);
  }

  // Wake up flush job
  sleeping_tasks.WakeUp();
  sleeping_tasks.WaitUntilDone();
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();

  uint64_t error_count = 0;
  db_->GetIntProperty("rocksdb.background-errors", &error_count);
  ASSERT_EQ(error_count, 0);
  ASSERT_GT(pick_intra_l0_count.load(), 0);
  for (int i = 0; i < 6; ++i) {
    ASSERT_EQ(bigvalue, Get(Key(i)));
  }
  for (int i = 6; i < 10; ++i) {
    ASSERT_EQ(value2, Get(Key(i)));
  }
}

TEST_P(DBCompactionTestWithBottommostParam, SequenceKeysManualCompaction) {
  constexpr int kSstNum = 10;
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);

  // Generate some sst files on level 0 with sequence keys (no overlap)
  for (int i = 0; i < kSstNum; i++) {
    for (int j = 1; j < UCHAR_MAX; j++) {
      auto key = std::string(kSstNum, '\0');
      key[kSstNum - i] += static_cast<char>(j);
      ASSERT_OK(Put(key, std::string(i % 1000, 'A')));
    }
    ASSERT_OK(Flush());
  }
  ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());

  ASSERT_EQ(ToString(kSstNum), FilesPerLevel(0));

  auto cro = CompactRangeOptions();
  cro.bottommost_level_compaction = bottommost_level_compaction_;
  ASSERT_OK(db_->CompactRange(cro, nullptr, nullptr));
  if (bottommost_level_compaction_ == BottommostLevelCompaction::kForce ||
      bottommost_level_compaction_ ==
          BottommostLevelCompaction::kForceOptimized) {
    // Real compaction to compact all sst files from level 0 to 1 file on level
    // 1
    ASSERT_EQ("0,1", FilesPerLevel(0));
  } else {
    // Just trivial move from level 0 -> 1
    ASSERT_EQ("0," + ToString(kSstNum), FilesPerLevel(0));
  }
}

INSTANTIATE_TEST_CASE_P(
    DBCompactionTestWithBottommostParam, DBCompactionTestWithBottommostParam,
    ::testing::Values(BottommostLevelCompaction::kSkip,
                      BottommostLevelCompaction::kIfHaveCompactionFilter,
                      BottommostLevelCompaction::kForce,
                      BottommostLevelCompaction::kForceOptimized));

TEST_F(DBCompactionTest, UpdateLevelSubCompactionTest) {
  Options options = CurrentOptions();
  options.max_subcompactions = 10;
  options.target_file_size_base = 1 << 10;  // 1KB
  DestroyAndReopen(options);

  bool has_compaction = false;
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "LevelCompactionPicker::PickCompaction:Return", [&](void* arg) {
        Compaction* compaction = reinterpret_cast<Compaction*>(arg);
        ASSERT_TRUE(compaction->max_subcompactions() == 10);
        has_compaction = true;
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  ASSERT_TRUE(dbfull()->GetDBOptions().max_subcompactions == 10);
  // Trigger compaction
  for (int i = 0; i < 32; i++) {
    for (int j = 0; j < 5000; j++) {
      ASSERT_OK(Put(std::to_string(j), std::string(1, 'A')));
    }
    ASSERT_OK(Flush());
    ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());
  }
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  ASSERT_TRUE(has_compaction);

  has_compaction = false;
  ASSERT_OK(dbfull()->SetDBOptions({{"max_subcompactions", "2"}}));
  ASSERT_TRUE(dbfull()->GetDBOptions().max_subcompactions == 2);

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "LevelCompactionPicker::PickCompaction:Return", [&](void* arg) {
        Compaction* compaction = reinterpret_cast<Compaction*>(arg);
        ASSERT_TRUE(compaction->max_subcompactions() == 2);
        has_compaction = true;
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  // Trigger compaction
  for (int i = 0; i < 32; i++) {
    for (int j = 0; j < 5000; j++) {
      ASSERT_OK(Put(std::to_string(j), std::string(1, 'A')));
    }
    ASSERT_OK(Flush());
    ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());
  }
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  ASSERT_TRUE(has_compaction);
}

TEST_F(DBCompactionTest, UpdateUniversalSubCompactionTest) {
  Options options = CurrentOptions();
  options.max_subcompactions = 10;
  options.compaction_style = kCompactionStyleUniversal;
  options.target_file_size_base = 1 << 10;  // 1KB
  DestroyAndReopen(options);

  bool has_compaction = false;
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "UniversalCompactionBuilder::PickCompaction:Return", [&](void* arg) {
        Compaction* compaction = reinterpret_cast<Compaction*>(arg);
        ASSERT_TRUE(compaction->max_subcompactions() == 10);
        has_compaction = true;
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  // Trigger compaction
  for (int i = 0; i < 32; i++) {
    for (int j = 0; j < 5000; j++) {
      ASSERT_OK(Put(std::to_string(j), std::string(1, 'A')));
    }
    ASSERT_OK(Flush());
    ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());
  }
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  ASSERT_TRUE(has_compaction);
  has_compaction = false;

  ASSERT_OK(dbfull()->SetDBOptions({{"max_subcompactions", "2"}}));
  ASSERT_TRUE(dbfull()->GetDBOptions().max_subcompactions == 2);

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "UniversalCompactionBuilder::PickCompaction:Return", [&](void* arg) {
        Compaction* compaction = reinterpret_cast<Compaction*>(arg);
        ASSERT_TRUE(compaction->max_subcompactions() == 2);
        has_compaction = true;
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  // Trigger compaction
  for (int i = 0; i < 32; i++) {
    for (int j = 0; j < 5000; j++) {
      ASSERT_OK(Put(std::to_string(j), std::string(1, 'A')));
    }
    ASSERT_OK(Flush());
    ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());
  }
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  ASSERT_TRUE(has_compaction);
}

TEST_P(ChangeLevelConflictsWithAuto, TestConflict) {
  // A `CompactRange()` may race with an automatic compaction, we'll need
  // to make sure it doesn't corrupte the data.
  Options options = CurrentOptions();
  options.level0_file_num_compaction_trigger = 2;
  Reopen(options);

  ASSERT_OK(Put("foo", "v1"));
  ASSERT_OK(Put("bar", "v1"));
  ASSERT_OK(Flush());
  ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());

  {
    CompactRangeOptions cro;
    cro.change_level = true;
    cro.target_level = 2;
    ASSERT_OK(dbfull()->CompactRange(cro, nullptr, nullptr));
  }
  ASSERT_EQ("0,0,1", FilesPerLevel(0));

  // Run a qury to refitting to level 1 while another thread writing to
  // the same level.
  SyncPoint::GetInstance()->LoadDependency({
      // The first two dependencies ensure the foreground creates an L0 file
      // between the background compaction's L0->L1 and its L1->L2.
      {
          "DBImpl::CompactRange:BeforeRefit:1",
          "AutoCompactionFinished1",
      },
      {
          "AutoCompactionFinished2",
          "DBImpl::CompactRange:BeforeRefit:2",
      },
  });
  SyncPoint::GetInstance()->EnableProcessing();

  std::thread auto_comp([&] {
    TEST_SYNC_POINT("AutoCompactionFinished1");
    ASSERT_OK(Put("bar", "v2"));
    ASSERT_OK(Put("foo", "v2"));
    ASSERT_OK(Flush());
    ASSERT_OK(Put("bar", "v3"));
    ASSERT_OK(Put("foo", "v3"));
    ASSERT_OK(Flush());
    ASSERT_OK(dbfull()->TEST_WaitForCompact());
    TEST_SYNC_POINT("AutoCompactionFinished2");
  });

  {
    CompactRangeOptions cro;
    cro.change_level = true;
    cro.target_level = GetParam() ? 1 : 0;
    // This should return non-OK, but it's more important for the test to
    // make sure that the DB is not corrupted.
    ASSERT_NOK(dbfull()->CompactRange(cro, nullptr, nullptr));
  }
  auto_comp.join();
  // Refitting didn't happen.
  SyncPoint::GetInstance()->DisableProcessing();

  // Write something to DB just make sure that consistency check didn't
  // fail and make the DB readable.
}

INSTANTIATE_TEST_CASE_P(ChangeLevelConflictsWithAuto,
                        ChangeLevelConflictsWithAuto, testing::Bool());

TEST_F(DBCompactionTest, ChangeLevelCompactRangeConflictsWithManual) {
  // A `CompactRange()` with `change_level == true` needs to execute its final
  // step, `ReFitLevel()`, in isolation. Previously there was a bug where
  // refitting could target the same level as an ongoing manual compaction,
  // leading to overlapping files in that level.
  //
  // This test ensures that case is not possible by verifying any manual
  // compaction issued during the `ReFitLevel()` phase fails with
  // `Status::Incomplete`.
  Options options = CurrentOptions();
  options.memtable_factory.reset(
      new SpecialSkipListFactory(KNumKeysByGenerateNewFile - 1));
  options.level0_file_num_compaction_trigger = 2;
  options.num_levels = 3;
  Reopen(options);

  // Setup an LSM with three levels populated.
  Random rnd(301);
  int key_idx = 0;
  GenerateNewFile(&rnd, &key_idx);
  {
    CompactRangeOptions cro;
    cro.change_level = true;
    cro.target_level = 2;
    ASSERT_OK(dbfull()->CompactRange(cro, nullptr, nullptr));
  }
  ASSERT_EQ("0,0,2", FilesPerLevel(0));

  GenerateNewFile(&rnd, &key_idx);
  GenerateNewFile(&rnd, &key_idx);
  ASSERT_EQ("1,1,2", FilesPerLevel(0));

  // The background thread will refit L2->L1 while the
  // foreground thread will try to simultaneously compact L0->L1.
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency({
      // The first two dependencies ensure the foreground creates an L0 file
      // between the background compaction's L0->L1 and its L1->L2.
      {
          "DBImpl::RunManualCompaction()::1",
          "DBCompactionTest::ChangeLevelCompactRangeConflictsWithManual:"
          "PutFG",
      },
      {
          "DBCompactionTest::ChangeLevelCompactRangeConflictsWithManual:"
          "FlushedFG",
          "DBImpl::RunManualCompaction()::2",
      },
      // The next two dependencies ensure the foreground invokes
      // `CompactRange()` while the background is refitting. The
      // foreground's `CompactRange()` is guaranteed to attempt an L0->L1
      // as we set it up with an empty memtable and a new L0 file.
      {
          "DBImpl::CompactRange:PreRefitLevel",
          "DBCompactionTest::ChangeLevelCompactRangeConflictsWithManual:"
          "CompactFG",
      },
      {
          "DBCompactionTest::ChangeLevelCompactRangeConflictsWithManual:"
          "CompactedFG",
          "DBImpl::CompactRange:PostRefitLevel",
      },
  });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  ROCKSDB_NAMESPACE::port::Thread refit_level_thread([&] {
    CompactRangeOptions cro;
    cro.change_level = true;
    cro.target_level = 1;
    ASSERT_OK(dbfull()->CompactRange(cro, nullptr, nullptr));
  });

  TEST_SYNC_POINT(
      "DBCompactionTest::ChangeLevelCompactRangeConflictsWithManual:PutFG");
  // Make sure we have something new to compact in the foreground.
  // Note key 1 is carefully chosen as it ensures the file we create here
  // overlaps with one of the files being refitted L2->L1 in the background.
  // If we chose key 0, the file created here would not overlap.
  ASSERT_OK(Put(Key(1), "val"));
  ASSERT_OK(Flush());
  TEST_SYNC_POINT(
      "DBCompactionTest::ChangeLevelCompactRangeConflictsWithManual:FlushedFG");

  TEST_SYNC_POINT(
      "DBCompactionTest::ChangeLevelCompactRangeConflictsWithManual:CompactFG");
  ASSERT_TRUE(dbfull()
                  ->CompactRange(CompactRangeOptions(), nullptr, nullptr)
                  .IsIncomplete());
  TEST_SYNC_POINT(
      "DBCompactionTest::ChangeLevelCompactRangeConflictsWithManual:"
      "CompactedFG");
  refit_level_thread.join();
}

TEST_F(DBCompactionTest, ChangeLevelErrorPathTest) {
  // This test is added to ensure that RefitLevel() error paths are clearing
  // internal flags and to test that subsequent valid RefitLevel() calls
  // succeeds
  Options options = CurrentOptions();
  options.memtable_factory.reset(
      new SpecialSkipListFactory(KNumKeysByGenerateNewFile - 1));
  options.level0_file_num_compaction_trigger = 2;
  options.num_levels = 3;
  Reopen(options);

  ASSERT_EQ("", FilesPerLevel(0));

  // Setup an LSM with three levels populated.
  Random rnd(301);
  int key_idx = 0;
  GenerateNewFile(&rnd, &key_idx);
  ASSERT_EQ("1", FilesPerLevel(0));
  {
    CompactRangeOptions cro;
    cro.change_level = true;
    cro.target_level = 2;
    ASSERT_OK(dbfull()->CompactRange(cro, nullptr, nullptr));
  }
  ASSERT_EQ("0,0,2", FilesPerLevel(0));

  auto start_idx = key_idx;
  GenerateNewFile(&rnd, &key_idx);
  GenerateNewFile(&rnd, &key_idx);
  auto end_idx = key_idx - 1;
  ASSERT_EQ("1,1,2", FilesPerLevel(0));

  // Next two CompactRange() calls are used to test exercise error paths within
  // RefitLevel() before triggering a valid RefitLevel() call

  // Trigger a refit to L1 first
  {
    std::string begin_string = Key(start_idx);
    std::string end_string = Key(end_idx);
    Slice begin(begin_string);
    Slice end(end_string);

    CompactRangeOptions cro;
    cro.change_level = true;
    cro.target_level = 1;
    ASSERT_OK(dbfull()->CompactRange(cro, &begin, &end));
  }
  ASSERT_EQ("0,3,2", FilesPerLevel(0));

  // Try a refit from L2->L1 - this should fail and exercise error paths in
  // RefitLevel()
  {
    // Select key range that matches the bottom most level (L2)
    std::string begin_string = Key(0);
    std::string end_string = Key(start_idx - 1);
    Slice begin(begin_string);
    Slice end(end_string);

    CompactRangeOptions cro;
    cro.change_level = true;
    cro.target_level = 1;
    ASSERT_NOK(dbfull()->CompactRange(cro, &begin, &end));
  }
  ASSERT_EQ("0,3,2", FilesPerLevel(0));

  // Try a valid Refit request to ensure, the path is still working
  {
    CompactRangeOptions cro;
    cro.change_level = true;
    cro.target_level = 1;
    ASSERT_OK(dbfull()->CompactRange(cro, nullptr, nullptr));
  }
  ASSERT_EQ("0,5", FilesPerLevel(0));
}

TEST_F(DBCompactionTest, CompactionWithBlob) {
  Options options;
  options.env = env_;
  options.disable_auto_compactions = true;

  Reopen(options);

  constexpr char first_key[] = "first_key";
  constexpr char second_key[] = "second_key";
  constexpr char first_value[] = "first_value";
  constexpr char second_value[] = "second_value";
  constexpr char third_value[] = "third_value";

  ASSERT_OK(Put(first_key, first_value));
  ASSERT_OK(Put(second_key, first_value));
  ASSERT_OK(Flush());

  ASSERT_OK(Put(first_key, second_value));
  ASSERT_OK(Put(second_key, second_value));
  ASSERT_OK(Flush());

  ASSERT_OK(Put(first_key, third_value));
  ASSERT_OK(Put(second_key, third_value));
  ASSERT_OK(Flush());

  options.enable_blob_files = true;

  Reopen(options);

  constexpr Slice* begin = nullptr;
  constexpr Slice* end = nullptr;

  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), begin, end));

  ASSERT_EQ(Get(first_key), third_value);
  ASSERT_EQ(Get(second_key), third_value);

  VersionSet* const versions = dbfull()->TEST_GetVersionSet();
  assert(versions);

  ColumnFamilyData* const cfd = versions->GetColumnFamilySet()->GetDefault();
  ASSERT_NE(cfd, nullptr);

  Version* const current = cfd->current();
  ASSERT_NE(current, nullptr);

  const VersionStorageInfo* const storage_info = current->storage_info();
  ASSERT_NE(storage_info, nullptr);

  const auto& l1_files = storage_info->LevelFiles(1);
  ASSERT_EQ(l1_files.size(), 1);

  const FileMetaData* const table_file = l1_files[0];
  ASSERT_NE(table_file, nullptr);

  const auto& blob_files = storage_info->GetBlobFiles();
  ASSERT_EQ(blob_files.size(), 1);

  const auto& blob_file = blob_files.begin()->second;
  ASSERT_NE(blob_file, nullptr);

  ASSERT_EQ(table_file->smallest.user_key(), first_key);
  ASSERT_EQ(table_file->largest.user_key(), second_key);
  ASSERT_EQ(table_file->oldest_blob_file_number,
            blob_file->GetBlobFileNumber());

  ASSERT_EQ(blob_file->GetTotalBlobCount(), 2);

  const InternalStats* const internal_stats = cfd->internal_stats();
  ASSERT_NE(internal_stats, nullptr);

  const auto& compaction_stats = internal_stats->TEST_GetCompactionStats();
  ASSERT_GE(compaction_stats.size(), 2);
  ASSERT_EQ(compaction_stats[1].bytes_read_blob, 0);
  ASSERT_EQ(compaction_stats[1].bytes_written, table_file->fd.GetFileSize());
  ASSERT_EQ(compaction_stats[1].bytes_written_blob,
            blob_file->GetTotalBlobBytes());
  ASSERT_EQ(compaction_stats[1].num_output_files, 1);
  ASSERT_EQ(compaction_stats[1].num_output_files_blob, 1);
}

class DBCompactionTestBlobError
    : public DBCompactionTest,
      public testing::WithParamInterface<std::string> {
 public:
  DBCompactionTestBlobError() : sync_point_(GetParam()) {}

  std::string sync_point_;
};

INSTANTIATE_TEST_CASE_P(DBCompactionTestBlobError, DBCompactionTestBlobError,
                        ::testing::ValuesIn(std::vector<std::string>{
                            "BlobFileBuilder::WriteBlobToFile:AddRecord",
                            "BlobFileBuilder::WriteBlobToFile:AppendFooter"}));

TEST_P(DBCompactionTestBlobError, CompactionError) {
  Options options;
  options.disable_auto_compactions = true;
  options.env = env_;

  Reopen(options);

  constexpr char first_key[] = "first_key";
  constexpr char second_key[] = "second_key";
  constexpr char first_value[] = "first_value";
  constexpr char second_value[] = "second_value";
  constexpr char third_value[] = "third_value";

  ASSERT_OK(Put(first_key, first_value));
  ASSERT_OK(Put(second_key, first_value));
  ASSERT_OK(Flush());

  ASSERT_OK(Put(first_key, second_value));
  ASSERT_OK(Put(second_key, second_value));
  ASSERT_OK(Flush());

  ASSERT_OK(Put(first_key, third_value));
  ASSERT_OK(Put(second_key, third_value));
  ASSERT_OK(Flush());

  options.enable_blob_files = true;

  Reopen(options);

  SyncPoint::GetInstance()->SetCallBack(sync_point_, [this](void* arg) {
    Status* const s = static_cast<Status*>(arg);
    assert(s);

    (*s) = Status::IOError(sync_point_);
  });
  SyncPoint::GetInstance()->EnableProcessing();

  constexpr Slice* begin = nullptr;
  constexpr Slice* end = nullptr;

  ASSERT_TRUE(db_->CompactRange(CompactRangeOptions(), begin, end).IsIOError());

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();

  VersionSet* const versions = dbfull()->TEST_GetVersionSet();
  assert(versions);

  ColumnFamilyData* const cfd = versions->GetColumnFamilySet()->GetDefault();
  ASSERT_NE(cfd, nullptr);

  Version* const current = cfd->current();
  ASSERT_NE(current, nullptr);

  const VersionStorageInfo* const storage_info = current->storage_info();
  ASSERT_NE(storage_info, nullptr);

  const auto& l1_files = storage_info->LevelFiles(1);
  ASSERT_TRUE(l1_files.empty());

  const auto& blob_files = storage_info->GetBlobFiles();
  ASSERT_TRUE(blob_files.empty());

  const InternalStats* const internal_stats = cfd->internal_stats();
  ASSERT_NE(internal_stats, nullptr);

  const auto& compaction_stats = internal_stats->TEST_GetCompactionStats();
  ASSERT_GE(compaction_stats.size(), 2);

  if (sync_point_ == "BlobFileBuilder::WriteBlobToFile:AddRecord") {
    ASSERT_EQ(compaction_stats[1].bytes_read_blob, 0);
    ASSERT_EQ(compaction_stats[1].bytes_written, 0);
    ASSERT_EQ(compaction_stats[1].bytes_written_blob, 0);
    ASSERT_EQ(compaction_stats[1].num_output_files, 0);
    ASSERT_EQ(compaction_stats[1].num_output_files_blob, 0);
  } else {
    // SST file writing succeeded; blob file writing failed (during Finish)
    ASSERT_EQ(compaction_stats[1].bytes_read_blob, 0);
    ASSERT_GT(compaction_stats[1].bytes_written, 0);
    ASSERT_EQ(compaction_stats[1].bytes_written_blob, 0);
    ASSERT_EQ(compaction_stats[1].num_output_files, 1);
    ASSERT_EQ(compaction_stats[1].num_output_files_blob, 0);
  }
}

class DBCompactionTestBlobGC
    : public DBCompactionTest,
      public testing::WithParamInterface<std::tuple<double, bool>> {
 public:
  DBCompactionTestBlobGC()
      : blob_gc_age_cutoff_(std::get<0>(GetParam())),
        updated_enable_blob_files_(std::get<1>(GetParam())) {}

  double blob_gc_age_cutoff_;
  bool updated_enable_blob_files_;
};

INSTANTIATE_TEST_CASE_P(DBCompactionTestBlobGC, DBCompactionTestBlobGC,
                        ::testing::Combine(::testing::Values(0.0, 0.5, 1.0),
                                           ::testing::Bool()));

TEST_P(DBCompactionTestBlobGC, CompactionWithBlobGC) {
  Options options;
  options.env = env_;
  options.disable_auto_compactions = true;
  options.enable_blob_files = true;
  options.blob_file_size = 32;  // one blob per file
  options.enable_blob_garbage_collection = true;
  options.blob_garbage_collection_age_cutoff = blob_gc_age_cutoff_;

  Reopen(options);

  constexpr char first_key[] = "first_key";
  constexpr char first_value[] = "first_value";
  constexpr char second_key[] = "second_key";
  constexpr char second_value[] = "second_value";

  ASSERT_OK(Put(first_key, first_value));
  ASSERT_OK(Put(second_key, second_value));
  ASSERT_OK(Flush());

  constexpr char third_key[] = "third_key";
  constexpr char third_value[] = "third_value";
  constexpr char fourth_key[] = "fourth_key";
  constexpr char fourth_value[] = "fourth_value";

  ASSERT_OK(Put(third_key, third_value));
  ASSERT_OK(Put(fourth_key, fourth_value));
  ASSERT_OK(Flush());

  const std::vector<uint64_t> original_blob_files = GetBlobFileNumbers();

  ASSERT_EQ(original_blob_files.size(), 4);

  const size_t cutoff_index = static_cast<size_t>(
      options.blob_garbage_collection_age_cutoff * original_blob_files.size());

  // Note: turning off enable_blob_files before the compaction results in
  // garbage collected values getting inlined.
  size_t expected_number_of_files = original_blob_files.size();

  if (!updated_enable_blob_files_) {
    ASSERT_OK(db_->SetOptions({{"enable_blob_files", "false"}}));

    expected_number_of_files -= cutoff_index;
  }

  constexpr Slice* begin = nullptr;
  constexpr Slice* end = nullptr;

  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), begin, end));

  ASSERT_EQ(Get(first_key), first_value);
  ASSERT_EQ(Get(second_key), second_value);
  ASSERT_EQ(Get(third_key), third_value);
  ASSERT_EQ(Get(fourth_key), fourth_value);

  const std::vector<uint64_t> new_blob_files = GetBlobFileNumbers();

  ASSERT_EQ(new_blob_files.size(), expected_number_of_files);

  // Original blob files below the cutoff should be gone, original blob files at
  // or above the cutoff should be still there
  for (size_t i = cutoff_index; i < original_blob_files.size(); ++i) {
    ASSERT_EQ(new_blob_files[i - cutoff_index], original_blob_files[i]);
  }

  VersionSet* const versions = dbfull()->TEST_GetVersionSet();
  assert(versions);
  assert(versions->GetColumnFamilySet());

  ColumnFamilyData* const cfd = versions->GetColumnFamilySet()->GetDefault();
  assert(cfd);

  const InternalStats* const internal_stats = cfd->internal_stats();
  assert(internal_stats);

  const auto& compaction_stats = internal_stats->TEST_GetCompactionStats();
  ASSERT_GE(compaction_stats.size(), 2);

  if (blob_gc_age_cutoff_ > 0.0) {
    ASSERT_GT(compaction_stats[1].bytes_read_blob, 0);

    if (updated_enable_blob_files_) {
      // GC relocated some blobs to new blob files
      ASSERT_GT(compaction_stats[1].bytes_written_blob, 0);
      ASSERT_EQ(compaction_stats[1].bytes_read_blob,
                compaction_stats[1].bytes_written_blob);
    } else {
      // GC moved some blobs back to the LSM, no new blob files
      ASSERT_EQ(compaction_stats[1].bytes_written_blob, 0);
    }
  } else {
    ASSERT_EQ(compaction_stats[1].bytes_read_blob, 0);
    ASSERT_EQ(compaction_stats[1].bytes_written_blob, 0);
  }
}

TEST_F(DBCompactionTest, CompactionWithBlobGCError_CorruptIndex) {
  Options options;
  options.env = env_;
  options.disable_auto_compactions = true;
  options.enable_blob_files = true;
  options.enable_blob_garbage_collection = true;
  options.blob_garbage_collection_age_cutoff = 1.0;

  Reopen(options);

  constexpr char first_key[] = "first_key";
  constexpr char first_value[] = "first_value";
  ASSERT_OK(Put(first_key, first_value));

  constexpr char second_key[] = "second_key";
  constexpr char second_value[] = "second_value";
  ASSERT_OK(Put(second_key, second_value));

  ASSERT_OK(Flush());

  constexpr char third_key[] = "third_key";
  constexpr char third_value[] = "third_value";
  ASSERT_OK(Put(third_key, third_value));

  constexpr char fourth_key[] = "fourth_key";
  constexpr char corrupt_blob_index[] = "foobar";

  WriteBatch batch;
  ASSERT_OK(WriteBatchInternal::PutBlobIndex(&batch, 0, fourth_key,
                                             corrupt_blob_index));
  ASSERT_OK(db_->Write(WriteOptions(), &batch));

  ASSERT_OK(Flush());

  constexpr Slice* begin = nullptr;
  constexpr Slice* end = nullptr;

  ASSERT_TRUE(
      db_->CompactRange(CompactRangeOptions(), begin, end).IsCorruption());
}

TEST_F(DBCompactionTest, CompactionWithBlobGCError_InlinedTTLIndex) {
  constexpr uint64_t min_blob_size = 10;

  Options options;
  options.env = env_;
  options.disable_auto_compactions = true;
  options.enable_blob_files = true;
  options.min_blob_size = min_blob_size;
  options.enable_blob_garbage_collection = true;
  options.blob_garbage_collection_age_cutoff = 1.0;

  Reopen(options);

  constexpr char first_key[] = "first_key";
  constexpr char first_value[] = "first_value";
  ASSERT_OK(Put(first_key, first_value));

  constexpr char second_key[] = "second_key";
  constexpr char second_value[] = "second_value";
  ASSERT_OK(Put(second_key, second_value));

  ASSERT_OK(Flush());

  constexpr char third_key[] = "third_key";
  constexpr char third_value[] = "third_value";
  ASSERT_OK(Put(third_key, third_value));

  constexpr char fourth_key[] = "fourth_key";
  constexpr char blob[] = "short";
  static_assert(sizeof(short) - 1 < min_blob_size,
                "Blob too long to be inlined");

  // Fake an inlined TTL blob index.
  std::string blob_index;

  constexpr uint64_t expiration = 1234567890;

  BlobIndex::EncodeInlinedTTL(&blob_index, expiration, blob);

  WriteBatch batch;
  ASSERT_OK(
      WriteBatchInternal::PutBlobIndex(&batch, 0, fourth_key, blob_index));
  ASSERT_OK(db_->Write(WriteOptions(), &batch));

  ASSERT_OK(Flush());

  constexpr Slice* begin = nullptr;
  constexpr Slice* end = nullptr;

  ASSERT_TRUE(
      db_->CompactRange(CompactRangeOptions(), begin, end).IsCorruption());
}

TEST_F(DBCompactionTest, CompactionWithBlobGCError_IndexWithInvalidFileNumber) {
  Options options;
  options.env = env_;
  options.disable_auto_compactions = true;
  options.enable_blob_files = true;
  options.enable_blob_garbage_collection = true;
  options.blob_garbage_collection_age_cutoff = 1.0;

  Reopen(options);

  constexpr char first_key[] = "first_key";
  constexpr char first_value[] = "first_value";
  ASSERT_OK(Put(first_key, first_value));

  constexpr char second_key[] = "second_key";
  constexpr char second_value[] = "second_value";
  ASSERT_OK(Put(second_key, second_value));

  ASSERT_OK(Flush());

  constexpr char third_key[] = "third_key";
  constexpr char third_value[] = "third_value";
  ASSERT_OK(Put(third_key, third_value));

  constexpr char fourth_key[] = "fourth_key";

  // Fake a blob index referencing a non-existent blob file.
  std::string blob_index;

  constexpr uint64_t blob_file_number = 1000;
  constexpr uint64_t offset = 1234;
  constexpr uint64_t size = 5678;

  BlobIndex::EncodeBlob(&blob_index, blob_file_number, offset, size,
                        kNoCompression);

  WriteBatch batch;
  ASSERT_OK(
      WriteBatchInternal::PutBlobIndex(&batch, 0, fourth_key, blob_index));
  ASSERT_OK(db_->Write(WriteOptions(), &batch));

  ASSERT_OK(Flush());

  constexpr Slice* begin = nullptr;
  constexpr Slice* end = nullptr;

  ASSERT_TRUE(
      db_->CompactRange(CompactRangeOptions(), begin, end).IsCorruption());
}

TEST_F(DBCompactionTest, CompactionWithChecksumHandoff1) {
  if (mem_env_ || encrypted_env_) {
    ROCKSDB_GTEST_SKIP("Test requires non-mem or non-encrypted environment");
    return;
  }
  std::shared_ptr<FaultInjectionTestFS> fault_fs(
      new FaultInjectionTestFS(FileSystem::Default()));
  std::unique_ptr<Env> fault_fs_env(NewCompositeEnv(fault_fs));
  Options options = CurrentOptions();
  options.level0_file_num_compaction_trigger = 2;
  options.num_levels = 3;
  options.env = fault_fs_env.get();
  options.create_if_missing = true;
  options.checksum_handoff_file_types.Add(FileType::kTableFile);
  Status s;
  Reopen(options);

  fault_fs->SetChecksumHandoffFuncType(ChecksumType::kCRC32c);
  ASSERT_OK(Put(Key(0), "value1"));
  ASSERT_OK(Put(Key(2), "value2"));
  s = Flush();
  ASSERT_EQ(s, Status::OK());
  ASSERT_OK(Put(Key(1), "value3"));
  s = Flush();
  ASSERT_EQ(s, Status::OK());
  s = dbfull()->TEST_WaitForCompact();
  ASSERT_EQ(s, Status::OK());
  Destroy(options);
  Reopen(options);

  // The hash does not match, compaction write fails
  // fault_fs->SetChecksumHandoffFuncType(ChecksumType::kxxHash);
  // Since the file system returns IOStatus::Corruption, it is an
  // unrecoverable error.
  ASSERT_OK(Put(Key(0), "value1"));
  ASSERT_OK(Put(Key(2), "value2"));
  s = Flush();
  ASSERT_EQ(s, Status::OK());
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency(
      {{"DBImpl::FlushMemTable:FlushMemTableFinished",
        "BackgroundCallCompaction:0"}});
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "BackgroundCallCompaction:0", [&](void*) {
        fault_fs->SetChecksumHandoffFuncType(ChecksumType::kxxHash);
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();
  ASSERT_OK(Put(Key(1), "value3"));
  s = Flush();
  ASSERT_EQ(s, Status::OK());
  s = dbfull()->TEST_WaitForCompact();
  ASSERT_EQ(s.severity(),
            ROCKSDB_NAMESPACE::Status::Severity::kUnrecoverableError);
  SyncPoint::GetInstance()->DisableProcessing();
  Destroy(options);
  Reopen(options);

  // The file system does not support checksum handoff. The check
  // will be ignored.
  fault_fs->SetChecksumHandoffFuncType(ChecksumType::kNoChecksum);
  ASSERT_OK(Put(Key(0), "value1"));
  ASSERT_OK(Put(Key(2), "value2"));
  s = Flush();
  ASSERT_EQ(s, Status::OK());
  ASSERT_OK(Put(Key(1), "value3"));
  s = Flush();
  ASSERT_EQ(s, Status::OK());
  s = dbfull()->TEST_WaitForCompact();
  ASSERT_EQ(s, Status::OK());

  // Each write will be similated as corrupted.
  // Since the file system returns IOStatus::Corruption, it is an
  // unrecoverable error.
  fault_fs->SetChecksumHandoffFuncType(ChecksumType::kCRC32c);
  ASSERT_OK(Put(Key(0), "value1"));
  ASSERT_OK(Put(Key(2), "value2"));
  s = Flush();
  ASSERT_EQ(s, Status::OK());
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency(
      {{"DBImpl::FlushMemTable:FlushMemTableFinished",
        "BackgroundCallCompaction:0"}});
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "BackgroundCallCompaction:0",
      [&](void*) { fault_fs->IngestDataCorruptionBeforeWrite(); });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();
  ASSERT_OK(Put(Key(1), "value3"));
  s = Flush();
  ASSERT_EQ(s, Status::OK());
  s = dbfull()->TEST_WaitForCompact();
  ASSERT_EQ(s.severity(),
            ROCKSDB_NAMESPACE::Status::Severity::kUnrecoverableError);
  SyncPoint::GetInstance()->DisableProcessing();

  Destroy(options);
}

TEST_F(DBCompactionTest, CompactionWithChecksumHandoff2) {
  if (mem_env_ || encrypted_env_) {
    ROCKSDB_GTEST_SKIP("Test requires non-mem or non-encrypted environment");
    return;
  }
  std::shared_ptr<FaultInjectionTestFS> fault_fs(
      new FaultInjectionTestFS(FileSystem::Default()));
  std::unique_ptr<Env> fault_fs_env(NewCompositeEnv(fault_fs));
  Options options = CurrentOptions();
  options.level0_file_num_compaction_trigger = 2;
  options.num_levels = 3;
  options.env = fault_fs_env.get();
  options.create_if_missing = true;
  Status s;
  Reopen(options);

  fault_fs->SetChecksumHandoffFuncType(ChecksumType::kCRC32c);
  ASSERT_OK(Put(Key(0), "value1"));
  ASSERT_OK(Put(Key(2), "value2"));
  s = Flush();
  ASSERT_EQ(s, Status::OK());
  ASSERT_OK(Put(Key(1), "value3"));
  s = Flush();
  ASSERT_EQ(s, Status::OK());
  s = dbfull()->TEST_WaitForCompact();
  ASSERT_EQ(s, Status::OK());
  Destroy(options);
  Reopen(options);

  // options is not set, the checksum handoff will not be triggered
  ASSERT_OK(Put(Key(0), "value1"));
  ASSERT_OK(Put(Key(2), "value2"));
  s = Flush();
  ASSERT_EQ(s, Status::OK());
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency(
      {{"DBImpl::FlushMemTable:FlushMemTableFinished",
        "BackgroundCallCompaction:0"}});
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "BackgroundCallCompaction:0", [&](void*) {
        fault_fs->SetChecksumHandoffFuncType(ChecksumType::kxxHash);
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();
  ASSERT_OK(Put(Key(1), "value3"));
  s = Flush();
  ASSERT_EQ(s, Status::OK());
  s = dbfull()->TEST_WaitForCompact();
  ASSERT_EQ(s, Status::OK());
  SyncPoint::GetInstance()->DisableProcessing();
  Destroy(options);
  Reopen(options);

  // The file system does not support checksum handoff. The check
  // will be ignored.
  fault_fs->SetChecksumHandoffFuncType(ChecksumType::kNoChecksum);
  ASSERT_OK(Put(Key(0), "value1"));
  ASSERT_OK(Put(Key(2), "value2"));
  s = Flush();
  ASSERT_EQ(s, Status::OK());
  ASSERT_OK(Put(Key(1), "value3"));
  s = Flush();
  ASSERT_EQ(s, Status::OK());
  s = dbfull()->TEST_WaitForCompact();
  ASSERT_EQ(s, Status::OK());

  // options is not set, the checksum handoff will not be triggered
  fault_fs->SetChecksumHandoffFuncType(ChecksumType::kCRC32c);
  ASSERT_OK(Put(Key(0), "value1"));
  ASSERT_OK(Put(Key(2), "value2"));
  s = Flush();
  ASSERT_EQ(s, Status::OK());
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency(
      {{"DBImpl::FlushMemTable:FlushMemTableFinished",
        "BackgroundCallCompaction:0"}});
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "BackgroundCallCompaction:0",
      [&](void*) { fault_fs->IngestDataCorruptionBeforeWrite(); });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();
  ASSERT_OK(Put(Key(1), "value3"));
  s = Flush();
  ASSERT_EQ(s, Status::OK());
  s = dbfull()->TEST_WaitForCompact();
  ASSERT_EQ(s, Status::OK());

  Destroy(options);
}

TEST_F(DBCompactionTest, CompactionWithChecksumHandoffManifest1) {
  if (mem_env_ || encrypted_env_) {
    ROCKSDB_GTEST_SKIP("Test requires non-mem or non-encrypted environment");
    return;
  }
  std::shared_ptr<FaultInjectionTestFS> fault_fs(
      new FaultInjectionTestFS(FileSystem::Default()));
  std::unique_ptr<Env> fault_fs_env(NewCompositeEnv(fault_fs));
  Options options = CurrentOptions();
  options.level0_file_num_compaction_trigger = 2;
  options.num_levels = 3;
  options.env = fault_fs_env.get();
  options.create_if_missing = true;
  options.checksum_handoff_file_types.Add(FileType::kDescriptorFile);
  Status s;
  fault_fs->SetChecksumHandoffFuncType(ChecksumType::kCRC32c);
  Reopen(options);

  ASSERT_OK(Put(Key(0), "value1"));
  ASSERT_OK(Put(Key(2), "value2"));
  s = Flush();
  ASSERT_EQ(s, Status::OK());
  ASSERT_OK(Put(Key(1), "value3"));
  s = Flush();
  ASSERT_EQ(s, Status::OK());
  s = dbfull()->TEST_WaitForCompact();
  ASSERT_EQ(s, Status::OK());
  Destroy(options);
  Reopen(options);

  // The hash does not match, compaction write fails
  // fault_fs->SetChecksumHandoffFuncType(ChecksumType::kxxHash);
  // Since the file system returns IOStatus::Corruption, it is mapped to
  // kFatalError error.
  ASSERT_OK(Put(Key(0), "value1"));
  ASSERT_OK(Put(Key(2), "value2"));
  s = Flush();
  ASSERT_EQ(s, Status::OK());
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency(
      {{"DBImpl::FlushMemTable:FlushMemTableFinished",
        "BackgroundCallCompaction:0"}});
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "BackgroundCallCompaction:0", [&](void*) {
        fault_fs->SetChecksumHandoffFuncType(ChecksumType::kxxHash);
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();
  ASSERT_OK(Put(Key(1), "value3"));
  s = Flush();
  ASSERT_EQ(s, Status::OK());
  s = dbfull()->TEST_WaitForCompact();
  ASSERT_EQ(s.severity(), ROCKSDB_NAMESPACE::Status::Severity::kFatalError);
  SyncPoint::GetInstance()->DisableProcessing();
  Destroy(options);
}

TEST_F(DBCompactionTest, CompactionWithChecksumHandoffManifest2) {
  if (mem_env_ || encrypted_env_) {
    ROCKSDB_GTEST_SKIP("Test requires non-mem or non-encrypted environment");
    return;
  }
  std::shared_ptr<FaultInjectionTestFS> fault_fs(
      new FaultInjectionTestFS(FileSystem::Default()));
  std::unique_ptr<Env> fault_fs_env(NewCompositeEnv(fault_fs));
  Options options = CurrentOptions();
  options.level0_file_num_compaction_trigger = 2;
  options.num_levels = 3;
  options.env = fault_fs_env.get();
  options.create_if_missing = true;
  options.checksum_handoff_file_types.Add(FileType::kDescriptorFile);
  Status s;
  fault_fs->SetChecksumHandoffFuncType(ChecksumType::kNoChecksum);
  Reopen(options);

  // The file system does not support checksum handoff. The check
  // will be ignored.
  ASSERT_OK(Put(Key(0), "value1"));
  ASSERT_OK(Put(Key(2), "value2"));
  s = Flush();
  ASSERT_EQ(s, Status::OK());
  ASSERT_OK(Put(Key(1), "value3"));
  s = Flush();
  ASSERT_EQ(s, Status::OK());
  s = dbfull()->TEST_WaitForCompact();
  ASSERT_EQ(s, Status::OK());

  // Each write will be similated as corrupted.
  // Since the file system returns IOStatus::Corruption, it is mapped to
  // kFatalError error.
  fault_fs->SetChecksumHandoffFuncType(ChecksumType::kCRC32c);
  ASSERT_OK(Put(Key(0), "value1"));
  ASSERT_OK(Put(Key(2), "value2"));
  s = Flush();
  ASSERT_EQ(s, Status::OK());
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency(
      {{"DBImpl::FlushMemTable:FlushMemTableFinished",
        "BackgroundCallCompaction:0"}});
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "BackgroundCallCompaction:0",
      [&](void*) { fault_fs->IngestDataCorruptionBeforeWrite(); });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();
  ASSERT_OK(Put(Key(1), "value3"));
  s = Flush();
  ASSERT_EQ(s, Status::OK());
  s = dbfull()->TEST_WaitForCompact();
  ASSERT_EQ(s.severity(), ROCKSDB_NAMESPACE::Status::Severity::kFatalError);
  SyncPoint::GetInstance()->DisableProcessing();

  Destroy(options);
}

#endif  // !defined(ROCKSDB_LITE)

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
#if !defined(ROCKSDB_LITE)
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
#else
  (void) argc;
  (void) argv;
  return 0;
#endif
}

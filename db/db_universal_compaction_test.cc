//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "port/stack_trace.h"
#include "util/db_test_util.h"
#if !(defined NDEBUG) || !defined(OS_WIN)
#include "util/sync_point.h"
#endif

namespace rocksdb {

static std::string CompressibleString(Random* rnd, int len) {
  std::string r;
  test::CompressibleString(rnd, 0.8, len, &r);
  return r;
}

class DBTestUniversalCompactionBase
    : public DBTestBase,
      public ::testing::WithParamInterface<int> {
 public:
  explicit DBTestUniversalCompactionBase(
      const std::string& path) : DBTestBase(path) {}
  virtual void SetUp() override { num_levels_ = GetParam(); }
  int num_levels_;
};

class DBTestUniversalCompaction : public DBTestUniversalCompactionBase {
 public:
  DBTestUniversalCompaction() :
      DBTestUniversalCompactionBase("/db_universal_compaction_test") {}
};

namespace {
class KeepFilter : public CompactionFilter {
 public:
  virtual bool Filter(int level, const Slice& key, const Slice& value,
                      std::string* new_value, bool* value_changed) const
      override {
    return false;
  }

  virtual const char* Name() const override { return "KeepFilter"; }
};

class KeepFilterFactory : public CompactionFilterFactory {
 public:
  explicit KeepFilterFactory(bool check_context = false)
      : check_context_(check_context) {}

  virtual std::unique_ptr<CompactionFilter> CreateCompactionFilter(
      const CompactionFilter::Context& context) override {
    if (check_context_) {
      EXPECT_EQ(expect_full_compaction_.load(), context.is_full_compaction);
      EXPECT_EQ(expect_manual_compaction_.load(), context.is_manual_compaction);
    }
    return std::unique_ptr<CompactionFilter>(new KeepFilter());
  }

  virtual const char* Name() const override { return "KeepFilterFactory"; }
  bool check_context_;
  std::atomic_bool expect_full_compaction_;
  std::atomic_bool expect_manual_compaction_;
};

class DelayFilter : public CompactionFilter {
 public:
  explicit DelayFilter(DBTestBase* d) : db_test(d) {}
  virtual bool Filter(int level, const Slice& key, const Slice& value,
                      std::string* new_value,
                      bool* value_changed) const override {
    db_test->env_->addon_time_.fetch_add(1000);
    return true;
  }

  virtual const char* Name() const override { return "DelayFilter"; }

 private:
  DBTestBase* db_test;
};

class DelayFilterFactory : public CompactionFilterFactory {
 public:
  explicit DelayFilterFactory(DBTestBase* d) : db_test(d) {}
  virtual std::unique_ptr<CompactionFilter> CreateCompactionFilter(
      const CompactionFilter::Context& context) override {
    return std::unique_ptr<CompactionFilter>(new DelayFilter(db_test));
  }

  virtual const char* Name() const override { return "DelayFilterFactory"; }

 private:
  DBTestBase* db_test;
};
}  // namespace

// SyncPoint is not supported in released Windows mode.
#if !(defined NDEBUG) || !defined(OS_WIN)

// TODO(kailiu) The tests on UniversalCompaction has some issues:
//  1. A lot of magic numbers ("11" or "12").
//  2. Made assumption on the memtable flush conditions, which may change from
//     time to time.
TEST_P(DBTestUniversalCompaction, UniversalCompactionTrigger) {
  Options options;
  options.compaction_style = kCompactionStyleUniversal;
  options.num_levels = num_levels_;
  options.write_buffer_size = 100 << 10;     // 100KB
  options.target_file_size_base = 32 << 10;  // 32KB
  // trigger compaction if there are >= 4 files
  options.level0_file_num_compaction_trigger = 4;
  KeepFilterFactory* filter = new KeepFilterFactory(true);
  filter->expect_manual_compaction_.store(false);
  options.compaction_filter_factory.reset(filter);

  options = CurrentOptions(options);
  DestroyAndReopen(options);
  CreateAndReopenWithCF({"pikachu"}, options);

  rocksdb::SyncPoint::GetInstance()->SetCallBack(
      "DBTestWritableFile.GetPreallocationStatus", [&](void* arg) {
        ASSERT_TRUE(arg != nullptr);
        size_t preallocation_size = *(static_cast<size_t*>(arg));
        if (num_levels_ > 3) {
          ASSERT_LE(preallocation_size, options.target_file_size_base * 1.1);
        }
      });
  rocksdb::SyncPoint::GetInstance()->EnableProcessing();

  Random rnd(301);
  int key_idx = 0;

  filter->expect_full_compaction_.store(true);
  // Stage 1:
  //   Generate a set of files at level 0, but don't trigger level-0
  //   compaction.
  for (int num = 0; num < options.level0_file_num_compaction_trigger - 1;
       num++) {
    // Write 110KB (11 values, each 10K)
    for (int i = 0; i < 12; i++) {
      ASSERT_OK(Put(1, Key(key_idx), RandomString(&rnd, 10000)));
      key_idx++;
    }
    dbfull()->TEST_WaitForFlushMemTable(handles_[1]);
    ASSERT_EQ(NumSortedRuns(1), num + 1);
  }

  // Generate one more file at level-0, which should trigger level-0
  // compaction.
  for (int i = 0; i < 11; i++) {
    ASSERT_OK(Put(1, Key(key_idx), RandomString(&rnd, 10000)));
    key_idx++;
  }
  dbfull()->TEST_WaitForCompact();
  // Suppose each file flushed from mem table has size 1. Now we compact
  // (level0_file_num_compaction_trigger+1)=4 files and should have a big
  // file of size 4.
  ASSERT_EQ(NumSortedRuns(1), 1);

  // Stage 2:
  //   Now we have one file at level 0, with size 4. We also have some data in
  //   mem table. Let's continue generating new files at level 0, but don't
  //   trigger level-0 compaction.
  //   First, clean up memtable before inserting new data. This will generate
  //   a level-0 file, with size around 0.4 (according to previously written
  //   data amount).
  filter->expect_full_compaction_.store(false);
  ASSERT_OK(Flush(1));
  for (int num = 0; num < options.level0_file_num_compaction_trigger - 3;
       num++) {
    // Write 110KB (11 values, each 10K)
    for (int i = 0; i < 11; i++) {
      ASSERT_OK(Put(1, Key(key_idx), RandomString(&rnd, 10000)));
      key_idx++;
    }
    dbfull()->TEST_WaitForFlushMemTable(handles_[1]);
    ASSERT_EQ(NumSortedRuns(1), num + 3);
  }

  // Generate one more file at level-0, which should trigger level-0
  // compaction.
  for (int i = 0; i < 11; i++) {
    ASSERT_OK(Put(1, Key(key_idx), RandomString(&rnd, 10000)));
    key_idx++;
  }
  dbfull()->TEST_WaitForCompact();
  // Before compaction, we have 4 files at level 0, with size 4, 0.4, 1, 1.
  // After compaction, we should have 2 files, with size 4, 2.4.
  ASSERT_EQ(NumSortedRuns(1), 2);

  // Stage 3:
  //   Now we have 2 files at level 0, with size 4 and 2.4. Continue
  //   generating new files at level 0.
  for (int num = 0; num < options.level0_file_num_compaction_trigger - 3;
       num++) {
    // Write 110KB (11 values, each 10K)
    for (int i = 0; i < 11; i++) {
      ASSERT_OK(Put(1, Key(key_idx), RandomString(&rnd, 10000)));
      key_idx++;
    }
    dbfull()->TEST_WaitForFlushMemTable(handles_[1]);
    ASSERT_EQ(NumSortedRuns(1), num + 3);
  }

  // Generate one more file at level-0, which should trigger level-0
  // compaction.
  for (int i = 0; i < 12; i++) {
    ASSERT_OK(Put(1, Key(key_idx), RandomString(&rnd, 10000)));
    key_idx++;
  }
  dbfull()->TEST_WaitForCompact();
  // Before compaction, we have 4 files at level 0, with size 4, 2.4, 1, 1.
  // After compaction, we should have 3 files, with size 4, 2.4, 2.
  ASSERT_EQ(NumSortedRuns(1), 3);

  // Stage 4:
  //   Now we have 3 files at level 0, with size 4, 2.4, 2. Let's generate a
  //   new file of size 1.
  for (int i = 0; i < 11; i++) {
    ASSERT_OK(Put(1, Key(key_idx), RandomString(&rnd, 10000)));
    key_idx++;
  }
  dbfull()->TEST_WaitForCompact();
  // Level-0 compaction is triggered, but no file will be picked up.
  ASSERT_EQ(NumSortedRuns(1), 4);

  // Stage 5:
  //   Now we have 4 files at level 0, with size 4, 2.4, 2, 1. Let's generate
  //   a new file of size 1.
  filter->expect_full_compaction_.store(true);
  for (int i = 0; i < 11; i++) {
    ASSERT_OK(Put(1, Key(key_idx), RandomString(&rnd, 10000)));
    key_idx++;
  }
  dbfull()->TEST_WaitForCompact();
  // All files at level 0 will be compacted into a single one.
  ASSERT_EQ(NumSortedRuns(1), 1);

  rocksdb::SyncPoint::GetInstance()->DisableProcessing();
}
#endif  // !(defined NDEBUG) || !defined(OS_WIN)

TEST_P(DBTestUniversalCompaction, UniversalCompactionSizeAmplification) {
  Options options;
  options.compaction_style = kCompactionStyleUniversal;
  options.num_levels = num_levels_;
  options.write_buffer_size = 100 << 10;     // 100KB
  options.target_file_size_base = 32 << 10;  // 32KB
  options.level0_file_num_compaction_trigger = 3;
  options = CurrentOptions(options);
  DestroyAndReopen(options);
  CreateAndReopenWithCF({"pikachu"}, options);

  // Trigger compaction if size amplification exceeds 110%
  options.compaction_options_universal.max_size_amplification_percent = 110;
  options = CurrentOptions(options);
  ReopenWithColumnFamilies({"default", "pikachu"}, options);

  Random rnd(301);
  int key_idx = 0;

  //   Generate two files in Level 0. Both files are approx the same size.
  for (int num = 0; num < options.level0_file_num_compaction_trigger - 1;
       num++) {
    // Write 110KB (11 values, each 10K)
    for (int i = 0; i < 11; i++) {
      ASSERT_OK(Put(1, Key(key_idx), RandomString(&rnd, 10000)));
      key_idx++;
    }
    dbfull()->TEST_WaitForFlushMemTable(handles_[1]);
    ASSERT_EQ(NumSortedRuns(1), num + 1);
  }
  ASSERT_EQ(NumSortedRuns(1), 2);

  // Flush whatever is remaining in memtable. This is typically
  // small, which should not trigger size ratio based compaction
  // but will instead trigger size amplification.
  ASSERT_OK(Flush(1));

  dbfull()->TEST_WaitForCompact();

  // Verify that size amplification did occur
  ASSERT_EQ(NumSortedRuns(1), 1);
}

class DBTestUniversalCompactionMultiLevels
    : public DBTestUniversalCompactionBase {
 public:
  DBTestUniversalCompactionMultiLevels() :
      DBTestUniversalCompactionBase(
          "/db_universal_compaction_multi_levels_test") {}
};

TEST_P(DBTestUniversalCompactionMultiLevels, UniversalCompactionMultiLevels) {
  Options options;
  options.compaction_style = kCompactionStyleUniversal;
  options.num_levels = num_levels_;
  options.write_buffer_size = 100 << 10;  // 100KB
  options.level0_file_num_compaction_trigger = 8;
  options.max_background_compactions = 3;
  options.target_file_size_base = 32 * 1024;
  options = CurrentOptions(options);
  CreateAndReopenWithCF({"pikachu"}, options);

  // Trigger compaction if size amplification exceeds 110%
  options.compaction_options_universal.max_size_amplification_percent = 110;
  options = CurrentOptions(options);
  ReopenWithColumnFamilies({"default", "pikachu"}, options);

  Random rnd(301);
  int num_keys = 100000;
  for (int i = 0; i < num_keys * 2; i++) {
    ASSERT_OK(Put(1, Key(i % num_keys), Key(i)));
  }

  dbfull()->TEST_WaitForCompact();

  for (int i = num_keys; i < num_keys * 2; i++) {
    ASSERT_EQ(Get(1, Key(i % num_keys)), Key(i));
  }
}
// Tests universal compaction with trivial move enabled
TEST_P(DBTestUniversalCompactionMultiLevels, UniversalCompactionTrivialMove) {
  int32_t trivial_move = 0;
  int32_t non_trivial_move = 0;
  rocksdb::SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::BackgroundCompaction:TrivialMove",
      [&](void* arg) { trivial_move++; });
  rocksdb::SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::BackgroundCompaction:NonTrivial",
      [&](void* arg) { non_trivial_move++; });
  rocksdb::SyncPoint::GetInstance()->EnableProcessing();

  Options options;
  options.compaction_style = kCompactionStyleUniversal;
  options.compaction_options_universal.allow_trivial_move = true;
  options.num_levels = 3;
  options.write_buffer_size = 100 << 10;  // 100KB
  options.level0_file_num_compaction_trigger = 3;
  options.max_background_compactions = 1;
  options.target_file_size_base = 32 * 1024;
  options = CurrentOptions(options);
  DestroyAndReopen(options);
  CreateAndReopenWithCF({"pikachu"}, options);

  // Trigger compaction if size amplification exceeds 110%
  options.compaction_options_universal.max_size_amplification_percent = 110;
  options = CurrentOptions(options);
  ReopenWithColumnFamilies({"default", "pikachu"}, options);

  Random rnd(301);
  int num_keys = 15000;
  for (int i = 0; i < num_keys; i++) {
    ASSERT_OK(Put(1, Key(i), Key(i)));
  }
  std::vector<std::string> values;

  ASSERT_OK(Flush(1));
  dbfull()->TEST_WaitForCompact();

  ASSERT_GT(trivial_move, 0);
  ASSERT_EQ(non_trivial_move, 0);

  rocksdb::SyncPoint::GetInstance()->DisableProcessing();
}

INSTANTIATE_TEST_CASE_P(DBTestUniversalCompactionMultiLevels,
                        DBTestUniversalCompactionMultiLevels,
                        ::testing::Values(3, 20));

class DBTestUniversalCompactionParallel :
    public DBTestUniversalCompactionBase {
 public:
  DBTestUniversalCompactionParallel() :
      DBTestUniversalCompactionBase(
          "/db_universal_compaction_prallel_test") {}
};

TEST_P(DBTestUniversalCompactionParallel, UniversalCompactionParallel) {
  Options options;
  options.compaction_style = kCompactionStyleUniversal;
  options.num_levels = num_levels_;
  options.write_buffer_size = 1 << 10;  // 1KB
  options.level0_file_num_compaction_trigger = 3;
  options.max_background_compactions = 3;
  options.max_background_flushes = 3;
  options.target_file_size_base = 1 * 1024;
  options.compaction_options_universal.max_size_amplification_percent = 110;
  options = CurrentOptions(options);
  DestroyAndReopen(options);
  CreateAndReopenWithCF({"pikachu"}, options);

  // Delay every compaction so multiple compactions will happen.
  std::atomic<int> num_compactions_running(0);
  std::atomic<bool> has_parallel(false);
  rocksdb::SyncPoint::GetInstance()->SetCallBack("CompactionJob::Run():Start",
                                                 [&](void* arg) {
    if (num_compactions_running.fetch_add(1) > 0) {
      has_parallel.store(true);
      return;
    }
    for (int nwait = 0; nwait < 20000; nwait++) {
      if (has_parallel.load() || num_compactions_running.load() > 1) {
        has_parallel.store(true);
        break;
      }
      env_->SleepForMicroseconds(1000);
    }
  });
  rocksdb::SyncPoint::GetInstance()->SetCallBack(
      "CompactionJob::Run():End",
      [&](void* arg) { num_compactions_running.fetch_add(-1); });
  rocksdb::SyncPoint::GetInstance()->EnableProcessing();

  options = CurrentOptions(options);
  ReopenWithColumnFamilies({"default", "pikachu"}, options);

  Random rnd(301);
  int num_keys = 30000;
  for (int i = 0; i < num_keys * 2; i++) {
    ASSERT_OK(Put(1, Key(i % num_keys), Key(i)));
  }
  dbfull()->TEST_WaitForCompact();

  rocksdb::SyncPoint::GetInstance()->DisableProcessing();
  ASSERT_EQ(num_compactions_running.load(), 0);
  ASSERT_TRUE(has_parallel.load());

  for (int i = num_keys; i < num_keys * 2; i++) {
    ASSERT_EQ(Get(1, Key(i % num_keys)), Key(i));
  }

  // Reopen and check.
  ReopenWithColumnFamilies({"default", "pikachu"}, options);
  for (int i = num_keys; i < num_keys * 2; i++) {
    ASSERT_EQ(Get(1, Key(i % num_keys)), Key(i));
  }
}

INSTANTIATE_TEST_CASE_P(DBTestUniversalCompactionParallel,
                        DBTestUniversalCompactionParallel,
                        ::testing::Values(1, 10));

TEST_P(DBTestUniversalCompaction, UniversalCompactionOptions) {
  Options options;
  options.compaction_style = kCompactionStyleUniversal;
  options.write_buffer_size = 100 << 10;     // 100KB
  options.target_file_size_base = 32 << 10;  // 32KB
  options.level0_file_num_compaction_trigger = 4;
  options.num_levels = num_levels_;
  options.compaction_options_universal.compression_size_percent = -1;
  options = CurrentOptions(options);
  DestroyAndReopen(options);
  CreateAndReopenWithCF({"pikachu"}, options);

  Random rnd(301);
  int key_idx = 0;

  for (int num = 0; num < options.level0_file_num_compaction_trigger; num++) {
    // Write 110KB (11 values, each 10K)
    for (int i = 0; i < 11; i++) {
      ASSERT_OK(Put(1, Key(key_idx), RandomString(&rnd, 10000)));
      key_idx++;
    }
    dbfull()->TEST_WaitForFlushMemTable(handles_[1]);

    if (num < options.level0_file_num_compaction_trigger - 1) {
      ASSERT_EQ(NumSortedRuns(1), num + 1);
    }
  }

  dbfull()->TEST_WaitForCompact();
  ASSERT_EQ(NumSortedRuns(1), 1);
}

TEST_P(DBTestUniversalCompaction, UniversalCompactionStopStyleSimilarSize) {
  Options options = CurrentOptions();
  options.compaction_style = kCompactionStyleUniversal;
  options.write_buffer_size = 100 << 10;     // 100KB
  options.target_file_size_base = 32 << 10;  // 32KB
  // trigger compaction if there are >= 4 files
  options.level0_file_num_compaction_trigger = 4;
  options.compaction_options_universal.size_ratio = 10;
  options.compaction_options_universal.stop_style =
      kCompactionStopStyleSimilarSize;
  options.num_levels = num_levels_;
  DestroyAndReopen(options);

  Random rnd(301);
  int key_idx = 0;

  // Stage 1:
  //   Generate a set of files at level 0, but don't trigger level-0
  //   compaction.
  for (int num = 0; num < options.level0_file_num_compaction_trigger - 1;
       num++) {
    // Write 110KB (11 values, each 10K)
    for (int i = 0; i < 11; i++) {
      ASSERT_OK(Put(Key(key_idx), RandomString(&rnd, 10000)));
      key_idx++;
    }
    dbfull()->TEST_WaitForFlushMemTable();
    ASSERT_EQ(NumSortedRuns(), num + 1);
  }

  // Generate one more file at level-0, which should trigger level-0
  // compaction.
  for (int i = 0; i < 11; i++) {
    ASSERT_OK(Put(Key(key_idx), RandomString(&rnd, 10000)));
    key_idx++;
  }
  dbfull()->TEST_WaitForCompact();
  // Suppose each file flushed from mem table has size 1. Now we compact
  // (level0_file_num_compaction_trigger+1)=4 files and should have a big
  // file of size 4.
  ASSERT_EQ(NumSortedRuns(), 1);

  // Stage 2:
  //   Now we have one file at level 0, with size 4. We also have some data in
  //   mem table. Let's continue generating new files at level 0, but don't
  //   trigger level-0 compaction.
  //   First, clean up memtable before inserting new data. This will generate
  //   a level-0 file, with size around 0.4 (according to previously written
  //   data amount).
  dbfull()->Flush(FlushOptions());
  for (int num = 0; num < options.level0_file_num_compaction_trigger - 3;
       num++) {
    // Write 110KB (11 values, each 10K)
    for (int i = 0; i < 11; i++) {
      ASSERT_OK(Put(Key(key_idx), RandomString(&rnd, 10000)));
      key_idx++;
    }
    dbfull()->TEST_WaitForFlushMemTable();
    ASSERT_EQ(NumSortedRuns(), num + 3);
  }

  // Generate one more file at level-0, which should trigger level-0
  // compaction.
  for (int i = 0; i < 11; i++) {
    ASSERT_OK(Put(Key(key_idx), RandomString(&rnd, 10000)));
    key_idx++;
  }
  dbfull()->TEST_WaitForCompact();
  // Before compaction, we have 4 files at level 0, with size 4, 0.4, 1, 1.
  // After compaction, we should have 3 files, with size 4, 0.4, 2.
  ASSERT_EQ(NumSortedRuns(), 3);
  // Stage 3:
  //   Now we have 3 files at level 0, with size 4, 0.4, 2. Generate one
  //   more file at level-0, which should trigger level-0 compaction.
  for (int i = 0; i < 11; i++) {
    ASSERT_OK(Put(Key(key_idx), RandomString(&rnd, 10000)));
    key_idx++;
  }
  dbfull()->TEST_WaitForCompact();
  // Level-0 compaction is triggered, but no file will be picked up.
  ASSERT_EQ(NumSortedRuns(), 4);
}

TEST_P(DBTestUniversalCompaction, UniversalCompactionCompressRatio1) {
  if (!Snappy_Supported()) {
    return;
  }

  Options options;
  options.compaction_style = kCompactionStyleUniversal;
  options.write_buffer_size = 100 << 10;     // 100KB
  options.target_file_size_base = 32 << 10;  // 32KB
  options.level0_file_num_compaction_trigger = 2;
  options.num_levels = num_levels_;
  options.compaction_options_universal.compression_size_percent = 70;
  options = CurrentOptions(options);
  DestroyAndReopen(options);

  Random rnd(301);
  int key_idx = 0;

  // The first compaction (2) is compressed.
  for (int num = 0; num < 2; num++) {
    // Write 110KB (11 values, each 10K)
    for (int i = 0; i < 11; i++) {
      ASSERT_OK(Put(Key(key_idx), CompressibleString(&rnd, 10000)));
      key_idx++;
    }
    dbfull()->TEST_WaitForFlushMemTable();
    dbfull()->TEST_WaitForCompact();
  }
  ASSERT_LT(TotalSize(), 110000U * 2 * 0.9);

  // The second compaction (4) is compressed
  for (int num = 0; num < 2; num++) {
    // Write 110KB (11 values, each 10K)
    for (int i = 0; i < 11; i++) {
      ASSERT_OK(Put(Key(key_idx), CompressibleString(&rnd, 10000)));
      key_idx++;
    }
    dbfull()->TEST_WaitForFlushMemTable();
    dbfull()->TEST_WaitForCompact();
  }
  ASSERT_LT(TotalSize(), 110000 * 4 * 0.9);

  // The third compaction (2 4) is compressed since this time it is
  // (1 1 3.2) and 3.2/5.2 doesn't reach ratio.
  for (int num = 0; num < 2; num++) {
    // Write 110KB (11 values, each 10K)
    for (int i = 0; i < 11; i++) {
      ASSERT_OK(Put(Key(key_idx), CompressibleString(&rnd, 10000)));
      key_idx++;
    }
    dbfull()->TEST_WaitForFlushMemTable();
    dbfull()->TEST_WaitForCompact();
  }
  ASSERT_LT(TotalSize(), 110000 * 6 * 0.9);

  // When we start for the compaction up to (2 4 8), the latest
  // compressed is not compressed.
  for (int num = 0; num < 8; num++) {
    // Write 110KB (11 values, each 10K)
    for (int i = 0; i < 11; i++) {
      ASSERT_OK(Put(Key(key_idx), CompressibleString(&rnd, 10000)));
      key_idx++;
    }
    dbfull()->TEST_WaitForFlushMemTable();
    dbfull()->TEST_WaitForCompact();
  }
  ASSERT_GT(TotalSize(), 110000 * 11 * 0.8 + 110000 * 2);
}

TEST_P(DBTestUniversalCompaction, UniversalCompactionCompressRatio2) {
  if (!Snappy_Supported()) {
    return;
  }
  Options options;
  options.compaction_style = kCompactionStyleUniversal;
  options.write_buffer_size = 100 << 10;     // 100KB
  options.target_file_size_base = 32 << 10;  // 32KB
  options.level0_file_num_compaction_trigger = 2;
  options.num_levels = num_levels_;
  options.compaction_options_universal.compression_size_percent = 95;
  options = CurrentOptions(options);
  DestroyAndReopen(options);

  Random rnd(301);
  int key_idx = 0;

  // When we start for the compaction up to (2 4 8), the latest
  // compressed is compressed given the size ratio to compress.
  for (int num = 0; num < 14; num++) {
    // Write 120KB (12 values, each 10K)
    for (int i = 0; i < 12; i++) {
      ASSERT_OK(Put(Key(key_idx), CompressibleString(&rnd, 10000)));
      key_idx++;
    }
    dbfull()->TEST_WaitForFlushMemTable();
    dbfull()->TEST_WaitForCompact();
  }
  ASSERT_LT(TotalSize(), 120000U * 12 * 0.8 + 120000 * 2);
}

INSTANTIATE_TEST_CASE_P(UniversalCompactionNumLevels, DBTestUniversalCompaction,
                        ::testing::Values(1, 3, 5));

class DBTestUniversalManualCompactionOutputPathId
    : public DBTestUniversalCompactionBase {
 public:
  DBTestUniversalManualCompactionOutputPathId() :
      DBTestUniversalCompactionBase(
          "/db_universal_compaction_manual_pid_test") {}
};

TEST_P(DBTestUniversalManualCompactionOutputPathId,
       ManualCompactionOutputPathId) {
  Options options = CurrentOptions();
  options.create_if_missing = true;
  options.db_paths.emplace_back(dbname_, 1000000000);
  options.db_paths.emplace_back(dbname_ + "_2", 1000000000);
  options.compaction_style = kCompactionStyleUniversal;
  options.num_levels = num_levels_;
  options.target_file_size_base = 1 << 30;  // Big size
  options.level0_file_num_compaction_trigger = 10;
  Destroy(options);
  DestroyAndReopen(options);
  CreateAndReopenWithCF({"pikachu"}, options);
  MakeTables(3, "p", "q", 1);
  dbfull()->TEST_WaitForCompact();
  ASSERT_EQ(2, TotalLiveFiles(1));
  ASSERT_EQ(2, GetSstFileCount(options.db_paths[0].path));
  ASSERT_EQ(0, GetSstFileCount(options.db_paths[1].path));

  // Full compaction to DB path 0
  CompactRangeOptions compact_options;
  compact_options.target_path_id = 1;
  db_->CompactRange(compact_options, handles_[1], nullptr, nullptr);
  ASSERT_EQ(1, TotalLiveFiles(1));
  ASSERT_EQ(0, GetSstFileCount(options.db_paths[0].path));
  ASSERT_EQ(1, GetSstFileCount(options.db_paths[1].path));

  ReopenWithColumnFamilies({kDefaultColumnFamilyName, "pikachu"}, options);
  ASSERT_EQ(1, TotalLiveFiles(1));
  ASSERT_EQ(0, GetSstFileCount(options.db_paths[0].path));
  ASSERT_EQ(1, GetSstFileCount(options.db_paths[1].path));

  MakeTables(1, "p", "q", 1);
  ASSERT_EQ(2, TotalLiveFiles(1));
  ASSERT_EQ(1, GetSstFileCount(options.db_paths[0].path));
  ASSERT_EQ(1, GetSstFileCount(options.db_paths[1].path));

  ReopenWithColumnFamilies({kDefaultColumnFamilyName, "pikachu"}, options);
  ASSERT_EQ(2, TotalLiveFiles(1));
  ASSERT_EQ(1, GetSstFileCount(options.db_paths[0].path));
  ASSERT_EQ(1, GetSstFileCount(options.db_paths[1].path));

  // Full compaction to DB path 0
  compact_options.target_path_id = 0;
  db_->CompactRange(compact_options, handles_[1], nullptr, nullptr);
  ASSERT_EQ(1, TotalLiveFiles(1));
  ASSERT_EQ(1, GetSstFileCount(options.db_paths[0].path));
  ASSERT_EQ(0, GetSstFileCount(options.db_paths[1].path));

  // Fail when compacting to an invalid path ID
  compact_options.target_path_id = 2;
  ASSERT_TRUE(db_->CompactRange(compact_options, handles_[1], nullptr, nullptr)
                  .IsInvalidArgument());
}

INSTANTIATE_TEST_CASE_P(DBTestUniversalManualCompactionOutputPathId,
                        DBTestUniversalManualCompactionOutputPathId,
                        ::testing::Values(1, 8));
}  // namespace rocksdb

int main(int argc, char** argv) {
#if !(defined NDEBUG) || !defined(OS_WIN)
  rocksdb::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
#else
  return 0;
#endif
}

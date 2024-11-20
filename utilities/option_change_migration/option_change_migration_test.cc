//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "rocksdb/utilities/option_change_migration.h"

#include <cstdint>
#include <limits>
#include <set>

#include "db/db_test_util.h"
#include "port/stack_trace.h"
#include "util/random.h"

namespace ROCKSDB_NAMESPACE {

class DBOptionChangeMigrationTests
    : public DBTestBase,
      public testing::WithParamInterface<
          std::tuple<int, int, bool, int, int, bool, uint64_t>> {
 public:
  DBOptionChangeMigrationTests()
      : DBTestBase("db_option_change_migration_test", /*env_do_fsync=*/true) {
    level1_ = std::get<0>(GetParam());
    compaction_style1_ = std::get<1>(GetParam());
    is_dynamic1_ = std::get<2>(GetParam());

    level2_ = std::get<3>(GetParam());
    compaction_style2_ = std::get<4>(GetParam());
    is_dynamic2_ = std::get<5>(GetParam());
    // This is set to be extremely large if not zero to avoid dropping any data
    // right after migration, which makes test verification difficult
    fifo_max_table_files_size_ = std::get<6>(GetParam());
  }

  // Required if inheriting from testing::WithParamInterface<>
  static void SetUpTestCase() {}
  static void TearDownTestCase() {}

  int level1_;
  int compaction_style1_;
  bool is_dynamic1_;

  int level2_;
  int compaction_style2_;
  bool is_dynamic2_;

  uint64_t fifo_max_table_files_size_;
};

TEST_P(DBOptionChangeMigrationTests, Migrate1) {
  Options old_options = CurrentOptions();
  old_options.compaction_style =
      static_cast<CompactionStyle>(compaction_style1_);
  if (old_options.compaction_style == CompactionStyle::kCompactionStyleLevel) {
    old_options.level_compaction_dynamic_level_bytes = is_dynamic1_;
  }
  if (old_options.compaction_style == CompactionStyle::kCompactionStyleFIFO) {
    old_options.max_open_files = -1;
  }
  old_options.level0_file_num_compaction_trigger = 3;
  old_options.write_buffer_size = 64 * 1024;
  old_options.target_file_size_base = 128 * 1024;
  // Make level target of L1, L2 to be 200KB and 600KB
  old_options.num_levels = level1_;
  old_options.max_bytes_for_level_multiplier = 3;
  old_options.max_bytes_for_level_base = 200 * 1024;

  Reopen(old_options);

  Random rnd(301);
  int key_idx = 0;

  // Generate at least 2MB of data
  for (int num = 0; num < 20; num++) {
    GenerateNewFile(&rnd, &key_idx);
  }
  ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());
  ASSERT_OK(dbfull()->TEST_WaitForCompact());

  // Will make sure exactly those keys are in the DB after migration.
  std::set<std::string> keys;
  {
    std::unique_ptr<Iterator> it(db_->NewIterator(ReadOptions()));
    it->SeekToFirst();
    for (; it->Valid(); it->Next()) {
      keys.insert(it->key().ToString());
    }
    ASSERT_OK(it->status());
  }
  Close();

  Options new_options = old_options;
  new_options.compaction_style =
      static_cast<CompactionStyle>(compaction_style2_);
  if (new_options.compaction_style == CompactionStyle::kCompactionStyleLevel) {
    new_options.level_compaction_dynamic_level_bytes = is_dynamic2_;
  }
  if (new_options.compaction_style == CompactionStyle::kCompactionStyleFIFO) {
    new_options.max_open_files = -1;
  }
  if (fifo_max_table_files_size_ != 0) {
    new_options.compaction_options_fifo.max_table_files_size =
        fifo_max_table_files_size_;
  }
  new_options.target_file_size_base = 256 * 1024;
  new_options.num_levels = level2_;
  new_options.max_bytes_for_level_base = 150 * 1024;
  new_options.max_bytes_for_level_multiplier = 4;
  ASSERT_OK(OptionChangeMigration(dbname_, old_options, new_options));
  Reopen(new_options);

  // Wait for compaction to finish and make sure it can reopen
  ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  Reopen(new_options);

  {
    std::unique_ptr<Iterator> it(db_->NewIterator(ReadOptions()));
    it->SeekToFirst();
    for (const std::string& key : keys) {
      ASSERT_TRUE(it->Valid());
      ASSERT_EQ(key, it->key().ToString());
      it->Next();
    }
    ASSERT_TRUE(!it->Valid());
    ASSERT_OK(it->status());
  }
}

TEST_P(DBOptionChangeMigrationTests, Migrate2) {
  Options old_options = CurrentOptions();
  old_options.compaction_style =
      static_cast<CompactionStyle>(compaction_style2_);
  if (old_options.compaction_style == CompactionStyle::kCompactionStyleLevel) {
    old_options.level_compaction_dynamic_level_bytes = is_dynamic2_;
  }
  if (old_options.compaction_style == CompactionStyle::kCompactionStyleFIFO) {
    old_options.max_open_files = -1;
  }
  old_options.level0_file_num_compaction_trigger = 3;
  old_options.write_buffer_size = 64 * 1024;
  old_options.target_file_size_base = 128 * 1024;
  // Make level target of L1, L2 to be 200KB and 600KB
  old_options.num_levels = level2_;
  old_options.max_bytes_for_level_multiplier = 3;
  old_options.max_bytes_for_level_base = 200 * 1024;

  Reopen(old_options);

  Random rnd(301);
  int key_idx = 0;

  // Generate at least 2MB of data
  for (int num = 0; num < 20; num++) {
    GenerateNewFile(&rnd, &key_idx);
  }
  ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());
  ASSERT_OK(dbfull()->TEST_WaitForCompact());

  // Will make sure exactly those keys are in the DB after migration.
  std::set<std::string> keys;
  {
    std::unique_ptr<Iterator> it(db_->NewIterator(ReadOptions()));
    it->SeekToFirst();
    for (; it->Valid(); it->Next()) {
      keys.insert(it->key().ToString());
    }
    ASSERT_OK(it->status());
  }

  Close();

  Options new_options = old_options;
  new_options.compaction_style =
      static_cast<CompactionStyle>(compaction_style1_);
  if (new_options.compaction_style == CompactionStyle::kCompactionStyleLevel) {
    new_options.level_compaction_dynamic_level_bytes = is_dynamic1_;
  }
  if (new_options.compaction_style == CompactionStyle::kCompactionStyleFIFO) {
    new_options.max_open_files = -1;
  }
  if (fifo_max_table_files_size_ != 0) {
    new_options.compaction_options_fifo.max_table_files_size =
        fifo_max_table_files_size_;
  }
  new_options.target_file_size_base = 256 * 1024;
  new_options.num_levels = level1_;
  new_options.max_bytes_for_level_base = 150 * 1024;
  new_options.max_bytes_for_level_multiplier = 4;
  ASSERT_OK(OptionChangeMigration(dbname_, old_options, new_options));
  Reopen(new_options);
  // Wait for compaction to finish and make sure it can reopen
  ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  Reopen(new_options);

  {
    std::unique_ptr<Iterator> it(db_->NewIterator(ReadOptions()));
    it->SeekToFirst();
    for (const std::string& key : keys) {
      ASSERT_TRUE(it->Valid());
      ASSERT_EQ(key, it->key().ToString());
      it->Next();
    }
    ASSERT_TRUE(!it->Valid());
    ASSERT_OK(it->status());
  }
}

TEST_P(DBOptionChangeMigrationTests, Migrate3) {
  Options old_options = CurrentOptions();
  old_options.compaction_style =
      static_cast<CompactionStyle>(compaction_style1_);
  if (old_options.compaction_style == CompactionStyle::kCompactionStyleLevel) {
    old_options.level_compaction_dynamic_level_bytes = is_dynamic1_;
  }
  if (old_options.compaction_style == CompactionStyle::kCompactionStyleFIFO) {
    old_options.max_open_files = -1;
  }
  old_options.level0_file_num_compaction_trigger = 3;
  old_options.write_buffer_size = 64 * 1024;
  old_options.target_file_size_base = 128 * 1024;
  // Make level target of L1, L2 to be 200KB and 600KB
  old_options.num_levels = level1_;
  old_options.max_bytes_for_level_multiplier = 3;
  old_options.max_bytes_for_level_base = 200 * 1024;

  Reopen(old_options);
  Random rnd(301);
  for (int num = 0; num < 20; num++) {
    for (int i = 0; i < 50; i++) {
      ASSERT_OK(Put(Key(num * 100 + i), rnd.RandomString(900)));
    }
    ASSERT_OK(Flush());
    ASSERT_OK(dbfull()->TEST_WaitForCompact());
    if (num == 9) {
      // Issue a full compaction to generate some zero-out files
      CompactRangeOptions cro;
      cro.bottommost_level_compaction = BottommostLevelCompaction::kForce;
      ASSERT_OK(dbfull()->CompactRange(cro, nullptr, nullptr));
    }
  }
  ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());
  ASSERT_OK(dbfull()->TEST_WaitForCompact());

  // Will make sure exactly those keys are in the DB after migration.
  std::set<std::string> keys;
  {
    std::unique_ptr<Iterator> it(db_->NewIterator(ReadOptions()));
    it->SeekToFirst();
    for (; it->Valid(); it->Next()) {
      keys.insert(it->key().ToString());
    }
    ASSERT_OK(it->status());
  }
  Close();

  Options new_options = old_options;
  new_options.compaction_style =
      static_cast<CompactionStyle>(compaction_style2_);
  if (new_options.compaction_style == CompactionStyle::kCompactionStyleLevel) {
    new_options.level_compaction_dynamic_level_bytes = is_dynamic2_;
  }
  if (new_options.compaction_style == CompactionStyle::kCompactionStyleFIFO) {
    new_options.max_open_files = -1;
  }
  if (fifo_max_table_files_size_ != 0) {
    new_options.compaction_options_fifo.max_table_files_size =
        fifo_max_table_files_size_;
  }
  new_options.target_file_size_base = 256 * 1024;
  new_options.num_levels = level2_;
  new_options.max_bytes_for_level_base = 150 * 1024;
  new_options.max_bytes_for_level_multiplier = 4;
  ASSERT_OK(OptionChangeMigration(dbname_, old_options, new_options));
  Reopen(new_options);

  // Wait for compaction to finish and make sure it can reopen
  ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  Reopen(new_options);

  {
    std::unique_ptr<Iterator> it(db_->NewIterator(ReadOptions()));
    it->SeekToFirst();
    for (const std::string& key : keys) {
      ASSERT_TRUE(it->Valid());
      ASSERT_EQ(key, it->key().ToString());
      it->Next();
    }
    ASSERT_TRUE(!it->Valid());
    ASSERT_OK(it->status());
  }
}

TEST_P(DBOptionChangeMigrationTests, Migrate4) {
  Options old_options = CurrentOptions();
  old_options.compaction_style =
      static_cast<CompactionStyle>(compaction_style2_);
  if (old_options.compaction_style == CompactionStyle::kCompactionStyleLevel) {
    old_options.level_compaction_dynamic_level_bytes = is_dynamic2_;
  }
  if (old_options.compaction_style == CompactionStyle::kCompactionStyleFIFO) {
    old_options.max_open_files = -1;
  }
  old_options.level0_file_num_compaction_trigger = 3;
  old_options.write_buffer_size = 64 * 1024;
  old_options.target_file_size_base = 128 * 1024;
  // Make level target of L1, L2 to be 200KB and 600KB
  old_options.num_levels = level2_;
  old_options.max_bytes_for_level_multiplier = 3;
  old_options.max_bytes_for_level_base = 200 * 1024;

  Reopen(old_options);
  Random rnd(301);
  for (int num = 0; num < 20; num++) {
    for (int i = 0; i < 50; i++) {
      ASSERT_OK(Put(Key(num * 100 + i), rnd.RandomString(900)));
    }
    ASSERT_OK(Flush());
    ASSERT_OK(dbfull()->TEST_WaitForCompact());
    if (num == 9) {
      // Issue a full compaction to generate some zero-out files
      CompactRangeOptions cro;
      cro.bottommost_level_compaction = BottommostLevelCompaction::kForce;
      ASSERT_OK(dbfull()->CompactRange(cro, nullptr, nullptr));
    }
  }
  ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());
  ASSERT_OK(dbfull()->TEST_WaitForCompact());

  // Will make sure exactly those keys are in the DB after migration.
  std::set<std::string> keys;
  {
    std::unique_ptr<Iterator> it(db_->NewIterator(ReadOptions()));
    it->SeekToFirst();
    for (; it->Valid(); it->Next()) {
      keys.insert(it->key().ToString());
    }
    ASSERT_OK(it->status());
  }

  Close();

  Options new_options = old_options;
  new_options.compaction_style =
      static_cast<CompactionStyle>(compaction_style1_);
  if (new_options.compaction_style == CompactionStyle::kCompactionStyleLevel) {
    new_options.level_compaction_dynamic_level_bytes = is_dynamic1_;
  }
  if (new_options.compaction_style == CompactionStyle::kCompactionStyleFIFO) {
    new_options.max_open_files = -1;
  }
  if (fifo_max_table_files_size_ != 0) {
    new_options.compaction_options_fifo.max_table_files_size =
        fifo_max_table_files_size_;
  }
  new_options.target_file_size_base = 256 * 1024;
  new_options.num_levels = level1_;
  new_options.max_bytes_for_level_base = 150 * 1024;
  new_options.max_bytes_for_level_multiplier = 4;
  ASSERT_OK(OptionChangeMigration(dbname_, old_options, new_options));
  Reopen(new_options);
  // Wait for compaction to finish and make sure it can reopen
  ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  Reopen(new_options);

  {
    std::unique_ptr<Iterator> it(db_->NewIterator(ReadOptions()));
    it->SeekToFirst();
    for (const std::string& key : keys) {
      ASSERT_TRUE(it->Valid());
      ASSERT_EQ(key, it->key().ToString());
      it->Next();
    }
    ASSERT_TRUE(!it->Valid());
    ASSERT_OK(it->status());
  }
}

INSTANTIATE_TEST_CASE_P(
    DBOptionChangeMigrationTests, DBOptionChangeMigrationTests,
    ::testing::Values(
        std::make_tuple(3 /* old num_levels */, 0 /* old compaction style */,
                        false /* is dynamic leveling in old option */,
                        4 /* old num_levels */, 0 /* new compaction style */,
                        false /* is dynamic leveling in new option */,
                        0 /*fifo max_table_files_size*/),
        std::make_tuple(3 /* old num_levels */, 0 /* old compaction style */,
                        true /* is dynamic leveling in old option */,
                        4 /* old num_levels */, 0 /* new compaction style */,
                        true /* is dynamic leveling in new option */,
                        0 /*fifo max_table_files_size*/),
        std::make_tuple(3 /* old num_levels */, 0 /* old compaction style */,
                        true /* is dynamic leveling in old option */,
                        4 /* old num_levels */, 0 /* new compaction style */,
                        false, 0 /*fifo max_table_files_size*/),
        std::make_tuple(3 /* old num_levels */, 0 /* old compaction style */,
                        false /* is dynamic leveling in old option */,
                        4 /* old num_levels */, 0 /* new compaction style */,
                        true /* is dynamic leveling in new option */,
                        0 /*fifo max_table_files_size*/),
        std::make_tuple(3 /* old num_levels */, 1 /* old compaction style */,
                        false /* is dynamic leveling in old option */,
                        4 /* old num_levels */, 1 /* new compaction style */,
                        false /* is dynamic leveling in new option */,
                        0 /*fifo max_table_files_size*/),
        std::make_tuple(1 /* old num_levels */, 1 /* old compaction style */,
                        false /* is dynamic leveling in old option */,
                        4 /* old num_levels */, 1 /* new compaction style */,
                        false /* is dynamic leveling in new option */,
                        0 /*fifo max_table_files_size*/),
        std::make_tuple(3 /* old num_levels */, 0 /* old compaction style */,
                        false /* is dynamic leveling in old option */,
                        4 /* old num_levels */, 1 /* new compaction style */,
                        false /* is dynamic leveling in new option */,
                        0 /*fifo max_table_files_size*/),
        std::make_tuple(3 /* old num_levels */, 0 /* old compaction style */,
                        false /* is dynamic leveling in old option */,
                        1 /* old num_levels */, 1 /* new compaction style */,
                        false /* is dynamic leveling in new option */,
                        0 /*fifo max_table_files_size*/),
        std::make_tuple(3 /* old num_levels */, 0 /* old compaction style */,
                        true /* is dynamic leveling in old option */,
                        4 /* old num_levels */, 1 /* new compaction style */,
                        false /* is dynamic leveling in new option */,
                        0 /*fifo max_table_files_size*/),
        std::make_tuple(3 /* old num_levels */, 0 /* old compaction style */,
                        true /* is dynamic leveling in old option */,
                        1 /* old num_levels */, 1 /* new compaction style */,
                        false /* is dynamic leveling in new option */,
                        0 /*fifo max_table_files_size*/),
        std::make_tuple(1 /* old num_levels */, 1 /* old compaction style */,
                        false /* is dynamic leveling in old option */,
                        4 /* old num_levels */, 0 /* new compaction style */,
                        false /* is dynamic leveling in new option */,
                        0 /*fifo max_table_files_size*/),
        std::make_tuple(4 /* old num_levels */, 0 /* old compaction style */,
                        false /* is dynamic leveling in old option */,
                        1 /* old num_levels */, 2 /* new compaction style */,
                        false /* is dynamic leveling in new option */,
                        0 /*fifo max_table_files_size*/),
        std::make_tuple(3 /* old num_levels */, 0 /* old compaction style */,
                        true /* is dynamic leveling in old option */,
                        2 /* old num_levels */, 2 /* new compaction style */,
                        false /* is dynamic leveling in new option */,
                        0 /*fifo max_table_files_size*/),
        std::make_tuple(3 /* old num_levels */, 1 /* old compaction style */,
                        false /* is dynamic leveling in old option */,
                        3 /* old num_levels */, 2 /* new compaction style */,
                        false /* is dynamic leveling in new option */,
                        0 /*fifo max_table_files_size*/),
        std::make_tuple(1 /* old num_levels */, 1 /* old compaction style */,
                        false /* is dynamic leveling in old option */,
                        4 /* old num_levels */, 2 /* new compaction style */,
                        false /* is dynamic leveling in new option */, 0),
        std::make_tuple(
            4 /* old num_levels */, 0 /* old compaction style */,
            false /* is dynamic leveling in old option */,
            1 /* old num_levels */, 2 /* new compaction style */,
            false /* is dynamic leveling in new option */,
            std::numeric_limits<uint64_t>::max() /*fifo max_table_files_size*/),
        std::make_tuple(
            3 /* old num_levels */, 0 /* old compaction style */,
            true /* is dynamic leveling in old option */,
            2 /* old num_levels */, 2 /* new compaction style */,
            false /* is dynamic leveling in new option */,
            std::numeric_limits<uint64_t>::max() /*fifo max_table_files_size*/),
        std::make_tuple(
            3 /* old num_levels */, 1 /* old compaction style */,
            false /* is dynamic leveling in old option */,
            3 /* old num_levels */, 2 /* new compaction style */,
            false /* is dynamic leveling in new option */,
            std::numeric_limits<uint64_t>::max() /*fifo max_table_files_size*/),
        std::make_tuple(1 /* old num_levels */, 1 /* old compaction style */,
                        false /* is dynamic leveling in old option */,
                        4 /* old num_levels */, 2 /* new compaction style */,
                        false /* is dynamic leveling in new option */,
                        std::numeric_limits<
                            uint64_t>::max() /*fifo max_table_files_size*/)));

class DBOptionChangeMigrationTest : public DBTestBase {
 public:
  DBOptionChangeMigrationTest()
      : DBTestBase("db_option_change_migration_test2", /*env_do_fsync=*/true) {}
};

TEST_F(DBOptionChangeMigrationTest, CompactedSrcToUniversal) {
  Options old_options = CurrentOptions();
  old_options.compaction_style = CompactionStyle::kCompactionStyleLevel;
  old_options.max_compaction_bytes = 200 * 1024;
  old_options.level_compaction_dynamic_level_bytes = false;
  old_options.level0_file_num_compaction_trigger = 3;
  old_options.write_buffer_size = 64 * 1024;
  old_options.target_file_size_base = 128 * 1024;
  // Make level target of L1, L2 to be 200KB and 600KB
  old_options.num_levels = 4;
  old_options.max_bytes_for_level_multiplier = 3;
  old_options.max_bytes_for_level_base = 200 * 1024;

  Reopen(old_options);
  Random rnd(301);
  for (int num = 0; num < 20; num++) {
    for (int i = 0; i < 50; i++) {
      ASSERT_OK(Put(Key(num * 100 + i), rnd.RandomString(900)));
    }
  }
  ASSERT_OK(Flush());
  CompactRangeOptions cro;
  cro.bottommost_level_compaction = BottommostLevelCompaction::kForce;
  ASSERT_OK(dbfull()->CompactRange(cro, nullptr, nullptr));

  // Will make sure exactly those keys are in the DB after migration.
  std::set<std::string> keys;
  {
    std::unique_ptr<Iterator> it(db_->NewIterator(ReadOptions()));
    it->SeekToFirst();
    for (; it->Valid(); it->Next()) {
      keys.insert(it->key().ToString());
    }
    ASSERT_OK(it->status());
  }

  Close();

  Options new_options = old_options;
  new_options.compaction_style = CompactionStyle::kCompactionStyleUniversal;
  new_options.target_file_size_base = 256 * 1024;
  new_options.num_levels = 1;
  new_options.max_bytes_for_level_base = 150 * 1024;
  new_options.max_bytes_for_level_multiplier = 4;
  ASSERT_OK(OptionChangeMigration(dbname_, old_options, new_options));
  Reopen(new_options);
  // Wait for compaction to finish and make sure it can reopen
  ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  Reopen(new_options);

  {
    std::unique_ptr<Iterator> it(db_->NewIterator(ReadOptions()));
    it->SeekToFirst();
    for (const std::string& key : keys) {
      ASSERT_TRUE(it->Valid());
      ASSERT_EQ(key, it->key().ToString());
      it->Next();
    }
    ASSERT_TRUE(!it->Valid());
    ASSERT_OK(it->status());
  }
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

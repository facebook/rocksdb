//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "rocksdb/utilities/option_change_migration.h"
#include <set>
#include "db/db_test_util.h"
#include "port/stack_trace.h"
namespace rocksdb {

class DBOptionChangeMigrationTest
    : public DBTestBase,
      public testing::WithParamInterface<
          std::tuple<int, bool, bool, int, bool, bool>> {
 public:
  DBOptionChangeMigrationTest()
      : DBTestBase("/db_option_change_migration_test") {
    level1_ = std::get<0>(GetParam());
    is_universal1_ = std::get<1>(GetParam());
    is_dynamic1_ = std::get<2>(GetParam());

    level2_ = std::get<3>(GetParam());
    is_universal2_ = std::get<4>(GetParam());
    is_dynamic2_ = std::get<5>(GetParam());
  }

  // Required if inheriting from testing::WithParamInterface<>
  static void SetUpTestCase() {}
  static void TearDownTestCase() {}

  int level1_;
  bool is_universal1_;
  bool is_dynamic1_;

  int level2_;
  bool is_universal2_;
  bool is_dynamic2_;
};

#ifndef ROCKSDB_LITE
TEST_P(DBOptionChangeMigrationTest, Migrate1) {
  Options old_options = CurrentOptions();
  if (is_universal1_) {
    old_options.compaction_style = CompactionStyle::kCompactionStyleUniversal;
  } else {
    old_options.compaction_style = CompactionStyle::kCompactionStyleLevel;
    old_options.level_compaction_dynamic_level_bytes = is_dynamic1_;
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
  dbfull()->TEST_WaitForFlushMemTable();
  dbfull()->TEST_WaitForCompact();

  // Will make sure exactly those keys are in the DB after migration.
  std::set<std::string> keys;
  {
    std::unique_ptr<Iterator> it(db_->NewIterator(ReadOptions()));
    it->SeekToFirst();
    for (; it->Valid(); it->Next()) {
      keys.insert(it->key().ToString());
    }
  }
  Close();

  Options new_options = old_options;
  if (is_universal2_) {
    new_options.compaction_style = CompactionStyle::kCompactionStyleUniversal;
  } else {
    new_options.compaction_style = CompactionStyle::kCompactionStyleLevel;
    new_options.level_compaction_dynamic_level_bytes = is_dynamic2_;
  }
  new_options.target_file_size_base = 256 * 1024;
  new_options.num_levels = level2_;
  new_options.max_bytes_for_level_base = 150 * 1024;
  new_options.max_bytes_for_level_multiplier = 4;
  ASSERT_OK(OptionChangeMigration(dbname_, old_options, new_options));
  Reopen(new_options);

  // Wait for compaction to finish and make sure it can reopen
  dbfull()->TEST_WaitForFlushMemTable();
  dbfull()->TEST_WaitForCompact();
  Reopen(new_options);

  {
    std::unique_ptr<Iterator> it(db_->NewIterator(ReadOptions()));
    it->SeekToFirst();
    for (std::string key : keys) {
      ASSERT_TRUE(it->Valid());
      ASSERT_EQ(key, it->key().ToString());
      it->Next();
    }
    ASSERT_TRUE(!it->Valid());
  }
}

TEST_P(DBOptionChangeMigrationTest, Migrate2) {
  Options old_options = CurrentOptions();
  if (is_universal2_) {
    old_options.compaction_style = CompactionStyle::kCompactionStyleUniversal;
  } else {
    old_options.compaction_style = CompactionStyle::kCompactionStyleLevel;
    old_options.level_compaction_dynamic_level_bytes = is_dynamic2_;
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
  dbfull()->TEST_WaitForFlushMemTable();
  dbfull()->TEST_WaitForCompact();

  // Will make sure exactly those keys are in the DB after migration.
  std::set<std::string> keys;
  {
    std::unique_ptr<Iterator> it(db_->NewIterator(ReadOptions()));
    it->SeekToFirst();
    for (; it->Valid(); it->Next()) {
      keys.insert(it->key().ToString());
    }
  }

  Close();

  Options new_options = old_options;
  if (is_universal1_) {
    new_options.compaction_style = CompactionStyle::kCompactionStyleUniversal;
  } else {
    new_options.compaction_style = CompactionStyle::kCompactionStyleLevel;
    new_options.level_compaction_dynamic_level_bytes = is_dynamic1_;
  }
  new_options.target_file_size_base = 256 * 1024;
  new_options.num_levels = level1_;
  new_options.max_bytes_for_level_base = 150 * 1024;
  new_options.max_bytes_for_level_multiplier = 4;
  ASSERT_OK(OptionChangeMigration(dbname_, old_options, new_options));
  Reopen(new_options);
  // Wait for compaction to finish and make sure it can reopen
  dbfull()->TEST_WaitForFlushMemTable();
  dbfull()->TEST_WaitForCompact();
  Reopen(new_options);

  {
    std::unique_ptr<Iterator> it(db_->NewIterator(ReadOptions()));
    it->SeekToFirst();
    for (std::string key : keys) {
      ASSERT_TRUE(it->Valid());
      ASSERT_EQ(key, it->key().ToString());
      it->Next();
    }
    ASSERT_TRUE(!it->Valid());
  }
}

INSTANTIATE_TEST_CASE_P(
    DBOptionChangeMigrationTest, DBOptionChangeMigrationTest,
    ::testing::Values(std::make_tuple(3, false, false, 4, false, false),
                      std::make_tuple(3, false, true, 4, false, true),
                      std::make_tuple(3, false, true, 4, false, false),
                      std::make_tuple(3, false, false, 4, false, true),
                      std::make_tuple(3, true, false, 4, true, false),
                      std::make_tuple(1, true, false, 4, true, false),
                      std::make_tuple(3, false, false, 4, true, false),
                      std::make_tuple(3, false, false, 1, true, false),
                      std::make_tuple(3, false, true, 4, true, false),
                      std::make_tuple(3, false, true, 1, true, false),
                      std::make_tuple(1, true, false, 4, false, false)));

#endif  // ROCKSDB_LITE
}  // namespace rocksdb

int main(int argc, char** argv) {
  rocksdb::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

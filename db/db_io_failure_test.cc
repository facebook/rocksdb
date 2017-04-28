//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//  This source code is also licensed under the GPLv2 license found in the
//  COPYING file in the root directory of this source tree.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/db_test_util.h"
#include "port/stack_trace.h"

namespace rocksdb {

class DBIOFailureTest : public DBTestBase {
 public:
  DBIOFailureTest() : DBTestBase("/db_io_failure_test") {}
};

#ifndef ROCKSDB_LITE
// Check that number of files does not grow when writes are dropped
TEST_F(DBIOFailureTest, DropWrites) {
  do {
    Options options = CurrentOptions();
    options.env = env_;
    options.paranoid_checks = false;
    Reopen(options);

    ASSERT_OK(Put("foo", "v1"));
    ASSERT_EQ("v1", Get("foo"));
    Compact("a", "z");
    const size_t num_files = CountFiles();
    // Force out-of-space errors
    env_->drop_writes_.store(true, std::memory_order_release);
    env_->sleep_counter_.Reset();
    env_->no_slowdown_ = true;
    for (int i = 0; i < 5; i++) {
      if (option_config_ != kUniversalCompactionMultiLevel &&
          option_config_ != kUniversalSubcompactions) {
        for (int level = 0; level < dbfull()->NumberLevels(); level++) {
          if (level > 0 && level == dbfull()->NumberLevels() - 1) {
            break;
          }
          dbfull()->TEST_CompactRange(level, nullptr, nullptr, nullptr,
                                      true /* disallow trivial move */);
        }
      } else {
        dbfull()->CompactRange(CompactRangeOptions(), nullptr, nullptr);
      }
    }

    std::string property_value;
    ASSERT_TRUE(db_->GetProperty("rocksdb.background-errors", &property_value));
    ASSERT_EQ("5", property_value);

    env_->drop_writes_.store(false, std::memory_order_release);
    ASSERT_LT(CountFiles(), num_files + 3);

    // Check that compaction attempts slept after errors
    // TODO @krad: Figure out why ASSERT_EQ 5 keeps failing in certain compiler
    // versions
    ASSERT_GE(env_->sleep_counter_.Read(), 4);
  } while (ChangeCompactOptions());
}

// Check background error counter bumped on flush failures.
TEST_F(DBIOFailureTest, DropWritesFlush) {
  do {
    Options options = CurrentOptions();
    options.env = env_;
    options.max_background_flushes = 1;
    Reopen(options);

    ASSERT_OK(Put("foo", "v1"));
    // Force out-of-space errors
    env_->drop_writes_.store(true, std::memory_order_release);

    std::string property_value;
    // Background error count is 0 now.
    ASSERT_TRUE(db_->GetProperty("rocksdb.background-errors", &property_value));
    ASSERT_EQ("0", property_value);

    dbfull()->TEST_FlushMemTable(true);

    ASSERT_TRUE(db_->GetProperty("rocksdb.background-errors", &property_value));
    ASSERT_EQ("1", property_value);

    env_->drop_writes_.store(false, std::memory_order_release);
  } while (ChangeCompactOptions());
}
#endif  // ROCKSDB_LITE

// Check that CompactRange() returns failure if there is not enough space left
// on device
TEST_F(DBIOFailureTest, NoSpaceCompactRange) {
  do {
    Options options = CurrentOptions();
    options.env = env_;
    options.disable_auto_compactions = true;
    Reopen(options);

    // generate 5 tables
    for (int i = 0; i < 5; ++i) {
      ASSERT_OK(Put(Key(i), Key(i) + "v"));
      ASSERT_OK(Flush());
    }

    // Force out-of-space errors
    env_->no_space_.store(true, std::memory_order_release);

    Status s = dbfull()->TEST_CompactRange(0, nullptr, nullptr, nullptr,
                                           true /* disallow trivial move */);
    ASSERT_TRUE(s.IsIOError());
    ASSERT_TRUE(s.IsNoSpace());

    env_->no_space_.store(false, std::memory_order_release);
  } while (ChangeCompactOptions());
}

TEST_F(DBIOFailureTest, NonWritableFileSystem) {
  do {
    Options options = CurrentOptions();
    options.write_buffer_size = 4096;
    options.arena_block_size = 4096;
    options.env = env_;
    Reopen(options);
    ASSERT_OK(Put("foo", "v1"));
    env_->non_writeable_rate_.store(100);
    std::string big(100000, 'x');
    int errors = 0;
    for (int i = 0; i < 20; i++) {
      if (!Put("foo", big).ok()) {
        errors++;
        env_->SleepForMicroseconds(100000);
      }
    }
    ASSERT_GT(errors, 0);
    env_->non_writeable_rate_.store(0);
  } while (ChangeCompactOptions());
}

#ifndef ROCKSDB_LITE
TEST_F(DBIOFailureTest, ManifestWriteError) {
  // Test for the following problem:
  // (a) Compaction produces file F
  // (b) Log record containing F is written to MANIFEST file, but Sync() fails
  // (c) GC deletes F
  // (d) After reopening DB, reads fail since deleted F is named in log record

  // We iterate twice.  In the second iteration, everything is the
  // same except the log record never makes it to the MANIFEST file.
  for (int iter = 0; iter < 2; iter++) {
    std::atomic<bool>* error_type = (iter == 0) ? &env_->manifest_sync_error_
                                                : &env_->manifest_write_error_;

    // Insert foo=>bar mapping
    Options options = CurrentOptions();
    options.env = env_;
    options.create_if_missing = true;
    options.error_if_exists = false;
    options.paranoid_checks = true;
    DestroyAndReopen(options);
    ASSERT_OK(Put("foo", "bar"));
    ASSERT_EQ("bar", Get("foo"));

    // Memtable compaction (will succeed)
    Flush();
    ASSERT_EQ("bar", Get("foo"));
    const int last = 2;
    MoveFilesToLevel(2);
    ASSERT_EQ(NumTableFilesAtLevel(last), 1);  // foo=>bar is now in last level

    // Merging compaction (will fail)
    error_type->store(true, std::memory_order_release);
    dbfull()->TEST_CompactRange(last, nullptr, nullptr);  // Should fail
    ASSERT_EQ("bar", Get("foo"));

    error_type->store(false, std::memory_order_release);

    // Since paranoid_checks=true, writes should fail
    ASSERT_NOK(Put("foo2", "bar2"));

    // Recovery: should not lose data
    ASSERT_EQ("bar", Get("foo"));

    // Try again with paranoid_checks=false
    Close();
    options.paranoid_checks = false;
    Reopen(options);

    // Merging compaction (will fail)
    error_type->store(true, std::memory_order_release);
    dbfull()->TEST_CompactRange(last, nullptr, nullptr);  // Should fail
    ASSERT_EQ("bar", Get("foo"));

    // Recovery: should not lose data
    error_type->store(false, std::memory_order_release);
    Reopen(options);
    ASSERT_EQ("bar", Get("foo"));

    // Since paranoid_checks=false, writes should succeed
    ASSERT_OK(Put("foo2", "bar2"));
    ASSERT_EQ("bar", Get("foo"));
    ASSERT_EQ("bar2", Get("foo2"));
  }
}
#endif  // ROCKSDB_LITE

TEST_F(DBIOFailureTest, PutFailsParanoid) {
  // Test the following:
  // (a) A random put fails in paranoid mode (simulate by sync fail)
  // (b) All other puts have to fail, even if writes would succeed
  // (c) All of that should happen ONLY if paranoid_checks = true

  Options options = CurrentOptions();
  options.env = env_;
  options.create_if_missing = true;
  options.error_if_exists = false;
  options.paranoid_checks = true;
  DestroyAndReopen(options);
  CreateAndReopenWithCF({"pikachu"}, options);
  Status s;

  ASSERT_OK(Put(1, "foo", "bar"));
  ASSERT_OK(Put(1, "foo1", "bar1"));
  // simulate error
  env_->log_write_error_.store(true, std::memory_order_release);
  s = Put(1, "foo2", "bar2");
  ASSERT_TRUE(!s.ok());
  env_->log_write_error_.store(false, std::memory_order_release);
  s = Put(1, "foo3", "bar3");
  // the next put should fail, too
  ASSERT_TRUE(!s.ok());
  // but we're still able to read
  ASSERT_EQ("bar", Get(1, "foo"));

  // do the same thing with paranoid checks off
  options.paranoid_checks = false;
  DestroyAndReopen(options);
  CreateAndReopenWithCF({"pikachu"}, options);

  ASSERT_OK(Put(1, "foo", "bar"));
  ASSERT_OK(Put(1, "foo1", "bar1"));
  // simulate error
  env_->log_write_error_.store(true, std::memory_order_release);
  s = Put(1, "foo2", "bar2");
  ASSERT_TRUE(!s.ok());
  env_->log_write_error_.store(false, std::memory_order_release);
  s = Put(1, "foo3", "bar3");
  // the next put should NOT fail
  ASSERT_TRUE(s.ok());
}

}  // namespace rocksdb

int main(int argc, char** argv) {
  rocksdb::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

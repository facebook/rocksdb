//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#include "db/db_test_util.h"
#include "port/stack_trace.h"
#include "util/sync_point.h"

namespace rocksdb {

class DBOptionsTest : public DBTestBase {
 public:
  DBOptionsTest() : DBTestBase("/db_options_test") {}
};

// When write stalls, user can enable auto compaction to unblock writes.
// However, we had an issue where the stalled write thread blocks the attempt
// to persist auto compaction option, thus creating a deadlock. The test
// verifies the issue is fixed.
TEST_F(DBOptionsTest, EnableAutoCompactionToUnblockWrites) {
  Options options;
  options.disable_auto_compactions = true;
  options.write_buffer_size = 1000 * 1000;  // 1M
  options.level0_file_num_compaction_trigger = 1;
  options.level0_slowdown_writes_trigger = 1;
  options.level0_stop_writes_trigger = 1;
  options.compression = kNoCompression;

  SyncPoint::GetInstance()->LoadDependency(
      {{"DBImpl::DelayWrite:Wait",
        "DBOptionsTest::EnableAutoCompactionToUnblockWrites:1"},
       {"DBImpl::BackgroundCompaction:Finish",
        "DBOptionsTest::EnableAutoCompactionToUnblockWrites:1"}});
  SyncPoint::GetInstance()->EnableProcessing();

  // Stall writes.
  Reopen(options);
  env_->StartThread(
      [](void* arg) {
        std::string value(1000, 'v');
        auto* t = static_cast<DBOptionsTest*>(arg);
        for (int i = 0; i < 2000; i++) {
          ASSERT_OK(t->Put(t->Key(i), value));
        }
      },
      this);
  TEST_SYNC_POINT("DBOptionsTest::EnableAutoCompactionToUnblockWrites:1");
  ASSERT_TRUE(dbfull()->TEST_write_controler().IsStopped());
  ColumnFamilyHandle* handle = dbfull()->DefaultColumnFamily();
  // We will get a deadlock here if we hit the issue.
  dbfull()->EnableAutoCompaction({handle});
  env_->WaitForJoin();
}

// Similar to EnableAutoCompactionAfterStallDeadlock. See comments there.
TEST_F(DBOptionsTest, ToggleStopTriggerToUnblockWrites) {
  Options options;
  options.disable_auto_compactions = true;
  options.write_buffer_size = 1000 * 1000;  // 1M
  options.level0_file_num_compaction_trigger = 1;
  options.level0_slowdown_writes_trigger = 1;
  options.level0_stop_writes_trigger = 1;
  options.compression = kNoCompression;

  SyncPoint::GetInstance()->LoadDependency(
      {{"DBImpl::DelayWrite:Wait",
        "DBOptionsTest::ToggleStopTriggerToUnblockWrites:1"},
       {"DBImpl::BackgroundCompaction:Finish",
        "DBOptionsTest::ToggleStopTriggerToUnblockWrites:1"}});
  SyncPoint::GetInstance()->EnableProcessing();

  // Stall writes.
  Reopen(options);
  env_->StartThread(
      [](void* arg) {
        std::string value(1000, 'v');
        auto* t = static_cast<DBOptionsTest*>(arg);
        for (int i = 0; i < 2000; i++) {
          ASSERT_OK(t->Put(t->Key(i), value));
        }
      },
      this);
  TEST_SYNC_POINT("DBOptionsTest::ToggleStopTriggerToUnblockWrites:1");
  ASSERT_TRUE(dbfull()->TEST_write_controler().IsStopped());
  // We will get a deadlock here if we hit the issue.
  dbfull()->SetOptions({{"level0_stop_writes_trigger", "1000000"},
                        {"level0_slowdown_writes_trigger", "1000000"}});
  env_->WaitForJoin();
}

}  // namespace rocksdb

int main(int argc, char** argv) {
  rocksdb::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

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

class DBFlushTest : public DBTestBase {
 public:
  DBFlushTest() : DBTestBase("/db_flush_test") {}
};

// We had issue when two background threads trying to flush at the same time,
// only one of them get committed. The test verifies the issue is fixed.
TEST_F(DBFlushTest, FlushWhileWritingManifest) {
  Options options;
  options.disable_auto_compactions = true;
  options.max_background_flushes = 2;
  Reopen(options);
  FlushOptions no_wait;
  no_wait.wait = false;

  SyncPoint::GetInstance()->LoadDependency(
      {{"VersionSet::LogAndApply:WriteManifest",
        "DBFlushTest::FlushWhileWritingManifest:1"},
       {"MemTableList::InstallMemtableFlushResults:InProgress",
        "VersionSet::LogAndApply:WriteManifestDone"}});
  SyncPoint::GetInstance()->EnableProcessing();

  ASSERT_OK(Put("foo", "v"));
  ASSERT_OK(dbfull()->Flush(no_wait));
  TEST_SYNC_POINT("DBFlushTest::FlushWhileWritingManifest:1");
  ASSERT_OK(Put("bar", "v"));
  ASSERT_OK(dbfull()->Flush(no_wait));
  // If the issue is hit we will wait here forever.
  dbfull()->TEST_WaitForFlushMemTable();
#ifndef ROCKSDB_LITE
  ASSERT_EQ(2, TotalTableFiles());
#endif  // ROCKSDB_LITE
}

}  // namespace rocksdb

int main(int argc, char** argv) {
  rocksdb::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

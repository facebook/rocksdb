//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#include "db/db_test_util.h"
#include "port/stack_trace.h"
#include "rocksdb/perf_context.h"
#include "util/fault_injection_test_env.h"
#if !defined(ROCKSDB_LITE)
#include "util/sync_point.h"
#endif

namespace rocksdb {

class DBErrorHandlingTest : public DBTestBase {
 public:
  DBErrorHandlingTest() : DBTestBase("/db_error_handling_test") {}
};

class DBErrorHandlingEnv : public EnvWrapper {
  public:
    DBErrorHandlingEnv() : EnvWrapper(Env::Default()),
      trig_no_space(false), trig_io_error(false) {}

    void SetTrigNoSpace() {trig_no_space = true;}
    void SetTrigIoError() {trig_io_error = true;}
  private:
    bool trig_no_space;
    bool trig_io_error;
};

TEST_F(DBErrorHandlingTest, FLushWriteError) {
  std::unique_ptr<FaultInjectionTestEnv> fault_env(
      new FaultInjectionTestEnv(Env::Default()));
  Options options = GetDefaultOptions();
  options.create_if_missing = true;
  options.env = fault_env.get();
  Status s;
  DestroyAndReopen(options);

  Put(Key(0), "va;");
  SyncPoint::GetInstance()->SetCallBack(
      "FlushJob::Start", [&](void *) {
    fault_env->SetFilesystemActive(false, Status::NoSpace("Out of space"));
  });
  SyncPoint::GetInstance()->EnableProcessing();
  s = Flush();
  ASSERT_EQ(s.severity(), rocksdb::Status::Severity::kSoftError);
  fault_env->SetFilesystemActive(true);
  s = dbfull()->Resume();
  ASSERT_EQ(s, Status::OK());

  Destroy(options);
}

TEST_F(DBErrorHandlingTest, CompactionWriteError) {
  std::unique_ptr<FaultInjectionTestEnv> fault_env(
      new FaultInjectionTestEnv(Env::Default()));
  Options options = GetDefaultOptions();
  options.create_if_missing = true;
  options.level0_file_num_compaction_trigger = 2;
  options.env = fault_env.get();
  Status s;
  DestroyAndReopen(options);

  Put(Key(0), "va;");
  Put(Key(2), "va;");
  s = Flush();
  ASSERT_EQ(s, Status::OK());

  rocksdb::SyncPoint::GetInstance()->LoadDependency(
      {{"FlushMemTableFinished", "BackgroundCallCompaction:0"}});
  rocksdb::SyncPoint::GetInstance()->SetCallBack(
      "BackgroundCallCompaction:0", [&](void *) {
      fault_env->SetFilesystemActive(false, Status::NoSpace("Out of space"));
      });
  rocksdb::SyncPoint::GetInstance()->EnableProcessing();

  Put(Key(1), "val");
  s = Flush();
  ASSERT_EQ(s, Status::OK());

  s = dbfull()->TEST_WaitForCompact();
  ASSERT_EQ(s.severity(), rocksdb::Status::Severity::kSoftError);

  fault_env->SetFilesystemActive(true);
  s = dbfull()->Resume();
  ASSERT_EQ(s, Status::OK());
  Destroy(options);
}

TEST_F(DBErrorHandlingTest, CorruptionError) {
  std::unique_ptr<FaultInjectionTestEnv> fault_env(
      new FaultInjectionTestEnv(Env::Default()));
  Options options = GetDefaultOptions();
  options.create_if_missing = true;
  options.level0_file_num_compaction_trigger = 2;
  options.env = fault_env.get();
  Status s;
  DestroyAndReopen(options);

  Put(Key(0), "va;");
  Put(Key(2), "va;");
  s = Flush();
  ASSERT_EQ(s, Status::OK());

  rocksdb::SyncPoint::GetInstance()->LoadDependency(
      {{"FlushMemTableFinished", "BackgroundCallCompaction:0"}});
  rocksdb::SyncPoint::GetInstance()->SetCallBack(
      "BackgroundCallCompaction:0", [&](void *) {
      fault_env->SetFilesystemActive(false, Status::Corruption("Corruption"));
      });
  rocksdb::SyncPoint::GetInstance()->EnableProcessing();

  Put(Key(1), "val");
  s = Flush();
  ASSERT_EQ(s, Status::OK());

  s = dbfull()->TEST_WaitForCompact();
  ASSERT_EQ(s.severity(), rocksdb::Status::Severity::kUnrecoverableError);

  fault_env->SetFilesystemActive(true);
  s = dbfull()->Resume();
  ASSERT_NE(s, Status::OK());
  Destroy(options);
}

}  // namespace rocksdb

int main(int argc, char** argv) {
  rocksdb::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

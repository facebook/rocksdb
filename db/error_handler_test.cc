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

class ErrorHandlerListener : public EventListener {
 public:
  ErrorHandlerListener()
      : mutex_(),
        cv_(&mutex_),
        no_auto_recovery_(false),
        recovery_complete_(false) {}

  void OnErrorRecoveryBegin(BackgroundErrorReason /*reason*/,
                            Status /*bg_error*/, bool* auto_recovery) {
    if (*auto_recovery && no_auto_recovery_) {
      *auto_recovery = false;
    }
  }

  void OnErrorRecoveryCompleted(Status /*old_bg_error*/) {
    InstrumentedMutexLock l(&mutex_);
    recovery_complete_ = true;
    cv_.SignalAll();
  }

  bool WaitForRecovery(uint64_t /*abs_time_us*/) {
    InstrumentedMutexLock l(&mutex_);
    if (!recovery_complete_) {
      cv_.Wait(/*abs_time_us*/);
    }
    if (recovery_complete_) {
      recovery_complete_ = false;
      return true;
    }
    return false;
  }

  void EnableAutoRecovery(bool enable = true) { no_auto_recovery_ = !enable; }

 private:
  InstrumentedMutex mutex_;
  InstrumentedCondVar cv_;
  bool no_auto_recovery_;
  bool recovery_complete_;
};

TEST_F(DBErrorHandlingTest, FLushWriteError) {
  std::unique_ptr<FaultInjectionTestEnv> fault_env(
      new FaultInjectionTestEnv(Env::Default()));
  ErrorHandlerListener* listener = new ErrorHandlerListener();
  Options options = GetDefaultOptions();
  options.create_if_missing = true;
  options.env = fault_env.get();
  options.listeners.emplace_back(listener);
  Status s;

  listener->EnableAutoRecovery(false);
  DestroyAndReopen(options);

  Put(Key(0), "val");
  SyncPoint::GetInstance()->SetCallBack(
      "FlushJob::Start", [&](void *) {
    fault_env->SetFilesystemActive(false, Status::NoSpace("Out of space"));
  });
  SyncPoint::GetInstance()->EnableProcessing();
  s = Flush();
  ASSERT_EQ(s.severity(), rocksdb::Status::Severity::kHardError);
  SyncPoint::GetInstance()->DisableProcessing();
  fault_env->SetFilesystemActive(true);
  s = dbfull()->Resume();
  ASSERT_EQ(s, Status::OK());

  Reopen(options);
  ASSERT_EQ("val", Get(Key(0)));
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

TEST_F(DBErrorHandlingTest, AutoRecoverFlushError) {
  std::unique_ptr<FaultInjectionTestEnv> fault_env(
      new FaultInjectionTestEnv(Env::Default()));
  ErrorHandlerListener* listener = new ErrorHandlerListener();
  Options options = GetDefaultOptions();
  options.create_if_missing = true;
  options.env = fault_env.get();
  options.listeners.emplace_back(listener);
  Status s;

  listener->EnableAutoRecovery();
  DestroyAndReopen(options);

  Put(Key(0), "val");
  SyncPoint::GetInstance()->SetCallBack("FlushJob::Start", [&](void*) {
    fault_env->SetFilesystemActive(false, Status::NoSpace("Out of space"));
  });
  SyncPoint::GetInstance()->EnableProcessing();
  s = Flush();
  ASSERT_EQ(s.severity(), rocksdb::Status::Severity::kHardError);
  SyncPoint::GetInstance()->DisableProcessing();
  fault_env->SetFilesystemActive(true);
  ASSERT_EQ(listener->WaitForRecovery(5000000), true);

  s = Put(Key(1), "val");
  ASSERT_EQ(s, Status::OK());

  Reopen(options);
  ASSERT_EQ("val", Get(Key(0)));
  ASSERT_EQ("val", Get(Key(1)));
  Destroy(options);
}

TEST_F(DBErrorHandlingTest, FailRecoverFlushError) {
  std::unique_ptr<FaultInjectionTestEnv> fault_env(
      new FaultInjectionTestEnv(Env::Default()));
  ErrorHandlerListener* listener = new ErrorHandlerListener();
  Options options = GetDefaultOptions();
  options.create_if_missing = true;
  options.env = fault_env.get();
  options.listeners.emplace_back(listener);
  Status s;

  listener->EnableAutoRecovery();
  DestroyAndReopen(options);

  Put(Key(0), "val");
  SyncPoint::GetInstance()->SetCallBack("FlushJob::Start", [&](void*) {
    fault_env->SetFilesystemActive(false, Status::NoSpace("Out of space"));
  });
  SyncPoint::GetInstance()->EnableProcessing();
  s = Flush();
  ASSERT_EQ(s.severity(), rocksdb::Status::Severity::kHardError);
  // We should be able to shutdown the database while auto recovery is going
  // on in the background
  Close();
  DestroyDB(dbname_, options);
}

}  // namespace rocksdb

int main(int argc, char** argv) {
  rocksdb::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#ifndef ROCKSDB_LITE

#include "db/db_test_util.h"
#include "port/stack_trace.h"
#include "rocksdb/io_status.h"
#include "rocksdb/perf_context.h"
#include "rocksdb/sst_file_manager.h"
#include "test_util/fault_injection_test_env.h"
#include "test_util/fault_injection_test_fs.h"
#if !defined(ROCKSDB_LITE)
#include "test_util/sync_point.h"
#endif

namespace ROCKSDB_NAMESPACE {

class DBErrorHandlingFSTest : public DBTestBase {
 public:
  DBErrorHandlingFSTest() : DBTestBase("/db_error_handling_fs_test") {}

  std::string GetManifestNameFromLiveFiles() {
    std::vector<std::string> live_files;
    uint64_t manifest_size;

    dbfull()->GetLiveFiles(live_files, &manifest_size, false);
    for (auto& file : live_files) {
      uint64_t num = 0;
      FileType type;
      if (ParseFileName(file, &num, &type) && type == kDescriptorFile) {
        return file;
      }
    }
    return "";
  }
};

class DBErrorHandlingFS : public FileSystemWrapper {
 public:
  DBErrorHandlingFS()
      : FileSystemWrapper(FileSystem::Default()),
        trig_no_space(false),
        trig_io_error(false) {}

  void SetTrigNoSpace() { trig_no_space = true; }
  void SetTrigIoError() { trig_io_error = true; }

 private:
  bool trig_no_space;
  bool trig_io_error;
};

class ErrorHandlerFSListener : public EventListener {
 public:
  ErrorHandlerFSListener()
      : mutex_(),
        cv_(&mutex_),
        no_auto_recovery_(false),
        recovery_complete_(false),
        file_creation_started_(false),
        override_bg_error_(false),
        file_count_(0),
        fault_fs_(nullptr) {}

  void OnTableFileCreationStarted(
      const TableFileCreationBriefInfo& /*ti*/) override {
    InstrumentedMutexLock l(&mutex_);
    file_creation_started_ = true;
    if (file_count_ > 0) {
      if (--file_count_ == 0) {
        fault_fs_->SetFilesystemActive(false, file_creation_error_);
        file_creation_error_ = IOStatus::OK();
      }
    }
    cv_.SignalAll();
  }

  void OnErrorRecoveryBegin(BackgroundErrorReason /*reason*/,
                            Status /*bg_error*/, bool* auto_recovery) override {
    if (*auto_recovery && no_auto_recovery_) {
      *auto_recovery = false;
    }
  }

  void OnErrorRecoveryCompleted(Status /*old_bg_error*/) override {
    InstrumentedMutexLock l(&mutex_);
    recovery_complete_ = true;
    cv_.SignalAll();
  }

  bool WaitForRecovery(uint64_t /*abs_time_us*/) {
    InstrumentedMutexLock l(&mutex_);
    while (!recovery_complete_) {
      cv_.Wait(/*abs_time_us*/);
    }
    if (recovery_complete_) {
      recovery_complete_ = false;
      return true;
    }
    return false;
  }

  void WaitForTableFileCreationStarted(uint64_t /*abs_time_us*/) {
    InstrumentedMutexLock l(&mutex_);
    while (!file_creation_started_) {
      cv_.Wait(/*abs_time_us*/);
    }
    file_creation_started_ = false;
  }

  void OnBackgroundError(BackgroundErrorReason /*reason*/,
                         Status* bg_error) override {
    if (override_bg_error_) {
      *bg_error = bg_error_;
      override_bg_error_ = false;
    }
  }

  void EnableAutoRecovery(bool enable = true) { no_auto_recovery_ = !enable; }

  void OverrideBGError(Status bg_err) {
    bg_error_ = bg_err;
    override_bg_error_ = true;
  }

  void InjectFileCreationError(FaultInjectionTestFS* fs, int file_count,
                               IOStatus io_s) {
    fault_fs_ = fs;
    file_count_ = file_count;
    file_creation_error_ = io_s;
  }

 private:
  InstrumentedMutex mutex_;
  InstrumentedCondVar cv_;
  bool no_auto_recovery_;
  bool recovery_complete_;
  bool file_creation_started_;
  bool override_bg_error_;
  int file_count_;
  IOStatus file_creation_error_;
  Status bg_error_;
  FaultInjectionTestFS* fault_fs_;
};

TEST_F(DBErrorHandlingFSTest, FLushWriteError) {
  std::shared_ptr<FaultInjectionTestFS> fault_fs(
      new FaultInjectionTestFS(FileSystem::Default()));
  std::unique_ptr<Env> fault_fs_env(NewCompositeEnv(fault_fs));
  std::shared_ptr<ErrorHandlerFSListener> listener(
      new ErrorHandlerFSListener());
  Options options = GetDefaultOptions();
  options.env = fault_fs_env.get();
  options.create_if_missing = true;
  options.listeners.emplace_back(listener);
  Status s;

  listener->EnableAutoRecovery(false);
  DestroyAndReopen(options);

  Put(Key(0), "val");
  SyncPoint::GetInstance()->SetCallBack("FlushJob::Start", [&](void*) {
    fault_fs->SetFilesystemActive(false, IOStatus::NoSpace("Out of space"));
  });
  SyncPoint::GetInstance()->EnableProcessing();
  s = Flush();
  ASSERT_EQ(s.severity(), ROCKSDB_NAMESPACE::Status::Severity::kHardError);
  SyncPoint::GetInstance()->DisableProcessing();
  fault_fs->SetFilesystemActive(true);
  s = dbfull()->Resume();
  ASSERT_EQ(s, Status::OK());

  Reopen(options);
  ASSERT_EQ("val", Get(Key(0)));
  Destroy(options);
}

TEST_F(DBErrorHandlingFSTest, FLushWritRetryableeError) {
  std::shared_ptr<FaultInjectionTestFS> fault_fs(
      new FaultInjectionTestFS(FileSystem::Default()));
  std::unique_ptr<Env> fault_fs_env(NewCompositeEnv(fault_fs));
  std::shared_ptr<ErrorHandlerFSListener> listener(
      new ErrorHandlerFSListener());
  Options options = GetDefaultOptions();
  options.env = fault_fs_env.get();
  options.create_if_missing = true;
  options.listeners.emplace_back(listener);
  Status s;

  listener->EnableAutoRecovery(false);
  DestroyAndReopen(options);

  IOStatus error_msg = IOStatus::IOError("Retryable IO Error");
  error_msg.SetRetryable(true);

  Put(Key(1), "val1");
  SyncPoint::GetInstance()->SetCallBack(
      "BuildTable:BeforeFinishBuildTable",
      [&](void*) { fault_fs->SetFilesystemActive(false, error_msg); });
  SyncPoint::GetInstance()->EnableProcessing();
  s = Flush();
  ASSERT_EQ(s.severity(), ROCKSDB_NAMESPACE::Status::Severity::kHardError);
  SyncPoint::GetInstance()->DisableProcessing();
  fault_fs->SetFilesystemActive(true);
  s = dbfull()->Resume();
  ASSERT_EQ(s, Status::OK());
  Reopen(options);
  ASSERT_EQ("val1", Get(Key(1)));

  Put(Key(2), "val2");
  SyncPoint::GetInstance()->SetCallBack(
      "BuildTable:BeforeSyncTable",
      [&](void*) { fault_fs->SetFilesystemActive(false, error_msg); });
  SyncPoint::GetInstance()->EnableProcessing();
  s = Flush();
  ASSERT_EQ(s.severity(), ROCKSDB_NAMESPACE::Status::Severity::kHardError);
  SyncPoint::GetInstance()->DisableProcessing();
  fault_fs->SetFilesystemActive(true);
  s = dbfull()->Resume();
  ASSERT_EQ(s, Status::OK());
  Reopen(options);
  ASSERT_EQ("val2", Get(Key(2)));

  Put(Key(3), "val3");
  SyncPoint::GetInstance()->SetCallBack(
      "BuildTable:BeforeCloseTableFile",
      [&](void*) { fault_fs->SetFilesystemActive(false, error_msg); });
  SyncPoint::GetInstance()->EnableProcessing();
  s = Flush();
  ASSERT_EQ(s.severity(), ROCKSDB_NAMESPACE::Status::Severity::kHardError);
  SyncPoint::GetInstance()->DisableProcessing();
  fault_fs->SetFilesystemActive(true);
  s = dbfull()->Resume();
  ASSERT_EQ(s, Status::OK());
  Reopen(options);
  ASSERT_EQ("val3", Get(Key(3)));

  Destroy(options);
}

TEST_F(DBErrorHandlingFSTest, ManifestWriteError) {
  std::shared_ptr<FaultInjectionTestFS> fault_fs(
      new FaultInjectionTestFS(FileSystem::Default()));
  std::unique_ptr<Env> fault_fs_env(NewCompositeEnv(fault_fs));
  std::shared_ptr<ErrorHandlerFSListener> listener(
      new ErrorHandlerFSListener());
  Options options = GetDefaultOptions();
  options.env = fault_fs_env.get();
  options.create_if_missing = true;
  options.listeners.emplace_back(listener);
  Status s;
  std::string old_manifest;
  std::string new_manifest;

  listener->EnableAutoRecovery(false);
  DestroyAndReopen(options);
  old_manifest = GetManifestNameFromLiveFiles();

  Put(Key(0), "val");
  Flush();
  Put(Key(1), "val");
  SyncPoint::GetInstance()->SetCallBack(
      "VersionSet::LogAndApply:WriteManifest", [&](void*) {
        fault_fs->SetFilesystemActive(false, IOStatus::NoSpace("Out of space"));
      });
  SyncPoint::GetInstance()->EnableProcessing();
  s = Flush();
  ASSERT_EQ(s.severity(), ROCKSDB_NAMESPACE::Status::Severity::kHardError);
  SyncPoint::GetInstance()->ClearAllCallBacks();
  SyncPoint::GetInstance()->DisableProcessing();
  fault_fs->SetFilesystemActive(true);
  s = dbfull()->Resume();
  ASSERT_EQ(s, Status::OK());

  new_manifest = GetManifestNameFromLiveFiles();
  ASSERT_NE(new_manifest, old_manifest);

  Reopen(options);
  ASSERT_EQ("val", Get(Key(0)));
  ASSERT_EQ("val", Get(Key(1)));
  Close();
}

TEST_F(DBErrorHandlingFSTest, ManifestWriteRetryableError) {
  std::shared_ptr<FaultInjectionTestFS> fault_fs(
      new FaultInjectionTestFS(FileSystem::Default()));
  std::unique_ptr<Env> fault_fs_env(NewCompositeEnv(fault_fs));
  std::shared_ptr<ErrorHandlerFSListener> listener(
      new ErrorHandlerFSListener());
  Options options = GetDefaultOptions();
  options.env = fault_fs_env.get();
  options.create_if_missing = true;
  options.listeners.emplace_back(listener);
  Status s;
  std::string old_manifest;
  std::string new_manifest;

  listener->EnableAutoRecovery(false);
  DestroyAndReopen(options);
  old_manifest = GetManifestNameFromLiveFiles();

  IOStatus error_msg = IOStatus::IOError("Retryable IO Error");
  error_msg.SetRetryable(true);

  Put(Key(0), "val");
  Flush();
  Put(Key(1), "val");
  SyncPoint::GetInstance()->SetCallBack(
      "VersionSet::LogAndApply:WriteManifest",
      [&](void*) { fault_fs->SetFilesystemActive(false, error_msg); });
  SyncPoint::GetInstance()->EnableProcessing();
  s = Flush();
  ASSERT_EQ(s.severity(), ROCKSDB_NAMESPACE::Status::Severity::kHardError);
  SyncPoint::GetInstance()->ClearAllCallBacks();
  SyncPoint::GetInstance()->DisableProcessing();
  fault_fs->SetFilesystemActive(true);
  s = dbfull()->Resume();
  ASSERT_EQ(s, Status::OK());

  new_manifest = GetManifestNameFromLiveFiles();
  ASSERT_NE(new_manifest, old_manifest);

  Reopen(options);
  ASSERT_EQ("val", Get(Key(0)));
  ASSERT_EQ("val", Get(Key(1)));
  Close();
}

TEST_F(DBErrorHandlingFSTest, DoubleManifestWriteError) {
  std::shared_ptr<FaultInjectionTestFS> fault_fs(
      new FaultInjectionTestFS(FileSystem::Default()));
  std::unique_ptr<Env> fault_fs_env(NewCompositeEnv(fault_fs));
  std::shared_ptr<ErrorHandlerFSListener> listener(
      new ErrorHandlerFSListener());
  Options options = GetDefaultOptions();
  options.env = fault_fs_env.get();
  options.create_if_missing = true;
  options.listeners.emplace_back(listener);
  Status s;
  std::string old_manifest;
  std::string new_manifest;

  listener->EnableAutoRecovery(false);
  DestroyAndReopen(options);
  old_manifest = GetManifestNameFromLiveFiles();

  Put(Key(0), "val");
  Flush();
  Put(Key(1), "val");
  SyncPoint::GetInstance()->SetCallBack(
      "VersionSet::LogAndApply:WriteManifest", [&](void*) {
        fault_fs->SetFilesystemActive(false, IOStatus::NoSpace("Out of space"));
      });
  SyncPoint::GetInstance()->EnableProcessing();
  s = Flush();
  ASSERT_EQ(s.severity(), ROCKSDB_NAMESPACE::Status::Severity::kHardError);
  fault_fs->SetFilesystemActive(true);

  // This Resume() will attempt to create a new manifest file and fail again
  s = dbfull()->Resume();
  ASSERT_EQ(s.severity(), ROCKSDB_NAMESPACE::Status::Severity::kHardError);
  fault_fs->SetFilesystemActive(true);
  SyncPoint::GetInstance()->ClearAllCallBacks();
  SyncPoint::GetInstance()->DisableProcessing();

  // A successful Resume() will create a new manifest file
  s = dbfull()->Resume();
  ASSERT_EQ(s, Status::OK());

  new_manifest = GetManifestNameFromLiveFiles();
  ASSERT_NE(new_manifest, old_manifest);

  Reopen(options);
  ASSERT_EQ("val", Get(Key(0)));
  ASSERT_EQ("val", Get(Key(1)));
  Close();
}

TEST_F(DBErrorHandlingFSTest, CompactionManifestWriteError) {
  std::shared_ptr<FaultInjectionTestFS> fault_fs(
      new FaultInjectionTestFS(FileSystem::Default()));
  std::unique_ptr<Env> fault_fs_env(NewCompositeEnv(fault_fs));
  std::shared_ptr<ErrorHandlerFSListener> listener(
      new ErrorHandlerFSListener());
  Options options = GetDefaultOptions();
  options.env = fault_fs_env.get();
  options.create_if_missing = true;
  options.level0_file_num_compaction_trigger = 2;
  options.listeners.emplace_back(listener);
  Status s;
  std::string old_manifest;
  std::string new_manifest;
  std::atomic<bool> fail_manifest(false);
  DestroyAndReopen(options);
  old_manifest = GetManifestNameFromLiveFiles();

  Put(Key(0), "val");
  Put(Key(2), "val");
  s = Flush();
  ASSERT_EQ(s, Status::OK());

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency(
      // Wait for flush of 2nd L0 file before starting compaction
      {{"DBImpl::FlushMemTable:FlushMemTableFinished",
        "BackgroundCallCompaction:0"},
       // Wait for compaction to detect manifest write error
       {"BackgroundCallCompaction:1", "CompactionManifestWriteError:0"},
       // Make compaction thread wait for error to be cleared
       {"CompactionManifestWriteError:1",
        "DBImpl::BackgroundCallCompaction:FoundObsoleteFiles"},
       // Wait for DB instance to clear bg_error before calling
       // TEST_WaitForCompact
       {"SstFileManagerImpl::ErrorCleared", "CompactionManifestWriteError:2"}});
  // trigger manifest write failure in compaction thread
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "BackgroundCallCompaction:0", [&](void*) { fail_manifest.store(true); });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "VersionSet::LogAndApply:WriteManifest", [&](void*) {
        if (fail_manifest.load()) {
          fault_fs->SetFilesystemActive(false,
                                        IOStatus::NoSpace("Out of space"));
        }
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  Put(Key(1), "val");
  // This Flush will trigger a compaction, which will fail when appending to
  // the manifest
  s = Flush();
  ASSERT_EQ(s, Status::OK());

  TEST_SYNC_POINT("CompactionManifestWriteError:0");
  // Clear all errors so when the compaction is retried, it will succeed
  fault_fs->SetFilesystemActive(true);
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearAllCallBacks();
  TEST_SYNC_POINT("CompactionManifestWriteError:1");
  TEST_SYNC_POINT("CompactionManifestWriteError:2");

  s = dbfull()->TEST_WaitForCompact();
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
  ASSERT_EQ(s, Status::OK());

  new_manifest = GetManifestNameFromLiveFiles();
  ASSERT_NE(new_manifest, old_manifest);
  Reopen(options);
  ASSERT_EQ("val", Get(Key(0)));
  ASSERT_EQ("val", Get(Key(1)));
  ASSERT_EQ("val", Get(Key(2)));
  Close();
}

TEST_F(DBErrorHandlingFSTest, CompactionManifestWriteRetryableError) {
  std::shared_ptr<FaultInjectionTestFS> fault_fs(
      new FaultInjectionTestFS(FileSystem::Default()));
  std::unique_ptr<Env> fault_fs_env(NewCompositeEnv(fault_fs));
  std::shared_ptr<ErrorHandlerFSListener> listener(
      new ErrorHandlerFSListener());
  Options options = GetDefaultOptions();
  options.env = fault_fs_env.get();
  options.create_if_missing = true;
  options.level0_file_num_compaction_trigger = 2;
  options.listeners.emplace_back(listener);
  Status s;
  std::string old_manifest;
  std::string new_manifest;
  std::atomic<bool> fail_manifest(false);
  DestroyAndReopen(options);
  old_manifest = GetManifestNameFromLiveFiles();

  IOStatus error_msg = IOStatus::IOError("Retryable IO Error");
  error_msg.SetRetryable(true);

  Put(Key(0), "val");
  Put(Key(2), "val");
  s = Flush();
  ASSERT_EQ(s, Status::OK());

  listener->OverrideBGError(Status(error_msg, Status::Severity::kHardError));
  listener->EnableAutoRecovery(false);
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency(
      // Wait for flush of 2nd L0 file before starting compaction
      {{"DBImpl::FlushMemTable:FlushMemTableFinished",
        "BackgroundCallCompaction:0"},
       // Wait for compaction to detect manifest write error
       {"BackgroundCallCompaction:1", "CompactionManifestWriteError:0"},
       // Make compaction thread wait for error to be cleared
       {"CompactionManifestWriteError:1",
        "DBImpl::BackgroundCallCompaction:FoundObsoleteFiles"}});
  // trigger manifest write failure in compaction thread
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "BackgroundCallCompaction:0", [&](void*) { fail_manifest.store(true); });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "VersionSet::LogAndApply:WriteManifest", [&](void*) {
        if (fail_manifest.load()) {
          fault_fs->SetFilesystemActive(false, error_msg);
        }
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  Put(Key(1), "val");
  s = Flush();
  ASSERT_EQ(s, Status::OK());

  TEST_SYNC_POINT("CompactionManifestWriteError:0");
  TEST_SYNC_POINT("CompactionManifestWriteError:1");

  s = dbfull()->TEST_WaitForCompact();
  ASSERT_EQ(s.severity(), ROCKSDB_NAMESPACE::Status::Severity::kHardError);

  fault_fs->SetFilesystemActive(true);
  SyncPoint::GetInstance()->ClearAllCallBacks();
  SyncPoint::GetInstance()->DisableProcessing();
  s = dbfull()->Resume();
  ASSERT_EQ(s, Status::OK());

  new_manifest = GetManifestNameFromLiveFiles();
  ASSERT_NE(new_manifest, old_manifest);

  Reopen(options);
  ASSERT_EQ("val", Get(Key(0)));
  ASSERT_EQ("val", Get(Key(1)));
  ASSERT_EQ("val", Get(Key(2)));
  Close();
}

TEST_F(DBErrorHandlingFSTest, CompactionWriteError) {
  std::shared_ptr<FaultInjectionTestFS> fault_fs(
      new FaultInjectionTestFS(FileSystem::Default()));
  std::unique_ptr<Env> fault_fs_env(NewCompositeEnv(fault_fs));
  std::shared_ptr<ErrorHandlerFSListener> listener(
      new ErrorHandlerFSListener());
  Options options = GetDefaultOptions();
  options.env = fault_fs_env.get();
  options.create_if_missing = true;
  options.level0_file_num_compaction_trigger = 2;
  options.listeners.emplace_back(listener);
  Status s;
  DestroyAndReopen(options);

  Put(Key(0), "va;");
  Put(Key(2), "va;");
  s = Flush();
  ASSERT_EQ(s, Status::OK());

  listener->OverrideBGError(
      Status(Status::NoSpace(), Status::Severity::kHardError));
  listener->EnableAutoRecovery(false);
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency(
      {{"DBImpl::FlushMemTable:FlushMemTableFinished",
        "BackgroundCallCompaction:0"}});
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "BackgroundCallCompaction:0", [&](void*) {
        fault_fs->SetFilesystemActive(false, IOStatus::NoSpace("Out of space"));
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  Put(Key(1), "val");
  s = Flush();
  ASSERT_EQ(s, Status::OK());

  s = dbfull()->TEST_WaitForCompact();
  ASSERT_EQ(s.severity(), ROCKSDB_NAMESPACE::Status::Severity::kHardError);

  fault_fs->SetFilesystemActive(true);
  s = dbfull()->Resume();
  ASSERT_EQ(s, Status::OK());
  Destroy(options);
}

TEST_F(DBErrorHandlingFSTest, CompactionWriteRetryableError) {
  std::shared_ptr<FaultInjectionTestFS> fault_fs(
      new FaultInjectionTestFS(FileSystem::Default()));
  std::unique_ptr<Env> fault_fs_env(NewCompositeEnv(fault_fs));
  std::shared_ptr<ErrorHandlerFSListener> listener(
      new ErrorHandlerFSListener());
  Options options = GetDefaultOptions();
  options.env = fault_fs_env.get();
  options.create_if_missing = true;
  options.level0_file_num_compaction_trigger = 2;
  options.listeners.emplace_back(listener);
  Status s;
  DestroyAndReopen(options);

  IOStatus error_msg = IOStatus::IOError("Retryable IO Error");
  error_msg.SetRetryable(true);

  Put(Key(0), "va;");
  Put(Key(2), "va;");
  s = Flush();
  ASSERT_EQ(s, Status::OK());

  listener->OverrideBGError(Status(error_msg, Status::Severity::kHardError));
  listener->EnableAutoRecovery(false);
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency(
      {{"DBImpl::FlushMemTable:FlushMemTableFinished",
        "BackgroundCallCompaction:0"}});
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "BackgroundCallCompaction:0",
      [&](void*) { fault_fs->SetFilesystemActive(false, error_msg); });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  Put(Key(1), "val");
  s = Flush();
  ASSERT_EQ(s, Status::OK());

  s = dbfull()->TEST_WaitForCompact();
  ASSERT_EQ(s.severity(), ROCKSDB_NAMESPACE::Status::Severity::kHardError);

  fault_fs->SetFilesystemActive(true);
  SyncPoint::GetInstance()->ClearAllCallBacks();
  SyncPoint::GetInstance()->DisableProcessing();
  s = dbfull()->Resume();
  ASSERT_EQ(s, Status::OK());
  Destroy(options);
}

TEST_F(DBErrorHandlingFSTest, CorruptionError) {
  std::shared_ptr<FaultInjectionTestFS> fault_fs(
      new FaultInjectionTestFS(FileSystem::Default()));
  std::unique_ptr<Env> fault_fs_env(NewCompositeEnv(fault_fs));
  Options options = GetDefaultOptions();
  options.env = fault_fs_env.get();
  options.create_if_missing = true;
  options.level0_file_num_compaction_trigger = 2;
  Status s;
  DestroyAndReopen(options);

  Put(Key(0), "va;");
  Put(Key(2), "va;");
  s = Flush();
  ASSERT_EQ(s, Status::OK());

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency(
      {{"DBImpl::FlushMemTable:FlushMemTableFinished",
        "BackgroundCallCompaction:0"}});
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "BackgroundCallCompaction:0", [&](void*) {
        fault_fs->SetFilesystemActive(false,
                                      IOStatus::Corruption("Corruption"));
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  Put(Key(1), "val");
  s = Flush();
  ASSERT_EQ(s, Status::OK());

  s = dbfull()->TEST_WaitForCompact();
  ASSERT_EQ(s.severity(),
            ROCKSDB_NAMESPACE::Status::Severity::kUnrecoverableError);

  fault_fs->SetFilesystemActive(true);
  s = dbfull()->Resume();
  ASSERT_NE(s, Status::OK());
  Destroy(options);
}

TEST_F(DBErrorHandlingFSTest, AutoRecoverFlushError) {
  std::shared_ptr<FaultInjectionTestFS> fault_fs(
      new FaultInjectionTestFS(FileSystem::Default()));
  std::unique_ptr<Env> fault_fs_env(NewCompositeEnv(fault_fs));
  std::shared_ptr<ErrorHandlerFSListener> listener(
      new ErrorHandlerFSListener());
  Options options = GetDefaultOptions();
  options.env = fault_fs_env.get();
  options.create_if_missing = true;
  options.listeners.emplace_back(listener);
  Status s;

  listener->EnableAutoRecovery();
  DestroyAndReopen(options);

  Put(Key(0), "val");
  SyncPoint::GetInstance()->SetCallBack("FlushJob::Start", [&](void*) {
    fault_fs->SetFilesystemActive(false, IOStatus::NoSpace("Out of space"));
  });
  SyncPoint::GetInstance()->EnableProcessing();
  s = Flush();
  ASSERT_EQ(s.severity(), ROCKSDB_NAMESPACE::Status::Severity::kHardError);
  SyncPoint::GetInstance()->DisableProcessing();
  fault_fs->SetFilesystemActive(true);
  ASSERT_EQ(listener->WaitForRecovery(5000000), true);

  s = Put(Key(1), "val");
  ASSERT_EQ(s, Status::OK());

  Reopen(options);
  ASSERT_EQ("val", Get(Key(0)));
  ASSERT_EQ("val", Get(Key(1)));
  Destroy(options);
}

TEST_F(DBErrorHandlingFSTest, FailRecoverFlushError) {
  std::shared_ptr<FaultInjectionTestFS> fault_fs(
      new FaultInjectionTestFS(FileSystem::Default()));
  std::unique_ptr<Env> fault_fs_env(NewCompositeEnv(fault_fs));
  std::shared_ptr<ErrorHandlerFSListener> listener(
      new ErrorHandlerFSListener());
  Options options = GetDefaultOptions();
  options.env = fault_fs_env.get();
  options.create_if_missing = true;
  options.listeners.emplace_back(listener);
  Status s;

  listener->EnableAutoRecovery();
  DestroyAndReopen(options);

  Put(Key(0), "val");
  SyncPoint::GetInstance()->SetCallBack("FlushJob::Start", [&](void*) {
    fault_fs->SetFilesystemActive(false, IOStatus::NoSpace("Out of space"));
  });
  SyncPoint::GetInstance()->EnableProcessing();
  s = Flush();
  ASSERT_EQ(s.severity(), ROCKSDB_NAMESPACE::Status::Severity::kHardError);
  // We should be able to shutdown the database while auto recovery is going
  // on in the background
  Close();
  DestroyDB(dbname_, options);
}

TEST_F(DBErrorHandlingFSTest, WALWriteError) {
  std::shared_ptr<FaultInjectionTestFS> fault_fs(
      new FaultInjectionTestFS(FileSystem::Default()));
  std::unique_ptr<Env> fault_fs_env(NewCompositeEnv(fault_fs));
  std::shared_ptr<ErrorHandlerFSListener> listener(
      new ErrorHandlerFSListener());
  Options options = GetDefaultOptions();
  options.env = fault_fs_env.get();
  options.create_if_missing = true;
  options.writable_file_max_buffer_size = 32768;
  options.listeners.emplace_back(listener);
  Status s;
  Random rnd(301);

  listener->EnableAutoRecovery();
  DestroyAndReopen(options);

  {
    WriteBatch batch;

    for (auto i = 0; i < 100; ++i) {
      batch.Put(Key(i), RandomString(&rnd, 1024));
    }

    WriteOptions wopts;
    wopts.sync = true;
    ASSERT_EQ(dbfull()->Write(wopts, &batch), Status::OK());
  };

  {
    WriteBatch batch;
    int write_error = 0;

    for (auto i = 100; i < 199; ++i) {
      batch.Put(Key(i), RandomString(&rnd, 1024));
    }

    SyncPoint::GetInstance()->SetCallBack(
        "WritableFileWriter::Append:BeforePrepareWrite", [&](void*) {
          write_error++;
          if (write_error > 2) {
            fault_fs->SetFilesystemActive(false,
                                          IOStatus::NoSpace("Out of space"));
          }
        });
    SyncPoint::GetInstance()->EnableProcessing();
    WriteOptions wopts;
    wopts.sync = true;
    s = dbfull()->Write(wopts, &batch);
    ASSERT_EQ(s, s.NoSpace());
  }
  SyncPoint::GetInstance()->DisableProcessing();
  fault_fs->SetFilesystemActive(true);
  ASSERT_EQ(listener->WaitForRecovery(5000000), true);
  for (auto i = 0; i < 199; ++i) {
    if (i < 100) {
      ASSERT_NE(Get(Key(i)), "NOT_FOUND");
    } else {
      ASSERT_EQ(Get(Key(i)), "NOT_FOUND");
    }
  }
  Reopen(options);
  for (auto i = 0; i < 199; ++i) {
    if (i < 100) {
      ASSERT_NE(Get(Key(i)), "NOT_FOUND");
    } else {
      ASSERT_EQ(Get(Key(i)), "NOT_FOUND");
    }
  }
  Close();
}

TEST_F(DBErrorHandlingFSTest, WALWriteRetryableError) {
  std::shared_ptr<FaultInjectionTestFS> fault_fs(
      new FaultInjectionTestFS(FileSystem::Default()));
  std::unique_ptr<Env> fault_fs_env(NewCompositeEnv(fault_fs));
  std::shared_ptr<ErrorHandlerFSListener> listener(
      new ErrorHandlerFSListener());
  Options options = GetDefaultOptions();
  options.env = fault_fs_env.get();
  options.create_if_missing = true;
  options.writable_file_max_buffer_size = 32768;
  options.listeners.emplace_back(listener);
  options.paranoid_checks = true;
  Status s;
  Random rnd(301);

  DestroyAndReopen(options);

  IOStatus error_msg = IOStatus::IOError("Retryable IO Error");
  error_msg.SetRetryable(true);

  // For the first batch, write is successful, require sync
  {
    WriteBatch batch;

    for (auto i = 0; i < 100; ++i) {
      batch.Put(Key(i), RandomString(&rnd, 1024));
    }

    WriteOptions wopts;
    wopts.sync = true;
    ASSERT_EQ(dbfull()->Write(wopts, &batch), Status::OK());
  };

  // For the second batch, the first 2 file Append are successful, then the
  // following Append fails due to file system retryable IOError.
  {
    WriteBatch batch;
    int write_error = 0;

    for (auto i = 100; i < 200; ++i) {
      batch.Put(Key(i), RandomString(&rnd, 1024));
    }

    SyncPoint::GetInstance()->SetCallBack(
        "WritableFileWriter::Append:BeforePrepareWrite", [&](void*) {
          write_error++;
          if (write_error > 2) {
            fault_fs->SetFilesystemActive(false, error_msg);
          }
        });
    SyncPoint::GetInstance()->EnableProcessing();
    WriteOptions wopts;
    wopts.sync = true;
    s = dbfull()->Write(wopts, &batch);
    ASSERT_EQ(true, s.IsIOError());
  }
  fault_fs->SetFilesystemActive(true);
  SyncPoint::GetInstance()->ClearAllCallBacks();
  SyncPoint::GetInstance()->DisableProcessing();

  // Data in corrupted WAL are not stored
  for (auto i = 0; i < 199; ++i) {
    if (i < 100) {
      ASSERT_NE(Get(Key(i)), "NOT_FOUND");
    } else {
      ASSERT_EQ(Get(Key(i)), "NOT_FOUND");
    }
  }

  // Resume and write a new batch, should be in the WAL
  s = dbfull()->Resume();
  ASSERT_EQ(s, Status::OK());
  {
    WriteBatch batch;

    for (auto i = 200; i < 300; ++i) {
      batch.Put(Key(i), RandomString(&rnd, 1024));
    }

    WriteOptions wopts;
    wopts.sync = true;
    ASSERT_EQ(dbfull()->Write(wopts, &batch), Status::OK());
  };

  Reopen(options);
  for (auto i = 0; i < 300; ++i) {
    if (i < 100 || i >= 200) {
      ASSERT_NE(Get(Key(i)), "NOT_FOUND");
    } else {
      ASSERT_EQ(Get(Key(i)), "NOT_FOUND");
    }
  }
  Close();
}

TEST_F(DBErrorHandlingFSTest, MultiCFWALWriteError) {
  std::shared_ptr<FaultInjectionTestFS> fault_fs(
      new FaultInjectionTestFS(FileSystem::Default()));
  std::unique_ptr<Env> fault_fs_env(NewCompositeEnv(fault_fs));
  std::shared_ptr<ErrorHandlerFSListener> listener(
      new ErrorHandlerFSListener());
  Options options = GetDefaultOptions();
  options.env = fault_fs_env.get();
  options.create_if_missing = true;
  options.writable_file_max_buffer_size = 32768;
  options.listeners.emplace_back(listener);
  Status s;
  Random rnd(301);

  listener->EnableAutoRecovery();
  CreateAndReopenWithCF({"one", "two", "three"}, options);

  {
    WriteBatch batch;

    for (auto i = 1; i < 4; ++i) {
      for (auto j = 0; j < 100; ++j) {
        batch.Put(handles_[i], Key(j), RandomString(&rnd, 1024));
      }
    }

    WriteOptions wopts;
    wopts.sync = true;
    ASSERT_EQ(dbfull()->Write(wopts, &batch), Status::OK());
  };

  {
    WriteBatch batch;
    int write_error = 0;

    // Write to one CF
    for (auto i = 100; i < 199; ++i) {
      batch.Put(handles_[2], Key(i), RandomString(&rnd, 1024));
    }

    SyncPoint::GetInstance()->SetCallBack(
        "WritableFileWriter::Append:BeforePrepareWrite", [&](void*) {
          write_error++;
          if (write_error > 2) {
            fault_fs->SetFilesystemActive(false,
                                          IOStatus::NoSpace("Out of space"));
          }
        });
    SyncPoint::GetInstance()->EnableProcessing();
    WriteOptions wopts;
    wopts.sync = true;
    s = dbfull()->Write(wopts, &batch);
    ASSERT_EQ(s, s.NoSpace());
  }
  SyncPoint::GetInstance()->DisableProcessing();
  fault_fs->SetFilesystemActive(true);
  ASSERT_EQ(listener->WaitForRecovery(5000000), true);

  for (auto i = 1; i < 4; ++i) {
    // Every CF should have been flushed
    ASSERT_EQ(NumTableFilesAtLevel(0, i), 1);
  }

  for (auto i = 1; i < 4; ++i) {
    for (auto j = 0; j < 199; ++j) {
      if (j < 100) {
        ASSERT_NE(Get(i, Key(j)), "NOT_FOUND");
      } else {
        ASSERT_EQ(Get(i, Key(j)), "NOT_FOUND");
      }
    }
  }
  ReopenWithColumnFamilies({"default", "one", "two", "three"}, options);
  for (auto i = 1; i < 4; ++i) {
    for (auto j = 0; j < 199; ++j) {
      if (j < 100) {
        ASSERT_NE(Get(i, Key(j)), "NOT_FOUND");
      } else {
        ASSERT_EQ(Get(i, Key(j)), "NOT_FOUND");
      }
    }
  }
  Close();
}

TEST_F(DBErrorHandlingFSTest, MultiDBCompactionError) {
  FaultInjectionTestEnv* def_env = new FaultInjectionTestEnv(Env::Default());
  std::vector<std::unique_ptr<Env>> fault_envs;
  std::vector<FaultInjectionTestFS*> fault_fs;
  std::vector<Options> options;
  std::vector<std::shared_ptr<ErrorHandlerFSListener>> listener;
  std::vector<DB*> db;
  std::shared_ptr<SstFileManager> sfm(NewSstFileManager(def_env));
  int kNumDbInstances = 3;
  Random rnd(301);

  for (auto i = 0; i < kNumDbInstances; ++i) {
    listener.emplace_back(new ErrorHandlerFSListener());
    options.emplace_back(GetDefaultOptions());
    fault_fs.emplace_back(new FaultInjectionTestFS(FileSystem::Default()));
    std::shared_ptr<FileSystem> fs(fault_fs.back());
    fault_envs.emplace_back(new CompositeEnvWrapper(def_env, fs));
    options[i].env = fault_envs.back().get();
    options[i].create_if_missing = true;
    options[i].level0_file_num_compaction_trigger = 2;
    options[i].writable_file_max_buffer_size = 32768;
    options[i].listeners.emplace_back(listener[i]);
    options[i].sst_file_manager = sfm;
    DB* dbptr;
    char buf[16];

    listener[i]->EnableAutoRecovery();
    // Setup for returning error for the 3rd SST, which would be level 1
    listener[i]->InjectFileCreationError(fault_fs[i], 3,
                                         IOStatus::NoSpace("Out of space"));
    snprintf(buf, sizeof(buf), "_%d", i);
    DestroyDB(dbname_ + std::string(buf), options[i]);
    ASSERT_EQ(DB::Open(options[i], dbname_ + std::string(buf), &dbptr),
              Status::OK());
    db.emplace_back(dbptr);
  }

  for (auto i = 0; i < kNumDbInstances; ++i) {
    WriteBatch batch;

    for (auto j = 0; j <= 100; ++j) {
      batch.Put(Key(j), RandomString(&rnd, 1024));
    }

    WriteOptions wopts;
    wopts.sync = true;
    ASSERT_EQ(db[i]->Write(wopts, &batch), Status::OK());
    ASSERT_EQ(db[i]->Flush(FlushOptions()), Status::OK());
  }

  def_env->SetFilesystemActive(false, Status::NoSpace("Out of space"));
  for (auto i = 0; i < kNumDbInstances; ++i) {
    WriteBatch batch;

    // Write to one CF
    for (auto j = 100; j < 199; ++j) {
      batch.Put(Key(j), RandomString(&rnd, 1024));
    }

    WriteOptions wopts;
    wopts.sync = true;
    ASSERT_EQ(db[i]->Write(wopts, &batch), Status::OK());
    ASSERT_EQ(db[i]->Flush(FlushOptions()), Status::OK());
  }

  for (auto i = 0; i < kNumDbInstances; ++i) {
    Status s = static_cast<DBImpl*>(db[i])->TEST_WaitForCompact(true);
    ASSERT_EQ(s.severity(), Status::Severity::kSoftError);
    fault_fs[i]->SetFilesystemActive(true);
  }

  def_env->SetFilesystemActive(true);
  for (auto i = 0; i < kNumDbInstances; ++i) {
    std::string prop;
    ASSERT_EQ(listener[i]->WaitForRecovery(5000000), true);
    ASSERT_EQ(static_cast<DBImpl*>(db[i])->TEST_WaitForCompact(true),
              Status::OK());
    EXPECT_TRUE(db[i]->GetProperty(
        "rocksdb.num-files-at-level" + NumberToString(0), &prop));
    EXPECT_EQ(atoi(prop.c_str()), 0);
    EXPECT_TRUE(db[i]->GetProperty(
        "rocksdb.num-files-at-level" + NumberToString(1), &prop));
    EXPECT_EQ(atoi(prop.c_str()), 1);
  }

  for (auto i = 0; i < kNumDbInstances; ++i) {
    char buf[16];
    snprintf(buf, sizeof(buf), "_%d", i);
    delete db[i];
    fault_fs[i]->SetFilesystemActive(true);
    if (getenv("KEEP_DB")) {
      printf("DB is still at %s%s\n", dbname_.c_str(), buf);
    } else {
      Status s = DestroyDB(dbname_ + std::string(buf), options[i]);
    }
  }
  options.clear();
  sfm.reset();
  delete def_env;
}

TEST_F(DBErrorHandlingFSTest, MultiDBVariousErrors) {
  FaultInjectionTestEnv* def_env = new FaultInjectionTestEnv(Env::Default());
  std::vector<std::unique_ptr<Env>> fault_envs;
  std::vector<FaultInjectionTestFS*> fault_fs;
  std::vector<Options> options;
  std::vector<std::shared_ptr<ErrorHandlerFSListener>> listener;
  std::vector<DB*> db;
  std::shared_ptr<SstFileManager> sfm(NewSstFileManager(def_env));
  int kNumDbInstances = 3;
  Random rnd(301);

  for (auto i = 0; i < kNumDbInstances; ++i) {
    listener.emplace_back(new ErrorHandlerFSListener());
    options.emplace_back(GetDefaultOptions());
    fault_fs.emplace_back(new FaultInjectionTestFS(FileSystem::Default()));
    std::shared_ptr<FileSystem> fs(fault_fs.back());
    fault_envs.emplace_back(new CompositeEnvWrapper(def_env, fs));
    options[i].env = fault_envs.back().get();
    options[i].create_if_missing = true;
    options[i].level0_file_num_compaction_trigger = 2;
    options[i].writable_file_max_buffer_size = 32768;
    options[i].listeners.emplace_back(listener[i]);
    options[i].sst_file_manager = sfm;
    DB* dbptr;
    char buf[16];

    listener[i]->EnableAutoRecovery();
    switch (i) {
      case 0:
        // Setup for returning error for the 3rd SST, which would be level 1
        listener[i]->InjectFileCreationError(fault_fs[i], 3,
                                             IOStatus::NoSpace("Out of space"));
        break;
      case 1:
        // Setup for returning error after the 1st SST, which would result
        // in a hard error
        listener[i]->InjectFileCreationError(fault_fs[i], 2,
                                             IOStatus::NoSpace("Out of space"));
        break;
      default:
        break;
    }
    snprintf(buf, sizeof(buf), "_%d", i);
    DestroyDB(dbname_ + std::string(buf), options[i]);
    ASSERT_EQ(DB::Open(options[i], dbname_ + std::string(buf), &dbptr),
              Status::OK());
    db.emplace_back(dbptr);
  }

  for (auto i = 0; i < kNumDbInstances; ++i) {
    WriteBatch batch;

    for (auto j = 0; j <= 100; ++j) {
      batch.Put(Key(j), RandomString(&rnd, 1024));
    }

    WriteOptions wopts;
    wopts.sync = true;
    ASSERT_EQ(db[i]->Write(wopts, &batch), Status::OK());
    ASSERT_EQ(db[i]->Flush(FlushOptions()), Status::OK());
  }

  def_env->SetFilesystemActive(false, Status::NoSpace("Out of space"));
  for (auto i = 0; i < kNumDbInstances; ++i) {
    WriteBatch batch;

    // Write to one CF
    for (auto j = 100; j < 199; ++j) {
      batch.Put(Key(j), RandomString(&rnd, 1024));
    }

    WriteOptions wopts;
    wopts.sync = true;
    ASSERT_EQ(db[i]->Write(wopts, &batch), Status::OK());
    if (i != 1) {
      ASSERT_EQ(db[i]->Flush(FlushOptions()), Status::OK());
    } else {
      ASSERT_EQ(db[i]->Flush(FlushOptions()), Status::NoSpace());
    }
  }

  for (auto i = 0; i < kNumDbInstances; ++i) {
    Status s = static_cast<DBImpl*>(db[i])->TEST_WaitForCompact(true);
    switch (i) {
      case 0:
        ASSERT_EQ(s.severity(), Status::Severity::kSoftError);
        break;
      case 1:
        ASSERT_EQ(s.severity(), Status::Severity::kHardError);
        break;
      case 2:
        ASSERT_EQ(s, Status::OK());
        break;
    }
    fault_fs[i]->SetFilesystemActive(true);
  }

  def_env->SetFilesystemActive(true);
  for (auto i = 0; i < kNumDbInstances; ++i) {
    std::string prop;
    if (i < 2) {
      ASSERT_EQ(listener[i]->WaitForRecovery(5000000), true);
    }
    if (i == 1) {
      ASSERT_EQ(static_cast<DBImpl*>(db[i])->TEST_WaitForCompact(true),
                Status::OK());
    }
    EXPECT_TRUE(db[i]->GetProperty(
        "rocksdb.num-files-at-level" + NumberToString(0), &prop));
    EXPECT_EQ(atoi(prop.c_str()), 0);
    EXPECT_TRUE(db[i]->GetProperty(
        "rocksdb.num-files-at-level" + NumberToString(1), &prop));
    EXPECT_EQ(atoi(prop.c_str()), 1);
  }

  for (auto i = 0; i < kNumDbInstances; ++i) {
    char buf[16];
    snprintf(buf, sizeof(buf), "_%d", i);
    fault_fs[i]->SetFilesystemActive(true);
    delete db[i];
    if (getenv("KEEP_DB")) {
      printf("DB is still at %s%s\n", dbname_.c_str(), buf);
    } else {
      DestroyDB(dbname_ + std::string(buf), options[i]);
    }
  }
  options.clear();
  delete def_env;
}
}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

#else
#include <stdio.h>

int main(int /*argc*/, char** /*argv*/) {
  fprintf(stderr, "SKIPPED as Cuckoo table is not supported in ROCKSDB_LITE\n");
  return 0;
}

#endif  // ROCKSDB_LITE

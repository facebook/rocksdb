//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#ifdef GFLAGS
#pragma once

#include <mutex>
#include <unordered_set>

#include "db_stress_tool/db_stress_shared_state.h"
#include "file/filename.h"
#include "file/writable_file_writer.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/file_system.h"
#include "rocksdb/listener.h"
#include "rocksdb/table_properties.h"
#include "rocksdb/unique_id.h"
#include "util/gflags_compat.h"
#include "util/random.h"
#include "utilities/fault_injection_fs.h"

DECLARE_int32(compact_files_one_in);

extern std::shared_ptr<ROCKSDB_NAMESPACE::FaultInjectionTestFS> fault_fs_guard;

namespace ROCKSDB_NAMESPACE {

// Verify across process executions that all seen IDs are unique
class UniqueIdVerifier {
 public:
  explicit UniqueIdVerifier(const std::string& db_name, Env* env);
  ~UniqueIdVerifier();

  void Verify(const std::string& id);

 private:
  void VerifyNoWrite(const std::string& id);

 private:
  std::mutex mutex_;
  // IDs persisted to a hidden file inside DB dir
  std::string path_;
  std::unique_ptr<WritableFileWriter> data_file_writer_;
  // Starting byte for which 8 bytes to check in memory within 24 byte ID
  size_t offset_;
  // Working copy of the set of 8 byte pieces
  std::unordered_set<uint64_t> id_set_;
};

class DbStressListener : public EventListener {
 public:
  DbStressListener(const std::string& db_name,
                   const std::vector<DbPath>& db_paths,
                   const std::vector<ColumnFamilyDescriptor>& column_families,
                   Env* env)
      : db_name_(db_name),
        db_paths_(db_paths),
        column_families_(column_families),
        num_pending_file_creations_(0),
        unique_ids_(db_name, env) {}

  const char* Name() const override { return kClassName(); }
  static const char* kClassName() { return "DBStressListener"; }

  ~DbStressListener() override { assert(num_pending_file_creations_ == 0); }
  void OnFlushCompleted(DB* /*db*/, const FlushJobInfo& info) override {
    assert(IsValidColumnFamilyName(info.cf_name));
    VerifyFilePath(info.file_path);
    // pretending doing some work here
    RandomSleep();
    if (fault_fs_guard) {
      fault_fs_guard->DisableAllThreadLocalErrorInjection();
    }
  }

  void OnFlushBegin(DB* /*db*/,
                    const FlushJobInfo& /*flush_job_info*/) override {
    RandomSleep();
    if (fault_fs_guard) {
      fault_fs_guard->SetThreadLocalErrorContext(
          FaultInjectionIOType::kRead, static_cast<uint32_t>(FLAGS_seed),
          FLAGS_read_fault_one_in,
          FLAGS_inject_error_severity == 1 /* retryable */,
          FLAGS_inject_error_severity == 2 /* has_data_loss*/);
      fault_fs_guard->EnableThreadLocalErrorInjection(
          FaultInjectionIOType::kRead);

      fault_fs_guard->SetThreadLocalErrorContext(
          FaultInjectionIOType::kWrite, static_cast<uint32_t>(FLAGS_seed),
          FLAGS_write_fault_one_in,
          FLAGS_inject_error_severity == 1 /* retryable */,
          FLAGS_inject_error_severity == 2 /* has_data_loss*/);
      fault_fs_guard->EnableThreadLocalErrorInjection(
          FaultInjectionIOType::kWrite);

      fault_fs_guard->SetThreadLocalErrorContext(
          FaultInjectionIOType::kMetadataRead,
          static_cast<uint32_t>(FLAGS_seed), FLAGS_metadata_read_fault_one_in,
          FLAGS_inject_error_severity == 1 /* retryable */,
          FLAGS_inject_error_severity == 2 /* has_data_loss*/);
      fault_fs_guard->EnableThreadLocalErrorInjection(
          FaultInjectionIOType::kMetadataRead);

      fault_fs_guard->SetThreadLocalErrorContext(
          FaultInjectionIOType::kMetadataWrite,
          static_cast<uint32_t>(FLAGS_seed), FLAGS_metadata_write_fault_one_in,
          FLAGS_inject_error_severity == 1 /* retryable */,
          FLAGS_inject_error_severity == 2 /* has_data_loss*/);
      fault_fs_guard->EnableThreadLocalErrorInjection(
          FaultInjectionIOType::kMetadataWrite);
    }
  }

  void OnTableFileDeleted(const TableFileDeletionInfo& /*info*/) override {
    RandomSleep();
  }

  void OnCompactionBegin(DB* /*db*/, const CompactionJobInfo& /*ci*/) override {
    RandomSleep();
  }

  void OnCompactionCompleted(DB* /*db*/, const CompactionJobInfo& ci) override {
    assert(IsValidColumnFamilyName(ci.cf_name));
    assert(ci.input_files.size() + ci.output_files.size() > 0U);
    for (const auto& file_path : ci.input_files) {
      VerifyFilePath(file_path);
    }
    for (const auto& file_path : ci.output_files) {
      VerifyFilePath(file_path);
    }
    // pretending doing some work here
    RandomSleep();
  }

  void OnSubcompactionBegin(const SubcompactionJobInfo& /* si */) override {
    if (fault_fs_guard) {
      fault_fs_guard->SetThreadLocalErrorContext(
          FaultInjectionIOType::kRead, static_cast<uint32_t>(FLAGS_seed),
          FLAGS_read_fault_one_in,
          FLAGS_inject_error_severity == 1 /* retryable */,
          FLAGS_inject_error_severity == 2 /* has_data_loss*/);
      fault_fs_guard->EnableThreadLocalErrorInjection(
          FaultInjectionIOType::kRead);

      fault_fs_guard->SetThreadLocalErrorContext(
          FaultInjectionIOType::kWrite, static_cast<uint32_t>(FLAGS_seed),
          FLAGS_write_fault_one_in,
          FLAGS_inject_error_severity == 1 /* retryable */,
          FLAGS_inject_error_severity == 2 /* has_data_loss*/);
      fault_fs_guard->EnableThreadLocalErrorInjection(
          FaultInjectionIOType::kWrite);

      fault_fs_guard->SetThreadLocalErrorContext(
          FaultInjectionIOType::kMetadataRead,
          static_cast<uint32_t>(FLAGS_seed), FLAGS_metadata_read_fault_one_in,
          FLAGS_inject_error_severity == 1 /* retryable */,
          FLAGS_inject_error_severity == 2 /* has_data_loss*/);
      fault_fs_guard->EnableThreadLocalErrorInjection(
          FaultInjectionIOType::kMetadataRead);

      fault_fs_guard->SetThreadLocalErrorContext(
          FaultInjectionIOType::kMetadataWrite,
          static_cast<uint32_t>(FLAGS_seed), FLAGS_metadata_write_fault_one_in,
          FLAGS_inject_error_severity == 1 /* retryable */,
          FLAGS_inject_error_severity == 2 /* has_data_loss*/);
      fault_fs_guard->EnableThreadLocalErrorInjection(
          FaultInjectionIOType::kMetadataWrite);
    }
  }

  void OnSubcompactionCompleted(const SubcompactionJobInfo& /* si */) override {
    if (fault_fs_guard) {
      fault_fs_guard->DisableAllThreadLocalErrorInjection();
    }
  }

  void OnTableFileCreationStarted(
      const TableFileCreationBriefInfo& /*info*/) override {
    ++num_pending_file_creations_;
  }

  void OnTableFileCreated(const TableFileCreationInfo& info) override {
    assert(info.db_name == db_name_);
    assert(IsValidColumnFamilyName(info.cf_name));
    assert(info.job_id > 0 || FLAGS_compact_files_one_in > 0);
    if (info.status.ok()) {
      assert(info.file_size > 0);
      VerifyFilePath(info.file_path);
      assert(info.table_properties.data_size > 0 ||
             info.table_properties.num_range_deletions > 0);
      assert(info.table_properties.raw_key_size > 0);
      assert(info.table_properties.num_entries > 0);
      VerifyTableFileUniqueId(info.table_properties, info.file_path);
    }
    --num_pending_file_creations_;
  }

  void OnMemTableSealed(const MemTableInfo& /*info*/) override {
    RandomSleep();
  }

  void OnColumnFamilyHandleDeletionStarted(
      ColumnFamilyHandle* /*handle*/) override {
    RandomSleep();
  }

  void OnExternalFileIngested(DB* /*db*/,
                              const ExternalFileIngestionInfo& info) override {
    RandomSleep();
    // Here we assume that each generated external file is ingested
    // exactly once (or thrown away in case of crash)
    VerifyTableFileUniqueId(info.table_properties, info.internal_file_path);
  }

  void OnBackgroundError(BackgroundErrorReason /* reason */,
                         Status* /* bg_error */) override {
    RandomSleep();
  }

  void OnStallConditionsChanged(const WriteStallInfo& /*info*/) override {
    RandomSleep();
  }

  void OnFileReadFinish(const FileOperationInfo& info) override {
    // Even empty callback is valuable because sometimes some locks are
    // released in order to make the callback.

    // Sleep carefully here as it is a frequent operation and we don't want
    // to slow down the tests. We always sleep when the read is large.
    // When read is small, sleep in a small chance.
    size_t length_read = info.length;
    if (length_read >= 1000000 || Random::GetTLSInstance()->OneIn(1000)) {
      RandomSleep();
    }
  }

  void OnFileWriteFinish(const FileOperationInfo& info) override {
    // Even empty callback is valuable because sometimes some locks are
    // released in order to make the callback.

    // Sleep carefully here as it is a frequent operation and we don't want
    // to slow down the tests. When the write is large, always sleep.
    // Otherwise, sleep in a relatively small chance.
    size_t length_write = info.length;
    if (length_write >= 1000000 || Random::GetTLSInstance()->OneIn(64)) {
      RandomSleep();
    }
  }

  bool ShouldBeNotifiedOnFileIO() override {
    RandomSleep();
    return static_cast<bool>(Random::GetTLSInstance()->OneIn(1));
  }

  void OnErrorRecoveryBegin(BackgroundErrorReason /* reason */,
                            Status /* bg_error */,
                            bool* /* auto_recovery */) override {
    RandomSleep();
    if (FLAGS_error_recovery_with_no_fault_injection && fault_fs_guard) {
      fault_fs_guard->DisableAllThreadLocalErrorInjection();
      // TODO(hx235): only exempt the flush thread during error recovery instead
      // of all the flush threads from error injection
      fault_fs_guard->SetIOActivtiesExcludedFromFaultInjection(
          {Env::IOActivity::kFlush});
    }
  }

  void OnErrorRecoveryEnd(
      const BackgroundErrorRecoveryInfo& /*info*/) override {
    RandomSleep();
    if (FLAGS_error_recovery_with_no_fault_injection && fault_fs_guard) {
      fault_fs_guard->EnableAllThreadLocalErrorInjection();
      fault_fs_guard->SetIOActivtiesExcludedFromFaultInjection({});
    }
  }

 protected:
  bool IsValidColumnFamilyName(const std::string& cf_name) const {
    if (cf_name == kDefaultColumnFamilyName) {
      return true;
    }
    // The column family names in the stress tests are numbers.
    for (size_t i = 0; i < cf_name.size(); ++i) {
      if (cf_name[i] < '0' || cf_name[i] > '9') {
        return false;
      }
    }
    return true;
  }

  void VerifyFileDir(const std::string& file_dir) {
#ifndef NDEBUG
    if (db_name_ == file_dir) {
      return;
    }
    for (const auto& db_path : db_paths_) {
      if (db_path.path == file_dir) {
        return;
      }
    }
    for (auto& cf : column_families_) {
      for (const auto& cf_path : cf.options.cf_paths) {
        if (cf_path.path == file_dir) {
          return;
        }
      }
    }
    assert(false);
#else
    (void)file_dir;
#endif  // !NDEBUG
  }

  void VerifyFileName(const std::string& file_name) {
#ifndef NDEBUG
    uint64_t file_number;
    FileType file_type;
    bool result = ParseFileName(file_name, &file_number, &file_type);
    assert(result);
    assert(file_type == kTableFile);
#else
    (void)file_name;
#endif  // !NDEBUG
  }

  void VerifyFilePath(const std::string& file_path) {
#ifndef NDEBUG
    size_t pos = file_path.find_last_of("/");
    if (pos == std::string::npos) {
      VerifyFileName(file_path);
    } else {
      if (pos > 0) {
        VerifyFileDir(file_path.substr(0, pos));
      }
      VerifyFileName(file_path.substr(pos));
    }
#else
    (void)file_path;
#endif  // !NDEBUG
  }

  // Unique id is verified using the TableProperties. file_path is only used
  // for reporting.
  void VerifyTableFileUniqueId(const TableProperties& new_file_properties,
                               const std::string& file_path);

  void RandomSleep() {
    std::this_thread::sleep_for(
        std::chrono::microseconds(Random::GetTLSInstance()->Uniform(5000)));
  }

 private:
  std::string db_name_;
  std::vector<DbPath> db_paths_;
  std::vector<ColumnFamilyDescriptor> column_families_;
  std::atomic<int> num_pending_file_creations_;
  UniqueIdVerifier unique_ids_;
};
}  // namespace ROCKSDB_NAMESPACE
#endif  // GFLAGS

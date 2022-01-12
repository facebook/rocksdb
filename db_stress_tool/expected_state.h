//  Copyright (c) 2021-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#ifdef GFLAGS

#pragma once

#include <stdint.h>

#include <atomic>
#include <memory>

#include "db/dbformat.h"
#include "file/file_util.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/file_system.h"
#include "rocksdb/rocksdb_namespace.h"
#include "rocksdb/types.h"
#include "util/string_util.h"

namespace ROCKSDB_NAMESPACE {

// An `ExpectedState` provides read/write access to expected values for every
// key.
class ExpectedState {
 public:
  explicit ExpectedState(size_t max_key, size_t num_column_families);

  virtual ~ExpectedState() {}

  // Requires external locking preventing concurrent execution with any other
  // member function.
  virtual Status Open(bool create) = 0;

  // Requires external locking covering all keys in `cf`.
  void ClearColumnFamily(int cf);

  // @param pending True if the update may have started but is not yet
  //    guaranteed finished. This is useful for crash-recovery testing when the
  //    process may crash before updating the expected values array.
  //
  // Requires external locking covering `key` in `cf`.
  void Put(int cf, int64_t key, uint32_t value_base, bool pending);

  // Requires external locking covering `key` in `cf`.
  uint32_t Get(int cf, int64_t key) const;

  // @param pending See comment above Put()
  // Returns true if the key was not yet deleted.
  //
  // Requires external locking covering `key` in `cf`.
  bool Delete(int cf, int64_t key, bool pending);

  // @param pending See comment above Put()
  // Returns true if the key was not yet deleted.
  //
  // Requires external locking covering `key` in `cf`.
  bool SingleDelete(int cf, int64_t key, bool pending);

  // @param pending See comment above Put()
  // Returns number of keys deleted by the call.
  //
  // Requires external locking covering keys in `[begin_key, end_key)` in `cf`.
  int DeleteRange(int cf, int64_t begin_key, int64_t end_key, bool pending);

  // Requires external locking covering `key` in `cf`.
  bool Exists(int cf, int64_t key);

 private:
  // Requires external locking covering `key` in `cf`.
  std::atomic<uint32_t>& Value(int cf, int64_t key) const {
    return values_[cf * max_key_ + key];
  }

  const size_t max_key_;
  const size_t num_column_families_;

 protected:
  size_t GetValuesLen() const {
    return sizeof(std::atomic<uint32_t>) * num_column_families_ * max_key_;
  }

  // Requires external locking preventing concurrent execution with any other
  // member function.
  void Reset();

  std::atomic<uint32_t>* values_;
};

// A `FileExpectedState` implements `ExpectedState` backed by a file.
class FileExpectedState : public ExpectedState {
 public:
  explicit FileExpectedState(std::string expected_state_file_path,
                             size_t max_key, size_t num_column_families);

  // Requires external locking preventing concurrent execution with any other
  // member function.
  Status Open(bool create) override;

 private:
  const std::string expected_state_file_path_;
  std::unique_ptr<MemoryMappedFileBuffer> expected_state_mmap_buffer_;
};

// An `AnonExpectedState` implements `ExpectedState` backed by a memory
// allocation.
class AnonExpectedState : public ExpectedState {
 public:
  explicit AnonExpectedState(size_t max_key, size_t num_column_families);

  // Requires external locking preventing concurrent execution with any other
  // member function.
  Status Open(bool create) override;

 private:
  std::unique_ptr<std::atomic<uint32_t>[]> values_allocation_;
};

// An `ExpectedStateManager` manages data about the expected state of the
// database. It exposes operations for reading and modifying the latest
// expected state.
class ExpectedStateManager {
 public:
  explicit ExpectedStateManager(size_t max_key, size_t num_column_families);

  virtual ~ExpectedStateManager();

  // Requires external locking preventing concurrent execution with any other
  // member function.
  virtual Status Open() = 0;

  // Saves expected values for the current state of `db` and begins tracking
  // changes. Following a successful `SaveAtAndAfter()`, `Restore()` can be
  // called on the same DB, as long as its state does not roll back to before
  // its current state.
  //
  // Requires external locking preventing concurrent execution with any other
  // member function. Furthermore, `db` must not be mutated while this function
  // is executing.
  virtual Status SaveAtAndAfter(DB* db) = 0;

  // Returns true if at least one state of historical expected values can be
  // restored.
  //
  // Requires external locking preventing concurrent execution with any other
  // member function.
  virtual bool HasHistory() = 0;

  // Restores expected values according to the current state of `db`. See
  // `SaveAtAndAfter()` for conditions where this can be called.
  //
  // Requires external locking preventing concurrent execution with any other
  // member function. Furthermore, `db` must not be mutated while this function
  // is executing.
  virtual Status Restore(DB* db) = 0;

  // Requires external locking covering all keys in `cf`.
  void ClearColumnFamily(int cf) { return latest_->ClearColumnFamily(cf); }

  // @param pending True if the update may have started but is not yet
  //    guaranteed finished. This is useful for crash-recovery testing when the
  //    process may crash before updating the expected values array.
  //
  // Requires external locking covering `key` in `cf`.
  void Put(int cf, int64_t key, uint32_t value_base, bool pending) {
    return latest_->Put(cf, key, value_base, pending);
  }

  // Requires external locking covering `key` in `cf`.
  uint32_t Get(int cf, int64_t key) const { return latest_->Get(cf, key); }

  // @param pending See comment above Put()
  // Returns true if the key was not yet deleted.
  //
  // Requires external locking covering `key` in `cf`.
  bool Delete(int cf, int64_t key, bool pending) {
    return latest_->Delete(cf, key, pending);
  }

  // @param pending See comment above Put()
  // Returns true if the key was not yet deleted.
  //
  // Requires external locking covering `key` in `cf`.
  bool SingleDelete(int cf, int64_t key, bool pending) {
    return latest_->SingleDelete(cf, key, pending);
  }

  // @param pending See comment above Put()
  // Returns number of keys deleted by the call.
  //
  // Requires external locking covering keys in `[begin_key, end_key)` in `cf`.
  int DeleteRange(int cf, int64_t begin_key, int64_t end_key, bool pending) {
    return latest_->DeleteRange(cf, begin_key, end_key, pending);
  }

  // Requires external locking covering `key` in `cf`.
  bool Exists(int cf, int64_t key) { return latest_->Exists(cf, key); }

 protected:
  const size_t max_key_;
  const size_t num_column_families_;
  std::unique_ptr<ExpectedState> latest_;
};

// A `FileExpectedStateManager` implements an `ExpectedStateManager` backed by
// a directory of files containing data about the expected state of the
// database.
class FileExpectedStateManager : public ExpectedStateManager {
 public:
  explicit FileExpectedStateManager(size_t max_key, size_t num_column_families,
                                    std::string expected_state_dir_path);

  // Requires external locking preventing concurrent execution with any other
  // member function.
  Status Open() override;

  // See `ExpectedStateManager::SaveAtAndAfter()` API doc.
  //
  // This implementation makes a copy of "LATEST.state" into
  // "<current seqno>.state", and starts a trace in "<current seqno>.trace".
  // Due to using external files, a following `Restore()` can happen even
  // from a different process.
  Status SaveAtAndAfter(DB* db) override;

  // See `ExpectedStateManager::HasHistory()` API doc.
  bool HasHistory() override;

  // See `ExpectedStateManager::Restore()` API doc.
  //
  // Say `db->GetLatestSequenceNumber()` was `a` last time `SaveAtAndAfter()`
  // was called and now it is `b`. Then this function replays `b - a` write
  // operations from "`a`.trace" onto "`a`.state", and then copies the resulting
  // file into "LATEST.state".
  Status Restore(DB* db) override;

 private:
  // Requires external locking preventing concurrent execution with any other
  // member function.
  Status Clean();

  std::string GetTempPathForFilename(const std::string& filename);
  std::string GetPathForFilename(const std::string& filename);

  static const std::string kLatestBasename;
  static const std::string kStateFilenameSuffix;
  static const std::string kTraceFilenameSuffix;
  static const std::string kTempFilenamePrefix;
  static const std::string kTempFilenameSuffix;

  const std::string expected_state_dir_path_;
  SequenceNumber saved_seqno_ = kMaxSequenceNumber;
};

// An `AnonExpectedStateManager` implements an `ExpectedStateManager` backed by
// a memory allocation containing data about the expected state of the database.
class AnonExpectedStateManager : public ExpectedStateManager {
 public:
  explicit AnonExpectedStateManager(size_t max_key, size_t num_column_families);

  // See `ExpectedStateManager::SaveAtAndAfter()` API doc.
  //
  // This implementation returns `Status::NotSupported` since we do not
  // currently have a need to keep history of expected state within a process.
  Status SaveAtAndAfter(DB* /* db */) override {
    return Status::NotSupported();
  }

  // See `ExpectedStateManager::HasHistory()` API doc.
  bool HasHistory() override { return false; }

  // See `ExpectedStateManager::Restore()` API doc.
  //
  // This implementation returns `Status::NotSupported` since we do not
  // currently have a need to keep history of expected state within a process.
  Status Restore(DB* /* db */) override { return Status::NotSupported(); }

  // Requires external locking preventing concurrent execution with any other
  // member function.
  Status Open() override;
};

}  // namespace ROCKSDB_NAMESPACE

#endif  // GFLAGS

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
// This class is not thread-safe.
class ExpectedValue {
 public:
  static uint32_t GetValueBaseMask() { return VALUE_BASE_MASK; }
  static uint32_t GetValueBaseDelta() { return VALUE_BASE_DELTA; }
  static uint32_t GetDelCounterDelta() { return DEL_COUNTER_DELTA; }
  static uint32_t GetDelMask() { return DEL_MASK; }
  static bool IsValueBaseValid(uint32_t value_base) {
    return IsValuePartValid(value_base, VALUE_BASE_MASK);
  }

  explicit ExpectedValue(uint32_t expected_value)
      : expected_value_(expected_value) {}

  bool Exists() const { return PendingWrite() || !IsDeleted(); }

  uint32_t Read() const { return expected_value_; }

  void Put(bool pending);

  bool Delete(bool pending);

  void SyncPut(uint32_t value_base);

  void SyncPendingPut();

  void SyncDelete();

  uint32_t GetValueBase() const { return GetValuePart(VALUE_BASE_MASK); }

  uint32_t NextValueBase() const {
    return GetIncrementedValuePart(VALUE_BASE_MASK, VALUE_BASE_DELTA);
  }

  void SetValueBase(uint32_t new_value_base) {
    SetValuePart(VALUE_BASE_MASK, new_value_base);
  }

  bool PendingWrite() const {
    const uint32_t pending_write = GetValuePart(PENDING_WRITE_MASK);
    return pending_write != 0;
  }

  void SetPendingWrite() {
    SetValuePart(PENDING_WRITE_MASK, PENDING_WRITE_MASK);
  }

  void ClearPendingWrite() { ClearValuePart(PENDING_WRITE_MASK); }

  uint32_t GetDelCounter() const { return GetValuePart(DEL_COUNTER_MASK); }

  uint32_t NextDelCounter() const {
    return GetIncrementedValuePart(DEL_COUNTER_MASK, DEL_COUNTER_DELTA);
  }

  void SetDelCounter(uint32_t new_del_counter) {
    SetValuePart(DEL_COUNTER_MASK, new_del_counter);
  }

  bool PendingDelete() const {
    const uint32_t pending_del = GetValuePart(PENDING_DEL_MASK);
    return pending_del != 0;
  }

  void SetPendingDel() { SetValuePart(PENDING_DEL_MASK, PENDING_DEL_MASK); }

  void ClearPendingDel() { ClearValuePart(PENDING_DEL_MASK); }

  bool IsDeleted() const {
    const uint32_t deleted = GetValuePart(DEL_MASK);
    return deleted != 0;
  }

  void SetDeleted() { SetValuePart(DEL_MASK, DEL_MASK); }

  void ClearDeleted() { ClearValuePart(DEL_MASK); }

  uint32_t GetFinalValueBase() const;

  uint32_t GetFinalDelCounter() const;

 private:
  static bool IsValuePartValid(uint32_t value_part, uint32_t value_part_mask) {
    return (value_part & (~value_part_mask)) == 0;
  }

  // The 32-bit expected_value_ is divided into following parts:
  // Bit 0 - 14: value base
  static constexpr uint32_t VALUE_BASE_MASK = 0x7fff;
  static constexpr uint32_t VALUE_BASE_DELTA = 1;
  // Bit 15: whether write to this value base is pending (0 equals `false`)
  static constexpr uint32_t PENDING_WRITE_MASK = (uint32_t)1 << 15;
  // Bit 16 - 29: deletion counter (i.e, number of times this value base has
  // been deleted)
  static constexpr uint32_t DEL_COUNTER_MASK = 0x3fff0000;
  static constexpr uint32_t DEL_COUNTER_DELTA = (uint32_t)1 << 16;
  // Bit 30: whether deletion of this value base is pending (0 equals `false`)
  static constexpr uint32_t PENDING_DEL_MASK = (uint32_t)1 << 30;
  // Bit 31: whether this value base is deleted (0 equals `false`)
  static constexpr uint32_t DEL_MASK = (uint32_t)1 << 31;

  uint32_t GetValuePart(uint32_t value_part_mask) const {
    return expected_value_ & value_part_mask;
  }

  uint32_t GetIncrementedValuePart(uint32_t value_part_mask,
                                   uint32_t value_part_delta) const {
    uint32_t current_value_part = GetValuePart(value_part_mask);
    ExpectedValue temp_expected_value(current_value_part + value_part_delta);
    return temp_expected_value.GetValuePart(value_part_mask);
  }

  void SetValuePart(uint32_t value_part_mask, uint32_t new_value_part) {
    assert(IsValuePartValid(new_value_part, value_part_mask));
    ClearValuePart(value_part_mask);
    expected_value_ |= new_value_part;
  }

  void ClearValuePart(uint32_t value_part_mask) {
    expected_value_ &= (~value_part_mask);
  }

  uint32_t expected_value_;
};

class PendingExpectedValue {
 public:
  explicit PendingExpectedValue(std::atomic<uint32_t>* value_ptr,
                                ExpectedValue orig_value,
                                ExpectedValue final_value)
      : value_ptr_(value_ptr),
        orig_value_(orig_value),
        final_value_(final_value) {}

  void Commit() {
    // To prevent low-level instruction reordering that results
    // in setting expected value happens before db write
    std::atomic_thread_fence(std::memory_order_release);
    value_ptr_->store(final_value_.Read());
  }

  uint32_t GetFinalValueBase() { return final_value_.GetValueBase(); }

 private:
  std::atomic<uint32_t>* const value_ptr_;
  const ExpectedValue orig_value_;
  const ExpectedValue final_value_;
};

class ExpectedValueHelper {
 public:
  // Return whether value is expected not to exist from begining till the end
  // of the read based on `pre_read_expected_value` and
  // `pre_read_expected_value`.
  static bool MustHaveNotExisted(ExpectedValue pre_read_expected_value,
                                 ExpectedValue post_read_expected_value);

  // Return whether value is expected to exist from begining till the end of
  // the read based on `pre_read_expected_value` and
  // `pre_read_expected_value`.
  static bool MustHaveExisted(ExpectedValue pre_read_expected_value,
                              ExpectedValue post_read_expected_value);

  // Return whether the `value_base` falls within the expected value base
  static bool InExpectedValueBaseRange(uint32_t value_base,
                                       ExpectedValue pre_read_expected_value,
                                       ExpectedValue post_read_expected_value);
};

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

  // Prepare a Put that will be started but not finished yet
  // This is useful for crash-recovery testing when the process may crash
  // before updating the corresponding expected value
  //
  // Requires external locking covering `key` in `cf` to prevent concurrent
  // write or delete to the same `key`.
  PendingExpectedValue PreparePut(int cf, int64_t key);

  // Does not requires external locking.
  ExpectedValue Get(int cf, int64_t key);

  // Prepare a Delete that will be started but not finished yet
  // This is useful for crash-recovery testing when the process may crash
  // before updating the corresponding expected value
  //
  // Requires external locking covering `key` in `cf` to prevent concurrent
  // write or delete to the same `key`.
  PendingExpectedValue PrepareDelete(int cf, int64_t key,
                                     bool* prepared = nullptr);

  // Requires external locking covering `key` in `cf` to prevent concurrent
  // write or delete to the same `key`.
  PendingExpectedValue PrepareSingleDelete(int cf, int64_t key);

  // Requires external locking covering keys in `[begin_key, end_key)` in `cf`
  // to prevent concurrent write or delete to the same `key`.
  std::vector<PendingExpectedValue> PrepareDeleteRange(int cf,
                                                       int64_t begin_key,
                                                       int64_t end_key);

  // Update the expected value for start of an incomplete write or delete
  // operation on the key assoicated with this expected value
  void Precommit(int cf, int64_t key, const ExpectedValue& value);

  // Requires external locking covering `key` in `cf` to prevent concurrent
  // delete to the same `key`.
  bool Exists(int cf, int64_t key);

  // Sync the `value_base` to the corresponding expected value
  //
  // Requires external locking covering `key` in `cf` or be in single thread
  // to prevent concurrent write or delete to the same `key`
  void SyncPut(int cf, int64_t key, uint32_t value_base);

  // Sync the corresponding expected value to be pending Put
  //
  // Requires external locking covering `key` in `cf` or be in single thread
  // to prevent concurrent write or delete to the same `key`
  void SyncPendingPut(int cf, int64_t key);

  // Sync the corresponding expected value to be deleted
  //
  // Requires external locking covering `key` in `cf` or be in single thread
  // to prevent concurrent write or delete to the same `key`
  void SyncDelete(int cf, int64_t key);

  // Sync the corresponding expected values to be deleted
  //
  // Requires external locking covering keys in `[begin_key, end_key)` in `cf`
  // to prevent concurrent write or delete to the same `key`
  void SyncDeleteRange(int cf, int64_t begin_key, int64_t end_key);

 private:
  // Does not requires external locking.
  std::atomic<uint32_t>& Value(int cf, int64_t key) const {
    return values_[cf * max_key_ + key];
  }

  // Does not requires external locking
  ExpectedValue Load(int cf, int64_t key) const {
    return ExpectedValue(Value(cf, key).load());
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

  // See ExpectedState::PreparePut()
  PendingExpectedValue PreparePut(int cf, int64_t key) {
    return latest_->PreparePut(cf, key);
  }

  // See ExpectedState::Get()
  ExpectedValue Get(int cf, int64_t key) { return latest_->Get(cf, key); }

  // See ExpectedState::PrepareDelete()
  PendingExpectedValue PrepareDelete(int cf, int64_t key) {
    return latest_->PrepareDelete(cf, key);
  }

  // See ExpectedState::PrepareSingleDelete()
  PendingExpectedValue PrepareSingleDelete(int cf, int64_t key) {
    return latest_->PrepareSingleDelete(cf, key);
  }

  // See ExpectedState::PrepareDeleteRange()
  std::vector<PendingExpectedValue> PrepareDeleteRange(int cf,
                                                       int64_t begin_key,
                                                       int64_t end_key) {
    return latest_->PrepareDeleteRange(cf, begin_key, end_key);
  }

  // See ExpectedState::Exists()
  bool Exists(int cf, int64_t key) { return latest_->Exists(cf, key); }

  // See ExpectedState::SyncPut()
  void SyncPut(int cf, int64_t key, uint32_t value_base) {
    return latest_->SyncPut(cf, key, value_base);
  }

  // See ExpectedState::SyncPendingPut()
  void SyncPendingPut(int cf, int64_t key) {
    return latest_->SyncPendingPut(cf, key);
  }

  // See ExpectedState::SyncDelete()
  void SyncDelete(int cf, int64_t key) { return latest_->SyncDelete(cf, key); }

  // See ExpectedState::SyncDeleteRange()
  void SyncDeleteRange(int cf, int64_t begin_key, int64_t end_key) {
    return latest_->SyncDeleteRange(cf, begin_key, end_key);
  }

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

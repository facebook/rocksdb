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

class ExpectedValueHelper {
 public:
  // Return whether value is expected not to exist from begining till the end
  // of the read based on `pre_read_expected_value` and
  // `pre_read_expected_value`.
  static bool MustHaveNotExisted(uint32_t pre_read_expected_value,
                                 uint32_t post_read_expected_value);

  // Return whether value is expected to exist from begining till the end of
  // the read based on `pre_read_expected_value` and `pre_read_expected_value`.
  static bool MustHaveExisted(uint32_t pre_read_expected_value,
                              uint32_t post_read_expected_value);

  // Return whether the `value_base` falls within the expected value base
  static bool InExpectedValueBaseRange(uint32_t value_base,
                                       uint32_t pre_read_expected_value_base,
                                       uint32_t post_read_expected_value_base,
                                       bool post_read_expected_pending_write);

  static uint32_t GetValueBase(uint32_t expected_value) {
    return GetValuePart(expected_value, VALUE_BASE_MASK);
  }

  static uint32_t NextValueBase(uint32_t cur_value_base) {
    return GetIncrementedValuePart(cur_value_base, VALUE_BASE_MASK,
                                   VALUE_BASE_DELTA);
  }

  static void SetValueBase(uint32_t* expected_value, uint32_t new_value_base) {
    SetValuePart(expected_value, VALUE_BASE_MASK, new_value_base);
  }

  static bool PendingWrite(uint32_t expected_value) {
    const uint32_t pending_write =
        GetValuePart(expected_value, PENDING_WRITE_MASK);
    return pending_write != 0;
  }

  static void SetPendingWrite(uint32_t* expected_value) {
    SetValuePart(expected_value, PENDING_WRITE_MASK, PENDING_WRITE_MASK);
  }

  static void ClearPendingWrite(uint32_t* expected_value) {
    ClearValuePart(expected_value, PENDING_WRITE_MASK);
  }

  static uint32_t GetDelCounter(uint32_t expected_value) {
    return GetValuePart(expected_value, DEL_COUNTER_MASK);
  }

  static uint32_t NextDelCounter(uint32_t cur_del_counter) {
    return GetIncrementedValuePart(cur_del_counter, DEL_COUNTER_MASK,
                                   DEL_COUNTER_DELTA);
  }

  static void SetDelCounter(uint32_t* expected_value,
                            uint32_t new_del_counter) {
    SetValuePart(expected_value, DEL_COUNTER_MASK, new_del_counter);
  }

  static bool PendingDelete(uint32_t expected_value) {
    const uint32_t pending_del = GetValuePart(expected_value, PENDING_DEL_MASK);
    return pending_del != 0;
  }

  static void SetPendingDel(uint32_t* expected_value) {
    SetValuePart(expected_value, PENDING_DEL_MASK, PENDING_DEL_MASK);
  }

  static void ClearPendingDel(uint32_t* expected_value) {
    ClearValuePart(expected_value, PENDING_DEL_MASK);
  }

  static bool IsDeleted(uint32_t expected_value) {
    const uint32_t deleted = GetValuePart(expected_value, DEL_MASK);
    return deleted != 0;
  }

  static void SetDeleted(uint32_t* expected_value) {
    SetValuePart(expected_value, DEL_MASK, DEL_MASK);
  }

  static void ClearDeleted(uint32_t* expected_value) {
    ClearValuePart(expected_value, DEL_MASK);
  }

  static uint32_t GetDelMask() { return DEL_MASK; }

 private:
  // Value in `std::atomic<uint32_t>* values_` is divided into following parts
  // by bits:
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

  static uint32_t GetValuePart(uint32_t expected_value,
                               uint32_t value_part_mask) {
    return expected_value & value_part_mask;
  }

  static uint32_t GetIncrementedValuePart(uint32_t current_value_part,
                                          uint32_t value_part_mask,
                                          uint32_t value_part_delta) {
    assert(IsValuePartValid(current_value_part, value_part_mask));
    return GetValuePart(current_value_part + value_part_delta, value_part_mask);
  }

  static bool IsValuePartValid(uint32_t value_part, uint32_t value_part_mask) {
    return (value_part & (~value_part_mask)) == 0;
  }

  static void SetValuePart(uint32_t* expected_value, uint32_t value_part_mask,
                           uint32_t new_value_part) {
    assert(expected_value);
    assert(IsValuePartValid(new_value_part, value_part_mask));
    ClearValuePart(expected_value, value_part_mask);
    *expected_value |= new_value_part;
  }

  static void ClearValuePart(uint32_t* expected_value,
                             uint32_t value_part_mask) {
    assert(expected_value);
    *expected_value &= (~value_part_mask);
  }
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

  // @param pending True if the update may have started but is not yet
  //    guaranteed finished. This is useful for crash-recovery testing when the
  //    process may crash before updating the expected values array.
  //
  // Requires external locking covering `key` in `cf` to prevent concurrent
  // write or delete to the same `key`.
  void Put(int cf, int64_t key, uint32_t value_base, bool pending);

  // Does not requires external locking.
  uint32_t Get(int cf, int64_t key) const;

  // @param pending See comment above Put()
  // Returns true if the key was not yet deleted.
  //
  // Requires external locking covering `key` in `cf` to prevent concurrent
  // write or delete to the same `key`.
  bool Delete(int cf, int64_t key, bool pending);

  // @param pending See comment above Put()
  // Returns true if the key was not yet deleted.
  //
  // Requires external locking covering `key` in `cf` to prevent concurrent
  // write or delete to the same `key`.
  bool SingleDelete(int cf, int64_t key, bool pending);

  // @param pending See comment above Put()
  // Returns number of keys deleted by the call.
  //
  // Requires external locking covering keys in `[begin_key, end_key)` in `cf`
  // to prevent concurrent write or delete to the same `key`.
  int DeleteRange(int cf, int64_t begin_key, int64_t end_key, bool pending);

  // Requires external locking covering `key` in `cf` to prevent concurrent
  // delete to the same `key`.
  bool Exists(int cf, int64_t key);

 private:
  // Does not requires external locking.
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
  // Requires external locking covering `key` in `cf` to prevent concurrent
  // write or delete to the same `key`.
  void Put(int cf, int64_t key, uint32_t value_base, bool pending) {
    return latest_->Put(cf, key, value_base, pending);
  }

  // Does not require external locking.
  uint32_t Get(int cf, int64_t key) const { return latest_->Get(cf, key); }

  // @param pending See comment above Put()
  // Returns true if the key was not yet deleted.
  //
  // Requires external locking covering `key` in `cf` to prevent concurrent
  // write or delete to the same `key`.
  bool Delete(int cf, int64_t key, bool pending) {
    return latest_->Delete(cf, key, pending);
  }

  // @param pending See comment above Put()
  // Returns true if the key was not yet deleted.
  //
  // Requires external locking covering `key` in `cf` to prevent concurrent
  // write or delete to the same `key`.
  bool SingleDelete(int cf, int64_t key, bool pending) {
    return latest_->SingleDelete(cf, key, pending);
  }

  // @param pending See comment above Put()
  // Returns number of keys deleted by the call.
  //
  // Requires external locking covering keys in `[begin_key, end_key)` in `cf`
  // to prevent concurrent write or delete to the same `key`.
  int DeleteRange(int cf, int64_t begin_key, int64_t end_key, bool pending) {
    return latest_->DeleteRange(cf, begin_key, end_key, pending);
  }

  // Requires external locking covering `key` in `cf` to prevent concurrent
  // delete to the same `key`.
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

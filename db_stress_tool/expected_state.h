//  Copyright (c) 2021-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#ifdef GFLAGS

#pragma once

#include <stdint.h>

#include <atomic>
#include <memory>

#include "rocksdb/env.h"
#include "rocksdb/rocksdb_namespace.h"

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

 private:
  // Requires external locking preventing concurrent execution with any other
  // member function.
  Status Clean();

  std::string GetTempPathForFilename(const std::string& filename);
  std::string GetPathForFilename(const std::string& filename);

  static const std::string kLatestFilename;

  const std::string expected_state_dir_path_;
};

// An `AnonExpectedStateManager` implements an `ExpectedStateManager` backed by
// a memory allocation containing data about the expected state of the database.
class AnonExpectedStateManager : public ExpectedStateManager {
 public:
  explicit AnonExpectedStateManager(size_t max_key, size_t num_column_families);

  // Requires external locking preventing concurrent execution with any other
  // member function.
  Status Open() override;
};

}  // namespace ROCKSDB_NAMESPACE

#endif  // GFLAGS

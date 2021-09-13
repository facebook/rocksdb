//  Copyright (c) 2021-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

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
  explicit ExpectedState(std::string expected_state_file_path, size_t max_key,
                         size_t num_column_families);

  ~ExpectedState();

  Status Open();

  std::atomic<uint32_t>* REMOVEME_GetValues() { return values_; }
  bool REMOVEME_ValuesNeedInit() { return REMOVEME_values_need_init_; }

 private:
  const std::string expected_state_file_path_;
  const size_t max_key_;
  const size_t num_column_families_;

  std::unique_ptr<MemoryMappedFileBuffer> expected_state_mmap_buffer_;
  std::atomic<uint32_t>* values_;
  bool REMOVEME_values_need_init_;
};

// An `ExpectedStateManager` manages a directory containing data about the
// expected state of the database. It exposes operations for reading and
// modifying the latest expected state.
class ExpectedStateManager {
 public:
  explicit ExpectedStateManager(std::string expected_state_dir_path,
                                size_t max_key, size_t num_column_families);

  ~ExpectedStateManager();

  // The following APIs are not thread-safe and require external synchronization
  // for the entire object.
  Status Open();

  std::atomic<uint32_t>* REMOVEME_GetValues() {
    return latest_->REMOVEME_GetValues();
  }
  bool REMOVEME_ValuesNeedInit() { return latest_->REMOVEME_ValuesNeedInit(); }

  // The following APIs are not thread-safe and require external synchronization
  // for the affected keys only. For example, `Put("a", ...)` and
  // `Put("b", ...)` could be executed in parallel with no external
  // synchronization.

 private:
  static const std::string kLatestFilename;

  const std::string expected_state_dir_path_;
  const size_t max_key_;
  const size_t num_column_families_;
  std::unique_ptr<ExpectedState> latest_;
};

}  // namespace ROCKSDB_NAMESPACE

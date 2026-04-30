//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//

#ifdef GFLAGS
#include "db_stress_tool/db_stress_shared_state.h"

#include "db_stress_tool/db_stress_test_base.h"

namespace ROCKSDB_NAMESPACE {
thread_local bool SharedState::ignore_read_error;

SharedState::SharedState(Env* /*env*/, StressTest* stress_test)
    : cv_(&mu_),
      seed_(static_cast<uint32_t>(FLAGS_seed)),
      max_key_(FLAGS_max_key),
      log2_keys_per_lock_(static_cast<uint32_t>(FLAGS_log2_keys_per_lock)),
      num_threads_(0),
      num_initialized_(0),
      num_populated_(0),
      vote_reopen_(0),
      num_done_(0),
      start_(false),
      start_verify_(false),
      num_bg_threads_(0),
      should_stop_bg_thread_(false),
      bg_thread_finished_(0),
      stress_test_(stress_test),
      verification_failure_(false),
      should_stop_test_(false),
      no_overwrite_ids_(GenerateNoOverwriteIds()),
      expected_state_manager_(nullptr),
      printing_verification_results_(false),
      start_timestamp_(Env::Default()->NowNanos()) {
  const std::string expected_values_dir = stress_test_->GetExpectedValuesDir();
  Status status;
  // TODO: We should introduce a way to explicitly disable verification
  // during shutdown. When that is disabled and expected_states_root
  // is empty (disabling verification at startup), we can skip tracking
  // expected state. Only then should we permit bypassing the below feature
  // compatibility checks.
  if (!expected_values_dir.empty()) {
    if (!std::atomic<uint32_t>{}.is_lock_free() ||
        !std::atomic<uint64_t>{}.is_lock_free()) {
      std::ostringstream status_s;
      status_s << "Cannot use --expected_states_root on platforms without "
                  "lock-free "
               << (!std::atomic<uint32_t>{}.is_lock_free()
                       ? "std::atomic<uint32_t>"
                       : "std::atomic<uint64_t>");
      status = Status::InvalidArgument(status_s.str());
    }

    if (status.ok() && FLAGS_clear_column_family_one_in > 0) {
      status = Status::InvalidArgument(
          "Cannot use --expected_states_root when "
          "--clear_column_family_one_in is greater than zero.");
    }
  }
  if (status.ok()) {
    if (expected_values_dir.empty()) {
      expected_state_manager_.reset(
          new AnonExpectedStateManager(FLAGS_max_key, FLAGS_column_families));
    } else {
      expected_state_manager_.reset(new FileExpectedStateManager(
          FLAGS_max_key, FLAGS_column_families, expected_values_dir));
    }
    status = expected_state_manager_->Open();
  }
  if (!status.ok()) {
    fprintf(stderr, "Failed setting up expected state with error: %s\n",
            status.ToString().c_str());
    exit(1);
  }

  if (FLAGS_test_batches_snapshots) {
    fprintf(stdout, "No lock creation because test_batches_snapshots set\n");
    return;
  }

  long num_locks = static_cast<long>(max_key_ >> log2_keys_per_lock_);
  if (max_key_ & ((1 << log2_keys_per_lock_) - 1)) {
    num_locks++;
  }
  fprintf(stdout, "Creating %ld locks\n", num_locks * FLAGS_column_families);
  key_locks_.resize(FLAGS_column_families);

  for (int i = 0; i < FLAGS_column_families; ++i) {
    key_locks_[i].reset(new port::Mutex[num_locks]);
  }
  if (FLAGS_read_fault_one_in || FLAGS_metadata_read_fault_one_in) {
#ifdef NDEBUG
    // Unsupported in release mode because it relies on
    // `IGNORE_STATUS_IF_ERROR` to distinguish faults not expected to lead to
    // failure.
    fprintf(stderr,
            "Cannot set nonzero value for --read_fault_one_in in "
            "release mode.");
    exit(1);
#else   // NDEBUG
    SyncPoint::GetInstance()->SetCallBack("FaultInjectionIgnoreError",
                                          IgnoreReadErrorCallback);
    SyncPoint::GetInstance()->EnableProcessing();
#endif  // NDEBUG
  }
}

bool SharedState::ShouldVerifyAtBeginning() const {
  return !stress_test_->GetExpectedValuesDir().empty();
}

}  // namespace ROCKSDB_NAMESPACE
#endif  // GFLAGS

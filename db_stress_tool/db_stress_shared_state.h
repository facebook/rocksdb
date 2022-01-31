//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors

#ifdef GFLAGS
#pragma once

#include "db_stress_tool/db_stress_stat.h"
#include "db_stress_tool/expected_state.h"
// SyncPoint is not supported in Released Windows Mode.
#if !(defined NDEBUG) || !defined(OS_WIN)
#include "test_util/sync_point.h"
#endif  // !(defined NDEBUG) || !defined(OS_WIN)
#include "util/gflags_compat.h"

DECLARE_uint64(seed);
DECLARE_int64(max_key);
DECLARE_uint64(log2_keys_per_lock);
DECLARE_int32(threads);
DECLARE_int32(column_families);
DECLARE_int32(nooverwritepercent);
DECLARE_string(expected_values_dir);
DECLARE_int32(clear_column_family_one_in);
DECLARE_bool(test_batches_snapshots);
DECLARE_int32(compaction_thread_pool_adjust_interval);
DECLARE_int32(continuous_verification_interval);
DECLARE_int32(read_fault_one_in);
DECLARE_int32(write_fault_one_in);
DECLARE_int32(open_metadata_write_fault_one_in);
DECLARE_int32(open_write_fault_one_in);
DECLARE_int32(open_read_fault_one_in);

DECLARE_int32(injest_error_severity);

namespace ROCKSDB_NAMESPACE {
class StressTest;

// State shared by all concurrent executions of the same benchmark.
class SharedState {
 public:
  // indicates a key may have any value (or not be present) as an operation on
  // it is incomplete.
  static const uint32_t UNKNOWN_SENTINEL;
  // indicates a key should definitely be deleted
  static const uint32_t DELETION_SENTINEL;

  // Errors when reading filter blocks are ignored, so we use a thread
  // local variable updated via sync points to keep track of errors injected
  // while reading filter blocks in order to ignore the Get/MultiGet result
  // for those calls
#if defined(ROCKSDB_SUPPORT_THREAD_LOCAL)
#if defined(OS_SOLARIS)
  static __thread bool ignore_read_error;
#else
  static thread_local bool ignore_read_error;
#endif // OS_SOLARIS
#else
  static bool ignore_read_error;
#endif // ROCKSDB_SUPPORT_THREAD_LOCAL

  SharedState(Env* /*env*/, StressTest* stress_test)
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
        no_overwrite_ids_(FLAGS_column_families),
        expected_state_manager_(nullptr),
        printing_verification_results_(false) {
    // Pick random keys in each column family that will not experience
    // overwrite

    fprintf(stdout, "Choosing random keys with no overwrite\n");
    Random64 rnd(seed_);
    // Start with the identity permutation. Subsequent iterations of
    // for loop below will start with perm of previous for loop
    int64_t* permutation = new int64_t[max_key_];
    for (int64_t i = 0; i < max_key_; i++) {
      permutation[i] = i;
    }
    // Now do the Knuth shuffle
    int64_t num_no_overwrite_keys = (max_key_ * FLAGS_nooverwritepercent) / 100;
    // Only need to figure out first num_no_overwrite_keys of permutation
    no_overwrite_ids_.reserve(num_no_overwrite_keys);
    for (int64_t i = 0; i < num_no_overwrite_keys; i++) {
      int64_t rand_index = i + rnd.Next() % (max_key_ - i);
      // Swap i and rand_index;
      int64_t temp = permutation[i];
      permutation[i] = permutation[rand_index];
      permutation[rand_index] = temp;
      // Fill no_overwrite_ids_ with the first num_no_overwrite_keys of
      // permutation
      no_overwrite_ids_.insert(permutation[i]);
    }
    delete[] permutation;

    Status status;
    // TODO: We should introduce a way to explicitly disable verification
    // during shutdown. When that is disabled and FLAGS_expected_values_dir
    // is empty (disabling verification at startup), we can skip tracking
    // expected state. Only then should we permit bypassing the below feature
    // compatibility checks.
    if (!FLAGS_expected_values_dir.empty()) {
      if (!std::atomic<uint32_t>{}.is_lock_free()) {
        status = Status::InvalidArgument(
            "Cannot use --expected_values_dir on platforms without lock-free "
            "std::atomic<uint32_t>");
      }
      if (status.ok() && FLAGS_clear_column_family_one_in > 0) {
        status = Status::InvalidArgument(
            "Cannot use --expected_values_dir on when "
            "--clear_column_family_one_in is greater than zero.");
      }
    }
    if (status.ok()) {
      if (FLAGS_expected_values_dir.empty()) {
        expected_state_manager_.reset(
            new AnonExpectedStateManager(FLAGS_max_key, FLAGS_column_families));
      } else {
        expected_state_manager_.reset(new FileExpectedStateManager(
            FLAGS_max_key, FLAGS_column_families, FLAGS_expected_values_dir));
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
#ifndef NDEBUG
    if (FLAGS_read_fault_one_in) {
      SyncPoint::GetInstance()->SetCallBack("FaultInjectionIgnoreError",
                                            IgnoreReadErrorCallback);
      SyncPoint::GetInstance()->EnableProcessing();
    }
#endif // NDEBUG
  }

  ~SharedState() {
#ifndef NDEBUG
    if (FLAGS_read_fault_one_in) {
      SyncPoint::GetInstance()->ClearAllCallBacks();
      SyncPoint::GetInstance()->DisableProcessing();
    }
#endif
  }

  port::Mutex* GetMutex() { return &mu_; }

  port::CondVar* GetCondVar() { return &cv_; }

  StressTest* GetStressTest() const { return stress_test_; }

  int64_t GetMaxKey() const { return max_key_; }

  uint32_t GetNumThreads() const { return num_threads_; }

  void SetThreads(int num_threads) { num_threads_ = num_threads; }

  void IncInitialized() { num_initialized_++; }

  void IncOperated() { num_populated_++; }

  void IncDone() { num_done_++; }

  void IncVotedReopen() { vote_reopen_ = (vote_reopen_ + 1) % num_threads_; }

  bool AllInitialized() const { return num_initialized_ >= num_threads_; }

  bool AllOperated() const { return num_populated_ >= num_threads_; }

  bool AllDone() const { return num_done_ >= num_threads_; }

  bool AllVotedReopen() { return (vote_reopen_ == 0); }

  void SetStart() { start_ = true; }

  void SetStartVerify() { start_verify_ = true; }

  bool Started() const { return start_; }

  bool VerifyStarted() const { return start_verify_; }

  void SetVerificationFailure() { verification_failure_.store(true); }

  bool HasVerificationFailedYet() const { return verification_failure_.load(); }

  void SetShouldStopTest() { should_stop_test_.store(true); }

  bool ShouldStopTest() const { return should_stop_test_.load(); }

  // Returns a lock covering `key` in `cf`.
  port::Mutex* GetMutexForKey(int cf, int64_t key) {
    return &key_locks_[cf][key >> log2_keys_per_lock_];
  }

  // Acquires locks for all keys in `cf`.
  void LockColumnFamily(int cf) {
    for (int i = 0; i < max_key_ >> log2_keys_per_lock_; ++i) {
      key_locks_[cf][i].Lock();
    }
  }

  // Releases locks for all keys in `cf`.
  void UnlockColumnFamily(int cf) {
    for (int i = 0; i < max_key_ >> log2_keys_per_lock_; ++i) {
      key_locks_[cf][i].Unlock();
    }
  }

  Status SaveAtAndAfter(DB* db) {
    return expected_state_manager_->SaveAtAndAfter(db);
  }

  bool HasHistory() { return expected_state_manager_->HasHistory(); }

  Status Restore(DB* db) { return expected_state_manager_->Restore(db); }

  // Requires external locking covering all keys in `cf`.
  void ClearColumnFamily(int cf) {
    return expected_state_manager_->ClearColumnFamily(cf);
  }

  // @param pending True if the update may have started but is not yet
  //    guaranteed finished. This is useful for crash-recovery testing when the
  //    process may crash before updating the expected values array.
  //
  // Requires external locking covering `key` in `cf`.
  void Put(int cf, int64_t key, uint32_t value_base, bool pending) {
    return expected_state_manager_->Put(cf, key, value_base, pending);
  }

  // Requires external locking covering `key` in `cf`.
  uint32_t Get(int cf, int64_t key) const {
    return expected_state_manager_->Get(cf, key);
  }

  // @param pending See comment above Put()
  // Returns true if the key was not yet deleted.
  //
  // Requires external locking covering `key` in `cf`.
  bool Delete(int cf, int64_t key, bool pending) {
    return expected_state_manager_->Delete(cf, key, pending);
  }

  // @param pending See comment above Put()
  // Returns true if the key was not yet deleted.
  //
  // Requires external locking covering `key` in `cf`.
  bool SingleDelete(int cf, int64_t key, bool pending) {
    return expected_state_manager_->Delete(cf, key, pending);
  }

  // @param pending See comment above Put()
  // Returns number of keys deleted by the call.
  //
  // Requires external locking covering keys in `[begin_key, end_key)` in `cf`.
  int DeleteRange(int cf, int64_t begin_key, int64_t end_key, bool pending) {
    return expected_state_manager_->DeleteRange(cf, begin_key, end_key,
                                                pending);
  }

  bool AllowsOverwrite(int64_t key) {
    return no_overwrite_ids_.find(key) == no_overwrite_ids_.end();
  }

  // Requires external locking covering `key` in `cf`.
  bool Exists(int cf, int64_t key) {
    return expected_state_manager_->Exists(cf, key);
  }

  uint32_t GetSeed() const { return seed_; }

  void SetShouldStopBgThread() { should_stop_bg_thread_ = true; }

  bool ShouldStopBgThread() { return should_stop_bg_thread_; }

  void IncBgThreads() { ++num_bg_threads_; }

  void IncBgThreadsFinished() { ++bg_thread_finished_; }

  bool BgThreadsFinished() const {
    return bg_thread_finished_ == num_bg_threads_;
  }

  bool ShouldVerifyAtBeginning() const {
    return !FLAGS_expected_values_dir.empty();
  }

  bool PrintingVerificationResults() {
    bool tmp = false;
    return !printing_verification_results_.compare_exchange_strong(
        tmp, true, std::memory_order_relaxed);
  }

  void FinishPrintingVerificationResults() {
    printing_verification_results_.store(false, std::memory_order_relaxed);
  }

 private:
  static void IgnoreReadErrorCallback(void*) {
    ignore_read_error = true;
  }

  port::Mutex mu_;
  port::CondVar cv_;
  const uint32_t seed_;
  const int64_t max_key_;
  const uint32_t log2_keys_per_lock_;
  int num_threads_;
  long num_initialized_;
  long num_populated_;
  long vote_reopen_;
  long num_done_;
  bool start_;
  bool start_verify_;
  int num_bg_threads_;
  bool should_stop_bg_thread_;
  int bg_thread_finished_;
  StressTest* stress_test_;
  std::atomic<bool> verification_failure_;
  std::atomic<bool> should_stop_test_;

  // Keys that should not be overwritten
  std::unordered_set<size_t> no_overwrite_ids_;

  std::unique_ptr<ExpectedStateManager> expected_state_manager_;
  // Cannot store `port::Mutex` directly in vector since it is not copyable
  // and storing it in the container may require copying depending on the impl.
  std::vector<std::unique_ptr<port::Mutex[]>> key_locks_;
  std::atomic<bool> printing_verification_results_;
};

// Per-thread state for concurrent executions of the same benchmark.
struct ThreadState {
  uint32_t tid;  // 0..n-1
  Random rand;   // Has different seeds for different threads
  SharedState* shared;
  Stats stats;
  struct SnapshotState {
    const Snapshot* snapshot;
    // The cf from which we did a Get at this snapshot
    int cf_at;
    // The name of the cf at the time that we did a read
    std::string cf_at_name;
    // The key with which we did a Get at this snapshot
    std::string key;
    // The status of the Get
    Status status;
    // The value of the Get
    std::string value;
    // optional state of all keys in the db
    std::vector<bool>* key_vec;

    std::string timestamp;
  };
  std::queue<std::pair<uint64_t, SnapshotState>> snapshot_queue;

  ThreadState(uint32_t index, SharedState* _shared)
      : tid(index), rand(1000 + index + _shared->GetSeed()), shared(_shared) {}
};
}  // namespace ROCKSDB_NAMESPACE
#endif  // GFLAGS

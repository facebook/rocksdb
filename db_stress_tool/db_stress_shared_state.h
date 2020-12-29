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
DECLARE_string(expected_values_path);
DECLARE_int32(clear_column_family_one_in);
DECLARE_bool(test_batches_snapshots);
DECLARE_int32(compaction_thread_pool_adjust_interval);
DECLARE_int32(continuous_verification_interval);
DECLARE_int32(read_fault_one_in);
DECLARE_int32(write_fault_one_in);

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

  SharedState(Env* env, StressTest* stress_test)
      : cv_(&mu_),
        seed_(static_cast<uint32_t>(FLAGS_seed)),
        max_key_(FLAGS_max_key),
        log2_keys_per_lock_(static_cast<uint32_t>(FLAGS_log2_keys_per_lock)),
        num_threads_(FLAGS_threads),
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
        values_(nullptr),
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

    size_t expected_values_size =
        sizeof(std::atomic<uint32_t>) * FLAGS_column_families * max_key_;
    bool values_init_needed = false;
    Status status;
    if (!FLAGS_expected_values_path.empty()) {
      if (!std::atomic<uint32_t>{}.is_lock_free()) {
        status = Status::InvalidArgument(
            "Cannot use --expected_values_path on platforms without lock-free "
            "std::atomic<uint32_t>");
      }
      if (status.ok() && FLAGS_clear_column_family_one_in > 0) {
        status = Status::InvalidArgument(
            "Cannot use --expected_values_path on when "
            "--clear_column_family_one_in is greater than zero.");
      }
      uint64_t size = 0;
      if (status.ok()) {
        status = env->GetFileSize(FLAGS_expected_values_path, &size);
      }
      std::unique_ptr<WritableFile> wfile;
      if (status.ok() && size == 0) {
        const EnvOptions soptions;
        status =
            env->NewWritableFile(FLAGS_expected_values_path, &wfile, soptions);
      }
      if (status.ok() && size == 0) {
        std::string buf(expected_values_size, '\0');
        status = wfile->Append(buf);
        values_init_needed = true;
      }
      if (status.ok()) {
        status = env->NewMemoryMappedFileBuffer(FLAGS_expected_values_path,
                                                &expected_mmap_buffer_);
      }
      if (status.ok()) {
        assert(expected_mmap_buffer_->GetLen() == expected_values_size);
        values_ = static_cast<std::atomic<uint32_t>*>(
            expected_mmap_buffer_->GetBase());
        assert(values_ != nullptr);
      } else {
        fprintf(stderr, "Failed opening shared file '%s' with error: %s\n",
                FLAGS_expected_values_path.c_str(), status.ToString().c_str());
        assert(values_ == nullptr);
      }
    }
    if (values_ == nullptr) {
      values_allocation_.reset(
          new std::atomic<uint32_t>[FLAGS_column_families * max_key_]);
      values_ = &values_allocation_[0];
      values_init_needed = true;
    }
    assert(values_ != nullptr);
    if (values_init_needed) {
      for (int i = 0; i < FLAGS_column_families; ++i) {
        for (int j = 0; j < max_key_; ++j) {
          Delete(i, j, false /* pending */);
        }
      }
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
      key_locks_[i].resize(num_locks);
      for (auto& ptr : key_locks_[i]) {
        ptr.reset(new port::Mutex);
      }
    }
    if (FLAGS_compaction_thread_pool_adjust_interval > 0) {
      ++num_bg_threads_;
      fprintf(stdout, "Starting compaction_thread_pool_adjust_thread\n");
    }
    if (FLAGS_continuous_verification_interval > 0) {
      ++num_bg_threads_;
      fprintf(stdout, "Starting continuous_verification_thread\n");
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

  port::Mutex* GetMutexForKey(int cf, int64_t key) {
    return key_locks_[cf][key >> log2_keys_per_lock_].get();
  }

  void LockColumnFamily(int cf) {
    for (auto& mutex : key_locks_[cf]) {
      mutex->Lock();
    }
  }

  void UnlockColumnFamily(int cf) {
    for (auto& mutex : key_locks_[cf]) {
      mutex->Unlock();
    }
  }

  std::atomic<uint32_t>& Value(int cf, int64_t key) const {
    return values_[cf * max_key_ + key];
  }

  void ClearColumnFamily(int cf) {
    std::fill(&Value(cf, 0 /* key */), &Value(cf + 1, 0 /* key */),
              DELETION_SENTINEL);
  }

  // @param pending True if the update may have started but is not yet
  //    guaranteed finished. This is useful for crash-recovery testing when the
  //    process may crash before updating the expected values array.
  void Put(int cf, int64_t key, uint32_t value_base, bool pending) {
    if (!pending) {
      // prevent expected-value update from reordering before Write
      std::atomic_thread_fence(std::memory_order_release);
    }
    Value(cf, key).store(pending ? UNKNOWN_SENTINEL : value_base,
                         std::memory_order_relaxed);
    if (pending) {
      // prevent Write from reordering before expected-value update
      std::atomic_thread_fence(std::memory_order_release);
    }
  }

  uint32_t Get(int cf, int64_t key) const { return Value(cf, key); }

  // @param pending See comment above Put()
  // Returns true if the key was not yet deleted.
  bool Delete(int cf, int64_t key, bool pending) {
    if (Value(cf, key) == DELETION_SENTINEL) {
      return false;
    }
    Put(cf, key, DELETION_SENTINEL, pending);
    return true;
  }

  // @param pending See comment above Put()
  // Returns true if the key was not yet deleted.
  bool SingleDelete(int cf, int64_t key, bool pending) {
    return Delete(cf, key, pending);
  }

  // @param pending See comment above Put()
  // Returns number of keys deleted by the call.
  int DeleteRange(int cf, int64_t begin_key, int64_t end_key, bool pending) {
    int covered = 0;
    for (int64_t key = begin_key; key < end_key; ++key) {
      if (Delete(cf, key, pending)) {
        ++covered;
      }
    }
    return covered;
  }

  bool AllowsOverwrite(int64_t key) {
    return no_overwrite_ids_.find(key) == no_overwrite_ids_.end();
  }

  bool Exists(int cf, int64_t key) {
    // UNKNOWN_SENTINEL counts as exists. That assures a key for which overwrite
    // is disallowed can't be accidentally added a second time, in which case
    // SingleDelete wouldn't be able to properly delete the key. It does allow
    // the case where a SingleDelete might be added which covers nothing, but
    // that's not a correctness issue.
    uint32_t expected_value = Value(cf, key).load();
    return expected_value != DELETION_SENTINEL;
  }

  uint32_t GetSeed() const { return seed_; }

  void SetShouldStopBgThread() { should_stop_bg_thread_ = true; }

  bool ShouldStopBgThread() { return should_stop_bg_thread_; }

  void IncBgThreadsFinished() { ++bg_thread_finished_; }

  bool BgThreadsFinished() const {
    return bg_thread_finished_ == num_bg_threads_;
  }

  bool ShouldVerifyAtBeginning() const {
    return expected_mmap_buffer_.get() != nullptr;
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
  const int num_threads_;
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

  std::atomic<uint32_t>* values_;
  std::unique_ptr<std::atomic<uint32_t>[]> values_allocation_;
  // Has to make it owned by a smart ptr as port::Mutex is not copyable
  // and storing it in the container may require copying depending on the impl.
  std::vector<std::vector<std::unique_ptr<port::Mutex>>> key_locks_;
  std::unique_ptr<MemoryMappedFileBuffer> expected_mmap_buffer_;
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
  };
  std::queue<std::pair<uint64_t, SnapshotState>> snapshot_queue;

  ThreadState(uint32_t index, SharedState* _shared)
      : tid(index), rand(1000 + index + _shared->GetSeed()), shared(_shared) {}
};
}  // namespace ROCKSDB_NAMESPACE
#endif  // GFLAGS

//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "rocksdb/io_status.h"
#ifdef GFLAGS
#pragma once

#include "db_stress_tool/db_stress_common.h"
#include "db_stress_tool/db_stress_shared_state.h"
#include "rocksdb/experimental.h"

namespace ROCKSDB_NAMESPACE {
class SystemClock;
class Transaction;
class TransactionDB;
class OptimisticTransactionDB;
struct TransactionDBOptions;
using experimental::SstQueryFilterConfigsManager;

class StressTest {
 public:
  StressTest();

  virtual ~StressTest() {}

  std::shared_ptr<Cache> NewCache(size_t capacity, int32_t num_shard_bits);

  static std::vector<std::string> GetBlobCompressionTags();

  bool BuildOptionsTable();

  void InitDb(SharedState*);
  // The initialization work is split into two parts to avoid a circular
  // dependency with `SharedState`.
  virtual void FinishInitDb(SharedState*);
  void TrackExpectedState(SharedState* shared);
  void OperateDb(ThreadState* thread);
  virtual void VerifyDb(ThreadState* thread) const = 0;
  virtual void ContinuouslyVerifyDb(ThreadState* /*thread*/) const = 0;
  void PrintStatistics();
  bool MightHaveUnsyncedDataLoss() {
    return FLAGS_sync_fault_injection || FLAGS_disable_wal ||
           FLAGS_manual_wal_flush_one_in > 0;
  }
  Status EnableAutoCompaction() {
    assert(options_.disable_auto_compactions);
    Status s = db_->EnableAutoCompaction(column_families_);
    return s;
  }
  void CleanUp();

 protected:
  static int GetMinInjectedErrorCount(int error_count_1, int error_count_2) {
    if (error_count_1 > 0 && error_count_2 > 0) {
      return std::min(error_count_1, error_count_2);
    } else if (error_count_1 > 0) {
      return error_count_1;
    } else if (error_count_2 > 0) {
      return error_count_2;
    } else {
      return 0;
    }
  }

  void UpdateIfInitialWriteFails(Env* db_stress_env, const Status& write_s,
                                 Status* initial_write_s,
                                 bool* initial_wal_write_may_succeed,
                                 uint64_t* wait_for_recover_start_time,
                                 bool commit_bypass_memtable = false) {
    assert(db_stress_env && initial_write_s && initial_wal_write_may_succeed &&
           wait_for_recover_start_time);
    // Only update `initial_write_s`, `initial_wal_write_may_succeed` when the
    // first write fails
    if (!write_s.ok() && (*initial_write_s).ok()) {
      *initial_write_s = write_s;
      // With commit_bypass_memtable, we create a new WAL after WAL write
      // succeeds, that wal creation may fail due to injected error. So the
      // initial wal write may succeed even if status is failed to write to wal
      *initial_wal_write_may_succeed =
          commit_bypass_memtable ||
          !FaultInjectionTestFS::IsFailedToWriteToWALError(*initial_write_s);
      *wait_for_recover_start_time = db_stress_env->NowMicros();
    }
  }

  void PrintWriteRecoveryWaitTimeIfNeeded(Env* db_stress_env,
                                          const Status& initial_write_s,
                                          bool initial_wal_write_may_succeed,
                                          uint64_t wait_for_recover_start_time,
                                          const std::string& thread_name) {
    assert(db_stress_env);
    bool waited_for_recovery = !initial_write_s.ok() &&
                               IsErrorInjectedAndRetryable(initial_write_s) &&
                               initial_wal_write_may_succeed;
    if (waited_for_recovery) {
      uint64_t elapsed_sec =
          (db_stress_env->NowMicros() - wait_for_recover_start_time) / 1000000;
      if (elapsed_sec > 10) {
        fprintf(stdout,
                "%s thread slept to wait for write recovery for "
                "%" PRIu64 " seconds\n",
                thread_name.c_str(), elapsed_sec);
      }
    }
  }
  void GetDeleteRangeKeyLocks(
      ThreadState* thread, int rand_column_family, int64_t rand_key,
      std::vector<std::unique_ptr<MutexLock>>* range_locks) {
    for (int j = 0; j < FLAGS_range_deletion_width; ++j) {
      if (j == 0 ||
          ((rand_key + j) & ((1 << FLAGS_log2_keys_per_lock) - 1)) == 0) {
        range_locks->emplace_back(new MutexLock(
            thread->shared->GetMutexForKey(rand_column_family, rand_key + j)));
      }
    }
  }

  Status AssertSame(DB* db, ColumnFamilyHandle* cf,
                    ThreadState::SnapshotState& snap_state);

  // Currently PreloadDb has to be single-threaded.
  void PreloadDbAndReopenAsReadOnly(int64_t number_of_keys,
                                    SharedState* shared);

  Status SetOptions(ThreadState* thread);

  // For transactionsDB, there can be txns prepared but not yet committeed
  // right before previous stress run crash.
  // They will be recovered and processed through
  // ProcessRecoveredPreparedTxnsHelper on the start of current stress run.
  void ProcessRecoveredPreparedTxns(SharedState* shared);

  // Default implementation will first update ExpectedState to be
  // `SharedState::UNKNOWN` for each keys in `txn` and then randomly
  // commit or rollback `txn`.
  virtual void ProcessRecoveredPreparedTxnsHelper(Transaction* txn,
                                                  SharedState* shared);

  // ExecuteTransaction is recommended instead
  // @param commit_bypass_memtable Whether commit_bypass_memtable is set to
  // true in transaction options.
  Status NewTxn(WriteOptions& write_opts, ThreadState* thread,
                std::unique_ptr<Transaction>* out_txn,
                bool* commit_bypass_memtable = nullptr);
  Status CommitTxn(Transaction& txn, ThreadState* thread = nullptr);

  // Creates a transaction, executes `ops`, and tries to commit
  // @param commit_bypass_memtable Whether commit_bypass_memtable is set to
  // true in transaction options.
  Status ExecuteTransaction(WriteOptions& write_opts, ThreadState* thread,
                            std::function<Status(Transaction&)>&& ops,
                            bool* commit_bypass_memtable = nullptr);

  virtual void MaybeClearOneColumnFamily(ThreadState* /* thread */) {}

  virtual bool ShouldAcquireMutexOnKey() const { return false; }

  // Returns true if DB state is tracked by the stress test.
  virtual bool IsStateTracked() const = 0;

  virtual std::vector<int> GenerateColumnFamilies(
      const int /* num_column_families */, int rand_column_family) const {
    return {rand_column_family};
  }

  virtual std::vector<int64_t> GenerateKeys(int64_t rand_key) const {
    return {rand_key};
  }

  virtual void TestKeyMayExist(ThreadState*, const ReadOptions&,
                               const std::vector<int>&,
                               const std::vector<int64_t>&) {}

  virtual Status TestGet(ThreadState* thread, const ReadOptions& read_opts,
                         const std::vector<int>& rand_column_families,
                         const std::vector<int64_t>& rand_keys) = 0;

  virtual std::vector<Status> TestMultiGet(
      ThreadState* thread, const ReadOptions& read_opts,
      const std::vector<int>& rand_column_families,
      const std::vector<int64_t>& rand_keys) = 0;

  virtual void TestGetEntity(ThreadState* thread, const ReadOptions& read_opts,
                             const std::vector<int>& rand_column_families,
                             const std::vector<int64_t>& rand_keys) = 0;

  virtual void TestMultiGetEntity(ThreadState* thread,
                                  const ReadOptions& read_opts,
                                  const std::vector<int>& rand_column_families,
                                  const std::vector<int64_t>& rand_keys) = 0;

  virtual Status TestPrefixScan(ThreadState* thread,
                                const ReadOptions& read_opts,
                                const std::vector<int>& rand_column_families,
                                const std::vector<int64_t>& rand_keys) = 0;

  virtual Status TestPut(ThreadState* thread, WriteOptions& write_opts,
                         const ReadOptions& read_opts,
                         const std::vector<int>& cf_ids,
                         const std::vector<int64_t>& keys,
                         char (&value)[100]) = 0;

  virtual Status TestDelete(ThreadState* thread, WriteOptions& write_opts,
                            const std::vector<int>& rand_column_families,
                            const std::vector<int64_t>& rand_keys) = 0;

  virtual Status TestDeleteRange(ThreadState* thread, WriteOptions& write_opts,
                                 const std::vector<int>& rand_column_families,
                                 const std::vector<int64_t>& rand_keys) = 0;

  virtual void TestIngestExternalFile(
      ThreadState* thread, const std::vector<int>& rand_column_families,
      const std::vector<int64_t>& rand_keys) = 0;

  // Issue compact range, starting with start_key, whose integer value
  // is rand_key.
  virtual void TestCompactRange(ThreadState* thread, int64_t rand_key,
                                const Slice& start_key,
                                ColumnFamilyHandle* column_family);

  virtual void TestPromoteL0(ThreadState* thread,
                             ColumnFamilyHandle* column_family);

  // Calculate a hash value for all keys in range [start_key, end_key]
  // at a certain snapshot.
  uint32_t GetRangeHash(ThreadState* thread, const Snapshot* snapshot,
                        ColumnFamilyHandle* column_family,
                        const Slice& start_key, const Slice& end_key);

  // Return a column family handle that mirrors what is pointed by
  // `column_family_id`, which will be used to validate data to be correct.
  // By default, the column family itself will be returned.
  virtual ColumnFamilyHandle* GetControlCfh(ThreadState* /* thread*/,
                                            int column_family_id) {
    return column_families_[column_family_id];
  }

  // Generated a list of keys that close to boundaries of SST keys.
  // If there isn't any SST file in the DB, return empty list.
  std::vector<std::string> GetWhiteBoxKeys(ThreadState* thread, DB* db,
                                           ColumnFamilyHandle* cfh,
                                           size_t num_keys);

  // Given a key K, this creates an iterator which scans to K and then
  // does a random sequence of Next/Prev operations.
  virtual Status TestIterate(ThreadState* thread, const ReadOptions& read_opts,
                             const std::vector<int>& rand_column_families,
                             const std::vector<int64_t>& rand_keys);

  // Given a key K, this creates an attribute group iterator which scans to K
  // and then does a random sequence of Next/Prev operations. Called only when
  // use_attribute_group=1
  virtual Status TestIterateAttributeGroups(
      ThreadState* thread, const ReadOptions& read_opts,
      const std::vector<int>& rand_column_families,
      const std::vector<int64_t>& rand_keys);

  template <typename IterType, typename NewIterFunc, typename VerifyFunc>
  Status TestIterateImpl(ThreadState* thread, const ReadOptions& read_opts,
                         const std::vector<int>& rand_column_families,
                         const std::vector<int64_t>& rand_keys,
                         NewIterFunc new_iter_func, VerifyFunc verify_func);

  virtual Status TestIterateAgainstExpected(
      ThreadState* /* thread */, const ReadOptions& /* read_opts */,
      const std::vector<int>& /* rand_column_families */,
      const std::vector<int64_t>& /* rand_keys */) {
    return Status::NotSupported();
  }

  // Enum used by VerifyIterator() to identify the mode to validate.
  enum LastIterateOp {
    kLastOpSeek,
    kLastOpSeekForPrev,
    kLastOpNextOrPrev,
    kLastOpSeekToFirst,
    kLastOpSeekToLast
  };

  // Compare the two iterator, iter and cmp_iter are in the same position,
  // unless iter might be made invalidate or undefined because of
  // upper or lower bounds, or prefix extractor.
  // Will flag failure if the verification fails.
  // diverged = true if the two iterator is already diverged.
  // True if verification passed, false if not.
  // op_logs is the information to print when validation fails.
  template <typename IterType, typename VerifyFuncType>
  void VerifyIterator(ThreadState* thread, ColumnFamilyHandle* cmp_cfh,
                      const ReadOptions& ro, IterType* iter, Iterator* cmp_iter,
                      LastIterateOp op, const Slice& seek_key,
                      const std::string& op_logs, VerifyFuncType verifyFunc,
                      bool* diverged);

  virtual Status TestBackupRestore(ThreadState* thread,
                                   const std::vector<int>& rand_column_families,
                                   const std::vector<int64_t>& rand_keys);

  virtual Status PrepareOptionsForRestoredDB(Options* options);

  virtual Status TestCheckpoint(ThreadState* thread,
                                const std::vector<int>& rand_column_families,
                                const std::vector<int64_t>& rand_keys);

  void TestCompactFiles(ThreadState* thread, ColumnFamilyHandle* column_family);

  Status TestFlush(const std::vector<int>& rand_column_families);

  Status TestResetStats();

  Status TestPauseBackground(ThreadState* thread);

  Status TestDisableFileDeletions(ThreadState* thread);

  Status TestDisableManualCompaction(ThreadState* thread);

  void TestAcquireSnapshot(ThreadState* thread, int rand_column_family,
                           const std::string& keystr, uint64_t i);

  Status MaybeReleaseSnapshots(ThreadState* thread, uint64_t i);

  Status TestGetLiveFiles() const;
  Status TestGetLiveFilesMetaData() const;
  Status TestGetLiveFilesStorageInfo() const;
  Status TestGetAllColumnFamilyMetaData() const;

  Status TestGetSortedWalFiles() const;
  Status TestGetCurrentWalFile() const;
  void TestGetProperty(ThreadState* thread) const;
  Status TestGetPropertiesOfAllTables() const;

  virtual Status TestApproximateSize(
      ThreadState* thread, uint64_t iteration,
      const std::vector<int>& rand_column_families,
      const std::vector<int64_t>& rand_keys);

  virtual Status TestCustomOperations(
      ThreadState* /*thread*/,
      const std::vector<int>& /*rand_column_families*/) {
    return Status::NotSupported("TestCustomOperations() must be overridden");
  }

  bool IsErrorInjectedAndRetryable(const Status& error_s) const {
    assert(!error_s.ok());
    return error_s.getState() &&
           FaultInjectionTestFS::IsInjectedError(error_s) &&
           !status_to_io_status(Status(error_s)).GetDataLoss();
  }

  void ProcessStatus(SharedState* shared, std::string msg, const Status& s,
                     bool ignore_injected_error = true) const;

  void VerificationAbort(SharedState* shared, std::string msg) const;

  void VerificationAbort(SharedState* shared, std::string msg, int cf,
                         int64_t key) const;

  void VerificationAbort(SharedState* shared, std::string msg, int cf,
                         int64_t key, Slice value_from_db,
                         Slice value_from_expected) const;

  void VerificationAbort(SharedState* shared, int cf, int64_t key,
                         const Slice& value, const WideColumns& columns) const;

  static std::string DebugString(const Slice& value,
                                 const WideColumns& columns);

  void PrintEnv() const;

  void Open(SharedState* shared, bool reopen = false);

  void Reopen(ThreadState* thread);

  virtual void RegisterAdditionalListeners() {}

  virtual void PrepareTxnDbOptions(SharedState* /*shared*/,
                                   TransactionDBOptions& /*txn_db_opts*/) {}

  // Returns whether the timestamp of read_opts is updated.
  bool MaybeUseOlderTimestampForPointLookup(ThreadState* thread,
                                            std::string& ts_str,
                                            Slice& ts_slice,
                                            ReadOptions& read_opts);

  void MaybeUseOlderTimestampForRangeScan(ThreadState* thread,
                                          std::string& ts_str, Slice& ts_slice,
                                          ReadOptions& read_opts);

  void CleanUpColumnFamilies();

  std::shared_ptr<Cache> cache_;
  std::shared_ptr<Cache> compressed_cache_;
  std::shared_ptr<const FilterPolicy> filter_policy_;
  DB* db_;
  TransactionDB* txn_db_;
  OptimisticTransactionDB* optimistic_txn_db_;

  // Currently only used in MultiOpsTxnsStressTest
  std::atomic<DB*> db_aptr_;

  Options options_;
  SystemClock* clock_;
  std::vector<ColumnFamilyHandle*> column_families_;
  std::vector<std::string> column_family_names_;
  std::atomic<int> new_column_family_name_;
  int num_times_reopened_;
  std::unordered_map<std::string, std::vector<std::string>> options_table_;
  std::vector<std::string> options_index_;
  std::atomic<bool> db_preload_finished_;
  std::shared_ptr<SstQueryFilterConfigsManager::Factory> sqfc_factory_;

  DB* secondary_db_;
  std::vector<ColumnFamilyHandle*> secondary_cfhs_;
  bool is_db_stopped_;
};

// Load options from OPTIONS file and populate `options`.
bool InitializeOptionsFromFile(Options& options);

// Initialize `options` using command line arguments.
// When this function is called, `cache`, `block_cache_compressed`,
// `filter_policy` have all been initialized. Therefore, we just pass them as
// input arguments.
void InitializeOptionsFromFlags(
    const std::shared_ptr<Cache>& cache,
    const std::shared_ptr<const FilterPolicy>& filter_policy, Options& options);

// Initialize `options` on which `InitializeOptionsFromFile()` and
// `InitializeOptionsFromFlags()` have both been called already.
// There are two cases.
// Case 1: OPTIONS file is not specified. Command line arguments have been used
//         to initialize `options`. InitializeOptionsGeneral() will use
//         `cache` and `filter_policy` to initialize
//         corresponding fields of `options`. InitializeOptionsGeneral() will
//         also set up other fields of `options` so that stress test can run.
//         Examples include `create_if_missing` and
//         `create_missing_column_families`, etc.
// Case 2: OPTIONS file is specified. It is possible that, after loading from
//         the given OPTIONS files, some shared object fields are still not
//         initialized because they are not set in the OPTIONS file. In this
//         case, if command line arguments indicate that the user wants to set
//         up such shared objects, e.g. block cache, compressed block cache,
//         row cache, filter policy, then InitializeOptionsGeneral() will honor
//         the user's choice, thus passing `cache`,
//         `filter_policy` as input arguments.
//
// InitializeOptionsGeneral() must not overwrite fields of `options` loaded
// from OPTIONS file.
void InitializeOptionsGeneral(
    const std::shared_ptr<Cache>& cache,
    const std::shared_ptr<const FilterPolicy>& filter_policy,
    const std::shared_ptr<SstQueryFilterConfigsManager::Factory>& sqfc_factory,
    Options& options);

// If no OPTIONS file is specified, set up `options` so that we can test
// user-defined timestamp which requires `-user_timestamp_size=8`.
// This function also checks for known (currently) incompatible features with
// user-defined timestamp.
void CheckAndSetOptionsForUserTimestamp(Options& options);

bool ShouldDisableAutoCompactionsBeforeVerifyDb();
}  // namespace ROCKSDB_NAMESPACE
#endif  // GFLAGS

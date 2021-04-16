//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifdef GFLAGS
#pragma once
#include "db_stress_tool/db_stress_common.h"
#include "db_stress_tool/db_stress_shared_state.h"

namespace ROCKSDB_NAMESPACE {
class SystemClock;
class Transaction;
class TransactionDB;

class StressTest {
 public:
  StressTest();

  virtual ~StressTest();

  std::shared_ptr<Cache> NewCache(size_t capacity);

  static std::vector<std::string> GetBlobCompressionTags();

  bool BuildOptionsTable();

  void InitDb();
  // The initialization work is split into two parts to avoid a circular
  // dependency with `SharedState`.
  void FinishInitDb(SharedState*);

  // Return false if verification fails.
  bool VerifySecondaries();

  void OperateDb(ThreadState* thread);
  virtual void VerifyDb(ThreadState* thread) const = 0;
  virtual void ContinuouslyVerifyDb(ThreadState* /*thread*/) const {}

  void PrintStatistics();

 protected:
  Status AssertSame(DB* db, ColumnFamilyHandle* cf,
                    ThreadState::SnapshotState& snap_state);

  // Currently PreloadDb has to be single-threaded.
  void PreloadDbAndReopenAsReadOnly(int64_t number_of_keys,
                                    SharedState* shared);

  Status SetOptions(ThreadState* thread);

#ifndef ROCKSDB_LITE
  Status NewTxn(WriteOptions& write_opts, Transaction** txn);

  Status CommitTxn(Transaction* txn);

  Status RollbackTxn(Transaction* txn);
#endif

  virtual void MaybeClearOneColumnFamily(ThreadState* /* thread */) {}

  virtual bool ShouldAcquireMutexOnKey() const { return false; }

  virtual std::vector<int> GenerateColumnFamilies(
      const int /* num_column_families */, int rand_column_family) const {
    return {rand_column_family};
  }

  virtual std::vector<int64_t> GenerateKeys(int64_t rand_key) const {
    return {rand_key};
  }

  virtual Status TestGet(ThreadState* thread, const ReadOptions& read_opts,
                         const std::vector<int>& rand_column_families,
                         const std::vector<int64_t>& rand_keys) = 0;

  virtual std::vector<Status> TestMultiGet(
      ThreadState* thread, const ReadOptions& read_opts,
      const std::vector<int>& rand_column_families,
      const std::vector<int64_t>& rand_keys) = 0;

  virtual Status TestPrefixScan(ThreadState* thread,
                                const ReadOptions& read_opts,
                                const std::vector<int>& rand_column_families,
                                const std::vector<int64_t>& rand_keys) = 0;

  virtual Status TestPut(ThreadState* thread, WriteOptions& write_opts,
                         const ReadOptions& read_opts,
                         const std::vector<int>& cf_ids,
                         const std::vector<int64_t>& keys, char (&value)[100],
                         std::unique_ptr<MutexLock>& lock) = 0;

  virtual Status TestDelete(ThreadState* thread, WriteOptions& write_opts,
                            const std::vector<int>& rand_column_families,
                            const std::vector<int64_t>& rand_keys,
                            std::unique_ptr<MutexLock>& lock) = 0;

  virtual Status TestDeleteRange(ThreadState* thread, WriteOptions& write_opts,
                                 const std::vector<int>& rand_column_families,
                                 const std::vector<int64_t>& rand_keys,
                                 std::unique_ptr<MutexLock>& lock) = 0;

  virtual void TestIngestExternalFile(
      ThreadState* thread, const std::vector<int>& rand_column_families,
      const std::vector<int64_t>& rand_keys,
      std::unique_ptr<MutexLock>& lock) = 0;

  // Issue compact range, starting with start_key, whose integer value
  // is rand_key.
  virtual void TestCompactRange(ThreadState* thread, int64_t rand_key,
                                const Slice& start_key,
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

#ifndef ROCKSDB_LITE
  // Generated a list of keys that close to boundaries of SST keys.
  // If there isn't any SST file in the DB, return empty list.
  std::vector<std::string> GetWhiteBoxKeys(ThreadState* thread, DB* db,
                                           ColumnFamilyHandle* cfh,
                                           size_t num_keys);
#else   // !ROCKSDB_LITE
  std::vector<std::string> GetWhiteBoxKeys(ThreadState*, DB*,
                                           ColumnFamilyHandle*, size_t) {
    // Not supported in LITE mode.
    return {};
  }
#endif  // !ROCKSDB_LITE

  // Given a key K, this creates an iterator which scans to K and then
  // does a random sequence of Next/Prev operations.
  virtual Status TestIterate(ThreadState* thread, const ReadOptions& read_opts,
                             const std::vector<int>& rand_column_families,
                             const std::vector<int64_t>& rand_keys);

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
  void VerifyIterator(ThreadState* thread, ColumnFamilyHandle* cmp_cfh,
                      const ReadOptions& ro, Iterator* iter, Iterator* cmp_iter,
                      LastIterateOp op, const Slice& seek_key,
                      const std::string& op_logs, bool* diverged);

  virtual Status TestBackupRestore(ThreadState* thread,
                                   const std::vector<int>& rand_column_families,
                                   const std::vector<int64_t>& rand_keys);

  virtual Status TestCheckpoint(ThreadState* thread,
                                const std::vector<int>& rand_column_families,
                                const std::vector<int64_t>& rand_keys);

  void TestCompactFiles(ThreadState* thread, ColumnFamilyHandle* column_family);

  Status TestFlush(const std::vector<int>& rand_column_families);

  Status TestPauseBackground(ThreadState* thread);

  void TestAcquireSnapshot(ThreadState* thread, int rand_column_family,
                           const std::string& keystr, uint64_t i);

  Status MaybeReleaseSnapshots(ThreadState* thread, uint64_t i);
#ifndef ROCKSDB_LITE
  Status VerifyGetLiveFiles() const;
  Status VerifyGetSortedWalFiles() const;
  Status VerifyGetCurrentWalFile() const;
  void TestGetProperty(ThreadState* thread) const;

  virtual Status TestApproximateSize(
      ThreadState* thread, uint64_t iteration,
      const std::vector<int>& rand_column_families,
      const std::vector<int64_t>& rand_keys);
#endif  // !ROCKSDB_LITE

  void VerificationAbort(SharedState* shared, std::string msg, Status s) const;

  void VerificationAbort(SharedState* shared, std::string msg, int cf,
                         int64_t key) const;

  void PrintEnv() const;

  void Open();

  void Reopen(ThreadState* thread);

  void CheckAndSetOptionsForUserTimestamp();

  std::shared_ptr<Cache> cache_;
  std::shared_ptr<Cache> compressed_cache_;
  std::shared_ptr<const FilterPolicy> filter_policy_;
  DB* db_;
#ifndef ROCKSDB_LITE
  TransactionDB* txn_db_;
#endif
  Options options_;
  SystemClock* clock_;
  std::vector<ColumnFamilyHandle*> column_families_;
  std::vector<std::string> column_family_names_;
  std::atomic<int> new_column_family_name_;
  int num_times_reopened_;
  std::unordered_map<std::string, std::vector<std::string>> options_table_;
  std::vector<std::string> options_index_;
  std::atomic<bool> db_preload_finished_;

  // Fields used for stress-testing secondary instance in the same process
  std::vector<DB*> secondaries_;
  std::vector<std::vector<ColumnFamilyHandle*>> secondary_cfh_lists_;

  // Fields used for continuous verification from another thread
  DB* cmp_db_;
  std::vector<ColumnFamilyHandle*> cmp_cfhs_;
};

}  // namespace ROCKSDB_NAMESPACE
#endif  // GFLAGS

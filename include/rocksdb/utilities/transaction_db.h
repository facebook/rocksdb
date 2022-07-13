//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once
#ifndef ROCKSDB_LITE

#include <string>
#include <utility>
#include <vector>

#include "rocksdb/comparator.h"
#include "rocksdb/db.h"
#include "rocksdb/utilities/stackable_db.h"
#include "rocksdb/utilities/transaction.h"

// Database with Transaction support.
//
// See transaction.h and examples/transaction_example.cc

namespace ROCKSDB_NAMESPACE {

class TransactionDBMutexFactory;

enum TxnDBWritePolicy {
  WRITE_COMMITTED = 0,  // write only the committed data
  WRITE_PREPARED,  // write data after the prepare phase of 2pc
  WRITE_UNPREPARED  // write data before the prepare phase of 2pc
};

constexpr uint32_t kInitialMaxDeadlocks = 5;

class LockManager;
struct RangeLockInfo;

// A lock manager handle
// The workflow is as follows:
//  * Use a factory method (like NewRangeLockManager()) to create a lock
//    manager and get its handle.
//  * A Handle for a particular kind of lock manager will have extra
//    methods and parameters to control the lock manager
//  * Pass the handle to RocksDB in TransactionDBOptions::lock_mgr_handle. It
//    will be used to perform locking.
class LockManagerHandle {
 public:
  // PessimisticTransactionDB will call this to get the Lock Manager it's going
  // to use.
  virtual LockManager* getLockManager() = 0;

  virtual ~LockManagerHandle() {}
};

// Same as class Endpoint, but use std::string to manage the buffer allocation
struct EndpointWithString {
  std::string slice;
  bool inf_suffix;
};

struct RangeDeadlockInfo {
  TransactionID m_txn_id;
  uint32_t m_cf_id;
  bool m_exclusive;

  EndpointWithString m_start;
  EndpointWithString m_end;
};

struct RangeDeadlockPath {
  std::vector<RangeDeadlockInfo> path;
  bool limit_exceeded;
  int64_t deadlock_time;

  explicit RangeDeadlockPath(std::vector<RangeDeadlockInfo> path_entry,
                             const int64_t& dl_time)
      : path(path_entry), limit_exceeded(false), deadlock_time(dl_time) {}

  // empty path, limit exceeded constructor and default constructor
  explicit RangeDeadlockPath(const int64_t& dl_time = 0, bool limit = false)
      : path(0), limit_exceeded(limit), deadlock_time(dl_time) {}

  bool empty() { return path.empty() && !limit_exceeded; }
};

// A handle to control RangeLockManager (Range-based lock manager) from outside
// RocksDB
class RangeLockManagerHandle : public LockManagerHandle {
 public:
  // Set total amount of lock memory to use.
  //
  //  @return 0 Ok
  //  @return EDOM Failed to set because currently using more memory than
  //        specified
  virtual int SetMaxLockMemory(size_t max_lock_memory) = 0;
  virtual size_t GetMaxLockMemory() = 0;

  using RangeLockStatus =
      std::unordered_multimap<ColumnFamilyId, RangeLockInfo>;

  // Lock Escalation barrier check function.
  // It is called for a couple of endpoints A and B, such that A < B.
  // If escalation_barrier_check_func(A, B)==true, then there's a lock
  // escalation barrier between A and B, and lock escalation is not allowed
  // to bridge the gap between A and B.
  //
  // The function may be called from any thread that acquires or releases
  // locks. It should not throw exceptions. There is currently no way to return
  // an error.
  using EscalationBarrierFunc =
      std::function<bool(const Endpoint& a, const Endpoint& b)>;

  // Set the user-provided barrier check function
  virtual void SetEscalationBarrierFunc(EscalationBarrierFunc func) = 0;

  virtual RangeLockStatus GetRangeLockStatusData() = 0;

  class Counters {
   public:
    // Number of times lock escalation was triggered (for all column families)
    uint64_t escalation_count;

    // Number of times lock acquisition had to wait for a conflicting lock
    // to be released. This counts both successful waits (where the desired
    // lock was acquired) and waits that timed out or got other error.
    uint64_t lock_wait_count;

    // How much memory is currently used for locks (total for all column
    // families)
    uint64_t current_lock_memory;
  };

  // Get the current counter values
  virtual Counters GetStatus() = 0;

  // Functions for range-based Deadlock reporting.
  virtual std::vector<RangeDeadlockPath> GetRangeDeadlockInfoBuffer() = 0;
  virtual void SetRangeDeadlockInfoBufferSize(uint32_t target_size) = 0;

  virtual ~RangeLockManagerHandle() {}
};

// A factory function to create a Range Lock Manager. The created object should
// be:
//  1. Passed in TransactionDBOptions::lock_mgr_handle to open the database in
//     range-locking mode
//  2. Used to control the lock manager when the DB is already open.
RangeLockManagerHandle* NewRangeLockManager(
    std::shared_ptr<TransactionDBMutexFactory> mutex_factory);

struct TransactionDBOptions {
  // Specifies the maximum number of keys that can be locked at the same time
  // per column family.
  // If the number of locked keys is greater than max_num_locks, transaction
  // writes (or GetForUpdate) will return an error.
  // If this value is not positive, no limit will be enforced.
  int64_t max_num_locks = -1;

  // Stores the number of latest deadlocks to track
  uint32_t max_num_deadlocks = kInitialMaxDeadlocks;

  // Increasing this value will increase the concurrency by dividing the lock
  // table (per column family) into more sub-tables, each with their own
  // separate mutex.
  size_t num_stripes = 16;

  // If positive, specifies the default wait timeout in milliseconds when
  // a transaction attempts to lock a key if not specified by
  // TransactionOptions::lock_timeout.
  //
  // If 0, no waiting is done if a lock cannot instantly be acquired.
  // If negative, there is no timeout.  Not using a timeout is not recommended
  // as it can lead to deadlocks.  Currently, there is no deadlock-detection to
  // recover from a deadlock.
  int64_t transaction_lock_timeout = 1000;  // 1 second

  // If positive, specifies the wait timeout in milliseconds when writing a key
  // OUTSIDE of a transaction (ie by calling DB::Put(),Merge(),Delete(),Write()
  // directly).
  // If 0, no waiting is done if a lock cannot instantly be acquired.
  // If negative, there is no timeout and will block indefinitely when acquiring
  // a lock.
  //
  // Not using a timeout can lead to deadlocks.  Currently, there
  // is no deadlock-detection to recover from a deadlock.  While DB writes
  // cannot deadlock with other DB writes, they can deadlock with a transaction.
  // A negative timeout should only be used if all transactions have a small
  // expiration set.
  int64_t default_lock_timeout = 1000;  // 1 second

  // If set, the TransactionDB will use this implementation of a mutex and
  // condition variable for all transaction locking instead of the default
  // mutex/condvar implementation.
  std::shared_ptr<TransactionDBMutexFactory> custom_mutex_factory;

  // The policy for when to write the data into the DB. The default policy is to
  // write only the committed data (WRITE_COMMITTED). The data could be written
  // before the commit phase. The DB then needs to provide the mechanisms to
  // tell apart committed from uncommitted data.
  TxnDBWritePolicy write_policy = TxnDBWritePolicy::WRITE_COMMITTED;

  // TODO(myabandeh): remove this option
  // Note: this is a temporary option as a hot fix in rollback of writeprepared
  // txns in myrocks. MyRocks uses merge operands for autoinc column id without
  // however obtaining locks. This breaks the assumption behind the rollback
  // logic in myrocks. This hack of simply not rolling back merge operands works
  // for the special way that myrocks uses this operands.
  bool rollback_merge_operands = false;

  // nullptr means use default lock manager.
  // Other value means the user provides a custom lock manager.
  std::shared_ptr<LockManagerHandle> lock_mgr_handle;

  // If true, the TransactionDB implementation might skip concurrency control
  // unless it is overridden by TransactionOptions or
  // TransactionDBWriteOptimizations. This can be used in conjunction with
  // DBOptions::unordered_write when the TransactionDB is used solely for write
  // ordering rather than concurrency control.
  bool skip_concurrency_control = false;

  // This option is only valid for write unprepared. If a write batch exceeds
  // this threshold, then the transaction will implicitly flush the currently
  // pending writes into the database. A value of 0 or less means no limit.
  int64_t default_write_batch_flush_threshold = 0;

  // This option is valid only for write-prepared/write-unprepared. Transaction
  // will rely on this callback to determine if a key should be rolled back
  // with Delete or SingleDelete when necessary. If the callback returns true,
  // then SingleDelete should be used. If the callback is not callable or the
  // callback returns false, then a Delete is used.
  // The application should ensure thread-safety of this callback.
  // The callback should not throw because RocksDB is not exception-safe.
  // The callback may be removed if we allow mixing Delete and SingleDelete in
  // the future.
  std::function<bool(TransactionDB* /*db*/,
                     ColumnFamilyHandle* /*column_family*/,
                     const Slice& /*key*/)>
      rollback_deletion_type_callback;

 private:
  // 128 entries
  // Should the default value change, please also update wp_snapshot_cache_bits
  // in db_stress_gflags.cc
  size_t wp_snapshot_cache_bits = static_cast<size_t>(7);
  // 8m entry, 64MB size
  // Should the default value change, please also update wp_commit_cache_bits
  // in db_stress_gflags.cc
  size_t wp_commit_cache_bits = static_cast<size_t>(23);

  // For testing, whether transaction name should be auto-generated or not. This
  // is useful for write unprepared which requires named transactions.
  bool autogenerate_name = false;

  friend class WritePreparedTxnDB;
  friend class WriteUnpreparedTxn;
  friend class WritePreparedTransactionTestBase;
  friend class TransactionTestBase;
  friend class MySQLStyleTransactionTest;
  friend class StressTest;
};

struct TransactionOptions {
  // Setting set_snapshot=true is the same as calling
  // Transaction::SetSnapshot().
  bool set_snapshot = false;

  // Setting to true means that before acquiring locks, this transaction will
  // check if doing so will cause a deadlock. If so, it will return with
  // Status::Busy.  The user should retry their transaction.
  bool deadlock_detect = false;

  // If set, it states that the CommitTimeWriteBatch represents the latest state
  // of the application, has only one sub-batch, i.e., no duplicate keys,  and
  // meant to be used later during recovery. It enables an optimization to
  // postpone updating the memtable with CommitTimeWriteBatch to only
  // SwitchMemtable or recovery.
  // This option does not affect write-committed. Only
  // write-prepared/write-unprepared transactions will be affected.
  bool use_only_the_last_commit_time_batch_for_recovery = false;

  // TODO(agiardullo): TransactionDB does not yet support comparators that allow
  // two non-equal keys to be equivalent.  Ie, cmp->Compare(a,b) should only
  // return 0 if
  // a.compare(b) returns 0.

  // If positive, specifies the wait timeout in milliseconds when
  // a transaction attempts to lock a key.
  //
  // If 0, no waiting is done if a lock cannot instantly be acquired.
  // If negative, TransactionDBOptions::transaction_lock_timeout will be used.
  int64_t lock_timeout = -1;

  // Expiration duration in milliseconds.  If non-negative, transactions that
  // last longer than this many milliseconds will fail to commit.  If not set,
  // a forgotten transaction that is never committed, rolled back, or deleted
  // will never relinquish any locks it holds.  This could prevent keys from
  // being written by other writers.
  int64_t expiration = -1;

  // The number of traversals to make during deadlock detection.
  int64_t deadlock_detect_depth = 50;

  // The maximum number of bytes used for the write batch. 0 means no limit.
  size_t max_write_batch_size = 0;

  // Skip Concurrency Control. This could be as an optimization if the
  // application knows that the transaction would not have any conflict with
  // concurrent transactions. It could also be used during recovery if (i)
  // application guarantees no conflict between prepared transactions in the WAL
  // (ii) application guarantees that recovered transactions will be rolled
  // back/commit before new transactions start.
  // Default: false
  bool skip_concurrency_control = false;

  // In pessimistic transaction, if this is true, then you can skip Prepare
  // before Commit, otherwise, you must Prepare before Commit.
  bool skip_prepare = true;

  // See TransactionDBOptions::default_write_batch_flush_threshold for
  // description. If a negative value is specified, then the default value from
  // TransactionDBOptions is used.
  int64_t write_batch_flush_threshold = -1;
};

// The per-write optimizations that do not involve transactions. TransactionDB
// implementation might or might not make use of the specified optimizations.
struct TransactionDBWriteOptimizations {
  // If it is true it means that the application guarantees that the
  // key-set in the write batch do not conflict with any concurrent transaction
  // and hence the concurrency control mechanism could be skipped for this
  // write.
  bool skip_concurrency_control = false;
  // If true, the application guarantees that there is no duplicate <column
  // family, key> in the write batch and any employed mechanism to handle
  // duplicate keys could be skipped.
  bool skip_duplicate_key_check = false;
};

struct KeyLockInfo {
  std::string key;
  std::vector<TransactionID> ids;
  bool exclusive;
};

struct RangeLockInfo {
  EndpointWithString start;
  EndpointWithString end;
  std::vector<TransactionID> ids;
  bool exclusive;
};

struct DeadlockInfo {
  TransactionID m_txn_id;
  uint32_t m_cf_id;
  bool m_exclusive;
  std::string m_waiting_key;
};

struct DeadlockPath {
  std::vector<DeadlockInfo> path;
  bool limit_exceeded;
  int64_t deadlock_time;

  explicit DeadlockPath(std::vector<DeadlockInfo> path_entry,
                        const int64_t& dl_time)
      : path(path_entry), limit_exceeded(false), deadlock_time(dl_time) {}

  // empty path, limit exceeded constructor and default constructor
  explicit DeadlockPath(const int64_t& dl_time = 0, bool limit = false)
      : path(0), limit_exceeded(limit), deadlock_time(dl_time) {}

  bool empty() { return path.empty() && !limit_exceeded; }
};

class TransactionDB : public StackableDB {
 public:
  // Optimized version of ::Write that receives more optimization request such
  // as skip_concurrency_control.
  using StackableDB::Write;
  virtual Status Write(const WriteOptions& opts,
                       const TransactionDBWriteOptimizations&,
                       WriteBatch* updates) {
    // The default implementation ignores TransactionDBWriteOptimizations and
    // falls back to the un-optimized version of ::Write
    return Write(opts, updates);
  }
  // Transactional `DeleteRange()` is not yet supported.
  // However, users who know their deleted range does not conflict with
  // anything can still use it via the `Write()` API. In all cases, the
  // `Write()` overload specifying `TransactionDBWriteOptimizations` must be
  // used and `skip_concurrency_control` must be set. When using either
  // WRITE_PREPARED or WRITE_UNPREPARED , `skip_duplicate_key_check` must
  // additionally be set.
  using StackableDB::DeleteRange;
  virtual Status DeleteRange(const WriteOptions&, ColumnFamilyHandle*,
                             const Slice&, const Slice&) override {
    return Status::NotSupported();
  }
  // Open a TransactionDB similar to DB::Open().
  // Internally call PrepareWrap() and WrapDB()
  // If the return status is not ok, then dbptr is set to nullptr.
  static Status Open(const Options& options,
                     const TransactionDBOptions& txn_db_options,
                     const std::string& dbname, TransactionDB** dbptr);

  static Status Open(const DBOptions& db_options,
                     const TransactionDBOptions& txn_db_options,
                     const std::string& dbname,
                     const std::vector<ColumnFamilyDescriptor>& column_families,
                     std::vector<ColumnFamilyHandle*>* handles,
                     TransactionDB** dbptr);
  // Note: PrepareWrap() may change parameters, make copies before the
  // invocation if needed.
  static void PrepareWrap(DBOptions* db_options,
                          std::vector<ColumnFamilyDescriptor>* column_families,
                          std::vector<size_t>* compaction_enabled_cf_indices);
  // If the return status is not ok, then dbptr will bet set to nullptr. The
  // input db parameter might or might not be deleted as a result of the
  // failure. If it is properly deleted it will be set to nullptr. If the return
  // status is ok, the ownership of db is transferred to dbptr.
  static Status WrapDB(DB* db, const TransactionDBOptions& txn_db_options,
                       const std::vector<size_t>& compaction_enabled_cf_indices,
                       const std::vector<ColumnFamilyHandle*>& handles,
                       TransactionDB** dbptr);
  // If the return status is not ok, then dbptr will bet set to nullptr. The
  // input db parameter might or might not be deleted as a result of the
  // failure. If it is properly deleted it will be set to nullptr. If the return
  // status is ok, the ownership of db is transferred to dbptr.
  static Status WrapStackableDB(
      StackableDB* db, const TransactionDBOptions& txn_db_options,
      const std::vector<size_t>& compaction_enabled_cf_indices,
      const std::vector<ColumnFamilyHandle*>& handles, TransactionDB** dbptr);
  // Since the destructor in StackableDB is virtual, this destructor is virtual
  // too. The root db will be deleted by the base's destructor.
  ~TransactionDB() override {}

  // Starts a new Transaction.
  //
  // Caller is responsible for deleting the returned transaction when no
  // longer needed.
  //
  // If old_txn is not null, BeginTransaction will reuse this Transaction
  // handle instead of allocating a new one.  This is an optimization to avoid
  // extra allocations when repeatedly creating transactions.
  virtual Transaction* BeginTransaction(
      const WriteOptions& write_options,
      const TransactionOptions& txn_options = TransactionOptions(),
      Transaction* old_txn = nullptr) = 0;

  virtual Transaction* GetTransactionByName(const TransactionName& name) = 0;
  virtual void GetAllPreparedTransactions(std::vector<Transaction*>* trans) = 0;

  // Returns set of all locks held.
  //
  // The mapping is column family id -> KeyLockInfo
  virtual std::unordered_multimap<uint32_t, KeyLockInfo>
  GetLockStatusData() = 0;

  virtual std::vector<DeadlockPath> GetDeadlockInfoBuffer() = 0;
  virtual void SetDeadlockInfoBufferSize(uint32_t target_size) = 0;

  // Create a snapshot and assign ts to it. Return the snapshot to caller. The
  // snapshot-timestamp mapping is also tracked by the database.
  // Caller must ensure there are no active writes when this API is called.
  virtual std::pair<Status, std::shared_ptr<const Snapshot>>
  CreateTimestampedSnapshot(TxnTimestamp ts) = 0;

  // Return the latest timestamped snapshot if present.
  std::shared_ptr<const Snapshot> GetLatestTimestampedSnapshot() const {
    return GetTimestampedSnapshot(kMaxTxnTimestamp);
  }
  // Return the snapshot correponding to given timestamp. If ts is
  // kMaxTxnTimestamp, then we return the latest timestamped snapshot if
  // present. Othersise, we return the snapshot whose timestamp is equal to
  // `ts`. If no such snapshot exists, then we return null.
  virtual std::shared_ptr<const Snapshot> GetTimestampedSnapshot(
      TxnTimestamp ts) const = 0;
  // Release timestamped snapshots whose timestamps are less than or equal to
  // ts.
  virtual void ReleaseTimestampedSnapshotsOlderThan(TxnTimestamp ts) = 0;

  // Get all timestamped snapshots which will be stored in
  // timestamped_snapshots.
  Status GetAllTimestampedSnapshots(
      std::vector<std::shared_ptr<const Snapshot>>& timestamped_snapshots)
      const {
    return GetTimestampedSnapshots(/*ts_lb=*/0, /*ts_ub=*/kMaxTxnTimestamp,
                                   timestamped_snapshots);
  }

  // Get all timestamped snapshots whose timestamps fall within [ts_lb, ts_ub).
  // timestamped_snapshots will be cleared and contain returned snapshots.
  virtual Status GetTimestampedSnapshots(
      TxnTimestamp ts_lb, TxnTimestamp ts_ub,
      std::vector<std::shared_ptr<const Snapshot>>& timestamped_snapshots)
      const = 0;

 protected:
  // To Create an TransactionDB, call Open()
  // The ownership of db is transferred to the base StackableDB
  explicit TransactionDB(DB* db) : StackableDB(db) {}
  // No copying allowed
  TransactionDB(const TransactionDB&) = delete;
  void operator=(const TransactionDB&) = delete;
};

}  // namespace ROCKSDB_NAMESPACE

#endif  // ROCKSDB_LITE

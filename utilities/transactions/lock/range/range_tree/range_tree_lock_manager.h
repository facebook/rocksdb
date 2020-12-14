//
// Range Lock Manager that uses PerconaFT's locktree library
//
#pragma once
#ifndef ROCKSDB_LITE
#ifndef OS_WIN

// For DeadlockInfoBuffer:
#include "util/thread_local.h"
#include "utilities/transactions/lock/point/point_lock_manager.h"
#include "utilities/transactions/lock/range/range_lock_manager.h"

// Lock Tree library:
#include "lib/locktree/lock_request.h"
#include "lib/locktree/locktree.h"
#include "range_tree_lock_tracker.h"

namespace ROCKSDB_NAMESPACE {

using namespace toku;

typedef DeadlockInfoBufferTempl<RangeDeadlockPath> RangeDeadlockInfoBuffer;

// A Range Lock Manager that uses PerconaFT's locktree library
class RangeTreeLockManager : public RangeLockManagerBase,
                             public RangeLockManagerHandle {
 public:
  LockManager* getLockManager() override { return this; }

  void AddColumnFamily(const ColumnFamilyHandle* cfh) override;
  void RemoveColumnFamily(const ColumnFamilyHandle* cfh) override;

  void Resize(uint32_t) override;
  std::vector<DeadlockPath> GetDeadlockInfoBuffer() override;

  std::vector<RangeDeadlockPath> GetRangeDeadlockInfoBuffer() override;
  void SetRangeDeadlockInfoBufferSize(uint32_t target_size) override;

  // Get a lock on a range
  //  @note only exclusive locks are currently supported (requesting a
  //  non-exclusive lock will get an exclusive one)
  using LockManager::TryLock;
  Status TryLock(PessimisticTransaction* txn, ColumnFamilyId column_family_id,
                 const Endpoint& start_endp, const Endpoint& end_endp, Env* env,
                 bool exclusive) override;

  void UnLock(PessimisticTransaction* txn, const LockTracker& tracker,
              Env* env) override;
  void UnLock(PessimisticTransaction* txn, ColumnFamilyId column_family_id,
              const std::string& key, Env* env) override;
  void UnLock(PessimisticTransaction*, ColumnFamilyId, const Endpoint&,
              const Endpoint&, Env*) override{
      // TODO: range unlock does nothing...
  };

  RangeTreeLockManager(
      std::shared_ptr<TransactionDBMutexFactory> mutex_factory);

  ~RangeTreeLockManager();

  int SetMaxLockMemory(size_t max_lock_memory) override {
    return ltm_.set_max_lock_memory(max_lock_memory);
  }

  size_t GetMaxLockMemory() override { return ltm_.get_max_lock_memory(); }

  Counters GetStatus() override;

  bool IsPointLockSupported() const override {
    // One could have acquired a point lock (it is reduced to range lock)
    return true;
  }

  PointLockStatus GetPointLockStatus() override;

  // This is from LockManager
  LockManager::RangeLockStatus GetRangeLockStatus() override;

  // This has the same meaning as GetRangeLockStatus but is from
  // RangeLockManagerHandle
  RangeLockManagerHandle::RangeLockStatus GetRangeLockStatusData() override {
    return GetRangeLockStatus();
  }

  bool IsRangeLockSupported() const override { return true; }

  const LockTrackerFactory& GetLockTrackerFactory() const override {
    return RangeTreeLockTrackerFactory::Get();
  }


  // Get the lock tree which stores locks for Column Family with given cf_id
  toku::locktree* get_locktree_by_cfid(ColumnFamilyId cf_id);
 private:
  toku::locktree_manager ltm_;

  std::shared_ptr<TransactionDBMutexFactory> mutex_factory_;

  // Map from cf_id to locktree*. Can only be accessed while holding the
  // ltree_map_mutex_.
  using LockTreeMap = std::unordered_map<ColumnFamilyId, locktree*>;
  LockTreeMap ltree_map_;

  InstrumentedMutex ltree_map_mutex_;

  // Per-thread cache of ltree_map_.
  // (uses the same approach as TransactionLockMgr::lock_maps_cache_)
  std::unique_ptr<ThreadLocalPtr> ltree_lookup_cache_;

  RangeDeadlockInfoBuffer dlock_buffer_;

  static int CompareDbtEndpoints(void* arg, const DBT* a_key,
                                 const DBT* b_key);

  // Callbacks
  static int on_create(locktree*, void*) { return 0; /* no error */ }
  static void on_destroy(locktree*) {}
  static void on_escalate(TXNID txnid, const locktree* lt,
                          const range_buffer& buffer, void* extra);
};

void serialize_endpoint(const Endpoint& endp, std::string* buf);

}  // namespace ROCKSDB_NAMESPACE
#endif  // OS_WIN
#endif  // ROCKSDB_LITE

//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "utilities/transactions/lock/point/point_lock_manager.h"

#include <algorithm>
#include <cinttypes>
#include <mutex>

#include "monitoring/perf_context_imp.h"
#include "rocksdb/slice.h"
#include "rocksdb/utilities/transaction_db_mutex.h"
#include "test_util/sync_point.h"
#include "util/hash.h"
#include "util/thread_local.h"
#include "utilities/transactions/pessimistic_transaction_db.h"
#include "utilities/transactions/transaction_db_mutex_impl.h"

namespace ROCKSDB_NAMESPACE {

// KeyLockWaiter represents a waiter for a key lock. It contains a pair of
// mutex and conditional variable to allow waiter to wait for the key lock. It
// also contains other metadata about the waiter such as transaction id, lock
// type etc.
struct KeyLockWaiter {
  KeyLockWaiter(
      std::shared_ptr<TransactionDBMutex> m,
      std::shared_ptr<TransactionDBCondVar> c, TransactionID i, bool ex,
      std::stack<std::pair<std::shared_ptr<TransactionDBMutex>,
                           std::shared_ptr<TransactionDBCondVar>>>& pool)
      : id(i),
        exclusive(ex),
        ready(false),
        mutex(std::move(m)),
        cv(std::move(c)),
        key_cv_pool(pool) {}

  ~KeyLockWaiter() {
    // return key_cv to the pool
    key_cv_pool.emplace(std::move(mutex), std::move(cv));
  }

  // disable copy constructor and assignment operator, move and move assignment
  KeyLockWaiter(const KeyLockWaiter&) = delete;
  KeyLockWaiter& operator=(const KeyLockWaiter&) = delete;
  KeyLockWaiter(KeyLockWaiter&&) = delete;
  KeyLockWaiter& operator=(KeyLockWaiter&&) = delete;

  // Wait until it is the turn for this waiter to take the lock forever
  Status Wait() {
    std::function<bool()> check_ready = [this]() -> bool { return ready; };
    // Wait() does not take the lock, so it has to be taken before the call
    // and released after
    LockHolder lock_holder(mutex);
    auto ret = cv->Wait(mutex, &check_ready);
    return ret;
  }

  // Wait until it is the turn for this waiter to take the lock within
  // timeout_us
  Status WaitFor(int64_t timeout_us) {
    std::function<bool()> check_ready = [this]() -> bool { return ready; };
    // WaitFor() does not take the lock, so it has to be taken before the call
    // and released after
    LockHolder lock_holder(mutex);
    auto ret = cv->WaitFor(mutex, timeout_us, &check_ready);
    return ret;
  }

  // Notify the waiter that it is their turn to take the lock
  void Notify() {
    {
      // hold the lock before update ready
      LockHolder lock_holder(mutex);
      ready = true;
    }
    cv->NotifyAll();
  }

  const TransactionID id;
  const bool exclusive;

 private:
  // A lock holder class that acquires the mutex when constructed and releases
  // it when destructed automatically.
  class LockHolder {
   public:
    explicit LockHolder(std::shared_ptr<TransactionDBMutex>& m) : mutex_(m) {
      auto s = mutex_->Lock();
      // This mutex is only used for waiting and wake up between a pair of
      // threads, which holds it for a very short amount of time, so it is ok to
      // just call Lock, instead of TryLock.
      // Assert s.ok() to mute the status check
      assert(s.ok());
    }
    ~LockHolder() { mutex_->UnLock(); }

    // disable copy constructor and assignment operator, move and move
    // assignment
    LockHolder(const LockHolder&) = delete;
    LockHolder& operator=(const LockHolder&) = delete;
    LockHolder(LockHolder&&) = delete;
    LockHolder& operator=(LockHolder&&) = delete;

   private:
    std::shared_ptr<TransactionDBMutex>& mutex_;
  };

  bool ready;
  // TODO(Xingbo), Switch to std::binary_semaphore, once we have c++20
  // semaphore is likely more performant than mutex + cv.
  // Although we will also need to implement TransactionDBSemaphore, which would
  // be required if external system wants to do instrumented lock wait tracking
  std::shared_ptr<TransactionDBMutex> mutex;
  std::shared_ptr<TransactionDBCondVar> cv;
  // used for returning mutex back to the pool
  std::stack<std::pair<std::shared_ptr<TransactionDBMutex>,
                       std::shared_ptr<TransactionDBCondVar>>>& key_cv_pool;
};

struct LockInfo {
  bool exclusive;
  autovector<TransactionID> txn_ids;

  // Transaction locks are not valid after this time in us
  uint64_t expiration_time;

  LockInfo(TransactionID id, uint64_t time, bool ex)
      : exclusive(ex), expiration_time(time) {
    txn_ids.push_back(id);
  }

  // waiter queue for this key
  std::unique_ptr<std::list<std::unique_ptr<KeyLockWaiter>>> waiter_queue;
};

struct LockMapStripe {
  explicit LockMapStripe(std::shared_ptr<TransactionDBMutexFactory> factory) {
    stripe_mutex = factory->AllocateMutex();
    assert(stripe_mutex);

    mutex_factory_ = std::move(factory);
    FillKeyCVPool();
  }

  // Allocate a batch of mutex and conditional variable pairs to minimize memory
  // fragmentation
  void FillKeyCVPool() {
    // get number of CPU count
    int num_cpu = std::thread::hardware_concurrency();
    if (num_cpu == 0) {
      // if hardware_concurrency() returns 0, use 32 as default
      num_cpu = 32;
    }

    // 8 threads per CPU is probably a good upper bound for batch fill to reduce
    // fragmentation.
    constexpr int kThreadPerCPU = 8;
    for (int i = 0; i < kThreadPerCPU * num_cpu; i++) {
      key_cv_pool.emplace(mutex_factory_->AllocateMutex(),
                          mutex_factory_->AllocateCondVar());
    }
  }

  // Wait until it is the turn for this waiter to take the lock of this key
  // forever
  Status WaitOnKey(const std::string& key, TransactionID id, bool exclusive,
                   bool isUpgrade) {
    return WaitOnKeyInternal(key, id, exclusive, isUpgrade, 0);
  }

  // Wait until it is the turn for this waiter to take the lock of this key
  // within timeout_us
  Status WaitOnKeyFor(const std::string& key, TransactionID id, bool exclusive,
                      bool isUpgrade, int64_t timeout_us) {
    return WaitOnKeyInternal(key, id, exclusive, isUpgrade, timeout_us);
  }

  // Mutex must be held before modifying keys map
  std::shared_ptr<TransactionDBMutex> stripe_mutex;

  // Locked keys mapped to the info about the transactions that locked them.
  // TODO(agiardullo): Explore performance of other data structures.
  UnorderedMap<std::string, std::unique_ptr<LockInfo>> keys;

 private:
  std::unique_ptr<KeyLockWaiter> GetKeyLockWaiterFromPool(TransactionID id,
                                                          bool exclusive) {
    std::unique_ptr<KeyLockWaiter> waiter;
    if (key_cv_pool.empty()) {
      FillKeyCVPool();
    }
    waiter = std::make_unique<KeyLockWaiter>(
        std::move(key_cv_pool.top().first), std::move(key_cv_pool.top().second),
        id, exclusive, key_cv_pool);
    key_cv_pool.pop();
    return waiter;
  }

  Status WaitOnKeyInternal(const std::string& key, TransactionID id,
                           bool exclusive, bool isUpgrade, int64_t timeout_us) {
    // find lock info for key
    auto lock_info_iter = keys.find(key);

    // key should have been created by another transaction which holding a lock
    // on it.
    assert(lock_info_iter != keys.end());

    if (lock_info_iter->second->waiter_queue == nullptr) {
      // no waiter, create a new one
      lock_info_iter->second->waiter_queue =
          std::make_unique<std::list<std::unique_ptr<KeyLockWaiter>>>();
    }

    auto& waiter_queue = lock_info_iter->second->waiter_queue;

    std::list<std::unique_ptr<KeyLockWaiter>>::iterator lock_waiter;
    if (isUpgrade) {
      // If transaction is upgrading a shared lock to exclusive lock, prioritize
      // it by moving its exclusive lock request to the left of the first
      // exclusive lock in the queue if there is one, or end of the queue if
      // not exist. It will be able to acquire the lock after the shared
      // locks waiters at the front of queue acquired locks. This reduces the
      // chance of deadlock, which makes transaction run more efficiently
      auto it = waiter_queue->begin();
      while (true) {
        if (it == waiter_queue->end() || (*it)->exclusive) {
          // insert waiter either at the end of the queue or before the first
          // exlusive lock waiter.
          lock_waiter =
              waiter_queue->insert(it, GetKeyLockWaiterFromPool(id, exclusive));
          break;
        }
        it++;
      }
    } else {
      // Otherwise, follow FIFO.
      waiter_queue->emplace_back(GetKeyLockWaiterFromPool(id, exclusive));
      lock_waiter = --waiter_queue->end();
    }

    // it is safe to unlock stripe_mutex here because no one could remove the
    // lock waiter except the current transaction itself
    stripe_mutex->UnLock();

    Status ret;
    if (timeout_us == 0) {
      ret = (*lock_waiter)->Wait();
    } else {
      ret = (*lock_waiter)->WaitFor(timeout_us);
    }

    TEST_SYNC_POINT_CALLBACK("LockMapStrpe::WaitOnKeyInternal:AfterWokenUp",
                             &id);
    TEST_SYNC_POINT("LockMapStrpe::WaitOnKeyInternal:BeforeTakeLock");

    // Re-acquire the stripe mutex, as the lock waiter need to be removed from
    // the waiter queue
    auto s = stripe_mutex->Lock();
    // we have to take the stripe mutex to remove the lock waiter from the queue
    assert(s.ok());
    waiter_queue->erase(lock_waiter);

    return ret;
  }

  std::stack<std::pair<std::shared_ptr<TransactionDBMutex>,
                       std::shared_ptr<TransactionDBCondVar>>>
      key_cv_pool;

  std::shared_ptr<TransactionDBMutexFactory> mutex_factory_;
};

// Map of #num_stripes LockMapStripes
struct LockMap {
  explicit LockMap(size_t num_stripes,
                   std::shared_ptr<TransactionDBMutexFactory> factory)
      : num_stripes_(num_stripes) {
    lock_map_stripes_.reserve(num_stripes);
    for (size_t i = 0; i < num_stripes; i++) {
      LockMapStripe* stripe = new LockMapStripe(factory);
      lock_map_stripes_.push_back(stripe);
    }
  }

  ~LockMap() {
    for (auto stripe : lock_map_stripes_) {
      delete stripe;
    }
  }

  // Number of sepearate LockMapStripes to create, each with their own Mutex
  const size_t num_stripes_;

  // Count of keys that are currently locked in this column family.
  // Note that multiple shared locks on the same key is counted as 1 lock.
  // (Only maintained if PointLockManager::max_num_locks_ is positive.)
  std::atomic<int64_t> locked_key_cnt{0};

  std::vector<LockMapStripe*> lock_map_stripes_;

  size_t GetStripe(const std::string& key) const;
};

namespace {
void UnrefLockMapsCache(void* ptr) {
  // Called when a thread exits or a ThreadLocalPtr gets destroyed.
  auto lock_maps_cache =
      static_cast<UnorderedMap<uint32_t, std::shared_ptr<LockMap>>*>(ptr);
  delete lock_maps_cache;
}
}  // anonymous namespace

PointLockManager::PointLockManager(PessimisticTransactionDB* txn_db,
                                   const TransactionDBOptions& opt)
    : txn_db_impl_(txn_db),
      default_num_stripes_(opt.num_stripes),
      max_num_locks_(opt.max_num_locks),
      lock_maps_cache_(new ThreadLocalPtr(&UnrefLockMapsCache)),
      dlock_buffer_(opt.max_num_deadlocks),
      mutex_factory_(opt.custom_mutex_factory
                         ? opt.custom_mutex_factory
                         : std::make_shared<TransactionDBMutexFactoryImpl>()) {}

size_t LockMap::GetStripe(const std::string& key) const {
  assert(num_stripes_ > 0);
  return FastRange64(GetSliceNPHash64(key), num_stripes_);
}

void PointLockManager::AddColumnFamily(const ColumnFamilyHandle* cf) {
  InstrumentedMutexLock l(&lock_map_mutex_);

  if (lock_maps_.find(cf->GetID()) == lock_maps_.end()) {
    lock_maps_.emplace(cf->GetID(), std::make_shared<LockMap>(
                                        default_num_stripes_, mutex_factory_));
  } else {
    // column_family already exists in lock map
    assert(false);
  }
}

void PointLockManager::RemoveColumnFamily(const ColumnFamilyHandle* cf) {
  // Remove lock_map for this column family.  Since the lock map is stored
  // as a shared ptr, concurrent transactions can still keep using it
  // until they release their references to it.
  {
    InstrumentedMutexLock l(&lock_map_mutex_);

    auto lock_maps_iter = lock_maps_.find(cf->GetID());
    if (lock_maps_iter == lock_maps_.end()) {
      return;
    }

    lock_maps_.erase(lock_maps_iter);
  }  // lock_map_mutex_

  // Clear all thread-local caches
  autovector<void*> local_caches;
  lock_maps_cache_->Scrape(&local_caches, nullptr);
  for (auto cache : local_caches) {
    delete static_cast<LockMaps*>(cache);
  }
}

// Look up the LockMap std::shared_ptr for a given column_family_id.
// Note:  The LockMap is only valid as long as the caller is still holding on
//   to the returned std::shared_ptr.
std::shared_ptr<LockMap> PointLockManager::GetLockMap(
    ColumnFamilyId column_family_id) {
  // First check thread-local cache
  if (lock_maps_cache_->Get() == nullptr) {
    lock_maps_cache_->Reset(new LockMaps());
  }

  auto lock_maps_cache = static_cast<LockMaps*>(lock_maps_cache_->Get());

  auto lock_map_iter = lock_maps_cache->find(column_family_id);
  if (lock_map_iter != lock_maps_cache->end()) {
    // Found lock map for this column family.
    return lock_map_iter->second;
  }

  // Not found in local cache, grab mutex and check shared LockMaps
  InstrumentedMutexLock l(&lock_map_mutex_);

  lock_map_iter = lock_maps_.find(column_family_id);
  if (lock_map_iter == lock_maps_.end()) {
    return std::shared_ptr<LockMap>(nullptr);
  } else {
    // Found lock map.  Store in thread-local cache and return.
    std::shared_ptr<LockMap>& lock_map = lock_map_iter->second;
    lock_maps_cache->insert({column_family_id, lock_map});

    return lock_map;
  }
}

// Returns true if this lock has expired and can be acquired by another
// transaction.
// If false, sets *expire_time to the expiration time of the lock according
// to Env->GetMicros() or 0 if no expiration.
bool PointLockManager::IsLockExpired(TransactionID txn_id,
                                     const LockInfo& lock_info, Env* env,
                                     uint64_t* expire_time) {
  if (lock_info.expiration_time == 0) {
    *expire_time = 0;
    return false;
  }

  auto now = env->NowMicros();
  bool expired = lock_info.expiration_time <= now;
  if (!expired) {
    // return how many microseconds until lock will be expired
    *expire_time = lock_info.expiration_time;
  } else {
    for (auto id : lock_info.txn_ids) {
      if (txn_id == id) {
        continue;
      }

      bool success = txn_db_impl_->TryStealingExpiredTransactionLocks(id);
      if (!success) {
        expired = false;
        *expire_time = 0;
        break;
      }
    }
  }

  return expired;
}

Status PointLockManager::TryLock(PessimisticTransaction* txn,
                                 ColumnFamilyId column_family_id,
                                 const std::string& key, Env* env,
                                 bool exclusive) {
  // Lookup lock map for this column family id
  std::shared_ptr<LockMap> lock_map_ptr = GetLockMap(column_family_id);
  LockMap* lock_map = lock_map_ptr.get();
  if (lock_map == nullptr) {
    char msg[255];
    snprintf(msg, sizeof(msg), "Column family id not found: %" PRIu32,
             column_family_id);

    return Status::InvalidArgument(msg);
  }

  // Need to lock the mutex for the stripe that this key hashes to
  size_t stripe_num = lock_map->GetStripe(key);
  assert(lock_map->lock_map_stripes_.size() > stripe_num);
  LockMapStripe* stripe = lock_map->lock_map_stripes_.at(stripe_num);

  LockInfo lock_info(txn->GetID(), txn->GetExpirationTime(), exclusive);
  int64_t timeout = txn->GetLockTimeout();

  return AcquireWithTimeout(txn, lock_map, stripe, column_family_id, key, env,
                            timeout, lock_info);
}

// Helper function for TryLock().
Status PointLockManager::AcquireWithTimeout(
    PessimisticTransaction* txn, LockMap* lock_map, LockMapStripe* stripe,
    ColumnFamilyId column_family_id, const std::string& key, Env* env,
    int64_t timeout, const LockInfo& lock_info) {
  Status result;
  uint64_t end_time = 0;

  if (timeout > 0) {
    uint64_t start_time = env->NowMicros();
    end_time = start_time + timeout;
  }

  if (timeout < 0) {
    // If timeout is negative, we wait indefinitely to acquire the lock
    result = stripe->stripe_mutex->Lock();
  } else {
    result = stripe->stripe_mutex->TryLockFor(timeout);
  }

  if (!result.ok()) {
    // failed to acquire mutex
    return result;
  }

  // Acquire lock if we are able to
  uint64_t expire_time_hint = 0;
  autovector<TransactionID> wait_ids;
  bool isUpgrade;
  result = AcquireLocked(lock_map, stripe, key, env, lock_info,
                         &expire_time_hint, &wait_ids, &isUpgrade, true);
  if (!result.ok() && timeout != 0) {
    PERF_TIMER_GUARD(key_lock_wait_time);
    PERF_COUNTER_ADD(key_lock_wait_count, 1);
    // If we weren't able to acquire the lock, we will keep retrying as long
    // as the timeout allows.
    bool timed_out = false;
    bool cv_wait_fail = false;
    do {
      // Decide how long to wait
      int64_t cv_end_time = -1;
      if (expire_time_hint > 0 && end_time > 0) {
        cv_end_time = std::min(expire_time_hint, end_time);
      } else if (expire_time_hint > 0) {
        cv_end_time = expire_time_hint;
      } else if (end_time > 0) {
        cv_end_time = end_time;
      }
      assert(result.IsLockLimit() == wait_ids.empty());

      // We are dependent on a transaction to finish, so perform deadlock
      // detection.
      if (wait_ids.size() != 0) {
        if (txn->IsDeadlockDetect()) {
          if (IncrementWaiters(txn, wait_ids, key, column_family_id,
                               lock_info.exclusive, env)) {
            result = Status::Busy(Status::SubCode::kDeadlock);
            stripe->stripe_mutex->UnLock();
            return result;
          }
        }
        txn->SetWaitingTxn(wait_ids, column_family_id, &key);
      }

      TEST_SYNC_POINT("PointLockManager::AcquireWithTimeout:WaitingTxn");
      if (cv_end_time < 0) {
        // Wait indefinitely
        result = stripe->WaitOnKey(key, lock_info.txn_ids[0],
                                   lock_info.exclusive, isUpgrade);
        cv_wait_fail = !result.ok();
      } else {
        // FIXME: in this case, cv_end_time could be `expire_time_hint` from the
        // current lock holder, a time out does not mean we reached the current
        // transaction's timeout, and we should continue to retry locking
        // instead of exiting this while loop below.
        uint64_t now = env->NowMicros();
        if (static_cast<uint64_t>(cv_end_time) > now) {
          // This may be invoked multiple times since we divide
          // the time into smaller intervals.
          (void)ROCKSDB_THREAD_YIELD_CHECK_ABORT();
          result = stripe->WaitOnKeyFor(key, lock_info.txn_ids[0],
                                        lock_info.exclusive, isUpgrade,
                                        cv_end_time - now);
          cv_wait_fail = !result.ok() && !result.IsTimedOut();
        } else {
          // now >= cv_end_time, we already timed out
          result = Status::TimedOut(Status::SubCode::kLockTimeout);
        }
      }

      if (wait_ids.size() != 0) {
        txn->ClearWaitingTxn();
        if (txn->IsDeadlockDetect()) {
          DecrementWaiters(txn, wait_ids);
        }
      }
      if (cv_wait_fail) {
        break;
      }

      if (result.IsTimedOut()) {
        timed_out = true;
        // Even though we timed out, we will still make one more attempt to
        // acquire lock below (it is possible the lock expired and we
        // were never signaled).
      }
      assert(result.ok() || result.IsTimedOut());
      wait_ids.clear();
      // If wait is timed out, respect the FIFO order. Otherwise, it is
      // its turn to acquire the lock.
      result =
          AcquireLocked(lock_map, stripe, key, env, lock_info,
                        &expire_time_hint, &wait_ids, &isUpgrade, timed_out);
    } while (!result.ok() && !timed_out);
  }

  stripe->stripe_mutex->UnLock();

  return result;
}

void PointLockManager::DecrementWaiters(
    const PessimisticTransaction* txn,
    const autovector<TransactionID>& wait_ids) {
  std::lock_guard<std::mutex> lock(wait_txn_map_mutex_);
  DecrementWaitersImpl(txn, wait_ids);
}

void PointLockManager::DecrementWaitersImpl(
    const PessimisticTransaction* txn,
    const autovector<TransactionID>& wait_ids) {
  auto id = txn->GetID();
  assert(wait_txn_map_.Contains(id));
  wait_txn_map_.Delete(id);

  for (auto wait_id : wait_ids) {
    rev_wait_txn_map_.Get(wait_id)--;
    if (rev_wait_txn_map_.Get(wait_id) == 0) {
      rev_wait_txn_map_.Delete(wait_id);
    }
  }
}

bool PointLockManager::IncrementWaiters(
    const PessimisticTransaction* txn,
    const autovector<TransactionID>& wait_ids, const std::string& key,
    const uint32_t& cf_id, const bool& exclusive, Env* const env) {
  auto id = txn->GetID();
  std::vector<int> queue_parents(
      static_cast<size_t>(txn->GetDeadlockDetectDepth()));
  std::vector<TransactionID> queue_values(
      static_cast<size_t>(txn->GetDeadlockDetectDepth()));
  std::lock_guard<std::mutex> lock(wait_txn_map_mutex_);
  assert(!wait_txn_map_.Contains(id));

  wait_txn_map_.Insert(id, {wait_ids, cf_id, exclusive, key});

  for (auto wait_id : wait_ids) {
    if (rev_wait_txn_map_.Contains(wait_id)) {
      rev_wait_txn_map_.Get(wait_id)++;
    } else {
      rev_wait_txn_map_.Insert(wait_id, 1);
    }
  }

  // No deadlock if nobody is waiting on self.
  if (!rev_wait_txn_map_.Contains(id)) {
    return false;
  }

  const auto* next_ids = &wait_ids;
  int parent = -1;
  int64_t deadlock_time = 0;
  for (int tail = 0, head = 0; head < txn->GetDeadlockDetectDepth(); head++) {
    int i = 0;
    if (next_ids) {
      for (; i < static_cast<int>(next_ids->size()) &&
             tail + i < txn->GetDeadlockDetectDepth();
           i++) {
        queue_values[tail + i] = (*next_ids)[i];
        queue_parents[tail + i] = parent;
      }
      tail += i;
    }

    // No more items in the list, meaning no deadlock.
    if (tail == head) {
      return false;
    }

    auto next = queue_values[head];
    if (next == id) {
      std::vector<DeadlockInfo> path;
      while (head != -1) {
        assert(wait_txn_map_.Contains(queue_values[head]));

        auto extracted_info = wait_txn_map_.Get(queue_values[head]);
        path.push_back({queue_values[head], extracted_info.m_cf_id,
                        extracted_info.m_exclusive,
                        extracted_info.m_waiting_key});
        head = queue_parents[head];
      }
      if (!env->GetCurrentTime(&deadlock_time).ok()) {
        /*
          TODO(AR) this preserves the current behaviour whilst checking the
          status of env->GetCurrentTime to ensure that ASSERT_STATUS_CHECKED
          passes. Should we instead raise an error if !ok() ?
        */
        deadlock_time = 0;
      }
      std::reverse(path.begin(), path.end());
      dlock_buffer_.AddNewPath(DeadlockPath(path, deadlock_time));
      deadlock_time = 0;
      DecrementWaitersImpl(txn, wait_ids);
      return true;
    } else if (!wait_txn_map_.Contains(next)) {
      next_ids = nullptr;
      continue;
    } else {
      parent = head;
      next_ids = &(wait_txn_map_.Get(next).m_neighbors);
    }
  }

  // Wait cycle too big, just assume deadlock.
  if (!env->GetCurrentTime(&deadlock_time).ok()) {
    /*
      TODO(AR) this preserves the current behaviour whilst checking the status
      of env->GetCurrentTime to ensure that ASSERT_STATUS_CHECKED passes.
      Should we instead raise an error if !ok() ?
    */
    deadlock_time = 0;
  }
  dlock_buffer_.AddNewPath(DeadlockPath(deadlock_time, true));
  DecrementWaitersImpl(txn, wait_ids);
  return true;
}

// Try to lock this key after we have acquired the mutex.
// Sets *expire_time to the expiration time in microseconds
//  or 0 if no expiration.
//
// Returns Status::TimeOut if the lock cannot be acquired due to it being
// held by other transactions, `txn_ids` will be populated with the id of
// transactions that hold the lock, excluding lock_info.txn_ids[0].
// Returns Status::Aborted(kLockLimit) if the lock cannot be acquired due to
// reaching per CF limit on the number of locks.
//
// REQUIRED:  Stripe mutex must be held. txn_ids must be empty.
// TODO Xingbo move the new implementation to a subclass, so we can switch
// between the 2 implementations
Status PointLockManager::AcquireLocked(LockMap* lock_map, LockMapStripe* stripe,
                                       const std::string& key, Env* env,
                                       const LockInfo& txn_lock_info,
                                       uint64_t* expire_time,
                                       autovector<TransactionID>* txn_ids,
                                       bool* isUpgrade, bool fifo) {
  assert(txn_lock_info.txn_ids.size() == 1);
  assert(txn_ids && txn_ids->empty());

  *isUpgrade = false;

  auto stripe_iter = stripe->keys.find(key);
  if (stripe_iter == stripe->keys.end()) {
    // No lock nor waiter on this key, so we can acquire it
    if (max_num_locks_ > 0 &&
        lock_map->locked_key_cnt.load(std::memory_order_acquire) >=
            max_num_locks_) {
      return Status::LockLimit();
    } else {
      // acquire lock
      // create a new entry, if not exist
      stripe->keys.emplace(
          key, std::make_unique<LockInfo>(txn_lock_info.txn_ids[0],
                                          txn_lock_info.expiration_time,
                                          txn_lock_info.exclusive));

      // Maintain lock count if there is a limit on the number of locks
      if (max_num_locks_) {
        lock_map->locked_key_cnt++;
      }

      return Status::OK();
    }
  }

  auto& lock_info = *(stripe_iter->second);
  auto locked = !lock_info.txn_ids.empty();
  auto solo_lock_owner = (lock_info.txn_ids.size() == 1) &&
                         (lock_info.txn_ids[0] == txn_lock_info.txn_ids[0]);

  // Handle lock downgrade and reentrant first, it should always succeed
  if (locked) {
    if (solo_lock_owner) {
      // Lock is already owned by itself.
      if (lock_info.exclusive && !txn_lock_info.exclusive) {
        // For downgrade, wake up all the shared lock waiters at the front of
        // the queue
        if (lock_info.waiter_queue != nullptr) {
          for (auto& waiter : *lock_info.waiter_queue) {
            if (waiter->exclusive) {
              break;
            }
            waiter->Notify();
          }
        }
      }

      if (!(!lock_info.exclusive && txn_lock_info.exclusive)) {
        // If it is not lock upgrade, grant it immediately
        lock_info.exclusive = txn_lock_info.exclusive;
        lock_info.expiration_time = txn_lock_info.expiration_time;
        return Status::OK();
      }
    }
    // handle read reentrant lock
    if (!txn_lock_info.exclusive && !lock_info.exclusive) {
      auto lock_it =
          std::find(lock_info.txn_ids.begin(), lock_info.txn_ids.end(),
                    txn_lock_info.txn_ids[0]);
      if (lock_it != lock_info.txn_ids.end()) {
        lock_info.expiration_time =
            std::max(lock_info.expiration_time, txn_lock_info.expiration_time);
        return Status::OK();
      }
    }
  }

  auto prepare_txn_wait_ids_with_locked_txn_ids = [&lock_info, &txn_lock_info,
                                                   &txn_ids, &isUpgrade]() {
    for (auto id : lock_info.txn_ids) {
      // A transaction is not blocked by itself
      if (id != txn_lock_info.txn_ids[0]) {
        txn_ids->push_back(id);
      } else {
        // Itself is already holding a lock, so it is either an upgrade or
        // downgrade. Downgrade has already been handled above. Assert it is
        // an upgrade here.
        assert(!lock_info.exclusive && txn_lock_info.exclusive);
        *isUpgrade = true;
      }
    }
  };

  auto has_waiter =
      (lock_info.waiter_queue != nullptr) && (!lock_info.waiter_queue->empty());

  if (fifo && has_waiter) {
    // handle fifo and has waiter in the queue
    {
      // handle shared lock request on a shared lock with only shared lock
      // waiters
      if (!txn_lock_info.exclusive &&
          (!locked || (locked && !lock_info.exclusive))) {
        bool has_exclusive_waiter = false;
        // check whether there is X waiter
        for (auto& waiter : *lock_info.waiter_queue) {
          has_exclusive_waiter |= waiter->exclusive;
          if (has_exclusive_waiter) {
            break;
          }
        }
        if (!has_exclusive_waiter) {
          // no X waiter, so we can acquire the lock without waiting
          lock_info.txn_ids.push_back(txn_lock_info.txn_ids[0]);
          lock_info.exclusive = false;
          // Using std::max means that expiration time never goes down even
          // when a transaction is removed from the list. The correct
          // solution would be to track expiry for every transaction, but
          // this would also work for now.
          lock_info.expiration_time = std::max(lock_info.expiration_time,
                                               txn_lock_info.expiration_time);
          return Status::OK();
        }
      }
    }

    // For other cases with fifo and lock waiter, try to wait in the queue
    // and fill the waiting txn list
    prepare_txn_wait_ids_with_locked_txn_ids();

    // Add the waiter txn ids to the blocking txn id list for better
    // deadlock detection
    if (lock_info.waiter_queue != nullptr) {
      for (auto& waiter : *lock_info.waiter_queue) {
        if (*isUpgrade && waiter->exclusive) {
          // For upgrade locks, it will be placed at the beginning of
          // the queue. However, for shared lock waiters that are at
          // the beginning of the queue that got waked up but haven't
          // taken the lock yet, they should still be added to the
          // blocking txn id list.
          break;
        }
        txn_ids->push_back(waiter->id);
      }
    }

    if (*isUpgrade && txn_ids->empty()) {
      // During lock upgrade, if no wait transaction is found, upgrade it now.
      lock_info.exclusive = txn_lock_info.exclusive;
      lock_info.expiration_time = txn_lock_info.expiration_time;
      return Status::OK();
    }
    return Status::TimedOut(Status::SubCode::kLockTimeout);
  } else {
    // there is no waiter or it is its turn to take the lock
    // For handle lock expiration
    if (!locked) {
      // no lock on this key, acquire it directly
      lock_info.txn_ids = txn_lock_info.txn_ids;
      lock_info.exclusive = txn_lock_info.exclusive;
      lock_info.expiration_time = txn_lock_info.expiration_time;
      return Status::OK();
    }

    if (IsLockExpired(txn_lock_info.txn_ids[0], lock_info, env, expire_time)) {
      // current lock is expired, steal it, no need to check lock limit
      lock_info.txn_ids = txn_lock_info.txn_ids;
      lock_info.exclusive = txn_lock_info.exclusive;
      lock_info.expiration_time = txn_lock_info.expiration_time;
      return Status::OK();
    }

    // Check lock compatibility
    if (txn_lock_info.exclusive) {
      // handle lock upgrade
      if (solo_lock_owner) {
        // Itself is already holding a lock, so it is either an upgrade or
        // downgrade. Downgrade has already been handled above. Assert it is
        // an upgrade here. Acquire the lock directly
        assert(!lock_info.exclusive && txn_lock_info.exclusive);
        lock_info.exclusive = txn_lock_info.exclusive;
        lock_info.expiration_time = txn_lock_info.expiration_time;
        return Status::OK();
      } else {
        // lock is already owned by other transactions
        prepare_txn_wait_ids_with_locked_txn_ids();
        return Status::TimedOut(Status::SubCode::kLockTimeout);
      }
    } else {
      // handle shared lock request
      if (lock_info.exclusive) {
        // lock is already owned by other exclusive lock
        prepare_txn_wait_ids_with_locked_txn_ids();
        return Status::TimedOut(Status::SubCode::kLockTimeout);
      } else {
        // lock is on shared lock state, acquire it
        lock_info.txn_ids.push_back(txn_lock_info.txn_ids[0]);
        // update the expiration time
        lock_info.expiration_time =
            std::max(lock_info.expiration_time, txn_lock_info.expiration_time);
        return Status::OK();
      }
    }
  }
}

void PointLockManager::UnLockKey(PessimisticTransaction* txn,
                                 const std::string& key, LockMapStripe* stripe,
                                 LockMap* lock_map, Env* env) {
#ifdef NDEBUG
  (void)env;
#endif
  TransactionID txn_id = txn->GetID();

  auto stripe_iter = stripe->keys.find(key);
  if (stripe_iter != stripe->keys.end()) {
    auto& txns = stripe_iter->second->txn_ids;
    auto txn_it = std::find(txns.begin(), txns.end(), txn_id);

    // Found the key we locked.  unlock it.
    if (txn_it != txns.end()) {
      auto erase_current_txn = [&txn_it, &txns]() {
        if (txns.size() > 1) {
          auto last_it = txns.end() - 1;
          if (txn_it != last_it) {
            *txn_it = *last_it;
          }
        }
        txns.pop_back();
      };

      auto handle_last_transaction_holding_the_lock =
          [this, &stripe_iter, &stripe, &erase_current_txn, &lock_map]() {
            // check whether there is other waiting transactions
            if (stripe_iter->second->waiter_queue == nullptr ||
                stripe_iter->second->waiter_queue->empty()) {
              stripe->keys.erase(stripe_iter);
              if (max_num_locks_ > 0) {
                // Maintain lock count if there is a limit on the number of
                // locks.
                assert(lock_map->locked_key_cnt.load(
                           std::memory_order_relaxed) > 0);
                lock_map->locked_key_cnt--;
              }
            } else {
              // there are waiters in the queue, so we need to wake the next
              // one up
              erase_current_txn();
              // loop through the waiter queue and wake up all the shared lock
              // waiters until the first exclusive lock waiter, or wake up the
              // first waiter, if it is waiting for an exclusive lock.
              bool first_waiter = true;
              for (auto& waiter : *stripe_iter->second->waiter_queue) {
                if (waiter->exclusive) {
                  if (first_waiter) {
                    // the first waiter is an exclusive lock waiter, wake it
                    // up Note that they are only notified, but not removed
                    // from the waiter queue. This allows new transaction to
                    // be aware that there are waiters ahead of them.
                    waiter->Notify();
                  }
                  // found the first exclusive lock waiter, stop
                  break;
                } else {
                  // wake up the shared lock waiter
                  waiter->Notify();
                }
                first_waiter = false;
              }
            }
          };

      // If the lock was held in exclusive mode, only one transaction should
      // holding it.
      if (stripe_iter->second->exclusive) {
        assert(txns.size() == 1);
        handle_last_transaction_holding_the_lock();
      } else {
        // In shared mode, it is possible that another transaction is holding
        // a shared lock and is waiting to upgrade the lock to exclusive.
        assert(txns.size() >= 1);
        if (txns.size() > 2) {
          // Including the current transaction, if there are more than 2
          // transactions holding the lock in shared mode, don't wake up any
          // waiter, as the next waiter will not be able to acquire the lock
          // anyway.
          erase_current_txn();
        } else if (txns.size() == 2) {
          // remove the current transaction first.
          erase_current_txn();
          // Check whether the one remained is trying to upgrade the lock by
          // checking whether its id matches.
          if (stripe_iter->second->waiter_queue != nullptr &&
              !stripe_iter->second->waiter_queue->empty() &&
              stripe_iter->second->waiter_queue->front()->id == txns[0]) {
            // There are waiters in the queue and the next one is same as the
            // one that is holding the shared lock wake it up
            stripe_iter->second->waiter_queue->front()->Notify();
          }
        } else {
          // Current transaction is the only one holding the shared lock
          handle_last_transaction_holding_the_lock();
        }
      }
    }
  } else {
    // This key is either not locked or locked by someone else.  This should
    // only happen if the unlocking transaction has expired.
    assert(txn->GetExpirationTime() > 0 &&
           txn->GetExpirationTime() < env->NowMicros());
  }
}

void PointLockManager::UnLock(PessimisticTransaction* txn,
                              ColumnFamilyId column_family_id,
                              const std::string& key, Env* env) {
  std::shared_ptr<LockMap> lock_map_ptr = GetLockMap(column_family_id);
  LockMap* lock_map = lock_map_ptr.get();
  if (lock_map == nullptr) {
    // Column Family must have been dropped.
    return;
  }

  // Lock the mutex for the stripe that this key hashes to
  size_t stripe_num = lock_map->GetStripe(key);
  assert(lock_map->lock_map_stripes_.size() > stripe_num);
  LockMapStripe* stripe = lock_map->lock_map_stripes_.at(stripe_num);

  stripe->stripe_mutex->Lock().PermitUncheckedError();
  UnLockKey(txn, key, stripe, lock_map, env);
  stripe->stripe_mutex->UnLock();
}

void PointLockManager::UnLock(PessimisticTransaction* txn,
                              const LockTracker& tracker, Env* env) {
  std::unique_ptr<LockTracker::ColumnFamilyIterator> cf_it(
      tracker.GetColumnFamilyIterator());
  assert(cf_it != nullptr);
  while (cf_it->HasNext()) {
    ColumnFamilyId cf = cf_it->Next();
    std::shared_ptr<LockMap> lock_map_ptr = GetLockMap(cf);
    LockMap* lock_map = lock_map_ptr.get();
    if (!lock_map) {
      // Column Family must have been dropped.
      return;
    }

    // Bucket keys by lock_map_ stripe
    UnorderedMap<size_t, std::vector<const std::string*>> keys_by_stripe(
        lock_map->num_stripes_);
    std::unique_ptr<LockTracker::KeyIterator> key_it(
        tracker.GetKeyIterator(cf));
    assert(key_it != nullptr);
    while (key_it->HasNext()) {
      const std::string& key = key_it->Next();
      size_t stripe_num = lock_map->GetStripe(key);
      keys_by_stripe[stripe_num].push_back(&key);
    }

    // For each stripe, grab the stripe mutex and unlock all keys in this
    // stripe
    for (auto& stripe_iter : keys_by_stripe) {
      size_t stripe_num = stripe_iter.first;
      auto& stripe_keys = stripe_iter.second;

      assert(lock_map->lock_map_stripes_.size() > stripe_num);
      LockMapStripe* stripe = lock_map->lock_map_stripes_.at(stripe_num);

      stripe->stripe_mutex->Lock().PermitUncheckedError();

      for (const std::string* key : stripe_keys) {
        UnLockKey(txn, *key, stripe, lock_map, env);
      }

      stripe->stripe_mutex->UnLock();
    }
  }
}

PointLockManager::PointLockStatus PointLockManager::GetPointLockStatus() {
  PointLockStatus data;
  // Lock order here is important. The correct order is lock_map_mutex_, then
  // for every column family ID in ascending order lock every stripe in
  // ascending order.
  InstrumentedMutexLock l(&lock_map_mutex_);

  std::vector<uint32_t> cf_ids;
  for (const auto& map : lock_maps_) {
    cf_ids.push_back(map.first);
  }
  std::sort(cf_ids.begin(), cf_ids.end());

  for (auto i : cf_ids) {
    const auto& stripes = lock_maps_[i]->lock_map_stripes_;
    // Iterate and lock all stripes in ascending order.
    for (const auto& j : stripes) {
      j->stripe_mutex->Lock().PermitUncheckedError();
      for (const auto& it : j->keys) {
        struct KeyLockInfo info;
        info.exclusive = it.second->exclusive;
        info.key = it.first;
        for (const auto& id : it.second->txn_ids) {
          info.ids.push_back(id);
        }
        data.insert({i, info});
      }
    }
  }

  // Unlock everything. Unlocking order is not important.
  for (auto i : cf_ids) {
    const auto& stripes = lock_maps_[i]->lock_map_stripes_;
    for (const auto& j : stripes) {
      j->stripe_mutex->UnLock();
    }
  }

  return data;
}

std::vector<DeadlockPath> PointLockManager::GetDeadlockInfoBuffer() {
  return dlock_buffer_.PrepareBuffer();
}

void PointLockManager::Resize(uint32_t target_size) {
  dlock_buffer_.Resize(target_size);
}

PointLockManager::RangeLockStatus PointLockManager::GetRangeLockStatus() {
  return {};
}

Status PointLockManager::TryLock(PessimisticTransaction* /* txn */,
                                 ColumnFamilyId /* cf_id */,
                                 const Endpoint& /* start */,
                                 const Endpoint& /* end */, Env* /* env */,
                                 bool /* exclusive */) {
  return Status::NotSupported(
      "PointLockManager does not support range locking");
}

void PointLockManager::UnLock(PessimisticTransaction* /* txn */,
                              ColumnFamilyId /* cf_id */,
                              const Endpoint& /* start */,
                              const Endpoint& /* end */, Env* /* env */) {
  // no-op
}

}  // namespace ROCKSDB_NAMESPACE

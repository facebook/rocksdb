//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#ifndef ROCKSDB_LITE

#include "utilities/transactions/transaction_lock_mgr.h"

#include <cinttypes>
#include <algorithm>
#include <mutex>

#include "monitoring/perf_context_imp.h"
#include "rocksdb/slice.h"
#include "rocksdb/utilities/transaction_db_mutex.h"
#include "test_util/sync_point.h"
#include "util/cast_util.h"
#include "util/hash.h"
#include "util/thread_local.h"
#include "utilities/transactions/pessimistic_transaction_db.h"

namespace ROCKSDB_NAMESPACE {

struct LockInfo {
  bool exclusive;
  autovector<TransactionID> txn_ids;

  // Transaction locks are not valid after this time in us
  uint64_t expiration_time;

  LockInfo(TransactionID id, uint64_t time, bool ex)
      : exclusive(ex), expiration_time(time) {
    txn_ids.push_back(id);
  }
  LockInfo(const LockInfo& lock_info)
      : exclusive(lock_info.exclusive),
        txn_ids(lock_info.txn_ids),
        expiration_time(lock_info.expiration_time) {}
};

struct LockMapStripe {
  explicit LockMapStripe(std::shared_ptr<TransactionDBMutexFactory> factory) {
    stripe_mutex = factory->AllocateMutex();
    stripe_cv = factory->AllocateCondVar();
    assert(stripe_mutex);
    assert(stripe_cv);
  }

  // Mutex must be held before modifying keys map
  std::shared_ptr<TransactionDBMutex> stripe_mutex;

  // Condition Variable per stripe for waiting on a lock
  std::shared_ptr<TransactionDBCondVar> stripe_cv;

  // Locked keys mapped to the info about the transactions that locked them.
  // TODO(agiardullo): Explore performance of other data structures.
  std::unordered_map<std::string, LockInfo> keys;
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
  // (Only maintained if TransactionLockMgr::max_num_locks_ is positive.)
  std::atomic<int64_t> lock_cnt{0};

  std::vector<LockMapStripe*> lock_map_stripes_;

  size_t GetStripe(const std::string& key) const;
};

void DeadlockInfoBuffer::AddNewPath(DeadlockPath path) {
  std::lock_guard<std::mutex> lock(paths_buffer_mutex_);

  if (paths_buffer_.empty()) {
    return;
  }

  paths_buffer_[buffer_idx_] = std::move(path);
  buffer_idx_ = (buffer_idx_ + 1) % paths_buffer_.size();
}

void DeadlockInfoBuffer::Resize(uint32_t target_size) {
  std::lock_guard<std::mutex> lock(paths_buffer_mutex_);

  paths_buffer_ = Normalize();

  // Drop the deadlocks that will no longer be needed ater the normalize
  if (target_size < paths_buffer_.size()) {
    paths_buffer_.erase(
        paths_buffer_.begin(),
        paths_buffer_.begin() + (paths_buffer_.size() - target_size));
    buffer_idx_ = 0;
  }
  // Resize the buffer to the target size and restore the buffer's idx
  else {
    auto prev_size = paths_buffer_.size();
    paths_buffer_.resize(target_size);
    buffer_idx_ = (uint32_t)prev_size;
  }
}

std::vector<DeadlockPath> DeadlockInfoBuffer::Normalize() {
  auto working = paths_buffer_;

  if (working.empty()) {
    return working;
  }

  // Next write occurs at a nonexistent path's slot
  if (paths_buffer_[buffer_idx_].empty()) {
    working.resize(buffer_idx_);
  } else {
    std::rotate(working.begin(), working.begin() + buffer_idx_, working.end());
  }

  return working;
}

std::vector<DeadlockPath> DeadlockInfoBuffer::PrepareBuffer() {
  std::lock_guard<std::mutex> lock(paths_buffer_mutex_);

  // Reversing the normalized vector returns the latest deadlocks first
  auto working = Normalize();
  std::reverse(working.begin(), working.end());

  return working;
}

namespace {
void UnrefLockMapsCache(void* ptr) {
  // Called when a thread exits or a ThreadLocalPtr gets destroyed.
  auto lock_maps_cache =
      static_cast<std::unordered_map<uint32_t, std::shared_ptr<LockMap>>*>(ptr);
  delete lock_maps_cache;
}
}  // anonymous namespace

TransactionLockMgr::TransactionLockMgr(
    TransactionDB* txn_db, size_t default_num_stripes, int64_t max_num_locks,
    uint32_t max_num_deadlocks,
    std::shared_ptr<TransactionDBMutexFactory> mutex_factory)
    : txn_db_impl_(nullptr),
      default_num_stripes_(default_num_stripes),
      max_num_locks_(max_num_locks),
      lock_maps_cache_(new ThreadLocalPtr(&UnrefLockMapsCache)),
      dlock_buffer_(max_num_deadlocks),
      mutex_factory_(mutex_factory) {
  assert(txn_db);
  txn_db_impl_ = static_cast_with_check<PessimisticTransactionDB>(txn_db);
}

TransactionLockMgr::~TransactionLockMgr() {}

size_t LockMap::GetStripe(const std::string& key) const {
  assert(num_stripes_ > 0);
  return fastrange64(GetSliceNPHash64(key), num_stripes_);
}

void TransactionLockMgr::AddColumnFamily(uint32_t column_family_id) {
  InstrumentedMutexLock l(&lock_map_mutex_);

  if (lock_maps_.find(column_family_id) == lock_maps_.end()) {
    lock_maps_.emplace(column_family_id,
                       std::make_shared<LockMap>(default_num_stripes_, mutex_factory_));
  } else {
    // column_family already exists in lock map
    assert(false);
  }
}

void TransactionLockMgr::RemoveColumnFamily(uint32_t column_family_id) {
  // Remove lock_map for this column family.  Since the lock map is stored
  // as a shared ptr, concurrent transactions can still keep using it
  // until they release their references to it.
  {
    InstrumentedMutexLock l(&lock_map_mutex_);

    auto lock_maps_iter = lock_maps_.find(column_family_id);
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
std::shared_ptr<LockMap> TransactionLockMgr::GetLockMap(
    uint32_t column_family_id) {
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
bool TransactionLockMgr::IsLockExpired(TransactionID txn_id,
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

Status TransactionLockMgr::TryLock(PessimisticTransaction* txn,
                                   uint32_t column_family_id,
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
                            timeout, std::move(lock_info));
}

// Helper function for TryLock().
Status TransactionLockMgr::AcquireWithTimeout(
    PessimisticTransaction* txn, LockMap* lock_map, LockMapStripe* stripe,
    uint32_t column_family_id, const std::string& key, Env* env,
    int64_t timeout, LockInfo&& lock_info) {
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
  result = AcquireLocked(lock_map, stripe, key, env, std::move(lock_info),
                         &expire_time_hint, &wait_ids);

  if (!result.ok() && timeout != 0) {
    PERF_TIMER_GUARD(key_lock_wait_time);
    PERF_COUNTER_ADD(key_lock_wait_count, 1);
    // If we weren't able to acquire the lock, we will keep retrying as long
    // as the timeout allows.
    bool timed_out = false;
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

      assert(result.IsBusy() || wait_ids.size() != 0);

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

      TEST_SYNC_POINT("TransactionLockMgr::AcquireWithTimeout:WaitingTxn");
      if (cv_end_time < 0) {
        // Wait indefinitely
        result = stripe->stripe_cv->Wait(stripe->stripe_mutex);
      } else {
        uint64_t now = env->NowMicros();
        if (static_cast<uint64_t>(cv_end_time) > now) {
          result = stripe->stripe_cv->WaitFor(stripe->stripe_mutex,
                                              cv_end_time - now);
        }
      }

      if (wait_ids.size() != 0) {
        txn->ClearWaitingTxn();
        if (txn->IsDeadlockDetect()) {
          DecrementWaiters(txn, wait_ids);
        }
      }

      if (result.IsTimedOut()) {
          timed_out = true;
          // Even though we timed out, we will still make one more attempt to
          // acquire lock below (it is possible the lock expired and we
          // were never signaled).
      }

      if (result.ok() || result.IsTimedOut()) {
        result = AcquireLocked(lock_map, stripe, key, env, std::move(lock_info),
                               &expire_time_hint, &wait_ids);
      }
    } while (!result.ok() && !timed_out);
  }

  stripe->stripe_mutex->UnLock();

  return result;
}

void TransactionLockMgr::DecrementWaiters(
    const PessimisticTransaction* txn,
    const autovector<TransactionID>& wait_ids) {
  std::lock_guard<std::mutex> lock(wait_txn_map_mutex_);
  DecrementWaitersImpl(txn, wait_ids);
}

void TransactionLockMgr::DecrementWaitersImpl(
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

bool TransactionLockMgr::IncrementWaiters(
    const PessimisticTransaction* txn,
    const autovector<TransactionID>& wait_ids, const std::string& key,
    const uint32_t& cf_id, const bool& exclusive, Env* const env) {
  auto id = txn->GetID();
  std::vector<int> queue_parents(static_cast<size_t>(txn->GetDeadlockDetectDepth()));
  std::vector<TransactionID> queue_values(static_cast<size_t>(txn->GetDeadlockDetectDepth()));
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
      env->GetCurrentTime(&deadlock_time);
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
  env->GetCurrentTime(&deadlock_time);
  dlock_buffer_.AddNewPath(DeadlockPath(deadlock_time, true));
  DecrementWaitersImpl(txn, wait_ids);
  return true;
}

// Try to lock this key after we have acquired the mutex.
// Sets *expire_time to the expiration time in microseconds
//  or 0 if no expiration.
// REQUIRED:  Stripe mutex must be held.
Status TransactionLockMgr::AcquireLocked(LockMap* lock_map,
                                         LockMapStripe* stripe,
                                         const std::string& key, Env* env,
                                         LockInfo&& txn_lock_info,
                                         uint64_t* expire_time,
                                         autovector<TransactionID>* txn_ids) {
  assert(txn_lock_info.txn_ids.size() == 1);

  Status result;
  // Check if this key is already locked
  auto stripe_iter = stripe->keys.find(key);
  if (stripe_iter != stripe->keys.end()) {
    // Lock already held
    LockInfo& lock_info = stripe_iter->second;
    assert(lock_info.txn_ids.size() == 1 || !lock_info.exclusive);

    if (lock_info.exclusive || txn_lock_info.exclusive) {
      if (lock_info.txn_ids.size() == 1 &&
          lock_info.txn_ids[0] == txn_lock_info.txn_ids[0]) {
        // The list contains one txn and we're it, so just take it.
        lock_info.exclusive = txn_lock_info.exclusive;
        lock_info.expiration_time = txn_lock_info.expiration_time;
      } else {
        // Check if it's expired. Skips over txn_lock_info.txn_ids[0] in case
        // it's there for a shared lock with multiple holders which was not
        // caught in the first case.
        if (IsLockExpired(txn_lock_info.txn_ids[0], lock_info, env,
                          expire_time)) {
          // lock is expired, can steal it
          lock_info.txn_ids = txn_lock_info.txn_ids;
          lock_info.exclusive = txn_lock_info.exclusive;
          lock_info.expiration_time = txn_lock_info.expiration_time;
          // lock_cnt does not change
        } else {
          result = Status::TimedOut(Status::SubCode::kLockTimeout);
          *txn_ids = lock_info.txn_ids;
        }
      }
    } else {
      // We are requesting shared access to a shared lock, so just grant it.
      lock_info.txn_ids.push_back(txn_lock_info.txn_ids[0]);
      // Using std::max means that expiration time never goes down even when
      // a transaction is removed from the list. The correct solution would be
      // to track expiry for every transaction, but this would also work for
      // now.
      lock_info.expiration_time =
          std::max(lock_info.expiration_time, txn_lock_info.expiration_time);
    }
  } else {  // Lock not held.
    // Check lock limit
    if (max_num_locks_ > 0 &&
        lock_map->lock_cnt.load(std::memory_order_acquire) >= max_num_locks_) {
      result = Status::Busy(Status::SubCode::kLockLimit);
    } else {
      // acquire lock
      stripe->keys.emplace(key, std::move(txn_lock_info));

      // Maintain lock count if there is a limit on the number of locks
      if (max_num_locks_) {
        lock_map->lock_cnt++;
      }
    }
  }

  return result;
}

void TransactionLockMgr::UnLockKey(const PessimisticTransaction* txn,
                                   const std::string& key,
                                   LockMapStripe* stripe, LockMap* lock_map,
                                   Env* env) {
#ifdef NDEBUG
  (void)env;
#endif
  TransactionID txn_id = txn->GetID();

  auto stripe_iter = stripe->keys.find(key);
  if (stripe_iter != stripe->keys.end()) {
    auto& txns = stripe_iter->second.txn_ids;
    auto txn_it = std::find(txns.begin(), txns.end(), txn_id);
    // Found the key we locked.  unlock it.
    if (txn_it != txns.end()) {
      if (txns.size() == 1) {
        stripe->keys.erase(stripe_iter);
      } else {
        auto last_it = txns.end() - 1;
        if (txn_it != last_it) {
          *txn_it = *last_it;
        }
        txns.pop_back();
      }

      if (max_num_locks_ > 0) {
        // Maintain lock count if there is a limit on the number of locks.
        assert(lock_map->lock_cnt.load(std::memory_order_relaxed) > 0);
        lock_map->lock_cnt--;
      }
    }
  } else {
    // This key is either not locked or locked by someone else.  This should
    // only happen if the unlocking transaction has expired.
    assert(txn->GetExpirationTime() > 0 &&
           txn->GetExpirationTime() < env->NowMicros());
  }
}

void TransactionLockMgr::UnLock(PessimisticTransaction* txn,
                                uint32_t column_family_id,
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

  stripe->stripe_mutex->Lock();
  UnLockKey(txn, key, stripe, lock_map, env);
  stripe->stripe_mutex->UnLock();

  // Signal waiting threads to retry locking
  stripe->stripe_cv->NotifyAll();
}

void TransactionLockMgr::UnLock(const PessimisticTransaction* txn,
                                const TransactionKeyMap* key_map, Env* env) {
  for (auto& key_map_iter : *key_map) {
    uint32_t column_family_id = key_map_iter.first;
    auto& keys = key_map_iter.second;

    std::shared_ptr<LockMap> lock_map_ptr = GetLockMap(column_family_id);
    LockMap* lock_map = lock_map_ptr.get();

    if (lock_map == nullptr) {
      // Column Family must have been dropped.
      return;
    }

    // Bucket keys by lock_map_ stripe
    std::unordered_map<size_t, std::vector<const std::string*>> keys_by_stripe(
        std::max(keys.size(), lock_map->num_stripes_));

    for (auto& key_iter : keys) {
      const std::string& key = key_iter.first;

      size_t stripe_num = lock_map->GetStripe(key);
      keys_by_stripe[stripe_num].push_back(&key);
    }

    // For each stripe, grab the stripe mutex and unlock all keys in this stripe
    for (auto& stripe_iter : keys_by_stripe) {
      size_t stripe_num = stripe_iter.first;
      auto& stripe_keys = stripe_iter.second;

      assert(lock_map->lock_map_stripes_.size() > stripe_num);
      LockMapStripe* stripe = lock_map->lock_map_stripes_.at(stripe_num);

      stripe->stripe_mutex->Lock();

      for (const std::string* key : stripe_keys) {
        UnLockKey(txn, *key, stripe, lock_map, env);
      }

      stripe->stripe_mutex->UnLock();

      // Signal waiting threads to retry locking
      stripe->stripe_cv->NotifyAll();
    }
  }
}

TransactionLockMgr::LockStatusData TransactionLockMgr::GetLockStatusData() {
  LockStatusData data;
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
      j->stripe_mutex->Lock();
      for (const auto& it : j->keys) {
        struct KeyLockInfo info;
        info.exclusive = it.second.exclusive;
        info.key = it.first;
        for (const auto& id : it.second.txn_ids) {
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
std::vector<DeadlockPath> TransactionLockMgr::GetDeadlockInfoBuffer() {
  return dlock_buffer_.PrepareBuffer();
}

void TransactionLockMgr::Resize(uint32_t target_size) {
  dlock_buffer_.Resize(target_size);
}


/////////////////////////////////////////////////////////////////////////////
// RangeLockMgr - a lock manager that supports range locking
/////////////////////////////////////////////////////////////////////////////

/*
  Storage for locks that are currently held by a transaction.

  Locks are kept in toku::range_buffer because toku::locktree::release_locks()
  accepts that as an argument.

  Note: the list of locks may differ slighly from the contents of the lock
  tree, due to concurrency between lock acquisition, lock release, and lock
  escalation. See MDEV-18227 and RangeLockMgr::UnLockAll for details.
  This property is currently harmless.
*/
class RangeLockList: public PessimisticTransaction::LockStorage {
public:
  virtual ~RangeLockList() {
    buffer_.destroy();
  }

  RangeLockList() : releasing_locks_(false) {
    buffer_.create();
  }

  void append(const DBT *left_key, const DBT *right_key) {
    MutexLock l(&mutex_);
    // there's only one thread that calls this function.
    // the same thread will do lock release.
    assert(!releasing_locks_);
    buffer_.append(left_key, right_key);
  }

  // Ranges that the transaction is holding locks on
  toku::range_buffer buffer_;

  // Synchronization. See RangeLockMgr::UnLockAll for details
  port::Mutex mutex_;
  bool releasing_locks_;
};


void RangeLockMgr::KillLockWait(TransactionID txnid) {
  ltm.kill_waiter((void*)txnid);
}

// Get a range lock on [start_key; end_key] range
Status RangeLockMgr::TryRangeLock(PessimisticTransaction* txn,
                                  uint32_t column_family_id,
                                  const rocksdb::Slice &start_key,
                                  const rocksdb::Slice &end_key,
                                  bool exclusive) {
  toku::lock_request request;
  request.create();
  DBT start_key_dbt, end_key_dbt;

  toku_fill_dbt(&start_key_dbt, start_key.data(), start_key.size());
  toku_fill_dbt(&end_key_dbt, end_key.data(), end_key.size());

  // Use the txnid as "extra" in the lock_request. Then, KillLockWait()
  // will be able to use kill_waiter(txn_id) to kill the wait if needed
  TransactionID wait_txn_id = txn->GetID();

  request.set(lt, (TXNID)txn, &start_key_dbt, &end_key_dbt,
              toku::lock_request::WRITE, false /* not a big txn */,
              (void*)wait_txn_id);
  
  uint64_t killed_time_msec = 0; // TODO: what should this have?
  uint64_t wait_time_msec = txn->GetLockTimeout();
  // convert microseconds to milliseconds
  if (wait_time_msec != (uint64_t)-1)
    wait_time_msec = (wait_time_msec + 500) / 1000;

  request.start();

  /*
    If we are going to wait on the lock, we should set appropriate status in
    the 'txn' object. This is done by the SetWaitingTxn() call below.
    The API we are using are MariaDB's wait notification API, so the way this
    is done is a bit convoluted.
    In MyRocks, the wait details are visible in I_S.rocksdb_trx.
  */
  std::string key_str(start_key.data(), start_key.size());
  struct st_wait_info {
    PessimisticTransaction* txn;
    uint32_t column_family_id;
    std::string *key_ptr;
    autovector<TransactionID> wait_ids;
    bool done= false;

    static void lock_wait_callback(void *cdata, TXNID waiter, TXNID waitee) {
      auto self= (struct st_wait_info*)cdata;
      if (!self->done)
      {
        self->wait_ids.push_back(waitee);
        self->txn->SetWaitingTxn(self->wait_ids, self->column_family_id,
                                 self->key_ptr);
        self->done= true;
      }
    }
  } wait_info;

  wait_info.txn= txn;
  wait_info.column_family_id= column_family_id;
  wait_info.key_ptr= &key_str;
  wait_info.done= false;

  const int r = request.wait(wait_time_msec, killed_time_msec,
                             nullptr, // killed_callback
                             st_wait_info::lock_wait_callback,
                             (void*)&wait_info);

  // Inform the txn that we are no longer waiting:
  txn->ClearWaitingTxn();

  request.destroy();
  switch (r) {
    case 0:
      break; /* fall through */
    case DB_LOCK_NOTGRANTED:
      return Status::TimedOut(Status::SubCode::kLockTimeout);
    case TOKUDB_OUT_OF_LOCKS:
      return Status::Busy(Status::SubCode::kLockLimit);
    case DB_LOCK_DEADLOCK:
      return Status::Busy(Status::SubCode::kDeadlock);
    default:
      assert(0);
      return Status::Busy(Status::SubCode::kLockLimit);
  }

  /* Save the acquired lock in txn->owned_locks */
  if (!txn->owned_locks)
  {
    //create the object
    txn->owned_locks= std::unique_ptr<RangeLockList>(new RangeLockList);
  }
  RangeLockList* range_list= (RangeLockList*)txn->owned_locks.get();
  range_list->append(&start_key_dbt, &end_key_dbt);

  return Status::OK();
}


// Get a singlepoint lock
//   (currently it is the same as getting a range lock)
Status RangeLockMgr::TryLock(PessimisticTransaction* txn,
                             uint32_t column_family_id,
                             const std::string& key, Env* env, 
                             bool exclusive) {
  std::string endpoint;
  convert_key_to_endpoint(rocksdb::Slice(key.data(), key.size()), &endpoint);
  rocksdb::Slice endp_slice(endpoint.data(), endpoint.length());
  return TryRangeLock(txn, column_family_id, endp_slice, endp_slice, exclusive);
}

static void 
range_lock_mgr_release_lock_int(toku::locktree *lt,
                                const PessimisticTransaction* txn,
                                uint32_t column_family_id,
                                const std::string& key) {
  DBT key_dbt; 
  toku_fill_dbt(&key_dbt, key.data(), key.size());
  toku::range_buffer range_buf;
  range_buf.create();
  range_buf.append(&key_dbt, &key_dbt);
  lt->release_locks((TXNID)txn, &range_buf);
  range_buf.destroy();
}

void RangeLockMgr::UnLock(PessimisticTransaction* txn,
                            uint32_t column_family_id,
                            const std::string& key, Env* env) {
  range_lock_mgr_release_lock_int(lt, txn, column_family_id, key);
  toku::lock_request::retry_all_lock_requests(lt, nullptr /* lock_wait_needed_callback */);
}

void RangeLockMgr::UnLock(const PessimisticTransaction* txn,
                          const TransactionKeyMap* key_map, Env* env) {
  //TODO: if we collect all locks in a range buffer and then
  // make one call to lock_tree::release_locks(), will that be faster?
  for (auto& key_map_iter : *key_map) {
    uint32_t column_family_id = key_map_iter.first;
    auto& keys = key_map_iter.second;

    for (auto& key_iter : keys) {
      const std::string& key = key_iter.first;
      range_lock_mgr_release_lock_int(lt, txn, column_family_id, key);
    }
  }

  toku::lock_request::retry_all_lock_requests(lt, nullptr /* lock_wait_needed_callback */);
}

void RangeLockMgr::UnLockAll(const PessimisticTransaction* txn, Env* env) {

  // owned_locks may hold nullptr if the transaction has never acquired any
  // locks.
  if (txn->owned_locks)
  {
    RangeLockList* range_list= (RangeLockList*)txn->owned_locks.get();

    {
      MutexLock l(&range_list->mutex_);
      /*
        The lt->release_locks() call below will walk range_list->buffer_. We
        need to prevent lock escalation callback from replacing
        range_list->buffer_ while we are doing that.

        Additional complication here is internal mutex(es) in the locktree
        (let's call them latches):
        - Lock escalation first obtains latches on the lock tree
        - Then, it calls RangeLockMgr::on_escalate to replace transaction's
          range_list->buffer_.
          = Access to that buffer must be synchronized, so it will want to
          acquire the range_list->mutex_.

        While in this function we would want to do the reverse:
        - Acquire range_list->mutex_ to prevent access to the range_list.
        - Then, lt->release_locks() call will walk through the range_list
        - and acquire latches on parts of the lock tree to remove locks from
          it.

        How do we avoid the deadlock? The idea is that here we set
        releasing_locks_=true, and release the mutex.
        All other users of the range_list must:
        - Acquire the mutex, then check that releasing_locks_=false.
          (the code in this function doesnt do that as there's only one thread
           that releases transaction's locks)
      */
      range_list->releasing_locks_= true;
    }

    // Don't try to call release_locks() if the buffer is empty! if we are
    //  not holding any locks, the lock tree might be in the STO-mode with 
    //  another transaction, and our attempt to release an empty set of locks 
    //  will cause an assertion failure.
    if (range_list->buffer_.get_num_ranges())
      lt->release_locks((TXNID)txn, &range_list->buffer_, true);
    range_list->buffer_.destroy();
    range_list->buffer_.create();
    range_list->releasing_locks_= false;

    toku::lock_request::retry_all_lock_requests(lt, nullptr /* lock_wait_needed_callback */);
  }
}

int RangeLockMgr::compare_dbt_endpoints(__toku_db*, void *arg,
                                        const DBT *a_key,
                                        const DBT *b_key) {
  RangeLockMgr* mgr= (RangeLockMgr*) arg;
  return mgr->compare_endpoints((const char*)a_key->data, a_key->size,
                                (const char*)b_key->data, b_key->size);
}


RangeLockMgr::RangeLockMgr(TransactionDB* txn_db) : my_txn_db(txn_db) {
  ltm.create(on_create, on_destroy, on_escalate, NULL);
  lt= nullptr;
}


/*
  @brief  Lock Escalation Callback function

  @param txnid   Transaction whose locks got escalated
  @param lt      Lock Tree where escalation is happening (currently there is only one)
  @param buffer  Escalation result: list of locks that this transaction now
                 owns in this lock tree.
  @param extra   Callback context
*/

void RangeLockMgr::on_escalate(TXNID txnid, const locktree *lt,
                               const range_buffer &buffer, void *extra) {
  auto txn= (PessimisticTransaction*)txnid;

  RangeLockList* trx_locks= (RangeLockList*)txn->owned_locks.get();

  MutexLock l(&trx_locks->mutex_);
  if (trx_locks->releasing_locks_) {
    /*
      Do nothing. The transaction is releasing its locks, so it will not care
      about having a correct list of ranges. (In TokuDB,
      toku_db_txn_escalate_callback() makes use of this property, too)
    */
    return;
  }

  // TODO: are we tracking this mem: lt->get_manager()->note_mem_released(trx_locks->ranges.buffer->total_memory_size());
  trx_locks->buffer_.destroy();
  trx_locks->buffer_.create();
  toku::range_buffer::iterator iter(&buffer);
  toku::range_buffer::iterator::record rec;
  while (iter.current(&rec)) {
    trx_locks->buffer_.append(rec.get_left_key(), rec.get_right_key());
    iter.next();
  }
  // TODO: same as above: lt->get_manager()->note_mem_used(ranges.buffer->total_memory_size());
}

void 
RangeLockMgr::set_endpoint_cmp_functions(convert_key_to_endpoint_func cvt_func,
                                         compare_endpoints_func cmp_func) {
  convert_key_to_endpoint= cvt_func;
  compare_endpoints= cmp_func;

  // The rest is like a constructor:
  assert(!lt);

  toku::comparator cmp;
  cmp.create(compare_dbt_endpoints, (void*)this, NULL);
  DICTIONARY_ID dict_id = { .dictid = 1 };
  lt= ltm.get_lt(dict_id, cmp , /* on_create_extra*/nullptr);
}


uint64_t RangeLockMgr::get_escalation_count() {
  LTM_STATUS_S ltm_status_test;
  ltm.get_status(&ltm_status_test);
  
  // Searching status variable by its string name is how Toku's unit tests
  // do it (why didn't they make LTM_ESCALATION_COUNT constant visible?)
  TOKU_ENGINE_STATUS_ROW key_status = NULL;
  // lookup keyname in status
  for (int i = 0; ; i++) {
      TOKU_ENGINE_STATUS_ROW status = &ltm_status_test.status[i];
      if (status->keyname == NULL)
          break;
      if (strcmp(status->keyname, "LTM_ESCALATION_COUNT") == 0) {
          key_status = status;
          break;
      }
  }
  assert(key_status);
  return key_status->value.num;
}


struct LOCK_PRINT_CONTEXT {
  TransactionLockMgr::LockStatusData *data;
  // this will not be needed when locks are per-column-family:
  uint32_t cfh_id;
};

static 
void push_into_lock_status_data(void* param, const DBT *left, 
                                const DBT *right, TXNID txnid_arg) {
  struct LOCK_PRINT_CONTEXT *ctx= (LOCK_PRINT_CONTEXT*)param;
  struct KeyLockInfo info;

  info.key.append((const char*)left->data, (size_t)left->size);
  info.exclusive= true;

  if (!(left->size == right->size && 
        !memcmp(left->data, right->data, left->size)))
  {
    // not a single-point lock 
    info.has_key2= true;
    info.key2.append((const char*)right->data, right->size);
  }

  TXNID txnid= ((PessimisticTransaction*)txnid_arg)->GetID();
  info.ids.push_back(txnid);
  ctx->data->insert({ctx->cfh_id, info});
}


TransactionLockMgr::LockStatusData RangeLockMgr::GetLockStatusData() {
  TransactionLockMgr::LockStatusData data;
  LOCK_PRINT_CONTEXT ctx = {&data, GetColumnFamilyID(my_txn_db->DefaultColumnFamily()) };
  lt->dump_locks((void*)&ctx, push_into_lock_status_data);
  return data;
}

}  // namespace ROCKSDB_NAMESPACE
#endif  // ROCKSDB_LITE

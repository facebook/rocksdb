//  Copyright (c) 2015, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#ifndef ROCKSDB_LITE

#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif

#include "utilities/transactions/transaction_lock_mgr.h"

#include <inttypes.h>

#include <algorithm>
#include <condition_variable>
#include <functional>
#include <mutex>
#include <string>
#include <vector>

#include "rocksdb/slice.h"
#include "util/autovector.h"
#include "util/murmurhash.h"
#include "util/thread_local.h"

namespace rocksdb {

struct LockInfo {
  TransactionID txn_id;
  uint64_t
      expiration_time;  // Transaction locks are not valid after this time in ms
  LockInfo(TransactionID id, uint64_t time)
      : txn_id(id), expiration_time(time) {}
  LockInfo(const LockInfo& lock_info)
      : txn_id(lock_info.txn_id), expiration_time(lock_info.expiration_time) {}
};

struct LockMapStripe {
  // Mutex must be held before modifying keys map
  std::timed_mutex stripe_mutex;

  // Condition Variable per stripe for waiting on a lock
  std::condition_variable_any stripe_cv;

  // Locked keys mapped to the info about the transactions that locked them.
  // TODO(agiardullo): Explore performance of other data structures.
  std::unordered_map<std::string, LockInfo> keys;
};

// Map of #num_stripes LockMapStripes
struct LockMap {
  explicit LockMap(size_t num_stripes)
      : num_stripes_(num_stripes), lock_map_stripes_(num_stripes) {}

  LockMap(const LockMap& lock_map)
      : num_stripes_(lock_map.num_stripes_), lock_map_stripes_(num_stripes_) {}

  // Number of sepearate LockMapStripes to create, each with their own Mutex
  const size_t num_stripes_;

  // Count of keys that are currently locked in this column family.
  // (Only maintained if TransactionLockMgr::max_num_locks_ is positive.)
  std::atomic<int64_t> lock_cnt{0};

  std::vector<LockMapStripe> lock_map_stripes_;

  size_t GetStripe(const std::string& key) const;
};

namespace {
void UnrefLockMapsCache(void* ptr) {
  // Called when a thread exits or a ThreadLocalPtr gets destroyed.
  auto lock_maps_cache =
      static_cast<std::unordered_map<uint32_t, std::shared_ptr<LockMap>>*>(ptr);
  delete lock_maps_cache;
}
}  // anonymous namespace

TransactionLockMgr::TransactionLockMgr(size_t default_num_stripes,
                                       int64_t max_num_locks)
    : default_num_stripes_(default_num_stripes),
      max_num_locks_(max_num_locks),
      lock_maps_cache_(new ThreadLocalPtr(&UnrefLockMapsCache)) {}

TransactionLockMgr::~TransactionLockMgr() {}

size_t LockMap::GetStripe(const std::string& key) const {
  assert(num_stripes_ > 0);
  static murmur_hash hash;
  size_t stripe = hash(key) % num_stripes_;
  return stripe;
}

void TransactionLockMgr::AddColumnFamily(uint32_t column_family_id) {
  InstrumentedMutexLock l(&lock_map_mutex_);

  if (lock_maps_.find(column_family_id) == lock_maps_.end()) {
    lock_maps_.emplace(
        column_family_id,
        std::shared_ptr<LockMap>(new LockMap(default_num_stripes_)));
  } else {
    // column_family already exists in lock map
    assert(false);
  }
}

void TransactionLockMgr::RemoveColumnFamily(uint32_t column_family_id) {
  // Remove lock_map for this column family.  Since the lock map is stored
  // as a shared ptr, concurrent transactions can still keep keep using it
  // until they release their reference to it.
  {
    InstrumentedMutexLock l(&lock_map_mutex_);

    auto lock_maps_iter = lock_maps_.find(column_family_id);
    assert(lock_maps_iter != lock_maps_.end());

    lock_maps_.erase(lock_maps_iter);
  }  // lock_map_mutex_

  // Clear all thread-local caches
  autovector<void*> local_caches;
  lock_maps_cache_->Scrape(&local_caches, nullptr);
  for (auto cache : local_caches) {
    delete static_cast<LockMaps*>(cache);
  }
}

// Look up the LockMap shared_ptr for a given column_family_id.
// Note:  The LockMap is only valid as long as the caller is still holding on
//   to the returned shared_ptr.
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
// If false, returns the number of microseconds until expiration in
// *wait_time_us, or 0 if no expiration.
bool TransactionLockMgr::IsLockExpired(const LockInfo& lock_info, Env* env,
                                       uint64_t* wait_time_us) {
  auto now = env->NowMicros();

  bool expired = (lock_info.expiration_time > 0 &&
                  lock_info.expiration_time * 1000 <= now);

  if (!expired && lock_info.expiration_time > 0 && wait_time_us != nullptr) {
    // return how many microseconds until lock will be expired
    *wait_time_us = (lock_info.expiration_time * 1000 - now);
  }

  return expired;
}

Status TransactionLockMgr::TryLock(const TransactionImpl* txn,
                                   uint32_t column_family_id,
                                   const std::string& key, Env* env) {
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
  LockMapStripe* stripe = &lock_map->lock_map_stripes_.at(stripe_num);

  LockInfo lock_info(txn->GetTxnID(), txn->GetExpirationTime());
  int64_t timeout = txn->GetLockTimeout();

  return AcquireWithTimeout(lock_map, stripe, key, env, timeout, lock_info);
}

// Helper function for TryLock().
Status TransactionLockMgr::AcquireWithTimeout(LockMap* lock_map,
                                              LockMapStripe* stripe,
                                              const std::string& key, Env* env,
                                              int64_t timeout,
                                              const LockInfo& lock_info) {
  std::chrono::system_clock::time_point end_time;

  if (timeout > 0) {
    end_time =
        std::chrono::system_clock::now() + std::chrono::milliseconds(timeout);
  }

  bool locked = true;
  if (timeout == 0) {
    // If timeout is 0, we do not wait to acquire the lock if it is not
    // available
    locked = stripe->stripe_mutex.try_lock();
  } else if (timeout < 0) {
    // If timeout is negative, we wait indefinitely to acquire the lock
    stripe->stripe_mutex.lock();
  } else {
    // If timeout is positive, we attempt to acquire the lock unless we timeout
    locked = stripe->stripe_mutex.try_lock_until(end_time);
  }

  if (!locked) {
    // timeout acquiring mutex
    return Status::TimedOut("Timeout Acquiring Mutex");
  }

  // Acquire lock if we are able to
  uint64_t wait_time_us = 0;
  Status result =
      AcquireLocked(lock_map, stripe, key, env, lock_info, &wait_time_us);

  if (!result.ok() && timeout != 0) {
    // If we weren't able to acquire the lock, we will keep retrying as long
    // as the
    // timeout allows.
    bool timed_out = false;
    do {
      // Check to see if the lock expires sooner than our timeout.
      std::chrono::system_clock::time_point wait_time_end;
      if (wait_time_us > 0 &&
          (timeout < 0 ||
           wait_time_us < static_cast<uint64_t>(timeout * 1000))) {
        wait_time_end = std::chrono::system_clock::now() +
                        std::chrono::microseconds(wait_time_us);
        if (timeout > 0 && wait_time_end >= end_time) {
          // lock expiration time is after our timeout.
          wait_time_us = 0;
        }
      } else {
        wait_time_us = 0;
      }

      if (wait_time_us > 0) {
        // Wait up to the locks current expiration time
        stripe->stripe_cv.wait_until(stripe->stripe_mutex, wait_time_end);
      } else if (timeout > 0) {
        // Wait until we timeout
        auto cv_status =
            stripe->stripe_cv.wait_until(stripe->stripe_mutex, end_time);

        if (cv_status == std::cv_status::timeout) {
          timed_out = true;
          // Even though we timed out, we will still make one more attempt to
          // acquire lock below (it is possible the lock expired and we
          // were never signaled).
        }
      } else {
        // No wait timeout.
        stripe->stripe_cv.wait(stripe->stripe_mutex);
      }

      result =
          AcquireLocked(lock_map, stripe, key, env, lock_info, &wait_time_us);
    } while (!result.ok() && !timed_out);
  }

  stripe->stripe_mutex.unlock();

  return result;
}

// Try to lock this key after we have acquired the mutex.
// Returns the number of microseconds until expiration in *wait_time_us,
//  or 0 if no expiration.
// REQUIRED:  Stripe mutex must be held.
Status TransactionLockMgr::AcquireLocked(LockMap* lock_map,
                                         LockMapStripe* stripe,
                                         const std::string& key, Env* env,
                                         const LockInfo& txn_lock_info,
                                         uint64_t* wait_time_us) {
  Status result;
  // Check if this key is already locked
  if (stripe->keys.find(key) != stripe->keys.end()) {
    // Lock already held

    LockInfo& lock_info = stripe->keys.at(key);
    if (lock_info.txn_id != txn_lock_info.txn_id) {
      // locked by another txn.  Check if it's expired
      if (IsLockExpired(lock_info, env, wait_time_us)) {
        // lock is expired, can steal it
        lock_info.txn_id = txn_lock_info.txn_id;
        lock_info.expiration_time = txn_lock_info.expiration_time;
        // lock_cnt does not change
      } else {
        result = Status::TimedOut("lock held");
      }
    }
  } else {  // Lock not held.
    // Check lock limit
    if (max_num_locks_ > 0 &&
        lock_map->lock_cnt.load(std::memory_order_acquire) >= max_num_locks_) {
      result =
          Status::Busy("Failed to acquire lock due to max_num_locks limit");
    } else {
      // acquire lock
      stripe->keys.insert({key, txn_lock_info});

      // Maintain lock count if there is a limit on the number of locks
      if (max_num_locks_) {
        lock_map->lock_cnt++;
      }
    }
  }

  return result;
}

void TransactionLockMgr::UnLock(TransactionImpl* txn, uint32_t column_family_id,
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
  LockMapStripe* stripe = &lock_map->lock_map_stripes_.at(stripe_num);

  TransactionID txn_id = txn->GetTxnID();
  {
    std::lock_guard<std::timed_mutex> lock(stripe->stripe_mutex);

    const auto& iter = stripe->keys.find(key);
    if (iter != stripe->keys.end() && iter->second.txn_id == txn_id) {
      // Found the key we locked.  unlock it.
      stripe->keys.erase(iter);
      if (max_num_locks_ > 0) {
        // Maintain lock count if there is a limit on the number of locks.
        assert(lock_map->lock_cnt.load(std::memory_order_relaxed) > 0);
        lock_map->lock_cnt--;
      }
    } else {
      // This key is either not locked or locked by someone else.  This should
      // only happen if the unlocking transaction has expired.
      assert(txn->GetExpirationTime() > 0 &&
             txn->GetExpirationTime() * 1000 < env->NowMicros());
    }
  }  // stripe_mutex unlocked

  // Signal waiting threads to retry locking
  stripe->stripe_cv.notify_all();
}

void TransactionLockMgr::UnLock(const TransactionImpl* txn,
                                const TransactionKeyMap* key_map, Env* env) {
  TransactionID txn_id = txn->GetTxnID();

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
      LockMapStripe* stripe = &lock_map->lock_map_stripes_.at(stripe_num);

      {
        std::lock_guard<std::timed_mutex> lock(stripe->stripe_mutex);

        for (const std::string* key : stripe_keys) {
          const auto& iter = stripe->keys.find(*key);
          if (iter != stripe->keys.end() && iter->second.txn_id == txn_id) {
            // Found the key we locked.  unlock it.
            stripe->keys.erase(iter);
            if (max_num_locks_ > 0) {
              // Maintain lock count if there is a limit on the number of locks.
              assert(lock_map->lock_cnt.load(std::memory_order_relaxed) > 0);
              lock_map->lock_cnt--;
            }
          } else {
            // This key is either not locked or locked by someone else.  This
            // should only
            // happen if the unlocking transaction has expired.
            assert(txn->GetExpirationTime() > 0 &&
                   txn->GetExpirationTime() * 1000 < env->NowMicros());
          }
        }
      }  // stripe_mutex unlocked

      // Signal waiting threads to retry locking
      stripe->stripe_cv.notify_all();
    }
  }
}

}  //  namespace rocksdb
#endif  // ROCKSDB_LITE

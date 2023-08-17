//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "cache/lru_cache.h"

#include <cassert>
#include <cstdint>
#include <cstdio>
#include <cstdlib>

#include "monitoring/perf_context_imp.h"
#include "monitoring/statistics.h"
#include "port/lang.h"
#include "util/distributed_mutex.h"

namespace ROCKSDB_NAMESPACE {
namespace lru_cache {

namespace {
// A distinct pointer value for marking "dummy" cache entries
struct DummyValue {
  char val[12] = "kDummyValue";
};
DummyValue kDummyValue{};
}  // namespace

LRUHandleTable::LRUHandleTable(int max_upper_hash_bits,
                               MemoryAllocator* allocator)
    : length_bits_(/* historical starting size*/ 4),
      list_(new LRUHandle* [size_t{1} << length_bits_] {}),
      elems_(0),
      max_length_bits_(max_upper_hash_bits),
      allocator_(allocator) {}

LRUHandleTable::~LRUHandleTable() {
  auto alloc = allocator_;
  ApplyToEntriesRange(
      [alloc](LRUHandle* h) {
        if (!h->HasRefs()) {
          h->Free(alloc);
        }
      },
      0, size_t{1} << length_bits_);
}

LRUHandle* LRUHandleTable::Lookup(const Slice& key, uint32_t hash) {
  return *FindPointer(key, hash);
}

LRUHandle* LRUHandleTable::Insert(LRUHandle* h) {
  LRUHandle** ptr = FindPointer(h->key(), h->hash);
  LRUHandle* old = *ptr;
  h->next_hash = (old == nullptr ? nullptr : old->next_hash);
  *ptr = h;
  if (old == nullptr) {
    ++elems_;
    if ((elems_ >> length_bits_) > 0) {  // elems_ >= length
      // Since each cache entry is fairly large, we aim for a small
      // average linked list length (<= 1).
      Resize();
    }
  }
  return old;
}

LRUHandle* LRUHandleTable::Remove(const Slice& key, uint32_t hash) {
  LRUHandle** ptr = FindPointer(key, hash);
  LRUHandle* result = *ptr;
  if (result != nullptr) {
    *ptr = result->next_hash;
    --elems_;
  }
  return result;
}

LRUHandle** LRUHandleTable::FindPointer(const Slice& key, uint32_t hash) {
  LRUHandle** ptr = &list_[hash >> (32 - length_bits_)];
  while (*ptr != nullptr && ((*ptr)->hash != hash || key != (*ptr)->key())) {
    ptr = &(*ptr)->next_hash;
  }
  return ptr;
}

void LRUHandleTable::Resize() {
  if (length_bits_ >= max_length_bits_) {
    // Due to reaching limit of hash information, if we made the table bigger,
    // we would allocate more addresses but only the same number would be used.
    return;
  }
  if (length_bits_ >= 31) {
    // Avoid undefined behavior shifting uint32_t by 32.
    return;
  }

  uint32_t old_length = uint32_t{1} << length_bits_;
  int new_length_bits = length_bits_ + 1;
  std::unique_ptr<LRUHandle* []> new_list {
    new LRUHandle* [size_t{1} << new_length_bits] {}
  };
  uint32_t count = 0;
  for (uint32_t i = 0; i < old_length; i++) {
    LRUHandle* h = list_[i];
    while (h != nullptr) {
      LRUHandle* next = h->next_hash;
      uint32_t hash = h->hash;
      LRUHandle** ptr = &new_list[hash >> (32 - new_length_bits)];
      h->next_hash = *ptr;
      *ptr = h;
      h = next;
      count++;
    }
  }
  assert(elems_ == count);
  list_ = std::move(new_list);
  length_bits_ = new_length_bits;
}

LRUCacheShard::LRUCacheShard(size_t capacity, bool strict_capacity_limit,
                             double high_pri_pool_ratio,
                             double low_pri_pool_ratio, bool use_adaptive_mutex,
                             CacheMetadataChargePolicy metadata_charge_policy,
                             int max_upper_hash_bits,
                             MemoryAllocator* allocator,
                             SecondaryCache* secondary_cache)
    : CacheShardBase(metadata_charge_policy),
      capacity_(0),
      high_pri_pool_usage_(0),
      low_pri_pool_usage_(0),
      strict_capacity_limit_(strict_capacity_limit),
      high_pri_pool_ratio_(high_pri_pool_ratio),
      high_pri_pool_capacity_(0),
      low_pri_pool_ratio_(low_pri_pool_ratio),
      low_pri_pool_capacity_(0),
      table_(max_upper_hash_bits, allocator),
      usage_(0),
      lru_usage_(0),
      mutex_(use_adaptive_mutex),
      secondary_cache_(secondary_cache) {
  // Make empty circular linked list.
  lru_.next = &lru_;
  lru_.prev = &lru_;
  lru_low_pri_ = &lru_;
  lru_bottom_pri_ = &lru_;
  SetCapacity(capacity);
}

void LRUCacheShard::EraseUnRefEntries() {
  autovector<LRUHandle*> last_reference_list;
  {
    DMutexLock l(mutex_);
    while (lru_.next != &lru_) {
      LRUHandle* old = lru_.next;
      // LRU list contains only elements which can be evicted.
      assert(old->InCache() && !old->HasRefs());
      LRU_Remove(old);
      table_.Remove(old->key(), old->hash);
      old->SetInCache(false);
      assert(usage_ >= old->total_charge);
      usage_ -= old->total_charge;
      last_reference_list.push_back(old);
    }
  }

  for (auto entry : last_reference_list) {
    entry->Free(table_.GetAllocator());
  }
}

void LRUCacheShard::ApplyToSomeEntries(
    const std::function<void(const Slice& key, Cache::ObjectPtr value,
                             size_t charge,
                             const Cache::CacheItemHelper* helper)>& callback,
    size_t average_entries_per_lock, size_t* state) {
  // The state is essentially going to be the starting hash, which works
  // nicely even if we resize between calls because we use upper-most
  // hash bits for table indexes.
  DMutexLock l(mutex_);
  int length_bits = table_.GetLengthBits();
  size_t length = size_t{1} << length_bits;

  assert(average_entries_per_lock > 0);
  // Assuming we are called with same average_entries_per_lock repeatedly,
  // this simplifies some logic (index_end will not overflow).
  assert(average_entries_per_lock < length || *state == 0);

  size_t index_begin = *state >> (sizeof(size_t) * 8u - length_bits);
  size_t index_end = index_begin + average_entries_per_lock;
  if (index_end >= length) {
    // Going to end
    index_end = length;
    *state = SIZE_MAX;
  } else {
    *state = index_end << (sizeof(size_t) * 8u - length_bits);
  }

  table_.ApplyToEntriesRange(
      [callback,
       metadata_charge_policy = metadata_charge_policy_](LRUHandle* h) {
        callback(h->key(), h->value, h->GetCharge(metadata_charge_policy),
                 h->helper);
      },
      index_begin, index_end);
}

void LRUCacheShard::TEST_GetLRUList(LRUHandle** lru, LRUHandle** lru_low_pri,
                                    LRUHandle** lru_bottom_pri) {
  DMutexLock l(mutex_);
  *lru = &lru_;
  *lru_low_pri = lru_low_pri_;
  *lru_bottom_pri = lru_bottom_pri_;
}

size_t LRUCacheShard::TEST_GetLRUSize() {
  DMutexLock l(mutex_);
  LRUHandle* lru_handle = lru_.next;
  size_t lru_size = 0;
  while (lru_handle != &lru_) {
    lru_size++;
    lru_handle = lru_handle->next;
  }
  return lru_size;
}

double LRUCacheShard::GetHighPriPoolRatio() {
  DMutexLock l(mutex_);
  return high_pri_pool_ratio_;
}

double LRUCacheShard::GetLowPriPoolRatio() {
  DMutexLock l(mutex_);
  return low_pri_pool_ratio_;
}

void LRUCacheShard::LRU_Remove(LRUHandle* e) {
  assert(e->next != nullptr);
  assert(e->prev != nullptr);
  if (lru_low_pri_ == e) {
    lru_low_pri_ = e->prev;
  }
  if (lru_bottom_pri_ == e) {
    lru_bottom_pri_ = e->prev;
  }
  e->next->prev = e->prev;
  e->prev->next = e->next;
  e->prev = e->next = nullptr;
  assert(lru_usage_ >= e->total_charge);
  lru_usage_ -= e->total_charge;
  assert(!e->InHighPriPool() || !e->InLowPriPool());
  if (e->InHighPriPool()) {
    assert(high_pri_pool_usage_ >= e->total_charge);
    high_pri_pool_usage_ -= e->total_charge;
  } else if (e->InLowPriPool()) {
    assert(low_pri_pool_usage_ >= e->total_charge);
    low_pri_pool_usage_ -= e->total_charge;
  }
}

void LRUCacheShard::LRU_Insert(LRUHandle* e) {
  assert(e->next == nullptr);
  assert(e->prev == nullptr);
  if (high_pri_pool_ratio_ > 0 && (e->IsHighPri() || e->HasHit())) {
    // Inset "e" to head of LRU list.
    e->next = &lru_;
    e->prev = lru_.prev;
    e->prev->next = e;
    e->next->prev = e;
    e->SetInHighPriPool(true);
    e->SetInLowPriPool(false);
    high_pri_pool_usage_ += e->total_charge;
    MaintainPoolSize();
  } else if (low_pri_pool_ratio_ > 0 &&
             (e->IsHighPri() || e->IsLowPri() || e->HasHit())) {
    // Insert "e" to the head of low-pri pool.
    e->next = lru_low_pri_->next;
    e->prev = lru_low_pri_;
    e->prev->next = e;
    e->next->prev = e;
    e->SetInHighPriPool(false);
    e->SetInLowPriPool(true);
    low_pri_pool_usage_ += e->total_charge;
    MaintainPoolSize();
    lru_low_pri_ = e;
  } else {
    // Insert "e" to the head of bottom-pri pool.
    e->next = lru_bottom_pri_->next;
    e->prev = lru_bottom_pri_;
    e->prev->next = e;
    e->next->prev = e;
    e->SetInHighPriPool(false);
    e->SetInLowPriPool(false);
    // if the low-pri pool is empty, lru_low_pri_ also needs to be updated.
    if (lru_bottom_pri_ == lru_low_pri_) {
      lru_low_pri_ = e;
    }
    lru_bottom_pri_ = e;
  }
  lru_usage_ += e->total_charge;
}

void LRUCacheShard::MaintainPoolSize() {
  while (high_pri_pool_usage_ > high_pri_pool_capacity_) {
    // Overflow last entry in high-pri pool to low-pri pool.
    lru_low_pri_ = lru_low_pri_->next;
    assert(lru_low_pri_ != &lru_);
    lru_low_pri_->SetInHighPriPool(false);
    lru_low_pri_->SetInLowPriPool(true);
    assert(high_pri_pool_usage_ >= lru_low_pri_->total_charge);
    high_pri_pool_usage_ -= lru_low_pri_->total_charge;
    low_pri_pool_usage_ += lru_low_pri_->total_charge;
  }

  while (low_pri_pool_usage_ > low_pri_pool_capacity_) {
    // Overflow last entry in low-pri pool to bottom-pri pool.
    lru_bottom_pri_ = lru_bottom_pri_->next;
    assert(lru_bottom_pri_ != &lru_);
    lru_bottom_pri_->SetInHighPriPool(false);
    lru_bottom_pri_->SetInLowPriPool(false);
    assert(low_pri_pool_usage_ >= lru_bottom_pri_->total_charge);
    low_pri_pool_usage_ -= lru_bottom_pri_->total_charge;
  }
}

void LRUCacheShard::EvictFromLRU(size_t charge,
                                 autovector<LRUHandle*>* deleted) {
  while ((usage_ + charge) > capacity_ && lru_.next != &lru_) {
    LRUHandle* old = lru_.next;
    // LRU list contains only elements which can be evicted.
    assert(old->InCache() && !old->HasRefs());
    LRU_Remove(old);
    table_.Remove(old->key(), old->hash);
    old->SetInCache(false);
    assert(usage_ >= old->total_charge);
    usage_ -= old->total_charge;
    deleted->push_back(old);
  }
}

void LRUCacheShard::TryInsertIntoSecondaryCache(
    autovector<LRUHandle*> evicted_handles) {
  for (auto entry : evicted_handles) {
    if (secondary_cache_ && entry->IsSecondaryCacheCompatible() &&
        !entry->IsInSecondaryCache()) {
      secondary_cache_->Insert(entry->key(), entry->value, entry->helper)
          .PermitUncheckedError();
    }
    // Free the entries here outside of mutex for performance reasons.
    entry->Free(table_.GetAllocator());
  }
}

void LRUCacheShard::SetCapacity(size_t capacity) {
  autovector<LRUHandle*> last_reference_list;
  {
    DMutexLock l(mutex_);
    capacity_ = capacity;
    high_pri_pool_capacity_ = capacity_ * high_pri_pool_ratio_;
    low_pri_pool_capacity_ = capacity_ * low_pri_pool_ratio_;
    EvictFromLRU(0, &last_reference_list);
  }

  TryInsertIntoSecondaryCache(last_reference_list);
}

void LRUCacheShard::SetStrictCapacityLimit(bool strict_capacity_limit) {
  DMutexLock l(mutex_);
  strict_capacity_limit_ = strict_capacity_limit;
}

Status LRUCacheShard::InsertItem(LRUHandle* e, LRUHandle** handle,
                                 bool free_handle_on_fail) {
  Status s = Status::OK();
  autovector<LRUHandle*> last_reference_list;

  {
    DMutexLock l(mutex_);

    // Free the space following strict LRU policy until enough space
    // is freed or the lru list is empty.
    EvictFromLRU(e->total_charge, &last_reference_list);

    if ((usage_ + e->total_charge) > capacity_ &&
        (strict_capacity_limit_ || handle == nullptr)) {
      e->SetInCache(false);
      if (handle == nullptr) {
        // Don't insert the entry but still return ok, as if the entry inserted
        // into cache and get evicted immediately.
        last_reference_list.push_back(e);
      } else {
        if (free_handle_on_fail) {
          free(e);
          *handle = nullptr;
        }
        s = Status::MemoryLimit("Insert failed due to LRU cache being full.");
      }
    } else {
      // Insert into the cache. Note that the cache might get larger than its
      // capacity if not enough space was freed up.
      LRUHandle* old = table_.Insert(e);
      usage_ += e->total_charge;
      if (old != nullptr) {
        s = Status::OkOverwritten();
        assert(old->InCache());
        old->SetInCache(false);
        if (!old->HasRefs()) {
          // old is on LRU because it's in cache and its reference count is 0.
          LRU_Remove(old);
          assert(usage_ >= old->total_charge);
          usage_ -= old->total_charge;
          last_reference_list.push_back(old);
        }
      }
      if (handle == nullptr) {
        LRU_Insert(e);
      } else {
        // If caller already holds a ref, no need to take one here.
        if (!e->HasRefs()) {
          e->Ref();
        }
        *handle = e;
      }
    }
  }

  TryInsertIntoSecondaryCache(last_reference_list);

  return s;
}

void LRUCacheShard::Promote(LRUHandle* e) {
  SecondaryCacheResultHandle* secondary_handle = e->sec_handle;

  assert(secondary_handle->IsReady());
  // e is not thread-shared here; OK to modify "immutable" fields as well as
  // "mutable" (normally requiring mutex)
  e->SetIsPending(false);
  e->value = secondary_handle->Value();
  assert(e->total_charge == 0);
  size_t value_size = secondary_handle->Size();
  delete secondary_handle;

  if (e->value) {
    e->CalcTotalCharge(value_size, metadata_charge_policy_);
    Status s;
    if (e->IsStandalone()) {
      assert(secondary_cache_ && secondary_cache_->SupportForceErase());

      // Insert a dummy handle and return a standalone handle to caller.
      // Charge the standalone handle.
      autovector<LRUHandle*> last_reference_list;
      bool free_standalone_handle{false};
      {
        DMutexLock l(mutex_);

        // Free the space following strict LRU policy until enough space
        // is freed or the lru list is empty.
        EvictFromLRU(e->total_charge, &last_reference_list);

        if ((usage_ + e->total_charge) > capacity_ && strict_capacity_limit_) {
          free_standalone_handle = true;
        } else {
          usage_ += e->total_charge;
        }
      }

      TryInsertIntoSecondaryCache(last_reference_list);
      if (free_standalone_handle) {
        e->Unref();
        e->Free(table_.GetAllocator());
        e = nullptr;
      } else {
        PERF_COUNTER_ADD(block_cache_standalone_handle_count, 1);
      }

      // Insert a dummy handle into the primary cache. This dummy handle is
      // not IsSecondaryCacheCompatible().
      // FIXME? This should not overwrite an existing non-dummy entry in the
      // rare case that one exists
      Cache::Priority priority =
          e->IsHighPri() ? Cache::Priority::HIGH : Cache::Priority::LOW;
      s = Insert(e->key(), e->hash, &kDummyValue, &kNoopCacheItemHelper,
                 /*charge=*/0,
                 /*handle=*/nullptr, priority);
    } else {
      e->SetInCache(true);
      LRUHandle* handle = e;
      // This InsertItem() could fail if the cache is over capacity and
      // strict_capacity_limit_ is true. In such a case, we don't want
      // InsertItem() to free the handle, since the item is already in memory
      // and the caller will most likely just read it from disk if we erase it
      // here.
      s = InsertItem(e, &handle, /*free_handle_on_fail=*/false);
      if (s.ok()) {
        PERF_COUNTER_ADD(block_cache_real_handle_count, 1);
      }
    }

    if (!s.ok()) {
      // Item is in memory, but not accounted against the cache capacity.
      // When the handle is released, the item should get deleted.
      assert(!e->InCache());
    }
  } else {
    // Secondary cache lookup failed. The caller will take care of detecting
    // this and eventually releasing e.
    assert(!e->value);
    assert(!e->InCache());
  }
}

LRUHandle* LRUCacheShard::Lookup(const Slice& key, uint32_t hash,
                                 const Cache::CacheItemHelper* helper,
                                 Cache::CreateContext* create_context,
                                 Cache::Priority priority, bool wait,
                                 Statistics* stats) {
  LRUHandle* e = nullptr;
  bool found_dummy_entry{false};
  {
    DMutexLock l(mutex_);
    e = table_.Lookup(key, hash);
    if (e != nullptr) {
      assert(e->InCache());
      if (e->value == &kDummyValue) {
        // For a dummy handle, if it was retrieved from secondary cache,
        // it may still exist in secondary cache.
        // If the handle exists in secondary cache, the value should be
        // erased from sec cache and be inserted into primary cache.
        found_dummy_entry = true;
        // Let the dummy entry be overwritten
        e = nullptr;
      } else {
        if (!e->HasRefs()) {
          // The entry is in LRU since it's in hash and has no external
          // references.
          LRU_Remove(e);
        }
        e->Ref();
        e->SetHit();
      }
    }
  }

  // If handle table lookup failed or the handle is a dummy one, allocate
  // a handle outside the mutex if we re going to lookup in the secondary cache.
  //
  // When a block is firstly Lookup from CompressedSecondaryCache, we just
  // insert a dummy block into the primary cache (charging the actual size of
  // the block) and don't erase the block from CompressedSecondaryCache. A
  // standalone handle is returned to the caller. Only if the block is hit
  // again, we erase it from CompressedSecondaryCache and add it into the
  // primary cache.
  if (!e && secondary_cache_ && helper && helper->create_cb) {
    bool is_in_sec_cache{false};
    std::unique_ptr<SecondaryCacheResultHandle> secondary_handle =
        secondary_cache_->Lookup(key, helper, create_context, wait,
                                 found_dummy_entry, is_in_sec_cache);
    if (secondary_handle != nullptr) {
      e = static_cast<LRUHandle*>(malloc(sizeof(LRUHandle) - 1 + key.size()));

      e->m_flags = 0;
      e->im_flags = 0;
      e->helper = helper;
      e->key_length = key.size();
      e->hash = hash;
      e->refs = 0;
      e->next = e->prev = nullptr;
      e->SetPriority(priority);
      memcpy(e->key_data, key.data(), key.size());
      e->value = nullptr;
      e->sec_handle = secondary_handle.release();
      e->total_charge = 0;
      e->Ref();
      e->SetIsInSecondaryCache(is_in_sec_cache);
      e->SetIsStandalone(secondary_cache_->SupportForceErase() &&
                         !found_dummy_entry);

      if (wait) {
        Promote(e);
        if (e) {
          if (!e->value) {
            // The secondary cache returned a handle, but the lookup failed.
            e->Unref();
            e->Free(table_.GetAllocator());
            e = nullptr;
          } else {
            PERF_COUNTER_ADD(secondary_cache_hit_count, 1);
            RecordTick(stats, SECONDARY_CACHE_HITS);
          }
        }
      } else {
        // If wait is false, we always return a handle and let the caller
        // release the handle after checking for success or failure.
        e->SetIsPending(true);
        // This may be slightly inaccurate, if the lookup eventually fails.
        // But the probability is very low.
        PERF_COUNTER_ADD(secondary_cache_hit_count, 1);
        RecordTick(stats, SECONDARY_CACHE_HITS);
      }
    } else {
      // Caller will most likely overwrite the dummy entry with an Insert
      // after this Lookup fails
      assert(e == nullptr);
    }
  }
  return e;
}

bool LRUCacheShard::Ref(LRUHandle* e) {
  DMutexLock l(mutex_);
  // To create another reference - entry must be already externally referenced.
  assert(e->HasRefs());
  // Pending handles are not for sharing
  assert(!e->IsPending());
  e->Ref();
  return true;
}

void LRUCacheShard::SetHighPriorityPoolRatio(double high_pri_pool_ratio) {
  DMutexLock l(mutex_);
  high_pri_pool_ratio_ = high_pri_pool_ratio;
  high_pri_pool_capacity_ = capacity_ * high_pri_pool_ratio_;
  MaintainPoolSize();
}

void LRUCacheShard::SetLowPriorityPoolRatio(double low_pri_pool_ratio) {
  DMutexLock l(mutex_);
  low_pri_pool_ratio_ = low_pri_pool_ratio;
  low_pri_pool_capacity_ = capacity_ * low_pri_pool_ratio_;
  MaintainPoolSize();
}

bool LRUCacheShard::Release(LRUHandle* e, bool /*useful*/,
                            bool erase_if_last_ref) {
  if (e == nullptr) {
    return false;
  }
  bool last_reference = false;
  // Must Wait or WaitAll first on pending handles. Otherwise, would leak
  // a secondary cache handle.
  assert(!e->IsPending());
  {
    DMutexLock l(mutex_);
    last_reference = e->Unref();
    if (last_reference && e->InCache()) {
      // The item is still in cache, and nobody else holds a reference to it.
      if (usage_ > capacity_ || erase_if_last_ref) {
        // The LRU list must be empty since the cache is full.
        assert(lru_.next == &lru_ || erase_if_last_ref);
        // Take this opportunity and remove the item.
        table_.Remove(e->key(), e->hash);
        e->SetInCache(false);
      } else {
        // Put the item back on the LRU list, and don't free it.
        LRU_Insert(e);
        last_reference = false;
      }
    }
    // If it was the last reference, then decrement the cache usage.
    if (last_reference) {
      assert(usage_ >= e->total_charge);
      usage_ -= e->total_charge;
    }
  }

  // Free the entry here outside of mutex for performance reasons.
  if (last_reference) {
    e->Free(table_.GetAllocator());
  }
  return last_reference;
}

Status LRUCacheShard::Insert(const Slice& key, uint32_t hash,
                             Cache::ObjectPtr value,
                             const Cache::CacheItemHelper* helper,
                             size_t charge, LRUHandle** handle,
                             Cache::Priority priority) {
  assert(helper);

  // Allocate the memory here outside of the mutex.
  // If the cache is full, we'll have to release it.
  // It shouldn't happen very often though.
  LRUHandle* e =
      static_cast<LRUHandle*>(malloc(sizeof(LRUHandle) - 1 + key.size()));

  e->value = value;
  e->m_flags = 0;
  e->im_flags = 0;
  e->helper = helper;
  e->key_length = key.size();
  e->hash = hash;
  e->refs = 0;
  e->next = e->prev = nullptr;
  e->SetInCache(true);
  e->SetPriority(priority);
  memcpy(e->key_data, key.data(), key.size());
  e->CalcTotalCharge(charge, metadata_charge_policy_);

  // value == nullptr is reserved for indicating failure for when secondary
  // cache compatible
  assert(!(e->IsSecondaryCacheCompatible() && value == nullptr));

  return InsertItem(e, handle, /* free_handle_on_fail */ true);
}

void LRUCacheShard::Erase(const Slice& key, uint32_t hash) {
  LRUHandle* e;
  bool last_reference = false;
  {
    DMutexLock l(mutex_);
    e = table_.Remove(key, hash);
    if (e != nullptr) {
      assert(e->InCache());
      e->SetInCache(false);
      if (!e->HasRefs()) {
        // The entry is in LRU since it's in hash and has no external references
        LRU_Remove(e);
        assert(usage_ >= e->total_charge);
        usage_ -= e->total_charge;
        last_reference = true;
      }
    }
  }

  // Free the entry here outside of mutex for performance reasons.
  // last_reference will only be true if e != nullptr.
  if (last_reference) {
    e->Free(table_.GetAllocator());
  }
}

bool LRUCacheShard::IsReady(LRUHandle* e) {
  bool ready = true;
  if (e->IsPending()) {
    assert(secondary_cache_);
    assert(e->sec_handle);
    ready = e->sec_handle->IsReady();
  }
  return ready;
}

size_t LRUCacheShard::GetUsage() const {
  DMutexLock l(mutex_);
  return usage_;
}

size_t LRUCacheShard::GetPinnedUsage() const {
  DMutexLock l(mutex_);
  assert(usage_ >= lru_usage_);
  return usage_ - lru_usage_;
}

size_t LRUCacheShard::GetOccupancyCount() const {
  DMutexLock l(mutex_);
  return table_.GetOccupancyCount();
}

size_t LRUCacheShard::GetTableAddressCount() const {
  DMutexLock l(mutex_);
  return size_t{1} << table_.GetLengthBits();
}

void LRUCacheShard::AppendPrintableOptions(std::string& str) const {
  const int kBufferSize = 200;
  char buffer[kBufferSize];
  {
    DMutexLock l(mutex_);
    snprintf(buffer, kBufferSize, "    high_pri_pool_ratio: %.3lf\n",
             high_pri_pool_ratio_);
    snprintf(buffer + strlen(buffer), kBufferSize - strlen(buffer),
             "    low_pri_pool_ratio: %.3lf\n", low_pri_pool_ratio_);
  }
  str.append(buffer);
}

LRUCache::LRUCache(size_t capacity, int num_shard_bits,
                   bool strict_capacity_limit, double high_pri_pool_ratio,
                   double low_pri_pool_ratio,
                   std::shared_ptr<MemoryAllocator> allocator,
                   bool use_adaptive_mutex,
                   CacheMetadataChargePolicy metadata_charge_policy,
                   std::shared_ptr<SecondaryCache> _secondary_cache)
    : ShardedCache(capacity, num_shard_bits, strict_capacity_limit,
                   std::move(allocator)),
      secondary_cache_(std::move(_secondary_cache)) {
  size_t per_shard = GetPerShardCapacity();
  SecondaryCache* secondary_cache = secondary_cache_.get();
  MemoryAllocator* alloc = memory_allocator();
  InitShards([=](LRUCacheShard* cs) {
    new (cs) LRUCacheShard(
        per_shard, strict_capacity_limit, high_pri_pool_ratio,
        low_pri_pool_ratio, use_adaptive_mutex, metadata_charge_policy,
        /* max_upper_hash_bits */ 32 - num_shard_bits, alloc, secondary_cache);
  });
}

Cache::ObjectPtr LRUCache::Value(Handle* handle) {
  auto h = reinterpret_cast<const LRUHandle*>(handle);
  assert(!h->IsPending() || h->value == nullptr);
  assert(h->value != &kDummyValue);
  return h->value;
}

size_t LRUCache::GetCharge(Handle* handle) const {
  return reinterpret_cast<const LRUHandle*>(handle)->GetCharge(
      GetShard(0).metadata_charge_policy_);
}

const Cache::CacheItemHelper* LRUCache::GetCacheItemHelper(
    Handle* handle) const {
  auto h = reinterpret_cast<const LRUHandle*>(handle);
  return h->helper;
}

size_t LRUCache::TEST_GetLRUSize() {
  return SumOverShards([](LRUCacheShard& cs) { return cs.TEST_GetLRUSize(); });
}

double LRUCache::GetHighPriPoolRatio() {
  return GetShard(0).GetHighPriPoolRatio();
}

void LRUCache::WaitAll(std::vector<Handle*>& handles) {
  if (secondary_cache_) {
    std::vector<SecondaryCacheResultHandle*> sec_handles;
    sec_handles.reserve(handles.size());
    for (Handle* handle : handles) {
      if (!handle) {
        continue;
      }
      LRUHandle* lru_handle = reinterpret_cast<LRUHandle*>(handle);
      if (!lru_handle->IsPending()) {
        continue;
      }
      sec_handles.emplace_back(lru_handle->sec_handle);
    }
    secondary_cache_->WaitAll(sec_handles);
    for (Handle* handle : handles) {
      if (!handle) {
        continue;
      }
      LRUHandle* lru_handle = reinterpret_cast<LRUHandle*>(handle);
      if (!lru_handle->IsPending()) {
        continue;
      }
      GetShard(lru_handle->hash).Promote(lru_handle);
    }
  }
}

void LRUCache::AppendPrintableOptions(std::string& str) const {
  ShardedCache::AppendPrintableOptions(str);  // options from shard
  if (secondary_cache_) {
    str.append("  secondary_cache:\n");
    str.append(secondary_cache_->GetPrintableOptions());
  }
}

}  // namespace lru_cache

std::shared_ptr<Cache> NewLRUCache(
    size_t capacity, int num_shard_bits, bool strict_capacity_limit,
    double high_pri_pool_ratio,
    std::shared_ptr<MemoryAllocator> memory_allocator, bool use_adaptive_mutex,
    CacheMetadataChargePolicy metadata_charge_policy,
    const std::shared_ptr<SecondaryCache>& secondary_cache,
    double low_pri_pool_ratio) {
  if (num_shard_bits >= 20) {
    return nullptr;  // The cache cannot be sharded into too many fine pieces.
  }
  if (high_pri_pool_ratio < 0.0 || high_pri_pool_ratio > 1.0) {
    // Invalid high_pri_pool_ratio
    return nullptr;
  }
  if (low_pri_pool_ratio < 0.0 || low_pri_pool_ratio > 1.0) {
    // Invalid low_pri_pool_ratio
    return nullptr;
  }
  if (low_pri_pool_ratio + high_pri_pool_ratio > 1.0) {
    // Invalid high_pri_pool_ratio and low_pri_pool_ratio combination
    return nullptr;
  }
  if (num_shard_bits < 0) {
    num_shard_bits = GetDefaultCacheShardBits(capacity);
  }
  return std::make_shared<LRUCache>(
      capacity, num_shard_bits, strict_capacity_limit, high_pri_pool_ratio,
      low_pri_pool_ratio, std::move(memory_allocator), use_adaptive_mutex,
      metadata_charge_policy, secondary_cache);
}

std::shared_ptr<Cache> NewLRUCache(const LRUCacheOptions& cache_opts) {
  return NewLRUCache(cache_opts.capacity, cache_opts.num_shard_bits,
                     cache_opts.strict_capacity_limit,
                     cache_opts.high_pri_pool_ratio,
                     cache_opts.memory_allocator, cache_opts.use_adaptive_mutex,
                     cache_opts.metadata_charge_policy,
                     cache_opts.secondary_cache, cache_opts.low_pri_pool_ratio);
}

std::shared_ptr<Cache> NewLRUCache(
    size_t capacity, int num_shard_bits, bool strict_capacity_limit,
    double high_pri_pool_ratio,
    std::shared_ptr<MemoryAllocator> memory_allocator, bool use_adaptive_mutex,
    CacheMetadataChargePolicy metadata_charge_policy,
    double low_pri_pool_ratio) {
  return NewLRUCache(capacity, num_shard_bits, strict_capacity_limit,
                     high_pri_pool_ratio, memory_allocator, use_adaptive_mutex,
                     metadata_charge_policy, nullptr, low_pri_pool_ratio);
}
}  // namespace ROCKSDB_NAMESPACE

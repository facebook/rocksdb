//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "cache/fast_lru_cache.h"

#include <cassert>
#include <cstdint>
#include <cstdio>
#include <functional>

#include "monitoring/perf_context_imp.h"
#include "monitoring/statistics.h"
#include "port/lang.h"
#include "util/distributed_mutex.h"
#include "util/hash.h"
#include "util/math.h"
#include "util/random.h"

namespace ROCKSDB_NAMESPACE {

namespace fast_lru_cache {

LRUHandleTable::LRUHandleTable(int hash_bits)
    : length_bits_(hash_bits),
      length_bits_mask_((uint32_t{1} << length_bits_) - 1),
      occupancy_(0),
      occupancy_limit_(static_cast<uint32_t>((uint32_t{1} << length_bits_) *
                                             kStrictLoadFactor)),
      array_(new LRUHandle[size_t{1} << length_bits_]) {
  assert(hash_bits <= 32);
}

LRUHandleTable::~LRUHandleTable() {
  ApplyToEntriesRange([](LRUHandle* h) { h->FreeData(); }, 0, GetTableSize());
}

LRUHandle* LRUHandleTable::Lookup(const Slice& key, uint32_t hash) {
  int probe = 0;
  int slot = FindVisibleElement(key, hash, probe, 0);
  return (slot == -1) ? nullptr : &array_[slot];
}

LRUHandle* LRUHandleTable::Insert(LRUHandle* h, LRUHandle** old) {
  int probe = 0;
  int slot = FindVisibleElementOrAvailableSlot(h->key(), h->hash, probe,
                                               1 /*displacement*/);
  *old = nullptr;
  if (slot == -1) {
    // TODO(Guido) Don't we need to roll back displacements here?
    return nullptr;
  }

  if (array_[slot].IsEmpty() || array_[slot].IsTombstone()) {
    bool empty = array_[slot].IsEmpty();
    Assign(slot, h);
    LRUHandle* new_entry = &array_[slot];
    if (empty) {
      // This used to be an empty slot.
      return new_entry;
    }
    // It used to be a tombstone, so there may already be a copy of the
    // key in the table.
    slot = FindVisibleElement(h->key(), h->hash, probe, 0 /*displacement*/);
    if (slot == -1) {
      // No existing copy of the key.
      return new_entry;
    }
    *old = &array_[slot];
    return new_entry;
  } else {
    // There is an existing copy of the key.
    *old = &array_[slot];
    // Find an available slot for the new element.
    array_[slot].displacements++;
    slot = FindAvailableSlot(h->key(), probe, 1 /*displacement*/);
    if (slot == -1) {
      // No available slots. Roll back displacements.
      probe = 0;
      slot = FindVisibleElement(h->key(), h->hash, probe, -1);
      array_[slot].displacements--;
      FindAvailableSlot(h->key(), probe, -1);
      return nullptr;
    }
    Assign(slot, h);
    return &array_[slot];
  }
}

void LRUHandleTable::Remove(LRUHandle* h) {
  assert(h->next == nullptr &&
         h->prev == nullptr);  // Already off the LRU list.
  int probe = 0;
  FindSlot(
      h->key(), [&h](LRUHandle* e) { return e == h; }, probe,
      -1 /*displacement*/);
  h->SetIsVisible(false);
  h->SetIsElement(false);
  occupancy_--;
}

void LRUHandleTable::Assign(int slot, LRUHandle* h) {
  LRUHandle* dst = &array_[slot];
  uint32_t disp = dst->displacements;
  *dst = *h;
  dst->displacements = disp;
  dst->SetIsVisible(true);
  dst->SetIsElement(true);
  occupancy_++;
}

void LRUHandleTable::Exclude(LRUHandle* h) { h->SetIsVisible(false); }

int LRUHandleTable::FindVisibleElement(const Slice& key, uint32_t hash,
                                       int& probe, int displacement) {
  return FindSlot(
      key,
      [&](LRUHandle* h) { return h->Matches(key, hash) && h->IsVisible(); },
      probe, displacement);
}

int LRUHandleTable::FindAvailableSlot(const Slice& key, int& probe,
                                      int displacement) {
  return FindSlot(
      key, [](LRUHandle* h) { return h->IsEmpty() || h->IsTombstone(); }, probe,
      displacement);
}

int LRUHandleTable::FindVisibleElementOrAvailableSlot(const Slice& key,
                                                      uint32_t hash, int& probe,
                                                      int displacement) {
  return FindSlot(
      key,
      [&](LRUHandle* h) {
        return h->IsEmpty() || h->IsTombstone() ||
               (h->Matches(key, hash) && h->IsVisible());
      },
      probe, displacement);
}

inline int LRUHandleTable::FindSlot(const Slice& key,
                                    std::function<bool(LRUHandle*)> cond,
                                    int& probe, int displacement) {
  uint32_t base = ModTableSize(Hash(key.data(), key.size(), kProbingSeed1));
  uint32_t increment =
      ModTableSize((Hash(key.data(), key.size(), kProbingSeed2) << 1) | 1);
  uint32_t current = ModTableSize(base + probe * increment);
  while (true) {
    LRUHandle* h = &array_[current];
    probe++;
    if (current == base && probe > 1) {
      // We looped back.
      return -1;
    }
    if (cond(h)) {
      return current;
    }
    if (h->IsEmpty()) {
      // We check emptyness after the condition, because
      // the condition may be emptyness.
      return -1;
    }
    h->displacements += displacement;
    current = ModTableSize(current + increment);
  }
}

LRUCacheShard::LRUCacheShard(size_t capacity, size_t estimated_value_size,
                             bool strict_capacity_limit,
                             CacheMetadataChargePolicy metadata_charge_policy)
    : CacheShard(metadata_charge_policy),
      capacity_(capacity),
      strict_capacity_limit_(strict_capacity_limit),
      table_(
          CalcHashBits(capacity, estimated_value_size, metadata_charge_policy)),
      usage_(0),
      lru_usage_(0) {
  // Make empty circular linked list.
  lru_.next = &lru_;
  lru_.prev = &lru_;
  lru_low_pri_ = &lru_;
}

void LRUCacheShard::EraseUnRefEntries() {
  autovector<LRUHandle> last_reference_list;
  {
    DMutexLock l(mutex_);
    while (lru_.next != &lru_) {
      LRUHandle* old = lru_.next;
      // LRU list contains only elements which can be evicted.
      assert(old->IsVisible() && !old->HasRefs());
      LRU_Remove(old);
      table_.Remove(old);
      assert(usage_ >= old->total_charge);
      usage_ -= old->total_charge;
      last_reference_list.push_back(*old);
    }
  }

  // Free the entries here outside of mutex for performance reasons.
  for (auto& h : last_reference_list) {
    h.FreeData();
  }
}

void LRUCacheShard::ApplyToSomeEntries(
    const std::function<void(const Slice& key, void* value, size_t charge,
                             DeleterFn deleter)>& callback,
    uint32_t average_entries_per_lock, uint32_t* state) {
  // The state is essentially going to be the starting hash, which works
  // nicely even if we resize between calls because we use upper-most
  // hash bits for table indexes.
  DMutexLock l(mutex_);
  uint32_t length_bits = table_.GetLengthBits();
  uint32_t length = table_.GetTableSize();

  assert(average_entries_per_lock > 0);
  // Assuming we are called with same average_entries_per_lock repeatedly,
  // this simplifies some logic (index_end will not overflow).
  assert(average_entries_per_lock < length || *state == 0);

  uint32_t index_begin = *state >> (32 - length_bits);
  uint32_t index_end = index_begin + average_entries_per_lock;
  if (index_end >= length) {
    // Going to end
    index_end = length;
    *state = UINT32_MAX;
  } else {
    *state = index_end << (32 - length_bits);
  }

  table_.ApplyToEntriesRange(
      [callback,
       metadata_charge_policy = metadata_charge_policy_](LRUHandle* h) {
        callback(h->key(), h->value, h->GetCharge(metadata_charge_policy),
                 h->deleter);
      },
      index_begin, index_end);
}

void LRUCacheShard::LRU_Remove(LRUHandle* h) {
  assert(h->next != nullptr);
  assert(h->prev != nullptr);
  h->next->prev = h->prev;
  h->prev->next = h->next;
  h->prev = h->next = nullptr;
  assert(lru_usage_ >= h->total_charge);
  lru_usage_ -= h->total_charge;
}

void LRUCacheShard::LRU_Insert(LRUHandle* h) {
  assert(h->next == nullptr);
  assert(h->prev == nullptr);
  // Insert h to head of LRU list.
  h->next = &lru_;
  h->prev = lru_.prev;
  h->prev->next = h;
  h->next->prev = h;
  lru_usage_ += h->total_charge;
}

void LRUCacheShard::EvictFromLRU(size_t charge,
                                 autovector<LRUHandle>* deleted) {
  while ((usage_ + charge) > capacity_ && lru_.next != &lru_) {
    LRUHandle* old = lru_.next;
    // LRU list contains only elements which can be evicted.
    assert(old->IsVisible() && !old->HasRefs());
    LRU_Remove(old);
    table_.Remove(old);
    assert(usage_ >= old->total_charge);
    usage_ -= old->total_charge;
    deleted->push_back(*old);
  }
}

size_t LRUCacheShard::CalcEstimatedHandleCharge(
    size_t estimated_value_size,
    CacheMetadataChargePolicy metadata_charge_policy) {
  LRUHandle h;
  h.CalcTotalCharge(estimated_value_size, metadata_charge_policy);
  return h.total_charge;
}

int LRUCacheShard::CalcHashBits(
    size_t capacity, size_t estimated_value_size,
    CacheMetadataChargePolicy metadata_charge_policy) {
  size_t handle_charge =
      CalcEstimatedHandleCharge(estimated_value_size, metadata_charge_policy);
  assert(handle_charge > 0);
  uint32_t num_entries =
      static_cast<uint32_t>(capacity / (kLoadFactor * handle_charge)) + 1;
  assert(num_entries <= uint32_t{1} << 31);
  return FloorLog2((num_entries << 1) - 1);
}

void LRUCacheShard::SetCapacity(size_t capacity) {
  autovector<LRUHandle> last_reference_list;
  {
    DMutexLock l(mutex_);
    if (capacity > capacity_) {
      assert(false);  // Not supported.
    }
    capacity_ = capacity;
    EvictFromLRU(0, &last_reference_list);
  }

  // Free the entries here outside of mutex for performance reasons.
  for (auto& h : last_reference_list) {
    h.FreeData();
  }
}

void LRUCacheShard::SetStrictCapacityLimit(bool strict_capacity_limit) {
  DMutexLock l(mutex_);
  strict_capacity_limit_ = strict_capacity_limit;
}

Status LRUCacheShard::Insert(const Slice& key, uint32_t hash, void* value,
                             size_t charge, Cache::DeleterFn deleter,
                             Cache::Handle** handle,
                             Cache::Priority /*priority*/) {
  if (key.size() != kCacheKeySize) {
    return Status::NotSupported("FastLRUCache only supports key size " +
                                std::to_string(kCacheKeySize) + "B");
  }

  LRUHandle tmp;
  tmp.value = value;
  tmp.deleter = deleter;
  tmp.hash = hash;
  tmp.CalcTotalCharge(charge, metadata_charge_policy_);
  for (int i = 0; i < kCacheKeySize; i++) {
    tmp.key_data[i] = key.data()[i];
  }

  Status s = Status::OK();
  autovector<LRUHandle> last_reference_list;
  {
    DMutexLock l(mutex_);
    assert(table_.GetOccupancy() <= table_.GetOccupancyLimit());

    // Free the space following strict LRU policy until enough space
    // is freed or the lru list is empty.
    EvictFromLRU(tmp.total_charge, &last_reference_list);
    if ((usage_ + tmp.total_charge > capacity_ &&
         (strict_capacity_limit_ || handle == nullptr)) ||
        table_.GetOccupancy() == table_.GetOccupancyLimit()) {
      // There are two measures of capacity:
      // - Space (or charge) capacity: The maximum possible sum of the charges
      //    of the elements.
      // - Table capacity: The number of slots in the hash table.
      // These are incomparable, in the sense that one doesn't imply the other.
      // Typically we will reach space capacity before table capacity---
      // if the user always inserts values with size equal to
      // estimated_value_size, then at most a kLoadFactor fraction of slots
      // will ever be occupied. But in some cases we may reach table capacity
      // before space capacity---if the user initially claims a very large
      // estimated_value_size but then inserts tiny values, more elements than
      // initially estimated will be inserted.

      // TODO(Guido) Some tests (at least two from cache_test, as well as the
      // stress tests) currently assume the table capacity is unbounded.
      if (handle == nullptr) {
        // Don't insert the entry but still return ok, as if the entry inserted
        // into cache and get evicted immediately.
        last_reference_list.push_back(tmp);
      } else {
        if (table_.GetOccupancy() == table_.GetOccupancyLimit()) {
          // TODO: Consider using a distinct status for this case, but usually
          // it will be handled the same way as reaching charge capacity limit
          s = Status::MemoryLimit(
              "Insert failed because all slots in the hash table are full.");
        } else {
          s = Status::MemoryLimit(
              "Insert failed because the total charge has exceeded the "
              "capacity.");
        }
      }
    } else {
      // Insert into the cache. Note that the cache might get larger than its
      // capacity if not enough space was freed up.
      LRUHandle* old;
      LRUHandle* h = table_.Insert(&tmp, &old);
      assert(h != nullptr);  // We're below occupancy, so this insertion should
                             // never fail.
      usage_ += h->total_charge;
      if (old != nullptr) {
        s = Status::OkOverwritten();
        assert(old->IsVisible());
        table_.Exclude(old);
        if (!old->HasRefs()) {
          // old is on LRU because it's in cache and its reference count is 0.
          LRU_Remove(old);
          table_.Remove(old);
          assert(usage_ >= old->total_charge);
          usage_ -= old->total_charge;
          last_reference_list.push_back(*old);
        }
      }
      if (handle == nullptr) {
        LRU_Insert(h);
      } else {
        // If caller already holds a ref, no need to take one here.
        if (!h->HasRefs()) {
          h->Ref();
        }
        *handle = reinterpret_cast<Cache::Handle*>(h);
      }
    }
  }

  // Free the entries here outside of mutex for performance reasons.
  for (auto& h : last_reference_list) {
    h.FreeData();
  }

  return s;
}

Cache::Handle* LRUCacheShard::Lookup(const Slice& key, uint32_t hash) {
  LRUHandle* h = nullptr;
  {
    DMutexLock l(mutex_);
    h = table_.Lookup(key, hash);
    if (h != nullptr) {
      assert(h->IsVisible());
      if (!h->HasRefs()) {
        // The entry is in LRU since it's in hash and has no external
        // references.
        LRU_Remove(h);
      }
      h->Ref();
    }
  }
  return reinterpret_cast<Cache::Handle*>(h);
}

bool LRUCacheShard::Ref(Cache::Handle* h) {
  LRUHandle* e = reinterpret_cast<LRUHandle*>(h);
  DMutexLock l(mutex_);
  // To create another reference - entry must be already externally referenced.
  assert(e->HasRefs());
  e->Ref();
  return true;
}

bool LRUCacheShard::Release(Cache::Handle* handle, bool erase_if_last_ref) {
  if (handle == nullptr) {
    return false;
  }
  LRUHandle* h = reinterpret_cast<LRUHandle*>(handle);
  LRUHandle copy;
  bool last_reference = false;
  {
    DMutexLock l(mutex_);
    last_reference = h->Unref();
    if (last_reference && h->IsVisible()) {
      // The item is still in cache, and nobody else holds a reference to it.
      if (usage_ > capacity_ || erase_if_last_ref) {
        // The LRU list must be empty since the cache is full.
        assert(lru_.next == &lru_ || erase_if_last_ref);
        // Take this opportunity and remove the item.
        table_.Remove(h);
      } else {
        // Put the item back on the LRU list, and don't free it.
        LRU_Insert(h);
        last_reference = false;
      }
    }
    // If it was the last reference, then decrement the cache usage.
    if (last_reference) {
      assert(usage_ >= h->total_charge);
      usage_ -= h->total_charge;
      copy = *h;
    }
  }

  // Free the entry here outside of mutex for performance reasons.
  if (last_reference) {
    copy.FreeData();
  }
  return last_reference;
}

void LRUCacheShard::Erase(const Slice& key, uint32_t hash) {
  LRUHandle copy;
  bool last_reference = false;
  {
    DMutexLock l(mutex_);
    LRUHandle* h = table_.Lookup(key, hash);
    if (h != nullptr) {
      table_.Exclude(h);
      if (!h->HasRefs()) {
        // The entry is in LRU since it's in cache and has no external
        // references.
        LRU_Remove(h);
        table_.Remove(h);
        assert(usage_ >= h->total_charge);
        usage_ -= h->total_charge;
        last_reference = true;
        copy = *h;
      }
    }
  }
  // Free the entry here outside of mutex for performance reasons.
  // last_reference will only be true if e != nullptr.
  if (last_reference) {
    copy.FreeData();
  }
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
  return table_.GetOccupancy();
}

size_t LRUCacheShard::GetTableAddressCount() const {
  DMutexLock l(mutex_);
  return table_.GetTableSize();
}

std::string LRUCacheShard::GetPrintableOptions() const { return std::string{}; }

LRUCache::LRUCache(size_t capacity, size_t estimated_value_size,
                   int num_shard_bits, bool strict_capacity_limit,
                   CacheMetadataChargePolicy metadata_charge_policy)
    : ShardedCache(capacity, num_shard_bits, strict_capacity_limit) {
  assert(estimated_value_size > 0 ||
         metadata_charge_policy != kDontChargeCacheMetadata);
  num_shards_ = 1 << num_shard_bits;
  shards_ = reinterpret_cast<LRUCacheShard*>(
      port::cacheline_aligned_alloc(sizeof(LRUCacheShard) * num_shards_));
  size_t per_shard = (capacity + (num_shards_ - 1)) / num_shards_;
  for (int i = 0; i < num_shards_; i++) {
    new (&shards_[i])
        LRUCacheShard(per_shard, estimated_value_size, strict_capacity_limit,
                      metadata_charge_policy);
  }
}

LRUCache::~LRUCache() {
  if (shards_ != nullptr) {
    assert(num_shards_ > 0);
    for (int i = 0; i < num_shards_; i++) {
      shards_[i].~LRUCacheShard();
    }
    port::cacheline_aligned_free(shards_);
  }
}

CacheShard* LRUCache::GetShard(uint32_t shard) {
  return reinterpret_cast<CacheShard*>(&shards_[shard]);
}

const CacheShard* LRUCache::GetShard(uint32_t shard) const {
  return reinterpret_cast<CacheShard*>(&shards_[shard]);
}

void* LRUCache::Value(Handle* handle) {
  return reinterpret_cast<const LRUHandle*>(handle)->value;
}

size_t LRUCache::GetCharge(Handle* handle) const {
  CacheMetadataChargePolicy metadata_charge_policy = kDontChargeCacheMetadata;
  if (num_shards_ > 0) {
    metadata_charge_policy = shards_[0].metadata_charge_policy_;
  }
  return reinterpret_cast<const LRUHandle*>(handle)->GetCharge(
      metadata_charge_policy);
}

Cache::DeleterFn LRUCache::GetDeleter(Handle* handle) const {
  auto h = reinterpret_cast<const LRUHandle*>(handle);
  return h->deleter;
}

uint32_t LRUCache::GetHash(Handle* handle) const {
  return reinterpret_cast<const LRUHandle*>(handle)->hash;
}

void LRUCache::DisownData() {
  // Leak data only if that won't generate an ASAN/valgrind warning.
  if (!kMustFreeHeapAllocations) {
    shards_ = nullptr;
    num_shards_ = 0;
  }
}

}  // namespace fast_lru_cache

std::shared_ptr<Cache> NewFastLRUCache(
    size_t capacity, size_t estimated_value_size, int num_shard_bits,
    bool strict_capacity_limit,
    CacheMetadataChargePolicy metadata_charge_policy) {
  if (num_shard_bits >= 20) {
    return nullptr;  // The cache cannot be sharded into too many fine pieces.
  }
  if (num_shard_bits < 0) {
    num_shard_bits = GetDefaultCacheShardBits(capacity);
  }
  return std::make_shared<fast_lru_cache::LRUCache>(
      capacity, estimated_value_size, num_shard_bits, strict_capacity_limit,
      metadata_charge_policy);
}

}  // namespace ROCKSDB_NAMESPACE

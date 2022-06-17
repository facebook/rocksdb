//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "cache/fast_lru_cache.h"

#include <math.h>

#include <cassert>
#include <cstdint>
#include <cstdio>
#include <functional>

#include "monitoring/perf_context_imp.h"
#include "monitoring/statistics.h"
#include "port/lang.h"
#include "util/hash.h"
#include "util/mutexlock.h"

namespace ROCKSDB_NAMESPACE {

namespace fast_lru_cache {

namespace {
// Returns x % 2^{bits}.
inline uint32_t BinaryMod(uint32_t x, uint8_t bits) {
  assert(bits <= 32);
  return (x << (32 - bits)) >> (32 - bits);
}
}  // anonymous namespace

LRUHandleTable::LRUHandleTable(uint8_t hash_bits)
    : length_bits_(hash_bits),
      occupancy_(0),
      array_(new LRUHandle[size_t{1} << length_bits_]) {
  assert(hash_bits <= 32);
}

LRUHandleTable::~LRUHandleTable() {
  ApplyToEntriesRange(
      [](LRUHandle* h) {
        if (!h->HasRefs()) {
          h->Free();
        }
      },
      0, uint32_t{1} << length_bits_);
}

LRUHandle* LRUHandleTable::Lookup(const Slice& key, uint32_t hash) {
  int probe = 0;
  int slot = FindPresentElement(key, hash, probe, 0);
  return (slot == -1) ? nullptr : &array_[slot];
}

LRUHandle* LRUHandleTable::Insert(LRUHandle* h, LRUHandle** old) {
  int probe = 0;
  int slot = FindPresentElementOrAvailableSlot(h->key(), h->hash, probe,
                                               1 /*displacement*/);
  *old = nullptr;
  if (slot == -1) {
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
    slot = FindElement(h->key(), h->hash, probe, 0 /*displacement*/);
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
      return nullptr;
    }
    Assign(slot, h);
    return &array_[slot];
  }
}

LRUHandle* LRUHandleTable::Remove(const Slice& key, uint32_t hash) {
  int probe = 0;
  int slot = FindElement(key, hash, probe, -1 /*displacement*/);
  assert(slot != -1); // Remove assumes the key/hash is in the hash table.
  LRUHandle* e = &array_[slot];
  e->SetIsElement(false);
  e->SetIsVisible(false);
  occupancy_--;
  return e;
}

void LRUHandleTable::Assign(int slot, LRUHandle* src) {
  LRUHandle* dst = &array_[slot];
  uint32_t disp = dst->displacements;
  memcpy(dst, src, sizeof(LRUHandle));
  dst->displacements = disp;
  dst->SetIsVisible(true);
  dst->SetIsElement(true);
  occupancy_++;
}

void LRUHandleTable::Exclude(LRUHandle* e) { e->SetIsVisible(false); }

int LRUHandleTable::FindElement(const Slice& key, uint32_t hash, int& probe,
                                int displacement) {
  std::function<bool(LRUHandle*)> cond = [&, key, hash](LRUHandle* e) {
    return e->Matches(key, hash) && e->IsElement();
  };
  return FindSlot(key, cond, probe, displacement);
}

int LRUHandleTable::FindPresentElement(const Slice& key, uint32_t hash,
                                       int& probe, int displacement) {
  std::function<bool(LRUHandle*)> cond = [&, key, hash](LRUHandle* e) {
    return e->Matches(key, hash) && e->IsVisible();
  };
  return FindSlot(key, cond, probe, displacement);
}

int LRUHandleTable::FindAvailableSlot(const Slice& key, int& probe,
                                      int displacement) {
  std::function<bool(LRUHandle*)> cond = [](LRUHandle* e) {
    return e->IsEmpty() || e->IsTombstone();
  };
  return FindSlot(key, cond, probe, displacement);
}

int LRUHandleTable::FindPresentElementOrAvailableSlot(const Slice& key,
                                                      uint32_t hash, int& probe,
                                                      int displacement) {
  std::function<bool(LRUHandle*)> cond = [&, key, hash](LRUHandle* e) {
    return e->IsEmpty() || e->IsTombstone() ||
           (e->Matches(key, hash) && e->IsVisible());
  };
  return FindSlot(key, cond, probe, displacement);
}

int LRUHandleTable::FindSlot(const Slice& key,
                             std::function<bool(LRUHandle*)> cond, int& probe,
                             int displacement) {
  uint32_t base =
      BinaryMod(Hash(key.data(), key.size(), kProbingSeed1), length_bits_);
  uint32_t increment = BinaryMod(
      (Hash(key.data(), key.size(), kProbingSeed2) << 1) | 1, length_bits_);
  uint32_t current = BinaryMod(base + probe * increment, length_bits_);
  while (1) {
    LRUHandle* e = &array_[current];
    probe++;
    if (cond(e)) {
      return current;
    }
    current = BinaryMod(current + increment, length_bits_);
    if (e->IsEmpty() || current == base) {
      return -1;
    }
    e->displacements += displacement;
  }
}

LRUCacheShard::LRUCacheShard(size_t capacity, size_t estimated_value_size,
                             bool strict_capacity_limit,
                             CacheMetadataChargePolicy metadata_charge_policy)
    : capacity_(capacity),
      strict_capacity_limit_(strict_capacity_limit),
      table_(
          GetHashBits(capacity, estimated_value_size, metadata_charge_policy) +
          static_cast<uint8_t>(ceil(log2(1.0 / kLoadFactor)))),
      usage_(0),
      lru_usage_(0) {
  set_metadata_charge_policy(metadata_charge_policy);
  // Make empty circular linked list.
  lru_.next = &lru_;
  lru_.prev = &lru_;
  lru_low_pri_ = &lru_;
}

void LRUCacheShard::EraseUnRefEntries() {
  autovector<LRUHandle*> last_reference_list;
  {
    MutexLock l(&mutex_);
    while (lru_.next != &lru_) {
      LRUHandle* old = lru_.next;
      // LRU list contains only elements which can be evicted.
      assert(old->IsVisible() && !old->HasRefs());
      LRU_Remove(old);
      table_.Remove(old->key(), old->hash);
      assert(usage_ >= old->total_charge);
      usage_ -= old->total_charge;
      last_reference_list.push_back(old);
    }
  }

  // Free the entries here outside of mutex for performance reasons.
  for (auto entry : last_reference_list) {
    entry->Free();
  }
}

// TODO(guido) Check this makes sense in the new data structure.
void LRUCacheShard::ApplyToSomeEntries(
    const std::function<void(const Slice& key, void* value, size_t charge,
                             DeleterFn deleter)>& callback,
    uint32_t average_entries_per_lock, uint32_t* state) {
  // The state is essentially going to be the starting hash, which works
  // nicely even if we resize between calls because we use upper-most
  // hash bits for table indexes.
  MutexLock l(&mutex_);
  uint32_t length_bits = table_.GetLengthBits();
  uint32_t length = uint32_t{1} << length_bits;

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

void LRUCacheShard::LRU_Remove(LRUHandle* e) {
  assert(e->next != nullptr);
  assert(e->prev != nullptr);
  e->next->prev = e->prev;
  e->prev->next = e->next;
  e->prev = e->next = nullptr;
  assert(lru_usage_ >= e->total_charge);
  lru_usage_ -= e->total_charge;
}

void LRUCacheShard::LRU_Insert(LRUHandle* e) {
  assert(e->next == nullptr);
  assert(e->prev == nullptr);
  // Inset "e" to head of LRU list.
  e->next = &lru_;
  e->prev = lru_.prev;
  e->prev->next = e;
  e->next->prev = e;
  lru_usage_ += e->total_charge;
}

void LRUCacheShard::EvictFromLRU(size_t charge,
                                 autovector<LRUHandle*>* deleted) {
  while ((usage_ + charge) > capacity_ && lru_.next != &lru_) {
    LRUHandle* old = lru_.next;
    // LRU list contains only elements which can be evicted.
    assert(old->IsVisible() && !old->HasRefs());
    LRU_Remove(old);
    table_.Remove(old->key(), old->hash);
    assert(usage_ >= old->total_charge);
    usage_ -= old->total_charge;
    deleted->push_back(old);
  }
}

uint8_t LRUCacheShard::GetHashBits(
    size_t capacity, size_t estimated_value_size,
    CacheMetadataChargePolicy metadata_charge_policy) {
  LRUHandle e;
  e.deleter = nullptr;
  e.refs = 0;
  e.flags = 0;
  e.refs = 0;

  e.CalcTotalCharge(estimated_value_size, metadata_charge_policy);
  size_t num_entries = capacity / e.total_charge;
  uint8_t num_hash_bits = 0;
  while (num_entries >>= 1) {
    ++num_hash_bits;
  }
  return num_hash_bits;
}

void LRUCacheShard::SetCapacity(size_t capacity) {
  assert(false);  // Not supported.
  // TODO(Guido) Add support?
  autovector<LRUHandle*> last_reference_list;
  {
    MutexLock l(&mutex_);
    capacity_ = capacity;
    EvictFromLRU(0, &last_reference_list);
  }

  // Free the entries here outside of mutex for performance reasons.
  for (auto entry : last_reference_list) {
    entry->Free();
  }
}

void LRUCacheShard::SetStrictCapacityLimit(bool strict_capacity_limit) {
  MutexLock l(&mutex_);
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
  tmp.displacements = 0;
  tmp.flags = 0;
  tmp.value = value;
  tmp.deleter = deleter;
  tmp.hash = hash;
  tmp.refs = 0;
  tmp.next = tmp.prev = nullptr;
  tmp.CalcTotalCharge(charge, metadata_charge_policy_);
  memcpy(tmp.key_data, key.data(), kCacheKeySize);

  Status s = Status::OK();
  autovector<LRUHandle*> last_reference_list;
  {
    MutexLock l(&mutex_);
    // Free the space following strict LRU policy until enough space
    // is freed or the lru list is empty.
    EvictFromLRU(tmp.total_charge, &last_reference_list);
    if ((usage_ + tmp.total_charge > capacity_ &&
         (strict_capacity_limit_ || handle == nullptr)) ||
        table_.GetOccupancy() == size_t{1} << table_.GetLengthBits()) {
      // The new occupancy check is there to avoid some bad states, because
      // the user can insert elements beyond capacity (strict_capacity_limit ==
      // false) as well as adjust capacity (SetCapacity function):
      //  - If the strict_capacity_limit == false and the user provides a
      //    handle, the old code
      //    forced the insert. But in our closed-address hash table we can't
      //    insert if the table occupancy is maximum.
      //  - When the capacity is adjusted, we currently don't rebuild the table.
      //    So an insert may pass the shard usage check, regardless of the table
      //    occupancy.
      // Unfortunately, the tests currently set strict_capacity_limit = false,
      // so we can't just disable it here.
      if (handle == nullptr) {
        // Don't insert the entry but still return ok, as if the entry inserted
        // into cache and get evicted immediately.
        last_reference_list.push_back(&tmp);
      } else {
        s = Status::Incomplete("Insert failed due to LRU cache being full.");
      }
    } else {
      // Insert into the cache. Note that the cache might get larger than its
      // capacity if not enough space was freed up.
      LRUHandle* old;
      LRUHandle* e = table_.Insert(&tmp, &old);
      assert(e != nullptr);  // Insertions should never fail.
      usage_ += e->total_charge;
      if (old != nullptr) {
        s = Status::OkOverwritten();
        assert(old->IsVisible());
        table_.Exclude(old);
        if (!old->HasRefs()) {
          // old is on LRU because it's in cache and its reference count is 0.
          LRU_Remove(old);
          table_.Remove(old->key(), old->hash);
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
        *handle = reinterpret_cast<Cache::Handle*>(e);
      }
    }
  }

  // Free the entries here outside of mutex for performance reasons.
  for (auto entry : last_reference_list) {
    entry->Free();
  }

  return s;
}

Cache::Handle* LRUCacheShard::Lookup(const Slice& key, uint32_t hash) {
  LRUHandle* e = nullptr;
  {
    MutexLock l(&mutex_);
    e = table_.Lookup(key, hash);
    if (e != nullptr) {
      assert(e->IsVisible());
      if (!e->HasRefs()) {
        // The entry is in LRU since it's in hash and has no external references
        LRU_Remove(e);
      }
      e->Ref();
    }
  }
  return reinterpret_cast<Cache::Handle*>(e);
}

bool LRUCacheShard::Ref(Cache::Handle* h) {
  LRUHandle* e = reinterpret_cast<LRUHandle*>(h);
  MutexLock l(&mutex_);
  // To create another reference - entry must be already externally referenced.
  assert(e->HasRefs());
  e->Ref();
  return true;
}

bool LRUCacheShard::Release(Cache::Handle* handle, bool erase_if_last_ref) {
  if (handle == nullptr) {
    return false;
  }
  LRUHandle* e = reinterpret_cast<LRUHandle*>(handle);
  bool last_reference = false;
  {
    MutexLock l(&mutex_);
    last_reference = e->Unref();
    if (last_reference && e->IsVisible()) {
      // The item is still in cache, and nobody else holds a reference to it.
      if (usage_ > capacity_ || erase_if_last_ref) {
        // The LRU list must be empty since the cache is full.
        assert(lru_.next == &lru_ || erase_if_last_ref);
        // Take this opportunity and remove the item.
        table_.Remove(e->key(), e->hash);
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
    e->Free();
  }
  return last_reference;
}

void LRUCacheShard::Erase(const Slice& key, uint32_t hash) {
  LRUHandle* e;
  bool last_reference = false;
  {
    MutexLock l(&mutex_);
    e = table_.Lookup(key, hash);
    if (e != nullptr) {
      table_.Exclude(e);
      if (!e->HasRefs()) {
        // The entry is in LRU since it's in cache and has no external
        // references
        LRU_Remove(e);
        table_.Remove(key, hash);
        assert(usage_ >= e->total_charge);
        usage_ -= e->total_charge;
        last_reference = true;
      }
    }
  }
  // Free the entry here outside of mutex for performance reasons.
  // last_reference will only be true if e != nullptr.
  if (last_reference) {
    e->Free();
  }
}

size_t LRUCacheShard::GetUsage() const {
  MutexLock l(&mutex_);
  return usage_;
}

size_t LRUCacheShard::GetPinnedUsage() const {
  MutexLock l(&mutex_);
  assert(usage_ >= lru_usage_);
  return usage_ - lru_usage_;
}

std::string LRUCacheShard::GetPrintableOptions() const { return std::string{}; }

LRUCache::LRUCache(size_t capacity, size_t estimated_value_size,
                   uint8_t num_shard_bits, bool strict_capacity_limit,
                   CacheMetadataChargePolicy metadata_charge_policy)
    : ShardedCache(capacity, num_shard_bits, strict_capacity_limit) {
  // // We must enforce strict capacity requirement, or else insertions
  // // will fail when all slots are occupied and also pinned.
  // assert(strict_capacity_limit);
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
      capacity, estimated_value_size, static_cast<uint8_t>(num_shard_bits),
      strict_capacity_limit, metadata_charge_policy);
}

}  // namespace ROCKSDB_NAMESPACE

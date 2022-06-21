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

#include "monitoring/perf_context_imp.h"
#include "monitoring/statistics.h"
#include "port/lang.h"
#include "util/distributed_mutex.h"

#define KEY_LENGTH \
  16  // TODO(guido) Make use of this symbol in other parts of the source code
      // (e.g., cache_key.h, cache_test.cc, etc.)

namespace ROCKSDB_NAMESPACE {

namespace fast_lru_cache {

LRUHandleTable::LRUHandleTable(int hash_bits)
    : length_bits_(hash_bits),
      list_(new LRUHandle* [size_t{1} << length_bits_] {}) {}

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
  return *FindPointer(key, hash);
}

inline LRUHandle** LRUHandleTable::Head(uint32_t hash) {
  return &list_[hash >> (32 - length_bits_)];
}

LRUHandle* LRUHandleTable::Insert(LRUHandle* h) {
  LRUHandle** ptr = FindPointer(h->key(), h->hash);
  LRUHandle* old = *ptr;
  h->next_hash = (old == nullptr ? nullptr : old->next_hash);
  *ptr = h;
  return old;
}

LRUHandle* LRUHandleTable::Remove(const Slice& key, uint32_t hash) {
  LRUHandle** ptr = FindPointer(key, hash);
  LRUHandle* result = *ptr;
  if (result != nullptr) {
    *ptr = result->next_hash;
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

LRUCacheShard::LRUCacheShard(size_t capacity, size_t estimated_value_size,
                             bool strict_capacity_limit,
                             CacheMetadataChargePolicy metadata_charge_policy)
    : capacity_(0),
      strict_capacity_limit_(strict_capacity_limit),
      table_(
          GetHashBits(capacity, estimated_value_size, metadata_charge_policy)),
      usage_(0),
      lru_usage_(0) {
  set_metadata_charge_policy(metadata_charge_policy);
  // Make empty circular linked list.
  lru_.next = &lru_;
  lru_.prev = &lru_;
  lru_low_pri_ = &lru_;
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

  // Free the entries here outside of mutex for performance reasons.
  for (auto entry : last_reference_list) {
    entry->Free();
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
    assert(old->InCache() && !old->HasRefs());
    LRU_Remove(old);
    table_.Remove(old->key(), old->hash);
    old->SetInCache(false);
    assert(usage_ >= old->total_charge);
    usage_ -= old->total_charge;
    deleted->push_back(old);
  }
}

int LRUCacheShard::GetHashBits(
    size_t capacity, size_t estimated_value_size,
    CacheMetadataChargePolicy metadata_charge_policy) {
  LRUHandle* e = reinterpret_cast<LRUHandle*>(
      new char[sizeof(LRUHandle) - 1 + KEY_LENGTH]);
  e->key_length = KEY_LENGTH;
  e->deleter = nullptr;
  e->refs = 0;
  e->flags = 0;
  e->refs = 0;

  e->CalcTotalCharge(estimated_value_size, metadata_charge_policy);
  size_t num_entries = capacity / e->total_charge;
  e->Free();
  int num_hash_bits = 0;
  while (num_entries >>= 1) {
    ++num_hash_bits;
  }
  return num_hash_bits;
}

void LRUCacheShard::SetCapacity(size_t capacity) {
  autovector<LRUHandle*> last_reference_list;
  {
    DMutexLock l(mutex_);
    capacity_ = capacity;
    EvictFromLRU(0, &last_reference_list);
  }

  // Free the entries here outside of mutex for performance reasons.
  for (auto entry : last_reference_list) {
    entry->Free();
  }
}

void LRUCacheShard::SetStrictCapacityLimit(bool strict_capacity_limit) {
  DMutexLock l(mutex_);
  strict_capacity_limit_ = strict_capacity_limit;
}

Status LRUCacheShard::InsertItem(LRUHandle* e, Cache::Handle** handle,
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
          delete[] reinterpret_cast<char*>(e);
          *handle = nullptr;
        }
        s = Status::Incomplete("Insert failed due to LRU cache being full.");
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
    DMutexLock l(mutex_);
    e = table_.Lookup(key, hash);
    if (e != nullptr) {
      assert(e->InCache());
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
  LRUHandle* e = reinterpret_cast<LRUHandle*>(handle);
  bool last_reference = false;
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
    e->Free();
  }
  return last_reference;
}

Status LRUCacheShard::Insert(const Slice& key, uint32_t hash, void* value,
                             size_t charge, Cache::DeleterFn deleter,
                             Cache::Handle** handle,
                             Cache::Priority /*priority*/) {
  if (key.size() != KEY_LENGTH) {
    return Status::NotSupported("FastLRUCache only supports key size " +
                                std::to_string(KEY_LENGTH) + "B");
  }

  // Allocate the memory here outside of the mutex.
  // If the cache is full, we'll have to release it.
  // It shouldn't happen very often though.
  LRUHandle* e = reinterpret_cast<LRUHandle*>(
      new char[sizeof(LRUHandle) - 1 + key.size()]);

  e->value = value;
  e->flags = 0;
  e->deleter = deleter;
  e->key_length = key.size();
  e->hash = hash;
  e->refs = 0;
  e->next = e->prev = nullptr;
  e->SetInCache(true);
  e->CalcTotalCharge(charge, metadata_charge_policy_);
  memcpy(e->key_data, key.data(), key.size());

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
    e->Free();
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

std::string LRUCacheShard::GetPrintableOptions() const { return std::string{}; }

LRUCache::LRUCache(size_t capacity, size_t estimated_value_size,
                   int num_shard_bits, bool strict_capacity_limit,
                   CacheMetadataChargePolicy metadata_charge_policy)
    : ShardedCache(capacity, num_shard_bits, strict_capacity_limit) {
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

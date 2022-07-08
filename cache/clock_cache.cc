//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "cache/clock_cache.h"

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

namespace clock_cache {

ClockHandleTable::ClockHandleTable(int hash_bits)
    : length_bits_(hash_bits),
      length_bits_mask_((uint32_t{1} << length_bits_) - 1),
      occupancy_(0),
      occupancy_limit_(static_cast<uint32_t>((uint32_t{1} << length_bits_) *
                                             kStrictLoadFactor)),
      array_(new ClockHandle[size_t{1} << length_bits_]) {
  assert(hash_bits <= 32);
}

ClockHandleTable::~ClockHandleTable() {
  ApplyToEntriesRange([](ClockHandle* h) { h->FreeData(); }, 0, GetTableSize());
}

ClockHandle* ClockHandleTable::Lookup(const Slice& key) {
  int probe = 0;
  int slot = FindVisibleElement(key, probe, 0);
  return (slot == -1) ? nullptr : &array_[slot];
}

ClockHandle* ClockHandleTable::Insert(ClockHandle* h, ClockHandle** old) {
  int probe = 0;
  int slot =
      FindVisibleElementOrAvailableSlot(h->key(), probe, 1 /*displacement*/);
  *old = nullptr;
  if (slot == -1) {
    return nullptr;
  }

  if (array_[slot].IsEmpty() || array_[slot].IsTombstone()) {
    bool empty = array_[slot].IsEmpty();
    Assign(slot, h);
    ClockHandle* new_entry = &array_[slot];
    if (empty) {
      // This used to be an empty slot.
      return new_entry;
    }
    // It used to be a tombstone, so there may already be a copy of the
    // key in the table.
    slot = FindVisibleElement(h->key(), probe, 0 /*displacement*/);
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
      slot = FindVisibleElement(h->key(), probe, -1);
      array_[slot].displacements--;
      FindAvailableSlot(h->key(), probe, -1);
      return nullptr;
    }
    Assign(slot, h);
    return &array_[slot];
  }
}

void ClockHandleTable::Remove(ClockHandle* h) {
  assert(!h->IsInClockList());  // Already off the clock list.
  int probe = 0;
  FindSlot(
      h->key(), [&h](ClockHandle* e) { return e == h; }, probe,
      -1 /*displacement*/);
  h->SetIsVisible(false);
  h->SetIsElement(false);
  occupancy_--;
}

void ClockHandleTable::Assign(int slot, ClockHandle* h) {
  ClockHandle* dst = &array_[slot];
  uint32_t disp = dst->displacements;
  *dst = *h;
  dst->displacements = disp;
  dst->SetIsVisible(true);
  dst->SetIsElement(true);
  dst->SetClockPriority(ClockHandle::ClockPriority::NONE);
  occupancy_++;
}

void ClockHandleTable::Exclude(ClockHandle* h) { h->SetIsVisible(false); }

int ClockHandleTable::FindVisibleElement(const Slice& key, int& probe,
                                         int displacement) {
  return FindSlot(
      key, [&](ClockHandle* h) { return h->Matches(key) && h->IsVisible(); },
      probe, displacement);
}

int ClockHandleTable::FindAvailableSlot(const Slice& key, int& probe,
                                        int displacement) {
  return FindSlot(
      key, [](ClockHandle* h) { return h->IsEmpty() || h->IsTombstone(); },
      probe, displacement);
}

int ClockHandleTable::FindVisibleElementOrAvailableSlot(const Slice& key,
                                                        int& probe,
                                                        int displacement) {
  return FindSlot(
      key,
      [&](ClockHandle* h) {
        return h->IsEmpty() || h->IsTombstone() ||
               (h->Matches(key) && h->IsVisible());
      },
      probe, displacement);
}

inline int ClockHandleTable::FindSlot(const Slice& key,
                                      std::function<bool(ClockHandle*)> cond,
                                      int& probe, int displacement) {
  uint32_t base = ModTableSize(Hash(key.data(), key.size(), kProbingSeed1));
  uint32_t increment =
      ModTableSize((Hash(key.data(), key.size(), kProbingSeed2) << 1) | 1);
  uint32_t current = ModTableSize(base + probe * increment);
  while (true) {
    ClockHandle* h = &array_[current];
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

ClockCacheShard::ClockCacheShard(
    size_t capacity, size_t estimated_value_size, bool strict_capacity_limit,
    CacheMetadataChargePolicy metadata_charge_policy)
    : capacity_(capacity),
      strict_capacity_limit_(strict_capacity_limit),
      clock_pointer_(0),
      table_(
          CalcHashBits(capacity, estimated_value_size, metadata_charge_policy)),
      usage_(0),
      clock_usage_(0) {
  set_metadata_charge_policy(metadata_charge_policy);
}

void ClockCacheShard::EraseUnRefEntries() {
  autovector<ClockHandle> last_reference_list;
  {
    DMutexLock l(mutex_);
    uint32_t slot = 0;
    do {
      ClockHandle* old = &(table_.array_[slot]);
      if (!old->IsInClockList()) {
        continue;
      }
      ClockRemove(old);
      table_.Remove(old);
      assert(usage_ >= old->total_charge);
      usage_ -= old->total_charge;
      last_reference_list.push_back(*old);
      slot = table_.ModTableSize(slot + 1);
    } while (slot != 0);
  }

  // Free the entries here outside of mutex for performance reasons.
  for (auto& h : last_reference_list) {
    h.FreeData();
  }
}

void ClockCacheShard::ApplyToSomeEntries(
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
       metadata_charge_policy = metadata_charge_policy_](ClockHandle* h) {
        callback(h->key(), h->value, h->GetCharge(metadata_charge_policy),
                 h->deleter);
      },
      index_begin, index_end);
}

void ClockCacheShard::ClockRemove(ClockHandle* h) {
  assert(h->IsInClockList());
  h->SetClockPriority(ClockHandle::ClockPriority::NONE);
  assert(clock_usage_ >= h->total_charge);
  clock_usage_ -= h->total_charge;
}

void ClockCacheShard::ClockInsert(ClockHandle* h) {
  assert(!h->IsInClockList());
  bool is_high_priority =
      h->HasHit() || h->GetCachePriority() == Cache::Priority::HIGH;
  h->SetClockPriority(static_cast<ClockHandle::ClockPriority>(
      is_high_priority * ClockHandle::ClockPriority::HIGH +
      (1 - is_high_priority) * ClockHandle::ClockPriority::MEDIUM));
  clock_usage_ += h->total_charge;
}

void ClockCacheShard::EvictFromClock(size_t charge,
                                     autovector<ClockHandle>* deleted) {
  assert(charge <= capacity_);
  while (clock_usage_ > 0 && (usage_ + charge) > capacity_) {
    ClockHandle* old = &table_.array_[clock_pointer_];
    clock_pointer_ = table_.ModTableSize(clock_pointer_ + 1);
    // Clock list contains only elements which can be evicted.
    if (!old->IsInClockList()) {
      continue;
    }
    if (old->GetClockPriority() == ClockHandle::ClockPriority::LOW) {
      ClockRemove(old);
      table_.Remove(old);
      assert(usage_ >= old->total_charge);
      usage_ -= old->total_charge;
      deleted->push_back(*old);
      return;
    }
    old->DecreaseClockPriority();
  }
}

size_t ClockCacheShard::CalcEstimatedHandleCharge(
    size_t estimated_value_size,
    CacheMetadataChargePolicy metadata_charge_policy) {
  ClockHandle h;
  h.CalcTotalCharge(estimated_value_size, metadata_charge_policy);
  return h.total_charge;
}

int ClockCacheShard::CalcHashBits(
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

void ClockCacheShard::SetCapacity(size_t capacity) {
  assert(false);  // Not supported. TODO(Guido) Support it?
  autovector<ClockHandle> last_reference_list;
  {
    DMutexLock l(mutex_);
    capacity_ = capacity;
    EvictFromClock(0, &last_reference_list);
  }

  // Free the entries here outside of mutex for performance reasons.
  for (auto& h : last_reference_list) {
    h.FreeData();
  }
}

void ClockCacheShard::SetStrictCapacityLimit(bool strict_capacity_limit) {
  DMutexLock l(mutex_);
  strict_capacity_limit_ = strict_capacity_limit;
}

Status ClockCacheShard::Insert(const Slice& key, uint32_t hash, void* value,
                               size_t charge, Cache::DeleterFn deleter,
                               Cache::Handle** handle,
                               Cache::Priority priority) {
  if (key.size() != kCacheKeySize) {
    return Status::NotSupported("ClockCache only supports key size " +
                                std::to_string(kCacheKeySize) + "B");
  }

  ClockHandle tmp;
  tmp.value = value;
  tmp.deleter = deleter;
  tmp.hash = hash;
  tmp.CalcTotalCharge(charge, metadata_charge_policy_);
  tmp.SetCachePriority(priority);
  for (int i = 0; i < kCacheKeySize; i++) {
    tmp.key_data[i] = key.data()[i];
  }

  Status s = Status::OK();
  autovector<ClockHandle> last_reference_list;
  {
    DMutexLock l(mutex_);
    assert(table_.GetOccupancy() <= table_.GetOccupancyLimit());
    // Free the space following strict clock policy until enough space
    // is freed or the clock list is empty.
    EvictFromClock(tmp.total_charge, &last_reference_list);
    if ((usage_ + tmp.total_charge > capacity_ &&
         (strict_capacity_limit_ || handle == nullptr)) ||
        table_.GetOccupancy() == table_.GetOccupancyLimit()) {
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
      ClockHandle* old;
      ClockHandle* h = table_.Insert(&tmp, &old);
      assert(h != nullptr);  // We're below occupancy, so this insertion should
                             // never fail.
      usage_ += h->total_charge;
      if (old != nullptr) {
        s = Status::OkOverwritten();
        assert(old->IsVisible());
        table_.Exclude(old);
        if (!old->HasRefs()) {
          // old is in clock because it's in cache and its reference count is 0.
          ClockRemove(old);
          table_.Remove(old);
          assert(usage_ >= old->total_charge);
          usage_ -= old->total_charge;
          last_reference_list.push_back(*old);
        }
      }
      if (handle == nullptr) {
        ClockInsert(h);
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

Cache::Handle* ClockCacheShard::Lookup(const Slice& key, uint32_t /* hash */) {
  ClockHandle* h = nullptr;
  {
    DMutexLock l(mutex_);
    h = table_.Lookup(key);
    if (h != nullptr) {
      assert(h->IsVisible());
      if (!h->HasRefs()) {
        // The entry is in clock since it's in the hash table and has no
        // external references.
        ClockRemove(h);
      }
      h->Ref();
      h->SetHit();
    }
  }
  return reinterpret_cast<Cache::Handle*>(h);
}

bool ClockCacheShard::Ref(Cache::Handle* h) {
  ClockHandle* e = reinterpret_cast<ClockHandle*>(h);
  DMutexLock l(mutex_);
  // To create another reference - entry must be already externally referenced.
  assert(e->HasRefs());
  e->Ref();
  return true;
}

bool ClockCacheShard::Release(Cache::Handle* handle, bool erase_if_last_ref) {
  if (handle == nullptr) {
    return false;
  }
  ClockHandle* h = reinterpret_cast<ClockHandle*>(handle);
  ClockHandle copy;
  bool last_reference = false;
  assert(!h->IsInClockList());
  {
    DMutexLock l(mutex_);
    last_reference = h->Unref();
    if (last_reference && h->IsVisible()) {
      // The item is still in cache, and nobody else holds a reference to it.
      if (usage_ > capacity_ || erase_if_last_ref) {
        // The clock list must be empty since the cache is full.
        assert(clock_usage_ == 0 || erase_if_last_ref);
        // Take this opportunity and remove the item.
        table_.Remove(h);
      } else {
        // Put the item back on the clock list, and don't free it.
        ClockInsert(h);
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

void ClockCacheShard::Erase(const Slice& key, uint32_t /* hash */) {
  ClockHandle copy;
  bool last_reference = false;
  {
    DMutexLock l(mutex_);
    ClockHandle* h = table_.Lookup(key);
    if (h != nullptr) {
      table_.Exclude(h);
      if (!h->HasRefs()) {
        // The entry is in Clock since it's in cache and has no external
        // references.
        ClockRemove(h);
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

size_t ClockCacheShard::GetUsage() const {
  DMutexLock l(mutex_);
  return usage_;
}

size_t ClockCacheShard::GetPinnedUsage() const {
  DMutexLock l(mutex_);
  assert(usage_ >= clock_usage_);
  return usage_ - clock_usage_;
}

std::string ClockCacheShard::GetPrintableOptions() const {
  return std::string{};
}

ClockCache::ClockCache(size_t capacity, size_t estimated_value_size,
                       int num_shard_bits, bool strict_capacity_limit,
                       CacheMetadataChargePolicy metadata_charge_policy)
    : ShardedCache(capacity, num_shard_bits, strict_capacity_limit) {
  assert(estimated_value_size > 0 ||
         metadata_charge_policy != kDontChargeCacheMetadata);
  num_shards_ = 1 << num_shard_bits;
  shards_ = reinterpret_cast<ClockCacheShard*>(
      port::cacheline_aligned_alloc(sizeof(ClockCacheShard) * num_shards_));
  size_t per_shard = (capacity + (num_shards_ - 1)) / num_shards_;
  for (int i = 0; i < num_shards_; i++) {
    new (&shards_[i])
        ClockCacheShard(per_shard, estimated_value_size, strict_capacity_limit,
                        metadata_charge_policy);
  }
}

ClockCache::~ClockCache() {
  if (shards_ != nullptr) {
    assert(num_shards_ > 0);
    for (int i = 0; i < num_shards_; i++) {
      shards_[i].~ClockCacheShard();
    }
    port::cacheline_aligned_free(shards_);
  }
}

CacheShard* ClockCache::GetShard(uint32_t shard) {
  return reinterpret_cast<CacheShard*>(&shards_[shard]);
}

const CacheShard* ClockCache::GetShard(uint32_t shard) const {
  return reinterpret_cast<CacheShard*>(&shards_[shard]);
}

void* ClockCache::Value(Handle* handle) {
  return reinterpret_cast<const ClockHandle*>(handle)->value;
}

size_t ClockCache::GetCharge(Handle* handle) const {
  CacheMetadataChargePolicy metadata_charge_policy = kDontChargeCacheMetadata;
  if (num_shards_ > 0) {
    metadata_charge_policy = shards_[0].metadata_charge_policy_;
  }
  return reinterpret_cast<const ClockHandle*>(handle)->GetCharge(
      metadata_charge_policy);
}

Cache::DeleterFn ClockCache::GetDeleter(Handle* handle) const {
  auto h = reinterpret_cast<const ClockHandle*>(handle);
  return h->deleter;
}

uint32_t ClockCache::GetHash(Handle* handle) const {
  return reinterpret_cast<const ClockHandle*>(handle)->hash;
}

void ClockCache::DisownData() {
  // Leak data only if that won't generate an ASAN/valgrind warning.
  if (!kMustFreeHeapAllocations) {
    shards_ = nullptr;
    num_shards_ = 0;
  }
}

}  // namespace clock_cache

std::shared_ptr<Cache> NewClockCache(
    size_t capacity, size_t estimated_value_size, int num_shard_bits,
    bool strict_capacity_limit,
    CacheMetadataChargePolicy metadata_charge_policy) {
  if (num_shard_bits >= 20) {
    return nullptr;  // The cache cannot be sharded into too many fine pieces.
  }
  if (num_shard_bits < 0) {
    num_shard_bits = GetDefaultCacheShardBits(capacity);
  }
  return std::make_shared<clock_cache::ClockCache>(
      capacity, estimated_value_size, num_shard_bits, strict_capacity_limit,
      metadata_charge_policy);
}

}  // namespace ROCKSDB_NAMESPACE

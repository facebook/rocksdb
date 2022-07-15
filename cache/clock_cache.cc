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
  ApplyToEntriesRange([](ClockHandle* h) { h->FreeData(); }, 0, GetTableSize(),
                      true);
}

ClockHandle* ClockHandleTable::Lookup(const Slice& key, uint32_t hash) {
  uint32_t probe = 0;
  int slot = FindElement(key, hash, probe);
  return (slot == -1) ? nullptr : &array_[slot];
}

ClockHandle* ClockHandleTable::Insert(ClockHandle* h, ClockHandle** old) {
  uint32_t probe = 0;
  int slot = FindElementOrAvailableSlot(h->key(), h->hash, probe);
  *old = nullptr;
  if (slot == -1) {
    // The key is not already present, and there's no available slot to place
    // the new copy.
    return nullptr;
  }

  if (!array_[slot].IsElement()) {
    // The slot is empty or is a tombstone.
    ClockHandle* new_entry = &array_[slot];
    new_entry->InternalToExclusiveRef();
    Assign(new_entry, h);
    if (new_entry->displacements == 0) {
      // The slot was empty.
      return new_entry;
    }
    // It used to be a tombstone, so there may already be a copy of the
    // key in the table.
    slot = FindElement(h->key(), h->hash, probe);
    if (slot == -1) {
      // Nope, no existing copy of the key.
      return new_entry;
    }
    ClockHandle* old_entry = &array_[slot];
    old_entry->ReleaseInternalRef();
    *old = old_entry;
    return new_entry;
  } else {
    // There is an existing copy of the key.
    ClockHandle* old_entry = &array_[slot];
    old_entry->ReleaseInternalRef();
    *old = old_entry;
    // Find an available slot for the new element.
    old_entry->displacements++;
    slot = FindAvailableSlot(h->key(), probe);
    if (slot == -1) {
      // No available slots.
      return nullptr;
    }
    ClockHandle* new_entry = &array_[slot];
    new_entry->InternalToExclusiveRef();
    Assign(new_entry, h);
    return new_entry;
  }
}

void ClockHandleTable::Remove(ClockHandle* h) {
  assert(!h->IsInClock());  // Already off clock.
  uint32_t probe = 0;
  FindSlot(
      h->key(), [&](ClockHandle* e) { return e == h; },
      [&](ClockHandle* /*e*/) { return false; },
      [&](ClockHandle* e) { e->displacements--; }, probe);
  h->SetWillBeDeleted(false);
  h->SetIsElement(false);
  occupancy_--;
}

void ClockHandleTable::Assign(ClockHandle* dst, ClockHandle* src) {
  // DON'T touch displacements and refs.
  dst->value = src->value;
  dst->deleter = src->deleter;
  dst->hash = src->hash;
  dst->total_charge = src->total_charge;
  dst->key_data = src->key_data;
  dst->flags.store(0);
  dst->SetIsElement(true);
  dst->SetClockPriority(ClockHandle::ClockPriority::NONE);
  dst->SetCachePriority(src->GetCachePriority());
  occupancy_++;
}

int ClockHandleTable::FindElement(const Slice& key, uint32_t hash,
                                  uint32_t& probe) {
  return FindSlot(
      key,
      [&](ClockHandle* h) {
        if (h->TryInternalRef()) {
          if (h->Matches(key, hash)) {
            return true;
          }
          h->ReleaseInternalRef();
        }
        return false;
      },
      [&](ClockHandle* h) { return h->displacements == 0; },
      [&](ClockHandle* /*h*/) {}, probe);
}

int ClockHandleTable::FindAvailableSlot(const Slice& key, uint32_t& probe) {
  int slot = FindSlot(
      key,
      [&](ClockHandle* h) {
        if (h->TryInternalRef()) {
          if (!h->IsElement()) {
            return true;
          }
          h->ReleaseInternalRef();
        }
        return false;
      },
      [&](ClockHandle* /*h*/) { return false; },
      [&](ClockHandle* h) { h->displacements++; }, probe);
  if (slot == -1) {
    Rollback(key, probe);
  }
  return slot;
}

int ClockHandleTable::FindElementOrAvailableSlot(const Slice& key,
                                                 uint32_t hash,
                                                 uint32_t& probe) {
  int slot = FindSlot(
      key,
      [&](ClockHandle* h) {
        if (h->TryInternalRef()) {
          if (!h->IsElement() || h->Matches(key, hash)) {
            return true;
          }
          h->ReleaseInternalRef();
        }
        return false;
      },
      [&](ClockHandle* /*h*/) { return false; },
      [&](ClockHandle* h) { h->displacements++; }, probe);
  if (slot == -1) {
    Rollback(key, probe);
  }
  return slot;
}

int ClockHandleTable::FindSlot(const Slice& key,
                               std::function<bool(ClockHandle*)> match,
                               std::function<bool(ClockHandle*)> abort,
                               std::function<void(ClockHandle*)> update,
                               uint32_t& probe) {
  // We use double-hashing probing. Every probe in the sequence is a
  // pseudorandom integer, computed as a linear function of two random hashes,
  // which we call base and increment. Specifically, the i-th probe is base + i
  // * increment modulo the table size.
  uint32_t base = ModTableSize(Hash(key.data(), key.size(), kProbingSeed1));
  // We use an odd increment, which is relatively prime with the power-of-two
  // table size. This implies that we cycle back to the first probe only
  // after probing every slot exactly once.
  uint32_t increment =
      ModTableSize((Hash(key.data(), key.size(), kProbingSeed2) << 1) | 1);
  uint32_t current = ModTableSize(base + probe * increment);
  while (true) {
    ClockHandle* h = &array_[current];
    if (current == base && probe > 0) {
      // We looped back.
      return -1;
    }
    if (match(h)) {
      probe++;
      return current;
    }
    if (abort(h)) {
      return -1;
    }
    probe++;
    update(h);
    current = ModTableSize(current + increment);
  }
}

void ClockHandleTable::Rollback(const Slice& key, uint32_t probe) {
  uint32_t current = ModTableSize(Hash(key.data(), key.size(), kProbingSeed1));
  uint32_t increment =
      ModTableSize((Hash(key.data(), key.size(), kProbingSeed2) << 1) | 1);
  for (uint32_t i = 0; i < probe; i++) {
    array_[current].displacements--;
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
      usage_(0) {
  set_metadata_charge_policy(metadata_charge_policy);
}

void ClockCacheShard::EraseUnRefEntries() {
  autovector<ClockHandle> last_reference_list;
  {
    DMutexLock l(mutex_);
    table_.ApplyToEntriesRange(
        [this, &last_reference_list](ClockHandle* h) {
          // Externally unreferenced element.
          last_reference_list.push_back(*h);
          Evict(h);
        },
        0, table_.GetTableSize(), true);
  }

  // Free the entry outside of the mutex for performance reasons.
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
      index_begin, index_end, false);
}

void ClockCacheShard::ClockOff(ClockHandle* h) {
  h->SetClockPriority(ClockHandle::ClockPriority::NONE);
}

void ClockCacheShard::ClockOn(ClockHandle* h) {
  assert(!h->IsInClock());
  bool is_high_priority =
      h->HasHit() || h->GetCachePriority() == Cache::Priority::HIGH;
  h->SetClockPriority(static_cast<ClockHandle::ClockPriority>(
      is_high_priority * ClockHandle::ClockPriority::HIGH +
      (1 - is_high_priority) * ClockHandle::ClockPriority::MEDIUM));
}

void ClockCacheShard::Evict(ClockHandle* h) {
  ClockOff(h);
  table_.Remove(h);
  assert(usage_ >= h->total_charge);
  usage_ -= h->total_charge;
}

void ClockCacheShard::EvictFromClock(size_t charge,
                                     autovector<ClockHandle>* deleted) {
  // TODO(Guido) When an element is in the probe sequence of a
  // hot element, it will be hard to get an exclusive ref.
  // We may need a mechanism to avoid that an element sits forever
  // in cache waiting to be evicted.
  assert(charge <= capacity_);
  uint32_t max_iterations = table_.GetTableSize();
  while (usage_ + charge > capacity_ && max_iterations--) {
    ClockHandle* h = &table_.array_[clock_pointer_];
    clock_pointer_ = table_.ModTableSize(clock_pointer_ + 1);

    if (h->TryExclusiveRef()) {
      if (!h->IsInClock() && h->IsElement()) {
        // We adjust the clock priority to make the element evictable again.
        // Why? Elements that are not in clock are either currently
        // externally referenced or used to be---because we are holding an
        // exclusive ref, we know we are in the latter case. This can only
        // happen when the last external reference to an element was released,
        // and the element was not immediately removed.
        ClockOn(h);
      }

      if (h->GetClockPriority() == ClockHandle::ClockPriority::LOW) {
        deleted->push_back(*h);
        Evict(h);
      } else if (h->GetClockPriority() > ClockHandle::ClockPriority::LOW) {
        h->DecreaseClockPriority();
      }
      h->ReleaseExclusiveRef();
    }
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

  // Free the entry outside of the mutex for performance reasons.
  for (auto& h : last_reference_list) {
    h.FreeData();
  }
}

void ClockCacheShard::SetStrictCapacityLimit(bool strict_capacity_limit) {
  assert(false);  // Not supported. TODO(Guido) Support it?
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
    // is freed or there are no evictable elements.
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
        assert(!old->WillBeDeleted());
        old->SetWillBeDeleted(true);
        // Try to evict the old copy of the element.
        if (old->TryExclusiveRef()) {
          last_reference_list.push_back(*old);
          Evict(old);
          old->ReleaseExclusiveRef();
        }
      }
      if (handle == nullptr) {
        // If the user didn't provide a handle, no reference is taken,
        // so we make the element evictable.
        ClockOn(h);
        h->ReleaseExclusiveRef();
      } else {
        // The caller already holds a ref.
        h->ExclusiveToExternalRef();
        *handle = reinterpret_cast<Cache::Handle*>(h);
      }
    }
  }

  // Free the entry outside of the mutex for performance reasons.
  for (auto& h : last_reference_list) {
    h.FreeData();
  }

  return s;
}

Cache::Handle* ClockCacheShard::Lookup(const Slice& key, uint32_t hash) {
  ClockHandle* h = nullptr;
  h = table_.Lookup(key, hash);
  if (h != nullptr) {
    // TODO(Guido) Comment from #10347: Here it looks like we have three atomic
    // updates where it would be possible to combine into one CAS (more metadata
    // under one atomic field) or maybe two atomic updates (one arithmetic, one
    // bitwise). Something to think about optimizing.
    h->InternalToExternalRef();
    h->SetHit();
    // The handle is now referenced, so we take it out of clock.
    ClockOff(h);
  }
  return reinterpret_cast<Cache::Handle*>(h);
}

bool ClockCacheShard::Ref(Cache::Handle* h) {
  ClockHandle* e = reinterpret_cast<ClockHandle*>(h);
  assert(e->HasExternalRefs());
  return e->TryExternalRef();
}

bool ClockCacheShard::Release(Cache::Handle* handle, bool erase_if_last_ref) {
  // In contrast with LRUCache's Release, this function won't delete the handle
  // when the reference is the last one and the cache is above capacity. Space
  // is only freed up by EvictFromClock (called by Insert when space is needed)
  // and Erase.
  if (handle == nullptr) {
    return false;
  }

  ClockHandle* h = reinterpret_cast<ClockHandle*>(handle);
  uint32_t hash = h->hash;
  uint32_t refs = h->ReleaseExternalRef();
  bool last_reference = !(refs & ClockHandle::EXTERNAL_REFS);
  bool will_be_deleted = refs & ClockHandle::WILL_BE_DELETED;

  if (last_reference && (will_be_deleted || erase_if_last_ref)) {
    // At this point we want to evict the element, so we need to take
    // a lock and an exclusive reference. But there's a problem:
    // as soon as we released the last reference, an Insert or Erase could've
    // replaced this element, and by the time we take the lock and ref
    // we could potentially be referencing a different element.
    // Thus, before evicting the (potentially different) element, we need to
    // re-check that it's unreferenced and marked as WILL_BE_DELETED, so the
    // eviction is safe. Additionally, we check that the hash doesn't change,
    // which will detect, most of the time, whether the element is a different
    // one. The bottomline is that we only guarantee that the input handle will
    // be deleted, and occasionally also another handle, but in any case all
    // deleted handles are safe to delete.
    // TODO(Guido) With lock-free inserts and deletes we may be able to
    // "atomically" transition to an exclusive ref, without creating a deadlock.
    ClockHandle copy;
    {
      DMutexLock l(mutex_);
      if (h->TrySpinExclusiveRef()) {
        will_be_deleted = h->refs & ClockHandle::WILL_BE_DELETED;
        // Check that it's still safe to delete.
        if (h->IsElement() && (will_be_deleted || erase_if_last_ref) &&
            h->hash == hash) {
          copy = *h;
          Evict(h);
        }
        h->ReleaseExclusiveRef();
      } else {
        // An external ref was detected.
        return false;
      }
    }

    // Free the entry outside of the mutex for performance reasons.
    copy.FreeData();
    return true;
  }

  return false;
}

void ClockCacheShard::Erase(const Slice& key, uint32_t hash) {
  ClockHandle copy;
  bool last_reference = false;
  {
    DMutexLock l(mutex_);
    ClockHandle* h = table_.Lookup(key, hash);
    if (h != nullptr) {
      h->SetWillBeDeleted(true);
      h->ReleaseInternalRef();
      if (h->TryExclusiveRef()) {
        copy = *h;
        Evict(h);
        last_reference = true;
        h->ReleaseExclusiveRef();
      }
    }
  }
  // Free the entry outside of the mutex for performance reasons.
  if (last_reference) {
    copy.FreeData();
  }
}

size_t ClockCacheShard::GetUsage() const {
  DMutexLock l(mutex_);
  return usage_;
}

size_t ClockCacheShard::GetPinnedUsage() const {
  // Computes the pinned usage scanning the whole hash table. This
  // is slow, but avoid keeping an exact counter on the clock usage,
  // i.e., the number of not externally referenced elements.
  // Why avoid this? Because Lookup removes elements from the clock
  // list, so it would need to update the pinned usage every time,
  // which creates additional synchronization costs.
  DMutexLock l(mutex_);

  size_t clock_usage = 0;

  table_.ConstApplyToEntriesRange(
      [&clock_usage](ClockHandle* h) {
        if (h->HasExternalRefs()) {
          clock_usage += h->total_charge;
        }
      },
      0, table_.GetTableSize(), true);

  return clock_usage;
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
    size_t capacity, int num_shard_bits, bool strict_capacity_limit,
    CacheMetadataChargePolicy metadata_charge_policy) {
  return NewLRUCache(capacity, num_shard_bits, strict_capacity_limit, 0.5,
                     nullptr, kDefaultToAdaptiveMutex, metadata_charge_policy);
}

std::shared_ptr<Cache> ExperimentalNewClockCache(
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

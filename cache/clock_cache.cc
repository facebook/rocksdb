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
#include "util/hash.h"
#include "util/math.h"
#include "util/random.h"

namespace ROCKSDB_NAMESPACE {

namespace clock_cache {

ClockHandleTable::ClockHandleTable(size_t capacity, int hash_bits)
    : length_bits_(hash_bits),
      length_bits_mask_((uint32_t{1} << length_bits_) - 1),
      occupancy_limit_(static_cast<uint32_t>((uint32_t{1} << length_bits_) *
                                             kStrictLoadFactor)),
      capacity_(capacity),
      array_(new ClockHandle[size_t{1} << length_bits_]),
      clock_pointer_(0),
      occupancy_(0),
      usage_(0) {
  assert(hash_bits <= 32);
}

ClockHandleTable::~ClockHandleTable() {
  // Assumes there are no references (of any type) to any slot in the table.
  for (uint32_t i = 0; i < GetTableSize(); i++) {
    ClockHandle* h = &array_[i];
    if (h->IsElement()) {
      h->FreeData();
    }
  }
}

ClockHandle* ClockHandleTable::Lookup(const Slice& key, uint32_t hash) {
  uint32_t probe = 0;
  ClockHandle* e = FindSlot(
      key,
      [&](ClockHandle* h) {
        if (h->TryInternalRef()) {
          if (h->IsElement() && h->Matches(key, hash)) {
            return true;
          }
          h->ReleaseInternalRef();
        }
        return false;
      },
      [&](ClockHandle* h) { return h->displacements == 0; },
      [&](ClockHandle* /*h*/) {}, probe);

  if (e != nullptr) {
    // TODO(Guido) Comment from #10347: Here it looks like we have three atomic
    // updates where it would be possible to combine into one CAS (more metadata
    // under one atomic field) or maybe two atomic updates (one arithmetic, one
    // bitwise). Something to think about optimizing.
    e->InternalToExternalRef();
    e->SetHit();
    // The handle is now referenced, so we take it out of clock.
    ClockOff(e);
  }

  return e;
}

ClockHandle* ClockHandleTable::Insert(ClockHandle* h,
                                      autovector<ClockHandle>* deleted,
                                      bool take_reference) {
  uint32_t probe = 0;
  ClockHandle* e = FindAvailableSlot(h->key(), h->hash, probe, deleted);
  if (e == nullptr) {
    // No available slot to place the handle.
    return nullptr;
  }

  // The slot is empty or is a tombstone. And we have an exclusive ref.
  Assign(e, h);
  // TODO(Guido) The following RemoveAll can probably be run outside of
  // the exclusive ref. I had a bad case in mind: multiple inserts could
  // annihilate each. Although I think this is impossible, I'm not sure
  // my mental proof covers every case.
  if (e->displacements != 0) {
    // It used to be a tombstone, so there may already be copies of the
    // key in the table.
    RemoveAll(h->key(), h->hash, probe, deleted);
  }

  if (take_reference) {
    // The user wants to take a reference.
    e->ExclusiveToExternalRef();
  } else {
    // The user doesn't want to immediately take a reference, so we make
    // it evictable.
    ClockOn(e);
    e->ReleaseExclusiveRef();
  }
  return e;
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
  dst->SetCachePriority(src->GetCachePriority());
  usage_ += dst->total_charge;
  occupancy_++;
}

bool ClockHandleTable::TryRemove(ClockHandle* h,
                                 autovector<ClockHandle>* deleted) {
  if (h->TryExclusiveRef()) {
    if (h->WillBeDeleted()) {
      Remove(h, deleted);
      return true;
    }
    h->ReleaseExclusiveRef();
  }
  return false;
}

bool ClockHandleTable::SpinTryRemove(ClockHandle* h,
                                     autovector<ClockHandle>* deleted) {
  if (h->SpinTryExclusiveRef()) {
    if (h->WillBeDeleted()) {
      Remove(h, deleted);
      return true;
    }
    h->ReleaseExclusiveRef();
  }
  return false;
}

void ClockHandleTable::ClockOff(ClockHandle* h) {
  h->SetClockPriority(ClockHandle::ClockPriority::NONE);
}

void ClockHandleTable::ClockOn(ClockHandle* h) {
  assert(!h->IsInClock());
  bool is_high_priority =
      h->HasHit() || h->GetCachePriority() == Cache::Priority::HIGH;
  h->SetClockPriority(static_cast<ClockHandle::ClockPriority>(
      is_high_priority ? ClockHandle::ClockPriority::HIGH
                       : ClockHandle::ClockPriority::MEDIUM));
}

void ClockHandleTable::Remove(ClockHandle* h,
                              autovector<ClockHandle>* deleted) {
  deleted->push_back(*h);
  ClockOff(h);
  uint32_t probe = 0;
  FindSlot(
      h->key(), [&](ClockHandle* e) { return e == h; },
      [&](ClockHandle* /*e*/) { return false; },
      [&](ClockHandle* e) { e->displacements--; }, probe);
  h->SetWillBeDeleted(false);
  h->SetIsElement(false);
}

void ClockHandleTable::RemoveAll(const Slice& key, uint32_t hash,
                                 uint32_t& probe,
                                 autovector<ClockHandle>* deleted) {
  FindSlot(
      key,
      [&](ClockHandle* h) {
        if (h->TryInternalRef()) {
          if (h->IsElement() && h->Matches(key, hash)) {
            h->SetWillBeDeleted(true);
            h->ReleaseInternalRef();
            if (TryRemove(h, deleted)) {
              h->ReleaseExclusiveRef();
            }
            return false;
          }
          h->ReleaseInternalRef();
        }
        return false;
      },
      [&](ClockHandle* h) { return h->displacements == 0; },
      [&](ClockHandle* /*h*/) {}, probe);
}

void ClockHandleTable::Free(autovector<ClockHandle>* deleted) {
  if (deleted->size() == 0) {
    // Avoid unnecessarily reading usage_ and occupancy_.
    return;
  }

  size_t deleted_charge = 0;
  for (auto& h : *deleted) {
    deleted_charge += h.total_charge;
    h.FreeData();
  }
  assert(usage_ >= deleted_charge);
  usage_ -= deleted_charge;
  occupancy_ -= static_cast<uint32_t>(deleted->size());
}

ClockHandle* ClockHandleTable::FindAvailableSlot(
    const Slice& key, uint32_t hash, uint32_t& probe,
    autovector<ClockHandle>* deleted) {
  ClockHandle* e = FindSlot(
      key,
      [&](ClockHandle* h) {
        // To read the handle, first acquire a shared ref.
        if (h->TryInternalRef()) {
          if (h->IsElement()) {
            // The slot is not available.
            // TODO(Guido) Is it worth testing h->WillBeDeleted()?
            if (h->WillBeDeleted() || h->Matches(key, hash)) {
              // The slot can be freed up, or the key we're inserting is already
              // in the table, so we try to delete it. When the attempt is
              // successful, the slot becomes available, so we stop probing.
              // Notice that in that case TryRemove returns an exclusive ref.
              h->SetWillBeDeleted(true);
              h->ReleaseInternalRef();
              if (TryRemove(h, deleted)) {
                return true;
              }
              return false;
            }
            h->ReleaseInternalRef();
            return false;
          }

          // Available slot.
          h->ReleaseInternalRef();
          // Try to acquire an exclusive ref. If we fail, continue probing.
          if (h->SpinTryExclusiveRef()) {
            // Check that the slot is still available.
            if (!h->IsElement()) {
              return true;
            }
            h->ReleaseExclusiveRef();
          }
        }
        return false;
      },
      [&](ClockHandle* /*h*/) { return false; },
      [&](ClockHandle* h) { h->displacements++; }, probe);
  if (e == nullptr) {
    Rollback(key, probe);
  }
  return e;
}

ClockHandle* ClockHandleTable::FindSlot(
    const Slice& key, std::function<bool(ClockHandle*)> match,
    std::function<bool(ClockHandle*)> abort,
    std::function<void(ClockHandle*)> update, uint32_t& probe) {
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
      return nullptr;
    }
    if (match(h)) {
      probe++;
      return h;
    }
    if (abort(h)) {
      return nullptr;
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

void ClockHandleTable::ClockRun(size_t charge) {
  // TODO(Guido) When an element is in the probe sequence of a
  // hot element, it will be hard to get an exclusive ref.
  // Do we need a mechanism to prevent an element from sitting
  // for a long time in cache waiting to be evicted?
  assert(charge <= capacity_);
  autovector<ClockHandle> deleted;
  uint32_t max_iterations =
      1 + static_cast<uint32_t>(GetTableSize() * kLoadFactor);
  size_t usage_local = usage_;
  while (usage_local + charge > capacity_ && max_iterations--) {
    uint32_t steps = 1 + static_cast<uint32_t>(1 / kLoadFactor);
    uint32_t clock_pointer_local = (clock_pointer_ += steps) - steps;
    for (uint32_t i = 0; i < steps; i++) {
      ClockHandle* h = &array_[ModTableSize(clock_pointer_local + i)];

      if (h->TryExclusiveRef()) {
        if (h->WillBeDeleted()) {
          Remove(h, &deleted);
          usage_local -= h->total_charge;
        } else {
          if (!h->IsInClock() && h->IsElement()) {
            // We adjust the clock priority to make the element evictable again.
            // Why? Elements that are not in clock are either currently
            // externally referenced or used to be. Because we are holding an
            // exclusive ref, we know we are in the latter case. This can only
            // happen when the last external reference to an element was
            // released, and the element was not immediately removed.

            ClockOn(h);
          }
          ClockHandle::ClockPriority priority = h->GetClockPriority();
          if (priority == ClockHandle::ClockPriority::LOW) {
            Remove(h, &deleted);
            usage_local -= h->total_charge;
          } else if (priority > ClockHandle::ClockPriority::LOW) {
            h->DecreaseClockPriority();
          }
        }
        h->ReleaseExclusiveRef();
      }
    }
  }

  Free(&deleted);
}

ClockCacheShard::ClockCacheShard(
    size_t capacity, size_t estimated_value_size, bool strict_capacity_limit,
    CacheMetadataChargePolicy metadata_charge_policy)
    : strict_capacity_limit_(strict_capacity_limit),
      table_(capacity, CalcHashBits(capacity, estimated_value_size,
                                    metadata_charge_policy)) {
  set_metadata_charge_policy(metadata_charge_policy);
}

void ClockCacheShard::EraseUnRefEntries() {
  autovector<ClockHandle> deleted;

  table_.ApplyToEntriesRange(
      [this, &deleted](ClockHandle* h) {
        // Externally unreferenced element.
        table_.Remove(h, &deleted);
      },
      0, table_.GetTableSize(), true);

  table_.Free(&deleted);
}

void ClockCacheShard::ApplyToSomeEntries(
    const std::function<void(const Slice& key, void* value, size_t charge,
                             DeleterFn deleter)>& callback,
    uint32_t average_entries_per_lock, uint32_t* state) {
  // The state is essentially going to be the starting hash, which works
  // nicely even if we resize between calls because we use upper-most
  // hash bits for table indexes.
  uint32_t length_bits = table_.GetLengthBits();
  uint32_t length = table_.GetTableSize();

  assert(average_entries_per_lock > 0);
  // Assuming we are called with same average_entries_per_lock repeatedly,
  // this simplifies some logic (index_end will not overflow).
  assert(average_entries_per_lock < length || *state == 0);

  uint32_t index_begin = *state >> (32 - length_bits);
  uint32_t index_end = index_begin + average_entries_per_lock;
  if (index_end >= length) {
    // Going to end.
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

void ClockCacheShard::SetCapacity(size_t /*capacity*/) {
  assert(false);  // Not supported.
}

void ClockCacheShard::SetStrictCapacityLimit(bool /*strict_capacity_limit*/) {
  assert(false);  // Not supported.
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

  // Free space with the clock policy until enough space is freed or there are
  // no evictable elements.
  table_.ClockRun(tmp.total_charge);

  // occupancy_ and usage_ are contended members across concurrent updates
  // on the same shard, so we use a single copy to reduce cache synchronization.
  uint32_t occupancy_local = table_.GetOccupancy();
  size_t usage_local = table_.GetUsage();
  assert(occupancy_local <= table_.GetOccupancyLimit());

  autovector<ClockHandle> deleted;

  if ((usage_local + tmp.total_charge > table_.GetCapacity() &&
       (strict_capacity_limit_ || handle == nullptr)) ||
      occupancy_local > table_.GetOccupancyLimit()) {
    if (handle == nullptr) {
      // Don't insert the entry but still return ok, as if the entry inserted
      // into cache and get evicted immediately.
      deleted.push_back(tmp);
    } else {
      if (occupancy_local > table_.GetOccupancyLimit()) {
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
    ClockHandle* h = table_.Insert(&tmp, &deleted, handle != nullptr);
    assert(h != nullptr);  // The occupancy is way below the table size, so this
                           // insertion should never fail.
    if (handle != nullptr) {
      *handle = reinterpret_cast<Cache::Handle*>(h);
    }

    if (deleted.size() > 0) {
      s = Status::OkOverwritten();
    }
  }

  table_.Free(&deleted);

  return s;
}

Cache::Handle* ClockCacheShard::Lookup(const Slice& key, uint32_t hash) {
  return reinterpret_cast<Cache::Handle*>(table_.Lookup(key, hash));
}

bool ClockCacheShard::Ref(Cache::Handle* h) {
  ClockHandle* e = reinterpret_cast<ClockHandle*>(h);
  assert(e->HasExternalRefs());
  return e->TryExternalRef();
}

bool ClockCacheShard::Release(Cache::Handle* handle, bool erase_if_last_ref) {
  // In contrast with LRUCache's Release, this function won't delete the handle
  // when the cache is above capacity and the reference is the last one. Space
  // is only freed up by EvictFromClock (called by Insert when space is needed)
  // and Erase. We do this to avoid an extra atomic read of the variable usage_.
  if (handle == nullptr) {
    return false;
  }

  ClockHandle* h = reinterpret_cast<ClockHandle*>(handle);
  uint32_t refs = h->refs;
  bool last_reference = ((refs & ClockHandle::EXTERNAL_REFS) == 1);
  bool will_be_deleted = refs & ClockHandle::WILL_BE_DELETED;

  if (last_reference && (will_be_deleted || erase_if_last_ref)) {
    autovector<ClockHandle> deleted;
    h->SetWillBeDeleted(true);
    h->ReleaseExternalRef();
    if (table_.SpinTryRemove(h, &deleted)) {
      h->ReleaseExclusiveRef();
      table_.Free(&deleted);
      return true;
    }
  } else {
    h->ReleaseExternalRef();
  }

  return false;
}

void ClockCacheShard::Erase(const Slice& key, uint32_t hash) {
  autovector<ClockHandle> deleted;
  uint32_t probe = 0;
  table_.RemoveAll(key, hash, probe, &deleted);
  table_.Free(&deleted);
}

size_t ClockCacheShard::GetUsage() const { return table_.GetUsage(); }

size_t ClockCacheShard::GetPinnedUsage() const {
  // Computes the pinned usage by scanning the whole hash table. This
  // is slow, but avoids keeping an exact counter on the clock usage,
  // i.e., the number of not externally referenced elements.
  // Why avoid this counter? Because Lookup removes elements from the clock
  // list, so it would need to update the pinned usage every time,
  // which creates additional synchronization costs.
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

ClockCache::ClockCache(size_t capacity, size_t estimated_value_size,
                       int num_shard_bits, bool strict_capacity_limit,
                       CacheMetadataChargePolicy metadata_charge_policy)
    : ShardedCache(capacity, num_shard_bits, strict_capacity_limit),
      num_shards_(1 << num_shard_bits) {
  assert(estimated_value_size > 0 ||
         metadata_charge_policy != kDontChargeCacheMetadata);
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

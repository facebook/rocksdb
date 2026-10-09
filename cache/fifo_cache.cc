//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "cache/fifo_cache.h"

#include <cassert>
#include <cstdint>
#include <cstdio>
#include <cstdlib>

#include "cache/secondary_cache_adapter.h"
#include "port/lang.h"
#include "util/distributed_mutex.h"

namespace ROCKSDB_NAMESPACE {
namespace fifo_cache {

FIFOCacheShard::FIFOHandleTable::FIFOHandleTable(int max_upper_hash_bits,
                                                 MemoryAllocator* allocator)
    : length_bits_(4),
      list_(new FIFOHandle* [size_t{1} << length_bits_] {}),
      elems_(0),
      max_length_bits_(max_upper_hash_bits),
      allocator_(allocator) {}

FIFOCacheShard::FIFOHandleTable::~FIFOHandleTable() {
  auto* alloc = allocator_;
  ApplyToEntriesRange(
      [alloc](FIFOHandle* h) {
        if (!h->HasRefs()) {
          h->Free(alloc);
        }
      },
      0, GetLength());
}

FIFOHandle* FIFOCacheShard::FIFOHandleTable::Lookup(const Slice& key,
                                                    uint32_t hash) {
  return *FindPointer(key, hash);
}

FIFOHandle* FIFOCacheShard::FIFOHandleTable::Insert(FIFOHandle* h) {
  FIFOHandle** ptr = FindPointer(h->key(), h->hash);
  FIFOHandle* old = *ptr;
  h->next_hash = (old == nullptr ? nullptr : old->next_hash);
  *ptr = h;
  if (old == nullptr) {
    ++elems_;
    if ((elems_ >> length_bits_) > 0) {
      Resize();
    }
  }
  return old;
}

FIFOHandle* FIFOCacheShard::FIFOHandleTable::Remove(const Slice& key,
                                                    uint32_t hash) {
  FIFOHandle** ptr = FindPointer(key, hash);
  FIFOHandle* result = *ptr;
  if (result != nullptr) {
    *ptr = result->next_hash;
    --elems_;
  }
  return result;
}

FIFOHandle** FIFOCacheShard::FIFOHandleTable::FindPointer(const Slice& key,
                                                          uint32_t hash) {
  FIFOHandle** ptr = &list_[hash >> (32 - length_bits_)];
  while (*ptr != nullptr && ((*ptr)->hash != hash || key != (*ptr)->key())) {
    ptr = &(*ptr)->next_hash;
  }
  return ptr;
}

void FIFOCacheShard::FIFOHandleTable::Resize() {
  if (length_bits_ >= max_length_bits_ || length_bits_ >= 31) {
    return;
  }

  const uint32_t old_length = uint32_t{1} << length_bits_;
  const int new_length_bits = length_bits_ + 1;
  std::unique_ptr<FIFOHandle*[]> new_list{
      new FIFOHandle* [size_t{1} << new_length_bits] {}};
  for (uint32_t i = 0; i < old_length; ++i) {
    FIFOHandle* h = list_[i];
    while (h != nullptr) {
      FIFOHandle* next = h->next_hash;
      FIFOHandle** ptr = &new_list[h->hash >> (32 - new_length_bits)];
      h->next_hash = *ptr;
      *ptr = h;
      h = next;
    }
  }
  list_ = std::move(new_list);
  length_bits_ = new_length_bits;
}

FIFOCacheShard::FIFOCacheShard(size_t capacity, bool strict_capacity_limit,
                               bool use_adaptive_mutex,
                               CacheMetadataChargePolicy metadata_charge_policy,
                               int max_upper_hash_bits,
                               MemoryAllocator* allocator,
                               const Cache::EvictionCallback* eviction_callback)
    : CacheShardBase(metadata_charge_policy),
      capacity_(0),
      strict_capacity_limit_(strict_capacity_limit),
      table_(max_upper_hash_bits, allocator),
      usage_(GetTableMetaCharge()),
      pinned_usage_(0),
      mutex_(use_adaptive_mutex),
      allocator_(allocator),
      eviction_callback_(*eviction_callback) {
  // Make empty circular linked list.
  fifo_.next = &fifo_;
  fifo_.prev = &fifo_;
  SetCapacity(capacity);
}

FIFOCacheShard::~FIFOCacheShard() {}

void FIFOCacheShard::EraseUnRefEntries() {
  autovector<FIFOHandle*> last_reference_list;
  {
    DMutexLock l(mutex_);
    FIFOHandle* cur = fifo_.next;
    while (cur != &fifo_) {
      FIFOHandle* next = cur->next;
      if (!cur->HasRefs()) {
        assert(cur->InCache());
        FIFO_Remove(cur);
        const size_t table_meta_before = GetTableMetaCharge();
        table_.Remove(cur->key(), cur->hash);
        const size_t table_meta_after = GetTableMetaCharge();
        cur->SetInCache(false);
        assert(usage_ >= table_meta_before - table_meta_after);
        usage_ -= table_meta_before - table_meta_after;
        assert(usage_ >= cur->total_charge);
        usage_ -= cur->total_charge;
        last_reference_list.push_back(cur);
      }
      cur = next;
    }
  }

  for (auto entry : last_reference_list) {
    entry->Free(allocator_);
  }
}

void FIFOCacheShard::ApplyToSomeEntries(
    const std::function<void(const Slice& key, Cache::ObjectPtr value,
                             size_t charge,
                             const Cache::CacheItemHelper* helper)>& callback,
    size_t average_entries_per_lock, size_t* state) {
  DMutexLock l(mutex_);
  assert(average_entries_per_lock > 0);
  if (*state == SIZE_MAX || table_.GetOccupancyCount() == 0) {
    *state = SIZE_MAX;
    return;
  }
  const int length_bits = table_.GetLengthBits();
  const size_t length = size_t{1} << length_bits;
  const size_t index_begin = *state >> (sizeof(size_t) * 8u - length_bits);
  size_t index_end = index_begin + average_entries_per_lock;
  if (index_end >= length) {
    index_end = length;
    *state = SIZE_MAX;
  } else {
    *state = index_end << (sizeof(size_t) * 8u - length_bits);
  }
  table_.ApplyToEntriesRange(
      [callback,
       metadata_charge_policy = metadata_charge_policy_](FIFOHandle* h) {
        callback(h->key(), h->value, h->GetCharge(metadata_charge_policy),
                 h->helper);
      },
      index_begin, index_end);
}

void FIFOCacheShard::TEST_GetFIFOList(FIFOHandle** fifo) {
  DMutexLock l(mutex_);
  *fifo = &fifo_;
}

size_t FIFOCacheShard::TEST_GetFIFOSize() {
  DMutexLock l(mutex_);
  FIFOHandle* handle = fifo_.next;
  size_t size = 0;
  while (handle != &fifo_) {
    size++;
    handle = handle->next;
  }
  return size;
}

void FIFOCacheShard::FIFO_Remove(FIFOHandle* e) {
  assert(e->next != nullptr);
  assert(e->prev != nullptr);
  e->next->prev = e->prev;
  e->prev->next = e->next;
  e->prev = e->next = nullptr;
  // No accounting here: queue membership alone affects neither usage_
  // (in-cache plus detached-pinned) nor pinned_usage_ (refs > 0).
}

void FIFOCacheShard::FIFO_Append(FIFOHandle* e) {
  assert(e->next == nullptr);
  assert(e->prev == nullptr);
  // Append to tail: fifo_.prev is newest, fifo_.next is oldest.
  e->next = &fifo_;
  e->prev = fifo_.prev;
  e->prev->next = e;
  e->next->prev = e;
}

void FIFOCacheShard::EvictFromFIFO(size_t charge,
                                   autovector<FIFOHandle*>* deleted) {
  if ((usage_ + charge) <= capacity_) {
    return;
  }
  size_t need = usage_ + charge - capacity_;
  size_t freed = 0;
  // Each step consumes the head: evict it, or move it to the tail if
  // pinned. The pass ends after the entry that was newest when it began,
  // so it visits each entry at most once and an all-pinned shard
  // terminates having evicted nothing.
  if (fifo_.next == &fifo_) {
    return;
  }
  FIFOHandle* const last = fifo_.prev;
  bool at_last = false;
  while (!at_last && freed < need) {
    FIFOHandle* cur = fifo_.next;
    at_last = cur == last;
    if (cur->HasRefs()) {
      FIFO_Remove(cur);
      FIFO_Append(cur);
    } else {
      assert(cur->InCache());
      FIFO_Remove(cur);
      const size_t table_meta_before = GetTableMetaCharge();
      table_.Remove(cur->key(), cur->hash);
      const size_t table_meta_after = GetTableMetaCharge();
      cur->SetInCache(false);
      assert(usage_ >= table_meta_before - table_meta_after);
      usage_ -= table_meta_before - table_meta_after;
      assert(usage_ >= cur->total_charge);
      usage_ -= cur->total_charge;
      freed += cur->total_charge;
      deleted->push_back(cur);
    }
  }
}

void FIFOCacheShard::NotifyEvicted(
    const autovector<FIFOHandle*>& evicted_handles) {
  for (FIFOHandle* entry : evicted_handles) {
    // was_hit is always false: FIFO keeps no hit state (frequency tracking
    // is out of scope), so eviction callbacks cannot distinguish hits.
    if (eviction_callback_ &&
        eviction_callback_(entry->key(), static_cast<Cache::Handle*>(entry),
                           false)) {
      // Callback took ownership of obj; just free handle
      free(entry);
      entry = nullptr;
    } else {
      // Free the entries here outside of mutex for performance reasons.
      entry->Free(allocator_);
    }
  }
}

void FIFOCacheShard::SetCapacity(size_t capacity) {
  autovector<FIFOHandle*> last_reference_list;
  {
    DMutexLock l(mutex_);
    capacity_ = capacity;
    EvictFromFIFO(0, &last_reference_list);
  }

  NotifyEvicted(last_reference_list);
}

void FIFOCacheShard::SetStrictCapacityLimit(bool strict_capacity_limit) {
  DMutexLock l(mutex_);
  strict_capacity_limit_ = strict_capacity_limit;
}

Status FIFOCacheShard::InsertItem(FIFOHandle* e, FIFOHandle** handle) {
  Status s = Status::OK();
  autovector<FIFOHandle*> last_reference_list;

  {
    DMutexLock l(mutex_);

    // Free the space following strict FIFO policy until enough space
    // is freed or no unpinned entry remains.
    EvictFromFIFO(e->total_charge, &last_reference_list);

    if ((usage_ + e->total_charge) > capacity_ &&
        (strict_capacity_limit_ || handle == nullptr)) {
      e->SetInCache(false);
      if (handle == nullptr) {
        // Don't insert the entry but still return ok, as if the entry inserted
        // into cache and get evicted immediately.
        last_reference_list.push_back(e);
      } else {
        free(e);
        e = nullptr;
        *handle = nullptr;
        s = Status::MemoryLimit("Insert failed due to FIFO cache being full.");
      }
    } else {
      // Insert into the cache. Note that the cache might get larger than its
      // capacity if not enough space was freed up.
      const size_t table_meta_before = GetTableMetaCharge();
      FIFOHandle* old = table_.Insert(e);
      const size_t table_meta_after = GetTableMetaCharge();
      usage_ += table_meta_after - table_meta_before;
      if (old != nullptr) {
        s = Status::OkOverwritten();
        assert(old->InCache());
        old->SetInCache(false);
        FIFO_Remove(old);
        if (!old->HasRefs()) {
          assert(usage_ >= old->total_charge);
          usage_ -= old->total_charge;
          last_reference_list.push_back(old);
        }
        // Else old stays charged until its last Release.
      }
      usage_ += e->total_charge;
      FIFO_Append(e);
      if (handle != nullptr) {
        // If caller already holds a ref, no need to take one here.
        if (!e->HasRefs()) {
          e->Ref();
          pinned_usage_ += e->total_charge;
        }
        *handle = e;
      }
    }
  }

  NotifyEvicted(last_reference_list);

  return s;
}

FIFOHandle* FIFOCacheShard::Lookup(const Slice& key, uint32_t hash,
                                   const Cache::CacheItemHelper* /*helper*/,
                                   Cache::CreateContext* /*create_context*/,
                                   Cache::Priority /*priority*/,
                                   Statistics* /*stats*/) {
  DMutexLock l(mutex_);
  FIFOHandle* e = table_.Lookup(key, hash);
  if (e == nullptr) {
    return nullptr;
  }
  assert(e->InCache());
  if (!e->HasRefs()) {
    pinned_usage_ += e->total_charge;
  }
  e->Ref();
  // FIFO order is insertion order: Lookup never reorders the queue.
  return e;
}

bool FIFOCacheShard::Ref(FIFOHandle* e) {
  DMutexLock l(mutex_);
  // To create another reference - entry must be already externally referenced.
  assert(e->HasRefs());
  e->Ref();
  return true;
}

bool FIFOCacheShard::Release(FIFOHandle* e, bool /*useful*/,
                             bool erase_if_last_ref) {
  if (e == nullptr) {
    return false;
  }
  bool must_free = false;
  {
    DMutexLock l(mutex_);
    if (!e->Unref()) {
      return false;
    }
    // Last reference dropped.
    assert(pinned_usage_ >= e->total_charge);
    pinned_usage_ -= e->total_charge;
    if (e->InCache()) {
      if (usage_ > capacity_ || erase_if_last_ref) {
        // Remove the item. The FIFO queue may still hold pinned entries,
        // so unlike LRU there is no emptiness assertion here.
        FIFO_Remove(e);
        const size_t table_meta_before = GetTableMetaCharge();
        table_.Remove(e->key(), e->hash);
        const size_t table_meta_after = GetTableMetaCharge();
        e->SetInCache(false);
        assert(usage_ >= table_meta_before - table_meta_after);
        usage_ -= table_meta_before - table_meta_after;
        assert(usage_ >= e->total_charge);
        usage_ -= e->total_charge;
        must_free = true;
      } else {
        // Stay in the queue at the insertion position, now unpinned.
        must_free = false;
      }
    } else {
      // Detached (erased/overwritten) or standalone: drop charge and free.
      assert(usage_ >= e->total_charge);
      usage_ -= e->total_charge;
      must_free = true;
    }
  }

  // Free the entry here outside of mutex for performance reasons.
  if (must_free) {
    e->Free(allocator_);
  }
  return must_free;
}

FIFOHandle* FIFOCacheShard::CreateHandle(const Slice& key, uint32_t hash,
                                         Cache::ObjectPtr value,
                                         const Cache::CacheItemHelper* helper,
                                         size_t charge) {
  assert(helper);
  // value == nullptr is reserved for indicating failure in SecondaryCache
  assert(!(helper->IsSecondaryCacheCompatible() && value == nullptr));

  // Allocate the memory here outside of the mutex.
  // If the cache is full, we'll have to release it.
  // It shouldn't happen very often though.
  FIFOHandle* e =
      static_cast<FIFOHandle*>(malloc(sizeof(FIFOHandle) - 1 + key.size()));

  e->value = value;
  e->m_flags = 0;
  e->im_flags = 0;
  e->helper = helper;
  e->key_length = key.size();
  e->hash = hash;
  e->refs = 0;
  e->next = e->prev = nullptr;
  memcpy(e->key_data, key.data(), key.size());
  e->CalcTotalCharge(charge, metadata_charge_policy_);

  return e;
}

Status FIFOCacheShard::Insert(const Slice& key, uint32_t hash,
                              Cache::ObjectPtr value,
                              const Cache::CacheItemHelper* helper,
                              size_t charge, FIFOHandle** handle,
                              Cache::Priority /*priority*/) {
  FIFOHandle* e = CreateHandle(key, hash, value, helper, charge);
  e->SetInCache(true);
  return InsertItem(e, handle);
}

FIFOHandle* FIFOCacheShard::CreateStandalone(
    const Slice& key, uint32_t hash, Cache::ObjectPtr value,
    const Cache::CacheItemHelper* helper, size_t charge, bool allow_uncharged) {
  FIFOHandle* e = CreateHandle(key, hash, value, helper, charge);
  e->SetIsStandalone(true);
  e->Ref();

  autovector<FIFOHandle*> last_reference_list;

  {
    DMutexLock l(mutex_);

    EvictFromFIFO(e->total_charge, &last_reference_list);

    if (strict_capacity_limit_ && (usage_ + e->total_charge) > capacity_) {
      if (allow_uncharged) {
        e->total_charge = 0;
      } else {
        free(e);
        e = nullptr;
      }
    } else {
      usage_ += e->total_charge;
      pinned_usage_ += e->total_charge;
    }
  }

  NotifyEvicted(last_reference_list);
  return e;
}

void FIFOCacheShard::Erase(const Slice& key, uint32_t hash) {
  FIFOHandle* e = nullptr;
  bool last_reference = false;
  {
    DMutexLock l(mutex_);
    const size_t table_meta_before = GetTableMetaCharge();
    e = table_.Remove(key, hash);
    const size_t table_meta_after = GetTableMetaCharge();
    assert(usage_ >= table_meta_before - table_meta_after);
    usage_ -= table_meta_before - table_meta_after;
    if (e != nullptr) {
      assert(e->InCache());
      e->SetInCache(false);
      FIFO_Remove(e);
      if (!e->HasRefs()) {
        assert(usage_ >= e->total_charge);
        usage_ -= e->total_charge;
        last_reference = true;
      }
      // Else e stays charged until its last Release.
    }
  }

  // Free the entry here outside of mutex for performance reasons.
  // last_reference will only be true if e != nullptr.
  if (last_reference) {
    e->Free(allocator_);
  }
}

size_t FIFOCacheShard::GetUsage() const {
  DMutexLock l(mutex_);
  return usage_;
}

size_t FIFOCacheShard::GetPinnedUsage() const {
  DMutexLock l(mutex_);
  return pinned_usage_;
}

size_t FIFOCacheShard::GetOccupancyCount() const {
  DMutexLock l(mutex_);
  return table_.GetOccupancyCount();
}

size_t FIFOCacheShard::GetTableAddressCount() const {
  DMutexLock l(mutex_);
  return table_.GetLength();
}

size_t FIFOCacheShard::GetTableMetaCharge() const {
  if (metadata_charge_policy_ != kFullChargeCacheMetadata) {
    return 0;
  }
  return table_.GetLength() * sizeof(FIFOHandle*);
}

size_t FIFOCacheShard::TEST_GetTableOccupancyCount() const {
  DMutexLock l(mutex_);
  return table_.GetOccupancyCount();
}

void FIFOCacheShard::AppendPrintableOptions(std::string& /*str*/) const {}

FIFOCache::FIFOCache(const FIFOCacheOptions& opts) : ShardedCache(opts) {
  size_t per_shard = GetPerShardCapacity();
  MemoryAllocator* alloc = memory_allocator();
  const int max_upper_hash_bits = 32 - opts.num_shard_bits;
  InitShards([&](FIFOCacheShard* cs) {
    new (cs)
        FIFOCacheShard(per_shard, opts.strict_capacity_limit,
                       opts.use_adaptive_mutex, opts.metadata_charge_policy,
                       max_upper_hash_bits, alloc, &eviction_callback_);
  });
}

Cache::ObjectPtr FIFOCache::Value(Handle* handle) {
  auto h = static_cast<const FIFOHandle*>(handle);
  return h->value;
}

size_t FIFOCache::GetCharge(Handle* handle) const {
  return static_cast<const FIFOHandle*>(handle)->GetCharge(
      GetShard(0).metadata_charge_policy_);
}

const Cache::CacheItemHelper* FIFOCache::GetCacheItemHelper(
    Handle* handle) const {
  auto h = static_cast<const FIFOHandle*>(handle);
  return h->helper;
}

void FIFOCache::ApplyToHandle(
    Cache* cache, Handle* handle,
    const std::function<void(const Slice& key, ObjectPtr value, size_t charge,
                             const CacheItemHelper* helper)>& callback) {
  auto cache_ptr = static_cast<FIFOCache*>(cache);
  auto h = static_cast<const FIFOHandle*>(handle);
  callback(h->key(), h->value,
           h->GetCharge(cache_ptr->GetShard(0).metadata_charge_policy_),
           h->helper);
}

size_t FIFOCache::TEST_GetFIFOSize() {
  return SumOverShards(
      [](FIFOCacheShard& cs) { return cs.TEST_GetFIFOSize(); });
}

}  // namespace fifo_cache

std::shared_ptr<Cache> FIFOCacheOptions::MakeSharedCache() const {
  if (num_shard_bits >= 20) {
    return nullptr;  // The cache cannot be sharded into too many fine pieces.
  }
  // For sanitized options
  FIFOCacheOptions opts = *this;
  if (opts.num_shard_bits < 0) {
    opts.num_shard_bits = GetDefaultCacheShardBits(capacity);
  }
  std::shared_ptr<Cache> cache = std::make_shared<FIFOCache>(opts);
  if (secondary_cache) {
    cache = std::make_shared<CacheWithSecondaryAdapter>(cache, secondary_cache);
  }
  return cache;
}

}  // namespace ROCKSDB_NAMESPACE

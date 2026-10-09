//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
#pragma once

#include <memory>

#include "cache/sharded_cache.h"
#include "port/lang.h"
#include "port/likely.h"
#include "port/malloc.h"
#include "port/port.h"
#include "util/autovector.h"
#include "util/distributed_mutex.h"

namespace ROCKSDB_NAMESPACE {
namespace fifo_cache {

// FIFO cache implementation. This class is not thread-safe.
//
// An entry is a variable length heap-allocated structure. Entries are
// referenced by cache and/or by any external entity. The cache keeps all
// its entries in a hash table and in a FIFO queue ordered by insertion.
//
// FIFOHandle can be in these states:
// 1. Referenced externally AND in hash table.
//    The entry stays in its queue. A probation scan that meets it moves it
//    to the probation tail so later scans do not re-walk it; resident
//    rotates under pressure (see EvictFromFIFO).
//    (refs >= 1 && in_cache == true)
// 2. Not referenced externally AND in hash table.
//    The entry is in one of the queues and can be evicted.
//    (refs == 0 && in_cache == true)
// 3. Referenced externally AND not in hash table.
//    The entry is in neither queue nor the hash table. It is freed
//    when refs drops to 0.
//    (refs >= 1 && in_cache == false)
// Unlike LRU, Lookup never moves an entry, between queues or within one:
// a hit only increments freq. While refs > 0, public properties like value
// and deleter must not change.
struct FIFOHandle : public Cache::Handle {
  Cache::ObjectPtr value;
  const Cache::CacheItemHelper* helper;
  FIFOHandle* next_hash;
  FIFOHandle* next;
  FIFOHandle* prev;
  size_t total_charge;  // TODO(opt): Only allow uint32_t?
  size_t key_length;
  // The hash of key(). Used for fast sharding and comparisons.
  uint32_t hash;
  // The number of external refs to this entry. The cache itself is not counted.
  uint32_t refs;
  // Saturating access counter, range 0..kMaxFrequency. Incremented by Lookup
  // only (never by Ref); promotion and second-chance decisions read it at
  // eviction time. Reset to 0 on promotion to resident.
  uint8_t freq;

  uint8_t m_flags;
  enum MFlags : uint8_t {
    // Whether this entry is referenced by the hash table.
    M_IN_CACHE = (1 << 0),
  };

  uint8_t im_flags;
  enum ImFlags : uint8_t {
    // Marks result handles that should not be inserted into cache
    IM_IS_STANDALONE = (1 << 0),
  };

  // Beginning of the key (MUST BE THE LAST FIELD IN THIS STRUCT!)
  char key_data[1];

  Slice key() const { return Slice(key_data, key_length); }

  // For HandleImpl concept
  uint32_t GetHash() const { return hash; }

  // Increase the reference count by 1.
  void Ref() { refs++; }

  // Just reduce the reference count by 1. Return true if it was last reference.
  bool Unref() {
    assert(refs > 0);
    refs--;
    return refs == 0;
  }

  // Return true if there are external refs, false otherwise.
  bool HasRefs() const { return refs > 0; }

  bool InCache() const { return m_flags & M_IN_CACHE; }
  bool IsStandalone() const { return im_flags & IM_IS_STANDALONE; }

  void SetInCache(bool in_cache) {
    if (in_cache) {
      m_flags |= M_IN_CACHE;
    } else {
      m_flags &= ~M_IN_CACHE;
    }
  }

  void SetIsStandalone(bool is_standalone) {
    if (is_standalone) {
      im_flags |= IM_IS_STANDALONE;
    } else {
      im_flags &= ~IM_IS_STANDALONE;
    }
  }

  void Free(MemoryAllocator* allocator) {
    assert(refs == 0);
    assert(helper);
    if (helper->del_cb) {
      helper->del_cb(value, allocator);
    }

    free(this);
  }

  inline size_t CalcuMetaCharge(
      CacheMetadataChargePolicy metadata_charge_policy) const {
    if (metadata_charge_policy != kFullChargeCacheMetadata) {
      return 0;
    } else {
#ifdef ROCKSDB_MALLOC_USABLE_SIZE
      return malloc_usable_size(
          const_cast<void*>(static_cast<const void*>(this)));
#else
      // This is the size that is used when a new handle is created.
      return sizeof(FIFOHandle) - 1 + key_length;
#endif
    }
  }

  // Calculate the memory usage by metadata.
  inline void CalcTotalCharge(
      size_t charge, CacheMetadataChargePolicy metadata_charge_policy) {
    total_charge = charge + CalcuMetaCharge(metadata_charge_policy);
  }

  inline size_t GetCharge(
      CacheMetadataChargePolicy metadata_charge_policy) const {
    size_t meta_charge = CalcuMetaCharge(metadata_charge_policy);
    assert(total_charge >= meta_charge);
    return total_charge - meta_charge;
  }
};

// A single shard of sharded cache.
//
// Hash table: intrusive hash chains keyed by the precomputed shard hash.
// This avoids per-operation key allocation and lets the bucket array be
// accounted separately from per-entry metadata.
class ALIGN_AS(CACHE_LINE_SIZE) FIFOCacheShard final : public CacheShardBase {
 public:
  // NOTE: the eviction_callback ptr is saved, as is it assumed to be kept
  // alive in Cache.
  FIFOCacheShard(size_t capacity, bool strict_capacity_limit,
                 bool use_adaptive_mutex,
                 CacheMetadataChargePolicy metadata_charge_policy,
                 int max_upper_hash_bits, MemoryAllocator* allocator,
                 const Cache::EvictionCallback* eviction_callback);
  ~FIFOCacheShard();

 public:  // Type definitions expected as parameter to ShardedCache
  using HandleImpl = FIFOHandle;
  using HashVal = uint32_t;
  using HashCref = uint32_t;

 public:  // Function definitions expected as parameter to ShardedCache
  static inline HashVal ComputeHash(const Slice& key, uint32_t seed) {
    return Lower32of64(GetSliceNPHash64(key, seed));
  }

  // Separate from constructor so caller can easily make an array of FIFOCache
  // if current usage is more than new capacity, the function will attempt to
  // free the needed space.
  void SetCapacity(size_t capacity);

  // Set the flag to reject insertion if cache if full.
  void SetStrictCapacityLimit(bool strict_capacity_limit);

  // Like Cache methods, but with an extra "hash" parameter.
  // priority is deliberately ignored: placement is inferred from access
  // frequency (freq), not declared by the caller, so HIGH entries must earn
  // residency through re-access like any other entry.
  Status Insert(const Slice& key, uint32_t hash, Cache::ObjectPtr value,
                const Cache::CacheItemHelper* helper, size_t charge,
                FIFOHandle** handle, Cache::Priority priority);

  FIFOHandle* CreateStandalone(const Slice& key, uint32_t hash,
                               Cache::ObjectPtr obj,
                               const Cache::CacheItemHelper* helper,
                               size_t charge, bool allow_uncharged);

  // helper, create_context, priority and stats are ignored: with no secondary
  // cache configured this is a primary-only lookup. Secondary promotion is
  // handled by CacheWithSecondaryAdapter above the shard, as with LRU.
  // A hit increments freq (saturating) and moves nothing.
  FIFOHandle* Lookup(const Slice& key, uint32_t hash,
                     const Cache::CacheItemHelper* helper,
                     Cache::CreateContext* create_context,
                     Cache::Priority priority, Statistics* stats);

  bool Release(FIFOHandle* handle, bool useful, bool erase_if_last_ref);
  bool Ref(FIFOHandle* handle);
  void Erase(const Slice& key, uint32_t hash);

  // Although in some platforms the update of size_t is atomic, to make sure
  // GetUsage() and GetPinnedUsage() work correctly under any platform, we'll
  // protect them with mutex_.

  size_t GetUsage() const;
  size_t GetPinnedUsage() const;
  size_t GetOccupancyCount() const;
  size_t GetTableAddressCount() const;

  void ApplyToSomeEntries(
      const std::function<void(const Slice& key, Cache::ObjectPtr value,
                               size_t charge,
                               const Cache::CacheItemHelper* helper)>& callback,
      size_t average_entries_per_lock, size_t* state);

  void EraseUnRefEntries();

 public:  // other function definitions
  // Test-only: moves a probation entry to the resident tail without the
  // promotion-threshold check, so tests can build exact resident layouts
  // that pressure-driven promotion cannot assemble without disturbing the
  // entry under test. Not threadsafe.
  void TEST_PromoteToResident(const Slice& key);

  // Probation list only; resident entries are not visible here.
  void TEST_GetFIFOList(FIFOHandle** fifo);

  // Retrieves number of elements in probation, for unit test purpose only.
  // Not threadsafe.
  size_t TEST_GetFIFOSize();

  size_t TEST_GetTableOccupancyCount() const;

  void AppendPrintableOptions(std::string& /*str*/) const;

 private:
  class FIFOHandleTable {
   public:
    explicit FIFOHandleTable(int max_upper_hash_bits,
                             MemoryAllocator* allocator);
    ~FIFOHandleTable();

    FIFOHandle* Lookup(const Slice& key, uint32_t hash);
    FIFOHandle* Insert(FIFOHandle* h);
    FIFOHandle* Remove(const Slice& key, uint32_t hash);

    template <typename T>
    void ApplyToEntriesRange(T func, size_t index_begin, size_t index_end) {
      for (size_t i = index_begin; i < index_end; ++i) {
        FIFOHandle* h = list_[i];
        while (h != nullptr) {
          FIFOHandle* next = h->next_hash;
          assert(h->InCache());
          func(h);
          h = next;
        }
      }
    }

    int GetLengthBits() const { return length_bits_; }
    size_t GetLength() const { return size_t{1} << length_bits_; }
    size_t GetOccupancyCount() const { return elems_; }

   private:
    FIFOHandle** FindPointer(const Slice& key, uint32_t hash);
    void Resize();

    int length_bits_;
    std::unique_ptr<FIFOHandle*[]> list_;
    uint32_t elems_;
    const int max_length_bits_;
    MemoryAllocator* const allocator_;
  };

  size_t GetTableMetaCharge() const;
  friend class FIFOCache;
  // Insert an item into the hash table and append it to the FIFO queue.
  // Older unpinned items are evicted as necessary. Frees `item` on
  // non-OK status.
  Status InsertItem(FIFOHandle* item, FIFOHandle** handle);

  void FIFO_Remove(FIFOHandle* e);
  void FIFO_Append(FIFOHandle* queue, FIFOHandle* e);
  // The resident entry after e in clock order, skipping the dummy head.
  FIFOHandle* ResidentNext(FIFOHandle* e);

  // Frequency counter range and promotion threshold. A Lookup saturates at
  // kMaxFrequency; a probation head at or above kPromoteThreshold is
  // promoted, below it is evicted.
  static constexpr uint8_t kMaxFrequency = 3;
  static constexpr uint8_t kPromoteThreshold = 2;
  // Resident second-chance bound: each eviction costs at most this many
  // passes over resident (3 to drain a saturated counter, 1 to reap). That
  // many consecutive eviction-free passes imply every remaining entry is
  // pinned, so eviction stops.
  static constexpr int kMaxResidentIdlePasses = 4;

  // Free some space until enough to hold (usage_ + charge) is freed or no
  // unpinned entry remains. Probation drains first: one pass over its head,
  // promoting freq >= kPromoteThreshold to the resident tail (reset to 0)
  // and evicting below it; pinned entries move to the probation tail.
  // Resident follows with second-chance: head with freq > 0 is decremented
  // and moved to the tail, head at 0 is evicted, pinned heads rotate
  // undecayed. Promotion touches only links: usage_ and pinned_usage_ are
  // unchanged by a move between queues. The resident head is
  // resident_hand_, so moving it to the tail is advancing the hand: no
  // relinking.
  // The probation pass consumes its head each step, so it ends after at
  // most the initial number of entries; the resident scan is bounded by
  // kMaxResidentIdlePasses per eviction. An all-pinned shard terminates
  // having evicted nothing.
  // This function is not thread safe - it needs to be executed while
  // holding the mutex_.
  void EvictFromFIFO(size_t charge, autovector<FIFOHandle*>* deleted);

  void NotifyEvicted(const autovector<FIFOHandle*>& evicted_handles);

  FIFOHandle* CreateHandle(const Slice& key, uint32_t hash,
                           Cache::ObjectPtr value,
                           const Cache::CacheItemHelper* helper, size_t charge);

  // Initialized before use.
  size_t capacity_;

  // Whether to reject insertion if cache reaches its full capacity.
  bool strict_capacity_limit_;

  // Dummy heads of the two queues. For probation .prev is newest and .next
  // is oldest; resident's order starts at resident_hand_.
  // New entries enter probation; probation victims with enough frequency
  // are promoted to resident, which holds the remainder. Together they
  // contain all in-cache items, pinned and unpinned.
  FIFOHandle probation_{};
  FIFOHandle resident_{};
  // Resident is a clock: its oldest entry is resident_hand_ and its newest
  // the entry linked just before the hand, so a second chance advances the
  // hand instead of relinking the entry. New resident entries are linked
  // just before the hand, and the hand moves past an entry that leaves, so
  // the order is the FIFO-reinsertion order. &resident_ when resident is
  // empty.
  FIFOHandle* resident_hand_;

  // ------------^^^^^^^^^^^^^-----------
  // Not frequently modified data members
  // ------------------------------------
  //
  // We separate data members that are updated frequently from the ones that
  // are not frequently updated so that they don't share the same cache line
  // which will lead into false cache sharing
  //
  // ------------------------------------
  // Frequently modified data members
  // ------------vvvvvvvvvvvvv-----------
  FIFOHandleTable table_;

  // Memory size for entries residing in the cache, including pinned
  // entries detached from the table but still referenced.
  size_t usage_;

  // Memory size for entries with external references. Tracked explicitly
  // because the FIFO queue (unlike an LRU list) contains pinned entries,
  // so pinned usage cannot be derived from queue membership.
  size_t pinned_usage_;

  // mutex_ protects the following state.
  // We don't count mutex_ as the cache's internal state so semantically we
  // don't mind mutex_ invoking the non-const actions.
  mutable DMutex mutex_;

  // From Cache, needed for delete
  MemoryAllocator* const allocator_;

  // A reference to Cache::eviction_callback_
  const Cache::EvictionCallback& eviction_callback_;
};

class FIFOCache
#ifdef NDEBUG
    final
#endif
    : public ShardedCache<FIFOCacheShard> {
 public:
  explicit FIFOCache(const FIFOCacheOptions& opts);
  const char* Name() const override { return "FIFOCache"; }
  ObjectPtr Value(Handle* handle) override;
  size_t GetCharge(Handle* handle) const override;
  const CacheItemHelper* GetCacheItemHelper(Handle* handle) const override;

  void ApplyToHandle(
      Cache* cache, Handle* handle,
      const std::function<void(const Slice& key, ObjectPtr obj, size_t charge,
                               const CacheItemHelper* helper)>& callback)
      override;

  // Retrieves number of elements in FIFO, for unit test purpose only.
  size_t TEST_GetFIFOSize();
};

}  // namespace fifo_cache

using FIFOCache = fifo_cache::FIFOCache;
using FIFOHandle = fifo_cache::FIFOHandle;
using FIFOCacheShard = fifo_cache::FIFOCacheShard;

}  // namespace ROCKSDB_NAMESPACE

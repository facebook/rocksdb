//
// Created by mymai on 2023/6/1.
//

#pragma once


#include "cache/sharded_cache.h"
#include<set>
#include<unordered_map>
#include<chrono>
#include<iostream>
#include<thread>
#include "port/malloc.h"
#include "port/port.h"
#include "util/distributed_mutex.h"


namespace ROCKSDB_NAMESPACE {
namespace lfu_cache {

struct LFUHandle {

  double rect{};

  const Cache::CacheItemHelper* helper_;

  LFUHandle() {

  }

  ~LFUHandle() {

  }

  LFUHandle(const LFUHandle&) = default;

  LFUHandle(LFUHandle&&) = default;

  bool operator==(const LFUHandle* lfuHandle) const {
    if (strcmp(this->key_data, lfuHandle->key_data) == 0) {
      return true;
    }

    return false;
  }

  bool UnRef() {
    assert(refs > 0);
    refs--;
    return refs == 0;
  }

  void Ref() {
    refs++;
  }


  uint32_t GetHash() const { return hash_; }

  size_t key_length_;

  Cache::ObjectPtr value_;

  uint32_t hash_;

  void UnDateVisitTime(std::chrono::time_point<std::chrono::steady_clock> start_clock) {
    std::chrono::time_point t = std::chrono::steady_clock::now();
    const std::chrono::duration<double> current_mills = t - start_clock;
    rect = current_mills.count();
  }

  Slice Key() {
    return Slice(key_data, key_length_);
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
      return sizeof(LFUHandle) - 1 + key_length_;
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

  void Free(MemoryAllocator* allocator) {
    assert(refs == 0);
    if (helper_ && helper_->del_cb) {
      helper_->del_cb(value_, allocator);
    }

    free(this);
  }

  friend class LFUCacheShard;

  size_t total_charge;

  size_t refs{};

  char key_data[1];

};

class LFUCacheShard final : public CacheShardBase {
 public:
  using HandleImpl = LFUHandle;
  using HashVal = uint32_t;
  using HashCref = uint32_t;

  LFUCacheShard(size_t capacity, bool strict_capacity_limit = false,
                CacheMetadataChargePolicy cacheMetadataChargePolicy = kDontChargeCacheMetadata,
                MemoryAllocator* allocator = nullptr) :
                    CacheShardBase(cacheMetadataChargePolicy),
                                       capacity_(capacity),
                                       strict_capacity_limit_(strict_capacity_limit),
                                       memory_allocator_(allocator){

  }

  ~LFUCacheShard();

  void ApplyToSomeEntries(const std::function<void(LFUHandle* handle)>& callback,
                          size_t start_index, size_t end_index);

  static inline HashVal ComputeHash(const Slice& key, uint32_t seed) {
    return Lower32of64(GetSliceNPHash64(key, seed));
  }

  // Separate from constructor so caller can easily make an array of LRUCache
  // if current usage is more than new capacity, the function will attempt to
  // free the needed space.
  void SetCapacity(size_t capacity);

  // Set the flag to reject insertion if cache if full.
  void SetStrictCapacityLimit(bool strict_capacity_limit);

  // Like Cache methods, but with an extra "hash" parameter.
  Status Insert(const Slice& key, uint32_t hash, Cache::ObjectPtr value,
                const Cache::CacheItemHelper* helper, size_t charge,
                LFUHandle** handle, Cache::Priority priority);

  LFUHandle* CreateStandalone(const Slice& key, uint32_t hash,
                              Cache::ObjectPtr obj,
                              const Cache::CacheItemHelper* helper,
                              size_t charge, bool allow_uncharged);

  LFUHandle* Lookup(const Slice& key, uint32_t hash,
                    const Cache::CacheItemHelper* helper,
                    Cache::CreateContext* create_context,
                    Cache::Priority priority, Statistics* stats);

  void Erase(const Slice& key, uint32_t hash);

  bool Release(LFUHandle* handle, bool useful, bool erase_if_last_ref);

  bool Ref(LFUHandle* handle);

  size_t GetUsage() const;

  size_t GetPinnedUsage() const;

  size_t GetTableAddressCount() const;

  size_t GetOccupancyCount() const { return elems_; }

  MemoryAllocator* GetAllocator() const { return memory_allocator_; }

  void ApplyToSomeEntries(
      const std::function<void(const Slice& key, Cache::ObjectPtr value,
                               size_t charge,
                               const Cache::CacheItemHelper* helper)>& callback,
      size_t average_entries_per_lock, size_t* state);

  void EraseUnRefEntries();

 private:

  class Hasher {
   public:
    size_t operator() (Slice const& key) const {     // the parameter type should be the same as the type of key of unordered_map
      int h = 5381;
      for (size_t i = 0; i < key.size(); i++) {
        h = h + (h << 5) + key[i];
      }

      h = h & 0x7fffffff;
      return h;
    }
  };

  // sort order
  // order by refs and rect time
  class CompareLfuhandle {
   public:
    bool operator()(const LFUHandle* lfuHandle1, const LFUHandle* lfuHandle2) const {

      if (lfuHandle1->refs == lfuHandle2->refs) {
        return lfuHandle1->rect < lfuHandle2->rect;
      }

      return lfuHandle1->refs < lfuHandle2->refs;
    }
  };

  void RefKey(LFUHandle* lfuHandle);

  LFUHandle* CreateLFUHandle(const Slice& key, Cache::ObjectPtr value, uint32_t hash, const Cache::CacheItemHelper* helper, size_t charge);

  bool Evict(size_t size);

  // store all element
  std::unordered_map<Slice, LFUHandle*, Hasher> all_elements_cache;

  // evict_order
  std::set<LFUHandle*, CompareLfuhandle> sorts_order_cache;

  size_t capacity_;

  // Memory size for entries residing in the cache.
  size_t usage_{};

  bool strict_capacity_limit_{};

  // Number of elements currently in the table.
  size_t elems_{};

  mutable DMutex mutex_;

  MemoryAllocator* memory_allocator_ = nullptr;

  friend class LFUCache;

  std::chrono::time_point<std::chrono::steady_clock> start_clock = std::chrono::steady_clock::now();

};


class LFUCache : public ShardedCache<LFUCacheShard> {
 public:
  explicit LFUCache(const LFUCacheOptions& opts);

  const char* Name() const override { return "LFUCache"; }

  ObjectPtr Value(Handle* handle) override;

  size_t GetCharge(Handle* handle) const override;

  const CacheItemHelper* GetCacheItemHelper(Handle* handle) const override;

};

}

}

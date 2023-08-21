//
// Created by mymai on 2023/5/26.
//
#include<iostream>
#include<vector>
#include<set>

#include "lfu_cache.h"


namespace ROCKSDB_NAMESPACE {
namespace lfu_cache {

void LFUCacheShard::SetCapacity(size_t capacity) {
  capacity_ = capacity;
}

void LFUCacheShard::SetStrictCapacityLimit(bool strict_capacity_limit) {
    strict_capacity_limit_ = strict_capacity_limit;
}

Status LFUCacheShard::Insert(const Slice& key, uint32_t hash, Cache::ObjectPtr value,
              const Cache::CacheItemHelper* helper, size_t charge,
              LFUHandle** handle, Cache::Priority priority){

  LFUHandle* lfuHandle = CreateLFUHandle(key, value, hash, helper, charge);

  if (strict_capacity_limit_ && usage_ + lfuHandle->total_charge > capacity_) {
    if (!Evict(lfuHandle->total_charge)) {
      lfuHandle->Free(memory_allocator_);
      return Status::NoSpace();
    }
  }

  if (all_elements_cache.find(key) == all_elements_cache.end()) {
    all_elements_cache.insert({key, lfuHandle});
    sorts_order_cache.insert(lfuHandle);
    usage_ = usage_ + lfuHandle->total_charge;
    elems_++;
    return Status::OK();
  } else {
    LFUHandle* origin = all_elements_cache[key];
    all_elements_cache[key] = lfuHandle;
    sorts_order_cache.erase(origin);
    sorts_order_cache.insert(lfuHandle);

    origin->Free(memory_allocator_);
    return Status::OkOverwritten();
  }

}

LFUHandle* LFUCacheShard::CreateStandalone(const Slice& key, uint32_t hash, Cache::ObjectPtr value,
                                           const Cache::CacheItemHelper* helper, size_t charge, bool allow_uncharged) {
  LFUHandle* lfuHandle = CreateLFUHandle(key, value, hash, helper, charge);

  if (strict_capacity_limit_ && usage_ + lfuHandle->total_charge >= capacity_) {
    if (!Evict(lfuHandle->total_charge)) {

      if (allow_uncharged) {
        lfuHandle->total_charge = 0;
      } else {
        lfuHandle->Free(memory_allocator_);
        return nullptr;
      }
    }

  } else {
    usage_ = usage_ + lfuHandle->total_charge;
  }

  return lfuHandle;
}

LFUHandle* LFUCacheShard::Lookup(const Slice& key, uint32_t hash,
                  const Cache::CacheItemHelper* helper,
                                 Cache::CreateContext* create_context,
                                 Cache::Priority priority, Statistics* stats) {

    if (all_elements_cache.count(key) > 0) {
      // std::cout << "查询到" << key.data_ << std::endl;
      const auto& it  = all_elements_cache.find(key);
      RefKey(it->second);
      return it->second;
    }

    return nullptr;
}

void LFUCacheShard::RefKey(LFUHandle* lfuHandle) {
    sorts_order_cache.erase(lfuHandle);
    lfuHandle->UnDateVisitTime(start_clock);
    lfuHandle->Ref();
    sorts_order_cache.insert(lfuHandle);
}


void LFUCacheShard::Erase(const Slice& key, uint32_t hash) {

    LFUHandle* handle = all_elements_cache[key];
    all_elements_cache.erase(key);
    sorts_order_cache.erase(handle);
    handle->Free(memory_allocator_);
}


bool LFUCacheShard::Ref(LFUHandle* handle) {
    handle->Ref();
    return true;
}

void LFUCacheShard::EraseUnRefEntries() {
    Evict(usage_);
}

void LFUCacheShard::ApplyToSomeEntries(const std::function<void(LFUHandle* handle)>& callback,
                        size_t start_index, size_t end_index) {
    size_t start_index_temp{};

    for (auto it = sorts_order_cache.begin(); it != sorts_order_cache.end(); it++) {
      if (start_index_temp >= start_index && start_index_temp < end_index) {
        LFUHandle* handle = *it;
        callback(handle);
      }

      start_index_temp++;
    }
}

void LFUCacheShard::ApplyToSomeEntries(
    const std::function<void(const Slice& key, Cache::ObjectPtr value,
                             size_t charge,
                             const Cache::CacheItemHelper* helper)>& callback,
    size_t average_entries_per_lock, size_t* state) {

    size_t length = elems_;
    size_t start_index = *state;
    size_t end_index = start_index + average_entries_per_lock;

    if (end_index >= length) {
      end_index = elems_;
      *state = SIZE_MAX;
    } else {
      *state = end_index;
    }

    size_t i{};
    auto it = sorts_order_cache.begin();

    std::function func = [callback,
     metadata_charge_policy = metadata_charge_policy_](LFUHandle* h) {
      callback(h->Key(), h->value_, h->GetCharge(metadata_charge_policy),
               h->helper_);
    };

    while (it != sorts_order_cache.end() &&  i < elems_) {
      if (i >= start_index && i < end_index) {
        LFUHandle* handle = *it;
        func(handle);
      }

      i++;
    }

}


bool LFUCacheShard::Release(LFUHandle* handle, bool useful, bool erase_if_last_ref) {
    // std::cout << "Release key: " << handle->Key().data_ << std::endl;
    sorts_order_cache.erase(handle);
    bool r = handle->UnRef();
    sorts_order_cache.insert(handle);

    if (erase_if_last_ref && r) {
      // Free handle
      Erase(handle->Key(), 0);
      return true;
    }

  return false;
}

LFUHandle* LFUCacheShard::CreateLFUHandle(const Slice& key, Cache::ObjectPtr value, uint32_t hash, const Cache::CacheItemHelper* helper, size_t charge) {
  // std::cout << "create lfuhandle: " << key.data_ << std::endl;
  LFUHandle* lfuHandle = static_cast<LFUHandle*>(malloc(sizeof(LFUHandle) - 1 + key.size()));
  lfuHandle->key_length_ = key.size();
  lfuHandle->value_ = value;
  lfuHandle->hash_ = hash;
  memcpy(lfuHandle->key_data, key.data_, key.size());
  lfuHandle->CalcTotalCharge(charge, metadata_charge_policy_);
  lfuHandle->refs = 0;
  lfuHandle->helper_ = helper;

  lfuHandle->UnDateVisitTime(start_clock);
  return lfuHandle;
}

bool LFUCacheShard::Evict(size_t size) {
  assert(size > 0 && usage_ > 0);
  size_t evict_memory{};
  while (evict_memory < size) {
      std::set<LFUHandle*>::iterator sort_cache_it = sorts_order_cache.begin();
      LFUHandle* f = *sort_cache_it;

      if (f->refs) {
        return false;
      }

      // std::cout << "evcit key: " << f->Key().data_ << std::endl;

      size_t erase_charge = f->total_charge;
      Erase(f->Key(), 0);
      usage_ = usage_ - erase_charge;
      evict_memory += erase_charge;
  }

  return evict_memory >= size;

}

size_t LFUCacheShard::GetPinnedUsage() const {
  return usage_;
}

size_t LFUCacheShard::GetTableAddressCount() const {
  return 0;
}

size_t LFUCacheShard::GetUsage() const {
  return usage_;
}


LFUCache::LFUCache(const LFUCacheOptions& opts):
            ShardedCache<LFUCacheShard>(opts){
  size_t per_shard = GetPerShardCapacity();
  bool hasStrictCapacity = HasStrictCapacityLimit();
  CacheMetadataChargePolicy cacheMetadataChargePolicy = opts.metadata_charge_policy;
  MemoryAllocator* alloc = memory_allocator();

  InitShards([&](LFUCacheShard* cs) {
    new (cs) LFUCacheShard(per_shard, hasStrictCapacity, cacheMetadataChargePolicy, alloc);
  });
}

LFUCacheShard::~LFUCacheShard() {
  auto allo = memory_allocator_;
  ApplyToSomeEntries([allo](LFUHandle* lfuHandle){
      lfuHandle->Free(allo);
  }, 0, elems_);

}

Cache::ObjectPtr LFUCache::Value(Handle* handle) {
  auto h = reinterpret_cast<const LFUHandle*>(handle);
  return h->value_;
}

size_t LFUCache::GetCharge(Handle* handle) const {
  auto h = reinterpret_cast<const LFUHandle*>(handle);
  return h->GetCharge(GetShard(0).metadata_charge_policy_);
}

const Cache::CacheItemHelper* LFUCache::GetCacheItemHelper(Handle* handle) const {
  auto h = reinterpret_cast<const LFUHandle*>(handle);
  return h->helper_;
}

}

std::shared_ptr<Cache> LFUCacheOptions::MakeSharedCache() const {
  // For sanitized options
  LFUCacheOptions opts = *this;
  std::shared_ptr<Cache> cache = std::make_shared<lfu_cache::LFUCache>(opts);
  return cache;

}

}


//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <gtest/gtest.h>

#include <functional>

#include "rocksdb/advanced_cache.h"

namespace ROCKSDB_NAMESPACE {

namespace secondary_cache_test_util {

class TestItem {
 public:
  TestItem(const char* buf, size_t size) : buf_(new char[size]), size_(size) {
    memcpy(buf_.get(), buf, size);
  }
  ~TestItem() = default;

  char* Buf() { return buf_.get(); }
  [[nodiscard]] size_t Size() const { return size_; }
  std::string ToString() { return std::string(Buf(), Size()); }

 private:
  std::unique_ptr<char[]> buf_;
  size_t size_;
};

struct TestCreateContext : public Cache::CreateContext {
  void SetFailCreate(bool fail) { fail_create_ = fail; }

  bool fail_create_ = false;
};

size_t SizeCallback(Cache::ObjectPtr obj);
Status SaveToCallback(Cache::ObjectPtr from_obj, size_t from_offset,
                      size_t length, char* out);
void DeletionCallback(Cache::ObjectPtr obj, MemoryAllocator* alloc);
Status SaveToCallbackFail(Cache::ObjectPtr obj, size_t offset, size_t size,
                          char* out);

Status CreateCallback(const Slice& data, Cache::CreateContext* context,
                      MemoryAllocator* allocator, Cache::ObjectPtr* out_obj,
                      size_t* out_charge);

const Cache::CacheItemHelper* GetHelper(
    CacheEntryRole r = CacheEntryRole::kDataBlock,
    bool secondary_compatible = true, bool fail = false);

const Cache::CacheItemHelper* GetHelperFail(
    CacheEntryRole r = CacheEntryRole::kDataBlock);

extern const std::string kLRU;
extern const std::string kHyperClock;

class WithCacheTestParam : public testing::WithParamInterface<std::string> {
 public:
  // For options other than capacity
  size_t estimated_value_size_ = 1;

  std::shared_ptr<Cache> NewCache(
      size_t capacity,
      std::function<void(ShardedCacheOptions&)> modify_opts_fn = {}) {
    auto type = GetParam();
    if (type == kLRU) {
      LRUCacheOptions lru_opts;
      lru_opts.capacity = capacity;
      if (modify_opts_fn) {
        modify_opts_fn(lru_opts);
      }
      return NewLRUCache(lru_opts);
    }
    if (type == kHyperClock) {
      HyperClockCacheOptions hc_opts{capacity, estimated_value_size_};
      if (modify_opts_fn) {
        modify_opts_fn(hc_opts);
      }
      return hc_opts.MakeSharedCache();
    }
    return nullptr;
  }

  std::shared_ptr<Cache> NewCache(
      size_t capacity, int num_shard_bits, bool strict_capacity_limit,
      CacheMetadataChargePolicy charge_policy = kDontChargeCacheMetadata) {
    return NewCache(capacity, [=](ShardedCacheOptions& opts) {
      opts.num_shard_bits = num_shard_bits;
      opts.strict_capacity_limit = strict_capacity_limit;
      opts.metadata_charge_policy = charge_policy;
    });
  }

  std::shared_ptr<Cache> NewCache(
      size_t capacity, int num_shard_bits, bool strict_capacity_limit,
      std::shared_ptr<SecondaryCache> secondary_cache) {
    return NewCache(capacity, [=](ShardedCacheOptions& opts) {
      opts.num_shard_bits = num_shard_bits;
      opts.strict_capacity_limit = strict_capacity_limit;
      opts.metadata_charge_policy = kDontChargeCacheMetadata;
      opts.secondary_cache = secondary_cache;
    });
  }
};

}  // namespace secondary_cache_test_util

}  // namespace ROCKSDB_NAMESPACE

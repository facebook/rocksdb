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

struct TestCreateContext : public Cache::CreateContext {
  void SetFailCreate(bool fail) { fail_create_ = fail; }

  bool fail_create_ = false;
};

class WithCacheType : public TestCreateContext {
 public:
  WithCacheType() {}
  virtual ~WithCacheType() {}

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

  static constexpr auto kLRU = "lru";
  static constexpr auto kHyperClock = "hyper_clock";

  // For options other than capacity
  size_t estimated_value_size_ = 1;

  virtual const std::string& Type() = 0;

  std::shared_ptr<Cache> NewCache(
      size_t capacity,
      std::function<void(ShardedCacheOptions&)> modify_opts_fn = {}) {
    const auto& type = Type();
    if (type == kLRU) {
      LRUCacheOptions lru_opts;
      lru_opts.capacity = capacity;
      lru_opts.hash_seed = 0;  // deterministic tests
      if (modify_opts_fn) {
        modify_opts_fn(lru_opts);
      }
      return lru_opts.MakeSharedCache();
    }
    if (type == kHyperClock) {
      HyperClockCacheOptions hc_opts{capacity, estimated_value_size_};
      hc_opts.hash_seed = 0;  // deterministic tests
      if (modify_opts_fn) {
        modify_opts_fn(hc_opts);
      }
      return hc_opts.MakeSharedCache();
    }
    assert(false);
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

  static const Cache::CacheItemHelper* GetHelper(
      CacheEntryRole r = CacheEntryRole::kDataBlock,
      bool secondary_compatible = true, bool fail = false);

  static const Cache::CacheItemHelper* GetHelperFail(
      CacheEntryRole r = CacheEntryRole::kDataBlock);
};

class WithCacheTypeParam : public WithCacheType,
                           public testing::WithParamInterface<std::string> {
  const std::string& Type() override { return GetParam(); }
};

constexpr auto kLRU = WithCacheType::kLRU;
constexpr auto kHyperClock = WithCacheType::kHyperClock;

inline auto GetTestingCacheTypes() {
  return testing::Values(std::string(kLRU), std::string(kHyperClock));
}

}  // namespace secondary_cache_test_util
}  // namespace ROCKSDB_NAMESPACE

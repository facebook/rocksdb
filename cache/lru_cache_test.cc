//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "cache/lru_cache.h"

#include <string>
#include <vector>

#include "cache/cache_key.h"
#include "cache/clock_cache.h"
#include "cache_helpers.h"
#include "db/db_test_util.h"
#include "file/sst_file_manager_impl.h"
#include "port/port.h"
#include "port/stack_trace.h"
#include "rocksdb/cache.h"
#include "rocksdb/io_status.h"
#include "rocksdb/sst_file_manager.h"
#include "rocksdb/utilities/cache_dump_load.h"
#include "test_util/testharness.h"
#include "typed_cache.h"
#include "util/coding.h"
#include "util/random.h"
#include "utilities/cache_dump_load_impl.h"
#include "utilities/fault_injection_fs.h"

namespace ROCKSDB_NAMESPACE {

class LRUCacheTest : public testing::Test {
 public:
  LRUCacheTest() {}
  ~LRUCacheTest() override { DeleteCache(); }

  void DeleteCache() {
    if (cache_ != nullptr) {
      cache_->~LRUCacheShard();
      port::cacheline_aligned_free(cache_);
      cache_ = nullptr;
    }
  }

  void NewCache(size_t capacity, double high_pri_pool_ratio = 0.0,
                double low_pri_pool_ratio = 1.0,
                bool use_adaptive_mutex = kDefaultToAdaptiveMutex) {
    DeleteCache();
    cache_ = reinterpret_cast<LRUCacheShard*>(
        port::cacheline_aligned_alloc(sizeof(LRUCacheShard)));
    new (cache_) LRUCacheShard(capacity, /*strict_capacity_limit=*/false,
                               high_pri_pool_ratio, low_pri_pool_ratio,
                               use_adaptive_mutex, kDontChargeCacheMetadata,
                               /*max_upper_hash_bits=*/24,
                               /*allocator*/ nullptr,
                               /*secondary_cache=*/nullptr);
  }

  void Insert(const std::string& key,
              Cache::Priority priority = Cache::Priority::LOW) {
    EXPECT_OK(cache_->Insert(key, 0 /*hash*/, nullptr /*value*/,
                             &kNoopCacheItemHelper, 1 /*charge*/,
                             nullptr /*handle*/, priority));
  }

  void Insert(char key, Cache::Priority priority = Cache::Priority::LOW) {
    Insert(std::string(1, key), priority);
  }

  bool Lookup(const std::string& key) {
    auto handle = cache_->Lookup(key, 0 /*hash*/, nullptr, nullptr,
                                 Cache::Priority::LOW, true, nullptr);
    if (handle) {
      cache_->Release(handle, true /*useful*/, false /*erase*/);
      return true;
    }
    return false;
  }

  bool Lookup(char key) { return Lookup(std::string(1, key)); }

  void Erase(const std::string& key) { cache_->Erase(key, 0 /*hash*/); }

  void ValidateLRUList(std::vector<std::string> keys,
                       size_t num_high_pri_pool_keys = 0,
                       size_t num_low_pri_pool_keys = 0,
                       size_t num_bottom_pri_pool_keys = 0) {
    LRUHandle* lru;
    LRUHandle* lru_low_pri;
    LRUHandle* lru_bottom_pri;
    cache_->TEST_GetLRUList(&lru, &lru_low_pri, &lru_bottom_pri);

    LRUHandle* iter = lru;

    bool in_low_pri_pool = false;
    bool in_high_pri_pool = false;

    size_t high_pri_pool_keys = 0;
    size_t low_pri_pool_keys = 0;
    size_t bottom_pri_pool_keys = 0;

    if (iter == lru_bottom_pri) {
      in_low_pri_pool = true;
      in_high_pri_pool = false;
    }
    if (iter == lru_low_pri) {
      in_low_pri_pool = false;
      in_high_pri_pool = true;
    }

    for (const auto& key : keys) {
      iter = iter->next;
      ASSERT_NE(lru, iter);
      ASSERT_EQ(key, iter->key().ToString());
      ASSERT_EQ(in_high_pri_pool, iter->InHighPriPool());
      ASSERT_EQ(in_low_pri_pool, iter->InLowPriPool());
      if (in_high_pri_pool) {
        ASSERT_FALSE(iter->InLowPriPool());
        high_pri_pool_keys++;
      } else if (in_low_pri_pool) {
        ASSERT_FALSE(iter->InHighPriPool());
        low_pri_pool_keys++;
      } else {
        bottom_pri_pool_keys++;
      }
      if (iter == lru_bottom_pri) {
        ASSERT_FALSE(in_low_pri_pool);
        ASSERT_FALSE(in_high_pri_pool);
        in_low_pri_pool = true;
        in_high_pri_pool = false;
      }
      if (iter == lru_low_pri) {
        ASSERT_TRUE(in_low_pri_pool);
        ASSERT_FALSE(in_high_pri_pool);
        in_low_pri_pool = false;
        in_high_pri_pool = true;
      }
    }
    ASSERT_EQ(lru, iter->next);
    ASSERT_FALSE(in_low_pri_pool);
    ASSERT_TRUE(in_high_pri_pool);
    ASSERT_EQ(num_high_pri_pool_keys, high_pri_pool_keys);
    ASSERT_EQ(num_low_pri_pool_keys, low_pri_pool_keys);
    ASSERT_EQ(num_bottom_pri_pool_keys, bottom_pri_pool_keys);
  }

 private:
  LRUCacheShard* cache_ = nullptr;
};

TEST_F(LRUCacheTest, BasicLRU) {
  NewCache(5);
  for (char ch = 'a'; ch <= 'e'; ch++) {
    Insert(ch);
  }
  ValidateLRUList({"a", "b", "c", "d", "e"}, 0, 5);
  for (char ch = 'x'; ch <= 'z'; ch++) {
    Insert(ch);
  }
  ValidateLRUList({"d", "e", "x", "y", "z"}, 0, 5);
  ASSERT_FALSE(Lookup("b"));
  ValidateLRUList({"d", "e", "x", "y", "z"}, 0, 5);
  ASSERT_TRUE(Lookup("e"));
  ValidateLRUList({"d", "x", "y", "z", "e"}, 0, 5);
  ASSERT_TRUE(Lookup("z"));
  ValidateLRUList({"d", "x", "y", "e", "z"}, 0, 5);
  Erase("x");
  ValidateLRUList({"d", "y", "e", "z"}, 0, 4);
  ASSERT_TRUE(Lookup("d"));
  ValidateLRUList({"y", "e", "z", "d"}, 0, 4);
  Insert("u");
  ValidateLRUList({"y", "e", "z", "d", "u"}, 0, 5);
  Insert("v");
  ValidateLRUList({"e", "z", "d", "u", "v"}, 0, 5);
}

TEST_F(LRUCacheTest, LowPriorityMidpointInsertion) {
  // Allocate 2 cache entries to high-pri pool and 3 to low-pri pool.
  NewCache(5, /* high_pri_pool_ratio */ 0.40, /* low_pri_pool_ratio */ 0.60);

  Insert("a", Cache::Priority::LOW);
  Insert("b", Cache::Priority::LOW);
  Insert("c", Cache::Priority::LOW);
  Insert("x", Cache::Priority::HIGH);
  Insert("y", Cache::Priority::HIGH);
  ValidateLRUList({"a", "b", "c", "x", "y"}, 2, 3);

  // Low-pri entries inserted to the tail of low-pri list (the midpoint).
  // After lookup, it will move to the tail of the full list.
  Insert("d", Cache::Priority::LOW);
  ValidateLRUList({"b", "c", "d", "x", "y"}, 2, 3);
  ASSERT_TRUE(Lookup("d"));
  ValidateLRUList({"b", "c", "x", "y", "d"}, 2, 3);

  // High-pri entries will be inserted to the tail of full list.
  Insert("z", Cache::Priority::HIGH);
  ValidateLRUList({"c", "x", "y", "d", "z"}, 2, 3);
}

TEST_F(LRUCacheTest, BottomPriorityMidpointInsertion) {
  // Allocate 2 cache entries to high-pri pool and 2 to low-pri pool.
  NewCache(6, /* high_pri_pool_ratio */ 0.35, /* low_pri_pool_ratio */ 0.35);

  Insert("a", Cache::Priority::BOTTOM);
  Insert("b", Cache::Priority::BOTTOM);
  Insert("i", Cache::Priority::LOW);
  Insert("j", Cache::Priority::LOW);
  Insert("x", Cache::Priority::HIGH);
  Insert("y", Cache::Priority::HIGH);
  ValidateLRUList({"a", "b", "i", "j", "x", "y"}, 2, 2, 2);

  // Low-pri entries will be inserted to the tail of low-pri list (the
  // midpoint). After lookup, 'k' will move to the tail of the full list, and
  // 'x' will spill over to the low-pri pool.
  Insert("k", Cache::Priority::LOW);
  ValidateLRUList({"b", "i", "j", "k", "x", "y"}, 2, 2, 2);
  ASSERT_TRUE(Lookup("k"));
  ValidateLRUList({"b", "i", "j", "x", "y", "k"}, 2, 2, 2);

  // High-pri entries will be inserted to the tail of full list. Although y was
  // inserted with high priority, it got spilled over to the low-pri pool. As
  // a result, j also got spilled over to the bottom-pri pool.
  Insert("z", Cache::Priority::HIGH);
  ValidateLRUList({"i", "j", "x", "y", "k", "z"}, 2, 2, 2);
  Erase("x");
  ValidateLRUList({"i", "j", "y", "k", "z"}, 2, 1, 2);
  Erase("y");
  ValidateLRUList({"i", "j", "k", "z"}, 2, 0, 2);

  // Bottom-pri entries will be inserted to the tail of bottom-pri list.
  Insert("c", Cache::Priority::BOTTOM);
  ValidateLRUList({"i", "j", "c", "k", "z"}, 2, 0, 3);
  Insert("d", Cache::Priority::BOTTOM);
  ValidateLRUList({"i", "j", "c", "d", "k", "z"}, 2, 0, 4);
  Insert("e", Cache::Priority::BOTTOM);
  ValidateLRUList({"j", "c", "d", "e", "k", "z"}, 2, 0, 4);

  // Low-pri entries will be inserted to the tail of low-pri list (the
  // midpoint).
  Insert("l", Cache::Priority::LOW);
  ValidateLRUList({"c", "d", "e", "l", "k", "z"}, 2, 1, 3);
  Insert("m", Cache::Priority::LOW);
  ValidateLRUList({"d", "e", "l", "m", "k", "z"}, 2, 2, 2);

  Erase("k");
  ValidateLRUList({"d", "e", "l", "m", "z"}, 1, 2, 2);
  Erase("z");
  ValidateLRUList({"d", "e", "l", "m"}, 0, 2, 2);

  // Bottom-pri entries will be inserted to the tail of bottom-pri list.
  Insert("f", Cache::Priority::BOTTOM);
  ValidateLRUList({"d", "e", "f", "l", "m"}, 0, 2, 3);
  Insert("g", Cache::Priority::BOTTOM);
  ValidateLRUList({"d", "e", "f", "g", "l", "m"}, 0, 2, 4);

  // High-pri entries will be inserted to the tail of full list.
  Insert("o", Cache::Priority::HIGH);
  ValidateLRUList({"e", "f", "g", "l", "m", "o"}, 1, 2, 3);
  Insert("p", Cache::Priority::HIGH);
  ValidateLRUList({"f", "g", "l", "m", "o", "p"}, 2, 2, 2);
}

TEST_F(LRUCacheTest, EntriesWithPriority) {
  // Allocate 2 cache entries to high-pri pool and 2 to low-pri pool.
  NewCache(6, /* high_pri_pool_ratio */ 0.35, /* low_pri_pool_ratio */ 0.35);

  Insert("a", Cache::Priority::LOW);
  Insert("b", Cache::Priority::LOW);
  ValidateLRUList({"a", "b"}, 0, 2, 0);
  // Low-pri entries can overflow to bottom-pri pool.
  Insert("c", Cache::Priority::LOW);
  ValidateLRUList({"a", "b", "c"}, 0, 2, 1);

  // Bottom-pri entries can take high-pri pool capacity if available
  Insert("t", Cache::Priority::LOW);
  Insert("u", Cache::Priority::LOW);
  ValidateLRUList({"a", "b", "c", "t", "u"}, 0, 2, 3);
  Insert("v", Cache::Priority::LOW);
  ValidateLRUList({"a", "b", "c", "t", "u", "v"}, 0, 2, 4);
  Insert("w", Cache::Priority::LOW);
  ValidateLRUList({"b", "c", "t", "u", "v", "w"}, 0, 2, 4);

  Insert("X", Cache::Priority::HIGH);
  Insert("Y", Cache::Priority::HIGH);
  ValidateLRUList({"t", "u", "v", "w", "X", "Y"}, 2, 2, 2);

  // After lookup, the high-pri entry 'X' got spilled over to the low-pri pool.
  // The low-pri entry 'v' got spilled over to the bottom-pri pool.
  Insert("Z", Cache::Priority::HIGH);
  ValidateLRUList({"u", "v", "w", "X", "Y", "Z"}, 2, 2, 2);

  // Low-pri entries will be inserted to head of low-pri pool.
  Insert("a", Cache::Priority::LOW);
  ValidateLRUList({"v", "w", "X", "a", "Y", "Z"}, 2, 2, 2);

  // After lookup, the high-pri entry 'Y' got spilled over to the low-pri pool.
  // The low-pri entry 'X' got spilled over to the bottom-pri pool.
  ASSERT_TRUE(Lookup("v"));
  ValidateLRUList({"w", "X", "a", "Y", "Z", "v"}, 2, 2, 2);

  // After lookup, the high-pri entry 'Z' got spilled over to the low-pri pool.
  // The low-pri entry 'a' got spilled over to the bottom-pri pool.
  ASSERT_TRUE(Lookup("X"));
  ValidateLRUList({"w", "a", "Y", "Z", "v", "X"}, 2, 2, 2);

  // After lookup, the low pri entry 'Z' got promoted back to high-pri pool. The
  // high-pri entry 'v' got spilled over to the low-pri pool.
  ASSERT_TRUE(Lookup("Z"));
  ValidateLRUList({"w", "a", "Y", "v", "X", "Z"}, 2, 2, 2);

  Erase("Y");
  ValidateLRUList({"w", "a", "v", "X", "Z"}, 2, 1, 2);
  Erase("X");
  ValidateLRUList({"w", "a", "v", "Z"}, 1, 1, 2);

  Insert("d", Cache::Priority::LOW);
  Insert("e", Cache::Priority::LOW);
  ValidateLRUList({"w", "a", "v", "d", "e", "Z"}, 1, 2, 3);

  Insert("f", Cache::Priority::LOW);
  Insert("g", Cache::Priority::LOW);
  ValidateLRUList({"v", "d", "e", "f", "g", "Z"}, 1, 2, 3);
  ASSERT_TRUE(Lookup("d"));
  ValidateLRUList({"v", "e", "f", "g", "Z", "d"}, 2, 2, 2);

  // Erase some entries.
  Erase("e");
  Erase("f");
  Erase("Z");
  ValidateLRUList({"v", "g", "d"}, 1, 1, 1);

  // Bottom-pri entries can take low- and high-pri pool capacity if available
  Insert("o", Cache::Priority::BOTTOM);
  ValidateLRUList({"v", "o", "g", "d"}, 1, 1, 2);
  Insert("p", Cache::Priority::BOTTOM);
  ValidateLRUList({"v", "o", "p", "g", "d"}, 1, 1, 3);
  Insert("q", Cache::Priority::BOTTOM);
  ValidateLRUList({"v", "o", "p", "q", "g", "d"}, 1, 1, 4);

  // High-pri entries can overflow to low-pri pool, and bottom-pri entries will
  // be evicted.
  Insert("x", Cache::Priority::HIGH);
  ValidateLRUList({"o", "p", "q", "g", "d", "x"}, 2, 1, 3);
  Insert("y", Cache::Priority::HIGH);
  ValidateLRUList({"p", "q", "g", "d", "x", "y"}, 2, 2, 2);
  Insert("z", Cache::Priority::HIGH);
  ValidateLRUList({"q", "g", "d", "x", "y", "z"}, 2, 2, 2);

  // 'g' is bottom-pri before this lookup, it will be inserted to head of
  // high-pri pool after lookup.
  ASSERT_TRUE(Lookup("g"));
  ValidateLRUList({"q", "d", "x", "y", "z", "g"}, 2, 2, 2);

  // High-pri entries will be inserted to head of high-pri pool after lookup.
  ASSERT_TRUE(Lookup("z"));
  ValidateLRUList({"q", "d", "x", "y", "g", "z"}, 2, 2, 2);

  // Bottom-pri entries will be inserted to head of high-pri pool after lookup.
  ASSERT_TRUE(Lookup("d"));
  ValidateLRUList({"q", "x", "y", "g", "z", "d"}, 2, 2, 2);

  // Bottom-pri entries will be inserted to the tail of bottom-pri list.
  Insert("m", Cache::Priority::BOTTOM);
  ValidateLRUList({"x", "m", "y", "g", "z", "d"}, 2, 2, 2);

  // Bottom-pri entries will be inserted to head of high-pri pool after lookup.
  ASSERT_TRUE(Lookup("m"));
  ValidateLRUList({"x", "y", "g", "z", "d", "m"}, 2, 2, 2);
}

namespace clock_cache {

class ClockCacheTest : public testing::Test {
 public:
  using Shard = HyperClockCache::Shard;
  using Table = HyperClockTable;
  using HandleImpl = Shard::HandleImpl;

  ClockCacheTest() {}
  ~ClockCacheTest() override { DeleteShard(); }

  void DeleteShard() {
    if (shard_ != nullptr) {
      shard_->~ClockCacheShard();
      port::cacheline_aligned_free(shard_);
      shard_ = nullptr;
    }
  }

  void NewShard(size_t capacity, bool strict_capacity_limit = true) {
    DeleteShard();
    shard_ =
        reinterpret_cast<Shard*>(port::cacheline_aligned_alloc(sizeof(Shard)));

    Table::Opts opts;
    opts.estimated_value_size = 1;
    new (shard_) Shard(capacity, strict_capacity_limit,
                       kDontChargeCacheMetadata, /*allocator*/ nullptr, opts);
  }

  Status Insert(const UniqueId64x2& hashed_key,
                Cache::Priority priority = Cache::Priority::LOW) {
    return shard_->Insert(TestKey(hashed_key), hashed_key, nullptr /*value*/,
                          &kNoopCacheItemHelper, 1 /*charge*/,
                          nullptr /*handle*/, priority);
  }

  Status Insert(char key, Cache::Priority priority = Cache::Priority::LOW) {
    return Insert(TestHashedKey(key), priority);
  }

  Status InsertWithLen(char key, size_t len) {
    std::string skey(len, key);
    return shard_->Insert(skey, TestHashedKey(key), nullptr /*value*/,
                          &kNoopCacheItemHelper, 1 /*charge*/,
                          nullptr /*handle*/, Cache::Priority::LOW);
  }

  bool Lookup(const Slice& key, const UniqueId64x2& hashed_key,
              bool useful = true) {
    auto handle = shard_->Lookup(key, hashed_key);
    if (handle) {
      shard_->Release(handle, useful, /*erase_if_last_ref=*/false);
      return true;
    }
    return false;
  }

  bool Lookup(const UniqueId64x2& hashed_key, bool useful = true) {
    return Lookup(TestKey(hashed_key), hashed_key, useful);
  }

  bool Lookup(char key, bool useful = true) {
    return Lookup(TestHashedKey(key), useful);
  }

  void Erase(char key) {
    UniqueId64x2 hashed_key = TestHashedKey(key);
    shard_->Erase(TestKey(hashed_key), hashed_key);
  }

  static inline Slice TestKey(const UniqueId64x2& hashed_key) {
    return Slice(reinterpret_cast<const char*>(&hashed_key), 16U);
  }

  static inline UniqueId64x2 TestHashedKey(char key) {
    // For testing hash near-collision behavior, put the variance in
    // hashed_key in bits that are unlikely to be used as hash bits.
    return {(static_cast<uint64_t>(key) << 56) + 1234U, 5678U};
  }

  Shard* shard_ = nullptr;
};

TEST_F(ClockCacheTest, Misc) {
  NewShard(3);

  // Key size stuff
  EXPECT_OK(InsertWithLen('a', 16));
  EXPECT_NOK(InsertWithLen('b', 15));
  EXPECT_OK(InsertWithLen('b', 16));
  EXPECT_NOK(InsertWithLen('c', 17));
  EXPECT_NOK(InsertWithLen('d', 1000));
  EXPECT_NOK(InsertWithLen('e', 11));
  EXPECT_NOK(InsertWithLen('f', 0));

  // Some of this is motivated by code coverage
  std::string wrong_size_key(15, 'x');
  EXPECT_FALSE(Lookup(wrong_size_key, TestHashedKey('x')));
  EXPECT_FALSE(shard_->Ref(nullptr));
  EXPECT_FALSE(shard_->Release(nullptr));
  shard_->Erase(wrong_size_key, TestHashedKey('x'));  // no-op
}

TEST_F(ClockCacheTest, Limits) {
  constexpr size_t kCapacity = 3;
  NewShard(kCapacity, false /*strict_capacity_limit*/);
  for (bool strict_capacity_limit : {false, true, false}) {
    SCOPED_TRACE("strict_capacity_limit = " +
                 std::to_string(strict_capacity_limit));

    // Also tests switching between strict limit and not
    shard_->SetStrictCapacityLimit(strict_capacity_limit);

    UniqueId64x2 hkey = TestHashedKey('x');

    // Single entry charge beyond capacity
    {
      Status s = shard_->Insert(TestKey(hkey), hkey, nullptr /*value*/,
                                &kNoopCacheItemHelper, 5 /*charge*/,
                                nullptr /*handle*/, Cache::Priority::LOW);
      if (strict_capacity_limit) {
        EXPECT_TRUE(s.IsMemoryLimit());
      } else {
        EXPECT_OK(s);
      }
    }

    // Single entry fills capacity
    {
      HandleImpl* h;
      ASSERT_OK(shard_->Insert(TestKey(hkey), hkey, nullptr /*value*/,
                               &kNoopCacheItemHelper, 3 /*charge*/, &h,
                               Cache::Priority::LOW));
      // Try to insert more
      Status s = Insert('a');
      if (strict_capacity_limit) {
        EXPECT_TRUE(s.IsMemoryLimit());
      } else {
        EXPECT_OK(s);
      }
      // Release entry filling capacity.
      // Cover useful = false case.
      shard_->Release(h, false /*useful*/, false /*erase_if_last_ref*/);
    }

    // Insert more than table size can handle to exceed occupancy limit.
    // (Cleverly using mostly zero-charge entries, but some non-zero to
    // verify usage tracking on detached entries.)
    {
      size_t n = shard_->GetTableAddressCount() + 1;
      std::unique_ptr<HandleImpl* []> ha { new HandleImpl* [n] {} };
      Status s;
      for (size_t i = 0; i < n && s.ok(); ++i) {
        hkey[1] = i;
        s = shard_->Insert(TestKey(hkey), hkey, nullptr /*value*/,
                           &kNoopCacheItemHelper,
                           (i + kCapacity < n) ? 0 : 1 /*charge*/, &ha[i],
                           Cache::Priority::LOW);
        if (i == 0) {
          EXPECT_OK(s);
        }
      }
      if (strict_capacity_limit) {
        EXPECT_TRUE(s.IsMemoryLimit());
      } else {
        EXPECT_OK(s);
      }
      // Same result if not keeping a reference
      s = Insert('a');
      if (strict_capacity_limit) {
        EXPECT_TRUE(s.IsMemoryLimit());
      } else {
        EXPECT_OK(s);
      }

      // Regardless, we didn't allow table to actually get full
      EXPECT_LT(shard_->GetOccupancyCount(), shard_->GetTableAddressCount());

      // Release handles
      for (size_t i = 0; i < n; ++i) {
        if (ha[i]) {
          shard_->Release(ha[i]);
        }
      }
    }
  }
}

TEST_F(ClockCacheTest, ClockEvictionTest) {
  for (bool strict_capacity_limit : {false, true}) {
    SCOPED_TRACE("strict_capacity_limit = " +
                 std::to_string(strict_capacity_limit));

    NewShard(6, strict_capacity_limit);
    EXPECT_OK(Insert('a', Cache::Priority::BOTTOM));
    EXPECT_OK(Insert('b', Cache::Priority::LOW));
    EXPECT_OK(Insert('c', Cache::Priority::HIGH));
    EXPECT_OK(Insert('d', Cache::Priority::BOTTOM));
    EXPECT_OK(Insert('e', Cache::Priority::LOW));
    EXPECT_OK(Insert('f', Cache::Priority::HIGH));

    EXPECT_TRUE(Lookup('a', /*use*/ false));
    EXPECT_TRUE(Lookup('b', /*use*/ false));
    EXPECT_TRUE(Lookup('c', /*use*/ false));
    EXPECT_TRUE(Lookup('d', /*use*/ false));
    EXPECT_TRUE(Lookup('e', /*use*/ false));
    EXPECT_TRUE(Lookup('f', /*use*/ false));

    // Ensure bottom are evicted first, even if new entries are low
    EXPECT_OK(Insert('g', Cache::Priority::LOW));
    EXPECT_OK(Insert('h', Cache::Priority::LOW));

    EXPECT_FALSE(Lookup('a', /*use*/ false));
    EXPECT_TRUE(Lookup('b', /*use*/ false));
    EXPECT_TRUE(Lookup('c', /*use*/ false));
    EXPECT_FALSE(Lookup('d', /*use*/ false));
    EXPECT_TRUE(Lookup('e', /*use*/ false));
    EXPECT_TRUE(Lookup('f', /*use*/ false));
    // Mark g & h useful
    EXPECT_TRUE(Lookup('g', /*use*/ true));
    EXPECT_TRUE(Lookup('h', /*use*/ true));

    // Then old LOW entries
    EXPECT_OK(Insert('i', Cache::Priority::LOW));
    EXPECT_OK(Insert('j', Cache::Priority::LOW));

    EXPECT_FALSE(Lookup('b', /*use*/ false));
    EXPECT_TRUE(Lookup('c', /*use*/ false));
    EXPECT_FALSE(Lookup('e', /*use*/ false));
    EXPECT_TRUE(Lookup('f', /*use*/ false));
    // Mark g & h useful once again
    EXPECT_TRUE(Lookup('g', /*use*/ true));
    EXPECT_TRUE(Lookup('h', /*use*/ true));
    EXPECT_TRUE(Lookup('i', /*use*/ false));
    EXPECT_TRUE(Lookup('j', /*use*/ false));

    // Then old HIGH entries
    EXPECT_OK(Insert('k', Cache::Priority::LOW));
    EXPECT_OK(Insert('l', Cache::Priority::LOW));

    EXPECT_FALSE(Lookup('c', /*use*/ false));
    EXPECT_FALSE(Lookup('f', /*use*/ false));
    EXPECT_TRUE(Lookup('g', /*use*/ false));
    EXPECT_TRUE(Lookup('h', /*use*/ false));
    EXPECT_TRUE(Lookup('i', /*use*/ false));
    EXPECT_TRUE(Lookup('j', /*use*/ false));
    EXPECT_TRUE(Lookup('k', /*use*/ false));
    EXPECT_TRUE(Lookup('l', /*use*/ false));

    // Then the (roughly) least recently useful
    EXPECT_OK(Insert('m', Cache::Priority::HIGH));
    EXPECT_OK(Insert('n', Cache::Priority::HIGH));

    EXPECT_TRUE(Lookup('g', /*use*/ false));
    EXPECT_TRUE(Lookup('h', /*use*/ false));
    EXPECT_FALSE(Lookup('i', /*use*/ false));
    EXPECT_FALSE(Lookup('j', /*use*/ false));
    EXPECT_TRUE(Lookup('k', /*use*/ false));
    EXPECT_TRUE(Lookup('l', /*use*/ false));

    // Now try changing capacity down
    shard_->SetCapacity(4);
    // Insert to ensure evictions happen
    EXPECT_OK(Insert('o', Cache::Priority::LOW));
    EXPECT_OK(Insert('p', Cache::Priority::LOW));

    EXPECT_FALSE(Lookup('g', /*use*/ false));
    EXPECT_FALSE(Lookup('h', /*use*/ false));
    EXPECT_FALSE(Lookup('k', /*use*/ false));
    EXPECT_FALSE(Lookup('l', /*use*/ false));
    EXPECT_TRUE(Lookup('m', /*use*/ false));
    EXPECT_TRUE(Lookup('n', /*use*/ false));
    EXPECT_TRUE(Lookup('o', /*use*/ false));
    EXPECT_TRUE(Lookup('p', /*use*/ false));

    // Now try changing capacity up
    EXPECT_TRUE(Lookup('m', /*use*/ true));
    EXPECT_TRUE(Lookup('n', /*use*/ true));
    shard_->SetCapacity(6);
    EXPECT_OK(Insert('q', Cache::Priority::HIGH));
    EXPECT_OK(Insert('r', Cache::Priority::HIGH));
    EXPECT_OK(Insert('s', Cache::Priority::HIGH));
    EXPECT_OK(Insert('t', Cache::Priority::HIGH));

    EXPECT_FALSE(Lookup('o', /*use*/ false));
    EXPECT_FALSE(Lookup('p', /*use*/ false));
    EXPECT_TRUE(Lookup('m', /*use*/ false));
    EXPECT_TRUE(Lookup('n', /*use*/ false));
    EXPECT_TRUE(Lookup('q', /*use*/ false));
    EXPECT_TRUE(Lookup('r', /*use*/ false));
    EXPECT_TRUE(Lookup('s', /*use*/ false));
    EXPECT_TRUE(Lookup('t', /*use*/ false));
  }
}

namespace {
struct DeleteCounter {
  int deleted = 0;
};
const Cache::CacheItemHelper kDeleteCounterHelper{
    CacheEntryRole::kMisc,
    [](Cache::ObjectPtr value, MemoryAllocator* /*alloc*/) {
      static_cast<DeleteCounter*>(value)->deleted += 1;
    }};
}  // namespace

// Testing calls to CorrectNearOverflow in Release
TEST_F(ClockCacheTest, ClockCounterOverflowTest) {
  NewShard(6, /*strict_capacity_limit*/ false);
  HandleImpl* h;
  DeleteCounter val;
  UniqueId64x2 hkey = TestHashedKey('x');
  ASSERT_OK(shard_->Insert(TestKey(hkey), hkey, &val, &kDeleteCounterHelper, 1,
                           &h, Cache::Priority::HIGH));

  // Some large number outstanding
  shard_->TEST_RefN(h, 123456789);
  // Simulate many lookup/ref + release, plenty to overflow counters
  for (int i = 0; i < 10000; ++i) {
    shard_->TEST_RefN(h, 1234567);
    shard_->TEST_ReleaseN(h, 1234567);
  }
  // Mark it invisible (to reach a different CorrectNearOverflow() in Release)
  shard_->Erase(TestKey(hkey), hkey);
  // Simulate many more lookup/ref + release (one-by-one would be too
  // expensive for unit test)
  for (int i = 0; i < 10000; ++i) {
    shard_->TEST_RefN(h, 1234567);
    shard_->TEST_ReleaseN(h, 1234567);
  }
  // Free all but last 1
  shard_->TEST_ReleaseN(h, 123456789);
  // Still alive
  ASSERT_EQ(val.deleted, 0);
  // Free last ref, which will finalize erasure
  shard_->Release(h);
  // Deleted
  ASSERT_EQ(val.deleted, 1);
}

// This test is mostly to exercise some corner case logic, by forcing two
// keys to have the same hash, and more
TEST_F(ClockCacheTest, CollidingInsertEraseTest) {
  NewShard(6, /*strict_capacity_limit*/ false);
  DeleteCounter val;
  UniqueId64x2 hkey1 = TestHashedKey('x');
  Slice key1 = TestKey(hkey1);
  UniqueId64x2 hkey2 = TestHashedKey('y');
  Slice key2 = TestKey(hkey2);
  UniqueId64x2 hkey3 = TestHashedKey('z');
  Slice key3 = TestKey(hkey3);
  HandleImpl* h1;
  ASSERT_OK(shard_->Insert(key1, hkey1, &val, &kDeleteCounterHelper, 1, &h1,
                           Cache::Priority::HIGH));
  HandleImpl* h2;
  ASSERT_OK(shard_->Insert(key2, hkey2, &val, &kDeleteCounterHelper, 1, &h2,
                           Cache::Priority::HIGH));
  HandleImpl* h3;
  ASSERT_OK(shard_->Insert(key3, hkey3, &val, &kDeleteCounterHelper, 1, &h3,
                           Cache::Priority::HIGH));

  // Can repeatedly lookup+release despite the hash collision
  HandleImpl* tmp_h;
  for (bool erase_if_last_ref : {true, false}) {  // but not last ref
    tmp_h = shard_->Lookup(key1, hkey1);
    ASSERT_EQ(h1, tmp_h);
    ASSERT_FALSE(shard_->Release(tmp_h, erase_if_last_ref));

    tmp_h = shard_->Lookup(key2, hkey2);
    ASSERT_EQ(h2, tmp_h);
    ASSERT_FALSE(shard_->Release(tmp_h, erase_if_last_ref));

    tmp_h = shard_->Lookup(key3, hkey3);
    ASSERT_EQ(h3, tmp_h);
    ASSERT_FALSE(shard_->Release(tmp_h, erase_if_last_ref));
  }

  // Make h1 invisible
  shard_->Erase(key1, hkey1);
  // Redundant erase
  shard_->Erase(key1, hkey1);

  // All still alive
  ASSERT_EQ(val.deleted, 0);

  // Invisible to Lookup
  tmp_h = shard_->Lookup(key1, hkey1);
  ASSERT_EQ(nullptr, tmp_h);

  // Can still find h2, h3
  for (bool erase_if_last_ref : {true, false}) {  // but not last ref
    tmp_h = shard_->Lookup(key2, hkey2);
    ASSERT_EQ(h2, tmp_h);
    ASSERT_FALSE(shard_->Release(tmp_h, erase_if_last_ref));

    tmp_h = shard_->Lookup(key3, hkey3);
    ASSERT_EQ(h3, tmp_h);
    ASSERT_FALSE(shard_->Release(tmp_h, erase_if_last_ref));
  }

  // Also Insert with invisible entry there
  ASSERT_OK(shard_->Insert(key1, hkey1, &val, &kDeleteCounterHelper, 1, nullptr,
                           Cache::Priority::HIGH));
  tmp_h = shard_->Lookup(key1, hkey1);
  // Found but distinct handle
  ASSERT_NE(nullptr, tmp_h);
  ASSERT_NE(h1, tmp_h);
  ASSERT_TRUE(shard_->Release(tmp_h, /*erase_if_last_ref*/ true));

  // tmp_h deleted
  ASSERT_EQ(val.deleted--, 1);

  // Release last ref on h1 (already invisible)
  ASSERT_TRUE(shard_->Release(h1, /*erase_if_last_ref*/ false));

  // h1 deleted
  ASSERT_EQ(val.deleted--, 1);
  h1 = nullptr;

  // Can still find h2, h3
  for (bool erase_if_last_ref : {true, false}) {  // but not last ref
    tmp_h = shard_->Lookup(key2, hkey2);
    ASSERT_EQ(h2, tmp_h);
    ASSERT_FALSE(shard_->Release(tmp_h, erase_if_last_ref));

    tmp_h = shard_->Lookup(key3, hkey3);
    ASSERT_EQ(h3, tmp_h);
    ASSERT_FALSE(shard_->Release(tmp_h, erase_if_last_ref));
  }

  // Release last ref on h2
  ASSERT_FALSE(shard_->Release(h2, /*erase_if_last_ref*/ false));

  // h2 still not deleted (unreferenced in cache)
  ASSERT_EQ(val.deleted, 0);

  // Can still find it
  tmp_h = shard_->Lookup(key2, hkey2);
  ASSERT_EQ(h2, tmp_h);

  // Release last ref on h2, with erase
  ASSERT_TRUE(shard_->Release(h2, /*erase_if_last_ref*/ true));

  // h2 deleted
  ASSERT_EQ(val.deleted--, 1);
  tmp_h = shard_->Lookup(key2, hkey2);
  ASSERT_EQ(nullptr, tmp_h);

  // Can still find h3
  for (bool erase_if_last_ref : {true, false}) {  // but not last ref
    tmp_h = shard_->Lookup(key3, hkey3);
    ASSERT_EQ(h3, tmp_h);
    ASSERT_FALSE(shard_->Release(tmp_h, erase_if_last_ref));
  }

  // Release last ref on h3, without erase
  ASSERT_FALSE(shard_->Release(h3, /*erase_if_last_ref*/ false));

  // h3 still not deleted (unreferenced in cache)
  ASSERT_EQ(val.deleted, 0);

  // Explicit erase
  shard_->Erase(key3, hkey3);

  // h3 deleted
  ASSERT_EQ(val.deleted--, 1);
  tmp_h = shard_->Lookup(key3, hkey3);
  ASSERT_EQ(nullptr, tmp_h);
}

// This uses the public API to effectively test CalcHashBits etc.
TEST_F(ClockCacheTest, TableSizesTest) {
  for (size_t est_val_size : {1U, 5U, 123U, 2345U, 345678U}) {
    SCOPED_TRACE("est_val_size = " + std::to_string(est_val_size));
    for (double est_count : {1.1, 2.2, 511.9, 512.1, 2345.0}) {
      SCOPED_TRACE("est_count = " + std::to_string(est_count));
      size_t capacity = static_cast<size_t>(est_val_size * est_count);
      // kDontChargeCacheMetadata
      auto cache = HyperClockCacheOptions(
                       capacity, est_val_size, /*num shard_bits*/ -1,
                       /*strict_capacity_limit*/ false,
                       /*memory_allocator*/ nullptr, kDontChargeCacheMetadata)
                       .MakeSharedCache();
      // Table sizes are currently only powers of two
      EXPECT_GE(cache->GetTableAddressCount(), est_count / kLoadFactor);
      EXPECT_LE(cache->GetTableAddressCount(), est_count / kLoadFactor * 2.0);
      EXPECT_EQ(cache->GetUsage(), 0);

      // kFullChargeMetaData
      // Because table sizes are currently only powers of two, sizes get
      // really weird when metadata is a huge portion of capacity. For example,
      // doubling the table size could cut by 90% the space available to
      // values. Therefore, we omit those weird cases for now.
      if (est_val_size >= 512) {
        cache = HyperClockCacheOptions(
                    capacity, est_val_size, /*num shard_bits*/ -1,
                    /*strict_capacity_limit*/ false,
                    /*memory_allocator*/ nullptr, kFullChargeCacheMetadata)
                    .MakeSharedCache();
        double est_count_after_meta =
            (capacity - cache->GetUsage()) * 1.0 / est_val_size;
        EXPECT_GE(cache->GetTableAddressCount(),
                  est_count_after_meta / kLoadFactor);
        EXPECT_LE(cache->GetTableAddressCount(),
                  est_count_after_meta / kLoadFactor * 2.0);
      }
    }
  }
}

}  // namespace clock_cache

class TestSecondaryCache : public SecondaryCache {
 public:
  // Specifies what action to take on a lookup for a particular key
  enum ResultType {
    SUCCESS,
    // Fail lookup immediately
    FAIL,
    // Defer the result. It will returned after Wait/WaitAll is called
    DEFER,
    // Defer the result and eventually return failure
    DEFER_AND_FAIL
  };

  using ResultMap = std::unordered_map<std::string, ResultType>;

  explicit TestSecondaryCache(size_t capacity)
      : cache_(NewLRUCache(capacity, 0, false, 0.5 /* high_pri_pool_ratio */,
                           nullptr, kDefaultToAdaptiveMutex,
                           kDontChargeCacheMetadata)),
        num_inserts_(0),
        num_lookups_(0),
        inject_failure_(false) {}

  const char* Name() const override { return "TestSecondaryCache"; }

  void InjectFailure() { inject_failure_ = true; }

  void ResetInjectFailure() { inject_failure_ = false; }

  Status Insert(const Slice& key, Cache::ObjectPtr value,
                const Cache::CacheItemHelper* helper) override {
    if (inject_failure_) {
      return Status::Corruption("Insertion Data Corrupted");
    }
    CheckCacheKeyCommonPrefix(key);
    size_t size;
    char* buf;
    Status s;

    num_inserts_++;
    size = (*helper->size_cb)(value);
    buf = new char[size + sizeof(uint64_t)];
    EncodeFixed64(buf, size);
    s = (*helper->saveto_cb)(value, 0, size, buf + sizeof(uint64_t));
    if (!s.ok()) {
      delete[] buf;
      return s;
    }
    return cache_.Insert(key, buf, size);
  }

  std::unique_ptr<SecondaryCacheResultHandle> Lookup(
      const Slice& key, const Cache::CacheItemHelper* helper,
      Cache::CreateContext* create_context, bool /*wait*/,
      bool /*advise_erase*/, bool& is_in_sec_cache) override {
    std::string key_str = key.ToString();
    TEST_SYNC_POINT_CALLBACK("TestSecondaryCache::Lookup", &key_str);

    std::unique_ptr<SecondaryCacheResultHandle> secondary_handle;
    is_in_sec_cache = false;
    ResultType type = ResultType::SUCCESS;
    auto iter = result_map_.find(key.ToString());
    if (iter != result_map_.end()) {
      type = iter->second;
    }
    if (type == ResultType::FAIL) {
      return secondary_handle;
    }

    TypedHandle* handle = cache_.Lookup(key);
    num_lookups_++;
    if (handle) {
      Cache::ObjectPtr value = nullptr;
      size_t charge = 0;
      Status s;
      if (type != ResultType::DEFER_AND_FAIL) {
        char* ptr = cache_.Value(handle);
        size_t size = DecodeFixed64(ptr);
        ptr += sizeof(uint64_t);
        s = helper->create_cb(Slice(ptr, size), create_context,
                              /*alloc*/ nullptr, &value, &charge);
      }
      if (s.ok()) {
        secondary_handle.reset(new TestSecondaryCacheResultHandle(
            cache_.get(), handle, value, charge, type));
        is_in_sec_cache = true;
      } else {
        cache_.Release(handle);
      }
    }
    return secondary_handle;
  }

  bool SupportForceErase() const override { return false; }

  void Erase(const Slice& /*key*/) override {}

  void WaitAll(std::vector<SecondaryCacheResultHandle*> handles) override {
    for (SecondaryCacheResultHandle* handle : handles) {
      TestSecondaryCacheResultHandle* sec_handle =
          static_cast<TestSecondaryCacheResultHandle*>(handle);
      sec_handle->SetReady();
    }
  }

  std::string GetPrintableOptions() const override { return ""; }

  void SetResultMap(ResultMap&& map) { result_map_ = std::move(map); }

  uint32_t num_inserts() { return num_inserts_; }

  uint32_t num_lookups() { return num_lookups_; }

  void CheckCacheKeyCommonPrefix(const Slice& key) {
    Slice current_prefix(key.data(), OffsetableCacheKey::kCommonPrefixSize);
    if (ckey_prefix_.empty()) {
      ckey_prefix_ = current_prefix.ToString();
    } else {
      EXPECT_EQ(ckey_prefix_, current_prefix.ToString());
    }
  }

 private:
  class TestSecondaryCacheResultHandle : public SecondaryCacheResultHandle {
   public:
    TestSecondaryCacheResultHandle(Cache* cache, Cache::Handle* handle,
                                   Cache::ObjectPtr value, size_t size,
                                   ResultType type)
        : cache_(cache),
          handle_(handle),
          value_(value),
          size_(size),
          is_ready_(true) {
      if (type != ResultType::SUCCESS) {
        is_ready_ = false;
      }
    }

    ~TestSecondaryCacheResultHandle() override { cache_->Release(handle_); }

    bool IsReady() override { return is_ready_; }

    void Wait() override {}

    Cache::ObjectPtr Value() override {
      assert(is_ready_);
      return value_;
    }

    size_t Size() override { return Value() ? size_ : 0; }

    void SetReady() { is_ready_ = true; }

   private:
    Cache* cache_;
    Cache::Handle* handle_;
    Cache::ObjectPtr value_;
    size_t size_;
    bool is_ready_;
  };

  using SharedCache =
      BasicTypedSharedCacheInterface<char[], CacheEntryRole::kMisc>;
  using TypedHandle = SharedCache::TypedHandle;
  SharedCache cache_;
  uint32_t num_inserts_;
  uint32_t num_lookups_;
  bool inject_failure_;
  std::string ckey_prefix_;
  ResultMap result_map_;
};

class DBSecondaryCacheTest : public DBTestBase {
 public:
  DBSecondaryCacheTest()
      : DBTestBase("db_secondary_cache_test", /*env_do_fsync=*/true) {
    fault_fs_.reset(new FaultInjectionTestFS(env_->GetFileSystem()));
    fault_env_.reset(new CompositeEnvWrapper(env_, fault_fs_));
  }

  std::shared_ptr<FaultInjectionTestFS> fault_fs_;
  std::unique_ptr<Env> fault_env_;
};

class LRUCacheSecondaryCacheTest : public LRUCacheTest,
                                   public Cache::CreateContext {
 public:
  LRUCacheSecondaryCacheTest() : fail_create_(false) {}
  ~LRUCacheSecondaryCacheTest() {}

 protected:
  class TestItem {
   public:
    TestItem(const char* buf, size_t size) : buf_(new char[size]), size_(size) {
      memcpy(buf_.get(), buf, size);
    }
    ~TestItem() {}

    char* Buf() { return buf_.get(); }
    size_t Size() { return size_; }
    std::string ToString() { return std::string(Buf(), Size()); }

   private:
    std::unique_ptr<char[]> buf_;
    size_t size_;
  };

  static size_t SizeCallback(Cache::ObjectPtr obj) {
    return static_cast<TestItem*>(obj)->Size();
  }

  static Status SaveToCallback(Cache::ObjectPtr from_obj, size_t from_offset,
                               size_t length, char* out) {
    TestItem* item = static_cast<TestItem*>(from_obj);
    char* buf = item->Buf();
    EXPECT_EQ(length, item->Size());
    EXPECT_EQ(from_offset, 0);
    memcpy(out, buf, length);
    return Status::OK();
  }

  static void DeletionCallback(Cache::ObjectPtr obj,
                               MemoryAllocator* /*alloc*/) {
    delete static_cast<TestItem*>(obj);
  }

  static Cache::CacheItemHelper helper_;

  static Status SaveToCallbackFail(Cache::ObjectPtr /*from_obj*/,
                                   size_t /*from_offset*/, size_t /*length*/,
                                   char* /*out*/) {
    return Status::NotSupported();
  }

  static Cache::CacheItemHelper helper_fail_;

  static Status CreateCallback(const Slice& data, Cache::CreateContext* context,
                               MemoryAllocator* /*allocator*/,
                               Cache::ObjectPtr* out_obj, size_t* out_charge) {
    auto t = static_cast<LRUCacheSecondaryCacheTest*>(context);
    if (t->fail_create_) {
      return Status::NotSupported();
    }
    *out_obj = new TestItem(data.data(), data.size());
    *out_charge = data.size();
    return Status::OK();
  };

  void SetFailCreate(bool fail) { fail_create_ = fail; }

 private:
  bool fail_create_;
};

Cache::CacheItemHelper LRUCacheSecondaryCacheTest::helper_{
    CacheEntryRole::kMisc, LRUCacheSecondaryCacheTest::DeletionCallback,
    LRUCacheSecondaryCacheTest::SizeCallback,
    LRUCacheSecondaryCacheTest::SaveToCallback,
    LRUCacheSecondaryCacheTest::CreateCallback};

Cache::CacheItemHelper LRUCacheSecondaryCacheTest::helper_fail_{
    CacheEntryRole::kMisc, LRUCacheSecondaryCacheTest::DeletionCallback,
    LRUCacheSecondaryCacheTest::SizeCallback,
    LRUCacheSecondaryCacheTest::SaveToCallbackFail,
    LRUCacheSecondaryCacheTest::CreateCallback};

TEST_F(LRUCacheSecondaryCacheTest, BasicTest) {
  LRUCacheOptions opts(1024 /* capacity */, 0 /* num_shard_bits */,
                       false /* strict_capacity_limit */,
                       0.5 /* high_pri_pool_ratio */,
                       nullptr /* memory_allocator */, kDefaultToAdaptiveMutex,
                       kDontChargeCacheMetadata);
  std::shared_ptr<TestSecondaryCache> secondary_cache =
      std::make_shared<TestSecondaryCache>(4096);
  opts.secondary_cache = secondary_cache;
  std::shared_ptr<Cache> cache = NewLRUCache(opts);
  std::shared_ptr<Statistics> stats = CreateDBStatistics();
  CacheKey k1 = CacheKey::CreateUniqueForCacheLifetime(cache.get());
  CacheKey k2 = CacheKey::CreateUniqueForCacheLifetime(cache.get());
  CacheKey k3 = CacheKey::CreateUniqueForCacheLifetime(cache.get());

  Random rnd(301);
  // Start with warming k3
  std::string str3 = rnd.RandomString(1021);
  ASSERT_OK(secondary_cache->InsertSaved(k3.AsSlice(), str3));

  std::string str1 = rnd.RandomString(1020);
  TestItem* item1 = new TestItem(str1.data(), str1.length());
  ASSERT_OK(cache->Insert(k1.AsSlice(), item1,
                          &LRUCacheSecondaryCacheTest::helper_, str1.length()));
  std::string str2 = rnd.RandomString(1021);
  TestItem* item2 = new TestItem(str2.data(), str2.length());
  // k1 should be demoted to NVM
  ASSERT_OK(cache->Insert(k2.AsSlice(), item2,
                          &LRUCacheSecondaryCacheTest::helper_, str2.length()));

  get_perf_context()->Reset();
  Cache::Handle* handle;
  handle =
      cache->Lookup(k2.AsSlice(), &LRUCacheSecondaryCacheTest::helper_,
                    /*context*/ this, Cache::Priority::LOW, true, stats.get());
  ASSERT_NE(handle, nullptr);
  ASSERT_EQ(static_cast<TestItem*>(cache->Value(handle))->Size(), str2.size());
  cache->Release(handle);

  // This lookup should promote k1 and demote k2
  handle =
      cache->Lookup(k1.AsSlice(), &LRUCacheSecondaryCacheTest::helper_,
                    /*context*/ this, Cache::Priority::LOW, true, stats.get());
  ASSERT_NE(handle, nullptr);
  ASSERT_EQ(static_cast<TestItem*>(cache->Value(handle))->Size(), str1.size());
  cache->Release(handle);

  // This lookup should promote k3 and demote k1
  handle =
      cache->Lookup(k3.AsSlice(), &LRUCacheSecondaryCacheTest::helper_,
                    /*context*/ this, Cache::Priority::LOW, true, stats.get());
  ASSERT_NE(handle, nullptr);
  ASSERT_EQ(static_cast<TestItem*>(cache->Value(handle))->Size(), str3.size());
  cache->Release(handle);

  ASSERT_EQ(secondary_cache->num_inserts(), 3u);
  ASSERT_EQ(secondary_cache->num_lookups(), 2u);
  ASSERT_EQ(stats->getTickerCount(SECONDARY_CACHE_HITS),
            secondary_cache->num_lookups());
  PerfContext perf_ctx = *get_perf_context();
  ASSERT_EQ(perf_ctx.secondary_cache_hit_count, secondary_cache->num_lookups());

  cache.reset();
  secondary_cache.reset();
}

TEST_F(LRUCacheSecondaryCacheTest, BasicFailTest) {
  LRUCacheOptions opts(1024 /* capacity */, 0 /* num_shard_bits */,
                       false /* strict_capacity_limit */,
                       0.5 /* high_pri_pool_ratio */,
                       nullptr /* memory_allocator */, kDefaultToAdaptiveMutex,
                       kDontChargeCacheMetadata);
  std::shared_ptr<TestSecondaryCache> secondary_cache =
      std::make_shared<TestSecondaryCache>(2048);
  opts.secondary_cache = secondary_cache;
  std::shared_ptr<Cache> cache = NewLRUCache(opts);
  CacheKey k1 = CacheKey::CreateUniqueForCacheLifetime(cache.get());
  CacheKey k2 = CacheKey::CreateUniqueForCacheLifetime(cache.get());

  Random rnd(301);
  std::string str1 = rnd.RandomString(1020);
  auto item1 = std::make_unique<TestItem>(str1.data(), str1.length());
  // NOTE: changed to assert helper != nullptr for efficiency / code size
  // ASSERT_TRUE(cache->Insert(k1.AsSlice(), item1.get(), nullptr,
  //                           str1.length()).IsInvalidArgument());
  ASSERT_OK(cache->Insert(k1.AsSlice(), item1.get(),
                          &LRUCacheSecondaryCacheTest::helper_, str1.length()));
  item1.release();  // Appease clang-analyze "potential memory leak"

  Cache::Handle* handle;
  handle = cache->Lookup(k2.AsSlice(), nullptr, /*context*/ this,
                         Cache::Priority::LOW, true);
  ASSERT_EQ(handle, nullptr);
  handle = cache->Lookup(k2.AsSlice(), &LRUCacheSecondaryCacheTest::helper_,
                         /*context*/ this, Cache::Priority::LOW, false);
  ASSERT_EQ(handle, nullptr);

  cache.reset();
  secondary_cache.reset();
}

TEST_F(LRUCacheSecondaryCacheTest, SaveFailTest) {
  LRUCacheOptions opts(1024 /* capacity */, 0 /* num_shard_bits */,
                       false /* strict_capacity_limit */,
                       0.5 /* high_pri_pool_ratio */,
                       nullptr /* memory_allocator */, kDefaultToAdaptiveMutex,
                       kDontChargeCacheMetadata);
  std::shared_ptr<TestSecondaryCache> secondary_cache =
      std::make_shared<TestSecondaryCache>(2048);
  opts.secondary_cache = secondary_cache;
  std::shared_ptr<Cache> cache = NewLRUCache(opts);
  CacheKey k1 = CacheKey::CreateUniqueForCacheLifetime(cache.get());
  CacheKey k2 = CacheKey::CreateUniqueForCacheLifetime(cache.get());

  Random rnd(301);
  std::string str1 = rnd.RandomString(1020);
  TestItem* item1 = new TestItem(str1.data(), str1.length());
  ASSERT_OK(cache->Insert(k1.AsSlice(), item1,
                          &LRUCacheSecondaryCacheTest::helper_fail_,
                          str1.length()));
  std::string str2 = rnd.RandomString(1020);
  TestItem* item2 = new TestItem(str2.data(), str2.length());
  // k1 should be demoted to NVM
  ASSERT_EQ(secondary_cache->num_inserts(), 0u);
  ASSERT_OK(cache->Insert(k2.AsSlice(), item2,
                          &LRUCacheSecondaryCacheTest::helper_fail_,
                          str2.length()));
  ASSERT_EQ(secondary_cache->num_inserts(), 1u);

  Cache::Handle* handle;
  handle =
      cache->Lookup(k2.AsSlice(), &LRUCacheSecondaryCacheTest::helper_fail_,
                    /*context*/ this, Cache::Priority::LOW, true);
  ASSERT_NE(handle, nullptr);
  cache->Release(handle);
  // This lookup should fail, since k1 demotion would have failed
  handle =
      cache->Lookup(k1.AsSlice(), &LRUCacheSecondaryCacheTest::helper_fail_,
                    /*context*/ this, Cache::Priority::LOW, true);
  ASSERT_EQ(handle, nullptr);
  // Since k1 didn't get promoted, k2 should still be in cache
  handle =
      cache->Lookup(k2.AsSlice(), &LRUCacheSecondaryCacheTest::helper_fail_,
                    /*context*/ this, Cache::Priority::LOW, true);
  ASSERT_NE(handle, nullptr);
  cache->Release(handle);
  ASSERT_EQ(secondary_cache->num_inserts(), 1u);
  ASSERT_EQ(secondary_cache->num_lookups(), 1u);

  cache.reset();
  secondary_cache.reset();
}

TEST_F(LRUCacheSecondaryCacheTest, CreateFailTest) {
  LRUCacheOptions opts(1024 /* capacity */, 0 /* num_shard_bits */,
                       false /* strict_capacity_limit */,
                       0.5 /* high_pri_pool_ratio */,
                       nullptr /* memory_allocator */, kDefaultToAdaptiveMutex,
                       kDontChargeCacheMetadata);
  std::shared_ptr<TestSecondaryCache> secondary_cache =
      std::make_shared<TestSecondaryCache>(2048);
  opts.secondary_cache = secondary_cache;
  std::shared_ptr<Cache> cache = NewLRUCache(opts);
  CacheKey k1 = CacheKey::CreateUniqueForCacheLifetime(cache.get());
  CacheKey k2 = CacheKey::CreateUniqueForCacheLifetime(cache.get());

  Random rnd(301);
  std::string str1 = rnd.RandomString(1020);
  TestItem* item1 = new TestItem(str1.data(), str1.length());
  ASSERT_OK(cache->Insert(k1.AsSlice(), item1,
                          &LRUCacheSecondaryCacheTest::helper_, str1.length()));
  std::string str2 = rnd.RandomString(1020);
  TestItem* item2 = new TestItem(str2.data(), str2.length());
  // k1 should be demoted to NVM
  ASSERT_OK(cache->Insert(k2.AsSlice(), item2,
                          &LRUCacheSecondaryCacheTest::helper_, str2.length()));

  Cache::Handle* handle;
  SetFailCreate(true);
  handle = cache->Lookup(k2.AsSlice(), &LRUCacheSecondaryCacheTest::helper_,
                         /*context*/ this, Cache::Priority::LOW, true);
  ASSERT_NE(handle, nullptr);
  cache->Release(handle);
  // This lookup should fail, since k1 creation would have failed
  handle = cache->Lookup(k1.AsSlice(), &LRUCacheSecondaryCacheTest::helper_,
                         /*context*/ this, Cache::Priority::LOW, true);
  ASSERT_EQ(handle, nullptr);
  // Since k1 didn't get promoted, k2 should still be in cache
  handle = cache->Lookup(k2.AsSlice(), &LRUCacheSecondaryCacheTest::helper_,
                         /*context*/ this, Cache::Priority::LOW, true);
  ASSERT_NE(handle, nullptr);
  cache->Release(handle);
  ASSERT_EQ(secondary_cache->num_inserts(), 1u);
  ASSERT_EQ(secondary_cache->num_lookups(), 1u);

  cache.reset();
  secondary_cache.reset();
}

TEST_F(LRUCacheSecondaryCacheTest, FullCapacityTest) {
  LRUCacheOptions opts(1024 /* capacity */, 0 /* num_shard_bits */,
                       true /* strict_capacity_limit */,
                       0.5 /* high_pri_pool_ratio */,
                       nullptr /* memory_allocator */, kDefaultToAdaptiveMutex,
                       kDontChargeCacheMetadata);
  std::shared_ptr<TestSecondaryCache> secondary_cache =
      std::make_shared<TestSecondaryCache>(2048);
  opts.secondary_cache = secondary_cache;
  std::shared_ptr<Cache> cache = NewLRUCache(opts);
  CacheKey k1 = CacheKey::CreateUniqueForCacheLifetime(cache.get());
  CacheKey k2 = CacheKey::CreateUniqueForCacheLifetime(cache.get());

  Random rnd(301);
  std::string str1 = rnd.RandomString(1020);
  TestItem* item1 = new TestItem(str1.data(), str1.length());
  ASSERT_OK(cache->Insert(k1.AsSlice(), item1,
                          &LRUCacheSecondaryCacheTest::helper_, str1.length()));
  std::string str2 = rnd.RandomString(1020);
  TestItem* item2 = new TestItem(str2.data(), str2.length());
  // k1 should be demoted to NVM
  ASSERT_OK(cache->Insert(k2.AsSlice(), item2,
                          &LRUCacheSecondaryCacheTest::helper_, str2.length()));

  Cache::Handle* handle;
  handle = cache->Lookup(k2.AsSlice(), &LRUCacheSecondaryCacheTest::helper_,
                         /*context*/ this, Cache::Priority::LOW, true);
  ASSERT_NE(handle, nullptr);
  // k1 promotion should fail due to the block cache being at capacity,
  // but the lookup should still succeed
  Cache::Handle* handle2;
  handle2 = cache->Lookup(k1.AsSlice(), &LRUCacheSecondaryCacheTest::helper_,
                          /*context*/ this, Cache::Priority::LOW, true);
  ASSERT_NE(handle2, nullptr);
  // Since k1 didn't get inserted, k2 should still be in cache
  cache->Release(handle);
  cache->Release(handle2);
  handle = cache->Lookup(k2.AsSlice(), &LRUCacheSecondaryCacheTest::helper_,
                         /*context*/ this, Cache::Priority::LOW, true);
  ASSERT_NE(handle, nullptr);
  cache->Release(handle);
  ASSERT_EQ(secondary_cache->num_inserts(), 1u);
  ASSERT_EQ(secondary_cache->num_lookups(), 1u);

  cache.reset();
  secondary_cache.reset();
}

// In this test, the block cache size is set to 4096, after insert 6 KV-pairs
// and flush, there are 5 blocks in this SST file, 2 data blocks and 3 meta
// blocks. block_1 size is 4096 and block_2 size is 2056. The total size
// of the meta blocks are about 900 to 1000. Therefore, in any situation,
// if we try to insert block_1 to the block cache, it will always fails. Only
// block_2 will be successfully inserted into the block cache.
TEST_F(DBSecondaryCacheTest, TestSecondaryCacheCorrectness1) {
  LRUCacheOptions opts(4 * 1024 /* capacity */, 0 /* num_shard_bits */,
                       false /* strict_capacity_limit */,
                       0.5 /* high_pri_pool_ratio */,
                       nullptr /* memory_allocator */, kDefaultToAdaptiveMutex,
                       kDontChargeCacheMetadata);
  std::shared_ptr<TestSecondaryCache> secondary_cache(
      new TestSecondaryCache(2048 * 1024));
  opts.secondary_cache = secondary_cache;
  std::shared_ptr<Cache> cache = NewLRUCache(opts);
  BlockBasedTableOptions table_options;
  table_options.block_cache = cache;
  table_options.block_size = 4 * 1024;
  Options options = GetDefaultOptions();
  options.create_if_missing = true;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  options.env = fault_env_.get();
  fault_fs_->SetFailGetUniqueId(true);

  // Set the file paranoid check, so after flush, the file will be read
  // all the blocks will be accessed.
  options.paranoid_file_checks = true;
  DestroyAndReopen(options);
  Random rnd(301);
  const int N = 6;
  for (int i = 0; i < N; i++) {
    std::string p_v = rnd.RandomString(1007);
    ASSERT_OK(Put(Key(i), p_v));
  }

  ASSERT_OK(Flush());
  // After Flush is successful, RocksDB will do the paranoid check for the new
  // SST file. Meta blocks are always cached in the block cache and they
  // will not be evicted. When block_2 is cache miss and read out, it is
  // inserted to the block cache. Note that, block_1 is never successfully
  // inserted to the block cache. Here are 2 lookups in the secondary cache
  // for block_1 and block_2
  ASSERT_EQ(secondary_cache->num_inserts(), 0u);
  ASSERT_EQ(secondary_cache->num_lookups(), 2u);

  Compact("a", "z");
  // Compaction will create the iterator to scan the whole file. So all the
  // blocks are needed. Meta blocks are always cached. When block_1 is read
  // out, block_2 is evicted from block cache and inserted to secondary
  // cache.
  ASSERT_EQ(secondary_cache->num_inserts(), 1u);
  ASSERT_EQ(secondary_cache->num_lookups(), 3u);

  std::string v = Get(Key(0));
  ASSERT_EQ(1007, v.size());
  // The first data block is not in the cache, similarly, trigger the block
  // cache Lookup and secondary cache lookup for block_1. But block_1 will not
  // be inserted successfully due to the size. Currently, cache only has
  // the meta blocks.
  ASSERT_EQ(secondary_cache->num_inserts(), 1u);
  ASSERT_EQ(secondary_cache->num_lookups(), 4u);

  v = Get(Key(5));
  ASSERT_EQ(1007, v.size());
  // The second data block is not in the cache, similarly, trigger the block
  // cache Lookup and secondary cache lookup for block_2 and block_2 is found
  // in the secondary cache. Now block cache has block_2
  ASSERT_EQ(secondary_cache->num_inserts(), 1u);
  ASSERT_EQ(secondary_cache->num_lookups(), 5u);

  v = Get(Key(5));
  ASSERT_EQ(1007, v.size());
  // block_2 is in the block cache. There is a block cache hit. No need to
  // lookup or insert the secondary cache.
  ASSERT_EQ(secondary_cache->num_inserts(), 1u);
  ASSERT_EQ(secondary_cache->num_lookups(), 5u);

  v = Get(Key(0));
  ASSERT_EQ(1007, v.size());
  // Lookup the first data block, not in the block cache, so lookup the
  // secondary cache. Also not in the secondary cache. After Get, still
  // block_1 is will not be cached.
  ASSERT_EQ(secondary_cache->num_inserts(), 1u);
  ASSERT_EQ(secondary_cache->num_lookups(), 6u);

  v = Get(Key(0));
  ASSERT_EQ(1007, v.size());
  // Lookup the first data block, not in the block cache, so lookup the
  // secondary cache. Also not in the secondary cache. After Get, still
  // block_1 is will not be cached.
  ASSERT_EQ(secondary_cache->num_inserts(), 1u);
  ASSERT_EQ(secondary_cache->num_lookups(), 7u);

  Destroy(options);
}

// In this test, the block cache size is set to 6100, after insert 6 KV-pairs
// and flush, there are 5 blocks in this SST file, 2 data blocks and 3 meta
// blocks. block_1 size is 4096 and block_2 size is 2056. The total size
// of the meta blocks are about 900 to 1000. Therefore, we can successfully
// insert and cache block_1 in the block cache (this is the different place
// from TestSecondaryCacheCorrectness1)
TEST_F(DBSecondaryCacheTest, TestSecondaryCacheCorrectness2) {
  LRUCacheOptions opts(6100 /* capacity */, 0 /* num_shard_bits */,
                       false /* strict_capacity_limit */,
                       0.5 /* high_pri_pool_ratio */,
                       nullptr /* memory_allocator */, kDefaultToAdaptiveMutex,
                       kDontChargeCacheMetadata);
  std::shared_ptr<TestSecondaryCache> secondary_cache(
      new TestSecondaryCache(2048 * 1024));
  opts.secondary_cache = secondary_cache;
  std::shared_ptr<Cache> cache = NewLRUCache(opts);
  BlockBasedTableOptions table_options;
  table_options.block_cache = cache;
  table_options.block_size = 4 * 1024;
  Options options = GetDefaultOptions();
  options.create_if_missing = true;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  options.paranoid_file_checks = true;
  options.env = fault_env_.get();
  fault_fs_->SetFailGetUniqueId(true);
  DestroyAndReopen(options);
  Random rnd(301);
  const int N = 6;
  for (int i = 0; i < N; i++) {
    std::string p_v = rnd.RandomString(1007);
    ASSERT_OK(Put(Key(i), p_v));
  }

  ASSERT_OK(Flush());
  // After Flush is successful, RocksDB will do the paranoid check for the new
  // SST file. Meta blocks are always cached in the block cache and they
  // will not be evicted. When block_2 is cache miss and read out, it is
  // inserted to the block cache. Thefore, block_1 is evicted from block
  // cache and successfully inserted to the secondary cache. Here are 2
  // lookups in the secondary cache for block_1 and block_2.
  ASSERT_EQ(secondary_cache->num_inserts(), 1u);
  ASSERT_EQ(secondary_cache->num_lookups(), 2u);

  Compact("a", "z");
  // Compaction will create the iterator to scan the whole file. So all the
  // blocks are needed. After Flush, only block_2 is cached in block cache
  // and block_1 is in the secondary cache. So when read block_1, it is
  // read out from secondary cache and inserted to block cache. At the same
  // time, block_2 is inserted to secondary cache. Now, secondary cache has
  // both block_1 and block_2. After compaction, block_1 is in the cache.
  ASSERT_EQ(secondary_cache->num_inserts(), 2u);
  ASSERT_EQ(secondary_cache->num_lookups(), 3u);

  std::string v = Get(Key(0));
  ASSERT_EQ(1007, v.size());
  // This Get needs to access block_1, since block_1 is cached in block cache
  // there is no secondary cache lookup.
  ASSERT_EQ(secondary_cache->num_inserts(), 2u);
  ASSERT_EQ(secondary_cache->num_lookups(), 3u);

  v = Get(Key(5));
  ASSERT_EQ(1007, v.size());
  // This Get needs to access block_2 which is not in the block cache. So
  // it will lookup the secondary cache for block_2 and cache it in the
  // block_cache.
  ASSERT_EQ(secondary_cache->num_inserts(), 2u);
  ASSERT_EQ(secondary_cache->num_lookups(), 4u);

  v = Get(Key(5));
  ASSERT_EQ(1007, v.size());
  // This Get needs to access block_2 which is already in the block cache.
  // No need to lookup secondary cache.
  ASSERT_EQ(secondary_cache->num_inserts(), 2u);
  ASSERT_EQ(secondary_cache->num_lookups(), 4u);

  v = Get(Key(0));
  ASSERT_EQ(1007, v.size());
  // This Get needs to access block_1, since block_1 is not in block cache
  // there is one econdary cache lookup. Then, block_1 is cached in the
  // block cache.
  ASSERT_EQ(secondary_cache->num_inserts(), 2u);
  ASSERT_EQ(secondary_cache->num_lookups(), 5u);

  v = Get(Key(0));
  ASSERT_EQ(1007, v.size());
  // This Get needs to access block_1, since block_1 is cached in block cache
  // there is no secondary cache lookup.
  ASSERT_EQ(secondary_cache->num_inserts(), 2u);
  ASSERT_EQ(secondary_cache->num_lookups(), 5u);

  Destroy(options);
}

// The block cache size is set to 1024*1024, after insert 6 KV-pairs
// and flush, there are 5 blocks in this SST file, 2 data blocks and 3 meta
// blocks. block_1 size is 4096 and block_2 size is 2056. The total size
// of the meta blocks are about 900 to 1000. Therefore, we can successfully
// cache all the blocks in the block cache and there is not secondary cache
// insertion. 2 lookup is needed for the blocks.
TEST_F(DBSecondaryCacheTest, NoSecondaryCacheInsertion) {
  LRUCacheOptions opts(1024 * 1024 /* capacity */, 0 /* num_shard_bits */,
                       false /* strict_capacity_limit */,
                       0.5 /* high_pri_pool_ratio */,
                       nullptr /* memory_allocator */, kDefaultToAdaptiveMutex,
                       kDontChargeCacheMetadata);
  std::shared_ptr<TestSecondaryCache> secondary_cache(
      new TestSecondaryCache(2048 * 1024));
  opts.secondary_cache = secondary_cache;
  std::shared_ptr<Cache> cache = NewLRUCache(opts);
  BlockBasedTableOptions table_options;
  table_options.block_cache = cache;
  table_options.block_size = 4 * 1024;
  Options options = GetDefaultOptions();
  options.create_if_missing = true;
  options.paranoid_file_checks = true;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  options.env = fault_env_.get();
  fault_fs_->SetFailGetUniqueId(true);

  DestroyAndReopen(options);
  Random rnd(301);
  const int N = 6;
  for (int i = 0; i < N; i++) {
    std::string p_v = rnd.RandomString(1000);
    ASSERT_OK(Put(Key(i), p_v));
  }

  ASSERT_OK(Flush());
  // After Flush is successful, RocksDB will do the paranoid check for the new
  // SST file. Meta blocks are always cached in the block cache and they
  // will not be evicted. Now, block cache is large enough, it cache
  // both block_1 and block_2. When first time read block_1 and block_2
  // there are cache misses. So 2 secondary cache lookups are needed for
  // the 2 blocks
  ASSERT_EQ(secondary_cache->num_inserts(), 0u);
  ASSERT_EQ(secondary_cache->num_lookups(), 2u);

  Compact("a", "z");
  // Compaction will iterate the whole SST file. Since all the data blocks
  // are in the block cache. No need to lookup the secondary cache.
  ASSERT_EQ(secondary_cache->num_inserts(), 0u);
  ASSERT_EQ(secondary_cache->num_lookups(), 2u);

  std::string v = Get(Key(0));
  ASSERT_EQ(1000, v.size());
  // Since the block cache is large enough, all the blocks are cached. we
  // do not need to lookup the seondary cache.
  ASSERT_EQ(secondary_cache->num_inserts(), 0u);
  ASSERT_EQ(secondary_cache->num_lookups(), 2u);

  Destroy(options);
}

TEST_F(DBSecondaryCacheTest, SecondaryCacheIntensiveTesting) {
  LRUCacheOptions opts(8 * 1024 /* capacity */, 0 /* num_shard_bits */,
                       false /* strict_capacity_limit */,
                       0.5 /* high_pri_pool_ratio */,
                       nullptr /* memory_allocator */, kDefaultToAdaptiveMutex,
                       kDontChargeCacheMetadata);
  std::shared_ptr<TestSecondaryCache> secondary_cache(
      new TestSecondaryCache(2048 * 1024));
  opts.secondary_cache = secondary_cache;
  std::shared_ptr<Cache> cache = NewLRUCache(opts);
  BlockBasedTableOptions table_options;
  table_options.block_cache = cache;
  table_options.block_size = 4 * 1024;
  Options options = GetDefaultOptions();
  options.create_if_missing = true;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  options.env = fault_env_.get();
  fault_fs_->SetFailGetUniqueId(true);
  DestroyAndReopen(options);
  Random rnd(301);
  const int N = 256;
  for (int i = 0; i < N; i++) {
    std::string p_v = rnd.RandomString(1000);
    ASSERT_OK(Put(Key(i), p_v));
  }
  ASSERT_OK(Flush());
  Compact("a", "z");

  Random r_index(47);
  std::string v;
  for (int i = 0; i < 1000; i++) {
    uint32_t key_i = r_index.Next() % N;
    v = Get(Key(key_i));
  }

  // We have over 200 data blocks there will be multiple insertion
  // and lookups.
  ASSERT_GE(secondary_cache->num_inserts(), 1u);
  ASSERT_GE(secondary_cache->num_lookups(), 1u);

  Destroy(options);
}

// In this test, the block cache size is set to 4096, after insert 6 KV-pairs
// and flush, there are 5 blocks in this SST file, 2 data blocks and 3 meta
// blocks. block_1 size is 4096 and block_2 size is 2056. The total size
// of the meta blocks are about 900 to 1000. Therefore, in any situation,
// if we try to insert block_1 to the block cache, it will always fails. Only
// block_2 will be successfully inserted into the block cache.
TEST_F(DBSecondaryCacheTest, SecondaryCacheFailureTest) {
  LRUCacheOptions opts(4 * 1024 /* capacity */, 0 /* num_shard_bits */,
                       false /* strict_capacity_limit */,
                       0.5 /* high_pri_pool_ratio */,
                       nullptr /* memory_allocator */, kDefaultToAdaptiveMutex,
                       kDontChargeCacheMetadata);
  std::shared_ptr<TestSecondaryCache> secondary_cache(
      new TestSecondaryCache(2048 * 1024));
  opts.secondary_cache = secondary_cache;
  std::shared_ptr<Cache> cache = NewLRUCache(opts);
  BlockBasedTableOptions table_options;
  table_options.block_cache = cache;
  table_options.block_size = 4 * 1024;
  Options options = GetDefaultOptions();
  options.create_if_missing = true;
  options.paranoid_file_checks = true;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  options.env = fault_env_.get();
  fault_fs_->SetFailGetUniqueId(true);
  DestroyAndReopen(options);
  Random rnd(301);
  const int N = 6;
  for (int i = 0; i < N; i++) {
    std::string p_v = rnd.RandomString(1007);
    ASSERT_OK(Put(Key(i), p_v));
  }

  ASSERT_OK(Flush());
  // After Flush is successful, RocksDB will do the paranoid check for the new
  // SST file. Meta blocks are always cached in the block cache and they
  // will not be evicted. When block_2 is cache miss and read out, it is
  // inserted to the block cache. Note that, block_1 is never successfully
  // inserted to the block cache. Here are 2 lookups in the secondary cache
  // for block_1 and block_2
  ASSERT_EQ(secondary_cache->num_inserts(), 0u);
  ASSERT_EQ(secondary_cache->num_lookups(), 2u);

  // Fail the insertion, in LRU cache, the secondary insertion returned status
  // is not checked, therefore, the DB will not be influenced.
  secondary_cache->InjectFailure();
  Compact("a", "z");
  // Compaction will create the iterator to scan the whole file. So all the
  // blocks are needed. Meta blocks are always cached. When block_1 is read
  // out, block_2 is evicted from block cache and inserted to secondary
  // cache.
  ASSERT_EQ(secondary_cache->num_inserts(), 0u);
  ASSERT_EQ(secondary_cache->num_lookups(), 3u);

  std::string v = Get(Key(0));
  ASSERT_EQ(1007, v.size());
  // The first data block is not in the cache, similarly, trigger the block
  // cache Lookup and secondary cache lookup for block_1. But block_1 will not
  // be inserted successfully due to the size. Currently, cache only has
  // the meta blocks.
  ASSERT_EQ(secondary_cache->num_inserts(), 0u);
  ASSERT_EQ(secondary_cache->num_lookups(), 4u);

  v = Get(Key(5));
  ASSERT_EQ(1007, v.size());
  // The second data block is not in the cache, similarly, trigger the block
  // cache Lookup and secondary cache lookup for block_2 and block_2 is found
  // in the secondary cache. Now block cache has block_2
  ASSERT_EQ(secondary_cache->num_inserts(), 0u);
  ASSERT_EQ(secondary_cache->num_lookups(), 5u);

  v = Get(Key(5));
  ASSERT_EQ(1007, v.size());
  // block_2 is in the block cache. There is a block cache hit. No need to
  // lookup or insert the secondary cache.
  ASSERT_EQ(secondary_cache->num_inserts(), 0u);
  ASSERT_EQ(secondary_cache->num_lookups(), 5u);

  v = Get(Key(0));
  ASSERT_EQ(1007, v.size());
  // Lookup the first data block, not in the block cache, so lookup the
  // secondary cache. Also not in the secondary cache. After Get, still
  // block_1 is will not be cached.
  ASSERT_EQ(secondary_cache->num_inserts(), 0u);
  ASSERT_EQ(secondary_cache->num_lookups(), 6u);

  v = Get(Key(0));
  ASSERT_EQ(1007, v.size());
  // Lookup the first data block, not in the block cache, so lookup the
  // secondary cache. Also not in the secondary cache. After Get, still
  // block_1 is will not be cached.
  ASSERT_EQ(secondary_cache->num_inserts(), 0u);
  ASSERT_EQ(secondary_cache->num_lookups(), 7u);
  secondary_cache->ResetInjectFailure();

  Destroy(options);
}

TEST_F(DBSecondaryCacheTest, TestSecondaryWithCompressedCache) {
  if (!Snappy_Supported()) {
    ROCKSDB_GTEST_SKIP("Compressed cache test requires snappy support");
    return;
  }
  LRUCacheOptions opts(2000 /* capacity */, 0 /* num_shard_bits */,
                       false /* strict_capacity_limit */,
                       0.5 /* high_pri_pool_ratio */,
                       nullptr /* memory_allocator */, kDefaultToAdaptiveMutex,
                       kDontChargeCacheMetadata);
  std::shared_ptr<TestSecondaryCache> secondary_cache(
      new TestSecondaryCache(2048 * 1024));
  opts.secondary_cache = secondary_cache;
  std::shared_ptr<Cache> cache = NewLRUCache(opts);
  BlockBasedTableOptions table_options;
  table_options.block_cache_compressed = cache;
  table_options.no_block_cache = true;
  table_options.block_size = 1234;
  Options options = GetDefaultOptions();
  options.compression = kSnappyCompression;
  options.create_if_missing = true;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  DestroyAndReopen(options);
  Random rnd(301);
  const int N = 6;
  for (int i = 0; i < N; i++) {
    // Partly compressible
    std::string p_v = rnd.RandomString(507) + std::string(500, ' ');
    ASSERT_OK(Put(Key(i), p_v));
  }
  ASSERT_OK(Flush());
  for (int i = 0; i < 2 * N; i++) {
    std::string v = Get(Key(i % N));
    ASSERT_EQ(1007, v.size());
  }
}

TEST_F(LRUCacheSecondaryCacheTest, BasicWaitAllTest) {
  LRUCacheOptions opts(1024 /* capacity */, 2 /* num_shard_bits */,
                       false /* strict_capacity_limit */,
                       0.5 /* high_pri_pool_ratio */,
                       nullptr /* memory_allocator */, kDefaultToAdaptiveMutex,
                       kDontChargeCacheMetadata);
  std::shared_ptr<TestSecondaryCache> secondary_cache =
      std::make_shared<TestSecondaryCache>(32 * 1024);
  opts.secondary_cache = secondary_cache;
  std::shared_ptr<Cache> cache = NewLRUCache(opts);
  const int num_keys = 32;
  OffsetableCacheKey ock{"foo", "bar", 1};

  Random rnd(301);
  std::vector<std::string> values;
  for (int i = 0; i < num_keys; ++i) {
    std::string str = rnd.RandomString(1020);
    values.emplace_back(str);
    TestItem* item = new TestItem(str.data(), str.length());
    ASSERT_OK(cache->Insert(ock.WithOffset(i).AsSlice(), item,
                            &LRUCacheSecondaryCacheTest::helper_,
                            str.length()));
  }
  // Force all entries to be evicted to the secondary cache
  cache->SetCapacity(0);
  ASSERT_EQ(secondary_cache->num_inserts(), 32u);
  cache->SetCapacity(32 * 1024);

  secondary_cache->SetResultMap(
      {{ock.WithOffset(3).AsSlice().ToString(),
        TestSecondaryCache::ResultType::DEFER},
       {ock.WithOffset(4).AsSlice().ToString(),
        TestSecondaryCache::ResultType::DEFER_AND_FAIL},
       {ock.WithOffset(5).AsSlice().ToString(),
        TestSecondaryCache::ResultType::FAIL}});
  std::vector<Cache::Handle*> results;
  for (int i = 0; i < 6; ++i) {
    results.emplace_back(cache->Lookup(
        ock.WithOffset(i).AsSlice(), &LRUCacheSecondaryCacheTest::helper_,
        /*context*/ this, Cache::Priority::LOW, false));
  }
  cache->WaitAll(results);
  for (int i = 0; i < 6; ++i) {
    if (i == 4) {
      ASSERT_EQ(cache->Value(results[i]), nullptr);
    } else if (i == 5) {
      ASSERT_EQ(results[i], nullptr);
      continue;
    } else {
      TestItem* item = static_cast<TestItem*>(cache->Value(results[i]));
      ASSERT_EQ(item->ToString(), values[i]);
    }
    cache->Release(results[i]);
  }

  cache.reset();
  secondary_cache.reset();
}

// In this test, we have one KV pair per data block. We indirectly determine
// the cache key associated with each data block (and thus each KV) by using
// a sync point callback in TestSecondaryCache::Lookup. We then control the
// lookup result by setting the ResultMap.
TEST_F(DBSecondaryCacheTest, TestSecondaryCacheMultiGet) {
  LRUCacheOptions opts(1 << 20 /* capacity */, 0 /* num_shard_bits */,
                       false /* strict_capacity_limit */,
                       0.5 /* high_pri_pool_ratio */,
                       nullptr /* memory_allocator */, kDefaultToAdaptiveMutex,
                       kDontChargeCacheMetadata);
  std::shared_ptr<TestSecondaryCache> secondary_cache(
      new TestSecondaryCache(2048 * 1024));
  opts.secondary_cache = secondary_cache;
  std::shared_ptr<Cache> cache = NewLRUCache(opts);
  BlockBasedTableOptions table_options;
  table_options.block_cache = cache;
  table_options.block_size = 4 * 1024;
  table_options.cache_index_and_filter_blocks = false;
  Options options = GetDefaultOptions();
  options.create_if_missing = true;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  options.paranoid_file_checks = true;
  DestroyAndReopen(options);
  Random rnd(301);
  const int N = 8;
  std::vector<std::string> keys;
  for (int i = 0; i < N; i++) {
    std::string p_v = rnd.RandomString(4000);
    keys.emplace_back(p_v);
    ASSERT_OK(Put(Key(i), p_v));
  }

  ASSERT_OK(Flush());
  // After Flush is successful, RocksDB does the paranoid check for the new
  // SST file. This will try to lookup all data blocks in the secondary
  // cache.
  ASSERT_EQ(secondary_cache->num_inserts(), 0u);
  ASSERT_EQ(secondary_cache->num_lookups(), 8u);

  cache->SetCapacity(0);
  ASSERT_EQ(secondary_cache->num_inserts(), 8u);
  cache->SetCapacity(1 << 20);

  std::vector<std::string> cache_keys;
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "TestSecondaryCache::Lookup", [&cache_keys](void* key) -> void {
        cache_keys.emplace_back(*(static_cast<std::string*>(key)));
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();
  for (int i = 0; i < N; ++i) {
    std::string v = Get(Key(i));
    ASSERT_EQ(4000, v.size());
    ASSERT_EQ(v, keys[i]);
  }
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
  ASSERT_EQ(secondary_cache->num_lookups(), 16u);
  cache->SetCapacity(0);
  cache->SetCapacity(1 << 20);

  ASSERT_EQ(Get(Key(2)), keys[2]);
  ASSERT_EQ(Get(Key(7)), keys[7]);
  secondary_cache->SetResultMap(
      {{cache_keys[3], TestSecondaryCache::ResultType::DEFER},
       {cache_keys[4], TestSecondaryCache::ResultType::DEFER_AND_FAIL},
       {cache_keys[5], TestSecondaryCache::ResultType::FAIL}});

  std::vector<std::string> mget_keys(
      {Key(0), Key(1), Key(2), Key(3), Key(4), Key(5), Key(6), Key(7)});
  std::vector<PinnableSlice> values(mget_keys.size());
  std::vector<Status> s(keys.size());
  std::vector<Slice> key_slices;
  for (const std::string& key : mget_keys) {
    key_slices.emplace_back(key);
  }
  uint32_t num_lookups = secondary_cache->num_lookups();
  dbfull()->MultiGet(ReadOptions(), dbfull()->DefaultColumnFamily(),
                     key_slices.size(), key_slices.data(), values.data(),
                     s.data(), false);
  ASSERT_EQ(secondary_cache->num_lookups(), num_lookups + 5);
  for (int i = 0; i < N; ++i) {
    ASSERT_OK(s[i]);
    ASSERT_EQ(values[i].ToString(), keys[i]);
    values[i].Reset();
  }
  Destroy(options);
}

class LRUCacheWithStat : public LRUCache {
 public:
  LRUCacheWithStat(
      size_t _capacity, int _num_shard_bits, bool _strict_capacity_limit,
      double _high_pri_pool_ratio, double _low_pri_pool_ratio,
      std::shared_ptr<MemoryAllocator> _memory_allocator = nullptr,
      bool _use_adaptive_mutex = kDefaultToAdaptiveMutex,
      CacheMetadataChargePolicy _metadata_charge_policy =
          kDontChargeCacheMetadata,
      const std::shared_ptr<SecondaryCache>& _secondary_cache = nullptr)
      : LRUCache(_capacity, _num_shard_bits, _strict_capacity_limit,
                 _high_pri_pool_ratio, _low_pri_pool_ratio, _memory_allocator,
                 _use_adaptive_mutex, _metadata_charge_policy,
                 _secondary_cache) {
    insert_count_ = 0;
    lookup_count_ = 0;
  }
  ~LRUCacheWithStat() {}

  Status Insert(const Slice& key, Cache::ObjectPtr value,
                const CacheItemHelper* helper, size_t charge,
                Handle** handle = nullptr,
                Priority priority = Priority::LOW) override {
    insert_count_++;
    return LRUCache::Insert(key, value, helper, charge, handle, priority);
  }
  Handle* Lookup(const Slice& key, const CacheItemHelper* helper,
                 CreateContext* create_context, Priority priority, bool wait,
                 Statistics* stats = nullptr) override {
    lookup_count_++;
    return LRUCache::Lookup(key, helper, create_context, priority, wait, stats);
  }

  uint32_t GetInsertCount() { return insert_count_; }
  uint32_t GetLookupcount() { return lookup_count_; }
  void ResetCount() {
    insert_count_ = 0;
    lookup_count_ = 0;
  }

 private:
  uint32_t insert_count_;
  uint32_t lookup_count_;
};

#ifndef ROCKSDB_LITE

TEST_F(DBSecondaryCacheTest, LRUCacheDumpLoadBasic) {
  LRUCacheOptions cache_opts(1024 * 1024 /* capacity */, 0 /* num_shard_bits */,
                             false /* strict_capacity_limit */,
                             0.5 /* high_pri_pool_ratio */,
                             nullptr /* memory_allocator */,
                             kDefaultToAdaptiveMutex, kDontChargeCacheMetadata);
  LRUCacheWithStat* tmp_cache = new LRUCacheWithStat(
      cache_opts.capacity, cache_opts.num_shard_bits,
      cache_opts.strict_capacity_limit, cache_opts.high_pri_pool_ratio,
      cache_opts.low_pri_pool_ratio, cache_opts.memory_allocator,
      cache_opts.use_adaptive_mutex, cache_opts.metadata_charge_policy,
      cache_opts.secondary_cache);
  std::shared_ptr<Cache> cache(tmp_cache);
  BlockBasedTableOptions table_options;
  table_options.block_cache = cache;
  table_options.block_size = 4 * 1024;
  Options options = GetDefaultOptions();
  options.create_if_missing = true;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  options.env = fault_env_.get();
  DestroyAndReopen(options);
  fault_fs_->SetFailGetUniqueId(true);

  Random rnd(301);
  const int N = 256;
  std::vector<std::string> value;
  char buf[1000];
  memset(buf, 'a', 1000);
  value.resize(N);
  for (int i = 0; i < N; i++) {
    // std::string p_v = rnd.RandomString(1000);
    std::string p_v(buf, 1000);
    value[i] = p_v;
    ASSERT_OK(Put(Key(i), p_v));
  }
  ASSERT_OK(Flush());
  Compact("a", "z");

  // do th eread for all the key value pairs, so all the blocks should be in
  // cache
  uint32_t start_insert = tmp_cache->GetInsertCount();
  uint32_t start_lookup = tmp_cache->GetLookupcount();
  std::string v;
  for (int i = 0; i < N; i++) {
    v = Get(Key(i));
    ASSERT_EQ(v, value[i]);
  }
  uint32_t dump_insert = tmp_cache->GetInsertCount() - start_insert;
  uint32_t dump_lookup = tmp_cache->GetLookupcount() - start_lookup;
  ASSERT_EQ(63,
            static_cast<int>(dump_insert));  // the insert in the block cache
  ASSERT_EQ(256,
            static_cast<int>(dump_lookup));  // the lookup in the block cache
  // We have enough blocks in the block cache

  CacheDumpOptions cd_options;
  cd_options.clock = fault_env_->GetSystemClock().get();
  std::string dump_path = db_->GetName() + "/cache_dump";
  std::unique_ptr<CacheDumpWriter> dump_writer;
  Status s = NewToFileCacheDumpWriter(fault_fs_, FileOptions(), dump_path,
                                      &dump_writer);
  ASSERT_OK(s);
  std::unique_ptr<CacheDumper> cache_dumper;
  s = NewDefaultCacheDumper(cd_options, cache, std::move(dump_writer),
                            &cache_dumper);
  ASSERT_OK(s);
  std::vector<DB*> db_list;
  db_list.push_back(db_);
  s = cache_dumper->SetDumpFilter(db_list);
  ASSERT_OK(s);
  s = cache_dumper->DumpCacheEntriesToWriter();
  ASSERT_OK(s);
  cache_dumper.reset();

  // we have a new cache it is empty, then, before we do the Get, we do the
  // dumpload
  std::shared_ptr<TestSecondaryCache> secondary_cache =
      std::make_shared<TestSecondaryCache>(2048 * 1024);
  cache_opts.secondary_cache = secondary_cache;
  tmp_cache = new LRUCacheWithStat(
      cache_opts.capacity, cache_opts.num_shard_bits,
      cache_opts.strict_capacity_limit, cache_opts.high_pri_pool_ratio,
      cache_opts.low_pri_pool_ratio, cache_opts.memory_allocator,
      cache_opts.use_adaptive_mutex, cache_opts.metadata_charge_policy,
      cache_opts.secondary_cache);
  std::shared_ptr<Cache> cache_new(tmp_cache);
  table_options.block_cache = cache_new;
  table_options.block_size = 4 * 1024;
  options.create_if_missing = true;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  options.env = fault_env_.get();

  // start to load the data to new block cache
  start_insert = secondary_cache->num_inserts();
  start_lookup = secondary_cache->num_lookups();
  std::unique_ptr<CacheDumpReader> dump_reader;
  s = NewFromFileCacheDumpReader(fault_fs_, FileOptions(), dump_path,
                                 &dump_reader);
  ASSERT_OK(s);
  std::unique_ptr<CacheDumpedLoader> cache_loader;
  s = NewDefaultCacheDumpedLoader(cd_options, table_options, secondary_cache,
                                  std::move(dump_reader), &cache_loader);
  ASSERT_OK(s);
  s = cache_loader->RestoreCacheEntriesToSecondaryCache();
  ASSERT_OK(s);
  uint32_t load_insert = secondary_cache->num_inserts() - start_insert;
  uint32_t load_lookup = secondary_cache->num_lookups() - start_lookup;
  // check the number we inserted
  ASSERT_EQ(64, static_cast<int>(load_insert));
  ASSERT_EQ(0, static_cast<int>(load_lookup));
  ASSERT_OK(s);

  Reopen(options);

  // After load, we do the Get again
  start_insert = secondary_cache->num_inserts();
  start_lookup = secondary_cache->num_lookups();
  uint32_t cache_insert = tmp_cache->GetInsertCount();
  uint32_t cache_lookup = tmp_cache->GetLookupcount();
  for (int i = 0; i < N; i++) {
    v = Get(Key(i));
    ASSERT_EQ(v, value[i]);
  }
  uint32_t final_insert = secondary_cache->num_inserts() - start_insert;
  uint32_t final_lookup = secondary_cache->num_lookups() - start_lookup;
  // no insert to secondary cache
  ASSERT_EQ(0, static_cast<int>(final_insert));
  // lookup the secondary to get all blocks
  ASSERT_EQ(64, static_cast<int>(final_lookup));
  uint32_t block_insert = tmp_cache->GetInsertCount() - cache_insert;
  uint32_t block_lookup = tmp_cache->GetLookupcount() - cache_lookup;
  // Check the new block cache insert and lookup, should be no insert since all
  // blocks are from the secondary cache.
  ASSERT_EQ(0, static_cast<int>(block_insert));
  ASSERT_EQ(256, static_cast<int>(block_lookup));

  fault_fs_->SetFailGetUniqueId(false);
  Destroy(options);
}

TEST_F(DBSecondaryCacheTest, LRUCacheDumpLoadWithFilter) {
  LRUCacheOptions cache_opts(1024 * 1024 /* capacity */, 0 /* num_shard_bits */,
                             false /* strict_capacity_limit */,
                             0.5 /* high_pri_pool_ratio */,
                             nullptr /* memory_allocator */,
                             kDefaultToAdaptiveMutex, kDontChargeCacheMetadata);
  LRUCacheWithStat* tmp_cache = new LRUCacheWithStat(
      cache_opts.capacity, cache_opts.num_shard_bits,
      cache_opts.strict_capacity_limit, cache_opts.high_pri_pool_ratio,
      cache_opts.low_pri_pool_ratio, cache_opts.memory_allocator,
      cache_opts.use_adaptive_mutex, cache_opts.metadata_charge_policy,
      cache_opts.secondary_cache);
  std::shared_ptr<Cache> cache(tmp_cache);
  BlockBasedTableOptions table_options;
  table_options.block_cache = cache;
  table_options.block_size = 4 * 1024;
  Options options = GetDefaultOptions();
  options.create_if_missing = true;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  options.env = fault_env_.get();
  std::string dbname1 = test::PerThreadDBPath("db_1");
  ASSERT_OK(DestroyDB(dbname1, options));
  DB* db1 = nullptr;
  ASSERT_OK(DB::Open(options, dbname1, &db1));
  std::string dbname2 = test::PerThreadDBPath("db_2");
  ASSERT_OK(DestroyDB(dbname2, options));
  DB* db2 = nullptr;
  ASSERT_OK(DB::Open(options, dbname2, &db2));
  fault_fs_->SetFailGetUniqueId(true);

  // write the KVs to db1
  Random rnd(301);
  const int N = 256;
  std::vector<std::string> value1;
  WriteOptions wo;
  char buf[1000];
  memset(buf, 'a', 1000);
  value1.resize(N);
  for (int i = 0; i < N; i++) {
    std::string p_v(buf, 1000);
    value1[i] = p_v;
    ASSERT_OK(db1->Put(wo, Key(i), p_v));
  }
  ASSERT_OK(db1->Flush(FlushOptions()));
  Slice bg("a");
  Slice ed("b");
  ASSERT_OK(db1->CompactRange(CompactRangeOptions(), &bg, &ed));

  // Write the KVs to DB2
  std::vector<std::string> value2;
  memset(buf, 'b', 1000);
  value2.resize(N);
  for (int i = 0; i < N; i++) {
    std::string p_v(buf, 1000);
    value2[i] = p_v;
    ASSERT_OK(db2->Put(wo, Key(i), p_v));
  }
  ASSERT_OK(db2->Flush(FlushOptions()));
  ASSERT_OK(db2->CompactRange(CompactRangeOptions(), &bg, &ed));

  // do th eread for all the key value pairs, so all the blocks should be in
  // cache
  uint32_t start_insert = tmp_cache->GetInsertCount();
  uint32_t start_lookup = tmp_cache->GetLookupcount();
  ReadOptions ro;
  std::string v;
  for (int i = 0; i < N; i++) {
    ASSERT_OK(db1->Get(ro, Key(i), &v));
    ASSERT_EQ(v, value1[i]);
  }
  for (int i = 0; i < N; i++) {
    ASSERT_OK(db2->Get(ro, Key(i), &v));
    ASSERT_EQ(v, value2[i]);
  }
  uint32_t dump_insert = tmp_cache->GetInsertCount() - start_insert;
  uint32_t dump_lookup = tmp_cache->GetLookupcount() - start_lookup;
  ASSERT_EQ(128,
            static_cast<int>(dump_insert));  // the insert in the block cache
  ASSERT_EQ(512,
            static_cast<int>(dump_lookup));  // the lookup in the block cache
  // We have enough blocks in the block cache

  CacheDumpOptions cd_options;
  cd_options.clock = fault_env_->GetSystemClock().get();
  std::string dump_path = db1->GetName() + "/cache_dump";
  std::unique_ptr<CacheDumpWriter> dump_writer;
  Status s = NewToFileCacheDumpWriter(fault_fs_, FileOptions(), dump_path,
                                      &dump_writer);
  ASSERT_OK(s);
  std::unique_ptr<CacheDumper> cache_dumper;
  s = NewDefaultCacheDumper(cd_options, cache, std::move(dump_writer),
                            &cache_dumper);
  ASSERT_OK(s);
  std::vector<DB*> db_list;
  db_list.push_back(db1);
  s = cache_dumper->SetDumpFilter(db_list);
  ASSERT_OK(s);
  s = cache_dumper->DumpCacheEntriesToWriter();
  ASSERT_OK(s);
  cache_dumper.reset();

  // we have a new cache it is empty, then, before we do the Get, we do the
  // dumpload
  std::shared_ptr<TestSecondaryCache> secondary_cache =
      std::make_shared<TestSecondaryCache>(2048 * 1024);
  cache_opts.secondary_cache = secondary_cache;
  tmp_cache = new LRUCacheWithStat(
      cache_opts.capacity, cache_opts.num_shard_bits,
      cache_opts.strict_capacity_limit, cache_opts.high_pri_pool_ratio,
      cache_opts.low_pri_pool_ratio, cache_opts.memory_allocator,
      cache_opts.use_adaptive_mutex, cache_opts.metadata_charge_policy,
      cache_opts.secondary_cache);
  std::shared_ptr<Cache> cache_new(tmp_cache);
  table_options.block_cache = cache_new;
  table_options.block_size = 4 * 1024;
  options.create_if_missing = true;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  options.env = fault_env_.get();

  // Start the cache loading process
  start_insert = secondary_cache->num_inserts();
  start_lookup = secondary_cache->num_lookups();
  std::unique_ptr<CacheDumpReader> dump_reader;
  s = NewFromFileCacheDumpReader(fault_fs_, FileOptions(), dump_path,
                                 &dump_reader);
  ASSERT_OK(s);
  std::unique_ptr<CacheDumpedLoader> cache_loader;
  s = NewDefaultCacheDumpedLoader(cd_options, table_options, secondary_cache,
                                  std::move(dump_reader), &cache_loader);
  ASSERT_OK(s);
  s = cache_loader->RestoreCacheEntriesToSecondaryCache();
  ASSERT_OK(s);
  uint32_t load_insert = secondary_cache->num_inserts() - start_insert;
  uint32_t load_lookup = secondary_cache->num_lookups() - start_lookup;
  // check the number we inserted
  ASSERT_EQ(64, static_cast<int>(load_insert));
  ASSERT_EQ(0, static_cast<int>(load_lookup));
  ASSERT_OK(s);

  ASSERT_OK(db1->Close());
  delete db1;
  ASSERT_OK(DB::Open(options, dbname1, &db1));

  // After load, we do the Get again. To validate the cache, we do not allow any
  // I/O, so we set the file system to false.
  IOStatus error_msg = IOStatus::IOError("Retryable IO Error");
  fault_fs_->SetFilesystemActive(false, error_msg);
  start_insert = secondary_cache->num_inserts();
  start_lookup = secondary_cache->num_lookups();
  uint32_t cache_insert = tmp_cache->GetInsertCount();
  uint32_t cache_lookup = tmp_cache->GetLookupcount();
  for (int i = 0; i < N; i++) {
    ASSERT_OK(db1->Get(ro, Key(i), &v));
    ASSERT_EQ(v, value1[i]);
  }
  uint32_t final_insert = secondary_cache->num_inserts() - start_insert;
  uint32_t final_lookup = secondary_cache->num_lookups() - start_lookup;
  // no insert to secondary cache
  ASSERT_EQ(0, static_cast<int>(final_insert));
  // lookup the secondary to get all blocks
  ASSERT_EQ(64, static_cast<int>(final_lookup));
  uint32_t block_insert = tmp_cache->GetInsertCount() - cache_insert;
  uint32_t block_lookup = tmp_cache->GetLookupcount() - cache_lookup;
  // Check the new block cache insert and lookup, should be no insert since all
  // blocks are from the secondary cache.
  ASSERT_EQ(0, static_cast<int>(block_insert));
  ASSERT_EQ(256, static_cast<int>(block_lookup));
  fault_fs_->SetFailGetUniqueId(false);
  fault_fs_->SetFilesystemActive(true);
  delete db1;
  delete db2;
  ASSERT_OK(DestroyDB(dbname1, options));
  ASSERT_OK(DestroyDB(dbname2, options));
}

// Test the option not to use the secondary cache in a certain DB.
TEST_F(DBSecondaryCacheTest, TestSecondaryCacheOptionBasic) {
  LRUCacheOptions opts(4 * 1024 /* capacity */, 0 /* num_shard_bits */,
                       false /* strict_capacity_limit */,
                       0.5 /* high_pri_pool_ratio */,
                       nullptr /* memory_allocator */, kDefaultToAdaptiveMutex,
                       kDontChargeCacheMetadata);
  std::shared_ptr<TestSecondaryCache> secondary_cache(
      new TestSecondaryCache(2048 * 1024));
  opts.secondary_cache = secondary_cache;
  std::shared_ptr<Cache> cache = NewLRUCache(opts);
  BlockBasedTableOptions table_options;
  table_options.block_cache = cache;
  table_options.block_size = 4 * 1024;
  Options options = GetDefaultOptions();
  options.create_if_missing = true;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  options.env = fault_env_.get();
  fault_fs_->SetFailGetUniqueId(true);
  options.lowest_used_cache_tier = CacheTier::kVolatileTier;

  // Set the file paranoid check, so after flush, the file will be read
  // all the blocks will be accessed.
  options.paranoid_file_checks = true;
  DestroyAndReopen(options);
  Random rnd(301);
  const int N = 6;
  for (int i = 0; i < N; i++) {
    std::string p_v = rnd.RandomString(1007);
    ASSERT_OK(Put(Key(i), p_v));
  }

  ASSERT_OK(Flush());

  for (int i = 0; i < N; i++) {
    std::string p_v = rnd.RandomString(1007);
    ASSERT_OK(Put(Key(i + 70), p_v));
  }

  ASSERT_OK(Flush());

  // Flush will trigger the paranoid check and read blocks. But only block cache
  // will be read. No operations for secondary cache.
  ASSERT_EQ(secondary_cache->num_inserts(), 0u);
  ASSERT_EQ(secondary_cache->num_lookups(), 0u);

  Compact("a", "z");

  // Compaction will also insert and evict blocks, no operations to the block
  // cache. No operations for secondary cache.
  ASSERT_EQ(secondary_cache->num_inserts(), 0u);
  ASSERT_EQ(secondary_cache->num_lookups(), 0u);

  std::string v = Get(Key(0));
  ASSERT_EQ(1007, v.size());

  // Check the data in first block. Cache miss, direclty read from SST file.
  ASSERT_EQ(secondary_cache->num_inserts(), 0u);
  ASSERT_EQ(secondary_cache->num_lookups(), 0u);

  v = Get(Key(5));
  ASSERT_EQ(1007, v.size());

  // Check the second block.
  ASSERT_EQ(secondary_cache->num_inserts(), 0u);
  ASSERT_EQ(secondary_cache->num_lookups(), 0u);

  v = Get(Key(5));
  ASSERT_EQ(1007, v.size());

  // block cache hit
  ASSERT_EQ(secondary_cache->num_inserts(), 0u);
  ASSERT_EQ(secondary_cache->num_lookups(), 0u);

  v = Get(Key(70));
  ASSERT_EQ(1007, v.size());

  // Check the first block in the second SST file. Cache miss and trigger SST
  // file read. No operations for secondary cache.
  ASSERT_EQ(secondary_cache->num_inserts(), 0u);
  ASSERT_EQ(secondary_cache->num_lookups(), 0u);

  v = Get(Key(75));
  ASSERT_EQ(1007, v.size());

  // Check the second block in the second SST file. Cache miss and trigger SST
  // file read. No operations for secondary cache.
  ASSERT_EQ(secondary_cache->num_inserts(), 0u);
  ASSERT_EQ(secondary_cache->num_lookups(), 0u);

  Destroy(options);
}

// We disable the secondary cache in DBOptions at first. Close and reopen the DB
// with new options, which set the lowest_used_cache_tier to
// kNonVolatileBlockTier. So secondary cache will be used.
TEST_F(DBSecondaryCacheTest, TestSecondaryCacheOptionChange) {
  LRUCacheOptions opts(4 * 1024 /* capacity */, 0 /* num_shard_bits */,
                       false /* strict_capacity_limit */,
                       0.5 /* high_pri_pool_ratio */,
                       nullptr /* memory_allocator */, kDefaultToAdaptiveMutex,
                       kDontChargeCacheMetadata);
  std::shared_ptr<TestSecondaryCache> secondary_cache(
      new TestSecondaryCache(2048 * 1024));
  opts.secondary_cache = secondary_cache;
  std::shared_ptr<Cache> cache = NewLRUCache(opts);
  BlockBasedTableOptions table_options;
  table_options.block_cache = cache;
  table_options.block_size = 4 * 1024;
  Options options = GetDefaultOptions();
  options.create_if_missing = true;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  options.env = fault_env_.get();
  fault_fs_->SetFailGetUniqueId(true);
  options.lowest_used_cache_tier = CacheTier::kVolatileTier;

  // Set the file paranoid check, so after flush, the file will be read
  // all the blocks will be accessed.
  options.paranoid_file_checks = true;
  DestroyAndReopen(options);
  Random rnd(301);
  const int N = 6;
  for (int i = 0; i < N; i++) {
    std::string p_v = rnd.RandomString(1007);
    ASSERT_OK(Put(Key(i), p_v));
  }

  ASSERT_OK(Flush());

  for (int i = 0; i < N; i++) {
    std::string p_v = rnd.RandomString(1007);
    ASSERT_OK(Put(Key(i + 70), p_v));
  }

  ASSERT_OK(Flush());

  // Flush will trigger the paranoid check and read blocks. But only block cache
  // will be read.
  ASSERT_EQ(secondary_cache->num_inserts(), 0u);
  ASSERT_EQ(secondary_cache->num_lookups(), 0u);

  Compact("a", "z");

  // Compaction will also insert and evict blocks, no operations to the block
  // cache.
  ASSERT_EQ(secondary_cache->num_inserts(), 0u);
  ASSERT_EQ(secondary_cache->num_lookups(), 0u);

  std::string v = Get(Key(0));
  ASSERT_EQ(1007, v.size());

  // Check the data in first block. Cache miss, direclty read from SST file.
  ASSERT_EQ(secondary_cache->num_inserts(), 0u);
  ASSERT_EQ(secondary_cache->num_lookups(), 0u);

  v = Get(Key(5));
  ASSERT_EQ(1007, v.size());

  // Check the second block.
  ASSERT_EQ(secondary_cache->num_inserts(), 0u);
  ASSERT_EQ(secondary_cache->num_lookups(), 0u);

  v = Get(Key(5));
  ASSERT_EQ(1007, v.size());

  // block cache hit
  ASSERT_EQ(secondary_cache->num_inserts(), 0u);
  ASSERT_EQ(secondary_cache->num_lookups(), 0u);

  // Change the option to enable secondary cache after we Reopen the DB
  options.lowest_used_cache_tier = CacheTier::kNonVolatileBlockTier;
  Reopen(options);

  v = Get(Key(70));
  ASSERT_EQ(1007, v.size());

  // Enable the secondary cache, trigger lookup of the first block in second SST
  ASSERT_EQ(secondary_cache->num_inserts(), 0u);
  ASSERT_EQ(secondary_cache->num_lookups(), 1u);

  v = Get(Key(75));
  ASSERT_EQ(1007, v.size());

  // trigger lookup of the second block in second SST
  ASSERT_EQ(secondary_cache->num_inserts(), 0u);
  ASSERT_EQ(secondary_cache->num_lookups(), 2u);
  Destroy(options);
}

// Two DB test. We create 2 DBs sharing the same block cache and secondary
// cache. We diable the secondary cache option for DB2.
TEST_F(DBSecondaryCacheTest, TestSecondaryCacheOptionTwoDB) {
  LRUCacheOptions opts(4 * 1024 /* capacity */, 0 /* num_shard_bits */,
                       false /* strict_capacity_limit */,
                       0.5 /* high_pri_pool_ratio */,
                       nullptr /* memory_allocator */, kDefaultToAdaptiveMutex,
                       kDontChargeCacheMetadata);
  std::shared_ptr<TestSecondaryCache> secondary_cache(
      new TestSecondaryCache(2048 * 1024));
  opts.secondary_cache = secondary_cache;
  std::shared_ptr<Cache> cache = NewLRUCache(opts);
  BlockBasedTableOptions table_options;
  table_options.block_cache = cache;
  table_options.block_size = 4 * 1024;
  Options options = GetDefaultOptions();
  options.create_if_missing = true;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  options.env = fault_env_.get();
  options.paranoid_file_checks = true;
  std::string dbname1 = test::PerThreadDBPath("db_t_1");
  ASSERT_OK(DestroyDB(dbname1, options));
  DB* db1 = nullptr;
  ASSERT_OK(DB::Open(options, dbname1, &db1));
  std::string dbname2 = test::PerThreadDBPath("db_t_2");
  ASSERT_OK(DestroyDB(dbname2, options));
  DB* db2 = nullptr;
  Options options2 = options;
  options2.lowest_used_cache_tier = CacheTier::kVolatileTier;
  ASSERT_OK(DB::Open(options2, dbname2, &db2));
  fault_fs_->SetFailGetUniqueId(true);

  WriteOptions wo;
  Random rnd(301);
  const int N = 6;
  for (int i = 0; i < N; i++) {
    std::string p_v = rnd.RandomString(1007);
    ASSERT_OK(db1->Put(wo, Key(i), p_v));
  }

  ASSERT_EQ(secondary_cache->num_inserts(), 0u);
  ASSERT_EQ(secondary_cache->num_lookups(), 0u);
  ASSERT_OK(db1->Flush(FlushOptions()));

  ASSERT_EQ(secondary_cache->num_inserts(), 0u);
  ASSERT_EQ(secondary_cache->num_lookups(), 2u);

  for (int i = 0; i < N; i++) {
    std::string p_v = rnd.RandomString(1007);
    ASSERT_OK(db2->Put(wo, Key(i), p_v));
  }

  // No change in the secondary cache, since it is disabled in DB2
  ASSERT_EQ(secondary_cache->num_inserts(), 0u);
  ASSERT_EQ(secondary_cache->num_lookups(), 2u);
  ASSERT_OK(db2->Flush(FlushOptions()));
  ASSERT_EQ(secondary_cache->num_inserts(), 1u);
  ASSERT_EQ(secondary_cache->num_lookups(), 2u);

  Slice bg("a");
  Slice ed("b");
  ASSERT_OK(db1->CompactRange(CompactRangeOptions(), &bg, &ed));
  ASSERT_OK(db2->CompactRange(CompactRangeOptions(), &bg, &ed));

  ASSERT_EQ(secondary_cache->num_inserts(), 1u);
  ASSERT_EQ(secondary_cache->num_lookups(), 2u);

  ReadOptions ro;
  std::string v;
  ASSERT_OK(db1->Get(ro, Key(0), &v));
  ASSERT_EQ(1007, v.size());

  // DB 1 has lookup block 1 and it is miss in block cache, trigger secondary
  // cache lookup
  ASSERT_EQ(secondary_cache->num_inserts(), 1u);
  ASSERT_EQ(secondary_cache->num_lookups(), 3u);

  ASSERT_OK(db1->Get(ro, Key(5), &v));
  ASSERT_EQ(1007, v.size());

  // DB 1 lookup the second block and it is miss in block cache, trigger
  // secondary cache lookup
  ASSERT_EQ(secondary_cache->num_inserts(), 1u);
  ASSERT_EQ(secondary_cache->num_lookups(), 4u);

  ASSERT_OK(db2->Get(ro, Key(0), &v));
  ASSERT_EQ(1007, v.size());

  // For db2, it is not enabled with secondary cache, so no search in the
  // secondary cache
  ASSERT_EQ(secondary_cache->num_inserts(), 1u);
  ASSERT_EQ(secondary_cache->num_lookups(), 4u);

  ASSERT_OK(db2->Get(ro, Key(5), &v));
  ASSERT_EQ(1007, v.size());

  // For db2, it is not enabled with secondary cache, so no search in the
  // secondary cache
  ASSERT_EQ(secondary_cache->num_inserts(), 1u);
  ASSERT_EQ(secondary_cache->num_lookups(), 4u);

  fault_fs_->SetFailGetUniqueId(false);
  fault_fs_->SetFilesystemActive(true);
  delete db1;
  delete db2;
  ASSERT_OK(DestroyDB(dbname1, options));
  ASSERT_OK(DestroyDB(dbname2, options));
}

#endif  // ROCKSDB_LITE

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

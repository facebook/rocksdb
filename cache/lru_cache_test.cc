//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "cache/lru_cache.h"

#include <string>
#include <vector>

#include "port/port.h"
#include "rocksdb/cache.h"
#include "test_util/testharness.h"
#include "util/coding.h"
#include "util/random.h"

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
                bool use_adaptive_mutex = kDefaultToAdaptiveMutex) {
    DeleteCache();
    cache_ = reinterpret_cast<LRUCacheShard*>(
        port::cacheline_aligned_alloc(sizeof(LRUCacheShard)));
    new (cache_) LRUCacheShard(
        capacity, false /*strict_capcity_limit*/, high_pri_pool_ratio,
        use_adaptive_mutex, kDontChargeCacheMetadata,
        24 /*max_upper_hash_bits*/, nullptr /*secondary_cache*/);
  }

  void Insert(const std::string& key,
              Cache::Priority priority = Cache::Priority::LOW) {
    EXPECT_OK(cache_->Insert(key, 0 /*hash*/, nullptr /*value*/, 1 /*charge*/,
                             nullptr /*deleter*/, nullptr /*handle*/,
                             priority));
  }

  void Insert(char key, Cache::Priority priority = Cache::Priority::LOW) {
    Insert(std::string(1, key), priority);
  }

  bool Lookup(const std::string& key) {
    auto handle = cache_->Lookup(key, 0 /*hash*/);
    if (handle) {
      cache_->Release(handle);
      return true;
    }
    return false;
  }

  bool Lookup(char key) { return Lookup(std::string(1, key)); }

  void Erase(const std::string& key) { cache_->Erase(key, 0 /*hash*/); }

  void ValidateLRUList(std::vector<std::string> keys,
                       size_t num_high_pri_pool_keys = 0) {
    LRUHandle* lru;
    LRUHandle* lru_low_pri;
    cache_->TEST_GetLRUList(&lru, &lru_low_pri);
    LRUHandle* iter = lru;
    bool in_high_pri_pool = false;
    size_t high_pri_pool_keys = 0;
    if (iter == lru_low_pri) {
      in_high_pri_pool = true;
    }
    for (const auto& key : keys) {
      iter = iter->next;
      ASSERT_NE(lru, iter);
      ASSERT_EQ(key, iter->key().ToString());
      ASSERT_EQ(in_high_pri_pool, iter->InHighPriPool());
      if (in_high_pri_pool) {
        high_pri_pool_keys++;
      }
      if (iter == lru_low_pri) {
        ASSERT_FALSE(in_high_pri_pool);
        in_high_pri_pool = true;
      }
    }
    ASSERT_EQ(lru, iter->next);
    ASSERT_TRUE(in_high_pri_pool);
    ASSERT_EQ(num_high_pri_pool_keys, high_pri_pool_keys);
  }

 private:
  LRUCacheShard* cache_ = nullptr;
};

TEST_F(LRUCacheTest, BasicLRU) {
  NewCache(5);
  for (char ch = 'a'; ch <= 'e'; ch++) {
    Insert(ch);
  }
  ValidateLRUList({"a", "b", "c", "d", "e"});
  for (char ch = 'x'; ch <= 'z'; ch++) {
    Insert(ch);
  }
  ValidateLRUList({"d", "e", "x", "y", "z"});
  ASSERT_FALSE(Lookup("b"));
  ValidateLRUList({"d", "e", "x", "y", "z"});
  ASSERT_TRUE(Lookup("e"));
  ValidateLRUList({"d", "x", "y", "z", "e"});
  ASSERT_TRUE(Lookup("z"));
  ValidateLRUList({"d", "x", "y", "e", "z"});
  Erase("x");
  ValidateLRUList({"d", "y", "e", "z"});
  ASSERT_TRUE(Lookup("d"));
  ValidateLRUList({"y", "e", "z", "d"});
  Insert("u");
  ValidateLRUList({"y", "e", "z", "d", "u"});
  Insert("v");
  ValidateLRUList({"e", "z", "d", "u", "v"});
}

TEST_F(LRUCacheTest, MidpointInsertion) {
  // Allocate 2 cache entries to high-pri pool.
  NewCache(5, 0.45);

  Insert("a", Cache::Priority::LOW);
  Insert("b", Cache::Priority::LOW);
  Insert("c", Cache::Priority::LOW);
  Insert("x", Cache::Priority::HIGH);
  Insert("y", Cache::Priority::HIGH);
  ValidateLRUList({"a", "b", "c", "x", "y"}, 2);

  // Low-pri entries inserted to the tail of low-pri list (the midpoint).
  // After lookup, it will move to the tail of the full list.
  Insert("d", Cache::Priority::LOW);
  ValidateLRUList({"b", "c", "d", "x", "y"}, 2);
  ASSERT_TRUE(Lookup("d"));
  ValidateLRUList({"b", "c", "x", "y", "d"}, 2);

  // High-pri entries will be inserted to the tail of full list.
  Insert("z", Cache::Priority::HIGH);
  ValidateLRUList({"c", "x", "y", "d", "z"}, 2);
}

TEST_F(LRUCacheTest, EntriesWithPriority) {
  // Allocate 2 cache entries to high-pri pool.
  NewCache(5, 0.45);

  Insert("a", Cache::Priority::LOW);
  Insert("b", Cache::Priority::LOW);
  Insert("c", Cache::Priority::LOW);
  ValidateLRUList({"a", "b", "c"}, 0);

  // Low-pri entries can take high-pri pool capacity if available
  Insert("u", Cache::Priority::LOW);
  Insert("v", Cache::Priority::LOW);
  ValidateLRUList({"a", "b", "c", "u", "v"}, 0);

  Insert("X", Cache::Priority::HIGH);
  Insert("Y", Cache::Priority::HIGH);
  ValidateLRUList({"c", "u", "v", "X", "Y"}, 2);

  // High-pri entries can overflow to low-pri pool.
  Insert("Z", Cache::Priority::HIGH);
  ValidateLRUList({"u", "v", "X", "Y", "Z"}, 2);

  // Low-pri entries will be inserted to head of low-pri pool.
  Insert("a", Cache::Priority::LOW);
  ValidateLRUList({"v", "X", "a", "Y", "Z"}, 2);

  // Low-pri entries will be inserted to head of high-pri pool after lookup.
  ASSERT_TRUE(Lookup("v"));
  ValidateLRUList({"X", "a", "Y", "Z", "v"}, 2);

  // High-pri entries will be inserted to the head of the list after lookup.
  ASSERT_TRUE(Lookup("X"));
  ValidateLRUList({"a", "Y", "Z", "v", "X"}, 2);
  ASSERT_TRUE(Lookup("Z"));
  ValidateLRUList({"a", "Y", "v", "X", "Z"}, 2);

  Erase("Y");
  ValidateLRUList({"a", "v", "X", "Z"}, 2);
  Erase("X");
  ValidateLRUList({"a", "v", "Z"}, 1);
  Insert("d", Cache::Priority::LOW);
  Insert("e", Cache::Priority::LOW);
  ValidateLRUList({"a", "v", "d", "e", "Z"}, 1);
  Insert("f", Cache::Priority::LOW);
  Insert("g", Cache::Priority::LOW);
  ValidateLRUList({"d", "e", "f", "g", "Z"}, 1);
  ASSERT_TRUE(Lookup("d"));
  ValidateLRUList({"e", "f", "g", "Z", "d"}, 2);
}

class TestSecondaryCache : public SecondaryCache {
 public:
  explicit TestSecondaryCache(size_t capacity)
      : num_inserts_(0), num_lookups_(0) {
    cache_ = NewLRUCache(capacity, 0, false, 0.5, nullptr,
                         kDefaultToAdaptiveMutex, kDontChargeCacheMetadata);
  }
  ~TestSecondaryCache() override { cache_.reset(); }

  std::string Name() override { return "TestSecondaryCache"; }

  Status Insert(const Slice& key, void* value,
                const Cache::CacheItemHelper* helper) override {
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
    return cache_->Insert(key, buf, size,
                          [](const Slice& /*key*/, void* val) -> void {
                            delete[] static_cast<char*>(val);
                          });
  }

  std::unique_ptr<SecondaryCacheHandle> Lookup(
      const Slice& key, const Cache::CreateCallback& create_cb,
      bool /*wait*/) override {
    std::unique_ptr<SecondaryCacheHandle> secondary_handle;
    Cache::Handle* handle = cache_->Lookup(key);
    num_lookups_++;
    if (handle) {
      void* value;
      size_t charge;
      char* ptr = (char*)cache_->Value(handle);
      size_t size = DecodeFixed64(ptr);
      ptr += sizeof(uint64_t);
      Status s = create_cb(ptr, size, &value, &charge);
      if (s.ok()) {
        secondary_handle.reset(
            new TestSecondaryCacheHandle(cache_.get(), handle, value, charge));
      } else {
        cache_->Release(handle);
      }
    }
    return secondary_handle;
  }

  void Erase(const Slice& /*key*/) override {}

  void WaitAll(std::vector<SecondaryCacheHandle*> /*handles*/) override {}

  std::string GetPrintableOptions() const override { return ""; }

  uint32_t num_inserts() { return num_inserts_; }

  uint32_t num_lookups() { return num_lookups_; }

 private:
  class TestSecondaryCacheHandle : public SecondaryCacheHandle {
   public:
    TestSecondaryCacheHandle(Cache* cache, Cache::Handle* handle, void* value,
                             size_t size)
        : cache_(cache), handle_(handle), value_(value), size_(size) {}
    ~TestSecondaryCacheHandle() override { cache_->Release(handle_); }

    bool IsReady() override { return true; }

    void Wait() override {}

    void* Value() override { return value_; }

    size_t Size() override { return size_; }

   private:
    Cache* cache_;
    Cache::Handle* handle_;
    void* value_;
    size_t size_;
  };

  std::shared_ptr<Cache> cache_;
  uint32_t num_inserts_;
  uint32_t num_lookups_;
};

class LRUSecondaryCacheTest : public LRUCacheTest {
 public:
  LRUSecondaryCacheTest() : fail_create_(false) {}
  ~LRUSecondaryCacheTest() {}

 protected:
  class TestItem {
   public:
    TestItem(const char* buf, size_t size) : buf_(new char[size]), size_(size) {
      memcpy(buf_.get(), buf, size);
    }
    ~TestItem() {}

    char* Buf() { return buf_.get(); }
    size_t Size() { return size_; }

   private:
    std::unique_ptr<char[]> buf_;
    size_t size_;
  };

  static size_t SizeCallback(void* obj) {
    return reinterpret_cast<TestItem*>(obj)->Size();
  }

  static Status SaveToCallback(void* obj, size_t offset, size_t size,
                               void* out) {
    TestItem* item = reinterpret_cast<TestItem*>(obj);
    char* buf = item->Buf();
    EXPECT_EQ(size, item->Size());
    EXPECT_EQ(offset, 0);
    memcpy(out, buf, size);
    return Status::OK();
  }

  static void DeletionCallback(const Slice& /*key*/, void* obj) {
    delete reinterpret_cast<TestItem*>(obj);
  }

  static Cache::CacheItemHelper helper_;

  static Status SaveToCallbackFail(void* /*obj*/, size_t /*offset*/,
                                   size_t /*size*/, void* /*out*/) {
    return Status::NotSupported();
  }

  static Cache::CacheItemHelper helper_fail_;

  Cache::CreateCallback test_item_creator =
      [&](void* buf, size_t size, void** out_obj, size_t* charge) -> Status {
    if (fail_create_) {
      return Status::NotSupported();
    }
    *out_obj = reinterpret_cast<void*>(new TestItem((char*)buf, size));
    *charge = size;
    return Status::OK();
  };

  void SetFailCreate(bool fail) { fail_create_ = fail; }

 private:
  bool fail_create_;
};

Cache::CacheItemHelper LRUSecondaryCacheTest::helper_(
    LRUSecondaryCacheTest::SizeCallback, LRUSecondaryCacheTest::SaveToCallback,
    LRUSecondaryCacheTest::DeletionCallback);

Cache::CacheItemHelper LRUSecondaryCacheTest::helper_fail_(
    LRUSecondaryCacheTest::SizeCallback,
    LRUSecondaryCacheTest::SaveToCallbackFail,
    LRUSecondaryCacheTest::DeletionCallback);

TEST_F(LRUSecondaryCacheTest, BasicTest) {
  LRUCacheOptions opts(1024, 0, false, 0.5, nullptr, kDefaultToAdaptiveMutex,
                       kDontChargeCacheMetadata);
  std::shared_ptr<TestSecondaryCache> secondary_cache =
      std::make_shared<TestSecondaryCache>(2048);
  opts.secondary_cache = secondary_cache;
  std::shared_ptr<Cache> cache = NewLRUCache(opts);

  Random rnd(301);
  std::string str1 = rnd.RandomString(1020);
  TestItem* item1 = new TestItem(str1.data(), str1.length());
  ASSERT_OK(cache->Insert("k1", item1, &LRUSecondaryCacheTest::helper_,
                          str1.length()));
  std::string str2 = rnd.RandomString(1020);
  TestItem* item2 = new TestItem(str2.data(), str2.length());
  // k2 should be demoted to NVM
  ASSERT_OK(cache->Insert("k2", item2, &LRUSecondaryCacheTest::helper_,
                          str2.length()));

  Cache::Handle* handle;
  handle = cache->Lookup("k2", &LRUSecondaryCacheTest::helper_,
                         test_item_creator, Cache::Priority::LOW, true);
  ASSERT_NE(handle, nullptr);
  cache->Release(handle);
  // This lookup should promote k1 and demote k2
  handle = cache->Lookup("k1", &LRUSecondaryCacheTest::helper_,
                         test_item_creator, Cache::Priority::LOW, true);
  ASSERT_NE(handle, nullptr);
  cache->Release(handle);
  ASSERT_EQ(secondary_cache->num_inserts(), 2u);
  ASSERT_EQ(secondary_cache->num_lookups(), 1u);

  cache.reset();
  secondary_cache.reset();
}

TEST_F(LRUSecondaryCacheTest, BasicFailTest) {
  LRUCacheOptions opts(1024, 0, false, 0.5, nullptr, kDefaultToAdaptiveMutex,
                       kDontChargeCacheMetadata);
  std::shared_ptr<TestSecondaryCache> secondary_cache =
      std::make_shared<TestSecondaryCache>(2048);
  opts.secondary_cache = secondary_cache;
  std::shared_ptr<Cache> cache = NewLRUCache(opts);

  Random rnd(301);
  std::string str1 = rnd.RandomString(1020);
  TestItem* item1 = new TestItem(str1.data(), str1.length());
  ASSERT_NOK(cache->Insert("k1", item1, nullptr, str1.length()));
  ASSERT_OK(cache->Insert("k1", item1, &LRUSecondaryCacheTest::helper_,
                          str1.length()));

  Cache::Handle* handle;
  handle = cache->Lookup("k2", nullptr, test_item_creator, Cache::Priority::LOW,
                         true);
  ASSERT_EQ(handle, nullptr);
  handle = cache->Lookup("k2", &LRUSecondaryCacheTest::helper_,
                         test_item_creator, Cache::Priority::LOW, false);
  ASSERT_EQ(handle, nullptr);

  cache.reset();
  secondary_cache.reset();
}

TEST_F(LRUSecondaryCacheTest, SaveFailTest) {
  LRUCacheOptions opts(1024, 0, false, 0.5, nullptr, kDefaultToAdaptiveMutex,
                       kDontChargeCacheMetadata);
  std::shared_ptr<TestSecondaryCache> secondary_cache =
      std::make_shared<TestSecondaryCache>(2048);
  opts.secondary_cache = secondary_cache;
  std::shared_ptr<Cache> cache = NewLRUCache(opts);

  Random rnd(301);
  std::string str1 = rnd.RandomString(1020);
  TestItem* item1 = new TestItem(str1.data(), str1.length());
  ASSERT_OK(cache->Insert("k1", item1, &LRUSecondaryCacheTest::helper_fail_,
                          str1.length()));
  std::string str2 = rnd.RandomString(1020);
  TestItem* item2 = new TestItem(str2.data(), str2.length());
  // k1 should be demoted to NVM
  ASSERT_OK(cache->Insert("k2", item2, &LRUSecondaryCacheTest::helper_fail_,
                          str2.length()));

  Cache::Handle* handle;
  handle = cache->Lookup("k2", &LRUSecondaryCacheTest::helper_fail_,
                         test_item_creator, Cache::Priority::LOW, true);
  ASSERT_NE(handle, nullptr);
  cache->Release(handle);
  // This lookup should fail, since k1 demotion would have failed
  handle = cache->Lookup("k1", &LRUSecondaryCacheTest::helper_fail_,
                         test_item_creator, Cache::Priority::LOW, true);
  ASSERT_EQ(handle, nullptr);
  // Since k1 didn't get promoted, k2 should still be in cache
  handle = cache->Lookup("k2", &LRUSecondaryCacheTest::helper_fail_,
                         test_item_creator, Cache::Priority::LOW, true);
  ASSERT_NE(handle, nullptr);
  cache->Release(handle);
  ASSERT_EQ(secondary_cache->num_inserts(), 1u);
  ASSERT_EQ(secondary_cache->num_lookups(), 1u);

  cache.reset();
  secondary_cache.reset();
}

TEST_F(LRUSecondaryCacheTest, CreateFailTest) {
  LRUCacheOptions opts(1024, 0, false, 0.5, nullptr, kDefaultToAdaptiveMutex,
                       kDontChargeCacheMetadata);
  std::shared_ptr<TestSecondaryCache> secondary_cache =
      std::make_shared<TestSecondaryCache>(2048);
  opts.secondary_cache = secondary_cache;
  std::shared_ptr<Cache> cache = NewLRUCache(opts);

  Random rnd(301);
  std::string str1 = rnd.RandomString(1020);
  TestItem* item1 = new TestItem(str1.data(), str1.length());
  ASSERT_OK(cache->Insert("k1", item1, &LRUSecondaryCacheTest::helper_,
                          str1.length()));
  std::string str2 = rnd.RandomString(1020);
  TestItem* item2 = new TestItem(str2.data(), str2.length());
  // k1 should be demoted to NVM
  ASSERT_OK(cache->Insert("k2", item2, &LRUSecondaryCacheTest::helper_,
                          str2.length()));

  Cache::Handle* handle;
  SetFailCreate(true);
  handle = cache->Lookup("k2", &LRUSecondaryCacheTest::helper_,
                         test_item_creator, Cache::Priority::LOW, true);
  ASSERT_NE(handle, nullptr);
  cache->Release(handle);
  // This lookup should fail, since k1 creation would have failed
  handle = cache->Lookup("k1", &LRUSecondaryCacheTest::helper_,
                         test_item_creator, Cache::Priority::LOW, true);
  ASSERT_EQ(handle, nullptr);
  // Since k1 didn't get promoted, k2 should still be in cache
  handle = cache->Lookup("k2", &LRUSecondaryCacheTest::helper_,
                         test_item_creator, Cache::Priority::LOW, true);
  ASSERT_NE(handle, nullptr);
  cache->Release(handle);
  ASSERT_EQ(secondary_cache->num_inserts(), 1u);
  ASSERT_EQ(secondary_cache->num_lookups(), 1u);

  cache.reset();
  secondary_cache.reset();
}

TEST_F(LRUSecondaryCacheTest, FullCapacityTest) {
  LRUCacheOptions opts(1024, 0, /*_strict_capacity_limit=*/true, 0.5, nullptr,
                       kDefaultToAdaptiveMutex, kDontChargeCacheMetadata);
  std::shared_ptr<TestSecondaryCache> secondary_cache =
      std::make_shared<TestSecondaryCache>(2048);
  opts.secondary_cache = secondary_cache;
  std::shared_ptr<Cache> cache = NewLRUCache(opts);

  Random rnd(301);
  std::string str1 = rnd.RandomString(1020);
  TestItem* item1 = new TestItem(str1.data(), str1.length());
  ASSERT_OK(cache->Insert("k1", item1, &LRUSecondaryCacheTest::helper_,
                          str1.length()));
  std::string str2 = rnd.RandomString(1020);
  TestItem* item2 = new TestItem(str2.data(), str2.length());
  // k1 should be demoted to NVM
  ASSERT_OK(cache->Insert("k2", item2, &LRUSecondaryCacheTest::helper_,
                          str2.length()));

  Cache::Handle* handle;
  handle = cache->Lookup("k2", &LRUSecondaryCacheTest::helper_,
                         test_item_creator, Cache::Priority::LOW, true);
  ASSERT_NE(handle, nullptr);
  // This lookup should fail, since k1 promotion would have failed due to
  // the block cache being at capacity
  Cache::Handle* handle2;
  handle2 = cache->Lookup("k1", &LRUSecondaryCacheTest::helper_,
                          test_item_creator, Cache::Priority::LOW, true);
  ASSERT_EQ(handle2, nullptr);
  // Since k1 didn't get promoted, k2 should still be in cache
  cache->Release(handle);
  handle = cache->Lookup("k2", &LRUSecondaryCacheTest::helper_,
                         test_item_creator, Cache::Priority::LOW, true);
  ASSERT_NE(handle, nullptr);
  cache->Release(handle);
  ASSERT_EQ(secondary_cache->num_inserts(), 1u);
  ASSERT_EQ(secondary_cache->num_lookups(), 1u);

  cache.reset();
  secondary_cache.reset();
}
}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

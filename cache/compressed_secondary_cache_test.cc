//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "cache/compressed_secondary_cache.h"

#include <algorithm>
#include <cstdint>

#include "memory/jemalloc_nodump_allocator.h"
#include "memory/memory_allocator.h"
#include "test_util/testharness.h"
#include "test_util/testutil.h"
#include "util/compression.h"
#include "util/random.h"

namespace ROCKSDB_NAMESPACE {

class CompressedSecondaryCacheTest : public testing::Test {
 public:
  CompressedSecondaryCacheTest() : fail_create_(false) {}
  ~CompressedSecondaryCacheTest() {}

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

  static Status SaveToCallback(void* from_obj, size_t from_offset,
                               size_t length, void* out) {
    TestItem* item = reinterpret_cast<TestItem*>(from_obj);
    const char* buf = item->Buf();
    EXPECT_EQ(length, item->Size());
    EXPECT_EQ(from_offset, 0);
    memcpy(out, buf, length);
    return Status::OK();
  }

  static void DeletionCallback(const Slice& /*key*/, void* obj) {
    delete reinterpret_cast<TestItem*>(obj);
    obj = nullptr;
  }

  static Cache::CacheItemHelper helper_;

  static Status SaveToCallbackFail(void* /*obj*/, size_t /*offset*/,
                                   size_t /*size*/, void* /*out*/) {
    return Status::NotSupported();
  }

  static Cache::CacheItemHelper helper_fail_;

  Cache::CreateCallback test_item_creator = [&](const void* buf, size_t size,
                                                void** out_obj,
                                                size_t* charge) -> Status {
    if (fail_create_) {
      return Status::NotSupported();
    }
    *out_obj = reinterpret_cast<void*>(new TestItem((char*)buf, size));
    *charge = size;
    return Status::OK();
  };

  void SetFailCreate(bool fail) { fail_create_ = fail; }

  void BasicTest(bool sec_cache_is_compressed, bool use_jemalloc) {
    CompressedSecondaryCacheOptions opts;
    opts.capacity = 2048;
    opts.num_shard_bits = 0;
    opts.metadata_charge_policy = kDontChargeCacheMetadata;

    if (sec_cache_is_compressed) {
      if (!LZ4_Supported()) {
        ROCKSDB_GTEST_SKIP("This test requires LZ4 support.");
        opts.compression_type = CompressionType::kNoCompression;
      }
    } else {
      opts.compression_type = CompressionType::kNoCompression;
    }

    if (use_jemalloc) {
      JemallocAllocatorOptions jopts;
      std::shared_ptr<MemoryAllocator> allocator;
      std::string msg;
      if (JemallocNodumpAllocator::IsSupported(&msg)) {
        Status s = NewJemallocNodumpAllocator(jopts, &allocator);
        if (s.ok()) {
          opts.memory_allocator = allocator;
        }
      } else {
        ROCKSDB_GTEST_BYPASS("JEMALLOC not supported");
      }
    }
    std::shared_ptr<SecondaryCache> sec_cache =
        NewCompressedSecondaryCache(opts);

    bool is_in_sec_cache{true};
    // Lookup an non-existent key.
    std::unique_ptr<SecondaryCacheResultHandle> handle0 =
        sec_cache->Lookup("k0", test_item_creator, true, is_in_sec_cache);
    ASSERT_EQ(handle0, nullptr);

    Random rnd(301);
    // Insert and Lookup the first item.
    std::string str1;
    test::CompressibleString(&rnd, 0.25, 1000, &str1);
    TestItem item1(str1.data(), str1.length());
    ASSERT_OK(sec_cache->Insert("k1", &item1,
                                &CompressedSecondaryCacheTest::helper_));

    std::unique_ptr<SecondaryCacheResultHandle> handle1 =
        sec_cache->Lookup("k1", test_item_creator, true, is_in_sec_cache);
    ASSERT_NE(handle1, nullptr);
    ASSERT_FALSE(is_in_sec_cache);

    std::unique_ptr<TestItem> val1 =
        std::unique_ptr<TestItem>(static_cast<TestItem*>(handle1->Value()));
    ASSERT_NE(val1, nullptr);
    ASSERT_EQ(memcmp(val1->Buf(), item1.Buf(), item1.Size()), 0);

    // Lookup the first item again.
    std::unique_ptr<SecondaryCacheResultHandle> handle1_1 =
        sec_cache->Lookup("k1", test_item_creator, true, is_in_sec_cache);
    ASSERT_EQ(handle1_1, nullptr);

    // Insert and Lookup the second item.
    std::string str2;
    test::CompressibleString(&rnd, 0.5, 1000, &str2);
    TestItem item2(str2.data(), str2.length());
    ASSERT_OK(sec_cache->Insert("k2", &item2,
                                &CompressedSecondaryCacheTest::helper_));
    std::unique_ptr<SecondaryCacheResultHandle> handle2 =
        sec_cache->Lookup("k2", test_item_creator, true, is_in_sec_cache);
    ASSERT_NE(handle2, nullptr);
    std::unique_ptr<TestItem> val2 =
        std::unique_ptr<TestItem>(static_cast<TestItem*>(handle2->Value()));
    ASSERT_NE(val2, nullptr);
    ASSERT_EQ(memcmp(val2->Buf(), item2.Buf(), item2.Size()), 0);

    std::vector<SecondaryCacheResultHandle*> handles = {handle1.get(),
                                                        handle2.get()};
    sec_cache->WaitAll(handles);

    sec_cache.reset();
  }

  void FailsTest(bool sec_cache_is_compressed) {
    CompressedSecondaryCacheOptions secondary_cache_opts;
    if (sec_cache_is_compressed) {
      if (!LZ4_Supported()) {
        ROCKSDB_GTEST_SKIP("This test requires LZ4 support.");
        secondary_cache_opts.compression_type = CompressionType::kNoCompression;
      }
    } else {
      secondary_cache_opts.compression_type = CompressionType::kNoCompression;
    }

    secondary_cache_opts.capacity = 1100;
    secondary_cache_opts.num_shard_bits = 0;
    secondary_cache_opts.metadata_charge_policy = kDontChargeCacheMetadata;
    std::shared_ptr<SecondaryCache> sec_cache =
        NewCompressedSecondaryCache(secondary_cache_opts);

    // Insert and Lookup the first item.
    Random rnd(301);
    std::string str1(rnd.RandomString(1000));
    TestItem item1(str1.data(), str1.length());
    ASSERT_OK(sec_cache->Insert("k1", &item1,
                                &CompressedSecondaryCacheTest::helper_));

    // Insert and Lookup the second item.
    std::string str2(rnd.RandomString(200));
    TestItem item2(str2.data(), str2.length());
    // k1 is evicted.
    ASSERT_OK(sec_cache->Insert("k2", &item2,
                                &CompressedSecondaryCacheTest::helper_));
    bool is_in_sec_cache{false};
    std::unique_ptr<SecondaryCacheResultHandle> handle1_1 =
        sec_cache->Lookup("k1", test_item_creator, true, is_in_sec_cache);
    ASSERT_EQ(handle1_1, nullptr);
    std::unique_ptr<SecondaryCacheResultHandle> handle2 =
        sec_cache->Lookup("k2", test_item_creator, true, is_in_sec_cache);
    ASSERT_NE(handle2, nullptr);
    std::unique_ptr<TestItem> val2 =
        std::unique_ptr<TestItem>(static_cast<TestItem*>(handle2->Value()));
    ASSERT_NE(val2, nullptr);
    ASSERT_EQ(memcmp(val2->Buf(), item2.Buf(), item2.Size()), 0);

    // Create Fails.
    SetFailCreate(true);
    std::unique_ptr<SecondaryCacheResultHandle> handle2_1 =
        sec_cache->Lookup("k2", test_item_creator, true, is_in_sec_cache);
    ASSERT_EQ(handle2_1, nullptr);

    // Save Fails.
    std::string str3 = rnd.RandomString(10);
    TestItem item3(str3.data(), str3.length());
    ASSERT_NOK(sec_cache->Insert("k3", &item3,
                                 &CompressedSecondaryCacheTest::helper_fail_));

    sec_cache.reset();
  }

  void BasicIntegrationTest(bool sec_cache_is_compressed) {
    CompressedSecondaryCacheOptions secondary_cache_opts;

    if (sec_cache_is_compressed) {
      if (!LZ4_Supported()) {
        ROCKSDB_GTEST_SKIP("This test requires LZ4 support.");
        secondary_cache_opts.compression_type = CompressionType::kNoCompression;
      }
    } else {
      secondary_cache_opts.compression_type = CompressionType::kNoCompression;
    }

    secondary_cache_opts.capacity = 2300;
    secondary_cache_opts.num_shard_bits = 0;
    secondary_cache_opts.metadata_charge_policy = kDontChargeCacheMetadata;
    std::shared_ptr<SecondaryCache> secondary_cache =
        NewCompressedSecondaryCache(secondary_cache_opts);
    LRUCacheOptions lru_cache_opts(1024, 0, false, 0.5, nullptr,
                                   kDefaultToAdaptiveMutex,
                                   kDontChargeCacheMetadata);
    lru_cache_opts.secondary_cache = secondary_cache;
    std::shared_ptr<Cache> cache = NewLRUCache(lru_cache_opts);
    std::shared_ptr<Statistics> stats = CreateDBStatistics();

    Random rnd(301);

    std::string str1 = rnd.RandomString(1010);
    std::string str1_clone{str1};
    TestItem* item1 = new TestItem(str1.data(), str1.length());
    ASSERT_OK(cache->Insert("k1", item1, &CompressedSecondaryCacheTest::helper_,
                            str1.length()));

    std::string str2 = rnd.RandomString(1020);
    TestItem* item2 = new TestItem(str2.data(), str2.length());
    // After Insert, lru cache contains k2 and secondary cache contains k1.
    ASSERT_OK(cache->Insert("k2", item2, &CompressedSecondaryCacheTest::helper_,
                            str2.length()));

    std::string str3 = rnd.RandomString(1020);
    TestItem* item3 = new TestItem(str3.data(), str3.length());
    // After Insert, lru cache contains k3 and secondary cache contains k1 and
    // k2
    ASSERT_OK(cache->Insert("k3", item3, &CompressedSecondaryCacheTest::helper_,
                            str3.length()));

    Cache::Handle* handle;
    handle = cache->Lookup("k3", &CompressedSecondaryCacheTest::helper_,
                           test_item_creator, Cache::Priority::LOW, true,
                           stats.get());
    ASSERT_NE(handle, nullptr);
    TestItem* val3 = static_cast<TestItem*>(cache->Value(handle));
    ASSERT_NE(val3, nullptr);
    ASSERT_EQ(memcmp(val3->Buf(), item3->Buf(), item3->Size()), 0);
    cache->Release(handle);

    // Lookup an non-existent key.
    handle = cache->Lookup("k0", &CompressedSecondaryCacheTest::helper_,
                           test_item_creator, Cache::Priority::LOW, true,
                           stats.get());
    ASSERT_EQ(handle, nullptr);

    // This Lookup should promote k1 and erase k1 from the secondary cache,
    // then k3 is demoted. So k2 and k3 are in the secondary cache.
    handle = cache->Lookup("k1", &CompressedSecondaryCacheTest::helper_,
                           test_item_creator, Cache::Priority::LOW, true,
                           stats.get());

    ASSERT_NE(handle, nullptr);
    TestItem* val1_1 = static_cast<TestItem*>(cache->Value(handle));
    ASSERT_NE(val1_1, nullptr);
    ASSERT_EQ(memcmp(val1_1->Buf(), str1_clone.data(), str1_clone.size()), 0);
    cache->Release(handle);

    handle = cache->Lookup("k2", &CompressedSecondaryCacheTest::helper_,
                           test_item_creator, Cache::Priority::LOW, true,
                           stats.get());
    ASSERT_NE(handle, nullptr);
    cache->Release(handle);

    cache.reset();
    secondary_cache.reset();
  }

  void BasicIntegrationFailTest(bool sec_cache_is_compressed) {
    CompressedSecondaryCacheOptions secondary_cache_opts;

    if (sec_cache_is_compressed) {
      if (!LZ4_Supported()) {
        ROCKSDB_GTEST_SKIP("This test requires LZ4 support.");
        secondary_cache_opts.compression_type = CompressionType::kNoCompression;
      }
    } else {
      secondary_cache_opts.compression_type = CompressionType::kNoCompression;
    }

    secondary_cache_opts.capacity = 2048;
    secondary_cache_opts.num_shard_bits = 0;
    secondary_cache_opts.metadata_charge_policy = kDontChargeCacheMetadata;
    std::shared_ptr<SecondaryCache> secondary_cache =
        NewCompressedSecondaryCache(secondary_cache_opts);

    LRUCacheOptions opts(1024, 0, false, 0.5, nullptr, kDefaultToAdaptiveMutex,
                         kDontChargeCacheMetadata);
    opts.secondary_cache = secondary_cache;
    std::shared_ptr<Cache> cache = NewLRUCache(opts);

    Random rnd(301);
    std::string str1 = rnd.RandomString(1020);
    auto item1 =
        std::unique_ptr<TestItem>(new TestItem(str1.data(), str1.length()));
    ASSERT_NOK(cache->Insert("k1", item1.get(), nullptr, str1.length()));
    ASSERT_OK(cache->Insert("k1", item1.get(),
                            &CompressedSecondaryCacheTest::helper_,
                            str1.length()));
    item1.release();  // Appease clang-analyze "potential memory leak"

    Cache::Handle* handle;
    handle = cache->Lookup("k2", nullptr, test_item_creator,
                           Cache::Priority::LOW, true);
    ASSERT_EQ(handle, nullptr);
    handle = cache->Lookup("k2", &CompressedSecondaryCacheTest::helper_,
                           test_item_creator, Cache::Priority::LOW, false);
    ASSERT_EQ(handle, nullptr);

    cache.reset();
    secondary_cache.reset();
  }

  void IntegrationSaveFailTest(bool sec_cache_is_compressed) {
    CompressedSecondaryCacheOptions secondary_cache_opts;

    if (sec_cache_is_compressed) {
      if (!LZ4_Supported()) {
        ROCKSDB_GTEST_SKIP("This test requires LZ4 support.");
        secondary_cache_opts.compression_type = CompressionType::kNoCompression;
      }
    } else {
      secondary_cache_opts.compression_type = CompressionType::kNoCompression;
    }

    secondary_cache_opts.capacity = 2048;
    secondary_cache_opts.num_shard_bits = 0;
    secondary_cache_opts.metadata_charge_policy = kDontChargeCacheMetadata;

    std::shared_ptr<SecondaryCache> secondary_cache =
        NewCompressedSecondaryCache(secondary_cache_opts);

    LRUCacheOptions opts(1024, 0, false, 0.5, nullptr, kDefaultToAdaptiveMutex,
                         kDontChargeCacheMetadata);
    opts.secondary_cache = secondary_cache;
    std::shared_ptr<Cache> cache = NewLRUCache(opts);

    Random rnd(301);
    std::string str1 = rnd.RandomString(1020);
    TestItem* item1 = new TestItem(str1.data(), str1.length());
    ASSERT_OK(cache->Insert("k1", item1,
                            &CompressedSecondaryCacheTest::helper_fail_,
                            str1.length()));
    std::string str2 = rnd.RandomString(1020);
    TestItem* item2 = new TestItem(str2.data(), str2.length());
    // k1 should be demoted to the secondary cache.
    ASSERT_OK(cache->Insert("k2", item2,
                            &CompressedSecondaryCacheTest::helper_fail_,
                            str2.length()));

    Cache::Handle* handle;
    handle = cache->Lookup("k2", &CompressedSecondaryCacheTest::helper_fail_,
                           test_item_creator, Cache::Priority::LOW, true);
    ASSERT_NE(handle, nullptr);
    cache->Release(handle);
    // This lookup should fail, since k1 demotion would have failed
    handle = cache->Lookup("k1", &CompressedSecondaryCacheTest::helper_fail_,
                           test_item_creator, Cache::Priority::LOW, true);
    ASSERT_EQ(handle, nullptr);
    // Since k1 didn't get promoted, k2 should still be in cache
    handle = cache->Lookup("k2", &CompressedSecondaryCacheTest::helper_fail_,
                           test_item_creator, Cache::Priority::LOW, true);
    ASSERT_NE(handle, nullptr);
    cache->Release(handle);

    cache.reset();
    secondary_cache.reset();
  }

  void IntegrationCreateFailTest(bool sec_cache_is_compressed) {
    CompressedSecondaryCacheOptions secondary_cache_opts;

    if (sec_cache_is_compressed) {
      if (!LZ4_Supported()) {
        ROCKSDB_GTEST_SKIP("This test requires LZ4 support.");
        secondary_cache_opts.compression_type = CompressionType::kNoCompression;
      }
    } else {
      secondary_cache_opts.compression_type = CompressionType::kNoCompression;
    }

    secondary_cache_opts.capacity = 2048;
    secondary_cache_opts.num_shard_bits = 0;
    secondary_cache_opts.metadata_charge_policy = kDontChargeCacheMetadata;

    std::shared_ptr<SecondaryCache> secondary_cache =
        NewCompressedSecondaryCache(secondary_cache_opts);

    LRUCacheOptions opts(1024, 0, false, 0.5, nullptr, kDefaultToAdaptiveMutex,
                         kDontChargeCacheMetadata);
    opts.secondary_cache = secondary_cache;
    std::shared_ptr<Cache> cache = NewLRUCache(opts);

    Random rnd(301);
    std::string str1 = rnd.RandomString(1020);
    TestItem* item1 = new TestItem(str1.data(), str1.length());
    ASSERT_OK(cache->Insert("k1", item1, &CompressedSecondaryCacheTest::helper_,
                            str1.length()));

    std::string str2 = rnd.RandomString(1020);
    TestItem* item2 = new TestItem(str2.data(), str2.length());
    // k1 should be demoted to the secondary cache.
    ASSERT_OK(cache->Insert("k2", item2, &CompressedSecondaryCacheTest::helper_,
                            str2.length()));

    Cache::Handle* handle;
    SetFailCreate(true);
    handle = cache->Lookup("k2", &CompressedSecondaryCacheTest::helper_,
                           test_item_creator, Cache::Priority::LOW, true);
    ASSERT_NE(handle, nullptr);
    cache->Release(handle);
    // This lookup should fail, since k1 creation would have failed
    handle = cache->Lookup("k1", &CompressedSecondaryCacheTest::helper_,
                           test_item_creator, Cache::Priority::LOW, true);
    ASSERT_EQ(handle, nullptr);
    // Since k1 didn't get promoted, k2 should still be in cache
    handle = cache->Lookup("k2", &CompressedSecondaryCacheTest::helper_,
                           test_item_creator, Cache::Priority::LOW, true);
    ASSERT_NE(handle, nullptr);
    cache->Release(handle);

    cache.reset();
    secondary_cache.reset();
  }

  void IntegrationFullCapacityTest(bool sec_cache_is_compressed) {
    CompressedSecondaryCacheOptions secondary_cache_opts;

    if (sec_cache_is_compressed) {
      if (!LZ4_Supported()) {
        ROCKSDB_GTEST_SKIP("This test requires LZ4 support.");
        secondary_cache_opts.compression_type = CompressionType::kNoCompression;
      }
    } else {
      secondary_cache_opts.compression_type = CompressionType::kNoCompression;
    }

    secondary_cache_opts.capacity = 2048;
    secondary_cache_opts.num_shard_bits = 0;
    secondary_cache_opts.metadata_charge_policy = kDontChargeCacheMetadata;

    std::shared_ptr<SecondaryCache> secondary_cache =
        NewCompressedSecondaryCache(secondary_cache_opts);

    LRUCacheOptions opts(1024, 0, /*_strict_capacity_limit=*/true, 0.5, nullptr,
                         kDefaultToAdaptiveMutex, kDontChargeCacheMetadata);
    opts.secondary_cache = secondary_cache;
    std::shared_ptr<Cache> cache = NewLRUCache(opts);

    Random rnd(301);
    std::string str1 = rnd.RandomString(1020);
    TestItem* item1 = new TestItem(str1.data(), str1.length());
    ASSERT_OK(cache->Insert("k1", item1, &CompressedSecondaryCacheTest::helper_,
                            str1.length()));
    std::string str2 = rnd.RandomString(1020);
    TestItem* item2 = new TestItem(str2.data(), str2.length());
    // k1 should be demoted to the secondary cache.
    ASSERT_OK(cache->Insert("k2", item2, &CompressedSecondaryCacheTest::helper_,
                            str2.length()));

    Cache::Handle* handle2;
    handle2 = cache->Lookup("k2", &CompressedSecondaryCacheTest::helper_,
                            test_item_creator, Cache::Priority::LOW, true);
    ASSERT_NE(handle2, nullptr);
    cache->Release(handle2);
    // k1 promotion should fail due to the block cache being at capacity,
    // but the lookup should still succeed
    Cache::Handle* handle1;
    handle1 = cache->Lookup("k1", &CompressedSecondaryCacheTest::helper_,
                            test_item_creator, Cache::Priority::LOW, true);
    ASSERT_NE(handle1, nullptr);
    cache->Release(handle1);

    // Since k1 didn't get inserted, k2 should still be in cache
    handle2 = cache->Lookup("k2", &CompressedSecondaryCacheTest::helper_,
                            test_item_creator, Cache::Priority::LOW, true);
    ASSERT_NE(handle2, nullptr);
    cache->Release(handle2);

    cache.reset();
    secondary_cache.reset();
  }

 private:
  bool fail_create_;
};

Cache::CacheItemHelper CompressedSecondaryCacheTest::helper_(
    CompressedSecondaryCacheTest::SizeCallback,
    CompressedSecondaryCacheTest::SaveToCallback,
    CompressedSecondaryCacheTest::DeletionCallback);

Cache::CacheItemHelper CompressedSecondaryCacheTest::helper_fail_(
    CompressedSecondaryCacheTest::SizeCallback,
    CompressedSecondaryCacheTest::SaveToCallbackFail,
    CompressedSecondaryCacheTest::DeletionCallback);

TEST_F(CompressedSecondaryCacheTest, BasicTestWithNoCompression) {
  BasicTest(false, false);
}

TEST_F(CompressedSecondaryCacheTest,
       BasicTestWithMemoryAllocatorAndNoCompression) {
  BasicTest(false, true);
}

TEST_F(CompressedSecondaryCacheTest, BasicTestWithCompression) {
  BasicTest(true, false);
}

TEST_F(CompressedSecondaryCacheTest,
       BasicTestWithMemoryAllocatorAndCompression) {
  BasicTest(true, true);
}

TEST_F(CompressedSecondaryCacheTest, FailsTestWithNoCompression) {
  FailsTest(false);
}

TEST_F(CompressedSecondaryCacheTest, FailsTestWithCompression) {
  FailsTest(true);
}

TEST_F(CompressedSecondaryCacheTest, BasicIntegrationTestWithNoCompression) {
  BasicIntegrationTest(false);
}

TEST_F(CompressedSecondaryCacheTest, BasicIntegrationTestWithCompression) {
  BasicIntegrationTest(true);
}

TEST_F(CompressedSecondaryCacheTest,
       BasicIntegrationFailTestWithNoCompression) {
  BasicIntegrationFailTest(false);
}

TEST_F(CompressedSecondaryCacheTest, BasicIntegrationFailTestWithCompression) {
  BasicIntegrationFailTest(true);
}

TEST_F(CompressedSecondaryCacheTest, IntegrationSaveFailTestWithNoCompression) {
  IntegrationSaveFailTest(false);
}

TEST_F(CompressedSecondaryCacheTest, IntegrationSaveFailTestWithCompression) {
  IntegrationSaveFailTest(true);
}

TEST_F(CompressedSecondaryCacheTest,
       IntegrationCreateFailTestWithNoCompression) {
  IntegrationCreateFailTest(false);
}

TEST_F(CompressedSecondaryCacheTest, IntegrationCreateFailTestWithCompression) {
  IntegrationCreateFailTest(true);
}

TEST_F(CompressedSecondaryCacheTest,
       IntegrationFullCapacityTestWithNoCompression) {
  IntegrationFullCapacityTest(false);
}

TEST_F(CompressedSecondaryCacheTest,
       IntegrationFullCapacityTestWithCompression) {
  IntegrationFullCapacityTest(true);
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

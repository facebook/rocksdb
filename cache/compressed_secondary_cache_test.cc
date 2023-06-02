//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "cache/compressed_secondary_cache.h"

#include <array>
#include <iterator>
#include <memory>
#include <tuple>

#include "cache/secondary_cache_adapter.h"
#include "memory/jemalloc_nodump_allocator.h"
#include "rocksdb/convenience.h"
#include "test_util/secondary_cache_test_util.h"
#include "test_util/testharness.h"
#include "test_util/testutil.h"
#include "util/cast_util.h"

namespace ROCKSDB_NAMESPACE {

using secondary_cache_test_util::GetTestingCacheTypes;
using secondary_cache_test_util::WithCacheType;

// 16 bytes for HCC compatibility
const std::string key0 = "____    ____key0";
const std::string key1 = "____    ____key1";
const std::string key2 = "____    ____key2";
const std::string key3 = "____    ____key3";

class CompressedSecondaryCacheTestBase : public testing::Test,
                                         public WithCacheType {
 public:
  CompressedSecondaryCacheTestBase() {}
  ~CompressedSecondaryCacheTestBase() override = default;

 protected:
  void BasicTestHelper(std::shared_ptr<SecondaryCache> sec_cache,
                       bool sec_cache_is_compressed) {
    get_perf_context()->Reset();
    bool kept_in_sec_cache{true};
    // Lookup an non-existent key.
    std::unique_ptr<SecondaryCacheResultHandle> handle0 =
        sec_cache->Lookup(key0, GetHelper(), this, true, /*advise_erase=*/true,
                          kept_in_sec_cache);
    ASSERT_EQ(handle0, nullptr);

    Random rnd(301);
    // Insert and Lookup the item k1 for the first time.
    std::string str1(rnd.RandomString(1000));
    TestItem item1(str1.data(), str1.length());
    // A dummy handle is inserted if the item is inserted for the first time.
    ASSERT_OK(sec_cache->Insert(key1, &item1, GetHelper()));
    ASSERT_EQ(get_perf_context()->compressed_sec_cache_insert_dummy_count, 1);
    ASSERT_EQ(get_perf_context()->compressed_sec_cache_uncompressed_bytes, 0);
    ASSERT_EQ(get_perf_context()->compressed_sec_cache_compressed_bytes, 0);

    std::unique_ptr<SecondaryCacheResultHandle> handle1_1 =
        sec_cache->Lookup(key1, GetHelper(), this, true, /*advise_erase=*/false,
                          kept_in_sec_cache);
    ASSERT_EQ(handle1_1, nullptr);

    // Insert and Lookup the item k1 for the second time and advise erasing it.
    ASSERT_OK(sec_cache->Insert(key1, &item1, GetHelper()));
    ASSERT_EQ(get_perf_context()->compressed_sec_cache_insert_real_count, 1);

    std::unique_ptr<SecondaryCacheResultHandle> handle1_2 =
        sec_cache->Lookup(key1, GetHelper(), this, true, /*advise_erase=*/true,
                          kept_in_sec_cache);
    ASSERT_NE(handle1_2, nullptr);
    ASSERT_FALSE(kept_in_sec_cache);
    if (sec_cache_is_compressed) {
      ASSERT_EQ(get_perf_context()->compressed_sec_cache_uncompressed_bytes,
                1000);
      ASSERT_EQ(get_perf_context()->compressed_sec_cache_compressed_bytes,
                1007);
    } else {
      ASSERT_EQ(get_perf_context()->compressed_sec_cache_uncompressed_bytes, 0);
      ASSERT_EQ(get_perf_context()->compressed_sec_cache_compressed_bytes, 0);
    }

    std::unique_ptr<TestItem> val1 =
        std::unique_ptr<TestItem>(static_cast<TestItem*>(handle1_2->Value()));
    ASSERT_NE(val1, nullptr);
    ASSERT_EQ(memcmp(val1->Buf(), item1.Buf(), item1.Size()), 0);

    // Lookup the item k1 again.
    std::unique_ptr<SecondaryCacheResultHandle> handle1_3 =
        sec_cache->Lookup(key1, GetHelper(), this, true, /*advise_erase=*/true,
                          kept_in_sec_cache);
    ASSERT_EQ(handle1_3, nullptr);

    // Insert and Lookup the item k2.
    std::string str2(rnd.RandomString(1000));
    TestItem item2(str2.data(), str2.length());
    ASSERT_OK(sec_cache->Insert(key2, &item2, GetHelper()));
    ASSERT_EQ(get_perf_context()->compressed_sec_cache_insert_dummy_count, 2);
    std::unique_ptr<SecondaryCacheResultHandle> handle2_1 =
        sec_cache->Lookup(key2, GetHelper(), this, true, /*advise_erase=*/false,
                          kept_in_sec_cache);
    ASSERT_EQ(handle2_1, nullptr);

    ASSERT_OK(sec_cache->Insert(key2, &item2, GetHelper()));
    ASSERT_EQ(get_perf_context()->compressed_sec_cache_insert_real_count, 2);
    if (sec_cache_is_compressed) {
      ASSERT_EQ(get_perf_context()->compressed_sec_cache_uncompressed_bytes,
                2000);
      ASSERT_EQ(get_perf_context()->compressed_sec_cache_compressed_bytes,
                2014);
    } else {
      ASSERT_EQ(get_perf_context()->compressed_sec_cache_uncompressed_bytes, 0);
      ASSERT_EQ(get_perf_context()->compressed_sec_cache_compressed_bytes, 0);
    }
    std::unique_ptr<SecondaryCacheResultHandle> handle2_2 =
        sec_cache->Lookup(key2, GetHelper(), this, true, /*advise_erase=*/false,
                          kept_in_sec_cache);
    ASSERT_NE(handle2_2, nullptr);
    std::unique_ptr<TestItem> val2 =
        std::unique_ptr<TestItem>(static_cast<TestItem*>(handle2_2->Value()));
    ASSERT_NE(val2, nullptr);
    ASSERT_EQ(memcmp(val2->Buf(), item2.Buf(), item2.Size()), 0);

    std::vector<SecondaryCacheResultHandle*> handles = {handle1_2.get(),
                                                        handle2_2.get()};
    sec_cache->WaitAll(handles);

    sec_cache.reset();
  }

  void BasicTest(bool sec_cache_is_compressed, bool use_jemalloc) {
    CompressedSecondaryCacheOptions opts;
    opts.capacity = 2048;
    opts.num_shard_bits = 0;

    if (sec_cache_is_compressed) {
      if (!LZ4_Supported()) {
        ROCKSDB_GTEST_SKIP("This test requires LZ4 support.");
        opts.compression_type = CompressionType::kNoCompression;
        sec_cache_is_compressed = false;
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

    BasicTestHelper(sec_cache, sec_cache_is_compressed);
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
    std::shared_ptr<SecondaryCache> sec_cache =
        NewCompressedSecondaryCache(secondary_cache_opts);

    // Insert and Lookup the first item.
    Random rnd(301);
    std::string str1(rnd.RandomString(1000));
    TestItem item1(str1.data(), str1.length());
    // Insert a dummy handle.
    ASSERT_OK(sec_cache->Insert(key1, &item1, GetHelper()));
    // Insert k1.
    ASSERT_OK(sec_cache->Insert(key1, &item1, GetHelper()));

    // Insert and Lookup the second item.
    std::string str2(rnd.RandomString(200));
    TestItem item2(str2.data(), str2.length());
    // Insert a dummy handle, k1 is not evicted.
    ASSERT_OK(sec_cache->Insert(key2, &item2, GetHelper()));
    bool kept_in_sec_cache{false};
    std::unique_ptr<SecondaryCacheResultHandle> handle1 =
        sec_cache->Lookup(key1, GetHelper(), this, true, /*advise_erase=*/false,
                          kept_in_sec_cache);
    ASSERT_EQ(handle1, nullptr);

    // Insert k2 and k1 is evicted.
    ASSERT_OK(sec_cache->Insert(key2, &item2, GetHelper()));
    std::unique_ptr<SecondaryCacheResultHandle> handle2 =
        sec_cache->Lookup(key2, GetHelper(), this, true, /*advise_erase=*/false,
                          kept_in_sec_cache);
    ASSERT_NE(handle2, nullptr);
    std::unique_ptr<TestItem> val2 =
        std::unique_ptr<TestItem>(static_cast<TestItem*>(handle2->Value()));
    ASSERT_NE(val2, nullptr);
    ASSERT_EQ(memcmp(val2->Buf(), item2.Buf(), item2.Size()), 0);

    // Insert k1 again and a dummy handle is inserted.
    ASSERT_OK(sec_cache->Insert(key1, &item1, GetHelper()));

    std::unique_ptr<SecondaryCacheResultHandle> handle1_1 =
        sec_cache->Lookup(key1, GetHelper(), this, true, /*advise_erase=*/false,
                          kept_in_sec_cache);
    ASSERT_EQ(handle1_1, nullptr);

    // Create Fails.
    SetFailCreate(true);
    std::unique_ptr<SecondaryCacheResultHandle> handle2_1 =
        sec_cache->Lookup(key2, GetHelper(), this, true, /*advise_erase=*/true,
                          kept_in_sec_cache);
    ASSERT_EQ(handle2_1, nullptr);

    // Save Fails.
    std::string str3 = rnd.RandomString(10);
    TestItem item3(str3.data(), str3.length());
    // The Status is OK because a dummy handle is inserted.
    ASSERT_OK(sec_cache->Insert(key3, &item3, GetHelperFail()));
    ASSERT_NOK(sec_cache->Insert(key3, &item3, GetHelperFail()));

    sec_cache.reset();
  }

  void BasicIntegrationTest(bool sec_cache_is_compressed,
                            bool enable_custom_split_merge) {
    CompressedSecondaryCacheOptions secondary_cache_opts;

    if (sec_cache_is_compressed) {
      if (!LZ4_Supported()) {
        ROCKSDB_GTEST_SKIP("This test requires LZ4 support.");
        secondary_cache_opts.compression_type = CompressionType::kNoCompression;
        sec_cache_is_compressed = false;
      }
    } else {
      secondary_cache_opts.compression_type = CompressionType::kNoCompression;
    }

    secondary_cache_opts.capacity = 6000;
    secondary_cache_opts.num_shard_bits = 0;
    secondary_cache_opts.enable_custom_split_merge = enable_custom_split_merge;
    std::shared_ptr<SecondaryCache> secondary_cache =
        NewCompressedSecondaryCache(secondary_cache_opts);
    std::shared_ptr<Cache> cache = NewCache(
        /*_capacity =*/1300, /*_num_shard_bits =*/0,
        /*_strict_capacity_limit =*/true, secondary_cache);
    std::shared_ptr<Statistics> stats = CreateDBStatistics();

    get_perf_context()->Reset();
    Random rnd(301);
    std::string str1 = rnd.RandomString(1001);
    auto item1_1 = new TestItem(str1.data(), str1.length());
    ASSERT_OK(cache->Insert(key1, item1_1, GetHelper(), str1.length()));

    std::string str2 = rnd.RandomString(1012);
    auto item2_1 = new TestItem(str2.data(), str2.length());
    // After this Insert, primary cache contains k2 and secondary cache contains
    // k1's dummy item.
    ASSERT_OK(cache->Insert(key2, item2_1, GetHelper(), str2.length()));
    ASSERT_EQ(get_perf_context()->compressed_sec_cache_insert_dummy_count, 1);
    ASSERT_EQ(get_perf_context()->compressed_sec_cache_uncompressed_bytes, 0);
    ASSERT_EQ(get_perf_context()->compressed_sec_cache_compressed_bytes, 0);

    std::string str3 = rnd.RandomString(1024);
    auto item3_1 = new TestItem(str3.data(), str3.length());
    // After this Insert, primary cache contains k3 and secondary cache contains
    // k1's dummy item and k2's dummy item.
    ASSERT_OK(cache->Insert(key3, item3_1, GetHelper(), str3.length()));
    ASSERT_EQ(get_perf_context()->compressed_sec_cache_insert_dummy_count, 2);

    // After this Insert, primary cache contains k1 and secondary cache contains
    // k1's dummy item, k2's dummy item, and k3's dummy item.
    auto item1_2 = new TestItem(str1.data(), str1.length());
    ASSERT_OK(cache->Insert(key1, item1_2, GetHelper(), str1.length()));
    ASSERT_EQ(get_perf_context()->compressed_sec_cache_insert_dummy_count, 3);

    // After this Insert, primary cache contains k2 and secondary cache contains
    // k1's item, k2's dummy item, and k3's dummy item.
    auto item2_2 = new TestItem(str2.data(), str2.length());
    ASSERT_OK(cache->Insert(key2, item2_2, GetHelper(), str2.length()));
    ASSERT_EQ(get_perf_context()->compressed_sec_cache_insert_real_count, 1);
    if (sec_cache_is_compressed) {
      ASSERT_EQ(get_perf_context()->compressed_sec_cache_uncompressed_bytes,
                str1.length());
      ASSERT_EQ(get_perf_context()->compressed_sec_cache_compressed_bytes,
                1008);
    } else {
      ASSERT_EQ(get_perf_context()->compressed_sec_cache_uncompressed_bytes, 0);
      ASSERT_EQ(get_perf_context()->compressed_sec_cache_compressed_bytes, 0);
    }

    // After this Insert, primary cache contains k3 and secondary cache contains
    // k1's item and k2's item.
    auto item3_2 = new TestItem(str3.data(), str3.length());
    ASSERT_OK(cache->Insert(key3, item3_2, GetHelper(), str3.length()));
    ASSERT_EQ(get_perf_context()->compressed_sec_cache_insert_real_count, 2);
    if (sec_cache_is_compressed) {
      ASSERT_EQ(get_perf_context()->compressed_sec_cache_uncompressed_bytes,
                str1.length() + str2.length());
      ASSERT_EQ(get_perf_context()->compressed_sec_cache_compressed_bytes,
                2027);
    } else {
      ASSERT_EQ(get_perf_context()->compressed_sec_cache_uncompressed_bytes, 0);
      ASSERT_EQ(get_perf_context()->compressed_sec_cache_compressed_bytes, 0);
    }

    Cache::Handle* handle;
    handle = cache->Lookup(key3, GetHelper(), this, Cache::Priority::LOW,
                           stats.get());
    ASSERT_NE(handle, nullptr);
    auto val3 = static_cast<TestItem*>(cache->Value(handle));
    ASSERT_NE(val3, nullptr);
    ASSERT_EQ(memcmp(val3->Buf(), item3_2->Buf(), item3_2->Size()), 0);
    cache->Release(handle);

    // Lookup an non-existent key.
    handle = cache->Lookup(key0, GetHelper(), this, Cache::Priority::LOW,
                           stats.get());
    ASSERT_EQ(handle, nullptr);

    // This Lookup should just insert a dummy handle in the primary cache
    // and the k1 is still in the secondary cache.
    handle = cache->Lookup(key1, GetHelper(), this, Cache::Priority::LOW,
                           stats.get());
    ASSERT_NE(handle, nullptr);
    ASSERT_EQ(get_perf_context()->block_cache_standalone_handle_count, 1);
    auto val1_1 = static_cast<TestItem*>(cache->Value(handle));
    ASSERT_NE(val1_1, nullptr);
    ASSERT_EQ(memcmp(val1_1->Buf(), str1.data(), str1.size()), 0);
    cache->Release(handle);

    // This Lookup should erase k1 from the secondary cache and insert
    // it into primary cache; then k3 is demoted.
    // k2 and k3 are in secondary cache.
    handle = cache->Lookup(key1, GetHelper(), this, Cache::Priority::LOW,
                           stats.get());
    ASSERT_NE(handle, nullptr);
    ASSERT_EQ(get_perf_context()->block_cache_standalone_handle_count, 1);
    ASSERT_EQ(get_perf_context()->compressed_sec_cache_insert_real_count, 3);
    cache->Release(handle);

    // k2 is still in secondary cache.
    handle = cache->Lookup(key2, GetHelper(), this, Cache::Priority::LOW,
                           stats.get());
    ASSERT_NE(handle, nullptr);
    ASSERT_EQ(get_perf_context()->block_cache_standalone_handle_count, 2);
    cache->Release(handle);

    // Testing SetCapacity().
    ASSERT_OK(secondary_cache->SetCapacity(0));
    handle = cache->Lookup(key3, GetHelper(), this, Cache::Priority::LOW,
                           stats.get());
    ASSERT_EQ(handle, nullptr);

    ASSERT_OK(secondary_cache->SetCapacity(7000));
    size_t capacity;
    ASSERT_OK(secondary_cache->GetCapacity(capacity));
    ASSERT_EQ(capacity, 7000);
    auto item1_3 = new TestItem(str1.data(), str1.length());
    // After this Insert, primary cache contains k1.
    ASSERT_OK(cache->Insert(key1, item1_3, GetHelper(), str2.length()));
    ASSERT_EQ(get_perf_context()->compressed_sec_cache_insert_dummy_count, 3);
    ASSERT_EQ(get_perf_context()->compressed_sec_cache_insert_real_count, 4);

    auto item2_3 = new TestItem(str2.data(), str2.length());
    // After this Insert, primary cache contains k2 and secondary cache contains
    // k1's dummy item.
    ASSERT_OK(cache->Insert(key2, item2_3, GetHelper(), str1.length()));
    ASSERT_EQ(get_perf_context()->compressed_sec_cache_insert_dummy_count, 4);

    auto item1_4 = new TestItem(str1.data(), str1.length());
    // After this Insert, primary cache contains k1 and secondary cache contains
    // k1's dummy item and k2's dummy item.
    ASSERT_OK(cache->Insert(key1, item1_4, GetHelper(), str2.length()));
    ASSERT_EQ(get_perf_context()->compressed_sec_cache_insert_dummy_count, 5);

    auto item2_4 = new TestItem(str2.data(), str2.length());
    // After this Insert, primary cache contains k2 and secondary cache contains
    // k1's real item and k2's dummy item.
    ASSERT_OK(cache->Insert(key2, item2_4, GetHelper(), str2.length()));
    ASSERT_EQ(get_perf_context()->compressed_sec_cache_insert_real_count, 5);
    // This Lookup should just insert a dummy handle in the primary cache
    // and the k1 is still in the secondary cache.
    handle = cache->Lookup(key1, GetHelper(), this, Cache::Priority::LOW,
                           stats.get());

    ASSERT_NE(handle, nullptr);
    cache->Release(handle);
    ASSERT_EQ(get_perf_context()->block_cache_standalone_handle_count, 3);

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

    secondary_cache_opts.capacity = 6000;
    secondary_cache_opts.num_shard_bits = 0;
    std::shared_ptr<SecondaryCache> secondary_cache =
        NewCompressedSecondaryCache(secondary_cache_opts);

    std::shared_ptr<Cache> cache = NewCache(
        /*_capacity=*/1300, /*_num_shard_bits=*/0,
        /*_strict_capacity_limit=*/false, secondary_cache);

    Random rnd(301);
    std::string str1 = rnd.RandomString(1001);
    auto item1 = std::make_unique<TestItem>(str1.data(), str1.length());
    ASSERT_OK(cache->Insert(key1, item1.get(), GetHelper(), str1.length()));
    item1.release();  // Appease clang-analyze "potential memory leak"

    Cache::Handle* handle;
    handle = cache->Lookup(key2, nullptr, this, Cache::Priority::LOW);
    ASSERT_EQ(handle, nullptr);
    handle = cache->Lookup(key2, GetHelper(), this, Cache::Priority::LOW);
    ASSERT_EQ(handle, nullptr);

    Cache::AsyncLookupHandle ah;
    ah.key = key2;
    ah.helper = GetHelper();
    ah.create_context = this;
    ah.priority = Cache::Priority::LOW;
    cache->StartAsyncLookup(ah);
    cache->Wait(ah);
    ASSERT_EQ(ah.Result(), nullptr);

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

    secondary_cache_opts.capacity = 6000;
    secondary_cache_opts.num_shard_bits = 0;

    std::shared_ptr<SecondaryCache> secondary_cache =
        NewCompressedSecondaryCache(secondary_cache_opts);

    std::shared_ptr<Cache> cache = NewCache(
        /*_capacity=*/1300, /*_num_shard_bits=*/0,
        /*_strict_capacity_limit=*/true, secondary_cache);

    Random rnd(301);
    std::string str1 = rnd.RandomString(1001);
    auto item1 = new TestItem(str1.data(), str1.length());
    ASSERT_OK(cache->Insert(key1, item1, GetHelperFail(), str1.length()));

    std::string str2 = rnd.RandomString(1002);
    auto item2 = new TestItem(str2.data(), str2.length());
    // k1 should be demoted to the secondary cache.
    ASSERT_OK(cache->Insert(key2, item2, GetHelperFail(), str2.length()));

    Cache::Handle* handle;
    handle = cache->Lookup(key2, GetHelperFail(), this, Cache::Priority::LOW);
    ASSERT_NE(handle, nullptr);
    cache->Release(handle);
    // This lookup should fail, since k1 demotion would have failed.
    handle = cache->Lookup(key1, GetHelperFail(), this, Cache::Priority::LOW);
    ASSERT_EQ(handle, nullptr);
    // Since k1 was not promoted, k2 should still be in cache.
    handle = cache->Lookup(key2, GetHelperFail(), this, Cache::Priority::LOW);
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

    secondary_cache_opts.capacity = 6000;
    secondary_cache_opts.num_shard_bits = 0;

    std::shared_ptr<SecondaryCache> secondary_cache =
        NewCompressedSecondaryCache(secondary_cache_opts);

    std::shared_ptr<Cache> cache = NewCache(
        /*_capacity=*/1300, /*_num_shard_bits=*/0,
        /*_strict_capacity_limit=*/true, secondary_cache);

    Random rnd(301);
    std::string str1 = rnd.RandomString(1001);
    auto item1 = new TestItem(str1.data(), str1.length());
    ASSERT_OK(cache->Insert(key1, item1, GetHelper(), str1.length()));

    std::string str2 = rnd.RandomString(1002);
    auto item2 = new TestItem(str2.data(), str2.length());
    // k1 should be demoted to the secondary cache.
    ASSERT_OK(cache->Insert(key2, item2, GetHelper(), str2.length()));

    Cache::Handle* handle;
    SetFailCreate(true);
    handle = cache->Lookup(key2, GetHelper(), this, Cache::Priority::LOW);
    ASSERT_NE(handle, nullptr);
    cache->Release(handle);
    // This lookup should fail, since k1 creation would have failed
    handle = cache->Lookup(key1, GetHelper(), this, Cache::Priority::LOW);
    ASSERT_EQ(handle, nullptr);
    // Since k1 didn't get promoted, k2 should still be in cache
    handle = cache->Lookup(key2, GetHelper(), this, Cache::Priority::LOW);
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

    secondary_cache_opts.capacity = 6000;
    secondary_cache_opts.num_shard_bits = 0;

    std::shared_ptr<SecondaryCache> secondary_cache =
        NewCompressedSecondaryCache(secondary_cache_opts);

    std::shared_ptr<Cache> cache = NewCache(
        /*_capacity=*/1300, /*_num_shard_bits=*/0,
        /*_strict_capacity_limit=*/false, secondary_cache);

    Random rnd(301);
    std::string str1 = rnd.RandomString(1001);
    auto item1_1 = new TestItem(str1.data(), str1.length());
    ASSERT_OK(cache->Insert(key1, item1_1, GetHelper(), str1.length()));

    std::string str2 = rnd.RandomString(1002);
    std::string str2_clone{str2};
    auto item2 = new TestItem(str2.data(), str2.length());
    // After this Insert, primary cache contains k2 and secondary cache contains
    // k1's dummy item.
    ASSERT_OK(cache->Insert(key2, item2, GetHelper(), str2.length()));

    // After this Insert, primary cache contains k1 and secondary cache contains
    // k1's dummy item and k2's dummy item.
    auto item1_2 = new TestItem(str1.data(), str1.length());
    ASSERT_OK(cache->Insert(key1, item1_2, GetHelper(), str1.length()));

    auto item2_2 = new TestItem(str2.data(), str2.length());
    // After this Insert, primary cache contains k2 and secondary cache contains
    // k1's item and k2's dummy item.
    ASSERT_OK(cache->Insert(key2, item2_2, GetHelper(), str2.length()));

    Cache::Handle* handle2;
    handle2 = cache->Lookup(key2, GetHelper(), this, Cache::Priority::LOW);
    ASSERT_NE(handle2, nullptr);
    cache->Release(handle2);

    // k1 promotion should fail because cache is at capacity and
    // strict_capacity_limit is true, but the lookup should still succeed.
    // A k1's dummy item is inserted into primary cache.
    Cache::Handle* handle1;
    handle1 = cache->Lookup(key1, GetHelper(), this, Cache::Priority::LOW);
    ASSERT_NE(handle1, nullptr);
    cache->Release(handle1);

    // Since k1 didn't get inserted, k2 should still be in cache
    handle2 = cache->Lookup(key2, GetHelper(), this, Cache::Priority::LOW);
    ASSERT_NE(handle2, nullptr);
    cache->Release(handle2);

    cache.reset();
    secondary_cache.reset();
  }

  void SplitValueIntoChunksTest() {
    JemallocAllocatorOptions jopts;
    std::shared_ptr<MemoryAllocator> allocator;
    std::string msg;
    if (JemallocNodumpAllocator::IsSupported(&msg)) {
      Status s = NewJemallocNodumpAllocator(jopts, &allocator);
      if (!s.ok()) {
        ROCKSDB_GTEST_BYPASS("JEMALLOC not supported");
      }
    } else {
      ROCKSDB_GTEST_BYPASS("JEMALLOC not supported");
    }

    using CacheValueChunk = CompressedSecondaryCache::CacheValueChunk;
    std::unique_ptr<CompressedSecondaryCache> sec_cache =
        std::make_unique<CompressedSecondaryCache>(
            CompressedSecondaryCacheOptions(1000, 0, true, 0.5, 0.0,
                                            allocator));
    Random rnd(301);
    // 8500 = 8169 + 233 + 98, so there should be 3 chunks after split.
    size_t str_size{8500};
    std::string str = rnd.RandomString(static_cast<int>(str_size));
    size_t charge{0};
    CacheValueChunk* chunks_head =
        sec_cache->SplitValueIntoChunks(str, kLZ4Compression, charge);
    ASSERT_EQ(charge, str_size + 3 * (sizeof(CacheValueChunk) - 1));

    CacheValueChunk* current_chunk = chunks_head;
    ASSERT_EQ(current_chunk->size, 8192 - sizeof(CacheValueChunk) + 1);
    current_chunk = current_chunk->next;
    ASSERT_EQ(current_chunk->size, 256 - sizeof(CacheValueChunk) + 1);
    current_chunk = current_chunk->next;
    ASSERT_EQ(current_chunk->size, 98);

    sec_cache->GetHelper(true)->del_cb(chunks_head, /*alloc*/ nullptr);
  }

  void MergeChunksIntoValueTest() {
    using CacheValueChunk = CompressedSecondaryCache::CacheValueChunk;
    Random rnd(301);
    size_t size1{2048};
    std::string str1 = rnd.RandomString(static_cast<int>(size1));
    CacheValueChunk* current_chunk = reinterpret_cast<CacheValueChunk*>(
        new char[sizeof(CacheValueChunk) - 1 + size1]);
    CacheValueChunk* chunks_head = current_chunk;
    memcpy(current_chunk->data, str1.data(), size1);
    current_chunk->size = size1;

    size_t size2{256};
    std::string str2 = rnd.RandomString(static_cast<int>(size2));
    current_chunk->next = reinterpret_cast<CacheValueChunk*>(
        new char[sizeof(CacheValueChunk) - 1 + size2]);
    current_chunk = current_chunk->next;
    memcpy(current_chunk->data, str2.data(), size2);
    current_chunk->size = size2;

    size_t size3{31};
    std::string str3 = rnd.RandomString(static_cast<int>(size3));
    current_chunk->next = reinterpret_cast<CacheValueChunk*>(
        new char[sizeof(CacheValueChunk) - 1 + size3]);
    current_chunk = current_chunk->next;
    memcpy(current_chunk->data, str3.data(), size3);
    current_chunk->size = size3;
    current_chunk->next = nullptr;

    std::string str = str1 + str2 + str3;

    std::unique_ptr<CompressedSecondaryCache> sec_cache =
        std::make_unique<CompressedSecondaryCache>(
            CompressedSecondaryCacheOptions(1000, 0, true, 0.5, 0.0));
    size_t charge{0};
    CacheAllocationPtr value =
        sec_cache->MergeChunksIntoValue(chunks_head, charge);
    ASSERT_EQ(charge, size1 + size2 + size3);
    std::string value_str{value.get(), charge};
    ASSERT_EQ(strcmp(value_str.data(), str.data()), 0);

    while (chunks_head != nullptr) {
      CacheValueChunk* tmp_chunk = chunks_head;
      chunks_head = chunks_head->next;
      tmp_chunk->Free();
    }
  }

  void SplictValueAndMergeChunksTest() {
    JemallocAllocatorOptions jopts;
    std::shared_ptr<MemoryAllocator> allocator;
    std::string msg;
    if (JemallocNodumpAllocator::IsSupported(&msg)) {
      Status s = NewJemallocNodumpAllocator(jopts, &allocator);
      if (!s.ok()) {
        ROCKSDB_GTEST_BYPASS("JEMALLOC not supported");
      }
    } else {
      ROCKSDB_GTEST_BYPASS("JEMALLOC not supported");
    }

    using CacheValueChunk = CompressedSecondaryCache::CacheValueChunk;
    std::unique_ptr<CompressedSecondaryCache> sec_cache =
        std::make_unique<CompressedSecondaryCache>(
            CompressedSecondaryCacheOptions(1000, 0, true, 0.5, 0.0,
                                            allocator));
    Random rnd(301);
    // 8500 = 8169 + 233 + 98, so there should be 3 chunks after split.
    size_t str_size{8500};
    std::string str = rnd.RandomString(static_cast<int>(str_size));
    size_t charge{0};
    CacheValueChunk* chunks_head =
        sec_cache->SplitValueIntoChunks(str, kLZ4Compression, charge);
    ASSERT_EQ(charge, str_size + 3 * (sizeof(CacheValueChunk) - 1));

    CacheAllocationPtr value =
        sec_cache->MergeChunksIntoValue(chunks_head, charge);
    ASSERT_EQ(charge, str_size);
    std::string value_str{value.get(), charge};
    ASSERT_EQ(strcmp(value_str.data(), str.data()), 0);

    sec_cache->GetHelper(true)->del_cb(chunks_head, /*alloc*/ nullptr);
  }
};

class CompressedSecondaryCacheTest
    : public CompressedSecondaryCacheTestBase,
      public testing::WithParamInterface<std::string> {
  const std::string& Type() override { return GetParam(); }
};

INSTANTIATE_TEST_CASE_P(CompressedSecondaryCacheTest,
                        CompressedSecondaryCacheTest, GetTestingCacheTypes());

class CompressedSecCacheTestWithCompressAndAllocatorParam
    : public CompressedSecondaryCacheTestBase,
      public ::testing::WithParamInterface<
          std::tuple<bool, bool, std::string>> {
 public:
  CompressedSecCacheTestWithCompressAndAllocatorParam() {
    sec_cache_is_compressed_ = std::get<0>(GetParam());
    use_jemalloc_ = std::get<1>(GetParam());
  }
  const std::string& Type() override { return std::get<2>(GetParam()); }
  bool sec_cache_is_compressed_;
  bool use_jemalloc_;
};

TEST_P(CompressedSecCacheTestWithCompressAndAllocatorParam, BasicTes) {
  BasicTest(sec_cache_is_compressed_, use_jemalloc_);
}

INSTANTIATE_TEST_CASE_P(CompressedSecCacheTests,
                        CompressedSecCacheTestWithCompressAndAllocatorParam,
                        ::testing::Combine(testing::Bool(), testing::Bool(),
                                           GetTestingCacheTypes()));

class CompressedSecondaryCacheTestWithCompressionParam
    : public CompressedSecondaryCacheTestBase,
      public ::testing::WithParamInterface<std::tuple<bool, std::string>> {
 public:
  CompressedSecondaryCacheTestWithCompressionParam() {
    sec_cache_is_compressed_ = std::get<0>(GetParam());
  }
  const std::string& Type() override { return std::get<1>(GetParam()); }
  bool sec_cache_is_compressed_;
};

TEST_P(CompressedSecondaryCacheTestWithCompressionParam, BasicTestFromString) {
  std::shared_ptr<SecondaryCache> sec_cache{nullptr};
  std::string sec_cache_uri;
  if (sec_cache_is_compressed_) {
    if (LZ4_Supported()) {
      sec_cache_uri =
          "compressed_secondary_cache://"
          "capacity=2048;num_shard_bits=0;compression_type=kLZ4Compression;"
          "compress_format_version=2";
    } else {
      ROCKSDB_GTEST_SKIP("This test requires LZ4 support.");
      sec_cache_uri =
          "compressed_secondary_cache://"
          "capacity=2048;num_shard_bits=0;compression_type=kNoCompression";
      sec_cache_is_compressed_ = false;
    }
    Status s = SecondaryCache::CreateFromString(ConfigOptions(), sec_cache_uri,
                                                &sec_cache);
    EXPECT_OK(s);
  } else {
    sec_cache_uri =
        "compressed_secondary_cache://"
        "capacity=2048;num_shard_bits=0;compression_type=kNoCompression";
    Status s = SecondaryCache::CreateFromString(ConfigOptions(), sec_cache_uri,
                                                &sec_cache);
    EXPECT_OK(s);
  }
  BasicTestHelper(sec_cache, sec_cache_is_compressed_);
}

TEST_P(CompressedSecondaryCacheTestWithCompressionParam,
       BasicTestFromStringWithSplit) {
  std::shared_ptr<SecondaryCache> sec_cache{nullptr};
  std::string sec_cache_uri;
  if (sec_cache_is_compressed_) {
    if (LZ4_Supported()) {
      sec_cache_uri =
          "compressed_secondary_cache://"
          "capacity=2048;num_shard_bits=0;compression_type=kLZ4Compression;"
          "compress_format_version=2;enable_custom_split_merge=true";
    } else {
      ROCKSDB_GTEST_SKIP("This test requires LZ4 support.");
      sec_cache_uri =
          "compressed_secondary_cache://"
          "capacity=2048;num_shard_bits=0;compression_type=kNoCompression;"
          "enable_custom_split_merge=true";
      sec_cache_is_compressed_ = false;
    }
    Status s = SecondaryCache::CreateFromString(ConfigOptions(), sec_cache_uri,
                                                &sec_cache);
    EXPECT_OK(s);
  } else {
    sec_cache_uri =
        "compressed_secondary_cache://"
        "capacity=2048;num_shard_bits=0;compression_type=kNoCompression;"
        "enable_custom_split_merge=true";
    Status s = SecondaryCache::CreateFromString(ConfigOptions(), sec_cache_uri,
                                                &sec_cache);
    EXPECT_OK(s);
  }
  BasicTestHelper(sec_cache, sec_cache_is_compressed_);
}


TEST_P(CompressedSecondaryCacheTestWithCompressionParam, FailsTest) {
  FailsTest(sec_cache_is_compressed_);
}

TEST_P(CompressedSecondaryCacheTestWithCompressionParam,
       BasicIntegrationFailTest) {
  BasicIntegrationFailTest(sec_cache_is_compressed_);
}

TEST_P(CompressedSecondaryCacheTestWithCompressionParam,
       IntegrationSaveFailTest) {
  IntegrationSaveFailTest(sec_cache_is_compressed_);
}

TEST_P(CompressedSecondaryCacheTestWithCompressionParam,
       IntegrationCreateFailTest) {
  IntegrationCreateFailTest(sec_cache_is_compressed_);
}

TEST_P(CompressedSecondaryCacheTestWithCompressionParam,
       IntegrationFullCapacityTest) {
  IntegrationFullCapacityTest(sec_cache_is_compressed_);
}

TEST_P(CompressedSecondaryCacheTestWithCompressionParam, EntryRoles) {
  CompressedSecondaryCacheOptions opts;
  opts.capacity = 2048;
  opts.num_shard_bits = 0;

  if (sec_cache_is_compressed_) {
    if (!LZ4_Supported()) {
      ROCKSDB_GTEST_SKIP("This test requires LZ4 support.");
      return;
    }
  } else {
    opts.compression_type = CompressionType::kNoCompression;
  }

  // Select a random subset to include, for fast test
  Random& r = *Random::GetTLSInstance();
  CacheEntryRoleSet do_not_compress;
  for (uint32_t i = 0; i < kNumCacheEntryRoles; ++i) {
    // A few included on average, but decent chance of zero
    if (r.OneIn(5)) {
      do_not_compress.Add(static_cast<CacheEntryRole>(i));
    }
  }
  opts.do_not_compress_roles = do_not_compress;

  std::shared_ptr<SecondaryCache> sec_cache = NewCompressedSecondaryCache(opts);

  // Fixed seed to ensure consistent compressibility (doesn't compress)
  std::string junk(Random(301).RandomString(1000));

  for (uint32_t i = 0; i < kNumCacheEntryRoles; ++i) {
    CacheEntryRole role = static_cast<CacheEntryRole>(i);

    // Uniquify `junk`
    junk[0] = static_cast<char>(i);
    TestItem item{junk.data(), junk.length()};
    Slice ith_key = Slice(junk.data(), 16);

    get_perf_context()->Reset();
    ASSERT_OK(sec_cache->Insert(ith_key, &item, GetHelper(role)));
    ASSERT_EQ(get_perf_context()->compressed_sec_cache_insert_dummy_count, 1U);

    ASSERT_OK(sec_cache->Insert(ith_key, &item, GetHelper(role)));
    ASSERT_EQ(get_perf_context()->compressed_sec_cache_insert_real_count, 1U);

    bool kept_in_sec_cache{true};
    std::unique_ptr<SecondaryCacheResultHandle> handle =
        sec_cache->Lookup(ith_key, GetHelper(role), this, true,
                          /*advise_erase=*/true, kept_in_sec_cache);
    ASSERT_NE(handle, nullptr);

    // Lookup returns the right data
    std::unique_ptr<TestItem> val =
        std::unique_ptr<TestItem>(static_cast<TestItem*>(handle->Value()));
    ASSERT_NE(val, nullptr);
    ASSERT_EQ(memcmp(val->Buf(), item.Buf(), item.Size()), 0);

    bool compressed =
        sec_cache_is_compressed_ && !do_not_compress.Contains(role);
    if (compressed) {
      ASSERT_EQ(get_perf_context()->compressed_sec_cache_uncompressed_bytes,
                1000);
      ASSERT_EQ(get_perf_context()->compressed_sec_cache_compressed_bytes,
                1007);
    } else {
      ASSERT_EQ(get_perf_context()->compressed_sec_cache_uncompressed_bytes, 0);
      ASSERT_EQ(get_perf_context()->compressed_sec_cache_compressed_bytes, 0);
    }
  }
}

INSTANTIATE_TEST_CASE_P(CompressedSecCacheTests,
                        CompressedSecondaryCacheTestWithCompressionParam,
                        testing::Combine(testing::Bool(),
                                         GetTestingCacheTypes()));

class CompressedSecCacheTestWithCompressAndSplitParam
    : public CompressedSecondaryCacheTestBase,
      public ::testing::WithParamInterface<
          std::tuple<bool, bool, std::string>> {
 public:
  CompressedSecCacheTestWithCompressAndSplitParam() {
    sec_cache_is_compressed_ = std::get<0>(GetParam());
    enable_custom_split_merge_ = std::get<1>(GetParam());
  }
  const std::string& Type() override { return std::get<2>(GetParam()); }
  bool sec_cache_is_compressed_;
  bool enable_custom_split_merge_;
};

TEST_P(CompressedSecCacheTestWithCompressAndSplitParam, BasicIntegrationTest) {
  BasicIntegrationTest(sec_cache_is_compressed_, enable_custom_split_merge_);
}

INSTANTIATE_TEST_CASE_P(CompressedSecCacheTests,
                        CompressedSecCacheTestWithCompressAndSplitParam,
                        ::testing::Combine(testing::Bool(), testing::Bool(),
                                           GetTestingCacheTypes()));

TEST_P(CompressedSecondaryCacheTest, SplitValueIntoChunksTest) {
  SplitValueIntoChunksTest();
}

TEST_P(CompressedSecondaryCacheTest, MergeChunksIntoValueTest) {
  MergeChunksIntoValueTest();
}

TEST_P(CompressedSecondaryCacheTest, SplictValueAndMergeChunksTest) {
  SplictValueAndMergeChunksTest();
}

class CompressedSecCacheTestWithTiered : public ::testing::Test {
 public:
  CompressedSecCacheTestWithTiered() {
    LRUCacheOptions lru_opts;
    TieredVolatileCacheOptions opts;
    lru_opts.capacity = 70 << 20;
    opts.cache_opts = &lru_opts;
    opts.cache_type = PrimaryCacheType::kCacheTypeLRU;
    opts.comp_cache_opts.capacity = 30 << 20;
    cache_ = NewTieredVolatileCache(opts);
    cache_res_mgr_ =
        std::make_shared<CacheReservationManagerImpl<CacheEntryRole::kMisc>>(
            cache_);
  }

 protected:
  CacheReservationManager* cache_res_mgr() { return cache_res_mgr_.get(); }

  Cache* GetCache() {
    return static_cast_with_check<CacheWithSecondaryAdapter, Cache>(
               cache_.get())
        ->TEST_GetCache();
  }

  SecondaryCache* GetSecondaryCache() {
    return static_cast_with_check<CacheWithSecondaryAdapter, Cache>(
               cache_.get())
        ->TEST_GetSecondaryCache();
  }

  size_t GetPercent(size_t val, unsigned int percent) {
    return static_cast<size_t>(val * percent / 100);
  }

 private:
  std::shared_ptr<Cache> cache_;
  std::shared_ptr<CacheReservationManager> cache_res_mgr_;
};

bool CacheUsageWithinBounds(size_t val1, size_t val2, size_t error) {
  return ((val1 < (val2 + error)) && (val1 > (val2 - error)));
}

TEST_F(CompressedSecCacheTestWithTiered, CacheReservationManager) {
  CompressedSecondaryCache* sec_cache =
      reinterpret_cast<CompressedSecondaryCache*>(GetSecondaryCache());

  // Use EXPECT_PRED3 instead of EXPECT_NEAR to void too many size_t to
  // double explicit casts
  EXPECT_PRED3(CacheUsageWithinBounds, GetCache()->GetUsage(), (30 << 20),
               GetPercent(30 << 20, 1));
  EXPECT_EQ(sec_cache->TEST_GetUsage(), 0);

  ASSERT_OK(cache_res_mgr()->UpdateCacheReservation(10 << 20));
  EXPECT_PRED3(CacheUsageWithinBounds, GetCache()->GetUsage(), (37 << 20),
               GetPercent(37 << 20, 1));
  EXPECT_PRED3(CacheUsageWithinBounds, sec_cache->TEST_GetUsage(), (3 << 20),
               GetPercent(3 << 20, 1));

  ASSERT_OK(cache_res_mgr()->UpdateCacheReservation(0));
  EXPECT_PRED3(CacheUsageWithinBounds, GetCache()->GetUsage(), (30 << 20),
               GetPercent(30 << 20, 1));
  EXPECT_EQ(sec_cache->TEST_GetUsage(), 0);
}

TEST_F(CompressedSecCacheTestWithTiered,
       CacheReservationManagerMultipleUpdate) {
  CompressedSecondaryCache* sec_cache =
      reinterpret_cast<CompressedSecondaryCache*>(GetSecondaryCache());

  EXPECT_PRED3(CacheUsageWithinBounds, GetCache()->GetUsage(), (30 << 20),
               GetPercent(30 << 20, 1));
  EXPECT_EQ(sec_cache->TEST_GetUsage(), 0);

  int i;
  for (i = 0; i < 10; ++i) {
    ASSERT_OK(cache_res_mgr()->UpdateCacheReservation((1 + i) << 20));
  }
  EXPECT_PRED3(CacheUsageWithinBounds, GetCache()->GetUsage(), (37 << 20),
               GetPercent(37 << 20, 1));
  EXPECT_PRED3(CacheUsageWithinBounds, sec_cache->TEST_GetUsage(), (3 << 20),
               GetPercent(3 << 20, 1));

  for (i = 10; i > 0; --i) {
    ASSERT_OK(cache_res_mgr()->UpdateCacheReservation(((i - 1) << 20)));
  }
  EXPECT_PRED3(CacheUsageWithinBounds, GetCache()->GetUsage(), (30 << 20),
               GetPercent(30 << 20, 1));
  EXPECT_EQ(sec_cache->TEST_GetUsage(), 0);
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

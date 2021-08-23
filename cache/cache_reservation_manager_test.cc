//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#include "cache/cache_reservation_manager.h"

#include <cstddef>
#include <cstring>
#include <memory>

#include "cache/cache_entry_roles.h"
#include "rocksdb/cache.h"
#include "rocksdb/slice.h"
#include "table/block_based/block_based_table_reader.h"
#include "test_util/testharness.h"
#include "util/coding.h"

namespace ROCKSDB_NAMESPACE {
class CacheReservationManagerTest : public ::testing::Test {
 protected:
  static constexpr std::size_t kOneGigabyte = 1024 * 1024 * 1024;
  static constexpr int kNumShardBits = 0;  // 2^0 shard

  static constexpr std::size_t kSizeDummyEntry = 256 * 1024;
  static const std::size_t kCacheKeyPrefixSize =
      BlockBasedTable::kMaxCacheKeyPrefixSize + kMaxVarint64Length;
  static constexpr std::size_t kMetaDataChargeOverhead = 10000;

  std::shared_ptr<Cache> cache = NewLRUCache(kOneGigabyte, kNumShardBits);
  std::unique_ptr<CacheReservationManager> test_cache_rev_mng;

  CacheReservationManagerTest() {
    test_cache_rev_mng.reset(new CacheReservationManager(cache));
  }
};

TEST_F(CacheReservationManagerTest, GenerateCacheKey) {
  // The first cache reservation manager owning the cache will have
  // cache->NewId() = 1
  constexpr std::size_t kCacheNewId = 1;
  // The first key generated inside of cache reservation manager will have
  // next_cache_key_id = 0
  constexpr std::size_t kCacheKeyId = 0;

  char expected_cache_key[kCacheKeyPrefixSize + kMaxVarint64Length];
  std::memset(expected_cache_key, 0, kCacheKeyPrefixSize + kMaxVarint64Length);

  EncodeVarint64(expected_cache_key, kCacheNewId);
  char* end =
      EncodeVarint64(expected_cache_key + kCacheKeyPrefixSize, kCacheKeyId);
  Slice expected_cache_key_slice(
      expected_cache_key, static_cast<std::size_t>(end - expected_cache_key));

  std::size_t new_mem_used = 1 * kSizeDummyEntry;
  Status s =
      test_cache_rev_mng
          ->UpdateCacheReservation<ROCKSDB_NAMESPACE::CacheEntryRole::kMisc>(
              new_mem_used);
  ASSERT_EQ(s, Status::OK());
  ASSERT_GE(cache->GetPinnedUsage(), 1 * kSizeDummyEntry);
  ASSERT_LT(cache->GetPinnedUsage(),
            1 * kSizeDummyEntry + kMetaDataChargeOverhead);

  Cache::Handle* handle = cache->Lookup(expected_cache_key_slice);
  EXPECT_NE(handle, nullptr)
      << "Failed to generate the cache key for the dummy entry correctly";
  // Clean up the returned handle from Lookup() to prevent memory leak
  cache->Release(handle);
}

TEST_F(CacheReservationManagerTest, KeepCacheReservationTheSame) {
  std::size_t new_mem_used = 1 * kSizeDummyEntry;
  Status s =
      test_cache_rev_mng
          ->UpdateCacheReservation<ROCKSDB_NAMESPACE::CacheEntryRole::kMisc>(
              new_mem_used);
  ASSERT_EQ(s, Status::OK());
  ASSERT_EQ(test_cache_rev_mng->GetTotalReservedCacheSize(),
            1 * kSizeDummyEntry);
  std::size_t initial_pinned_usage = cache->GetPinnedUsage();
  ASSERT_GE(initial_pinned_usage, 1 * kSizeDummyEntry);
  ASSERT_LT(initial_pinned_usage,
            1 * kSizeDummyEntry + kMetaDataChargeOverhead);

  s = test_cache_rev_mng
          ->UpdateCacheReservation<ROCKSDB_NAMESPACE::CacheEntryRole::kMisc>(
              new_mem_used);
  EXPECT_EQ(s, Status::OK())
      << "Failed to keep cache reservation the same when new_mem_used equals "
         "to current cache reservation";
  EXPECT_EQ(test_cache_rev_mng->GetTotalReservedCacheSize(),
            1 * kSizeDummyEntry)
      << "Failed to bookkeep correctly when new_mem_used equals to current "
         "cache reservation";
  EXPECT_EQ(cache->GetPinnedUsage(), initial_pinned_usage)
      << "Failed to keep underlying dummy entries the same when new_mem_used "
         "equals to current cache reservation";
}

TEST_F(CacheReservationManagerTest,
       IncreaseCacheReservationByMultiplesOfDummyEntrySize) {
  std::size_t new_mem_used = 2 * kSizeDummyEntry;
  Status s =
      test_cache_rev_mng
          ->UpdateCacheReservation<ROCKSDB_NAMESPACE::CacheEntryRole::kMisc>(
              new_mem_used);
  EXPECT_EQ(s, Status::OK())
      << "Failed to increase cache reservation correctly";
  EXPECT_EQ(test_cache_rev_mng->GetTotalReservedCacheSize(),
            2 * kSizeDummyEntry)
      << "Failed to bookkeep cache reservation increase correctly";
  EXPECT_GE(cache->GetPinnedUsage(), 2 * kSizeDummyEntry)
      << "Failed to increase underlying dummy entries in cache correctly";
  EXPECT_LT(cache->GetPinnedUsage(),
            2 * kSizeDummyEntry + kMetaDataChargeOverhead)
      << "Failed to increase underlying dummy entries in cache correctly";
}

TEST_F(CacheReservationManagerTest,
       IncreaseCacheReservationNotByMultiplesOfDummyEntrySize) {
  std::size_t new_mem_used = 2 * kSizeDummyEntry + kSizeDummyEntry / 2;
  Status s =
      test_cache_rev_mng
          ->UpdateCacheReservation<ROCKSDB_NAMESPACE::CacheEntryRole::kMisc>(
              new_mem_used);
  EXPECT_EQ(s, Status::OK())
      << "Failed to increase cache reservation correctly";
  EXPECT_EQ(test_cache_rev_mng->GetTotalReservedCacheSize(),
            3 * kSizeDummyEntry)
      << "Failed to bookkeep cache reservation increase correctly";
  EXPECT_GE(cache->GetPinnedUsage(), 3 * kSizeDummyEntry)
      << "Failed to increase underlying dummy entries in cache correctly";
  EXPECT_LT(cache->GetPinnedUsage(),
            3 * kSizeDummyEntry + kMetaDataChargeOverhead)
      << "Failed to increase underlying dummy entries in cache correctly";
}

TEST(CacheReservationManagerIncreaseReservcationOnFullCacheTest,
     IncreaseCacheReservationOnFullCache) {
  constexpr std::size_t kOneMegabyte = 1024 * 1024;
  constexpr std::size_t kOneGigabyte = 1024 * 1024 * 1024;
  constexpr std::size_t kSizeDummyEntry = 256 * 1024;
  constexpr std::size_t kMetaDataChargeOverhead = 10000;

  LRUCacheOptions lo;
  lo.capacity = kOneMegabyte;
  lo.num_shard_bits = 0;  // 2^0 shard
  lo.strict_capacity_limit = true;
  std::shared_ptr<Cache> cache = NewLRUCache(lo);
  std::unique_ptr<CacheReservationManager> test_cache_rev_mng(
      new CacheReservationManager(cache));

  std::size_t new_mem_used = kOneMegabyte + 1;
  Status s =
      test_cache_rev_mng
          ->UpdateCacheReservation<ROCKSDB_NAMESPACE::CacheEntryRole::kMisc>(
              new_mem_used);
  EXPECT_EQ(s, Status::Incomplete())
      << "Failed to return status to indicate failure of dummy entry insertion "
         "during cache reservation on full cache";
  EXPECT_GE(test_cache_rev_mng->GetTotalReservedCacheSize(),
            1 * kSizeDummyEntry)
      << "Failed to bookkeep correctly before cache resevation failure happens "
         "due to full cache";
  EXPECT_LE(test_cache_rev_mng->GetTotalReservedCacheSize(), kOneMegabyte)
      << "Failed to bookkeep correctly (i.e, bookkeep only successful dummy "
         "entry insertions) when encountering cache resevation failure due to "
         "full cache";
  EXPECT_GE(cache->GetPinnedUsage(), 1 * kSizeDummyEntry)
      << "Failed to insert underlying dummy entries correctly when "
         "encountering cache resevation failure due to full cache";
  EXPECT_LE(cache->GetPinnedUsage(), kOneMegabyte)
      << "Failed to insert underlying dummy entries correctly when "
         "encountering cache resevation failure due to full cache";

  new_mem_used = kOneMegabyte / 2;  // 2 dummy entries
  s = test_cache_rev_mng
          ->UpdateCacheReservation<ROCKSDB_NAMESPACE::CacheEntryRole::kMisc>(
              new_mem_used);
  EXPECT_EQ(s, Status::OK())
      << "Failed to decrease cache reservation after encountering cache "
         "reservation failure due to full cache";
  EXPECT_EQ(test_cache_rev_mng->GetTotalReservedCacheSize(),
            2 * kSizeDummyEntry)
      << "Failed to bookkeep cache reservation decrease correctly after "
         "encountering cache reservation due to full cache";
  EXPECT_GE(cache->GetPinnedUsage(), 2 * kSizeDummyEntry)
      << "Failed to release underlying dummy entries correctly on cache "
         "reservation decrease after encountering cache resevation failure due "
         "to full cache";
  EXPECT_LT(cache->GetPinnedUsage(),
            2 * kSizeDummyEntry + kMetaDataChargeOverhead)
      << "Failed to release underlying dummy entries correctly on cache "
         "reservation decrease after encountering cache resevation failure due "
         "to full cache";

  // Create cache full again for subsequent tests
  new_mem_used = kOneMegabyte + 1;
  s = test_cache_rev_mng
          ->UpdateCacheReservation<ROCKSDB_NAMESPACE::CacheEntryRole::kMisc>(
              new_mem_used);
  EXPECT_EQ(s, Status::Incomplete())
      << "Failed to return status to indicate failure of dummy entry insertion "
         "during cache reservation on full cache";
  EXPECT_GE(test_cache_rev_mng->GetTotalReservedCacheSize(),
            1 * kSizeDummyEntry)
      << "Failed to bookkeep correctly before cache resevation failure happens "
         "due to full cache";
  EXPECT_LE(test_cache_rev_mng->GetTotalReservedCacheSize(), kOneMegabyte)
      << "Failed to bookkeep correctly (i.e, bookkeep only successful dummy "
         "entry insertions) when encountering cache resevation failure due to "
         "full cache";
  EXPECT_GE(cache->GetPinnedUsage(), 1 * kSizeDummyEntry)
      << "Failed to insert underlying dummy entries correctly when "
         "encountering cache resevation failure due to full cache";
  EXPECT_LE(cache->GetPinnedUsage(), kOneMegabyte)
      << "Failed to insert underlying dummy entries correctly when "
         "encountering cache resevation failure due to full cache";

  // Increase cache capacity so the previously failed insertion can fully
  // succeed
  cache->SetCapacity(kOneGigabyte);
  new_mem_used = kOneMegabyte + 1;
  s = test_cache_rev_mng
          ->UpdateCacheReservation<ROCKSDB_NAMESPACE::CacheEntryRole::kMisc>(
              new_mem_used);
  EXPECT_EQ(s, Status::OK())
      << "Failed to increase cache reservation after increasing cache capacity "
         "and mitigating cache full error";
  EXPECT_EQ(test_cache_rev_mng->GetTotalReservedCacheSize(),
            5 * kSizeDummyEntry)
      << "Failed to bookkeep cache reservation increase correctly after "
         "increasing cache capacity and mitigating cache full error";
  EXPECT_GE(cache->GetPinnedUsage(), 5 * kSizeDummyEntry)
      << "Failed to insert underlying dummy entries correctly after increasing "
         "cache capacity and mitigating cache full error";
  EXPECT_LT(cache->GetPinnedUsage(),
            5 * kSizeDummyEntry + kMetaDataChargeOverhead)
      << "Failed to insert underlying dummy entries correctly after increasing "
         "cache capacity and mitigating cache full error";
}

TEST_F(CacheReservationManagerTest,
       DecreaseCacheReservationByMultiplesOfDummyEntrySize) {
  std::size_t new_mem_used = 2 * kSizeDummyEntry;
  Status s =
      test_cache_rev_mng
          ->UpdateCacheReservation<ROCKSDB_NAMESPACE::CacheEntryRole::kMisc>(
              new_mem_used);
  ASSERT_EQ(s, Status::OK());
  ASSERT_EQ(test_cache_rev_mng->GetTotalReservedCacheSize(),
            2 * kSizeDummyEntry);
  ASSERT_GE(cache->GetPinnedUsage(), 2 * kSizeDummyEntry);
  ASSERT_LT(cache->GetPinnedUsage(),
            2 * kSizeDummyEntry + kMetaDataChargeOverhead);

  new_mem_used = 1 * kSizeDummyEntry;
  s = test_cache_rev_mng
          ->UpdateCacheReservation<ROCKSDB_NAMESPACE::CacheEntryRole::kMisc>(
              new_mem_used);
  EXPECT_EQ(s, Status::OK())
      << "Failed to decrease cache reservation correctly";
  EXPECT_EQ(test_cache_rev_mng->GetTotalReservedCacheSize(),
            1 * kSizeDummyEntry)
      << "Failed to bookkeep cache reservation decrease correctly";
  EXPECT_GE(cache->GetPinnedUsage(), 1 * kSizeDummyEntry)
      << "Failed to decrease underlying dummy entries in cache correctly";
  EXPECT_LT(cache->GetPinnedUsage(),
            1 * kSizeDummyEntry + kMetaDataChargeOverhead)
      << "Failed to decrease underlying dummy entries in cache correctly";
}

TEST_F(CacheReservationManagerTest,
       DecreaseCacheReservationNotByMultiplesOfDummyEntrySize) {
  std::size_t new_mem_used = 2 * kSizeDummyEntry;
  Status s =
      test_cache_rev_mng
          ->UpdateCacheReservation<ROCKSDB_NAMESPACE::CacheEntryRole::kMisc>(
              new_mem_used);
  ASSERT_EQ(s, Status::OK());
  ASSERT_EQ(test_cache_rev_mng->GetTotalReservedCacheSize(),
            2 * kSizeDummyEntry);
  ASSERT_GE(cache->GetPinnedUsage(), 2 * kSizeDummyEntry);
  ASSERT_LT(cache->GetPinnedUsage(),
            2 * kSizeDummyEntry + kMetaDataChargeOverhead);

  new_mem_used = kSizeDummyEntry / 2;
  s = test_cache_rev_mng
          ->UpdateCacheReservation<ROCKSDB_NAMESPACE::CacheEntryRole::kMisc>(
              new_mem_used);
  EXPECT_EQ(s, Status::OK())
      << "Failed to decrease cache reservation correctly";
  EXPECT_EQ(test_cache_rev_mng->GetTotalReservedCacheSize(),
            1 * kSizeDummyEntry)
      << "Failed to bookkeep cache reservation decrease correctly";
  EXPECT_GE(cache->GetPinnedUsage(), 1 * kSizeDummyEntry)
      << "Failed to decrease underlying dummy entries in cache correctly";
  EXPECT_LT(cache->GetPinnedUsage(),
            1 * kSizeDummyEntry + kMetaDataChargeOverhead)
      << "Failed to decrease underlying dummy entries in cache correctly";
}

TEST(CacheReservationManagerWithDelayedDecreaseTest,
     DecreaseCacheReservationWithDelayedDecrease) {
  constexpr std::size_t kOneGigabyte = 1024 * 1024 * 1024;
  constexpr std::size_t kSizeDummyEntry = 256 * 1024;
  constexpr std::size_t kMetaDataChargeOverhead = 10000;

  LRUCacheOptions lo;
  lo.capacity = kOneGigabyte;
  lo.num_shard_bits = 0;
  std::shared_ptr<Cache> cache = NewLRUCache(lo);
  std::unique_ptr<CacheReservationManager> test_cache_rev_mng(
      new CacheReservationManager(cache, true /* delayed_decrease */));

  std::size_t new_mem_used = 8 * kSizeDummyEntry;
  Status s =
      test_cache_rev_mng
          ->UpdateCacheReservation<ROCKSDB_NAMESPACE::CacheEntryRole::kMisc>(
              new_mem_used);
  ASSERT_EQ(s, Status::OK());
  ASSERT_EQ(test_cache_rev_mng->GetTotalReservedCacheSize(),
            8 * kSizeDummyEntry);
  std::size_t initial_pinned_usage = cache->GetPinnedUsage();
  ASSERT_GE(initial_pinned_usage, 8 * kSizeDummyEntry);
  ASSERT_LT(initial_pinned_usage,
            8 * kSizeDummyEntry + kMetaDataChargeOverhead);

  new_mem_used = 6 * kSizeDummyEntry;
  s = test_cache_rev_mng
          ->UpdateCacheReservation<ROCKSDB_NAMESPACE::CacheEntryRole::kMisc>(
              new_mem_used);
  EXPECT_EQ(s, Status::OK()) << "Failed to delay decreasing cache reservation";
  EXPECT_EQ(test_cache_rev_mng->GetTotalReservedCacheSize(),
            8 * kSizeDummyEntry)
      << "Failed to bookkeep correctly when delaying cache reservation "
         "decrease";
  EXPECT_EQ(cache->GetPinnedUsage(), initial_pinned_usage)
      << "Failed to delay decreasing underlying dummy entries in cache";

  new_mem_used = 7 * kSizeDummyEntry;
  s = test_cache_rev_mng
          ->UpdateCacheReservation<ROCKSDB_NAMESPACE::CacheEntryRole::kMisc>(
              new_mem_used);
  EXPECT_EQ(s, Status::OK()) << "Failed to delay decreasing cache reservation";
  EXPECT_EQ(test_cache_rev_mng->GetTotalReservedCacheSize(),
            8 * kSizeDummyEntry)
      << "Failed to bookkeep correctly when delaying cache reservation "
         "decrease";
  EXPECT_EQ(cache->GetPinnedUsage(), initial_pinned_usage)
      << "Failed to delay decreasing underlying dummy entries in cache";

  new_mem_used = 6 * kSizeDummyEntry - 1;
  s = test_cache_rev_mng
          ->UpdateCacheReservation<ROCKSDB_NAMESPACE::CacheEntryRole::kMisc>(
              new_mem_used);
  EXPECT_EQ(s, Status::OK())
      << "Failed to decrease cache reservation correctly when new_mem_used < "
         "GetTotalReservedCacheSize() * 3 / 4 on delayed decrease mode";
  EXPECT_EQ(test_cache_rev_mng->GetTotalReservedCacheSize(),
            6 * kSizeDummyEntry)
      << "Failed to bookkeep correctly when new_mem_used < "
         "GetTotalReservedCacheSize() * 3 / 4 on delayed decrease mode";
  EXPECT_GE(cache->GetPinnedUsage(), 6 * kSizeDummyEntry)
      << "Failed to decrease underlying dummy entries in cache when "
         "new_mem_used < GetTotalReservedCacheSize() * 3 / 4 on delayed "
         "decrease mode";
  EXPECT_LT(cache->GetPinnedUsage(),
            6 * kSizeDummyEntry + kMetaDataChargeOverhead)
      << "Failed to decrease underlying dummy entries in cache when "
         "new_mem_used < GetTotalReservedCacheSize() * 3 / 4 on delayed "
         "decrease mode";
}

TEST(CacheReservationManagerDestructorTest,
     ReleaseRemainingDummyEntriesOnDestruction) {
  constexpr std::size_t kOneGigabyte = 1024 * 1024 * 1024;
  constexpr std::size_t kSizeDummyEntry = 256 * 1024;
  constexpr std::size_t kMetaDataChargeOverhead = 10000;

  LRUCacheOptions lo;
  lo.capacity = kOneGigabyte;
  lo.num_shard_bits = 0;
  std::shared_ptr<Cache> cache = NewLRUCache(lo);
  {
    std::unique_ptr<CacheReservationManager> test_cache_rev_mng(
        new CacheReservationManager(cache));
    std::size_t new_mem_used = 1 * kSizeDummyEntry;
    Status s =
        test_cache_rev_mng
            ->UpdateCacheReservation<ROCKSDB_NAMESPACE::CacheEntryRole::kMisc>(
                new_mem_used);
    ASSERT_EQ(s, Status::OK());
    ASSERT_GE(cache->GetPinnedUsage(), 1 * kSizeDummyEntry);
    ASSERT_LT(cache->GetPinnedUsage(),
              1 * kSizeDummyEntry + kMetaDataChargeOverhead);
  }
  EXPECT_EQ(cache->GetPinnedUsage(), 0 * kSizeDummyEntry)
      << "Failed to release remaining underlying dummy entries in cache in "
         "CacheReservationManager's destructor";
}
}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
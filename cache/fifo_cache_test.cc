//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "cache/fifo_cache.h"

#include <memory>
#include <set>
#include <string>
#include <vector>

#include "port/port.h"
#include "rocksdb/cache.h"
#include "test_util/testharness.h"
#include "util/random.h"

namespace ROCKSDB_NAMESPACE {

namespace {
// Secondary-compatible helper whose create callback must never fire: the
// shard performs primary-only lookup and ignores promotion parameters.
Status DummyCreate(const Slice& /*data*/, CompressionType /*type*/,
                   CacheTier /*source*/, Cache::CreateContext* /*context*/,
                   MemoryAllocator* /*allocator*/,
                   Cache::ObjectPtr* /*out_obj*/, size_t* /*out_charge*/) {
  ADD_FAILURE() << "CreateCallback must not be called without secondary cache";
  return Status::NotSupported();
}
size_t DummySize(Cache::ObjectPtr /*obj*/) { return 0; }
Status DummySaveTo(Cache::ObjectPtr /*from_obj*/, size_t /*from_offset*/,
                   size_t /*length*/, char* /*out_buf*/) {
  return Status::OK();
}
// NOTE: no global secondary-compatible helper: its constructor reads the
// without_secondary_compat pointee, which would be an initialization-order
// fiasco against kNoopCacheItemHelper from another TU. Tests build it
// locally instead.
}  // namespace

class FIFOCacheTest : public testing::Test {
 public:
  FIFOCacheTest() = default;
  ~FIFOCacheTest() override { DeleteCache(); }

  void DeleteCache() {
    if (cache_ != nullptr) {
      cache_->~FIFOCacheShard();
      port::cacheline_aligned_free(cache_);
      cache_ = nullptr;
    }
  }

  void NewCache(size_t capacity, bool strict_capacity_limit = false,
                CacheMetadataChargePolicy metadata_charge_policy =
                    kDontChargeCacheMetadata) {
    DeleteCache();
    cache_ = static_cast<FIFOCacheShard*>(
        port::cacheline_aligned_alloc(sizeof(FIFOCacheShard)));
    new (cache_) FIFOCacheShard(capacity, strict_capacity_limit,
                                kDefaultToAdaptiveMutex, metadata_charge_policy,
                                /*max_upper_hash_bits*/ 24,
                                /*allocator*/ nullptr, &eviction_callback_);
  }

  Status Insert(const std::string& key, size_t charge = 1,
                FIFOHandle** handle = nullptr,
                Cache::Priority priority = Cache::Priority::LOW) {
    return cache_->Insert(key, 0 /*hash*/, nullptr /*value*/,
                          &kNoopCacheItemHelper, charge, handle, priority);
  }

  FIFOHandle* Lookup(const std::string& key) {
    return cache_->Lookup(key, 0 /*hash*/, nullptr, nullptr,
                          Cache::Priority::LOW, nullptr);
  }

  bool LookupBool(const std::string& key) {
    FIFOHandle* handle = Lookup(key);
    if (handle) {
      cache_->Release(handle, true /*useful*/, false /*erase*/);
      return true;
    }
    return false;
  }

  void Erase(const std::string& key) { cache_->Erase(key, 0 /*hash*/); }

  // Keys from oldest to newest.
  void ValidateFIFOList(const std::vector<std::string>& keys) {
    FIFOHandle* fifo;
    cache_->TEST_GetFIFOList(&fifo);
    FIFOHandle* iter = fifo->next;
    for (const auto& key : keys) {
      ASSERT_NE(fifo, iter);
      ASSERT_EQ(key, iter->key().ToString());
      iter = iter->next;
    }
    ASSERT_EQ(fifo, iter);
  }

  FIFOCacheShard* cache_ = nullptr;
  Cache::EvictionCallback eviction_callback_;
};

// R5: oldest unpinned entry is evicted first.
TEST_F(FIFOCacheTest, FifoOrderEvictsOldest) {
  NewCache(/*capacity*/ 3);
  EXPECT_OK(Insert("a"));
  EXPECT_OK(Insert("b"));
  EXPECT_OK(Insert("c"));
  ValidateFIFOList({"a", "b", "c"});
  EXPECT_OK(Insert("d"));
  EXPECT_FALSE(LookupBool("a"));
  EXPECT_TRUE(LookupBool("b"));
  EXPECT_TRUE(LookupBool("c"));
  EXPECT_TRUE(LookupBool("d"));
  ValidateFIFOList({"b", "c", "d"});
  EXPECT_OK(Insert("e"));
  EXPECT_FALSE(LookupBool("b"));
  ValidateFIFOList({"c", "d", "e"});
  EXPECT_EQ(3, cache_->GetUsage());
  EXPECT_EQ(3, cache_->GetOccupancyCount());
}

// R5 neuter vs LRU: a hit does not protect an entry from FIFO eviction.
TEST_F(FIFOCacheTest, LookupDoesNotReorder) {
  NewCache(/*capacity*/ 3);
  EXPECT_OK(Insert("a"));
  EXPECT_OK(Insert("b"));
  EXPECT_OK(Insert("c"));
  EXPECT_TRUE(LookupBool("a"));
  ValidateFIFOList({"a", "b", "c"});
  EXPECT_OK(Insert("d"));
  EXPECT_FALSE(LookupBool("a"));
  ValidateFIFOList({"b", "c", "d"});
}

// R5: a pin/release cycle does not change insertion position.
TEST_F(FIFOCacheTest, ReleaseDoesNotReorder) {
  NewCache(/*capacity*/ 3);
  EXPECT_OK(Insert("a"));
  EXPECT_OK(Insert("b"));
  EXPECT_OK(Insert("c"));
  FIFOHandle* h = Lookup("b");
  ASSERT_NE(nullptr, h);
  EXPECT_FALSE(cache_->Release(h, true, false));
  ValidateFIFOList({"a", "b", "c"});
  EXPECT_OK(Insert("d"));
  EXPECT_FALSE(LookupBool("a"));
  EXPECT_TRUE(LookupBool("b"));
}

// R2: a pinned entry survives eviction pressure; the next unpinned goes.
TEST_F(FIFOCacheTest, PinnedEntriesSurviveEviction) {
  NewCache(/*capacity*/ 3);
  EXPECT_OK(Insert("a"));
  EXPECT_OK(Insert("b"));
  EXPECT_OK(Insert("c"));
  FIFOHandle* pinned = Lookup("a");
  ASSERT_NE(nullptr, pinned);
  EXPECT_OK(Insert("d"));
  EXPECT_TRUE(LookupBool("a"));
  EXPECT_FALSE(LookupBool("b"));
  EXPECT_TRUE(LookupBool("c"));
  EXPECT_TRUE(LookupBool("d"));
  EXPECT_FALSE(cache_->Release(pinned, true, false));
  // The scan that evicted b moved pinned a behind it.
  ValidateFIFOList({"c", "a", "d"});
}

// A pinned entry the eviction scan meets moves to the tail, so the next
// eviction starts at an unpinned entry instead of re-walking the pins.
TEST_F(FIFOCacheTest, PinnedEntriesMoveBehindTheScan) {
  NewCache(/*capacity*/ 5);
  FIFOHandle* p1 = nullptr;
  FIFOHandle* p2 = nullptr;
  EXPECT_OK(Insert("p1", 1, &p1));
  EXPECT_OK(Insert("p2", 1, &p2));
  EXPECT_OK(Insert("c0"));
  EXPECT_OK(Insert("c1"));
  EXPECT_OK(Insert("c2"));
  EXPECT_OK(Insert("d"));
  ValidateFIFOList({"c1", "c2", "p1", "p2", "d"});
  EXPECT_OK(Insert("e"));
  ValidateFIFOList({"c2", "p1", "p2", "d", "e"});
  EXPECT_FALSE(cache_->Release(p1, true, false));
  EXPECT_FALSE(cache_->Release(p2, true, false));
}

// R2: eviction skips over multiple pinned entries and keeps scanning.
TEST_F(FIFOCacheTest, EvictionSkipsMultiplePinned) {
  NewCache(/*capacity*/ 4);
  EXPECT_OK(Insert("a"));
  EXPECT_OK(Insert("b"));
  EXPECT_OK(Insert("c"));
  EXPECT_OK(Insert("d"));
  FIFOHandle* pa = Lookup("a");
  FIFOHandle* pb = Lookup("b");
  ASSERT_NE(nullptr, pa);
  ASSERT_NE(nullptr, pb);
  EXPECT_OK(Insert("e"));
  EXPECT_OK(Insert("f"));
  EXPECT_TRUE(LookupBool("a"));
  EXPECT_TRUE(LookupBool("b"));
  EXPECT_FALSE(LookupBool("c"));
  EXPECT_FALSE(LookupBool("d"));
  EXPECT_TRUE(LookupBool("e"));
  EXPECT_TRUE(LookupBool("f"));
  EXPECT_FALSE(cache_->Release(pa, true, false));
  EXPECT_FALSE(cache_->Release(pb, true, false));
  ValidateFIFOList({"a", "b", "e", "f"});
}

// R2: an all-pinned shard evicts nothing and terminates.
TEST_F(FIFOCacheTest, AllPinnedEvictsNothing) {
  NewCache(/*capacity*/ 2);
  EXPECT_OK(Insert("a"));
  EXPECT_OK(Insert("b"));
  FIFOHandle* pa = Lookup("a");
  FIFOHandle* pb = Lookup("b");
  ASSERT_NE(nullptr, pa);
  ASSERT_NE(nullptr, pb);
  // Non-strict insert without a handle is dropped but returns OK.
  EXPECT_OK(Insert("c"));
  EXPECT_FALSE(LookupBool("c"));
  EXPECT_EQ(2, cache_->GetUsage());
  EXPECT_EQ(2, cache_->GetOccupancyCount());
  EXPECT_FALSE(cache_->Release(pa, true, false));
  EXPECT_FALSE(cache_->Release(pb, true, false));
  EXPECT_TRUE(LookupBool("a"));
  EXPECT_TRUE(LookupBool("b"));
}

// R2: non-strict insert with a handle over a fully-pinned shard exceeds
// capacity rather than evicting.
TEST_F(FIFOCacheTest, AllPinnedInsertWithHandleExceedsCapacity) {
  NewCache(/*capacity*/ 2);
  EXPECT_OK(Insert("a"));
  EXPECT_OK(Insert("b"));
  FIFOHandle* pa = Lookup("a");
  FIFOHandle* pb = Lookup("b");
  FIFOHandle* pc = nullptr;
  EXPECT_OK(Insert("c", /*charge*/ 1, &pc));
  ASSERT_NE(nullptr, pc);
  EXPECT_EQ(3, cache_->GetUsage());
  EXPECT_EQ(3, cache_->GetPinnedUsage());
  EXPECT_TRUE(cache_->Release(pa, true, false));
  EXPECT_FALSE(cache_->Release(pb, true, false));
  EXPECT_FALSE(cache_->Release(pc, true, false));
  EXPECT_TRUE(LookupBool("b"));
  EXPECT_TRUE(LookupBool("c"));
  EXPECT_EQ(2, cache_->GetUsage());
}

// R3: usage and pinned usage stay exact across pin, ref, release, erase.
TEST_F(FIFOCacheTest, VariableChargeAccounting) {
  NewCache(/*capacity*/ 10);
  EXPECT_OK(Insert("a", /*charge*/ 3));
  EXPECT_OK(Insert("b", /*charge*/ 5));
  EXPECT_EQ(8, cache_->GetUsage());
  EXPECT_EQ(0, cache_->GetPinnedUsage());
  FIFOHandle* h = Lookup("a");
  ASSERT_NE(nullptr, h);
  EXPECT_EQ(3, cache_->GetPinnedUsage());
  EXPECT_TRUE(cache_->Ref(h));
  EXPECT_EQ(3, cache_->GetPinnedUsage());
  EXPECT_FALSE(cache_->Release(h, true, false));
  EXPECT_EQ(3, cache_->GetPinnedUsage());
  EXPECT_FALSE(cache_->Release(h, true, false));
  EXPECT_EQ(0, cache_->GetPinnedUsage());
  EXPECT_EQ(8, cache_->GetUsage());
  Erase("b");
  EXPECT_EQ(3, cache_->GetUsage());
  EXPECT_EQ(1, cache_->GetOccupancyCount());
}

// R3: a single large oldest entry can cover the whole deficit.
TEST_F(FIFOCacheTest, MixedChargesSingleVictim) {
  NewCache(/*capacity*/ 10);
  EXPECT_OK(Insert("a", /*charge*/ 6));
  EXPECT_OK(Insert("b", /*charge*/ 2));
  EXPECT_OK(Insert("c", /*charge*/ 2));
  EXPECT_OK(Insert("d", /*charge*/ 5));
  EXPECT_FALSE(LookupBool("a"));
  EXPECT_TRUE(LookupBool("b"));
  EXPECT_TRUE(LookupBool("c"));
  EXPECT_TRUE(LookupBool("d"));
  EXPECT_EQ(9, cache_->GetUsage());
  ValidateFIFOList({"b", "c", "d"});
}

// R3: eviction takes as many oldest entries as needed.
TEST_F(FIFOCacheTest, MixedChargesMultiVictim) {
  NewCache(/*capacity*/ 10);
  EXPECT_OK(Insert("a", /*charge*/ 2));
  EXPECT_OK(Insert("b", /*charge*/ 2));
  EXPECT_OK(Insert("c", /*charge*/ 2));
  EXPECT_OK(Insert("d", /*charge*/ 2));
  EXPECT_OK(Insert("e", /*charge*/ 6));
  EXPECT_FALSE(LookupBool("a"));
  EXPECT_FALSE(LookupBool("b"));
  EXPECT_TRUE(LookupBool("c"));
  EXPECT_TRUE(LookupBool("d"));
  EXPECT_TRUE(LookupBool("e"));
  EXPECT_EQ(10, cache_->GetUsage());
  ValidateFIFOList({"c", "d", "e"});
}

// R1/R3: overwrite reports OkOverwritten and replaces charge.
TEST_F(FIFOCacheTest, OverwriteUnpinned) {
  NewCache(/*capacity*/ 10);
  EXPECT_OK(Insert("a", /*charge*/ 2));
  Status s = Insert("a", /*charge*/ 5);
  EXPECT_TRUE(s.IsOkOverwritten());
  EXPECT_EQ(5, cache_->GetUsage());
  EXPECT_EQ(1, cache_->GetOccupancyCount());
  FIFOHandle* h = Lookup("a");
  ASSERT_NE(nullptr, h);
  EXPECT_EQ(5, h->GetCharge(kDontChargeCacheMetadata));
  EXPECT_FALSE(cache_->Release(h, true, false));
}

// R1/R3: overwriting a pinned entry keeps the old charge until release.
TEST_F(FIFOCacheTest, OverwritePinnedKeepsOldCharged) {
  NewCache(/*capacity*/ 10);
  FIFOHandle* old = nullptr;
  EXPECT_OK(Insert("a", /*charge*/ 2, &old));
  ASSERT_NE(nullptr, old);
  Status s = Insert("a", /*charge*/ 5);
  EXPECT_TRUE(s.IsOkOverwritten());
  EXPECT_EQ(7, cache_->GetUsage());
  EXPECT_EQ(2, cache_->GetPinnedUsage());
  EXPECT_EQ(1, cache_->GetOccupancyCount());
  FIFOHandle* cur = Lookup("a");
  ASSERT_NE(nullptr, cur);
  EXPECT_EQ(5, cur->GetCharge(kDontChargeCacheMetadata));
  EXPECT_EQ(7, cache_->GetPinnedUsage());
  EXPECT_TRUE(cache_->Release(old, true, false));
  EXPECT_EQ(5, cache_->GetUsage());
  EXPECT_FALSE(cache_->Release(cur, true, false));
  EXPECT_EQ(0, cache_->GetPinnedUsage());
}

// R1/R3: standalone entries are unfindable but charged.
TEST_F(FIFOCacheTest, CreateStandaloneCharged) {
  NewCache(/*capacity*/ 10);
  FIFOHandle* h = cache_->CreateStandalone("s", 0 /*hash*/, nullptr,
                                           &kNoopCacheItemHelper, /*charge*/ 4,
                                           /*allow_uncharged*/ false);
  ASSERT_NE(nullptr, h);
  EXPECT_TRUE(h->IsStandalone());
  EXPECT_EQ(nullptr, Lookup("s"));
  EXPECT_EQ(4, cache_->GetUsage());
  EXPECT_EQ(4, cache_->GetPinnedUsage());
  EXPECT_EQ(0, cache_->GetOccupancyCount());
  EXPECT_TRUE(cache_->Release(h, true, false));
  EXPECT_EQ(0, cache_->GetUsage());
  EXPECT_EQ(0, cache_->GetPinnedUsage());
}

// R3: strict + no room + allow_uncharged creates a zero-charge entry.
TEST_F(FIFOCacheTest, CreateStandaloneUnchargedAllowed) {
  NewCache(/*capacity*/ 5, /*strict*/ true);
  FIFOHandle* pinned = nullptr;
  EXPECT_OK(Insert("a", /*charge*/ 5, &pinned));
  ASSERT_NE(nullptr, pinned);
  FIFOHandle* h = cache_->CreateStandalone("s", 0 /*hash*/, nullptr,
                                           &kNoopCacheItemHelper, /*charge*/ 3,
                                           /*allow_uncharged*/ true);
  ASSERT_NE(nullptr, h);
  EXPECT_EQ(0, h->GetCharge(kDontChargeCacheMetadata));
  EXPECT_EQ(5, cache_->GetUsage());
  EXPECT_EQ(5, cache_->GetPinnedUsage());
  EXPECT_TRUE(cache_->Release(h, true, false));
  EXPECT_EQ(5, cache_->GetUsage());
  EXPECT_FALSE(cache_->Release(pinned, true, false));
  EXPECT_EQ(5, cache_->GetUsage());
}

// R3: strict + no room + !allow_uncharged fails.
TEST_F(FIFOCacheTest, CreateStandaloneUnchargedNotAllowed) {
  NewCache(/*capacity*/ 5, /*strict*/ true);
  FIFOHandle* pinned = nullptr;
  EXPECT_OK(Insert("a", /*charge*/ 5, &pinned));
  ASSERT_NE(nullptr, pinned);
  FIFOHandle* h = cache_->CreateStandalone("s", 0 /*hash*/, nullptr,
                                           &kNoopCacheItemHelper, /*charge*/ 3,
                                           /*allow_uncharged*/ false);
  EXPECT_EQ(nullptr, h);
  EXPECT_EQ(5, cache_->GetUsage());
  EXPECT_FALSE(cache_->Release(pinned, true, false));
}

// R3: metadata charging adds overhead but preserves logical charge.
TEST_F(FIFOCacheTest, FullMetadataCharge) {
  NewCache(/*capacity*/ 100000, /*strict*/ false, kFullChargeCacheMetadata);
  EXPECT_OK(Insert("a", /*charge*/ 1));
  FIFOHandle* h = Lookup("a");
  ASSERT_NE(nullptr, h);
  EXPECT_EQ(1, h->GetCharge(kFullChargeCacheMetadata));
  EXPECT_GT(cache_->GetUsage(), 1);
  EXPECT_EQ(cache_->GetUsage(),
            cache_->GetPinnedUsage() +
                cache_->GetTableAddressCount() * sizeof(FIFOHandle*));
  EXPECT_FALSE(cache_->Release(h, true, false));
  EXPECT_EQ(0, cache_->GetPinnedUsage());
  EXPECT_GT(cache_->GetUsage(), 1);
}

// R4: strict, no handle, no room -> OK but not inserted.
TEST_F(FIFOCacheTest, StrictInsertWithoutHandleDropped) {
  NewCache(/*capacity*/ 3, /*strict*/ true);
  EXPECT_OK(Insert("a"));
  EXPECT_OK(Insert("b"));
  EXPECT_OK(Insert("c"));
  FIFOHandle* pa = Lookup("a");
  FIFOHandle* pb = Lookup("b");
  FIFOHandle* pc = Lookup("c");
  ASSERT_NE(nullptr, pa);
  ASSERT_NE(nullptr, pb);
  ASSERT_NE(nullptr, pc);
  EXPECT_OK(Insert("d"));
  EXPECT_EQ(nullptr, Lookup("d"));
  EXPECT_EQ(3, cache_->GetUsage());
  EXPECT_EQ(3, cache_->GetOccupancyCount());
  EXPECT_FALSE(cache_->Release(pa, true, false));
  EXPECT_FALSE(cache_->Release(pb, true, false));
  EXPECT_FALSE(cache_->Release(pc, true, false));
}

// R4: strict, with handle, no room -> MemoryLimit.
TEST_F(FIFOCacheTest, StrictInsertWithHandleFails) {
  NewCache(/*capacity*/ 3, /*strict*/ true);
  EXPECT_OK(Insert("a"));
  EXPECT_OK(Insert("b"));
  EXPECT_OK(Insert("c"));
  FIFOHandle* pa = Lookup("a");
  FIFOHandle* pb = Lookup("b");
  FIFOHandle* pc = Lookup("c");
  ASSERT_NE(nullptr, pa);
  ASSERT_NE(nullptr, pb);
  ASSERT_NE(nullptr, pc);
  FIFOHandle* pd = nullptr;
  Status s = Insert("d", /*charge*/ 1, &pd);
  EXPECT_TRUE(s.IsMemoryLimit());
  EXPECT_EQ(nullptr, pd);
  EXPECT_EQ(nullptr, Lookup("d"));
  EXPECT_EQ(3, cache_->GetUsage());
  EXPECT_FALSE(cache_->Release(pa, true, false));
  EXPECT_FALSE(cache_->Release(pb, true, false));
  EXPECT_FALSE(cache_->Release(pc, true, false));
}

// R4 with mixed charges: both strict branches on one pinned shard.
TEST_F(FIFOCacheTest, StrictMixedChargesBothBranches) {
  NewCache(/*capacity*/ 10, /*strict*/ true);
  FIFOHandle* pa = nullptr;
  FIFOHandle* pb = nullptr;
  EXPECT_OK(Insert("a", /*charge*/ 6, &pa));
  EXPECT_OK(Insert("b", /*charge*/ 4, &pb));
  ASSERT_NE(nullptr, pa);
  ASSERT_NE(nullptr, pb);
  EXPECT_OK(Insert("c", /*charge*/ 5));
  EXPECT_EQ(nullptr, Lookup("c"));
  FIFOHandle* pc = nullptr;
  Status s = Insert("c", /*charge*/ 5, &pc);
  EXPECT_TRUE(s.IsMemoryLimit());
  EXPECT_EQ(nullptr, pc);
  EXPECT_EQ(10, cache_->GetUsage());
  EXPECT_FALSE(cache_->Release(pa, true, false));
  EXPECT_FALSE(cache_->Release(pb, true, false));
}

// R4: strict still evicts unpinned entries to make room.
TEST_F(FIFOCacheTest, StrictEvictsUnpinned) {
  NewCache(/*capacity*/ 3, /*strict*/ true);
  EXPECT_OK(Insert("a"));
  EXPECT_OK(Insert("b"));
  EXPECT_OK(Insert("c"));
  FIFOHandle* pd = nullptr;
  EXPECT_OK(Insert("d", /*charge*/ 1, &pd));
  ASSERT_NE(nullptr, pd);
  EXPECT_FALSE(LookupBool("a"));
  EXPECT_TRUE(LookupBool("d"));
  EXPECT_FALSE(cache_->Release(pd, true, false));
}

// R1: erase of a pinned entry detaches it; charge drops on last release.
TEST_F(FIFOCacheTest, ErasePinnedDetaches) {
  NewCache(/*capacity*/ 10);
  EXPECT_OK(Insert("a", /*charge*/ 3));
  FIFOHandle* h = Lookup("a");
  ASSERT_NE(nullptr, h);
  Erase("a");
  EXPECT_EQ(nullptr, Lookup("a"));
  EXPECT_EQ(3, cache_->GetUsage());
  EXPECT_EQ(3, cache_->GetPinnedUsage());
  EXPECT_EQ(0, cache_->GetOccupancyCount());
  EXPECT_TRUE(cache_->Release(h, true, false));
  EXPECT_EQ(0, cache_->GetUsage());
}

// R1: erasing a missing key is a no-op.
TEST_F(FIFOCacheTest, EraseMissingIsNoop) {
  NewCache(/*capacity*/ 10);
  EXPECT_OK(Insert("a"));
  Erase("missing");
  EXPECT_EQ(1, cache_->GetUsage());
  EXPECT_TRUE(LookupBool("a"));
}

// R1: release with erase_if_last_ref removes an under-capacity entry.
TEST_F(FIFOCacheTest, ReleaseEraseIfLastRef) {
  NewCache(/*capacity*/ 10);
  EXPECT_OK(Insert("a"));
  EXPECT_OK(Insert("b"));
  FIFOHandle* h = Lookup("a");
  ASSERT_NE(nullptr, h);
  EXPECT_TRUE(cache_->Release(h, true, true /*erase*/));
  EXPECT_EQ(nullptr, Lookup("a"));
  EXPECT_EQ(1, cache_->GetUsage());
  EXPECT_TRUE(LookupBool("b"));
}

// R1: shrinking capacity evicts oldest-first; growing keeps everything.
TEST_F(FIFOCacheTest, SetCapacity) {
  NewCache(/*capacity*/ 5);
  EXPECT_OK(Insert("a"));
  EXPECT_OK(Insert("b"));
  EXPECT_OK(Insert("c"));
  EXPECT_OK(Insert("d"));
  EXPECT_OK(Insert("e"));
  cache_->SetCapacity(2);
  EXPECT_FALSE(LookupBool("a"));
  EXPECT_FALSE(LookupBool("b"));
  EXPECT_FALSE(LookupBool("c"));
  EXPECT_TRUE(LookupBool("d"));
  EXPECT_TRUE(LookupBool("e"));
  EXPECT_EQ(2, cache_->GetUsage());
  cache_->SetCapacity(10);
  EXPECT_EQ(2, cache_->GetUsage());
  EXPECT_TRUE(LookupBool("d"));
}

// R1: toggling the strict limit changes insert behavior.
TEST_F(FIFOCacheTest, SetStrictCapacityLimit) {
  NewCache(/*capacity*/ 1, /*strict*/ false);
  FIFOHandle* pa = nullptr;
  EXPECT_OK(Insert("a", /*charge*/ 1, &pa));
  ASSERT_NE(nullptr, pa);
  cache_->SetStrictCapacityLimit(true);
  FIFOHandle* pb = nullptr;
  Status s = Insert("b", /*charge*/ 1, &pb);
  EXPECT_TRUE(s.IsMemoryLimit());
  cache_->SetStrictCapacityLimit(false);
  EXPECT_OK(Insert("b", /*charge*/ 1, &pb));
  ASSERT_NE(nullptr, pb);
  EXPECT_EQ(2, cache_->GetUsage());
  EXPECT_TRUE(cache_->Release(pa, true, false));
  EXPECT_FALSE(cache_->Release(pb, true, false));
}

// R1: occupancy counts table entries; address count is a positive load
// factor denominator.
TEST_F(FIFOCacheTest, OccupancyAndTableAddresses) {
  NewCache(/*capacity*/ 10);
  EXPECT_EQ(0, cache_->GetOccupancyCount());
  EXPECT_OK(Insert("a"));
  EXPECT_OK(Insert("b"));
  EXPECT_EQ(2, cache_->GetOccupancyCount());
  EXPECT_GT(cache_->GetTableAddressCount(), 0);
  FIFOHandle* h = cache_->CreateStandalone("s", 0 /*hash*/, nullptr,
                                           &kNoopCacheItemHelper, /*charge*/ 1,
                                           /*allow_uncharged*/ false);
  ASSERT_NE(nullptr, h);
  EXPECT_EQ(2, cache_->GetOccupancyCount());
  EXPECT_TRUE(cache_->Release(h, true, false));
}

// R1: batched iteration visits every entry exactly once.
TEST_F(FIFOCacheTest, ApplyToSomeEntriesCoversAll) {
  NewCache(/*capacity*/ 100);
  for (int i = 0; i < 10; i++) {
    EXPECT_OK(Insert("key" + std::to_string(i)));
  }
  std::set<std::string> seen;
  size_t total = 0;
  size_t state = 0;
  while (state != SIZE_MAX) {
    cache_->ApplyToSomeEntries(
        [&](const Slice& key, Cache::ObjectPtr /*value*/, size_t charge,
            const Cache::CacheItemHelper* /*helper*/) {
          EXPECT_EQ(1, charge);
          seen.insert(key.ToString());
          total++;
        },
        /*average_entries_per_lock*/ 3, &state);
  }
  EXPECT_EQ(10, total);
  EXPECT_EQ(10, seen.size());
}

// R1: EraseUnRefEntries drops only unpinned entries.
TEST_F(FIFOCacheTest, EraseUnRefEntriesKeepsPinned) {
  NewCache(/*capacity*/ 10);
  EXPECT_OK(Insert("a", /*charge*/ 2));
  EXPECT_OK(Insert("b", /*charge*/ 3));
  EXPECT_OK(Insert("c", /*charge*/ 4));
  FIFOHandle* h = Lookup("b");
  ASSERT_NE(nullptr, h);
  cache_->EraseUnRefEntries();
  EXPECT_EQ(nullptr, Lookup("a"));
  EXPECT_EQ(nullptr, Lookup("c"));
  EXPECT_EQ(3, cache_->GetUsage());
  EXPECT_EQ(3, cache_->GetPinnedUsage());
  EXPECT_FALSE(cache_->Release(h, true, false));
  EXPECT_TRUE(LookupBool("b"));
  EXPECT_EQ(3, cache_->GetUsage());
}

// R1: secondary-cache lookup parameters are ignored without secondary.
TEST_F(FIFOCacheTest, LookupIgnoresSecondaryParameters) {
  NewCache(/*capacity*/ 10);
  EXPECT_OK(Insert("a"));
  Cache::CreateContext context;
  Cache::CacheItemHelper secondary_helper(CacheEntryRole::kMisc, nullptr,
                                          DummySize, DummySaveTo, DummyCreate,
                                          &kNoopCacheItemHelper);
  FIFOHandle* h = cache_->Lookup("a", 0, &secondary_helper, &context,
                                 Cache::Priority::HIGH, nullptr);
  ASSERT_NE(nullptr, h);
  EXPECT_FALSE(cache_->Release(h, true, false));
  EXPECT_EQ(nullptr, cache_->Lookup("missing", 0, &secondary_helper, &context,
                                    Cache::Priority::HIGH, nullptr));
}

// R1: priorities do not affect FIFO order.
TEST_F(FIFOCacheTest, PriorityIgnored) {
  NewCache(/*capacity*/ 2);
  EXPECT_OK(Insert("a", /*charge*/ 1, nullptr, Cache::Priority::HIGH));
  EXPECT_OK(Insert("b", /*charge*/ 1, nullptr, Cache::Priority::BOTTOM));
  ValidateFIFOList({"a", "b"});
  EXPECT_OK(Insert("c", /*charge*/ 1, nullptr, Cache::Priority::HIGH));
  EXPECT_FALSE(LookupBool("a"));
  EXPECT_TRUE(LookupBool("b"));
  EXPECT_TRUE(LookupBool("c"));
}

// R6: factory builds a working cache behind the Cache interface.
TEST_F(FIFOCacheTest, MakeSharedCacheBasic) {
  FIFOCacheOptions opts(/*capacity*/ 10, /*num_shard_bits*/ 0,
                        /*strict_capacity_limit*/ false, nullptr,
                        kDefaultToAdaptiveMutex, kDontChargeCacheMetadata);
  std::shared_ptr<Cache> cache = opts.MakeSharedCache();
  ASSERT_NE(nullptr, cache);
  EXPECT_STREQ("FIFOCache", cache->Name());
  EXPECT_EQ(10, cache->GetCapacity());
  EXPECT_FALSE(cache->HasStrictCapacityLimit());
  EXPECT_OK(cache->Insert("k1", reinterpret_cast<Cache::ObjectPtr>(42),
                          &kNoopCacheItemHelper, /*charge*/ 1));
  Cache::Handle* h = cache->Lookup("k1");
  ASSERT_NE(nullptr, h);
  EXPECT_EQ(42, reinterpret_cast<uintptr_t>(cache->Value(h)));
  EXPECT_EQ(1, cache->GetCharge(h));
  EXPECT_EQ(&kNoopCacheItemHelper, cache->GetCacheItemHelper(h));
  EXPECT_EQ(1, cache->GetUsage());
  EXPECT_EQ(1, cache->GetPinnedUsage());
  cache->Release(h);
  EXPECT_EQ(0, cache->GetPinnedUsage());
  cache->Erase("k1");
  EXPECT_EQ(nullptr, cache->Lookup("k1"));
  EXPECT_EQ(0, cache->GetUsage());
}

// R5/R6: FIFO order holds through the sharded Cache interface (1 shard).
TEST_F(FIFOCacheTest, CacheInterfaceFifoOrder) {
  FIFOCacheOptions opts(/*capacity*/ 3, /*num_shard_bits*/ 0,
                        /*strict_capacity_limit*/ false, nullptr,
                        kDefaultToAdaptiveMutex, kDontChargeCacheMetadata);
  std::shared_ptr<Cache> cache = opts.MakeSharedCache();
  ASSERT_NE(nullptr, cache);
  EXPECT_OK(cache->Insert("a", nullptr, &kNoopCacheItemHelper, 1));
  EXPECT_OK(cache->Insert("b", nullptr, &kNoopCacheItemHelper, 1));
  EXPECT_OK(cache->Insert("c", nullptr, &kNoopCacheItemHelper, 1));
  EXPECT_OK(cache->Insert("d", nullptr, &kNoopCacheItemHelper, 1));
  EXPECT_EQ(nullptr, cache->Lookup("a"));
  for (const char* k : {"b", "c", "d"}) {
    Cache::Handle* h = cache->Lookup(k);
    EXPECT_NE(nullptr, h) << k;
    if (h) {
      cache->Release(h);
    }
  }
  EXPECT_EQ(3, cache->GetOccupancyCount());
  EXPECT_GT(cache->GetTableAddressCount(), 0);
}

// R4/R6: strict asymmetry through the Cache interface.
TEST_F(FIFOCacheTest, CacheInterfaceStrict) {
  FIFOCacheOptions opts(/*capacity*/ 2, /*num_shard_bits*/ 0,
                        /*strict_capacity_limit*/ true, nullptr,
                        kDefaultToAdaptiveMutex, kDontChargeCacheMetadata);
  std::shared_ptr<Cache> cache = opts.MakeSharedCache();
  ASSERT_NE(nullptr, cache);
  EXPECT_OK(cache->Insert("a", nullptr, &kNoopCacheItemHelper, 1));
  EXPECT_OK(cache->Insert("b", nullptr, &kNoopCacheItemHelper, 1));
  Cache::Handle* pa = cache->Lookup("a");
  Cache::Handle* pb = cache->Lookup("b");
  ASSERT_NE(nullptr, pa);
  ASSERT_NE(nullptr, pb);
  EXPECT_OK(cache->Insert("c", nullptr, &kNoopCacheItemHelper, 1));
  EXPECT_EQ(nullptr, cache->Lookup("c"));
  Cache::Handle* pc = nullptr;
  Status s = cache->Insert("c", nullptr, &kNoopCacheItemHelper, 1, &pc);
  EXPECT_TRUE(s.IsMemoryLimit());
  EXPECT_EQ(nullptr, pc);
  cache->Release(pa);
  cache->Release(pb);
}

// R6: option validation and defaults.
TEST_F(FIFOCacheTest, MakeSharedCacheValidation) {
  FIFOCacheOptions bad(/*capacity*/ 10, /*num_shard_bits*/ 20,
                       /*strict_capacity_limit*/ false);
  EXPECT_EQ(nullptr, bad.MakeSharedCache());
  // Large enough for the bucket array, charged under the default
  // kFullChargeCacheMetadata.
  FIFOCacheOptions dflt(/*capacity*/ 1000, /*num_shard_bits*/ -1,
                        /*strict_capacity_limit*/ false);
  std::shared_ptr<Cache> cache = dflt.MakeSharedCache();
  ASSERT_NE(nullptr, cache);
  EXPECT_OK(cache->Insert("a", nullptr, &kNoopCacheItemHelper, 1));
  Cache::Handle* h = cache->Lookup("a");
  EXPECT_NE(nullptr, h);
  if (h) {
    cache->Release(h);
  }
}

// R1/R6: ApplyToAllEntries and EraseUnRefEntries through the interface.
TEST_F(FIFOCacheTest, CacheInterfaceBulkOps) {
  FIFOCacheOptions opts(/*capacity*/ 100, /*num_shard_bits*/ 0,
                        /*strict_capacity_limit*/ false, nullptr,
                        kDefaultToAdaptiveMutex, kDontChargeCacheMetadata);
  std::shared_ptr<Cache> cache = opts.MakeSharedCache();
  ASSERT_NE(nullptr, cache);
  for (int i = 0; i < 5; i++) {
    EXPECT_OK(cache->Insert("k" + std::to_string(i), nullptr,
                            &kNoopCacheItemHelper, 1));
  }
  size_t total = 0;
  cache->ApplyToAllEntries(
      [&](const Slice& /*key*/, Cache::ObjectPtr /*value*/, size_t charge,
          const Cache::CacheItemHelper* /*helper*/) {
        EXPECT_EQ(1, charge);
        total++;
      },
      {});
  EXPECT_EQ(5, total);
  Cache::Handle* pinned = cache->Lookup("k0");
  ASSERT_NE(nullptr, pinned);
  cache->EraseUnRefEntries();
  EXPECT_EQ(1, cache->GetUsage());
  EXPECT_EQ(nullptr, cache->Lookup("k1"));
  cache->Release(pinned);
  Cache::Handle* h = cache->Lookup("k0");
  EXPECT_NE(nullptr, h);
  if (h) {
    cache->Release(h);
  }
  EXPECT_EQ(0, cache->GetPinnedUsage());
  EXPECT_EQ(1, cache->GetUsage());
}

TEST_F(FIFOCacheTest, MetadataChargeIncludesTableAddresses) {
  NewCache(/*capacity*/ 1 << 20, /*strict_capacity_limit*/ false,
           kFullChargeCacheMetadata);
  const size_t empty_usage = cache_->GetUsage();
  EXPECT_GT(empty_usage, 0);
  EXPECT_EQ(0, cache_->TEST_GetTableOccupancyCount());

  for (int i = 0; i < 32; ++i) {
    EXPECT_OK(Insert("k" + std::to_string(i)));
  }

  EXPECT_EQ(32, cache_->GetOccupancyCount());
  EXPECT_EQ(32, cache_->TEST_GetTableOccupancyCount());
  EXPECT_GT(cache_->GetUsage(), empty_usage + 32);
  EXPECT_GT(cache_->GetTableAddressCount(), 16);
}

TEST_F(FIFOCacheTest, ApplyToAllEntriesCoversEachKeyOnceAcrossResize) {
  NewCache(/*capacity*/ 1 << 20);
  auto insert_hashed = [&](const std::string& key) {
    EXPECT_OK(cache_->Insert(key, FIFOCacheShard::ComputeHash(key, 0), nullptr,
                             &kNoopCacheItemHelper, 1, nullptr,
                             Cache::Priority::LOW));
  };
  for (int i = 0; i < 15; ++i) {
    insert_hashed("k" + std::to_string(i));
  }
  const size_t length_before = cache_->GetTableAddressCount();

  std::multiset<std::string> visited;
  auto record = [&](const Slice& key, Cache::ObjectPtr /*value*/,
                    size_t /*charge*/,
                    const Cache::CacheItemHelper* /*helper*/) {
    visited.insert(key.ToString());
  };
  size_t state = 0;
  cache_->ApplyToSomeEntries(record, length_before / 2, &state);
  ASSERT_NE(SIZE_MAX, state);
  EXPECT_GT(visited.size(), 0);
  EXPECT_LT(visited.size(), 15);

  // ShardedCache::ApplyToAllEntries releases the shard mutex between calls,
  // so an insert may grow the table here.
  for (int i = 15; i < 64; ++i) {
    insert_hashed("k" + std::to_string(i));
  }
  ASSERT_GT(cache_->GetTableAddressCount(), length_before);
  while (state != SIZE_MAX) {
    cache_->ApplyToSomeEntries(record, /*average_entries_per_lock*/ 4, &state);
  }

  for (int i = 0; i < 15; ++i) {
    EXPECT_EQ(1, visited.count("k" + std::to_string(i)));
  }
  for (const std::string& key : visited) {
    EXPECT_EQ(1, visited.count(key)) << key;
  }
}

TEST_F(FIFOCacheTest, ConcurrentOpsKeepUsageConsistent) {
  FIFOCacheOptions opts(/*capacity*/ 1000, /*num_shard_bits*/ 2,
                        /*strict_capacity_limit*/ false);
  opts.metadata_charge_policy = kDontChargeCacheMetadata;
  std::shared_ptr<Cache> cache = opts.MakeSharedCache();
  ASSERT_NE(nullptr, cache);

  constexpr int kThreads = 4;
  constexpr int kOpsPerThread = 20000;
  std::vector<port::Thread> threads;
  threads.reserve(kThreads);
  for (int t = 0; t < kThreads; ++t) {
    threads.emplace_back([&cache, t]() {
      Random rnd(301 + t);
      std::vector<Cache::Handle*> held;
      for (int i = 0; i < kOpsPerThread; ++i) {
        const std::string key = "k" + std::to_string(rnd.Uniform(400));
        switch (rnd.Uniform(6)) {
          case 0: {
            Cache::Handle* h = nullptr;
            EXPECT_OK(cache->Insert(key, nullptr, &kNoopCacheItemHelper,
                                    1 + rnd.Uniform(10),
                                    rnd.OneIn(2) ? &h : nullptr));
            if (h != nullptr) {
              held.push_back(h);
            }
            break;
          }
          case 1:
          case 2: {
            Cache::Handle* h = cache->Lookup(key);
            if (h != nullptr) {
              held.push_back(h);
            }
            break;
          }
          case 3:
            cache->Erase(key);
            break;
          case 4:
            if (rnd.OneIn(50)) {
              cache->SetCapacity(rnd.OneIn(2) ? 500 : 1000);
            }
            break;
          default:
            if (rnd.OneIn(50)) {
              cache->ApplyToAllEntries(
                  [](const Slice& /*key*/, Cache::ObjectPtr /*value*/,
                     size_t /*charge*/,
                     const Cache::CacheItemHelper* /*helper*/) {},
                  {});
            }
            break;
        }
        if (held.size() > 8 || (!held.empty() && rnd.OneIn(3))) {
          const size_t idx = rnd.Uniform(static_cast<int>(held.size()));
          cache->Release(held[idx], /*erase_if_last_ref*/ rnd.OneIn(10));
          held[idx] = held.back();
          held.pop_back();
        }
      }
      for (Cache::Handle* h : held) {
        cache->Release(h);
      }
    });
  }
  for (port::Thread& thread : threads) {
    thread.join();
  }

  size_t charge_sum = 0;
  cache->ApplyToAllEntries(
      [&](const Slice& /*key*/, Cache::ObjectPtr /*value*/, size_t charge,
          const Cache::CacheItemHelper* /*helper*/) { charge_sum += charge; },
      {});
  EXPECT_EQ(0, cache->GetPinnedUsage());
  EXPECT_EQ(charge_sum, cache->GetUsage());
}

TEST_F(FIFOCacheTest, UsageStaysPayloadOnlyAcrossResizeWithNoMetadataCharge) {
  NewCache(/*capacity*/ 1 << 20, /*strict_capacity_limit*/ false,
           kDontChargeCacheMetadata);
  for (int i = 0; i < 100; ++i) {
    EXPECT_OK(Insert("k" + std::to_string(i), /*charge*/ 2));
  }
  EXPECT_EQ(200, cache_->GetUsage());
  for (int i = 100; i < 500; ++i) {
    EXPECT_OK(Insert("k" + std::to_string(i), /*charge*/ 1));
  }
  EXPECT_EQ(600, cache_->GetUsage());
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

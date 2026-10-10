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
    return cache_->Insert(key, FIFOCacheShard::ComputeHash(key, 0),
                          nullptr /*value*/, &kNoopCacheItemHelper, charge,
                          handle, priority);
  }

  FIFOHandle* Lookup(const std::string& key) {
    return cache_->Lookup(key, FIFOCacheShard::ComputeHash(key, 0), nullptr,
                          nullptr, Cache::Priority::LOW, nullptr);
  }

  bool LookupBool(const std::string& key) {
    FIFOHandle* handle = Lookup(key);
    if (handle) {
      cache_->Release(handle, true /*useful*/, false /*erase*/);
      return true;
    }
    return false;
  }

  void HitNTimes(const std::string& key, int times) {
    for (int i = 0; i < times; i++) {
      FIFOHandle* h = Lookup(key);
      ASSERT_NE(nullptr, h);
      EXPECT_FALSE(cache_->Release(h, true, false));
    }
  }

  // Presence probe that performs no Lookup, so it never credits frequency.
  bool ContainsForTest(const std::string& key) {
    bool found = false;
    size_t state = 0;
    while (state != SIZE_MAX) {
      cache_->ApplyToSomeEntries(
          [&](const Slice& k, Cache::ObjectPtr /*value*/, size_t /*charge*/,
              const Cache::CacheItemHelper* /*helper*/) {
            if (k.ToString() == key) {
              found = true;
            }
          },
          /*average_entries_per_lock*/ 1000, &state);
    }
    return found;
  }

  // Drives resident second-chance laps around "k" and returns the number of
  // resident evictions before "k" itself is evicted. Everything travels the
  // public path: fillers reach resident through pressure promotion (hit
  // twice, then an over-budget probation sweep promotes them while a cold
  // sacrificial covers the eviction). Resident holds exactly 10 entries
  // whenever round pressure is applied, so k is visited once per 9
  // evictions: counter n means exactly 9n evictions before k. Capacity is
  // 11 so a refill (filler plus sacrificial) stages without pressure,
  // while each round still draws from resident first with a single
  // trigger in probation at (not over) the budget of 1. Rounds are capped
  // so a broken termination bound fails instead of hanging.
  int CountEvictionsBeforeK(int hits) {
    NewCache(/*capacity*/ 11);
    EXPECT_OK(Insert("k"));
    HitNTimes("k", 2);
    for (int i = 0; i < 9; i++) {
      std::string y = "y" + std::to_string(i);
      EXPECT_OK(Insert(y));
      HitNTimes(y, 2);
    }
    EXPECT_OK(Insert("s"));
    // Promotes k and y0..y8, evicts s: resident holds k plus 9 cold.
    EXPECT_OK(Insert("t"));
    Erase("t");
    HitNTimes("k", hits);
    int evicted = 0;
    int tag = 9;
    for (int round = 0; round < 40; round++) {
      // Two triggers: the first lands without pressure, the second applies
      // it with probation at (not over) budget, so resident goes first.
      std::string l = "l" + std::to_string(round);
      EXPECT_OK(Insert(l));
      std::string m = "m" + std::to_string(round);
      EXPECT_OK(Insert(m));
      if (!ContainsForTest("k")) {
        break;
      }
      Erase(l);
      Erase(m);
      evicted++;
      // Restore resident to 10 without disturbing it: the sweep promotes
      // the filler and spends the sacrificial.
      std::string y = "y" + std::to_string(tag++);
      EXPECT_OK(Insert(y));
      HitNTimes(y, 2);
      EXPECT_OK(Insert("s" + std::to_string(round)));
      std::string t = "t" + std::to_string(round);
      EXPECT_OK(Insert(t));
      Erase(t);
    }
    EXPECT_FALSE(ContainsForTest("k"));
    return evicted;
  }

  void Erase(const std::string& key) {
    cache_->Erase(key, FIFOCacheShard::ComputeHash(key, 0));
  }

  // Whether key is linked in probation. Performs no Lookup.
  bool InProbationForTest(const std::string& key) {
    FIFOHandle* fifo;
    cache_->TEST_GetFIFOList(&fifo);
    for (FIFOHandle* h = fifo->next; h != fifo; h = h->next) {
      if (h->key().ToString() == key) {
        return true;
      }
    }
    return false;
  }

  void InsertN(const std::string& prefix, int n) {
    for (int i = 0; i < n; i++) {
      ASSERT_OK(Insert(prefix + std::to_string(i)));
    }
  }

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
  FIFOHandle* h = cache_->Lookup("a", FIFOCacheShard::ComputeHash("a", 0),
                                 &secondary_helper, &context,
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

// Rules out promoting at >= 1 (b would survive), at >= 3 or never (a would
// be evicted), and plain FIFO (a, the oldest, would be evicted).
TEST_F(FIFOCacheTest, TwoHitsPromoteOneHitEvicts) {
  NewCache(/*capacity*/ 10);
  EXPECT_OK(Insert("a"));
  HitNTimes("a", 2);
  EXPECT_OK(Insert("b"));
  HitNTimes("b", 1);
  for (int i = 0; i < 8; i++) {
    EXPECT_OK(Insert("c" + std::to_string(i)));
  }
  EXPECT_OK(Insert("x"));
  EXPECT_TRUE(LookupBool("a"));
  EXPECT_FALSE(LookupBool("b"));
  EXPECT_EQ(10, cache_->GetUsage());
}

// Rules out plain FIFO and never-promote: the oldest entry survives pressure
// that evicts newer cold entries because its hits promoted it.
TEST_F(FIFOCacheTest, PromotedEntrySurvivesColdPressure) {
  NewCache(/*capacity*/ 10);
  EXPECT_OK(Insert("old"));
  HitNTimes("old", 2);
  for (int i = 0; i < 9; i++) {
    EXPECT_OK(Insert("c" + std::to_string(i)));
  }
  EXPECT_OK(Insert("new"));
  EXPECT_TRUE(LookupBool("old"));
  EXPECT_FALSE(LookupBool("c0"));
  EXPECT_EQ(10, cache_->GetUsage());
}

// A resident entry with counter n is evicted after exactly 9n other resident
// evictions. Rules out moving to the head instead of the tail (k would go
// after 0), a single reference bit (k with 2 hits would go after 9), and not
// decrementing (k would never go).
TEST_F(FIFOCacheTest, ResidentSecondChanceExactLaps) {
  EXPECT_EQ(0, CountEvictionsBeforeK(0));
  EXPECT_EQ(9, CountEvictionsBeforeK(1));
  EXPECT_EQ(18, CountEvictionsBeforeK(2));
}

// The resident clock evicts in FIFO-reinsertion order: an entry promoted
// while the hand is mid-ring becomes the newest, and erasing the entry under
// the hand moves the hand to the next oldest. Reinsertion order, traced:
// [a1 b0 c0] +u -> a rotates, b evicted -> [c0 u0 a0]; +w -> c evicted
// -> [u0 a0 w0]; erase u -> [a0 w1]; +p -> a evicted. A hand left on the
// erased entry, or moved to its predecessor, would evict w or p instead.
TEST_F(FIFOCacheTest, ResidentClockKeepsReinsertionOrder) {
  std::vector<std::string> evicted;
  eviction_callback_ = [&evicted](const Slice& key, Cache::Handle* /*h*/,
                                  bool /*was_hit*/) {
    evicted.push_back(key.ToString());
    return false;
  };
  NewCache(/*capacity*/ 4);
  for (const char* k : {"a", "b", "c"}) {
    EXPECT_OK(Insert(k));
    HitNTimes(k, 2);
  }
  EXPECT_OK(Insert("s"));
  // Promotes a, b, c in that order and evicts s.
  EXPECT_OK(Insert("t"));
  Erase("t");
  HitNTimes("a", 1);
  EXPECT_OK(Insert("u"));
  HitNTimes("u", 2);
  EXPECT_OK(Insert("v"));
  Erase("v");
  EXPECT_OK(Insert("w"));
  HitNTimes("w", 2);
  EXPECT_OK(Insert("x"));
  Erase("x");
  Erase("u");
  HitNTimes("w", 1);
  EXPECT_OK(Insert("p"));
  HitNTimes("p", 2);
  EXPECT_OK(Insert("q", /*charge*/ 2));
  EXPECT_EQ((std::vector<std::string>{"s", "b", "c", "a"}), evicted);
  EXPECT_TRUE(ContainsForTest("w"));
  EXPECT_TRUE(ContainsForTest("p"));
  EXPECT_TRUE(ContainsForTest("q"));
  EXPECT_EQ(4, cache_->GetUsage());
  eviction_callback_ = nullptr;
}

// Rules out an unbounded counter (100 hits would survive 100 laps worth of
// evictions) and saturation at any value other than 3.
TEST_F(FIFOCacheTest, FrequencySaturatesAtThree) {
  EXPECT_EQ(27, CountEvictionsBeforeK(3));
  EXPECT_EQ(27, CountEvictionsBeforeK(100));
}

// Rules out counting a promoted entry's charge again on entering resident,
// and dropping it.
TEST_F(FIFOCacheTest, PromotionPreservesUsage) {
  NewCache(/*capacity*/ 30);
  EXPECT_OK(Insert("k", /*charge*/ 10));
  HitNTimes("k", 2);
  EXPECT_OK(Insert("a", /*charge*/ 9));
  EXPECT_OK(Insert("b", /*charge*/ 9));
  EXPECT_OK(Insert("c", /*charge*/ 2));
  EXPECT_EQ(30, cache_->GetUsage());
  // Promotes k and evicts a: 30 - 9 + 5.
  EXPECT_OK(Insert("d", /*charge*/ 5));
  EXPECT_TRUE(LookupBool("k"));
  EXPECT_FALSE(LookupBool("a"));
  EXPECT_TRUE(LookupBool("b"));
  EXPECT_TRUE(LookupBool("c"));
  EXPECT_TRUE(LookupBool("d"));
  EXPECT_EQ(26, cache_->GetUsage());
  EXPECT_EQ(0, cache_->GetPinnedUsage());
}

// Priority is ignored: HIGH and LOW entries with the same access pattern
// behave identically. Rules out seeding the counter from priority and
// bypassing probation for HIGH. The first scenario passes on plain FIFO too
// (it pins the R7 decision); the second fails there (it pins promotion).
TEST_F(FIFOCacheTest, PriorityIgnoredForPlacement) {
  NewCache(/*capacity*/ 6);
  EXPECT_OK(Insert("h", /*charge*/ 1, nullptr, Cache::Priority::HIGH));
  EXPECT_OK(Insert("l", /*charge*/ 1, nullptr, Cache::Priority::LOW));
  for (int i = 0; i < 4; i++) {
    EXPECT_OK(Insert("c" + std::to_string(i)));
  }
  EXPECT_OK(Insert("x"));
  EXPECT_FALSE(LookupBool("h"));
  EXPECT_TRUE(LookupBool("l"));

  NewCache(/*capacity*/ 6);
  EXPECT_OK(Insert("h2", /*charge*/ 1, nullptr, Cache::Priority::HIGH));
  HitNTimes("h2", 2);
  EXPECT_OK(Insert("l2", /*charge*/ 1, nullptr, Cache::Priority::LOW));
  HitNTimes("l2", 2);
  for (int i = 0; i < 4; i++) {
    EXPECT_OK(Insert("c" + std::to_string(i)));
  }
  EXPECT_OK(Insert("y"));
  EXPECT_TRUE(LookupBool("h2"));
  EXPECT_TRUE(LookupBool("l2"));
}

// Rules out an eviction loop that re-reads a queue head, which would spin on
// a pinned head forever.
TEST_F(FIFOCacheTest, FullyPinnedTerminates) {
  NewCache(/*capacity*/ 10);
  EXPECT_OK(Insert("a"));
  HitNTimes("a", 2);
  EXPECT_OK(Insert("b"));
  HitNTimes("b", 2);
  for (int i = 0; i < 8; i++) {
    EXPECT_OK(Insert("c" + std::to_string(i)));
  }
  // Promotes a and b and evicts c0, populating both queues.
  EXPECT_OK(Insert("g"));
  EXPECT_EQ(10, cache_->GetOccupancyCount());

  std::vector<std::string> keys = {"a", "b", "g"};
  for (int i = 1; i < 8; i++) {
    keys.push_back("c" + std::to_string(i));
  }
  std::vector<FIFOHandle*> pinned;
  for (const auto& key : keys) {
    FIFOHandle* h = Lookup(key);
    ASSERT_NE(nullptr, h);
    pinned.push_back(h);
  }
  EXPECT_OK(Insert("h"));
  EXPECT_FALSE(LookupBool("h"));
  EXPECT_EQ(10, cache_->GetOccupancyCount());
  FIFOHandle* pi = nullptr;
  EXPECT_OK(Insert("i", /*charge*/ 1, &pi));
  ASSERT_NE(nullptr, pi);
  EXPECT_EQ(11, cache_->GetUsage());
  EXPECT_EQ(11, cache_->GetPinnedUsage());

  pinned.push_back(pi);
  for (FIFOHandle* h : pinned) {
    cache_->Release(h, true, false);
  }
  EXPECT_EQ(10, cache_->GetUsage());
  EXPECT_EQ(0, cache_->GetPinnedUsage());
}

// C1: the adaptive-mutex option is threaded from the options struct into the
// shard constructor. DMutex exposes no introspection, so this pins the
// plumbing: both values construct a working shard and cache.
TEST_F(FIFOCacheTest, UseAdaptiveMutexReachesShard) {
  for (bool adaptive : {false, true}) {
    FIFOCacheShard* shard = static_cast<FIFOCacheShard*>(
        port::cacheline_aligned_alloc(sizeof(FIFOCacheShard)));
    Cache::EvictionCallback no_cb;
    new (shard)
        FIFOCacheShard(/*capacity*/ 10,
                       /*strict_capacity_limit*/ false, adaptive,
                       kDontChargeCacheMetadata, /*max_upper_hash_bits*/ 24,
                       /*allocator*/ nullptr, &no_cb);
    EXPECT_OK(shard->Insert("a", 0, nullptr, &kNoopCacheItemHelper, 1, nullptr,
                            Cache::Priority::LOW));
    FIFOHandle* h =
        shard->Lookup("a", 0, nullptr, nullptr, Cache::Priority::LOW, nullptr);
    EXPECT_NE(nullptr, h);
    if (h != nullptr) {
      EXPECT_FALSE(shard->Release(h, true, false));
    }
    shard->~FIFOCacheShard();
    port::cacheline_aligned_free(shard);

    FIFOCacheOptions opts(/*capacity*/ 10, /*num_shard_bits*/ 0,
                          /*strict_capacity_limit*/ false, nullptr,
                          kDefaultToAdaptiveMutex, kDontChargeCacheMetadata);
    opts.use_adaptive_mutex = adaptive;
    std::shared_ptr<Cache> cache = opts.MakeSharedCache();
    ASSERT_NE(nullptr, cache);
    EXPECT_OK(cache->Insert("a", nullptr, &kNoopCacheItemHelper, 1));
    Cache::Handle* ch = cache->Lookup("a");
    EXPECT_NE(nullptr, ch);
    if (ch != nullptr) {
      cache->Release(ch);
    }
  }
}

// R1/R2: exceeding the probation budget under total capacity evicts
// nothing; the cold head waits first in line and goes on first pressure
// while a later twice-hit entry survives.
TEST_F(FIFOCacheTest, ProbationOverBudgetEvictsProbationHead) {
  NewCache(/*capacity*/ 100);
  EXPECT_EQ(12, cache_->TEST_GetProbationBudget());
  EXPECT_OK(Insert("a"));
  EXPECT_OK(Insert("b"));
  HitNTimes("b", 2);
  for (int i = 0; i < 11; i++) {
    EXPECT_OK(Insert("c" + std::to_string(i)));
  }
  // Probation holds 13, over the budget of 12, but total 13 is under
  // capacity: nothing evicted.
  EXPECT_EQ(13, cache_->TEST_GetProbationUsage());
  EXPECT_EQ(13, cache_->GetUsage());
  EXPECT_TRUE(ContainsForTest("a"));
  ValidateFIFOList({"a", "b", "c0", "c1", "c2", "c3", "c4", "c5", "c6", "c7",
                    "c8", "c9", "c10"});
  for (int i = 0; i < 87; i++) {
    EXPECT_OK(Insert("d" + std::to_string(i)));
  }
  EXPECT_EQ(100, cache_->GetUsage());
  EXPECT_OK(Insert("e"));
  EXPECT_FALSE(ContainsForTest("a"));
  EXPECT_TRUE(ContainsForTest("b"));
  EXPECT_TRUE(ContainsForTest("e"));
  EXPECT_EQ(100, cache_->GetUsage());
}

// R2: with probation under budget, pressure draws from resident first.
// Fails on the unbounded base, which always drains probation first.
TEST_F(FIFOCacheTest, ProbationUnderBudgetDrawsFromResident) {
  NewCache(/*capacity*/ 100);
  EXPECT_OK(Insert("r"));
  HitNTimes("r", 2);
  for (int i = 0; i < 87; i++) {
    std::string y = "y" + std::to_string(i);
    EXPECT_OK(Insert(y));
    HitNTimes(y, 2);
  }
  EXPECT_OK(Insert("s"));
  for (int i = 0; i < 11; i++) {
    EXPECT_OK(Insert("p" + std::to_string(i)));
  }
  // Total is 100; the sweep promotes r and y0..y86 and evicts s.
  EXPECT_EQ(100, cache_->GetUsage());
  EXPECT_OK(Insert("t"));
  EXPECT_TRUE(ContainsForTest("r"));
  EXPECT_FALSE(ContainsForTest("s"));
  // Probation holds p0..p10 plus t: 12, at (not over) the budget.
  EXPECT_EQ(12, cache_->TEST_GetProbationUsage());
  EXPECT_EQ(100, cache_->GetUsage());
  EXPECT_OK(Insert("x"));
  EXPECT_FALSE(ContainsForTest("r"));
  EXPECT_TRUE(ContainsForTest("p0"));
  EXPECT_TRUE(ContainsForTest("x"));
}

// The queue choice reads the current probation charge, before the incoming
// entry is accounted: probation exactly at budget draws from resident even
// when the incoming entry would push it over.
TEST_F(FIFOCacheTest, ProbationBudgetBoundaryUsesCurrentUsage) {
  NewCache(/*capacity*/ 100);
  EXPECT_OK(Insert("big", /*charge*/ 88));
  HitNTimes("big", 2);
  for (int i = 0; i < 12; i++) {
    EXPECT_OK(Insert("p" + std::to_string(i)));
  }
  EXPECT_EQ(100, cache_->GetUsage());
  // Promotes big, evicts p0.
  EXPECT_OK(Insert("t"));
  EXPECT_TRUE(ContainsForTest("big"));
  EXPECT_FALSE(ContainsForTest("p0"));
  // Probation holds p1..p11 plus t: 12, exactly at budget.
  EXPECT_EQ(12, cache_->TEST_GetProbationUsage());
  EXPECT_EQ(100, cache_->GetUsage());
  // Incoming charge 5 would push probation to 17 if counted; the choice
  // ignores it and draws from resident.
  EXPECT_OK(Insert("x", /*charge*/ 5));
  EXPECT_FALSE(ContainsForTest("big"));
  EXPECT_TRUE(ContainsForTest("p1"));
  EXPECT_TRUE(ContainsForTest("x"));
}

// R5: promotion moves charge out of probation only; usage and pinned usage
// are unchanged. A pinned bystander keeps pinned usage nonzero across it.
TEST_F(FIFOCacheTest, PromotionPreservesUsageAndPinnedUsage) {
  NewCache(/*capacity*/ 30);
  FIFOHandle* pinned = nullptr;
  EXPECT_OK(Insert("pin", /*charge*/ 4, &pinned));
  ASSERT_NE(nullptr, pinned);
  EXPECT_OK(Insert("k", /*charge*/ 10));
  HitNTimes("k", 2);
  EXPECT_OK(Insert("a", /*charge*/ 9));
  EXPECT_OK(Insert("b", /*charge*/ 7));
  EXPECT_EQ(30, cache_->GetUsage());
  EXPECT_EQ(4, cache_->GetPinnedUsage());
  EXPECT_EQ(30, cache_->TEST_GetProbationUsage());
  // Promotes k and evicts a: usage drops by a's charge only; pinned usage
  // holds; probation drops by k's and a's charges and gains d's.
  EXPECT_OK(Insert("d", /*charge*/ 5));
  EXPECT_TRUE(ContainsForTest("k"));
  EXPECT_FALSE(ContainsForTest("a"));
  EXPECT_EQ(26, cache_->GetUsage());
  EXPECT_EQ(4, cache_->GetPinnedUsage());
  EXPECT_EQ(16, cache_->TEST_GetProbationUsage());
  EXPECT_FALSE(cache_->Release(pinned, true, false));
  EXPECT_EQ(0, cache_->GetPinnedUsage());
}

// R1: the probation charge tracks inserts, evictions, promotions, erases
// and overwrites by charge, not entry count.
TEST_F(FIFOCacheTest, ProbationUsageAccounting) {
  NewCache(/*capacity*/ 100);
  EXPECT_EQ(0, cache_->TEST_GetProbationUsage());
  EXPECT_OK(Insert("a", /*charge*/ 5));
  EXPECT_OK(Insert("b", /*charge*/ 7));
  EXPECT_EQ(12, cache_->TEST_GetProbationUsage());
  HitNTimes("a", 2);
  // Fill to capacity without pressure, then promote a and evict b.
  for (int i = 0; i < 88; i++) {
    EXPECT_OK(Insert("f" + std::to_string(i)));
  }
  EXPECT_EQ(100, cache_->GetUsage());
  EXPECT_EQ(100, cache_->TEST_GetProbationUsage());
  EXPECT_OK(Insert("t"));
  EXPECT_TRUE(ContainsForTest("a"));
  EXPECT_FALSE(ContainsForTest("b"));
  // a promoted (5 left probation), b evicted (7 left), t entered (1).
  EXPECT_EQ(89, cache_->TEST_GetProbationUsage());
  EXPECT_EQ(94, cache_->GetUsage());
  // Erasing from resident leaves probation alone; erasing from probation
  // drops it.
  Erase("a");
  EXPECT_EQ(89, cache_->TEST_GetProbationUsage());
  EXPECT_EQ(89, cache_->GetUsage());
  Erase("f0");
  EXPECT_EQ(88, cache_->TEST_GetProbationUsage());
  EXPECT_EQ(88, cache_->GetUsage());
  // Overwriting a probation entry swaps its charge.
  Status s = Insert("f1", /*charge*/ 3);
  EXPECT_TRUE(s.IsOkOverwritten());
  EXPECT_EQ(90, cache_->TEST_GetProbationUsage());
  EXPECT_EQ(90, cache_->GetUsage());
}

// R3: the budget tracks capacity up and down; shrinking below the
// probation charge makes the shrink draw probation first.
TEST_F(FIFOCacheTest, SetCapacityRetracksProbationBudget) {
  NewCache(/*capacity*/ 100);
  EXPECT_EQ(12, cache_->TEST_GetProbationBudget());
  cache_->SetCapacity(200);
  EXPECT_EQ(24, cache_->TEST_GetProbationBudget());
  cache_->SetCapacity(50);
  EXPECT_EQ(6, cache_->TEST_GetProbationBudget());
  EXPECT_OK(Insert("r"));
  HitNTimes("r", 2);
  for (int i = 0; i < 49; i++) {
    EXPECT_OK(Insert("f" + std::to_string(i)));
  }
  EXPECT_EQ(50, cache_->GetUsage());
  // Promotes r, evicts f0.
  EXPECT_OK(Insert("t"));
  EXPECT_TRUE(ContainsForTest("r"));
  EXPECT_FALSE(ContainsForTest("f0"));
  EXPECT_EQ(49, cache_->TEST_GetProbationUsage());
  cache_->SetCapacity(10);
  EXPECT_EQ(1, cache_->TEST_GetProbationBudget());
  // Shrink draws probation first (over budget): oldest probation entries
  // go, resident r stays.
  EXPECT_TRUE(ContainsForTest("r"));
  EXPECT_EQ(10, cache_->GetUsage());
  cache_->SetCapacity(100);
  EXPECT_EQ(12, cache_->TEST_GetProbationBudget());
  EXPECT_EQ(10, cache_->GetUsage());
}

// R3: a shrink re-derives the budget before evicting. Probation at 10 is
// under the old budget of 12 but over the new budget of 6, so the shrink
// draws probation first.
TEST_F(FIFOCacheTest, SetCapacityShrinkUsesFreshBudget) {
  NewCache(/*capacity*/ 100);
  EXPECT_OK(Insert("big", /*charge*/ 90));
  HitNTimes("big", 2);
  for (int i = 0; i < 10; i++) {
    EXPECT_OK(Insert("p" + std::to_string(i)));
  }
  EXPECT_EQ(100, cache_->GetUsage());
  // Promotes big, evicts p0.
  EXPECT_OK(Insert("t"));
  EXPECT_TRUE(ContainsForTest("big"));
  EXPECT_FALSE(ContainsForTest("p0"));
  // Probation holds p1..p9 plus t: 10.
  EXPECT_EQ(10, cache_->TEST_GetProbationUsage());
  cache_->SetCapacity(50);
  // Fresh budget is 6: probation first, so p1 goes. A stale budget of 12
  // would draw resident first and leave p1 alone.
  EXPECT_FALSE(ContainsForTest("p1"));
}

// R4: the budget is exact on small capacities, never overflows, and rounds
// to zero (probation-first) on tiny shards.
TEST_F(FIFOCacheTest, ProbationBudgetArithmetic) {
  NewCache(/*capacity*/ 100);
  EXPECT_EQ(12, cache_->TEST_GetProbationBudget());
  NewCache(/*capacity*/ 99);
  EXPECT_EQ(11, cache_->TEST_GetProbationBudget());
  NewCache(/*capacity*/ 8);
  EXPECT_EQ(0, cache_->TEST_GetProbationBudget());
  // SIZE_MAX = 100q + 15, so floor(12%) is 12q + 1.
  NewCache(SIZE_MAX);
  size_t q = SIZE_MAX / 100;
  EXPECT_EQ(12 * q + 1, cache_->TEST_GetProbationBudget());
  // capacity * 12 / 100 would wrap here; the split form does not.
  EXPECT_GT(cache_->TEST_GetProbationBudget(), SIZE_MAX / 10);
}

// An entry larger than the whole probation budget waits in probation like
// any other and is promoted or evicted on its turn; the bound never
// splits, refuses, or spins on it.
TEST_F(FIFOCacheTest, SingleEntryExceedsProbationBudget) {
  NewCache(/*capacity*/ 100);
  EXPECT_OK(Insert("big", /*charge*/ 20));
  EXPECT_EQ(20, cache_->TEST_GetProbationUsage());
  for (int i = 0; i < 80; i++) {
    EXPECT_OK(Insert("f" + std::to_string(i)));
  }
  EXPECT_EQ(100, cache_->GetUsage());
  EXPECT_OK(Insert("t"));
  // Probation over budget, head cold: big goes.
  EXPECT_FALSE(ContainsForTest("big"));
  EXPECT_TRUE(ContainsForTest("t"));
  EXPECT_EQ(81, cache_->TEST_GetProbationUsage());
  EXPECT_EQ(81, cache_->GetUsage());

  // Twice-hit instead: the same oversized entry promotes to resident.
  NewCache(/*capacity*/ 100);
  EXPECT_OK(Insert("big", /*charge*/ 20));
  HitNTimes("big", 2);
  for (int i = 0; i < 80; i++) {
    EXPECT_OK(Insert("f" + std::to_string(i)));
  }
  EXPECT_OK(Insert("t"));
  EXPECT_TRUE(ContainsForTest("big"));
  EXPECT_FALSE(ContainsForTest("f0"));
  EXPECT_EQ(80, cache_->TEST_GetProbationUsage());
  EXPECT_EQ(100, cache_->GetUsage());
}

// A fully pinned shard with probation over budget terminates and evicts
// nothing; a pinned probation still falls back to resident when resident
// has evictable entries.
TEST_F(FIFOCacheTest, FullyPinnedProbationOverBudgetTerminates) {
  NewCache(/*capacity*/ 100);
  for (int i = 0; i < 100; i++) {
    EXPECT_OK(Insert("p" + std::to_string(i)));
  }
  EXPECT_EQ(100, cache_->TEST_GetProbationUsage());
  std::vector<FIFOHandle*> pinned;
  for (int i = 0; i < 100; i++) {
    FIFOHandle* h = Lookup("p" + std::to_string(i));
    ASSERT_NE(nullptr, h);
    pinned.push_back(h);
  }
  // Probation over budget and fully pinned, resident empty: terminates,
  // evicts nothing, drops the insert.
  EXPECT_OK(Insert("x"));
  EXPECT_FALSE(ContainsForTest("x"));
  EXPECT_EQ(100, cache_->GetUsage());
  for (FIFOHandle* h : pinned) {
    EXPECT_FALSE(cache_->Release(h, true, false));
  }
  EXPECT_EQ(0, cache_->GetPinnedUsage());

  // Pinned probation over budget with an evictable resident entry: the
  // probation sweep skips, resident covers.
  NewCache(/*capacity*/ 100);
  EXPECT_OK(Insert("r"));
  HitNTimes("r", 2);
  for (int i = 0; i < 99; i++) {
    EXPECT_OK(Insert("p" + std::to_string(i)));
  }
  EXPECT_EQ(100, cache_->GetUsage());
  EXPECT_OK(Insert("t"));
  // Promotes r, evicts p0.
  EXPECT_TRUE(ContainsForTest("r"));
  EXPECT_FALSE(ContainsForTest("p0"));
  std::vector<FIFOHandle*> pinned2;
  for (int i = 1; i < 99; i++) {
    FIFOHandle* h = Lookup("p" + std::to_string(i));
    ASSERT_NE(nullptr, h);
    pinned2.push_back(h);
  }
  FIFOHandle* ht = Lookup("t");
  ASSERT_NE(nullptr, ht);
  pinned2.push_back(ht);
  // Probation (99) over budget and fully pinned; resident r unpinned.
  EXPECT_EQ(99, cache_->TEST_GetProbationUsage());
  EXPECT_OK(Insert("x"));
  EXPECT_FALSE(ContainsForTest("r"));
  EXPECT_TRUE(ContainsForTest("p1"));
  for (FIFOHandle* h : pinned2) {
    cache_->Release(h, true, false);
  }
}

TEST_F(FIFOCacheTest, ProbationFallbackPromotionRetriesResident) {
  for (bool strict : {true, false}) {
    SCOPED_TRACE("strict=" + std::to_string(strict));
    NewCache(/*capacity*/ 100, strict);
    EXPECT_OK(Insert("r", /*charge*/ 89));
    HitNTimes("r", 2);
    EXPECT_OK(Insert("s", /*charge*/ 11));
    // Promotes r, evicts s.
    EXPECT_OK(Insert("t"));
    Erase("t");
    FIFOHandle* hr = Lookup("r");
    ASSERT_NE(nullptr, hr);
    EXPECT_OK(Insert("p", /*charge*/ 10));
    HitNTimes("p", 2);
    ASSERT_FALSE(InProbationForTest("r"));
    ASSERT_EQ(10, cache_->TEST_GetProbationUsage());
    ASSERT_EQ(99, cache_->GetUsage());

    // Probation stays within its budget of 12, so resident is tried first
    // and frees nothing; the probation fallback only promotes p, so the
    // resident pass must run again to evict it.
    FIFOHandle* hx = nullptr;
    EXPECT_OK(Insert("x", /*charge*/ 2, &hx));
    EXPECT_NE(nullptr, hx);
    EXPECT_LE(cache_->GetUsage(), 100);
    EXPECT_FALSE(ContainsForTest("p"));
    if (hx != nullptr) {
      cache_->Release(hx, true, false);
    }
    cache_->Release(hr, true, false);
  }
}

// A probation fallback that promotes entries but frees too little runs the
// resident pass again: the promoted entry, unpinned at freq 0, makes room.
TEST_F(FIFOCacheTest, ResidentRetriedAfterProbationPromotes) {
  NewCache(/*capacity*/ 86, /*strict_capacity_limit*/ true);
  EXPECT_OK(Insert("r", 80));
  HitNTimes("r", 2);
  EXPECT_OK(Insert("s", 6));
  // Promotes r and evicts s.
  EXPECT_OK(Insert("t", 1));
  Erase("t");
  FIFOHandle* hr = Lookup("r");
  ASSERT_NE(nullptr, hr);
  EXPECT_OK(Insert("p", 5));
  HitNTimes("p", 2);
  EXPECT_OK(Insert("c", 1));
  // Probation (6) is within budget (10), so resident goes first and frees
  // nothing (r is pinned). Probation promotes p and evicts c, freeing 1 of
  // the 2 needed; resident then evicts p.
  FIFOHandle* hx = nullptr;
  EXPECT_OK(Insert("x", 2, &hx));
  ASSERT_NE(nullptr, hx);
  EXPECT_FALSE(ContainsForTest("p"));
  EXPECT_LE(cache_->GetUsage(), 86);
  EXPECT_FALSE(cache_->Release(hx, true, false));
  EXPECT_FALSE(cache_->Release(hr, true, false));
}

// With every entry pinned eviction returns at once; releasing one entry,
// in either queue, must make it evictable again.
TEST_F(FIFOCacheTest, ReleasedEntryEvictableInFullyPinnedShard) {
  NewCache(/*capacity*/ 10);
  std::vector<FIFOHandle*> pinned;
  for (int i = 0; i < 10; ++i) {
    FIFOHandle* h = nullptr;
    EXPECT_OK(Insert("p" + std::to_string(i), /*charge*/ 1, &h));
    pinned.push_back(h);
  }
  EXPECT_OK(Insert("x"));
  EXPECT_FALSE(ContainsForTest("x"));
  EXPECT_FALSE(cache_->Release(pinned[3], true, false));
  EXPECT_OK(Insert("y"));
  EXPECT_TRUE(ContainsForTest("y"));
  EXPECT_FALSE(ContainsForTest("p3"));
  for (int i = 0; i < 10; ++i) {
    if (i != 3) {
      cache_->Release(pinned[i], true, false);
    }
  }

  NewCache(/*capacity*/ 10);
  for (int i = 0; i < 9; ++i) {
    EXPECT_OK(Insert("r" + std::to_string(i)));
    HitNTimes("r" + std::to_string(i), 2);
  }
  EXPECT_OK(Insert("s"));
  // Promotes r0..r8, evicts s.
  EXPECT_OK(Insert("t"));
  Erase("t");
  pinned.clear();
  for (int i = 0; i < 9; ++i) {
    FIFOHandle* h = Lookup("r" + std::to_string(i));
    ASSERT_NE(nullptr, h);
    ASSERT_FALSE(InProbationForTest("r" + std::to_string(i)));
    pinned.push_back(h);
  }
  EXPECT_FALSE(cache_->Release(pinned[4], true, false));
  EXPECT_OK(Insert("y", /*charge*/ 2));
  EXPECT_TRUE(ContainsForTest("y"));
  EXPECT_FALSE(ContainsForTest("r4"));
  for (int i = 0; i < 9; ++i) {
    if (i != 4) {
      cache_->Release(pinned[i], true, false);
    }
  }
  EXPECT_EQ(0, cache_->GetPinnedUsage());
}

// C2: a configured secondary cache wraps the primary instead of failing.
TEST_F(FIFOCacheTest, SecondaryCacheWrapped) {
  FIFOCacheOptions opts(/*capacity*/ 10, /*num_shard_bits*/ 0,
                        /*strict_capacity_limit*/ false, nullptr,
                        kDefaultToAdaptiveMutex, kDontChargeCacheMetadata);
  opts.secondary_cache =
      CompressedSecondaryCacheOptions(/*capacity*/ 1000, /*num_shard_bits*/ 0,
                                      /*strict_capacity_limit*/ false, 0.5)
          .MakeSharedSecondaryCache();
  std::shared_ptr<Cache> cache = opts.MakeSharedCache();
  ASSERT_NE(nullptr, cache);
  EXPECT_OK(cache->Insert("a", nullptr, &kNoopCacheItemHelper, 1));
  Cache::Handle* h = cache->Lookup("a");
  EXPECT_NE(nullptr, h);
  if (h != nullptr) {
    cache->Release(h);
  }
}

// R1/R3: a probation victim re-inserted lands in resident, so the same
// pressure that evicted it once no longer does. Resident holds only a, so
// probation stays over budget and every eviction draws from probation.
TEST_F(FIFOCacheTest, GhostReadmitsProbationVictimToResident) {
  NewCache(/*capacity*/ 100);
  EXPECT_OK(Insert("a"));
  InsertN("c", 99);
  EXPECT_EQ(100, cache_->GetUsage());
  EXPECT_OK(Insert("x"));
  EXPECT_FALSE(ContainsForTest("a"));
  // Evicts c0; a goes to resident.
  EXPECT_OK(Insert("a"));
  EXPECT_TRUE(ContainsForTest("a"));
  EXPECT_FALSE(InProbationForTest("a"));
  // A full probation's worth of cold inserts: in probation, a would go.
  InsertN("d", 100);
  EXPECT_TRUE(ContainsForTest("a"));
  EXPECT_EQ(100, cache_->GetUsage());
}

// R2: a resident victim is not remembered and re-enters probation, while a
// probation victim from the same run is readmitted to resident.
TEST_F(FIFOCacheTest, GhostIgnoresResidentVictims) {
  NewCache(/*capacity*/ 100);
  EXPECT_OK(Insert("big", /*charge*/ 87));
  HitNTimes("big", 2);
  EXPECT_OK(Insert("a"));
  HitNTimes("a", 2);
  InsertN("p", 12);
  EXPECT_EQ(100, cache_->GetUsage());
  // Promotes big and a, evicts p0 from probation.
  EXPECT_OK(Insert("t"));
  EXPECT_FALSE(ContainsForTest("p0"));
  EXPECT_EQ(12, cache_->TEST_GetProbationUsage());
  HitNTimes("big", 1);
  // Probation at budget: resident first. big spends its hit; a is evicted.
  EXPECT_OK(Insert("u"));
  EXPECT_FALSE(ContainsForTest("a"));
  EXPECT_TRUE(ContainsForTest("big"));
  EXPECT_OK(Insert("a"));
  EXPECT_TRUE(InProbationForTest("a"));
  EXPECT_OK(Insert("p0"));
  EXPECT_TRUE(ContainsForTest("p0"));
  EXPECT_FALSE(InProbationForTest("p0"));
}

// R3: readmission consumes the ghost. After a is readmitted and then
// evicted from resident, a third insert goes to probation.
TEST_F(FIFOCacheTest, GhostForgetsReadmittedKey) {
  NewCache(/*capacity*/ 100);
  EXPECT_OK(Insert("big", /*charge*/ 87));
  HitNTimes("big", 2);
  EXPECT_OK(Insert("a"));
  InsertN("p", 12);
  EXPECT_EQ(100, cache_->GetUsage());
  // Promotes big, evicts a from probation.
  EXPECT_OK(Insert("t"));
  EXPECT_FALSE(ContainsForTest("a"));
  // Evicts p0; a is readmitted to resident.
  EXPECT_OK(Insert("a"));
  EXPECT_TRUE(ContainsForTest("a"));
  EXPECT_FALSE(InProbationForTest("a"));
  EXPECT_EQ(12, cache_->TEST_GetProbationUsage());
  HitNTimes("big", 1);
  // Probation at budget: resident first. big spends its hit; a is evicted.
  EXPECT_OK(Insert("u"));
  EXPECT_FALSE(ContainsForTest("a"));
  EXPECT_TRUE(ContainsForTest("big"));
  EXPECT_OK(Insert("a"));
  EXPECT_TRUE(InProbationForTest("a"));
}

// A slot left stale by readmission must not drop a newer record of the same
// key when it reaches the head of the ghost queue.
TEST_F(FIFOCacheTest, GhostStaleSlotKeepsNewerRecord) {
  NewCache(/*capacity*/ 10);
  EXPECT_OK(Insert("a"));
  InsertN("c", 9);
  // Evicts a. Ghost: [a].
  EXPECT_OK(Insert("x"));
  // Evicts c0, then readmits a. Ghost: [stale a, c0].
  EXPECT_OK(Insert("a"));
  EXPECT_FALSE(InProbationForTest("a"));
  Erase("a");
  Erase("x");
  for (int i = 1; i < 9; i++) {
    Erase("c" + std::to_string(i));
  }
  EXPECT_EQ(0, cache_->GetUsage());
  EXPECT_OK(Insert("a"));
  // e9 evicts a from probation. Ghost: [stale a, c0, a].
  InsertN("e", 10);
  EXPECT_FALSE(ContainsForTest("a"));
  EXPECT_EQ(3, cache_->TEST_GetGhostSize());
  for (int i = 3; i < 10; i++) {
    Erase("e" + std::to_string(i));
  }
  // Evicts e0 with 3 entries in the table, dropping the stale slot.
  cache_->SetCapacity(2);
  EXPECT_EQ(3, cache_->TEST_GetGhostSize());
  cache_->SetCapacity(10);
  EXPECT_OK(Insert("a"));
  EXPECT_TRUE(ContainsForTest("a"));
  EXPECT_FALSE(InProbationForTest("a"));
}

// R4: under a long unique-key workload the ghost queue holds exactly as
// many keys as the shard holds entries, and follows the shard when it
// shrinks.
TEST_F(FIFOCacheTest, GhostBoundedByOccupancy) {
  NewCache(/*capacity*/ 100);
  InsertN("k", 10000);
  EXPECT_EQ(100, cache_->GetOccupancyCount());
  EXPECT_EQ(100, cache_->TEST_GetGhostSize());
  cache_->SetCapacity(50);
  InsertN("j", 10000);
  EXPECT_EQ(50, cache_->GetOccupancyCount());
  EXPECT_EQ(50, cache_->TEST_GetGhostSize());
}

// The ghost ring keeps the newest records across growth and many wraps:
// after 100 probation victims with room for 40, the newest victim is
// readmitted to resident and one dropped earlier is not.
TEST_F(FIFOCacheTest, GhostRingWrapsKeepingNewestRecords) {
  NewCache(/*capacity*/ 40);
  InsertN("k", 40);
  InsertN("m", 100);
  // Victims k0..k39 then m0..m59; the ghost remembers m20..m59.
  EXPECT_EQ(40, cache_->TEST_GetGhostSize());
  EXPECT_OK(Insert("m59"));
  EXPECT_TRUE(ContainsForTest("m59"));
  EXPECT_FALSE(InProbationForTest("m59"));
  EXPECT_OK(Insert("m19"));
  EXPECT_TRUE(InProbationForTest("m19"));
}

// The ghost is keyed on the shard hash, so a different key with a
// remembered hash is readmitted to resident, at freq 0.
TEST_F(FIFOCacheTest, GhostHashCollisionAdmitsToResident) {
  NewCache(/*capacity*/ 10);
  constexpr uint32_t kHash = 12345;
  auto insert_with_hash = [&](const std::string& key) {
    return cache_->Insert(key, kHash, nullptr, &kNoopCacheItemHelper, 1,
                          nullptr, Cache::Priority::LOW);
  };
  EXPECT_OK(insert_with_hash("a"));
  InsertN("c", 9);
  // Evicts a, remembering kHash.
  EXPECT_OK(Insert("x"));
  EXPECT_FALSE(ContainsForTest("a"));
  EXPECT_OK(insert_with_hash("b"));
  EXPECT_TRUE(ContainsForTest("b"));
  EXPECT_FALSE(InProbationForTest("b"));
}

// A placeholder insert (zero charge, kNoopCacheItemHelper), as
// CacheWithSecondaryAdapter uses to record recent use, neither takes nor
// leaves a ghost: readmission credit waits for the real entry.
TEST_F(FIFOCacheTest, GhostIgnoresPlaceholderInserts) {
  NewCache(/*capacity*/ 10);
  EXPECT_OK(Insert("a"));
  InsertN("c", 9);
  // Evicts a, remembering it.
  EXPECT_OK(Insert("x"));
  EXPECT_FALSE(ContainsForTest("a"));
  EXPECT_OK(Insert("a", /*charge*/ 0));
  EXPECT_TRUE(InProbationForTest("a"));
  Erase("a");
  EXPECT_OK(Insert("a"));
  EXPECT_TRUE(ContainsForTest("a"));
  EXPECT_FALSE(InProbationForTest("a"));
}

// End to end through the Cache interface: a key read once per scan longer
// than the cache misses forever without the ghost queue. With it, the
// second miss lands in resident and every later read hits despite scans.
TEST_F(FIFOCacheTest, GhostKeepsSlowReuseKeyAcrossScans) {
  FIFOCacheOptions opts(/*capacity*/ 100, /*num_shard_bits*/ 0,
                        /*strict_capacity_limit*/ false, nullptr,
                        kDefaultToAdaptiveMutex, kDontChargeCacheMetadata);
  std::shared_ptr<Cache> cache = opts.MakeSharedCache();
  ASSERT_NE(nullptr, cache);
  auto access = [&](const std::string& key) {
    Cache::Handle* h = cache->Lookup(key);
    if (h != nullptr) {
      cache->Release(h);
      return true;
    }
    EXPECT_OK(cache->Insert(key, nullptr, &kNoopCacheItemHelper, 1));
    return false;
  };
  int hits = 0;
  int scanned = 0;
  for (int round = 0; round < 5; round++) {
    if (access("hot")) {
      hits++;
    }
    for (int i = 0; i < 150; i++) {
      EXPECT_FALSE(access("s" + std::to_string(scanned++)));
    }
  }
  // Misses in rounds 0 and 1, hits in rounds 2 to 4.
  EXPECT_EQ(3, hits);
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

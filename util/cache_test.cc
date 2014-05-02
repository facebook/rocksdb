//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "rocksdb/cache.h"

#include <vector>
#include <string>
#include <iostream>
#include "util/coding.h"
#include "util/testharness.h"

namespace rocksdb {

// Conversions between numeric keys/values and the types expected by Cache.
static std::string EncodeKey(int k) {
  std::string result;
  PutFixed32(&result, k);
  return result;
}
static int DecodeKey(const Slice& k) {
  assert(k.size() == 4);
  return DecodeFixed32(k.data());
}
static void* EncodeValue(uintptr_t v) { return reinterpret_cast<void*>(v); }
static int DecodeValue(void* v) { return reinterpret_cast<uintptr_t>(v); }

class CacheTest {
 public:
  static CacheTest* current_;

  static void Deleter(const Slice& key, void* v) {
    current_->deleted_keys_.push_back(DecodeKey(key));
    current_->deleted_values_.push_back(DecodeValue(v));
  }

  static const int kCacheSize = 1000;
  static const int kNumShardBits = 4;
  static const int kRemoveScanCountLimit = 16;

  static const int kCacheSize2 = 100;
  static const int kNumShardBits2 = 2;
  static const int kRemoveScanCountLimit2 = 200;

  std::vector<int> deleted_keys_;
  std::vector<int> deleted_values_;
  shared_ptr<Cache> cache_;
  shared_ptr<Cache> cache2_;

  CacheTest() :
      cache_(NewLRUCache(kCacheSize, kNumShardBits, kRemoveScanCountLimit)),
      cache2_(NewLRUCache(kCacheSize2, kNumShardBits2,
                          kRemoveScanCountLimit2)) {
    current_ = this;
  }

  ~CacheTest() {
  }

  int Lookup(shared_ptr<Cache> cache, int key) {
    Cache::Handle* handle = cache->Lookup(EncodeKey(key));
    const int r = (handle == nullptr) ? -1 : DecodeValue(cache->Value(handle));
    if (handle != nullptr) {
      cache->Release(handle);
    }
    return r;
  }

  void Insert(shared_ptr<Cache> cache, int key, int value, int charge = 1) {
    cache->Release(cache->Insert(EncodeKey(key), EncodeValue(value), charge,
                                  &CacheTest::Deleter));
  }

  void Erase(shared_ptr<Cache> cache, int key) {
    cache->Erase(EncodeKey(key));
  }


  int Lookup(int key) {
    return Lookup(cache_, key);
  }

  void Insert(int key, int value, int charge = 1) {
    Insert(cache_, key, value, charge);
  }

  void Erase(int key) {
    Erase(cache_, key);
  }

  int Lookup2(int key) {
    return Lookup(cache2_, key);
  }

  void Insert2(int key, int value, int charge = 1) {
    Insert(cache2_, key, value, charge);
  }

  void Erase2(int key) {
    Erase(cache2_, key);
  }
};
CacheTest* CacheTest::current_;

namespace {
void dumbDeleter(const Slice& key, void* value) { }
}  // namespace

TEST(CacheTest, UsageTest) {
  // cache is shared_ptr and will be automatically cleaned up.
  const uint64_t kCapacity = 100000;
  auto cache = NewLRUCache(kCapacity, 8, 200);

  size_t usage = 0;
  const char* value = "abcdef";
  // make sure everything will be cached
  for (int i = 1; i < 100; ++i) {
    std::string key(i, 'a');
    auto kv_size = key.size() + 5;
    cache->Release(
        cache->Insert(key, (void*)value, kv_size, dumbDeleter)
    );
    usage += kv_size;
    ASSERT_EQ(usage, cache->GetUsage());
  }

  // make sure the cache will be overloaded
  for (uint64_t i = 1; i < kCapacity; ++i) {
    auto key = std::to_string(i);
    cache->Release(
        cache->Insert(key, (void*)value, key.size() + 5, dumbDeleter)
    );
  }

  // the usage should be close to the capacity
  ASSERT_GT(kCapacity, cache->GetUsage());
  ASSERT_LT(kCapacity * 0.95, cache->GetUsage());
}

TEST(CacheTest, HitAndMiss) {
  ASSERT_EQ(-1, Lookup(100));

  Insert(100, 101);
  ASSERT_EQ(101, Lookup(100));
  ASSERT_EQ(-1,  Lookup(200));
  ASSERT_EQ(-1,  Lookup(300));

  Insert(200, 201);
  ASSERT_EQ(101, Lookup(100));
  ASSERT_EQ(201, Lookup(200));
  ASSERT_EQ(-1,  Lookup(300));

  Insert(100, 102);
  ASSERT_EQ(102, Lookup(100));
  ASSERT_EQ(201, Lookup(200));
  ASSERT_EQ(-1,  Lookup(300));

  ASSERT_EQ(1U, deleted_keys_.size());
  ASSERT_EQ(100, deleted_keys_[0]);
  ASSERT_EQ(101, deleted_values_[0]);
}

TEST(CacheTest, Erase) {
  Erase(200);
  ASSERT_EQ(0U, deleted_keys_.size());

  Insert(100, 101);
  Insert(200, 201);
  Erase(100);
  ASSERT_EQ(-1,  Lookup(100));
  ASSERT_EQ(201, Lookup(200));
  ASSERT_EQ(1U, deleted_keys_.size());
  ASSERT_EQ(100, deleted_keys_[0]);
  ASSERT_EQ(101, deleted_values_[0]);

  Erase(100);
  ASSERT_EQ(-1,  Lookup(100));
  ASSERT_EQ(201, Lookup(200));
  ASSERT_EQ(1U, deleted_keys_.size());
}

TEST(CacheTest, EntriesArePinned) {
  Insert(100, 101);
  Cache::Handle* h1 = cache_->Lookup(EncodeKey(100));
  ASSERT_EQ(101, DecodeValue(cache_->Value(h1)));

  Insert(100, 102);
  Cache::Handle* h2 = cache_->Lookup(EncodeKey(100));
  ASSERT_EQ(102, DecodeValue(cache_->Value(h2)));
  ASSERT_EQ(0U, deleted_keys_.size());

  cache_->Release(h1);
  ASSERT_EQ(1U, deleted_keys_.size());
  ASSERT_EQ(100, deleted_keys_[0]);
  ASSERT_EQ(101, deleted_values_[0]);

  Erase(100);
  ASSERT_EQ(-1, Lookup(100));
  ASSERT_EQ(1U, deleted_keys_.size());

  cache_->Release(h2);
  ASSERT_EQ(2U, deleted_keys_.size());
  ASSERT_EQ(100, deleted_keys_[1]);
  ASSERT_EQ(102, deleted_values_[1]);
}

TEST(CacheTest, EvictionPolicy) {
  Insert(100, 101);
  Insert(200, 201);

  // Frequently used entry must be kept around
  for (int i = 0; i < kCacheSize + 100; i++) {
    Insert(1000+i, 2000+i);
    ASSERT_EQ(2000+i, Lookup(1000+i));
    ASSERT_EQ(101, Lookup(100));
  }
  ASSERT_EQ(101, Lookup(100));
  ASSERT_EQ(-1, Lookup(200));
}

TEST(CacheTest, EvictionPolicyRef) {
  Insert(100, 101);
  Insert(101, 102);
  Insert(102, 103);
  Insert(103, 104);
  Insert(200, 101);
  Insert(201, 102);
  Insert(202, 103);
  Insert(203, 104);
  Cache::Handle* h201 = cache_->Lookup(EncodeKey(200));
  Cache::Handle* h202 = cache_->Lookup(EncodeKey(201));
  Cache::Handle* h203 = cache_->Lookup(EncodeKey(202));
  Cache::Handle* h204 = cache_->Lookup(EncodeKey(203));
  Insert(300, 101);
  Insert(301, 102);
  Insert(302, 103);
  Insert(303, 104);

  // Insert entries much more than Cache capacity
  for (int i = 0; i < kCacheSize + 100; i++) {
    Insert(1000 + i, 2000 + i);
  }

  // Check whether the entries inserted in the beginning
  // are evicted. Ones without extra ref are evicted and
  // those with are not.
  ASSERT_EQ(-1, Lookup(100));
  ASSERT_EQ(-1, Lookup(101));
  ASSERT_EQ(-1, Lookup(102));
  ASSERT_EQ(-1, Lookup(103));

  ASSERT_EQ(-1, Lookup(300));
  ASSERT_EQ(-1, Lookup(301));
  ASSERT_EQ(-1, Lookup(302));
  ASSERT_EQ(-1, Lookup(303));

  ASSERT_EQ(101, Lookup(200));
  ASSERT_EQ(102, Lookup(201));
  ASSERT_EQ(103, Lookup(202));
  ASSERT_EQ(104, Lookup(203));

  // Cleaning up all the handles
  cache_->Release(h201);
  cache_->Release(h202);
  cache_->Release(h203);
  cache_->Release(h204);
}

TEST(CacheTest, EvictionPolicyRef2) {
  std::vector<Cache::Handle*> handles;

  Insert(100, 101);
  // Insert entries much more than Cache capacity
  for (int i = 0; i < kCacheSize + 100; i++) {
    Insert(1000 + i, 2000 + i);
    if (i < kCacheSize ) {
      handles.push_back(cache_->Lookup(EncodeKey(1000 + i)));
    }
  }

  // Make sure referenced keys are also possible to be deleted
  // if there are not sufficient non-referenced keys
  for (int i = 0; i < 5; i++) {
    ASSERT_EQ(-1, Lookup(1000 + i));
  }

  for (int i = kCacheSize; i < kCacheSize + 100; i++) {
    ASSERT_EQ(2000 + i, Lookup(1000 + i));
  }
  ASSERT_EQ(-1, Lookup(100));

  // Cleaning up all the handles
  while (handles.size() > 0) {
    cache_->Release(handles.back());
    handles.pop_back();
  }
}

TEST(CacheTest, EvictionPolicyRefLargeScanLimit) {
  std::vector<Cache::Handle*> handles2;

  // Cache2 has a cache RemoveScanCountLimit higher than cache size
  // so it would trigger a boundary condition.

  // Populate the cache with 10 more keys than its size.
  // Reference all keys except one close to the end.
  for (int i = 0; i < kCacheSize2 + 10; i++) {
    Insert2(1000 + i, 2000+i);
    if (i != kCacheSize2 ) {
      handles2.push_back(cache2_->Lookup(EncodeKey(1000 + i)));
    }
  }

  // Make sure referenced keys are also possible to be deleted
  // if there are not sufficient non-referenced keys
  for (int i = 0; i < 3; i++) {
    ASSERT_EQ(-1, Lookup2(1000 + i));
  }
  // The non-referenced value is deleted even if it's accessed
  // recently.
  ASSERT_EQ(-1, Lookup2(1000 + kCacheSize2));
  // Other values recently accessed are not deleted since they
  // are referenced.
  for (int i = kCacheSize2 - 10; i < kCacheSize2 + 10; i++) {
    if (i != kCacheSize2) {
      ASSERT_EQ(2000 + i, Lookup2(1000 + i));
    }
  }

  // Cleaning up all the handles
  while (handles2.size() > 0) {
    cache2_->Release(handles2.back());
    handles2.pop_back();
  }
}



TEST(CacheTest, HeavyEntries) {
  // Add a bunch of light and heavy entries and then count the combined
  // size of items still in the cache, which must be approximately the
  // same as the total capacity.
  const int kLight = 1;
  const int kHeavy = 10;
  int added = 0;
  int index = 0;
  while (added < 2*kCacheSize) {
    const int weight = (index & 1) ? kLight : kHeavy;
    Insert(index, 1000+index, weight);
    added += weight;
    index++;
  }

  int cached_weight = 0;
  for (int i = 0; i < index; i++) {
    const int weight = (i & 1 ? kLight : kHeavy);
    int r = Lookup(i);
    if (r >= 0) {
      cached_weight += weight;
      ASSERT_EQ(1000+i, r);
    }
  }
  ASSERT_LE(cached_weight, kCacheSize + kCacheSize/10);
}

TEST(CacheTest, NewId) {
  uint64_t a = cache_->NewId();
  uint64_t b = cache_->NewId();
  ASSERT_NE(a, b);
}


class Value {
 private:
  int v_;
 public:
  explicit Value(int v) : v_(v) { }

  ~Value() { std::cout << v_ << " is destructed\n"; }
};

namespace {
void deleter(const Slice& key, void* value) {
  delete (Value *)value;
}
}  // namespace

TEST(CacheTest, BadEviction) {
  int n = 10;

  // a LRUCache with n entries and one shard only
  std::shared_ptr<Cache> cache = NewLRUCache(n, 0);

  std::vector<Cache::Handle*> handles(n+1);

  // Insert n+1 entries, but not releasing.
  for (int i = 0; i < n+1; i++) {
    std::string key = std::to_string(i+1);
    handles[i] = cache->Insert(key, new Value(i+1), 1, &deleter);
  }

  // Guess what's in the cache now?
  for (int i = 0; i < n+1; i++) {
    std::string key = std::to_string(i+1);
    auto h = cache->Lookup(key);
    std::cout << key << (h?" found\n":" not found\n");
    // Only the first entry should be missing
    ASSERT_TRUE(h || i == 0);
    if (h) cache->Release(h);
  }

  for (int i = 0; i < n+1; i++) {
    cache->Release(handles[i]);
  }
  std::cout << "Poor entries\n";
}

namespace {
std::vector<std::pair<int, int>> callback_state;
void callback(void* entry, size_t charge) {
  callback_state.push_back({DecodeValue(entry), static_cast<int>(charge)});
}
};

TEST(CacheTest, ApplyToAllCacheEntiresTest) {
  std::vector<std::pair<int, int>> inserted;
  callback_state.clear();

  for (int i = 0; i < 10; ++i) {
    Insert(i, i * 2, i + 1);
    inserted.push_back({i * 2, i + 1});
  }
  cache_->ApplyToAllCacheEntries(callback, true);

  sort(inserted.begin(), inserted.end());
  sort(callback_state.begin(), callback_state.end());
  ASSERT_TRUE(inserted == callback_state);
}

}  // namespace rocksdb

int main(int argc, char** argv) {
  return rocksdb::test::RunAllTests();
}

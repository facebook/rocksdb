//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "cache/fast_lru_cache.h"

#include "db/db_test_util.h"

namespace ROCKSDB_NAMESPACE {

class FastLRUCacheTest : public testing::Test {
 public:
  FastLRUCacheTest() {}
  ~FastLRUCacheTest() override { DeleteCache(); }

  void DeleteCache() {
    if (cache_ != nullptr) {
      cache_->~LRUCacheShard();
      port::cacheline_aligned_free(cache_);
      cache_ = nullptr;
    }
  }

  void NewCache(size_t capacity) {
    DeleteCache();
    cache_ = reinterpret_cast<fast_lru_cache::LRUCacheShard*>(
        port::cacheline_aligned_alloc(sizeof(fast_lru_cache::LRUCacheShard)));
    new (cache_) fast_lru_cache::LRUCacheShard(
        capacity, false /*strict_capcity_limit*/, kDontChargeCacheMetadata,
        24 /*max_upper_hash_bits*/);
  }

  Status Insert(const std::string& key) {
    return cache_->Insert(key, 0 /*hash*/, nullptr /*value*/, 1 /*charge*/,
                          nullptr /*deleter*/, nullptr /*handle*/,
                          Cache::Priority::LOW);
  }

  void Insert(char key, size_t len) {
    Status s = Insert(std::string(len, key));
    if (len == 16) {
      EXPECT_OK(s);
    } else {
      EXPECT_NOK(s);
    }
  }

 private:
  fast_lru_cache::LRUCacheShard* cache_ = nullptr;
};

// TODO(guido) This file should eventually contain similar tests than
// lru_cache_test.cc.

TEST_F(FastLRUCacheTest, ValidateKeySize) {
  NewCache(3);
  Insert('a', 16);
  Insert('b', 15);
  Insert('b', 16);
  Insert('c', 17);
  Insert('d', 1000);
  Insert('e', 11);
  Insert('f', 0);
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

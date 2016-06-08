//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#ifndef ROCKSDB_LITE

#include "utilities/persistent_cache/persistent_cache_test.h"

#include <functional>
#include <memory>
#include <thread>

namespace rocksdb {

// Volatile cache tests
TEST_F(PersistentCacheTierTest, VolatileCacheInsert) {
  for (auto nthreads : {1, 5}) {
    for (auto max_keys : {10 * 1024, 1 * 1024 * 1024}) {
      cache_ = std::make_shared<VolatileCacheTier>();
      RunInsertTest(nthreads, max_keys);
    }
  }
}

#ifndef ROCKSDB_TSAN_RUN
TEST_F(PersistentCacheTierTest, VolatileCacheInsertWithEviction) {
  for (auto nthreads : {1, 5}) {
    for (auto max_keys : {1 * 1024 * 1024}) {
      cache_ = std::make_shared<VolatileCacheTier>(/*compressed=*/true,
                                                   /*size=*/1 * 1024 * 1024);
      RunInsertTestWithEviction(nthreads, max_keys);
    }
  }
}
#endif

// test table with volatile page cache
TEST_F(PersistentCacheDBTest, VolatileCacheTest) {
  RunTest(std::bind(&PersistentCacheDBTest::MakeVolatileCache, this));
}

}  // namespace rocksdb

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
#else
int main() { return 0; }
#endif

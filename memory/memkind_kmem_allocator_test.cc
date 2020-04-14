//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  Copyright (c) 2019 Intel Corporation
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <cstdio>

#ifdef MEMKIND
#include "memkind_kmem_allocator.h"
#include "rocksdb/cache.h"
#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "table/block_based/block_based_table_factory.h"
#include "test_util/testharness.h"

namespace rocksdb {
TEST(MemkindKmemAllocatorTest, Allocate) {
  MemkindKmemAllocator allocator;
  void* p;
  try {
    p = allocator.Allocate(1024);
  } catch (const std::bad_alloc& e) {
    return;
  }
  ASSERT_NE(p, nullptr);
  size_t size = allocator.UsableSize(p, 1024);
  ASSERT_GE(size, 1024);
  allocator.Deallocate(p);
}

TEST(MemkindKmemAllocatorTest, DatabaseBlockCache) {
  // Check if a memory node is available for allocation
  try {
    MemkindKmemAllocator allocator;
    allocator.Allocate(1024);
  } catch (const std::bad_alloc& e) {
    return;  // if no node available, skip the test
  }

  // Create database with block cache using MemkindKmemAllocator
  Options options;
  std::string dbname = test::PerThreadDBPath("memkind_kmem_allocator_test");
  ASSERT_OK(DestroyDB(dbname, options));

  options.create_if_missing = true;
  std::shared_ptr<Cache> cache = NewLRUCache(
      1024 * 1024, 6, false, false, std::make_shared<MemkindKmemAllocator>());
  BlockBasedTableOptions table_options;
  table_options.block_cache = cache;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));

  DB* db = nullptr;
  Status s = DB::Open(options, dbname, &db);
  ASSERT_OK(s);
  ASSERT_NE(db, nullptr);
  ASSERT_EQ(cache->GetUsage(), 0);

  // Write 2kB (200 values, each 10 bytes)
  int num_keys = 200;
  WriteOptions wo;
  std::string val = "0123456789";
  for (int i = 0; i < num_keys; i++) {
    std::string key = std::to_string(i);
    s = db->Put(wo, Slice(key), Slice(val));
    ASSERT_OK(s);
  }
  ASSERT_OK(db->Flush(FlushOptions()));  // Flush all data from memtable so that
                                         // reads are from block cache

  // Read and check block cache usage
  ReadOptions ro;
  std::string result;
  for (int i = 0; i < num_keys; i++) {
    std::string key = std::to_string(i);
    s = db->Get(ro, key, &result);
    ASSERT_OK(s);
    ASSERT_EQ(result, val);
  }
  ASSERT_GT(cache->GetUsage(), 2000);

  // Close database
  s = db->Close();
  ASSERT_OK(s);
  ASSERT_OK(DestroyDB(dbname, options));
}
}  // namespace rocksdb

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

#else

int main(int /*argc*/, char** /*argv*/) {
  printf(
      "Skip memkind_kmem_allocator_test as the required library memkind is "
      "missing.");
}

#endif  // MEMKIND

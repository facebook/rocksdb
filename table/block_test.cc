// Copyright (c) 2012 Facebook.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "leveldb/table.h"

#include <stdio.h>
#include <algorithm>
#include <string>
#include <utility>
#include <vector>
#include "db/dbformat.h"
#include "db/memtable.h"
#include "db/write_batch_internal.h"
#include "leveldb/db.h"
#include "leveldb/env.h"
#include "leveldb/iterator.h"
#include "leveldb/table_builder.h"
#include "table/block.h"
#include "table/block_builder.h"
#include "table/format.h"
#include "table/block.h"
#include "util/random.h"
#include "util/testharness.h"
#include "util/testutil.h"

namespace leveldb {

static std::string RandomString(Random* rnd, int len) {
  std::string r;
  test::RandomString(rnd, len, &r);
  return r;
}

class BlockTest {};

// block test
TEST(BlockTest, SimpleTest) {
  Random rnd(301);
  Options options = Options();
  std::vector<std::string> keys;
  std::vector<std::string> values;
  BlockBuilder builder(&options);
  int num_records = 100000;
  char buf[10];
  char* p = &buf[0];

  // add a bunch of records to a block
  for (int i = 0; i < num_records; i++) {
    // generate random kvs
    sprintf(p, "%6d", i);
    std::string k(p);
    std::string v = RandomString(&rnd, 100); // 100 byte values

    // write kvs to the block
    Slice key(k);
    Slice value(v);
    builder.Add(key, value);

    // remember kvs in a lookaside array
    keys.push_back(k);
    values.push_back(v);
  }

  // read serialized contents of the block
  Slice rawblock = builder.Finish();

  // create block reader
  BlockContents contents;
  contents.data = rawblock;
  contents.cachable = false;
  contents.heap_allocated = false;
  Block reader(contents);

  // read contents of block sequentially
  int count = 0;
  Iterator* iter = reader.NewIterator(options.comparator);
  for (iter->SeekToFirst();iter->Valid(); count++, iter->Next()) {

    // read kv from block
    Slice k = iter->key();
    Slice v = iter->value();

    // compare with lookaside array
    ASSERT_EQ(k.ToString().compare(keys[count]), 0);
    ASSERT_EQ(v.ToString().compare(values[count]), 0);
  }
  delete iter;

  // read block contents randomly
  iter = reader.NewIterator(options.comparator);
  for (int i = 0; i < num_records; i++) {

    // find a random key in the lookaside array
    int index = rnd.Uniform(num_records);
    Slice k(keys[index]);

    // search in block for this key
    iter->Seek(k);
    ASSERT_TRUE(iter->Valid());
    Slice v = iter->value();
    ASSERT_EQ(v.ToString().compare(values[index]), 0);
  }
  delete iter;
}

class BlockMetricsTest {
 private:
 public:
  BlockMetricsTest() {
  }

  static bool IsAnyHot(const BlockMetrics* bm, uint32_t num_restarts,
                        uint32_t bytes_per_restart, uint32_t restart_index) {
    ASSERT_TRUE(num_restarts > restart_index);

    for (uint32_t restart_offset = 0; restart_offset < bytes_per_restart*8u;
         ++ restart_offset) {
      if (bm->IsHot(restart_index, restart_offset)) {
        return true;
      }
    }

    return false;
  }

  static bool IsNoneHot(const BlockMetrics* bm, uint32_t num_restarts,
                        uint32_t bytes_per_restart) {
    for (uint32_t restart_index = 0; restart_index < num_restarts;
         ++restart_index) {
      if (IsAnyHot(bm, num_restarts, bytes_per_restart, restart_index)) {
        return false;
      }
    }

    return true;
  }

  static void Access(BlockMetrics* bm, uint32_t num_restarts,
                     const std::vector<std::pair<uint32_t, uint32_t> >& access) {
    for (size_t i = 0; i < access.size(); ++i) {
      if (access[i].first >= num_restarts) continue;

      bm->RecordAccess(access[i].first, access[i].second);
    }
  }

  static bool AreAccessesHot(const BlockMetrics* bm, uint32_t num_restarts,
                    const std::vector<std::pair<uint32_t, uint32_t> >& accessed) {
    for (size_t i = 0; i < accessed.size(); ++i) {
      if (accessed[i].first >= num_restarts) continue;

      if (!bm->IsHot(accessed[i].first, accessed[i].second)) {
        return false;
      }
    }

    return true;
  }
};

TEST(BlockMetricsTest, Empty) {
  uint32_t num_restarts;
  uint32_t bytes_per_restart;
  BlockMetrics* bm;

  num_restarts = 5;
  bytes_per_restart = 2;
  bm = new BlockMetrics(0, 0, num_restarts, bytes_per_restart);
  ASSERT_TRUE(IsNoneHot(bm, num_restarts, bytes_per_restart));
  delete bm;

  num_restarts = 10;
  bytes_per_restart = 1;
  bm = new BlockMetrics(0, 0, num_restarts, bytes_per_restart);
  ASSERT_TRUE(IsNoneHot(bm, num_restarts, bytes_per_restart));
  delete bm;

  num_restarts = 7;
  bytes_per_restart = 4;
  bm = new BlockMetrics(0, 0, num_restarts, bytes_per_restart);
  ASSERT_TRUE(IsNoneHot(bm, num_restarts, bytes_per_restart));
  delete bm;
}

TEST(BlockMetricsTest, Hot) {
  std::vector<std::pair<uint32_t, uint32_t> > accessed;

  for (uint32_t restart_index = 0; restart_index < 10; ++restart_index) {
    for (uint32_t restart_offset = 0; restart_offset < 100; ++restart_offset) {
      if (restart_offset % 3 != 0) continue;
      accessed.push_back(std::make_pair(restart_index, restart_offset));
    }
  }

  uint32_t num_restarts;
  uint32_t bytes_per_restart;
  BlockMetrics* bm;

  num_restarts = 5;
  bytes_per_restart = 2;
  bm = new BlockMetrics(0, 0, num_restarts, bytes_per_restart);
  Access(bm, num_restarts, accessed);
  ASSERT_TRUE(AreAccessesHot(bm, num_restarts, accessed));
  delete bm;

  num_restarts = 5;
  bytes_per_restart = 2;
  bm = new BlockMetrics(0, 0, num_restarts, bytes_per_restart);
  Access(bm, num_restarts, accessed);
  ASSERT_TRUE(AreAccessesHot(bm, num_restarts, accessed));
  delete bm;
}

TEST(BlockMetricsTest, Compatible) {
  BlockMetrics* bm1 = new BlockMetrics(10, 0, 10, 2);
  BlockMetrics* bm2 = new BlockMetrics(10, 0, 10, 2);

  ASSERT_TRUE(bm1->IsCompatible(bm2));

  delete bm2;
  bm2 = new BlockMetrics(11, 0, 10, 2);
  ASSERT_TRUE(!bm1->IsCompatible(bm2));

  delete bm2;
  bm2 = new BlockMetrics(10, 1, 10, 2);
  ASSERT_TRUE(!bm1->IsCompatible(bm2));

  delete bm2;
  bm2 = new BlockMetrics(10, 0, 1l, 2);
  ASSERT_TRUE(!bm1->IsCompatible(bm2));

  delete bm2;
  bm2 = new BlockMetrics(10, 0, 10, 1);
  ASSERT_TRUE(!bm1->IsCompatible(bm2));

  delete bm1;
  delete bm2;
}

TEST(BlockMetricsTest, Join) {
  BlockMetrics* bm1 = new BlockMetrics(10, 0, 10, 2);
  BlockMetrics* bm2 = new BlockMetrics(10, 0, 10, 2);

  std::vector<std::pair<uint32_t, uint32_t> > accessed1;
  std::vector<std::pair<uint32_t, uint32_t> > accessed2;

  for (uint32_t restart_index = 0; restart_index < 10; ++restart_index) {
    for (uint32_t restart_offset = 1; restart_offset < 100; ++restart_offset) {
      if (restart_offset % 3 != 0) {
        accessed1.push_back(std::make_pair(restart_index, restart_offset));
      }
      if (restart_offset % 2 != 0) {
        accessed2.push_back(std::make_pair(restart_index, restart_offset));
      }
    }
  }

  Access(bm1, 10, accessed1);
  Access(bm2, 10, accessed2);

  ASSERT_TRUE(bm1->IsCompatible(bm2));
  ASSERT_TRUE(AreAccessesHot(bm1, 10, accessed1));
  ASSERT_TRUE(AreAccessesHot(bm2, 10, accessed2));

  bm1->Join(bm2);
  size_t accessed1_size = accessed1.size();
  accessed1.resize(accessed1.size() + accessed2.size());
  std::copy(accessed2.begin(), accessed2.end(), accessed1.begin()+accessed1_size);
  ASSERT_TRUE(AreAccessesHot(bm1, 10, accessed1));

  delete bm1;
  delete bm2;
}

TEST(BlockMetricsTest, StoreAndRetrieve) {
  BlockMetrics* bm = new BlockMetrics(10, 0, 10, 2);

  std::vector<std::pair<uint32_t, uint32_t> > accessed;

  for (uint32_t restart_index = 0; restart_index < 10; ++restart_index) {
    for (uint32_t restart_offset = 1; restart_offset < 100; ++restart_offset) {
      if (restart_offset % 3 != 0) {
        accessed.push_back(std::make_pair(restart_index, restart_offset));
      }
    }
  }

  Access(bm, 10, accessed);
  BlockMetrics* bm_r = BlockMetrics::Create(bm->GetDBKey(), bm->GetDBValue());
  ASSERT_TRUE(bm->IsCompatible(bm_r));

  for (uint32_t restart_index = 0; restart_index < 10; ++restart_index) {
    for (uint32_t restart_offset = 1; restart_offset < 100; ++restart_offset) {
      ASSERT_TRUE(bm->IsHot(restart_index, restart_offset) ==
		  bm_r->IsHot(restart_index, restart_offset));
    }
  }

  delete bm;
  delete bm_r;
}

TEST(BlockMetricsTest, DBKey) {
  BlockMetrics* bm1 = new BlockMetrics(10, 0, 10, 2);
  std::string db_key1 = bm1->GetDBKey();
  delete bm1;

  BlockMetrics* bm2;
  std::string db_key2;

  bm2 = new BlockMetrics(10, 256, 10, 2);
  db_key2 = bm2->GetDBKey();
  delete bm2;
  ASSERT_NE(db_key1, db_key2);

  bm2 = new BlockMetrics(266, 0, 10, 2);
  db_key2 = bm2->GetDBKey();
  delete bm2;
  ASSERT_NE(db_key1, db_key2);

  bm2 = new BlockMetrics(10, 0, 13, 2);
  db_key2 = bm2->GetDBKey();
  delete bm2;
  ASSERT_EQ(db_key1, db_key2);
}

}  // namespace leveldb

int main(int argc, char** argv) {
  return leveldb::test::RunAllTests();
}

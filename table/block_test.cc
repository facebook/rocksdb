// Copyright (c) 2012 Facebook.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#include <string>
#include "db/dbformat.h"
#include "db/memtable.h"
#include "db/write_batch_internal.h"
#include "leveldb/db.h"
#include "leveldb/env.h"
#include "leveldb/iterator.h"
#include "leveldb/table_builder.h"
#include "table/block.h"
#include "table/block_builder.h"
#include "table/table.h"
#include "table/format.h"
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

}  // namespace leveldb

int main(int argc, char** argv) {
  return leveldb::test::RunAllTests();
}

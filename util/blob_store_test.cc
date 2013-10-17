// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "util/blob_store.h"

#include "util/testharness.h"
#include "util/testutil.h"
#include "util/random.h"

#include <cstdlib>
#include <string>

namespace rocksdb {

using namespace std;

class BlobStoreTest { };

TEST(BlobStoreTest, RangeParseTest) {
  Blob e;
  for (int i = 0; i < 5; ++i) {
    e.chunks.push_back(BlobChunk(rand(), rand(), rand()));
  }
  string x = e.ToString();
  Blob nx(x);

  ASSERT_EQ(nx.ToString(), x);
}

// make sure we're reusing the freed space
TEST(BlobStoreTest, SanityTest) {
  const uint64_t block_size = 10;
  const uint32_t blocks_per_file = 20;
  Random random(5);

  BlobStore blob_store(test::TmpDir() + "/blob_store_test",
                       block_size,
                       blocks_per_file,
                       Env::Default());

  string buf;

  // put string of size 170
  test::RandomString(&random, 170, &buf);
  Blob r1;
  ASSERT_OK(blob_store.Put(buf.data(), 170, &r1));
  // use the first file
  for (size_t i = 0; i < r1.chunks.size(); ++i) {
    ASSERT_EQ(r1.chunks[0].bucket_id, 0u);
  }

  // put string of size 30
  test::RandomString(&random, 30, &buf);
  Blob r2;
  ASSERT_OK(blob_store.Put(buf.data(), 30, &r2));
  // use the first file
  for (size_t i = 0; i < r2.chunks.size(); ++i) {
    ASSERT_EQ(r2.chunks[0].bucket_id, 0u);
  }

  // delete blob of size 170
  ASSERT_OK(blob_store.Delete(r1));

  // put a string of size 100
  test::RandomString(&random, 100, &buf);
  Blob r3;
  ASSERT_OK(blob_store.Put(buf.data(), 100, &r3));
  // use the first file
  for (size_t i = 0; i < r3.chunks.size(); ++i) {
    ASSERT_EQ(r3.chunks[0].bucket_id, 0u);
  }

  // put a string of size 70
  test::RandomString(&random, 70, &buf);
  Blob r4;
  ASSERT_OK(blob_store.Put(buf.data(), 70, &r4));
  // use the first file
  for (size_t i = 0; i < r4.chunks.size(); ++i) {
    ASSERT_EQ(r4.chunks[0].bucket_id, 0u);
  }

  // put a string of size 5
  test::RandomString(&random, 5, &buf);
  Blob r5;
  ASSERT_OK(blob_store.Put(buf.data(), 70, &r5));
  // now you get to use the second file
  for (size_t i = 0; i < r5.chunks.size(); ++i) {
    ASSERT_EQ(r5.chunks[0].bucket_id, 1u);
  }

}

TEST(BlobStoreTest, CreateAndStoreTest) {
  const uint64_t block_size = 10;
  const uint32_t blocks_per_file = 1000;
  const int max_blurb_size = 300;
  Random random(5);

  BlobStore blob_store(test::TmpDir() + "/blob_store_test",
                       block_size,
                       blocks_per_file,
                       Env::Default());
  vector<pair<Blob, string>> ranges;

  for (int i = 0; i < 20000; ++i) {
    int decision = rand() % 5;
    if (decision <= 2 || ranges.size() == 0) {
      string buf;
      int size_blocks = (rand() % max_blurb_size + 1);
      int string_size = size_blocks * block_size - (rand() % block_size);
      test::RandomString(&random, string_size, &buf);
      Blob r;
      ASSERT_OK(blob_store.Put(buf.data(), string_size, &r));
      ranges.push_back(make_pair(r, buf));
    } else if (decision == 3) {
      int ti = rand() % ranges.size();
      string out_buf;
      ASSERT_OK(blob_store.Get(ranges[ti].first, &out_buf));
      ASSERT_EQ(ranges[ti].second, out_buf);
    } else {
      int ti = rand() % ranges.size();
      ASSERT_OK(blob_store.Delete(ranges[ti].first));
      ranges.erase(ranges.begin() + ti);
    }
  }
}

}  // namespace rocksdb

int main(int argc, char** argv) {
  return rocksdb::test::RunAllTests();
}

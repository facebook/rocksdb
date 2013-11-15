//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "util/arena_impl.h"
#include "util/random.h"
#include "util/testharness.h"

namespace rocksdb {

class ArenaImplTest { };

TEST(ArenaImplTest, Empty) {
  ArenaImpl arena0;
}

TEST(ArenaImplTest, MemoryAllocatedBytes) {
  const int N = 17;
  size_t req_sz;  //requested size
  size_t bsz = 8192;  // block size
  size_t expected_memory_allocated;

  ArenaImpl arena_impl(bsz);

  // requested size > quarter of a block:
  //   allocate requested size separately
  req_sz = 3001;
  for (int i = 0; i < N; i++) {
    arena_impl.Allocate(req_sz);
  }
  expected_memory_allocated = req_sz * N;
  ASSERT_EQ(arena_impl.MemoryAllocatedBytes(), expected_memory_allocated);

  // requested size < quarter of a block:
  //   allocate a block with the default size, then try to use unused part
  //   of the block. So one new block will be allocated for the first
  //   Allocate(99) call. All the remaining calls won't lead to new allocation.
  req_sz = 99;
  for (int i = 0; i < N; i++) {
    arena_impl.Allocate(req_sz);
  }
  expected_memory_allocated += bsz;
  ASSERT_EQ(arena_impl.MemoryAllocatedBytes(), expected_memory_allocated);

  // requested size > quarter of a block:
  //   allocate requested size separately
  req_sz = 99999999;
  for (int i = 0; i < N; i++) {
    arena_impl.Allocate(req_sz);
  }
  expected_memory_allocated += req_sz * N;
  ASSERT_EQ(arena_impl.MemoryAllocatedBytes(), expected_memory_allocated);
}

TEST(ArenaImplTest, Simple) {
  std::vector<std::pair<size_t, char*> > allocated;
  ArenaImpl arena_impl;
  const int N = 100000;
  size_t bytes = 0;
  Random rnd(301);
  for (int i = 0; i < N; i++) {
    size_t s;
    if (i % (N / 10) == 0) {
      s = i;
    } else {
      s = rnd.OneIn(4000) ? rnd.Uniform(6000) :
          (rnd.OneIn(10) ? rnd.Uniform(100) : rnd.Uniform(20));
    }
    if (s == 0) {
      // Our arena disallows size 0 allocations.
      s = 1;
    }
    char* r;
    if (rnd.OneIn(10)) {
      r = arena_impl.AllocateAligned(s);
    } else {
      r = arena_impl.Allocate(s);
    }

    for (unsigned int b = 0; b < s; b++) {
      // Fill the "i"th allocation with a known bit pattern
      r[b] = i % 256;
    }
    bytes += s;
    allocated.push_back(std::make_pair(s, r));
    ASSERT_GE(arena_impl.ApproximateMemoryUsage(), bytes);
    if (i > N/10) {
      ASSERT_LE(arena_impl.ApproximateMemoryUsage(), bytes * 1.10);
    }
  }
  for (unsigned int i = 0; i < allocated.size(); i++) {
    size_t num_bytes = allocated[i].first;
    const char* p = allocated[i].second;
    for (unsigned int b = 0; b < num_bytes; b++) {
      // Check the "i"th allocation for the known bit pattern
      ASSERT_EQ(int(p[b]) & 0xff, (int)(i % 256));
    }
  }
}

}  // namespace rocksdb

int main(int argc, char** argv) {
  return rocksdb::test::RunAllTests();
}

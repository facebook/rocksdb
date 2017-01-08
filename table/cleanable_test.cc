//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include <functional>

#include "port/port.h"
#include "port/stack_trace.h"
#include "rocksdb/iostats_context.h"
#include "rocksdb/perf_context.h"
#include "util/testharness.h"
#include "util/testutil.h"

namespace rocksdb {

class CleanableTest : public testing::Test {};

// Use this to keep track of the cleanups that were actually performed
void Multiplier(void* arg1, void* arg2) {
  int* res = reinterpret_cast<int*>(arg1);
  int* num = reinterpret_cast<int*>(arg2);
  *res *= *num;
}

// the first Cleanup is on stack and the rest on heap, so test with both cases
TEST_F(CleanableTest, Register) {
  int n2 = 2, n3 = 3;
  int res = 1;
  { Cleanable c1; }
  // ~Cleanable
  ASSERT_EQ(1, res);

  res = 1;
  {
    Cleanable c1;
    c1.RegisterCleanup(Multiplier, &res, &n2);  // res = 2;
  }
  // ~Cleanable
  ASSERT_EQ(2, res);

  res = 1;
  {
    Cleanable c1;
    c1.RegisterCleanup(Multiplier, &res, &n2);  // res = 2;
    c1.RegisterCleanup(Multiplier, &res, &n3);  // res = 2 * 3;
  }
  // ~Cleanable
  ASSERT_EQ(6, res);
}

// the first Cleanup is on stack and the rest on heap,
// so test all the combinations of them
TEST_F(CleanableTest, Delegation) {
  int n2 = 2, n3 = 3, n5 = 5, n7 = 7;
  int res = 1;
  {
    Cleanable c2;
    {
      Cleanable c1;
      c1.RegisterCleanup(Multiplier, &res, &n2);  // res = 2;
      c1.DelegateCleanupsTo(&c2);
    }
    // ~Cleanable
    ASSERT_EQ(1, res);
  }
  // ~Cleanable
  ASSERT_EQ(2, res);

  res = 1;
  {
    Cleanable c2;
    {
      Cleanable c1;
      c1.DelegateCleanupsTo(&c2);
    }
    // ~Cleanable
    ASSERT_EQ(1, res);
  }
  // ~Cleanable
  ASSERT_EQ(1, res);

  res = 1;
  {
    Cleanable c2;
    {
      Cleanable c1;
      c1.RegisterCleanup(Multiplier, &res, &n2);  // res = 2;
      c1.RegisterCleanup(Multiplier, &res, &n3);  // res = 2 * 3;
      c1.DelegateCleanupsTo(&c2);
    }
    // ~Cleanable
    ASSERT_EQ(1, res);
  }
  // ~Cleanable
  ASSERT_EQ(6, res);

  res = 1;
  {
    Cleanable c2;
    c2.RegisterCleanup(Multiplier, &res, &n5);  // res = 5;
    {
      Cleanable c1;
      c1.RegisterCleanup(Multiplier, &res, &n2);  // res = 2;
      c1.RegisterCleanup(Multiplier, &res, &n3);  // res = 2 * 3;
      c1.DelegateCleanupsTo(&c2);                 // res = 2 * 3 * 5;
    }
    // ~Cleanable
    ASSERT_EQ(1, res);
  }
  // ~Cleanable
  ASSERT_EQ(30, res);

  res = 1;
  {
    Cleanable c2;
    c2.RegisterCleanup(Multiplier, &res, &n5);  // res = 5;
    c2.RegisterCleanup(Multiplier, &res, &n7);  // res = 5 * 7;
    {
      Cleanable c1;
      c1.RegisterCleanup(Multiplier, &res, &n2);  // res = 2;
      c1.RegisterCleanup(Multiplier, &res, &n3);  // res = 2 * 3;
      c1.DelegateCleanupsTo(&c2);                 // res = 2 * 3 * 5 * 7;
    }
    // ~Cleanable
    ASSERT_EQ(1, res);
  }
  // ~Cleanable
  ASSERT_EQ(210, res);

  res = 1;
  {
    Cleanable c2;
    c2.RegisterCleanup(Multiplier, &res, &n5);  // res = 5;
    c2.RegisterCleanup(Multiplier, &res, &n7);  // res = 5 * 7;
    {
      Cleanable c1;
      c1.RegisterCleanup(Multiplier, &res, &n2);  // res = 2;
      c1.DelegateCleanupsTo(&c2);                 // res = 2 * 5 * 7;
    }
    // ~Cleanable
    ASSERT_EQ(1, res);
  }
  // ~Cleanable
  ASSERT_EQ(70, res);

  res = 1;
  {
    Cleanable c2;
    c2.RegisterCleanup(Multiplier, &res, &n5);  // res = 5;
    c2.RegisterCleanup(Multiplier, &res, &n7);  // res = 5 * 7;
    {
      Cleanable c1;
      c1.DelegateCleanupsTo(&c2);  // res = 5 * 7;
    }
    // ~Cleanable
    ASSERT_EQ(1, res);
  }
  // ~Cleanable
  ASSERT_EQ(35, res);

  res = 1;
  {
    Cleanable c2;
    c2.RegisterCleanup(Multiplier, &res, &n5);  // res = 5;
    {
      Cleanable c1;
      c1.DelegateCleanupsTo(&c2);  // res = 5;
    }
    // ~Cleanable
    ASSERT_EQ(1, res);
  }
  // ~Cleanable
  ASSERT_EQ(5, res);
}

}  // namespace rocksdb

int main(int argc, char** argv) {
  rocksdb::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

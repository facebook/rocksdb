//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "port/port.h"
#include "port/stack_trace.h"
#include "rocksdb/slice.h"
#include "test_util/testharness.h"
#include "test_util/testutil.h"

namespace rocksdb {

// Use this to keep track of the cleanups that were actually performed
void Multiplier(void* arg1, void* arg2) {
  int* res = reinterpret_cast<int*>(arg1);
  int* num = reinterpret_cast<int*>(arg2);
  *res *= *num;
}

class PinnableSliceTest : public testing::Test {
 public:
  void AssertEmpty(const PinnableSlice& slice) {
    ASSERT_EQ(0, slice.size());
    ASSERT_FALSE(slice.IsPinned());
  }

  void AssertSameData(const std::string& expected,
                      const PinnableSlice& slice) {
    std::string got;
    got.assign(slice.data(), slice.size());
    ASSERT_EQ(expected, got);
  }

  // Asserts that pinnable is in a clean state after being moved to
  // another PinnableSlice.
  // It asserts by trying to pin the slice.
  void AssertCleanState(PinnableSlice& pinnable, const Slice& slice) {
    AssertEmpty(pinnable);

    pinnable.PinSelf(slice);
    AssertSameData(slice.ToString(), pinnable);

    int res = 1;
    int n2 = 2;
    pinnable.PinSlice(slice, Multiplier, &res, &n2);
    AssertSameData(slice.ToString(), pinnable);
    ASSERT_EQ(1, res);
    pinnable.Reset();
    ASSERT_EQ(2, res);
  }
};

TEST_F(PinnableSliceTest, Move) {
  int n2 = 2;
  int res = 1;
  const std::string const_str1 = "123";
  const std::string const_str2 = "ABC";
  Slice slice1(const_str1);
  Slice slice2(const_str2);

  {
    // Test move constructor on a pinned slice.
    res = 1;
    PinnableSlice v1;
    v1.PinSlice(slice1, Multiplier, &res, &n2);
    PinnableSlice v2(std::move(v1));

    // Since v1's Cleanable has been moved to v2,
    // no cleanup should happen in Reset.
    v1.Reset();
    ASSERT_EQ(1, res);

    AssertSameData(const_str1, v2);
    AssertCleanState(v1, slice2);
  }
  // v2 is cleaned up.
  ASSERT_EQ(2, res);

  {
    // Test move constructor on an unpinned slice.
    PinnableSlice v1;
    v1.PinSelf(slice1);
    PinnableSlice v2(std::move(v1));

    AssertSameData(const_str1, v2);
    AssertCleanState(v1, slice2);
  }

  {
    // Test move assignment from a pinned slice to
    // another pinned slice.
    res = 1;
    PinnableSlice v1;
    v1.PinSlice(slice1, Multiplier, &res, &n2);
    PinnableSlice v2;
    v2.PinSlice(slice2, Multiplier, &res, &n2);
    v2 = std::move(v1);

    // v2's Cleanable will be Reset before moving
    // anything from v1.
    ASSERT_EQ(2, res);
    // Since v1's Cleanable has been moved to v2,
    // no cleanup should happen in Reset.
    v1.Reset();
    ASSERT_EQ(2, res);

    AssertSameData(const_str1, v2);
    AssertCleanState(v1, slice2);
  }
  // The Cleanable moved from v1 to v2 will be Reset.
  ASSERT_EQ(4, res);

  {
    // Test move assignment from a pinned slice to
    // an unpinned slice.
    res = 1;
    PinnableSlice v1;
    v1.PinSlice(slice1, Multiplier, &res, &n2);
    PinnableSlice v2;
    v2.PinSelf(slice2);
    v2 = std::move(v1);

    // Since v1's Cleanable has been moved to v2,
    // no cleanup should happen in Reset.
    v1.Reset();
    ASSERT_EQ(1, res);

    AssertSameData(const_str1, v2);
    AssertCleanState(v1, slice2);
  }
  // The Cleanable moved from v1 to v2 will be Reset.
  ASSERT_EQ(2, res);

  {
    // Test move assignment from an upinned slice to
    // another unpinned slice.
    PinnableSlice v1;
    v1.PinSelf(slice1);
    PinnableSlice v2;
    v2.PinSelf(slice2);
    v2 = std::move(v1);

    AssertSameData(const_str1, v2);
    AssertCleanState(v1, slice2);
  }

  {
    // Test move assignment from an upinned slice to
    // a pinned slice.
    res = 1;
    PinnableSlice v1;
    v1.PinSelf(slice1);
    PinnableSlice v2;
    v2.PinSlice(slice2, Multiplier, &res, &n2);
    v2 = std::move(v1);

    // v2's Cleanable will be Reset before moving
    // anything from v1.
    ASSERT_EQ(2, res);

    AssertSameData(const_str1, v2);
    AssertCleanState(v1, slice2);
  }
  // No Cleanable is moved from v1 to v2, so no more cleanup.
  ASSERT_EQ(2, res);
}

}  // namespace rocksdb

int main(int argc, char** argv) {
  rocksdb::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

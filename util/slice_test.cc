//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "rocksdb/slice.h"

#include <gtest/gtest.h>

#include "port/port.h"
#include "port/stack_trace.h"
#include "rocksdb/data_structure.h"
#include "rocksdb/types.h"
#include "test_util/testharness.h"
#include "test_util/testutil.h"

namespace ROCKSDB_NAMESPACE {

TEST(SliceTest, StringView) {
  std::string s = "foo";
  std::string_view sv = s;
  ASSERT_EQ(Slice(s), Slice(sv));
  ASSERT_EQ(Slice(s), Slice(std::move(sv)));
}

// Use this to keep track of the cleanups that were actually performed
void Multiplier(void* arg1, void* arg2) {
  int* res = reinterpret_cast<int*>(arg1);
  int* num = reinterpret_cast<int*>(arg2);
  *res *= *num;
}

class PinnableSliceTest : public testing::Test {
 public:
  void AssertSameData(const std::string& expected,
                      const PinnableSlice& slice) {
    std::string got;
    got.assign(slice.data(), slice.size());
    ASSERT_EQ(expected, got);
  }
};

// Test that the external buffer is moved instead of being copied.
TEST_F(PinnableSliceTest, MoveExternalBuffer) {
  Slice s("123");
  std::string buf;
  PinnableSlice v1(&buf);
  v1.PinSelf(s);

  PinnableSlice v2(std::move(v1));
  ASSERT_EQ(buf.data(), v2.data());
  ASSERT_EQ(&buf, v2.GetSelf());

  PinnableSlice v3;
  v3 = std::move(v2);
  ASSERT_EQ(buf.data(), v3.data());
  ASSERT_EQ(&buf, v3.GetSelf());
}

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
  }
  // v2 is cleaned up.
  ASSERT_EQ(2, res);

  {
    // Test move constructor on an unpinned slice.
    PinnableSlice v1;
    v1.PinSelf(slice1);
    PinnableSlice v2(std::move(v1));

    AssertSameData(const_str1, v2);
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
  }
  // No Cleanable is moved from v1 to v2, so no more cleanup.
  ASSERT_EQ(2, res);
}

// ***************************************************************** //
// Unit test for SmallEnumSet
class SmallEnumSetTest : public testing::Test {
 public:
  SmallEnumSetTest() {}
  ~SmallEnumSetTest() {}
};

TEST_F(SmallEnumSetTest, SmallSetTest) {
  FileTypeSet fs;
  ASSERT_TRUE(fs.Add(FileType::kIdentityFile));
  ASSERT_FALSE(fs.Add(FileType::kIdentityFile));
  ASSERT_TRUE(fs.Add(FileType::kInfoLogFile));
  ASSERT_TRUE(fs.Contains(FileType::kIdentityFile));
  ASSERT_FALSE(fs.Contains(FileType::kDBLockFile));
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

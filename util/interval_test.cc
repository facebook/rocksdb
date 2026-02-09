//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/db_test_util.h"
#include "rocksdb/data_structure.h"
#include "rocksdb/slice.h"

namespace ROCKSDB_NAMESPACE {

class IntervalSetTest : public testing::Test {};

TEST_F(IntervalSetTest, BasicTest) {
  IntervalSet<int> set;
  set.insert({2, 15});
  EXPECT_EQ(set.size(), 1);
  set.insert({5, 9});
  EXPECT_EQ(set.size(), 1);
  set.insert({0, 10});
  EXPECT_EQ(set.size(), 1);
  set.insert({25, 30});
  EXPECT_EQ(set.size(), 2);
  set.insert({16, 25});
  EXPECT_EQ(set.size(), 2);
  set.insert({45, 85});
  ASSERT_EQ(set.size(), 3);
  auto iter = set.begin();
  ASSERT_EQ(*iter, Interval<int>(0, 15));
  iter++;
  ASSERT_EQ(*iter, Interval<int>(16, 30));
  iter++;
  ASSERT_EQ(*iter, Interval<int>(45, 85));
  set.insert({31});
  iter = set.begin();
  ASSERT_EQ(*iter, Interval<int>(0, 15));
  iter++;
  ASSERT_EQ(*iter, Interval<int>(16, 30));
  iter++;
  ASSERT_EQ(*iter, Interval<int>(31));
}

TEST_F(IntervalSetTest, SliceTest) {
  IntervalSet<Slice, Comparator> set(BytewiseComparator());
  EXPECT_TRUE(set.insert("k00", "k10"));
  // Should do nothing
  EXPECT_TRUE(set.insert("k02", "k08"));
  auto iter = set.begin();
  ASSERT_EQ(iter->start().ToString(), "k00");
  ASSERT_EQ(iter->end().ToString(), "k10");
  ASSERT_EQ(set.size(), 1);
  iter++;
  ASSERT_EQ(iter, set.end());
  EXPECT_TRUE(set.insert("k15", "k20"));
  EXPECT_TRUE(set.insert("k16"));
  ASSERT_EQ(set.size(), 2);
  iter = set.begin();
  ASSERT_EQ(iter->start().ToString(), "k00");
  ASSERT_EQ(iter->end().ToString(), "k10");
  iter++;
  ASSERT_EQ(iter->start().ToString(), "k15");
  ASSERT_EQ(iter->has_end(), false);
  //
}

TEST_F(IntervalSetTest, PropModeTest) {
  IntervalSet<Slice, Comparator> set(BytewiseComparator(), true);
  EXPECT_TRUE(set.insert("k00", "k10"));
  // Should do nothing
  EXPECT_FALSE(set.insert("k02", "k08"));
  EXPECT_EQ(set.size(), 1);
  EXPECT_TRUE(set.insert("k15", "k20"));
  EXPECT_EQ(set.size(), 2);
  EXPECT_FALSE(set.insert("k16"));
  ASSERT_EQ(set.size(), 2);
  auto iter = set.begin();
  ASSERT_EQ(iter->start().ToString(), "k00");
  ASSERT_EQ(iter->end().ToString(), "k10");
  iter++;
  ASSERT_EQ(iter->start().ToString(), "k15");
  ASSERT_EQ(iter->end().ToString(), "k20");
  EXPECT_TRUE(set.insert("k12", "k14"));
  iter = set.begin();
  ASSERT_EQ(set.size(), 3);
  ASSERT_EQ(iter->start().ToString(), "k00");
  ASSERT_EQ(iter->end().ToString(), "k10");
  iter++;
  ASSERT_EQ(iter->start().ToString(), "k12");
  ASSERT_EQ(iter->end().ToString(), "k14");
  iter++;
  ASSERT_EQ(iter->start().ToString(), "k15");
  ASSERT_EQ(iter->end().ToString(), "k20");
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);

  return RUN_ALL_TESTS();
}

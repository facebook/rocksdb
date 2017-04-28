//  Copyright (c) 2016-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//  This source code is also licensed under the GPLv2 license found in the
//  COPYING file in the root directory of this source tree.

#include <algorithm>

#include "db/db_test_util.h"
#include "db/range_del_aggregator.h"
#include "rocksdb/comparator.h"
#include "util/testutil.h"

namespace rocksdb {

class RangeDelAggregatorTest : public testing::Test {};

namespace {

struct ExpectedPoint {
  Slice begin;
  SequenceNumber seq;
};

enum Direction {
  kForward,
  kReverse,
};

void VerifyRangeDels(const std::vector<RangeTombstone>& range_dels,
                     const std::vector<ExpectedPoint>& expected_points) {
  // Test same result regardless of which order the range deletions are added.
  for (Direction dir : {kForward, kReverse}) {
    auto icmp = InternalKeyComparator(BytewiseComparator());
    RangeDelAggregator range_del_agg(icmp, {} /* snapshots */, true);
    std::vector<std::string> keys, values;
    for (const auto& range_del : range_dels) {
      auto key_and_value = range_del.Serialize();
      keys.push_back(key_and_value.first.Encode().ToString());
      values.push_back(key_and_value.second.ToString());
    }
    if (dir == kReverse) {
      std::reverse(keys.begin(), keys.end());
      std::reverse(values.begin(), values.end());
    }
    std::unique_ptr<test::VectorIterator> range_del_iter(
        new test::VectorIterator(keys, values));
    range_del_agg.AddTombstones(std::move(range_del_iter));

    for (const auto expected_point : expected_points) {
      ParsedInternalKey parsed_key;
      parsed_key.user_key = expected_point.begin;
      parsed_key.sequence = expected_point.seq;
      parsed_key.type = kTypeValue;
      ASSERT_FALSE(range_del_agg.ShouldDelete(
          parsed_key,
          RangeDelAggregator::RangePositioningMode::kForwardTraversal));
      if (parsed_key.sequence > 0) {
        --parsed_key.sequence;
        ASSERT_TRUE(range_del_agg.ShouldDelete(
            parsed_key,
            RangeDelAggregator::RangePositioningMode::kForwardTraversal));
      }
    }
  }
}

}  // anonymous namespace

TEST_F(RangeDelAggregatorTest, Empty) { VerifyRangeDels({}, {{"a", 0}}); }

TEST_F(RangeDelAggregatorTest, SameStartAndEnd) {
  VerifyRangeDels({{"a", "a", 5}}, {{" ", 0}, {"a", 0}, {"b", 0}});
}

TEST_F(RangeDelAggregatorTest, Single) {
  VerifyRangeDels({{"a", "b", 10}}, {{" ", 0}, {"a", 10}, {"b", 0}});
}

TEST_F(RangeDelAggregatorTest, OverlapAboveLeft) {
  VerifyRangeDels({{"a", "c", 10}, {"b", "d", 5}},
                  {{" ", 0}, {"a", 10}, {"c", 5}, {"d", 0}});
}

TEST_F(RangeDelAggregatorTest, OverlapAboveRight) {
  VerifyRangeDels({{"a", "c", 5}, {"b", "d", 10}},
                  {{" ", 0}, {"a", 5}, {"b", 10}, {"d", 0}});
}

TEST_F(RangeDelAggregatorTest, OverlapAboveMiddle) {
  VerifyRangeDels({{"a", "d", 5}, {"b", "c", 10}},
                  {{" ", 0}, {"a", 5}, {"b", 10}, {"c", 5}, {"d", 0}});
}

TEST_F(RangeDelAggregatorTest, OverlapFully) {
  VerifyRangeDels({{"a", "d", 10}, {"b", "c", 5}},
                  {{" ", 0}, {"a", 10}, {"d", 0}});
}

TEST_F(RangeDelAggregatorTest, OverlapPoint) {
  VerifyRangeDels({{"a", "b", 5}, {"b", "c", 10}},
                  {{" ", 0}, {"a", 5}, {"b", 10}, {"c", 0}});
}

TEST_F(RangeDelAggregatorTest, SameStartKey) {
  VerifyRangeDels({{"a", "c", 5}, {"a", "b", 10}},
                  {{" ", 0}, {"a", 10}, {"b", 5}, {"c", 0}});
}

TEST_F(RangeDelAggregatorTest, SameEndKey) {
  VerifyRangeDels({{"a", "d", 5}, {"b", "d", 10}},
                  {{" ", 0}, {"a", 5}, {"b", 10}, {"d", 0}});
}

TEST_F(RangeDelAggregatorTest, GapsBetweenRanges) {
  VerifyRangeDels(
      {{"a", "b", 5}, {"c", "d", 10}, {"e", "f", 15}},
      {{" ", 0}, {"a", 5}, {"b", 0}, {"c", 10}, {"d", 0}, {"e", 15}, {"f", 0}});
}

// Note the Cover* tests also test cases where tombstones are inserted under a
// larger one when VerifyRangeDels() runs them in reverse
TEST_F(RangeDelAggregatorTest, CoverMultipleFromLeft) {
  VerifyRangeDels(
      {{"b", "d", 5}, {"c", "f", 10}, {"e", "g", 15}, {"a", "f", 20}},
      {{" ", 0}, {"a", 20}, {"f", 15}, {"g", 0}});
}

TEST_F(RangeDelAggregatorTest, CoverMultipleFromRight) {
  VerifyRangeDels(
      {{"b", "d", 5}, {"c", "f", 10}, {"e", "g", 15}, {"c", "h", 20}},
      {{" ", 0}, {"b", 5}, {"c", 20}, {"h", 0}});
}

TEST_F(RangeDelAggregatorTest, CoverMultipleFully) {
  VerifyRangeDels(
      {{"b", "d", 5}, {"c", "f", 10}, {"e", "g", 15}, {"a", "h", 20}},
      {{" ", 0}, {"a", 20}, {"h", 0}});
}

TEST_F(RangeDelAggregatorTest, AlternateMultipleAboveBelow) {
  VerifyRangeDels(
      {{"b", "d", 15}, {"c", "f", 10}, {"e", "g", 20}, {"a", "h", 5}},
      {{" ", 0},
       {"a", 5},
       {"b", 15},
       {"d", 10},
       {"e", 20},
       {"g", 5},
       {"h", 0}});
}

}  // namespace rocksdb

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

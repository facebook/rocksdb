//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2012 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <cstring>
#include <vector>

#include "test_util/testharness.h"
#include "util/random.h"

using ROCKSDB_NAMESPACE::Random;

TEST(RandomTest, Uniform) {
  const int average = 20;
  for (uint32_t seed : {0, 1, 2, 37, 4096}) {
    Random r(seed);
    for (int range : {1, 2, 8, 12, 100}) {
      std::vector<int> counts(range, 0);

      for (int i = 0; i < range * average; ++i) {
        ++counts.at(r.Uniform(range));
      }
      int max_variance = static_cast<int>(std::sqrt(range) * 2 + 4);
      for (int i = 0; i < range; ++i) {
        EXPECT_GE(counts[i], std::max(1, average - max_variance));
        EXPECT_LE(counts[i], average + max_variance + 1);
      }
    }
  }
}

TEST(RandomTest, OneIn) {
  Random r(42);
  for (int range : {1, 2, 8, 12, 100, 1234}) {
    const int average = 100;
    int count = 0;
    for (int i = 0; i < average * range; ++i) {
      if (r.OneIn(range)) {
        ++count;
      }
    }
    if (range == 1) {
      EXPECT_EQ(count, average);
    } else {
      int max_variance = static_cast<int>(std::sqrt(average) * 1.5);
      EXPECT_GE(count, average - max_variance);
      EXPECT_LE(count, average + max_variance);
    }
  }
}

TEST(RandomTest, OneInOpt) {
  Random r(42);
  for (int range : {-12, 0, 1, 2, 8, 12, 100, 1234}) {
    const int average = 100;
    int count = 0;
    for (int i = 0; i < average * range; ++i) {
      if (r.OneInOpt(range)) {
        ++count;
      }
    }
    if (range < 1) {
      EXPECT_EQ(count, 0);
    } else if (range == 1) {
      EXPECT_EQ(count, average);
    } else {
      int max_variance = static_cast<int>(std::sqrt(average) * 1.5);
      EXPECT_GE(count, average - max_variance);
      EXPECT_LE(count, average + max_variance);
    }
  }
}

TEST(RandomTest, PercentTrue) {
  Random r(42);
  for (int pct : {-12, 0, 1, 2, 10, 50, 90, 98, 99, 100, 1234}) {
    const int samples = 10000;

    int count = 0;
    for (int i = 0; i < samples; ++i) {
      if (r.PercentTrue(pct)) {
        ++count;
      }
    }
    if (pct <= 0) {
      EXPECT_EQ(count, 0);
    } else if (pct >= 100) {
      EXPECT_EQ(count, samples);
    } else {
      int est = (count * 100 + (samples / 2)) / samples;
      EXPECT_EQ(est, pct);
    }
  }
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);

  return RUN_ALL_TESTS();
}

//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include <gflags/gflags.h>

#include "dynamic_bloom.h"
#include "util/logging.h"
#include "util/testharness.h"
#include "util/testutil.h"

DEFINE_int32(bits_per_key, 10, "");
DEFINE_int32(num_probes, 6, "");

namespace rocksdb {

static Slice Key(int i, char* buffer) {
  memcpy(buffer, &i, sizeof(i));
  return Slice(buffer, sizeof(i));
}

class DynamicBloomTest {
};

TEST(DynamicBloomTest, EmptyFilter) {
  DynamicBloom bloom(100, 2);
  ASSERT_TRUE(! bloom.MayContain("hello"));
  ASSERT_TRUE(! bloom.MayContain("world"));
}

TEST(DynamicBloomTest, Small) {
  DynamicBloom bloom(100, 2);
  bloom.Add("hello");
  bloom.Add("world");
  ASSERT_TRUE(bloom.MayContain("hello"));
  ASSERT_TRUE(bloom.MayContain("world"));
  ASSERT_TRUE(! bloom.MayContain("x"));
  ASSERT_TRUE(! bloom.MayContain("foo"));
}

static int NextLength(int length) {
  if (length < 10) {
    length += 1;
  } else if (length < 100) {
    length += 10;
  } else if (length < 1000) {
    length += 100;
  } else {
    length += 1000;
  }
  return length;
}

TEST(DynamicBloomTest, VaryingLengths) {
  char buffer[sizeof(int)];

  // Count number of filters that significantly exceed the false positive rate
  int mediocre_filters = 0;
  int good_filters = 0;

  fprintf(stderr, "bits_per_key: %d  num_probes: %d\n",
          FLAGS_bits_per_key, FLAGS_num_probes);

  for (int length = 1; length <= 10000; length = NextLength(length)) {
    uint32_t bloom_bits = std::max(length * FLAGS_bits_per_key, 64);
    DynamicBloom bloom(bloom_bits, FLAGS_num_probes);
    for (int i = 0; i < length; i++) {
      bloom.Add(Key(i, buffer));
      ASSERT_TRUE(bloom.MayContain(Key(i, buffer)));
    }

    // All added keys must match
    for (int i = 0; i < length; i++) {
      ASSERT_TRUE(bloom.MayContain(Key(i, buffer)))
        << "Length " << length << "; key " << i;
    }

    // Check false positive rate

    int result = 0;
    for (int i = 0; i < 10000; i++) {
      if (bloom.MayContain(Key(i + 1000000000, buffer))) {
        result++;
      }
    }
    double rate = result / 10000.0;

    fprintf(stderr, "False positives: %5.2f%% @ length = %6d ; \n",
            rate*100.0, length);

    //ASSERT_LE(rate, 0.02);   // Must not be over 2%
    if (rate > 0.0125)
      mediocre_filters++;  // Allowed, but not too often
    else
      good_filters++;
  }

  fprintf(stderr, "Filters: %d good, %d mediocre\n",
          good_filters, mediocre_filters);

  ASSERT_LE(mediocre_filters, good_filters/5);
}

// Different bits-per-byte

}  // namespace rocksdb

int main(int argc, char** argv) {
  google::ParseCommandLineFlags(&argc, &argv, true);

  return rocksdb::test::RunAllTests();
}

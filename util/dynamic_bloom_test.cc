//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#define __STDC_FORMAT_MACROS
#include <inttypes.h>
#include <algorithm>
#include <gflags/gflags.h>

#include "dynamic_bloom.h"
#include "port/port.h"
#include "util/logging.h"
#include "util/testharness.h"
#include "util/testutil.h"
#include "util/stop_watch.h"

DEFINE_int32(bits_per_key, 10, "");
DEFINE_int32(num_probes, 6, "");
DEFINE_bool(enable_perf, false, "");

namespace rocksdb {

static Slice Key(uint64_t i, char* buffer) {
  memcpy(buffer, &i, sizeof(i));
  return Slice(buffer, sizeof(i));
}

class DynamicBloomTest {
};

TEST(DynamicBloomTest, EmptyFilter) {
  DynamicBloom bloom1(100, 0, 2);
  ASSERT_TRUE(!bloom1.MayContain("hello"));
  ASSERT_TRUE(!bloom1.MayContain("world"));

  DynamicBloom bloom2(CACHE_LINE_SIZE * 8 * 2 - 1, 1, 2);
  ASSERT_TRUE(!bloom2.MayContain("hello"));
  ASSERT_TRUE(!bloom2.MayContain("world"));
}

TEST(DynamicBloomTest, Small) {
  DynamicBloom bloom1(100, 0, 2);
  bloom1.Add("hello");
  bloom1.Add("world");
  ASSERT_TRUE(bloom1.MayContain("hello"));
  ASSERT_TRUE(bloom1.MayContain("world"));
  ASSERT_TRUE(!bloom1.MayContain("x"));
  ASSERT_TRUE(!bloom1.MayContain("foo"));

  DynamicBloom bloom2(CACHE_LINE_SIZE * 8 * 2 - 1, 1, 2);
  bloom2.Add("hello");
  bloom2.Add("world");
  ASSERT_TRUE(bloom2.MayContain("hello"));
  ASSERT_TRUE(bloom2.MayContain("world"));
  ASSERT_TRUE(!bloom2.MayContain("x"));
  ASSERT_TRUE(!bloom2.MayContain("foo"));
}

static uint32_t NextNum(uint32_t num) {
  if (num < 10) {
    num += 1;
  } else if (num < 100) {
    num += 10;
  } else if (num < 1000) {
    num += 100;
  } else {
    num += 1000;
  }
  return num;
}

TEST(DynamicBloomTest, VaryingLengths) {
  char buffer[sizeof(uint64_t)];

  // Count number of filters that significantly exceed the false positive rate
  int mediocre_filters = 0;
  int good_filters = 0;
  uint32_t num_probes = static_cast<uint32_t>(FLAGS_num_probes);

  fprintf(stderr, "bits_per_key: %d  num_probes: %d\n",
          FLAGS_bits_per_key, num_probes);

  for (uint32_t cl_per_block = 0; cl_per_block < num_probes;
      ++cl_per_block) {
    for (uint32_t num = 1; num <= 10000; num = NextNum(num)) {
      uint32_t bloom_bits = 0;
      if (cl_per_block == 0) {
        bloom_bits = std::max(num * FLAGS_bits_per_key, 64U);
      } else {
        bloom_bits = std::max(num * FLAGS_bits_per_key,
            cl_per_block * CACHE_LINE_SIZE * 8);
      }
      DynamicBloom bloom(bloom_bits, cl_per_block, num_probes);
      for (uint64_t i = 0; i < num; i++) {
        bloom.Add(Key(i, buffer));
        ASSERT_TRUE(bloom.MayContain(Key(i, buffer)));
      }

      // All added keys must match
      for (uint64_t i = 0; i < num; i++) {
        ASSERT_TRUE(bloom.MayContain(Key(i, buffer)))
          << "Num " << num << "; key " << i;
      }

      // Check false positive rate

      int result = 0;
      for (uint64_t i = 0; i < 10000; i++) {
        if (bloom.MayContain(Key(i + 1000000000, buffer))) {
          result++;
        }
      }
      double rate = result / 10000.0;

      fprintf(stderr, "False positives: %5.2f%% @ num = %6u, bloom_bits = %6u, "
              "cl per block = %u\n", rate*100.0, num, bloom_bits, cl_per_block);

      if (rate > 0.0125)
        mediocre_filters++;  // Allowed, but not too often
      else
        good_filters++;
    }

    fprintf(stderr, "Filters: %d good, %d mediocre\n",
            good_filters, mediocre_filters);
    ASSERT_LE(mediocre_filters, good_filters/5);
  }
}

TEST(DynamicBloomTest, perf) {
  StopWatchNano timer(Env::Default());
  uint32_t num_probes = static_cast<uint32_t>(FLAGS_num_probes);

  if (!FLAGS_enable_perf) {
    return;
  }

  for (uint64_t m = 1; m <= 8; ++m) {
    const uint64_t num_keys = m * 8 * 1024 * 1024;
    fprintf(stderr, "testing %" PRIu64 "M keys\n", m * 8);

    DynamicBloom std_bloom(num_keys * 10, 0, num_probes);

    timer.Start();
    for (uint64_t i = 1; i <= num_keys; ++i) {
      std_bloom.Add(Slice(reinterpret_cast<const char*>(&i), 8));
    }

    uint64_t elapsed = timer.ElapsedNanos();
    fprintf(stderr, "standard bloom, avg add latency %" PRIu64 "\n",
            elapsed / num_keys);

    uint64_t count = 0;
    timer.Start();
    for (uint64_t i = 1; i <= num_keys; ++i) {
      if (std_bloom.MayContain(Slice(reinterpret_cast<const char*>(&i), 8))) {
        ++count;
      }
    }
    elapsed = timer.ElapsedNanos();
    fprintf(stderr, "standard bloom, avg query latency %" PRIu64 "\n",
            elapsed / count);
    ASSERT_TRUE(count == num_keys);

    for (uint32_t cl_per_block = 1; cl_per_block <= num_probes;
        ++cl_per_block) {
      DynamicBloom blocked_bloom(num_keys * 10, cl_per_block, num_probes);

      timer.Start();
      for (uint64_t i = 1; i <= num_keys; ++i) {
        blocked_bloom.Add(Slice(reinterpret_cast<const char*>(&i), 8));
      }

      uint64_t elapsed = timer.ElapsedNanos();
      fprintf(stderr, "blocked bloom(%d), avg add latency %" PRIu64 "\n",
              cl_per_block, elapsed / num_keys);

      uint64_t count = 0;
      timer.Start();
      for (uint64_t i = 1; i <= num_keys; ++i) {
        if (blocked_bloom.MayContain(
              Slice(reinterpret_cast<const char*>(&i), 8))) {
          ++count;
        }
      }

      elapsed = timer.ElapsedNanos();
      fprintf(stderr, "blocked bloom(%d), avg query latency %" PRIu64 "\n",
              cl_per_block, elapsed / count);
      ASSERT_TRUE(count == num_keys);
    }
  }
}

}  // namespace rocksdb

int main(int argc, char** argv) {
  google::ParseCommandLineFlags(&argc, &argv, true);

  return rocksdb::test::RunAllTests();
}

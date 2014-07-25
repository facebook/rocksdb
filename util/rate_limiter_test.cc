//  Copyright (c) 2014, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#define __STDC_FORMAT_MACROS
#include <inttypes.h>
#include <limits>
#include "util/testharness.h"
#include "util/rate_limiter.h"
#include "util/random.h"
#include "rocksdb/env.h"

namespace rocksdb {

class RateLimiterTest {
};

TEST(RateLimiterTest, StartStop) {
  std::unique_ptr<RateLimiter> limiter(new GenericRateLimiter(100, 100, 10));
}

TEST(RateLimiterTest, Rate) {
  auto* env = Env::Default();
  struct Arg {
    Arg(int64_t target_rate, int burst)
      : limiter(new GenericRateLimiter(target_rate, 100 * 1000, 10)),
        request_size(target_rate / 10),
        burst(burst) {}
    std::unique_ptr<RateLimiter> limiter;
    int64_t request_size;
    int burst;
  };

  auto writer = [](void* p) {
    auto* env = Env::Default();
    auto* arg = static_cast<Arg*>(p);
    // Test for 2 seconds
    auto until = env->NowMicros() + 2 * 1000000;
    Random r((uint32_t)(env->NowNanos() %
          std::numeric_limits<uint32_t>::max()));
    while (env->NowMicros() < until) {
      for (int i = 0; i < static_cast<int>(r.Skewed(arg->burst) + 1); ++i) {
        arg->limiter->Request(r.Uniform(arg->request_size - 1) + 1,
                              Env::IO_HIGH);
      }
      arg->limiter->Request(r.Uniform(arg->request_size - 1) + 1,
                            Env::IO_LOW);
    }
  };

  for (int i = 1; i <= 16; i*=2) {
    int64_t target = i * 1024 * 10;
    Arg arg(target, i / 4 + 1);
    auto start = env->NowMicros();
    for (int t = 0; t < i; ++t) {
      env->StartThread(writer, &arg);
    }
    env->WaitForJoin();

    auto elapsed = env->NowMicros() - start;
    double rate = arg.limiter->GetTotalBytesThrough()
                  * 1000000.0 / elapsed;
    fprintf(stderr, "request size [1 - %" PRIi64 "], limit %" PRIi64
                    " KB/sec, actual rate: %lf KB/sec, elapsed %.2lf seconds\n",
            arg.request_size - 1, target / 1024, rate / 1024,
            elapsed / 1000000.0);

    ASSERT_GE(rate / target, 0.95);
    ASSERT_LE(rate / target, 1.05);
  }
}

}  // namespace rocksdb

int main(int argc, char** argv) {
  return rocksdb::test::RunAllTests();
}

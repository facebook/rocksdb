//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//

#include "util/benchharness.h"
#include <vector>

namespace rocksdb {

BENCHMARK(insertFrontVector) {
  std::vector<int> v;
  for (int i = 0; i < 100; i++) {
    v.insert(v.begin(), i);
  }
}

BENCHMARK_RELATIVE(insertBackVector) {
  std::vector<int> v;
  for (size_t i = 0; i < 100; i++) {
    v.insert(v.end(), i);
  }
}

BENCHMARK_N(insertFrontVector_n, n) {
  std::vector<int> v;
  for (size_t i = 0; i < n; i++) {
    v.insert(v.begin(), i);
  }
}

BENCHMARK_RELATIVE_N(insertBackVector_n, n) {
  std::vector<int> v;
  for (size_t i = 0; i < n; i++) {
    v.insert(v.end(), i);
  }
}

BENCHMARK_N(insertFrontEnd_n, n) {
  std::vector<int> v;
  for (size_t i = 0; i < n; i++) {
    v.insert(v.begin(), i);
  }
  for (size_t i = 0; i < n; i++) {
    v.insert(v.end(), i);
  }
}

BENCHMARK_RELATIVE_N(insertFrontEndSuspend_n, n) {
  std::vector<int> v;
  for (size_t i = 0; i < n; i++) {
    v.insert(v.begin(), i);
  }
  BENCHMARK_SUSPEND {
    for (size_t i = 0; i < n; i++) {
      v.insert(v.end(), i);
    }
  }
}

}  // namespace rocksdb

int main(int argc, char** argv) {
  rocksdb::benchmark::RunBenchmarks();
  return 0;
}

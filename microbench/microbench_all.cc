//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

// this is a simple micro-benchmark for compare ribbon filter vs. other filter
// for more comprehensive, please check the dedicate util/filter_bench.
#include <benchmark/benchmark.h>
#include "microbench/ribbon_bench.cc"
#include "microbench/db_basic_bench.cc"

#ifndef BENCHMARK_MAIN_EXPANDED
BENCHMARK_MAIN();
#define BENCHMARK_MAIN_EXPANDED
#endif

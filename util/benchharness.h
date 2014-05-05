//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// This code is derived from Benchmark.h implemented in Folly, the opensourced
// Facebook C++ library available at https://github.com/facebook/folly
// The code has removed any dependence on other folly and boost libraries

#pragma once

#include <gflags/gflags.h>

#include <cassert>
#include <functional>
#include <limits>

#include "util/testharness.h"
#include "rocksdb/env.h"

namespace rocksdb {
namespace benchmark {

/**
 * Runs all benchmarks defined. Usually put in main().
 */
void RunBenchmarks();

namespace detail {

/**
 * Adds a benchmark wrapped in a std::function. Only used
 * internally. Pass by value is intentional.
 */
void AddBenchmarkImpl(const char* file,
                      const char* name,
                      std::function<uint64_t(unsigned int)>);

}  // namespace detail


/**
 * Supporting type for BENCHMARK_SUSPEND defined below.
 */
struct BenchmarkSuspender {
  BenchmarkSuspender() { start_ = Env::Default()->NowNanos(); }

  BenchmarkSuspender(const BenchmarkSuspender&) = delete;
  BenchmarkSuspender(BenchmarkSuspender && rhs) {
    start_ = rhs.start_;
    rhs.start_ = 0;
  }

  BenchmarkSuspender& operator=(const BenchmarkSuspender &) = delete;
  BenchmarkSuspender& operator=(BenchmarkSuspender && rhs) {
    if (start_ > 0) {
      tally();
    }
    start_ = rhs.start_;
    rhs.start_ = 0;
    return *this;
  }

  ~BenchmarkSuspender() {
    if (start_ > 0) {
      tally();
    }
  }

  void Dismiss() {
    assert(start_ > 0);
    tally();
    start_ = 0;
  }

  void Rehire() { start_ = Env::Default()->NowNanos(); }

  /**
   * This helps the macro definition. To get around the dangers of
   * operator bool, returns a pointer to member (which allows no
   * arithmetic).
   */
  /* implicit */
  operator int BenchmarkSuspender::*() const { return nullptr; }

  /**
   * Accumulates nanoseconds spent outside benchmark.
   */
  typedef uint64_t NanosecondsSpent;
  static NanosecondsSpent nsSpent;

 private:
  void tally() {
    uint64_t end = Env::Default()->NowNanos();
    nsSpent += start_ - end;
    start_ = end;
  }

  uint64_t start_;
};

/**
 * Adds a benchmark. Usually not called directly but instead through
 * the macro BENCHMARK defined below. The lambda function involved
 * must take exactly one parameter of type unsigned, and the benchmark
 * uses it with counter semantics (iteration occurs inside the
 * function).
 */
template <typename Lambda>
void
AddBenchmark_n(const char* file, const char* name, Lambda&& lambda) {
  auto execute = [=](unsigned int times) -> uint64_t {
    BenchmarkSuspender::nsSpent = 0;
    uint64_t start, end;
    auto env = Env::Default();

    // CORE MEASUREMENT STARTS
    start = env->NowNanos();
    lambda(times);
    end = env->NowNanos();
    // CORE MEASUREMENT ENDS
    return (end - start) - BenchmarkSuspender::nsSpent;
  };

  detail::AddBenchmarkImpl(file, name,
                           std::function<uint64_t(unsigned int)>(execute));
}

/**
 * Adds a benchmark. Usually not called directly but instead through
 * the macro BENCHMARK defined below. The lambda function involved
 * must take zero parameters, and the benchmark calls it repeatedly
 * (iteration occurs outside the function).
 */
template <typename Lambda>
void
AddBenchmark(const char* file, const char* name, Lambda&& lambda) {
  AddBenchmark_n(file, name, [=](unsigned int times) {
      while (times-- > 0) {
        lambda();
      }
    });
}

}  // namespace benchmark
}  // namespace rocksdb

/**
 * FB_ONE_OR_NONE(hello, world) expands to hello and
 * FB_ONE_OR_NONE(hello) expands to nothing. This macro is used to
 * insert or eliminate text based on the presence of another argument.
 */
#define FB_ONE_OR_NONE(a, ...) FB_THIRD(a, ## __VA_ARGS__, a)
#define FB_THIRD(a, b, ...) __VA_ARGS__

#define FB_CONCATENATE_IMPL(s1, s2) s1##s2
#define FB_CONCATENATE(s1, s2) FB_CONCATENATE_IMPL(s1, s2)

#define FB_ANONYMOUS_VARIABLE(str) FB_CONCATENATE(str, __LINE__)

#define FB_STRINGIZE(x) #x

/**
 * Introduces a benchmark function. Used internally, see BENCHMARK and
 * friends below.
 */
#define BENCHMARK_IMPL_N(funName, stringName, paramType, paramName)     \
  static void funName(paramType);                                       \
  static bool FB_ANONYMOUS_VARIABLE(rocksdbBenchmarkUnused) = (         \
    ::rocksdb::benchmark::AddBenchmark_n(__FILE__, stringName,          \
      [](paramType paramName) { funName(paramName); }),                 \
    true);                                                              \
  static void funName(paramType paramName)

#define BENCHMARK_IMPL(funName, stringName)                             \
  static void funName();                                                \
  static bool FB_ANONYMOUS_VARIABLE(rocksdbBenchmarkUnused) = (         \
    ::rocksdb::benchmark::AddBenchmark(__FILE__, stringName,            \
      []() { funName(); }),                                             \
    true);                                                              \
  static void funName()

/**
 * Introduces a benchmark function. Use with either one one or two
 * arguments. The first is the name of the benchmark. Use something
 * descriptive, such as insertVectorBegin. The second argument may be
 * missing, or could be a symbolic counter. The counter dictates how
 * many internal iteration the benchmark does. Example:
 *
 * BENCHMARK(vectorPushBack) {
 *   vector<int> v;
 *   v.push_back(42);
 * }
 *
 * BENCHMARK_N(insertVectorBegin, n) {
 *   vector<int> v;
 *   FOR_EACH_RANGE (i, 0, n) {
 *     v.insert(v.begin(), 42);
 *   }
 * }
 */
#define BENCHMARK_N(name, ...)                                  \
  BENCHMARK_IMPL_N(                                             \
    name,                                                       \
    FB_STRINGIZE(name),                                         \
    FB_ONE_OR_NONE(unsigned, ## __VA_ARGS__),                   \
    __VA_ARGS__)

#define BENCHMARK(name)                                         \
  BENCHMARK_IMPL(                                               \
    name,                                                       \
    FB_STRINGIZE(name))

/**
 * Defines a benchmark that passes a parameter to another one. This is
 * common for benchmarks that need a "problem size" in addition to
 * "number of iterations". Consider:
 *
 * void pushBack(uint n, size_t initialSize) {
 *   vector<int> v;
 *   BENCHMARK_SUSPEND {
 *     v.resize(initialSize);
 *   }
 *   FOR_EACH_RANGE (i, 0, n) {
 *    v.push_back(i);
 *   }
 * }
 * BENCHMARK_PARAM(pushBack, 0)
 * BENCHMARK_PARAM(pushBack, 1000)
 * BENCHMARK_PARAM(pushBack, 1000000)
 *
 * The benchmark above estimates the speed of push_back at different
 * initial sizes of the vector. The framework will pass 0, 1000, and
 * 1000000 for initialSize, and the iteration count for n.
 */
#define BENCHMARK_PARAM(name, param)                                    \
  BENCHMARK_NAMED_PARAM(name, param, param)

/*
 * Like BENCHMARK_PARAM(), but allows a custom name to be specified for each
 * parameter, rather than using the parameter value.
 *
 * Useful when the parameter value is not a valid token for string pasting,
 * of when you want to specify multiple parameter arguments.
 *
 * For example:
 *
 * void addValue(uint n, int64_t bucketSize, int64_t min, int64_t max) {
 *   Histogram<int64_t> hist(bucketSize, min, max);
 *   int64_t num = min;
 *   FOR_EACH_RANGE (i, 0, n) {
 *     hist.addValue(num);
 *     ++num;
 *     if (num > max) { num = min; }
 *   }
 * }
 *
 * BENCHMARK_NAMED_PARAM(addValue, 0_to_100, 1, 0, 100)
 * BENCHMARK_NAMED_PARAM(addValue, 0_to_1000, 10, 0, 1000)
 * BENCHMARK_NAMED_PARAM(addValue, 5k_to_20k, 250, 5000, 20000)
 */
#define BENCHMARK_NAMED_PARAM(name, param_name, ...)                    \
  BENCHMARK_IMPL(                                                       \
      FB_CONCATENATE(name, FB_CONCATENATE(_, param_name)),              \
      FB_STRINGIZE(name) "(" FB_STRINGIZE(param_name) ")") {            \
    name(__VA_ARGS__);                                                  \
  }

#define BENCHMARK_NAMED_PARAM_N(name, param_name, ...)                  \
  BENCHMARK_IMPL_N(                                                     \
      FB_CONCATENATE(name, FB_CONCATENATE(_, param_name)),              \
      FB_STRINGIZE(name) "(" FB_STRINGIZE(param_name) ")",              \
      unsigned,                                                         \
      iters) {                                                          \
    name(iters, ## __VA_ARGS__);                                        \
  }

/**
 * Just like BENCHMARK, but prints the time relative to a
 * baseline. The baseline is the most recent BENCHMARK() seen in
 * lexical order. Example:
 *
 * // This is the baseline
 * BENCHMARK_N(insertVectorBegin, n) {
 *   vector<int> v;
 *   FOR_EACH_RANGE (i, 0, n) {
 *     v.insert(v.begin(), 42);
 *   }
 * }
 *
 * BENCHMARK_RELATIVE_N(insertListBegin, n) {
 *   list<int> s;
 *   FOR_EACH_RANGE (i, 0, n) {
 *     s.insert(s.begin(), 42);
 *   }
 * }
 *
 * Any number of relative benchmark can be associated with a
 * baseline. Another BENCHMARK() occurrence effectively establishes a
 * new baseline.
 */
#define BENCHMARK_RELATIVE_N(name, ...)                         \
  BENCHMARK_IMPL_N(                                             \
    name,                                                       \
    "%" FB_STRINGIZE(name),                                     \
    FB_ONE_OR_NONE(unsigned, ## __VA_ARGS__),                   \
    __VA_ARGS__)

#define BENCHMARK_RELATIVE(name)                                \
  BENCHMARK_IMPL(                                               \
    name,                                                       \
    "%" FB_STRINGIZE(name))

/**
 * A combination of BENCHMARK_RELATIVE and BENCHMARK_PARAM.
 */
#define BENCHMARK_RELATIVE_PARAM(name, param)                   \
  BENCHMARK_RELATIVE_NAMED_PARAM(name, param, param)

/**
 * A combination of BENCHMARK_RELATIVE and BENCHMARK_NAMED_PARAM.
 */
#define BENCHMARK_RELATIVE_NAMED_PARAM(name, param_name, ...)           \
  BENCHMARK_IMPL_N(                                                     \
      FB_CONCATENATE(name, FB_CONCATENATE(_, param_name)),              \
      "%" FB_STRINGIZE(name) "(" FB_STRINGIZE(param_name) ")",          \
      unsigned,                                                         \
      iters) {                                                          \
    name(iters, ## __VA_ARGS__);                                        \
  }

/**
 * Draws a line of dashes.
 */
#define BENCHMARK_DRAW_LINE()                                       \
  static bool FB_ANONYMOUS_VARIABLE(rocksdbBenchmarkUnused) = (     \
    ::rocksdb::benchmark::AddBenchmark(__FILE__, "-", []() { }),    \
    true);

/**
 * Allows execution of code that doesn't count torward the benchmark's
 * time budget. Example:
 *
 * BENCHMARK_START_GROUP(insertVectorBegin, n) {
 *   vector<int> v;
 *   BENCHMARK_SUSPEND {
 *     v.reserve(n);
 *   }
 *   FOR_EACH_RANGE (i, 0, n) {
 *     v.insert(v.begin(), 42);
 *   }
 * }
 */
#define BENCHMARK_SUSPEND                               \
  if (auto FB_ANONYMOUS_VARIABLE(BENCHMARK_SUSPEND) =   \
      ::rocksdb::benchmark::BenchmarkSuspender()) {}    \
  else

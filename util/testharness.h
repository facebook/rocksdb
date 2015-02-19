//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once

#include <stdio.h>
#include <stdlib.h>
#include <sstream>
#include <string>
#include "port/stack_trace.h"
#include "rocksdb/env.h"
#include "rocksdb/slice.h"
#include "util/random.h"
#include "util/string_util.h"

namespace rocksdb {
namespace test {

// Run some of the tests registered by the TEST() macro.  If the
// environment variable "ROCKSDB_TESTS" and "ROCKSDB_TESTS_FROM"
// are not set, runs all tests. Otherwise, run all tests after
// ROCKSDB_TESTS_FROM and those specified by ROCKSDB_TESTS.
// Partial name match also works for ROCKSDB_TESTS and
// ROCKSDB_TESTS_FROM. E.g., suppose the tests are:
//    TEST(Foo, Hello) { ... }
//    TEST(Foo, World) { ... }
// ROCKSDB_TESTS=Hello will run the first test
// ROCKSDB_TESTS=o     will run both tests
// ROCKSDB_TESTS=Junk  will run no tests
//
// Returns 0 if all tests pass.
// Dies or returns a non-zero value if some test fails.
extern int RunAllTests();

// Return the directory to use for temporary storage.
extern std::string TmpDir(Env* env = Env::Default());

// Return a randomization seed for this run.  Typically returns the
// same number on repeated invocations of this binary, but automated
// runs may be able to vary the seed.
extern int RandomSeed();

class TesterHelper;

// An instance of Tester is allocated to hold temporary state during
// the execution of an assertion.
class Tester {
  friend class TesterHelper;

 private:
  bool ok_;
  std::stringstream ss_;

 public:
  Tester() : ok_(true) {}

  Tester& Is(bool b, const char* msg) {
    if (!b) {
      ss_ << " Assertion failure " << msg;
      ok_ = false;
    }
    return *this;
  }

  Tester& IsOk(const Status& s) {
    if (!s.ok()) {
      ss_ << " " << s.ToString();
      ok_ = false;
    }
    return *this;
  }

  Tester& IsNotOk(const Status& s) {
    if (s.ok()) {
      ss_ << " Error status expected";
      ok_ = false;
    }
    return *this;
  }

#define BINARY_OP(name,op)                              \
  template <class X, class Y>                           \
  Tester& name(const X& x, const Y& y) {                \
    if (! (x op y)) {                                   \
      ss_ << " failed: " << x << (" " #op " ") << y;    \
      ok_ = false;                                      \
    }                                                   \
    return *this;                                       \
  }

  BINARY_OP(IsEq, ==)
  BINARY_OP(IsNe, !=)
  BINARY_OP(IsGe, >=)
  BINARY_OP(IsGt, >)
  BINARY_OP(IsLe, <=)
  BINARY_OP(IsLt, <)
#undef BINARY_OP

  // Attach the specified value to the error message if an error has occurred
  template <class V>
  Tester& operator<<(const V& value) {
    if (!ok_) {
      ss_ << " " << value;
    }
    return *this;
  }

  operator bool() const { return ok_; }
};

class TesterHelper {
 private:
  const char* fname_;
  int line_;

 public:
  TesterHelper(const char* f, int l) : fname_(f), line_(l) {}

  void operator=(const Tester& tester) {
    fprintf(stderr, "%s:%d:%s\n", fname_, line_, tester.ss_.str().c_str());
    port::PrintStack(2);
    exit(1);
  }
};

// This is trying to solve:
// * Evaluate expression
// * Abort the test if the evaluation is not successful with the evaluation
// details.
// * Support operator << with ASSERT* for extra messages provided by the user
// code of ASSERT*
//
// For the third, we need to make sure that an expression at the end of macro
// supports << operator. But since we can have multiple of << we cannot abort
// inside implementation of operator <<, as we may miss some extra message. That
// is why there is TesterHelper with operator = which has lower precedence then
// operator <<, and it will be called after all messages from use code are
// accounted by <<.
//
// operator bool is added to Tester to make possible its declaration inside if
// statement and do not pollute its outer scope with the name tester. But in C++
// we cannot do any other operations inside if statement besides declaration.
// Then in order to get inside if body there are two options: make operator
// Tester::bool return true if ok_ == false or put the body into else part.
#define TEST_EXPRESSION_(expression)                  \
  if (::rocksdb::test::Tester& tester = (expression)) \
    ;                                                 \
  else                                                \
  ::rocksdb::test::TesterHelper(__FILE__, __LINE__) = tester

#define ASSERT_TRUE(c) TEST_EXPRESSION_(::rocksdb::test::Tester().Is((c), #c))
#define ASSERT_OK(s) TEST_EXPRESSION_(::rocksdb::test::Tester().IsOk((s)))
#define ASSERT_NOK(s) TEST_EXPRESSION_(::rocksdb::test::Tester().IsNotOk((s)))
#define ASSERT_EQ(a, b) \
  TEST_EXPRESSION_(::rocksdb::test::Tester().IsEq((a), (b)))
#define ASSERT_NE(a, b) \
  TEST_EXPRESSION_(::rocksdb::test::Tester().IsNe((a), (b)))
#define ASSERT_GE(a, b) \
  TEST_EXPRESSION_(::rocksdb::test::Tester().IsGe((a), (b)))
#define ASSERT_GT(a, b) \
  TEST_EXPRESSION_(::rocksdb::test::Tester().IsGt((a), (b)))
#define ASSERT_LE(a, b) \
  TEST_EXPRESSION_(::rocksdb::test::Tester().IsLe((a), (b)))
#define ASSERT_LT(a, b) \
  TEST_EXPRESSION_(::rocksdb::test::Tester().IsLt((a), (b)))

#define TCONCAT(a, b) TCONCAT1(a, b)
#define TCONCAT1(a, b) a##b

#define TEST(base, name)                                              \
  class TCONCAT(_Test_, name) : public base {                         \
   public:                                                            \
    void _Run();                                                      \
    static void _RunIt() {                                            \
      TCONCAT(_Test_, name) t;                                        \
      t._Run();                                                       \
    }                                                                 \
  };                                                                  \
  bool TCONCAT(_Test_ignored_, name) __attribute__((__unused__))      \
    = ::rocksdb::test::RegisterTest(#base, #name,                     \
                                    &TCONCAT(_Test_, name)::_RunIt);  \
  void TCONCAT(_Test_, name)::_Run()

// Register the specified test.  Typically not used directly, but
// invoked via the macro expansion of TEST.
extern bool RegisterTest(const char* base, const char* name, void (*func)());

}  // namespace test
}  // namespace rocksdb

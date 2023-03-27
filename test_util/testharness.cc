//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "test_util/testharness.h"

#include <regex>
#include <string>
#include <thread>

namespace ROCKSDB_NAMESPACE {
namespace test {

#ifdef OS_WIN
#include <windows.h>

std::string GetPidStr() { return std::to_string(GetCurrentProcessId()); }
#else
std::string GetPidStr() { return std::to_string(getpid()); }
#endif

::testing::AssertionResult AssertStatus(const char* s_expr, const Status& s) {
  if (s.ok()) {
    return ::testing::AssertionSuccess();
  } else {
    return ::testing::AssertionFailure() << s_expr << std::endl << s.ToString();
  }
}

std::string TmpDir(Env* env) {
  std::string dir;
  Status s = env->GetTestDirectory(&dir);
  EXPECT_OK(s);
  return dir;
}

std::string PerThreadDBPath(std::string dir, std::string name) {
  size_t tid = std::hash<std::thread::id>()(std::this_thread::get_id());
  return dir + "/" + name + "_" + GetPidStr() + "_" + std::to_string(tid);
}

std::string PerThreadDBPath(std::string name) {
  return PerThreadDBPath(test::TmpDir(), name);
}

std::string PerThreadDBPath(Env* env, std::string name) {
  return PerThreadDBPath(test::TmpDir(env), name);
}

int RandomSeed() {
  const char* env = getenv("TEST_RANDOM_SEED");
  int result = (env != nullptr ? atoi(env) : 301);
  if (result <= 0) {
    result = 301;
  }
  return result;
}

TestRegex::TestRegex(const std::string& pattern)
    : impl_(std::make_shared<Impl>(pattern)), pattern_(pattern) {}
TestRegex::TestRegex(const char* pattern)
    : impl_(std::make_shared<Impl>(pattern)), pattern_(pattern) {}

const std::string& TestRegex::GetPattern() const { return pattern_; }

class TestRegex::Impl : public std::regex {
 public:
  using std::regex::basic_regex;
};

bool TestRegex::Matches(const std::string& str) const {
  if (impl_) {
    return std::regex_match(str, *impl_);
  } else {
    // Should not call Matches on unset Regex
    assert(false);
    return false;
  }
}

::testing::AssertionResult AssertMatchesRegex(const char* str_expr,
                                              const char* pattern_expr,
                                              const std::string& str,
                                              const TestRegex& pattern) {
  if (pattern.Matches(str)) {
    return ::testing::AssertionSuccess();
  } else if (TestRegex("\".*\"").Matches(pattern_expr)) {
    // constant regex string
    return ::testing::AssertionFailure()
           << str << " (" << str_expr << ")" << std::endl
           << "does not match regex " << pattern.GetPattern();
  } else {
    // runtime regex string
    return ::testing::AssertionFailure()
           << str << " (" << str_expr << ")" << std::endl
           << "does not match regex" << std::endl
           << pattern.GetPattern() << " (" << pattern_expr << ")";
  }
}

}  // namespace test
}  // namespace ROCKSDB_NAMESPACE

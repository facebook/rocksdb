//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "test_util/testharness.h"

#include <algorithm>
#include <cstdlib>
#include <mutex>
#include <regex>
#include <string>
#include <thread>
#include <unordered_set>
#include <vector>

#ifndef OS_WIN
#include <unistd.h>
#endif

#include "file/file_util.h"
#ifndef NDEBUG
#include "test_util/sync_point.h"
#endif

namespace {
std::mutex& RegisteredPerTestPathsMutex() {
  static std::mutex mutex;
  return mutex;
}

std::unordered_set<std::string>& RegisteredPerTestPaths() {
  static auto* paths = new std::unordered_set<std::string>();
  return *paths;
}

void RegisterPerTestPath(std::string path) {
  std::lock_guard<std::mutex> lock(RegisteredPerTestPathsMutex());
  RegisteredPerTestPaths().insert(std::move(path));
}

void ClearRegisteredPerTestPathsImpl() {
  std::lock_guard<std::mutex> lock(RegisteredPerTestPathsMutex());
  RegisteredPerTestPaths().clear();
}

ROCKSDB_NAMESPACE::Status CleanupRegisteredPerTestPathsImpl() {
  std::vector<std::string> paths;
  {
    std::lock_guard<std::mutex> lock(RegisteredPerTestPathsMutex());
    paths.assign(RegisteredPerTestPaths().begin(),
                 RegisteredPerTestPaths().end());
    RegisteredPerTestPaths().clear();
  }

  std::sort(paths.begin(), paths.end(),
            [](const std::string& lhs, const std::string& rhs) {
              if (lhs.size() != rhs.size()) {
                return lhs.size() > rhs.size();
              }
              return lhs < rhs;
            });

  ROCKSDB_NAMESPACE::Env* env = ROCKSDB_NAMESPACE::Env::Default();
  for (const auto& path : paths) {
    if (path.empty()) {
      continue;
    }

    ROCKSDB_NAMESPACE::Status exists = env->FileExists(path);
    if (exists.IsNotFound()) {
      continue;
    }
    if (!exists.ok()) {
      return ROCKSDB_NAMESPACE::Status::IOError(
          "Failed to stat registered test path " + path + ": " +
          exists.ToString());
    }

    bool is_dir = false;
    ROCKSDB_NAMESPACE::Status s = env->IsDirectory(path, &is_dir);
    if (s.ok()) {
      s = is_dir ? ROCKSDB_NAMESPACE::DestroyDir(env, path)
                 : env->DeleteFile(path);
    }
    if (!s.ok() && !s.IsNotFound()) {
      return ROCKSDB_NAMESPACE::Status::IOError(
          "Failed to clean up registered test path " + path + ": " +
          s.ToString());
    }
  }
  return ROCKSDB_NAMESPACE::Status::OK();
}

void CleanupSyncPointState() {
#ifndef NDEBUG
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearAllCallBacks();
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearTrace();
  // LoadDependency({}) clears successors_, predecessors_, and
  // cleared_points_ maps. Without this, stale dependencies from a previous
  // test can block SyncPoint::Process() in the next test.
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency({});
#endif  // !NDEBUG
}

// Global gtest event listener that cleans up shared per-test state after every
// test. Under sharded execution, multiple tests now share one process, so any
// leaked SyncPoint callbacks or leftover PerThreadDBPath() directories can
// accumulate across thousands of test cases.
class TestStateCleanupListener : public ::testing::EmptyTestEventListener {
  void OnTestStart(const ::testing::TestInfo& /*test_info*/) override {
    ClearRegisteredPerTestPathsImpl();
  }

  void OnTestEnd(const ::testing::TestInfo& test_info) override {
    CleanupSyncPointState();
    if (getenv("KEEP_DB") == nullptr && test_info.result()->Passed()) {
      EXPECT_OK(CleanupRegisteredPerTestPathsImpl());
    } else {
      ClearRegisteredPerTestPathsImpl();
    }
  }
};

// Auto-register the listener via static initialization.
// This runs before main() and before any test fixtures are constructed.
static int RegisterTestStateCleanup() noexcept {
  ::testing::TestEventListeners& listeners =
      ::testing::UnitTest::GetInstance()->listeners();
  listeners.Append(new TestStateCleanupListener());
  return 0;
}
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
[[maybe_unused]] static int test_state_cleanup_registered_ =
    RegisterTestStateCleanup();
}  // namespace

namespace ROCKSDB_NAMESPACE::test {

namespace detail {

void ClearRegisteredPerTestPaths() { ClearRegisteredPerTestPathsImpl(); }

Status CleanupRegisteredPerTestPaths() {
  return CleanupRegisteredPerTestPathsImpl();
}

}  // namespace detail

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
  std::string path =
      dir + "/" + name + "_" + GetPidStr() + "_" + std::to_string(tid);
  RegisterPerTestPath(path);
  return path;
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

bool HasBigMem() {
  const char* env = getenv("ROCKSDB_BIGMEM_TESTS");
  if (env != nullptr && env[0] != '\0') {
    return true;
  }
#ifdef _SC_PHYS_PAGES
  // Check whether the system has at least 128GB of physical RAM.
  // _SC_PHYS_PAGES is available on Linux and macOS but is not standard POSIX.
  long pages = sysconf(_SC_PHYS_PAGES);
  long page_size = sysconf(_SC_PAGE_SIZE);
  if (pages > 0 && page_size > 0) {
    size_t total_bytes =
        static_cast<size_t>(pages) * static_cast<size_t>(page_size);
    return total_bytes >= size_t{128} << 30;
  }
#endif
  return false;
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

}  // namespace ROCKSDB_NAMESPACE::test

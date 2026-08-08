//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <cassert>
#include <cstdio>
#include <cstdlib>

#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {

template <typename... Args>
inline void DbStressLogStatusFailure(const Status& status,
                                     const char* expression,
                                     const char* message_format, Args... args) {
  assert(!status.ok());
  if (message_format != nullptr) {
    if constexpr (sizeof...(Args) == 0) {
      std::fprintf(stderr, "%s", message_format);
    } else {
      std::fprintf(stderr, message_format, args...);
    }
    std::fprintf(stderr, ": ");
  }
  std::fprintf(stderr, "Status assertion failed for %s: %s\n", expression,
               status.ToString().c_str());
  std::fflush(stderr);
}

}  // namespace ROCKSDB_NAMESPACE

#define DB_STRESS_ASSERT_OK(status)                                            \
  do {                                                                         \
    const auto& db_stress_status = (status);                                   \
    if (!db_stress_status.ok()) {                                              \
      ::ROCKSDB_NAMESPACE::DbStressLogStatusFailure(db_stress_status, #status, \
                                                    nullptr);                  \
      assert(db_stress_status.ok());                                           \
      std::exit(1);                                                            \
    }                                                                          \
  } while (false)

#define DB_STRESS_ASSERT_OK_MSG(status, ...)                                   \
  do {                                                                         \
    const auto& db_stress_status = (status);                                   \
    if (!db_stress_status.ok()) {                                              \
      ::ROCKSDB_NAMESPACE::DbStressLogStatusFailure(db_stress_status, #status, \
                                                    __VA_ARGS__);              \
      assert(db_stress_status.ok());                                           \
      std::exit(1);                                                            \
    }                                                                          \
  } while (false)

//  Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#ifndef ROCKSDB_LITE

#include <memory>
#include <string>

#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {

// A wrapper for parsed regular expressions. The regex syntax and matching is
// compatible with std::regex.
//
// !!!!!! WARNING !!!!!!: The implementation currently uses std::regex, which
// has terrible performance in some cases, including possible crash due to
// stack overflow. See https://gcc.gnu.org/bugzilla/show_bug.cgi?id=61582
// for example. Avoid use in production as much as possible.
//
// Internal note: see also TestRegex
class Regex {
 public:
  // Note: Cannot be constructed with a pattern, so that syntax errors can
  // be handled without using exceptions.

  // Parse returns OK and saves to `out` when the pattern is valid regex
  // syntax (modified ECMAScript), or else returns InvalidArgument.
  // See https://en.cppreference.com/w/cpp/regex/ecmascript
  static Status Parse(const char *pattern, Regex *out);
  static Status Parse(const std::string &pattern, Regex *out);

  // Checks that the whole of str is matched by this regex. If called on a
  // default-constructed Regex, will trigger assertion failure in DEBUG build
  // or return false in release build.
  bool Matches(const std::string &str) const;

 private:
  class Impl;
  std::shared_ptr<Impl> impl_;  // shared_ptr for simple implementation
};
}  // namespace ROCKSDB_NAMESPACE

#endif  // ROCKSDB_LITE

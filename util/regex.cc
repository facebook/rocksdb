//  Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

// LITE not supported here in part because of exception handling
#ifndef ROCKSDB_LITE

#include "rocksdb/utilities/regex.h"

#include <cassert>
#include <regex>

namespace ROCKSDB_NAMESPACE {

// This section would change for alternate underlying implementations other
// than std::regex.
#if 1
class Regex::Impl : public std::regex {
 public:
  using std::regex::basic_regex;
};

bool Regex::Matches(const std::string &str) const {
  if (impl_) {
    return std::regex_match(str, *impl_);
  } else {
    // Should not call Matches on unset Regex
    assert(false);
    return false;
  }
}

Status Regex::Parse(const std::string &pattern, Regex *out) {
  try {
    out->impl_.reset(new Impl(pattern));
    return Status::OK();
  } catch (const std::regex_error &e) {
    return Status::InvalidArgument(e.what());
  }
}
#endif

Status Regex::Parse(const char *pattern, Regex *out) {
  return Parse(std::string(pattern), out);
}

}  // namespace ROCKSDB_NAMESPACE

#endif  // ROCKSDB_LITE

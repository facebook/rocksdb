//  Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "rocksdb/utilities/regex.h"

#include <cassert>
#include <regex>

namespace ROCKSDB_NAMESPACE {

// This section would change for alternate underlying implementations other
// than std::regex.
#if 1
class Regex::Impl {
 public:
  std::regex regex_;
};

bool Regex::Matches(const std::string& str) const {
  return std::regex_match(str, impl_->regex_);
}

Status Regex::Parse(const std::string &pattern, Regex *out) {
  try {
    out->impl_->regex_ = std::regex(pattern);
    return Status::OK();
  } catch (const std::regex_error& e) {
    return Status::InvalidArgument();
  }
}
#endif

Status Regex::Parse(const char *pattern, Regex *out) {
  return Parse(std::string(pattern), out);
}

Regex::Regex() : impl_(new Impl()) {}
Regex::Regex(const Regex& other) : impl_(new Impl(*other.impl_)) {}
Regex& Regex::operator=(const Regex& other) {
  *impl_ = *other.impl_;
  return *this;
}

Regex::~Regex() { delete impl_; }



}  // namespace ROCKSDB_NAMESPACE

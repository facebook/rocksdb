// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <memory>

#include "rocksdb/listener.h"

namespace rocksdb {

struct CompactionInfo {
  int output_level;
};

struct CompactionOutputInfo {
  Slice next_key;
  uint64_t current_output_file_size;
};

class CompactionPolicy {
 public:
  virtual ~CompactionPolicy() = default;

  virtual void NotifyKeyAdded(const Slice& key) = 0;

  virtual bool ShouldEndCurrentOutputFile(const CompactionOutputInfo& info) = 0;
};

class CompactionPolicyFactory {
 public:
  virtual ~CompactionPolicyFactory() = default;

  virtual const char* Name() const = 0;

  virtual std::unique_ptr<CompactionPolicy> NewCompactionPolicy(
      const CompactionInfo& info) = 0;
};

}  // namespace rocksdb

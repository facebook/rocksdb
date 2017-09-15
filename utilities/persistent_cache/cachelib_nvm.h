// Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
#pragma once

#include "rocksdb/persistent_cache.h"

namespace rocksdb {

class CacheLibNvm : public PersistentCache {
 public:
  ~CacheLibNvm() override = default;

  Status Insert(const Slice& key, const char* data,
                const size_t size) override;

  Status Lookup(const Slice& key, std::unique_ptr<char[]>* data,
                size_t* size) override;

  bool IsCompressed() { return true; }

  StatsType Stats() override;

  std::string GetPrintableOptions() const override;
};

}  // namespace rocksdb


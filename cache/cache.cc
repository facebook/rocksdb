//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "rocksdb/cache.h"

#include "cache/lru_cache.h"
#include "options/options_helper.h"
#include "util/string_util.h"

namespace ROCKSDB_NAMESPACE {
Status Cache::CreateFromString(const ConfigOptions& /*opts*/,
                               const std::string& value,
                               std::shared_ptr<Cache>* result) {
  Status status;
  std::shared_ptr<Cache> cache;
  if (value.find('=') == std::string::npos) {
    cache = NewLRUCache(ParseSizeT(value));
  } else {
#ifndef ROCKSDB_LITE
    LRUCacheOptions cache_opts;
    if (!ParseOptionHelper(reinterpret_cast<char*>(&cache_opts),
                           OptionType::kLRUCacheOptions, value)) {
      status = Status::InvalidArgument("Invalid cache options");
    }
    cache = NewLRUCache(cache_opts);
#else
    status = Status::NotSupported("Cannot load cache in LITE mode ", value);
#endif  //! ROCKSDB_LITE
  }
  if (status.ok()) {
    result->swap(cache);
  }
  return status;
}
}  // namespace ROCKSDB_NAMESPACE

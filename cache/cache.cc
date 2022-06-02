//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "rocksdb/cache.h"

#include "cache/clock_cache.h"
#include "cache/fast_lru_cache.h"
#include "cache/lru_cache.h"
#include "rocksdb/configurable.h"
#include "rocksdb/secondary_cache.h"
#include "rocksdb/utilities/customizable_util.h"
#include "util/string_util.h"

namespace ROCKSDB_NAMESPACE {
namespace {
#ifndef ROCKSDB_LITE
static int RegisterBuiltinCache(ObjectLibrary& library,
                                const std::string& /*arg*/) {
  library.AddFactory<Cache>(
      lru_cache::LRUCache::kClassName(),
      [](const std::string& /*uri*/, std::unique_ptr<Cache>* guard,
         std::string* /* errmsg */) {
        guard->reset(new lru_cache::LRUCache());
        return guard->get();
      });
  library.AddFactory<Cache>(
      fast_lru_cache::LRUCache::kClassName(),
      [](const std::string& /*uri*/, std::unique_ptr<Cache>* guard,
         std::string* /* errmsg */) {
        guard->reset(new fast_lru_cache::LRUCache());
        return guard->get();
      });
  library.AddFactory<Cache>(
      ClockCache::kClassName(),
      [](const std::string& /*uri*/, std::unique_ptr<Cache>* guard,
         std::string* errmsg) {
        std::unique_ptr<ClockCache> clock;
        Status s = ClockCache::CreateClockCache(&clock);
        if (!s.ok()) {
          *errmsg = s.ToString();
        }
        guard->reset(clock.release());
        return guard->get();
      });
  return 1;
}
#endif  // ROCKSDB_LITE
}  // namespace

Status SecondaryCache::CreateFromString(
    const ConfigOptions& config_options, const std::string& value,
    std::shared_ptr<SecondaryCache>* result) {
  return LoadSharedObject<SecondaryCache>(config_options, value, nullptr,
                                          result);
}

Status Cache::CreateFromString(const ConfigOptions& config_options,
                               const std::string& value,
                               std::shared_ptr<Cache>* result) {
#ifndef ROCKSDB_LITE
  static std::once_flag once;
  std::call_once(once, [&]() {
    RegisterBuiltinCache(*(ObjectLibrary::Default().get()), "");
  });
#endif  // ROCKSDB_LITE
  Status status;
  std::shared_ptr<Cache> cache;
  if (!value.empty()) {
    std::string id;
    std::unordered_map<std::string, std::string> opt_map;
    status = Configurable::GetOptionsMap(value, LRUCache::kClassName(), &id,
                                         &opt_map);
    if (!status.ok()) {
      return status;
    } else if (opt_map.empty() && !id.empty() && isdigit(id.at(0))) {
      // If there are no name=value options and the id is a digit, assume
      // it is an old-style LRUCache created by capacity only
      cache = NewLRUCache(ParseSizeT(id));
    } else {
      status = NewSharedObject<Cache>(config_options, id, opt_map, &cache);
      if (status.ok() && !config_options.invoke_prepare_options) {
        // Always invoke PrepareOptions for a cache...
        status = cache->PrepareOptions(config_options);
      }
    }
  }
  if (status.ok()) {
    result->swap(cache);
  }
  return status;
}
}  // namespace ROCKSDB_NAMESPACE

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
#include "options/customizable_helper.h"
#include "rocksdb/utilities/options_type.h"
#include "util/string_util.h"

namespace ROCKSDB_NAMESPACE {
Cache::Cache(const std::shared_ptr<MemoryAllocator>& allocator)
    : memory_allocator_(std::move(allocator)) {}

const std::string Cache::kLRUCacheName = "LRUCache";
const std::string Cache::kClockCacheName = "ClockCache";

static bool LoadCache(const std::string& id, std::shared_ptr<Cache>* cache) {
  bool success = true;
  if (id.empty()) {
    cache->reset();
  } else if (id == Cache::kLRUCacheName) {
    cache->reset(new LRUCache(0));
  } else if (isdigit(id.at(0))) {
    auto lru = NewLRUCache(ParseSizeT(id));
    cache->swap(lru);
  } else {
    success = false;
  }
  return success;
}

Status Cache::CreateFromString(const std::string& value,
                               const ConfigOptions& opts,
                               std::shared_ptr<Cache>* result) {
  std::string id;
  std::unordered_map<std::string, std::string> opt_map;
  Status status =
      Customizable::GetOptionsMap(value, Cache::kLRUCacheName, &id, &opt_map);
  if (!status.ok()) {
    return status;
  } else if (!LoadCache(id, result)) {
#ifndef ROCKSDB_LITE
    status = opts.registry->NewSharedObject<Cache>(id, result);
#else
    status = Status::NotSupported("Cannot load cache in LITE mode ", id);
#endif
    if (!status.ok()) {
      if (opts.ignore_unknown_objects && status.IsNotSupported()) {
        return Status::OK();
      } else {
        return status;
      }
    }
  }
  return Customizable::ConfigureNewObject(result->get(), id, "", opt_map, opts);
}
}  // namespace ROCKSDB_NAMESPACE

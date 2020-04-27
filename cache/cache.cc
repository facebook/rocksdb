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
static const std::string kLRUCacheName = "LRUCache";
const char* LRUCache::Name() const { return kLRUCacheName.c_str(); }

static bool LoadCache(const std::string& id, std::shared_ptr<Cache>* cache) {
  bool success = true;
  if (id.empty()) {
    cache->reset();
  } else if (id == kLRUCacheName) {
    cache->reset(new LRUCache(0));
  } else if (isdigit(id.at(0))) {
    auto lru = NewLRUCache(ParseSizeT(id));
    cache->swap(lru);
  } else {
    success = false;
  }
  return success;
}

static std::unordered_map<std::string, OptionTypeInfo> cache_options_type_info =
    {
#ifndef ROCKSDB_LITE
        {"allocator",
         OptionTypeInfo::AsCustomS<MemoryAllocator>(
             0, OptionVerificationType::kNormal,
             OptionTypeFlags::kCompareNever | OptionTypeFlags::kAllowNull)},
#endif  // ROCKSDB_LITE
};

Cache::Cache(const std::shared_ptr<MemoryAllocator>& allocator)
    : memory_allocator_(std::move(allocator)) {
  RegisterOptions("MemoryAllocator", &memory_allocator_,
                  &cache_options_type_info);
}

Status Cache::CreateFromString(const ConfigOptions& config_options,
                               const std::string& value,
                               std::shared_ptr<Cache>* result) {
  std::string id;
  std::unordered_map<std::string, std::string> opt_map;
  Status status =
      Customizable::GetOptionsMap(value, kLRUCacheName, &id, &opt_map);
  if (!status.ok()) {
    return status;
  } else if (!LoadCache(id, result)) {
#ifndef ROCKSDB_LITE
    status = config_options.registry->NewSharedObject<Cache>(id, result);
#else
    status = Status::NotSupported("Cannot load cache in LITE mode ", value);
#endif  //! ROCKSDB_LITE
    if (!status.ok()) {
      if (config_options.ignore_unknown_objects && status.IsNotSupported()) {
        return Status::OK();
      } else {
        return status;
      }
    }
  }
  return Customizable::ConfigureNewObject(config_options, result->get(), id, "",
                                          opt_map);
}
}  // namespace ROCKSDB_NAMESPACE

//  Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "cache/cache_entry_roles.h"

#include <mutex>

#include "port/lang.h"

namespace ROCKSDB_NAMESPACE {

std::array<const char*, kNumCacheEntryRoles> kCacheEntryRoleToCamelString{{
    "DataBlock",
    "FilterBlock",
    "FilterMetaBlock",
    "DeprecatedFilterBlock",
    "IndexBlock",
    "OtherBlock",
    "WriteBuffer",
    "CompressionDictionaryBuildingBuffer",
    "FilterConstruction",
    "Misc",
}};

std::array<const char*, kNumCacheEntryRoles> kCacheEntryRoleToHyphenString{{
    "data-block",
    "filter-block",
    "filter-meta-block",
    "deprecated-filter-block",
    "index-block",
    "other-block",
    "write-buffer",
    "compression-dictionary-building-buffer",
    "filter-construction",
    "misc",
}};

namespace {

struct Registry {
  std::mutex mutex;
  std::unordered_map<Cache::DeleterFn, CacheEntryRole> role_map;
  void Register(Cache::DeleterFn fn, CacheEntryRole role) {
    std::lock_guard<std::mutex> lock(mutex);
    role_map[fn] = role;
  }
  std::unordered_map<Cache::DeleterFn, CacheEntryRole> Copy() {
    std::lock_guard<std::mutex> lock(mutex);
    return role_map;
  }
};

Registry& GetRegistry() {
  STATIC_AVOID_DESTRUCTION(Registry, registry);
  return registry;
}

}  // namespace

void RegisterCacheDeleterRole(Cache::DeleterFn fn, CacheEntryRole role) {
  GetRegistry().Register(fn, role);
}

std::unordered_map<Cache::DeleterFn, CacheEntryRole> CopyCacheDeleterRoleMap() {
  return GetRegistry().Copy();
}

}  // namespace ROCKSDB_NAMESPACE

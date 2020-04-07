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
#ifndef ROCKSDB_LITE
LRUCacheOptions dummy_lru_cache_options;

template <typename T1>
int offset_of(T1 LRUCacheOptions::*member) {
  return int(size_t(&(dummy_lru_cache_options.*member)) -
             size_t(&dummy_lru_cache_options));
}

static std::unordered_map<std::string, OptionTypeInfo>
    lru_cache_options_type_info = {
        {"capacity",
         {offset_of(&LRUCacheOptions::capacity), OptionType::kSizeT,
          OptionVerificationType::kNormal, OptionTypeFlags::kMutable,
          offsetof(struct LRUCacheOptions, capacity)}},
        {"num_shard_bits",
         {offset_of(&LRUCacheOptions::num_shard_bits), OptionType::kInt,
          OptionVerificationType::kNormal, OptionTypeFlags::kMutable,
          offsetof(struct LRUCacheOptions, num_shard_bits)}},
        {"strict_capacity_limit",
         {offset_of(&LRUCacheOptions::strict_capacity_limit),
          OptionType::kBoolean, OptionVerificationType::kNormal,
          OptionTypeFlags::kMutable,
          offsetof(struct LRUCacheOptions, strict_capacity_limit)}},
        {"high_pri_pool_ratio",
         {offset_of(&LRUCacheOptions::high_pri_pool_ratio), OptionType::kDouble,
          OptionVerificationType::kNormal, OptionTypeFlags::kMutable,
          offsetof(struct LRUCacheOptions, high_pri_pool_ratio)}}};
#endif  // ROCKSDB_LITE

Status Cache::CreateFromString(const std::string& value,
                               const ConfigOptions& opts,
                               std::shared_ptr<Cache>* result) {
  Status status;
  std::shared_ptr<Cache> cache;
  if (value.find('=') == std::string::npos) {
    cache = NewLRUCache(ParseSizeT(value));
  } else {
#ifndef ROCKSDB_LITE
    LRUCacheOptions cache_opts;
    status =
        OptionTypeInfo::ParseStruct("", &lru_cache_options_type_info, "", value,
                                    opts, reinterpret_cast<char*>(&cache_opts));
    if (status.ok()) {
      cache = NewLRUCache(cache_opts);
    }
#else
    (void)opts;
    status = Status::NotSupported("Cannot load cache in LITE mode ", value);
#endif  //! ROCKSDB_LITE
  }
  if (status.ok()) {
    result->swap(cache);
  }
  return status;
}
}  // namespace ROCKSDB_NAMESPACE

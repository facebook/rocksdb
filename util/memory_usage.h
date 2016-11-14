//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#pragma once

#include <unordered_map>

namespace rocksdb {

// Helper methods to estimate memroy usage by std containers.

template <class Key, class Value, class Hash>
size_t ApproximateMemoryUsage(
    const std::unordered_map<Key, Value, Hash>& umap) {
  typedef std::unordered_map<Key, Value, Hash> Map;
  return sizeof(umap) +
         // Size of all items plus a next pointer for each item.
         (sizeof(typename Map::value_type) + sizeof(void*)) * umap.size() +
         // Size of hash buckets.
         umap.bucket_count() * sizeof(void*);
}

}  // namespace rocksdb

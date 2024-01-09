//  Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

// This header establishes compile-time pluggable implementations of hashed
// container structures, so that deployments have the option of minimal
// dependencies with ok performance (e.g. std::unordered_map) or more
// dependencies with optimized performance (e.g. folly::F14FastMap).

#pragma once

#include "rocksdb/rocksdb_namespace.h"

#ifdef USE_FOLLY

#include <folly/container/F14Map.h>
#include <folly/container/F14Set.h>

namespace ROCKSDB_NAMESPACE {

template <typename K, typename V>
using UnorderedMap = folly::F14FastMap<K, V>;

template <typename K, typename V, typename H>
using UnorderedMapH = folly::F14FastMap<K, V, H>;

template <typename K>
using UnorderedSet = folly::F14FastSet<K>;

}  // namespace ROCKSDB_NAMESPACE

#else

#include <unordered_map>
#include <unordered_set>

namespace ROCKSDB_NAMESPACE {

template <typename K, typename V>
using UnorderedMap = std::unordered_map<K, V>;

template <typename K, typename V, typename H>
using UnorderedMapH = std::unordered_map<K, V, H>;

template <typename K>
using UnorderedSet = std::unordered_set<K>;

}  // namespace ROCKSDB_NAMESPACE

#endif

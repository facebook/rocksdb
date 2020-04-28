//  Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <assert.h>
#include <stdint.h>
#ifdef _MSC_VER
#include <intrin.h>
#endif

namespace ROCKSDB_NAMESPACE {

template <typename T>
inline int BitsSetToOne(T v) {
  static_assert(std::is_integral<T>::value, "non-integral type");
#ifdef _MSC_VER
  static_assert(sizeof(T) <= sizeof(uint64_t), "type too big");
  if (sizeof(T) > sizeof(uint32_t)) {
    return static_cast<int>(__popcnt64(static_cast<uint64_t>(v)));
  } else {
    return static_cast<int>(__popcnt(static_cast<uint32_t>(v)));
  }
#else
  static_assert(sizeof(T) <= sizeof(unsigned long long), "type too big");
  if (sizeof(T) > sizeof(unsigned long)) {
    return __builtin_popcountll(static_cast<unsigned long long>(v));
  } else if (sizeof(T) > sizeof(unsigned int)) {
    return __builtin_popcountl(static_cast<unsigned long>(v));
  } else {
    return __builtin_popcount(static_cast<unsigned int>(v));
  }
#endif
}

}  // namespace ROCKSDB_NAMESPACE

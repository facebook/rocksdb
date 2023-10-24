//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <initializer_list>
#include <memory>
#include <type_traits>

#include "rocksdb/rocksdb_namespace.h"

namespace ROCKSDB_NAMESPACE {
// The helper function to assert the move from dynamic_cast<> to
// static_cast<> is correct. This function is to deal with legacy code.
// It is not recommended to add new code to issue class casting. The preferred
// solution is to implement the functionality without a need of casting.
template <class DestClass, class SrcClass>
inline DestClass* static_cast_with_check(SrcClass* x) {
  DestClass* ret = static_cast<DestClass*>(x);
#ifdef ROCKSDB_USE_RTTI
  assert(ret == dynamic_cast<DestClass*>(x));
#endif
  return ret;
}

template <class DestClass, class SrcClass>
inline std::shared_ptr<DestClass> static_cast_with_check(
    std::shared_ptr<SrcClass>&& x) {
#if defined(ROCKSDB_USE_RTTI) && !defined(NDEBUG)
  auto orig_raw = x.get();
#endif
  auto ret = std::static_pointer_cast<DestClass>(std::move(x));
#if defined(ROCKSDB_USE_RTTI) && !defined(NDEBUG)
  assert(ret.get() == dynamic_cast<DestClass*>(orig_raw));
#endif
  return ret;
}

// A wrapper around static_cast for lossless conversion between integral
// types, including enum types. For example, this can be used for converting
// between signed/unsigned or enum type and underlying type without fear of
// stripping away data, now or in the future.
template <typename To, typename From>
inline To lossless_cast(From x) {
  using FromValue = typename std::remove_reference<From>::type;
  static_assert(
      std::is_integral<FromValue>::value || std::is_enum<FromValue>::value,
      "Only works on integral types");
  static_assert(std::is_integral<To>::value || std::is_enum<To>::value,
                "Only works on integral types");
  static_assert(sizeof(To) >= sizeof(FromValue), "Must be lossless");
  return static_cast<To>(x);
}

// For disambiguating a potentially heterogeneous aggregate as a homogeneous
// initializer list. E.g. might be able to write List({x, y}) in some cases
// instead of std::vector<const Widget&>({x, y}).
template <typename T>
inline const std::initializer_list<T>& List(
    const std::initializer_list<T>& list) {
  return list;
}

}  // namespace ROCKSDB_NAMESPACE
